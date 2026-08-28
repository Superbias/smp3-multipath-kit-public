package multipath

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.MultipathInboundOptions](registry, C.TypeMultipath, NewInbound)
}

type serverSession struct {
	id          [16]byte
	mode        helloMode
	destination M.Socksaddr

	streamCore *mpCore
	streamConn net.Conn

	datagramCore *mpDatagramCore
	datagramConn *datagramPacketConn
}

func (s *serverSession) close() {
	if s.streamCore != nil {
		_ = s.streamCore.Close()
	}
	if s.datagramCore != nil {
		_ = s.datagramCore.Close()
	}
}

func (s *serverSession) done() <-chan struct{} {
	if s.mode == helloModeDatagram {
		return s.datagramCore.Done()
	}
	return s.streamCore.Done()
}

func (s *serverSession) attachLeg(id uint8, conn net.Conn, onClose func(error)) error {
	// Each core owns same-ID retirement serialization. A replacement may wait for
	// the old generation workers to retire without blocking the other leg.
	if s.mode == helloModeDatagram {
		return s.datagramCore.addLeg(id, conn, onClose)
	}
	return s.streamCore.addLeg(id, conn, onClose)
}

type Inbound struct {
	inbound.Adapter
	ctx      context.Context
	router   adapter.ConnectionRouterEx
	logger   log.ContextLogger
	listener *listener.Listener
	cfg      coreConfig
	password string

	udpEnabled bool
	udpCfg     datagramConfig

	nonceMu sync.Mutex
	nonces  map[[16]byte]time.Time

	access             sync.Mutex
	closed             bool
	sessions           map[[16]byte]*serverSession
	tombstones         map[[16]byte]time.Time
	lastTombstonePrune time.Time
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathInboundOptions) (adapter.Inbound, error) {
	if options.Password == "" {
		return nil, E.New("missing multipath password")
	}
	if len(options.BandwidthMbps) != 0 && len(options.BandwidthMbps) != 2 {
		return nil, E.New("multipath bandwidth_mbps must contain exactly 2 entries")
	}
	cfg, err := makeCoreConfig(
		options.ActivationThresholdMbps,
		time.Duration(options.ActivationWindow),
		int(options.ChunkSize),
		int(options.QueueFrames),
		options.BandwidthMbps,
		int(options.MaxReorderFrames),
		int(options.MaxInflightFrames),
		time.Duration(options.AckInterval),
		time.Duration(options.RetransmitTimeout),
		time.Duration(options.RecoveryTimeout),
	)
	if err != nil {
		return nil, err
	}
	scheduler, err := parseSchedulerMode(options.SchedulerMode)
	if err != nil {
		return nil, err
	}
	cfg.SchedulerMode = scheduler
	cfg.NotifyPeerOnActivate = true

	udpEnabled := options.UDPMultipath != nil && options.UDPMultipath.Enabled
	udpCfg, err := makeDatagramConfig(options.UDPMultipath, options.BandwidthMbps, cfg.RecoveryTimeout)
	if err != nil {
		return nil, err
	}

	i := &Inbound{
		Adapter:    inbound.NewAdapter(C.TypeMultipath, tag),
		ctx:        ctx,
		router:     router,
		logger:     logger,
		sessions:   make(map[[16]byte]*serverSession),
		tombstones: make(map[[16]byte]time.Time),
		password:   options.Password,
		nonces:     make(map[[16]byte]time.Time),
		cfg:        cfg,
		udpEnabled: udpEnabled,
		udpCfg:     udpCfg,
	}
	i.listener = listener.New(listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP},
		Listen:            options.ListenOptions,
		ConnectionHandler: i,
	})
	return i, nil
}

func (i *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	return i.listener.Start()
}

func (i *Inbound) Close() error {
	i.access.Lock()
	i.closed = true
	sessions := make([]*serverSession, 0, len(i.sessions))
	for _, session := range i.sessions {
		sessions = append(sessions, session)
	}
	i.sessions = make(map[[16]byte]*serverSession)
	i.tombstones = make(map[[16]byte]time.Time)
	i.access.Unlock()
	for _, session := range sessions {
		session.close()
	}
	return i.listener.Close()
}

func (i *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	hello, nonce, err := readHello(conn, i.password)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, E.Cause(err, "read multipath hello"))
		return
	}
	if !i.acceptNonce(nonce) {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("replayed multipath hello"))
		return
	}
	if hello.LegID > 1 {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("invalid multipath leg id: ", hello.LegID))
		return
	}
	if hello.Mode == helloModeDatagram && !i.udpEnabled {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("multipath datagram mode is disabled on server"))
		return
	}
	destination := M.ParseSocksaddr(hello.Destination)
	if !destination.IsValid() || destination.Port == 0 {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("invalid multipath destination: ", hello.Destination))
		return
	}

	// Establish/join independent of path role. Either authenticated leg may win
	// bootstrap; later same-ID transport generations rejoin the same logical core.
	session, created, err := i.createOrJoinSession(ctx, hello, destination, conn, onClose)
	if err != nil {
		if !created {
			i.logger.WarnContext(ctx, "multipath leg ", hello.LegID, " join rejected for ", destination, ": ", err)
		}
		N.CloseOnHandshakeFailure(conn, onClose, err)
		return
	}
	if !created {
		i.logger.InfoContext(ctx, "multipath leg ", hello.LegID, " joined/rejoined ", hello.Mode.String(), " session for ", destination)
		return
	}

	metadata.Inbound = i.Tag()
	metadata.InboundType = i.Type()
	metadata.Destination = destination
	if hello.Mode == helloModeDatagram {
		metadata.Network = N.NetworkUDP
		i.logger.InfoContext(ctx, "multipath datagram session established to ", destination, " on first authenticated leg ", hello.LegID)
		logicalOnClose := N.OnceClose(func(closeErr error) {
			_ = session.datagramCore.Close()
		})
		// Route through sing's native PacketConn interface so per-datagram Socksaddr
		// metadata is preserved without a net.Addr conversion shim.
		i.router.RoutePacketConnectionEx(ctx, newSingDatagramPacketConn(session.datagramConn), metadata, logicalOnClose)
		return
	}

	metadata.Network = N.NetworkTCP
	i.logger.InfoContext(ctx, "multipath stream session established to ", destination, " on first authenticated leg ", hello.LegID)
	logicalOnClose := N.OnceClose(func(closeErr error) {
		// Ordered stream EOF retains r10's graceful tail drain: routed close does
		// not discard response bytes already accepted but not cumulatively ACKed.
		session.streamCore.startGracefulClose(closeErr)
	})
	i.router.RouteConnectionEx(ctx, session.streamConn, metadata, logicalOnClose)
}

// createOrJoinSession is the serialization point for both stream and datagram
// logical sessions. A session ID is bound to one mode and one destination for
// its entire lifetime; a v4 stream leg can therefore never join a v5 datagram
// core (or vice versa).
func (i *Inbound) createOrJoinSession(ctx context.Context, hello helloMessage, destination M.Socksaddr, conn net.Conn, onClose N.CloseHandlerFunc) (*serverSession, bool, error) {
	i.access.Lock()
	if i.closed {
		i.access.Unlock()
		return nil, false, errCoreClosed
	}
	now := time.Now()
	i.pruneTombstonesLocked(now)
	if expires, retired := i.tombstones[hello.Session]; retired {
		if now.Before(expires) {
			i.access.Unlock()
			return nil, false, E.New("multipath session id is retired")
		}
		delete(i.tombstones, hello.Session)
	}
	if existing := i.sessions[hello.Session]; existing != nil {
		i.access.Unlock()
		if existing.mode != hello.Mode {
			return existing, false, E.New("multipath session mode mismatch")
		}
		if existing.destination.String() != destination.String() {
			return existing, false, E.New("multipath session destination mismatch")
		}
		if err := existing.attachLeg(hello.LegID, conn, onClose); err != nil {
			return existing, false, err
		}
		return existing, false, nil
	}

	session := &serverSession{id: hello.Session, mode: hello.Mode, destination: destination}
	if hello.Mode == helloModeDatagram {
		cfg := i.udpCfg
		if i.logger != nil {
			cfg.OnLegDown = func(id uint8, legErr error) {
				i.logger.WarnContext(ctx, "multipath datagram server leg ", id, " down for ", destination, ": ", legErr)
			}
		}
		core, packetConn := newDatagramCore(cfg)
		session.datagramCore = core
		session.datagramConn = packetConn
		if err := core.addLeg(hello.LegID, conn, onClose); err != nil {
			i.access.Unlock()
			_ = packetConn.Close()
			_ = core.Close()
			return nil, true, err
		}
	} else {
		cfg := i.cfg
		if i.logger != nil {
			cfg.OnFutureAck = func(next, max, count uint64) {
				i.logger.WarnContext(ctx, "multipath server ignored future ACK for ", destination, ": next=", next, " tx_seq=", max, " count=", count)
			}
			cfg.OnActivate = func() {
				i.logger.InfoContext(ctx, "multipath server booster activated for ", destination)
			}
			cfg.OnLegDown = func(id uint8, legErr error) {
				i.logger.WarnContext(ctx, "multipath stream server leg ", id, " down for ", destination, ": ", legErr)
			}
		}
		core, appConn := newCore(cfg)
		session.streamCore = core
		session.streamConn = appConn
		if err := core.addLeg(hello.LegID, conn, onClose); err != nil {
			i.access.Unlock()
			_ = appConn.Close()
			_ = core.Close()
			return nil, true, err
		}
	}

	// Publish only after the creator leg is fully installed, so a concurrent
	// joiner can never observe a zero-transport logical session.
	i.sessions[hello.Session] = session
	i.access.Unlock()

	go func() {
		<-session.done()
		i.removeSession(hello.Session, session)
	}()
	return session, true, nil
}

func (i *Inbound) removeSession(id [16]byte, session *serverSession) {
	i.access.Lock()
	if current := i.sessions[id]; current == session {
		delete(i.sessions, id)
		// Retire completed IDs for the full HELLO replay horizon so delayed old
		// authenticated legs cannot accidentally create a second logical session.
		if i.tombstones == nil {
			i.tombstones = make(map[[16]byte]time.Time)
		}
		i.tombstones[id] = time.Now().Add(2 * helloSkew)
	}
	i.access.Unlock()
}

func (i *Inbound) pruneTombstonesLocked(now time.Time) {
	if i.tombstones == nil {
		i.tombstones = make(map[[16]byte]time.Time)
	}
	if !i.lastTombstonePrune.IsZero() && now.Sub(i.lastTombstonePrune) < 30*time.Second {
		return
	}
	for id, expires := range i.tombstones {
		if !now.Before(expires) {
			delete(i.tombstones, id)
		}
	}
	i.lastTombstonePrune = now
}

func (i *Inbound) acceptNonce(nonce [16]byte) bool {
	now := time.Now()
	i.nonceMu.Lock()
	defer i.nonceMu.Unlock()
	for k, t := range i.nonces {
		if now.Sub(t) > 2*helloSkew {
			delete(i.nonces, k)
		}
	}
	if _, ok := i.nonces[nonce]; ok {
		return false
	}
	i.nonces[nonce] = now
	return true
}

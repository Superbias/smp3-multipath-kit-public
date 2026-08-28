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
	destination M.Socksaddr
	core        *mpCore
	appConn     net.Conn
}

type Inbound struct {
	inbound.Adapter
	ctx      context.Context
	router   adapter.ConnectionRouterEx
	logger   log.ContextLogger
	listener *listener.Listener
	cfg      coreConfig
	password string
	nonceMu  sync.Mutex
	nonces   map[[16]byte]time.Time

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
	cfg.NotifyPeerOnActivate = true
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
		session.core.Close()
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
	destination := M.ParseSocksaddr(hello.Destination)
	if !destination.IsValid() || destination.Port == 0 {
		N.CloseOnHandshakeFailure(conn, onClose, E.New("invalid multipath destination: ", hello.Destination))
		return
	}

	// Session establishment is intentionally independent from path role. The
	// client still decides when leg 1 may be dialed and still treats leg 0 as the
	// preferred path, but once an authenticated HELLO reaches the server either
	// leg is allowed to create the logical core. This removes cross-carrier HELLO
	// ordering as a correctness dependency: a faster Hy2/Snell leg 1 no longer
	// waits for, races with, or gets rejected behind a slower line-path leg 0.
	session, created, err := i.createOrJoinSession(ctx, hello, destination, conn, onClose)
	if err != nil {
		if !created {
			i.logger.WarnContext(ctx, "multipath leg ", hello.LegID, " join rejected for ", destination, ": ", err)
		}
		N.CloseOnHandshakeFailure(conn, onClose, err)
		return
	}
	if !created {
		i.logger.InfoContext(ctx, "multipath leg ", hello.LegID, " joined/rejoined session for ", destination)
		return
	}

	metadata.Inbound = i.Tag()
	metadata.InboundType = i.Type()
	metadata.Destination = destination
	i.logger.InfoContext(ctx, "multipath session established to ", destination, " on first authenticated leg ", hello.LegID)
	logicalOnClose := N.OnceClose(func(closeErr error) {
		// Ordinary routed EOF/close must not discard response bytes that have
		// already been accepted into SMP3 but are still awaiting cumulative ACKs.
		// The session remains registered until core.Done(), allowing any transport
		// repair needed during the graceful tail drain to rejoin the same core.
		session.core.startGracefulClose(closeErr)
	})
	i.router.RouteConnectionEx(ctx, session.appConn, metadata, logicalOnClose)
}

// createOrJoinSession performs the only create-vs-join decision for a logical
// session. The sessions-map mutex is the creation serialization point: if leg 0
// and leg 1 arrive concurrently, exactly one of them publishes the core and the
// other observes that same fully initialized core and joins it. Holding the map
// lock through newCore/addLeg is deliberate; addLeg is local/non-blocking and it
// prevents a joiner from observing a half-created session.
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
		if existing.destination.String() != destination.String() {
			return existing, false, E.New("multipath session destination mismatch")
		}
		if err := existing.attachLeg(hello.LegID, conn, onClose); err != nil {
			return existing, false, err
		}
		return existing, false, nil
	}

	cfg := i.cfg
	if i.logger != nil {
		cfg.OnFutureAck = func(next, max, count uint64) {
			i.logger.WarnContext(ctx, "multipath server ignored future ACK for ", destination, ": next=", next, " tx_seq=", max, " count=", count)
		}
		cfg.OnActivate = func() {
			i.logger.InfoContext(ctx, "multipath server booster activated for ", destination)
		}
		cfg.OnLegDown = func(id uint8, legErr error) {
			i.logger.WarnContext(ctx, "multipath server leg ", id, " down for ", destination, ": ", legErr)
		}
	}
	core, appConn := newCore(cfg)
	session := &serverSession{
		id:          hello.Session,
		destination: destination,
		core:        core,
		appConn:     appConn,
	}
	if err := core.addLeg(hello.LegID, conn, onClose); err != nil {
		i.access.Unlock()
		_ = appConn.Close()
		_ = core.Close()
		return nil, true, err
	}
	// Publish only after the creator leg is installed. A concurrent arrival can
	// therefore never join a core that has zero transport legs.
	i.sessions[hello.Session] = session
	i.access.Unlock()

	go func() {
		<-core.Done()
		i.removeSession(hello.Session, session)
	}()
	return session, true, nil
}

func (s *serverSession) attachLeg(id uint8, conn net.Conn, onClose func(error)) error {
	// mpCore owns attachment serialization. Avoid a session-wide join mutex here:
	// a same-ID replacement may legitimately wait for its old workers to retire,
	// and that wait must not head-of-line block the other leg from joining.
	return s.core.addLeg(id, conn, onClose)
}

func (i *Inbound) removeSession(id [16]byte, session *serverSession) {
	i.access.Lock()
	if current := i.sessions[id]; current == session {
		delete(i.sessions, id)
		// A completed wire-v4 session ID must not be allowed to create another
		// core when a delayed authenticated leg arrives after teardown. New logical
		// connections use cryptographically random IDs, so retiring the old ID for
		// the full HELLO replay horizon has no effect on legitimate new sessions.
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
	// Avoid an O(number-of-recent-sessions) scan on every short connection. A
	// direct lookup above still expires the requested ID immediately.
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

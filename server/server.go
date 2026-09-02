package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

var (
	ErrServerClosed       = errors.New("standalone SMP3 server closed")
	errSessionRetired     = errors.New("multipath session id is retired")
	errSessionMode        = errors.New("multipath session mode mismatch")
	errSessionDestination = errors.New("multipath session destination mismatch")
)

const helloReplayTTL = 180 * time.Second

type Server struct {
	cfg    Config
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	access             sync.Mutex
	closed             bool
	listener           net.Listener
	listeners          []net.Listener
	sidecarListeners   map[net.Listener]struct{}
	sessions           map[smp3core.SessionID]*serverSession
	tombstones         map[smp3core.SessionID]time.Time
	lastTombstonePrune time.Time

	nonceMu sync.Mutex
	nonces  map[[16]byte]time.Time

	acceptWG  sync.WaitGroup
	handlerWG sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
}

func New(cfg Config, logger *slog.Logger) (*Server, error) {
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:              cfg,
		logger:           loggerOrDefault(logger),
		ctx:              ctx,
		cancel:           cancel,
		sessions:         make(map[smp3core.SessionID]*serverSession),
		tombstones:       make(map[smp3core.SessionID]time.Time),
		nonces:           make(map[[16]byte]time.Time),
		sidecarListeners: make(map[net.Listener]struct{}),
		closeDone:        make(chan struct{}),
	}, nil
}

func (s *Server) Config() Config { return s.cfg }

func (s *Server) Start() error {
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		return ErrServerClosed
	}
	if s.listener != nil {
		s.access.Unlock()
		return errors.New("server already started")
	}
	s.access.Unlock()

	type listenerConfig struct {
		address string
		sidecar bool
	}
	addresses := []listenerConfig{{address: s.cfg.Listen}}
	for _, address := range s.cfg.Listeners {
		addresses = append(addresses, listenerConfig{address: address})
	}
	for _, address := range s.cfg.SidecarListeners {
		addresses = append(addresses, listenerConfig{address: address, sidecar: true})
	}
	listeners := make([]net.Listener, 0, len(addresses))
	sidecarListeners := make(map[net.Listener]struct{})
	for _, configured := range addresses {
		listener, err := net.Listen("tcp", configured.address)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen %s: %w", configured.address, err)
		}
		listeners = append(listeners, listener)
		if configured.sidecar {
			sidecarListeners[listener] = struct{}{}
		}
	}
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		return ErrServerClosed
	}
	s.listener = listeners[0]
	s.listeners = listeners
	s.sidecarListeners = sidecarListeners
	s.access.Unlock()
	s.logger.Info("standalone SMP3 server started", "address", listeners[0].Addr().String(), "listeners", len(listeners))
	for _, listener := range listeners {
		s.acceptWG.Add(1)
		_, sidecar := sidecarListeners[listener]
		go s.acceptLoop(listener, sidecar)
	}
	return nil
}

func (s *Server) Addr() net.Addr {
	s.access.Lock()
	defer s.access.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// ListenerAddrs returns all active listener addresses in config order. The
// first address is the backward-compatible primary returned by Addr.
func (s *Server) ListenerAddrs() []string {
	s.access.Lock()
	defer s.access.Unlock()
	addresses := make([]string, 0, len(s.listeners))
	for _, listener := range s.listeners {
		addresses = append(addresses, listener.Addr().String())
	}
	if len(addresses) == 0 && s.listener != nil {
		addresses = append(addresses, s.listener.Addr().String())
	}
	return addresses
}

func (s *Server) acceptLoop(listener net.Listener, sidecar bool) {
	defer s.acceptWG.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Error("SMP3 listener accept failed", "error", err)
			continue
		}
		s.handlerWG.Add(1)
		go func() {
			defer s.handlerWG.Done()
			s.handleCarrier(s.ctx, conn, sidecar)
		}()
	}
}

func (s *Server) isClosed() bool {
	s.access.Lock()
	closed := s.closed
	s.access.Unlock()
	return closed
}

func (s *Server) handleCarrier(ctx context.Context, conn net.Conn, sidecar bool) {
	if err := conn.SetReadDeadline(time.Now().Add(s.cfg.HelloReadTimeout.Time())); err != nil {
		s.logger.Debug("failed to set HELLO read deadline", "error", err)
	}
	hello, err := smp3core.ReadHelloAt(conn, []byte(s.cfg.Password), time.Now())
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		s.rejectCarrier(conn, "hello", err)
		return
	}
	if !s.acceptNonce(hello.Nonce) {
		s.rejectCarrier(conn, "replayed_nonce", errors.New("replayed multipath hello"))
		return
	}
	if hello.Mode == smp3core.ModeDatagram && !s.cfg.UDP.Enabled {
		s.rejectCarrier(conn, "datagram_disabled", errors.New("multipath datagram mode is disabled on server"))
		return
	}
	destination, err := normalizeDestination(hello.Destination)
	if err != nil {
		s.rejectCarrier(conn, "destination", err)
		return
	}
	session, created, err := s.admitSession(hello, destination)
	if err != nil {
		s.rejectCarrier(conn, "session", err)
		return
	}
	if sidecar {
		if err := session.reserveLeg(hello.LegID); err != nil {
			s.rejectCarrier(conn, "session", err)
			if created {
				session.close()
			}
			return
		}
		defer session.releaseLeg(hello.LegID)
		if err := writeSidecarReadyV1(conn, hello, []byte(s.cfg.Password)); err != nil {
			s.rejectCarrier(conn, "ready", err)
			if created {
				session.close()
			}
			return
		}
	}
	if err := session.attachLeg(hello.LegID, conn); err != nil {
		s.rejectCarrier(conn, "session", err)
		if created {
			session.close()
		}
		return
	}
	if !created {
		s.logger.Info("multipath leg joined/rejoined", "session", sessionLogID(hello.SessionID), "leg", hello.LegID, "mode", modeName(hello.Mode))
		return
	}
	s.logger.Info("multipath session created", "session", sessionLogID(hello.SessionID), "leg", hello.LegID, "mode", modeName(hello.Mode), "destination", destination)
	if hello.Mode == smp3core.ModeDatagram {
		if err := s.startDatagramHost(session); err != nil {
			s.logger.Error("datagram host setup failed", "session", sessionLogID(hello.SessionID), "error", err)
			session.close()
		}
		return
	}
	if err := s.startStreamHost(session); err != nil {
		s.logger.Error("stream target dial failed", "session", sessionLogID(hello.SessionID), "destination", destination, "error", err)
		session.close()
	}
}

func (s *Server) rejectCarrier(conn net.Conn, class string, err error) {
	s.logger.Warn("multipath HELLO rejected", "class", class, "error", err)
	_ = conn.Close()
}

func modeName(mode smp3core.HelloMode) string {
	if mode == smp3core.ModeDatagram {
		return "datagram"
	}
	return "stream"
}

func (s *Server) acceptNonce(nonce [16]byte) bool {
	now := time.Now()
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	for key, created := range s.nonces {
		if now.Sub(created) > helloReplayTTL {
			delete(s.nonces, key)
		}
	}
	if _, exists := s.nonces[nonce]; exists {
		return false
	}
	s.nonces[nonce] = now
	return true
}

func (s *Server) createOrJoinSession(hello smp3core.Hello, destination string, conn net.Conn) (*serverSession, bool, error) {
	session, created, err := s.admitSession(hello, destination)
	if err != nil {
		return nil, false, err
	}
	if err := session.attachLeg(hello.LegID, conn); err != nil {
		if created {
			session.close()
		}
		return session, created, err
	}
	return session, created, nil
}

func (s *Server) admitSession(hello smp3core.Hello, destination string) (*serverSession, bool, error) {
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		return nil, false, ErrServerClosed
	}
	s.pruneTombstonesLocked(time.Now())
	if expires, exists := s.tombstones[hello.SessionID]; exists && time.Now().Before(expires) {
		s.access.Unlock()
		return nil, false, errSessionRetired
	}
	if existing := s.sessions[hello.SessionID]; existing != nil {
		s.access.Unlock()
		if existing.mode != hello.Mode {
			return existing, false, errSessionMode
		}
		if existing.destination != destination {
			return existing, false, errSessionDestination
		}
		return existing, false, nil
	}

	session := s.newSession(hello, destination)
	s.sessions[hello.SessionID] = session
	s.access.Unlock()
	s.startSessionWatcher(session)
	return session, true, nil
}

func (s *Server) newSession(hello smp3core.Hello, destination string) *serverSession {
	session := &serverSession{id: hello.SessionID, mode: hello.Mode, destination: destination}
	if hello.Mode == smp3core.ModeDatagram {
		cfg := smp3core.DatagramConfig{
			Mode:                       datagramMode(s.cfg.UDP.Mode),
			QueueFrames:                s.cfg.UDP.QueueFrames,
			MaxDatagramSize:            s.cfg.UDP.MaxDatagramSize,
			DedupWindow:                s.cfg.UDP.DedupWindow,
			IdleTimeout:                s.cfg.UDP.IdleTimeout.Time(),
			RecoveryTimeout:            s.cfg.RecoveryTimeout.Time(),
			AdaptiveQueueDelay:         s.cfg.UDP.AdaptiveQueueDelay.Time(),
			AdaptiveDuplicateThreshold: s.cfg.UDP.AdaptiveDuplicateThreshold,
			OnLegDown: func(id smp3core.LegID, err error) {
				s.logger.Warn("multipath datagram leg down", "session", sessionLogID(hello.SessionID), "leg", id, "error", err)
			},
		}
		session.dgram = smp3core.NewDatagramEngine(cfg)
		return session
	}
	cfg := smp3core.StreamConfig{
		SchedulerMode:        streamSchedulerMode(s.cfg.Stream.SchedulerMode),
		ChunkSize:            s.cfg.Stream.ChunkSize,
		QueueFrames:          s.cfg.Stream.QueueFrames,
		ThresholdBytesPS:     uint64(s.cfg.Stream.ActivationThresholdMbps) * 125000,
		ActivationWindow:     s.cfg.Stream.ActivationWindow.Time(),
		BandwidthMbps:        append([]uint32(nil), s.cfg.Stream.BandwidthMbps...),
		MaxReorderFrames:     s.cfg.Stream.MaxReorderFrames,
		MaxInflightFrames:    s.cfg.Stream.MaxInflightFrames,
		AckInterval:          s.cfg.Stream.AckInterval.Time(),
		RetransmitTimeout:    s.cfg.Stream.RetransmitTimeout.Time(),
		RecoveryTimeout:      s.cfg.RecoveryTimeout.Time(),
		NotifyPeerOnActivate: true,
		OnActivate:           func() { s.logger.Info("multipath stream activated", "session", sessionLogID(hello.SessionID)) },
		OnLegDown: func(id uint8, err error) {
			s.logger.Warn("multipath stream leg down", "session", sessionLogID(hello.SessionID), "leg", id, "error", err)
		},
		OnFutureAck: func(next, max, count uint64) {
			s.logger.Warn("multipath future ACK ignored", "session", sessionLogID(hello.SessionID), "next", next, "tx_seq", max, "count", count)
		},
	}
	session.stream, session.streamApp = smp3core.NewStreamEngine(cfg)
	return session
}

func (s *Server) startSessionWatcher(session *serverSession) {
	session.goWorker(func() {
		<-session.done()
		session.releaseHostResources()
		s.removeSession(session.id, session)
	})
}

func (s *Server) removeSession(id smp3core.SessionID, session *serverSession) {
	s.access.Lock()
	defer s.access.Unlock()
	if current := s.sessions[id]; current == session {
		delete(s.sessions, id)
		s.tombstones[id] = time.Now().Add(helloReplayTTL)
		s.logger.Debug("multipath session retired", "session", sessionLogID(id))
	}
}

func (s *Server) pruneTombstonesLocked(now time.Time) {
	if !s.lastTombstonePrune.IsZero() && now.Sub(s.lastTombstonePrune) < 30*time.Second {
		return
	}
	for id, expires := range s.tombstones {
		if !now.Before(expires) {
			delete(s.tombstones, id)
		}
	}
	s.lastTombstonePrune = now
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.access.Lock()
		s.closed = true
		listeners := append([]net.Listener(nil), s.listeners...)
		if len(listeners) == 0 && s.listener != nil {
			listeners = []net.Listener{s.listener}
		}
		sessions := make([]*serverSession, 0, len(s.sessions))
		for _, session := range s.sessions {
			sessions = append(sessions, session)
		}
		s.sessions = make(map[smp3core.SessionID]*serverSession)
		s.tombstones = make(map[smp3core.SessionID]time.Time)
		s.access.Unlock()
		s.cancel()

		for _, session := range sessions {
			session.close()
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		s.acceptWG.Wait()
		s.handlerWG.Wait()
		for _, session := range sessions {
			session.waitWorkers()
		}
		close(s.closeDone)
	})
	<-s.closeDone
	return nil
}

func (s *Server) SessionCount() int {
	s.access.Lock()
	defer s.access.Unlock()
	return len(s.sessions)
}

func (s *Server) TombstoneCount() int {
	s.access.Lock()
	defer s.access.Unlock()
	return len(s.tombstones)
}

func (s *Server) String() string {
	if addr := s.Addr(); addr != nil {
		return addr.String()
	}
	return ""
}

var _ io.Closer = (*Server)(nil)

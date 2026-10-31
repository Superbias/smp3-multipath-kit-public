package client

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type streamSession struct {
	client      *Client
	runtime     atomic.Pointer[clientRuntimeConfig]
	id          smp3core.SessionID
	engine      *smp3core.StreamEngine
	app         net.Conn
	destination string
	ctx         context.Context
	cancel      context.CancelFunc

	closeOnce     sync.Once
	repairMu      sync.Mutex
	repairing     []bool
	startupMu     sync.Mutex
	startupActive bool
	v6Accepted    []*atomic.Bool
	v7Accepted    []*atomic.Bool
	desiredLegs   [smp3core.MaxStreamLegIDCount]atomic.Bool
	legStateReady bool
}

type streamBootstrapResult struct {
	id       uint8
	conn     net.Conn
	pathAuth *smp3core.StreamPathAuth
	err      error
}

type streamV6TransportError struct{ err error }

func (e streamV6TransportError) Error() string { return e.err.Error() }
func (e streamV6TransportError) Unwrap() error { return e.err }

type streamV7TransportError struct{ err error }

func (e streamV7TransportError) Error() string { return e.err.Error() }
func (e streamV7TransportError) Unwrap() error { return e.err }

func newStreamSession(client *Client, destination string) (*streamSession, error) {
	runtime := client.currentRuntime()
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("generate stream session id: %w", err)
	}
	ctx, cancel := context.WithCancel(client.ctx)
	legCount := len(runtime.cfg.SMP3.Routes.entries())
	var session *streamSession
	onActivate := func() {
		if session != nil {
			slog.Default().Info("SMP3 stream aggregation activated")
			if !session.startupInFlight() {
				for id := 1; id < session.configuredLegCount(); id++ {
					session.scheduleRepair(uint8(id))
				}
			}
		}
	}
	onLegDown := func(id uint8, err error) {
		slog.Default().Warn("SMP3 carrier leg down", "leg", id, "error", err)
		if session != nil && !session.startupInFlight() {
			session.scheduleRepair(id)
		}
	}
	config := runtime.cfg.SMP3.streamConfig(onActivate, onLegDown)
	config.DrainEventObserver = func(event smp3core.StreamDrainEvent) {
		slog.Default().Info("SMP3 coordinated drain event",
			"stage", event.Stage, "session", event.SessionID, "direction", event.Direction,
			"role", event.Role, "leg", event.LegID, "generation", event.Generation,
			"operation", event.DrainOperationID, "counter", event.Counter,
			"local_ack_frontier", event.LocalAckFrontier, "peer_ack_frontier", event.PeerAckFrontier,
			"local_tx_outstanding_frames", event.LocalTXOutstandingFrames,
			"local_tx_outstanding_bytes", event.LocalTXOutstandingBytes,
			"peer_tx_outstanding_frames", event.PeerTXOutstandingFrames,
			"peer_tx_outstanding_bytes", event.PeerTXOutstandingBytes,
			"rx_pending_frames", event.RXPendingFrames, "rx_pending_bytes", event.RXPendingBytes,
			"local_quiesce_started", event.LocalQuiesceStarted,
			"local_quiesce_completed", event.LocalQuiesceCompleted,
			"peer_quiesce_confirmed", event.PeerQuiesceConfirmed,
			"local_ready", event.LocalReady, "peer_ready", event.PeerReady,
			"commit_sent", event.CommitSent, "commit_requested", event.CommitRequested,
			"commit_ack", event.CommitAck, "target_closed", event.TargetClosed,
			"target_active_binding", event.TargetActiveBinding,
			"retry_written_bytes", event.RetryWrittenBytes, "terminal", event.Terminal)
	}
	config.CapacityProvider = client.streamCapacityProviderFor(runtime)
	engine, app := smp3core.NewStreamEngine(config)
	session = &streamSession{
		client: client, id: id, engine: engine, app: app, destination: destination, ctx: ctx, cancel: cancel,
		repairing: make([]bool, smp3core.MaxStreamLegIDCount), v6Accepted: make([]*atomic.Bool, smp3core.MaxStreamLegIDCount),
		v7Accepted: make([]*atomic.Bool, smp3core.MaxStreamLegIDCount),
	}
	for i := range session.v6Accepted {
		session.v6Accepted[i] = new(atomic.Bool)
		session.v7Accepted[i] = new(atomic.Bool)
	}
	session.legStateReady = true
	for i := 0; i < legCount; i++ {
		session.desiredLegs[i].Store(true)
	}
	session.watchEngine()
	if runtime.cfg.SMP3.Stream.StartupPolicy == "preferred" {
		return session.startPreferred()
	}
	return session.startFirstReady()
}

func (s *streamSession) watchEngine() {
	go func() {
		select {
		case <-s.engine.Done():
			s.cancel()
		case <-s.ctx.Done():
			_ = s.engine.Close()
		}
	}()
}

func (s *streamSession) setStartupActive(active bool) {
	s.startupMu.Lock()
	s.startupActive = active
	s.startupMu.Unlock()
}

func (s *streamSession) startupInFlight() bool {
	s.startupMu.Lock()
	active := s.startupActive
	s.startupMu.Unlock()
	return active
}

func (s *streamSession) startFirstReady() (*streamSession, error) {
	conn, pathAuth, err := s.dialLeg(0)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("connect stream leg0: %w", err)
	}
	if err := attachStreamLeg(s.engine, 0, conn, pathAuth); err != nil {
		_ = conn.Close()
		_ = s.Close()
		return nil, fmt.Errorf("attach stream leg0: %w", err)
	}
	slog.Default().Info("SMP3 carrier leg connected", "leg", 0)
	if s.currentRuntime().hostCarrier != nil {
		for id := 1; id < s.configuredLegCount(); id++ {
			leg, pathAuth, err := s.dialLeg(uint8(id))
			if err != nil {
				_ = s.Close()
				return nil, fmt.Errorf("connect stream leg%d: %w", id, err)
			}
			if err := attachStreamLeg(s.engine, smp3core.LegID(id), leg, pathAuth); err != nil {
				_ = leg.Close()
				_ = s.Close()
				return nil, fmt.Errorf("attach stream leg%d: %w", id, err)
			}
			slog.Default().Info("SMP3 carrier leg connected", "leg", id)
		}
	}
	return s, nil
}

func (s *streamSession) startPreferred() (*streamSession, error) {
	s.setStartupActive(true)
	legCount := s.configuredLegCount()
	results := make(chan streamBootstrapResult, legCount)
	for id := 0; id < legCount; id++ {
		go func(id uint8) {
			conn, pathAuth, err := s.dialLeg(id)
			results <- streamBootstrapResult{id: id, conn: conn, pathAuth: pathAuth, err: err}
		}(uint8(id))
	}

	preferred := uint8(s.currentRuntime().cfg.SMP3.Stream.StartupPreferredLeg)
	var causes []error
	completed := 0
	for completed < legCount {
		select {
		case <-s.ctx.Done():
			_ = s.engine.Close()
			s.setStartupActive(false)
			go drainBootstrapResults(results, legCount-completed)
			return nil, s.ctx.Err()
		case result := <-results:
			completed++
			if result.err != nil {
				causes = append(causes, fmt.Errorf("connect stream leg%d: %w", result.id, result.err))
				if result.id == preferred {
					s.engine.MarkStartupLegTerminalUnavailable(smp3core.LegID(result.id))
				}
				continue
			}
			if s.isClosed() || s.engine.Finalizing() {
				_ = result.conn.Close()
				continue
			}
			if err := attachStreamLeg(s.engine, smp3core.LegID(result.id), result.conn, result.pathAuth); err != nil {
				_ = result.conn.Close()
				causes = append(causes, fmt.Errorf("attach stream leg%d: %w", result.id, err))
				if result.id == preferred && !s.isClosed() {
					s.engine.MarkStartupLegTerminalUnavailable(smp3core.LegID(result.id))
				}
				continue
			}
			slog.Default().Info("SMP3 carrier leg connected", "leg", result.id)
			go s.finishPreferredCompanions(results, legCount-completed, preferred)
			if completed == legCount {
				s.setStartupActive(false)
			}
			return s, nil
		}
	}

	_ = s.Close()
	s.setStartupActive(false)
	if len(causes) == 0 {
		return nil, errors.New("preferred stream startup failed")
	}
	return nil, errors.Join(causes...)
}

func drainBootstrapResults(results <-chan streamBootstrapResult, remaining int) {
	for i := 0; i < remaining; i++ {
		result := <-results
		if result.conn != nil {
			_ = result.conn.Close()
		}
	}
}

func (s *streamSession) finishPreferredCompanions(results <-chan streamBootstrapResult, remaining int, preferred uint8) {
	if remaining == 0 {
		s.setStartupActive(false)
		return
	}
	defer s.setStartupActive(false)
	for i := 0; i < remaining; i++ {
		result := <-results
		if result.err != nil {
			if result.id == preferred {
				s.engine.MarkStartupLegTerminalUnavailable(smp3core.LegID(result.id))
			}
			continue
		}
		if s.isClosed() || s.engine.Finalizing() || !s.wantsLeg(result.id) || s.engine.HasLeg(smp3core.LegID(result.id)) {
			_ = result.conn.Close()
			continue
		}
		if err := attachStreamLeg(s.engine, smp3core.LegID(result.id), result.conn, result.pathAuth); err != nil {
			_ = result.conn.Close()
		}
	}
}

func (s *streamSession) configuredLegCount() int {
	if s == nil || s.client == nil {
		return 0
	}
	runtime := s.currentRuntime()
	if runtime == nil {
		return 0
	}
	count := len(runtime.cfg.SMP3.Routes.entries())
	if count == 0 {
		return 2
	}
	return count
}

func (s *streamSession) ensureLegState() {
	count := s.configuredLegCount()
	s.repairMu.Lock()
	if !s.legStateReady {
		s.repairing = make([]bool, smp3core.MaxStreamLegIDCount)
		s.v6Accepted = make([]*atomic.Bool, smp3core.MaxStreamLegIDCount)
		s.v7Accepted = make([]*atomic.Bool, smp3core.MaxStreamLegIDCount)
		for i := range s.v6Accepted {
			s.v6Accepted[i] = new(atomic.Bool)
			s.v7Accepted[i] = new(atomic.Bool)
		}
		for i := 0; i < count; i++ {
			s.desiredLegs[i].Store(true)
		}
		s.legStateReady = true
	}
	s.repairMu.Unlock()
}

func (s *streamSession) wantsLeg(id uint8) bool {
	return int(id) < len(s.desiredLegs) && s.desiredLegs[id].Load()
}

func (s *streamSession) setDesiredLeg(id uint8, desired bool) {
	if int(id) < len(s.desiredLegs) {
		s.desiredLegs[id].Store(desired)
	}
}

func (s *streamSession) legGeneration(id uint8) uint64 {
	if s == nil || s.engine == nil {
		return 0
	}
	generation, _ := s.engine.LegGeneration(smp3core.LegID(id))
	return generation
}

func (s *streamSession) markV6Accepted(id uint8) {
	s.ensureLegState()
	if int(id) < len(s.v6Accepted) {
		s.repairMu.Lock()
		accepted := s.v6Accepted[id]
		s.repairMu.Unlock()
		accepted.Store(true)
	}
}

func (s *streamSession) v6WasAccepted(id uint8) bool {
	s.ensureLegState()
	if int(id) >= len(s.v6Accepted) {
		return false
	}
	s.repairMu.Lock()
	accepted := s.v6Accepted[id]
	s.repairMu.Unlock()
	return accepted.Load()
}

func (s *streamSession) markV7Accepted(id uint8) {
	s.ensureLegState()
	if int(id) < len(s.v7Accepted) {
		s.repairMu.Lock()
		accepted := s.v7Accepted[id]
		s.repairMu.Unlock()
		accepted.Store(true)
	}
}

func (s *streamSession) v7WasAccepted(id uint8) bool {
	s.ensureLegState()
	if int(id) >= len(s.v7Accepted) {
		return false
	}
	s.repairMu.Lock()
	accepted := s.v7Accepted[id]
	s.repairMu.Unlock()
	return accepted.Load()
}

func newSessionID() (smp3core.SessionID, error) {
	var id smp3core.SessionID
	if _, err := cryptorand.Read(id[:]); err != nil {
		return id, err
	}
	return id, nil
}

func (s *streamSession) dialLeg(id uint8) (net.Conn, *smp3core.StreamPathAuth, error) {
	return s.dialLegWithRuntime(id, s.currentRuntime())
}

func (s *streamSession) dialLegWithRuntime(id uint8, runtime *clientRuntimeConfig) (net.Conn, *smp3core.StreamPathAuth, error) {
	if runtime == nil {
		return nil, nil, errors.New("client runtime config unavailable")
	}
	if int(id) >= s.configuredLegCount() {
		if int(id) >= len(runtime.cfg.SMP3.Routes.entries()) {
			return nil, nil, errors.New("invalid stream leg")
		}
	}
	pathAware := runtime.cfg.SMP3.Stream.ExperimentalPathAware
	if runtime.hostCarrier != nil {
		openFresh := func() (net.Conn, error) {
			carrier, err := runtime.hostCarrier.Open(s.ctx, s.id, id)
			if err != nil {
				return nil, err
			}
			carrier, err = s.client.secureCarrierWithRuntime(s.ctx, carrier, runtime)
			if err != nil {
				_ = carrier.Close()
				return nil, err
			}
			return carrier, nil
		}
		conn, err := openFresh()
		if err != nil {
			return nil, nil, fmt.Errorf("host carrier OPEN leg %d: %w", id, err)
		}
		conn, auth, err := s.negotiateCarrierWithCompatibilityFallback(conn, openFresh, id, pathAware, runtime)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, nil, fmt.Errorf("host carrier handshake leg %d: %w", id, err)
		}
		return conn, auth, nil
	}
	route, ok := runtime.cfg.SMP3.Routes.leg(id)
	if !ok {
		return nil, nil, fmt.Errorf("no route configured for stream leg %d", id)
	}
	names := []string{route.Address}
	if route.Fallback != "" {
		names = append(names, route.Fallback)
	}
	var causes []error
	upstream := runtime.cfg.effectiveUpstream(id)
	for _, endpoint := range names {
		openFresh := func() (net.Conn, error) {
			carrier, err := dialUpstream(s.ctx, upstream, endpoint)
			if err != nil {
				return nil, err
			}
			carrier, err = s.client.secureCarrierWithRuntime(s.ctx, carrier, runtime)
			if err != nil {
				_ = carrier.Close()
				return nil, err
			}
			return carrier, nil
		}
		conn, err := openFresh()
		if err != nil {
			if ctxErr := s.ctx.Err(); ctxErr != nil {
				return nil, nil, ctxErr
			}
			causes = append(causes, fmt.Errorf("%s: %w", endpoint, err))
			continue
		}
		conn, auth, handshakeErr := s.negotiateCarrierWithCompatibilityFallback(conn, openFresh, id, pathAware, runtime)
		if handshakeErr != nil {
			if conn != nil {
				_ = conn.Close()
			}
			causes = append(causes, fmt.Errorf("%s stream handshake: %w", endpoint, handshakeErr))
			continue
		}
		if ctxErr := s.ctx.Err(); ctxErr != nil {
			_ = conn.Close()
			return nil, nil, ctxErr
		}
		return conn, auth, nil
	}
	return nil, nil, errors.Join(causes...)
}

// negotiateCarrierWithCompatibilityFallback first attempts the authenticated
// Version7 coordinated-drain capability. A legacy peer is retried only on a
// newly opened Carrier; no legacy HELLO is appended to a connection that has
// already received a Version7 HELLO.
func (s *streamSession) negotiateCarrierWithCompatibilityFallback(conn net.Conn, openFresh func() (net.Conn, error), id uint8, pathAware bool, runtime *clientRuntimeConfig) (net.Conn, *smp3core.StreamPathAuth, error) {
	auth, acceptedV7, err := s.negotiateCarrierV7WithRuntime(conn, id, pathAware, runtime)
	if acceptedV7 {
		s.markV7Accepted(id)
	}
	if err == nil {
		return conn, auth, nil
	}
	if s.v7WasAccepted(id) {
		return conn, nil, err
	}
	var capabilityUnavailable streamV7CapabilityUnavailableError
	if !errors.As(err, &capabilityUnavailable) {
		var transportErr streamV7TransportError
		if !errors.As(err, &transportErr) || !runtime.cfg.SMP3.Stream.AllowV7LegacyFallback {
			return conn, nil, err
		}
	}

	slog.Default().Warn("SMP3 V7 coordinated-drain negotiation unavailable; retrying an older protocol on a fresh carrier", "leg", id, "authenticated_capability_refusal", errors.As(err, &capabilityUnavailable), "explicit_transport_fallback", runtime.cfg.SMP3.Stream.AllowV7LegacyFallback, "error", err)
	_ = conn.Close()
	conn, err = openFresh()
	if err != nil {
		return nil, nil, fmt.Errorf("open fresh carrier for legacy compatibility: %w", err)
	}
	auth, acceptedV6, err := s.negotiateCarrierVersionWithRuntime(conn, id, pathAware, runtime)
	if acceptedV6 {
		s.markV6Accepted(id)
	}
	if err == nil {
		return conn, auth, nil
	}
	if !pathAware || !runtime.cfg.SMP3.Stream.AllowPathRTTV4Fallback ||
		!runtime.cfg.SMP3.Stream.AllowPathRTTV4FallbackOnTransportError || s.v6WasAccepted(id) || !isStreamV6TransportFailure(err) {
		return conn, nil, err
	}

	slog.Default().Warn("SMP3 V6 handshake transport failed; retrying V4 on a fresh carrier (peer rejection was not authenticated)", "leg", id, "error", err)
	_ = conn.Close()
	conn, err = openFresh()
	if err != nil {
		return nil, nil, fmt.Errorf("open fresh V4 compatibility carrier: %w", err)
	}
	auth, _, err = s.negotiateCarrierVersionWithRuntime(conn, id, false, runtime)
	if err != nil {
		return conn, nil, err
	}
	return conn, auth, nil
}

type streamV7CapabilityUnavailableError struct{ err error }

func (e streamV7CapabilityUnavailableError) Error() string { return e.err.Error() }
func (e streamV7CapabilityUnavailableError) Unwrap() error { return e.err }

func (s *streamSession) negotiateCarrierV7WithRuntime(conn net.Conn, id uint8, pathAware bool, runtime *clientRuntimeConfig) (*smp3core.StreamPathAuth, bool, error) {
	if runtime == nil {
		return nil, false, errors.New("client runtime config unavailable")
	}
	handshakeStarted := time.Now()
	readyTimeout := runtime.cfg.SMP3.CarrierReadyTimeout.Time()
	if readyTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(readyTimeout)); err != nil {
			return nil, false, err
		}
		defer conn.SetDeadline(time.Time{})
	}
	hello, err := writeStreamHelloV7Version(conn, s.id, id, s.destination, runtime.cfg.SMP3.Password, pathAware)
	if err != nil {
		return nil, false, streamV7TransportError{err: err}
	}
	_, auth, err := smp3core.ReadStreamReadyV2(conn, hello, []byte(runtime.cfg.SMP3.Password))
	if err != nil {
		if isStreamV7TransportError(err) {
			return nil, false, streamV7TransportError{err: err}
		}
		return nil, false, err
	}
	if !auth.CoordinatedDrainEnabled() {
		return nil, false, streamV7CapabilityUnavailableError{err: errors.New("authenticated V7 READY did not select coordinated drain")}
	}
	if auth.PathRTTEnabled() {
		if measured, measureErr := auth.WithCarrierHandshakeRTT(time.Since(handshakeStarted)); measureErr == nil {
			auth = measured
		}
	}
	return &auth, true, nil
}

func isStreamV7TransportError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func (s *streamSession) negotiateCarrierVersion(conn net.Conn, id uint8, pathAware bool) (*smp3core.StreamPathAuth, bool, error) {
	return s.negotiateCarrierVersionWithRuntime(conn, id, pathAware, s.currentRuntime())
}

func (s *streamSession) negotiateCarrierVersionWithRuntime(conn net.Conn, id uint8, pathAware bool, runtime *clientRuntimeConfig) (*smp3core.StreamPathAuth, bool, error) {
	if runtime == nil {
		return nil, false, errors.New("client runtime config unavailable")
	}
	var handshakeStarted time.Time
	if pathAware {
		handshakeStarted = time.Now()
		if err := conn.SetDeadline(time.Now().Add(runtime.cfg.SMP3.CarrierReadyTimeout.Time())); err != nil {
			return nil, false, err
		}
		defer conn.SetDeadline(time.Time{})
	}
	hello, err := writeStreamHelloVersion(conn, s.id, id, s.destination, runtime.cfg.SMP3.Password, pathAware)
	if err != nil {
		return nil, false, err
	}
	if !pathAware {
		if err := readSidecarReadyV1(conn, hello, []byte(runtime.cfg.SMP3.Password), runtime.cfg.SMP3.CarrierReadyTimeout.Time()); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if err := conn.SetReadDeadline(time.Now().Add(runtime.cfg.SMP3.CarrierReadyTimeout.Time())); err != nil {
		return nil, false, err
	}
	_, auth, err := smp3core.ReadStreamReadyV2(conn, hello, []byte(runtime.cfg.SMP3.Password))
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		var opErr *net.OpError
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &opErr) {
			return nil, false, streamV6TransportError{err: err}
		}
		return nil, false, err
	}
	if auth.PathRTTEnabled() {
		if measured, measureErr := auth.WithCarrierHandshakeRTT(time.Since(handshakeStarted)); measureErr == nil {
			auth = measured
		}
		return &auth, true, nil
	}
	// A valid authenticated V6 READY with no selected RTT capability still
	// proves V6 support; it must not be mistaken for a legacy peer.
	return nil, true, nil
}

func isStreamV6TransportFailure(err error) bool {
	var transportErr streamV6TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func writeStreamHello(conn io.Writer, sessionID smp3core.SessionID, leg uint8, destination, password string) (smp3core.Hello, error) {
	return writeStreamHelloVersion(conn, sessionID, leg, destination, password, false)
}

func writeStreamHelloVersion(conn io.Writer, sessionID smp3core.SessionID, leg uint8, destination, password string, pathAware bool) (smp3core.Hello, error) {
	version := smp3core.Version4
	var capabilities uint32
	if pathAware {
		version = smp3core.Version6
		capabilities = smp3core.StreamCapabilityPathRTT
	}
	return writeStreamHelloWithCapabilities(conn, sessionID, leg, destination, password, version, capabilities, pathAware)
}

func writeStreamHelloV7Version(conn io.Writer, sessionID smp3core.SessionID, leg uint8, destination, password string, pathAware bool) (smp3core.Hello, error) {
	capabilities := smp3core.StreamCapabilityCoordinatedDrain
	if pathAware {
		capabilities |= smp3core.StreamCapabilityPathRTT
	}
	return writeStreamHelloWithCapabilities(conn, sessionID, leg, destination, password, smp3core.Version7, capabilities, true)
}

func writeStreamHelloWithCapabilities(conn io.Writer, sessionID smp3core.SessionID, leg uint8, destination, password string, version smp3core.Version, capabilities uint32, classifyWriteAsTransport bool) (smp3core.Hello, error) {
	var nonce [16]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return smp3core.Hello{}, err
	}
	hello := smp3core.Hello{
		Version:      version,
		Capabilities: capabilities,
		SessionID:    sessionID,
		LegID:        smp3core.LegID(leg),
		Mode:         smp3core.ModeStream,
		Timestamp:    time.Now().Unix(),
		Nonce:        nonce,
		Destination:  destination,
	}
	header, dest, mac, err := smp3core.EncodeHelloParts(hello, []byte(password))
	if err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, header); err != nil {
		if classifyWriteAsTransport {
			return smp3core.Hello{}, streamV6TransportError{err: err}
		}
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, dest); err != nil {
		if classifyWriteAsTransport {
			return smp3core.Hello{}, streamV6TransportError{err: err}
		}
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, mac); err != nil {
		if classifyWriteAsTransport {
			return smp3core.Hello{}, streamV6TransportError{err: err}
		}
		return smp3core.Hello{}, err
	}
	return hello, nil
}

func attachStreamLeg(engine *smp3core.StreamEngine, id smp3core.LegID, conn net.Conn, auth *smp3core.StreamPathAuth) error {
	if auth != nil {
		return engine.AttachLegWithPathAuth(id, conn, nil, *auth)
	}
	return engine.AttachLeg(id, conn, nil)
}

func attachStreamLegWithPrior(engine *smp3core.StreamEngine, id smp3core.LegID, conn net.Conn, auth *smp3core.StreamPathAuth, prior uint32) error {
	if auth != nil {
		return engine.AttachLegWithPathAuthAndBandwidthPrior(id, conn, nil, *auth, prior)
	}
	return engine.AttachLegWithBandwidthPrior(id, conn, nil, prior)
}

func (s *streamSession) scheduleRepair(id uint8) {
	if int(id) >= s.configuredLegCount() || !s.wantsLeg(id) || s.isClosed() {
		return
	}
	s.ensureLegState()
	s.repairMu.Lock()
	if s.repairing[id] {
		s.repairMu.Unlock()
		return
	}
	s.repairing[id] = true
	s.repairMu.Unlock()
	go func() {
		defer func() {
			s.repairMu.Lock()
			s.repairing[id] = false
			s.repairMu.Unlock()
		}()
		for {
			if s.isClosed() || !s.wantsLeg(id) || s.engine.HasLeg(smp3core.LegID(id)) {
				return
			}
			conn, pathAuth, err := s.dialLeg(id)
			if err == nil {
				if s.isClosed() || !s.wantsLeg(id) || s.engine.HasLeg(smp3core.LegID(id)) {
					_ = conn.Close()
					return
				}
				if err := attachStreamLeg(s.engine, smp3core.LegID(id), conn, pathAuth); err == nil {
					slog.Default().Info("SMP3 carrier leg rejoined", "leg", id)
					return
				}
				_ = conn.Close()
			}
			timer := time.NewTimer(s.currentRuntime().cfg.SMP3.Stream.RedialInterval.Time())
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (s *streamSession) ensureLegOnce(id uint8) {
	if int(id) >= s.configuredLegCount() || !s.wantsLeg(id) || s.isClosed() {
		return
	}
	s.ensureLegState()
	s.repairMu.Lock()
	if s.repairing[id] {
		s.repairMu.Unlock()
		return
	}
	s.repairing[id] = true
	s.repairMu.Unlock()
	go func() {
		defer func() {
			s.repairMu.Lock()
			s.repairing[id] = false
			s.repairMu.Unlock()
		}()
		if s.isClosed() || !s.wantsLeg(id) || s.engine.HasLeg(smp3core.LegID(id)) {
			return
		}
		conn, pathAuth, err := s.dialLeg(id)
		if err != nil || s.isClosed() || !s.wantsLeg(id) || s.engine.HasLeg(smp3core.LegID(id)) {
			if conn != nil {
				_ = conn.Close()
			}
			return
		}
		if err := attachStreamLeg(s.engine, smp3core.LegID(id), conn, pathAuth); err != nil {
			_ = conn.Close()
		} else {
			slog.Default().Info("SMP3 carrier leg rejoined", "leg", id)
		}
	}()
}

func (s *streamSession) run(local net.Conn, reader io.Reader) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(s.app, reader)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(local, s.app)
		done <- struct{}{}
	}()
	<-done
	_ = s.Close()
	_ = local.Close()
	<-done
}

func (s *streamSession) isClosed() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *streamSession) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.engine.Close()
		_ = s.app.Close()
		s.client.unregisterStreamSession(s)
	})
	return nil
}

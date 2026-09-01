package outbound

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
)

const smp3UDPBootstrapDelay = 250 * time.Millisecond

var nextSMP3UDPAssociationID atomic.Uint64

type smp3UDPDualSession struct {
	id               smp3core.SessionID
	engine           *smp3core.DatagramEngine
	engineMu         sync.RWMutex
	engineGeneration uint64
	associationID    uint64
	newEngine        func(smp3core.DatagramConfig) *smp3core.DatagramEngine
	owner            *SMP3
	destination      string
	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	bootstrapped     atomic.Bool
	closing          atomic.Bool
	closeOnce        sync.Once
	recreateMu       sync.Mutex
	repairMu         sync.Mutex
	repairing        [2]bool
	stateMu          sync.RWMutex
	lastSendError    error
	lastReceiveError error
}

func (s *smp3UDPDualSession) Send(p []byte, address string, deadline time.Time) error {
	for {
		if s.closing.Load() {
			s.recordSendError(smp3core.ErrDatagramClosed)
			return smp3core.ErrDatagramClosed
		}
		engine := s.currentEngine()
		if engine == nil {
			s.recordSendError(smp3core.ErrDatagramClosed)
			return smp3core.ErrDatagramClosed
		}
		if datagramEngineDone(engine) {
			if err := s.recreateTerminalEngine(engine); err != nil {
				s.recordSendError(err)
				return err
			}
			continue
		}
		err := engine.Send(p, address, deadline)
		s.recordSendError(err)
		if errors.Is(err, smp3core.ErrDatagramClosed) && !s.closing.Load() {
			if recreateErr := s.recreateTerminalEngine(engine); recreateErr != nil {
				s.recordSendError(recreateErr)
				return recreateErr
			}
			continue
		}
		return err
	}
}
func (s *smp3UDPDualSession) Receive(deadline time.Time) (smp3core.Datagram, error) {
	for {
		if s.closing.Load() {
			s.recordReceiveError(smp3core.ErrDatagramClosed)
			return smp3core.Datagram{}, smp3core.ErrDatagramClosed
		}
		engine := s.currentEngine()
		if engine == nil {
			s.recordReceiveError(smp3core.ErrDatagramClosed)
			return smp3core.Datagram{}, smp3core.ErrDatagramClosed
		}
		datagram, err := engine.Receive(deadline)
		s.recordReceiveError(err)
		if errors.Is(err, smp3core.ErrDatagramClosed) && !s.closing.Load() {
			if recreateErr := s.recreateTerminalEngine(engine); recreateErr != nil {
				s.recordReceiveError(recreateErr)
				return smp3core.Datagram{}, recreateErr
			}
			continue
		}
		return datagram, err
	}
}
func (s *smp3UDPDualSession) Done() <-chan struct{} { return s.done }
func (s *smp3UDPDualSession) Close() error {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		s.cancel()
		s.recreateMu.Lock()
		engine := s.currentEngine()
		if engine != nil {
			_ = engine.Close()
		}
		s.recreateMu.Unlock()
		close(s.done)
	})
	return nil
}

func datagramEngineDone(engine *smp3core.DatagramEngine) bool {
	if engine == nil {
		return true
	}
	select {
	case <-engine.Done():
		return true
	default:
		return false
	}
}

func (s *smp3UDPDualSession) currentEngine() *smp3core.DatagramEngine {
	s.engineMu.RLock()
	engine := s.engine
	s.engineMu.RUnlock()
	return engine
}

func (s *smp3UDPDualSession) currentState() (*smp3core.DatagramEngine, smp3core.SessionID, uint64) {
	s.engineMu.RLock()
	engine, id, generation := s.engine, s.id, s.engineGeneration
	s.engineMu.RUnlock()
	return engine, id, generation
}

func (s *smp3UDPDualSession) recordSendError(err error) {
	s.stateMu.Lock()
	s.lastSendError = err
	s.stateMu.Unlock()
}

func (s *smp3UDPDualSession) recordReceiveError(err error) {
	s.stateMu.Lock()
	s.lastReceiveError = err
	s.stateMu.Unlock()
}

type smp3UDPSessionDebugSnapshot struct {
	AssociationID    uint64
	SessionID        smp3core.SessionID
	Engine           *smp3core.DatagramEngine
	EngineGeneration uint64
	EngineDone       bool
	LegUp            [2]bool
	Repairing        [2]bool
	LastSendError    string
	LastReceiveError string
	Closed           bool
}

func (s *smp3UDPDualSession) debugSnapshotForTest() smp3UDPSessionDebugSnapshot {
	engine, id, generation := s.currentState()
	var stats smp3core.DatagramStats
	if engine != nil {
		stats = engine.Snapshot()
	}
	s.repairMu.Lock()
	repairing := s.repairing
	s.repairMu.Unlock()
	s.stateMu.RLock()
	lastSend, lastReceive := s.lastSendError, s.lastReceiveError
	s.stateMu.RUnlock()
	debugError := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	return smp3UDPSessionDebugSnapshot{
		AssociationID:    s.associationID,
		SessionID:        id,
		Engine:           engine,
		EngineGeneration: generation,
		EngineDone:       datagramEngineDone(engine),
		LegUp:            stats.LegUp,
		Repairing:        repairing,
		LastSendError:    debugError(lastSend),
		LastReceiveError: debugError(lastReceive),
		Closed:           s.closing.Load(),
	}
}

func (s *SMP3) listenPacketContextDual(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	_, raw, err := s.newDualLegUDPPacketConn(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return NewPacketConn(raw, s), nil
}

func (s *SMP3) newDualLegUDPPacketConn(ctx context.Context, metadata *C.Metadata) (*smp3UDPDualSession, *smp3PacketConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if metadata == nil || metadata.NetWork != C.UDP || !metadata.Valid() || metadata.DstPort == 0 {
		return nil, nil, errors.New("smp3: invalid UDP application destination")
	}
	if _, err := s.child(s.option.Legs[0].Proxy); err != nil {
		return nil, nil, err
	}
	if _, err := s.child(s.option.Legs[1].Proxy); err != nil {
		return nil, nil, err
	}
	if s.option.Leg1Fallback != "" {
		if _, err := s.child(s.option.Leg1Fallback); err != nil {
			return nil, nil, err
		}
	}
	cfg, err := s.datagramConfig()
	if err != nil {
		return nil, nil, err
	}
	sid, err := newSMP3UDPSessionID()
	if err != nil {
		return nil, nil, fmt.Errorf("smp3: generate UDP session id: %w", err)
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &smp3UDPDualSession{
		id:               sid,
		owner:            s,
		destination:      metadata.RemoteAddress(),
		ctx:              sessionCtx,
		cancel:           cancel,
		done:             make(chan struct{}),
		associationID:    nextSMP3UDPAssociationID.Add(1),
		engineGeneration: 1,
		newEngine: func(config smp3core.DatagramConfig) *smp3core.DatagramEngine {
			return smp3core.NewDatagramEngine(config)
		},
	}
	cfg.OnLegDown = func(id smp3core.LegID, legErr error) { session.scheduleRepair(id, legErr) }
	session.engine = session.newEngine(cfg)
	go func() {
		select {
		case <-ctx.Done():
			if !session.bootstrapped.Load() {
				session.cancel()
			}
		case <-sessionCtx.Done():
		}
	}()

	type result struct {
		id  uint8
		err error
	}
	results := make(chan result, 2)
	var starts [2]sync.Once
	start := func(id uint8) {
		starts[id].Do(func() {
			go func() {
				err := session.bootstrapLeg(id)
				if session.bootstrapped.Load() {
					if err != nil {
						session.scheduleRepair(smp3core.LegID(id), err)
					}
					return
				}
				results <- result{id: id, err: err}
			}()
		})
	}
	start(0)
	startedAt := time.Now()
	timer := time.NewTimer(smp3UDPBootstrapDelay)
	defer timer.Stop()
	var done [2]bool
	var errs [2]error
	for {
		select {
		case <-sessionCtx.Done():
			if session.bootstrapped.Load() {
				return session, newSMP3PacketConn(session), nil
			}
			_ = session.Close()
			return nil, nil, ctx.Err()
		case <-timer.C:
			start(1)
		case r := <-results:
			done[r.id] = true
			errs[r.id] = r.err
			if r.err == nil {
				session.bootstrapped.Store(true)
				s.udpSessions.Store(session, struct{}{})
				go func() { <-session.ctx.Done(); s.udpSessions.Delete(session) }()
				other := 1 - r.id
				if done[other] && errs[other] != nil {
					session.scheduleRepair(smp3core.LegID(other), errs[other])
				}
				if r.id == 0 {
					remaining := smp3UDPBootstrapDelay - time.Since(startedAt)
					if remaining <= 0 {
						start(1)
					} else {
						time.AfterFunc(remaining, func() { start(1) })
					}
				}
				return session, newSMP3PacketConn(session), nil
			}
			if r.id == 0 {
				start(1)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			if done[0] && done[1] {
				_ = session.Close()
				return nil, nil, fmt.Errorf("smp3: UDP dual bootstrap failed: leg0=%v; leg1=%v", errs[0], errs[1])
			}
		}
	}
}

func (s *smp3UDPDualSession) bootstrapLeg(id uint8) error {
	engine, sid, _ := s.currentState()
	if engine == nil {
		return smp3core.ErrDatagramClosed
	}
	conn, err := s.owner.dialUDPLeg(s.ctx, id, sid, s.destination)
	if err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		_ = conn.Close()
		return err
	}
	if err := engine.AttachLeg(smp3core.LegID(id), conn, nil); err != nil {
		_ = conn.Close()
		return fmt.Errorf("attach leg %d: %w", id, err)
	}
	return nil
}

func (s *SMP3) dialUDPLeg(ctx context.Context, id uint8, sid smp3core.SessionID, destination string) (net.Conn, error) {
	if id > 1 {
		return nil, errors.New("smp3: invalid UDP leg")
	}
	names := []string{s.option.Legs[id].Proxy}
	if id == 1 && s.option.Leg1Fallback != "" {
		names = append(names, s.option.Leg1Fallback)
	}
	endpoint, err := s.endpointMetadata()
	if err != nil {
		return nil, err
	}
	var causes []error
	for _, name := range names {
		child, childErr := s.child(name)
		if childErr != nil {
			causes = append(causes, fmt.Errorf("%s: %w", name, childErr))
			continue
		}
		conn, dialErr := child.DialContext(ctx, endpoint)
		if dialErr != nil {
			causes = append(causes, fmt.Errorf("%s: %w", name, dialErr))
			continue
		}
		if conn == nil {
			causes = append(causes, fmt.Errorf("%s: nil carrier", name))
			continue
		}
		if err := writeSMP3DatagramHelloForLeg(conn, sid, smp3core.LegID(id), destination, []byte(s.option.Password)); err != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s HELLO: %w", name, err))
			continue
		}
		return conn, nil
	}
	return nil, errors.Join(causes...)
}

func writeSMP3DatagramHelloForLeg(conn io.Writer, sid smp3core.SessionID, leg smp3core.LegID, destination string, password []byte) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	return writeSMP3DatagramHelloForLegAt(conn, sid, leg, destination, password, time.Now().Unix(), nonce)
}
func writeSMP3DatagramHelloForLegAt(conn io.Writer, sid smp3core.SessionID, leg smp3core.LegID, destination string, password []byte, timestamp int64, nonce [16]byte) error {
	header, dest, mac, err := smp3core.EncodeHelloParts(smp3core.Hello{Version: smp3core.Version5, SessionID: sid, LegID: leg, Mode: smp3core.ModeDatagram, Timestamp: timestamp, Nonce: nonce, Destination: destination}, password)
	if err != nil {
		return err
	}
	buffers := net.Buffers{header, dest, mac}
	_, err = buffers.WriteTo(conn)
	return err
}

func (s *smp3UDPDualSession) scheduleRepair(id smp3core.LegID, legErr error) {
	engine, _, _ := s.currentState()
	s.scheduleRepairForEngine(engine, id, legErr)
}

func (s *smp3UDPDualSession) scheduleRepairForEngine(expected *smp3core.DatagramEngine, id smp3core.LegID, _ error) {
	if id > 1 || s.closing.Load() || !s.bootstrapped.Load() || expected == nil {
		return
	}
	if s.currentEngine() != expected {
		return
	}
	if datagramEngineDone(expected) {
		go func() { _ = s.recreateTerminalEngine(expected) }()
		return
	}
	n := int(id)
	s.repairMu.Lock()
	if s.repairing[n] {
		s.repairMu.Unlock()
		return
	}
	s.repairing[n] = true
	s.repairMu.Unlock()
	_, sid, _ := s.currentState()
	go func() {
		defer func() { s.repairMu.Lock(); s.repairing[n] = false; s.repairMu.Unlock() }()
		for {
			timer := time.NewTimer(s.owner.redialInterval)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if s.closing.Load() || s.currentEngine() != expected {
				return
			}
			if datagramEngineDone(expected) {
				_ = s.recreateTerminalEngine(expected)
				return
			}
			conn, err := s.owner.dialUDPLeg(s.ctx, uint8(id), sid, s.destination)
			if err == nil {
				if s.currentEngine() == expected && !datagramEngineDone(expected) {
					if attachErr := expected.AttachLeg(id, conn, nil); attachErr == nil {
						return
					}
				}
				_ = conn.Close()
			}
		}
	}()
}

func (s *smp3UDPDualSession) recreateTerminalEngine(expected *smp3core.DatagramEngine) error {
	s.recreateMu.Lock()
	defer s.recreateMu.Unlock()
	if s.closing.Load() {
		return smp3core.ErrDatagramClosed
	}
	current := s.currentEngine()
	if current != expected && !datagramEngineDone(current) {
		return nil
	}
	if !datagramEngineDone(current) {
		return nil
	}
	cfg, err := s.owner.datagramConfig()
	if err != nil {
		return err
	}
	sid, err := newSMP3UDPSessionID()
	if err != nil {
		return fmt.Errorf("smp3: generate replacement UDP session id: %w", err)
	}
	var fresh *smp3core.DatagramEngine
	cfg.OnLegDown = func(id smp3core.LegID, legErr error) { s.scheduleRepairForEngine(fresh, id, legErr) }
	factory := s.newEngine
	if factory == nil {
		factory = func(config smp3core.DatagramConfig) *smp3core.DatagramEngine {
			return smp3core.NewDatagramEngine(config)
		}
	}
	fresh = factory(cfg)
	if fresh == nil {
		return errors.New("smp3: replacement UDP engine factory returned nil")
	}
	conn, err := s.owner.dialUDPLeg(s.ctx, 0, sid, s.destination)
	if err != nil {
		_ = fresh.Close()
		return fmt.Errorf("smp3: replacement UDP leg0: %w", err)
	}
	if err := fresh.AttachLeg(0, conn, nil); err != nil {
		_ = conn.Close()
		_ = fresh.Close()
		return fmt.Errorf("smp3: attach replacement UDP leg0: %w", err)
	}
	s.engineMu.Lock()
	if s.closing.Load() || s.engine != current || datagramEngineDone(current) == false {
		s.engineMu.Unlock()
		_ = fresh.Close()
		return nil
	}
	old := s.engine
	s.engine = fresh
	s.id = sid
	s.engineGeneration++
	s.engineMu.Unlock()
	_ = old.Close()
	go s.bootstrapReplacementLeg(fresh, sid)
	return nil
}

func (s *smp3UDPDualSession) bootstrapReplacementLeg(engine *smp3core.DatagramEngine, sid smp3core.SessionID) {
	if s.closing.Load() || s.currentEngine() != engine {
		return
	}
	conn, err := s.owner.dialUDPLeg(s.ctx, 1, sid, s.destination)
	if err != nil {
		s.scheduleRepairForEngine(engine, 1, err)
		return
	}
	if s.closing.Load() || s.currentEngine() != engine || datagramEngineDone(engine) {
		_ = conn.Close()
		return
	}
	if err := engine.AttachLeg(1, conn, nil); err != nil {
		_ = conn.Close()
		s.scheduleRepairForEngine(engine, 1, err)
	}
}

var _ smp3DatagramIO = (*smp3UDPDualSession)(nil)
var _ net.PacketConn = (*smp3PacketConn)(nil)

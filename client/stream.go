package client

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type streamSession struct {
	client      *Client
	id          smp3core.SessionID
	engine      *smp3core.StreamEngine
	app         net.Conn
	destination string
	ctx         context.Context
	cancel      context.CancelFunc

	closeOnce     sync.Once
	repairMu      sync.Mutex
	repairing     [2]bool
	startupMu     sync.Mutex
	startupActive bool
}

type streamBootstrapResult struct {
	id   uint8
	conn net.Conn
	err  error
}

func newStreamSession(client *Client, destination string) (*streamSession, error) {
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("generate stream session id: %w", err)
	}
	ctx, cancel := context.WithCancel(client.ctx)
	var session *streamSession
	onActivate := func() {
		if session != nil {
			if client.hostCarrier != nil {
				if client.cfg.SMP3.Stream.StartupPolicy != "preferred" {
					session.ensureLegOnce(1)
				}
			} else if !session.startupInFlight() {
				session.scheduleRepair(1)
			}
		}
	}
	onLegDown := func(id uint8, _ error) {
		if session != nil && client.hostCarrier == nil && !session.startupInFlight() {
			session.scheduleRepair(id)
		}
	}
	config := client.cfg.SMP3.streamConfig(onActivate, onLegDown)
	engine, app := smp3core.NewStreamEngine(config)
	session = &streamSession{client: client, id: id, engine: engine, app: app, destination: destination, ctx: ctx, cancel: cancel}
	session.watchEngine()
	if client.cfg.SMP3.Stream.StartupPolicy == "preferred" {
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
	conn, err := s.dialLeg(0)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("connect stream leg0: %w", err)
	}
	if err := s.engine.AttachLeg(0, conn, nil); err != nil {
		_ = conn.Close()
		_ = s.Close()
		return nil, fmt.Errorf("attach stream leg0: %w", err)
	}
	if s.client.hostCarrier != nil {
		leg1, err := s.dialLeg(1)
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("connect stream leg1: %w", err)
		}
		if err := s.engine.AttachLeg(1, leg1, nil); err != nil {
			_ = leg1.Close()
			_ = s.Close()
			return nil, fmt.Errorf("attach stream leg1: %w", err)
		}
	}
	return s, nil
}

func (s *streamSession) startPreferred() (*streamSession, error) {
	s.setStartupActive(true)
	results := make(chan streamBootstrapResult, 2)
	for id := uint8(0); id < 2; id++ {
		go func(id uint8) {
			conn, err := s.dialLeg(id)
			results <- streamBootstrapResult{id: id, conn: conn, err: err}
		}(id)
	}

	preferred := uint8(s.client.cfg.SMP3.Stream.StartupPreferredLeg)
	var causes []error
	completed := 0
	for completed < 2 {
		select {
		case <-s.ctx.Done():
			_ = s.engine.Close()
			s.setStartupActive(false)
			go drainBootstrapResults(results, 2-completed)
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
			if err := s.engine.AttachLeg(smp3core.LegID(result.id), result.conn, nil); err != nil {
				_ = result.conn.Close()
				causes = append(causes, fmt.Errorf("attach stream leg%d: %w", result.id, err))
				if result.id == preferred && !s.isClosed() {
					s.engine.MarkStartupLegTerminalUnavailable(smp3core.LegID(result.id))
				}
				continue
			}
			go s.finishPreferredCompanion(results, 2-completed, preferred)
			if completed == 2 {
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

func (s *streamSession) finishPreferredCompanion(results <-chan streamBootstrapResult, remaining int, preferred uint8) {
	if remaining == 0 {
		s.setStartupActive(false)
		return
	}
	result := <-results
	defer s.setStartupActive(false)
	if result.err != nil {
		if result.id == preferred {
			s.engine.MarkStartupLegTerminalUnavailable(smp3core.LegID(result.id))
		}
		return
	}
	if s.isClosed() || s.engine.Finalizing() || s.engine.HasLeg(smp3core.LegID(result.id)) {
		_ = result.conn.Close()
		return
	}
	if err := s.engine.AttachLeg(smp3core.LegID(result.id), result.conn, nil); err != nil {
		_ = result.conn.Close()
	}
}

func newSessionID() (smp3core.SessionID, error) {
	var id smp3core.SessionID
	if _, err := cryptorand.Read(id[:]); err != nil {
		return id, err
	}
	return id, nil
}

func (s *streamSession) dialLeg(id uint8) (net.Conn, error) {
	if id > 1 {
		return nil, errors.New("invalid stream leg")
	}
	if s.client.hostCarrier != nil {
		conn, err := s.client.hostCarrier.Open(s.ctx, s.id, id)
		if err != nil {
			return nil, fmt.Errorf("host carrier OPEN leg %d: %w", id, err)
		}
		hello, err := writeStreamHello(conn, s.id, id, s.destination, s.client.cfg.SMP3.Password)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("host carrier HELLO leg %d: %w", id, err)
		}
		if err := readSidecarReadyV1(conn, hello, []byte(s.client.cfg.SMP3.Password), s.client.cfg.SMP3.CarrierReadyTimeout.Time()); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("host carrier READY leg %d: %w", id, err)
		}
		return conn, nil
	}
	route := s.client.cfg.SMP3.Routes.Leg0
	if id == 1 {
		route = s.client.cfg.SMP3.Routes.Leg1
	}
	names := []string{route}
	if id == 1 && s.client.cfg.SMP3.Routes.Leg1Fallback != "" {
		names = append(names, s.client.cfg.SMP3.Routes.Leg1Fallback)
	}
	var causes []error
	upstream := s.client.cfg.effectiveUpstream(id)
	for _, endpoint := range names {
		conn, err := dialUpstream(s.ctx, upstream, endpoint)
		if err != nil {
			if ctxErr := s.ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			causes = append(causes, fmt.Errorf("%s: %w", endpoint, err))
			continue
		}
		hello, err := writeStreamHello(conn, s.id, id, s.destination, s.client.cfg.SMP3.Password)
		if err != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s HELLO: %w", endpoint, err))
			continue
		}
		if err := readSidecarReadyV1(conn, hello, []byte(s.client.cfg.SMP3.Password), s.client.cfg.SMP3.CarrierReadyTimeout.Time()); err != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s READY: %w", endpoint, err))
			continue
		}
		if ctxErr := s.ctx.Err(); ctxErr != nil {
			_ = conn.Close()
			return nil, ctxErr
		}
		return conn, nil
	}
	return nil, errors.Join(causes...)
}

func writeStreamHello(conn io.Writer, sessionID smp3core.SessionID, leg uint8, destination, password string) (smp3core.Hello, error) {
	var nonce [16]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return smp3core.Hello{}, err
	}
	hello := smp3core.Hello{
		Version:     smp3core.Version4,
		SessionID:   sessionID,
		LegID:       smp3core.LegID(leg),
		Mode:        smp3core.ModeStream,
		Timestamp:   time.Now().Unix(),
		Nonce:       nonce,
		Destination: destination,
	}
	header, dest, mac, err := smp3core.EncodeHelloParts(hello, []byte(password))
	if err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, header); err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, dest); err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, mac); err != nil {
		return smp3core.Hello{}, err
	}
	return hello, nil
}

func (s *streamSession) scheduleRepair(id uint8) {
	if id > 1 || s.isClosed() {
		return
	}
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
			if s.isClosed() || s.engine.HasLeg(smp3core.LegID(id)) {
				return
			}
			conn, err := s.dialLeg(id)
			if err == nil {
				if s.isClosed() || s.engine.HasLeg(smp3core.LegID(id)) {
					_ = conn.Close()
					return
				}
				if err := s.engine.AttachLeg(smp3core.LegID(id), conn, nil); err == nil {
					return
				}
				_ = conn.Close()
			}
			timer := time.NewTimer(s.client.cfg.SMP3.Stream.RedialInterval.Time())
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
	if id > 1 || s.isClosed() {
		return
	}
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
		if s.isClosed() || s.engine.HasLeg(smp3core.LegID(id)) {
			return
		}
		conn, err := s.dialLeg(id)
		if err != nil || s.isClosed() || s.engine.HasLeg(smp3core.LegID(id)) {
			if conn != nil {
				_ = conn.Close()
			}
			return
		}
		if err := s.engine.AttachLeg(smp3core.LegID(id), conn, nil); err != nil {
			_ = conn.Close()
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
	})
	return nil
}

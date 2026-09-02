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

	closeOnce sync.Once
	repairMu  sync.Mutex
	repairing [2]bool
}

func newStreamSession(client *Client, destination string) (*streamSession, error) {
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("generate stream session id: %w", err)
	}
	ctx, cancel := context.WithCancel(client.ctx)
	var session *streamSession
	config := client.cfg.SMP3.streamConfig(func() {
		if session != nil {
			session.scheduleRepair(1)
		}
	}, func(id uint8, _ error) {
		if session != nil {
			session.scheduleRepair(id)
		}
	})
	engine, app := smp3core.NewStreamEngine(config)
	session = &streamSession{client: client, id: id, engine: engine, app: app, destination: destination, ctx: ctx, cancel: cancel}
	conn, err := session.dialLeg(0)
	if err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("connect stream leg0: %w", err)
	}
	if err := engine.AttachLeg(0, conn, nil); err != nil {
		_ = conn.Close()
		_ = session.Close()
		return nil, fmt.Errorf("attach stream leg0: %w", err)
	}
	go func() {
		select {
		case <-engine.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return session, nil
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
	route := s.client.cfg.SMP3.Routes.Leg0
	if id == 1 {
		route = s.client.cfg.SMP3.Routes.Leg1
	}
	names := []string{route}
	if id == 1 && s.client.cfg.SMP3.Routes.Leg1Fallback != "" {
		names = append(names, s.client.cfg.SMP3.Routes.Leg1Fallback)
	}
	var causes []error
	for _, endpoint := range names {
		conn, err := dialUpstream(s.ctx, s.client.cfg.UpstreamSocks, endpoint)
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

package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type delayedPathAwareCarrier struct {
	password string
	delay    [2]time.Duration

	mu          sync.Mutex
	sessionID   smp3core.SessionID
	sessionSet  bool
	engine      *smp3core.StreamEngine
	app         net.Conn
	serverConns []net.Conn
	started     [2]chan struct{}
	startOnce   [2]sync.Once
	ready       [2]chan struct{}
	readyOnce   [2]sync.Once
	readyGate   [2]chan struct{}
	rejectOpen  [2]bool
	closed      chan struct{}
	closeOnce   sync.Once
}

func newDelayedPathAwareCarrier(password string, delay [2]time.Duration) *delayedPathAwareCarrier {
	c := &delayedPathAwareCarrier{password: password, delay: delay, closed: make(chan struct{})}
	for id := range c.started {
		c.started[id] = make(chan struct{})
		c.ready[id] = make(chan struct{})
	}
	return c
}

func (c *delayedPathAwareCarrier) Open(_ context.Context, sessionID smp3core.SessionID, legID uint8) (net.Conn, error) {
	if legID > 1 {
		return nil, errors.New("invalid leg id")
	}
	c.startOnce[legID].Do(func() { close(c.started[legID]) })
	if c.rejectOpen[legID] {
		return nil, errors.New("test carrier rejected companion OPEN")
	}
	clientConn, serverConn := net.Pipe()
	c.mu.Lock()
	if c.sessionSet && c.sessionID != sessionID {
		c.mu.Unlock()
		_ = clientConn.Close()
		_ = serverConn.Close()
		return nil, errors.New("carrier opened with a different session id")
	}
	if !c.sessionSet {
		c.sessionID = sessionID
		c.sessionSet = true
		c.engine, c.app = smp3core.NewStreamEngine(smp3core.StreamConfig{
			SchedulerMode:         smp3core.StreamSchedulerStatic,
			ExperimentalPathAware: true,
			PathRole:              smp3core.StreamPathRoleServer,
			ChunkSize:             64 << 10,
			QueueFrames:           64,
			MaxInflightFrames:     128,
			MaxReorderFrames:      256,
			AckInterval:           20 * time.Millisecond,
			RetransmitTimeout:     time.Second,
			RecoveryTimeout:       2 * time.Second,
			BandwidthMbps:         []uint32{100, 100},
			StartupPolicy:         smp3core.StreamStartupPreferred,
		})
		go preferredEcho(c.app)
	}
	c.serverConns = append(c.serverConns, serverConn)
	engine := c.engine
	c.mu.Unlock()
	go c.serve(engine, legID, serverConn)
	return clientConn, nil
}

func (c *delayedPathAwareCarrier) serve(engine *smp3core.StreamEngine, legID uint8, conn net.Conn) {
	reader := bufio.NewReader(conn)
	hello, err := smp3core.ReadHelloAt(reader, []byte(c.password), time.Now())
	if err != nil {
		_ = conn.Close()
		return
	}
	if c.delay[legID] > 0 {
		time.Sleep(c.delay[legID])
	}
	if gate := c.readyGate[legID]; gate != nil {
		select {
		case <-gate:
		case <-c.closed:
			_ = conn.Close()
			return
		}
	}
	selected := uint32(smp3core.StreamCapabilityPathRTT)
	if hello.Version == smp3core.Version7 {
		selected |= smp3core.StreamCapabilityCoordinatedDrain
	}
	auth, err := smp3core.WriteStreamReadyV2(conn, hello, selected, []byte(c.password))
	if err != nil {
		_ = conn.Close()
		return
	}
	if err := engine.AttachLegWithProtocolAuth(hello.LegID, &preferredBufferedConn{Conn: conn, reader: reader}, nil, auth); err != nil {
		_ = conn.Close()
		return
	}
	c.readyOnce[legID].Do(func() { close(c.ready[legID]) })
	<-engine.Done()
}

func (c *delayedPathAwareCarrier) Close() {
	c.closeOnce.Do(func() { close(c.closed) })
	c.mu.Lock()
	engine, app := c.engine, c.app
	conns := append([]net.Conn(nil), c.serverConns...)
	c.mu.Unlock()
	if engine != nil {
		_ = engine.Close()
	}
	if app != nil {
		_ = app.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func TestPathAwarePreferredFirstFlowSurvivesCompanionNeverReadyOrRejected(t *testing.T) {
	for _, test := range []struct {
		name       string
		rejectOpen bool
		release    bool
	}{
		{name: "never_ready_then_late_ready", release: true},
		{name: "open_rejected", rejectOpen: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newDelayedPathAwareCarrier(streamPathTestPassword, [2]time.Duration{2 * time.Millisecond, 0})
			if test.release {
				carrier.readyGate[1] = make(chan struct{})
			}
			carrier.rejectOpen[1] = test.rejectOpen
			defer carrier.Close()

			cfg := DefaultConfig()
			cfg.SMP3.Password = streamPathTestPassword
			cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
			cfg.SMP3.Stream.ExperimentalPathAware = true
			cfg.SMP3.Stream.StartupPolicy = "preferred"
			cfg.SMP3.Stream.StartupPreferredLeg = 0
			cfg.SMP3.Stream.StartupGrace = NonNegativeDuration(0)
			client, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			client.hostCarrier = carrier
			defer client.Close()

			session, err := newStreamSession(client, "example.com:443")
			if err != nil {
				t.Fatalf("preferred leg0 startup failed with unusable companion: %v", err)
			}
			defer session.Close()
			select {
			case <-carrier.started[1]:
			case <-time.After(time.Second):
				t.Fatal("companion OPEN was not attempted")
			}
			if !session.engine.HasLeg(0) || session.engine.HasLeg(1) {
				t.Fatalf("unexpected legs before companion completion: leg0=%v leg1=%v", session.engine.HasLeg(0), session.engine.HasLeg(1))
			}

			payload := bytes.Repeat([]byte("preferred-companion-independent/"), (100<<10)/len("preferred-companion-independent/")+1)
			payload = payload[:100<<10]
			_ = session.app.SetDeadline(time.Now().Add(3 * time.Second))
			if err := writeAll(session.app, payload); err != nil {
				t.Fatalf("write first flow while companion is unavailable: %v", err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(session.app, got); err != nil {
				t.Fatalf("read first flow while companion is unavailable: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("first flow payload mismatch")
			}
			if session.engine.Finalizing() || !session.engine.HasLeg(0) || session.engine.HasLeg(1) {
				t.Fatalf("unavailable companion affected the preferred session: finalizing=%v leg0=%v leg1=%v", session.engine.Finalizing(), session.engine.HasLeg(0), session.engine.HasLeg(1))
			}
			select {
			case <-carrier.ready[1]:
				t.Fatal("companion became READY before its explicit release")
			default:
			}
			t.Logf("companion_case=%s first_flow_bytes=%d completed_with_leg0_only=true", test.name, len(got))

			if test.release {
				close(carrier.readyGate[1])
				select {
				case <-carrier.ready[1]:
				case <-time.After(2 * time.Second):
					t.Fatal("late companion did not complete READY")
				}
				deadline := time.Now().Add(time.Second)
				for !session.engine.HasLeg(1) && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if !session.engine.HasLeg(1) {
					t.Fatal("late companion READY was not attached to shared Core")
				}
				feedback, ok := session.engine.PathFeedback(1)
				if !ok || feedback.Identity.Generation != session.engine.Snapshot().LegGeneration[1] || feedback.Identity.Direction != smp3core.StreamRTTClientToServer {
					t.Fatalf("late companion feedback is not bound to its active generation: %+v ok=%v", feedback, ok)
				}
			}
		})
	}
}

func TestPathAwarePreferredColdStartDoesNotWaitForSlowCompanion(t *testing.T) {
	const slowReadyDelay = 800 * time.Millisecond
	carrier := newDelayedPathAwareCarrier(streamPathTestPassword, [2]time.Duration{2 * time.Millisecond, slowReadyDelay})
	defer carrier.Close()

	cfg := DefaultConfig()
	cfg.SMP3.Password = streamPathTestPassword
	cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
	cfg.SMP3.Stream.ExperimentalPathAware = true
	cfg.SMP3.Stream.StartupPolicy = "preferred"
	cfg.SMP3.Stream.StartupPreferredLeg = 0
	cfg.SMP3.Stream.StartupGrace = NonNegativeDuration(0)
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.hostCarrier = carrier
	defer client.Close()

	started := time.Now()
	session, err := newStreamSession(client, "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if elapsed := time.Since(started); elapsed >= slowReadyDelay/2 {
		t.Fatalf("preferred startup waited for slow companion: %v", elapsed)
	}
	select {
	case <-carrier.started[1]:
	case <-time.After(time.Second):
		t.Fatal("slow companion did not start concurrently")
	}
	if !session.engine.HasLeg(0) || session.engine.HasLeg(1) {
		t.Fatalf("unexpected legs before slow READY: leg0=%v leg1=%v", session.engine.HasLeg(0), session.engine.HasLeg(1))
	}
	feedback, ok := session.engine.PathFeedback(0)
	if !ok || feedback.RTT <= 0 || feedback.Identity.Direction != smp3core.StreamRTTClientToServer {
		t.Fatalf("fast-leg authenticated handshake RTT missing: feedback=%+v ok=%v", feedback, ok)
	}

	payload := bytes.Repeat([]byte("cold-start/"), (100<<10)/len("cold-start/")+1)
	payload = payload[:100<<10]
	_ = session.app.SetDeadline(time.Now().Add(2 * time.Second))
	started = time.Now()
	if err := writeAll(session.app, payload); err != nil {
		t.Fatalf("write first application flow: %v", err)
	}
	first := make([]byte, 1)
	if _, err := io.ReadFull(session.app, first); err != nil {
		t.Fatalf("read first echoed byte: %v", err)
	}
	firstByte := time.Since(started)
	if firstByte >= slowReadyDelay/2 {
		t.Fatalf("first application byte waited for slow companion READY: %v", firstByte)
	}
	if session.engine.HasLeg(1) {
		t.Fatalf("slow companion attached before first response byte (%v)", firstByte)
	}
	received := make([]byte, len(payload))
	received[0] = first[0]
	if _, err := io.ReadFull(session.app, received[1:]); err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("first application flow payload mismatch")
	}
	flowElapsed := time.Since(started)
	select {
	case <-carrier.ready[1]:
		t.Fatalf("slow companion became READY before the first application flow completed (%v)", flowElapsed)
	default:
	}
	if session.engine.HasLeg(1) {
		t.Fatalf("slow companion attached before the first application flow completed (%v)", flowElapsed)
	}
	t.Logf("first_flow_bytes=%d first_byte_before_slow_READY_ms=%.3f full_flow_before_slow_READY=true full_flow_elapsed_ms=%.3f fast_leg_rtt_ms=%.3f", len(payload), float64(firstByte.Nanoseconds())/1e6, float64(flowElapsed.Nanoseconds())/1e6, float64(feedback.RTT.Nanoseconds())/1e6)

	select {
	case <-carrier.ready[1]:
	case <-time.After(2 * time.Second):
		t.Fatal("slow companion never completed authenticated READY")
	}
	deadline := time.Now().Add(time.Second)
	for !session.engine.HasLeg(1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !session.engine.HasLeg(1) {
		t.Fatal("slow companion READY was not attached to shared Core")
	}
	feedback, ok = session.engine.PathFeedback(1)
	if !ok || feedback.Identity.Generation != session.engine.Snapshot().LegGeneration[1] || feedback.Identity.Direction != smp3core.StreamRTTClientToServer {
		t.Fatalf("slow-leg RTT was not bound to its current generation: feedback=%+v ok=%v", feedback, ok)
	}
}

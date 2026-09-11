package client

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const preferredTestPassword = "preferred-startup-test-password"

type preferredBufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *preferredBufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

type preferredServerSession struct {
	engine *smp3core.StreamEngine
	app    net.Conn
}

type preferredFakeCarrier struct {
	password string

	mu           sync.Mutex
	opens        [2]int
	started      [2]chan struct{}
	startOnce    [2]sync.Once
	returned     [2]chan struct{}
	returnOnce   [2]sync.Once
	release      [2]chan struct{}
	releaseOnce  [2]sync.Once
	fail         [2]error
	ignoreCancel [2]bool
	sessions     map[smp3core.SessionID]*preferredServerSession
	attach       map[smp3core.SessionID][2]int
	servers      []net.Conn
}

func newPreferredFakeCarrier(password string) *preferredFakeCarrier {
	f := &preferredFakeCarrier{
		password: password,
		sessions: make(map[smp3core.SessionID]*preferredServerSession),
		attach:   make(map[smp3core.SessionID][2]int),
	}
	for id := 0; id < 2; id++ {
		f.started[id] = make(chan struct{})
		f.returned[id] = make(chan struct{})
		f.release[id] = make(chan struct{})
	}
	return f
}

func (f *preferredFakeCarrier) Open(ctx context.Context, sessionID smp3core.SessionID, legID uint8) (net.Conn, error) {
	if legID > 1 {
		return nil, errors.New("invalid fake leg")
	}
	f.mu.Lock()
	f.opens[legID]++
	fail := f.fail[legID]
	ignoreCancel := f.ignoreCancel[legID]
	f.mu.Unlock()
	defer f.returnOnce[legID].Do(func() { close(f.returned[legID]) })
	f.startOnce[legID].Do(func() { close(f.started[legID]) })
	if ignoreCancel {
		<-f.release[legID]
	} else {
		select {
		case <-f.release[legID]:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail != nil {
		return nil, fail
	}
	client, server := net.Pipe()
	f.mu.Lock()
	f.servers = append(f.servers, server)
	f.mu.Unlock()
	go f.serve(sessionID, legID, server)
	return client, nil
}

func (f *preferredFakeCarrier) serve(sessionID smp3core.SessionID, legID uint8, conn net.Conn) {
	reader := bufio.NewReader(conn)
	hello, err := smp3core.ReadHelloAt(reader, []byte(f.password), time.Now())
	if err != nil {
		_ = conn.Close()
		return
	}
	ready, err := encodeSidecarReadyV1(hello, []byte(f.password))
	if err != nil || writeAll(conn, ready) != nil {
		_ = conn.Close()
		return
	}
	f.mu.Lock()
	session := f.sessions[sessionID]
	if session == nil {
		engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
			SchedulerMode:     smp3core.StreamSchedulerStatic,
			BandwidthMbps:     []uint32{1, 1},
			QueueFrames:       256,
			MaxInflightFrames: 1024,
			MaxReorderFrames:  4096,
			RecoveryTimeout:   time.Second,
		})
		session = &preferredServerSession{engine: engine, app: app}
		f.sessions[sessionID] = session
		go preferredEcho(app)
	}
	counts := f.attach[sessionID]
	counts[legID]++
	f.attach[sessionID] = counts
	f.mu.Unlock()
	wrapped := &preferredBufferedConn{Conn: conn, reader: reader}
	if err := session.engine.AttachLeg(smp3core.LegID(legID), wrapped, nil); err != nil {
		_ = conn.Close()
		return
	}
	<-session.engine.Done()
}

func preferredEcho(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			if _, writeErr := conn.Write(buffer[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (f *preferredFakeCarrier) releaseLeg(id uint8) {
	f.releaseOnce[id].Do(func() { close(f.release[id]) })
}

func (f *preferredFakeCarrier) waitStarted(t *testing.T, id uint8) {
	t.Helper()
	select {
	case <-f.started[id]:
	case <-time.After(time.Second):
		t.Fatalf("Leg%d startup did not begin", id)
	}
}

func (f *preferredFakeCarrier) assertNotReturned(t *testing.T, id uint8) {
	t.Helper()
	select {
	case <-f.returned[id]:
		t.Fatalf("Leg%d startup was canceled after the first successful attach", id)
	default:
	}
}

func (f *preferredFakeCarrier) counts() [2]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens
}

func (f *preferredFakeCarrier) attachCounts(id smp3core.SessionID) [2]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attach[id]
}

func (f *preferredFakeCarrier) waitRxLeg(t *testing.T, id smp3core.SessionID, leg uint8, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		session := f.sessions[id]
		var received uint64
		if session != nil {
			received = session.engine.TelemetrySnapshot().RxUniqueBytesByLeg[leg]
		}
		f.mu.Unlock()
		if received > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no DATA observed on Leg%d", leg)
}

func (f *preferredFakeCarrier) waitAttachBoth(t *testing.T, id smp3core.SessionID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f.attachCounts(id) == [2]int{1, 1} {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("AttachLeg did not complete for both legs: %v", f.attachCounts(id))
}

func (f *preferredFakeCarrier) close() {
	f.mu.Lock()
	for _, session := range f.sessions {
		_ = session.engine.Close()
		_ = session.app.Close()
	}
	servers := append([]net.Conn(nil), f.servers...)
	f.mu.Unlock()
	for _, conn := range servers {
		_ = conn.Close()
	}
}

func newPreferredTestClient(t *testing.T, carrier *preferredFakeCarrier, policy string, preferred uint8, grace time.Duration) *Client {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.SMP3.Password = preferredTestPassword
	cfg.SMP3.CarrierMode = "host_bridge"
	cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
	cfg.SMP3.HostCarrier = HostCarrierOptions{
		ControlAddress: "127.0.0.1:1",
		Leg0Handle:     "leg0",
		Leg1Handle:     "leg1",
		TargetHandle:   "target",
		ConnectTimeout: Duration(time.Second),
	}
	cfg.SMP3.Stream.StartupPolicy = policy
	cfg.SMP3.Stream.StartupPreferredLeg = preferred
	cfg.SMP3.Stream.StartupGrace = NonNegativeDuration(grace)
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.hostCarrier = carrier
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startPreferredSession(t *testing.T, client *Client) *streamSession {
	t.Helper()
	session, err := newStreamSession(client, "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func writePreferredData(t *testing.T, session *streamSession, payload []byte) {
	t.Helper()
	if err := writeAll(session.app, payload); err != nil {
		t.Fatal(err)
	}
}

func TestPreferredStandaloneAnyLegReadyFirstAndActualFirstData(t *testing.T) {
	for _, test := range []struct {
		name      string
		preferred uint8
		first     uint8
	}{
		{name: "preferred-leg0", preferred: 0, first: 1},
		{name: "preferred-leg1", preferred: 1, first: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			carrier := newPreferredFakeCarrier(preferredTestPassword)
			defer carrier.close()
			client := newPreferredTestClient(t, carrier, "preferred", test.preferred, time.Second)
			result := make(chan *streamSession, 1)
			go func() {
				session, err := newStreamSession(client, "example.com:443")
				if err != nil {
					t.Errorf("preferred startup: %v", err)
					return
				}
				result <- session
			}()
			carrier.waitStarted(t, 0)
			carrier.waitStarted(t, 1)
			carrier.releaseLeg(test.first)
			var session *streamSession
			select {
			case session = <-result:
			case <-time.After(time.Second):
				t.Fatal("first successful leg did not return application session")
			}
			if got := carrier.counts(); got != [2]int{1, 1} {
				t.Fatalf("OPEN counts=%v, want one per leg", got)
			}
			carrier.assertNotReturned(t, test.preferred)
			carrier.releaseLeg(test.preferred)
			writePreferredData(t, session, []byte("first-data"))
			carrier.waitRxLeg(t, session.id, test.preferred, time.Second)
			carrier.waitAttachBoth(t, session.id, time.Second)
			deadline := time.Now().Add(time.Second)
			for session.startupInFlight() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if session.startupInFlight() {
				t.Fatal("startup owner did not hand off after companion completion")
			}
		})
	}
}

func TestPreferredStandaloneGraceExpiryIdleAndTerminalFailure(t *testing.T) {
	t.Run("grace-expiry", func(t *testing.T) {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		defer carrier.close()
		client := newPreferredTestClient(t, carrier, "preferred", 0, 40*time.Millisecond)
		carrier.releaseLeg(1)
		session := startPreferredSession(t, client)
		writePreferredData(t, session, []byte("grace-fallback"))
		carrier.waitRxLeg(t, session.id, 1, time.Second)
		carrier.releaseLeg(0)
		carrier.waitAttachBoth(t, session.id, time.Second)
		if !session.engine.HasLeg(0) {
			t.Fatal("late preferred AttachLeg did not complete")
		}
	})

	t.Run("idle-does-not-burn-grace", func(t *testing.T) {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		defer carrier.close()
		client := newPreferredTestClient(t, carrier, "preferred", 0, 30*time.Millisecond)
		carrier.releaseLeg(1)
		session := startPreferredSession(t, client)
		time.Sleep(120 * time.Millisecond)
		writePreferredData(t, session, []byte("idle-fallback"))
		carrier.waitRxLeg(t, session.id, 1, time.Second)
	})

	t.Run("terminal-failure-immediate-release", func(t *testing.T) {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		carrier.fail[0] = errors.New("preferred unavailable")
		defer carrier.close()
		client := newPreferredTestClient(t, carrier, "preferred", 0, time.Second)
		carrier.releaseLeg(1)
		session := startPreferredSession(t, client)
		writeDone := make(chan error, 1)
		go func() {
			_, err := session.app.Write([]byte("terminal-fallback"))
			writeDone <- err
		}()
		carrier.releaseLeg(0)
		select {
		case err := <-writeDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatal("terminal preferred failure did not release immediately")
		}
		carrier.waitRxLeg(t, session.id, 1, time.Second)
	})
}

func TestPreferredStandaloneFirstReadyAndBothFailure(t *testing.T) {
	t.Run("first-ready-sequential", func(t *testing.T) {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		defer carrier.close()
		client := newPreferredTestClient(t, carrier, "first-ready", 0, time.Second)
		result := make(chan *streamSession, 1)
		go func() {
			session, err := newStreamSession(client, "example.com:443")
			if err == nil {
				result <- session
			}
		}()
		carrier.waitStarted(t, 0)
		carrier.releaseLeg(0)
		carrier.waitStarted(t, 1)
		if got := carrier.counts(); got != [2]int{1, 1} {
			t.Fatalf("first-ready OPEN counts=%v", got)
		}
		carrier.releaseLeg(1)
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("first-ready session did not complete")
		}
	})

	t.Run("both-legs-fail", func(t *testing.T) {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		carrier.fail[0] = errors.New("leg0 failed")
		carrier.fail[1] = errors.New("leg1 failed")
		defer carrier.close()
		client := newPreferredTestClient(t, carrier, "preferred", 0, time.Second)
		carrier.releaseLeg(0)
		carrier.releaseLeg(1)
		if session, err := newStreamSession(client, "example.com:443"); err == nil || session != nil {
			t.Fatalf("both failure returned session=%v err=%v", session, err)
		}
		if got := carrier.counts(); got != [2]int{1, 1} {
			t.Fatalf("both-failure OPEN counts=%v", got)
		}
	})
}

func TestPreferredStandaloneCancellationAndLateResult(t *testing.T) {
	carrier := newPreferredFakeCarrier(preferredTestPassword)
	carrier.ignoreCancel[0] = true
	carrier.ignoreCancel[1] = true
	defer carrier.close()
	client := newPreferredTestClient(t, carrier, "preferred", 0, time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := newStreamSession(client, "example.com:443")
		result <- err
	}()
	carrier.waitStarted(t, 0)
	carrier.waitStarted(t, 1)
	_ = client.Close()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled startup returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("session cancellation did not stop pending startup")
	}
	carrier.releaseLeg(0)
	carrier.releaseLeg(1)
}

func TestPreferredStandaloneCloseAfterFirstAttachDoesNotResurrect(t *testing.T) {
	carrier := newPreferredFakeCarrier(preferredTestPassword)
	carrier.ignoreCancel[1] = true
	defer carrier.close()
	client := newPreferredTestClient(t, carrier, "preferred", 0, time.Second)
	carrier.releaseLeg(0)
	session := startPreferredSession(t, client)
	carrier.waitStarted(t, 1)
	carrier.assertNotReturned(t, 1)
	_ = session.Close()
	carrier.releaseLeg(1)
	select {
	case <-carrier.returned[1]:
	case <-time.After(time.Second):
		t.Fatal("late companion result did not return")
	}
	if session.engine.HasLeg(1) {
		t.Fatal("late companion resurrected a closed session")
	}
}

func TestPreferredStandaloneStress(t *testing.T) {
	for i := 0; i < 100; i++ {
		carrier := newPreferredFakeCarrier(preferredTestPassword)
		client := newPreferredTestClient(t, carrier, "preferred", uint8(i%2), time.Second)
		carrier.releaseLeg(uint8(i % 2))
		session := startPreferredSession(t, client)
		carrier.releaseLeg(uint8((i + 1) % 2))
		writePreferredData(t, session, []byte(fmt.Sprintf("stress-%03d", i)))
		carrier.waitRxLeg(t, session.id, uint8(i%2), time.Second)
		_ = session.Close()
		carrier.close()
		_ = client.Close()
	}
}

func TestPreferredStandaloneStreamIntegrity(t *testing.T) {
	carrier := newPreferredFakeCarrier(preferredTestPassword)
	defer carrier.close()
	client := newPreferredTestClient(t, carrier, "preferred", 0, time.Second)
	carrier.releaseLeg(0)
	session := startPreferredSession(t, client)
	carrier.releaseLeg(1)
	payload := make([]byte, 16*1024*1024)
	for i := range payload {
		payload[i] = byte((i*31 + 17) % 251)
	}
	source := sha256.Sum256(payload)
	if err := session.app.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(session.app, payload); err != nil {
		t.Fatal(err)
	}
	destination := make([]byte, len(payload))
	if _, err := io.ReadFull(session.app, destination); err != nil {
		t.Fatal(err)
	}
	got := sha256.Sum256(destination)
	if source != got {
		t.Fatalf("preferred stream hash source=%x destination=%x", source, got)
	}
	carrier.waitRxLeg(t, session.id, 0, time.Second)
}

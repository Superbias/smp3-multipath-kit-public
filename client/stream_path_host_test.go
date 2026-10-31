package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const streamPathTestPassword = "stream-path-host-test-password"

type handshakeScript uint8

const (
	handshakeCloseAfterHello handshakeScript = iota
	handshakeLegacyReady
	handshakeInvalidV6Ready
	handshakePathAwareReady
	handshakeV6ReadyWithoutRTT
	handshakePartialV6Ready
	handshakeWaitForReady
	handshakeV7DrainReady
	handshakeV7ReadyWithoutDrain
	handshakeInvalidV7Ready
)

type scriptedHandshakeCarrier struct {
	mu        sync.Mutex
	responses []handshakeScript
	versions  []smp3core.Version
	opens     int
}

func (c *scriptedHandshakeCarrier) Open(ctx context.Context, _ smp3core.SessionID, _ uint8) (net.Conn, error) {
	c.mu.Lock()
	index := c.opens
	c.opens++
	var response handshakeScript
	if index < len(c.responses) {
		response = c.responses[index]
	}
	c.mu.Unlock()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	clientConn, err := dialer.DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	serverConn, err := listener.Accept()
	_ = listener.Close()
	if err != nil {
		_ = clientConn.Close()
		return nil, err
	}
	go func() {
		defer serverConn.Close()
		reader := bufio.NewReader(serverConn)
		if response == handshakeCloseAfterHello {
			prefix := make([]byte, 6)
			if _, err := io.ReadFull(reader, prefix); err == nil {
				c.mu.Lock()
				c.versions = append(c.versions, smp3core.Version(prefix[4]))
				c.mu.Unlock()
			}
			return
		}
		hello, err := smp3core.ReadHelloAt(reader, []byte(streamPathTestPassword), time.Now())
		if err != nil {
			return
		}
		c.mu.Lock()
		c.versions = append(c.versions, hello.Version)
		c.mu.Unlock()
		switch response {
		case handshakeLegacyReady:
			ready, err := encodeSidecarReadyV1(hello, []byte(streamPathTestPassword))
			if err == nil {
				_ = writeAll(serverConn, ready)
			}
		case handshakeInvalidV6Ready:
			ready, err := smp3core.EncodeStreamReadyV2(hello, smp3core.StreamCapabilityPathRTT, [16]byte{1}, []byte(streamPathTestPassword))
			if err == nil {
				ready[len(ready)-1] ^= 1
				_ = writeAll(serverConn, ready)
			}
		case handshakePathAwareReady, handshakeV6ReadyWithoutRTT:
			selected := uint32(0)
			if response == handshakePathAwareReady {
				selected = smp3core.StreamCapabilityPathRTT
			}
			ready, err := smp3core.EncodeStreamReadyV2(hello, selected, [16]byte{0x5a}, []byte(streamPathTestPassword))
			if err == nil {
				_ = writeAll(serverConn, ready)
			}
		case handshakePartialV6Ready:
			ready, err := smp3core.EncodeStreamReadyV2(hello, 0, [16]byte{0x5b}, []byte(streamPathTestPassword))
			if err == nil {
				_ = writeAll(serverConn, ready[:12])
			}
		case handshakeV7DrainReady, handshakeV7ReadyWithoutDrain, handshakeInvalidV7Ready:
			selected := uint32(0)
			if response == handshakeV7DrainReady {
				selected = smp3core.StreamCapabilityCoordinatedDrain | smp3core.StreamCapabilityPathRTT
			}
			ready, err := smp3core.EncodeStreamReadyV2(hello, selected, [16]byte{0x7a}, []byte(streamPathTestPassword))
			if err == nil && response == handshakeInvalidV7Ready {
				ready[len(ready)-1] ^= 1
			}
			if err == nil {
				_ = writeAll(serverConn, ready)
			}
		case handshakeWaitForReady:
			time.Sleep(250 * time.Millisecond)
		}
	}()
	return clientConn, nil
}

func TestStreamV7CoordinatedDrainNegotiationAndLegacyFallbackUseFreshCarriers(t *testing.T) {
	t.Run("authenticated-v7-drain", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeV7DrainReady}}
		session := newHandshakeTestSession(carrier, false)
		conn, auth, err := session.dialLeg(0)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if auth == nil || !auth.CoordinatedDrainEnabled() || !auth.PathRTTEnabled() {
			t.Fatalf("V7 authenticated capabilities not returned: auth=%+v", auth)
		}
		if !session.v7WasAccepted(0) {
			t.Fatal("authenticated V7 capability was not pinned for this leg")
		}
		opens, versions := carrier.snapshot()
		if opens != 1 || len(versions) != 1 || versions[0] != smp3core.Version7 {
			t.Fatalf("V7 negotiation opened unexpected carriers: opens=%d versions=%v", opens, versions)
		}
	})

	t.Run("old-peer-requires-fresh-v6-and-v4-carriers", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{
			handshakeCloseAfterHello, handshakeLegacyReady, handshakeLegacyReady,
		}}
		session := newHandshakeTestSession(carrier, true)
		session.client.cfg.SMP3.Stream.AllowV7LegacyFallback = true
		conn, auth, err := session.dialLeg(0)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if auth != nil {
			t.Fatal("legacy fallback unexpectedly produced authenticated capabilities")
		}
		opens, versions := carrier.snapshot()
		want := []smp3core.Version{smp3core.Version7, smp3core.Version6, smp3core.Version4}
		if opens != len(want) || len(versions) != len(want) {
			t.Fatalf("legacy fallback did not use fresh carriers: opens=%d versions=%v", opens, versions)
		}
		for i := range want {
			if versions[i] != want[i] {
				t.Fatalf("carrier %d HELLO version=%d, want %d (all=%v)", i, versions[i], want[i], versions)
			}
		}
	})

	t.Run("authenticated-v7-without-drain-retries-v6-on-new-carrier", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{
			handshakeV7ReadyWithoutDrain, handshakePathAwareReady,
		}}
		session := newHandshakeTestSession(carrier, false)
		conn, auth, err := session.dialLeg(0)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if auth == nil || !auth.PathRTTEnabled() || auth.CoordinatedDrainEnabled() {
			t.Fatalf("V6 compatibility did not preserve only the negotiated capability: auth=%+v", auth)
		}
		opens, versions := carrier.snapshot()
		if opens != 2 || len(versions) != 2 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 {
			t.Fatalf("capability fallback did not use a fresh V6 carrier: opens=%d versions=%v", opens, versions)
		}
	})

	t.Run("authenticated-v7-rejection-does-not-downgrade", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeInvalidV7Ready}}
		session := newHandshakeTestSession(carrier, true)
		if conn, _, err := session.dialLeg(0); err == nil {
			_ = conn.Close()
			t.Fatal("invalid V7 READY authentication was accepted")
		}
		opens, versions := carrier.snapshot()
		if opens != 1 || len(versions) != 1 || versions[0] != smp3core.Version7 {
			t.Fatalf("invalid V7 authentication triggered downgrade: opens=%d versions=%v", opens, versions)
		}
	})
}

func TestStreamV7TransportFailureDoesNotSilentlyDowngradeByDefault(t *testing.T) {
	carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeCloseAfterHello, handshakeLegacyReady}}
	session := newHandshakeTestSession(carrier, false)
	if conn, _, err := session.dialLeg(0); err == nil {
		_ = conn.Close()
		t.Fatal("ambiguous V7 transport failure unexpectedly downgraded to an older HELLO")
	}
	opens, versions := carrier.snapshot()
	if opens != 1 || len(versions) != 1 || versions[0] != smp3core.Version7 {
		t.Fatalf("default V7 transport failure opened fallback Carrier: opens=%d versions=%v", opens, versions)
	}
}

func TestStreamV7TransportFallbackRequiresExplicitOptInAndFreshCarrier(t *testing.T) {
	carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeCloseAfterHello, handshakeLegacyReady}}
	session := newHandshakeTestSession(carrier, false)
	session.client.cfg.SMP3.Stream.ExperimentalPathAware = false
	session.client.cfg.SMP3.Stream.AllowV7LegacyFallback = true
	conn, _, err := session.dialLeg(0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	opens, versions := carrier.snapshot()
	if opens != 2 || len(versions) != 2 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version4 {
		t.Fatalf("explicit compatibility mode did not use a fresh V4 Carrier: opens=%d versions=%v", opens, versions)
	}
}

func (c *scriptedHandshakeCarrier) snapshot() (int, []smp3core.Version) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens, append([]smp3core.Version(nil), c.versions...)
}

func TestStreamV6TransportFailureFallbackRequiresBothExplicitOptIns(t *testing.T) {
	carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{
		handshakeV7ReadyWithoutDrain, handshakeCloseAfterHello, handshakeLegacyReady,
	}}
	session := newHandshakeTestSession(carrier, true)
	conn, auth, err := session.dialLeg(0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if auth != nil {
		t.Fatal("legacy READY unexpectedly produced path RTT authentication")
	}
	opens, versions := carrier.snapshot()
	if opens != 3 || len(versions) != 3 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 || versions[2] != smp3core.Version4 {
		t.Fatalf("fallback did not use fresh V7, V6, then V4 carriers: opens=%d versions=%v", opens, versions)
	}
}

func TestStreamV6TransportFailureNeedsSeparateFallbackOptIn(t *testing.T) {
	carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeV7ReadyWithoutDrain, handshakeCloseAfterHello}}
	session := newHandshakeTestSession(carrier, true)
	session.client.cfg.SMP3.Stream.AllowPathRTTV4FallbackOnTransportError = false
	if conn, _, err := session.dialLeg(0); err == nil {
		_ = conn.Close()
		t.Fatal("ambiguous V6 transport failure downgraded without the transport-error opt-in")
	}
	opens, versions := carrier.snapshot()
	if opens != 2 || len(versions) != 2 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 {
		t.Fatalf("V6 transport failure without the second opt-in attempted V4: opens=%d versions=%v", opens, versions)
	}
}

func TestStreamV6PartialAndTimedOutHandshakeFallbackUseFreshV4Carrier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response handshakeScript
	}{
		{name: "partial-ready", response: handshakePartialV6Ready},
		{name: "ready-timeout", response: handshakeWaitForReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeV7ReadyWithoutDrain, tc.response, handshakeLegacyReady}}
			session := newHandshakeTestSession(carrier, true)
			session.client.cfg.SMP3.CarrierReadyTimeout = Duration(40 * time.Millisecond)
			conn, auth, err := session.dialLeg(0)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if auth != nil {
				t.Fatal("transport fallback retained V6 path authentication")
			}
			opens, versions := carrier.snapshot()
			if opens != 3 || len(versions) != 3 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 || versions[2] != smp3core.Version4 {
				t.Fatalf("transport fallback did not use fresh V7, V6, then V4 carriers: opens=%d versions=%v", opens, versions)
			}
		})
	}
}

func TestStreamV6AcceptedWithoutRTTCannotLaterDowngrade(t *testing.T) {
	carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{
		handshakeV7ReadyWithoutDrain, handshakeV6ReadyWithoutRTT,
		handshakeCloseAfterHello, handshakeCloseAfterHello,
	}}
	session := newHandshakeTestSession(carrier, true)
	session.client.cfg.SMP3.Stream.AllowV7LegacyFallback = true
	conn, auth, err := session.dialLeg(0)
	if err != nil {
		t.Fatal(err)
	}
	if auth != nil {
		t.Fatal("server selected no RTT capability but client returned path auth")
	}
	_ = conn.Close()
	if !session.v6Accepted[0].Load() {
		t.Fatal("authenticated V6 READY without RTT capability was not remembered")
	}
	if conn, _, err := session.dialLeg(0); err == nil {
		_ = conn.Close()
		t.Fatal("a leg that previously accepted V6 silently downgraded after transport failure")
	}
	opens, versions := carrier.snapshot()
	if opens != 4 || len(versions) != 4 ||
		versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 ||
		versions[2] != smp3core.Version7 || versions[3] != smp3core.Version6 {
		t.Fatalf("sticky V6 policy emitted a V4 retry or skipped fresh V7/V6 negotiation: opens=%d versions=%v", opens, versions)
	}
}

func TestStreamV6FallbackDefaultsOffAndAuthenticationFailureNeverDowngrades(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeV7ReadyWithoutDrain, handshakeCloseAfterHello}}
		session := newHandshakeTestSession(carrier, false)
		if conn, _, err := session.dialLeg(0); err == nil {
			_ = conn.Close()
			t.Fatal("V6 transport failure unexpectedly downgraded without explicit V4 fallback settings")
		}
		opens, versions := carrier.snapshot()
		if opens != 2 || len(versions) != 2 || versions[0] != smp3core.Version7 || versions[1] != smp3core.Version6 {
			t.Fatalf("disabled V6-to-V4 fallback attempted V4: opens=%d versions=%v", opens, versions)
		}
	})
	t.Run("bad-authentication", func(t *testing.T) {
		carrier := &scriptedHandshakeCarrier{responses: []handshakeScript{handshakeInvalidV7Ready}}
		session := newHandshakeTestSession(carrier, true)
		if conn, _, err := session.dialLeg(0); err == nil {
			_ = conn.Close()
			t.Fatal("invalid V6 READY authentication was accepted")
		}
		opens, versions := carrier.snapshot()
		if opens != 1 || len(versions) != 1 || versions[0] != smp3core.Version7 {
			t.Fatalf("authentication failure triggered protocol downgrade: opens=%d versions=%v", opens, versions)
		}
	})
}

func newHandshakeTestSession(carrier hostCarrierOpener, allowFallback bool) *streamSession {
	cfg := DefaultConfig()
	cfg.SMP3.Password = streamPathTestPassword
	cfg.SMP3.Stream.ExperimentalPathAware = true
	cfg.SMP3.Stream.AllowPathRTTV4Fallback = allowFallback
	cfg.SMP3.Stream.AllowPathRTTV4FallbackOnTransportError = allowFallback
	cfg.SMP3.CarrierReadyTimeout = Duration(time.Second)
	client := &Client{cfg: cfg, hostCarrier: carrier, ctx: context.Background()}
	var id smp3core.SessionID
	id[0] = 0x5a
	return &streamSession{client: client, id: id, destination: "example.com:443", ctx: context.Background()}
}

// A separate real Core pair proves that the client host adapter passes the
// authenticated V6 capability into the shared engine and that same-Carrier
// probes do not break ordinary stream traffic.
type pathAwareLoopbackCarrier struct {
	password   string
	readyDelay time.Duration
	mu         sync.Mutex
	engine     *smp3core.StreamEngine
	servers    []net.Conn
	opened     chan struct{}
	pathRead   atomic.Uint32
}

type pathFrameCountingConn struct {
	net.Conn
	reads *atomic.Uint32
}

func (c pathFrameCountingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == smp3core.StreamFrameHeaderSize && smp3core.StreamFrameType(p[0]) == smp3core.StreamFramePathRTT {
		c.reads.Add(1)
	}
	return n, err
}

func newPathAwareLoopbackCarrier() *pathAwareLoopbackCarrier {
	return &pathAwareLoopbackCarrier{password: streamPathTestPassword, readyDelay: 2 * time.Millisecond, opened: make(chan struct{}, 1)}
}

func (c *pathAwareLoopbackCarrier) Open(_ context.Context, _ smp3core.SessionID, _ uint8) (net.Conn, error) {
	clientConn, serverConn := net.Pipe()
	c.mu.Lock()
	c.servers = append(c.servers, serverConn)
	c.mu.Unlock()
	go c.serve(serverConn)
	return clientConn, nil
}

func (c *pathAwareLoopbackCarrier) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	hello, err := smp3core.ReadHelloAt(reader, []byte(c.password), time.Now())
	if err != nil {
		_ = conn.Close()
		return
	}
	if c.readyDelay > 0 {
		time.Sleep(c.readyDelay)
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
	engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
		ExperimentalPathAware: true,
		PathRole:              smp3core.StreamPathRoleServer,
		SchedulerMode:         smp3core.StreamSchedulerStatic,
		BandwidthMbps:         []uint32{1, 1},
		QueueFrames:           32,
		MaxInflightFrames:     64,
		MaxReorderFrames:      128,
		RecoveryTimeout:       time.Second,
	})
	c.mu.Lock()
	c.engine = engine
	c.mu.Unlock()
	wrapped := pathFrameCountingConn{
		Conn:  &preferredBufferedConn{Conn: conn, reader: reader},
		reads: &c.pathRead,
	}
	if err := engine.AttachLegWithProtocolAuth(hello.LegID, wrapped, nil, auth); err != nil {
		_ = conn.Close()
		_ = engine.Close()
		return
	}
	c.opened <- struct{}{}
	go preferredEcho(app)
	<-engine.Done()
}

func (c *pathAwareLoopbackCarrier) Close() {
	c.mu.Lock()
	servers := append([]net.Conn(nil), c.servers...)
	engine := c.engine
	c.mu.Unlock()
	if engine != nil {
		_ = engine.Close()
	}
	for _, conn := range servers {
		_ = conn.Close()
	}
}

func TestStreamPathAwareClientUsesAuthenticatedCarrierAndPreservesData(t *testing.T) {
	carrier := newPathAwareLoopbackCarrier()
	defer carrier.Close()
	session := newHandshakeTestSession(carrier, false)
	session.client.cfg.SMP3.Stream.AllowPathRTTV4Fallback = false
	conn, auth, err := session.dialLeg(0)
	if err != nil {
		t.Fatal(err)
	}
	if auth == nil || !auth.PathRTTEnabled() {
		_ = conn.Close()
		t.Fatal("authenticated V6 path RTT capability was not returned to the client adapter")
	}
	if handshakeRTT, ok := auth.CarrierHandshakeRTT(); !ok {
		_ = conn.Close()
		t.Fatalf("client did not retain same-Carrier handshake RTT: rtt=%v present=%v", handshakeRTT, ok)
	}
	cfg := session.client.cfg.SMP3.streamConfig(nil, nil)
	engine, app := smp3core.NewStreamEngine(cfg)
	var clientPathRead atomic.Uint32
	countedConn := pathFrameCountingConn{Conn: conn, reads: &clientPathRead}
	if err := attachStreamLeg(engine, 0, countedConn, auth); err != nil {
		_ = conn.Close()
		_ = engine.Close()
		t.Fatal(err)
	}
	if feedback, ok := engine.PathFeedback(0); !ok || feedback.RTT <= 0 || feedback.Samples == 0 || feedback.Identity.Direction != smp3core.StreamRTTClientToServer {
		t.Fatalf("client same-Carrier handshake RTT did not seed shared Core: feedback=%+v ok=%v", feedback, ok)
	}
	t.Cleanup(func() { _ = engine.Close(); _ = app.Close() })
	select {
	case <-carrier.opened:
	case <-time.After(time.Second):
		t.Fatal("server Core did not attach the V6 Carrier")
	}
	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		carrier.mu.Lock()
		serverEngine := carrier.engine
		carrier.mu.Unlock()
		if !engine.HasLeg(0) || serverEngine == nil || !serverEngine.HasLeg(0) {
			t.Fatal("same-Carrier RTT probe exchange caused an unexpected leg failure")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if clientPathRead.Load() == 0 || carrier.pathRead.Load() == 0 {
		t.Fatalf("same-Carrier probe/echo not observed: client_received=%d server_received=%d", clientPathRead.Load(), carrier.pathRead.Load())
	}
	payload := bytes.Repeat([]byte("v6-shared-core-path-feedback/"), 128)
	_ = app.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeAll(app, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(payload) {
		t.Fatal("V6 stream payload hash mismatch")
	}
	carrier.mu.Lock()
	serverEngine := carrier.engine
	carrier.mu.Unlock()
	if !engine.HasLeg(0) || serverEngine == nil || !serverEngine.HasLeg(0) {
		t.Fatal("path-aware Core leg was lost during stream transfer")
	}
}

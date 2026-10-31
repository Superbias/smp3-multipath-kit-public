package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
)

type smp3TestProxy struct {
	C.Proxy
	name      string
	backend   *smp3TestBackend
	dialErr   error
	dialDelay time.Duration
	started   chan struct{}
	startOnce sync.Once
	failed    chan struct{}
	failOnce  sync.Once
}

func (p *smp3TestProxy) Name() string           { return p.name }
func (p *smp3TestProxy) Type() C.AdapterType    { return C.Socks5 }
func (p *smp3TestProxy) Addr() string           { return p.name + ":carrier" }
func (p *smp3TestProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }
func (p *smp3TestProxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"type": "socks5", "name": p.name})
}
func (p *smp3TestProxy) DialContext(_ context.Context, metadata *C.Metadata) (C.Conn, error) {
	if p.started != nil {
		p.startOnce.Do(func() { close(p.started) })
	}
	if p.dialDelay > 0 {
		time.Sleep(p.dialDelay)
	}
	if p.dialErr != nil {
		if p.failed != nil {
			p.failOnce.Do(func() { close(p.failed) })
		}
		return nil, p.dialErr
	}
	p.backend.recordTarget(metadata.RemoteAddress())
	client, server := net.Pipe()
	p.backend.accept(server)
	return NewConn(client, p), nil
}

func TestSMP3StartupConfigMapsPreferredCorePolicy(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs:          []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}},
		StartupPolicy: "preferred", StartupPreferredLeg: 1, StartupGrace: "40ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.streamConfig.StartupPolicy != smp3core.StreamStartupPreferred || adapter.streamConfig.StartupPreferredLeg != 1 || adapter.streamConfig.StartupGrace != 40*time.Millisecond {
		t.Fatalf("startup config=%+v", adapter.streamConfig)
	}
	if _, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, StartupPolicy: "invalid",
	}); err == nil {
		t.Fatal("invalid startup policy accepted")
	}
}

func TestSMP3AggregationSchedulerMapsToCore(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, SchedulerMode: "aggregation",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.streamConfig.SchedulerMode != smp3core.StreamSchedulerAggregation {
		t.Fatalf("scheduler mode=%v, want aggregation", adapter.streamConfig.SchedulerMode)
	}
}

func TestSMP3DynamicCapacityEnablesCoreAdmissionProtection(t *testing.T) {
	for _, mode := range []string{"fixed", "dynamic"} {
		cfg, err := makeStreamConfig(SMP3Option{SchedulerMode: "aggregation", CapacityMode: mode, BandwidthMbps: []uint32{100, 100}})
		if err != nil {
			t.Fatal(err)
		}
		engine, _ := smp3core.NewStreamEngine(cfg)
		stats := engine.Snapshot()
		engine.Close()
		if mode == "dynamic" && stats.OrdinaryAdmissionState != [2]string{"normal", "normal"} {
			t.Fatalf("Native dynamic did not enable canonical Core protection: %v", stats.OrdinaryAdmissionState)
		}
		if mode == "fixed" && stats.OrdinaryAdmissionState != [2]string{} {
			t.Fatal("fixed mode changed admission semantics")
		}
	}
}

func TestSMP3FallbackCapacityRemainsSessionPrivate(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{Name: "mp", Server: "127.0.0.1", Port: 24444, Password: "local", Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, Leg1Fallback: "fallback", SchedulerMode: "aggregation", CapacityMode: "dynamic", BandwidthMbps: []uint32{100, 200}})
	if err != nil {
		t.Fatal(err)
	}
	p1, p2 := adapter.capacityProvider(), adapter.capacityProvider()
	defer p1.(interface{ Close() }).Close()
	defer p2.(interface{ Close() }).Close()
	now := time.Unix(100, 0)
	p1.Admit(1, 256<<10, true, now)
	p1.Ack(1, 256<<10, now.Add(2*time.Second))
	_, e1, _, v1, _, _ := p1.Telemetry()
	_, e2, _, v2, _, _ := p2.Telemetry()
	if !v1[1] || v2[1] || e1[1] == e2[1] {
		t.Fatal("fallback-capable slot shared physical capacity across sessions")
	}
}

func TestSMP3DynamicActivationMapsToCore(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, SchedulerMode: "aggregation",
		ActivationMode: "dynamic", CapacityMode: "dynamic",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.streamConfig.ActivationMode != smp3core.StreamActivationDynamic {
		t.Fatalf("activation mode=%v, want dynamic", adapter.streamConfig.ActivationMode)
	}
	if _, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, ActivationMode: "dynamic",
	}); err == nil {
		t.Fatal("dynamic activation without aggregation was accepted")
	}
}

func TestSMP3HOLCompletionMapsToCore(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, SchedulerMode: "aggregation", HOLMode: "completion",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.streamConfig.HOLMode != smp3core.StreamHOLCompletion {
		t.Fatalf("HOL mode=%v, want completion", adapter.streamConfig.HOLMode)
	}
	if _, err := NewSMP3(SMP3Option{
		Name: "mp", Server: "10.66.66.1", Port: 24444, Password: "pw",
		Legs: []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}}, HOLMode: "completion",
	}); err == nil {
		t.Fatal("HOL completion without aggregation was accepted")
	}
}

func TestSMP3PreferredTerminalLegFailureReleasesThroughCore(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	line := &smp3TestProxy{name: "line-path", backend: backend}
	failedPreferred := &smp3TestProxy{name: "public-hy2", backend: backend, dialErr: errors.New("preferred unavailable"), failed: make(chan struct{})}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:          []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
		StartupPolicy: "preferred", StartupPreferredLeg: 1, StartupGrace: "1s", RedialInterval: "100ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "line-path":
			return line, true
		case "public-hy2":
			return failedPreferred, true
		default:
			return nil, false
		}
	}
	conn, err := adapter.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	serverApp := <-backend.appReady
	select {
	case <-failedPreferred.failed:
	case <-time.After(time.Second):
		t.Fatal("preferred terminal failure was not observed")
	}
	if _, err := conn.Write([]byte("terminal-fallback")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("terminal-fallback"))
	if _, err := io.ReadFull(serverApp, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "terminal-fallback" {
		t.Fatalf("server received %q", got)
	}
}

func TestSMP3PreferredBootstrapAllowsLeg1First(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	slowLeg0 := &smp3TestProxy{name: "line-path", backend: backend, dialDelay: 200 * time.Millisecond, started: make(chan struct{})}
	fastLeg1 := &smp3TestProxy{name: "public-hy2", backend: backend, started: make(chan struct{})}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:          []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
		StartupPolicy: "preferred", StartupPreferredLeg: 1, StartupGrace: "100ms",
		RedialInterval: "100ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "line-path":
			return slowLeg0, true
		case "public-hy2":
			return fastLeg1, true
		default:
			return nil, false
		}
	}
	started := time.Now()
	conn, err := adapter.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if elapsed := time.Since(started); elapsed >= 180*time.Millisecond {
		t.Fatalf("preferred fast leg did not win bootstrap promptly: %v", elapsed)
	}
	select {
	case <-slowLeg0.started:
	case <-time.After(time.Second):
		t.Fatal("slow Leg0 attempt did not start concurrently")
	}
	backend.waitForLegs(t, 2)
}

func TestSMP3DestinationFormattingParity(t *testing.T) {
	tests := []string{"192.0.2.1:443", "example.com:443", "[2001:db8::1]:443"}
	for _, address := range tests {
		metadata := &C.Metadata{NetWork: C.TCP}
		if err := metadata.SetRemoteAddress(address); err != nil {
			t.Fatal(err)
		}
		if got := metadata.RemoteAddress(); got != address {
			t.Fatalf("RemoteAddress() = %q, want %q", got, address)
		}
	}
}

func TestSMP3Leg1PrimaryFailureUsesFallback(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	primary := &smp3TestProxy{name: "public-hy2", backend: backend, dialErr: errors.New("primary unavailable")}
	fallback := &smp3TestProxy{name: "public-snell", backend: backend}
	adapter, err := NewSMP3(SMP3Option{Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password, Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}}, Leg1Fallback: "public-snell"})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "public-hy2":
			return primary, true
		case "public-snell":
			return fallback, true
		default:
			return nil, false
		}
	}
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	conn, tag, _, err := session.dialCarrier(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tag != "public-snell" {
		t.Fatalf("fallback carrier = %q, want public-snell", tag)
	}
	hello := <-backend.helloReady
	if hello.LegID != 1 || hello.Destination != "example.com:443" {
		t.Fatalf("unexpected fallback HELLO: leg=%d destination=%q", hello.LegID, hello.Destination)
	}
}

type smp3TestServerSession struct {
	engine *smp3core.StreamEngine
	app    net.Conn
	legs   map[uint8]net.Conn
	sid    smp3core.SessionID
}

type smp3PathFrameCountingConn struct {
	C.Conn
	reads *atomic.Uint32
}

func (c smp3PathFrameCountingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == smp3core.StreamFrameHeaderSize && smp3core.StreamFrameType(p[0]) == smp3core.StreamFramePathRTT {
		c.reads.Add(1)
	}
	return n, err
}

type smp3PathFrameCountingNetConn struct {
	net.Conn
	reads *atomic.Uint32
}

func (c smp3PathFrameCountingNetConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == smp3core.StreamFrameHeaderSize && smp3core.StreamFrameType(p[0]) == smp3core.StreamFramePathRTT {
		c.reads.Add(1)
	}
	return n, err
}

type smp3TestBackend struct {
	password           string
	pathAware          bool
	closeV6BeforeReady bool
	invalidV6          bool
	readyDelay         time.Duration
	pathReads          atomic.Uint32

	mu         sync.Mutex
	targets    []string
	accepts    int
	session    *smp3TestServerSession
	appReady   chan net.Conn
	legReady   chan struct{}
	helloReady chan smp3core.Hello
}

func newSMP3TestBackend(password string) *smp3TestBackend {
	return &smp3TestBackend{
		password:   password,
		appReady:   make(chan net.Conn, 1),
		legReady:   make(chan struct{}, 8),
		helloReady: make(chan smp3core.Hello, 8),
	}
}

func (b *smp3TestBackend) recordTarget(target string) {
	b.mu.Lock()
	b.targets = append(b.targets, target)
	b.mu.Unlock()
}

func (b *smp3TestBackend) accept(conn net.Conn) {
	go func() {
		hello, err := smp3core.ReadHelloAt(conn, []byte(b.password), time.Now())
		if err != nil {
			_ = conn.Close()
			return
		}
		var pathAuth *smp3core.StreamPathAuth
		if hello.Version == smp3core.Version6 {
			b.helloReady <- hello
			b.mu.Lock()
			closeV6BeforeReady, pathAware, invalidV6, readyDelay := b.closeV6BeforeReady, b.pathAware, b.invalidV6, b.readyDelay
			b.mu.Unlock()
			if closeV6BeforeReady {
				_ = conn.Close()
				return
			}
			selected := uint32(0)
			if pathAware {
				selected = smp3core.StreamCapabilityPathRTT
			}
			if readyDelay > 0 {
				time.Sleep(readyDelay)
			}
			var auth smp3core.StreamPathAuth
			if invalidV6 {
				ready, encodeErr := smp3core.EncodeStreamReadyV2(hello, selected, [16]byte{0x44}, []byte(b.password))
				if encodeErr != nil {
					_ = conn.Close()
					return
				}
				ready[len(ready)-1] ^= 1
				if writeSMP3TestAll(conn, ready) != nil {
					_ = conn.Close()
					return
				}
				return
			}
			auth, err = smp3core.WriteStreamReadyV2(conn, hello, selected, []byte(b.password))
			if err != nil {
				_ = conn.Close()
				return
			}
			if auth.PathRTTEnabled() {
				pathAuth = &auth
			}
		} else {
			b.helloReady <- hello
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		b.accepts++
		if b.session == nil {
			streamCfg := smp3core.StreamConfig{
				ThresholdBytesPS:  0,
				QueueFrames:       64,
				MaxInflightFrames: 64,
				RecoveryTimeout:   time.Second,
			}
			if pathAuth != nil {
				streamCfg.ExperimentalPathAware = true
				streamCfg.PathRole = smp3core.StreamPathRoleServer
			}
			engine, app := smp3core.NewStreamEngine(streamCfg)
			b.session = &smp3TestServerSession{engine: engine, app: app, legs: make(map[uint8]net.Conn), sid: hello.SessionID}
			b.appReady <- app
		}
		if hello.SessionID != b.session.sid {
			_ = conn.Close()
			return
		}
		var attachErr error
		if pathAuth != nil {
			attachConn := smp3PathFrameCountingNetConn{Conn: conn, reads: &b.pathReads}
			attachErr = b.session.engine.AttachLegWithPathAuth(hello.LegID, attachConn, nil, *pathAuth)
		} else {
			attachErr = b.session.engine.AttachLeg(hello.LegID, conn, nil)
		}
		if attachErr != nil {
			_ = conn.Close()
			return
		}
		b.session.legs[uint8(hello.LegID)] = conn
		b.legReady <- struct{}{}
	}()
}

func writeSMP3TestAll(conn net.Conn, value []byte) error {
	for len(value) > 0 {
		n, err := conn.Write(value)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		value = value[n:]
	}
	return nil
}

func (b *smp3TestBackend) closeLeg(id uint8) {
	b.mu.Lock()
	if b.session != nil && b.session.legs[id] != nil {
		_ = b.session.legs[id].Close()
	}
	b.mu.Unlock()
}

func (b *smp3TestBackend) waitForLegs(t *testing.T, count int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		b.mu.Lock()
		accepts := b.accepts
		b.mu.Unlock()
		if accepts >= count {
			return
		}
		select {
		case <-b.legReady:
		case <-deadline:
			t.Fatalf("timed out waiting for %d carrier accepts; got %d", count, accepts)
		}
	}
}

func (b *smp3TestBackend) close() {
	b.mu.Lock()
	if b.session != nil {
		_ = b.session.engine.Close()
		_ = b.session.app.Close()
		for _, leg := range b.session.legs {
			_ = leg.Close()
		}
	}
	b.mu.Unlock()
}

func TestSMP3DialCarrierUsesAggregateAndCanonicalHello(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name:     "mp-jp",
		Server:   "10.66.66.1",
		Port:     24444,
		Password: password,
		Legs:     []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		if name == "line-path" {
			return proxy, true
		}
		return nil, false
	}
	session := &smp3Session{
		owner:       adapter,
		ctx:         context.Background(),
		cancel:      func() {},
		destination: "example.com:443",
	}
	session.sessionID[0] = 0x42
	conn, tag, _, err := session.dialCarrier(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tag != "line-path" {
		t.Fatalf("unexpected carrier tag %q", tag)
	}
	server := <-backend.appReady
	_ = server
	backend.mu.Lock()
	if len(backend.targets) != 1 || backend.targets[0] != "10.66.66.1:24444" {
		t.Fatalf("child target = %v, want aggregate endpoint", backend.targets)
	}
	backend.mu.Unlock()
	hello := <-backend.helloReady
	if hello.Version != smp3core.Version4 || hello.Mode != smp3core.ModeStream {
		t.Fatalf("unexpected HELLO version/mode: %v/%v", hello.Version, hello.Mode)
	}
	if hello.Destination != "example.com:443" || hello.SessionID != session.sessionID || hello.LegID != 0 {
		t.Fatalf("unexpected HELLO: destination=%q session=%x leg=%d", hello.Destination, hello.SessionID, hello.LegID)
	}
}

func TestSMP3PathAwareHandshakeAttachesAuthenticatedCoreAndKeepsCarrierAlive(t *testing.T) {
	const password = "native-path-aware-test-password"
	backend := newSMP3TestBackend(password)
	backend.pathAware = true
	backend.readyDelay = 2 * time.Millisecond
	defer backend.close()
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:                  []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "other-path"}},
		ExperimentalPathAware: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !adapter.streamConfig.ExperimentalPathAware || adapter.streamConfig.PathRole != smp3core.StreamPathRoleClient {
		t.Fatalf("Native adapter did not map opt-in to canonical client Core: %+v", adapter.streamConfig)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		if name == "line-path" {
			return proxy, true
		}
		return nil, false
	}
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	session.sessionID[0] = 0x6d
	conn, carrierName, auth, err := session.dialCarrier(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if carrierName != "line-path" || auth == nil || !auth.PathRTTEnabled() {
		t.Fatalf("V6 result carrier=%q auth=%v", carrierName, auth != nil && auth.PathRTTEnabled())
	}
	clientEngine, clientApp := smp3core.NewStreamEngine(adapter.streamConfig)
	defer clientEngine.Close()
	defer clientApp.Close()
	var clientPathFrames atomic.Uint32
	if err := attachMihomoStreamLeg(clientEngine, 0, smp3PathFrameCountingConn{Conn: conn, reads: &clientPathFrames}, auth); err != nil {
		t.Fatal(err)
	}
	if feedback, ok := clientEngine.PathFeedback(0); !ok || feedback.RTT <= 0 || feedback.Samples == 0 || feedback.Identity.Direction != smp3core.StreamRTTClientToServer {
		t.Fatalf("Native same-Carrier handshake RTT did not seed shared Core: feedback=%+v ok=%v", feedback, ok)
	}
	serverApp := <-backend.appReady
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, err := serverApp.Read(buffer)
			if n > 0 && writeSMP3TestAll(serverApp, buffer[:n]) != nil {
				return
			}
			if err != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		backend.mu.Lock()
		serverEngine := backend.session.engine
		backend.mu.Unlock()
		if !clientEngine.HasLeg(0) || serverEngine == nil || !serverEngine.HasLeg(0) {
			t.Fatal("same-Carrier RTT probe retired an authenticated Native/peer Core leg")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if clientPathFrames.Load() == 0 || backend.pathReads.Load() == 0 {
		t.Fatalf("no same-Carrier probe/echo observed: client=%d peer=%d", clientPathFrames.Load(), backend.pathReads.Load())
	}
	payload := []byte("native adapter uses the shared Core for v6 RTT and stream data")
	_ = clientApp.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeSMP3TestAll(clientApp, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(clientApp, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("Native v6 payload changed in shared Core")
	}
}

func TestSMP3V6TransportFailureUsesFreshV4ConnectionOnlyWithExplicitPolicy(t *testing.T) {
	const password = "native-v6-fallback-test-password"
	backend := newSMP3TestBackend(password)
	backend.closeV6BeforeReady = true
	defer backend.close()
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:                  []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "other-path"}},
		ExperimentalPathAware: true, AllowPathRTTV4Fallback: true, AllowPathRTTV4FallbackOnTransportError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		if name == "line-path" {
			return proxy, true
		}
		return nil, false
	}
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	conn, _, auth, err := session.dialCarrier(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if auth != nil {
		t.Fatal("V4 fallback unexpectedly retained V6 path authentication")
	}
	first, second := <-backend.helloReady, <-backend.helloReady
	if first.Version != smp3core.Version6 || second.Version != smp3core.Version4 {
		t.Fatalf("protocol fallback did not open a fresh V4 connection: %v then %v", first.Version, second.Version)
	}
	backend.mu.Lock()
	opens := len(backend.targets)
	backend.mu.Unlock()
	if opens != 2 {
		t.Fatalf("fallback opened %d carrier connections, want separate V6 and V4 connections", opens)
	}
}

func TestSMP3V6AuthenticationFailureDoesNotDowngrade(t *testing.T) {
	const password = "native-v6-auth-failure-test-password"
	backend := newSMP3TestBackend(password)
	backend.invalidV6 = true
	defer backend.close()
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:                  []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "other-path"}},
		ExperimentalPathAware: true, AllowPathRTTV4Fallback: true, AllowPathRTTV4FallbackOnTransportError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		if name == "line-path" {
			return proxy, true
		}
		return nil, false
	}
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	if conn, _, _, err := session.dialCarrier(context.Background(), 0); err == nil {
		_ = conn.Close()
		t.Fatal("invalid V6 READY was accepted")
	}
	<-backend.helloReady
	backend.mu.Lock()
	opens := len(backend.targets)
	backend.mu.Unlock()
	if opens != 1 {
		t.Fatalf("authentication failure caused V4 downgrade on a new connection: opens=%d", opens)
	}
}

func TestSMP3TransportFailureDoesNotDowngradeWithoutSeparateOptIn(t *testing.T) {
	const password = "native-v6-transport-fallback-test-password"
	backend := newSMP3TestBackend(password)
	backend.closeV6BeforeReady = true
	defer backend.close()
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:                  []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "other-path"}},
		ExperimentalPathAware: true, AllowPathRTTV4Fallback: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) { return proxy, name == "line-path" }
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	if conn, _, _, err := session.dialCarrier(context.Background(), 0); err == nil {
		_ = conn.Close()
		t.Fatal("ambiguous V6 transport failure downgraded without transport-error opt-in")
	}
	<-backend.helloReady
	backend.mu.Lock()
	opens := len(backend.targets)
	backend.mu.Unlock()
	if opens != 1 {
		t.Fatalf("transport fallback without second opt-in opened %d carriers, want one V6 attempt", opens)
	}
}

func TestSMP3PreviouslyAcceptedV6CannotLaterDowngrade(t *testing.T) {
	const password = "native-v6-sticky-version-test-password"
	backend := newSMP3TestBackend(password)
	defer backend.close()
	proxy := &smp3TestProxy{name: "line-path", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: password,
		Legs:                  []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "other-path"}},
		ExperimentalPathAware: true, AllowPathRTTV4Fallback: true, AllowPathRTTV4FallbackOnTransportError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) { return proxy, name == "line-path" }
	session := &smp3Session{owner: adapter, ctx: context.Background(), cancel: func() {}, destination: "example.com:443"}
	conn, _, auth, err := session.dialCarrier(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if auth != nil {
		t.Fatal("V6 READY selecting no RTT capability returned path auth")
	}
	<-backend.helloReady
	if !session.v6Accepted[0].Load() {
		t.Fatal("authenticated V6 READY without RTT capability was not remembered")
	}
	backend.mu.Lock()
	backend.closeV6BeforeReady = true
	backend.mu.Unlock()
	_ = conn.Close()
	if conn, _, _, err := session.dialCarrier(context.Background(), 0); err == nil {
		_ = conn.Close()
		t.Fatal("a leg that previously accepted V6 silently downgraded after transport failure")
	}
	<-backend.helloReady
	backend.mu.Lock()
	opens := len(backend.targets)
	backend.mu.Unlock()
	if opens != 2 {
		t.Fatalf("sticky V6 policy opened %d carriers, want one initial and one V6 rejoin", opens)
	}
}

func TestSMP3DialContextLocalInteropAndLeg1Rejoin(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	line := &smp3TestProxy{name: "line-path", backend: backend}
	hy2 := &smp3TestProxy{name: "public-hy2", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name:           "mp-jp",
		Server:         "10.66.66.1",
		Port:           24444,
		Password:       password,
		Legs:           []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
		Leg1Fallback:   "public-snell",
		RedialInterval: "100ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "line-path":
			return line, true
		case "public-hy2":
			return hy2, true
		case "public-snell":
			return hy2, true
		default:
			return nil, false
		}
	}
	adapter.streamConfig.ThresholdBytesPS = 0
	destination := &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443}
	conn, err := adapter.DialContext(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	serverApp := <-backend.appReady
	backend.waitForLegs(t, 2)
	initialHellos := make(map[smp3core.LegID]smp3core.Hello)
	for len(initialHellos) < 2 {
		hello := <-backend.helloReady
		initialHellos[hello.LegID] = hello
	}
	firstHello, ok0 := initialHellos[0]
	secondHello, ok1 := initialHellos[1]
	if !ok0 || !ok1 || firstHello.Destination != "example.com:443" || secondHello.Destination != "example.com:443" {
		t.Fatalf("HELLO destination/legs mismatch: %#v", initialHellos)
	}
	if firstHello.SessionID != secondHello.SessionID {
		t.Fatalf("activation changed session identity: %x/%x", firstHello.SessionID, secondHello.SessionID)
	}

	request := []byte("request-one")
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(serverApp, gotRequest); err != nil {
		t.Fatal(err)
	}
	if string(gotRequest) != string(request) {
		t.Fatalf("server request = %q, want %q", gotRequest, request)
	}
	if _, err := serverApp.Write([]byte("reply-one")); err != nil {
		t.Fatal(err)
	}
	gotReply := make([]byte, len("reply-one"))
	if _, err := io.ReadFull(conn, gotReply); err != nil {
		t.Fatal(err)
	}
	if string(gotReply) != "reply-one" {
		t.Fatalf("client reply = %q", gotReply)
	}

	backend.closeLeg(1)
	backend.waitForLegs(t, 3)
	rejoinHello := <-backend.helloReady
	if rejoinHello.SessionID != firstHello.SessionID || rejoinHello.LegID != 1 {
		t.Fatalf("rejoin changed session/leg identity: session=%x leg=%d", rejoinHello.SessionID, rejoinHello.LegID)
	}
	if _, err := conn.Write([]byte("request-two")); err != nil {
		t.Fatal(err)
	}
	gotRequest = make([]byte, len("request-two"))
	if _, err := io.ReadFull(serverApp, gotRequest); err != nil {
		t.Fatal(err)
	}
	if string(gotRequest) != "request-two" {
		t.Fatalf("post-rejoin request = %q", gotRequest)
	}
}

func TestSMP3RXActivationJoinsLeg1(t *testing.T) {
	const password = "test-password"
	backend := newSMP3TestBackend(password)
	line := &smp3TestProxy{name: "line-path", backend: backend}
	hy2 := &smp3TestProxy{name: "public-hy2", backend: backend}
	adapter, err := NewSMP3(SMP3Option{
		Name:                    "mp-jp",
		Server:                  "10.66.66.1",
		Port:                    24444,
		Password:                password,
		Legs:                    []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
		ActivationThresholdMbps: 1,
		ActivationWindow:        "100ms",
		RedialInterval:          "100ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "line-path":
			return line, true
		case "public-hy2":
			return hy2, true
		default:
			return nil, false
		}
	}

	metadata := &C.Metadata{NetWork: C.TCP, Host: "download.example", DstPort: 443}
	conn, err := adapter.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	serverApp := <-backend.appReady

	payload := bytes.Repeat([]byte("download-rx-activation"), 32*1024)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.CopyN(io.Discard, conn, int64(len(payload)))
		readDone <- readErr
	}()
	if _, err := serverApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out reading RX activation payload")
	}

	backend.waitForLegs(t, 2)
	var hellos [2]smp3core.Hello
	seen := [2]bool{}
	for !(seen[0] && seen[1]) {
		hello := <-backend.helloReady
		if hello.LegID > 1 {
			t.Fatalf("unexpected leg id %d", hello.LegID)
		}
		hellos[hello.LegID] = hello
		seen[hello.LegID] = true
	}
	if hellos[0].SessionID != hellos[1].SessionID {
		t.Fatalf("RX activation changed session identity: %x/%x", hellos[0].SessionID, hellos[1].SessionID)
	}
}

func TestSMP3ConfigRejectsInvalidChildGraph(t *testing.T) {
	available := map[string]struct{}{"line-path": {}, "public-hy2": {}, "public-snell": {}}
	if err := ValidateSMP3Config(map[string]any{
		"type": "smp3", "name": "mp-jp",
		"legs": []map[string]any{{"proxy": "line-path"}, {"proxy": "missing"}},
	}, available); err == nil {
		t.Fatal("missing child was accepted")
	}
	if err := ValidateSMP3Config(map[string]any{
		"type": "smp3", "name": "mp-jp",
		"legs": []map[string]any{{"proxy": "mp-jp"}, {"proxy": "public-hy2"}},
	}, available); err == nil {
		t.Fatal("recursive child was accepted")
	}
	if err := ValidateSMP3Config(map[string]any{
		"type": "smp3", "name": "mp-jp",
		"legs": []map[string]any{{"proxy": "line-path"}, {"proxy": "line-path"}},
	}, available); err == nil {
		t.Fatal("duplicate child was accepted")
	}
}

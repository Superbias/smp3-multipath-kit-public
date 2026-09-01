package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
)

type smp3TestProxy struct {
	C.Proxy
	name    string
	backend *smp3TestBackend
	dialErr error
}

func (p *smp3TestProxy) Name() string           { return p.name }
func (p *smp3TestProxy) Type() C.AdapterType    { return C.Socks5 }
func (p *smp3TestProxy) Addr() string           { return p.name + ":carrier" }
func (p *smp3TestProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }
func (p *smp3TestProxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"type": "socks5", "name": p.name})
}
func (p *smp3TestProxy) DialContext(_ context.Context, metadata *C.Metadata) (C.Conn, error) {
	if p.dialErr != nil {
		return nil, p.dialErr
	}
	p.backend.recordTarget(metadata.RemoteAddress())
	client, server := net.Pipe()
	p.backend.accept(server)
	return NewConn(client, p), nil
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
	conn, tag, err := session.dialCarrier(context.Background(), 1)
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

type smp3TestBackend struct {
	password string

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
		b.helloReady <- hello
		b.mu.Lock()
		defer b.mu.Unlock()
		b.accepts++
		if b.session == nil {
			engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
				ThresholdBytesPS:  0,
				QueueFrames:       64,
				MaxInflightFrames: 64,
				RecoveryTimeout:   time.Second,
			})
			b.session = &smp3TestServerSession{engine: engine, app: app, legs: make(map[uint8]net.Conn), sid: hello.SessionID}
			b.appReady <- app
		}
		if hello.SessionID != b.session.sid {
			_ = conn.Close()
			return
		}
		if err := b.session.engine.AttachLeg(hello.LegID, conn, nil); err != nil {
			_ = conn.Close()
			return
		}
		b.session.legs[uint8(hello.LegID)] = conn
		b.legReady <- struct{}{}
	}()
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
	conn, tag, err := session.dialCarrier(context.Background(), 0)
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

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
	M "github.com/metacubex/sing/common/metadata"
)

func TestSMP3UDPV5HelloUsesCanonicalEncoding(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	var sid smp3core.SessionID
	for i := range sid {
		sid[i] = byte(i + 1)
	}
	var nonce [16]byte
	for i := range nonce {
		nonce[i] = byte(0xa0 + i)
	}
	const destination = "bootstrap.example:53"
	const password = "test-password"
	now := time.Unix(1700000000, 0)
	header, dest, mac, err := smp3core.EncodeHelloParts(smp3core.Hello{Version: smp3core.Version5, SessionID: sid, LegID: 0, Mode: smp3core.ModeDatagram, Timestamp: now.Unix(), Nonce: nonce, Destination: destination}, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(append([]byte(nil), header...), dest...), mac...)
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- writeSMP3DatagramHelloAt(left, sid, destination, []byte(password), now.Unix(), nonce)
	}()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(right, got); err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("HELLO differs: got=%x want=%x", got, want)
	}
	decoded, err := smp3core.ReadHelloAt(bytes.NewReader(got), []byte(password), now)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version != smp3core.Version5 || decoded.Mode != smp3core.ModeDatagram || decoded.LegID != 0 || decoded.SessionID != sid || decoded.Destination != destination {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestSMP3UDPSessionIDUnique(t *testing.T) {
	seen := make(map[smp3core.SessionID]struct{}, 100)
	for i := 0; i < 100; i++ {
		sid, err := newSMP3UDPSessionID()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[sid]; ok {
			t.Fatalf("duplicate SessionID at %d", i)
		}
		seen[sid] = struct{}{}
	}
}

func TestSMP3UDPSingleLegLocalInterop(t *testing.T) {
	server := newB1Server("test-password")
	child := newB1Proxy("line-path", server)
	adapter := newB1Adapter(t, child)
	metadata := &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53}
	session, pc, err := adapter.newSingleLegUDPPacketConn(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	defer server.Close()
	hello := <-server.hello
	engine := <-server.ready
	if hello.Version != smp3core.Version5 || hello.Mode != smp3core.ModeDatagram || hello.LegID != 0 || hello.SessionID != session.id || hello.Destination != "bootstrap.example:53" {
		t.Fatalf("HELLO=%+v", hello)
	}
	if child.dialCalls.Load() != 1 || child.listenCalls.Load() != 0 {
		t.Fatalf("dial=%d listen=%d", child.dialCalls.Load(), child.listenCalls.Load())
	}
	if target := <-child.targets; target != "10.66.66.1:24444" {
		t.Fatalf("target=%q", target)
	}
	if !adapter.SupportUDP() {
		t.Fatal("B2 must enable UDP capability")
	}
	if _, err := pc.WriteTo([]byte("dns-like"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 53), Port: 53}); err != nil {
		t.Fatal(err)
	}
	got := receiveB1Datagram(t, engine)
	if got.Address != "192.0.2.53:53" || string(got.Payload) != "dns-like" {
		t.Fatalf("datagram=%+v", got)
	}
	if err := engine.Send([]byte("reply"), got.Address, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, addr, err := pc.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "reply" || addr.String() != "192.0.2.53:53" {
		t.Fatalf("reply n=%d addr=%v err=%v", n, addr, err)
	}
}

func TestSMP3UDPHelloDestinationSeparatedFromPackets(t *testing.T) {
	server := newB1Server("test-password")
	child := newB1Proxy("line-path", server)
	adapter := newB1Adapter(t, child)
	_, pc, err := adapter.newSingleLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	defer server.Close()
	hello := <-server.hello
	engine := <-server.ready
	if hello.Destination != "bootstrap.example:53" {
		t.Fatalf("HELLO destination=%q", hello.Destination)
	}
	addresses := []net.Addr{&net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}, M.Socksaddr{Fqdn: "second.example", Port: 443}, &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 5353}}
	want := []string{"192.0.2.1:53", "second.example:443", "[2001:db8::1]:5353"}
	for i, addr := range addresses {
		if _, err := pc.WriteTo([]byte{byte(i)}, addr); err != nil {
			t.Fatal(err)
		}
		got := receiveB1Datagram(t, engine)
		if got.Address != want[i] {
			t.Fatalf("got=%q want=%q", got.Address, want[i])
		}
	}
}

func TestSMP3UDPSingleLegContextDetaches(t *testing.T) {
	server := newB1Server("test-password")
	child := newB1Proxy("line-path", server)
	adapter := newB1Adapter(t, child)
	ctx, cancel := context.WithCancel(context.Background())
	_, pc, err := adapter.newSingleLegUDPPacketConn(ctx, &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	defer server.Close()
	<-server.hello
	engine := <-server.ready
	cancel()
	if _, err := pc.WriteTo([]byte("after-cancel"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if got := receiveB1Datagram(t, engine); string(got.Payload) != "after-cancel" {
		t.Fatalf("payload=%q", got.Payload)
	}
}

func TestSMP3UDPDialFailureAndContextCancellation(t *testing.T) {
	server := newB1Server("test-password")
	child := newB1Proxy("line-path", server)
	child.dialErr = errors.New("dial failed")
	adapter := newB1Adapter(t, child)
	fake := newB1FakeEngine()
	_, _, err := adapter.newSingleLegUDPPacketConnWith(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53}, smp3UDPBootstrapDeps{newEngine: func(smp3core.DatagramConfig) smp3DatagramEngine { return fake }, writeHello: writeSMP3DatagramHello})
	if err == nil || !errors.Is(fake.errClosed(), smp3core.ErrDatagramClosed) {
		t.Fatalf("err=%v closed=%v", err, fake.errClosed())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	child.dialErr = nil
	_, _, err = adapter.newSingleLegUDPPacketConnWith(ctx, &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53}, smp3UDPBootstrapDeps{newEngine: func(smp3core.DatagramConfig) smp3DatagramEngine { return newB1FakeEngine() }, writeHello: writeSMP3DatagramHello})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestSMP3UDPHelloAndAttachFailureCleanup(t *testing.T) {
	server := newB1Server("test-password")
	child := newB1Proxy("line-path", server)
	adapter := newB1Adapter(t, child)
	fake := newB1FakeEngine()
	sentinel := errors.New("hello failed")
	_, _, err := adapter.newSingleLegUDPPacketConnWith(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53}, smp3UDPBootstrapDeps{newEngine: func(smp3core.DatagramConfig) smp3DatagramEngine { return fake }, writeHello: func(io.Writer, smp3core.SessionID, string, []byte) error { return sentinel }})
	if err == nil || !errors.Is(fake.errClosed(), smp3core.ErrDatagramClosed) {
		t.Fatalf("hello err=%v closed=%v", err, fake.errClosed())
	}
	fake = newB1FakeEngine()
	fake.attachErr = errors.New("attach failed")
	_, _, err = adapter.newSingleLegUDPPacketConnWith(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53}, smp3UDPBootstrapDeps{newEngine: func(smp3core.DatagramConfig) smp3DatagramEngine { return fake }, writeHello: func(io.Writer, smp3core.SessionID, string, []byte) error { return nil }})
	if err == nil || !errors.Is(fake.errClosed(), smp3core.ErrDatagramClosed) {
		t.Fatalf("attach err=%v closed=%v", err, fake.errClosed())
	}
	server.Close()
}

func newB1Adapter(t *testing.T, child C.Proxy) *SMP3 {
	t.Helper()
	adapter, err := NewSMP3(SMP3Option{Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: "test-password", Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "unused-leg1"}}, UDP: SMP3UDPOption{Enabled: true, Mode: "adaptive", MaxDatagramSize: 16384}})
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		if name == "line-path" {
			return child, true
		}
		return nil, false
	}
	return adapter
}
func receiveB1Datagram(t *testing.T, e *smp3core.DatagramEngine) smp3core.Datagram {
	t.Helper()
	d, err := e.Receive(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type b1Proxy struct {
	C.Proxy
	name        string
	server      *b1Server
	dialErr     error
	dialCalls   atomic.Int32
	listenCalls atomic.Int32
	targets     chan string
}

func newB1Proxy(name string, server *b1Server) *b1Proxy {
	return &b1Proxy{name: name, server: server, targets: make(chan string, 8)}
}
func (p *b1Proxy) Name() string           { return p.name }
func (p *b1Proxy) Type() C.AdapterType    { return C.Socks5 }
func (p *b1Proxy) Addr() string           { return p.name + ":carrier" }
func (p *b1Proxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }
func (p *b1Proxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"type": "socks5", "name": p.name})
}
func (p *b1Proxy) DialContext(ctx context.Context, m *C.Metadata) (C.Conn, error) {
	p.dialCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.dialErr != nil {
		return nil, p.dialErr
	}
	p.targets <- m.RemoteAddress()
	client, server := net.Pipe()
	p.server.accept(server)
	return NewConn(client, p), nil
}
func (p *b1Proxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	p.listenCalls.Add(1)
	return nil, C.ErrNotSupport
}

type b1Server struct {
	password string
	hello    chan smp3core.Hello
	ready    chan *smp3core.DatagramEngine
	mu       sync.Mutex
	engines  []*smp3core.DatagramEngine
	conns    []net.Conn
}

func newB1Server(password string) *b1Server {
	return &b1Server{password: password, hello: make(chan smp3core.Hello, 8), ready: make(chan *smp3core.DatagramEngine, 8)}
}
func (s *b1Server) accept(conn net.Conn) {
	go func() {
		hello, err := smp3core.ReadHelloAt(conn, []byte(s.password), time.Now())
		if err != nil {
			_ = conn.Close()
			return
		}
		s.hello <- hello
		engine := smp3core.NewDatagramEngine(smp3core.DatagramConfig{Mode: smp3core.DatagramAdaptive, QueueFrames: 32, MaxDatagramSize: 16384, DedupWindow: 64, IdleTimeout: time.Minute, RecoveryTimeout: time.Second, AdaptiveQueueDelay: 20 * time.Millisecond})
		if err := engine.AttachLeg(0, conn, nil); err != nil {
			_ = conn.Close()
			_ = engine.Close()
			return
		}
		s.mu.Lock()
		s.engines = append(s.engines, engine)
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		s.ready <- engine
	}()
}
func (s *b1Server) Close() {
	s.mu.Lock()
	engines := append([]*smp3core.DatagramEngine(nil), s.engines...)
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()
	for _, e := range engines {
		_ = e.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
}

type b1FakeEngine struct {
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	attachErr error
}

func newB1FakeEngine() *b1FakeEngine                         { return &b1FakeEngine{done: make(chan struct{})} }
func (f *b1FakeEngine) Send([]byte, string, time.Time) error { return f.closeErr }
func (f *b1FakeEngine) Receive(time.Time) (smp3core.Datagram, error) {
	<-f.done
	return smp3core.Datagram{}, smp3core.ErrDatagramClosed
}
func (f *b1FakeEngine) Done() <-chan struct{} { return f.done }
func (f *b1FakeEngine) Close() error {
	f.closeOnce.Do(func() { f.closeErr = smp3core.ErrDatagramClosed; close(f.done) })
	return nil
}
func (f *b1FakeEngine) AttachLeg(smp3core.LegID, smp3core.DatagramLeg, func(error)) error {
	return f.attachErr
}
func (f *b1FakeEngine) errClosed() error { return f.closeErr }

var _ net.PacketConn = (*smp3PacketConn)(nil)

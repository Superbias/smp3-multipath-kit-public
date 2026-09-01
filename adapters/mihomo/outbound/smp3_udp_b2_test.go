package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
)

type b2Ready struct {
	sid    smp3core.SessionID
	leg    smp3core.LegID
	engine *smp3core.DatagramEngine
	conn   net.Conn
}

type b2Server struct {
	password string
	hello    chan smp3core.Hello
	ready    chan b2Ready
	mu       sync.Mutex
	engines  map[smp3core.SessionID]*smp3core.DatagramEngine
	conns    map[smp3core.SessionID]map[smp3core.LegID][]net.Conn
}

func newB2Server(password string) *b2Server {
	return &b2Server{password: password, hello: make(chan smp3core.Hello, 64), ready: make(chan b2Ready, 64), engines: make(map[smp3core.SessionID]*smp3core.DatagramEngine), conns: make(map[smp3core.SessionID]map[smp3core.LegID][]net.Conn)}
}

func (s *b2Server) accept(conn net.Conn) {
	go func() {
		hello, err := smp3core.ReadHelloAt(conn, []byte(s.password), time.Now())
		if err != nil {
			_ = conn.Close()
			return
		}
		s.hello <- hello
		s.mu.Lock()
		engine := s.engines[hello.SessionID]
		if engine == nil {
			engine = smp3core.NewDatagramEngine(smp3core.DatagramConfig{Mode: smp3core.DatagramAdaptive, QueueFrames: 64, MaxDatagramSize: 16384, DedupWindow: 64, IdleTimeout: time.Minute, RecoveryTimeout: time.Second, AdaptiveQueueDelay: 20 * time.Millisecond})
			s.engines[hello.SessionID] = engine
			s.conns[hello.SessionID] = make(map[smp3core.LegID][]net.Conn)
		}
		s.mu.Unlock()
		if err := engine.AttachLeg(hello.LegID, conn, nil); err != nil {
			_ = conn.Close()
			return
		}
		s.mu.Lock()
		s.conns[hello.SessionID][hello.LegID] = append(s.conns[hello.SessionID][hello.LegID], conn)
		s.mu.Unlock()
		s.ready <- b2Ready{sid: hello.SessionID, leg: hello.LegID, engine: engine, conn: conn}
	}()
}

func (s *b2Server) closeLeg(sid smp3core.SessionID, leg smp3core.LegID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if list := s.conns[sid][leg]; len(list) > 0 {
		_ = list[len(list)-1].Close()
	}
}

func (s *b2Server) Close() {
	s.mu.Lock()
	var engines []*smp3core.DatagramEngine
	var conns []net.Conn
	for _, engine := range s.engines {
		engines = append(engines, engine)
	}
	for _, byLeg := range s.conns {
		for _, list := range byLeg {
			conns = append(conns, list...)
		}
	}
	s.mu.Unlock()
	for _, engine := range engines {
		_ = engine.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}

type b2Proxy struct {
	C.Proxy
	name         string
	server       *b2Server
	dialCalls    atomic.Int32
	listenCalls  atomic.Int32
	targets      chan string
	mu           sync.Mutex
	errs         []error
	delays       []time.Duration
	active       atomic.Int32
	maxActive    atomic.Int32
	blockAfter   int32
	block        chan struct{}
	blockStarted chan struct{}
}

func newB2Proxy(name string, server *b2Server) *b2Proxy {
	return &b2Proxy{name: name, server: server, targets: make(chan string, 64), blockStarted: make(chan struct{}, 8)}
}
func (p *b2Proxy) Name() string           { return p.name }
func (p *b2Proxy) Type() C.AdapterType    { return C.Socks5 }
func (p *b2Proxy) Addr() string           { return p.name + ":carrier" }
func (p *b2Proxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }
func (p *b2Proxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"type": "socks5", "name": p.name})
}

func (p *b2Proxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	n := int(p.dialCalls.Add(1))
	p.targets <- metadata.RemoteAddress()
	p.mu.Lock()
	var delay time.Duration
	var planned error
	if n-1 < len(p.delays) {
		delay = p.delays[n-1]
	}
	if n-1 < len(p.errs) {
		planned = p.errs[n-1]
	}
	p.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if planned != nil {
		return nil, planned
	}
	active := p.active.Add(1)
	for {
		old := p.maxActive.Load()
		if active <= old || p.maxActive.CompareAndSwap(old, active) {
			break
		}
	}
	defer p.active.Add(-1)
	if p.blockAfter > 0 && int32(n) > p.blockAfter && p.block != nil {
		select {
		case p.blockStarted <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.block:
		}
	}
	client, server := net.Pipe()
	p.server.accept(server)
	return NewConn(client, p), nil
}

func (p *b2Proxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	p.listenCalls.Add(1)
	return nil, C.ErrNotSupport
}

func newB2Adapter(t *testing.T, leg0, leg1, fallback *b2Proxy) *SMP3 {
	t.Helper()
	option := SMP3Option{Name: "mp-jp", Server: "10.66.66.1", Port: 24444, Password: "test-password", Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}}, RedialInterval: "100ms", UDP: SMP3UDPOption{Enabled: true, Mode: "adaptive", MaxDatagramSize: 16384}}
	if fallback != nil {
		option.Leg1Fallback = "public-snell"
	}
	adapter, err := NewSMP3(option)
	if err != nil {
		t.Fatal(err)
	}
	adapter.lookup = func(name string) (C.Proxy, bool) {
		switch name {
		case "line-path":
			return leg0, true
		case "public-hy2":
			return leg1, true
		case "public-snell":
			if fallback != nil {
				return fallback, true
			}
		}
		return nil, false
	}
	return adapter
}

func collectB2(t *testing.T, ch <-chan b2Ready, count int) []b2Ready {
	t.Helper()
	out := make([]b2Ready, 0, count)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for len(out) < count {
		select {
		case ready := <-ch:
			out = append(out, ready)
		case <-timer.C:
			t.Fatalf("ready=%d want=%d", len(out), count)
		}
	}
	return out
}

func waitB2(t *testing.T, ch <-chan b2Ready, sid smp3core.SessionID, leg smp3core.LegID) b2Ready {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case ready := <-ch:
			if ready.sid == sid && ready.leg == leg {
				return ready
			}
		case <-timer.C:
			t.Fatalf("repair leg=%d timeout", leg)
		}
	}
}

func bootstrapB2(t *testing.T, leg0, leg1, fallback *b2Proxy) (*SMP3, *b2Server, *smp3UDPDualSession, *smp3PacketConn, []b2Ready) {
	t.Helper()
	adapter := newB2Adapter(t, leg0, leg1, fallback)
	server := leg0.server
	session, packetConn, err := adapter.newDualLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, server, session, packetConn, collectB2(t, server.ready, 2)
}

func TestSMP3UDPDualBootstrapLeg0Wins(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter, server, session, packetConn, ready := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	if !adapter.SupportUDP() {
		t.Fatal("UDP capability disabled")
	}
	if ready[0].sid != session.id || ready[1].sid != session.id || ready[0].leg == ready[1].leg {
		t.Fatalf("ready=%+v sid=%x", ready, session.id)
	}
	if leg0.listenCalls.Load() != 0 || leg1.listenCalls.Load() != 0 {
		t.Fatal("child UDP API used")
	}
	for _, targets := range []<-chan string{leg0.targets, leg1.targets} {
		if got := <-targets; got != "10.66.66.1:24444" {
			t.Fatalf("target=%q", got)
		}
	}
}

func TestSMP3UDPDualBootstrapLeg1Wins(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg0.delays = []time.Duration{500 * time.Millisecond}
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	start := time.Now()
	session, packetConn, err := adapter.newDualLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	defer server.Close()
	if time.Since(start) > 450*time.Millisecond {
		t.Fatalf("leg1 winner waited for leg0: %s", time.Since(start))
	}
	ready := collectB2(t, server.ready, 2)
	if ready[0].sid != session.id || ready[1].sid != session.id {
		t.Fatalf("ready=%+v", ready)
	}
}

func TestSMP3UDPLeg0HardFailureStartsLeg1(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg0.errs = []error{errors.New("leg0 failed")}
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	started := time.Now()
	_, packetConn, err := adapter.newDualLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	defer server.Close()
	if time.Since(started) > 200*time.Millisecond || leg1.dialCalls.Load() == 0 {
		t.Fatalf("leg1 not immediate: elapsed=%s calls=%d", time.Since(started), leg1.dialCalls.Load())
	}
}

func TestSMP3UDPDualBootstrapBothFail(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	leg0.errs = []error{errors.New("leg0 failed")}
	leg1.errs = []error{errors.New("leg1 failed")}
	adapter := newB2Adapter(t, leg0, leg1, nil)
	_, packetConn, err := adapter.newDualLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if packetConn != nil || err == nil {
		t.Fatalf("pc=%v err=%v", packetConn, err)
	}
	server.Close()
}

func TestSMP3ProductionListenPacketContextEnabled(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	packetConn, err := adapter.ListenPacketContext(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	if packetConn == nil || !adapter.SupportUDP() {
		t.Fatalf("pc=%v udp=%v", packetConn, adapter.SupportUDP())
	}
	defer packetConn.Close()
	defer server.Close()
	ready := collectB2(t, server.ready, 2)
	if ready[0].sid != ready[1].sid {
		t.Fatal("different SessionIDs")
	}
}

func TestSMP3UDPLocalAdaptive1000(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, ready := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	serverEngine := ready[0].engine
	receiveErr := make(chan error, 1)
	go func() {
		seen := make([]bool, 1000)
		for i := 0; i < 1000; i++ {
			datagram, err := serverEngine.Receive(time.Now().Add(5 * time.Second))
			if err != nil || len(datagram.Payload) != 2 {
				receiveErr <- fmt.Errorf("i=%d datagram=%+v err=%w", i, datagram, err)
				return
			}
			value := int(datagram.Payload[0]) | int(datagram.Payload[1])<<8
			if value < 0 || value >= 1000 || seen[value] {
				receiveErr <- fmt.Errorf("bad/duplicate payload value=%d", value)
				return
			}
			seen[value] = true
		}
		receiveErr <- nil
	}()
	for i := 0; i < 1000; i++ {
		if _, err := packetConn.WriteTo([]byte{byte(i), byte(i >> 8)}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-receiveErr; err != nil {
		t.Fatal(err)
	}
	stats := session.engine.Snapshot()
	if stats.TxBytes[0] == 0 || stats.TxBytes[1] == 0 {
		t.Fatalf("both legs not active: %+v", stats)
	}
}

func TestSMP3UDPLeg0RepairSameSession(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, ready := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	sid := session.id
	server.closeLeg(sid, 0)
	repaired := waitB2(t, server.ready, sid, 0)
	if repaired.sid != sid || repaired.leg != 0 || leg0.dialCalls.Load() < 2 {
		t.Fatalf("repair=%+v calls=%d", repaired, leg0.dialCalls.Load())
	}
	if _, err := packetConn.WriteTo([]byte("after-leg0"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if datagram, err := ready[0].engine.Receive(time.Now().Add(time.Second)); err != nil || string(datagram.Payload) != "after-leg0" {
		t.Fatalf("datagram=%+v err=%v", datagram, err)
	}
}

func TestSMP3UDPLeg1RepairSameSession(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, ready := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	if _, err := packetConn.WriteTo([]byte("before-leg1"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if datagram, err := ready[0].engine.Receive(time.Now().Add(time.Second)); err != nil || string(datagram.Payload) != "before-leg1" {
		t.Fatalf("before=%+v err=%v", datagram, err)
	}
	var oldConn net.Conn
	for _, initial := range ready {
		if initial.leg == 1 {
			oldConn = initial.conn
		}
	}
	sid := session.id
	engine := session.engine
	server.closeLeg(sid, 1)
	repaired := waitB2(t, server.ready, sid, 1)
	if repaired.sid != sid || repaired.leg != 1 || leg1.dialCalls.Load() < 2 {
		t.Fatalf("repair=%+v calls=%d", repaired, leg1.dialCalls.Load())
	}
	if session.engine != engine {
		t.Fatal("repair replaced DatagramEngine")
	}
	if oldConn == nil || repaired.conn == oldConn {
		t.Fatal("leg1 concrete carrier generation was not replaced")
	}
	if _, err := packetConn.WriteTo([]byte("after-leg1"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if datagram, err := ready[0].engine.Receive(time.Now().Add(time.Second)); err != nil || string(datagram.Payload) != "after-leg1" {
		t.Fatalf("datagram=%+v err=%v", datagram, err)
	}
}

func TestSMP3UDPLeg1RepairPrimaryToFallback(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	primary := newB2Proxy("public-hy2", server)
	fallback := newB2Proxy("public-snell", server)
	primary.errs = []error{nil, errors.New("primary repair failed")}
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, primary, fallback)
	defer packetConn.Close()
	defer server.Close()
	server.closeLeg(session.id, 1)
	repaired := waitB2(t, server.ready, session.id, 1)
	if repaired.sid != session.id || fallback.dialCalls.Load() == 0 {
		t.Fatalf("repair=%+v fallback=%d", repaired, fallback.dialCalls.Load())
	}
}

func TestSMP3UDPRepairSingleInflightPerLeg(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	leg1.blockAfter = 1
	leg1.block = make(chan struct{})
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	server.closeLeg(session.id, 1)
	select {
	case <-leg1.blockStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("repair did not block")
	}
	for i := 0; i < 10; i++ {
		session.scheduleRepair(1, errors.New("duplicate down"))
	}
	if leg1.maxActive.Load() > 1 {
		t.Fatalf("max concurrent=%d", leg1.maxActive.Load())
	}
	close(leg1.block)
	_ = waitB2(t, server.ready, session.id, 1)
}

func TestSMP3UDPRepairSurvivesCallerCancelAndCloseStops(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	session, packetConn, err := adapter.newDualLegUDPPacketConn(ctx, &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	_ = collectB2(t, server.ready, 2)
	cancel()
	server.closeLeg(session.id, 0)
	_ = waitB2(t, server.ready, session.id, 0)
	before := leg0.dialCalls.Load()
	_ = packetConn.Close()
	session.scheduleRepair(0, errors.New("after close"))
	time.Sleep(250 * time.Millisecond)
	if leg0.dialCalls.Load() != before {
		t.Fatalf("dial after close=%d before=%d", leg0.dialCalls.Load(), before)
	}
}

func TestSMP3UDPDualChildUDPAPIUnused(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	packetConn, err := adapter.ListenPacketContext(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "example.com", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	_ = packetConn.Close()
	server.Close()
	if leg0.listenCalls.Load() != 0 || leg1.listenCalls.Load() != 0 {
		t.Fatalf("listen calls=%d/%d", leg0.listenCalls.Load(), leg1.listenCalls.Load())
	}
}

func TestSMP3UDPAdapterCloseStopsSessionAndRepair(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	before := leg0.dialCalls.Load()
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("adapter Close did not close UDP session")
	}
	session.scheduleRepair(0, errors.New("after adapter close"))
	time.Sleep(250 * time.Millisecond)
	if leg0.dialCalls.Load() != before {
		t.Fatalf("repair after adapter close=%d before=%d", leg0.dialCalls.Load(), before)
	}
	_ = packetConn.Close()
	server.Close()
}

func TestSMP3UDPCompanionFailureRepairsAfterWinner(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	leg1.errs = []error{errors.New("initial companion failed"), nil}
	_, server, session, packetConn, ready := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	if len(ready) != 2 || ready[0].sid != session.id || ready[1].sid != session.id || leg1.dialCalls.Load() < 2 {
		t.Fatalf("ready=%+v calls=%d", ready, leg1.dialCalls.Load())
	}
}

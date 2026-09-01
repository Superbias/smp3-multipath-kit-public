package outbound

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
	M "github.com/metacubex/sing/common/metadata"
)

func TestSMP3PacketConnRawUnsupported(t *testing.T) {
	conn := &smp3PacketConn{}
	if n, addr, err := conn.ReadFrom(make([]byte, 32)); n != 0 || addr != nil || !errors.Is(err, errSMP3UDPNotImplemented) {
		t.Fatalf("ReadFrom = n=%d addr=%v err=%v", n, addr, err)
	}
	if n, err := conn.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4zero, Port: 53}); n != 0 || !errors.Is(err, errSMP3UDPNotImplemented) {
		t.Fatalf("WriteTo = n=%d err=%v", n, err)
	}
}

func TestSMP3PacketConnCloseIsIdempotent(t *testing.T) {
	conn := &smp3PacketConn{}
	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if n, addr, err := conn.ReadFrom(nil); n != 0 || addr != nil || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ReadFrom after Close = n=%d addr=%v err=%v", n, addr, err)
	}
	if n, err := conn.WriteTo(nil, nil); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("WriteTo after Close = n=%d err=%v", n, err)
	}
}

func TestSMP3PacketConnDeadlineAndLocalAddr(t *testing.T) {
	conn := &smp3PacketConn{}
	deadline := time.Now().Add(time.Second)
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	first := conn.LocalAddr()
	second := conn.LocalAddr()
	if first == nil || second == nil || first.Network() != "udp" || first.String() != second.String() {
		t.Fatalf("unstable LocalAddr: %v / %v", first, second)
	}
}

func TestSMP3PacketConnMihomoWrapper(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "127.0.0.1", Port: 24444, Password: "test-password",
		Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := &smp3PacketConn{}
	var wrapped C.PacketConn = NewPacketConn(raw, adapter)
	if wrapped == nil {
		t.Fatal("NewPacketConn returned nil")
	}

	data, put, addr, err := wrapped.WaitReadFrom()
	if data != nil || put != nil || addr != nil || !errors.Is(err, errSMP3UDPNotImplemented) {
		t.Fatalf("WaitReadFrom = data=%v putNonNil=%v addr=%v err=%v", data, put != nil, addr, err)
	}
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("192.0.2.1"), DstPort: 53}
	if err := wrapped.ResolveUDP(context.Background(), metadata); err != nil {
		t.Fatalf("wrapper ResolveUDP: %v", err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSMP3ListenPacketContextRejectsMissingChildren(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{
		Name: "mp-jp", Server: "127.0.0.1", Port: 24444, Password: "test-password",
		Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("192.0.2.1"), DstPort: 53}
	pc, err := adapter.ListenPacketContext(context.Background(), metadata)
	if pc != nil || err == nil || errors.Is(err, C.ErrNotSupport) {
		t.Fatalf("ListenPacketContext = pc=%v err=%v", pc, err)
	}
}

func TestSMP3UDPDisabledDoesNotAdvertiseCapability(t *testing.T) {
	adapter, err := NewSMP3(SMP3Option{Name: "tcp-only", Server: "127.0.0.1", Port: 24444, Password: "test-password", Legs: []SMP3LegOption{{Proxy: "line-path"}, {Proxy: "public-hy2"}}})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.SupportUDP() {
		t.Fatal("disabled UDP was advertised")
	}
}

func TestSMP3DatagramConfigModesAndDefaults(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want smp3core.DatagramMode
	}{
		{name: "stripe", mode: "stripe", want: smp3core.DatagramStripe},
		{name: "duplicate", mode: "duplicate", want: smp3core.DatagramDuplicate},
		{name: "adaptive", mode: "adaptive", want: smp3core.DatagramAdaptive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := bareSMP3ForUDP(SMP3UDPOption{Enabled: true, Mode: test.mode, MaxDatagramSize: 16384, AdaptiveDuplicateThreshold: 128}, 7*time.Second, []uint32{100, 500})
			cfg, err := adapter.datagramConfig()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Mode != test.want || cfg.QueueFrames != 256 || cfg.DedupWindow != 4096 || cfg.MaxDatagramSize != 16384 || cfg.IdleTimeout != 2*time.Minute || cfg.RecoveryTimeout != 7*time.Second || cfg.AdaptiveQueueDelay != 120*time.Millisecond || cfg.AdaptiveDuplicateThreshold != 128 || len(cfg.BandwidthMbps) != 2 || cfg.BandwidthMbps[0] != 100 || cfg.BandwidthMbps[1] != 500 {
				t.Fatalf("unexpected datagram config: %+v", cfg)
			}
		})
	}
	if _, err := bareSMP3ForUDP(SMP3UDPOption{Enabled: true, Mode: "invalid"}, 0, nil).datagramConfig(); err == nil {
		t.Fatal("invalid UDP mode was accepted")
	}
	if _, err := bareSMP3ForUDP(SMP3UDPOption{}, 0, nil).datagramConfig(); !errors.Is(err, errSMP3UDPDisabled) {
		t.Fatalf("disabled UDP = %v", err)
	}
}

func TestSMP3PacketConnAddressMapping(t *testing.T) {
	fake := newFakeSMP3DatagramIO()
	conn := newSMP3PacketConn(fake)
	addresses := []struct {
		addr net.Addr
		want string
	}{
		{addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}, want: "192.0.2.1:53"},
		{addr: M.Socksaddr{Fqdn: "example.com", Port: 443}, want: "example.com:443"},
		{addr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 5353}, want: "[2001:db8::1]:5353"},
	}
	for _, test := range addresses {
		if _, err := conn.WriteTo([]byte("x"), test.addr); err != nil {
			t.Fatal(err)
		}
		got := <-fake.sent
		if got.address != test.want {
			t.Fatalf("address=%q want %q", got.address, test.want)
		}
	}
	for _, want := range []string{"192.0.2.1:53", "example.com:443", "[2001:db8::1]:5353"} {
		fake.received <- smp3core.Datagram{Address: want, Payload: []byte("reply")}
		buf := make([]byte, 16)
		n, addr, err := conn.ReadFrom(buf)
		if err != nil || n != 5 || addr == nil || addr.String() != want {
			t.Fatalf("read n=%d addr=%v err=%v want=%q", n, addr, err, want)
		}
	}
}

func TestSMP3PacketConnDeadlinesAndTimeout(t *testing.T) {
	fake := newFakeSMP3DatagramIO()
	conn := newSMP3PacketConn(fake)
	readDeadline := time.Now().Add(time.Second)
	writeDeadline := time.Now().Add(2 * time.Second)
	if err := conn.SetReadDeadline(readDeadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(writeDeadline); err != nil {
		t.Fatal(err)
	}
	fake.receiveErr = smp3core.ErrDatagramTimeout
	_, _, err := conn.ReadFrom(make([]byte, 8))
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || !fake.lastReceiveDeadline.Equal(readDeadline) {
		t.Fatalf("read timeout=%v deadline=%v", err, fake.lastReceiveDeadline)
	}
	fake.receiveErr = nil
	if _, err := conn.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if !fake.lastSendDeadline.Equal(writeDeadline) {
		t.Fatalf("write deadline=%v want %v", fake.lastSendDeadline, writeDeadline)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	if !fake.lastSendDeadline.IsZero() {
		t.Fatalf("zero deadline was not cleared: %v", fake.lastSendDeadline)
	}
	fake.sendErr = errors.New("queue unavailable")
	if err := conn.SetWriteDeadline(time.Now().Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err == nil {
		t.Fatal("expired write deadline returned nil")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("expired write error=%v", err)
	}
}

func TestSMP3PacketConnCloseUnblocksRead(t *testing.T) {
	fake := newFakeSMP3DatagramIO()
	conn := newSMP3PacketConn(fake)
	result := make(chan error, 1)
	go func() {
		_, _, err := conn.ReadFrom(make([]byte, 8))
		result <- err
	}()
	select {
	case <-fake.receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("Receive did not start")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("ReadFrom after Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadFrom remained blocked after Close")
	}
}

func TestSMP3PacketConnShortBufferDoesNotKillAssociation(t *testing.T) {
	left, right := newConnectedSMP3PacketConns(t, smp3core.DatagramStripe)
	engine := right.engine.(*smp3core.DatagramEngine)
	if err := engine.Send([]byte("long-payload"), "example.com:53", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := left.ReadFrom(make([]byte, 1)); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("short read error=%v", err)
	}
	if err := engine.Send([]byte("ok"), "example.com:53", time.Time{}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	n, addr, err := left.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "ok" || addr.String() != "example.com:53" {
		t.Fatalf("post-short read n=%d addr=%v err=%v payload=%q", n, addr, err, buf[:n])
	}
}

func TestSMP3PacketConnOversizeDoesNotKillAssociation(t *testing.T) {
	left, right := newConnectedSMP3PacketConns(t, smp3core.DatagramStripe)
	for _, payload := range [][]byte{bytes.Repeat([]byte{'a'}, 1200), bytes.Repeat([]byte{'b'}, 16383), bytes.Repeat([]byte{'c'}, 16384)} {
		if n, err := left.WriteTo(payload, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil || n != len(payload) {
			t.Fatalf("legal write n=%d err=%v", n, err)
		}
		buf := make([]byte, 16384)
		n, _, err := right.ReadFrom(buf)
		if err != nil || n != len(payload) || !bytes.Equal(buf[:n], payload) {
			t.Fatalf("legal read n=%d err=%v", n, err)
		}
	}
	oversize := bytes.Repeat([]byte{'x'}, 16385)
	if n, err := left.WriteTo(oversize, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil || n != len(oversize) {
		t.Fatalf("oversize write n=%d err=%v", n, err)
	}
	final := []byte("final")
	if _, err := left.WriteTo(final, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, _, err := right.ReadFrom(buf)
	if err != nil || !bytes.Equal(buf[:n], final) {
		t.Fatalf("post-oversize read n=%d err=%v payload=%q", n, err, buf[:n])
	}
}

func TestSMP3PacketConnPayloadOwnership(t *testing.T) {
	left, right := newConnectedSMP3PacketConns(t, smp3core.DatagramStripe)
	payload := []byte("immutable")
	if _, err := left.WriteTo(payload, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	for i := range payload {
		payload[i] = 'x'
	}
	buf := make([]byte, 32)
	n, _, err := right.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "immutable" {
		t.Fatalf("owned payload=%q err=%v", buf[:n], err)
	}
}

func TestSMP3PacketConnCanonicalEngineRoundTrip(t *testing.T) {
	left, right := newConnectedSMP3PacketConns(t, smp3core.DatagramStripe)
	tests := []struct {
		payload []byte
		addr    net.Addr
		want    string
	}{
		{payload: []byte("ipv4"), addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}, want: "192.0.2.1:53"},
		{payload: []byte("domain"), addr: M.Socksaddr{Fqdn: "example.com", Port: 443}, want: "example.com:443"},
		{payload: []byte("ipv6"), addr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 5353}, want: "[2001:db8::1]:5353"},
	}
	for _, test := range tests {
		if _, err := left.WriteTo(test.payload, test.addr); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 32)
		n, addr, err := right.ReadFrom(buf)
		if err != nil || string(buf[:n]) != string(test.payload) || addr == nil || addr.String() != test.want {
			t.Fatalf("roundtrip n=%d addr=%v err=%v payload=%q want=%q/%q", n, addr, err, buf[:n], test.want, test.payload)
		}
	}
	if _, err := right.WriteTo([]byte("reverse"), M.Socksaddr{Fqdn: "reply.example", Port: 5353}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, addr, err := left.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "reverse" || addr.String() != "reply.example:5353" {
		t.Fatalf("reverse n=%d addr=%v err=%v payload=%q", n, addr, err, buf[:n])
	}
}

func TestSMP3PacketConnDuplicateModeExactlyOnce(t *testing.T) {
	left, right := newConnectedSMP3PacketConns(t, smp3core.DatagramDuplicate)
	for i := 0; i < 100; i++ {
		payload := []byte{byte(i)}
		if _, err := left.WriteTo(payload, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 8)
		n, _, err := right.ReadFrom(buf)
		if err != nil || n != 1 || buf[0] != byte(i) {
			t.Fatalf("i=%d n=%d err=%v payload=%v", i, n, err, buf[:n])
		}
	}
	deadline := time.Now().Add(time.Second)
	for right.engine.(*smp3core.DatagramEngine).Snapshot().DuplicateRxDrop == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if right.engine.(*smp3core.DatagramEngine).Snapshot().DuplicateRxDrop == 0 {
		t.Fatal("duplicate copy was not observed and deduplicated")
	}
}

func TestSMP3PacketConnReadCloseRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		fake := newFakeSMP3DatagramIO()
		conn := newSMP3PacketConn(fake)
		result := make(chan error, 1)
		go func() { _, _, err := conn.ReadFrom(make([]byte, 8)); result <- err }()
		_ = conn.Close()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("read/close race hung")
		}
	}
}

func TestSMP3PacketConnWriteCloseRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		fake := newFakeSMP3DatagramIO()
		conn := newSMP3PacketConn(fake)
		result := make(chan error, 1)
		go func() {
			_, err := conn.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53})
			result <- err
		}()
		_ = conn.Close()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("write/close race hung")
		}
	}
}

func bareSMP3ForUDP(udp SMP3UDPOption, recovery time.Duration, weights []uint32) *SMP3 {
	return &SMP3{option: SMP3Option{UDP: udp}, streamConfig: smp3core.StreamConfig{RecoveryTimeout: recovery, BandwidthMbps: weights}}
}

func newConnectedSMP3PacketConns(t *testing.T, mode smp3core.DatagramMode) (*smp3PacketConn, *smp3PacketConn) {
	t.Helper()
	cfg := smp3core.DatagramConfig{Mode: mode, QueueFrames: 32, MaxDatagramSize: 16384, DedupWindow: 64, IdleTimeout: time.Minute, RecoveryTimeout: time.Second, AdaptiveQueueDelay: 20 * time.Millisecond, BandwidthMbps: []uint32{100, 500}}
	left := smp3core.NewDatagramEngine(cfg)
	right := smp3core.NewDatagramEngine(cfg)
	for id := smp3core.LegID(0); id < 2; id++ {
		a, b := net.Pipe()
		if err := left.AttachLeg(id, a, nil); err != nil {
			t.Fatal(err)
		}
		if err := right.AttachLeg(id, b, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	return newSMP3PacketConn(left), newSMP3PacketConn(right)
}

type fakeSentDatagram struct {
	payload []byte
	address string
}

type fakeSMP3DatagramIO struct {
	sent                chan fakeSentDatagram
	received            chan smp3core.Datagram
	done                chan struct{}
	receiveStarted      chan struct{}
	receiveErr          error
	sendErr             error
	lastReceiveDeadline time.Time
	lastSendDeadline    time.Time
	mu                  sync.Mutex
	closeOnce           sync.Once
}

func newFakeSMP3DatagramIO() *fakeSMP3DatagramIO {
	return &fakeSMP3DatagramIO{sent: make(chan fakeSentDatagram, 128), received: make(chan smp3core.Datagram, 128), done: make(chan struct{}), receiveStarted: make(chan struct{})}
}

func (f *fakeSMP3DatagramIO) Send(payload []byte, address string, deadline time.Time) error {
	f.mu.Lock()
	f.lastSendDeadline = deadline
	f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	select {
	case <-f.done:
		return smp3core.ErrDatagramClosed
	case f.sent <- fakeSentDatagram{payload: append([]byte(nil), payload...), address: address}:
		return nil
	}
}

func (f *fakeSMP3DatagramIO) Receive(deadline time.Time) (smp3core.Datagram, error) {
	f.mu.Lock()
	f.lastReceiveDeadline = deadline
	f.mu.Unlock()
	select {
	case <-f.receiveStarted:
	default:
		close(f.receiveStarted)
	}
	if f.receiveErr != nil {
		return smp3core.Datagram{}, f.receiveErr
	}
	var timer *time.Timer
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		delay := time.Until(deadline)
		if delay <= 0 {
			return smp3core.Datagram{}, smp3core.ErrDatagramTimeout
		}
		timer = time.NewTimer(delay)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-f.done:
		return smp3core.Datagram{}, smp3core.ErrDatagramClosed
	case <-timeout:
		return smp3core.Datagram{}, smp3core.ErrDatagramTimeout
	case datagram := <-f.received:
		return datagram, nil
	}
}

func (f *fakeSMP3DatagramIO) Done() <-chan struct{} { return f.done }

func (f *fakeSMP3DatagramIO) Close() error {
	f.closeOnce.Do(func() { close(f.done) })
	return nil
}

var _ net.PacketConn = (*smp3PacketConn)(nil)

package outbound

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
)

func waitB2Replacement(t *testing.T, server *b2Server, timeout time.Duration) b2Ready {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ready := <-server.ready:
		return ready
	case <-timer.C:
		t.Fatal("replacement leg did not reach test server")
		return b2Ready{}
	}
}

func closeB2Engine(t *testing.T, engine *smp3core.DatagramEngine) {
	t.Helper()
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.Done():
	case <-time.After(time.Second):
		t.Fatal("engine did not become terminal")
	}
}

func writeAndReadReplacement(t *testing.T, packetConn *smp3PacketConn, server *b2Server, payload []byte) b2Ready {
	t.Helper()
	if _, err := packetConn.WriteTo(payload, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ready := waitB2Replacement(t, server, time.Until(deadline))
		if ready.leg != 0 {
			continue
		}
		datagram, err := ready.engine.Receive(time.Now().Add(time.Until(deadline)))
		if err == nil && string(datagram.Payload) == string(payload) {
			return ready
		}
	}
	t.Fatalf("replacement datagram was not observed payload=%q", payload)
	return b2Ready{}
}

func TestSMP3UDPTerminalEngineRecreatesForSendAndReceive(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()

	before := session.debugSnapshotForTest()
	closeB2Engine(t, before.Engine)
	if _, err := packetConn.WriteTo([]byte("fresh-send"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	replacement := waitB2Replacement(t, server, time.Second)
	datagram, err := replacement.engine.Receive(time.Now().Add(time.Second))
	if err != nil || string(datagram.Payload) != "fresh-send" {
		t.Fatalf("send replacement datagram=%+v err=%v", datagram, err)
	}
	after := session.debugSnapshotForTest()
	if after.AssociationID != before.AssociationID || after.Engine == before.Engine || after.EngineGeneration != before.EngineGeneration+1 || after.SessionID == before.SessionID || after.EngineDone {
		t.Fatalf("unexpected replacement state before=%+v after=%+v", before, after)
	}
	if err := replacement.engine.Send([]byte("fresh-receive"), "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, addr, err := packetConn.ReadFrom(buffer)
	if err != nil || string(buffer[:n]) != "fresh-receive" || addr.String() != "192.0.2.1:53" {
		t.Fatalf("receive replacement n=%d addr=%v err=%v payload=%q", n, addr, err, buffer[:n])
	}
}

func TestSMP3UDPTerminalReceiveSwitchesToNewEngine(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()

	before := session.debugSnapshotForTest()
	closeB2Engine(t, before.Engine)
	result := make(chan struct {
		n    int
		addr net.Addr
		err  error
		data []byte
	}, 1)
	go func() {
		buffer := make([]byte, 64)
		n, addr, err := packetConn.ReadFrom(buffer)
		result <- struct {
			n    int
			addr net.Addr
			err  error
			data []byte
		}{n: n, addr: addr, err: err, data: append([]byte(nil), buffer[:n]...)}
	}()
	replacement := waitB2Replacement(t, server, time.Second)
	if err := replacement.engine.Send([]byte("receive-after-swap"), "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.n != len("receive-after-swap") || string(got.data) != "receive-after-swap" || got.addr.String() != "192.0.2.1:53" {
			t.Fatalf("receive result=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not switch to replacement engine")
	}
}

func TestSMP3UDPTerminalEngineConcurrentSendSingleRecreation(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()
	closeB2Engine(t, session.debugSnapshotForTest().Engine)

	const count = 16
	errs := make(chan error, count)
	before := session.debugSnapshotForTest()
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			_, err := packetConn.WriteTo([]byte{byte(index)}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53})
			errs <- err
		}(i)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	replacement := waitB2Replacement(t, server, time.Second)
	for i := 0; i < count; i++ {
		datagram, err := replacement.engine.Receive(time.Now().Add(time.Second))
		if err != nil || len(datagram.Payload) != 1 {
			t.Fatalf("concurrent datagram=%+v err=%v", datagram, err)
		}
	}
	state := session.debugSnapshotForTest()
	if state.EngineGeneration != 2 || state.Engine == before.Engine {
		t.Fatalf("concurrent recreation state=%+v", state)
	}
}

func TestSMP3UDPIdleDoneRecreatesOnLaterSend(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	adapter := newB2Adapter(t, leg0, leg1, nil)
	adapter.option.UDP.IdleTimeout = "5s"
	session, packetConn, err := adapter.newDualLegUDPPacketConn(context.Background(), &C.Metadata{NetWork: C.UDP, Host: "bootstrap.example", DstPort: 53})
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	defer server.Close()
	_ = collectB2(t, server.ready, 2)
	before := session.debugSnapshotForTest()
	select {
	case <-before.Engine.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("idle engine did not become terminal")
	}
	if _, err := packetConn.WriteTo([]byte("after-idle"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); err != nil {
		t.Fatal(err)
	}
	replacement := waitB2Replacement(t, server, time.Second)
	datagram, err := replacement.engine.Receive(time.Now().Add(time.Second))
	if err != nil || string(datagram.Payload) != "after-idle" {
		t.Fatalf("idle replacement datagram=%+v err=%v", datagram, err)
	}
	after := session.debugSnapshotForTest()
	if after.Engine == before.Engine || after.SessionID == before.SessionID || after.EngineGeneration != before.EngineGeneration+1 {
		t.Fatalf("idle replacement state before=%+v after=%+v", before, after)
	}
}

func TestSMP3UDPRepeatedTerminalEngineReplacement(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer packetConn.Close()
	defer server.Close()

	for cycle := 1; cycle <= 10; cycle++ {
		before := session.debugSnapshotForTest()
		closeB2Engine(t, before.Engine)
		writeAndReadReplacement(t, packetConn, server, []byte{byte(cycle)})
		after := session.debugSnapshotForTest()
		if after.Engine == before.Engine || after.EngineGeneration != uint64(cycle+1) || after.SessionID == before.SessionID || after.EngineDone {
			t.Fatalf("cycle=%d before=%+v after=%+v", cycle, before, after)
		}
	}
}

func TestSMP3UDPAssociationCloseSuppressesRecreation(t *testing.T) {
	server := newB2Server("test-password")
	leg0 := newB2Proxy("line-path", server)
	leg1 := newB2Proxy("public-hy2", server)
	_, server, session, packetConn, _ := bootstrapB2(t, leg0, leg1, nil)
	defer server.Close()
	if err := packetConn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := packetConn.WriteTo([]byte("late"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 53}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late send err=%v", err)
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("association Done did not close")
	}
	state := session.debugSnapshotForTest()
	if !state.Closed || state.EngineGeneration != 1 {
		t.Fatalf("closed state=%+v", state)
	}
}

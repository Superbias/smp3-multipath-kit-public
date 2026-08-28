package multipath

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

func connectDatagramCores(t *testing.T, cfg datagramConfig) (*mpDatagramCore, net.PacketConn, *mpDatagramCore, net.PacketConn) {
	t.Helper()
	left, leftPC := newDatagramCore(cfg)
	right, rightPC := newDatagramCore(cfg)
	for id := uint8(0); id < 2; id++ {
		a, b := net.Pipe()
		if err := left.addLeg(id, a, nil); err != nil {
			t.Fatal(err)
		}
		if err := right.addLeg(id, b, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	return left, leftPC, right, rightPC
}

func TestDatagramFrameRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	want := datagramPacket{id: 99, address: "192.0.2.1:53", data: []byte("hello")}
	errCh := make(chan error, 1)
	go func() { errCh <- writeDatagramFrame(a, want) }()
	got, err := readDatagramFrame(b, 65507)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if got.id != want.id || got.address != want.address || string(got.data) != string(want.data) {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
}

func TestDatagramDuplicateModeDeliversOnce(t *testing.T) {
	cfg := datagramConfig{Mode: datagramModeDuplicate, QueueFrames: 16, DedupWindow: 64, IdleTimeout: time.Minute}
	left, leftPC, right, rightPC := connectDatagramCores(t, cfg)
	_ = right
	if _, err := leftPC.WriteTo([]byte("dns"), smp3PacketAddr("198.51.100.8:53")); err != nil {
		t.Fatal(err)
	}
	_ = rightPC.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 64)
	n, addr, err := rightPC.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "dns" || addr.String() != "198.51.100.8:53" {
		t.Fatalf("n=%d addr=%v payload=%q", n, addr, buf[:n])
	}
	deadline := time.Now().Add(time.Second)
	for right.snapshotStats().DuplicateRxDrop == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := right.snapshotStats()
	if stats.DuplicateRxDrop == 0 {
		t.Fatalf("duplicate was not deduplicated: %+v", stats)
	}
	if left.snapshotStats().DuplicateTx == 0 {
		t.Fatalf("duplicate mode did not transmit a second copy")
	}
}

func TestDatagramDoesNotWaitForEarlierID(t *testing.T) {
	cfg := datagramConfig{Mode: datagramModeStripe, QueueFrames: 16, DedupWindow: 64, IdleTimeout: time.Minute}
	core, pc := newDatagramCore(cfg)
	defer core.Close()
	leg0Peer, leg0Core := net.Pipe()
	leg1Peer, leg1Core := net.Pipe()
	defer leg0Peer.Close()
	defer leg1Peer.Close()
	if err := core.addLeg(0, leg0Core, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, leg1Core, nil); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = writeDatagramFrame(leg1Peer, datagramPacket{id: 1, address: "203.0.113.9:53", data: []byte("second")})
	}()
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 64)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "second" {
		t.Fatalf("datagram core imposed global order: %q", buf[:n])
	}
	// An older id arriving later is still a valid unique UDP datagram.
	go func() {
		_ = writeDatagramFrame(leg0Peer, datagramPacket{id: 0, address: "203.0.113.9:53", data: []byte("first")})
	}()
	n, _, err = pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "first" {
		t.Fatalf("late unique datagram lost: %q", buf[:n])
	}
}

func TestDatagramAdaptiveAvoidsSlowLeg(t *testing.T) {
	cfg := datagramConfig{
		Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64,
		IdleTimeout: time.Minute, AdaptiveQueueDelay: 20 * time.Millisecond,
		BandwidthMbps: []uint32{100, 100},
	}
	core, _ := newDatagramCore(cfg)
	defer core.Close()
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	defer b0.Close()
	defer b1.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	leg0 := core.availableLegs()[0]
	leg1 := core.availableLegs()[1]
	leg0.perf.mu.Lock()
	leg0.perf.ewmaBytesPerSec = 10 * 1e6
	leg0.perf.ewmaDelay = 100 * time.Millisecond
	leg0.perf.mu.Unlock()
	leg1.perf.mu.Lock()
	leg1.perf.ewmaBytesPerSec = 10 * 1e6
	leg1.perf.ewmaDelay = time.Millisecond
	leg1.perf.mu.Unlock()
	if got := core.chooseLeg([]*datagramLeg{leg0, leg1}, 1200); got == nil || got.id != 1 {
		t.Fatalf("adaptive scheduler chose slow leg: %#v", got)
	}
}

func TestDatagramSameIDLegCanRejoin(t *testing.T) {
	cfg := datagramConfig{Mode: datagramModeStripe, QueueFrames: 16, IdleTimeout: time.Minute, RecoveryTimeout: time.Second}
	core, _ := newDatagramCore(cfg)
	defer core.Close()
	a, b := net.Pipe()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	deadline := time.Now().Add(time.Second)
	for core.hasLeg(0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if core.hasLeg(0) {
		t.Fatal("old leg did not retire")
	}
	newA, newB := net.Pipe()
	defer newB.Close()
	if err := core.addLeg(0, newA, nil); err != nil {
		t.Fatalf("same-id rejoin failed: %v", err)
	}
	if !core.hasLeg(0) {
		t.Fatal("replacement leg missing")
	}
}

func TestDatagramAdaptiveDuplicatesSmallPacketsOnly(t *testing.T) {
	cfg := datagramConfig{
		Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64,
		IdleTimeout: time.Minute, AdaptiveQueueDelay: 20 * time.Millisecond,
		AdaptiveDuplicateThreshold: 64,
	}
	core, _ := newDatagramCore(cfg)
	defer core.Close()
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	defer b0.Close()
	defer b1.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	legs := core.availableLegs()
	if !core.shouldDuplicate(32, legs) {
		t.Fatal("adaptive mode did not duplicate latency-sensitive small datagram")
	}
	if core.shouldDuplicate(128, legs) {
		t.Fatal("adaptive mode duplicated datagram above threshold")
	}
}

func TestDatagramCloseDoesNotReportCarrierFailure(t *testing.T) {
	down := make(chan uint8, 2)
	cfg := datagramConfig{
		Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64,
		IdleTimeout: time.Minute, RecoveryTimeout: time.Second,
		OnLegDown: func(id uint8, _ error) { down <- id },
	}
	core, _ := newDatagramCore(cfg)
	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	_ = core.Close()
	select {
	case id := <-down:
		t.Fatalf("intentional datagram close reported leg%d failure", id)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDatagramDedupRejectsVeryLateDuplicateOutsideWindow(t *testing.T) {
	core, _ := newDatagramCore(datagramConfig{Mode: datagramModeDuplicate, DedupWindow: 4})
	defer core.Close()
	for id := uint64(0); id <= 16; id++ {
		if !core.acceptDatagramID(id, 32) {
			t.Fatalf("fresh datagram %d was rejected", id)
		}
	}
	if core.acceptDatagramID(0, 32) {
		t.Fatal("very late duplicate was accepted after its concrete seen entry aged out")
	}
	// A unique out-of-order ID that is still inside the active receive window must
	// remain valid; UDP is intentionally not globally ordered.
	inside := core.maxSeen - core.cfg.DedupWindow
	if _, exists := core.seen[inside]; !exists {
		t.Fatalf("test setup expected active-window id %d to remain tracked", inside)
	}
}

func TestDatagramRejectsOversizedAddressBeforeWireAllocation(t *testing.T) {
	core, _ := newDatagramCore(datagramConfig{MaxDatagramSize: 16384})
	defer core.Close()
	address := string(make([]byte, maxDatagramAddressSize+1))
	if err := core.sendDatagram([]byte("x"), address, time.Time{}); err == nil {
		t.Fatal("oversized datagram address was accepted")
	}
}

func TestDatagramReadRejectsOversizedFrameBeforePayloadRead(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var header [frameHeaderSize]byte
		header[0] = frameTypeDatagram
		// This is within the generic 1 MiB frame cap but exceeds the UDP-specific
		// address + configured datagram bound, so the reader must reject it from the
		// header without waiting for or allocating the declared payload.
		binary.BigEndian.PutUint32(header[9:13], uint32(2+maxDatagramAddressSize+1024+1))
		_, _ = a.Write(header[:])
	}()
	if _, err := readDatagramFrame(b, 1024); err == nil {
		t.Fatal("oversized datagram frame length was accepted")
	}
	<-done
}

func TestDatagramStripeAcceptsVeryLateUniqueOutsideDedupWindow(t *testing.T) {
	core, _ := newDatagramCore(datagramConfig{Mode: datagramModeStripe, DedupWindow: 4})
	defer core.Close()
	for id := uint64(8); id <= 16; id++ {
		if !core.acceptDatagramID(id, 1200) {
			t.Fatalf("fresh stripe datagram %d was rejected", id)
		}
	}
	if !core.acceptDatagramID(1, 1200) {
		t.Fatal("very late unique stripe datagram was incorrectly treated as stale duplicate")
	}
}

func TestDatagramAdaptiveStaleRuleOnlyAppliesToDuplicatedSizes(t *testing.T) {
	core, _ := newDatagramCore(datagramConfig{
		Mode: datagramModeAdaptive, DedupWindow: 4, AdaptiveDuplicateThreshold: 128,
	})
	defer core.Close()
	for id := uint64(8); id <= 16; id++ {
		if !core.acceptDatagramID(id, 64) {
			t.Fatalf("fresh adaptive datagram %d was rejected", id)
		}
	}
	if core.acceptDatagramID(1, 64) {
		t.Fatal("very late potentially-duplicated adaptive datagram was accepted")
	}
	if !core.acceptDatagramID(2, 1200) {
		t.Fatal("very late non-duplicated adaptive datagram was rejected")
	}
}

func TestDatagramAdaptiveSchedulerUsesByteBacklog(t *testing.T) {
	cfg := datagramConfig{
		Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64,
		IdleTimeout: time.Minute, AdaptiveQueueDelay: 20 * time.Millisecond,
		BandwidthMbps: []uint32{100, 100},
	}
	core, _ := newDatagramCore(cfg)
	defer core.Close()
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	defer b0.Close()
	defer b1.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	legs := core.availableLegs()
	leg0, leg1 := legs[0], legs[1]
	leg0.queuedBytes.Store(16 * 1024)
	leg1.queuedBytes.Store(64)
	if got := core.chooseLeg(legs, 1200); got == nil || got.id != 1 {
		t.Fatalf("byte-aware scheduler ignored backlog: leg=%v q0=%d q1=%d", got, leg0.queuedBytes.Load(), leg1.queuedBytes.Load())
	}
}

func TestDatagramMaxPayloadBoundary(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
	}{
		{name: "below", size: maxRoutedDatagramSize - 1},
		{name: "maximum", size: maxRoutedDatagramSize},
		{name: "above", size: maxRoutedDatagramSize + 1},
		{name: "oversize", size: maxRoutedDatagramSize + 4096},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, _ := newDatagramCore(datagramConfig{MaxDatagramSize: maxRoutedDatagramSize})
			defer core.Close()
			data := make([]byte, test.size)
			if test.size > maxRoutedDatagramSize {
				err := core.sendDatagram(data, "192.0.2.1:53", time.Time{})
				if !errors.Is(err, errDatagramTooLarge) {
					t.Fatalf("oversize error = %v, want errDatagramTooLarge", err)
				}
				if core.txSeq.Load() != 0 {
					t.Fatal("oversize datagram consumed a datagram id")
				}
				return
			}

			local, peer := net.Pipe()
			defer peer.Close()
			if err := core.addLeg(0, local, nil); err != nil {
				t.Fatal(err)
			}
			frames := make(chan datagramPacket, 1)
			errCh := make(chan error, 1)
			go func() {
				frame, err := readDatagramFrame(peer, maxRoutedDatagramSize)
				if err == nil {
					frames <- frame
				}
				errCh <- err
			}()
			if err := core.sendDatagram(data, "192.0.2.1:53", time.Time{}); err != nil {
				t.Fatal(err)
			}
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			frame := <-frames
			if len(frame.data) != test.size {
				t.Fatalf("wire payload length = %d, want %d", len(frame.data), test.size)
			}
		})
	}
}

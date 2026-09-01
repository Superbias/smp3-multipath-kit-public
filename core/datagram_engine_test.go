package smp3core

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func testDatagramConfig(mode DatagramMode) DatagramConfig {
	return DatagramConfig{
		Mode:                       mode,
		QueueFrames:                32,
		MaxDatagramSize:            MaxDatagramPayload,
		DedupWindow:                64,
		IdleTimeout:                time.Hour,
		RecoveryTimeout:            time.Second,
		AdaptiveQueueDelay:         20 * time.Millisecond,
		AdaptiveDuplicateThreshold: 128,
		BandwidthMbps:              []uint32{100, 500},
	}
}

type datagramNetPair struct {
	left  DatagramLeg
	right DatagramLeg
}

func newDatagramNetPair() datagramNetPair {
	left, right := net.Pipe()
	return datagramNetPair{left: left, right: right}
}

func attachDatagramPair(t *testing.T, left, right *DatagramEngine, id LegID) datagramNetPair {
	t.Helper()
	pair := newDatagramNetPair()
	if err := left.AttachLeg(id, pair.left, nil); err != nil {
		t.Fatal(err)
	}
	if err := right.AttachLeg(id, pair.right, nil); err != nil {
		t.Fatal(err)
	}
	return pair
}

func receiveDatagram(t *testing.T, engine *DatagramEngine) Datagram {
	t.Helper()
	datagram, err := engine.Receive(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return datagram
}

func TestDatagramEngineStripeTwoWayIndependentTransport(t *testing.T) {
	left := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	right := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	defer left.Close()
	defer right.Close()

	pairs := []datagramNetPair{
		attachDatagramPair(t, left, right, 0),
		attachDatagramPair(t, left, right, 1),
	}
	defer pairs[0].left.Close()
	defer pairs[0].right.Close()
	defer pairs[1].left.Close()
	defer pairs[1].right.Close()

	for _, payload := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		if err := left.Send(payload, "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		got := receiveDatagram(t, right)
		if got.Address != "192.0.2.1:53" || !bytes.Equal(got.Payload, payload) {
			t.Fatalf("got=%+v want address/payload=%q/%q", got, "192.0.2.1:53", payload)
		}
	}
	if err := right.Send([]byte("reverse"), "198.51.100.8:443", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := receiveDatagram(t, left)
	if got.Address != "198.51.100.8:443" || string(got.Payload) != "reverse" {
		t.Fatalf("reverse datagram=%+v", got)
	}
}

func TestDatagramEngineDuplicateDeliversExactlyOnce(t *testing.T) {
	left := NewDatagramEngine(testDatagramConfig(DatagramDuplicate))
	right := NewDatagramEngine(testDatagramConfig(DatagramDuplicate))
	defer left.Close()
	defer right.Close()
	pair0 := attachDatagramPair(t, left, right, 0)
	pair1 := attachDatagramPair(t, left, right, 1)
	defer pair0.left.Close()
	defer pair0.right.Close()
	defer pair1.left.Close()
	defer pair1.right.Close()

	source := []byte("immutable duplicate payload")
	if err := left.Send(source, "203.0.113.9:53", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	copy(source, bytes.Repeat([]byte("x"), len(source)))
	got := receiveDatagram(t, right)
	if string(got.Payload) != "immutable duplicate payload" {
		t.Fatalf("payload changed after Send: %q", got.Payload)
	}
	deadline := time.Now().Add(time.Second)
	for right.Snapshot().DuplicateRxDrop == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := right.Snapshot()
	if stats.DuplicateRxDrop == 0 || left.Snapshot().DuplicateTx == 0 {
		t.Fatalf("duplicate stats=%+v left=%+v", stats, left.Snapshot())
	}
}

func TestDatagramEngineAdaptiveSchedulerAndDuplicateBoundary(t *testing.T) {
	engine := NewDatagramEngine(testDatagramConfig(DatagramAdaptive))
	defer engine.Close()
	pair0 := newDatagramNetPair()
	pair1 := newDatagramNetPair()
	if err := engine.AttachLeg(0, pair0.left, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.AttachLeg(1, pair1.left, nil); err != nil {
		t.Fatal(err)
	}
	defer pair0.left.Close()
	defer pair0.right.Close()
	defer pair1.left.Close()
	defer pair1.right.Close()
	legs := engine.availableLegs()
	legs[0].perf.mu.Lock()
	legs[0].perf.ewmaBytesPerSec = 10 * 1e6
	legs[0].perf.ewmaDelay = 100 * time.Millisecond
	legs[0].perf.mu.Unlock()
	legs[1].perf.mu.Lock()
	legs[1].perf.ewmaBytesPerSec = 10 * 1e6
	legs[1].perf.ewmaDelay = time.Millisecond
	legs[1].perf.mu.Unlock()
	if got := engine.chooseLeg(legs, 1200); got == nil || got.id != 1 {
		t.Fatalf("adaptive scheduler chose slow leg: %#v", got)
	}
	if !engine.shouldDuplicate(127, legs) || !engine.shouldDuplicate(128, legs) || engine.shouldDuplicate(129, legs) {
		t.Fatal("adaptive duplicate threshold boundary changed")
	}
}

func runDatagramEngineReplacement(t *testing.T, id LegID) {
	t.Helper()
	left := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	right := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	defer left.Close()
	defer right.Close()
	old := attachDatagramPair(t, left, right, id)
	defer old.left.Close()
	defer old.right.Close()
	if err := left.Send([]byte("before"), "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := receiveDatagram(t, right); string(got.Payload) != "before" {
		t.Fatalf("before replacement payload=%q", got.Payload)
	}
	if !left.ReplaceLeg(id, errors.New("test leg failure")) {
		t.Fatalf("left leg %d was not replaced", id)
	}
	deadline := time.Now().Add(time.Second)
	for right.hasLeg(uint8(id)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if right.hasLeg(uint8(id)) {
		t.Fatalf("right leg %d did not retire", id)
	}
	replacement := attachDatagramPair(t, left, right, id)
	defer replacement.left.Close()
	defer replacement.right.Close()
	if err := left.Send([]byte("after"), "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := receiveDatagram(t, right); string(got.Payload) != "after" {
		t.Fatalf("after replacement payload=%q", got.Payload)
	}
}

func TestDatagramEngineLeg0Replacement(t *testing.T) { runDatagramEngineReplacement(t, 0) }
func TestDatagramEngineLeg1Replacement(t *testing.T) { runDatagramEngineReplacement(t, 1) }

func TestDatagramEngineIdleCloseDoesNotReportLegFailure(t *testing.T) {
	down := make(chan error, 1)
	cfg := testDatagramConfig(DatagramAdaptive)
	cfg.IdleTimeout = 20 * time.Millisecond
	cfg.OnLegDown = func(_ LegID, err error) { down <- err }
	engine := NewDatagramEngine(cfg)
	defer engine.Close()
	select {
	case <-engine.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("idle timeout did not close datagram engine")
	}
	select {
	case err := <-down:
		t.Fatalf("idle close reported leg failure: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestDatagramEngineMaxPayloadBoundary(t *testing.T) {
	engine := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	defer engine.Close()
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	if err := engine.AttachLeg(0, local, nil); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{MaxDatagramPayload - 1, MaxDatagramPayload} {
		payload := bytes.Repeat([]byte{byte(size)}, size)
		if err := engine.Send(payload, "192.0.2.1:53", time.Now().Add(time.Second)); err != nil {
			t.Fatalf("size=%d: %v", size, err)
		}
		frameID, _, got, err := ReadDatagramFrame(peer, MaxDatagramPayload)
		if err != nil || frameID != uint64(size-MaxDatagramPayload+1) || len(got) != size {
			t.Fatalf("size=%d id=%d len=%d err=%v", size, frameID, len(got), err)
		}
	}
	if err := engine.Send(make([]byte, MaxDatagramPayload+1), "192.0.2.1:53", time.Time{}); !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversize error=%v", err)
	}
	if got := engine.TxSequenceForTest(); got != 2 {
		t.Fatalf("oversize consumed wire ID: %d", got)
	}
}

type fakeDatagramLeg struct {
	reader io.Reader
	writer io.Writer
	close  func() error
	once   sync.Once
}

var _ DatagramLeg = (*fakeDatagramLeg)(nil)

func (l *fakeDatagramLeg) Read(p []byte) (int, error)  { return l.reader.Read(p) }
func (l *fakeDatagramLeg) Write(p []byte) (int, error) { return l.writer.Write(p) }
func (l *fakeDatagramLeg) Close() error {
	var err error
	l.once.Do(func() {
		if l.close != nil {
			err = l.close()
		}
	})
	return err
}

func TestDatagramEngineFakeLegWithoutNetConn(t *testing.T) {
	aToBReader, aToBWriter := io.Pipe()
	bToAReader, bToAWriter := io.Pipe()
	left := &fakeDatagramLeg{reader: bToAReader, writer: aToBWriter}
	right := &fakeDatagramLeg{reader: aToBReader, writer: bToAWriter}
	left.close = func() error { _ = aToBWriter.Close(); return bToAReader.Close() }
	right.close = func() error { _ = bToAWriter.Close(); return aToBReader.Close() }
	engineA := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	engineB := NewDatagramEngine(testDatagramConfig(DatagramStripe))
	defer engineA.Close()
	defer engineB.Close()
	if err := engineA.AttachLeg(0, left, nil); err != nil {
		t.Fatal(err)
	}
	if err := engineB.AttachLeg(0, right, nil); err != nil {
		t.Fatal(err)
	}
	if err := engineA.Send([]byte("fake-leg"), "198.51.100.8:443", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := receiveDatagram(t, engineB)
	if got.Address != "198.51.100.8:443" || string(got.Payload) != "fake-leg" {
		t.Fatalf("fake leg datagram=%+v", got)
	}
}

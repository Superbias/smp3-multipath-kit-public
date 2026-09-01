package smp3core

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func testStreamConfig() StreamConfig {
	return StreamConfig{
		ChunkSize:         1024,
		QueueFrames:       16,
		MaxReorderFrames:  64,
		MaxInflightFrames: 32,
		AckInterval:       2 * time.Millisecond,
		RetransmitTimeout: 20 * time.Millisecond,
		RecoveryTimeout:   time.Second,
		BandwidthMbps:     []uint32{100, 500},
	}
}

type engineLegPair struct {
	left  StreamLeg
	right StreamLeg
}

func newEngineLegPair() engineLegPair {
	left, right := net.Pipe()
	return engineLegPair{left: left, right: right}
}

func attachEngineLegPair(t *testing.T, left, right *StreamEngine, id LegID) engineLegPair {
	t.Helper()
	pair := newEngineLegPair()
	if err := left.AttachLeg(id, pair.left, nil); err != nil {
		_ = pair.left.Close()
		_ = pair.right.Close()
		t.Fatalf("attach left leg %d: %v", id, err)
	}
	if err := right.AttachLeg(id, pair.right, nil); err != nil {
		_ = pair.left.Close()
		_ = pair.right.Close()
		t.Fatalf("attach right leg %d: %v", id, err)
	}
	return pair
}

func readExactWithTimeout(t *testing.T, reader io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(reader, got)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exact application payload")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload mismatch: got %q want %q", got, want)
	}
}

func TestStreamEngineTwoWayIndependentRoundTrip(t *testing.T) {
	left, leftApp := NewStreamEngine(testStreamConfig())
	right, rightApp := NewStreamEngine(testStreamConfig())
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()

	attachEngineLegPair(t, left, right, 0)
	attachEngineLegPair(t, left, right, 1)

	leftPayload := bytes.Repeat([]byte("left-to-right"), 200)
	rightPayload := bytes.Repeat([]byte("right-to-left"), 200)
	leftRead := make(chan error, 1)
	rightRead := make(chan error, 1)
	go func() {
		got := make([]byte, len(rightPayload))
		_, err := io.ReadFull(leftApp, got)
		if err == nil && !bytes.Equal(got, rightPayload) {
			err = io.ErrUnexpectedEOF
		}
		leftRead <- err
	}()
	go func() {
		got := make([]byte, len(leftPayload))
		_, err := io.ReadFull(rightApp, got)
		if err == nil && !bytes.Equal(got, leftPayload) {
			err = io.ErrUnexpectedEOF
		}
		rightRead <- err
	}()
	leftWrite := make(chan error, 1)
	rightWrite := make(chan error, 1)
	go func() { _, err := leftApp.Write(leftPayload); leftWrite <- err }()
	go func() { _, err := rightApp.Write(rightPayload); rightWrite <- err }()
	for name, ch := range map[string]chan error{
		"left write":  leftWrite,
		"right write": rightWrite,
		"left read":   leftRead,
		"right read":  rightRead,
	} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s timed out", name)
		}
	}
}

func runStreamEngineLegReplacement(t *testing.T, id LegID) {
	t.Helper()
	left, leftApp := NewStreamEngine(testStreamConfig())
	right, rightApp := NewStreamEngine(testStreamConfig())
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()

	old := attachEngineLegPair(t, left, right, id)
	first := bytes.Repeat([]byte("before-replacement"), 80)
	writeDone := make(chan error, 1)
	go func() { _, err := leftApp.Write(first); writeDone <- err }()
	readExactWithTimeout(t, rightApp, first)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	if !left.ReplaceLeg(id, io.ErrClosedPipe) {
		t.Fatalf("failed to replace leg %d on left engine", id)
	}
	_ = old.left.Close()
	_ = old.right.Close()
	deadline := time.Now().Add(time.Second)
	for right.HasLeg(id) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if right.HasLeg(id) {
		t.Fatalf("right engine did not observe leg %d failure", id)
	}

	replacement := attachEngineLegPair(t, left, right, id)
	defer replacement.left.Close()
	defer replacement.right.Close()
	second := bytes.Repeat([]byte("after-replacement"), 80)
	writeDone = make(chan error, 1)
	go func() { _, err := leftApp.Write(second); writeDone <- err }()
	readExactWithTimeout(t, rightApp, second)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestStreamEngineLeg1ReplacementPreservesLogicalStream(t *testing.T) {
	runStreamEngineLegReplacement(t, 1)
}

func TestStreamEngineLeg0ReplacementPreservesLogicalStream(t *testing.T) {
	runStreamEngineLegReplacement(t, 0)
}

func TestStreamEngineGracefulCloseDrainsTail(t *testing.T) {
	left, leftApp := NewStreamEngine(testStreamConfig())
	right, rightApp := NewStreamEngine(testStreamConfig())
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()
	attachEngineLegPair(t, left, right, 0)

	payload := bytes.Repeat([]byte("write-then-close-tail"), 200)
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(rightApp, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := leftApp.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("graceful close did not drain the full application tail")
	}
}

type schedulerStubLeg struct{}

func (schedulerStubLeg) Read([]byte) (int, error)    { return 0, io.EOF }
func (schedulerStubLeg) Write(p []byte) (int, error) { return len(p), nil }
func (schedulerStubLeg) Close() error                { return nil }

func TestRetransmitLoopFrontierRescueSurvivesBlockedOrdinaryRetry(t *testing.T) {
	cfg := testStreamConfig()
	cfg.QueueFrames = 4
	cfg.RetransmitTimeout = 20 * time.Millisecond
	core, app := NewStreamEngine(cfg)
	defer core.Close()
	defer app.Close()

	frontier := core.txLedger.Add([]byte("frontier"), time.Now().Add(-time.Second))
	if !core.txLedger.MarkTransit(frontier, 0, time.Now().Add(-time.Second)) {
		t.Fatal("frontier was not marked in transit")
	}
	ordinaryRetry := core.txLedger.Add([]byte("ordinary-retry"), time.Now().Add(-time.Second))
	if got := core.txLedger.PlanRetries(StreamLegAvailability{true, true}); len(got) != 1 || got[0].Record != ordinaryRetry {
		t.Fatalf("ordinary retry setup=%+v", got)
	}

	legs := make([]*streamLeg, 2)
	core.legsMu.Lock()
	for id := uint8(0); id < 2; id++ {
		leg := &streamLeg{
			id: id, conn: schedulerStubLeg{},
			send:    make(chan txSendAttempt, cfg.QueueFrames),
			rescue:  make(chan txSendAttempt, 8),
			control: make(chan legControl, 8),
			ackWake: make(chan struct{}, 1),
			done:    make(chan struct{}), retired: make(chan struct{}),
		}
		for n := 0; n < cap(leg.send); n++ {
			leg.send <- txSendAttempt{}
		}
		core.legs[id] = leg
		legs[id] = leg
	}
	core.legsMu.Unlock()
	core.active.Store(true)

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	deadline := time.Now().Add(time.Second)
	for core.FrontierRescueAttempts() == 0 && time.Now().Before(deadline) {
		select {
		case <-ticker.C:
		case <-timeout.C:
		}
	}
	if core.FrontierRescueAttempts() == 0 {
		t.Fatal("frontier rescue was starved by a blocked ordinary retry")
	}
	if len(legs[1].rescue) != 1 {
		t.Fatalf("rescue queue length=%d, want 1", len(legs[1].rescue))
	}
}

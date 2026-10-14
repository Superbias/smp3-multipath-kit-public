package smp3core

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestClosureActivationBeforeAttach(t *testing.T) {
	notified := make(chan struct{}, 1)
	c, app := NewStreamEngine(StreamConfig{NotifyPeerOnActivate: true, activateNotifyHook: func() {
		select {
		case notified <- struct{}{}:
		default:
		}
	}})
	defer c.Close()
	defer app.Close()
	<-notified // finish the no-leg attempt before attachment
	a, b := net.Pipe()
	defer b.Close()
	if err := c.AttachLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	b.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	header := make([]byte, StreamFrameHeaderSize)
	if err := func() error { _, err := io.ReadFull(b, header); return err }(); err != nil {
		t.Fatalf("pending activation lost: %v", err)
	}
	if header[0] != frameTypeActivate {
		t.Fatalf("expected ACTIVATE, got %v", header)
	}
}

func TestClosureFullControlQueueNonblocking(t *testing.T) {
	c := &StreamEngine{done: make(chan struct{}), legs: map[uint8]*streamLeg{}}
	l := &streamLeg{id: 0, done: make(chan struct{}), control: make(chan legControl, 1)}
	c.legs[0] = l
	l.control <- legControl{}
	finished := make(chan struct{})
	go func() { c.notifyPeerActivate(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(100 * time.Millisecond):
		<-l.control
		<-finished
		t.Fatal("full control queue blocked activation maintenance")
	}
}

func TestClosureQueueRejectRaceActivates(t *testing.T) {
	c := &StreamEngine{done: make(chan struct{}), activeCh: make(chan struct{}), spaceWake: make(chan streamSpaceNotification, 1), txLedger: NewStreamTXLedger(), legs: map[uint8]*streamLeg{}, cfg: StreamConfig{ChunkSize: 1024}}
	l := &streamLeg{id: 0, done: make(chan struct{}), send: make(chan txSendAttempt, 1)}
	c.legs[0] = l
	l.send <- txSendAttempt{}
	c.cfg.queueRejectHook = func(l *streamLeg, r *StreamTXRecord, retry bool) { <-l.send; c.kickSpaceSource("TEST_RELEASE", 0) }
	r := c.txLedger.Add(make([]byte, 1024), time.Now())
	if err := c.enqueueRecord(r, -1); err != nil {
		t.Fatal(err)
	}
	if !c.active.Load() {
		t.Fatal("queue-full evidence lost when writer dequeued before second len check")
	}
}

func TestClosureLocalEnqueueRetryOwnership(t *testing.T) {
	l := NewStreamTXLedger()
	r := l.addLocal([]byte{1}, time.Now())
	if got := l.PlanRetries(StreamLegAvailability{true, false}); len(got) != 0 {
		t.Fatal("maintenance stole application's initial enqueue")
	}
	l.finishLocalEnqueue(r)
	if got := l.PlanRetries(StreamLegAvailability{true, false}); len(got) != 1 {
		t.Fatal("finished enqueue lost retry exit path")
	}
}
func TestClosureNoIdleActivation(t *testing.T) {
	c := slowLegUnitEngine()
	delete(c.legs, 1)
	now := time.Now()
	c.cfg.ActivationWindow = time.Second
	c.txLedger.Add([]byte{1}, now.Add(-2*time.Second))
	if c.stalledActivationReady(now) {
		t.Fatal("outstanding-only idle tail activated")
	}
	c.inputBlocked.Store(true)
	if !c.stalledActivationReady(now) {
		t.Fatal("real blocked demand without delivery never activates")
	}
	c.txLedger.ApplyACK(1, now)
	c.txLedger.Add([]byte{1}, now)
	if c.stalledActivationReady(now.Add(100 * time.Millisecond)) {
		t.Fatal("short healthy ACK delay activated")
	}
}
func TestClosureNotificationFullQueueRetryAndGeneration(t *testing.T) {
	c := &StreamEngine{done: make(chan struct{}), legs: map[uint8]*streamLeg{}}
	l := &streamLeg{id: 1, done: make(chan struct{}), control: make(chan legControl, 1)}
	c.legs[1] = l
	l.control <- legControl{}
	c.notifyPeerActivate()
	if l.activateQueued.Load() {
		t.Fatal("full queue consumed intent")
	}
	<-l.control
	c.notifyPeerActivate()
	if (<-l.control).typ != frameTypeActivate {
		t.Fatal("intent did not retry")
	}
	c.notifyPeerActivate()
	if len(l.control) != 0 {
		t.Fatal("duplicate generation notification")
	}
	next := &streamLeg{id: 1, done: make(chan struct{}), control: make(chan legControl, 1)}
	c.legs[1] = next
	c.notifyPeerActivate()
	if len(next.control) != 1 {
		t.Fatal("rejoined generation lost intent")
	}
	close(c.done)
	next.activateQueued.Store(false)
	c.notifyPeerActivate()
}

func TestClosureFiniteSentTailActivation(t *testing.T) {
	c := slowLegUnitEngine()
	delete(c.legs, 1)
	c.cfg.ActivationWindow = time.Second
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 2*c.cfg.ChunkSize), now.Add(-3*time.Second))
	c.txLedger.assignOrdinary(r, 0, now.Add(-3*time.Second))
	c.txLedger.MarkTransit(r, 0, now.Add(-3*time.Second))
	c.txLedger.MarkAttemptSent(r, 0, false, now.Add(-3*time.Second))
	if !c.stalledActivationReady(now) {
		t.Fatal("finite written tail with no delivery lost companion exit path")
	}
}

func TestClosureQueueReleasePreservesLocalWriter(t *testing.T) {
	c := &StreamEngine{done: make(chan struct{}), activeCh: make(chan struct{}), spaceWake: make(chan streamSpaceNotification, 1), txLedger: NewStreamTXLedger(), legs: map[uint8]*streamLeg{}, cfg: StreamConfig{ChunkSize: 1024}}
	leg := &streamLeg{id: 0, done: make(chan struct{}), send: make(chan txSendAttempt, 1)}
	c.legs[0] = leg
	leg.send <- txSendAttempt{}
	c.cfg.queueRejectHook = func(l *streamLeg, r *StreamTXRecord, retry bool) {
		<-l.send
		c.scheduleRetries()
		c.kickSpaceSource("TEST_RELEASE", 0)
	}
	record := c.txLedger.addLocal([]byte{1}, time.Now())
	finished := make(chan error, 1)
	go func() { finished <- c.enqueueRecord(record, -1) }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		c.txLedger.ApplyACK(record.Sequence()+1, time.Now())
		c.kickSpaceSource("TEST_CLEANUP", 0)
		<-finished
		t.Fatal("release allowed maintenance to steal the original local enqueue, leaving writer asleep without ACK")
	}
	c.txLedger.finishLocalEnqueue(record)
}

func TestClosureEOFNotificationIsBounded(t *testing.T) {
	c, app := NewStreamEngine(StreamConfig{NotifyPeerOnActivate: true, RecoveryTimeout: 100 * time.Millisecond})
	defer c.Close()
	a, b := net.Pipe()
	defer b.Close()
	if err := c.AttachLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	app.Close() // no DATA tail, but the last carrier never consumes control
	select {
	case <-c.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("EOF notification on an unread last carrier ignored the recovery stall bound")
	}
}

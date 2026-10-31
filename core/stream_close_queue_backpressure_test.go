package smp3core

import (
	"testing"
	"time"
)

func TestStreamCloseRetriesTemporarilyFullPreferredControlQueue(t *testing.T) {
	events := make(chan streamCloseTraceEvent, 16)
	// Keep enough separation between the retry ticker and the RTT fallback to
	// make this test robust to Windows timer coalescing. Other CLOSE tests retain
	// the 20 ms preference bound.
	engine, legs := closeFanoutNewSenderWithRTTs(t, events, [2]time.Duration{100 * time.Millisecond, 400 * time.Millisecond})
	defer engine.Close()

	// Occupy the fast leg's only control slot before CLOSE fanout begins. The
	// slow leg can still accept and locally write its redundant CLOSE.
	queuedControl := legControl{typ: frameTypeActivate}
	legs[0].control <- queuedControl
	result := closeFanoutStartSend(engine)
	control1 := closeFanoutWaitControl(t, legs[1])
	_ = closeFanoutWriteControl(t, legs[1], control1)

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			if event.Stage == streamCloseWaitingPreferred && event.LegID == 0 {
				goto preferredWaitObserved
			}
		case ok := <-result:
			t.Fatalf("CLOSE completed before temporarily full preferred queue recovered; ok=%v", ok)
		case <-deadline.C:
			t.Fatal("Core did not report waiting for the preferred fast leg")
		}
	}

preferredWaitObserved:
	// Keep the queue full briefly, then release its slot well within the
	// authenticated 100 ms preferred-path fallback bound.
	blocked := time.NewTimer(5 * time.Millisecond)
	defer blocked.Stop()
	select {
	case ok := <-result:
		t.Fatalf("CLOSE completed while preferred queue was briefly full; ok=%v", ok)
	case <-blocked.C:
	}

	if got := <-legs[0].control; got.typ != queuedControl.typ {
		t.Fatalf("drained control type=%d, want occupied activation type=%d", got.typ, queuedControl.typ)
	}
	var control0 legControl
	select {
	case control0 = <-legs[0].control:
	case ok := <-result:
		t.Fatalf("CLOSE completed before retrying preferred queue; ok=%v", ok)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Core did not retry CLOSE after the preferred control queue recovered")
	}
	_ = closeFanoutWriteControl(t, legs[0], control0)
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("CLOSE failed after the preferred fast leg accepted its queued Write")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("CLOSE did not complete after the preferred fast-leg Write succeeded")
	}
}

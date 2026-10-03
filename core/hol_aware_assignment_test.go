package smp3core

import (
	"net"
	"testing"
	"time"
)

func newHOLTestEngine(t *testing.T, mode StreamHOLMode, weights []uint32) *StreamEngine {
	t.Helper()
	cfg := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityFixed,
		HOLMode: mode, BandwidthMbps: weights, ChunkSize: 1024,
		QueueFrames: 32, MaxInflightFrames: 64, MaxReorderFrames: 128,
		AckInterval: time.Millisecond, RetransmitTimeout: time.Second,
	}
	e, _ := NewStreamEngine(cfg)
	for id := 0; id < 2; id++ {
		local, remote := net.Pipe()
		if err := e.AttachLeg(LegID(id), local, nil); err != nil {
			remote.Close()
			e.Close()
			t.Fatal(err)
		}
		// The test only exercises assignment. Closing the peer after engine
		// teardown wakes the transport workers without adding a second scheduler.
		t.Cleanup(func() { remote.Close() })
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func assignHOLRecord(t *testing.T, e *StreamEngine, n int) *streamLeg {
	t.Helper()
	r := e.txLedger.Add(make([]byte, n), time.Now())
	a := &benchmarkAssignment{}
	leg := e.assignBenchmark(r, a)
	if leg == nil {
		t.Fatal("assignment returned no leg")
	}
	return leg
}

func TestHOLCompletionCorrectionIsFrontierBounded(t *testing.T) {
	e := newHOLTestEngine(t, StreamHOLCompletion, []uint32{100, 100})
	e.SetLegPerformanceForTest(0, 100e6, 100e6, 20*time.Millisecond)
	e.SetLegPerformanceForTest(1, 100e6, 100e6, 100*time.Millisecond)
	e.weightedMu.Lock()
	e.assignedService[0] = 100 << 10 // entitlement baseline points at leg1
	e.weightedMu.Unlock()
	if got := assignHOLRecord(t, e, 1024).id; got != 0 {
		t.Fatalf("frontier correction selected leg%d, want leg0", got)
	}
	for i := 0; i < 12; i++ {
		assignHOLRecord(t, e, 1024)
	}
	stats := e.Snapshot()
	if stats.HOLCorrections == 0 || stats.HOLPredictedEarlier == 0 {
		t.Fatalf("HOL correction did not fire: %+v", stats)
	}
	if stats.HOLMaxCorrectionStreak > holMaxCorrectionStreak {
		t.Fatalf("correction streak=%d exceeds bound", stats.HOLMaxCorrectionStreak)
	}
	if stats.CapacityAssignmentShare[0] == 0 || stats.CapacityAssignmentShare[1] == 0 {
		t.Fatalf("correction collapsed assignment share: %+v", stats.CapacityAssignmentShare)
	}
}

func TestHOLLegacyAndSymmetricControlDoNotCorrect(t *testing.T) {
	legacy := newHOLTestEngine(t, StreamHOLLegacy, []uint32{100, 100})
	legacy.SetLegPerformanceForTest(0, 100e6, 100e6, 20*time.Millisecond)
	legacy.SetLegPerformanceForTest(1, 100e6, 100e6, 100*time.Millisecond)
	legacy.weightedMu.Lock()
	legacy.assignedService[0] = 100 << 10
	legacy.weightedMu.Unlock()
	assignHOLRecord(t, legacy, 1024)
	if got := legacy.Snapshot().HOLCorrections; got != 0 {
		t.Fatalf("legacy mode corrected=%d", got)
	}

	symmetric := newHOLTestEngine(t, StreamHOLCompletion, []uint32{100, 100})
	symmetric.SetLegPerformanceForTest(0, 100e6, 100e6, 20*time.Millisecond)
	symmetric.SetLegPerformanceForTest(1, 100e6, 100e6, 20*time.Millisecond)
	symmetric.weightedMu.Lock()
	symmetric.assignedService[0] = 100 << 10
	symmetric.weightedMu.Unlock()
	assignHOLRecord(t, symmetric, 1024)
	if got := symmetric.Snapshot().HOLCorrections; got != 0 {
		t.Fatalf("symmetric control corrected=%d", got)
	}
}

func TestHOLBacklogAndStallPrediction(t *testing.T) {
	e := newHOLTestEngine(t, StreamHOLCompletion, []uint32{100, 100})
	e.SetLegPerformanceForTest(0, 100e6, 100e6, 20*time.Millisecond)
	e.SetLegPerformanceForTest(1, 100e6, 100e6, 500*time.Millisecond)
	e.pendingDepth[1].Store(16)
	e.weightedMu.Lock()
	e.assignedService[0] = 100 << 10
	e.weightedMu.Unlock()
	if got := assignHOLRecord(t, e, 1024).id; got != 0 {
		t.Fatalf("stall prediction selected leg%d, want healthy leg0", got)
	}
	stats := e.Snapshot()
	if stats.HOLPredictedEarlier == 0 || stats.FrontierRescueAttempts != 0 {
		t.Fatalf("stall assignment state=%+v", stats)
	}
	// Once the artificial backlog/latency is removed, completion correction
	// must not remain a permanent low-latency preference.
	e.pendingDepth[1].Store(0)
	e.SetLegPerformanceForTest(1, 100e6, 100e6, 20*time.Millisecond)
	for i := 0; i < 12; i++ {
		assignHOLRecord(t, e, 1024)
	}
	stats = e.Snapshot()
	if stats.CapacityAssignmentShare[1] == 0 {
		t.Fatalf("entitlement did not recover after backlog cleared: %+v", stats.CapacityAssignmentShare)
	}
}

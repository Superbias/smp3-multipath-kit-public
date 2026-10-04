package smp3core

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// TestHOLCompletionMatrix is an opt-in virtual-carrier evidence gate. It uses
// the same serializer and ACK ledger as the production aggregation path while
// keeping the default package test suite fast.
func TestHOLCompletionMatrix(t *testing.T) {
	if os.Getenv("SMP3_HOL_BENCHMARK") == "" {
		t.Skip("set SMP3_HOL_BENCHMARK=1")
	}
	type scenario struct {
		name      string
		size      int
		rates     [2]int64
		latencies [2]time.Duration
		faults    [2]benchmarkFault
	}
	scenarios := []scenario{
		{name: "A_equal_capacity_rtt", size: 8 << 20, rates: [2]int64{100e6, 100e6}, latencies: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}},
		{name: "B_capacity_rtt", size: 8 << 20, rates: [2]int64{50e6, 200e6}, latencies: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}},
		{name: "C_short_stall", size: 8 << 20, rates: [2]int64{100e6, 100e6}, latencies: [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}, faults: [2]benchmarkFault{{}, {stallAfterBytes: 128 << 10, stallDuration: 300 * time.Millisecond}}},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SMP3_PRODUCTION_AGGREGATION", "1")
			t.Setenv("SMP3_BENCHMARK_FRONTIER_EXPOSURE_GRACE", "1")
			t.Setenv("SMP3_BENCHMARK_DISABLE_RESCUE", "1")
			results := make(map[string]struct {
				elapsed time.Duration
				stats   StreamStats
			})
			for _, mode := range []string{"legacy", "completion"} {
				t.Setenv("SMP3_BENCHMARK_HOL_MODE", mode)
				started := time.Now()
				elapsed, stats, err := runStreamAggregationSampleWithMode(tc.size, 2, 0, tc.rates, tc.latencies, tc.faults, StreamSchedulerAggregation)
				if err != nil {
					t.Fatal(err)
				}
				results[mode] = struct {
					elapsed time.Duration
					stats   StreamStats
				}{elapsed: elapsed, stats: stats}
				fmt.Fprintf(os.Stderr, "HOL_MATRIX scenario=%s mode=%s size=%d elapsed=%s wall=%s share=[%.3f,%.3f] corrections=%d frontier_candidates=%d predicted_earlier=%d max_streak=%d gap=%s max_gap=%s rescue=%d retransmit=%v\n",
					tc.name, mode, tc.size, elapsed, time.Since(started), stats.CapacityAssignmentShare[0], stats.CapacityAssignmentShare[1], stats.HOLCorrections, stats.HOLFrontierCandidates, stats.HOLPredictedEarlier, stats.HOLMaxCorrectionStreak, stats.RxGapAge, stats.RxMaxGapAge, stats.FrontierRescueAttempts, stats.TxRetransmitBytesByLeg)
			}
			off, on := results["legacy"], results["completion"]
			physical := on.stats.TxSentBytesByLeg[0] + on.stats.TxSentBytesByLeg[1]
			if physical < uint64(tc.size) || on.stats.OutstandingFrames != 0 {
				t.Fatalf("completion run did not retire payload: physical=%d outstanding=%d", physical, on.stats.OutstandingFrames)
			}
			if on.stats.HOLMaxCorrectionStreak > holMaxCorrectionStreak {
				t.Fatalf("correction streak exceeded bound: %d", on.stats.HOLMaxCorrectionStreak)
			}
			if tc.name != "C_short_stall" && off.stats.FrontierRescueAttempts != 0 && on.stats.FrontierRescueAttempts > off.stats.FrontierRescueAttempts+1 {
				t.Fatalf("completion mode increased rescue activity: off=%d on=%d", off.stats.FrontierRescueAttempts, on.stats.FrontierRescueAttempts)
			}
			if tc.name == "B_capacity_rtt" && (on.stats.CapacityAssignmentShare[0] < 0.10 || on.stats.CapacityAssignmentShare[0] > 0.40) {
				t.Fatalf("capacity entitlement drifted from 20%% target: share=%v", on.stats.CapacityAssignmentShare)
			}
		})
	}
}

func BenchmarkHOLAssignment(b *testing.B) {
	for _, tc := range []struct {
		name string
		mode StreamHOLMode
	}{
		{name: "legacy", mode: StreamHOLLegacy},
		{name: "completion", mode: StreamHOLCompletion},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e := newHOLBenchmarkEngine(tc.mode)
			defer e.Close()
			e.SetLegPerformanceForTest(0, 100e6, 100e6, 20*time.Millisecond)
			e.SetLegPerformanceForTest(1, 100e6, 100e6, 100*time.Millisecond)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := e.txLedger.Add(make([]byte, 1024), time.Now())
				e.assignBenchmark(r, &benchmarkAssignment{})
			}
		})
	}
}

func newHOLBenchmarkEngine(mode StreamHOLMode) *StreamEngine {
	cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityFixed, HOLMode: mode,
		BandwidthMbps: []uint32{100, 100}, ChunkSize: 1024, QueueFrames: 32, MaxInflightFrames: 64, MaxReorderFrames: 128,
		AckInterval: time.Millisecond, RetransmitTimeout: time.Second}
	e, _ := NewStreamEngine(cfg)
	e.legsMu.Lock()
	for id := 0; id < 2; id++ {
		local, _ := net.Pipe()
		e.legs[uint8(id)] = &streamLeg{id: uint8(id), conn: local, send: make(chan txSendAttempt, 1), rescue: make(chan txSendAttempt, 1), done: make(chan struct{}), retired: make(chan struct{})}
	}
	e.legsMu.Unlock()
	return e
}

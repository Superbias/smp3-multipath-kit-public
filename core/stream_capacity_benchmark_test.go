package smp3core

import (
	"testing"
	"time"
)

// These are opt-in because they transfer tens of MiB and are intended to
// produce reviewable time-series evidence rather than slow every unit run.
func TestDynamicCapacityBenchmarkMatrix(t *testing.T) {
	if testing.Short() || !envEnabled("SMP3_DYNAMIC_CAPACITY_BENCHMARK") {
		t.Skip("set SMP3_DYNAMIC_CAPACITY_BENCHMARK=1")
	}
	t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "1")
	t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "1")
	t.Setenv("SMP3_BENCHMARK_PACER", "serializer")
	for _, tc := range []struct {
		name      string
		rates     [2]int64
		latencies [2]time.Duration
	}{
		{"100+100", [2]int64{100e6, 100e6}, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{"50+100", [2]int64{50e6, 100e6}, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{"50+150", [2]int64{50e6, 150e6}, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{"50+200", [2]int64{50e6, 200e6}, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{"rtt-only-100-20ms-100ms", [2]int64{100e6, 100e6}, [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}},
		{"mixed-50-20ms-200-100ms", [2]int64{50e6, 200e6}, [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "")
			t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "")
			fixedElapsed, fixedStats, err := runStreamAggregationSampleWithMode(32<<20, 2, 0, tc.rates, tc.latencies, [2]benchmarkFault{}, StreamSchedulerAggregation)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "1")
			t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "1")
			elapsed, stats, err := runStreamAggregationSampleWithMode(32<<20, 2, 0, tc.rates, tc.latencies, [2]benchmarkFault{}, StreamSchedulerAggregation)
			if err != nil {
				t.Fatal(err)
			}
			if stats.TxAckedUsefulByLeg[0]+stats.TxAckedUsefulByLeg[1] != 32<<20 {
				t.Fatalf("useful ACK mismatch: %v", stats.TxAckedUsefulByLeg)
			}
			fixedMbps := float64(32<<20) * 8 / fixedElapsed.Seconds() / 1e6
			dynamicMbps := float64(32<<20) * 8 / elapsed.Seconds() / 1e6
			if dynamicMbps < fixedMbps*0.97 {
				t.Fatalf("dynamic throughput regressed: fixed=%.2f dynamic=%.2f fixed_stats=%+v dynamic_stats=%+v", fixedMbps, dynamicMbps, fixedStats, stats)
			}
			t.Logf("stationary fixed=%.2fMbps dynamic=%.2fMbps ratio=%.3f elapsed=%s raw=%v smoothed=%v confidence=%v valid=%v weights=%v acked=%v retry=%v rescue=%d", fixedMbps, dynamicMbps, dynamicMbps/fixedMbps, elapsed, stats.CapacityRawBPS, stats.CapacitySmoothedBPS, stats.CapacityConfidence, stats.CapacityValid, stats.CapacityWeight, stats.TxAckedUsefulByLeg, stats.TxRetransmitBytesByLeg, stats.FrontierRescueAttempts)
		})
	}
}

func TestDynamicCapacityStepAndDemandControl(t *testing.T) {
	if testing.Short() || !envEnabled("SMP3_DYNAMIC_CAPACITY_BENCHMARK") {
		t.Skip("set SMP3_DYNAMIC_CAPACITY_BENCHMARK=1")
	}
	t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "1")
	t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "1")
	t.Setenv("SMP3_BENCHMARK_PACER", "serializer")
	for _, tc := range []struct {
		name   string
		faults [2]benchmarkFault
	}{
		{"step-down-200-to-80", [2]benchmarkFault{{}, {degradeAfter: 1500 * time.Millisecond, degradedBPS: 80e6}}},
		{"step-up-80-to-200", [2]benchmarkFault{{}, {recoverAfter: 1500 * time.Millisecond, recoveredBPS: 200e6}}},
		{"transient-200-to-50", [2]benchmarkFault{{}, {degradeAfter: 1500 * time.Millisecond, degradedBPS: 50e6, recoverAfter: 3500 * time.Millisecond, recoveredBPS: 200e6}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "step-up-80-to-200" {
				t.Setenv("SMP3_BENCHMARK_BASELINE_RATES", "50,200")
			} else {
				t.Setenv("SMP3_BENCHMARK_BASELINE_RATES", "")
			}
			rates := [2]int64{50e6, 200e6}
			if tc.name == "step-up-80-to-200" {
				rates[1] = 80e6
			}
			size := 128 << 20
			if tc.name == "transient-200-to-50" {
				// Keep the post-recovery interval long enough to observe the
				// estimator returning toward the original 200 Mbps capacity.
				size = 512 << 20
			}
			elapsed, stats, err := runStreamAggregationSampleWithMode(size, 2, 0, rates, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}, tc.faults, StreamSchedulerAggregation)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "step-down-200-to-80" {
				if stats.CapacityWeight[1] < 65 || stats.CapacityWeight[1] > 110 {
					t.Fatalf("step-down did not reflect persistent loss: weights=%v stats=%+v", stats.CapacityWeight, stats)
				}
			} else if tc.name == "step-up-80-to-200" {
				if stats.CapacityWeight[1] < 150 {
					t.Fatalf("step-up did not recover entitlement: weights=%v stats=%+v", stats.CapacityWeight, stats)
				}
			} else if tc.name == "transient-200-to-50" {
				if stats.CapacityWeight[1] < 140 || stats.CapacityWeight[1] > 220 {
					t.Fatalf("transient recovery left unstable entitlement: weights=%v stats=%+v", stats.CapacityWeight, stats)
				}
			}
			t.Logf("step elapsed=%s raw=%v smoothed=%v confidence=%v valid=%v weights=%v acked=%v retry=%v rescue=%d", elapsed, stats.CapacityRawBPS, stats.CapacitySmoothedBPS, stats.CapacityConfidence, stats.CapacityValid, stats.CapacityWeight, stats.TxAckedUsefulByLeg, stats.TxRetransmitBytesByLeg, stats.FrontierRescueAttempts)
		})
	}
}

func TestDynamicCapacityStationaryConvergence(t *testing.T) {
	if testing.Short() || !envEnabled("SMP3_DYNAMIC_CAPACITY_BENCHMARK") {
		t.Skip("set SMP3_DYNAMIC_CAPACITY_BENCHMARK=1")
	}
	t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "1")
	t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "1")
	t.Setenv("SMP3_BENCHMARK_PACER", "serializer")
	for _, tc := range []struct {
		name  string
		rates [2]int64
	}{
		{"100+100", [2]int64{100e6, 100e6}},
		{"50+100", [2]int64{50e6, 100e6}},
		{"50+150", [2]int64{50e6, 150e6}},
		{"50+200", [2]int64{50e6, 200e6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			elapsed, stats, err := runStreamAggregationSampleWithMode(128<<20, 2, 0, tc.rates, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}, [2]benchmarkFault{}, StreamSchedulerAggregation)
			if err != nil {
				t.Fatal(err)
			}
			if !stats.CapacityValid[1] {
				t.Fatalf("high-capacity leg never produced a valid sample: elapsed=%s stats=%+v", elapsed, stats)
			}
			wantShare := float64(tc.rates[1]) / float64(tc.rates[0]+tc.rates[1])
			gotShare := stats.CapacityWeight[1] / (stats.CapacityWeight[0] + stats.CapacityWeight[1])
			if diff := gotShare - wantShare; diff < -0.10 || diff > 0.10 {
				t.Fatalf("stationary entitlement did not converge: want=%.3f got=%.3f weights=%v stats=%+v", wantShare, gotShare, stats.CapacityWeight, stats)
			}
			// The virtual serializer can expose one or two timer-boundary repairs
			// at the end of a run. Bound that jitter to one chunk and two rescues;
			// anything larger indicates real stationary amplification.
			if stats.TxRetransmitBytesByLeg[0]+stats.TxRetransmitBytesByLeg[1] > uint64(2*64*1024) || stats.FrontierRescueAttempts > 2 {
				t.Fatalf("stationary run produced repair amplification: retry=%v rescue=%d stats=%+v", stats.TxRetransmitBytesByLeg, stats.FrontierRescueAttempts, stats)
			}
			t.Logf("stationary convergence elapsed=%s raw=%v smoothed=%v confidence=%v weights=%v share=%.3f acked=%v", elapsed, stats.CapacityRawBPS, stats.CapacitySmoothedBPS, stats.CapacityConfidence, stats.CapacityWeight, gotShare, stats.TxAckedUsefulByLeg)
		})
	}
}

func TestDynamicCapacityDemandLimitedControl(t *testing.T) {
	if testing.Short() || !envEnabled("SMP3_DYNAMIC_CAPACITY_BENCHMARK") {
		t.Skip("set SMP3_DYNAMIC_CAPACITY_BENCHMARK=1")
	}
	t.Setenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY", "1")
	t.Setenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY", "1")
	t.Setenv("SMP3_BENCHMARK_PACER", "serializer")
	// Sustain only 40 Mbps of application demand for several seconds. Both
	// 100 Mbps legs remain under-exercised, so a delivery rate of roughly
	// 20 Mbps per leg must not be learned as a new capacity ceiling.
	t.Setenv("SMP3_BENCHMARK_APP_RATE_BPS", "40000000")
	_, stats, err := runStreamAggregationSampleWithMode(32<<20, 2, 0, [2]int64{100e6, 100e6}, [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}, [2]benchmarkFault{}, StreamSchedulerAggregation)
	if err != nil {
		t.Fatal(err)
	}
	for id, weight := range stats.CapacityWeight {
		if weight < 90 || weight > 110 {
			t.Fatalf("demand-limited capacity collapsed leg %d: weight=%v stats=%+v", id, weight, stats)
		}
	}
	t.Logf("demand-limited raw=%v smoothed=%v confidence=%v valid=%v weights=%v", stats.CapacityRawBPS, stats.CapacitySmoothedBPS, stats.CapacityConfidence, stats.CapacityValid, stats.CapacityWeight)
}

func envEnabled(name string) bool { return benchmarkIntEnv(name, 0) != 0 }

package smp3core

import (
	"math"
	"testing"
	"time"
)

func TestDynamicCapacityWeightChangePreservesFutureEntitlement(t *testing.T) {
	c := &StreamEngine{cfg: StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{50, 200}, benchmarkEpochRebase: true}, legs: make(map[uint8]*streamLeg)}
	c.capacity = newStreamCapacityEstimator(c.cfg.BandwidthMbps, 1024)
	for id := uint8(0); id < 2; id++ {
		c.legs[id] = &streamLeg{id: id}
	}
	assign := func() uint8 {
		return c.assignBenchmark(&StreamTXRecord{payload: make([]byte, 1024)}, &benchmarkAssignment{}).id
	}
	// Accumulate enough historical service to expose a lifetime-score burst.
	for i := 0; i < 10000; i++ {
		assign()
	}
	c.capacity.mu.Lock()
	c.capacity.legs[1].estimate, c.capacity.legs[1].confidence = 80e6/8, 1
	c.capacity.mu.Unlock()
	var counts [2]int
	for i := 0; i < 256; i++ {
		counts[assign()]++
	}
	if math.Abs(float64(counts[0])/256-50.0/130) > .02 {
		t.Fatalf("step-down assignment includes historical debt: %v", counts)
	}
	c.capacity.mu.Lock()
	c.capacity.legs[1].estimate = 200e6 / 8
	c.capacity.mu.Unlock()
	counts = [2]int{}
	for i := 0; i < 256; i++ {
		counts[assign()]++
	}
	if math.Abs(float64(counts[0])/256-.2) > .02 {
		t.Fatalf("step-up assignment includes historical debt: %v", counts)
	}
}

func TestStreamCapacityDemandLimitedWindowFreezes(t *testing.T) {
	e := newStreamCapacityEstimator([]uint32{100, 200}, 1024)
	t0 := time.Unix(0, 0)
	e.admit(0, 4096, false, t0)
	e.ack(0, 4096, t0.Add(2*time.Second))
	_, smoothed, confidence, valid := e.snapshot()
	if valid[0] || confidence[0] != 0 || smoothed[0] != 100*1e6/8 {
		t.Fatalf("demand-limited sample changed estimate: smoothed=%v confidence=%v valid=%v", smoothed[0], confidence[0], valid[0])
	}
}

func TestStreamCapacityUnderDemandRecoveryProbe(t *testing.T) {
	e := newStreamCapacityEstimator([]uint32{50, 200}, 1024)
	t0 := time.Unix(0, 0)
	e.mu.Lock()
	e.legs[1].estimate = 80e6 / 8
	e.legs[1].confidence = 1
	e.legs[1].valid = true
	e.mu.Unlock()
	const sampleBytes = 16 << 20
	e.admit(1, sampleBytes, false, t0)
	e.ack(1, sampleBytes, t0)
	e.admit(1, sampleBytes, false, t0.Add(1100*time.Millisecond))
	e.ack(1, sampleBytes, t0.Add(1100*time.Millisecond))
	_, estimate, _, valid := e.snapshot()
	if !valid[1] || estimate[1] <= 80e6/8 {
		t.Fatalf("under-demand recovery probe did not reopen estimate: estimate=%v valid=%v", estimate, valid)
	}
}

func TestStreamCapacityUsefulACKUpdatesAndIdleFreezes(t *testing.T) {
	e := newStreamCapacityEstimator([]uint32{100, 200}, 1024)
	t0 := time.Unix(0, 0)
	e.admit(0, 4096, true, t0)
	e.ack(0, 4096, t0.Add(time.Second))
	e.ack(0, 4096, t0.Add(2*time.Second))
	raw, smoothed, confidence, valid := e.snapshot()
	if raw[0] <= 0 || !valid[0] || confidence[0] <= 0 || smoothed[0] <= 0 {
		t.Fatalf("valid sample missing: raw=%v smoothed=%v confidence=%v valid=%v", raw[0], smoothed[0], confidence[0], valid[0])
	}
	before := smoothed[0]
	e.ack(0, 0, t0.Add(10*time.Second))
	_, after, _, _ := e.snapshot()
	if after[0] != before {
		t.Fatalf("idle changed estimate: before=%v after=%v", before, after[0])
	}
}

func TestStreamCapacityDynamicModeIsExplicit(t *testing.T) {
	c := &StreamEngine{cfg: StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityFixed, BandwidthMbps: []uint32{100}}, legs: make(map[uint8]*streamLeg)}
	if got := c.effectiveSchedulerWeight(&streamLeg{id: 0}); got != 100 {
		t.Fatalf("fixed aggregation weight changed: %v", got)
	}
}

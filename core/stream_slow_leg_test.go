package smp3core

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlowLegLowRateSampling(t *testing.T) {
	for _, chunk := range []int{16 << 10, 64 << 10, 128 << 10} {
		for _, rate := range []float64{.1, .5, 1, 5} {
			e := newStreamCapacityEstimator([]uint32{100, 100}, chunk)
			start := time.Unix(100, 0)
			interval := time.Duration(float64(chunk) * 8 / (rate * 1e6) * float64(time.Second))
			for now := start; now.Before(start.Add(20 * time.Second)); now = now.Add(interval) {
				e.admit(1, uint64(chunk), true, now)
				e.ack(1, uint64(chunk), now.Add(interval))
			}
			_, _, _, valid := e.snapshot()
			if !valid[1] || e.weight(1, 100) > rate*2 {
				t.Errorf("chunk=%d rate=%g valid=%v effective=%g", chunk, rate, valid[1], e.weight(1, 100))
			}
		}
	}
}

func slowLegUnitEngine() *StreamEngine {
	c := &StreamEngine{cfg: StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, MaxInflightFrames: 128, MaxActiveLegs: DefaultMaxActiveStreamLegs, benchmarkEpochRebase: true}, maxActiveLegs: DefaultMaxActiveStreamLegs, txLedger: newStreamTXLedger(DefaultMaxActiveStreamLegs), legs: make(map[uint8]*streamLeg), admission: streamAdmission{legs: make([]streamAdmissionLeg, DefaultMaxActiveStreamLegs)}, assignedService: make([]uint64, DefaultMaxActiveStreamLegs), assignedRecords: make([]uint64, DefaultMaxActiveStreamLegs), ordinaryAssignedBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), legGeneration: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), txWrittenBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), txRetryWrittenBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), txSentBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), txAckedUseful: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), txRetransmitBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), rxUniqueBytes: make([]atomic.Uint64, DefaultMaxActiveStreamLegs), pendingDepth: make([]atomic.Int64, DefaultMaxActiveStreamLegs), assignedEpochOrigin: make([]float64, DefaultMaxActiveStreamLegs), assignedEpochWeight: make([]float64, DefaultMaxActiveStreamLegs), assignedEpochReady: make([]bool, DefaultMaxActiveStreamLegs)}
	for id := uint8(0); id < 2; id++ {
		c.legs[id] = &streamLeg{id: id}
	}
	return c
}

// startRecoveryProbeForTest models assignment, dequeue, entry to the carrier
// DATA Write, and a successful local write commit. Recovery starts only at the
// Write boundary; assignment time retains its separate bounded pending budget.
func startRecoveryProbeForTest(t *testing.T, c *StreamEngine, id uint8, at time.Time, size int) *StreamTXRecord {
	t.Helper()
	r := c.txLedger.Add(make([]byte, size), at)
	if !c.txLedger.assignOrdinary(r, id, at) {
		t.Fatal("recovery probe ordinary assignment failed")
	}
	c.ordinaryAssignedRecord(id, r, at, size)
	c.txLedger.MarkTransit(r, LegID(id), at)
	if !c.recoveryProbeWriteStarted(id, r, at) {
		t.Fatal("recovery probe rejected at DATA Write start")
	}
	c.txLedger.MarkAttemptSent(r, LegID(id), false, at)
	if !c.admission.legs[id].recovering {
		t.Fatal("recovery did not begin at DATA Write start")
	}
	return r
}

func TestSlowLegNoACKAndAmbiguousRecovery(t *testing.T) {
	for id := uint8(0); id < 2; id++ {
		c := slowLegUnitEngine()
		now := time.Unix(100, 0)
		r := c.txLedger.Add(make([]byte, 64<<10), now)
		c.txLedger.assignOrdinary(r, id, now)
		c.txLedger.MarkTransit(r, LegID(id), now)
		c.legs[id].writeStartedNs.Store(now.UnixNano())
		c.legs[1-id].perf.writeLatency = time.Millisecond
		c.legs[1-id].debugWriteCount.Store(3)
		for _, d := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
			c.protectFrontier(c.txLedger.FrontierCandidate(now.Add(d), time.Second), now.Add(d))
			if !c.ordinaryOwnerAllowed(id) {
				t.Fatal("short jitter restricted leg")
			}
		}
		c.protectFrontier(c.txLedger.FrontierCandidate(now.Add(3*time.Second), time.Second), now.Add(3*time.Second))
		if c.ordinaryEligible(id, 1, now.Add(10*time.Second)) || c.ordinaryOwnerAllowed(id) {
			t.Fatal("outstanding blackhole not restricted")
		}
		c.txLedger.MarkRescueTransit(r, LegID(1-id), now.Add(3*time.Second))
		ack := c.txLedger.ApplyACK(1, now.Add(3100*time.Millisecond))
		if ack.UniqueTimelyByLeg != [2]int{} || ack.SampleBytesByLeg != [2]uint64{} {
			t.Fatal("rescue ACK forged capacity/recovery evidence")
		}
		if c.txLedger.OrdinaryFrames() != [2]int{} {
			t.Fatal("rescue ACK did not retire single reservation")
		}
		if c.ordinaryEligible(id, 0, now.Add(4*time.Second)) {
			t.Fatal("cooldown skipped")
		}
		if !c.ordinaryEligible(id, 0, now.Add(9*time.Second)) {
			t.Fatal("probe never reopened")
		}
		startRecoveryProbeForTest(t, c, id, now.Add(9*time.Second), 64<<10)
		if c.ordinaryEligible(id, 1, now.Add(9*time.Second)) {
			t.Fatal("probe budget unbounded")
		}
		c.admissionACK(ack)
		if !c.admission.legs[id].recovering {
			t.Fatal("ambiguous ACK completed probation")
		}
		// Real ledger retirement supplies actual payload bytes as well as count.
		for step := 0; step < 128 && c.admission.legs[id].recovering; step++ {
			at := now.Add(10*time.Second + time.Duration(step)*10*time.Millisecond)
			record := c.txLedger.Add(make([]byte, 64<<10), at)
			c.txLedger.assignOrdinary(record, id, at)
			c.txLedger.MarkTransit(record, LegID(id), at)
			c.txLedger.MarkAttemptSent(record, LegID(id), false, at)
			c.admissionACK(c.txLedger.ApplyACK(record.Sequence()+1, at.Add(time.Millisecond)))
		}
		if c.admission.legs[id].recovering {
			t.Fatal("unique evidence failed to recover")
		}
	}
}

func TestSlowLegAdmissionMigrationAndACKRace(t *testing.T) {
	l := NewStreamTXLedger()
	now := time.Unix(100, 0)
	for round := 0; round < 100; round++ {
		r := l.Add(make([]byte, 1024), now)
		l.assignOrdinary(r, 0, now)
		var workers sync.WaitGroup
		workers.Add(3)
		go func() { defer workers.Done(); l.assignOrdinary(r, 1, now); l.MarkTransit(r, 1, now) }()
		go func() { defer workers.Done(); l.MarkRescueTransit(r, 0, now); l.ClearAttempt(r, 0, true) }()
		go func() { defer workers.Done(); l.ApplyACK(r.Sequence()+1, now.Add(time.Second)) }()
		workers.Wait()
		if l.OrdinaryFrames() != [2]int{} || l.IsOutstanding(r) {
			t.Fatal("reservation leaked after race")
		}
		if l.assignOrdinary(r, 0, now) {
			t.Fatal("stale pending changed retired record")
		}
	}
}

func TestSlowLegHealthyFrontierNotBlamedForACKDelay(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Unix(100, 0)
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	c.txLedger.assignOrdinary(r, 0, now)
	c.txLedger.MarkTransit(r, 0, now)
	c.txLedger.MarkAttemptSent(r, 0, false, now)
	for _, leg := range c.legs {
		leg.perf.writeLatency = time.Millisecond
		leg.debugWriteCount.Store(100)
	}
	c.protectFrontier(c.txLedger.FrontierCandidate(now.Add(10*time.Second), time.Second), now.Add(10*time.Second))
	if !c.ordinaryOwnerAllowed(0) {
		t.Fatal("ACK delay alone restricted fast writer")
	}
}

func TestSlowLegSharedProviderEpochAndReset(t *testing.T) {
	registry := NewCarrierCapacityRegistry()
	provider := registry.Provider([2]string{"owner|out|a", "owner|out|b"}, []uint32{100, 100}, 64<<10)
	defer provider.(interface{ Close() }).Close()
	c := slowLegUnitEngine()
	// This test measures normalized scheduler entitlement in isolation from
	// slow-leg admission. A nil ledger preserves that historical scope while
	// the explicit two-leg bound keeps the fixture aligned with its provider.
	c.maxActiveLegs = 2
	c.cfg.MaxActiveLegs = 2
	c.txLedger = nil
	c.capacityProvider = provider
	assign := func() uint8 {
		return c.assignBenchmark(&StreamTXRecord{payload: make([]byte, 1024)}, &benchmarkAssignment{}).id
	}
	for i := 0; i < 10000; i++ {
		assign()
	}
	p := provider.(*carrierCapacityProvider)
	p.states[1].mu.Lock()
	p.states[1].legs[0].estimate = 1e6 / 8
	p.states[1].legs[0].confidence = 1
	p.states[1].legs[0].samples = 2
	p.states[1].mu.Unlock()
	provider.Reset(1, time.Unix(100, 0))
	if provider.Weight(1, 100) > 1.1 {
		t.Fatal("reconnect reset low knowledge")
	}
	var count int
	for i := 0; i < 1000; i++ {
		if assign() == 1 {
			count++
		}
	}
	if math.Abs(float64(count)/1000-1.0/101) > .01 {
		t.Fatalf("provider weight change created historical debt: count=%d", count)
	}
}

func TestSlowLegIdleGapAndLowPriorRecovery(t *testing.T) {
	e := newStreamCapacityEstimator([]uint32{100, 1}, 64<<10)
	now := time.Unix(100, 0)
	e.admit(0, 64<<10, false, now)
	e.ack(0, 64<<10, now.Add(time.Second))
	e.admit(0, 4<<20, true, now.Add(time.Minute))
	e.ack(0, 4<<20, now.Add(time.Minute+time.Second))
	raw, _, _, valid := e.snapshot()
	if !valid[0] || raw[0] < 30e6/8 {
		t.Fatal("idle time contaminated resumed service sample")
	}
	c := slowLegUnitEngine()
	c.cfg.BandwidthMbps = []uint32{100, 1}
	c.capacityProvider = e
	c.restrictOrdinary(1, now, "test_low_prior")
	startRecoveryProbeForTest(t, c, 1, now.Add(6*time.Second), 64<<10)
	for step := 0; step < 128 && c.admission.legs[1].recovering; step++ {
		at := now.Add(7*time.Second + time.Duration(step)*10*time.Millisecond)
		r := c.txLedger.Add(make([]byte, 64<<10), at)
		c.txLedger.assignOrdinary(r, 1, at)
		c.txLedger.MarkTransit(r, 1, at)
		c.txLedger.MarkAttemptSent(r, 1, false, at)
		c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, at.Add(time.Millisecond)))
	}
	if c.admission.legs[1].recovering || e.Weight(1, 1) < 30 {
		t.Fatalf("low prior capped confirmed recovery: weight=%g", e.Weight(1, 1))
	}
}

func TestSlowLegBudgetReadinessDoesNotRebaseEpoch(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.assignedEpochMask = 3
	c.assignedEpochWeight = []float64{100, 100}
	c.assignedEpochReady[0], c.assignedEpochReady[1] = true, true
	for i := 0; i < c.ordinaryBudget(0); i++ {
		r := c.txLedger.Add(make([]byte, 64<<10), now)
		c.txLedger.assignOrdinary(r, 0, now)
		c.assignedService[0] += 64 << 10
	}
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	if leg := c.assignBenchmark(r, &benchmarkAssignment{}); leg == nil || leg.id != 1 {
		t.Fatal("full ordinary budget did not use ready alternate")
	}
	if c.assignedEpochMask != 3 || c.assignedEpochOrigin[0] != 0 || c.assignedEpochOrigin[1] != 0 {
		t.Fatal("temporary budget readiness created a membership epoch")
	}
}

func TestSlowLegRescueACKBeforeWriteCommitRetiresUsefulBytes(t *testing.T) {
	l := NewStreamTXLedger()
	now := time.Now()
	r := l.Add(make([]byte, 64<<10), now)
	l.assignOrdinary(r, 1, now)
	l.MarkRescueTransit(r, 0, now)
	result := l.ApplyACK(1, now.Add(50*time.Millisecond))
	if result.Released != 1 || result.AckedBytesByLeg != [2]uint64{0, 64 << 10} || result.SampleBytesByLeg != [2]uint64{} || result.UniqueTimelyByLeg != [2]int{} {
		t.Fatalf("rescue race accounting/attribution failed: %+v", result)
	}
	bytes, pending := l.OrdinaryAdmissionBytes()
	if bytes != [2]uint64{} || pending != [2]uint64{} {
		t.Fatal("pending rescue retirement leaked bytes")
	}
}

func TestSlowLegBoundedFrontierBatchAndSmallChunks(t *testing.T) {
	l := NewStreamTXLedger()
	now := time.Now()
	var records []*StreamTXRecord
	for i := 0; i < 20; i++ {
		r := l.Add(make([]byte, 16<<10), now)
		l.assignOrdinary(r, 1, now)
		records = append(records, r)
	}
	batch := l.RestrictedFrontierBatch(records[0], 1, now.Add(3*time.Second), now.Add(2*time.Second), time.Second, 256<<10)
	if len(batch) != 8 {
		t.Fatalf("batch budget wrong: %d", len(batch))
	}
	for i, r := range batch {
		if r.Sequence() != uint64(i) {
			t.Fatal("repair did not preserve frontier order")
		}
	}
	l.assignOrdinary(records[0], 0, now)
	if _, ok := l.MarkRestrictedRescueTransit(records[0], 0, 1, now); ok {
		t.Fatal("stale owner authorized rescue")
	}
	if next := l.RestrictedFrontierBatch(records[0], 1, now.Add(3*time.Second), now.Add(2*time.Second), time.Second, 256<<10); len(next) != 0 {
		t.Fatal("migrated frontier authorized old-owner batch")
	}
	c := slowLegUnitEngine()
	c.cfg.ChunkSize = 16 << 10
	c.cfg.MaxInflightFrames = 1024
	if c.ordinaryBudget(0)*c.cfg.ChunkSize < 1250000 {
		t.Fatal("small-frame budget cut a 100Mbps/100ms healthy baseline")
	}
}

func TestSlowLegRetiredUnassignedPendingDoesNotStallPlanner(t *testing.T) {
	c := slowLegUnitEngine()
	c.done = make(chan struct{})
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 1024), now)
	c.txLedger.MarkRescueTransit(r, 1, now)
	result := c.txLedger.ApplyACK(1, now.Add(time.Millisecond))
	if result.RetiredBytes != 1024 || result.Released != 1 {
		t.Fatal("unattributed retirement lost unique bytes")
	}
	done := make(chan error, 1)
	go func() { done <- c.enqueueAssignedDecoupled(r, -1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(c.done)
		t.Fatal("retired unassigned record pinned planner")
	}
}

func TestSlowLegProviderCloseWithdrawsOnlyOwnDemand(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	keys := [2]string{"test|tx|a", "test|tx|b"}
	now := time.Now()
	a := r.Provider(keys, []uint32{100, 100}, 1024).(*carrierCapacityProvider)
	b := r.Provider(keys, []uint32{100, 100}, 1024).(*carrierCapacityProvider)
	a.ObserveActivationDemand(4096, 0, now)
	b.ObserveActivationDemand(8192, 0, now)
	a.Close()
	a.Close()
	a.ObserveActivationDemand(999999, 999999, now)
	a.Admit(0, 1<<20, true, now)
	a.Ack(0, 1<<20, now.Add(time.Second))
	_, backlog := b.ActivationDemand(now)
	if backlog != 8192 {
		t.Fatalf("closed provider changed another stream demand: %d", backlog)
	}
	_, _, _, valid, _, _ := b.Telemetry()
	if valid[0] {
		t.Fatal("closed provider mutated shared sample")
	}
	b.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.entries {
		if entry.refs != 0 || entry.backlog != 0 {
			t.Fatal("provider close leaked references/demand")
		}
	}
}

func TestSlowLegShortReadsAndWarmCooldownWake(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	for i := 0; i < 64; i++ {
		r := c.txLedger.Add(make([]byte, 8<<10), now)
		c.txLedger.assignOrdinary(r, 0, now)
	}
	if !c.ordinaryEligible(0, 64, now) {
		t.Fatal("configured chunk treated short reads as full frames")
	}
	c.txLedger.ApplyACK(64, now)
	for id := range c.decoupledWake {
		c.decoupledWake[id] = make(chan struct{}, 1)
	}
	c.restrictOrdinary(0, now, "warm_low_capacity")
	c.restrictOrdinary(1, now, "warm_low_capacity")
	for {
		select {
		case <-c.decoupledWake[2]:
		default:
			goto drained
		}
	}
drained:
	c.maintainAdmission(now.Add(6 * time.Second))
	select {
	case <-c.decoupledWake[2]:
	default:
		t.Fatal("zero-ACK warm cooldown never woke planner")
	}
}

func TestSlowLegRescueDoesNotChangeGuardOwnerOrProveRejoin(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	c.txLedger.assignOrdinary(r, 1, now)
	c.txLedger.MarkTransit(r, 1, now)
	c.txLedger.MarkAttemptSent(r, 1, false, now)
	c.txLedger.MarkRescueTransit(r, 0, now)
	c.txLedger.MarkAttemptSent(r, 0, true, now)
	c.legs[1].writeStartedNs.Store(now.UnixNano())
	c.legs[0].perf.writeLatency = time.Millisecond
	c.legs[0].debugWriteCount.Store(3)
	later := now.Add(3 * time.Second)
	c.protectFrontier(c.txLedger.FrontierCandidate(later, time.Second), later)
	if c.ordinaryOwnerAllowed(1) || !c.ordinaryOwnerAllowed(0) {
		t.Fatal("rescue completion blamed healthy attempt instead of ordinary owner")
	}
	l := NewStreamTXLedger()
	old := l.Add(make([]byte, 1024), now)
	l.assignOrdinary(old, 1, now)
	l.MarkTransit(old, 1, now)
	l.MarkAttemptSent(old, 1, false, now)
	l.InvalidateLeg(1)
	result := l.ApplyACK(1, now.Add(10*time.Millisecond))
	if result.UniqueTimelyByLeg != [2]int{} || result.SampleBytesByLeg != [2]uint64{} {
		t.Fatal("old-generation catch-up ACK proved rejoin capacity")
	}
}

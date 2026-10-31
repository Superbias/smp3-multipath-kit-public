package smp3core

import (
	"testing"
	"time"
)

// Replay valid unique ACK callbacks while the earlier healthy pipeline delayed
// their exposure. The real Native counterexample is retained separately.
func TestClosureReachabilityIsNotRestoredService(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	send := func(id uint8, size int, at time.Time) *StreamTXRecord {
		r := c.txLedger.Add(make([]byte, size), at)
		c.txLedger.assignOrdinary(r, id, at)
		c.txLedger.MarkTransit(r, LegID(id), at)
		c.txLedger.MarkAttemptSent(r, LegID(id), false, at)
		return r
	}
	for i := 0; i < 52; i++ {
		at := now.Add(time.Duration(i) * 5 * time.Millisecond)
		r := send(0, 64<<10, at)
		c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, at.Add(time.Millisecond)))
	}
	now = now.Add(time.Second)
	c.restrictOrdinary(1, now, "test")
	now = now.Add(6 * time.Second)
	startRecoveryProbeForTest(t, c, 1, now, 64<<10)
	for step := 0; step < 100 && c.admission.legs[1].limit < 16; step++ {
		now = now.Add(5 * time.Millisecond)
		r := send(1, 64<<10, now)
		c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, now.Add(time.Millisecond)))
	}
	if !c.admission.legs[1].recovering {
		t.Fatal("fixture ramp completed too early")
	}
	head := c.txLedger.ProgressSnapshot().AckedNext
	for i := 0; i < 16; i++ {
		send(1, 32<<10, now)
	}
	first := now.Add(300 * time.Millisecond)
	c.admissionACK(c.txLedger.ApplyACK(head+1, first))
	send(1, 32<<10, first)
	c.admissionACK(c.txLedger.ApplyACK(head+2, first.Add(262*time.Millisecond)))
	if !c.admission.legs[1].restricted {
		t.Fatal("uniquely reachable ~1Mbps probe was allowed to reopen against ~100Mbps alternate")
	}
}

func TestClosureGraduationDoesNotRepayProbationDebt(t *testing.T) {
	c := slowLegUnitEngine()
	c.assignedService = []uint64{100 << 20, 1 << 20}
	c.assignedEpochMask = 3
	c.assignedEpochWeight = []float64{100, 100}
	now := time.Now()
	c.admission.legs[1].recovering = true
	c.admission.legs[1].limit = 128
	c.admission.legs[1].byteLimit = 4 << 20
	c.admission.legs[1].seedMbps = 100
	c.admissionACK(StreamTXACKResult{AckedNext: 1, UniqueTimelyByLeg: [2]int{0, 1}, UniqueBytesByLeg: [2]uint64{0, 64 << 10}, LastACKProgress: now})
	if c.admission.legs[1].recovering {
		t.Fatal("fixture did not graduate")
	}
	var count [2]int
	for i := 0; i < 20; i++ {
		r := c.txLedger.Add(make([]byte, 32<<10), now)
		leg := c.assignBenchmark(r, &benchmarkAssignment{})
		if leg == nil {
			t.Fatal("fixture no owner")
		}
		count[leg.id]++
	}
	if count[0] < 8 || count[1] < 8 {
		t.Fatalf("probation's historic deficit caused a catch-up burst: %v", count)
	}
}

func TestClosureDeliveredServiceOverridesLocalWriteStall(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.legs[1].debugLastWriteDurationNs.Store(int64(80 * time.Millisecond))
	c.legs[0].perf.observeWrite(32768, time.Millisecond)
	c.legs[0].debugWriteCount.Store(4)
	c.admission.legs[1].recovering = true
	c.admission.legs[1].since = now.Add(-time.Second)
	c.admission.legs[1].limit = c.cfg.MaxInflightFrames
	c.admission.legs[1].byteLimit = 4 << 20
	c.admission.legs[1].openingRate = 12.5e6
	c.admission.legs[1].probeWindow = now.Add(-300 * time.Millisecond)
	c.admission.legs[1].probeBytes = 2 << 20
	r := c.txLedger.Add(make([]byte, 32768), now)
	c.txLedger.assignOrdinary(r, 1, now)
	c.txLedger.MarkTransit(r, 1, now)
	c.txLedger.MarkAttemptSent(r, 1, false, now)
	c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, now.Add(time.Millisecond)))
	if c.admission.legs[1].restricted || c.admission.legs[1].recovering {
		t.Fatalf("clean delivered service rejected by a local buffered Write stall: %+v", c.admission.legs[1])
	}
}

func TestClosureProbeOpeningUsesFreshProbeClock(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	s := &c.admission.legs[1]
	s.recovering = true
	s.since = now
	s.openingRate = 3.125e6
	s.serviceAt = now.Add(-time.Minute)
	if b := c.ordinaryByteBudget(1); b > 1_110_000 {
		t.Fatalf("fresh per-session probe hypothesis ignored using stale old service time: %d", b)
	}
}
func TestClosureGraduationRestartsConfoundedHealthyMeter(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.admission.legs[0].serviceRate = 1e6
	c.admission.legs[0].serviceAt = now
	c.admission.legs[0].feedback = 40 * time.Millisecond
	c.admission.legs[0].feedbackAt = now
	s := &c.admission.legs[1]
	s.recovering = true
	s.since = now.Add(-time.Second)
	s.limit = c.cfg.MaxInflightFrames
	s.byteLimit = 4 << 20
	s.openingRate = 3.125e6
	s.probeWindow = now.Add(-300 * time.Millisecond)
	s.probeBytes = 1 << 20
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	c.txLedger.assignOrdinary(r, 1, now)
	c.txLedger.MarkTransit(r, 1, now)
	c.txLedger.MarkAttemptSent(r, 1, false, now)
	c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, now.Add(time.Millisecond)))
	if c.admission.legs[1].recovering {
		t.Fatal("fixture did not graduate")
	}
	if b := c.ordinaryByteBudget(0); b < 781250 {
		t.Fatalf("normal but HOL-confounded healthy meter kept shrinking its pipeline after peer graduation: %d", b)
	}
}

func TestClosureOldCohortCannotRetrainGraduatedService(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	s := &c.admission.legs[0]
	s.since = now
	s.openingRate = 3.125e6
	old := now.Add(-time.Second)
	r := c.txLedger.Add(make([]byte, 64<<10), old)
	c.txLedger.assignOrdinary(r, 0, old)
	c.txLedger.MarkTransit(r, 0, old)
	c.txLedger.MarkAttemptSent(r, 0, false, old)
	c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, now.Add(time.Millisecond)))
	if !s.serviceWindow.IsZero() {
		t.Fatal("pre-graduation records entered the new normal service cohort")
	}
	fresh := now.Add(time.Second)
	r = c.txLedger.Add(make([]byte, 64<<10), fresh)
	c.txLedger.assignOrdinary(r, 0, fresh)
	c.txLedger.MarkTransit(r, 0, fresh)
	c.txLedger.MarkAttemptSent(r, 0, false, fresh)
	c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, fresh.Add(time.Millisecond)))
	if s.serviceWindow.IsZero() {
		t.Fatal("fresh normal records did not start service sampling")
	}
}

func TestClosureProbeDiscardsStaleNormalMeter(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.admission.legs[0].serviceRate = 3.125e6
	c.admission.legs[0].serviceAt = now
	c.admission.legs[1].serviceRate = 1e6
	c.admission.legs[1].serviceAt = now.Add(-time.Minute)
	c.restrictOrdinary(1, now, "fixture_restriction")
	c.ordinaryAssigned(1, now.Add(time.Millisecond), 32<<10)
	if b := c.ordinaryByteBudget(1); b > 1_110_000 {
		t.Fatalf("stale normal service bypassed fresh bounded opening hypothesis: budget=%d", b)
	}
}

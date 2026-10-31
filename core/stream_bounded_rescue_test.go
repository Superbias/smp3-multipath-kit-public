package smp3core

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type boundedRescueFakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newBoundedRescueFakeClock() *boundedRescueFakeClock {
	return &boundedRescueFakeClock{t: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
}
func (c *boundedRescueFakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *boundedRescueFakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newBoundedRescueTestBudget(t *testing.T, bp uint16, burst uint64, hook func(StreamRescueEvent)) (*streamBoundedRescueBudget, *boundedRescueFakeClock) {
	t.Helper()
	clock := newBoundedRescueFakeClock()
	budget, err := newStreamBoundedRescueBudget(StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: bp, BurstBytes: burst,
	}, 64<<10, clock.Now, hook)
	if err != nil {
		t.Fatal(err)
	}
	return budget, clock
}

func boundedHealthyEvidence(seq, epoch uint64, target uint8) StreamRescueEvidence {
	return StreamRescueEvidence{
		Sequence: seq, OwnerLeg: 0, RecordOwnerLeg: 0, TargetLeg: target,
		TargetAvailable:  true,
		OriginGeneration: 7, OwnerGeneration: 7, RecoveryEpoch: epoch,
		Cause: StreamRescueCauseFrontier, IsData: true, IsRescue: true,
		Outstanding: true, FrontierOverdue: true, OriginalWriteObserved: true,
		OwnerLive: true, OwnerRestricted: true, GenerationMatchesOrigin: true, RecoveryEpochCurrent: true,
	}
}

func newBoundedRescueFactorTestBudget(t *testing.T, mode boundedRescueFactorMode, bp uint16, hook func(StreamRescueEvent)) (*streamBoundedRescueBudget, *boundedRescueFakeClock) {
	t.Helper()
	clock := newBoundedRescueFakeClock()
	budget, err := newStreamBoundedRescueBudget(StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: bp, BurstBytes: 0, factorMode: mode,
	}, 64<<10, clock.Now, hook)
	if err != nil {
		t.Fatal(err)
	}
	return budget, clock
}

func TestPhase26CandidateIdentityEligibilityAndACKLink(t *testing.T) {
	var events []StreamRescueEvent
	b, clock := newBoundedRescueFactorTestBudget(t, boundedRescueFactorFull, 100, func(event StreamRescueEvent) {
		events = append(events, event)
	})
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	evidence := boundedHealthyEvidence(1, 1, 1)
	first := b.Reserve(evidence, 512, clock.Now())
	if !first.Admitted || first.CandidateID == 0 {
		t.Fatalf("first candidate not admitted with stable identity: %+v", first)
	}
	if !b.BeginWrite(first.ReservationID, clock.Now()) {
		t.Fatal("first candidate did not reach Carrier Write")
	}
	if err := b.Settle(first.ReservationID, 512, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	second := b.Reserve(evidence, 512, clock.Now())
	if second.Admitted || second.Reason != "per_record_epoch_limit" {
		t.Fatalf("second same-epoch candidate was not denied by the actual per-record gate: %+v", second)
	}
	if len(events) != 4 {
		t.Fatalf("events=%d, want reserve/start/settle/reject; %+v", len(events), events)
	}
	if events[0].CandidateID != first.CandidateID || events[1].CandidateID != first.CandidateID || events[2].CandidateID != first.CandidateID {
		t.Fatalf("candidate identity changed over reserve/write/settle: %+v", events[:3])
	}
	rejected := events[3]
	if rejected.CandidateID != second.CandidateID || rejected.PolicyVariant != "B1-F" ||
		!rejected.ClassificationEligible || rejected.PerRecordEligible || rejected.RollingBudgetEligible ||
		!rejected.PerRecordEnabled || !rejected.RollingBudgetEnabled || !rejected.TargetLegAvailable ||
		rejected.FinalDecision != "rejected" || rejected.FinalDecisionReason != "per_record_epoch_limit" {
		t.Fatalf("independent C/R/W snapshot or actual decision mismatch: %+v", rejected)
	}
	b.AckThrough(2, clock.Now())
	ackEvents := 0
	for _, event := range events {
		if event.Kind == "candidate_ack_outcome" {
			ackEvents++
			if event.Evidence.Sequence != 1 || event.AckOutcome != "acked_after_candidate" || event.FinalDecision != "data_acked" {
				t.Fatalf("invalid ACK outcome linkage: %+v", event)
			}
		}
	}
	if ackEvents != 2 {
		t.Fatalf("ACK outcome links=%d, want 2 candidate links; %+v", ackEvents, events)
	}
	for i, event := range events {
		if !event.MonotonicTimeValid || event.MonotonicElapsedNS < 0 {
			t.Fatalf("event %d has no serializable monotonic timestamp: %+v", i, event)
		}
		if i > 0 && event.MonotonicElapsedNS < events[i-1].MonotonicElapsedNS {
			t.Fatalf("event %d monotonic timestamp moved backwards: prev=%d current=%d", i, events[i-1].MonotonicElapsedNS, event.MonotonicElapsedNS)
		}
	}
}

func TestPhase26B0CandidateACKLinkIsBoundedAndObserverOnly(t *testing.T) {
	var events []StreamRescueEvent
	c := &StreamEngine{cfg: StreamConfig{rescueEventHook: func(event StreamRescueEvent) {
		events = append(events, event)
	}}, rescueObservationStart: time.Now()}
	class := StreamRescueHealthyHOL
	c.linkB0RescueCandidateToACK(4, 19, class, time.Unix(10, 0))
	if c.rescueCandidateAckLinks[4][0].candidateID != 19 {
		t.Fatalf("candidate link was not stored: %+v", c.rescueCandidateAckLinks)
	}
	c.emitB0RescueCandidateACKs(5, time.Unix(11, 0))
	if len(c.rescueCandidateAckLinks) != 0 || len(events) != 1 ||
		events[0].Kind != "candidate_ack_outcome" || events[0].CandidateID != 19 ||
		events[0].PolicyVariant != "B0" || events[0].Evidence.Sequence != 4 ||
		events[0].AckOutcome != "acked_after_candidate" || events[0].EligibilitySnapshot || !events[0].MonotonicTimeValid || events[0].MonotonicElapsedNS < 0 {
		t.Fatalf("B0 ACK link event mismatch: events=%+v links=%+v", events, c.rescueCandidateAckLinks)
	}

	c.txAckedNext.Store(9)
	c.linkB0RescueCandidateToACK(8, 20, class, time.Unix(12, 0))
	if len(events) != 2 || events[1].CandidateID != 20 || events[1].AckOutcome != "acked_before_link" {
		t.Fatalf("late candidate link lost an already observed ACK: %+v", events)
	}
}

func TestPhase26FactorIsolationCThenRThenW(t *testing.T) {
	makeScenario := func(mode boundedRescueFactorMode, repeat bool) (first, second boundedRescueAdmission, events []StreamRescueEvent) {
		b, clock := newBoundedRescueFactorTestBudget(t, mode, 300, func(event StreamRescueEvent) {
			events = append(events, event)
		})
		addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
		evidence := boundedHealthyEvidence(1, 1, 1)
		first = b.Reserve(evidence, 512, clock.Now())
		if first.Admitted {
			if !b.BeginWrite(first.ReservationID, clock.Now()) {
				t.Fatal("admitted first attempt did not start")
			}
			if err := b.Settle(first.ReservationID, 512, nil, clock.Now()); err != nil {
				t.Fatal(err)
			}
		}
		if repeat {
			second = b.Reserve(evidence, 512, clock.Now())
			if second.Admitted {
				if !b.BeginWrite(second.ReservationID, clock.Now()) {
					t.Fatal("admitted repeated attempt did not start")
				}
				if err := b.Settle(second.ReservationID, 512, nil, clock.Now()); err != nil {
					t.Fatal(err)
				}
			}
		}
		return
	}

	c, cSecond, cEvents := makeScenario(boundedRescueFactorCOnly, true)
	cr, crSecond, crEvents := makeScenario(boundedRescueFactorCR, true)
	if !c.Admitted || !cSecond.Admitted || !cr.Admitted || crSecond.Admitted || crSecond.Reason != "per_record_epoch_limit" {
		t.Fatalf("C/R incremental effect mismatch: C=%+v second=%+v CR=%+v second=%+v", c, cSecond, cr, crSecond)
	}
	if cEvents[0].PolicyVariant != "B1-C" || cEvents[0].PerRecordEnabled || cEvents[0].RollingBudgetEnabled ||
		crEvents[0].PolicyVariant != "B1-CR" || !crEvents[0].PerRecordEnabled || crEvents[0].RollingBudgetEnabled {
		t.Fatalf("C/R test-only variant identity mismatch: C=%+v CR=%+v", cEvents[0], crEvents[0])
	}

	crBudget, crClock := newBoundedRescueFactorTestBudget(t, boundedRescueFactorCR, 300, nil)
	fBudget, fClock := newBoundedRescueFactorTestBudget(t, boundedRescueFactorFull, 300, nil)
	addBoundedRescueOriginal(t, crBudget, 1, 65_536, crClock.Now())
	addBoundedRescueOriginal(t, fBudget, 1, 65_536, fClock.Now())
	crAdmit := crBudget.Reserve(boundedHealthyEvidence(1, 1, 1), 4_096, crClock.Now())
	fReject := fBudget.Reserve(boundedHealthyEvidence(1, 1, 1), 4_096, fClock.Now())
	if !crAdmit.Admitted || fReject.Admitted || fReject.Reason != "rolling_budget_exhausted" {
		t.Fatalf("W incremental effect mismatch: CR=%+v F=%+v", crAdmit, fReject)
	}
}

func TestPhase26ClassificationRejectReplayKeepsRAndWEligible(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*StreamRescueEvidence)
	}{
		{"not_overdue", func(e *StreamRescueEvidence) { e.FrontierOverdue = false }},
		{"stale_owner_generation", func(e *StreamRescueEvidence) { e.GenerationMatchesOrigin = false }},
		{"no_original_write", func(e *StreamRescueEvidence) { e.OriginalWriteObserved = false }},
		{"wrong_record_owner", func(e *StreamRescueEvidence) { e.RecordOwnerLeg = 1 }},
		{"stale_recovery_epoch", func(e *StreamRescueEvidence) { e.RecoveryEpochCurrent = false }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			var events []StreamRescueEvent
			b, clock := newBoundedRescueTestBudget(t, 300, 0, func(event StreamRescueEvent) {
				events = append(events, event)
			})
			addBoundedRescueOriginal(t, b, 1, 10<<20, clock.Now())
			evidence := boundedHealthyEvidence(1, 1, 1)
			tc.mutate(&evidence)
			decision := b.Reserve(evidence, 64<<10, clock.Now())
			if decision.Admitted || decision.Reason != "classification_not_healthy_hol" || len(events) != 1 {
				t.Fatalf("classification short-circuit mismatch: decision=%+v events=%+v", decision, events)
			}
			event := events[0]
			if !event.EligibilitySnapshot || !event.ClassificationEnabled || event.ClassificationEligible ||
				!event.PerRecordEnabled || !event.PerRecordEligible || !event.RollingBudgetEnabled || !event.RollingBudgetEligible ||
				event.FinalDecision != "rejected" || event.FinalDecisionReason != "classification_not_healthy_hol" {
				t.Fatalf("independent C/R/W facts were hidden by the first actual rejection: %+v", event)
			}
			stats := b.Snapshot(clock.Now())
			if stats.UnclassifiedRejected != 1 || stats.PerRecordRejected != 0 || stats.BudgetRejected != 0 {
				t.Fatalf("classification rejection altered later gate counters: %+v", stats)
			}
		})
	}
}

func TestPhase26ExhaustedThreeHundredBPBudgetDoesNotBlockOwnerFailureRetry(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 300, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	quota := boundedRescueRatio(65_536, 300)
	first := b.Reserve(boundedHealthyEvidence(1, 1, 1), quota, clock.Now())
	if !first.Admitted || first.Reason != "budget_reserved" {
		t.Fatalf("healthy Rescue could not reserve the exact 300 bp allowance: %+v quota=%d", first, quota)
	}
	if !b.BeginWrite(first.ReservationID, clock.Now()) {
		t.Fatal("budgeted Carrier attempt did not start")
	}
	if err := b.Settle(first.ReservationID, quota, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	addBoundedRescueOriginal(t, b, 2, 1, clock.Now())
	budgetAfterCharge := b.Snapshot(clock.Now())
	if budgetAfterCharge.WindowBudgetDebtBytes != 0 ||
		budgetAfterCharge.WindowCarrierAcceptedBytes != quota ||
		budgetAfterCharge.WindowBaseQuotaBytes != quota {
		t.Fatalf("test setup did not consume the exact rolling allowance: %+v quota=%d", budgetAfterCharge, quota)
	}
	failure := boundedHealthyEvidence(2, 2, 1)
	failure.OwnerInvalidated = true // positive ledger invalidation; not restriction alone
	retry := b.Reserve(failure, 64<<10, clock.Now())
	if !retry.Admitted || !retry.Exempt || retry.Reason != "positive_owner_invalidation" {
		t.Fatalf("necessary failure recovery was blocked after healthy budget exhaustion: %+v", retry)
	}
	stats := b.Snapshot(clock.Now())
	if stats.FailureRecoveryExempted != 1 || stats.BudgetRejected != 0 || stats.ReservedBytes != 0 || stats.OutstandingReservations != 0 {
		t.Fatalf("failure exemption consumed healthy quota or leaked a reservation: %+v", stats)
	}
}

func TestPhase26ThreeHundredBPRollingBudgetSyntheticSchedulerPathDiagnostic(t *testing.T) {
	clock := newBoundedRescueFakeClock()
	var events []StreamRescueEvent
	c := slowLegUnitEngine()
	c.cfg.boundedRescueClock = clock.Now
	c.cfg.RetransmitTimeout = time.Millisecond
	c.cfg.rescueEventHook = func(event StreamRescueEvent) { events = append(events, event) }
	for id := uint8(0); id < 2; id++ {
		leg := c.legs[id]
		leg.generation = 7
		leg.send = make(chan txSendAttempt, 2)
		leg.rescue = make(chan txSendAttempt, 2)
		leg.done = make(chan struct{})
		c.legGeneration[id].Store(7)
	}
	now := clock.Now()
	recordAt := now.Add(-time.Second)
	c.admission.legs[0].restricted = true
	c.admission.legs[0].since = now.Add(-500 * time.Millisecond)
	c.admission.legs[0].recoveryEpoch = 1
	manager, err := newStreamBoundedRescueBudget(StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: 300, BurstBytes: 0,
	}, 64<<10, clock.Now, c.cfg.rescueEventHook)
	if err != nil {
		t.Fatal(err)
	}
	c.boundedRescue = manager
	record := c.txLedger.Add(make([]byte, 64<<10), recordAt)
	if !c.txLedger.assignOrdinary(record, 0, recordAt) {
		t.Fatal("owner assignment failed")
	}
	c.txLedger.setOrigin(record, 7, recordAt)
	if !c.txLedger.MarkTransit(record, 0, recordAt) || !c.txLedger.MarkAttemptSent(record, 0, false, recordAt).Applied {
		t.Fatal("original DATA setup failed")
	}
	if !manager.RecordOriginal(record.Sequence(), uint64(len(record.Payload())), now) {
		t.Fatal("business denominator setup failed")
	}

	// Invoke the actual Core frontier planner. It discovers and classifies the
	// overdue ledger record, then executes Reserve through the ordinary B1 path.
	frontier := c.txLedger.FrontierCandidate(time.Now(), c.cfg.RetransmitTimeout)
	plan := DecideStreamTXFrontierRepair(frontier, StreamLegAvailability{true, true})
	c.scheduleFrontierRescue()
	var rejected *StreamRescueEvent
	for i := range events {
		if events[i].Kind == "rescue_rejected" && events[i].Reason == "rolling_budget_exhausted" {
			rejected = &events[i]
			break
		}
	}
	if rejected == nil {
		t.Fatalf("natural Core scheduler did not reach a 300 bp rejection: frontier={exists:%v overdue:%v owner:%d}, plan=%+v events=%+v stats=%+v", frontier.exists, frontier.overdue, frontier.owner, plan, events, manager.Snapshot(clock.Now()))
	}
	if rejected.CandidateID == 0 || rejected.Class != StreamRescueHealthyHOL ||
		!rejected.EligibilitySnapshot || !rejected.ClassificationEnabled ||
		!rejected.ClassificationEligible || !rejected.PerRecordEligible || rejected.RollingBudgetEligible ||
		!rejected.RollingBudgetEnabled || !rejected.TargetLegAvailable || rejected.FinalDecision != "rejected" {
		t.Fatalf("positive control did not isolate W after C/R eligibility: %+v", *rejected)
	}
	stats := manager.Snapshot(clock.Now())
	if stats.BudgetRejected != 1 || stats.PerRecordRejected != 0 || stats.UnclassifiedRejected != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("300 bp treatment counters do not isolate budget: %+v", stats)
	}
	if !c.txLedger.IsOutstanding(record) || len(c.legs[1].rescue) != 0 {
		t.Fatalf("budget rejection changed DATA ledger or queued a rescue: outstanding=%v queue=%d", c.txLedger.IsOutstanding(record), len(c.legs[1].rescue))
	}
}

func addBoundedRescueOriginal(t *testing.T, b *streamBoundedRescueBudget, seq, bytes uint64, at time.Time) {
	t.Helper()
	if !b.RecordOriginal(seq, bytes, at) {
		t.Fatalf("original sequence %d rejected", seq)
	}
}

func reserveBoundedRescue(t *testing.T, b *streamBoundedRescueBudget, e StreamRescueEvidence, bytes uint64, at time.Time) uint64 {
	t.Helper()
	decision := b.Reserve(e, bytes, at)
	if !decision.Admitted || decision.ReservationID == 0 {
		t.Fatalf("reserve denied: %+v", decision)
	}
	return decision.ReservationID
}

func TestBoundedRescueC01SameDataSameEpochCommitsOnce(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("first Carrier attempt did not commit")
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	d := b.Reserve(boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if d.Admitted || d.Reason != "per_record_epoch_limit" {
		t.Fatalf("duplicate epoch attempt admitted: %+v", d)
	}
}

func TestBoundedRescueC02ConcurrentWritersShareOneSessionBudget(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	addBoundedRescueOriginal(t, b, 2, 1, clock.Now())
	addBoundedRescueOriginal(t, b, 3, 1, clock.Now())
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for seq, leg := range map[uint64]uint8{2: 0, 3: 1} {
		wg.Add(1)
		go func(seq uint64, leg uint8) {
			defer wg.Done()
			<-start
			d := b.Reserve(boundedHealthyEvidence(seq, 1, leg), 64<<10, clock.Now())
			if d.Admitted {
				admitted.Add(1)
			}
		}(seq, leg)
	}
	close(start)
	wg.Wait()
	if got := admitted.Load(); got != 1 {
		t.Fatalf("admitted %d concurrent reservations, want 1", got)
	}
	if got := b.Snapshot(clock.Now()).ReservedBytes; got != 64<<10 {
		t.Fatalf("reserved %d bytes, want exactly one chunk", got)
	}
}

func TestBoundedRescueC03ExactBudgetBoundary(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("boundary reservation did not begin")
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	d := b.Reserve(boundedHealthyEvidence(1, 2, 1), 1, clock.Now())
	if d.Admitted {
		t.Fatalf("admitted over exact budget boundary: %+v", d)
	}
}

func TestBoundedRescueC04RollingWindowExpiresAtExactCutoff(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	t0 := clock.Now()
	addBoundedRescueOriginal(t, b, 1, 6_553_600, t0)
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, t0)
	if !b.BeginWrite(id, t0) {
		t.Fatal("write did not begin")
	}
	if err := b.Settle(id, 64<<10, nil, t0); err != nil {
		t.Fatal(err)
	}
	clock.Advance(streamBoundedRescueWindow)
	s := b.Snapshot(clock.Now())
	if s.WindowOriginalBytes != 0 || s.WindowCarrierAcceptedBytes != 0 {
		t.Fatalf("cutoff event retained: %+v", s)
	}
	addBoundedRescueOriginal(t, b, 2, 65_536, clock.Now())
	d := b.Reserve(boundedHealthyEvidence(2, 1, 1), 512, clock.Now())
	if !d.Admitted {
		t.Fatalf("new window headroom not available: %+v", d)
	}
	if err := b.Release(d.ReservationID, "test cleanup", clock.Now()); !err {
		t.Fatal("queued reservation was not released")
	}
}

func TestBoundedRescueC05WindowBoundaryConcurrentAdmission(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	t0 := clock.Now()
	addBoundedRescueOriginal(t, b, 1, 6_553_600, t0)
	clock.Advance(streamBoundedRescueWindow)
	addBoundedRescueOriginal(t, b, 2, 6_553_600, clock.Now())
	addBoundedRescueOriginal(t, b, 3, 1, clock.Now())
	start := make(chan struct{})
	var wg sync.WaitGroup
	var admitted atomic.Int32
	for _, seq := range []uint64{2, 3} {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			<-start
			if b.Reserve(boundedHealthyEvidence(seq, 1, uint8(seq%2)), 64<<10, clock.Now()).Admitted {
				admitted.Add(1)
			}
		}(seq)
	}
	close(start)
	wg.Wait()
	if got := admitted.Load(); got != 1 {
		t.Fatalf("boundary races admitted %d, want 1", got)
	}
}

func TestBoundedRescueC06CompleteCarrierWriteSettlesOnce(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin")
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err == nil {
		t.Fatal("duplicate settlement was accepted")
	}
	s := b.Snapshot(clock.Now())
	if s.CarrierAcceptedBytes != 64<<10 || s.OutstandingReservations != 0 {
		t.Fatalf("bad complete settlement: %+v", s)
	}
}

func TestBoundedRescueC07PartialWriteSettlesAcceptedPayloadOnly(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin")
	}
	if err := b.Settle(id, 4096, errors.New("partial"), clock.Now()); err != nil {
		t.Fatal(err)
	}
	s := b.Snapshot(clock.Now())
	if s.CarrierAcceptedBytes != 4096 || s.PartialAcceptedBytes != 4096 || s.ReleasedBytes != (64<<10)-4096 {
		t.Fatalf("bad partial settlement: %+v", s)
	}
}

func TestBoundedRescueC08FailedCarrierWriteReleasesBytesButConsumesAttempt(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 300, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin")
	}
	if err := b.Settle(id, 0, errors.New("carrier down"), clock.Now()); err != nil {
		t.Fatal(err)
	}
	d := b.Reserve(boundedHealthyEvidence(1, 1, 1), 1, clock.Now())
	if d.Admitted || d.Reason != "per_record_epoch_limit" {
		t.Fatalf("failed Carrier attempt did not consume epoch cap: %+v", d)
	}
	if got := b.Snapshot(clock.Now()).CarrierAcceptedBytes; got != 0 {
		t.Fatalf("failed Write charged %d accepted bytes", got)
	}
}

func TestBoundedRescueC09ACKBeforeWriteReturnKeepsReservation(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin")
	}
	b.AckThrough(2, clock.Now())
	if got := b.Snapshot(clock.Now()).OutstandingReservations; got != 1 {
		t.Fatalf("ACK dropped in-flight reservation: %d", got)
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if got := b.Snapshot(clock.Now()).CarrierAcceptedBytes; got != 64<<10 {
		t.Fatalf("ACK race lost settlement: %d", got)
	}
}

func TestBoundedRescueC10ACKAndCommitBarrierRace(t *testing.T) {
	for run := 0; run < 25; run++ {
		b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
		addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
		id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
		start := make(chan struct{})
		var wg sync.WaitGroup
		var began atomic.Bool
		wg.Add(2)
		go func() { defer wg.Done(); <-start; began.Store(b.BeginWrite(id, clock.Now())) }()
		go func() { defer wg.Done(); <-start; b.AckThrough(2, clock.Now()) }()
		close(start)
		wg.Wait()
		if began.Load() {
			if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
				t.Fatal(err)
			}
		} else if s := b.Snapshot(clock.Now()); s.OutstandingReservations != 0 {
			t.Fatalf("ACK won but reservation leaked: %+v", s)
		}
		if got := b.Snapshot(clock.Now()).OutstandingReservations; got != 0 {
			t.Fatalf("reservation leak after commit/ACK race: %d", got)
		}
	}
}

func TestBoundedRescueC11SessionCloseReleasesQueuedReservation(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	b.Close()
	if !b.Release(id, "session close", clock.Now()) {
		t.Fatal("queued reservation did not release on close")
	}
	if got := b.Snapshot(clock.Now()).OutstandingReservations; got != 0 {
		t.Fatalf("reservation leak after close: %d", got)
	}
	decision := b.Reserve(boundedHealthyEvidence(1, 2, 1), 1, clock.Now())
	if decision.Admitted {
		t.Fatal("closed session admitted a new reservation")
	}
}

func TestBoundedRescueC12GenerationAndEpochArePartOfRecordKey(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 300, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	for epoch := uint64(1); epoch <= 2; epoch++ {
		id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, epoch, 1), 64<<10, clock.Now())
		if !b.BeginWrite(id, clock.Now()) {
			t.Fatalf("epoch %d did not begin", epoch)
		}
		if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	stale := boundedHealthyEvidence(1, 3, 1)
	stale.OwnerGeneration++
	stale.GenerationMatchesOrigin = false
	d := b.Reserve(stale, 1, clock.Now())
	if d.Admitted || d.Exempt {
		t.Fatalf("generation mismatch bypassed classification: %+v", d)
	}
	s := b.Snapshot(clock.Now())
	if s.LifetimeRescueAttempts != 2 || s.LifetimeMaxPerRecord != 2 || s.CrossEpochRecordCount == 0 {
		t.Fatalf("lifetime/epoch counters missing: %+v", s)
	}
}

func TestBoundedRescueC13HealthyBudgetExhaustionRejectsOnlyRescue(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	addBoundedRescueOriginal(t, b, 2, 1, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin")
	}
	if err := b.Settle(id, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	d := b.Reserve(boundedHealthyEvidence(2, 1, 1), 64<<10, clock.Now())
	if d.Admitted || d.Reason != "rolling_budget_exhausted" {
		t.Fatalf("exhausted healthy rescue admitted: %+v", d)
	}
}

func TestBoundedRescueC14PositiveOwnerInvalidationIsExempt(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 25, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	e := boundedHealthyEvidence(1, 1, 1)
	e.OwnerInvalidated = true
	d := b.Reserve(e, 64<<10, clock.Now())
	if !d.Admitted || !d.Exempt || d.Class != StreamRescueFailureRecovery {
		t.Fatalf("positive failure evidence not exempt: %+v", d)
	}
	s := b.Snapshot(clock.Now())
	if s.ReservedBytes != 0 || s.FailureRecoveryExempted != 1 {
		t.Fatalf("failure exemption consumed healthy budget: %+v", s)
	}
}

func TestBoundedRescueC15RestrictedAloneIsNeverFailureEvidence(t *testing.T) {
	e := boundedHealthyEvidence(1, 1, 1)
	e.OwnerRestricted = true
	e.OwnerInvalidated = false
	if got := ClassifyStreamRescue(e); got != StreamRescueHealthyHOL {
		t.Fatalf("restricted-only candidate class = %s", got)
	}
	e.OwnerLive = false
	if got := ClassifyStreamRescue(e); got == StreamRescueFailureRecovery {
		t.Fatalf("dead/unclassified owner was treated as positive failure without invalidation: %s", got)
	}
}

func TestBoundedRescueC16OriginalDataClassificationUnaffected(t *testing.T) {
	e := StreamRescueEvidence{IsData: true, Outstanding: true}
	if got := ClassifyStreamRescue(e); got != StreamRescueOriginalData {
		t.Fatalf("ordinary DATA class = %s", got)
	}
	// The budget API is only called for rescue candidates; recording new original
	// payload continues even when a previous rescue exhausted its window.
	b, clock := newBoundedRescueTestBudget(t, 25, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 100, clock.Now())
	if !b.RecordOriginal(2, 100, clock.Now()) {
		t.Fatal("budget state blocked ordinary DATA accounting")
	}
}

func TestBoundedRescueC17TwoLegsCannotBypassSessionBudget(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	addBoundedRescueOriginal(t, b, 2, 1, clock.Now())
	var wg sync.WaitGroup
	start := make(chan struct{})
	var admitted atomic.Int32
	for seq, target := range map[uint64]uint8{1: 0, 2: 1} {
		wg.Add(1)
		go func(seq uint64, target uint8) {
			defer wg.Done()
			<-start
			if b.Reserve(boundedHealthyEvidence(seq, 1, target), 64<<10, clock.Now()).Admitted {
				admitted.Add(1)
			}
		}(seq, target)
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("session budget was bypassed across target Legs: %d admitted", admitted.Load())
	}
}

func TestBoundedRescueC18ObserverDoesNotChangeAccounting(t *testing.T) {
	var observed atomic.Uint64
	hook := func(StreamRescueEvent) { observed.Add(1) }
	without, clockA := newBoundedRescueTestBudget(t, 100, 0, nil)
	with, clockB := newBoundedRescueTestBudget(t, 100, 0, hook)
	for _, item := range []struct {
		b *streamBoundedRescueBudget
		c *boundedRescueFakeClock
	}{{without, clockA}, {with, clockB}} {
		addBoundedRescueOriginal(t, item.b, 1, 6_553_600, item.c.Now())
		id := reserveBoundedRescue(t, item.b, boundedHealthyEvidence(1, 1, 1), 64<<10, item.c.Now())
		if !item.b.BeginWrite(id, item.c.Now()) {
			t.Fatal("write did not begin")
		}
		if err := item.b.Settle(id, 64<<10, nil, item.c.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if a, z := without.Snapshot(clockA.Now()), with.Snapshot(clockB.Now()); a.CarrierAcceptedBytes != z.CarrierAcceptedBytes || a.LifetimeRescueAttempts != z.LifetimeRescueAttempts {
		t.Fatalf("observer altered treatment: off=%+v on=%+v", a, z)
	}
	if observed.Load() == 0 {
		t.Fatal("trace-on observer received no events")
	}
}

func TestBoundedRescueC19LongSlidingWindowDoesNotRetainACKedState(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 909, 0, nil)
	for seq := uint64(1); seq <= 120; seq++ {
		at := clock.Now()
		addBoundedRescueOriginal(t, b, seq, 1<<20, at)
		id := reserveBoundedRescue(t, b, boundedHealthyEvidence(seq, 1, uint8(seq%2)), 512, at)
		if !b.BeginWrite(id, at) {
			t.Fatalf("sequence %d did not begin", seq)
		}
		if err := b.Settle(id, 512, nil, at); err != nil {
			t.Fatal(err)
		}
		b.AckThrough(seq+1, at)
		clock.Advance(61 * time.Second)
	}
	s := b.Snapshot(clock.Now())
	if s.OutstandingReservations != 0 || len(b.records) != 0 || b.windowOriginal != 0 || b.windowAccepted != 0 || b.acceptedEvents.Len() != 0 || len(b.originalEvents) != 0 {
		t.Fatalf("rolling state retained after 120 expired/ACKed windows: stats=%+v records=%d originals=%d accepted=%d", s, len(b.records), len(b.originalEvents), b.acceptedEvents.Len())
	}
}

func TestBoundedRescueACKBeforeOriginalAccountingDoesNotRetainStaleRecord(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	// The TX ledger record can become visible to the ACK loop before txLoop
	// reaches RecordOriginal. Model that serialized manager-level interleaving.
	b.AckThrough(2, clock.Now())
	if !b.RecordOriginal(1, 64<<10, clock.Now()) {
		t.Fatal("original payload must remain in the rolling denominator")
	}
	if got := b.Snapshot(clock.Now()).WindowOriginalBytes; got != 64<<10 {
		t.Fatalf("window denominator=%d, want %d", got, 64<<10)
	}
	if len(b.records) != 0 {
		t.Fatalf("ACKed record state retained after ACK-before-accounting race: %d records", len(b.records))
	}
	decision := b.Reserve(boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if decision.Admitted || decision.Reason != "already_acked" {
		t.Fatalf("ACKed DATA became Rescue-eligible: %+v", decision)
	}
}

func TestBoundedRescueC20FailureRetryRemainsReachableAtZeroHealthyBudget(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 25, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	e := boundedHealthyEvidence(1, 1, 1)
	e.OwnerInvalidated = true // from StreamTXLedger.InvalidateLeg, not restricted state
	d := b.Reserve(e, 64<<10, clock.Now())
	if !d.Admitted || !d.Exempt {
		t.Fatalf("necessary retry was denied by healthy budget: %+v", d)
	}
	if got := b.Snapshot(clock.Now()).OutstandingReservations; got != 0 {
		t.Fatalf("exempt retry acquired healthy reservation: %d", got)
	}
}

func newBoundedRescueEngineQueueTest(t *testing.T, cfg *StreamBoundedRescueConfig) (*StreamEngine, *StreamTXRecord, *boundedRescueFakeClock) {
	t.Helper()
	clock := newBoundedRescueFakeClock()
	c := slowLegUnitEngine()
	c.cfg.boundedRescueClock = clock.Now
	for id := uint8(0); id < 2; id++ {
		leg := c.legs[id]
		leg.generation = 7
		leg.send = make(chan txSendAttempt, 2)
		leg.rescue = make(chan txSendAttempt, 2)
		leg.done = make(chan struct{})
		c.legGeneration[id].Store(7)
	}
	c.admission.legs[0].restricted = true
	c.admission.legs[0].recoveryEpoch = 1
	if cfg != nil {
		manager, err := newStreamBoundedRescueBudget(*cfg, 64<<10, clock.Now, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.boundedRescue = manager
	}
	now := clock.Now()
	record := c.txLedger.Add(make([]byte, 64<<10), now)
	if !c.txLedger.assignOrdinary(record, 0, now) {
		t.Fatal("ordinary owner assignment failed")
	}
	c.txLedger.setOrigin(record, 7, now)
	if !c.txLedger.MarkTransit(record, 0, now) {
		t.Fatal("original DATA transit claim failed")
	}
	if result := c.txLedger.MarkAttemptSent(record, 0, false, now); !result.Applied {
		t.Fatal("original DATA write completion was not recorded")
	}
	if c.boundedRescue != nil && !c.boundedRescue.RecordOriginal(record.Sequence(), uint64(len(record.Payload())), now) {
		t.Fatal("original DATA was not recorded in the experimental budget")
	}
	return c, record, clock
}

func TestBoundedRescueEngineB0RetainsLegacyQueuePath(t *testing.T) {
	c, record, _ := newBoundedRescueEngineQueueTest(t, nil)
	if !c.tryQueueBoundedFrontierRescue(c.legs[1], record, 0, 1, true) {
		t.Fatal("B0 legacy frontier rescue was not queued")
	}
	if got := len(c.legs[1].rescue); got != 1 {
		t.Fatalf("B0 rescue queue length=%d, want 1", got)
	}
	attempt := <-c.legs[1].rescue
	if attempt.boundedRescueReservation != 0 || attempt.rescueClass != StreamRescueOtherCoreRetry {
		t.Fatalf("B0 path unexpectedly used bounded-budget state: %+v", attempt)
	}
	if !c.txLedger.IsOutstanding(record) {
		t.Fatal("queueing legacy Rescue retired the original record")
	}
}

func TestBoundedRescueEngineB1BudgetSuppressesHealthyRescueButNotOriginalDATA(t *testing.T) {
	c, record, clock := newBoundedRescueEngineQueueTest(t, &StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: 25, BurstBytes: 0,
	})
	if c.tryQueueBoundedFrontierRescue(c.legs[1], record, 0, 1, true) {
		t.Fatal("0.25% base budget admitted a 64 KiB healthy HOL Rescue for a 64 KiB stream")
	}
	if got := len(c.legs[1].rescue); got != 0 {
		t.Fatalf("budget-rejected Rescue entered queue: %d", got)
	}
	stats := c.boundedRescue.Snapshot(clock.Now())
	if stats.CandidateCount != 1 || stats.EligibleCount != 1 || stats.BudgetRejected != 1 || stats.ReservedBytes != 0 {
		t.Fatalf("unexpected B1 healthy-rescue budget result: %+v", stats)
	}
	if !c.tryQueue(c.legs[0], record, false) {
		t.Fatal("budgeting a duplicate Rescue blocked ordinary original DATA queueing")
	}
	if got := len(c.legs[0].send); got != 1 {
		t.Fatalf("ordinary DATA queue length=%d, want 1", got)
	}
}

func TestBoundedRescueEngineB1PositiveInvalidationIsExempt(t *testing.T) {
	c, record, clock := newBoundedRescueEngineQueueTest(t, &StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: 25, BurstBytes: 0,
	})
	c.txLedger.InvalidateLeg(0)
	if !c.tryQueueBoundedFrontierRescue(c.legs[1], record, 0, 1, true) {
		t.Fatal("positively invalidated owner retry was denied by healthy HOL budget")
	}
	if got := len(c.legs[1].rescue); got != 1 {
		t.Fatalf("failure recovery rescue queue length=%d, want 1", got)
	}
	attempt := <-c.legs[1].rescue
	if attempt.rescueClass != StreamRescueFailureRecovery || attempt.boundedRescueReservation != 0 {
		t.Fatalf("invalidated owner did not use unbudgeted failure classification: %+v", attempt)
	}
	stats := c.boundedRescue.Snapshot(clock.Now())
	if stats.FailureRecoveryExempted != 1 || stats.ReservedBytes != 0 || stats.BudgetRejected != 0 {
		t.Fatalf("failure recovery consumed healthy budget: %+v", stats)
	}
}

func TestBoundedRescueC21LedgerACKAndWriteCommitLinearize(t *testing.T) {
	for run := 0; run < 100; run++ {
		clock := newBoundedRescueFakeClock()
		now := clock.Now()
		ledger := NewStreamTXLedger()
		record := ledger.Add(make([]byte, 64<<10), now)
		if !ledger.assignOrdinary(record, 0, now) {
			t.Fatal("owner assignment failed")
		}
		ledger.setOrigin(record, 7, now)
		if !ledger.MarkTransit(record, 0, now) || !ledger.MarkAttemptSent(record, 0, false, now).Applied {
			t.Fatal("original write setup failed")
		}
		budget, err := newStreamBoundedRescueBudget(StreamBoundedRescueConfig{
			Enabled: true, BudgetBasisPoints: 100, BurstBytes: 0,
		}, 64<<10, clock.Now, nil)
		if err != nil {
			t.Fatal(err)
		}
		// The large denominator is already-observed session traffic; the ledger
		// record under the ACK/Write race remains one 64 KiB frontier record.
		if !budget.RecordOriginal(record.Sequence(), 6_553_600, now) {
			t.Fatal("original DATA accounting failed")
		}
		evidence := boundedHealthyEvidence(record.Sequence(), 1, 1)
		id := reserveBoundedRescue(t, budget, evidence, 64<<10, now)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var began atomic.Bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			began.Store(ledger.beginBoundedRescueWrite(record, budget, id, clock.Now()))
		}()
		go func() {
			defer wg.Done()
			<-start
			ack := ledger.ApplyACK(record.Sequence()+1, clock.Now())
			budget.AckThrough(ack.AckedNext, ack.LastACKProgress)
		}()
		close(start)
		wg.Wait()
		if began.Load() {
			if err := budget.Settle(id, uint64(len(record.Payload())), nil, clock.Now()); err != nil {
				t.Fatal(err)
			}
		}
		stats := budget.Snapshot(clock.Now())
		if stats.OutstandingReservations != 0 {
			t.Fatalf("run %d leaked reservation: %+v", run, stats)
		}
		if began.Load() {
			if stats.CommittedAttemptCount != 1 || stats.CarrierAcceptedBytes != uint64(len(record.Payload())) {
				t.Fatalf("run %d: write won but attempt/settlement mismatched: %+v", run, stats)
			}
		} else if stats.CommittedAttemptCount != 0 || stats.CarrierAcceptedBytes != 0 {
			t.Fatalf("run %d: ACK won but write committed: %+v", run, stats)
		}
	}
}

func TestBoundedRescueClassifierRequiresAuditableFrontierEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*StreamRescueEvidence)
		want   StreamRescueClass
	}{
		{"not overdue", func(e *StreamRescueEvidence) { e.FrontierOverdue = false }, StreamRescueOtherCoreRetry},
		{"stale generation", func(e *StreamRescueEvidence) { e.GenerationMatchesOrigin = false }, StreamRescueOtherCoreRetry},
		{"no original write", func(e *StreamRescueEvidence) { e.OriginalWriteObserved = false }, StreamRescueOtherCoreRetry},
		{"wrong record owner", func(e *StreamRescueEvidence) { e.RecordOwnerLeg = 1 }, StreamRescueOtherCoreRetry},
		{"stale recovery epoch", func(e *StreamRescueEvidence) { e.RecoveryEpochCurrent = false }, StreamRescueOtherCoreRetry},
		{"probe", func(e *StreamRescueEvidence) { e.ProbeData = true }, StreamRescueProbeData},
		{"not outstanding", func(e *StreamRescueEvidence) { e.Outstanding = false }, StreamRescueDuplicateOrRejected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := boundedHealthyEvidence(1, 1, 1)
			tc.mutate(&e)
			if got := ClassifyStreamRescue(e); got != tc.want {
				t.Fatalf("class=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestBoundedRescueRejectsForgedInvalidationAndStaleProbe(t *testing.T) {
	e := boundedHealthyEvidence(1, 1, 1)
	e.OwnerInvalidated = true
	e.OwnerLeg = 0
	e.RecordOwnerLeg = 1
	if got := ClassifyStreamRescue(e); got == StreamRescueFailureRecovery {
		t.Fatalf("invalidation for a different owner exempted rescue: %s", got)
	}
	e = boundedHealthyEvidence(1, 1, 1)
	e.ProbeData, e.RecoveryEpochCurrent = true, false
	if got := ClassifyStreamRescue(e); got == StreamRescueProbeData {
		t.Fatalf("stale-generation/epoch probe was exempted: %s", got)
	}
}

func TestBoundedRescueCloseRejectsFailureAndProbeExemptions(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 25, 0, nil)
	e := boundedHealthyEvidence(1, 1, 1)
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	b.Close()
	e.OwnerInvalidated = true
	for _, evidence := range []StreamRescueEvidence{e, func() StreamRescueEvidence {
		probe := boundedHealthyEvidence(1, 1, 1)
		probe.ProbeData = true
		return probe
	}()} {
		decision := b.Reserve(evidence, 64<<10, clock.Now())
		if decision.Admitted || decision.Exempt || decision.Reason != "session_closed" {
			t.Fatalf("closed session admitted an exempt rescue: %+v", decision)
		}
	}
}

func TestBoundedRescueRevalidateBeforeWrite(t *testing.T) {
	t.Run("same healthy epoch keeps reservation", func(t *testing.T) {
		b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
		addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
		e := boundedHealthyEvidence(1, 1, 1)
		id := reserveBoundedRescue(t, b, e, 64<<10, clock.Now())
		class, currentID, ok := b.RevalidateBeforeWrite(id, e, clock.Now())
		if !ok || class != StreamRescueHealthyHOL || currentID != id {
			t.Fatalf("valid healthy candidate changed: class=%s id=%d ok=%v", class, currentID, ok)
		}
	})
	t.Run("positive invalidation promotes outside budget", func(t *testing.T) {
		b, clock := newBoundedRescueTestBudget(t, 25, 64<<10, nil)
		addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
		e := boundedHealthyEvidence(1, 1, 1)
		id := reserveBoundedRescue(t, b, e, 64<<10, clock.Now())
		failed := e
		failed.OwnerInvalidated = true
		class, currentID, ok := b.RevalidateBeforeWrite(id, failed, clock.Now())
		if !ok || !currentIDIsZero(currentID) || class != StreamRescueFailureRecovery {
			t.Fatalf("positive invalidation did not promote: class=%s id=%d ok=%v", class, currentID, ok)
		}
		s := b.Snapshot(clock.Now())
		if s.ReservedBytes != 0 || s.FailureRecoveryExempted != 1 {
			t.Fatalf("promoted recovery still charged healthy budget: %+v", s)
		}
	})
	t.Run("ACKed record cancels before carrier", func(t *testing.T) {
		b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
		addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
		e := boundedHealthyEvidence(1, 1, 1)
		id := reserveBoundedRescue(t, b, e, 64<<10, clock.Now())
		acked := e
		acked.Outstanding = false
		class, nextID, ok := b.RevalidateBeforeWrite(id, acked, clock.Now())
		if ok || nextID != 0 || class != StreamRescueDuplicateOrRejected {
			t.Fatalf("stale record crossed Carrier boundary: class=%s id=%d ok=%v", class, nextID, ok)
		}
		if s := b.Snapshot(clock.Now()); s.OutstandingReservations != 0 || s.ReservedBytes != 0 {
			t.Fatalf("stale reservation leaked: %+v", s)
		}
	})
}

func currentIDIsZero(id uint64) bool { return id == 0 }

func TestBoundedRescueRejectsOverAcceptanceAndUnstartedSettlement(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 100, 0, nil)
	addBoundedRescueOriginal(t, b, 1, 6_553_600, clock.Now())
	id := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if err := b.Settle(id, 0, errors.New("not started"), clock.Now()); err == nil {
		t.Fatal("settlement without Carrier Write boundary accepted")
	}
	if !b.BeginWrite(id, clock.Now()) {
		t.Fatal("write did not begin after rejected premature settlement")
	}
	err := b.Settle(id, 64<<10+1, nil, clock.Now())
	if err == nil {
		t.Fatal("over-acceptance was silently clamped")
	}
	s := b.Snapshot(clock.Now())
	if s.CarrierAcceptedBytes != 64<<10 || s.OverAcceptedBytes != 1 || s.OutstandingReservations != 0 {
		t.Fatalf("over-acceptance audit mismatch: %+v", s)
	}
}

func TestBoundedRescueBurstIsSingleSessionToken(t *testing.T) {
	b, clock := newBoundedRescueTestBudget(t, 25, 64<<10, nil)
	addBoundedRescueOriginal(t, b, 1, 65_536, clock.Now())
	first := reserveBoundedRescue(t, b, boundedHealthyEvidence(1, 1, 1), 64<<10, clock.Now())
	if !b.BeginWrite(first, clock.Now()) {
		t.Fatal("burst attempt did not begin")
	}
	if err := b.Settle(first, 64<<10, nil, clock.Now()); err != nil {
		t.Fatal(err)
	}
	addBoundedRescueOriginal(t, b, 2, 1, clock.Now())
	d := b.Reserve(boundedHealthyEvidence(2, 1, 1), 64<<10, clock.Now())
	if d.Admitted {
		t.Fatalf("session burst was renewed: %+v", d)
	}
	wantBurst := uint64(64<<10) - boundedRescueRatio(65_536, 25)
	if got := b.Snapshot(clock.Now()).BurstCommittedBytes; got != wantBurst {
		t.Fatalf("burst committed bytes=%d want=%d", got, wantBurst)
	}
}

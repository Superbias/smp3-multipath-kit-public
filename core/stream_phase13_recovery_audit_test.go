package smp3core

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests document the current recovery decision inputs. They do not decide
// whether timing an unsent frontier or measuring ACK service is the desired
// product policy.
func TestPhase13UnsentRecoveryFrontierTimeoutIsBounded(t *testing.T) {
	newEngine := func(now time.Time) (*StreamEngine, *StreamTXRecord) {
		c := slowLegUnitEngine()
		c.admission.legs[1] = streamAdmissionLeg{
			restricted: true, probePending: true, probePendingAt: now,
			since: now, limit: 1, byteLimit: uint64(c.cfg.ChunkSize),
			reason: "probe_pending_write",
		}
		r := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), now)
		if !c.txLedger.assignOrdinary(r, 1, now) {
			t.Fatal("failed to assign recovery frontier")
		}
		c.admission.legs[1].probeSequence = r.Sequence()
		c.admission.legs[1].probeSequenceSet = true
		return c, r
	}

	t.Run("never_written_frontier_expires_at_existing_budget", func(t *testing.T) {
		now := time.Unix(100, 0)
		c, r := newEngine(now)
		candidate := c.txLedger.FrontierCandidate(now.Add(streamProbeBudget-time.Nanosecond), time.Second)
		candidate = c.protectFrontier(candidate, now.Add(streamProbeBudget-time.Nanosecond))
		if candidate.overdue || c.admission.legs[1].recovering || !c.admission.legs[1].probePending {
			t.Fatal("unsent frontier expired before the existing budget")
		}
		candidate = c.txLedger.FrontierCandidate(now.Add(streamProbeBudget), time.Second)
		candidate = c.protectFrontier(candidate, now.Add(streamProbeBudget))
		state := c.admission.legs[1]
		if !candidate.overdue || !state.restricted || state.recovering || state.probePending || state.reason != "probe_write_start_timeout" {
			t.Fatalf("unsent frontier did not trigger bounded timeout at %s: candidate=%+v state=%+v", streamProbeBudget, candidate, c.admission.legs[1])
		}
		if !r.firstWriteAt.IsZero() || r.sendCount != 0 || r.inTransit {
			t.Fatalf("fixture unexpectedly wrote the frontier: firstWrite=%s sendCount=%d transit=%v", r.firstWriteAt, r.sendCount, r.inTransit)
		}
		t.Logf("frontier=%d reference=%s age=%s prewrite_budget=%s firstWrite=unset sendCount=0 state=%s", r.Sequence(), candidate.reference, streamProbeBudget, streamProbeBudget, state.reason)
	})

	t.Run("first_write_moves_reference_but_does_not_remove_bound", func(t *testing.T) {
		now := time.Unix(200, 0)
		c, r := newEngine(now)
		writeStart := now.Add(100 * time.Millisecond)
		if !c.txLedger.MarkTransit(r, 1, writeStart) {
			t.Fatal("failed to claim first write")
		}
		if !c.recoveryProbeWriteStarted(1, r, writeStart) {
			t.Fatal("recovery did not begin at the underlying Write start")
		}
		if got := c.txLedger.MarkAttemptSent(r, 1, false, writeStart.Add(time.Millisecond)); !got.Applied {
			t.Fatal("failed to record first write")
		}
		before := writeStart.Add(streamProbeBudget - time.Nanosecond)
		candidate := c.protectFrontier(c.txLedger.FrontierCandidate(before, time.Second), before)
		if candidate.overdue || !c.admission.legs[1].recovering {
			t.Fatalf("frontier remained timed from exposure after first write: candidate=%+v state=%+v", candidate, c.admission.legs[1])
		}
		at := writeStart.Add(streamProbeBudget)
		candidate = c.protectFrontier(c.txLedger.FrontierCandidate(at, time.Second), at)
		if !candidate.overdue || !c.admission.legs[1].restricted || c.admission.legs[1].reason != "probe_timeout" {
			t.Fatalf("first-write clock lost its finite timeout: candidate=%+v state=%+v", candidate, c.admission.legs[1])
		}
		t.Logf("frontier=%d frontierAt=%s firstWriteAt=%s expiry=%s budget=%s", r.Sequence(), r.frontierAt, r.firstWriteAt, at, streamProbeBudget)
	})
}

func phase13WriteRecord(t *testing.T, c *StreamEngine, size int, at time.Time) *StreamTXRecord {
	t.Helper()
	r := c.txLedger.Add(make([]byte, size), at)
	if !c.txLedger.assignOrdinary(r, 1, at) {
		t.Fatal("failed to assign probe record")
	}
	c.txLedger.setOrigin(r, c.legGeneration[1].Load(), at)
	if !c.txLedger.MarkTransit(r, 1, at) {
		t.Fatal("failed to claim probe write")
	}
	if got := c.txLedger.MarkAttemptSent(r, 1, false, at); !got.Applied {
		t.Fatal("failed to record probe write")
	}
	return r
}

func phase13ProbeFixture(t *testing.T, now time.Time, secondSizes []int, ackBoth bool) (*StreamEngine, StreamTXACKResult, time.Time, time.Time) {
	t.Helper()
	const healthyOpeningRate = 6.25e6 // bytes/s; unique-service target is 625,000 bytes/s.
	c := slowLegUnitEngine()
	c.legGeneration[0].Store(3)
	c.legGeneration[1].Store(7)
	c.admission.legs[0].serviceRate = healthyOpeningRate
	c.admission.legs[0].serviceAt = now
	c.admission.legs[1] = streamAdmissionLeg{
		recovering:  true,
		since:       now,
		limit:       1,
		byteLimit:   uint64(c.cfg.ChunkSize),
		openingRate: healthyOpeningRate,
		reason:      "unique_probe",
	}

	first := phase13WriteRecord(t, c, 8<<10, now)
	firstAckAt := now.Add(time.Millisecond)
	firstAck := c.txLedger.ApplyACK(first.Sequence()+1, firstAckAt)
	c.admissionACK(firstAck) // establishes the probe window; the seed batch is discarded.

	var records []*StreamTXRecord
	for i, size := range secondSizes {
		records = append(records, phase13WriteRecord(t, c, size, now.Add(2*time.Millisecond+time.Duration(i)*time.Millisecond)))
	}
	lastWriteAt := now.Add(2*time.Millisecond + time.Duration(len(records)-1)*time.Millisecond)
	ackAt := now.Add(251 * time.Millisecond)
	ackNext := records[0].Sequence() + 1
	if ackBoth {
		ackNext = records[len(records)-1].Sequence() + 1
	}
	result := c.txLedger.ApplyACK(ackNext, ackAt)
	if ackBoth && len(records) < 2 {
		t.Fatal("cumulative ACK fixture needs at least two records")
	}
	return c, result, lastWriteAt, ackAt
}

func TestPhase13ProbeUniqueServiceLowSeparatesMeasuredInputs(t *testing.T) {
	base := time.Unix(300, 0)
	const target = 625000.0
	const meaningful = uint64(125000)

	t.Run("sustained_low_unique_ack_service_restricts", func(t *testing.T) {
		// The first 8 KiB ACK seeds the window and is discarded. One 64 KiB
		// unique ACK over 250 ms is 262,144 B/s, below the 625,000 B/s target.
		c, ack, _, ackAt := phase13ProbeFixture(t, base, []int{64 << 10, 64 << 10}, false)
		c.telemetry = &StreamTelemetry{}
		trace := newV27RecoveryCausalityTrace()
		c.cfg.recoveryTraceHook = trace.record
		before := c.admission.legs[1]
		c.admissionACK(ack)
		after := c.admission.legs[1]
		if !after.restricted || after.reason != "probe_unique_service_low" {
			t.Fatalf("expected low unique ACK service to restrict: before=%+v ack=%+v after=%+v", before, ack, after)
		}
		if ack.BeforeOrdinaryBytes[1] < meaningful || ack.BeforeOrdinaryFrames[1] < before.limit {
			t.Fatalf("fixture missed demand gate: bytes=%d frames=%d limit=%d", ack.BeforeOrdinaryBytes[1], ack.BeforeOrdinaryFrames[1], before.limit)
		}
		events := c.TelemetrySnapshot().Telemetry.ProbeUniqueServiceLowEvents
		if len(events) != 1 {
			t.Fatalf("expected one transition snapshot, got %d: %+v", len(events), events)
		}
		event := events[0]
		if event.LegID != 1 || event.PeerLegID != 0 || event.StateBefore != "recovering" || event.ReasonBefore != "unique_probe" || event.StateAfter != "restricted" || event.ReasonAfter != "probe_unique_service_low" {
			t.Fatalf("snapshot transition identity/state mismatch: %+v", event)
		}
		if event.LegGeneration != 7 || event.AckGeneration != 7 || event.PeerGeneration != 3 {
			t.Fatalf("snapshot generation mismatch: %+v", event)
		}
		if event.AckProgressAt != ackAt || event.ProbeWindowStart != before.probeWindow || event.ProbeWindow != ackAt.Sub(before.probeWindow) {
			t.Fatalf("snapshot ACK/probe window mismatch: %+v", event)
		}
		if event.OpeningRateBPS != before.openingRate || event.TargetRateBPS != target || event.ProbeRateBPS >= target || event.ProbeBytes != ack.UniqueBytesByLeg[1] || event.UniqueAckBytes != ack.UniqueBytesByLeg[1] {
			t.Fatalf("snapshot service measurement mismatch: %+v ack=%+v", event, ack)
		}
		if event.BeforeOrdinaryBytes != ack.BeforeOrdinaryBytes[1] || event.BeforeOrdinaryFrames != ack.BeforeOrdinaryFrames[1] || event.ProbeFrameLimit != before.limit || event.MeaningfulDemandBytes != meaningful || event.FeedbackEnvelope != streamProbeBudget || event.FeedbackEnvelopeSource != "stream_probe_budget" {
			t.Fatalf("snapshot demand/envelope mismatch: %+v ack=%+v", event, ack)
		}
		traceEvents, eventCount, dropped := trace.snapshot()
		if dropped != 0 || uint64(len(traceEvents)) != eventCount || len(traceEvents) != 2 {
			t.Fatalf("unexpected service-low trace completeness/count: count=%d dropped=%d events=%+v", eventCount, dropped, traceEvents)
		}
		decision, action := traceEvents[0], traceEvents[1]
		if decision.Kind != "probe_unique_service_low_decision" || action.Kind != "probe_unique_service_low_action" || action.DecisionEventIndex != decision.EventIndex {
			t.Fatalf("service-low trace decision/action order mismatch: %+v", traceEvents)
		}
		if decision.LegID != 1 || decision.PeerLegID != 0 || decision.LegGeneration != 7 || decision.RecordGeneration != 7 || decision.StateBefore != "recovering" || decision.ReasonBefore != "unique_probe" || decision.ProbeServiceLow == nil {
			t.Fatalf("service-low trace decision identity mismatch: %+v", decision)
		}
		if decision.ProbeServiceLow.EventIndex != event.EventIndex || decision.ProbeServiceLow.ProbeRateBPS != event.ProbeRateBPS || decision.ProbeServiceLow.TargetRateBPS != event.TargetRateBPS || decision.ProbeServiceLow.AckedNext != event.AckedNext || decision.ProbeServiceLow.ObservedAt != decision.PolicyAt {
			t.Fatalf("service-low trace decision inputs differ from telemetry snapshot: trace=%+v telemetry=%+v", decision.ProbeServiceLow, event)
		}
		if action.StateAfter != "restricted" || action.ReasonAfter != "probe_unique_service_low" || !action.Applied || action.ActionAt.Before(action.PolicyAt) {
			t.Fatalf("service-low trace action mismatch: %+v", action)
		}
		t.Logf("reason=%s openingRate=%.0f target=%.0f probeWindow=%s probeBytes=%d probeRate=%.0f ackUniqueBytes=%d ackedNext=%d ackAt=%s pendingBefore=%dB/%df envelopeFloor=%s meaningful=%dB", after.reason, before.openingRate, target, before.probeWindow, before.probeBytes, float64(64<<10)/.25, ack.UniqueBytesByLeg[1], ack.AckedNext, ackAt, ack.BeforeOrdinaryBytes[1], ack.BeforeOrdinaryFrames[1], streamProbeBudget, meaningful)
		t.Logf("probe_unique_service_low_snapshot=%+v", event)
	})

	t.Run("above_target_service_does_not_trigger_low_service", func(t *testing.T) {
		c, ack, _, _ := phase13ProbeFixture(t, base, []int{160 << 10, 1}, false)
		before := c.admission.legs[1]
		c.admissionACK(ack)
		after := c.admission.legs[1]
		if after.restricted || !after.recovering || after.reason == "probe_unique_service_low" {
			t.Fatalf("above-target ACK service was rejected: before=%+v ack=%+v after=%+v", before, ack, after)
		}
		rate := float64(ack.UniqueBytesByLeg[1]) / ack.LastACKProgress.Sub(before.probeWindow).Seconds()
		if rate < target {
			t.Fatalf("fixture did not exceed target: rate=%.0f target=%.0f", rate, target)
		}
		t.Logf("reason=%s openingRate=%.0f target=%.0f probeWindow=%s uniqueBytes=%d measuredRate=%.0f pendingBefore=%dB/%dF", after.reason, before.openingRate, target, before.probeWindow, ack.UniqueBytesByLeg[1], rate, ack.BeforeOrdinaryBytes[1], ack.BeforeOrdinaryFrames[1])
	})

	t.Run("fast_writes_with_delayed_cumulative_ack_are_measured_as_ack_service", func(t *testing.T) {
		// Both records are written within 1 ms, but a single cumulative ACK is
		// applied 250 ms later. The current recovery policy measures ACK progress,
		// not the local Carrier Write completion rate.
		c, ack, lastWriteAt, ackAt := phase13ProbeFixture(t, base, []int{64 << 10, 64 << 10}, true)
		before := c.admission.legs[1]
		c.admissionACK(ack)
		after := c.admission.legs[1]
		rate := float64(ack.UniqueBytesByLeg[1]) / ack.LastACKProgress.Sub(before.probeWindow).Seconds()
		if !after.restricted || after.reason != "probe_unique_service_low" || rate >= target {
			t.Fatalf("fixture failed to expose ACK-clock behavior: rate=%.0f target=%.0f ack=%+v after=%+v", rate, target, ack, after)
		}
		t.Logf("reason=%s openingRate=%.0f target=%.0f probeWindow=%s uniqueBytes=%d measuredACKRate=%.0f writeSpan=%s ACKWindow=%s pendingBefore=%dB/%dF", after.reason, before.openingRate, target, before.probeWindow, ack.UniqueBytesByLeg[1], rate, lastWriteAt.Sub(base.Add(2*time.Millisecond)), ackAt.Sub(before.probeWindow), ack.BeforeOrdinaryBytes[1], ack.BeforeOrdinaryFrames[1])
	})

	t.Run("low_rate_without_enough_pending_demand_does_not_restrict", func(t *testing.T) {
		c, ack, _, _ := phase13ProbeFixture(t, base, []int{64 << 10}, false)
		before := c.admission.legs[1]
		c.admissionACK(ack)
		after := c.admission.legs[1]
		if after.restricted || !after.recovering {
			t.Fatalf("low rate without meaningful pending demand triggered restriction: before=%+v ack=%+v after=%+v", before, ack, after)
		}
		if ack.BeforeOrdinaryBytes[1] >= meaningful {
			t.Fatalf("fixture unexpectedly met meaningful-demand gate: pending=%d threshold=%d", ack.BeforeOrdinaryBytes[1], meaningful)
		}
		t.Logf("reason=%s openingRate=%.0f target=%.0f probeRate=%.0f pending=%dB/%dF meaningfulThreshold=%dB", after.reason, before.openingRate, target, float64(ack.UniqueBytesByLeg[1])/.25, ack.BeforeOrdinaryBytes[1], ack.BeforeOrdinaryFrames[1], meaningful)
	})
}

func TestPhase13RecoveryCauseTelemetryIsBounded(t *testing.T) {
	telemetry := &StreamTelemetry{}
	for i := 0; i < streamProbeUniqueServiceLowHistoryLimit+5; i++ {
		telemetry.recordProbeUniqueServiceLow(StreamProbeUniqueServiceLowSnapshot{ObservedAt: time.Unix(int64(i+1), 0)})
	}
	snapshot := telemetry.snapshot()
	if got := len(snapshot.ProbeUniqueServiceLowEvents); got != streamProbeUniqueServiceLowHistoryLimit {
		t.Fatalf("event history grew beyond its bound: got=%d want=%d", got, streamProbeUniqueServiceLowHistoryLimit)
	}
	if snapshot.DroppedProbeUniqueServiceLowEvents != 5 {
		t.Fatalf("dropped event count=%d want=5", snapshot.DroppedProbeUniqueServiceLowEvents)
	}
	for index, event := range snapshot.ProbeUniqueServiceLowEvents {
		wantEventIndex := uint64(index + 6)
		wantObservedAt := time.Unix(int64(index+6), 0)
		if event.EventIndex != wantEventIndex || !event.ObservedAt.Equal(wantObservedAt) {
			t.Fatalf("event[%d]=%+v want index=%d time=%s", index, event, wantEventIndex, wantObservedAt)
		}
	}
}

func TestPhase13RecoveryCauseTelemetryConcurrentSnapshots(t *testing.T) {
	const (
		writerCount     = 8
		readerCount     = 5
		eventsPerWriter = 1000
	)
	telemetry := &StreamTelemetry{}
	start := make(chan struct{})
	var writers sync.WaitGroup
	var readers sync.WaitGroup
	var writersDone atomic.Bool
	var invalid atomic.Bool

	for reader := 0; reader < readerCount; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var lastDropped uint64
			for !writersDone.Load() {
				select {
				case <-start:
				default:
					runtime.Gosched()
					continue
				}
				snapshot := telemetry.snapshot()
				if len(snapshot.ProbeUniqueServiceLowEvents) > streamProbeUniqueServiceLowHistoryLimit || snapshot.DroppedProbeUniqueServiceLowEvents < lastDropped {
					invalid.Store(true)
					return
				}
				lastDropped = snapshot.DroppedProbeUniqueServiceLowEvents
				for i, event := range snapshot.ProbeUniqueServiceLowEvents {
					if event.EventIndex == 0 || (i > 0 && event.EventIndex <= snapshot.ProbeUniqueServiceLowEvents[i-1].EventIndex) {
						invalid.Store(true)
						return
					}
				}
				runtime.Gosched()
			}
		}()
	}

	for writer := 0; writer < writerCount; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			<-start
			for event := 0; event < eventsPerWriter; event++ {
				telemetry.recordProbeUniqueServiceLow(StreamProbeUniqueServiceLowSnapshot{ObservedAt: time.Now()})
			}
		}()
	}
	close(start)
	writers.Wait()
	writersDone.Store(true)
	readers.Wait()
	if invalid.Load() {
		t.Fatal("concurrent telemetry snapshot violated bounded, ordered, monotonic history invariants")
	}

	snapshot := telemetry.snapshot()
	wantTotal := uint64(writerCount * eventsPerWriter)
	if got := len(snapshot.ProbeUniqueServiceLowEvents); got != streamProbeUniqueServiceLowHistoryLimit {
		t.Fatalf("final event history length=%d want=%d", got, streamProbeUniqueServiceLowHistoryLimit)
	}
	if snapshot.DroppedProbeUniqueServiceLowEvents != wantTotal-streamProbeUniqueServiceLowHistoryLimit {
		t.Fatalf("dropped event count=%d want=%d", snapshot.DroppedProbeUniqueServiceLowEvents, wantTotal-streamProbeUniqueServiceLowHistoryLimit)
	}
	firstWant := wantTotal - streamProbeUniqueServiceLowHistoryLimit + 1
	for i, event := range snapshot.ProbeUniqueServiceLowEvents {
		if want := firstWant + uint64(i); event.EventIndex != want {
			t.Fatalf("final event[%d] index=%d want=%d", i, event.EventIndex, want)
		}
	}
}

// Queue-full probe admission is distinct from a DATA write. This test freezes
// the current bounded timeout behavior without deciding whether product policy
// should expose the leg as recovering before a real Write begins.
func TestPhase13QueueFullProbeDoesNotEnterWriter(t *testing.T) {
	c := slowLegUnitEngine()
	leg := c.legs[1]
	leg.send = make(chan txSendAttempt, 1)
	leg.send <- txSendAttempt{} // occupy the only slot; no writer is running.
	now := time.Now().Add(-time.Second)
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, probePending: true, probePendingAt: now,
		since: now, limit: 1,
		byteLimit: uint64(c.cfg.ChunkSize), reason: "unique_probe",
	}
	r := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), now)
	if !c.txLedger.assignOrdinary(r, 1, now) {
		t.Fatal("failed to assign recovery frontier")
	}
	c.admission.legs[1].probeSequence = r.Sequence()
	c.admission.legs[1].probeSequenceSet = true
	c.txLedger.setOrigin(r, 1, now)
	ackedBefore := c.txLedger.ProgressSnapshot().AckedNext
	for i := 0; i < 16; i++ {
		if c.tryQueue(leg, r, false) {
			t.Fatalf("queue attempt %d unexpectedly succeeded while the only slot was occupied", i+1)
		}
		if len(leg.send) != 1 {
			t.Fatalf("queue attempt %d changed the occupied queue length to %d", i+1, len(leg.send))
		}
	}
	snapshot := c.txLedger.probeReferenceSnapshot(r, true)
	if !snapshot.FirstWriteAt.IsZero() || snapshot.InTransit || snapshot.SendCount != 0 {
		t.Fatalf("queue rejection created write evidence: %+v", snapshot)
	}

	decisionAt := time.Now()
	candidate := c.txLedger.FrontierCandidate(decisionAt, time.Second)
	candidate = c.protectFrontier(candidate, decisionAt)
	state := c.admission.legs[1]
	if !candidate.overdue || !state.restricted || state.recovering || state.probePending || state.reason != "probe_write_start_timeout" {
		t.Fatalf("repeated queue rejection did not reach the existing bounded timeout: candidate=%+v state=%+v", candidate, c.admission.legs[1])
	}
	if snapshot.AckedNext != ackedBefore || c.txLedger.ProgressSnapshot().AckedNext != ackedBefore {
		t.Fatal("queue rejection advanced cumulative ACK without a wire acknowledgement")
	}
	if !snapshot.FirstWriteAt.IsZero() {
		t.Fatal("unexpected first write after queue-only rejection")
	}
	if !state.probeExpired || state.probeExpiredSequence != r.Sequence() {
		t.Fatalf("timed-out probe was not marked stale: state=%+v record=%d", state, r.Sequence())
	}
	if c.recoveryProbeWriteStarted(1, r, decisionAt.Add(time.Millisecond)) {
		t.Fatal("late writer start was allowed to revive an expired pre-write probe")
	}
	t.Logf("frontier=%d queue_full_rejections=16 successful_enqueue=0 writer_entries=0 firstWriteAt=unset timeout_reason=%s late_start=rejected", r.Sequence(), state.reason)
}

func TestPhase13ProbeSequenceZeroIsBound(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Unix(500, 0)
	first := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), now)
	second := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), now)
	if first.Sequence() != 0 || second.Sequence() != 1 {
		t.Fatalf("unexpected test sequence allocation: first=%d second=%d", first.Sequence(), second.Sequence())
	}
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, probePending: true, probeSequence: first.Sequence(), probeSequenceSet: true,
		probePendingAt: now, since: now, limit: 1, byteLimit: uint64(c.cfg.ChunkSize), reason: "probe_pending_write",
	}
	if !c.recoveryProbeWriteStarted(1, second, now.Add(time.Millisecond)) {
		t.Fatal("unrelated sequence was treated as an expired probe")
	}
	state := c.admission.legs[1]
	if !state.restricted || !state.probePending || state.recovering {
		t.Fatalf("sequence 1 matched the sequence 0 probe: %+v", state)
	}
	if !c.recoveryProbeWriteStarted(1, first, now.Add(2*time.Millisecond)) {
		t.Fatal("bound sequence 0 probe did not start recovery")
	}
	state = c.admission.legs[1]
	if state.restricted || state.probePending || !state.recovering || !state.since.Equal(now.Add(2*time.Millisecond)) {
		t.Fatalf("bound sequence 0 probe did not transition at its write start: %+v", state)
	}
}

func TestPhase13PrewriteProbeHasIndependentBound(t *testing.T) {
	c := slowLegUnitEngine()
	pendingAt := time.Unix(400, 0)
	r := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), pendingAt)
	if !c.txLedger.assignOrdinary(r, 1, pendingAt) {
		t.Fatal("failed to assign pre-write recovery frontier")
	}
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, probePending: true, probeSequence: r.Sequence(), probeSequenceSet: true, probePendingAt: pendingAt,
		since: pendingAt, limit: 1, byteLimit: uint64(c.cfg.ChunkSize), reason: "probe_pending_write",
	}

	before := pendingAt.Add(streamProbeBudget - time.Nanosecond)
	candidate := c.protectFrontier(c.txLedger.FrontierCandidate(before, time.Second), before)
	state := c.admission.legs[1]
	if candidate.overdue || !state.restricted || state.recovering || !state.probePending {
		t.Fatalf("pre-write probe expired before its independent bound: candidate=%+v state=%+v", candidate, state)
	}

	at := pendingAt.Add(streamProbeBudget)
	candidate = c.protectFrontier(c.txLedger.FrontierCandidate(at, time.Second), at)
	state = c.admission.legs[1]
	if !candidate.overdue || !state.restricted || state.recovering || state.probePending || state.reason != "probe_write_start_timeout" || !state.nextProbe.Equal(at.Add(streamRestrictionCooldown)) {
		t.Fatalf("pre-write bound did not expire independently and finitely: candidate=%+v state=%+v", candidate, state)
	}
	if c.recoveryProbeWriteStarted(1, r, at.Add(time.Millisecond)) {
		t.Fatal("writer starting after the pre-write bound revived the expired probe")
	}
	if !r.firstWriteAt.IsZero() || c.txLedger.ProgressSnapshot().AckedNext != 0 {
		t.Fatalf("pre-write timeout forged write/ACK evidence: firstWrite=%s ackedNext=%d", r.firstWriteAt, c.txLedger.ProgressSnapshot().AckedNext)
	}
	t.Logf("prewrite_elapsed=%s bound=%s reason=%s cooldown=%s recovering=false late_write=rejected", streamProbeBudget, streamProbeBudget, state.reason, streamRestrictionCooldown)
}

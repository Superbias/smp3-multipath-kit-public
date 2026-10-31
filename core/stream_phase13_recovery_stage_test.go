package smp3core

import (
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

type phase13RecoveryStageWriter struct {
	entered        chan struct{}
	payloadWritten chan struct{}
	release        chan struct{}
	block          bool
	dataSeen       bool
	dataWriteAt    time.Time
	onDataWrite    func(time.Time)
	enteredOnce    sync.Once
	payloadOnce    sync.Once
	releaseOnce    sync.Once
}

func newPhase13RecoveryStageWriter(block bool) *phase13RecoveryStageWriter {
	return &phase13RecoveryStageWriter{
		entered: make(chan struct{}), payloadWritten: make(chan struct{}),
		release: make(chan struct{}), block: block,
	}
}

func (w *phase13RecoveryStageWriter) Read([]byte) (int, error) { return 0, io.EOF }

func (w *phase13RecoveryStageWriter) Write(p []byte) (int, error) {
	if len(p) == frameHeaderSize && len(p) > 0 && StreamFrameType(p[0]) == StreamFrameData {
		w.dataSeen = true
		w.dataWriteAt = time.Now()
		if w.onDataWrite != nil {
			w.onDataWrite(w.dataWriteAt)
		}
		w.enteredOnce.Do(func() { close(w.entered) })
		if w.block {
			<-w.release
		}
		return len(p), nil
	}
	if w.dataSeen && len(p) > frameHeaderSize {
		w.payloadOnce.Do(func() { close(w.payloadWritten) })
	}
	return len(p), nil
}

func (w *phase13RecoveryStageWriter) releaseWrite() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func (w *phase13RecoveryStageWriter) Close() error {
	w.releaseWrite()
	return nil
}

func newPhase13RecoveryWriteStage(t *testing.T, block bool) (*StreamEngine, *streamLeg, *StreamTXRecord, *phase13RecoveryStageWriter, <-chan struct{}, uint64) {
	return newPhase13RecoveryWriteStageWithTrace(t, block, nil)
}

func newPhase13RecoveryWriteStageWithTrace(t *testing.T, block bool, traceHook func(streamRecoveryTraceEvent) uint64) (*StreamEngine, *streamLeg, *StreamTXRecord, *phase13RecoveryStageWriter, <-chan struct{}, uint64) {
	c, leg, r, writer, ackedBefore := preparePhase13RecoveryWriteStage(t, block, traceHook)
	writerExited := startPhase13RecoveryWriteStage(c, leg)
	return c, leg, r, writer, writerExited, ackedBefore
}

func preparePhase13RecoveryWriteStage(t *testing.T, block bool, traceHook func(streamRecoveryTraceEvent) uint64) (*StreamEngine, *streamLeg, *StreamTXRecord, *phase13RecoveryStageWriter, uint64) {
	t.Helper()
	c := slowLegUnitEngine()
	c.cfg.RetransmitTimeout = time.Second
	c.cfg.recoveryTraceHook = traceHook
	c.done = make(chan struct{})
	writer := newPhase13RecoveryStageWriter(block)
	leg := &streamLeg{
		id: 1, generation: 1, conn: writer,
		send: make(chan txSendAttempt, 1), rescue: make(chan txSendAttempt, 1),
		control: make(chan legControl, 1), ackWake: make(chan struct{}, 1),
		done: make(chan struct{}), retired: make(chan struct{}),
	}
	c.legs[1] = leg
	c.legGeneration[1].Store(1)
	base := time.Now()
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, probePending: true, probeSequence: 0, probeSequenceSet: true, probePendingAt: base, since: base, limit: 1,
		byteLimit: uint64(c.cfg.ChunkSize), reason: "probe_pending_write",
	}
	r := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), base)
	if !c.txLedger.assignOrdinary(r, 1, base) {
		t.Fatal("failed to assign recovery frontier")
	}
	c.admission.legs[1].probeSequence = r.Sequence()
	c.admission.legs[1].probeSequenceSet = true
	c.txLedger.setOrigin(r, 1, base)
	ackedBefore := c.txLedger.ProgressSnapshot().AckedNext
	if !c.tryQueue(leg, r, false) {
		t.Fatal("failed to enqueue recovery frontier")
	}
	return c, leg, r, writer, ackedBefore
}

func startPhase13RecoveryWriteStage(c *StreamEngine, leg *streamLeg) <-chan struct{} {
	writerExited := make(chan struct{})
	go func() {
		defer close(writerExited)
		c.legWriteLoop(leg)
	}()
	return writerExited
}

func stopPhase13RecoveryWriteStage(t *testing.T, c *StreamEngine, leg *streamLeg, writer *phase13RecoveryStageWriter, exited <-chan struct{}) {
	t.Helper()
	writer.releaseWrite()
	_ = writer.Close()
	leg.close(io.ErrClosedPipe)
	close(c.done)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Error("DATA writer did not stop after releasing the deterministic gate")
	}
}

func waitPhase13WriteSnapshot(t *testing.T, c *StreamEngine, r *StreamTXRecord, sent bool) streamTXProbeReferenceSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := c.txLedger.probeReferenceSnapshot(r, true)
		if !snapshot.FirstWriteAt.IsZero() && (!sent || !snapshot.LastSentAt.IsZero()) {
			return snapshot
		}
		runtime.Gosched()
	}
	t.Fatal("timed out waiting for the expected DATA write ledger stage")
	return streamTXProbeReferenceSnapshot{}
}

func TestPhase13RecoveryProbeWriteHasNoProofWithoutUniqueACK(t *testing.T) {
	t.Run("underlying_data_write_started_but_blocked", func(t *testing.T) {
		c, leg, record, writer, exited, ackedBefore := newPhase13RecoveryWriteStage(t, true)
		defer stopPhase13RecoveryWriteStage(t, c, leg, writer, exited)
		select {
		case <-writer.entered:
		case <-time.After(time.Second):
			t.Fatal("underlying DATA Write did not start")
		}
		first := waitPhase13WriteSnapshot(t, c, record, false)
		state := c.admission.legs[1]
		if !state.recovering || state.restricted || state.probePending || state.since.After(writer.dataWriteAt) || state.since.Before(first.FirstWriteAt) {
			t.Fatalf("recovery did not begin at the underlying DATA Write boundary: state=%+v firstWrite=%s write=%s", state, first.FirstWriteAt, writer.dataWriteAt)
		}
		if !first.LastSentAt.IsZero() || first.AckedNext != ackedBefore {
			t.Fatalf("blocked Write produced send/ACK completion: %+v ackedBefore=%d", first, ackedBefore)
		}
		before := state.since.Add(streamProbeBudget - time.Nanosecond)
		candidate := c.protectFrontier(c.txLedger.FrontierCandidate(before, time.Second), before)
		if candidate.overdue || !c.admission.legs[1].recovering {
			t.Fatalf("blocked Write expired before the existing budget: candidate=%+v state=%+v", candidate, c.admission.legs[1])
		}
		at := state.since.Add(streamProbeBudget)
		candidate = c.protectFrontier(c.txLedger.FrontierCandidate(at, time.Second), at)
		state = c.admission.legs[1]
		if !candidate.overdue || !state.restricted || state.recovering || state.reason != "probe_timeout" || state.proofs != 0 {
			t.Fatalf("blocked Write without ACK did not terminate as bounded restriction: candidate=%+v state=%+v", candidate, state)
		}
		if !c.txLedger.IsOutstanding(record) || c.txLedger.ProgressSnapshot().AckedNext != ackedBefore {
			t.Fatal("blocked Write was retired without unique ACK proof")
		}
		t.Logf("frontier=%d writer_entered=true writer_returned=false unique_ack=false elapsed=%s budget=%s result=restricted", record.Sequence(), streamProbeBudget, streamProbeBudget)
	})

	t.Run("underlying_data_write_returned_but_no_unique_ack", func(t *testing.T) {
		c, leg, record, writer, exited, ackedBefore := newPhase13RecoveryWriteStage(t, false)
		defer stopPhase13RecoveryWriteStage(t, c, leg, writer, exited)
		select {
		case <-writer.payloadWritten:
		case <-time.After(time.Second):
			t.Fatal("underlying DATA payload Write did not return")
		}
		first := waitPhase13WriteSnapshot(t, c, record, true)
		state := c.admission.legs[1]
		if !state.recovering || state.restricted || state.probePending || state.since.After(writer.dataWriteAt) || state.since.Before(first.FirstWriteAt) {
			t.Fatalf("Write return moved recovery start away from the underlying Write boundary: state=%+v firstWrite=%s write=%s", state, first.FirstWriteAt, writer.dataWriteAt)
		}
		if first.LastSentAt.IsZero() || first.AckedNext != ackedBefore || !c.txLedger.IsOutstanding(record) {
			t.Fatalf("write return did not leave a pending unacknowledged record: %+v ackedBefore=%d outstanding=%v", first, ackedBefore, c.txLedger.IsOutstanding(record))
		}
		state = c.admission.legs[1]
		if !state.recovering || state.proofs != 0 {
			t.Fatalf("write return alone counted as recovery proof: %+v", state)
		}
		at := state.since.Add(streamProbeBudget)
		candidate := c.protectFrontier(c.txLedger.FrontierCandidate(at, time.Second), at)
		state = c.admission.legs[1]
		if !candidate.overdue || !state.restricted || state.recovering || state.reason != "probe_timeout" || state.proofs != 0 {
			t.Fatalf("missing unique ACK did not terminate as bounded restriction: candidate=%+v state=%+v", candidate, state)
		}
		if !c.txLedger.IsOutstanding(record) || c.txLedger.ProgressSnapshot().AckedNext != ackedBefore {
			t.Fatal("write completion forged ACK progress")
		}
		t.Logf("frontier=%d writer_entered=true writer_returned=true unique_ack=false send_count=%d elapsed=%s budget=%s result=restricted", record.Sequence(), first.SendCount, streamProbeBudget, streamProbeBudget)
	})
}

func TestPhase13PendingRecoveryProbeRetainsOwnerUntilWriteOrBound(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	r := c.txLedger.Add([]byte{1}, now)
	if !c.txLedger.assignOrdinary(r, 1, now) {
		t.Fatal("failed to assign recovery record to restricted leg")
	}
	owner := c.legs[1]
	a := &benchmarkAssignment{owner: owner}
	c.assignedService[1] = uint64(len(r.Payload()))
	c.assignedRecords[1] = 1
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, nextProbe: now.Add(-time.Millisecond), since: now,
		limit: 1, reason: "probe_timeout",
	}
	c.ordinaryAssignedRecord(1, r, now, len(r.Payload()))
	if state := c.admission.legs[1]; !state.probePending || !state.probeSequenceSet || state.probeSequence != r.Sequence() {
		t.Fatalf("fixture did not establish a record-bound pre-write probe: %+v", state)
	}

	if got := c.assignBenchmark(r, a); got != owner {
		t.Fatalf("pending recovery record was migrated before its underlying DATA Write: owner=%p selected=%p", owner, got)
	}
	if state := c.admission.legs[1]; !state.restricted || !state.probePending || state.recovering {
		t.Fatalf("scheduler transition started recovery before the underlying DATA Write: %+v", state)
	}
	if c.assignedService[1] != uint64(len(r.Payload())) || c.assignedService[0] != 0 {
		t.Fatalf("virtual service moved while preserving the pending recovery record: %v", c.assignedService)
	}

	expiredAt := now.Add(streamProbeBudget)
	candidate := c.txLedger.FrontierCandidate(expiredAt, c.cfg.RetransmitTimeout)
	candidate = c.protectFrontier(candidate, expiredAt)
	state := c.admission.legs[1]
	if !candidate.overdue || !state.restricted || state.recovering || state.probePending || state.reason != "probe_write_start_timeout" || !state.nextProbe.Equal(expiredAt.Add(streamRestrictionCooldown)) {
		t.Fatalf("pre-write timeout did not remain bounded with the existing cooldown: %+v", state)
	}
	if c.recoveryProbeWriteStarted(1, r, expiredAt.Add(time.Nanosecond)) {
		t.Fatal("expired pre-write probe was promoted after its bounded deadline")
	}
	if got := c.assignBenchmark(r, a); got != c.legs[0] {
		t.Fatalf("expired unadmitted recovery record was not released to the healthy leg: got=%p want=%p", got, c.legs[0])
	}
	if c.assignedService[0] != uint64(len(r.Payload())) || c.assignedService[1] != 0 || c.decoupledStats.rollback.Load() != uint64(len(r.Payload())) || c.decoupledStats.reassigned.Load() != uint64(len(r.Payload())) || c.decoupledStats.reassignments.Load() != 1 {
		t.Fatalf("expired recovery ownership did not migrate and roll back exactly once: service=%v rollback=%d reassigned=%d migrations=%d", c.assignedService, c.decoupledStats.rollback.Load(), c.decoupledStats.reassigned.Load(), c.decoupledStats.reassignments.Load())
	}
}

func TestPhase13PendingRecoveryProbeExpiresWhenAnotherLegOwnsFrontier(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()

	// Keep an earlier cumulative-ACK frontier on the healthy leg. The pending
	// recovery record is still queued on the restricted leg, but it is not yet
	// the frontier inspected by scheduleFrontierRescue.
	head := c.txLedger.Add([]byte{0}, now)
	if !c.txLedger.assignOrdinary(head, 0, now) {
		t.Fatal("failed to assign healthy-leg ACK frontier")
	}
	probe := c.txLedger.Add([]byte{1}, now)
	if !c.txLedger.assignOrdinary(probe, 1, now) {
		t.Fatal("failed to assign restricted-leg recovery record")
	}
	owner := c.legs[1]
	a := &benchmarkAssignment{owner: owner}
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, nextProbe: now.Add(-time.Millisecond), since: now,
		limit: 1, reason: "probe_timeout",
	}
	c.ordinaryAssignedRecord(1, probe, now, len(probe.Payload()))

	frontier := c.txLedger.FrontierCandidate(now, time.Second)
	if !frontier.exists || frontier.record != head || frontier.owner != 0 {
		t.Fatalf("fixture did not keep the ACK frontier on the healthy leg: %+v", frontier)
	}
	if !c.pendingRecoveryProbeOwner(1, probe.Sequence()) {
		t.Fatal("fixture did not preserve the recovery record's restricted-leg ownership")
	}

	c.maintainAdmission(now.Add(streamProbeBudget))
	state := c.admission.legs[1]
	if state.probePending || !state.probeExpired || state.probeExpiredSequence != probe.Sequence() || !state.restricted || state.recovering || state.reason != "probe_write_start_timeout" {
		t.Fatalf("independent pre-write budget did not expire the queued probe while another leg owned the frontier: %+v", state)
	}
	if c.recoveryProbeWriteStarted(1, probe, now.Add(streamProbeBudget+time.Nanosecond)) {
		t.Fatal("expired recovery probe was promoted after its independent pre-write budget")
	}
	if got := c.assignBenchmark(probe, a); got != c.legs[0] {
		t.Fatalf("expired pending owner did not release the record to the healthy leg: got=%p want=%p", got, c.legs[0])
	}
	if !c.txLedger.IsOutstanding(head) || !c.txLedger.IsOutstanding(probe) {
		t.Fatal("pre-write expiry incorrectly retired unacknowledged records")
	}
}

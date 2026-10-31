package smp3core

import (
	"sync"
	"testing"
	"time"
)

type phase13DataWriteObservation struct {
	at    time.Time
	state streamAdmissionLeg
}

func observePhase13DataWrite(c *StreamEngine, observations chan<- phase13DataWriteObservation) func(time.Time) {
	return func(at time.Time) {
		c.admission.mu.Lock()
		state := c.admission.legs[1]
		c.admission.mu.Unlock()
		observations <- phase13DataWriteObservation{at: at, state: state}
	}
}

func TestPhase13RecoveryStartsAtUnderlyingWriteBoundary(t *testing.T) {
	traceEntered := make(chan struct{})
	traceRelease := make(chan struct{})
	var traceOnce sync.Once
	var releaseTrace sync.Once
	releaseTraceHook := func() { releaseTrace.Do(func() { close(traceRelease) }) }
	traceHook := func(event streamRecoveryTraceEvent) uint64 {
		if event.Kind == "core_data_write_enter" {
			traceOnce.Do(func() { close(traceEntered) })
			<-traceRelease
		}
		return 0
	}
	c, leg, _, writer, ackedBefore := preparePhase13RecoveryWriteStage(t, true, traceHook)
	observed := make(chan phase13DataWriteObservation, 1)
	writer.onDataWrite = observePhase13DataWrite(c, observed)
	exited := startPhase13RecoveryWriteStage(c, leg)
	defer func() {
		releaseTraceHook()
		stopPhase13RecoveryWriteStage(t, c, leg, writer, exited)
	}()

	select {
	case <-traceEntered:
	case <-time.After(time.Second):
		t.Fatal("DATA write trace hook was not reached")
	}
	c.admission.mu.Lock()
	beforeWrite := c.admission.legs[1]
	c.admission.mu.Unlock()
	if !beforeWrite.restricted || !beforeWrite.probePending || beforeWrite.recovering {
		t.Fatalf("recovery began before the synchronous trace hook released: %+v", beforeWrite)
	}

	releaseTraceHook()
	select {
	case entry := <-observed:
		if !entry.state.recovering || entry.state.restricted || entry.state.probePending {
			t.Fatalf("recovery was not active at the first underlying DATA Write: %+v", entry.state)
		}
		if entry.state.since.After(entry.at) {
			t.Fatalf("recovery timestamp is after the underlying DATA Write entry: since=%s write=%s", entry.state.since, entry.at)
		}
		if entry.state.since.Before(beforeWrite.probePendingAt) {
			t.Fatalf("recovery timestamp preceded this pending probe: since=%s pending=%s", entry.state.since, beforeWrite.probePendingAt)
		}
	case <-time.After(time.Second):
		t.Fatal("underlying DATA Write was not reached after releasing the trace hook")
	}
	if c.txLedger.ProgressSnapshot().AckedNext != ackedBefore {
		t.Fatal("starting the underlying Write advanced ACK progress")
	}
}

func TestPhase13RecoveryRemainsPendingDuringLedgerTraceHook(t *testing.T) {
	traceEntered := make(chan struct{})
	traceRelease := make(chan struct{})
	var traceOnce sync.Once
	var releaseTrace sync.Once
	releaseTraceHook := func() { releaseTrace.Do(func() { close(traceRelease) }) }
	traceHook := func(event streamRecoveryTraceEvent) uint64 {
		if event.Kind == "tx_mark_write_started" {
			traceOnce.Do(func() { close(traceEntered) })
			<-traceRelease
		}
		return 0
	}
	c, leg, _, writer, _ := preparePhase13RecoveryWriteStage(t, true, traceHook)
	c.txLedger.recoveryTraceHook = traceHook
	observed := make(chan phase13DataWriteObservation, 1)
	writer.onDataWrite = observePhase13DataWrite(c, observed)
	exited := startPhase13RecoveryWriteStage(c, leg)
	defer func() {
		releaseTraceHook()
		stopPhase13RecoveryWriteStage(t, c, leg, writer, exited)
	}()

	select {
	case <-traceEntered:
	case <-time.After(time.Second):
		t.Fatal("synchronous TX ledger trace hook was not reached")
	}
	c.admission.mu.Lock()
	beforeWrite := c.admission.legs[1]
	c.admission.mu.Unlock()
	if !beforeWrite.restricted || !beforeWrite.probePending || beforeWrite.recovering {
		t.Fatalf("recovery began before the TX ledger trace hook released: %+v", beforeWrite)
	}

	releaseTraceHook()
	select {
	case entry := <-observed:
		if !entry.state.recovering || entry.state.restricted || entry.state.probePending || entry.state.since.After(entry.at) {
			t.Fatalf("recovery did not start at the underlying DATA Write boundary: state=%+v write=%s", entry.state, entry.at)
		}
	case <-time.After(time.Second):
		t.Fatal("underlying DATA Write was not reached after releasing the TX ledger trace hook")
	}
}

func TestPhase13RecoveryWriteStartUsesFreshTimeAfterAdmissionLock(t *testing.T) {
	traceEntered := make(chan struct{})
	var traceOnce sync.Once
	traceHook := func(event streamRecoveryTraceEvent) uint64 {
		if event.Kind == "core_data_write_enter" {
			traceOnce.Do(func() { close(traceEntered) })
		}
		return 0
	}
	c, leg, _, writer, _ := preparePhase13RecoveryWriteStage(t, true, traceHook)
	observed := make(chan phase13DataWriteObservation, 1)
	writer.onDataWrite = observePhase13DataWrite(c, observed)

	c.admission.mu.Lock()
	locked := true
	var exited <-chan struct{}
	defer func() {
		if locked {
			c.admission.mu.Unlock()
		}
		stopPhase13RecoveryWriteStage(t, c, leg, writer, exited)
	}()
	pendingAt := c.admission.legs[1].probePendingAt
	exited = startPhase13RecoveryWriteStage(c, leg)

	select {
	case <-traceEntered:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach the pre-I/O trace point while admission was locked")
	}
	if wait := time.Until(pendingAt.Add(streamProbeBudget + 10*time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	c.admission.mu.Unlock()
	locked = false

	select {
	case entry := <-observed:
		if entry.state.recovering || !entry.state.restricted || entry.state.probePending {
			t.Fatalf("expired pre-write probe was promoted after admission lock wait: %+v", entry.state)
		}
		if entry.state.reason != "probe_write_start_timeout" {
			t.Fatalf("stale pre-write probe has unexpected terminal reason %q", entry.state.reason)
		}
		if !entry.state.probeExpired || entry.state.probeExpiredSequence != 0 {
			t.Fatalf("expired probe identity was not retained at write entry: %+v", entry.state)
		}
	case <-time.After(time.Second):
		t.Fatal("underlying DATA Write was not reached after the admission lock was released")
	}
}

func TestPhase13RecoveryProofBudgetStartsAtWriteBoundary(t *testing.T) {
	for _, tc := range []struct {
		name       string
		delay      time.Duration
		wantTimely int
	}{
		{name: "at_700ms", delay: streamProofBudget, wantTimely: 1},
		{name: "after_700ms", delay: streamProofBudget + time.Nanosecond, wantTimely: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := slowLegUnitEngine()
			pendingAt := time.Unix(400, 0)
			record := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), pendingAt)
			if !c.txLedger.assignOrdinary(record, 1, pendingAt) {
				t.Fatal("failed to assign the recovery proof record")
			}
			c.admission.legs[1] = streamAdmissionLeg{
				restricted: true, probePending: true, probeSequence: record.Sequence(), probeSequenceSet: true,
				probePendingAt: pendingAt, since: pendingAt, limit: 1, byteLimit: uint64(c.cfg.ChunkSize),
				reason: "probe_pending_write",
			}
			firstWriteAt := pendingAt.Add(50 * time.Millisecond)
			writeBoundary := pendingAt.Add(streamProbeBudget - 10*time.Millisecond)
			if !c.txLedger.MarkTransit(record, 1, writeBoundary) {
				t.Fatal("failed to claim the underlying recovery write")
			}
			c.txLedger.markWriteStarted(record, 1, false, firstWriteAt)
			if !c.recoveryProbeWriteStarted(1, record, writeBoundary) {
				t.Fatal("fresh recovery probe was not promoted at the write boundary")
			}
			state := c.admission.legs[1]
			if !state.recovering || !state.since.Equal(writeBoundary) {
				t.Fatalf("recovery state did not retain the write-boundary timestamp: %+v", state)
			}
			result := c.txLedger.ApplyACK(1, writeBoundary.Add(tc.delay))
			if result.UniqueTimelyByLeg[1] != tc.wantTimely {
				t.Fatalf("unique ACK timing did not use the recovery write boundary: timely=%d rejected=%v firstWrite=%s writeBoundary=%s delay=%s", result.UniqueTimelyByLeg[1], result.ProofRejected[1], firstWriteAt, writeBoundary, tc.delay)
			}
		})
	}
}

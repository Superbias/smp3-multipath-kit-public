package smp3core

import (
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestStreamTXLedgerSequenceAndPayloadOwnership(t *testing.T) {
	ledger := NewStreamTXLedger()
	start := time.Unix(2000000000, 0)
	records := make([]*StreamTXRecord, 4)
	for sequence, size := range []int{1, 1200, 16384, 65535} {
		payload := bytes.Repeat([]byte{byte(sequence + 1)}, size)
		records[sequence] = ledger.Add(payload, start.Add(time.Duration(sequence)*time.Second))
		if records[sequence].Sequence() != uint64(sequence) || &records[sequence].Payload()[0] != &payload[0] {
			t.Fatalf("record sequence=%d payload ownership mismatch", records[sequence].Sequence())
		}
	}
	if ledger.NextSequence() != 4 {
		t.Fatalf("next sequence=%d want=4", ledger.NextSequence())
	}
	result := ledger.ApplyACK(4, start.Add(5*time.Second))
	if result.Released != 4 || ledger.HasOutstanding() {
		t.Fatalf("ACK result=%+v outstanding=%v", result, ledger.HasOutstanding())
	}
	for sequence, record := range records {
		if record.Sequence() != uint64(sequence) || len(record.Payload()) == 0 {
			t.Fatalf("retired record payload was cleared: sequence=%d", sequence)
		}
	}
}

func TestStreamTXLedgerACKSemantics(t *testing.T) {
	start := time.Unix(2100000000, 0)
	ledger := NewStreamTXLedger()
	records := make([]*StreamTXRecord, 5)
	for i := range records {
		records[i] = ledger.Add([]byte{byte(i)}, start)
		if !ledger.MarkTransit(records[i], LegID(i%2), start.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatal("MarkTransit failed")
		}
		if result := ledger.MarkAttemptSent(records[i], LegID(i%2), false, start.Add(time.Duration(i+1)*time.Millisecond)); !result.Applied {
			t.Fatal("MarkAttemptSent failed")
		}
	}
	advanced := ledger.ApplyACK(3, start.Add(time.Second))
	if advanced.Disposition != StreamTXACKAdvanced || advanced.Released != 3 || advanced.AckedNext != 3 || advanced.AckedBytesByLeg != [2]uint64{2, 1} {
		t.Fatalf("ACK=3 result=%+v", advanced)
	}
	for _, next := range []uint64{3, 2} {
		duplicate := ledger.ApplyACK(next, start.Add(2*time.Second))
		if duplicate.Disposition != StreamTXACKNoProgress || duplicate.Released != 0 || duplicate.AckedNext != 3 {
			t.Fatalf("ACK=%d result=%+v", next, duplicate)
		}
	}
	future := ledger.ApplyACK(6, start.Add(3*time.Second))
	if future.Disposition != StreamTXACKFuture || future.Released != 0 || future.AckedNext != 3 || future.FutureCount != 1 || future.Max != 5 {
		t.Fatalf("future ACK result=%+v", future)
	}
	if records[3].Payload()[0] != 3 || records[4].Payload()[0] != 4 {
		t.Fatal("unacked payload changed")
	}
}

func TestStreamTXLedgerHoleRetirementAndProgress(t *testing.T) {
	start := time.Unix(2200000000, 0)
	ledger := NewStreamTXLedger()
	for i := 0; i < 5; i++ {
		ledger.Add([]byte{byte(i)}, start)
	}
	ledger.mu.Lock()
	delete(ledger.outstanding, 1)
	ledger.mu.Unlock()
	result := ledger.ApplyACK(4, start.Add(time.Second))
	if result.Disposition != StreamTXACKAdvanced || result.Released != 3 || result.AckedNext != 4 {
		t.Fatalf("hole ACK result=%+v", result)
	}
	if result.Released == 4 {
		t.Fatal("Released was simplified to ACK range width")
	}
	if ledger.ProgressSnapshot().Outstanding != 1 {
		t.Fatalf("wrong remaining outstanding=%+v", ledger.ProgressSnapshot())
	}
}

func TestStreamTXLedgerACKBeforeMarkSentAttribution(t *testing.T) {
	start := time.Unix(2300000000, 0)
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("attributed"), start)
	if !ledger.MarkTransit(record, 1, start.Add(time.Millisecond)) {
		t.Fatal("MarkTransit failed")
	}
	result := ledger.ApplyACK(1, start.Add(2*time.Millisecond))
	if result.AckedBytesByLeg[1] != uint64(len(record.Payload())) || result.AckedBytesByLeg[0] != 0 {
		t.Fatalf("transit attribution=%v", result.AckedBytesByLeg)
	}
	if sent := ledger.MarkAttemptSent(record, 1, false, start.Add(3*time.Millisecond)); sent.Applied {
		t.Fatal("stale MarkAttemptSent resurrected retired record")
	}
}

func TestStreamTXLedgerAttemptLifecycleAndInvalidate(t *testing.T) {
	start := time.Unix(2400000000, 0)
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("attempt"), start)
	if !ledger.MarkTransit(record, 0, start.Add(time.Second)) || !ledger.AttemptCurrent(record, 0, false) {
		t.Fatal("normal attempt was not current")
	}
	if result := ledger.MarkAttemptSent(record, 0, false, start.Add(2*time.Second)); !result.Applied || result.Retransmit {
		t.Fatalf("normal attempt result=%+v", result)
	}
	started, ok := ledger.MarkRescueTransit(record, 1, start.Add(3*time.Second))
	if !ok || !ledger.AttemptCurrent(record, 1, true) {
		t.Fatal("rescue attempt was not current")
	}
	if !ledger.MarkRescueQueued(record, 1, started) {
		t.Fatal("rescue queue commit failed")
	}
	if result := ledger.MarkAttemptSent(record, 1, true, start.Add(4*time.Second)); !result.Applied || !result.Retransmit || result.Bytes != len(record.Payload()) {
		t.Fatalf("rescue attempt result=%+v", result)
	}
	if !ledger.MarkTransit(record, 1, start.Add(5*time.Second)) {
		t.Fatal("second normal attempt failed")
	}
	ledger.InvalidateLeg(1)
	state := ledger.retrySnapshot()
	if len(state) != 1 || state[0].LastSentLeg != -1 || state[0].InTransit {
		t.Fatalf("invalidated state=%+v", state)
	}
}

func TestStreamTXLedgerStaleCompletionKeepsNewerOwner(t *testing.T) {
	start := time.Unix(2500000000, 0)
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("stale"), start)
	if !ledger.MarkTransit(record, 0, start.Add(time.Second)) {
		t.Fatal("normal transit failed")
	}
	started, ok := ledger.MarkRescueTransit(record, 1, start.Add(2*time.Second))
	if !ok || !ledger.MarkRescueQueued(record, 1, started) {
		t.Fatal("rescue transit failed")
	}
	if result := ledger.MarkAttemptSent(record, 1, true, start.Add(3*time.Second)); !result.Applied {
		t.Fatal("rescue send failed")
	}
	if result := ledger.MarkAttemptSent(record, 0, false, start.Add(5*time.Second)); !result.Applied {
		t.Fatal("slow completion failed")
	}
	snapshot := ledger.Snapshot(start.Add(6 * time.Second))
	if !snapshot.AckFrontierValid || snapshot.AckFrontierLeg != 1 {
		t.Fatalf("frontier owner=%+v", snapshot)
	}
}

type txReferenceRecord struct {
	sequence                                 uint64
	payloadLen                               int
	createdAt, lastSentAt, lastSentAttemptAt time.Time
	lastSentLeg                              int16
	inTransit                                bool
	transitLeg                               uint8
	transitSince                             time.Time
	rescueInTransit                          bool
	rescueLeg                                uint8
	rescueSince, lastRescueAt                time.Time
	sendCount                                uint32
}

type txReference struct {
	next, acked, future uint64
	lastACK             time.Time
	out                 map[uint64]*txReferenceRecord
}

func newTXReference() *txReference { return &txReference{out: make(map[uint64]*txReferenceRecord)} }

func (r *txReference) add(payloadLen int, now time.Time) *txReferenceRecord {
	record := &txReferenceRecord{sequence: r.next, payloadLen: payloadLen, createdAt: now, lastSentLeg: -1}
	r.next++
	r.out[record.sequence] = record
	return record
}

func (r *txReference) markTransit(record *txReferenceRecord, leg uint8, now time.Time) bool {
	if record == nil || r.out[record.sequence] != record || record.inTransit {
		return false
	}
	record.inTransit, record.transitLeg, record.transitSince = true, leg, now
	return true
}

func (r *txReference) markRescueTransit(record *txReferenceRecord, leg uint8, now time.Time) (time.Time, bool) {
	if record == nil || r.out[record.sequence] != record || record.rescueInTransit {
		return time.Time{}, false
	}
	if record.inTransit && record.transitLeg == leg {
		return time.Time{}, false
	}
	if !record.inTransit && record.lastSentLeg == int16(leg) {
		return time.Time{}, false
	}
	record.rescueInTransit, record.rescueLeg, record.rescueSince = true, leg, now
	return now, true
}

func (r *txReference) markRescueQueued(record *txReferenceRecord, leg uint8, started time.Time) bool {
	if record == nil || r.out[record.sequence] != record {
		return false
	}
	if !(record.rescueInTransit && record.rescueLeg == leg && record.rescueSince.Equal(started)) && !record.lastSentAttemptAt.Equal(started) {
		return false
	}
	record.lastRescueAt = started
	return true
}

func (r *txReference) clearAttempt(record *txReferenceRecord, leg uint8, rescue bool) {
	if record == nil || r.out[record.sequence] != record {
		return
	}
	if rescue {
		if record.rescueInTransit && record.rescueLeg == leg {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit, record.rescueSince = false, time.Time{}
		}
		return
	}
	if record.inTransit && record.transitLeg == leg {
		record.inTransit, record.transitSince = false, time.Time{}
	}
}

func (r *txReference) markSent(record *txReferenceRecord, leg uint8, rescue bool, now time.Time) StreamTXAttemptResult {
	if record == nil || r.out[record.sequence] != record {
		return StreamTXAttemptResult{}
	}
	var started time.Time
	if rescue {
		if !record.rescueInTransit || record.rescueLeg != leg {
			return StreamTXAttemptResult{}
		}
		started = record.rescueSince
		record.rescueInTransit, record.rescueSince = false, time.Time{}
	} else {
		if !record.inTransit || record.transitLeg != leg {
			return StreamTXAttemptResult{}
		}
		started = record.transitSince
		record.inTransit, record.transitSince = false, time.Time{}
	}
	if started.IsZero() {
		started = now
	}
	if record.lastSentAttemptAt.IsZero() || !started.Before(record.lastSentAttemptAt) {
		record.lastSentAttemptAt, record.lastSentAt, record.lastSentLeg = started, now, int16(leg)
	}
	record.sendCount++
	return StreamTXAttemptResult{Applied: true, Bytes: record.payloadLen, Retransmit: rescue || record.sendCount > 1}
}

func (r *txReference) invalidate(leg uint8) {
	for _, record := range r.out {
		if record.inTransit && record.transitLeg == leg {
			record.inTransit, record.transitSince = false, time.Time{}
		}
		if record.rescueInTransit && record.rescueLeg == leg {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit, record.rescueSince = false, time.Time{}
		}
		if record.lastSentLeg == int16(leg) {
			record.lastSentLeg = -1
		}
	}
}

func (r *txReference) ack(next uint64, now time.Time) StreamTXACKResult {
	result := StreamTXACKResult{Next: next, Max: r.next, AckedNext: r.acked, LastACKProgress: r.lastACK}
	if next > r.next {
		r.future++
		result.Disposition, result.FutureCount = StreamTXACKFuture, r.future
		return result
	}
	if next <= r.acked {
		result.Disposition = StreamTXACKNoProgress
		return result
	}
	result.Disposition = StreamTXACKAdvanced
	for seq := r.acked; seq < next; seq++ {
		record := r.out[seq]
		if record == nil {
			continue
		}
		leg := record.lastSentLeg
		if leg < 0 && record.inTransit {
			leg = int16(record.transitLeg)
		}
		if leg >= 0 && leg < 2 {
			result.AckedBytesByLeg[leg] += uint64(record.payloadLen)
		}
		delete(r.out, seq)
		result.Released++
	}
	r.acked = next
	result.AckedNext = next
	if result.Released > 0 {
		r.lastACK = now
		result.LastACKProgress = now
	}
	return result
}

func TestStreamTXLedgerRandomizedDifferential10000(t *testing.T) {
	rng := rand.New(rand.NewSource(0x534d50335458))
	start := time.Unix(2600000000, 0)
	ledger, reference := NewStreamTXLedger(), newTXReference()
	coreRecords := make(map[uint64]*StreamTXRecord)
	refRecords := make(map[uint64]*txReferenceRecord)
	for operation := 0; operation < 10000; operation++ {
		now := start.Add(time.Duration(operation+1) * time.Microsecond)
		op := rng.Intn(10)
		if operation < 5 || reference.next == 0 || op == 0 {
			payload := make([]byte, 1+rng.Intn(4096))
			record := ledger.Add(payload, now)
			ref := reference.add(len(payload), now)
			coreRecords[record.Sequence()], refRecords[ref.sequence] = record, ref
		} else {
			sequence := uint64(rng.Int63n(int64(reference.next)))
			record, ref := coreRecords[sequence], refRecords[sequence]
			leg := LegID(rng.Intn(2))
			switch op {
			case 1:
				if ledger.MarkTransit(record, leg, now) != reference.markTransit(ref, uint8(leg), now) {
					t.Fatalf("op %d MarkTransit mismatch", operation)
				}
			case 2:
				started, got := ledger.MarkRescueTransit(record, leg, now)
				expected, want := reference.markRescueTransit(ref, uint8(leg), now)
				if got != want || !started.Equal(expected) {
					t.Fatalf("op %d MarkRescueTransit mismatch", operation)
				}
			case 3:
				started := now
				if ref != nil && ref.rescueInTransit {
					started = ref.rescueSince
				}
				if ledger.MarkRescueQueued(record, leg, started) != reference.markRescueQueued(ref, uint8(leg), started) {
					t.Fatalf("op %d MarkRescueQueued mismatch", operation)
				}
			case 4:
				rescue := rng.Intn(2) == 0
				ledger.ClearAttempt(record, leg, rescue)
				reference.clearAttempt(ref, uint8(leg), rescue)
			case 5:
				rescue := rng.Intn(2) == 0
				got := ledger.MarkAttemptSent(record, leg, rescue, now)
				want := reference.markSent(ref, uint8(leg), rescue, now)
				if got != want {
					t.Fatalf("op %d MarkAttemptSent got=%+v want=%+v", operation, got, want)
				}
			case 6:
				next := uint64(rng.Intn(int(reference.next + 3)))
				got := ledger.ApplyACK(next, now)
				want := reference.ack(next, now)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("op %d ACK got=%+v want=%+v", operation, got, want)
				}
			case 7:
				ledger.InvalidateLeg(leg)
				reference.invalidate(uint8(leg))
			case 8:
				_ = ledger.retrySnapshot()
				_ = ledger.frontierSnapshot()
				_ = ledger.Snapshot(now)
			case 9:
				_ = ledger.ProgressSnapshot()
			}
		}
		ledger.mu.Lock()
		if ledger.nextSequence != reference.next || ledger.ackedNext != reference.acked || ledger.futureACKCount != reference.future || len(ledger.outstanding) != len(reference.out) {
			ledger.mu.Unlock()
			t.Fatalf("op %d header mismatch", operation)
		}
		for seq, expected := range reference.out {
			got := ledger.outstanding[seq]
			if got == nil || got.sequence != expected.sequence || len(got.payload) != expected.payloadLen || got.lastSentLeg != expected.lastSentLeg || got.inTransit != expected.inTransit || got.transitLeg != LegID(expected.transitLeg) || got.rescueInTransit != expected.rescueInTransit || got.rescueLeg != LegID(expected.rescueLeg) || got.sendCount != expected.sendCount || !got.createdAt.Equal(expected.createdAt) || !got.lastSentAt.Equal(expected.lastSentAt) || !got.lastSentAttemptAt.Equal(expected.lastSentAttemptAt) || !got.transitSince.Equal(expected.transitSince) || !got.rescueSince.Equal(expected.rescueSince) || !got.lastRescueAt.Equal(expected.lastRescueAt) {
				ledger.mu.Unlock()
				t.Fatalf("op %d sequence %d mismatch", operation, seq)
			}
		}
		ledger.mu.Unlock()
	}
}

func TestStreamTXLedgerConcurrentACKAttemptInvalidate(t *testing.T) {
	start := time.Unix(2700000000, 0)
	ledger := NewStreamTXLedger()
	records := make([]*StreamTXRecord, 128)
	for i := range records {
		records[i] = ledger.Add([]byte{byte(i)}, start)
	}
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	worker := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); <-startGate; fn() }() }
	worker(func() {
		for i := uint64(1); i <= 128; i++ {
			ledger.ApplyACK(i, start.Add(time.Duration(i)*time.Microsecond))
		}
	})
	worker(func() {
		for i, record := range records {
			leg := LegID(i % 2)
			if ledger.MarkTransit(record, leg, start) {
				ledger.MarkAttemptSent(record, leg, false, start.Add(time.Millisecond))
			}
			ledger.MarkRescueTransit(record, 1-leg, start.Add(2*time.Millisecond))
		}
	})
	worker(func() {
		for i := 0; i < 1000; i++ {
			ledger.InvalidateLeg(LegID(i % 2))
			ledger.retrySnapshot()
		}
	})
	worker(func() {
		for i := 0; i < 1000; i++ {
			ledger.Snapshot(start.Add(time.Second))
			ledger.frontierSnapshot()
		}
	})
	close(startGate)
	wg.Wait()
	if ledger.NextSequence() != 128 || ledger.ProgressSnapshot().AckedNext > 128 {
		t.Fatalf("invalid concurrent final state=%+v", ledger.ProgressSnapshot())
	}
}

func BenchmarkStreamTXLedgerAddAck(b *testing.B) {
	ledger := NewStreamTXLedger()
	now := time.Unix(2800000000, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := ledger.Add([]byte("payload"), now)
		ledger.MarkTransit(record, 0, now)
		ledger.MarkAttemptSent(record, 0, false, now)
		ledger.ApplyACK(record.Sequence()+1, now)
	}
}

func BenchmarkStreamTXLedgerAttemptLifecycle(b *testing.B) {
	ledger := NewStreamTXLedger()
	now := time.Unix(2900000000, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := ledger.Add([]byte("payload"), now)
		ledger.MarkTransit(record, 0, now)
		ledger.MarkAttemptSent(record, 0, false, now)
		started, ok := ledger.MarkRescueTransit(record, 1, now.Add(time.Nanosecond))
		if ok {
			ledger.MarkRescueQueued(record, 1, started)
			ledger.MarkAttemptSent(record, 1, true, now.Add(time.Nanosecond))
		}
		ledger.ApplyACK(record.Sequence()+1, now)
	}
}

package smp3core

import (
	"sync"
	"time"
)

// StreamTXACKDisposition describes the result of applying a peer cumulative
// ACK to the local TX ledger.
type StreamTXACKDisposition uint8

const (
	StreamTXACKNoProgress StreamTXACKDisposition = iota
	StreamTXACKAdvanced
	StreamTXACKFuture
)

// StreamTXACKResult contains only state-transition data. Legacy owns callbacks,
// adaptive counters, inflight tokens, and frontier wakeups.
type StreamTXACKResult struct {
	Disposition     StreamTXACKDisposition
	Next            uint64
	Max             uint64
	FutureCount     uint64
	Released        int
	AckedBytesByLeg [2]uint64
	AckedNext       uint64
	LastACKProgress time.Time
}

// StreamTXAttemptResult reports the legacy side effects of a successful DATA
// attempt without exposing mutable record fields.
type StreamTXAttemptResult struct {
	Applied    bool
	Bytes      int
	Retransmit bool
}

// StreamTXProgress is the lifecycle-safe portion of TX state used by graceful
// drain. It does not own the drain timer or close policy.
type StreamTXProgress struct {
	Outstanding int
	AckedNext   uint64
}

// StreamTXRetryState is a read-only retry candidate view. The record pointer is
// intentionally opaque; retry policy remains in legacy core.go.
type StreamTXRetryState struct {
	Record          *StreamTXRecord
	InTransit       bool
	RescueInTransit bool
	LastSentLeg     int16
}

// StreamTXFrontierSnapshot describes the cumulative-ACK blocker without
// exposing the record's mutable attempt fields.
type StreamTXFrontierSnapshot struct {
	Record          *StreamTXRecord
	Exists          bool
	RescueInTransit bool
	Owner           int16
	ReferenceTime   time.Time
}

// StreamTXSnapshot contains read-only derived TX state for adaptive-facing
// compatibility mirrors.
type StreamTXSnapshot struct {
	NextSequence           uint64
	AckedNext              uint64
	OutstandingFrames      int
	OutstandingBytes       uint64
	OutstandingFramesByLeg [2]int
	OldestOutstandingAge   time.Duration
	OldestOutstandingByLeg [2]time.Duration
	AckFrontierValid       bool
	AckFrontierLeg         LegID
	AckFrontierMultiPath   bool
	AckFrontierAge         time.Duration
	LastACKProgress        time.Time
}

// StreamTXRecord owns one immutable DATA payload after Add. It must only be
// mutated by its containing StreamTXLedger. The payload is deliberately not
// copied and is never cleared on ACK retirement because stale retry queue
// pointers may still refer to this record.
type StreamTXRecord struct {
	sequence uint64
	payload  []byte

	createdAt         time.Time
	lastSentAt        time.Time
	lastSentLeg       int16
	lastSentAttemptAt time.Time

	inTransit    bool
	transitLeg   LegID
	transitSince time.Time

	rescueInTransit bool
	rescueLeg       LegID
	rescueSince     time.Time
	lastRescueAt    time.Time

	sendCount uint32
}

// Sequence returns the immutable frame ordinal.
func (r *StreamTXRecord) Sequence() uint64 { return r.sequence }

// Payload returns the original immutable payload backing storage. Callers must
// treat it as read-only and must not return it to a pool before the record is no
// longer reachable by queued attempts.
func (r *StreamTXRecord) Payload() []byte { return r.payload }

// StreamTXLedger is the single ownership domain for local TX sequencing,
// outstanding records, attempt state, and peer cumulative-ACK retirement.
// Methods are safe for concurrent use. Payloads are not copied by Add.
type StreamTXLedger struct {
	mu sync.Mutex

	nextSequence uint64
	ackedNext    uint64

	outstanding map[uint64]*StreamTXRecord

	futureACKCount  uint64
	lastACKProgress time.Time
}

// NewStreamTXLedger creates an empty TX ledger. It is intentionally zero-policy:
// inflight limits and queue admission remain legacy responsibilities.
func NewStreamTXLedger() *StreamTXLedger {
	return &StreamTXLedger{outstanding: make(map[uint64]*StreamTXRecord)}
}

func (l *StreamTXLedger) ensureLocked() {
	if l.outstanding == nil {
		l.outstanding = make(map[uint64]*StreamTXRecord)
	}
}

// Add assigns one frame-ordinal sequence and takes immutable ownership of the
// supplied payload without copying it.
func (l *StreamTXLedger) Add(payload []byte, now time.Time) *StreamTXRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensureLocked()
	record := &StreamTXRecord{
		sequence:    l.nextSequence,
		payload:     payload,
		createdAt:   now,
		lastSentLeg: -1,
	}
	l.nextSequence++
	l.outstanding[record.sequence] = record
	return record
}

// NextSequence returns the next sequence that Add will allocate.
func (l *StreamTXLedger) NextSequence() uint64 {
	l.mu.Lock()
	next := l.nextSequence
	l.mu.Unlock()
	return next
}

// HasOutstanding reports whether any unretired record remains.
func (l *StreamTXLedger) HasOutstanding() bool {
	l.mu.Lock()
	has := len(l.outstanding) != 0
	l.mu.Unlock()
	return has
}

// ProgressSnapshot returns the state needed by legacy graceful drain polling.
func (l *StreamTXLedger) ProgressSnapshot() StreamTXProgress {
	l.mu.Lock()
	progress := StreamTXProgress{Outstanding: len(l.outstanding), AckedNext: l.ackedNext}
	l.mu.Unlock()
	return progress
}

// IsOutstanding verifies pointer identity, preventing a stale queued attempt
// from acting on a newer record with the same sequence.
func (l *StreamTXLedger) IsOutstanding(record *StreamTXRecord) bool {
	if record == nil {
		return false
	}
	l.mu.Lock()
	ok := l.outstanding[record.sequence] == record
	l.mu.Unlock()
	return ok
}

// RescueInTransit reports whether the record currently has a queued rescue
// attempt. It is a read-only query for legacy queue bookkeeping.
func (l *StreamTXLedger) RescueInTransit(record *StreamTXRecord) bool {
	if record == nil {
		return false
	}
	l.mu.Lock()
	value := l.outstanding[record.sequence] == record && record.rescueInTransit
	l.mu.Unlock()
	return value
}

// LastRescueAt returns the committed rescue-attempt timestamp for diagnostics.
func (l *StreamTXLedger) LastRescueAt(record *StreamTXRecord) time.Time {
	if record == nil {
		return time.Time{}
	}
	l.mu.Lock()
	value := time.Time{}
	if l.outstanding[record.sequence] == record {
		value = record.lastRescueAt
	}
	l.mu.Unlock()
	return value
}

// MarkTransit claims a normal carrier attempt.
func (l *StreamTXLedger) MarkTransit(record *StreamTXRecord, leg LegID, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record || record.inTransit {
		return false
	}
	record.inTransit = true
	record.transitLeg = leg
	record.transitSince = now
	return true
}

// MarkRescueTransit claims a diverse rescue attempt. The returned timestamp is
// the attempt identity used by MarkRescueQueued/ClearAttempt.
func (l *StreamTXLedger) MarkRescueTransit(record *StreamTXRecord, leg LegID, now time.Time) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record || record.rescueInTransit {
		return time.Time{}, false
	}
	if record.inTransit && record.transitLeg == leg {
		return time.Time{}, false
	}
	if !record.inTransit && record.lastSentLeg == int16(leg) {
		return time.Time{}, false
	}
	record.rescueInTransit = true
	record.rescueLeg = leg
	record.rescueSince = now
	return now, true
}

// MarkRescueQueued commits the rescue cooldown only after the queue accepted the
// exact attempt. The caller owns any separate rescue-attempt counter.
func (l *StreamTXLedger) MarkRescueQueued(record *StreamTXRecord, leg LegID, started time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return false
	}
	pending := record.rescueInTransit && record.rescueLeg == leg && record.rescueSince.Equal(started)
	sent := record.lastSentAttemptAt.Equal(started)
	if !pending && !sent {
		return false
	}
	record.lastRescueAt = started
	return true
}

// ClearAttempt abandons a normal or rescue attempt. An exactly committed rescue
// cooldown is rolled back when the matching attempt fails before completion.
func (l *StreamTXLedger) ClearAttempt(record *StreamTXRecord, leg LegID, rescue bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return
	}
	if rescue {
		if record.rescueInTransit && record.rescueLeg == leg {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit = false
			record.rescueSince = time.Time{}
		}
		return
	}
	if record.inTransit && record.transitLeg == leg {
		record.inTransit = false
		record.transitSince = time.Time{}
	}
}

// AttemptCurrent rejects a stale queued attempt after ACK retirement or a
// newer attempt transition.
func (l *StreamTXLedger) AttemptCurrent(record *StreamTXRecord, leg LegID, rescue bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return false
	}
	if rescue {
		return record.rescueInTransit && record.rescueLeg == leg
	}
	return record.inTransit && record.transitLeg == leg
}

// MarkAttemptSent commits one successful carrier write and returns the legacy
// byte/statistics side effects. All record mutation remains under this ledger's
// single lock.
func (l *StreamTXLedger) MarkAttemptSent(record *StreamTXRecord, leg LegID, rescue bool, now time.Time) StreamTXAttemptResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return StreamTXAttemptResult{}
	}
	var attemptStarted time.Time
	if rescue {
		if !record.rescueInTransit || record.rescueLeg != leg {
			return StreamTXAttemptResult{}
		}
		attemptStarted = record.rescueSince
		record.rescueInTransit = false
		record.rescueSince = time.Time{}
	} else {
		if !record.inTransit || record.transitLeg != leg {
			return StreamTXAttemptResult{}
		}
		attemptStarted = record.transitSince
		record.inTransit = false
		record.transitSince = time.Time{}
	}
	if attemptStarted.IsZero() {
		attemptStarted = now
	}
	if record.lastSentAttemptAt.IsZero() || !attemptStarted.Before(record.lastSentAttemptAt) {
		record.lastSentAttemptAt = attemptStarted
		record.lastSentAt = now
		record.lastSentLeg = int16(leg)
	}
	record.sendCount++
	return StreamTXAttemptResult{Applied: true, Bytes: len(record.payload), Retransmit: rescue || record.sendCount > 1}
}

// InvalidateLeg clears only TX attempt state owned by the failed logical leg.
// Leg registry, generation, worker retirement and recovery remain legacy.
func (l *StreamTXLedger) InvalidateLeg(leg LegID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.outstanding {
		if record.inTransit && record.transitLeg == leg {
			record.inTransit = false
			record.transitSince = time.Time{}
		}
		if record.rescueInTransit && record.rescueLeg == leg {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit = false
			record.rescueSince = time.Time{}
		}
		if record.lastSentLeg == int16(leg) {
			record.lastSentLeg = -1
		}
	}
}

// ApplyACK applies a peer cumulative ACK. next > NextSequence is future and
// never retires data. next <= AckedNext is duplicate/old and does nothing.
// Valid advancement retires [oldAckedNext, next), counting only records that
// actually existed in the map. RescueInTransit is intentionally excluded from
// ACK attribution to preserve r11 behavior.
func (l *StreamTXLedger) ApplyACK(next uint64, now time.Time) StreamTXACKResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := StreamTXACKResult{Next: next, Max: l.nextSequence, AckedNext: l.ackedNext, LastACKProgress: l.lastACKProgress}
	if next > l.nextSequence {
		l.futureACKCount++
		result.Disposition = StreamTXACKFuture
		result.FutureCount = l.futureACKCount
		return result
	}
	if next <= l.ackedNext {
		result.Disposition = StreamTXACKNoProgress
		return result
	}
	result.Disposition = StreamTXACKAdvanced
	for sequence := l.ackedNext; sequence < next; sequence++ {
		record, exists := l.outstanding[sequence]
		if !exists {
			continue
		}
		leg := record.lastSentLeg
		if leg < 0 && record.inTransit {
			leg = int16(record.transitLeg)
		}
		if leg >= 0 && leg < 2 {
			result.AckedBytesByLeg[leg] += uint64(len(record.payload))
		}
		delete(l.outstanding, sequence)
		result.Released++
	}
	l.ackedNext = next
	if result.Released > 0 {
		l.lastACKProgress = now
		result.LastACKProgress = now
	}
	result.AckedNext = l.ackedNext
	return result
}

// FutureACKCount returns the number of future ACKs observed by the ledger.
func (l *StreamTXLedger) FutureACKCount() uint64 {
	l.mu.Lock()
	count := l.futureACKCount
	l.mu.Unlock()
	return count
}

// retrySnapshot returns a consistent read-only candidate list. Sorting and
// retry eligibility policy stay in legacy.
func (l *StreamTXLedger) retrySnapshot() []StreamTXRetryState {
	l.mu.Lock()
	states := make([]StreamTXRetryState, 0, len(l.outstanding))
	for _, record := range l.outstanding {
		states = append(states, StreamTXRetryState{Record: record, InTransit: record.inTransit, RescueInTransit: record.rescueInTransit, LastSentLeg: record.lastSentLeg})
	}
	l.mu.Unlock()
	return states
}

// frontierSnapshot returns the ACK-frontier record and the exact reference time
// used by the existing rescue policy.
func (l *StreamTXLedger) frontierSnapshot() StreamTXFrontierSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	record, exists := l.outstanding[l.ackedNext]
	if !exists {
		return StreamTXFrontierSnapshot{Owner: -1}
	}
	owner := record.lastSentLeg
	if record.inTransit {
		owner = int16(record.transitLeg)
	}
	reference := record.createdAt
	for _, candidate := range []time.Time{record.transitSince, record.lastSentAt, record.lastRescueAt} {
		if candidate.After(reference) {
			reference = candidate
		}
	}
	return StreamTXFrontierSnapshot{Record: record, Exists: true, RescueInTransit: record.rescueInTransit, Owner: owner, ReferenceTime: reference}
}

// Snapshot derives the legacy adaptive-facing TX view at now.
func (l *StreamTXLedger) Snapshot(now time.Time) StreamTXSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot := StreamTXSnapshot{NextSequence: l.nextSequence, AckedNext: l.ackedNext, LastACKProgress: l.lastACKProgress}
	for _, record := range l.outstanding {
		snapshot.OutstandingFrames++
		snapshot.OutstandingBytes += uint64(len(record.payload))
		age := now.Sub(record.createdAt)
		if record.createdAt.IsZero() || age < 0 {
			age = 0
		}
		if age > snapshot.OldestOutstandingAge {
			snapshot.OldestOutstandingAge = age
		}
		var owners [2]bool
		if record.lastSentLeg >= 0 && record.lastSentLeg < 2 {
			owners[record.lastSentLeg] = true
		}
		if record.inTransit && record.transitLeg < 2 {
			owners[record.transitLeg] = true
		}
		if record.rescueInTransit && record.rescueLeg < 2 {
			owners[record.rescueLeg] = true
		}
		for leg, owned := range owners {
			if owned {
				snapshot.OutstandingFramesByLeg[leg]++
				if age > snapshot.OldestOutstandingByLeg[leg] {
					snapshot.OldestOutstandingByLeg[leg] = age
				}
			}
		}
	}
	frontier, exists := l.outstanding[l.ackedNext]
	if exists {
		var owners [2]bool
		ownerCount := 0
		markOwner := func(owner int16) {
			if owner >= 0 && owner < 2 && !owners[owner] {
				owners[owner] = true
				ownerCount++
			}
		}
		markOwner(frontier.lastSentLeg)
		if frontier.inTransit {
			markOwner(int16(frontier.transitLeg))
		}
		if frontier.rescueInTransit {
			markOwner(int16(frontier.rescueLeg))
		}
		if ownerCount > 0 {
			snapshot.AckFrontierValid = true
			snapshot.AckFrontierMultiPath = ownerCount > 1
			owner := frontier.lastSentLeg
			latest := frontier.lastSentAttemptAt
			if frontier.inTransit && (latest.IsZero() || frontier.transitSince.After(latest)) {
				owner = int16(frontier.transitLeg)
				latest = frontier.transitSince
			}
			if frontier.rescueInTransit && (latest.IsZero() || frontier.rescueSince.After(latest)) {
				owner = int16(frontier.rescueLeg)
			}
			if owner < 0 {
				for id, marked := range owners {
					if marked {
						owner = int16(id)
						break
					}
				}
			}
			snapshot.AckFrontierLeg = LegID(owner)
			if !frontier.createdAt.IsZero() {
				snapshot.AckFrontierAge = now.Sub(frontier.createdAt)
				if snapshot.AckFrontierAge < 0 {
					snapshot.AckFrontierAge = 0
				}
			}
		}
	}
	return snapshot
}

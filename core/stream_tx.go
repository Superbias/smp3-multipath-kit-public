package smp3core

import (
	"sync"
	"sync/atomic"
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
	Disposition            StreamTXACKDisposition
	Next                   uint64
	Max                    uint64
	FutureCount            uint64
	Released               int
	RetiredBytes           uint64 // unique logical bytes, independent of uncertain leg attribution
	AckedBytesByLeg        [2]uint64
	AckedNext              uint64
	LastACKProgress        time.Time
	UniqueTimelyByLeg      [2]int // single-attempt records retired within the bounded proof budget
	UniqueGeneration       [2]uint64
	UniqueOrderedByLeg     [2]int
	UniqueBytesByLeg       [2]uint64
	FeedbackMinByLeg       [2]time.Duration
	FeedbackMaxByLeg       [2]time.Duration
	BeforeOrdinaryFrames   [2]int
	BeforeOrdinaryBytes    [2]uint64
	ProofRejected          [2][3]int    // duplicate/stale, never-written, late-frontier
	SampleBytesByLeg       [2]uint64    // excludes ambiguous duplicate attempts
	SampleAssignedMinByLeg [2]time.Time // oldest assignment in the clean normal sample cohort
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
	LocalEnqueue    bool
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
	benchmarkAssignment atomic.Pointer[benchmarkAssignment]
	sequence            uint64
	payload             []byte

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

	sendCount        uint32
	ordinaryOwner    int16
	ordinaryAdmitted bool
	assignedAt       time.Time
	firstWriteAt     time.Time
	frontierAt       time.Time
	originGeneration uint64
	originEpoch      int64
	invalidated      bool
	duplicated       bool
	localEnqueue     bool
	trace            *StreamHandoffTelemetry
	traceLeg         uint8
	traceT0Ns        int64
	traceT1Ns        int64
	traceT2Ns        int64
	traceT3Ns        int64
	traceT4Ns        int64
	traceT5Ns        int64
	traceT6Ns        int64
	traceT7Ns        int64
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
	ordinaryFrames  [2]int
	ordinaryBytes   [2]uint64
	pendingBytes    [2]uint64
	evidence        []StreamTXEvidence
	evidenceNext    int
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
	return l.add(payload, now, false)
}
func (l *StreamTXLedger) add(payload []byte, now time.Time, local bool) *StreamTXRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensureLocked()
	record := &StreamTXRecord{
		sequence:      l.nextSequence,
		localEnqueue:  local,
		payload:       payload,
		createdAt:     now,
		lastSentLeg:   -1,
		ordinaryOwner: -1,
	}
	l.nextSequence++
	if record.sequence == l.ackedNext {
		record.frontierAt = now
	}
	l.outstanding[record.sequence] = record
	l.evidenceLocked("created", record, now)
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
	if record.sendCount > 0 {
		record.duplicated = true
	}
	record.transitLeg = leg
	record.transitSince = now
	return true
}

// MarkRescueTransit claims a diverse rescue attempt. The returned timestamp is
// the attempt identity used by MarkRescueQueued/ClearAttempt.
func (l *StreamTXLedger) MarkRescueTransit(record *StreamTXRecord, leg LegID, now time.Time) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.markRescueTransitLocked(record, leg, now)
}

func (l *StreamTXLedger) MarkRestrictedRescueTransit(record *StreamTXRecord, leg LegID, owner int16, now time.Time) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || record.ordinaryOwner != owner {
		return time.Time{}, false
	}
	return l.markRescueTransitLocked(record, leg, now)
}

func (l *StreamTXLedger) markRescueTransitLocked(record *StreamTXRecord, leg LegID, now time.Time) (time.Time, bool) {
	if record == nil || l.outstanding[record.sequence] != record || record.rescueInTransit {
		return time.Time{}, false
	}
	if record.inTransit && record.transitLeg == leg {
		return time.Time{}, false
	}
	// A same-leg rescue is valid when this is the only live carrier. The
	// scheduler excludes same-leg rescue whenever an alternate leg exists.
	record.rescueInTransit = true
	// Once an alternate copy may be sent, cumulative ACK cannot identify the
	// winning copy. Conservatively exclude this record from recovery proofs.
	record.duplicated = true
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
	l.clearAttemptAt(record, leg, rescue, time.Time{})
}

// Engine asynchronous cleanup carries its own attempt identity. A retired
// producer must not clear a newer attempt using the same logical leg ID.
func (l *StreamTXLedger) clearAttemptAt(record *StreamTXRecord, leg LegID, rescue bool, started time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return
	}
	if rescue {
		if record.rescueInTransit && record.rescueLeg == leg && (started.IsZero() || record.rescueSince.Equal(started)) {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit = false
			record.rescueSince = time.Time{}
		}
		return
	}
	if record.inTransit && record.transitLeg == leg && (started.IsZero() || record.transitSince.Equal(started)) {
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
	if record.firstWriteAt.IsZero() {
		record.firstWriteAt = attemptStarted
	}
	record.sendCount++
	l.evidenceLocked("write_complete", record, now)
	return StreamTXAttemptResult{Applied: true, Bytes: len(record.payload), Retransmit: rescue || record.sendCount > 1}
}

// InvalidateLeg clears only TX attempt state owned by the failed logical leg.
// Leg registry, generation, worker retirement and recovery remain legacy.
func (l *StreamTXLedger) InvalidateLeg(leg LegID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.outstanding {
		// Catch-up ACKs after rejoin may describe the retired generation.
		if record.ordinaryOwner == int16(leg) {
			record.duplicated = true
			record.invalidated = true
		}
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

func (l *StreamTXLedger) OrdinaryOwner(record *StreamTXRecord) int16 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return -1
	}
	return record.ordinaryOwner
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
	result.BeforeOrdinaryFrames = l.ordinaryFrames
	result.BeforeOrdinaryBytes = l.ordinaryBytes
	head := l.outstanding[l.ackedNext]
	for sequence := l.ackedNext; sequence < next; sequence++ {
		record, exists := l.outstanding[sequence]
		if !exists {
			continue
		}
		leg := record.lastSentLeg
		result.RetiredBytes += uint64(len(record.payload))
		if leg < 0 && record.inTransit {
			leg = int16(record.transitLeg)
		}
		// A pending-only record can retire through a rescue before the writer
		// commits its attempt. Preserve exactly-once useful bytes, attributing
		// them to the logical ordinary owner, never claiming the winning copy.
		if leg < 0 && record.ordinaryOwner >= 0 {
			leg = record.ordinaryOwner
		}
		if leg >= 0 && leg < 2 {
			result.AckedBytesByLeg[leg] += uint64(len(record.payload))
			if record.ordinaryOwner >= 0 && !record.duplicated && !record.firstWriteAt.IsZero() {
				result.SampleBytesByLeg[leg] += uint64(len(record.payload))
				if at := record.assignedAt; !at.IsZero() && (result.SampleAssignedMinByLeg[leg].IsZero() || at.Before(result.SampleAssignedMinByLeg[leg])) {
					result.SampleAssignedMinByLeg[leg] = at
				}
			}
		}
		l.evidenceLocked("ACK_retire", record, now)
		delete(l.outstanding, sequence)
		if record.ordinaryOwner >= 0 && record.ordinaryOwner < 2 {
			id := record.ordinaryOwner
			l.ordinaryFrames[id]--
			l.ordinaryBytes[id] -= uint64(len(record.payload))
			if !record.ordinaryAdmitted {
				l.pendingBytes[id] -= uint64(len(record.payload))
			}
			if record.duplicated || record.sendCount > 1 {
				result.ProofRejected[id][0]++
			} else if record.firstWriteAt.IsZero() {
				result.ProofRejected[id][1]++
			} else {
				result.UniqueGeneration[id] = record.originGeneration
				age := now.Sub(record.firstWriteAt)
				if age > result.FeedbackMaxByLeg[id] {
					result.FeedbackMaxByLeg[id] = age
				}
				if age > 0 && (result.FeedbackMinByLeg[id] == 0 || age < result.FeedbackMinByLeg[id]) {
					result.FeedbackMinByLeg[id] = age
				}
				if !record.firstWriteAt.IsZero() && now.Sub(record.firstWriteAt) <= streamProofBudget {
					result.UniqueTimelyByLeg[id]++
				} else if (!record.frontierAt.IsZero() && now.Sub(record.frontierAt) <= streamProofBudget) || (head != nil && head != record && head.ordinaryOwner != id && !head.duplicated && !head.firstWriteAt.IsZero()) {
					result.UniqueOrderedByLeg[id]++
				} else {
					result.ProofRejected[id][2]++
				}
				if !record.assignedAt.IsZero() && (now.Sub(record.firstWriteAt) <= streamProofBudget || (!record.frontierAt.IsZero() && now.Sub(record.frontierAt) <= streamProofBudget) || (head != nil && head != record && head.ordinaryOwner != id && !head.duplicated && !head.firstWriteAt.IsZero())) {
					result.UniqueBytesByLeg[id] += uint64(len(record.payload))
				}
			}
		}
		result.Released++
	}
	l.ackedNext = next
	if r := l.outstanding[next]; r != nil && r.frontierAt.IsZero() {
		r.frontierAt = now
		l.evidenceLocked("frontier_exposed", r, now)
	}
	if result.Released > 0 {
		l.lastACKProgress = now
		result.LastACKProgress = now
	}
	result.AckedNext = l.ackedNext
	return result
}

// OrdinaryFrames counts unique logical records, including pending and a
// blocked writer. It never counts rescue copies as additional business data.
func (l *StreamTXLedger) OrdinaryFrames() [2]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ordinaryFrames
}

func (l *StreamTXLedger) OrdinaryAdmissionBytes() (outstanding, pending [2]uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ordinaryBytes, l.pendingBytes
}

func (l *StreamTXLedger) markOrdinaryAdmitted(record *StreamTXRecord, id uint8) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record || record.ordinaryOwner != int16(id) || record.ordinaryAdmitted {
		return
	}
	record.ordinaryAdmitted = true
	l.pendingBytes[id] -= uint64(len(record.payload))
}

// FrontierAge is an O(1) telemetry read, including a pending-only blocker.
func (l *StreamTXLedger) FrontierAge(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.outstanding[l.ackedNext]
	if r == nil || r.createdAt.IsZero() || now.Before(r.createdAt) {
		return 0
	}
	return now.Sub(r.createdAt)
}

// FrontierSnapshot is an O(1) copy of the current blocker/attempt identity.
func (l *StreamTXLedger) FrontierSnapshot() StreamTXFrontierSnapshot { return l.frontierSnapshot() }

func (l *StreamTXLedger) assignOrdinary(record *StreamTXRecord, id uint8, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record == nil || l.outstanding[record.sequence] != record {
		return false
	}
	if record.ordinaryOwner == int16(id) {
		return true
	}
	if record.ordinaryOwner >= 0 {
		l.ordinaryFrames[record.ordinaryOwner]--
		l.ordinaryBytes[record.ordinaryOwner] -= uint64(len(record.payload))
		if !record.ordinaryAdmitted {
			l.pendingBytes[record.ordinaryOwner] -= uint64(len(record.payload))
		}
	}
	record.ordinaryOwner, record.assignedAt = int16(id), now
	l.evidenceLocked("assigned", record, now)
	l.ordinaryFrames[id]++
	l.ordinaryBytes[id] += uint64(len(record.payload))
	if !record.ordinaryAdmitted {
		l.pendingBytes[id] += uint64(len(record.payload))
	}
	return true
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
		states = append(states, StreamTXRetryState{Record: record, LocalEnqueue: record.localEnqueue, InTransit: record.inTransit, RescueInTransit: record.rescueInTransit, LastSentLeg: record.lastSentLeg})
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
	if owner < 0 {
		owner = record.ordinaryOwner
	}
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
		if record.ordinaryOwner >= 0 && record.ordinaryOwner < 2 {
			owners[record.ordinaryOwner] = true
		}
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

// Local enqueue ownership prevents the maintenance retry planner from stealing
// an unsent record between a failed queue attempt and the application's retry.
func (l *StreamTXLedger) addLocal(payload []byte, now time.Time) *StreamTXRecord {
	return l.add(payload, now, true)
}
func (l *StreamTXLedger) finishLocalEnqueue(r *StreamTXRecord) {
	l.mu.Lock()
	r.localEnqueue = false
	l.mu.Unlock()
}

func (l *StreamTXLedger) markWriteStarted(r *StreamTXRecord, leg uint8, rescue bool, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.outstanding[r.sequence] != r {
		return
	}
	if !rescue && r.inTransit && uint8(r.transitLeg) == leg && r.firstWriteAt.IsZero() {
		r.firstWriteAt = now
		l.evidenceLocked("first_write", r, now)
	}
}

// Opt-in fixed ring, disabled during ordinary operation. No payloads are kept.
type StreamTXEvidence struct {
	Event                              string
	Sequence                           uint64
	Bytes                              int
	Owner                              int16
	At, Assigned, FirstWrite, Frontier time.Time
	Attempts                           uint32
	Duplicated, Invalidated            bool
	Generation                         uint64
	Epoch                              int64
}

func (l *StreamTXLedger) EnableEvidenceTraceForTest() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evidence = make([]StreamTXEvidence, 128)
	l.evidenceNext = 0
}
func (l *StreamTXLedger) evidenceLocked(event string, r *StreamTXRecord, now time.Time) {
	if len(l.evidence) == 0 {
		return
	}
	l.evidence[l.evidenceNext%len(l.evidence)] = StreamTXEvidence{event, r.sequence, len(r.payload), r.ordinaryOwner, now, r.assignedAt, r.firstWriteAt, r.frontierAt, r.sendCount, r.duplicated, r.invalidated, r.originGeneration, r.originEpoch}
	l.evidenceNext++
}
func (l *StreamTXLedger) EvidenceTraceForTest() []StreamTXEvidence {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.evidenceNext
	if n > len(l.evidence) {
		n = len(l.evidence)
	}
	out := make([]StreamTXEvidence, 0, n)
	start := l.evidenceNext - n
	for i := 0; i < n; i++ {
		out = append(out, l.evidence[(start+i)%len(l.evidence)])
	}
	return out
}
func (l *StreamTXLedger) setOrigin(r *StreamTXRecord, generation uint64, epoch time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.outstanding[r.sequence] != r {
		return
	}
	r.originGeneration = generation
	r.originEpoch = epoch.UnixNano()
	l.evidenceLocked("admission_origin", r, epoch)
}

func (l *StreamTXLedger) probeReference(r *StreamTXRecord, recovering bool) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r == nil || l.outstanding[r.sequence] != r {
		return time.Time{}
	}
	ref := r.frontierAt
	if !recovering {
		ref = r.createdAt
	}
	if ref.IsZero() {
		ref = r.createdAt
	}
	if r.firstWriteAt.After(ref) {
		ref = r.firstWriteAt
	}
	return ref
}

// A finite transmitted tail is unfinished demand too. Unsent idle records and
// a small ordinary delay do not count as a no-delivery activation stall.
func (l *StreamTXLedger) sentTailStalled(now time.Time, required time.Duration, minBytes uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	var bytes uint64
	repeated := false
	old := time.Time{}
	for _, r := range l.outstanding {
		if r.firstWriteAt.IsZero() {
			continue
		}
		bytes += uint64(len(r.payload))
		if r.sendCount >= 2 {
			repeated = true
		}
		if old.IsZero() || r.firstWriteAt.Before(old) {
			old = r.firstWriteAt
		}
	}
	return (bytes >= minBytes || repeated) && !old.IsZero() && now.Sub(old) >= required && (l.lastACKProgress.IsZero() || now.Sub(l.lastACKProgress) >= required)
}

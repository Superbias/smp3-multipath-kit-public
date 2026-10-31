// EXPERIMENTAL — NOT APPROVED FOR PRODUCTION.
// Bounded HOL Rescue is an opt-in research policy; nil configuration preserves B0.
package smp3core

import (
	"container/heap"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"
)

// StreamRescueClass is a typed, auditable reason for one DATA attempt. It is
// deliberately separate from admission-state strings such as "restricted".
type StreamRescueClass uint8

const (
	StreamRescueOriginalData StreamRescueClass = iota + 1
	StreamRescueHealthyHOL
	StreamRescueFailureRecovery
	StreamRescueOtherCoreRetry
	StreamRescueProbeData
	StreamRescueDuplicateOrRejected
)

func (c StreamRescueClass) String() string {
	switch c {
	case StreamRescueOriginalData:
		return "original_data"
	case StreamRescueHealthyHOL:
		return "healthy_hol_rescue"
	case StreamRescueFailureRecovery:
		return "failure_recovery_retransmission"
	case StreamRescueOtherCoreRetry:
		return "other_core_retry"
	case StreamRescueProbeData:
		return "probe_data"
	case StreamRescueDuplicateOrRejected:
		return "duplicate_or_rejected"
	default:
		return "unknown"
	}
}

type StreamRescueCause uint8

const (
	StreamRescueCauseNone StreamRescueCause = iota
	StreamRescueCauseFrontier
	StreamRescueCauseRetryPlanner
	StreamRescueCauseProbe
)

// StreamRescueEvidence is an immutable decision snapshot. OwnerRestricted is
// recorded for audit context only; it is never sufficient to classify a retry
// as failure recovery or as a healthy HOL Rescue.
type StreamRescueEvidence struct {
	Sequence                uint64
	AckedNext               uint64
	OwnerLeg                int16
	RecordOwnerLeg          int16
	TargetLeg               uint8
	TargetGeneration        uint64
	OriginGeneration        uint64
	OwnerGeneration         uint64
	RecoveryEpoch           uint64
	RecoveryEpochCurrent    bool
	Cause                   StreamRescueCause
	IsData                  bool
	IsRescue                bool
	ProbeData               bool
	Outstanding             bool
	OriginalWriteInTransit  bool
	OriginalWriteLeg        uint8
	FrontierOverdue         bool
	OriginalWriteObserved   bool
	OwnerLive               bool
	TargetAvailable         bool
	TargetGenerationMatches bool
	OwnerRestricted         bool
	OwnerInvalidated        bool
	GenerationMatchesOrigin bool
}

// ClassifyStreamRescue derives a class from positive event evidence. In
// particular, restriction, low service, an overdue frontier, or a blocked
// Carrier write alone never claims the failure-recovery exemption.
func ClassifyStreamRescue(e StreamRescueEvidence) StreamRescueClass {
	if !e.IsData {
		return StreamRescueOtherCoreRetry
	}
	if !e.IsRescue {
		return StreamRescueOriginalData
	}
	if !e.Outstanding {
		return StreamRescueDuplicateOrRejected
	}
	if e.OwnerInvalidated {
		if e.OwnerLeg >= 0 && e.OwnerLeg == e.RecordOwnerLeg && e.OriginGeneration != 0 {
			return StreamRescueFailureRecovery
		}
	}
	probeCandidate := e.ProbeData || e.Cause == StreamRescueCauseProbe
	if probeCandidate && e.OwnerLeg >= 0 &&
		e.OwnerLeg == e.RecordOwnerLeg && e.OwnerLive && e.GenerationMatchesOrigin && e.RecoveryEpochCurrent && e.OriginGeneration != 0 {
		return StreamRescueProbeData
	}
	if probeCandidate {
		return StreamRescueOtherCoreRetry
	}
	if e.Cause == StreamRescueCauseFrontier && e.FrontierOverdue &&
		e.OwnerLeg >= 0 && e.RecordOwnerLeg == e.OwnerLeg && e.OwnerLive && e.GenerationMatchesOrigin &&
		e.RecoveryEpochCurrent && e.OriginGeneration != 0 && e.RecoveryEpoch != 0 && e.OriginalWriteObserved {
		return StreamRescueHealthyHOL
	}
	return StreamRescueOtherCoreRetry
}

type StreamRescueEvent struct {
	At                     time.Time
	MonotonicElapsedNS     int64
	MonotonicTimeValid     bool
	SessionID              SessionID
	CandidateID            uint64
	AttemptID              uint64
	PerRecordAttempt       uint64
	PreviousResult         string
	AckedAtAttempt         bool
	Kind                   string
	Evidence               StreamRescueEvidence
	Class                  StreamRescueClass
	ReservationID          uint64
	PayloadBytes           uint64
	AcceptedBytes          uint64
	Reason                 string
	RecoveryExempted       bool
	PolicyVariant          string
	EligibilitySnapshot    bool
	ClassificationEnabled  bool
	ClassificationEligible bool
	PerRecordEligible      bool
	PerRecordEnabled       bool
	RollingBudgetEligible  bool
	RollingBudgetEnabled   bool
	TargetLegAvailable     bool
	AckedAtDecision        bool
	AckOutcome             string
	FinalDecision          string
	FinalDecisionReason    string
}

// boundedRescueFactorMode exists only for isolated Phase 26 factor analysis.
// It is intentionally unexported and is not mapped from any host/user config.
type boundedRescueFactorMode uint8

const (
	boundedRescueFactorFull boundedRescueFactorMode = iota
	boundedRescueFactorCOnly
	boundedRescueFactorCR
)

func (m boundedRescueFactorMode) String() string {
	switch m {
	case boundedRescueFactorCOnly:
		return "B1-C"
	case boundedRescueFactorCR:
		return "B1-CR"
	default:
		return "B1-F"
	}
}

// StreamBoundedRescueConfig is opt-in experiment configuration. No Native,
// Standalone, or user-facing config parser maps to this field. Enabled must be
// explicitly true. The byte window is fixed at 60 seconds in this experiment.
type StreamBoundedRescueConfig struct {
	Enabled           bool
	BudgetBasisPoints uint16
	BurstBytes        uint64
	factorMode        boundedRescueFactorMode
}

// StreamBoundedRescueStats is a point-in-time experiment snapshot. Carrier
// accepted means local Write acceptance, never proof of wire delivery.
type StreamBoundedRescueStats struct {
	Enabled                    bool
	ConfigError                string
	Closed                     bool
	BudgetBasisPoints          uint16
	Window                     time.Duration
	WindowOriginalBytes        uint64
	WindowCarrierAcceptedBytes uint64
	WindowBaseQuotaBytes       uint64
	WindowBudgetDebtBytes      uint64
	ReservedBytes              uint64
	ReservedBaseBytes          uint64
	ReservedBurstBytes         uint64
	BurstBytes                 uint64
	BurstCommittedBytes        uint64
	PolicyVariant              string
	CandidateAckLinkDropped    uint64
	CandidateCount             uint64
	EligibleCount              uint64
	AdmittedCount              uint64
	PerRecordRejected          uint64
	BudgetRejected             uint64
	StaleOrAckedRejected       uint64
	UnclassifiedRejected       uint64
	FailureRecoveryExempted    uint64
	ProbeDataExempted          uint64
	CarrierAttemptBytes        uint64
	CarrierAcceptedBytes       uint64
	PartialAcceptedBytes       uint64
	ReleasedBytes              uint64
	DuplicateOriginalRecords   uint64
	ClosedOriginalRecords      uint64
	OverAcceptedBytes          uint64
	CommittedAttemptCount      uint64
	LifetimeRescueAttempts     uint64
	LifetimeMaxPerRecord       uint64
	CrossEpochRecordCount      uint64
	OutstandingReservations    int
	AcceptedByTargetLeg        [2]uint64
	AttemptedByTargetLeg       [2]uint64
	RejectedReasons            map[string]uint64
}

const streamBoundedRescueWindow = 60 * time.Second
const streamBoundedRescueCandidateAckLinks = 64

var errBoundedRescueNotWritable = errors.New("bounded rescue canceled before Carrier Write")

type rescueByteEvent struct {
	at     time.Time
	bytes  uint64
	target uint8
}
type rescueByteEventHeap []rescueByteEvent

func (h rescueByteEventHeap) Len() int           { return len(h) }
func (h rescueByteEventHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h rescueByteEventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rescueByteEventHeap) Push(x any)        { *h = append(*h, x.(rescueByteEvent)) }
func (h *rescueByteEventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = rescueByteEvent{}
	*h = old[:n-1]
	return x
}

type rescueEpochKey struct {
	originGeneration uint64
	epoch            uint64
}
type boundedRescueRecordState struct {
	acked           bool
	candidateIDs    []uint64 // bounded decision-to-ACK links for the opt-in observer
	pendingByEpoch  map[rescueEpochKey]uint64
	usedByEpoch     map[rescueEpochKey]struct{}
	lifetimeAttempt uint64
	lastResult      string
}
type boundedRescueReservation struct {
	id                     uint64
	candidateID            uint64
	seq                    uint64
	key                    rescueEpochKey
	target                 uint8
	bytes                  uint64
	baseBytes              uint64
	burstBytes             uint64
	started                bool
	evidence               StreamRescueEvidence
	attemptID              uint64
	previousResult         string
	perRecordAttempt       uint64
	classificationEligible bool
	perRecordEligible      bool
	rollingBudgetEligible  bool
	targetLegAvailable     bool
	ackedAtDecision        bool
}

type streamBoundedRescueBudget struct {
	mu              sync.Mutex
	config          StreamBoundedRescueConfig
	chunkBytes      uint64
	now             func() time.Time
	monotonicStart  time.Time
	eventHook       func(StreamRescueEvent)
	closed          bool
	sessionID       SessionID
	ackedNext       uint64
	lastOriginal    uint64
	hasOriginal     bool
	nextReservation uint64
	nextAttempt     uint64
	nextCandidate   uint64
	originalEvents  []rescueByteEvent // txLoop is the sole writer; timestamps are monotonic
	acceptedEvents  rescueByteEventHeap
	windowOriginal  uint64
	windowAccepted  uint64
	acceptedByLeg   [2]uint64
	baseReserved    uint64
	burstReserved   uint64
	burstCommitted  uint64 // lifetime token use; never rolls over
	reservations    map[uint64]*boundedRescueReservation
	records         map[uint64]*boundedRescueRecordState // outstanding records plus ACK/write races only
	stats           StreamBoundedRescueStats
}

func newStreamBoundedRescueBudget(cfg StreamBoundedRescueConfig, chunkBytes uint64, now func() time.Time, hook func(StreamRescueEvent)) (*streamBoundedRescueBudget, error) {
	if !cfg.Enabled {
		return nil, errors.New("bounded rescue must be explicitly enabled")
	}
	if !validBoundedRescueBasisPoints(cfg.BudgetBasisPoints) {
		return nil, fmt.Errorf("unsupported bounded rescue ratio: %d basis points", cfg.BudgetBasisPoints)
	}
	if cfg.factorMode > boundedRescueFactorCR {
		return nil, fmt.Errorf("unsupported bounded rescue factor mode: %d", cfg.factorMode)
	}
	if chunkBytes == 0 || cfg.BurstBytes > chunkBytes {
		return nil, fmt.Errorf("bounded rescue burst %d exceeds one DATA chunk %d", cfg.BurstBytes, chunkBytes)
	}
	if now == nil {
		now = time.Now
	}
	var sessionID SessionID
	if _, err := rand.Read(sessionID[:]); err != nil {
		return nil, fmt.Errorf("create bounded rescue session identity: %w", err)
	}
	b := &streamBoundedRescueBudget{
		config: cfg, chunkBytes: chunkBytes, now: now, monotonicStart: time.Now(), eventHook: hook, sessionID: sessionID,
		reservations: make(map[uint64]*boundedRescueReservation),
		records:      make(map[uint64]*boundedRescueRecordState),
	}
	heap.Init(&b.acceptedEvents)
	b.stats.Enabled = true
	b.stats.BudgetBasisPoints = cfg.BudgetBasisPoints
	b.stats.Window = streamBoundedRescueWindow
	b.stats.BurstBytes = cfg.BurstBytes
	b.stats.PolicyVariant = cfg.factorMode.String()
	return b, nil
}

func validBoundedRescueBasisPoints(bp uint16) bool {
	switch bp {
	case 25, 50, 100, 300, 500, 909:
		return true
	default:
		return false
	}
}

func (b *streamBoundedRescueBudget) emit(ev StreamRescueEvent) {
	if b.eventHook != nil {
		if !b.monotonicStart.IsZero() {
			ev.MonotonicElapsedNS = time.Since(b.monotonicStart).Nanoseconds()
			ev.MonotonicTimeValid = true
		}
		b.eventHook(ev)
	}
}

// RecordOriginal accounts unique logical application payload once at the point
// it enters the TX ledger. Sequence numbers are monotonic for one Core session.
func (b *streamBoundedRescueBudget) RecordOriginal(sequence, bytes uint64, at time.Time) bool {
	if b == nil || bytes == 0 {
		return false
	}
	b.mu.Lock()
	if b.closed {
		b.stats.ClosedOriginalRecords++
		b.mu.Unlock()
		return false
	}
	if b.hasOriginal && sequence <= b.lastOriginal {
		b.stats.DuplicateOriginalRecords++
		b.mu.Unlock()
		return false
	}
	b.lastOriginal, b.hasOriginal = sequence, true
	b.originalEvents = append(b.originalEvents, rescueByteEvent{at: at, bytes: bytes})
	b.windowOriginal += bytes
	// ACK handling can win the narrow race after the TX ledger creates the
	// original record but before this budget manager records it. Preserve the
	// byte denominator, but do not retain rescue state for a sequence already
	// below the manager's cumulative ACK frontier.
	if sequence >= b.ackedNext {
		if _, ok := b.records[sequence]; !ok {
			b.records[sequence] = &boundedRescueRecordState{
				pendingByEpoch: make(map[rescueEpochKey]uint64),
				usedByEpoch:    make(map[rescueEpochKey]struct{}),
			}
		}
	}
	b.pruneLocked(at)
	b.mu.Unlock()
	return true
}

type boundedRescueAdmission struct {
	CandidateID   uint64
	ReservationID uint64
	Class         StreamRescueClass
	Admitted      bool
	Exempt        bool
	Reason        string
}

type streamRescueEligibilitySnapshot struct {
	classification bool
	perRecord      bool
	rollingBudget  bool
	target         bool
	acked          bool
	used           bool
	pending        bool
	recordPresent  bool
}

// eligibilityLocked reports independent C/R/W facts at one candidate instant.
// It is observational: the rolling window is read without pruning or changing
// state, and Reserve still applies the historical short-circuit order below.
func (b *streamBoundedRescueBudget) eligibilityLocked(e StreamRescueEvidence, payloadBytes uint64, at time.Time, class StreamRescueClass) streamRescueEligibilitySnapshot {
	record := b.records[e.Sequence]
	acked := e.Sequence < b.ackedNext || (record != nil && record.acked)
	out := streamRescueEligibilitySnapshot{
		classification: class == StreamRescueHealthyHOL || class == StreamRescueFailureRecovery || class == StreamRescueProbeData,
		target:         e.TargetAvailable,
		acked:          acked,
		recordPresent:  record != nil,
	}
	if record != nil {
		key := rescueEpochKey{originGeneration: e.OriginGeneration, epoch: e.RecoveryEpoch}
		_, out.used = record.usedByEpoch[key]
		_, out.pending = record.pendingByEpoch[key]
	}
	out.perRecord = !acked && record != nil && !record.acked && !out.used && !out.pending && payloadBytes > 0
	out.rollingBudget = b.rollingBudgetEligibleLocked(payloadBytes, at)
	return out
}

// rollingBudgetEligibleLocked uses the same 60-second window and quota math as
// Reserve, without pruning or changing the accounting state.
func (b *streamBoundedRescueBudget) rollingBudgetEligibleLocked(payloadBytes uint64, at time.Time) bool {
	if payloadBytes == 0 {
		return false
	}
	cutoff := at.Add(-streamBoundedRescueWindow)
	windowOriginal := uint64(0)
	for _, event := range b.originalEvents {
		if event.at.After(cutoff) {
			windowOriginal = saturatingAdd(windowOriginal, event.bytes)
		}
	}
	windowAccepted := uint64(0)
	for _, event := range b.acceptedEvents {
		if event.at.After(cutoff) {
			windowAccepted = saturatingAdd(windowAccepted, event.bytes)
		}
	}
	baseQuota := boundedRescueRatio(windowOriginal, b.config.BudgetBasisPoints)
	baseUsed := saturatingAdd(windowAccepted, b.baseReserved)
	baseAvailable := uint64(0)
	if baseQuota > baseUsed {
		baseAvailable = baseQuota - baseUsed
	}
	baseDebt := uint64(0)
	if baseUsed > baseQuota {
		baseDebt = baseUsed - baseQuota
	}
	burstAvailable := uint64(0)
	burstUsed := saturatingAdd(b.burstCommitted, b.burstReserved)
	if b.config.BurstBytes > burstUsed {
		burstAvailable = b.config.BurstBytes - burstUsed
	}
	if baseDebt >= burstAvailable {
		burstAvailable = 0
	} else {
		burstAvailable -= baseDebt
	}
	return payloadBytes <= saturatingAdd(baseAvailable, burstAvailable)
}

func (b *streamBoundedRescueBudget) annotateDecisionLocked(event *StreamRescueEvent, candidateID uint64, eligibility streamRescueEligibilitySnapshot, final, reason string) {
	event.CandidateID = candidateID
	event.PolicyVariant = b.config.factorMode.String()
	event.EligibilitySnapshot = true
	event.ClassificationEnabled = true
	event.ClassificationEligible = eligibility.classification
	event.PerRecordEligible = eligibility.perRecord
	event.PerRecordEnabled = b.config.factorMode != boundedRescueFactorCOnly
	event.RollingBudgetEligible = eligibility.rollingBudget
	event.RollingBudgetEnabled = b.config.factorMode == boundedRescueFactorFull
	event.TargetLegAvailable = eligibility.target
	event.AckedAtDecision = eligibility.acked
	event.AckedAtAttempt = eligibility.acked
	if eligibility.acked {
		event.AckOutcome = "already_acked_at_decision"
	} else {
		event.AckOutcome = "pending"
	}
	event.FinalDecision = final
	event.FinalDecisionReason = reason
	if reason != "" {
		event.Reason = reason
	}
}

func (b *streamBoundedRescueBudget) annotateReservationEventLocked(event *StreamRescueEvent, r *boundedRescueReservation, final, reason string) {
	if event == nil || r == nil {
		return
	}
	event.CandidateID = r.candidateID
	event.PolicyVariant = b.config.factorMode.String()
	event.EligibilitySnapshot = true
	event.ClassificationEnabled = true
	event.ClassificationEligible = r.classificationEligible
	event.PerRecordEligible = r.perRecordEligible
	event.PerRecordEnabled = b.config.factorMode != boundedRescueFactorCOnly
	event.RollingBudgetEligible = r.rollingBudgetEligible
	event.RollingBudgetEnabled = b.config.factorMode == boundedRescueFactorFull
	event.TargetLegAvailable = r.targetLegAvailable
	event.AckedAtDecision = r.ackedAtDecision
	if r.ackedAtDecision {
		event.AckOutcome = "already_acked_at_decision"
	} else {
		event.AckOutcome = "pending"
	}
	event.FinalDecision = final
	event.FinalDecisionReason = reason
}

func (b *streamBoundedRescueBudget) Reserve(e StreamRescueEvidence, payloadBytes uint64, at time.Time) boundedRescueAdmission {
	decision := boundedRescueAdmission{Class: ClassifyStreamRescue(e)}
	if b == nil {
		decision.Reason = "budget_disabled"
		return decision
	}
	b.mu.Lock()
	b.nextCandidate++
	candidateID := b.nextCandidate
	decision.CandidateID = candidateID
	b.stats.CandidateCount++
	class := decision.Class
	eligibility := streamRescueEligibilitySnapshot{}
	if b.eventHook != nil {
		eligibility = b.eligibilityLocked(e, payloadBytes, at, class)
		if record := b.records[e.Sequence]; record != nil {
			if len(record.candidateIDs) < streamBoundedRescueCandidateAckLinks {
				record.candidateIDs = append(record.candidateIDs, candidateID)
			} else {
				b.stats.CandidateAckLinkDropped++
			}
		}
	}
	event := StreamRescueEvent{At: at, SessionID: b.sessionID, CandidateID: candidateID,
		Evidence: e, Class: class, PayloadBytes: payloadBytes}
	finish := func(kind, final, reason string) {
		event.Kind = kind
		b.annotateDecisionLocked(&event, candidateID, eligibility, final, reason)
	}
	emit := func() {
		b.mu.Unlock()
		b.emit(event)
	}
	if b.closed {
		decision.Reason = "session_closed"
		b.rejectReasonLocked(decision.Reason)
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	if decision.Class == StreamRescueFailureRecovery {
		b.stats.FailureRecoveryExempted++
		decision.Exempt, decision.Admitted, decision.Reason = true, true, "positive_owner_invalidation"
		event.RecoveryExempted = true
		finish("failure_recovery_exempt", "exempted", decision.Reason)
		emit()
		return decision
	}
	if decision.Class == StreamRescueProbeData {
		b.stats.ProbeDataExempted++
		decision.Exempt, decision.Admitted, decision.Reason = true, true, "recovery_probe_data"
		finish("probe_data_exempt", "exempted", decision.Reason)
		emit()
		return decision
	}
	if decision.Class != StreamRescueHealthyHOL {
		b.stats.UnclassifiedRejected++
		decision.Reason = "classification_not_healthy_hol"
		b.rejectReasonLocked(decision.Reason)
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	b.stats.EligibleCount++
	if e.Sequence < b.ackedNext {
		b.stats.StaleOrAckedRejected++
		decision.Reason = "already_acked"
		b.rejectReasonLocked(decision.Reason)
		event.AckedAtAttempt = true
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	record := b.records[e.Sequence]
	if record == nil || record.acked {
		b.stats.StaleOrAckedRejected++
		decision.Reason = "missing_or_acked_original"
		b.rejectReasonLocked(decision.Reason)
		event.AckedAtAttempt = record != nil && record.acked
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	key := rescueEpochKey{originGeneration: e.OriginGeneration, epoch: e.RecoveryEpoch}
	_, used := record.usedByEpoch[key]
	_, pending := record.pendingByEpoch[key]
	if b.config.factorMode != boundedRescueFactorCOnly && used {
		b.stats.PerRecordRejected++
		decision.Reason = "per_record_epoch_limit"
		b.rejectReasonLocked(decision.Reason)
		event.PerRecordAttempt, event.PreviousResult = record.lifetimeAttempt, record.lastResult
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	if pending {
		decision.Reason = "per_record_epoch_reservation_pending"
		if b.config.factorMode != boundedRescueFactorCOnly {
			b.stats.PerRecordRejected++
		}
		b.rejectReasonLocked(decision.Reason)
		event.PerRecordAttempt, event.PreviousResult = record.lifetimeAttempt+1, record.lastResult
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	if payloadBytes == 0 {
		decision.Reason = "empty_payload"
		b.rejectReasonLocked(decision.Reason)
		event.PreviousResult = record.lastResult
		finish("rescue_rejected", "rejected", decision.Reason)
		emit()
		return decision
	}
	b.pruneLocked(at)
	basePart, burstPart := payloadBytes, uint64(0)
	if b.config.factorMode == boundedRescueFactorFull {
		baseQuota := boundedRescueRatio(b.windowOriginal, b.config.BudgetBasisPoints)
		baseUsed := saturatingAdd(b.windowAccepted, b.baseReserved)
		baseAvailable := uint64(0)
		if baseQuota > baseUsed {
			baseAvailable = baseQuota - baseUsed
		}
		baseDebt := uint64(0)
		if baseUsed > baseQuota {
			baseDebt = baseUsed - baseQuota
		}
		burstAvailable := uint64(0)
		burstUsed := saturatingAdd(b.burstCommitted, b.burstReserved)
		if b.config.BurstBytes > burstUsed {
			burstAvailable = b.config.BurstBytes - burstUsed
		}
		if baseDebt >= burstAvailable {
			burstAvailable = 0
		} else {
			burstAvailable -= baseDebt
		}
		available := saturatingAdd(baseAvailable, burstAvailable)
		if payloadBytes > available {
			b.stats.BudgetRejected++
			decision.Reason = "rolling_budget_exhausted"
			b.rejectReasonLocked(decision.Reason)
			event.PerRecordAttempt, event.PreviousResult = record.lifetimeAttempt+1, record.lastResult
			finish("rescue_rejected", "rejected", decision.Reason)
			emit()
			return decision
		}
		if basePart > baseAvailable {
			basePart = baseAvailable
		}
		burstPart = payloadBytes - basePart
	}
	b.nextReservation++
	id := b.nextReservation
	reservation := &boundedRescueReservation{
		id: id, seq: e.Sequence, key: key, target: e.TargetLeg, bytes: payloadBytes,
		baseBytes: basePart, burstBytes: burstPart, evidence: e, candidateID: candidateID,
		previousResult: record.lastResult, perRecordAttempt: record.lifetimeAttempt + 1,
		classificationEligible: eligibility.classification, perRecordEligible: eligibility.perRecord,
		rollingBudgetEligible: eligibility.rollingBudget, targetLegAvailable: eligibility.target,
		ackedAtDecision: eligibility.acked,
	}
	b.reservations[id] = reservation
	record.pendingByEpoch[key] = id
	b.baseReserved += basePart
	b.burstReserved += burstPart
	b.stats.AdmittedCount++
	b.stats.ReservedBytes += payloadBytes
	b.stats.ReservedBaseBytes += basePart
	b.stats.ReservedBurstBytes += burstPart
	decision.ReservationID, decision.Admitted = id, true
	decision.Reason = "budget_reserved"
	if b.config.factorMode != boundedRescueFactorFull {
		decision.Reason = "experimental_variant_admitted_without_rolling_budget"
	}
	event.ReservationID, event.PreviousResult, event.AckedAtAttempt = id, record.lastResult, record.acked
	finish("rescue_reserved", "admitted", decision.Reason)
	emit()
	return decision
}

// BeginWrite commits the one-per-epoch attempt at the first physical Carrier
// Write boundary. ACK may race with queueing; an ACK already observed here
// cancels the queued copy before it reaches the Carrier.
func (b *streamBoundedRescueBudget) BeginWrite(id uint64, at time.Time) bool {
	started, event := b.beginWriteDeferred(id, at)
	if event != nil {
		b.emit(*event)
	}
	return started
}

// beginWriteDeferred performs the transition without invoking the observer.
// The engine may call it while holding the TX ledger lock to serialize the
// physical write boundary against cumulative ACK retirement, then emit after
// releasing that lock.
func (b *streamBoundedRescueBudget) beginWriteDeferred(id uint64, at time.Time) (bool, *StreamRescueEvent) {
	if b == nil || id == 0 {
		return false, nil
	}
	var event *StreamRescueEvent
	b.mu.Lock()
	r := b.reservations[id]
	if b.closed && r != nil && !r.started {
		b.releaseLocked(r, "session_closed_before_carrier_write")
		event = &StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "reservation_released", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: "session_closed_before_carrier_write"}
		b.annotateReservationEventLocked(event, r, "released", event.Reason)
		b.mu.Unlock()
		return false, event
	}
	if r == nil || r.started || r.seq < b.ackedNext {
		if r != nil && !r.started {
			b.releaseLocked(r, "acked_before_carrier_write")
			event = &StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "reservation_released", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: "acked_before_carrier_write"}
			b.annotateReservationEventLocked(event, r, "released", event.Reason)
		}
		b.mu.Unlock()
		return false, event
	}
	record := b.records[r.seq]
	if record == nil || record.acked {
		b.releaseLocked(r, "acked_before_carrier_write")
		event = &StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "reservation_released", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: "acked_before_carrier_write"}
		b.annotateReservationEventLocked(event, r, "released", event.Reason)
		b.mu.Unlock()
		return false, event
	}
	if _, used := record.usedByEpoch[r.key]; used && b.config.factorMode != boundedRescueFactorCOnly {
		b.releaseLocked(r, "per_record_epoch_already_committed")
		event = &StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "reservation_released", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: "per_record_epoch_already_committed"}
		b.annotateReservationEventLocked(event, r, "released", event.Reason)
		b.mu.Unlock()
		return false, event
	}
	r.started = true
	b.nextAttempt++
	r.attemptID = b.nextAttempt
	record.usedByEpoch[r.key] = struct{}{}
	record.lifetimeAttempt++
	b.stats.CommittedAttemptCount++
	b.stats.LifetimeRescueAttempts++
	if record.lifetimeAttempt > b.stats.LifetimeMaxPerRecord {
		b.stats.LifetimeMaxPerRecord = record.lifetimeAttempt
	}
	if len(record.usedByEpoch) > 1 {
		b.stats.CrossEpochRecordCount++
	}
	b.stats.CarrierAttemptBytes += r.bytes
	if r.target < 2 {
		b.stats.AttemptedByTargetLeg[r.target] += r.bytes
	}
	r.perRecordAttempt = record.lifetimeAttempt
	event = &StreamRescueEvent{At: at, SessionID: b.sessionID, AttemptID: r.attemptID, PerRecordAttempt: r.perRecordAttempt, PreviousResult: r.previousResult, AckedAtAttempt: record.acked, Kind: "carrier_write_started", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes}
	b.annotateReservationEventLocked(event, r, "write_started", "carrier_write_started")
	b.mu.Unlock()
	return true, event
}

func (b *streamBoundedRescueBudget) reservationEvidence(id uint64) (StreamRescueEvidence, bool) {
	if b == nil || id == 0 {
		return StreamRescueEvidence{}, false
	}
	b.mu.Lock()
	r := b.reservations[id]
	if r == nil || r.started {
		b.mu.Unlock()
		return StreamRescueEvidence{}, false
	}
	e := r.evidence
	b.mu.Unlock()
	return e, true
}

// RevalidateBeforeWrite closes the decision-to-writer gap. A queued healthy
// HOL reservation is either still the same eligible record/owner/epoch, is
// positively reclassified as a recovery/probe exemption, or is canceled before
// any Carrier bytes are accepted.
func (b *streamBoundedRescueBudget) RevalidateBeforeWrite(id uint64, current StreamRescueEvidence, at time.Time) (StreamRescueClass, uint64, bool) {
	if b == nil || id == 0 {
		return StreamRescueOtherCoreRetry, 0, false
	}
	var event StreamRescueEvent
	b.mu.Lock()
	r := b.reservations[id]
	if r == nil || r.started || b.closed {
		if r != nil && !r.started {
			b.releaseLocked(r, "invalid_before_carrier_write")
		}
		b.mu.Unlock()
		return StreamRescueDuplicateOrRejected, 0, false
	}
	class := ClassifyStreamRescue(current)
	if class == StreamRescueFailureRecovery || class == StreamRescueProbeData {
		if class == StreamRescueFailureRecovery {
			b.stats.FailureRecoveryExempted++
		} else {
			b.stats.ProbeDataExempted++
		}
		b.releaseAccountingLocked(r)
		delete(b.reservations, id)
		if record := b.records[r.seq]; record != nil {
			if pendingID, ok := record.pendingByEpoch[r.key]; ok && pendingID == id {
				delete(record.pendingByEpoch, r.key)
			}
			b.maybeDeleteRecordLocked(r.seq, record)
		}
		b.stats.ReleasedBytes += r.bytes
		reason := "reclassified_positive_owner_invalidation"
		if class == StreamRescueProbeData {
			reason = "reclassified_current_recovery_probe"
		}
		event = StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "carrier_write_reclassified_exempt", Evidence: current, Class: class, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: reason, RecoveryExempted: class == StreamRescueFailureRecovery}
		b.annotateReservationEventLocked(&event, r, "reclassified_exempt", reason)
		b.mu.Unlock()
		b.emit(event)
		return class, 0, true
	}
	if class == StreamRescueHealthyHOL && current.Sequence == r.evidence.Sequence &&
		current.OwnerLeg == r.evidence.OwnerLeg && current.RecordOwnerLeg == r.evidence.RecordOwnerLeg &&
		current.TargetLeg == r.target && current.OriginGeneration == r.key.originGeneration &&
		current.RecoveryEpoch == r.key.epoch {
		b.mu.Unlock()
		return class, id, true
	}
	b.releaseLocked(r, "stale_or_reclassified_before_carrier_write")
	event = StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "rescue_rejected", Evidence: current, Class: class, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: "stale_or_reclassified_before_carrier_write"}
	b.annotateReservationEventLocked(&event, r, "rejected", event.Reason)
	b.mu.Unlock()
	b.emit(event)
	return class, 0, false
}

// Settle charges only payload bytes accepted by the local Carrier Write. It is
// valid after ACK retirement because reservation ownership is independent of
// the TX ledger record map.
func (b *streamBoundedRescueBudget) Settle(id uint64, acceptedBytes uint64, writeErr error, at time.Time) error {
	if b == nil || id == 0 {
		return nil
	}
	var event StreamRescueEvent
	b.mu.Lock()
	r := b.reservations[id]
	if r == nil {
		b.mu.Unlock()
		return errors.New("bounded rescue reservation not found")
	}
	if !r.started {
		b.mu.Unlock()
		return errors.New("bounded rescue settled without reaching the Carrier Write boundary")
	}
	overAccepted := acceptedBytes > r.bytes
	if overAccepted {
		b.stats.OverAcceptedBytes = saturatingAdd(b.stats.OverAcceptedBytes, acceptedBytes-r.bytes)
		acceptedBytes = r.bytes
	}
	if !r.started && acceptedBytes != 0 {
		b.mu.Unlock()
		return errors.New("Carrier accepted bytes before bounded rescue write boundary")
	}
	if acceptedBytes > 0 {
		heap.Push(&b.acceptedEvents, rescueByteEvent{at: at, bytes: acceptedBytes, target: r.target})
		b.windowAccepted += acceptedBytes
		if r.target < 2 {
			b.acceptedByLeg[r.target] += acceptedBytes
		}
		b.stats.CarrierAcceptedBytes += acceptedBytes
		if acceptedBytes < r.bytes {
			b.stats.PartialAcceptedBytes += acceptedBytes
		}
		burstAccepted := uint64(0)
		baseAccepted := acceptedBytes
		if baseAccepted > r.baseBytes {
			baseAccepted = r.baseBytes
		}
		if acceptedBytes > baseAccepted {
			burstAccepted = acceptedBytes - baseAccepted
		}
		b.burstCommitted += burstAccepted
	}
	unused := r.bytes - acceptedBytes
	b.releaseAccountingLocked(r)
	delete(b.reservations, id)
	if record := b.records[r.seq]; record != nil {
		if writeErr != nil {
			record.lastResult = "carrier_write_error"
		} else if acceptedBytes < r.bytes {
			record.lastResult = "partial_carrier_acceptance"
		} else {
			record.lastResult = "carrier_write_accepted"
		}
		if pendingID, ok := record.pendingByEpoch[r.key]; ok && pendingID == id {
			delete(record.pendingByEpoch, r.key)
		}
		b.maybeDeleteRecordLocked(r.seq, record)
	}
	b.stats.ReleasedBytes += unused
	b.pruneLocked(at)
	reason := "carrier_write_complete"
	if writeErr != nil {
		reason = "carrier_write_error"
	} else if acceptedBytes < r.bytes {
		reason = "partial_carrier_acceptance"
	}
	if overAccepted {
		reason = "carrier_overaccepted_bytes_clamped"
	}
	event = StreamRescueEvent{At: at, SessionID: b.sessionID, AttemptID: r.attemptID, PerRecordAttempt: r.perRecordAttempt, PreviousResult: r.previousResult, AckedAtAttempt: r.seq < b.ackedNext, Kind: "carrier_write_settled", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, AcceptedBytes: acceptedBytes, Reason: reason}
	final := "write_settled"
	if writeErr != nil || acceptedBytes < r.bytes {
		final = "write_failed_or_partial"
	}
	b.annotateReservationEventLocked(&event, r, final, reason)
	b.mu.Unlock()
	b.emit(event)
	if overAccepted {
		return errors.New("Carrier accepted more bounded Rescue payload than the reservation")
	}
	return nil
}

// Release is legal only before the Carrier Write boundary. Once a write has
// started, callers must settle its accepted-byte outcome instead.
func (b *streamBoundedRescueBudget) Release(id uint64, reason string, at time.Time) bool {
	if b == nil || id == 0 {
		return true
	}
	var event StreamRescueEvent
	b.mu.Lock()
	r := b.reservations[id]
	if r == nil || r.started {
		b.mu.Unlock()
		return false
	}
	event = StreamRescueEvent{At: at, SessionID: b.sessionID, Kind: "reservation_released", Evidence: r.evidence, Class: StreamRescueHealthyHOL, ReservationID: id, PayloadBytes: r.bytes, PreviousResult: r.previousResult, Reason: reason}
	b.annotateReservationEventLocked(&event, r, "released", reason)
	b.releaseLocked(r, reason)
	b.mu.Unlock()
	b.emit(event)
	return true
}

func (b *streamBoundedRescueBudget) releaseLocked(r *boundedRescueReservation, reason string) {
	if r == nil || b.reservations[r.id] != r {
		return
	}
	b.releaseAccountingLocked(r)
	delete(b.reservations, r.id)
	if state := b.records[r.seq]; state != nil {
		if id, ok := state.pendingByEpoch[r.key]; ok && id == r.id {
			delete(state.pendingByEpoch, r.key)
		}
		b.maybeDeleteRecordLocked(r.seq, state)
	}
	b.stats.ReleasedBytes += r.bytes
	b.rejectReasonLocked("released:" + reason)
}

func (b *streamBoundedRescueBudget) releaseAccountingLocked(r *boundedRescueReservation) {
	if b.baseReserved >= r.baseBytes {
		b.baseReserved -= r.baseBytes
	} else {
		b.baseReserved = 0
	}
	if b.burstReserved >= r.burstBytes {
		b.burstReserved -= r.burstBytes
	} else {
		b.burstReserved = 0
	}
}

func (b *streamBoundedRescueBudget) AckThrough(next uint64, at time.Time) {
	if b == nil {
		return
	}
	var events []StreamRescueEvent
	b.mu.Lock()
	if next > b.ackedNext {
		b.ackedNext = next
	}
	for seq, record := range b.records {
		if seq < b.ackedNext {
			record.acked = true
			for _, candidateID := range record.candidateIDs {
				events = append(events, StreamRescueEvent{
					At: at, SessionID: b.sessionID, CandidateID: candidateID,
					PolicyVariant: b.config.factorMode.String(), Kind: "candidate_ack_outcome",
					Evidence: StreamRescueEvidence{Sequence: seq, AckedNext: b.ackedNext},
					Class:    StreamRescueDuplicateOrRejected, AckedAtAttempt: true, AckedAtDecision: true,
					AckOutcome: "acked_after_candidate", FinalDecision: "data_acked",
					FinalDecisionReason: "cumulative_ack_advanced_past_candidate_sequence",
				})
			}
			record.candidateIDs = nil
			b.maybeDeleteRecordLocked(seq, record)
		}
	}
	b.mu.Unlock()
	for _, event := range events {
		b.emit(event)
	}
}

func (b *streamBoundedRescueBudget) maybeDeleteRecordLocked(sequence uint64, state *boundedRescueRecordState) {
	if state != nil && state.acked && len(state.pendingByEpoch) == 0 {
		delete(b.records, sequence)
	}
}

func (b *streamBoundedRescueBudget) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.closed = true
	b.stats.Closed = true
	b.mu.Unlock()
}

func (b *streamBoundedRescueBudget) Snapshot(at time.Time) StreamBoundedRescueStats {
	if b == nil {
		return StreamBoundedRescueStats{}
	}
	b.mu.Lock()
	b.pruneLocked(at)
	out := b.stats
	out.Enabled = true
	out.Closed = b.closed
	out.Window = streamBoundedRescueWindow
	out.BudgetBasisPoints = b.config.BudgetBasisPoints
	out.PolicyVariant = b.config.factorMode.String()
	out.WindowOriginalBytes = b.windowOriginal
	out.WindowCarrierAcceptedBytes = b.windowAccepted
	out.WindowBaseQuotaBytes = boundedRescueRatio(b.windowOriginal, b.config.BudgetBasisPoints)
	baseUsed := saturatingAdd(b.windowAccepted, b.baseReserved)
	if baseUsed > out.WindowBaseQuotaBytes {
		out.WindowBudgetDebtBytes = baseUsed - out.WindowBaseQuotaBytes
	}
	out.ReservedBaseBytes = b.baseReserved
	out.ReservedBurstBytes = b.burstReserved
	out.ReservedBytes = saturatingAdd(b.baseReserved, b.burstReserved)
	out.BurstBytes = b.config.BurstBytes
	out.BurstCommittedBytes = b.burstCommitted
	out.OutstandingReservations = len(b.reservations)
	out.AcceptedByTargetLeg = b.acceptedByLeg
	out.RejectedReasons = make(map[string]uint64, len(b.stats.RejectedReasons))
	for reason, count := range b.stats.RejectedReasons {
		out.RejectedReasons[reason] = count
	}
	b.mu.Unlock()
	return out
}

func (b *streamBoundedRescueBudget) rejectReasonLocked(reason string) {
	if b.stats.RejectedReasons == nil {
		b.stats.RejectedReasons = make(map[string]uint64)
	}
	b.stats.RejectedReasons[reason]++
}

func (b *streamBoundedRescueBudget) pruneLocked(now time.Time) {
	cutoff := now.Add(-streamBoundedRescueWindow)
	for len(b.originalEvents) > 0 && !b.originalEvents[0].at.After(cutoff) {
		event := b.originalEvents[0]
		b.originalEvents[0] = rescueByteEvent{}
		b.originalEvents = b.originalEvents[1:]
		if b.windowOriginal >= event.bytes {
			b.windowOriginal -= event.bytes
		} else {
			b.windowOriginal = 0
		}
	}
	for b.acceptedEvents.Len() > 0 {
		event := b.acceptedEvents[0]
		if event.at.After(cutoff) {
			break
		}
		heap.Pop(&b.acceptedEvents)
		if b.windowAccepted >= event.bytes {
			b.windowAccepted -= event.bytes
		} else {
			b.windowAccepted = 0
		}
		if event.target < 2 {
			if b.acceptedByLeg[event.target] >= event.bytes {
				b.acceptedByLeg[event.target] -= event.bytes
			} else {
				b.acceptedByLeg[event.target] = 0
			}
		}
	}
}

func boundedRescueRatio(bytes uint64, basisPoints uint16) uint64 {
	// Division before multiplication avoids overflowing for large counters.
	return (bytes/10000)*uint64(basisPoints) + ((bytes%10000)*uint64(basisPoints))/10000
}

func saturatingAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

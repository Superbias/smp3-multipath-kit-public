package smp3core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const v27BlackholeChunkSize = 64 << 10
const v27BlackholeStateWatchInterval = time.Millisecond
const v27BlackholeMaxRuntime = 30 * time.Second

type v27BlackholeSource struct {
	total   int64
	rateBPS int64
	started time.Time
	offset  uint64
	read    atomic.Uint64
}

func (s *v27BlackholeSource) Read(p []byte) (int, error) {
	n := len(p)
	if n > v27BlackholeChunkSize {
		n = v27BlackholeChunkSize
	}
	if left := s.total - int64(s.offset); left <= 0 {
		return 0, io.EOF
	} else if int64(n) > left {
		n = int(left)
	}
	for i := 0; i < n; i++ {
		p[i] = byte((s.offset + uint64(i)) % 251)
	}
	s.offset += uint64(n)
	s.read.Add(uint64(n))
	if s.rateBPS > 0 {
		if s.started.IsZero() {
			s.started = time.Now()
		}
		target := s.started.Add(time.Duration(float64(s.offset) * float64(time.Second) / float64(s.rateBPS)))
		if wait := time.Until(target); wait > 0 {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			<-timer.C
		}
	}
	return n, nil
}

type v27BlackholeSink struct {
	bytes atomic.Uint64
	hash  hashWriter
}

// hashWriter is deliberately tiny so the receiver goroutine remains the sole
// writer of the hash state while byte counts are available to the sampler.
type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (s *v27BlackholeSink) Write(p []byte) (int, error) {
	s.bytes.Add(uint64(len(p)))
	if s.hash != nil {
		_, _ = s.hash.Write(p)
	}
	return len(p), nil
}

type v27BlackholeCopyResult struct {
	bytes                int64
	err                  error
	terminalErr          error
	finish               time.Time
	pathFeedbackForClose bool
	feedbackAgedForClose bool
}

type v27BlackholeReadObserver struct {
	net.Conn
	mu          sync.Mutex
	terminalErr error
}

func (r *v27BlackholeReadObserver) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	if err != nil {
		r.mu.Lock()
		r.terminalErr = err
		r.mu.Unlock()
	}
	return n, err
}

func (r *v27BlackholeReadObserver) terminalError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminalErr
}

type v27BlackholeAdmission struct {
	state              string
	reason             string
	proofs             int
	proofAfter         uint64
	proofBytes         uint64
	byteLimit          uint64
	limit              int
	generation         uint64
	probePending       bool
	probePendingAt     time.Time
	probeSequence      uint64
	probeSequenceSet   bool
	probeExpired       bool
	probeExpiredSeq    uint64
	nextProbe          time.Time
	since              time.Time
	observedAt         time.Time
	probeTimeoutTiming map[string]any
}

func v27BlackholeSendAdmission(stop <-chan struct{}, changes chan<- v27BlackholeAdmission, point v27BlackholeAdmission) bool {
	select {
	case changes <- point:
		return true
	case <-stop:
		return false
	}
}

func v27BlackholeAdmissionPoint(engine *StreamEngine, id uint8) v27BlackholeAdmission {
	engine.admission.mu.Lock()
	defer engine.admission.mu.Unlock()
	leg := engine.admission.legs[id]
	state := "normal"
	if leg.restricted {
		state = "restricted"
	} else if leg.recovering {
		state = "recovering"
	}
	return v27BlackholeAdmission{
		state: state, reason: leg.reason, proofs: leg.proofs,
		proofAfter: leg.proofAfter, proofBytes: leg.proofBytes,
		byteLimit: leg.byteLimit, limit: leg.limit,
		generation:   engine.legGeneration[id].Load(),
		probePending: leg.probePending, probePendingAt: leg.probePendingAt,
		probeSequence: leg.probeSequence, probeSequenceSet: leg.probeSequenceSet,
		probeExpired: leg.probeExpired, probeExpiredSeq: leg.probeExpiredSequence,
		nextProbe: leg.nextProbe, since: leg.since,
	}
}

func (a v27BlackholeAdmission) json() map[string]any {
	return map[string]any{
		"state": a.state, "reason": a.reason, "proofs": a.proofs,
		"proof_after": a.proofAfter, "proof_bytes": a.proofBytes,
		"byte_limit": a.byteLimit, "limit": a.limit, "generation": a.generation,
		"probe_pending": a.probePending, "probe_pending_at": a.probePendingAt,
		"probe_sequence": a.probeSequence, "probe_sequence_set": a.probeSequenceSet,
		"probe_expired": a.probeExpired, "probe_expired_sequence": a.probeExpiredSeq,
		"next_probe": a.nextProbe, "since": a.since,
		"probe_timeout_timing": a.probeTimeoutTiming,
	}
}

func v27BlackholeProbeTimeoutTiming(engine *StreamEngine, id uint8, observedAt, started time.Time) map[string]any {
	observedElapsed := observedAt.Sub(started).Nanoseconds()
	candidate := engine.txLedger.FrontierCandidate(observedAt, engine.cfg.RetransmitTimeout)
	if !candidate.exists || candidate.record == nil {
		return map[string]any{"available": false, "observed_elapsed_ns": observedElapsed, "reason": "no outstanding frontier record at observation"}
	}
	ledger := engine.txLedger
	ledger.mu.Lock()
	record := candidate.record
	stillOutstanding := ledger.outstanding[record.sequence] == record
	createdAt := record.createdAt
	assignedAt := record.assignedAt
	firstWriteAt := record.firstWriteAt
	frontierAt := record.frontierAt
	lastSentAt := record.lastSentAt
	sendCount := record.sendCount
	ordinaryOwner := record.ordinaryOwner
	inTransit := record.inTransit
	transitLeg := record.transitLeg
	ackedNext := ledger.ackedNext
	nextSequence := ledger.nextSequence
	ledger.mu.Unlock()
	reference := engine.txLedger.probeReference(candidate.record, true)
	if reference.IsZero() {
		return map[string]any{"available": false, "observed_elapsed_ns": observedElapsed, "reason": "frontier probe reference unavailable"}
	}
	budget := engine.feedbackHorizon(id, observedAt)
	if other := engine.feedbackHorizon(1-id, observedAt); other > budget {
		budget = other
	}
	if budget < streamProbeBudget {
		budget = streamProbeBudget
	}
	age := observedAt.Sub(reference)
	elapsedAt := func(at time.Time) any {
		if at.IsZero() {
			return nil
		}
		return at.Sub(started).Nanoseconds()
	}
	return map[string]any{
		"available": true, "observed_elapsed_ns": observedElapsed,
		"frontier_sequence":    candidate.record.Sequence(),
		"reference_elapsed_ns": reference.Sub(started).Nanoseconds(),
		"age_ns":               age.Nanoseconds(), "budget_ns": budget.Nanoseconds(),
		"overdue_by_ns":                 age.Nanoseconds() - budget.Nanoseconds(),
		"record_still_outstanding":      stillOutstanding,
		"record_created_elapsed_ns":     elapsedAt(createdAt),
		"record_assigned_elapsed_ns":    elapsedAt(assignedAt),
		"record_first_write_elapsed_ns": elapsedAt(firstWriteAt),
		"record_frontier_elapsed_ns":    elapsedAt(frontierAt),
		"record_last_sent_elapsed_ns":   elapsedAt(lastSentAt),
		"record_send_count":             sendCount,
		"record_ordinary_owner":         ordinaryOwner,
		"record_in_transit":             inTransit,
		"record_transit_leg":            transitLeg,
		"ledger_acked_next":             ackedNext,
		"ledger_next_sequence":          nextSequence,
	}
}

func v27BlackholeWatchAdmission(engine *StreamEngine, legID uint8, started time.Time, stop <-chan struct{}, changes chan<- v27BlackholeAdmission, transitions chan<- v27BlackholeAdmission, done chan<- struct{}) {
	defer close(done)
	// Capture recovery timeout transitions close to the point at which the
	// maintenance loop publishes them, without adding a hook to production Core.
	ticker := time.NewTicker(v27BlackholeStateWatchInterval)
	defer ticker.Stop()
	var last v27BlackholeAdmission
	var lastRecoveringFrontier map[string]any
	var lastRecoveringFrontierAt time.Time
	initialized := false
	proofObserved := false
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			point := v27BlackholeAdmissionPoint(engine, legID)
			point.observedAt = time.Now()
			if point.state == "recovering" {
				lastRecoveringFrontier = v27BlackholeProbeTimeoutTiming(engine, legID, point.observedAt, started)
				lastRecoveringFrontierAt = point.observedAt
			}
			if point.state == "restricted" && point.reason == "probe_timeout" {
				postTransition := v27BlackholeProbeTimeoutTiming(engine, legID, point.observedAt, started)
				postTransition["observation_mode"] = "poll_after_transition"
				postTransition["last_recovering_poll"] = lastRecoveringFrontier
				if !lastRecoveringFrontierAt.IsZero() {
					postTransition["last_recovering_poll_gap_ns"] = point.observedAt.Sub(lastRecoveringFrontierAt).Nanoseconds()
				}
				if lastSequence, ok := lastRecoveringFrontier["frontier_sequence"].(uint64); ok {
					if currentSequence, currentOK := postTransition["frontier_sequence"].(uint64); currentOK {
						postTransition["same_frontier_as_last_recovering_poll"] = lastSequence == currentSequence
					}
				}
				point.probeTimeoutTiming = postTransition
			}
			changed := !initialized || point.state != last.state || point.reason != last.reason || point.proofAfter != last.proofAfter
			newProof := point.state == "recovering" && point.proofs > 0 && !proofObserved
			if changed || newProof {
				if !v27BlackholeSendAdmission(stop, changes, point) {
					return
				}
			}
			if !initialized || point.state != last.state || point.reason != last.reason {
				select {
				case transitions <- point:
				case <-stop:
					return
				}
			}
			if newProof {
				proofObserved = true
			}
			last, initialized = point, true
		}
	}
}

type v27BlackholeStateTransition struct {
	from string
	to   string
	prev v27BlackholeAdmission
	next v27BlackholeAdmission
}

type v27BlackholeRecoveryGateResult struct {
	OK                     bool     `json:"ok"`
	Failure                string   `json:"failure,omitempty"`
	StateSequence          []string `json:"state_sequence"`
	RecoveryRetryCount     int      `json:"recovery_retry_count"`
	RecoveryRetryLimit     int      `json:"recovery_retry_limit"`
	RecoveryRetryReasons   []string `json:"recovery_retry_reasons"`
	RetryEventIndices      []uint64 `json:"retry_event_indices"`
	RetryCooldownElapsedNS []int64  `json:"retry_cooldown_elapsed_ns"`
}

// v27BlackholeRecoveryStateGate accepts only the approved bounded graph. The
// one-cycle path remains valid. recovering->restricted may follow either the
// existing low-service policy with a matching telemetry snapshot, or an
// existing frontier timeout with a fresh, same-frontier observation proving
// the timeout budget had elapsed. The retry count is bounded by the 30-second
// harness horizon and the existing five-second cooldown; it is not a new Core
// retry cap.
func v27BlackholeMapBool(values map[string]any, key string) (bool, bool) {
	value, ok := values[key].(bool)
	return value, ok
}

func v27BlackholeMapInt64(values map[string]any, key string) (int64, bool) {
	value, ok := values[key].(int64)
	return value, ok
}

func v27BlackholeMapUint64(values map[string]any, key string) (uint64, bool) {
	value, ok := values[key].(uint64)
	return value, ok
}

func v27BlackholeProbeTimeoutEvidence(
	point v27BlackholeAdmission,
	transition v27BlackholeStateTransition,
	started time.Time,
	faultLeg uint8,
	faultGeneration uint64,
	traceEvents []streamRecoveryTraceEvent,
) (string, bool) {
	timing := point.probeTimeoutTiming
	if timing == nil {
		return "probe_timeout edge has no timing evidence", false
	}
	pre, ok := timing["last_recovering_poll"].(map[string]any)
	if !ok {
		return "probe_timeout edge has no immediately preceding recovering poll", false
	}
	preAvailable, preOK := v27BlackholeMapBool(pre, "available")
	preOutstanding, outOK := v27BlackholeMapBool(pre, "record_still_outstanding")
	sequence, seqOK := v27BlackholeMapUint64(pre, "frontier_sequence")
	preAckedNext, preAckOK := v27BlackholeMapUint64(pre, "ledger_acked_next")
	preAge, ageOK := v27BlackholeMapInt64(pre, "age_ns")
	preBudget, budgetOK := v27BlackholeMapInt64(pre, "budget_ns")
	preOverdue, overdueOK := v27BlackholeMapInt64(pre, "overdue_by_ns")
	preObserved, preTimeOK := v27BlackholeMapInt64(pre, "observed_elapsed_ns")
	postObserved, postTimeOK := v27BlackholeMapInt64(timing, "observed_elapsed_ns")
	gap, gapOK := v27BlackholeMapInt64(timing, "last_recovering_poll_gap_ns")
	if !preOK || !outOK || !seqOK || !preAckOK || !ageOK || !budgetOK || !overdueOK || !preTimeOK || !postTimeOK || !gapOK {
		return "probe_timeout edge has incomplete typed timing fields", false
	}
	if !preAvailable || !preOutstanding || sequence == 0 || preAckedNext != sequence {
		return "probe_timeout edge lacks an outstanding pre-action ACK frontier", false
	}
	if preBudget < int64(streamProbeBudget) || preAge < preBudget || preOverdue < 0 {
		return fmt.Sprintf("probe_timeout edge lacks an elapsed Core timeout budget: age=%d budget=%d", preAge, preBudget), false
	}
	transitionElapsed := transition.next.observedAt.Sub(started).Nanoseconds()
	if preObserved < transition.prev.observedAt.Sub(started).Nanoseconds() || preObserved >= postObserved || postObserved != transitionElapsed || postObserved-preObserved != gap {
		return "probe_timeout timing polls do not bracket the state transition", false
	}
	if gap <= 0 || gap > int64(5*v27BlackholeStateWatchInterval) {
		return fmt.Sprintf("probe_timeout transition is not tightly bracketed by polls: gap=%s", time.Duration(gap)), false
	}
	// A post-transition sample may follow both the timeout commit and another
	// ACK, so it can legitimately observe a newer frontier. Certify the timeout
	// at its actual commit boundary instead of requiring the later sample to
	// retain the old sequence. Applied=true is emitted only after Core's locked
	// helper revalidates outstanding[ackedNext], generation, and recovery epoch.
	decisionsByIndex := make(map[uint64]streamRecoveryTraceEvent, len(traceEvents))
	for _, event := range traceEvents {
		if event.Kind == "probe_timeout_decision" {
			decisionsByIndex[event.EventIndex] = event
		}
	}
	matchedActions := 0
	for _, action := range traceEvents {
		if action.Kind != "probe_timeout_action" || !action.Applied ||
			action.Action != "restrict_ordinary_probe_timeout" ||
			action.StateBefore != "recovering" || action.StateAfter != "restricted" || action.ReasonAfter != "probe_timeout" {
			continue
		}
		if action.LegID != int16(faultLeg) || action.LegGeneration != faultGeneration || action.RecordGeneration != faultGeneration ||
			action.RecordSequence != sequence || action.AckedNext != sequence {
			continue
		}
		if action.DecisionEventIndex == 0 || action.PolicyAt.IsZero() || action.ActionAt.IsZero() || action.ReferenceAt.IsZero() ||
			action.Budget < streamProbeBudget || action.Age < action.Budget || action.PolicyAt.Before(action.ReferenceAt) ||
			action.ActionAt.Before(action.PolicyAt) {
			continue
		}
		// PolicyAt is captured before the admission+ledger revalidation and may
		// legitimately precede the last poll that still observed recovering.
		// ActionAt is captured after the locked action and state read; use that
		// commit-side timestamp to place this exact Applied action in the tightly
		// bracketed state transition instead of requiring its earlier decision
		// timestamp to fall between two samples.
		actionElapsed := action.ActionAt.Sub(started).Nanoseconds()
		if actionElapsed < preObserved || actionElapsed > postObserved {
			continue
		}
		decision, ok := decisionsByIndex[action.DecisionEventIndex]
		if !ok || decision.EventIndex >= action.EventIndex || decision.Action != action.Action ||
			decision.LegID != action.LegID || decision.LegGeneration != action.LegGeneration ||
			decision.RecordGeneration != action.RecordGeneration || decision.RecordSequence != action.RecordSequence ||
			decision.AckedNext != action.AckedNext || decision.PolicyAt != action.PolicyAt ||
			decision.ReferenceAt != action.ReferenceAt || decision.Age != action.Age || decision.Budget != action.Budget ||
			decision.StateBefore != "recovering" {
			continue
		}
		matchedActions++
	}
	if matchedActions != 1 {
		return fmt.Sprintf("probe_timeout edge requires exactly one applied action-time decision for leg=%d generation=%d sequence=%d, matched=%d", faultLeg, faultGeneration, sequence, matchedActions), false
	}
	return "", true
}

func v27BlackholeRecoveryStateGate(
	started time.Time,
	observations []v27BlackholeAdmission,
	lowEvents []StreamProbeUniqueServiceLowSnapshot,
	droppedLowEvents uint64,
	telemetryEnabled bool,
	faultLeg uint8,
	faultGeneration uint64,
	peerGeneration uint64,
	traceSets ...[]streamRecoveryTraceEvent,
) v27BlackholeRecoveryGateResult {
	return v27BlackholeRecoveryStateGateWithHorizon(started, observations, lowEvents, droppedLowEvents,
		telemetryEnabled, faultLeg, faultGeneration, peerGeneration, v27BlackholeMaxRuntime, traceSets...)
}

func v27BlackholeRecoveryStateGateWithHorizon(
	started time.Time,
	observations []v27BlackholeAdmission,
	lowEvents []StreamProbeUniqueServiceLowSnapshot,
	droppedLowEvents uint64,
	telemetryEnabled bool,
	faultLeg uint8,
	faultGeneration uint64,
	peerGeneration uint64,
	harnessHorizon time.Duration,
	traceSets ...[]streamRecoveryTraceEvent,
) v27BlackholeRecoveryGateResult {
	var traceEvents []streamRecoveryTraceEvent
	if len(traceSets) > 0 {
		traceEvents = traceSets[0]
	}
	result := v27BlackholeRecoveryGateResult{
		RecoveryRetryReasons:   []string{},
		RetryEventIndices:      []uint64{},
		RetryCooldownElapsedNS: []int64{},
	}
	fail := func(format string, args ...any) v27BlackholeRecoveryGateResult {
		result.OK = false
		result.Failure = fmt.Sprintf(format, args...)
		return result
	}
	if faultLeg > 1 || faultGeneration == 0 || peerGeneration == 0 {
		return fail("invalid expected leg/generation: leg=%d generation=%d peer_generation=%d", faultLeg, faultGeneration, peerGeneration)
	}
	if len(observations) == 0 {
		return fail("state sampler produced no observations")
	}
	if harnessHorizon <= 0 {
		return fail("invalid harness horizon %s", harnessHorizon)
	}
	if droppedLowEvents != 0 {
		return fail("low-service telemetry dropped %d events", droppedLowEvents)
	}
	if !telemetryEnabled && len(lowEvents) != 0 {
		return fail("telemetry is disabled but %d low-service events were supplied", len(lowEvents))
	}

	transitions := make([]v27BlackholeStateTransition, 0, len(observations))
	current := observations[0]
	if current.state != "normal" {
		return fail("state graph must begin normal, got %q", current.state)
	}
	if current.generation != faultGeneration || current.observedAt.IsZero() || current.observedAt.Before(started) {
		return fail("invalid initial observation: state=%q generation=%d at=%v", current.state, current.generation, current.observedAt)
	}
	result.StateSequence = append(result.StateSequence, current.state)
	lastObservedAt := current.observedAt
	for _, point := range observations[1:] {
		if point.state != "normal" && point.state != "restricted" && point.state != "recovering" {
			return fail("unknown admission state %q", point.state)
		}
		if point.generation != faultGeneration {
			return fail("leg generation changed during blackhole test: got=%d want=%d", point.generation, faultGeneration)
		}
		if point.observedAt.IsZero() || point.observedAt.Before(lastObservedAt) {
			return fail("state observations are missing timestamps or out of order")
		}
		lastObservedAt = point.observedAt
		if point.state == current.state {
			current = point
			continue
		}
		transitions = append(transitions, v27BlackholeStateTransition{from: current.state, to: point.state, prev: current, next: point})
		result.StateSequence = append(result.StateSequence, point.state)
		current = point
	}
	if len(transitions) < 3 {
		return fail("incomplete recovery graph: transitions=%v", result.StateSequence)
	}
	if transitions[0].from != "normal" || transitions[0].to != "restricted" {
		return fail("initial transition must be normal->restricted, got %s->%s", transitions[0].from, transitions[0].to)
	}
	if transitions[1].from != "restricted" || transitions[1].to != "recovering" {
		return fail("initial probe transition must be restricted->recovering, got %s->%s", transitions[1].from, transitions[1].to)
	}
	if transitions[1].next.reason != "unique_probe" {
		return fail("initial recovery reason=%q, want unique_probe", transitions[1].next.reason)
	}
	minimumCooldown := streamRestrictionCooldown - 2*v27BlackholeStateWatchInterval
	initialCooldown := transitions[1].next.observedAt.Sub(transitions[0].next.observedAt)
	if initialCooldown < minimumCooldown {
		return fail("initial recovery began before existing cooldown: elapsed=%s minimum=%s", initialCooldown, minimumCooldown)
	}
	if lastObservedAt.After(started.Add(harnessHorizon + 2*v27BlackholeStateWatchInterval)) {
		return fail("state observations exceed the %s harness horizon", harnessHorizon)
	}
	remainingRuntime := started.Add(harnessHorizon).Sub(transitions[0].next.observedAt)
	if remainingRuntime < 0 {
		remainingRuntime = 0
	}
	// A completed run needs one cooldown for the initial probe and one after
	// every low-service re-restriction. Subtract the terminal recovery cooldown
	// from the available interval count; this bounds retries by the actual
	// harness horizon rather than accepting an arbitrary loop count.
	availableCooldowns := int((remainingRuntime + 2*v27BlackholeStateWatchInterval) / minimumCooldown)
	if availableCooldowns > 0 {
		result.RecoveryRetryLimit = availableCooldowns - 1
	}

	lastEventIndex := uint64(0)
	lastAckedNext := uint64(0)
	lastEventAt := time.Time{}
	lowEventIndex := 0
	for i := 2; i < len(transitions); {
		transition := transitions[i]
		if transition.from != "recovering" {
			return fail("unexpected state edge %s->%s", transition.from, transition.to)
		}
		if transition.to == "normal" {
			if i != len(transitions)-1 {
				return fail("normal is terminal but more state transitions follow")
			}
			if transition.next.reason != "unique_ack_recovered" {
				return fail("terminal normal state reason=%q, want unique_ack_recovered", transition.next.reason)
			}
			if transition.next.observedAt.After(started.Add(harnessHorizon + 2*v27BlackholeStateWatchInterval)) {
				return fail("terminal recovery exceeded the %s harness horizon", harnessHorizon)
			}
			if lowEventIndex != len(lowEvents) {
				return fail("low-service evidence count=%d does not match low-service re-restrictions=%d", len(lowEvents), lowEventIndex)
			}
			result.OK = true
			return result
		}
		if transition.to != "restricted" {
			return fail("recovery may only re-restrict through restricted state, got %s->%s", transition.from, transition.to)
		}
		if result.RecoveryRetryCount >= result.RecoveryRetryLimit {
			return fail("recovery retry bound exceeded: limit=%d", result.RecoveryRetryLimit)
		}
		if i+1 >= len(transitions) {
			return fail("recovering->restricted retry has no following retry transition")
		}
		var cooldownFrom time.Time
		switch transition.next.reason {
		case "probe_unique_service_low":
			if lowEventIndex >= len(lowEvents) {
				return fail("re-restriction %d has no matching low-service event", result.RecoveryRetryCount+1)
			}
			event := lowEvents[lowEventIndex]
			if event.EventIndex == 0 || event.EventIndex <= lastEventIndex {
				return fail("low-service event index is zero, duplicated, or regressed: %d", event.EventIndex)
			}
			if !lastEventAt.IsZero() && event.ObservedAt.Before(lastEventAt) {
				return fail("low-service event time regressed")
			}
			if event.AckedNext == 0 || event.AckedNext <= lastAckedNext {
				return fail("low-service ACK frontier is zero, duplicated, or regressed: %d", event.AckedNext)
			}
			if event.LegID != faultLeg || event.PeerLegID != 1-faultLeg || event.LegGeneration != faultGeneration || event.AckGeneration != faultGeneration || event.PeerGeneration != peerGeneration {
				return fail("low-service event leg/generation mismatch: event=%+v", event)
			}
			if event.StateBefore != "recovering" || event.StateAfter != "restricted" || event.ReasonAfter != "probe_unique_service_low" || event.ReasonBefore == "" {
				return fail("low-service event does not prove the approved state edge: event=%+v", event)
			}
			if event.ObservedAt.IsZero() || event.AckProgressAt.IsZero() || event.ObservedAt.Before(event.AckProgressAt) || event.AckProgressAt.Before(started) || event.ObservedAt.Before(transition.prev.observedAt) || event.ObservedAt.After(transition.next.observedAt) {
				return fail("low-service event timestamp does not fall inside its recovering->restricted edge")
			}
			target := event.OpeningRateBPS * 0.1
			tolerance := math.Max(1e-6, math.Abs(target)*1e-9)
			if event.OpeningRateBPS <= 0 || event.TargetRateBPS <= 0 || math.Abs(event.TargetRateBPS-target) > tolerance || event.ProbeRateBPS <= 0 || event.ProbeRateBPS >= event.TargetRateBPS {
				return fail("low-service rate predicates are not met: opening=%.3f target=%.3f probe=%.3f", event.OpeningRateBPS, event.TargetRateBPS, event.ProbeRateBPS)
			}
			if event.MeaningfulDemandBytes == 0 || event.ProbeFrameLimit <= 0 || event.BeforeOrdinaryBytes < event.MeaningfulDemandBytes || event.BeforeOrdinaryFrames < event.ProbeFrameLimit {
				return fail("low-service ordinary-demand predicates are not met")
			}
			if event.UniqueAckBytes == 0 || event.UniqueTimelyRecords <= 0 || event.ProbeBytes < event.UniqueAckBytes {
				return fail("low-service event lacks unique ACK evidence")
			}
			if !telemetryEnabled {
				return fail("service-low retry cannot be accepted without telemetry")
			}
			cooldownFrom = event.AckProgressAt
			lastEventIndex = event.EventIndex
			lastAckedNext = event.AckedNext
			lastEventAt = event.ObservedAt
			lowEventIndex++
			result.RetryEventIndices = append(result.RetryEventIndices, event.EventIndex)
		case "probe_timeout":
			if detail, ok := v27BlackholeProbeTimeoutEvidence(transition.next, transition, started, faultLeg, faultGeneration, traceEvents); !ok {
				return fail("invalid probe_timeout re-restriction: %s", detail)
			}
			cooldownFrom = transition.next.observedAt
		default:
			return fail("uncertified recovering->restricted cause %q", transition.next.reason)
		}
		retryStart := transitions[i+1]
		if retryStart.from != "restricted" || retryStart.to != "recovering" || retryStart.next.reason != "unique_probe" {
			return fail("re-restriction must be followed by restricted->recovering unique_probe, got %s->%s reason=%q", retryStart.from, retryStart.to, retryStart.next.reason)
		}
		cooldownElapsed := retryStart.next.observedAt.Sub(cooldownFrom)
		if cooldownElapsed < minimumCooldown {
			return fail("retry began before existing cooldown for %q: elapsed=%s minimum=%s", transition.next.reason, cooldownElapsed, minimumCooldown)
		}
		result.RecoveryRetryCount++
		result.RecoveryRetryReasons = append(result.RecoveryRetryReasons, transition.next.reason)
		result.RetryCooldownElapsedNS = append(result.RetryCooldownElapsedNS, cooldownElapsed.Nanoseconds())
		i += 2
	}
	return fail("state graph ended without terminal normal/unique_ack_recovered")
}

func TestV27BlackholeSendAdmissionStopsWhenHistoryChannelIsFull(t *testing.T) {
	stop := make(chan struct{})
	changes := make(chan v27BlackholeAdmission)
	done := make(chan bool, 1)
	go func() {
		done <- v27BlackholeSendAdmission(stop, changes, v27BlackholeAdmission{state: "normal"})
	}()

	select {
	case sent := <-done:
		t.Fatalf("send unexpectedly completed without a receiver: sent=%v", sent)
	case <-time.After(10 * time.Millisecond):
	}

	close(stop)
	select {
	case sent := <-done:
		if sent {
			t.Fatal("blocked observation was reported as recorded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked history send did not stop after cancellation")
	}
}

// v27BlackholePathRTTDelayConn adds controlled write service time only while
// establishing the test's initial Path RTT feedback. Its caller must put the
// DATA frame coalescer outside this wrapper so DATA header and payload are
// presented as one frame. After the initial feedback snapshot is captured,
// dropRTT freezes that snapshot without keeping synthetic RTT sleeps in the
// Core writer queue. This is test feedback, not a claim about physical RTT.
type v27BlackholePathRTTDelayConn struct {
	net.Conn
	delay         time.Duration
	dropRTT       atomic.Bool
	rttWrites     atomic.Uint64
	legID         uint8
	generation    uint64
	endpoint      string
	recorder      *v27BlackholeWriteRecorder
	recoveryTrace *v27RecoveryCausalityTrace
}

type v27BlackholeCarrierDataEvent struct {
	Leg                uint8     `json:"leg"`
	Sequence           uint64    `json:"sequence"`
	Generation         uint64    `json:"generation"`
	State              string    `json:"state"`
	Reason             string    `json:"reason"`
	ProbePending       bool      `json:"probe_pending"`
	ProbePendingAt     time.Time `json:"probe_pending_at,omitempty"`
	ProbeSequence      uint64    `json:"probe_sequence,omitempty"`
	ProbeSequenceSet   bool      `json:"probe_sequence_set"`
	CoreWriteStartedAt time.Time `json:"core_write_started_at,omitempty"`
	NextProbe          time.Time `json:"next_probe,omitempty"`
	At                 time.Time `json:"at"`
}

type v27BlackholeProbeWriteGateRequest struct {
	Event v27BlackholeCarrierDataEvent
	Ready chan error
}

type v27BlackholeDataGateConn struct {
	net.Conn
	engine                 *StreamEngine
	legID                  uint8
	faultLeg               uint8
	generation             uint64
	start                  time.Time
	firstDrop              atomic.Bool
	triggerEnabled         bool
	firstData              chan<- v27BlackholeCarrierDataEvent
	firstDataReady         <-chan error
	recoveryWrites         chan<- v27BlackholeCarrierDataEvent
	probeWriteGateRequests chan<- v27BlackholeProbeWriteGateRequest
	firstRecoveryProbeGate atomic.Bool
	recoveryTrace          *v27RecoveryCausalityTrace
}

func (c *v27BlackholeDataGateConn) Write(p []byte) (int, error) {
	header, complete := v27BlackholeFrameHeader(p)
	if !complete || header.Type != StreamFrameData {
		return c.Conn.Write(p)
	}
	if c.triggerEnabled && c.legID == c.faultLeg && c.firstDrop.CompareAndSwap(false, true) {
		trigger := v27BlackholeCarrierDataEvent{
			Leg: c.legID, Sequence: header.Value, Generation: c.generation,
			State: "pre_write_gate", Reason: "first_fault_leg_data_write",
			At: time.Now(),
		}
		select {
		case c.firstData <- trigger:
		case <-time.After(2 * time.Second):
			return 0, fmt.Errorf("event-triggered blackhole controller did not receive first DATA boundary")
		}
		select {
		case err := <-c.firstDataReady:
			if err != nil {
				return 0, fmt.Errorf("install event-triggered blackhole: %w", err)
			}
		case <-time.After(2 * time.Second):
			return 0, fmt.Errorf("event-triggered blackhole was not installed before the first DATA write")
		}
	}
	point := v27BlackholeAdmissionPoint(c.engine, c.legID)
	event := v27BlackholeCarrierDataEvent{
		Leg: c.legID, Sequence: header.Value, Generation: c.generation,
		State: point.state, Reason: point.reason, ProbePending: point.probePending,
		ProbePendingAt: point.probePendingAt, ProbeSequence: point.probeSequence, ProbeSequenceSet: point.probeSequenceSet,
		NextProbe: point.nextProbe, At: time.Now(),
	}
	if c.recoveryTrace != nil {
		c.recoveryTrace.record(streamRecoveryTraceEvent{
			Kind: "carrier_data_write_enter", Endpoint: "client",
			FrameSequence: header.Value, FrameType: "data", LegID: int16(c.legID),
			LegGeneration: c.generation, PayloadBytes: int(header.Length),
			ReferenceAt: point.probePendingAt, StateBefore: point.state, ReasonBefore: point.reason,
			ActionAt: event.At,
		})
	}
	isRecoveryProbe := event.State == "recovering" && event.Reason == "unique_probe"
	if isRecoveryProbe && c.recoveryTrace != nil {
		traceEvents, _, _ := c.recoveryTrace.snapshot()
		for i := len(traceEvents) - 1; i >= 0; i-- {
			candidate := traceEvents[i]
			if candidate.Kind == "recovery_probe_write_started" && candidate.RecordSequence == header.Value &&
				candidate.LegID == int16(c.legID) && candidate.LegGeneration == c.generation &&
				!candidate.ReferenceAt.IsZero() && !candidate.ActionAt.IsZero() {
				event.ProbePending = true
				event.ProbePendingAt = candidate.ReferenceAt
				event.ProbeSequence = candidate.RecordSequence
				event.ProbeSequenceSet = true
				event.CoreWriteStartedAt = candidate.ActionAt
				break
			}
		}
	}
	if isRecoveryProbe && c.probeWriteGateRequests != nil && c.firstRecoveryProbeGate.CompareAndSwap(false, true) {
		request := v27BlackholeProbeWriteGateRequest{Event: event, Ready: make(chan error, 1)}
		select {
		case c.probeWriteGateRequests <- request:
		case <-time.After(2 * time.Second):
			return 0, fmt.Errorf("recovery probe fault controller did not receive carrier write gate")
		}
		select {
		case err := <-request.Ready:
			if err != nil {
				return 0, fmt.Errorf("install recovery probe blackhole: %w", err)
			}
		case <-time.After(2 * time.Second):
			return 0, fmt.Errorf("recovery probe blackhole was not ready before carrier write")
		}
	}
	var socketWriteStart time.Time
	if isRecoveryProbe {
		socketWriteStart = time.Now()
		// This timestamp is taken immediately before calling the Carrier's Write
		// method. The controller receives it after Write returns so a blocking
		// socket cannot stall fault-rule state observation.
		event.At = socketWriteStart
	}
	n, err := c.Conn.Write(p)
	if c.recoveryTrace != nil && isRecoveryProbe {
		c.recoveryTrace.record(streamRecoveryTraceEvent{
			Kind: "carrier_socket_write_start", Endpoint: "client",
			FrameSequence: header.Value, FrameType: "data", LegID: int16(c.legID),
			LegGeneration: c.generation, PayloadBytes: int(header.Length), ActionAt: socketWriteStart,
			StateBefore: event.State, ReasonBefore: event.Reason,
		})
	}
	if isRecoveryProbe && c.recoveryWrites != nil {
		select {
		case c.recoveryWrites <- event:
		default:
		}
	}
	if c.recoveryTrace != nil {
		writeEvent := streamRecoveryTraceEvent{
			Kind: "carrier_data_write_return", Endpoint: "client",
			FrameSequence: header.Value, FrameType: "data", LegID: int16(c.legID),
			LegGeneration: c.generation, PayloadBytes: int(header.Length),
			WriteBytes: len(p), WriteResultBytes: n, Applied: err == nil,
		}
		if err != nil {
			writeEvent.Error = err.Error()
		}
		c.recoveryTrace.record(writeEvent)
	}
	return n, err
}

func TestV27BlackholeDataGateWaitsForActualFaultInstall(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	firstData := make(chan v27BlackholeCarrierDataEvent, 1)
	firstDataReady := make(chan error, 1)
	gate := &v27BlackholeDataGateConn{
		Conn: client, engine: &StreamEngine{
			admission:     streamAdmission{legs: make([]streamAdmissionLeg, 2)},
			legGeneration: make([]atomic.Uint64, 2),
		}, legID: 1, faultLeg: 1,
		generation: 7, triggerEnabled: true, firstData: firstData, firstDataReady: firstDataReady,
	}
	payload := []byte("phase16-gated-data")
	wire := make([]byte, StreamFrameHeaderSize+len(payload))
	if err := EncodeStreamFrameHeader(wire[:StreamFrameHeaderSize], StreamFrameHeader{
		Type: StreamFrameData, Value: 42, Length: uint32(len(payload)),
	}); err != nil {
		t.Fatal(err)
	}
	copy(wire[StreamFrameHeaderSize:], payload)
	writeDone := make(chan error, 1)
	go func() {
		n, err := gate.Write(wire)
		if err == nil && n != len(wire) {
			err = fmt.Errorf("short write %d/%d", n, len(wire))
		}
		writeDone <- err
	}()
	trigger := <-firstData
	if trigger.Sequence != 42 || trigger.Generation != 7 || trigger.Leg != 1 {
		t.Fatalf("unexpected actual DATA trigger identity: %+v", trigger)
	}
	if err := server.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	probe := make([]byte, 1)
	if _, err := server.Read(probe); err == nil {
		t.Fatal("DATA reached the peer before the fault controller acknowledged installation")
	}
	if err := server.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstDataReady <- nil
	got := make([]byte, len(wire))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wire) {
		t.Fatalf("carrier write changed gated DATA: got=%x want=%x", got, wire)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestV27BlackholeRecoveryProbeGateWaitsForRetryFaultInstall(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	engine := &StreamEngine{
		admission:     streamAdmission{legs: make([]streamAdmissionLeg, 2)},
		legGeneration: make([]atomic.Uint64, 2),
	}
	engine.admission.legs[1] = streamAdmissionLeg{recovering: true, reason: "unique_probe"}
	engine.legGeneration[1].Store(11)
	requests := make(chan v27BlackholeProbeWriteGateRequest, 1)
	recoveryWrites := make(chan v27BlackholeCarrierDataEvent, 1)
	trace := newV27RecoveryCausalityTraceMode(v27RecoveryTraceModeLite)
	gate := &v27BlackholeDataGateConn{
		Conn: client, engine: engine, legID: 1, faultLeg: 1, generation: 11,
		probeWriteGateRequests: requests, recoveryWrites: recoveryWrites, recoveryTrace: trace,
	}
	payload := []byte("phase16-recovery-probe-gated-data")
	wire := make([]byte, StreamFrameHeaderSize+len(payload))
	if err := EncodeStreamFrameHeader(wire[:StreamFrameHeaderSize], StreamFrameHeader{
		Type: StreamFrameData, Value: 42, Length: uint32(len(payload)),
	}); err != nil {
		t.Fatal(err)
	}
	copy(wire[StreamFrameHeaderSize:], payload)
	writeDone := make(chan error, 1)
	go func() {
		n, err := gate.Write(wire)
		if err == nil && n != len(wire) {
			err = fmt.Errorf("short write %d/%d", n, len(wire))
		}
		writeDone <- err
	}()
	var request v27BlackholeProbeWriteGateRequest
	select {
	case request = <-requests:
	case <-time.After(time.Second):
		t.Fatal("recovery probe did not request the second fault gate")
	}
	if request.Event.Leg != 1 || request.Event.Generation != 11 || request.Event.Sequence != 42 ||
		request.Event.State != "recovering" || request.Event.Reason != "unique_probe" {
		t.Fatalf("unexpected recovery probe gate identity: %+v", request.Event)
	}
	if err := server.SetReadDeadline(time.Now().Add(15 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Fatal("recovery-probe DATA reached the peer before the second fault gate was installed")
	}
	if err := server.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	releasedAt := time.Now()
	request.Ready <- nil
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, len(wire))
		_, err := io.ReadFull(server, got)
		if err == nil && !bytes.Equal(got, wire) {
			err = fmt.Errorf("Carrier write changed recovery DATA")
		}
		readDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery probe Carrier Write did not finish after gate release")
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-recoveryWrites:
		if event.Sequence != 42 || event.Leg != 1 || event.Generation != 11 || event.At.Before(releasedAt) {
			t.Fatalf("unexpected recovery Carrier Write boundary: %+v release=%v", event, releasedAt)
		}
	default:
		t.Fatal("actual Carrier Write boundary was not reported")
	}
	events, _, dropped := trace.snapshot()
	if dropped != 0 {
		t.Fatalf("trace dropped %d events", dropped)
	}
	var foundSocketBoundary bool
	for _, event := range events {
		if event.Kind == "carrier_socket_write_start" && event.FrameSequence == 42 &&
			event.LegGeneration == 11 && !event.ActionAt.IsZero() && !event.ActionAt.Before(releasedAt) {
			foundSocketBoundary = true
		}
	}
	if !foundSocketBoundary {
		t.Fatalf("no trace entry at the post-gate Carrier Write boundary: %+v", events)
	}
}

type v27BlackholeFaultControlResult struct {
	Timeline                                []map[string]any `json:"timeline"`
	DropEvidence                            string           `json:"drop_evidence,omitempty"`
	InitialDropEvidence                     string           `json:"initial_drop_evidence,omitempty"`
	RetryDropEvidence                       string           `json:"retry_drop_evidence,omitempty"`
	InstallError                            string           `json:"install_error,omitempty"`
	InitialBlackholeRemovedAfterRestriction bool             `json:"initial_blackhole_removed_after_restriction"`
	RetryBlackholeInstalled                 bool             `json:"retry_blackhole_installed"`
	RetryBlackholeRemovedAfterRerestriction bool             `json:"retry_blackhole_removed_after_rerestriction"`
	RestoredAfterRecoveryRetry              bool             `json:"restored_after_recovery_retry"`
	PrewriteBoundValid                      bool             `json:"prewrite_bound_valid"`
	MatchedProbeSequence                    uint64           `json:"matched_probe_sequence,omitempty"`
	MatchedProbeGeneration                  uint64           `json:"matched_probe_generation,omitempty"`
	ProbeGateElapsedNS                      int64            `json:"probe_gate_elapsed_ns,omitempty"`
	ProbeGateReleaseElapsedNS               int64            `json:"probe_gate_release_elapsed_ns,omitempty"`
	ProbeSocketWriteStartNS                 int64            `json:"probe_socket_write_start_ns,omitempty"`
	ProbeGateToSocketWriteNS                int64            `json:"probe_gate_to_socket_write_ns,omitempty"`
}

func v27BlackholeNFTCommand(args ...string) (string, error) {
	output, err := exec.Command("nft", args...).CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(output)), fmt.Errorf("nft %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

func v27BlackholeRunEventFaultController(
	port uint16, faultLeg uint8, start time.Time,
	firstData <-chan v27BlackholeCarrierDataEvent, firstDataReady chan<- error,
	probeWriteGateRequests <-chan v27BlackholeProbeWriteGateRequest,
	recoveryWrites <-chan v27BlackholeCarrierDataEvent,
	transitions <-chan v27BlackholeAdmission, stop <-chan struct{}, done chan<- v27BlackholeFaultControlResult,
) {
	result := v27BlackholeFaultControlResult{Timeline: make([]map[string]any, 0, 8)}
	installed := false
	phase := 0
	portText := strconv.Itoa(int(port))
	appendEvent := func(name string, fields map[string]any) {
		if fields == nil {
			fields = make(map[string]any)
		}
		fields["event"] = name
		fields["elapsed_ns"] = time.Since(start).Nanoseconds()
		result.Timeline = append(result.Timeline, fields)
	}
	appendDropEvidence := func(phase string, evidence string) {
		if evidence == "" {
			return
		}
		if result.DropEvidence != "" {
			result.DropEvidence += "\n"
		}
		result.DropEvidence += evidence
		if phase == "initial" {
			if result.InitialDropEvidence != "" {
				result.InitialDropEvidence += "\n"
			}
			result.InitialDropEvidence += evidence
		} else {
			if result.RetryDropEvidence != "" {
				result.RetryDropEvidence += "\n"
			}
			result.RetryDropEvidence += evidence
		}
	}
	installBlackhole := func(stage string, fields map[string]any) error {
		appendEvent("carrier_tcp_blackhole_enable_requested", fields)
		_, errA := v27BlackholeNFTCommand("add", "rule", "inet", "smp3_v27", "output", "tcp", "dport", portText, "counter", "drop")
		var errB error
		if errA == nil {
			_, errB = v27BlackholeNFTCommand("add", "rule", "inet", "smp3_v27", "output", "tcp", "sport", portText, "counter", "drop")
		}
		if errA != nil || errB != nil {
			err := errA
			if err == nil {
				err = errB
			}
			result.InstallError = fmt.Sprintf("%s: %v", stage, err)
			_, _ = v27BlackholeNFTCommand("flush", "chain", "inet", "smp3_v27", "output")
			appendEvent("carrier_tcp_blackhole_install_failed", map[string]any{"stage": stage, "error": err.Error()})
			return err
		}
		installed = true
		phase = 1
		if stage == "retry_probe" {
			phase = 2
			result.RetryBlackholeInstalled = true
		}
		appendEvent("carrier_tcp_blackhole_enabled", map[string]any{"stage": stage, "fault_leg": faultLeg, "port": port})
		return nil
	}
	captureAndRemove := func(cause string, point v27BlackholeAdmission) bool {
		evidence, err := v27BlackholeNFTCommand("-a", "list", "chain", "inet", "smp3_v27", "output")
		appendDropEvidence(map[bool]string{true: "retry", false: "initial"}[phase == 2], evidence)
		if err != nil {
			appendEvent("blackhole_evidence_error", map[string]any{"cause": cause, "error": err.Error()})
			return false
		}
		if _, err := v27BlackholeNFTCommand("flush", "chain", "inet", "smp3_v27", "output"); err != nil {
			appendEvent("blackhole_remove_error", map[string]any{"cause": cause, "error": err.Error()})
			return false
		}
		installed = false
		removedPhase := phase
		phase = 0
		if removedPhase == 1 {
			result.InitialBlackholeRemovedAfterRestriction = true
		} else if removedPhase == 2 {
			result.RetryBlackholeRemovedAfterRerestriction = true
		}
		appendEvent("carrier_tcp_blackhole_removed", map[string]any{
			"trigger": cause, "phase": removedPhase, "fault_leg": faultLeg, "drop_evidence": evidence,
			"state": point.state, "reason": point.reason,
			"probe_pending": point.probePending, "probe_pending_at": point.probePendingAt,
			"probe_sequence": point.probeSequence, "next_probe": point.nextProbe,
		})
		return true
	}
	select {
	case trigger := <-firstData:
		fields := map[string]any{
			"trigger": "first_actual_fault_leg_DATA_carrier_write", "stage": "initial_fault",
			"leg": trigger.Leg, "sequence": trigger.Sequence, "generation": trigger.Generation,
		}
		if err := installBlackhole("initial_fault", fields); err != nil {
			firstDataReady <- err
			done <- result
			return
		}
		firstDataReady <- nil
	case <-stop:
		done <- result
		return
	}

	pending := make(map[string]v27BlackholeAdmission)
	var recoveryProbe *v27BlackholeCarrierDataEvent
	var retryRestricted *v27BlackholeAdmission
	var waitingProbeGate *v27BlackholeProbeWriteGateRequest
	keyFor := func(leg uint8, generation, sequence uint64) string {
		return fmt.Sprintf("%d/%d/%d", leg, generation, sequence)
	}
	tryRestore := func() bool {
		if recoveryProbe == nil || retryRestricted == nil {
			return false
		}
		point := *retryRestricted
		if captureAndRemove("first_recovery_probe_restricted_after_actual_carrier_write", point) {
			result.RestoredAfterRecoveryRetry = true
			result.MatchedProbeSequence = recoveryProbe.Sequence
			result.MatchedProbeGeneration = recoveryProbe.Generation
			return true
		}
		return false
	}
	installRetryGateIfReady := func() {
		if waitingProbeGate == nil || installed || !result.InitialBlackholeRemovedAfterRestriction {
			return
		}
		event := waitingProbeGate.Event
		// recoveryProbeWriteStarted clears the pending fields while transitioning
		// to recovering. Match the DATA sequence against the previously observed
		// pending transition rather than expecting those fields to remain set.
		matchingPending, ok := pending[keyFor(event.Leg, event.Generation, event.Sequence)]
		if !ok || event.Leg != faultLeg || event.State != "recovering" || event.Reason != "unique_probe" {
			return
		}
		fields := map[string]any{
			"trigger": "actual_recovery_probe_Carrier_Write_boundary", "stage": "retry_probe",
			"leg": event.Leg, "sequence": event.Sequence, "generation": event.Generation,
			"probe_sequence": event.Sequence, "probe_pending_at": matchingPending.probePendingAt,
			"carrier_boundary_elapsed_ns":   event.At.Sub(start).Nanoseconds(),
			"core_write_started_elapsed_ns": event.CoreWriteStartedAt.Sub(start).Nanoseconds(),
		}
		if err := installBlackhole("retry_probe", fields); err != nil {
			waitingProbeGate.Ready <- err
			waitingProbeGate = nil
			return
		}
		releasedAt := time.Now()
		result.ProbeGateElapsedNS = event.At.Sub(start).Nanoseconds()
		result.ProbeGateReleaseElapsedNS = releasedAt.Sub(start).Nanoseconds()
		appendEvent("recovery_probe_blackhole_gate_released", map[string]any{
			"leg": event.Leg, "sequence": event.Sequence, "generation": event.Generation,
			"probe_sequence":                      event.Sequence,
			"probe_pending_at":                    matchingPending.probePendingAt,
			"probe_wrapper_boundary_elapsed_ns":   event.At.Sub(start).Nanoseconds(),
			"core_write_started_elapsed_ns":       event.CoreWriteStartedAt.Sub(start).Nanoseconds(),
			"gate_release_elapsed_ns":             releasedAt.Sub(start).Nanoseconds(),
			"wrapper_boundary_to_gate_release_ns": releasedAt.Sub(event.At).Nanoseconds(),
		})
		waitingProbeGate.Ready <- nil
		waitingProbeGate = nil
	}
	for {
		select {
		case request := <-probeWriteGateRequests:
			if waitingProbeGate != nil {
				request.Ready <- fmt.Errorf("received a second recovery probe write gate while one is pending")
				continue
			}
			waitingProbeGate = &request
			if request.Event.ProbePending && request.Event.ProbeSequenceSet &&
				request.Event.ProbeSequence == request.Event.Sequence && !request.Event.ProbePendingAt.IsZero() {
				pending[keyFor(request.Event.Leg, request.Event.Generation, request.Event.Sequence)] = v27BlackholeAdmission{
					state: "restricted", reason: "probe_pending_write", generation: request.Event.Generation,
					probePending: true, probePendingAt: request.Event.ProbePendingAt,
					probeSequence: request.Event.ProbeSequence, probeSequenceSet: true,
				}
				appendEvent("recovery_probe_pending_from_core_write_trace", map[string]any{
					"leg": request.Event.Leg, "sequence": request.Event.Sequence,
					"generation": request.Event.Generation, "probe_pending_at": request.Event.ProbePendingAt,
					"core_write_started_at": request.Event.CoreWriteStartedAt,
				})
			}
			appendEvent("recovery_probe_write_gate_requested", map[string]any{
				"leg": request.Event.Leg, "sequence": request.Event.Sequence,
				"generation": request.Event.Generation, "state": request.Event.State,
				"reason": request.Event.Reason, "probe_sequence": request.Event.Sequence,
				"probe_pending":               request.Event.ProbePending,
				"probe_pending_at":            request.Event.ProbePendingAt,
				"core_write_started_at":       request.Event.CoreWriteStartedAt,
				"wrapper_boundary_elapsed_ns": request.Event.At.Sub(start).Nanoseconds(),
			})
			installRetryGateIfReady()
		case event := <-recoveryWrites:
			k := keyFor(event.Leg, event.Generation, event.Sequence)
			if _, ok := pending[k]; ok && recoveryProbe == nil {
				pendingPoint := pending[k]
				prewriteElapsed := event.At.Sub(pendingPoint.probePendingAt)
				validPrewrite := prewriteElapsed >= 0 && prewriteElapsed < streamProbeBudget
				if validPrewrite {
					copyEvent := event
					recoveryProbe = &copyEvent
					result.PrewriteBoundValid = true
				}
				appendEvent("recovery_probe_carrier_write_matched", map[string]any{
					"leg": event.Leg, "sequence": event.Sequence, "generation": event.Generation,
					"probe_pending_at":            pendingPoint.probePendingAt,
					"pending_to_carrier_write_ns": prewriteElapsed.Nanoseconds(),
					"prewrite_budget_ns":          streamProbeBudget.Nanoseconds(),
					"prewrite_bound_valid":        validPrewrite,
					"state":                       event.State, "reason": event.Reason,
				})
				if recoveryProbe != nil {
					result.ProbeSocketWriteStartNS = event.At.Sub(start).Nanoseconds()
				}
				if tryRestore() {
					done <- result
					return
				}
			}
		case point := <-transitions:
			if point.state == "restricted" && point.reason == "probe_pending_write" && point.probePending && point.probeSequenceSet {
				pending[keyFor(faultLeg, point.generation, point.probeSequence)] = point
				appendEvent("recovery_probe_pending", map[string]any{
					"leg": faultLeg, "sequence": point.probeSequence, "generation": point.generation,
					"probe_pending_at": point.probePendingAt, "next_probe": point.nextProbe,
				})
			}
			if point.state == "restricted" && phase == 1 && point.reason != "" {
				if captureAndRemove("first_blackhole_released_after_actual_restriction", point) {
					installRetryGateIfReady()
				}
			}
			if point.state == "restricted" && phase == 2 && (point.reason == "probe_timeout" || point.reason == "probe_unique_service_low") {
				copyPoint := point
				retryRestricted = &copyPoint
				if tryRestore() {
					done <- result
					return
				}
			}
			installRetryGateIfReady()
		case <-stop:
			if waitingProbeGate != nil {
				waitingProbeGate.Ready <- fmt.Errorf("event fault controller stopped before recovery probe gate release")
				waitingProbeGate = nil
			}
			if installed {
				evidence, _ := v27BlackholeNFTCommand("-a", "list", "chain", "inet", "smp3_v27", "output")
				appendDropEvidence(map[bool]string{true: "retry", false: "initial"}[phase == 2], evidence)
				_, _ = v27BlackholeNFTCommand("flush", "chain", "inet", "smp3_v27", "output")
				installed = false
				appendEvent("blackhole_cleanup_at_test_end", map[string]any{"drop_evidence": evidence})
			}
			done <- result
			return
		}
	}
}

type v27BlackholeWriteEvent struct {
	Endpoint         string `json:"endpoint"`
	Leg              uint8  `json:"leg"`
	FrameType        string `json:"frame_type"`
	FrameValue       uint64 `json:"frame_value"`
	StartElapsedNS   int64  `json:"start_elapsed_ns"`
	TransferStartNS  int64  `json:"transfer_start_ns"`
	EndElapsedNS     int64  `json:"end_elapsed_ns"`
	DurationNS       int64  `json:"duration_ns"`
	SyntheticDelayNS int64  `json:"synthetic_delay_ns"`
	CarrierWriteNS   int64  `json:"carrier_write_ns"`
	Bytes            int    `json:"bytes"`
	Dropped          bool   `json:"dropped"`
	Error            string `json:"error,omitempty"`
}

type v27BlackholeWriteRecorder struct {
	mu       sync.Mutex
	origin   time.Time
	transfer time.Time
	events   []v27BlackholeWriteEvent
}

type v27BlackholeWriteCounterConn struct {
	net.Conn
	writes atomic.Uint64
}

func (c *v27BlackholeWriteCounterConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return len(p), nil
}

func (r *v27BlackholeWriteRecorder) setTransferStart(start time.Time) {
	r.mu.Lock()
	r.transfer = start
	r.mu.Unlock()
}

func (r *v27BlackholeWriteRecorder) record(event v27BlackholeWriteEvent, started, ended time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	event.StartElapsedNS = started.Sub(r.origin).Nanoseconds()
	event.EndElapsedNS = ended.Sub(r.origin).Nanoseconds()
	event.DurationNS = ended.Sub(started).Nanoseconds()
	if !r.transfer.IsZero() {
		event.TransferStartNS = started.Sub(r.transfer).Nanoseconds()
	}
	r.events = append(r.events, event)
}

func (r *v27BlackholeWriteRecorder) snapshot() []v27BlackholeWriteEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]v27BlackholeWriteEvent(nil), r.events...)
}

func v27BlackholeFrameHeader(p []byte) (StreamFrameHeader, bool) {
	if len(p) < StreamFrameHeaderSize {
		return StreamFrameHeader{}, false
	}
	header, err := ReadStreamFrameHeader(bytes.NewReader(p[:StreamFrameHeaderSize]))
	if err != nil || len(p) != StreamFrameHeaderSize+int(header.Length) {
		return StreamFrameHeader{}, false
	}
	return header, true
}

func (c *v27BlackholePathRTTDelayConn) Write(p []byte) (int, error) {
	header, completeFrame := v27BlackholeFrameHeader(p)
	frameType := ""
	tracked := false
	if completeFrame {
		switch header.Type {
		case StreamFrameData:
			frameType, tracked = "data", true
		case StreamFrameACK:
			frameType, tracked = "ack", true
		case StreamFramePathRTT:
			if header.Value == 0 && header.Length == StreamRTTMessageSize {
				frameType, tracked = "path_rtt", true
			}
		}
	}
	started := time.Now()
	dropped := false
	var syntheticDelay time.Duration
	var carrierWriteStarted time.Time
	var n int
	var err error
	if frameType == "path_rtt" {
		c.rttWrites.Add(1)
		if c.dropRTT.Load() {
			n, dropped = len(p), true
		}
		if !dropped && c.delay > 0 {
			delayStarted := time.Now()
			timer := time.NewTimer(c.delay)
			<-timer.C
			timer.Stop()
			syntheticDelay = time.Since(delayStarted)
		}
		if !dropped && c.dropRTT.Load() {
			n, dropped = len(p), true
		}
	}
	if !dropped {
		carrierWriteStarted = time.Now()
		if c.recoveryTrace != nil && tracked {
			c.recoveryTrace.record(streamRecoveryTraceEvent{
				Kind: "carrier_conn_write_enter", Endpoint: c.endpoint,
				FrameSequence: header.Value, FrameType: frameType, LegID: int16(c.legID),
				LegGeneration: c.generation, WriteBytes: len(p),
			})
		}
		n, err = c.Conn.Write(p)
		if c.recoveryTrace != nil && tracked {
			writeEvent := streamRecoveryTraceEvent{
				Kind: "carrier_conn_write_return", Endpoint: c.endpoint,
				FrameSequence: header.Value, FrameType: frameType, LegID: int16(c.legID),
				LegGeneration: c.generation, WriteBytes: len(p), WriteResultBytes: n,
				Applied: err == nil,
			}
			if err != nil {
				writeEvent.Error = err.Error()
			}
			c.recoveryTrace.record(writeEvent)
		}
	} else if c.recoveryTrace != nil && tracked {
		c.recoveryTrace.record(streamRecoveryTraceEvent{
			Kind: "carrier_conn_write_suppressed", Endpoint: c.endpoint,
			FrameSequence: header.Value, FrameType: frameType, LegID: int16(c.legID),
			LegGeneration: c.generation, WriteBytes: len(p), Error: "fixture_drop_before_conn_write",
		})
	}
	ended := time.Now()
	if c.recorder != nil && tracked {
		event := v27BlackholeWriteEvent{
			Endpoint: c.endpoint, Leg: c.legID, FrameType: frameType, FrameValue: header.Value,
			SyntheticDelayNS: syntheticDelay.Nanoseconds(), Bytes: n, Dropped: dropped,
		}
		if !carrierWriteStarted.IsZero() {
			event.CarrierWriteNS = ended.Sub(carrierWriteStarted).Nanoseconds()
		}
		if err != nil {
			event.Error = err.Error()
		}
		c.recorder.record(event, started, ended)
	}
	return n, err
}

func TestV27BlackholeFrozenRTTControlDoesNotWriteToCarrier(t *testing.T) {
	carrier := &v27BlackholeWriteCounterConn{}
	recorder := &v27BlackholeWriteRecorder{origin: time.Now()}
	rttConn := &v27BlackholePathRTTDelayConn{
		Conn: carrier, delay: 200 * time.Millisecond, recorder: recorder,
	}
	rttConn.dropRTT.Store(true)
	wire := make([]byte, StreamFrameHeaderSize+StreamRTTMessageSize)
	if err := EncodeStreamFrameHeader(wire[:StreamFrameHeaderSize], StreamFrameHeader{
		Type: StreamFramePathRTT, Length: StreamRTTMessageSize,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := rttConn.Write(wire)
	if err != nil || n != len(wire) {
		t.Fatalf("frozen RTT write = %d, %v; want successful fixture drop", n, err)
	}
	if got := carrier.writes.Load(); got != 0 {
		t.Fatalf("frozen RTT control reached carrier %d times; want 0", got)
	}
	events := recorder.snapshot()
	if len(events) != 1 || !events[0].Dropped || events[0].SyntheticDelayNS != 0 {
		t.Fatalf("frozen RTT write evidence = %+v; want one immediate dropped event", events)
	}
}

func TestV27BlackholeRTTDelayConnDoesNotDropDataPayloadStartingWithRTTType(t *testing.T) {
	write, read := net.Pipe()
	defer write.Close()
	defer read.Close()

	// Keep the historical wrapper ordering in this regression: DATA header and
	// payload reach the RTT wrapper as separate net.Buffers writes.
	rttConn := &v27BlackholePathRTTDelayConn{Conn: &p0cdDataFrameCoalescer{Conn: write}}
	rttConn.dropRTT.Store(true)
	readDeadline := time.Now().Add(2 * time.Second)
	if err := read.SetReadDeadline(readDeadline); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, v27BlackholeChunkSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	payload[0] = byte(StreamFramePathRTT)
	want := make([]byte, StreamFrameHeaderSize+len(payload))
	if err := EncodeStreamFrameHeader(want[:StreamFrameHeaderSize], StreamFrameHeader{
		Type: StreamFrameData, Length: uint32(len(payload)),
	}); err != nil {
		t.Fatal(err)
	}
	copy(want[StreamFrameHeaderSize:], payload)

	readResult := make(chan struct {
		wire []byte
		err  error
	}, 1)
	go func() {
		wire := make([]byte, len(want))
		_, err := io.ReadFull(read, wire)
		readResult <- struct {
			wire []byte
			err  error
		}{wire: wire, err: err}
	}()

	if err := writeDataFrame(rttConn, dataFrame{seq: 0, data: payload}); err != nil {
		t.Fatalf("write DATA with RTT type byte in payload: %v", err)
	}
	select {
	case result := <-readResult:
		if result.err != nil {
			t.Fatalf("read DATA after RTT filter: %v", result.err)
		}
		if !bytes.Equal(result.wire, want) {
			t.Fatal("DATA frame changed while passing through RTT delay wrapper")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DATA payload that begins with the RTT frame type")
	}
	if got := rttConn.rttWrites.Load(); got != 0 {
		t.Fatalf("DATA payload was classified as an RTT frame: matched=%d", got)
	}
	if err := writePathRTTFrame(rttConn, make([]byte, StreamRTTMessageSize)); err != nil {
		t.Fatalf("drop a complete RTT frame after data regression: %v", err)
	}
	if got := rttConn.rttWrites.Load(); got != 1 {
		t.Fatalf("complete RTT frame was not recognized: matched=%d", got)
	}
}

func v27BlackholeWaitPathFeedback(t *testing.T, engine *StreamEngine, legID uint8, direction StreamRTTDirection) StreamPathFeedback {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leg := engine.getLeg(legID)
		if leg == nil {
			t.Fatalf("path-aware blackhole leg%d detached before feedback was ready", legID)
		}
		if feedback, ok := engine.PathFeedback(LegID(legID)); ok {
			if feedback.Identity != leg.pathIdentity || feedback.Identity.Direction != direction || feedback.Identity.Generation != leg.generation || feedback.Identity.CarrierID != leg.pathIdentity.CarrierID || feedback.RTT <= 0 || feedback.Samples == 0 {
				t.Fatalf("leg%d feedback is not bound to the active authenticated Carrier: feedback=%+v leg=%+v", legID, feedback, leg.pathIdentity)
			}
			return feedback
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for same-Carrier feedback leg%d direction=%d", legID, direction)
	return StreamPathFeedback{}
}

// v27BlackholeAgeFeedback moves only the test fixture's immutable sample
// timestamps outside the configured freshness window. This makes the expiry
// fallback deterministic without waiting 30 seconds or weakening production
// freshness rules.
func v27BlackholeAgeFeedback(engine *StreamEngine, legID uint8, direction StreamRTTDirection) bool {
	if engine == nil || engine.pathRTTCache == nil || legID > 1 || !validStreamRTTDirection(direction) {
		return false
	}
	cache := engine.pathRTTCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	staleAt := time.Now().Add(-StreamRTTSampleWindow - time.Second)
	index := int(direction - 1)
	for i := range cache.samples[legID][index] {
		cache.samples[legID][index][i].at = staleAt
	}
	snapshot := cache.feedback[legID][index].Load()
	if snapshot == nil || snapshot.count == 0 {
		return false
	}
	stale := *snapshot
	for i := 0; i < stale.count; i++ {
		stale.samples[i].at = staleAt
	}
	cache.feedback[legID][index].Store(&stale)
	return true
}

func v27BlackholeTCPPair(t *testing.T) (net.Conn, net.Conn, uint16) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	select {
	case server := <-accepted:
		_ = listener.Close()
		_ = client.SetNoDelay(true)
		_ = server.(*net.TCPConn).SetNoDelay(true)
		return client, server, port
	case err := <-acceptErr:
		_ = listener.Close()
		_ = client.Close()
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		_ = listener.Close()
		_ = client.Close()
		t.Fatal("isolated TCP loopback accept timed out")
	}
	return nil, nil, 0
}

func v27BlackholeNFT(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("nft", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("nft %s failed: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output))
}

func v27BlackholeInstallDrop(t *testing.T, port uint16) {
	t.Helper()
	portText := strconv.Itoa(int(port))
	v27BlackholeNFT(t, "add", "rule", "inet", "smp3_v27", "output", "tcp", "dport", portText, "counter", "drop")
	v27BlackholeNFT(t, "add", "rule", "inet", "smp3_v27", "output", "tcp", "sport", portText, "counter", "drop")
}

func v27BlackholeHash(total int64) string {
	h := sha256.New()
	buf := make([]byte, v27BlackholeChunkSize)
	var offset uint64
	for remaining := total; remaining > 0; {
		n := len(buf)
		if int64(n) > remaining {
			n = int(remaining)
		}
		for i := 0; i < n; i++ {
			buf[i] = byte((offset + uint64(i)) % 251)
		}
		_, _ = h.Write(buf[:n])
		offset += uint64(n)
		remaining -= int64(n)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func v27BlackholeWaitUntil(t *testing.T, target time.Time) {
	t.Helper()
	if wait := time.Until(target); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		<-timer.C
	}
}

func v27BlackholeJSON(t *testing.T, event map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Logf("V27_BLACKHOLE_JSON_ERROR %v", err)
		return
	}
	t.Log("V27_BLACKHOLE " + string(encoded))
}

func v27ProbeEventHistoryStatus(enabled bool, dropped uint64) (bool, string) {
	if !enabled {
		return false, "telemetry_disabled"
	}
	if dropped > 0 {
		return false, "oldest_events_dropped"
	}
	return true, ""
}

func TestV27ProbeEventHistoryStatusMarksIncompleteCapture(t *testing.T) {
	for _, test := range []struct {
		name     string
		enabled  bool
		dropped  uint64
		complete bool
		reason   string
	}{
		{name: "enabled_no_overflow", enabled: true, complete: true},
		{name: "enabled_overflow", enabled: true, dropped: 1, reason: "oldest_events_dropped"},
		{name: "disabled", reason: "telemetry_disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			complete, reason := v27ProbeEventHistoryStatus(test.enabled, test.dropped)
			if complete != test.complete || reason != test.reason {
				t.Fatalf("history status=(%v,%q), want=(%v,%q)", complete, reason, test.complete, test.reason)
			}
		})
	}
}

func v27BlackholeTCPRetransSegs() (uint64, bool) {
	data, err := os.ReadFile("/proc/net/snmp")
	if err != nil {
		return 0, false
	}
	lines := strings.Split(string(data), "\n")
	for i := 0; i+1 < len(lines); i++ {
		header, values := strings.Fields(lines[i]), strings.Fields(lines[i+1])
		if len(header) == 0 || len(values) == 0 || header[0] != "Tcp:" || values[0] != "Tcp:" || len(header) != len(values) {
			continue
		}
		for field := 1; field < len(header); field++ {
			if header[field] == "RetransSegs" {
				value, parseErr := strconv.ParseUint(values[field], 10, 64)
				return value, parseErr == nil
			}
		}
	}
	return 0, false
}

func v27BlackholeTCPInfoSet(carriers [2][2]net.Conn) map[string]any {
	result := make(map[string]any, 4)
	for leg := range carriers {
		for endpoint := range carriers[leg] {
			name := fmt.Sprintf("leg%d_%s", leg, map[int]string{0: "client_tx", 1: "server_rx"}[endpoint])
			result[name] = v27BlackholeTCPInfoSnapshot(carriers[leg][endpoint])
		}
	}
	return result
}

func TestV27KernelBlackholeRecovery(t *testing.T) {
	if os.Getenv("SMP3_V27_KERNEL_BLACKHOLE") != "1" {
		t.Skip("run only in a fresh isolated network namespace")
	}
	if runtime.GOOS != "linux" {
		t.Fatalf("kernel blackhole test requires Linux, GOOS=%q", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		t.Fatal("nftables blackhole test requires root inside an isolated network namespace")
	}
	tcpRetransBefore, tcpRetransBeforeOK := v27BlackholeTCPRetransSegs()
	selfNS, selfErr := os.Readlink("/proc/self/ns/net")
	initNS, initErr := os.Readlink("/proc/1/ns/net")
	if selfErr != nil || initErr != nil {
		t.Fatalf("cannot verify isolated network namespace: self=%v init=%v", selfErr, initErr)
	}
	if selfNS == initNS {
		t.Fatal("refusing to install nftables rules outside a new network namespace")
	}
	pathAware := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_PATH_AWARE") == "1"
	rescuePolicy, err := v27BlackholeRescuePolicyFromEnv(os.Getenv)
	if err != nil {
		t.Fatalf("invalid blackhole Rescue policy: %v", err)
	}
	requireBudgetExhausted := false
	switch os.Getenv(v27BlackholeRequireBudgetEnv) {
	case "", "0":
	case "1":
		requireBudgetExhausted = true
	default:
		t.Fatalf("invalid %s; want 0 or 1", v27BlackholeRequireBudgetEnv)
	}
	if requireBudgetExhausted && (rescuePolicy.Name != "b1" || rescuePolicy.Config == nil || rescuePolicy.Config.BurstBytes != 0) {
		t.Fatalf("budget-exhaustion gate requires B1 with zero burst; policy=%q config=%+v", rescuePolicy.Name, rescuePolicy.Config)
	}
	retryProfile := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_RETRY_PROFILE") == "1"
	faultTriggerMode := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_FAULT_TRIGGER")
	if faultTriggerMode == "" {
		faultTriggerMode = "fixed_time_legacy"
	}
	if faultTriggerMode != "fixed_time_legacy" && faultTriggerMode != "event_retry" && faultTriggerMode != "b1_budget_exhausted" {
		t.Fatalf("invalid SMP3_V27_KERNEL_BLACKHOLE_FAULT_TRIGGER=%q; want fixed_time_legacy, event_retry, or b1_budget_exhausted", faultTriggerMode)
	}
	eventTriggeredFault := faultTriggerMode == "event_retry"
	budgetTriggeredFault := faultTriggerMode == "b1_budget_exhausted"
	if budgetTriggeredFault {
		if rescuePolicy.Name != "b1" || rescuePolicy.Config == nil || rescuePolicy.Config.BurstBytes != 0 {
			t.Fatalf("b1_budget_exhausted requires B1 with zero burst; policy=%q config=%+v", rescuePolicy.Name, rescuePolicy.Config)
		}
		requireBudgetExhausted = true
	}
	harnessHorizon := v27BlackholeMaxRuntime
	if eventTriggeredFault || budgetTriggeredFault {
		// This extends only the isolated test observation window, not any Core
		// recovery/cooldown threshold. It leaves room for two real cooldowns and
		// active transfer completion after the second fault is removed.
		harnessHorizon = 45 * time.Second
	}
	total := int64(128 << 20)
	rateBPS := int64(5_000_000)
	writeBufferRequest := 0
	if retryProfile {
		total = 128 << 20
		rateBPS = 5_000_000
		writeBufferRequest = 96 << 20
	}
	faultLeg := uint8(1)
	if raw := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_FAULT_LEG"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || parsed > 1 {
			t.Fatalf("invalid SMP3_V27_KERNEL_BLACKHOLE_FAULT_LEG=%q", raw)
		}
		faultLeg = uint8(parsed)
	}
	var firstFaultData chan v27BlackholeCarrierDataEvent
	var firstFaultDataReady chan error
	var probeWriteGateRequests chan v27BlackholeProbeWriteGateRequest
	var recoveryWriteEvents chan v27BlackholeCarrierDataEvent
	if eventTriggeredFault {
		firstFaultData = make(chan v27BlackholeCarrierDataEvent, 1)
		firstFaultDataReady = make(chan error, 1)
		probeWriteGateRequests = make(chan v27BlackholeProbeWriteGateRequest, 1)
		recoveryWriteEvents = make(chan v27BlackholeCarrierDataEvent, 8192)
	}
	restoreSecond := 10
	if retryProfile {
		restoreSecond = 16
	}
	if raw := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_RESTORE_SECOND"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 10 || parsed >= int(harnessHorizon/time.Second) {
			t.Fatalf("invalid SMP3_V27_KERNEL_BLACKHOLE_RESTORE_SECOND=%q", raw)
		}
		restoreSecond = parsed
	}
	expireFeedbackLeg := -1
	if raw := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_EXPIRE_FEEDBACK_LEG"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 1 || !pathAware {
			t.Fatalf("invalid SMP3_V27_KERNEL_BLACKHOLE_EXPIRE_FEEDBACK_LEG=%q (Path-aware must be enabled)", raw)
		}
		expireFeedbackLeg = parsed
	}
	if output, err := exec.Command("nft", "list", "table", "inet", "smp3_v27").CombinedOutput(); err == nil {
		t.Fatalf("refusing to reuse existing nft table in isolated namespace: %s", strings.TrimSpace(string(output)))
	}
	v27BlackholeNFT(t, "add", "table", "inet", "smp3_v27")
	v27BlackholeNFT(t, "add", "chain", "inet", "smp3_v27", "output", "{ type filter hook output priority 0; policy accept; }")
	defer func() { _, _ = exec.Command("nft", "delete", "table", "inet", "smp3_v27").CombinedOutput() }()

	telemetryEnabled := os.Getenv("SMP3_V27_KERNEL_BLACKHOLE_TELEMETRY") != "0"
	traceMode, traceModeOK := v27ResolveRecoveryTraceMode(os.Getenv("SMP3_V27_RECOVERY_TRACE_MODE"), os.Getenv("SMP3_V27_RECOVERY_TRACE"))
	if !traceModeOK {
		t.Fatalf("invalid SMP3_V27_RECOVERY_TRACE_MODE=%q; want off, decision, lite, or full", traceMode)
	}
	recoveryTraceEnabled := traceMode != "off"
	if eventTriggeredFault && (!telemetryEnabled || (traceMode != v27RecoveryTraceModeLite && traceMode != v27RecoveryTraceModeFull)) {
		t.Fatalf("event_retry requires telemetry and recovery trace mode lite or full; telemetry=%v trace=%q", telemetryEnabled, traceMode)
	}
	if budgetTriggeredFault && !telemetryEnabled {
		t.Fatalf("b1_budget_exhausted requires bounded Rescue budget telemetry")
	}
	var recoveryTrace *v27RecoveryCausalityTrace
	if recoveryTraceEnabled {
		recoveryTrace = newV27RecoveryCausalityTraceMode(traceMode)
	}
	var txTelemetry, rxTelemetry *StreamTelemetry
	if telemetryEnabled {
		txTelemetry = &StreamTelemetry{}
		rxTelemetry = &StreamTelemetry{}
	}
	txConfig := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic,
		BandwidthMbps: []uint32{100, 100}, ChunkSize: v27BlackholeChunkSize,
		QueueFrames: 8, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
		AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond,
		RecoveryTimeout: 15 * time.Second, ExperimentalPathAware: pathAware, Telemetry: txTelemetry,
	}
	txConfig.ExperimentalBoundedRescue = rescuePolicy.Config
	var rescueEventRecorder *v27BlackholeRescueEventRecorder
	if rescuePolicy.Name == "b1" {
		rescueEventRecorder = newV27BlackholeRescueEventRecorder()
		txConfig.rescueEventHook = rescueEventRecorder.record
	}
	if recoveryTrace != nil {
		txConfig.recoveryTraceHook = recoveryTrace.record
	}
	if pathAware {
		txConfig.PathRole = StreamPathRoleClient
	}
	rxConfig := txConfig
	rxConfig.Telemetry = rxTelemetry
	// Bounded Rescue is an outbound policy: only the client-side TX Core under
	// test receives B1. The receiving Core must remain at the frozen B0 policy.
	rxConfig.ExperimentalBoundedRescue = nil
	rxConfig.rescueEventHook = nil
	if pathAware {
		rxConfig.PathRole = StreamPathRoleServer
	}
	tx, app := NewStreamEngine(txConfig)
	rx, out := NewStreamEngine(rxConfig)
	if err := v27ValidateBlackholePolicyBinding(tx, rescuePolicy); err != nil {
		t.Fatalf("sender Core did not bind requested Rescue policy: %v", err)
	}
	if err := v27ValidateBlackholePolicyBinding(rx, v27BlackholeRescuePolicy{Name: "b0"}); err != nil {
		t.Fatalf("receiver Core unexpectedly enabled B1: %v", err)
	}
	var ports [2]uint16
	var carriers [2][2]net.Conn
	var pathRTTConns [2][2]*v27BlackholePathRTTDelayConn
	var dataGateConns [2]*v27BlackholeDataGateConn
	var pathFeedbackAtStart [2][2]StreamPathFeedback
	var socketWriteBufferActual [2][2]int
	pathFeedbackFrozen := false
	writeRecorder := &v27BlackholeWriteRecorder{origin: time.Now()}
	var sessionID SessionID
	sessionID[0], sessionID[1] = 0x27, 0x12
	for id := range ports {
		write, read, port := v27BlackholeTCPPair(t)
		ports[id], carriers[id] = port, [2]net.Conn{write, read}
		var txCarrierBase net.Conn = write
		if eventTriggeredFault {
			dataGateConns[id] = &v27BlackholeDataGateConn{
				Conn: write, engine: tx, legID: uint8(id), faultLeg: faultLeg,
				generation:     tx.legGeneration[id].Load() + 1,
				triggerEnabled: true, firstData: firstFaultData,
				firstDataReady: firstFaultDataReady, recoveryWrites: recoveryWriteEvents,
				probeWriteGateRequests: func() chan<- v27BlackholeProbeWriteGateRequest {
					if uint8(id) == faultLeg {
						return probeWriteGateRequests
					}
					return nil
				}(),
				recoveryTrace: recoveryTrace,
			}
			txCarrierBase = dataGateConns[id]
		}
		if retryProfile {
			for endpoint, conn := range []net.Conn{write, read} {
				actual, err := v27BlackholeSetTCPWriteBuffer(conn, writeBufferRequest)
				if err != nil {
					t.Fatalf("set/read isolated carrier write buffer: %v", err)
				}
				socketWriteBufferActual[id][endpoint] = actual
			}
		}
		if pathAware {
			clientAuth, serverAuth := testPathAuthPair(t, sessionID, LegID(id), byte(0x70+id))
			delay := 10 * time.Millisecond
			if id == 1 {
				delay = 200 * time.Millisecond
			}
			pathRTTConns[0][id] = &v27BlackholePathRTTDelayConn{Conn: txCarrierBase, delay: delay, legID: uint8(id), endpoint: "client", recorder: writeRecorder, recoveryTrace: recoveryTrace}
			pathRTTConns[1][id] = &v27BlackholePathRTTDelayConn{Conn: read, delay: delay, legID: uint8(id), endpoint: "server", recorder: writeRecorder, recoveryTrace: recoveryTrace}
			// attachLeg starts its path RTT writer before returning. Bind the
			// wrapper identity to the next generation first, so that the worker
			// never races a post-attach metadata write from this test goroutine.
			pathRTTConns[0][id].generation = tx.legGeneration[id].Load() + 1
			pathRTTConns[1][id].generation = rx.legGeneration[id].Load() + 1
			// The coalescer must be outside the RTT wrapper: net.Buffers emits
			// DATA header and payload as separate Write calls on wrappers that do
			// not implement net.BuffersWriter.
			txCarrier := &p0cdDataFrameCoalescer{Conn: pathRTTConns[0][id]}
			rxCarrier := &p0cdDataFrameCoalescer{Conn: pathRTTConns[1][id]}
			if err := tx.AttachLegWithPathAuth(LegID(id), txCarrier, nil, clientAuth); err != nil {
				t.Fatalf("attach path-aware client leg%d: %v", id, err)
			}
			if err := rx.AttachLegWithPathAuth(LegID(id), rxCarrier, nil, serverAuth); err != nil {
				t.Fatalf("attach path-aware server leg%d: %v", id, err)
			}
			if tx.legGeneration[id].Load() != pathRTTConns[0][id].generation || rx.legGeneration[id].Load() != pathRTTConns[1][id].generation {
				t.Fatalf("path RTT wrapper generation mismatch leg%d client=%d/%d server=%d/%d", id,
					pathRTTConns[0][id].generation, tx.legGeneration[id].Load(),
					pathRTTConns[1][id].generation, rx.legGeneration[id].Load())
			}
		} else {
			if err := tx.AttachLeg(LegID(id), &p0cdDataFrameCoalescer{Conn: txCarrierBase}, nil); err != nil {
				t.Fatal(err)
			}
			if err := rx.AttachLeg(LegID(id), read, nil); err != nil {
				t.Fatal(err)
			}
		}
		if dataGateConns[id] != nil {
			dataGateConns[id].generation = tx.legGeneration[id].Load()
		}
	}
	sessionIdentity := hex.EncodeToString(sessionID[:])
	carrierIdentityByLeg := map[string]string{
		"leg0": "kernel-blackhole-loopback-tcp/" + strconv.FormatUint(uint64(ports[0]), 10),
		"leg1": "kernel-blackhole-loopback-tcp/" + strconv.FormatUint(uint64(ports[1]), 10),
	}
	telemetryContext := func(engineRole string) map[string]any {
		return map[string]any{
			"session_id": sessionIdentity, "engine_role": engineRole,
			"carrier_id_by_leg": carrierIdentityByLeg,
		}
	}
	defer func() {
		_ = app.Close()
		_ = out.Close()
		tx.Close()
		rx.Close()
		for _, pair := range carriers {
			_ = pair[0].Close()
			_ = pair[1].Close()
		}
	}()
	if pathAware {
		for id := uint8(0); id < 2; id++ {
			pathFeedbackAtStart[0][id] = v27BlackholeWaitPathFeedback(t, tx, id, StreamRTTClientToServer)
			pathFeedbackAtStart[1][id] = v27BlackholeWaitPathFeedback(t, rx, id, StreamRTTServerToClient)
			// The delayed first probe/echo establishes authenticated, same-Carrier
			// test feedback. Stop injecting synchronous RTT-frame write service
			// time after both endpoint snapshots exist: it is not network
			// propagation and can occupy the serial Core writer ahead of recovery
			// DATA for 200 ms per queued control frame. Subsequent synthetic RTT
			// controls are recorded as fixture drops; the initial samples remain
			// fresh for this bounded 128 MiB run.
			pathRTTConns[0][id].dropRTT.Store(true)
			pathRTTConns[1][id].dropRTT.Store(true)
		}
		pathFeedbackFrozen = true
		if pathFeedbackAtStart[0][0].RTT >= pathFeedbackAtStart[0][1].RTT || pathFeedbackAtStart[1][0].RTT >= pathFeedbackAtStart[1][1].RTT {
			t.Fatalf("authenticated same-Carrier RTT feedback did not distinguish test service paths: client=%+v server=%+v", pathFeedbackAtStart[0], pathFeedbackAtStart[1])
		}
		for engineID := range pathFeedbackAtStart {
			if pathFeedbackAtStart[engineID][0].RTT > 100*time.Millisecond || pathFeedbackAtStart[engineID][1].RTT < 250*time.Millisecond {
				t.Fatalf("path RTT service-delay bounds not met engine=%d feedback=%+v", engineID, pathFeedbackAtStart[engineID])
			}
		}
	}

	v27BlackholeJSON(t, map[string]any{
		"event": "isolated_carriers_ready", "network_namespace": selfNS,
		"interface": "lo", "leg_tcp_ports": ports,
		"session_id": sessionIdentity, "carrier_id_by_leg": carrierIdentityByLeg,
		"telemetry_enabled":  telemetryEnabled,
		"path_aware_enabled": pathAware, "fault_leg": faultLeg,
		"path_feedback_frozen_after_start": pathFeedbackFrozen,
		"fault_leg_role":                   map[bool]string{true: "preferred_fast", false: "slow"}[faultLeg == 0],
		"expire_feedback_leg_at_close":     expireFeedbackLeg,
		"fault":                            "nft output drops TCP packets matching source or destination port for the selected leg",
		"fault_trigger_mode":               faultTriggerMode,
		"network_model": func() string {
			if pathAware {
				return "real loopback TCP with nftables packet drops; initial authenticated same-Carrier test RTT samples use per-leg RTT-frame write delay, later RTT control writes are fixture-dropped to freeze the fresh snapshots; no DATA RTT or bandwidth shaping"
			}
			return "loopback TCP; no RTT or bandwidth shaping"
		}(),
		"path_feedback_client_to_server": pathFeedbackAtStart[0],
		"path_feedback_server_to_client": pathFeedbackAtStart[1],
	})

	source := &v27BlackholeSource{total: total, rateBPS: rateBPS}
	sink := &v27BlackholeSink{hash: sha256.New()}
	start := time.Now()
	writeRecorder.setTransferStart(start)
	writeDone := make(chan v27BlackholeCopyResult, 1)
	readDone := make(chan v27BlackholeCopyResult, 1)
	readerObserver := &v27BlackholeReadObserver{Conn: out}
	stopWatch, watchDone := make(chan struct{}), make(chan struct{})
	statePoints := make(chan v27BlackholeAdmission, 4096)
	stateTransitions := make(chan v27BlackholeAdmission, 128)
	initialAdmission := v27BlackholeAdmissionPoint(tx, faultLeg)
	initialAdmission.observedAt = start
	if initialAdmission.state != "normal" {
		t.Fatalf("event-triggered recovery test must begin normal, got %+v", initialAdmission)
	}
	statePoints <- initialAdmission
	stateTransitions <- initialAdmission
	go v27BlackholeWatchAdmission(tx, faultLeg, start, stopWatch, statePoints, stateTransitions, watchDone)
	var watchStopOnce sync.Once
	stopWatcher := func() { watchStopOnce.Do(func() { close(stopWatch) }) }
	defer stopWatcher()
	var faultControlStop chan struct{}
	var faultControlDone chan v27BlackholeFaultControlResult
	faultControlCollected := false
	if eventTriggeredFault {
		faultControlStop = make(chan struct{})
		faultControlDone = make(chan v27BlackholeFaultControlResult, 1)
		go v27BlackholeRunEventFaultController(ports[faultLeg], faultLeg, start,
			firstFaultData, firstFaultDataReady, probeWriteGateRequests, recoveryWriteEvents,
			stateTransitions, faultControlStop, faultControlDone)
		defer func() {
			if !faultControlCollected {
				close(faultControlStop)
				<-faultControlDone
			}
		}()
	}
	tcpInfoSamples := []map[string]any{{"event": "tcp_info_before_transfer", "elapsed_ns": int64(0), "sockets": v27BlackholeTCPInfoSet(carriers)}}
	go func() {
		n, err := io.CopyBuffer(app, source, make([]byte, v27BlackholeChunkSize))
		finished := time.Now()
		pathFeedbackForClose := false
		feedbackAgedForClose := false
		if err == nil {
			if expireFeedbackLeg >= 0 {
				pathRTTConns[0][expireFeedbackLeg].dropRTT.Store(true)
				pathRTTConns[1][expireFeedbackLeg].dropRTT.Store(true)
				time.Sleep(500 * time.Millisecond)
				for attempt := 0; attempt < 5; attempt++ {
					txAged := v27BlackholeAgeFeedback(tx, uint8(expireFeedbackLeg), StreamRTTClientToServer)
					rxAged := v27BlackholeAgeFeedback(rx, uint8(expireFeedbackLeg), StreamRTTServerToClient)
					_, txFresh := tx.PathFeedback(LegID(expireFeedbackLeg))
					_, rxFresh := rx.PathFeedback(LegID(expireFeedbackLeg))
					feedbackAgedForClose = txAged && rxAged && !txFresh && !rxFresh
					if feedbackAgedForClose {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			if pathAware {
				_, _, pathFeedbackForClose = tx.preferredCloseLeg(tx.availableLegs())
			}
			_ = app.Close()
		}
		writeDone <- v27BlackholeCopyResult{
			bytes: n, err: err, finish: finished,
			pathFeedbackForClose: pathFeedbackForClose, feedbackAgedForClose: feedbackAgedForClose,
		}
	}()
	go func() {
		n, err := io.CopyBuffer(sink, readerObserver, make([]byte, v27BlackholeChunkSize))
		readDone <- v27BlackholeCopyResult{bytes: n, err: err, terminalErr: readerObserver.terminalError(), finish: time.Now()}
	}()

	var writer, reader *v27BlackholeCopyResult
	var dropEvidence string
	var reportedProbeEventIndex uint64
	var reportedDroppedProbeEvents uint64
	var budgetFaultInstalled, budgetFaultRestored, budgetExhaustedBeforeFault bool
	var budgetFaultInstallElapsedNS, budgetFaultRestoreElapsedNS int64
	var budgetFaultStatsAtInstall StreamBoundedRescueStats
	const budgetFaultHold = 7 * time.Second
	for second := 1; second <= int(harnessHorizon/time.Second) &&
		(writer == nil || reader == nil || (budgetFaultInstalled && !budgetFaultRestored)); second++ {
		v27BlackholeWaitUntil(t, start.Add(time.Duration(second)*time.Second))
		if faultTriggerMode == "fixed_time_legacy" {
			if second == 3 {
				v27BlackholeInstallDrop(t, ports[faultLeg])
				v27BlackholeJSON(t, map[string]any{"event": "carrier_tcp_blackhole_enabled", "fault_leg": faultLeg, "elapsed_ms": time.Since(start).Milliseconds(), "nft": v27BlackholeNFT(t, "-a", "list", "chain", "inet", "smp3_v27", "output")})
			}
			if second == 4 || second == restoreSecond {
				dropEvidence = v27BlackholeNFT(t, "-a", "list", "chain", "inet", "smp3_v27", "output")
			}
			if second == restoreSecond {
				v27BlackholeJSON(t, map[string]any{"event": "carrier_tcp_blackhole_removed", "fault_leg": faultLeg, "elapsed_ms": time.Since(start).Milliseconds(), "drops_before_restore": dropEvidence})
				v27BlackholeNFT(t, "flush", "chain", "inet", "smp3_v27", "output")
			}
		}
		if budgetTriggeredFault {
			if !budgetFaultInstalled {
				budgetState := tx.TelemetrySnapshot().BoundedRescue
				if v27BlackholeBudgetExhausted(budgetState) {
					budgetFaultInstalled = true
					budgetExhaustedBeforeFault = true
					budgetFaultStatsAtInstall = budgetState
					budgetFaultInstallElapsedNS = time.Since(start).Nanoseconds()
					v27BlackholeInstallDrop(t, ports[faultLeg])
					v27BlackholeJSON(t, map[string]any{
						"event":  "carrier_tcp_blackhole_enabled_after_b1_budget_exhausted",
						"policy": rescuePolicy.Name, "fault_leg": faultLeg,
						"elapsed_ns": budgetFaultInstallElapsedNS, "budget": budgetState,
						"nft": v27BlackholeNFT(t, "-a", "list", "chain", "inet", "smp3_v27", "output"),
					})
				}
			} else if !budgetFaultRestored && time.Since(start) >= time.Duration(budgetFaultInstallElapsedNS)+budgetFaultHold {
				dropEvidence = v27BlackholeNFT(t, "-a", "list", "chain", "inet", "smp3_v27", "output")
				v27BlackholeJSON(t, map[string]any{
					"event":  "carrier_tcp_blackhole_removed_after_b1_budget_exhausted",
					"policy": rescuePolicy.Name, "fault_leg": faultLeg,
					"elapsed_ns": time.Since(start).Nanoseconds(), "drops_before_restore": dropEvidence,
					"budget_at_install": budgetFaultStatsAtInstall,
				})
				v27BlackholeNFT(t, "flush", "chain", "inet", "smp3_v27", "output")
				budgetFaultRestored = true
				budgetFaultRestoreElapsedNS = time.Since(start).Nanoseconds()
			}
		}
		tcpInfoSample := map[string]any{"event": "tcp_info_sample", "second": second, "elapsed_ns": time.Since(start).Nanoseconds(), "sockets": v27BlackholeTCPInfoSet(carriers)}
		tcpInfoSamples = append(tcpInfoSamples, tcpInfoSample)
		if writer == nil {
			select {
			case value := <-writeDone:
				writer = &value
			default:
			}
		}
		if reader == nil {
			select {
			case value := <-readDone:
				reader = &value
			default:
			}
		}
		stats := tx.TelemetrySnapshot()
		for _, event := range stats.Telemetry.ProbeUniqueServiceLowEvents {
			if event.EventIndex <= reportedProbeEventIndex {
				continue
			}
			v27BlackholeJSON(t, map[string]any{
				"event":             "probe_unique_service_low_trigger",
				"telemetry_context": telemetryContext("client_tx_engine"),
				"snapshot":          event,
			})
			reportedProbeEventIndex = event.EventIndex
		}
		if stats.Telemetry.DroppedProbeUniqueServiceLowEvents > reportedDroppedProbeEvents {
			v27BlackholeJSON(t, map[string]any{
				"event":             "probe_unique_service_low_events_dropped",
				"telemetry_context": telemetryContext("client_tx_engine"),
				"count":             stats.Telemetry.DroppedProbeUniqueServiceLowEvents,
			})
			reportedDroppedProbeEvents = stats.Telemetry.DroppedProbeUniqueServiceLowEvents
		}
		point := v27BlackholeAdmissionPoint(tx, faultLeg)
		v27BlackholeJSON(t, map[string]any{
			"event": "second", "second": second, "telemetry_context": telemetryContext("client_tx_engine"),
			"source_generated_bytes": source.read.Load(), "application_received_bytes": sink.bytes.Load(),
			"unique_acked_bytes": stats.TxAckedUsefulTotal, "unique_acked_by_leg": stats.TxAckedUsefulByLeg,
			"outstanding_frames": stats.OutstandingFrames, "outstanding_bytes": stats.OutstandingBytes,
			"tx_admission": point.json(), "goroutines": runtime.NumGoroutine(),
			"tcp_info":                         tcpInfoSample["sockets"],
			"bounded_rescue":                   stats.BoundedRescue,
			"budget_triggered_fault_installed": budgetFaultInstalled,
			"budget_exhausted_before_fault":    budgetExhaustedBeforeFault,
			"nft_chain":                        v27BlackholeNFT(t, "-a", "list", "chain", "inet", "smp3_v27", "output"),
		})
	}
	stopWatcher()
	<-watchDone
	var faultControlResult v27BlackholeFaultControlResult
	if eventTriggeredFault {
		select {
		case faultControlResult = <-faultControlDone:
			faultControlCollected = true
		case <-time.After(250 * time.Millisecond):
			close(faultControlStop)
			faultControlResult = <-faultControlDone
			faultControlCollected = true
		}
		dropEvidence = faultControlResult.DropEvidence
		for _, event := range faultControlResult.Timeline {
			v27BlackholeJSON(t, event)
		}
	}
	close(statePoints)
	var stateSequence []string
	var stateEvents []map[string]any
	stateObservations := make([]v27BlackholeAdmission, 0, len(statePoints))
	proofObserved := false
	for point := range statePoints {
		stateObservations = append(stateObservations, point)
		stateEvents = append(stateEvents, map[string]any{
			"telemetry_context": telemetryContext("client_tx_engine"),
			"elapsed_ns":        point.observedAt.Sub(start).Nanoseconds(),
			"elapsed_ms":        point.observedAt.Sub(start).Milliseconds(), "state": point.state,
			"reason": point.reason, "proofs": point.proofs, "proof_after": point.proofAfter,
			"proof_bytes": point.proofBytes, "byte_limit": point.byteLimit,
			"limit": point.limit, "generation": point.generation,
			"probe_timeout_timing": point.probeTimeoutTiming,
		})
		if len(stateSequence) == 0 || stateSequence[len(stateSequence)-1] != point.state {
			stateSequence = append(stateSequence, point.state)
		}
		if point.state == "recovering" && point.proofs > 0 {
			proofObserved = true
		}
	}
	if writer == nil {
		select {
		case value := <-writeDone:
			writer = &value
		default:
		}
	}
	if reader == nil {
		select {
		case value := <-readDone:
			reader = &value
		default:
		}
	}
	stats := tx.TelemetrySnapshot()
	rxStats := rx.TelemetrySnapshot()
	tcpRetransAfter, tcpRetransAfterOK := v27BlackholeTCPRetransSegs()
	tcpRetransDeltaOK := tcpRetransBeforeOK && tcpRetransAfterOK && tcpRetransAfter >= tcpRetransBefore
	var tcpRetransDelta uint64
	if tcpRetransDeltaOK {
		tcpRetransDelta = tcpRetransAfter - tcpRetransBefore
	}
	finalAdmission := v27BlackholeAdmissionPoint(tx, faultLeg)
	rescueEventSnapshot := rescueEventRecorder.snapshot()
	budgetExhausted := v27BlackholeBudgetExhausted(stats.BoundedRescue)
	budgetExhaustionOK := !requireBudgetExhausted || (budgetExhausted && stats.BoundedRescue.FailureRecoveryExempted > 0)
	budgetFaultGateOK := !budgetTriggeredFault || (budgetFaultInstalled && budgetFaultRestored && budgetExhaustedBeforeFault &&
		v27BlackholeBudgetExhausted(budgetFaultStatsAtInstall))
	rescuePolicyCoreEnabled := stats.BoundedRescue.Enabled
	rescuePolicyBindingOK := rescuePolicyCoreEnabled == (rescuePolicy.Name == "b1") &&
		(rescuePolicy.Name != "b1" || (rescuePolicy.Config != nil && stats.BoundedRescue.BudgetBasisPoints == rescuePolicy.Config.BudgetBasisPoints && stats.BoundedRescue.BurstBytes == rescuePolicy.Config.BurstBytes))
	recoveryTraceEvents, recoveryTraceEventCount, recoveryTraceDropped := recoveryTrace.snapshot()
	actualHash := hex.EncodeToString(sink.hash.Sum(nil))
	expectedHash := v27BlackholeHash(total)
	readerEOF := reader != nil && reader.terminalErr == io.EOF
	finalErr := error(nil)
	readerTerminalError := "NOT_AVAILABLE"
	if reader != nil {
		readerTerminalError = "nil"
	}
	if reader != nil && !readerEOF {
		finalErr = reader.terminalErr
	}
	if finalErr != nil {
		readerTerminalError = finalErr.Error()
	} else if readerEOF {
		readerTerminalError = "io.EOF"
	}
	legacyExactStateSequenceOK := strings.Join(stateSequence, "->") == "normal->restricted->recovering->normal"
	recoveryGate := v27BlackholeRecoveryStateGateWithHorizon(
		start,
		stateObservations,
		stats.Telemetry.ProbeUniqueServiceLowEvents,
		stats.Telemetry.DroppedProbeUniqueServiceLowEvents,
		txTelemetry != nil,
		faultLeg,
		tx.legGeneration[faultLeg].Load(),
		tx.legGeneration[1-faultLeg].Load(),
		harnessHorizon,
		recoveryTraceEvents,
	)
	stateOK := recoveryGate.OK
	if faultControlResult.ProbeSocketWriteStartNS > 0 && faultControlResult.ProbeGateReleaseElapsedNS > 0 {
		faultControlResult.ProbeGateToSocketWriteNS = faultControlResult.ProbeSocketWriteStartNS - faultControlResult.ProbeGateReleaseElapsedNS
	}
	txProbeHistoryComplete, txProbeHistoryReason := v27ProbeEventHistoryStatus(txTelemetry != nil, stats.Telemetry.DroppedProbeUniqueServiceLowEvents)
	rxProbeHistoryComplete, rxProbeHistoryReason := v27ProbeEventHistoryStatus(rxTelemetry != nil, rxStats.Telemetry.DroppedProbeUniqueServiceLowEvents)
	result := map[string]any{
		"event": "result", "state_sequence": stateSequence, "state_sequence_ok": stateOK,
		"legacy_exact_single_cycle_sequence_ok": legacyExactStateSequenceOK,
		"state_graph_contract":                  "normal->restricted->recovering; recovering->restricted only for audited probe_unique_service_low evidence or a directly traced Applied probe_timeout action matching the overdue ACK frontier and leg generation; each retry must observe the existing cooldown; retries bounded by run horizon; terminal normal/unique_ack_recovered",
		"state_graph":                           recoveryGate, "state_events": stateEvents,
		"session_id": sessionIdentity, "carrier_id_by_leg": carrierIdentityByLeg,
		"unique_ack_proof_observed": proofObserved, "final_admission": finalAdmission.json(),
		"writer_bytes": func() int64 {
			if writer != nil {
				return writer.bytes
			}
			return -1
		}(),
		"writer_error": func() string {
			if writer != nil && writer.err != nil {
				return writer.err.Error()
			}
			return ""
		}(),
		"reader_bytes": func() int64 {
			if reader != nil {
				return reader.bytes
			}
			return -1
		}(),
		"reader_copy_error": func() string {
			if reader != nil && reader.err != nil {
				return reader.err.Error()
			}
			return ""
		}(),
		"reader_terminal_error": readerTerminalError,
		"strict_io_eof":         readerEOF, "received_bytes": sink.bytes.Load(), "expected_bytes": total,
		"sha256": actualHash, "sha256_expected": expectedHash,
		"unique_acked_bytes":                               stats.TxAckedUsefulTotal,
		"bounded_rescue_policy_requested":                  rescuePolicy.Name,
		"bounded_rescue_config_requested":                  rescuePolicy.Config,
		"bounded_rescue_policy_core_enabled":               rescuePolicyCoreEnabled,
		"bounded_rescue_policy_binding_ok":                 rescuePolicyBindingOK,
		"bounded_rescue_budget_exhaustion_required":        requireBudgetExhausted,
		"bounded_rescue_budget_exhausted":                  budgetExhausted,
		"bounded_rescue_budget_exhaustion_gate_ok":         budgetExhaustionOK && budgetFaultGateOK,
		"bounded_rescue_budget_exhausted_before_blackhole": budgetExhaustedBeforeFault,
		"bounded_rescue_budget_at_blackhole_install":       budgetFaultStatsAtInstall,
		"budget_blackhole_installed":                       budgetFaultInstalled,
		"budget_blackhole_restored":                        budgetFaultRestored,
		"budget_blackhole_install_elapsed_ns":              budgetFaultInstallElapsedNS,
		"budget_blackhole_restore_elapsed_ns":              budgetFaultRestoreElapsedNS,
		"bounded_rescue_final_stats":                       stats.BoundedRescue,
		"bounded_rescue_event_capture":                     rescueEventSnapshot,
		"tx_outstanding_frames":                            stats.OutstandingFrames, "tx_outstanding_bytes": stats.OutstandingBytes,
		"rx_pending_frames": rxStats.RxPendingFrames, "rx_pending_bytes": rxStats.RxPendingBytes,
		"blackhole_drop_packets": v27BlackholeDropCount(dropEvidence), "drop_evidence": dropEvidence,
		"blackhole_restore_second":      restoreSecond,
		"fault_trigger_mode":            faultTriggerMode,
		"event_triggered_fault_control": faultControlResult,
		"phase16_fault_phases": map[string]any{
			"initial_blackhole_removed_after_restriction":   faultControlResult.InitialBlackholeRemovedAfterRestriction,
			"retry_blackhole_installed_at_probe_write_gate": faultControlResult.RetryBlackholeInstalled,
			"retry_blackhole_removed_after_rerestriction":   faultControlResult.RetryBlackholeRemovedAfterRerestriction,
			"initial_drop_packets":                          v27BlackholeDropCount(faultControlResult.InitialDropEvidence),
			"retry_drop_packets":                            v27BlackholeDropCount(faultControlResult.RetryDropEvidence),
			"probe_wrapper_gate_elapsed_ns":                 faultControlResult.ProbeGateElapsedNS,
			"probe_gate_release_elapsed_ns":                 faultControlResult.ProbeGateReleaseElapsedNS,
			"probe_gate_release_to_net_conn_write_ns":       faultControlResult.ProbeGateToSocketWriteNS,
			"net_conn_write_entry_elapsed_ns":               faultControlResult.ProbeSocketWriteStartNS,
		},
		"harness_max_runtime_ns": int64(harnessHorizon),
		"event_retry_load_profile": func() string {
			if retryProfile {
				return "phase15_128MiB_5MBps_large_socket_buffer"
			}
			if eventTriggeredFault {
				return "phase16_two_phase_event_blackhole_128MiB_5MBps_default_socket_buffer"
			}
			return "legacy_128MiB_5MBps"
		}(),
		"tcp_info_samples":      tcpInfoSamples,
		"phase15_retry_profile": retryProfile, "source_total_bytes": total, "source_rate_bytes_per_second": rateBPS,
		"socket_write_buffer_requested_bytes": writeBufferRequest,
		"socket_write_buffer_actual_bytes":    socketWriteBufferActual,
		"tcp_retrans_segments_before":         tcpRetransBefore, "tcp_retrans_segments_before_valid": tcpRetransBeforeOK,
		"tcp_retrans_segments_after": tcpRetransAfter, "tcp_retrans_segments_after_valid": tcpRetransAfterOK,
		"tcp_retrans_segments_delta": tcpRetransDelta, "tcp_retrans_segments_delta_valid": tcpRetransDeltaOK,
		"tx_written_bytes_by_leg": stats.TxWrittenBytesByLeg, "tx_retry_written_bytes_by_leg": stats.TxRetryWrittenBytesByLeg,
		"tx_retransmit_bytes_by_leg": stats.TxRetransmitBytesByLeg, "frontier_rescue_attempts": stats.FrontierRescueAttempts,
		"data_tx_attempt_frames_by_leg":    stats.Telemetry.DataTxAttemptFrames,
		"data_tx_retransmit_frames_by_leg": stats.Telemetry.DataTxRetransmitFrames,
		"rescue_attempt_frames_by_leg":     stats.Telemetry.RescueAttemptFrames, "rescue_attempt_bytes_by_leg": stats.Telemetry.RescueAttemptBytes,
		"rx_data_duplicate_frames_by_leg": rxStats.Telemetry.DataRxDuplicateFrames,
		"rx_data_duplicate_bytes_by_leg":  rxStats.Telemetry.DataRxDuplicateBytes,
		"path_aware_enabled":              pathAware, "fault_leg": faultLeg,
		"expire_feedback_leg_at_close":                         expireFeedbackLeg,
		"path_feedback_preference_eligible_at_close":           writer != nil && writer.pathFeedbackForClose,
		"feedback_aged_for_close":                              writer != nil && writer.feedbackAgedForClose,
		"path_aware_wire_writes":                               writeRecorder.snapshot(),
		"recovery_trace_mode":                                  traceMode,
		"recovery_trace_enabled":                               recoveryTraceEnabled,
		"recovery_trace_event_count":                           recoveryTraceEventCount,
		"recovery_trace_dropped":                               recoveryTraceDropped,
		"recovery_trace_complete":                              recoveryTraceEnabled && recoveryTraceDropped == 0 && uint64(len(recoveryTraceEvents)) == recoveryTraceEventCount,
		"recovery_trace_events":                                recoveryTraceEvents,
		"telemetry_enabled":                                    telemetryEnabled,
		"probe_unique_service_low_telemetry_context":           telemetryContext("client_tx_engine"),
		"rx_probe_unique_service_low_telemetry_context":        telemetryContext("server_rx_engine"),
		"probe_unique_service_low_events":                      stats.Telemetry.ProbeUniqueServiceLowEvents,
		"probe_unique_service_low_events_dropped":              stats.Telemetry.DroppedProbeUniqueServiceLowEvents,
		"probe_unique_service_low_events_complete":             txProbeHistoryComplete,
		"probe_unique_service_low_events_incomplete_reason":    txProbeHistoryReason,
		"rx_probe_unique_service_low_events":                   rxStats.Telemetry.ProbeUniqueServiceLowEvents,
		"rx_probe_unique_service_low_events_dropped":           rxStats.Telemetry.DroppedProbeUniqueServiceLowEvents,
		"rx_probe_unique_service_low_events_complete":          rxProbeHistoryComplete,
		"rx_probe_unique_service_low_events_incomplete_reason": rxProbeHistoryReason,
	}
	v27BlackholeJSON(t, result)
	expectedEventRetrySequence := "normal->restricted->recovering->restricted->recovering->normal"
	eventRetryOK := !eventTriggeredFault || (faultControlResult.InitialBlackholeRemovedAfterRestriction &&
		faultControlResult.RetryBlackholeInstalled && faultControlResult.RetryBlackholeRemovedAfterRerestriction &&
		faultControlResult.RestoredAfterRecoveryRetry && faultControlResult.InstallError == "" &&
		faultControlResult.PrewriteBoundValid && faultControlResult.ProbeGateToSocketWriteNS >= 0 &&
		strings.Join(stateSequence, "->") == expectedEventRetrySequence && recoveryGate.RecoveryRetryCount >= 1 &&
		v27BlackholeDropCount(faultControlResult.InitialDropEvidence) > 0 &&
		v27BlackholeDropCount(faultControlResult.RetryDropEvidence) > 0)
	pass := writer != nil && writer.err == nil && writer.bytes == total &&
		reader != nil && reader.err == nil && reader.terminalErr == io.EOF && reader.bytes == total &&
		sink.bytes.Load() == uint64(total) && actualHash == expectedHash &&
		stats.TxAckedUsefulTotal == uint64(total) && stats.OutstandingFrames == 0 && stats.OutstandingBytes == 0 &&
		stateOK && proofObserved && finalAdmission.state == "normal" && finalAdmission.reason == "unique_ack_recovered" &&
		rescuePolicyBindingOK && budgetExhaustionOK && budgetFaultGateOK &&
		(!pathAware || (expireFeedbackLeg >= 0 && writer != nil && !writer.pathFeedbackForClose && writer.feedbackAgedForClose) || (expireFeedbackLeg < 0 && writer != nil && writer.pathFeedbackForClose)) &&
		rxStats.RxPendingFrames == 0 && rxStats.RxPendingBytes == 0 && v27BlackholeDropCount(dropEvidence) > 0 && eventRetryOK
	if !pass {
		t.Errorf("strict blackhole EOF/data/SHA/ACK/TX-RX ledger/recovery gate failed")
	}
}

func v27BlackholeDropCount(rules string) uint64 {
	fields := strings.Fields(rules)
	var count uint64
	for i := 0; i+2 < len(fields); i++ {
		if fields[i] == "counter" && fields[i+1] == "packets" {
			if value, err := strconv.ParseUint(fields[i+2], 10, 64); err == nil {
				count += value
			}
		}
	}
	return count
}

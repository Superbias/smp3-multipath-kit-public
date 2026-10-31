package smp3core

import "time"

// phase20PolicyAShadowEvaluation reproduces the isolated Policy A eligibility
// predicate as a read-only diagnostic. It is called only from opt-in trace
// runs and never changes a Policy B decision.
func (c *StreamEngine) phase20PolicyAShadowEvaluation(candidate StreamTXFrontierCandidate, now time.Time) streamRecoveryTraceEvent {
	ownerID := candidate.owner
	event := streamRecoveryTraceEvent{
		Kind: "phase19_policy_a_shadow_evaluation", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
		RecordSequence: 0, FrameSequence: 0, LegID: ownerID, PeerLegID: -1,
		Action: "not_eligible", ObservedAt: now,
	}
	if candidate.record != nil {
		event.RecordSequence = candidate.record.Sequence()
		event.FrameSequence = candidate.record.Sequence()
		event.PayloadBytes = len(candidate.record.Payload())
	}
	if ownerID < 0 || ownerID >= 2 {
		event.Action = "invalid_owner_leg"
		return event
	}
	owner := c.getLeg(uint8(ownerID))
	alternateID := int16(1 - ownerID)
	event.PeerLegID = alternateID
	alternate := c.getLeg(uint8(alternateID))
	if owner != nil {
		event.LegGeneration = c.legGeneration[uint8(ownerID)].Load()
	}
	if !c.slowLegProtection() {
		event.Action = "slow_leg_protection_disabled"
		return event
	}
	if !candidate.exists || !candidate.overdue || candidate.record == nil {
		event.Action = "frontier_not_overdue_or_missing"
		return event
	}
	if owner == nil || alternate == nil {
		event.Action = "owner_or_alternate_unavailable"
		return event
	}
	ledger := c.txLedger.probeReferenceSnapshot(candidate.record, false)
	event.RecordGeneration = ledger.OriginGeneration
	event.LedgerObservedAt = ledger.LedgerObservedAt
	event.LedgerPresent = ledger.Exists
	event.InTransit = ledger.InTransit
	event.TransitLeg = ledger.TransitLeg
	event.RescueInTransit = ledger.RescueInTransit
	event.RescueLeg = ledger.RescueLeg
	event.RescueSince = ledger.RescueSince
	event.FirstWriteAt = ledger.FirstWriteAt
	event.LastSentAt = ledger.LastSentAt
	event.FrontierAt = ledger.FrontierAt
	event.AckedNext = ledger.AckedNext
	event.SendCount = ledger.SendCount
	if !ledger.Exists || !ledger.InTransit || ledger.RescueInTransit || ledger.TransitLeg != uint8(ownerID) {
		event.Action = "record_not_active_on_original_owner"
		return event
	}
	startedNS := owner.writeStartedNs.Load()
	if startedNS == 0 {
		event.Action = "no_active_owner_write"
		return event
	}
	writeStartedAt := time.Unix(0, startedNS)
	writeAge := now.Sub(writeStartedAt)
	deadline := 2 * time.Second
	if c.cfg.RetransmitTimeout > deadline {
		deadline = c.cfg.RetransmitTimeout
	}
	decision := &streamRecoveryDecisionSnapshot{
		FrontierOverdue: candidate.overdue, CarrierWriteInProgress: true,
		CarrierWriteLeg: ownerID, CarrierWriteAge: writeAge, WriteStartedAt: writeStartedAt,
		OwnEffectiveWeight:       c.effectiveSchedulerWeight(owner),
		AlternateEffectiveWeight: c.effectiveSchedulerWeight(alternate),
	}
	event.Decision = decision
	if writeAge < 0 {
		event.Action = "write_clock_in_future"
		return event
	}
	if writeAge >= deadline {
		event.Action = "owner_write_exceeded_policy_deadline"
		return event
	}
	ownerWeight := decision.OwnEffectiveWeight
	alternateWeight := decision.AlternateEffectiveWeight
	if ownerWeight <= 0 || alternateWeight <= 0 || ownerWeight*4 >= alternateWeight {
		event.Action = "effective_weight_gap_insufficient"
		return event
	}
	c.admission.mu.Lock()
	state := c.admission.legs[uint8(ownerID)]
	c.admission.mu.Unlock()
	if state.recovering || state.probePending || (state.restricted && state.reason != "sustained_frontier") {
		event.StateBefore = "restricted"
		event.ReasonBefore = state.reason
		if state.recovering {
			event.StateBefore = "recovering"
			event.Action = "recovery_in_progress"
		} else if state.probePending {
			event.Action = "probe_pending"
		} else {
			event.Action = "non_hol_restriction"
		}
		return event
	}
	event.StateObservedAt = now
	if state.restricted {
		event.StateBefore, event.ReasonBefore = "restricted", state.reason
	} else {
		event.StateBefore = "normal"
	}
	event.Action = "eligible_active_slow_leg_hol"
	event.Applied = true
	return event
}

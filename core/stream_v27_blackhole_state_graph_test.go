package smp3core

import (
	"testing"
	"time"
)

func v27RecoveryGateObservation(start time.Time, elapsed time.Duration, state, reason string, generation uint64, proofAfter uint64) v27BlackholeAdmission {
	return v27BlackholeAdmission{
		state: state, reason: reason, generation: generation, proofAfter: proofAfter,
		observedAt: start.Add(elapsed),
	}
}

func v27RecoveryGateFixture(retries int) (time.Time, []v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
	start := time.Now()
	observations := []v27BlackholeAdmission{
		v27RecoveryGateObservation(start, 0, "normal", "", 1, 0),
		v27RecoveryGateObservation(start, time.Second, "restricted", "frontier_timeout", 1, 0),
		v27RecoveryGateObservation(start, 6*time.Second, "recovering", "unique_probe", 1, 0),
	}
	events := make([]StreamProbeUniqueServiceLowSnapshot, 0, retries)
	recoveryAt := 6 * time.Second
	for index := 0; index < retries; index++ {
		observations = append(observations, v27RecoveryGateObservation(start, recoveryAt+250*time.Millisecond, "recovering", "unique_ack_ramp", 1, uint64(100+index)))
		ackAt := start.Add(recoveryAt + 300*time.Millisecond)
		eventAt := ackAt.Add(time.Millisecond)
		events = append(events, StreamProbeUniqueServiceLowSnapshot{
			EventIndex: uint64(index + 1), ObservedAt: eventAt, AckProgressAt: ackAt,
			ProbeWindowStart: ackAt.Add(-412 * time.Millisecond), ProbeWindow: 412 * time.Millisecond,
			LegID: 1, PeerLegID: 0, StateBefore: "recovering", ReasonBefore: "unique_ack_ramp",
			StateAfter: "restricted", ReasonAfter: "probe_unique_service_low",
			LegGeneration: 1, AckGeneration: 1, PeerGeneration: 1,
			OpeningRateBPS: 4999721.47, TargetRateBPS: 4999721.47 * 0.1, ProbeRateBPS: 476663.95,
			ProbeBytes: 196608, UniqueAckBytes: 131072, UniqueTimelyRecords: 2,
			AckedNext: uint64(1267 + index), BeforeOrdinaryBytes: 131072,
			BeforeOrdinaryFrames: 2, ProbeFrameLimit: 2, MeaningfulDemandBytes: 108995,
		})
		restrictedAt := recoveryAt + 302*time.Millisecond
		observations = append(observations, v27RecoveryGateObservation(start, restrictedAt, "restricted", "probe_unique_service_low", 1, uint64(100+index)))
		recoveryAt += 300*time.Millisecond + streamRestrictionCooldown + time.Millisecond
		observations = append(observations, v27RecoveryGateObservation(start, recoveryAt, "recovering", "unique_probe", 1, uint64(100+index)))
	}
	observations = append(observations, v27RecoveryGateObservation(start, recoveryAt+time.Second, "normal", "unique_ack_recovered", 1, uint64(200+retries)))
	return start, observations, events
}

func v27ProbeTimeoutEvidence(start time.Time, lastPollElapsed, transitionElapsed, age, budget time.Duration, sequence uint64) map[string]any {
	pre := map[string]any{
		"available": true, "observed_elapsed_ns": lastPollElapsed.Nanoseconds(),
		"frontier_sequence": sequence, "ledger_acked_next": sequence,
		"record_still_outstanding": true, "age_ns": age.Nanoseconds(),
		"budget_ns": budget.Nanoseconds(), "overdue_by_ns": (age - budget).Nanoseconds(),
	}
	return map[string]any{
		"available": true, "observed_elapsed_ns": transitionElapsed.Nanoseconds(),
		// The next poll can observe a later frontier after the applied timeout
		// action and intervening ACK progress. This post-sample is intentionally
		// different from the action frontier.
		"frontier_sequence": sequence + 3, "ledger_acked_next": sequence + 3,
		"record_still_outstanding": true, "same_frontier_as_last_recovering_poll": false,
		"last_recovering_poll_gap_ns": (transitionElapsed - lastPollElapsed).Nanoseconds(),
		"last_recovering_poll":        pre,
	}
}

func v27ProbeTimeoutTraceEvents(start, edgeStart, edgeEnd time.Time, leg uint8, generation, sequence uint64) []streamRecoveryTraceEvent {
	policyAt := edgeEnd.Add(-750 * time.Microsecond)
	actionAt := edgeEnd.Add(-400 * time.Microsecond)
	referenceAt := policyAt.Add(-236 * time.Millisecond)
	decision := streamRecoveryTraceEvent{
		EventIndex: 1, ElapsedNS: policyAt.Sub(start).Nanoseconds(), ObservedAt: policyAt,
		Kind: "probe_timeout_decision", LegID: int16(leg), LegGeneration: generation,
		RecordGeneration: generation, RecordSequence: sequence, AckedNext: sequence,
		ReferenceAt: referenceAt, PolicyAt: policyAt, StateObservedAt: policyAt.Add(-time.Microsecond),
		Age: 236 * time.Millisecond, Budget: streamProbeBudget,
		StateBefore: "recovering", ReasonBefore: "unique_ack_ramp", Action: "restrict_ordinary_probe_timeout",
	}
	action := decision
	action.EventIndex = 2
	action.ElapsedNS = actionAt.Sub(start).Nanoseconds()
	action.ObservedAt = actionAt.Add(time.Nanosecond)
	action.Kind = "probe_timeout_action"
	action.ActionAt = actionAt
	action.StateAfter = "restricted"
	action.ReasonAfter = "probe_timeout"
	action.DecisionEventIndex = decision.EventIndex
	action.Applied = true
	if policyAt.Before(edgeStart) || actionAt.After(edgeEnd) {
		return nil
	}
	return []streamRecoveryTraceEvent{decision, action}
}

func v27ProbeTimeoutRecoveryGateFixture() (time.Time, []v27BlackholeAdmission) {
	start := time.Now()
	transitionAt := 7 * time.Second
	lastPollAt := transitionAt - time.Millisecond
	observations := []v27BlackholeAdmission{
		v27RecoveryGateObservation(start, 0, "normal", "initial", 1, 0),
		v27RecoveryGateObservation(start, time.Second, "restricted", "sustained_frontier", 1, 0),
		v27RecoveryGateObservation(start, 6*time.Second, "recovering", "unique_probe", 1, 100),
		{
			state: "restricted", reason: "probe_timeout", generation: 1,
			observedAt:         start.Add(transitionAt),
			probeTimeoutTiming: v27ProbeTimeoutEvidence(start, lastPollAt, transitionAt, 236*time.Millisecond, streamProbeBudget, 100),
		},
		v27RecoveryGateObservation(start, 12*time.Second, "recovering", "unique_probe", 1, 100),
		v27RecoveryGateObservation(start, 13*time.Second, "normal", "unique_ack_recovered", 1, 101),
	}
	return start, observations
}

func TestV27BlackholeRecoveryStateGate(t *testing.T) {
	tests := []struct {
		name           string
		retries        int
		telemetry      bool
		dropped        uint64
		mutate         func([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot)
		wantOK         bool
		wantRetryCount int
	}{
		{name: "single cycle remains accepted", wantOK: true},
		{name: "one evidenced low-service retry", retries: 1, telemetry: true, wantOK: true, wantRetryCount: 1},
		{name: "four retries fit the 30 second harness bound", retries: 4, telemetry: true, wantOK: true, wantRetryCount: 4},
		{name: "retry without snapshot fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				return states, nil
			}},
		{name: "unpaired extra snapshot fails", telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				_, _, oneEvent := v27RecoveryGateFixture(1)
				return states, oneEvent
			}},
		{name: "wrong fault generation fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].LegGeneration++
				return states, events
			}},
		{name: "wrong ACK generation fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].AckGeneration++
				return states, events
			}},
		{name: "wrong peer generation fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].PeerGeneration++
				return states, events
			}},
		{name: "wrong fault leg fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].LegID = 0
				return states, events
			}},
		{name: "rate threshold mismatch fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].ProbeRateBPS = events[0].TargetRateBPS
				return states, events
			}},
		{name: "ordinary demand mismatch fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].BeforeOrdinaryFrames = events[0].ProbeFrameLimit - 1
				return states, events
			}},
		{name: "uncertified re-restriction fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				states[4].reason = "probe_write_start_timeout"
				return states, events
			}},
		{name: "duplicate event index fails", retries: 2, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[1].EventIndex = events[0].EventIndex
				return states, events
			}},
		{name: "ACK frontier regression fails", retries: 2, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[1].AckedNext = events[0].AckedNext
				return states, events
			}},
		{name: "event outside transition time fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				events[0].ObservedAt = states[2].observedAt.Add(-time.Millisecond)
				return states, events
			}},
		{name: "cooldown short by one millisecond fails", retries: 1, telemetry: true, wantOK: false,
			mutate: func(states []v27BlackholeAdmission, events []StreamProbeUniqueServiceLowSnapshot) ([]v27BlackholeAdmission, []StreamProbeUniqueServiceLowSnapshot) {
				states[5].observedAt = events[0].AckProgressAt.Add(streamRestrictionCooldown - 3*v27BlackholeStateWatchInterval)
				states[6].observedAt = states[5].observedAt.Add(time.Second)
				return states, events
			}},
		{name: "dropped event history fails", retries: 1, telemetry: true, dropped: 1, wantOK: false},
		{name: "retry without telemetry fails", retries: 1, wantOK: false},
		{name: "five retries exceed completed-harness bound", retries: 5, telemetry: true, wantOK: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, states, events := v27RecoveryGateFixture(test.retries)
			if test.mutate != nil {
				states, events = test.mutate(states, events)
			}
			gate := v27BlackholeRecoveryStateGate(start, states, events, test.dropped, test.telemetry, 1, 1, 1)
			if gate.OK != test.wantOK {
				t.Fatalf("gate ok=%v want=%v failure=%q sequence=%v retries=%d/%d", gate.OK, test.wantOK, gate.Failure, gate.StateSequence, gate.RecoveryRetryCount, gate.RecoveryRetryLimit)
			}
			if gate.OK && gate.RecoveryRetryCount != test.wantRetryCount {
				t.Fatalf("retry count=%d want=%d", gate.RecoveryRetryCount, test.wantRetryCount)
			}
		})
	}
	t.Run("frontier timeout retry requires an overdue action frontier and cooldown", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
		gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
		if !gate.OK || gate.RecoveryRetryCount != 1 || len(gate.RecoveryRetryReasons) != 1 || gate.RecoveryRetryReasons[0] != "probe_timeout" {
			t.Fatalf("valid frontier-timeout retry rejected: %+v", gate)
		}
	})
	t.Run("frontier timeout without elapsed budget fails", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		pre := states[3].probeTimeoutTiming["last_recovering_poll"].(map[string]any)
		pre["age_ns"] = int64(streamProbeBudget - time.Nanosecond)
		pre["overdue_by_ns"] = int64(-1)
		trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
		gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
		if gate.OK {
			t.Fatalf("frontier-timeout retry without a measured expired budget was accepted: %+v", gate)
		}
	})
	t.Run("frontier timeout accepts later ACK advancement only with applied action proof", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
		gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
		if !gate.OK {
			t.Fatalf("frontier-timeout retry with exact action proof and later ACK advancement was rejected: %+v", gate)
		}
		withoutAction := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1)
		if withoutAction.OK {
			t.Fatalf("frontier-timeout retry without action-time proof was accepted: %+v", withoutAction)
		}
	})
	t.Run("frontier timeout action with wrong identity or no commit fails", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		for _, mutate := range []struct {
			name string
			fn   func([]streamRecoveryTraceEvent)
		}{
			{name: "wrong leg generation", fn: func(events []streamRecoveryTraceEvent) { events[1].LegGeneration++ }},
			{name: "wrong record generation", fn: func(events []streamRecoveryTraceEvent) { events[1].RecordGeneration++ }},
			{name: "wrong sequence", fn: func(events []streamRecoveryTraceEvent) { events[1].RecordSequence++ }},
			{name: "not applied", fn: func(events []streamRecoveryTraceEvent) { events[1].Applied = false }},
			{name: "decision timestamp after action", fn: func(events []streamRecoveryTraceEvent) {
				events[0].PolicyAt = events[1].ActionAt.Add(time.Nanosecond)
				events[1].PolicyAt = events[0].PolicyAt
			}},
			{name: "decision link mismatch", fn: func(events []streamRecoveryTraceEvent) { events[1].DecisionEventIndex++ }},
		} {
			t.Run(mutate.name, func(t *testing.T) {
				trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
				mutate.fn(trace)
				gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
				if gate.OK {
					t.Fatalf("invalid action evidence was accepted: %+v", gate)
				}
			})
		}
	})
	t.Run("decision timestamp may precede poll bracket when applied action is bracketed", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
		pre := states[3].probeTimeoutTiming["last_recovering_poll"].(map[string]any)
		preElapsed := time.Duration(pre["observed_elapsed_ns"].(int64))
		trace[0].PolicyAt = start.Add(preElapsed - time.Nanosecond)
		trace[0].ReferenceAt = trace[0].PolicyAt.Add(-236 * time.Millisecond)
		trace[1].PolicyAt = trace[0].PolicyAt
		trace[1].ReferenceAt = trace[0].ReferenceAt
		gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
		if !gate.OK {
			t.Fatalf("a pre-commit decision just before the last poll rejected its exact applied action: %+v", gate)
		}
	})
	t.Run("applied action outside poll bracket fails", func(t *testing.T) {
		start, states := v27ProbeTimeoutRecoveryGateFixture()
		trace := v27ProbeTimeoutTraceEvents(start, states[2].observedAt, states[3].observedAt, 1, 1, 100)
		trace[1].ActionAt = states[3].observedAt.Add(v27BlackholeStateWatchInterval)
		trace[1].ObservedAt = trace[1].ActionAt.Add(time.Nanosecond)
		gate := v27BlackholeRecoveryStateGate(start, states, nil, 0, true, 1, 1, 1, trace)
		if gate.OK {
			t.Fatalf("applied action outside the bounded transition poll bracket was accepted: %+v", gate)
		}
	})
	t.Run("mixed existing low-service and timeout retries are bounded", func(t *testing.T) {
		start, states, lowEvents := v27RecoveryGateFixture(1)
		states[6].state = "restricted"
		states[6].reason = "probe_timeout"
		transitionElapsed := states[6].observedAt.Sub(start)
		states[6].probeTimeoutTiming = v27ProbeTimeoutEvidence(start, transitionElapsed-time.Millisecond, transitionElapsed, 236*time.Millisecond, streamProbeBudget, 300)
		states = append(states,
			v27RecoveryGateObservation(start, transitionElapsed+5*time.Second, "recovering", "unique_probe", 1, 300),
			v27RecoveryGateObservation(start, transitionElapsed+6*time.Second, "normal", "unique_ack_recovered", 1, 301),
		)
		trace := v27ProbeTimeoutTraceEvents(start, states[5].observedAt, states[6].observedAt, 1, 1, 300)
		gate := v27BlackholeRecoveryStateGate(start, states, lowEvents, 0, true, 1, 1, 1, trace)
		if !gate.OK || gate.RecoveryRetryCount != 2 || len(gate.RecoveryRetryReasons) != 2 || gate.RecoveryRetryReasons[0] != "probe_unique_service_low" || gate.RecoveryRetryReasons[1] != "probe_timeout" {
			t.Fatalf("mixed bounded retries were not accepted with both causes: %+v", gate)
		}
	})
}

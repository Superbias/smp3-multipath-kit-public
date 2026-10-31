package smp3core

import (
	"testing"
	"time"
)

func TestPhase14RetiredFrontierCannotRestrictCurrentRecovery(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	oldAt := now.Add(-time.Second)
	old := c.txLedger.Add([]byte("old-frontier"), oldAt)
	if !c.txLedger.assignOrdinary(old, 1, oldAt) {
		t.Fatal("failed to assign old recovery frontier")
	}
	c.admission.legs[1] = streamAdmissionLeg{
		recovering: true, since: oldAt, reason: "unique_probe", proofAfter: old.Sequence(),
	}
	stale := c.txLedger.FrontierCandidate(now, streamProbeBudget)
	if !stale.overdue || stale.record != old {
		t.Fatalf("fixture did not capture an overdue old frontier: %+v", stale)
	}

	ack := c.txLedger.ApplyACK(old.Sequence()+1, now.Add(-time.Millisecond))
	if ack.Disposition != StreamTXACKAdvanced || ack.AckedNext != old.Sequence()+1 || c.txLedger.IsOutstanding(old) {
		t.Fatalf("fixture did not retire the captured frontier: %+v", ack)
	}
	freshAt := now.Add(-10 * time.Millisecond)
	fresh := c.txLedger.Add([]byte("fresh-frontier"), freshAt)
	if !c.txLedger.assignOrdinary(fresh, 1, freshAt) {
		t.Fatal("failed to assign fresh frontier")
	}
	current := c.txLedger.FrontierSnapshot()
	if !current.Exists || current.Record != fresh || now.Sub(current.ReferenceTime) >= streamProbeBudget {
		t.Fatalf("fixture did not replace the old frontier with a fresh one: %+v", current)
	}

	got := c.protectFrontier(stale, now)
	state := c.admission.legs[1]
	if state.restricted || !state.recovering || state.reason != "unique_probe" {
		t.Fatalf("stale candidate re-restricted a recovery after the ACK frontier advanced: state=%+v stale=%+v current=%+v", state, got, current)
	}
	if got.overdue {
		t.Fatalf("retired frontier remained overdue after revalidation: %+v", got)
	}
}

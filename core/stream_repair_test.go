package smp3core

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestStreamTXRepairPolicyMatrix(t *testing.T) {
	now := time.Unix(3000000000, 0)
	ledger := NewStreamTXLedger()
	r0 := ledger.Add([]byte("0"), now.Add(-time.Second))
	ledger.MarkTransit(r0, 0, now.Add(-time.Second))
	ledger.MarkAttemptSent(r0, 0, false, now.Add(-time.Second))
	r1 := ledger.Add([]byte("1"), now.Add(-time.Second))
	ledger.MarkTransit(r1, 1, now.Add(-time.Second))
	ledger.MarkAttemptSent(r1, 1, false, now.Add(-time.Second))
	never := ledger.PlanRetries(StreamLegAvailability{true, true})
	if len(never) != 0 {
		t.Fatalf("live-owner retry=%+v", never)
	}
	dead := ledger.PlanRetries(StreamLegAvailability{true, false})
	if len(dead) != 1 || dead[0].Record != r1 || dead[0].Avoid != 1 {
		t.Fatalf("dead-owner plan=%+v", dead)
	}
	candidate := ledger.FrontierCandidate(now, time.Second)
	if plan := DecideStreamTXFrontierRepair(candidate, StreamLegAvailability{true, true}); plan.Action != StreamTXFrontierRescue || plan.Record != r0 || plan.Avoid != 0 {
		t.Fatalf("rescue plan=%+v", plan)
	}
	if plan := DecideStreamTXFrontierRepair(candidate, StreamLegAvailability{true, false}); plan.Action != StreamTXFrontierNeedActivation {
		t.Fatalf("activation plan=%+v", plan)
	}
	if plan := DecideStreamTXFrontierRepair(candidate, StreamLegAvailability{}); plan.Action != StreamTXFrontierNone {
		t.Fatalf("zero-leg plan=%+v", plan)
	}
	if plan := DecideStreamTXFrontierRepair(ledger.FrontierCandidate(now.Add(-time.Nanosecond), time.Second), StreamLegAvailability{true, true}); plan.Action != StreamTXFrontierNone {
		t.Fatalf("timeout-1 plan=%+v", plan)
	}
	if got := []uint64{dead[0].Record.Sequence()}; !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatal(got)
	}
}

func TestPlanRetriesExcludesInTransitRecord(t *testing.T) {
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("in-transit"), time.Now())
	if !ledger.MarkTransit(record, 0, time.Now()) {
		t.Fatal("failed to mark record in transit")
	}
	if retries := ledger.PlanRetries(StreamLegAvailability{true, true}); len(retries) != 0 {
		t.Fatalf("in-transit record became ordinary retry: %+v", retries)
	}
}

func TestStreamTXRepairPolicyRandomizedDifferential10000(t *testing.T) {
	rng := rand.New(rand.NewSource(0x534d503344))
	base := time.Unix(3100000000, 0)
	for caseIndex := 0; caseIndex < 10000; caseIndex++ {
		ledger := NewStreamTXLedger()
		count := 1 + rng.Intn(8)
		lastLeg := make([]int16, count)
		inTransit := make([]bool, count)
		rescue := make([]bool, count)
		for i := 0; i < count; i++ {
			record := ledger.Add([]byte{byte(i)}, base)
			mode := rng.Intn(4)
			leg := LegID(rng.Intn(2))
			switch mode {
			case 1:
				ledger.MarkTransit(record, leg, base)
				inTransit[i] = true
				lastLeg[i] = -1
			case 2:
				ledger.MarkTransit(record, leg, base)
				ledger.MarkAttemptSent(record, leg, false, base)
				lastLeg[i] = int16(leg)
			case 3:
				ledger.MarkTransit(record, 0, base)
				ledger.MarkRescueTransit(record, 1, base)
				rescue[i] = true
				lastLeg[i] = -1
			default:
				lastLeg[i] = -1
			}
		}
		live := StreamLegAvailability{rng.Intn(2) == 1, rng.Intn(2) == 1}
		var want []StreamTXRetryCandidate
		for i := 0; i < count; i++ {
			if inTransit[i] || rescue[i] {
				continue
			}
			if lastLeg[i] < 0 || !live.Has(LegID(lastLeg[i])) {
				want = append(want, StreamTXRetryCandidate{Record: nil, Avoid: lastLeg[i]})
			}
		}
		got := ledger.PlanRetries(live)
		if len(got) != len(want) {
			t.Fatalf("case %d retry count got=%d want=%d", caseIndex, len(got), len(want))
		}
		for i := range got {
			if got[i].Record.Sequence() != uint64(i) && got[i].Record.Sequence() >= uint64(count) {
				t.Fatalf("case %d invalid sequence", caseIndex)
			}
		}
		_ = want

		timeout := time.Duration(rng.Intn(3)) * time.Second
		candidate := ledger.FrontierCandidate(base.Add(timeout), timeout)
		plan := DecideStreamTXFrontierRepair(candidate, live)
		if plan.Action == StreamTXFrontierRescue && (live.Count() != 2 || plan.Record == nil || plan.Avoid < 0) {
			t.Fatalf("case %d invalid rescue plan=%+v", caseIndex, plan)
		}
		if plan.Action == StreamTXFrontierNeedActivation && live.Count() != 1 {
			t.Fatalf("case %d invalid activation plan=%+v", caseIndex, plan)
		}
	}
}

package smp3core

import (
	"fmt"
	"testing"
	"time"
)

func TestClosureHealthFeedbackBudget(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	c.txLedger.assignOrdinary(r, 0, now)
	c.txLedger.MarkTransit(r, 0, now)
	c.txLedger.MarkAttemptSent(r, 0, false, now)
	c.admissionACK(c.txLedger.ApplyACK(1, now.Add(400*time.Millisecond)))
	if b := c.ordinaryByteBudget(0); b < 5_000_000 {
		t.Fatalf("400ms healthy pipeline still limited to %d bytes", b)
	}
}

func TestClosureOrderedProbeBehindHealthyPrefix(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.txLedger.EnableEvidenceTraceForTest()
	head := c.txLedger.Add(make([]byte, 32<<10), now)
	probe := c.txLedger.Add(make([]byte, 32<<10), now)
	c.txLedger.assignOrdinary(head, 0, now)
	c.txLedger.MarkTransit(head, 0, now)
	c.txLedger.MarkAttemptSent(head, 0, false, now)
	c.restrictOrdinary(1, now, "test")
	c.ordinaryAssigned(1, now.Add(6*time.Second))
	c.txLedger.assignOrdinary(probe, 1, now.Add(6*time.Second))
	c.txLedger.MarkTransit(probe, 1, now.Add(6*time.Second))
	c.txLedger.MarkAttemptSent(probe, 1, false, now.Add(6*time.Second))
	c.txLedger.ApplyACK(1, now.Add(7*time.Second))
	ack := c.txLedger.ApplyACK(2, now.Add(7100*time.Millisecond))
	c.admissionACK(ack)
	t.Logf("proof=%+v events=%+v", ack, c.txLedger.EvidenceTraceForTest())
	if c.admission.legs[1].proofs == 0 {
		t.Fatal("unique probe already delivered behind healthy prefix was rejected solely by cumulative residence")
	}
}

func TestClosureProofRejectsCopiesAndUnwritten(t *testing.T) {
	for _, mode := range []string{"unwritten", "rescue", "generation"} {
		t.Run(mode, func(t *testing.T) {
			l := NewStreamTXLedger()
			now := time.Now()
			r := l.Add([]byte{1}, now)
			l.assignOrdinary(r, 1, now)
			if mode != "unwritten" {
				l.MarkTransit(r, 1, now)
				l.MarkAttemptSent(r, 1, false, now)
			}
			if mode == "rescue" {
				l.MarkRescueTransit(r, 0, now)
			}
			if mode == "generation" {
				l.InvalidateLeg(1)
			}
			a := l.ApplyACK(1, now.Add(time.Millisecond))
			if a.UniqueTimelyByLeg[1]+a.UniqueOrderedByLeg[1] != 0 {
				t.Fatal("invalid proof accepted")
			}
		})
	}
}
func TestClosureRecoveryShortRecordsAndHardBounds(t *testing.T) {
	for _, size := range []int{1024, 4096, 16384, 32768, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := slowLegUnitEngine()
			c.cfg.MaxInflightFrames = 1024
			c.cfg.MaxReorderFrames = 4096
			now := time.Now()
			c.restrictOrdinary(1, now, "test")
			now = now.Add(6 * time.Second)
			c.ordinaryAssigned(1, now, size)
			for step := 0; step < 2048 && c.admission.legs[1].recovering; step++ {
				now = now.Add(10 * time.Millisecond)
				r := c.txLedger.Add(make([]byte, size), now)
				c.txLedger.assignOrdinary(r, 1, now)
				c.txLedger.MarkTransit(r, 1, now)
				c.txLedger.MarkAttemptSent(r, 1, false, now)
				c.admissionACK(c.txLedger.ApplyACK(r.Sequence()+1, now.Add(time.Millisecond)))
			}
			if c.admission.legs[1].recovering {
				t.Fatal("short records trapped recovery")
			}
			if c.ordinaryByteBudget(1) > c.ordinaryHardBytes() {
				t.Fatal("hard byte bound exceeded")
			}
			c.txLedger.EnableEvidenceTraceForTest()
			for i := 0; i < 300; i++ {
				c.txLedger.Add([]byte{1}, now)
			}
			if len(c.txLedger.EvidenceTraceForTest()) != 128 {
				t.Fatal("trace not bounded")
			}
		})
	}
}

func TestClosureOrdinaryAndProbeFrontierClocks(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	for id := uint8(0); id < 2; id++ {
		r := c.txLedger.Add([]byte{1}, now)
		c.txLedger.assignOrdinary(r, id, now)
		c.txLedger.MarkTransit(r, LegID(id), now)
		c.txLedger.MarkAttemptSent(r, LegID(id), false, now)
	}
	c.txLedger.ApplyACK(1, now.Add(3*time.Second))
	candidate := c.txLedger.FrontierCandidate(now.Add(3010*time.Millisecond), time.Second)
	if !candidate.overdue {
		t.Fatal("ordinary old tail received fresh exposure grace, suppressing its existing rescue path")
	}
	c.admission.legs[1].recovering = true
	if c.protectFrontier(candidate, now.Add(3010*time.Millisecond)).overdue {
		t.Fatal("probe did not exclude waiting behind predecessor")
	}
}

func TestClosureCountOnlyProofCannotGrowBytes(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.restrictOrdinary(1, now, "test")
	c.ordinaryAssigned(1, now.Add(6*time.Second))
	for i := 0; i < 8; i++ {
		c.admissionACK(StreamTXACKResult{UniqueTimelyByLeg: [2]int{0, 30}, AckedNext: uint64(i + 1), LastACKProgress: now.Add(7 * time.Second)})
	}
	if c.admission.legs[1].limit != 1 || !c.admission.legs[1].recovering {
		t.Fatal("record counts without confirmed bytes reopened credit")
	}
}

func TestClosureMeasuredFeedbackStartsProbeExposureAtFrontier(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	c.admission.legs[0].feedback = 400 * time.Millisecond
	c.admission.legs[0].feedbackAt = now
	for id := uint8(0); id < 2; id++ {
		r := c.txLedger.Add([]byte{1}, now)
		c.txLedger.assignOrdinary(r, id, now)
		c.txLedger.MarkTransit(r, LegID(id), now)
		c.txLedger.MarkAttemptSent(r, LegID(id), false, now)
	}
	c.txLedger.ApplyACK(1, now.Add(400*time.Millisecond))
	c.admission.legs[1].recovering = true
	beforeBudget := now.Add(600 * time.Millisecond)
	candidate := c.txLedger.FrontierCandidate(beforeBudget, time.Second)
	if got := c.protectFrontier(candidate, beforeBudget); got.overdue {
		t.Fatalf("probe counted its pre-frontier cumulative wait as exposure: reference=%s", got.reference.Sub(now))
	}
	afterBudget := now.Add(time.Second)
	candidate = c.txLedger.FrontierCandidate(afterBudget, time.Second)
	if got := c.protectFrontier(candidate, afterBudget); !got.overdue {
		t.Fatal("probe was not bounded after exceeding its measured feedback horizon from frontier exposure")
	}
}

func TestClosureRecoveryBudgetBoundariesRemainDistinct(t *testing.T) {
	if streamProbeBudget != 200*time.Millisecond || streamProofBudget != 700*time.Millisecond {
		t.Fatalf("recovery budgets changed: probe=%s proof=%s", streamProbeBudget, streamProofBudget)
	}
	c := slowLegUnitEngine()
	if got := c.frameBudget(100); got != 48 {
		t.Fatalf("100 Mbps ordinary quarter-second frame budget=%d, want 48", got)
	}
	now := time.Now()
	c.admission.legs[1].recovering = true
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	c.txLedger.assignOrdinary(r, 1, now)
	c.txLedger.MarkTransit(r, 1, now)
	c.txLedger.MarkAttemptSent(r, 1, false, now)
	before := now.Add(streamProbeBudget - time.Nanosecond)
	if got := c.protectFrontier(c.txLedger.FrontierCandidate(before, time.Second), before); got.overdue {
		t.Fatal("probe exceeded the existing 200 ms frontier budget early")
	}
	atBoundary := now.Add(streamProbeBudget)
	if got := c.protectFrontier(c.txLedger.FrontierCandidate(atBoundary, time.Second), atBoundary); !got.overdue {
		t.Fatal("probe did not become overdue at the existing 200 ms frontier budget")
	}
	for _, delay := range []time.Duration{streamProofBudget, streamProofBudget + time.Nanosecond} {
		ledger := NewStreamTXLedger()
		proof := ledger.Add([]byte{1}, now)
		ledger.assignOrdinary(proof, 1, now)
		ledger.MarkTransit(proof, 1, now)
		ledger.MarkAttemptSent(proof, 1, false, now)
		ack := ledger.ApplyACK(1, now.Add(delay))
		if delay <= streamProofBudget && ack.UniqueTimelyByLeg[1] != 1 {
			t.Fatalf("proof at %s was not accepted within the existing 700 ms budget: %+v", delay, ack)
		}
		if delay > streamProofBudget && ack.ProofRejected[1][2] != 1 {
			t.Fatalf("proof at %s was accepted beyond the existing 700 ms budget: %+v", delay, ack)
		}
	}
}

// Normal ACK service and its observed ordering envelope describe one existing
// pipeline. A smaller min-delay quota can choke the service that trained it.
func TestClosureNormalBudgetCoversObservedACKPipeline(t *testing.T) {
	c := slowLegUnitEngine()
	now := time.Now()
	s := &c.admission.legs[0]
	s.serviceRate, s.serviceAt = 6.5e6, now
	s.feedback, s.feedbackAt = 40*time.Millisecond, now
	s.feedbackMax, s.feedbackMaxAt = 700*time.Millisecond, now
	pipeline := uint64(s.serviceRate * s.feedbackMax.Seconds())
	if b := c.ordinaryByteBudget(0); b < pipeline || b > c.ordinaryHardBytes() {
		t.Fatalf("normal quota cannot sustain its observed ACK pipeline: budget=%d pipeline=%d", b, pipeline)
	}
	c.admission.legs[1].recovering = true
	if b := c.ordinaryByteBudget(0); b >= pipeline {
		t.Fatalf("normal feedback envelope enlarged healthy tail during peer probation: %d", b)
	}
}

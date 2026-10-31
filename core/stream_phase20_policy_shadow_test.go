package smp3core

import (
	"testing"
	"time"
)

func phase20PolicyShadowCandidate(t *testing.T) (*StreamEngine, StreamTXFrontierCandidate, time.Time) {
	t.Helper()
	c := slowLegUnitEngine()
	c.cfg.BandwidthMbps = []uint32{100, 1}
	c.cfg.RetransmitTimeout = 1500 * time.Millisecond
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 64<<10), now.Add(-2*time.Second))
	if !c.txLedger.assignOrdinary(r, 1, now.Add(-2*time.Second)) {
		t.Fatal("assign slow-leg frontier")
	}
	c.txLedger.MarkTransit(r, 1, now.Add(-2*time.Second))
	c.legs[1].writeStartedNs.Store(now.Add(-100 * time.Millisecond).UnixNano())
	c.legs[0].perf.writeBPS = 100e6 / 8
	c.legs[1].perf.writeBPS = 1e6 / 8
	return c, StreamTXFrontierCandidate{record: r, owner: 1, reference: now.Add(-2 * time.Second), overdue: true, exists: true}, now
}

func TestPhase20PolicyAShadowEvaluationPositiveControl(t *testing.T) {
	c, candidate, now := phase20PolicyShadowCandidate(t)
	event := c.phase20PolicyAShadowEvaluation(candidate, now)
	if !event.Applied || event.Action != "eligible_active_slow_leg_hol" {
		t.Fatalf("Policy A shadow positive control: applied=%t action=%q", event.Applied, event.Action)
	}
	if event.RecordSequence != candidate.record.Sequence() || event.PayloadBytes != len(candidate.record.Payload()) ||
		!event.InTransit || event.TransitLeg != 1 || event.Decision == nil || !event.Decision.CarrierWriteInProgress ||
		event.Decision.OwnEffectiveWeight*4 >= event.Decision.AlternateEffectiveWeight {
		t.Fatalf("shadow event omitted decision identity/input: %+v", event)
	}
}

func TestPhase20PolicyAShadowEvaluationNegativeControls(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*StreamEngine, time.Time)
		want   string
	}{
		{name: "write absent", mutate: func(c *StreamEngine, _ time.Time) { c.legs[1].writeStartedNs.Store(0) }, want: "no_active_owner_write"},
		{name: "recovering", mutate: func(c *StreamEngine, _ time.Time) { c.admission.legs[1] = streamAdmissionLeg{recovering: true} }, want: "recovery_in_progress"},
		{name: "pending probe", mutate: func(c *StreamEngine, _ time.Time) {
			c.admission.legs[1] = streamAdmissionLeg{restricted: true, probePending: true, reason: "probe_pending_write"}
		}, want: "probe_pending"},
		{name: "other restriction", mutate: func(c *StreamEngine, _ time.Time) {
			c.admission.legs[1] = streamAdmissionLeg{restricted: true, reason: "probe_timeout"}
		}, want: "non_hol_restriction"},
		{name: "balanced effective weights", mutate: func(c *StreamEngine, _ time.Time) { c.cfg.BandwidthMbps = []uint32{100, 100} }, want: "effective_weight_gap_insufficient"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, candidate, now := phase20PolicyShadowCandidate(t)
			tc.mutate(c, now)
			event := c.phase20PolicyAShadowEvaluation(candidate, now)
			if event.Applied || event.Action != tc.want {
				t.Fatalf("shadow negative control: applied=%t action=%q, want reason=%q", event.Applied, event.Action, tc.want)
			}
		})
	}
}

package smp3core

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestReleaseRetiredGenerationCannotProveRetainedRecovery(t *testing.T) {
	c := releaseIdleEngine(t)
	now := time.Now()
	c.restrictOrdinary(1, now, "test")
	probe := startRecoveryProbeForTest(t, c, 1, now, 1)
	for i := 0; i < 2; i++ {
		r := c.txLedger.Add([]byte{1}, now)
		c.txLedger.assignOrdinary(r, 1, now)
		c.txLedger.MarkTransit(r, 1, now)
		c.txLedger.MarkAttemptSent(r, 1, false, now)
	}
	old := c.txLedger.ApplyACK(probe.Sequence()+3, time.Now())
	if old.UniqueTimelyByLeg[1] != 3 {
		t.Fatal("fixture")
	}
	if !c.ReplaceLeg(1, io.ErrClosedPipe) {
		t.Fatal("replace")
	}
	a, b := net.Pipe()
	defer b.Close()
	if err := c.AttachLeg(1, a, nil); err != nil {
		t.Fatal(err)
	}
	c.admissionACK(old)
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	s := c.admission.legs[1]
	if s.limit != 1 || s.proofs != 0 {
		t.Fatalf("retired generation advanced retained probation: limit=%d proofs=%d", s.limit, s.proofs)
	}
}

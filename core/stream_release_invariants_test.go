package smp3core

import (
	"net"
	"testing"
	"time"
)

func releaseIdleEngine(t *testing.T) *StreamEngine {
	t.Helper()
	c, _ := NewStreamEngine(StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128})
	for id := 0; id < 2; id++ {
		a, b := net.Pipe()
		t.Cleanup(func() { b.Close() })
		if err := c.AttachLeg(LegID(id), a, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestReleaseStaleACKResultCannotProveNewRecovery(t *testing.T) {
	c := releaseIdleEngine(t)
	now := time.Now()
	for i := 0; i < 3; i++ {
		r := c.txLedger.Add([]byte{1}, now)
		c.txLedger.assignOrdinary(r, 1, now)
		c.txLedger.MarkTransit(r, 1, now)
		c.txLedger.MarkAttemptSent(r, 1, false, now)
	}
	old := c.txLedger.ApplyACK(3, now.Add(10*time.Millisecond))
	if old.UniqueTimelyByLeg[1] != 3 {
		t.Fatal("fixture lacked old clean proofs")
	}
	// ACK has retired old records, then the other leg's ACK callback is delayed
	// across failure/rejoin and a later probation epoch on this logical leg.
	c.txLedger.InvalidateLeg(1)
	c.legGeneration[1].Add(1)
	c.restrictOrdinary(1, now.Add(20*time.Millisecond), "new_generation")
	c.ordinaryAssigned(1, now.Add(time.Second))
	c.admissionACK(old)
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	s := c.admission.legs[1]
	if s.limit != 1 || s.proofs != 0 {
		t.Fatalf("old ACK result advanced new recovery: limit=%d proofs=%d", s.limit, s.proofs)
	}
}
func TestReleaseSamePendingOwnerDoesNotRollback(t *testing.T) {
	c := releaseIdleEngine(t)
	now := time.Now()
	r := c.txLedger.Add(make([]byte, 64<<10), now)
	a := &benchmarkAssignment{}
	owner := c.assignBenchmark(r, a)
	if owner == nil || owner.id != 0 {
		t.Fatal("fixture initial owner")
	}
	// Saturate the alternate, leaving only an expired-cooldown reselect of the
	// same still-unadmitted owner. This is a probe, not an owner migration.
	for i := 0; i < 64; i++ {
		other := c.txLedger.Add(make([]byte, 64<<10), now)
		c.txLedger.assignOrdinary(other, 1, now)
	}
	c.restrictOrdinary(0, now, "test")
	c.admission.mu.Lock()
	c.admission.legs[0].nextProbe = now.Add(-time.Second)
	c.admission.mu.Unlock()
	before, _ := c.assignedSnapshot()
	rollback := c.decoupledStats.rollback.Load()
	got := c.assignBenchmark(r, a)
	if got != owner {
		t.Fatal("fixture did not reselect same owner")
	}
	after, _ := c.assignedSnapshot()
	if before != after {
		t.Fatal("virtual service changed")
	}
	if c.decoupledStats.rollback.Load() != rollback {
		t.Fatal("same-owner probe was charged a migration rollback")
	}
}

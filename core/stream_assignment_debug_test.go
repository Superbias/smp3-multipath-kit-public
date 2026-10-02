package smp3core

import (
	"testing"
)

func TestBenchmarkPendingAssignmentRollback(t *testing.T) {
	c := &StreamEngine{
		cfg:  StreamConfig{SchedulerMode: StreamSchedulerStatic, ChunkSize: 32 * 1024, BandwidthMbps: []uint32{50, 200}},
		legs: make(map[uint8]*streamLeg),
	}
	c.legs[0] = &streamLeg{id: 0, send: make(chan txSendAttempt, 1), done: make(chan struct{})}
	c.legs[1] = &streamLeg{id: 1, send: make(chan txSendAttempt, 1), done: make(chan struct{})}
	r := &StreamTXRecord{payload: make([]byte, 32*1024)}
	a := &benchmarkAssignment{}
	if got := c.assignBenchmark(r, a); got == nil {
		t.Fatal("initial assignment returned nil")
	}
	if a.owner.id != 1 {
		t.Fatalf("initial owner=%d, want high leg", a.owner.id)
	}
	c.assignedService[1] = uint64(len(r.payload))
	c.assignedRecords[1] = 1
	c.legs[1].closed.Store(true)
	if got := c.assignBenchmark(r, a); got == nil || got.id != 0 {
		t.Fatalf("reassignment owner=%v, want low leg", got)
	}
	if c.assignedService[1] != 0 || c.assignedService[0] != uint64(len(r.payload)) {
		t.Fatalf("assigned bytes=%v, want rollback then one reassignment", c.assignedService)
	}
	if got := c.decoupledStats.rollback.Load(); got != uint64(len(r.payload)) {
		t.Fatalf("rollback=%d, want %d", got, len(r.payload))
	}
	if got := c.decoupledStats.reassigned.Load(); got != uint64(len(r.payload)) {
		t.Fatalf("reassigned=%d, want %d", got, len(r.payload))
	}
}

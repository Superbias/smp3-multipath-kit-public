package smp3core

import (
	"net"
	"testing"
)

func TestBenchmarkPendingAssignmentRollback(t *testing.T) {
	c, app := NewStreamEngine(StreamConfig{SchedulerMode: StreamSchedulerStatic, MaxActiveLegs: 2, ChunkSize: 32 * 1024, BandwidthMbps: []uint32{50, 200}})
	t.Cleanup(func() { _ = app.Close(); _ = c.Close() })
	peers := make([]net.Conn, 0, 2)
	t.Cleanup(func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	for id := uint8(0); id < 2; id++ {
		carrier, peer := net.Pipe()
		peers = append(peers, peer)
		if err := c.AttachLeg(LegID(id), carrier, nil); err != nil {
			t.Fatalf("attach leg %d: %v", id, err)
		}
	}
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

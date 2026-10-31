package smp3core

import (
	"io"
	"testing"
	"time"
)

func TestSlowLegGracefulDrainReliable(t *testing.T) {
	cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128, MaxReorderFrames: 256, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second, RecoveryTimeout: 5 * time.Second}
	sender, app := NewStreamEngine(cfg)
	receiver, out := NewStreamEngine(cfg)
	defer sender.Close()
	defer receiver.Close()
	for id, rate := range []int64{100e6, 1e6} {
		a, b := newSlowTestCarrier(rate)
		if err := sender.AttachLeg(LegID(id), a, nil); err != nil {
			t.Fatal(err)
		}
		if err := receiver.AttachLeg(LegID(id), b, nil); err != nil {
			t.Fatal(err)
		}
	}
	const size = 16 << 20
	sink := &slowTestSink{}
	readDone, writeDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := io.CopyN(sink, out, size); readDone <- err }()
	go func() {
		source := &slowTestSource{}
		buffer := make([]byte, 64<<10)
		for sent := 0; sent < size; sent += len(buffer) {
			source.Read(buffer)
			if err := writeAll(app, buffer); err != nil {
				writeDone <- err
				return
			}
		}
		app.Close()
		writeDone <- nil
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("finite sender stalled")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("finite payload did not drain")
	}
	select {
	case <-sender.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("graceful EOF stalled")
	}
	if sink.bytes.Load() != size {
		t.Fatal("payload length mismatch")
	}
	stats := sender.Snapshot()
	if stats.OutstandingFrames != 0 || stats.OrdinaryOutstandingFrames != [2]int{} || len(sender.inflight) != 0 {
		t.Fatalf("logical retirement failed: %+v", stats)
	}
	t.Logf("payload=%d ledger=0 inflight=0 pending=%v successful_writes=%v retries=%v", sink.bytes.Load(), stats.CapacityPending, stats.TxWrittenBytesByLeg, stats.TxRetryWrittenBytesByLeg)
}

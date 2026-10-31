package smp3core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"testing"
	"time"
)

func TestStreamDrainLegKeepsSessionAndDeliversInFlightData(t *testing.T) {
	cfg := testStreamConfig()
	cfg.ChunkSize = 16 * 1024
	cfg.MaxInflightFrames = 128
	cfg.MaxReorderFrames = 256
	cfg.PathRole = StreamPathRoleClient
	left, leftApp := NewStreamEngine(cfg)
	cfg.PathRole = StreamPathRoleServer
	right, rightApp := NewStreamEngine(cfg)
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()
	phase36AttachDrainLegPair(t, left, right, 0)
	phase36AttachDrainLegPair(t, left, right, 1)

	payload := make([]byte, 2*1024*1024)
	for i := range payload {
		payload[i] = byte((i*37 + i/257) % 251)
	}
	writeDone := make(chan error, 1)
	readDone := make(chan error, 1)
	got := make([]byte, len(payload))
	go func() {
		_, err := io.Copy(leftApp, bytes.NewReader(payload))
		writeDone <- err
	}()
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()

	// Begin removal while application data is flowing. The drain must stop new
	// ownership on Leg 1, wait for its outstanding DATA, then keep the same Core
	// Session and the surviving carrier alive.
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := left.DrainLeg(ctx, 1); err != nil {
		t.Fatalf("DrainLeg: %v", err)
	}
	for name, ch := range map[string]<-chan error{"application write": writeDone, "application read": readDone} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not finish after graceful leg drain", name)
		}
	}
	if left.LegCount() != 1 || !left.HasLeg(0) || left.HasLeg(1) {
		t.Fatalf("unexpected Core registry after drain: count=%d leg0=%t leg1=%t", left.LegCount(), left.HasLeg(0), left.HasLeg(1))
	}
	if err := left.WaitLegRetired(ctx, 1); err != nil {
		t.Fatalf("wait for drained generation retirement: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(payload) {
		t.Fatal("application payload SHA-256 changed during graceful leg drain")
	}
	if got := left.TXLedger().Snapshot(time.Now()).OutstandingFrames; got != 0 {
		t.Fatalf("TX outstanding after drain=%d, want 0", got)
	}
}

func TestStreamDrainLegRejectsRemovingLastCarrier(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	local, peer := net.Pipe()
	defer peer.Close()
	if err := engine.AttachLeg(0, local, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.DrainLeg(context.Background(), 0); err != ErrStreamLastLeg {
		t.Fatalf("DrainLeg(last leg)=%v, want %v", err, ErrStreamLastLeg)
	}
	if !engine.HasLeg(0) || engine.LegCount() != 1 {
		t.Fatal("last leg was removed after the operation was rejected")
	}
}

func TestStreamDrainLegTimeoutRestoresSchedulingEligibility(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	peers := []net.Conn{
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 0),
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 1),
	}
	defer peers[0].Close()
	defer peers[1].Close()
	// Hold one outstanding record to simulate an ACK that does not arrive
	// before the caller's drain deadline.
	record := engine.txLedger.Add([]byte("pending"), time.Now())
	engine.txLedger.mu.Lock()
	record.ordinaryOwner = 1
	engine.txLedger.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := engine.DrainLeg(ctx, 1); err != context.DeadlineExceeded {
		t.Fatalf("DrainLeg timeout=%v, want deadline exceeded", err)
	}
	leg := engine.getLeg(1)
	if leg == nil || leg.draining.Load() {
		t.Fatal("timed-out drain did not restore the still-live leg to schedulable state")
	}
}

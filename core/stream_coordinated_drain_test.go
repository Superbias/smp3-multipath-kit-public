package smp3core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestStreamV7CoordinatedDrainRetiresBothEndpoints(t *testing.T) {
	clientCfg := testStreamConfig()
	clientCfg.PathRole = StreamPathRoleClient
	clientCfg.MaxActiveLegs = 2
	landingCfg := clientCfg
	landingCfg.PathRole = StreamPathRoleServer
	client, clientApp := NewStreamEngine(clientCfg)
	landing, landingApp := NewStreamEngine(landingCfg)
	defer client.Close()
	defer landing.Close()
	defer clientApp.Close()
	defer landingApp.Close()
	phase36AttachDrainLegPair(t, client, landing, 0)
	phase36AttachDrainLegPair(t, client, landing, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.DrainLeg(ctx, 1); err != nil {
		t.Fatalf("coordinated DrainLeg: %v", err)
	}
	retireDeadline := time.Now().Add(time.Second)
	for time.Now().Before(retireDeadline) && (client.HasLeg(1) || landing.HasLeg(1)) {
		time.Sleep(time.Millisecond)
	}
	if client.LegCount() != 1 || landing.LegCount() != 1 || client.HasLeg(1) || landing.HasLeg(1) {
		t.Fatalf("both endpoints did not retire Leg 1: client=%d/%t landing=%d/%t", client.LegCount(), client.HasLeg(1), landing.LegCount(), landing.HasLeg(1))
	}
	if err := client.WaitLegRetired(ctx, 1); err != nil {
		t.Fatalf("client worker retirement: %v", err)
	}
	if err := landing.WaitLegRetired(ctx, 1); err != nil {
		t.Fatalf("Landing worker retirement: %v", err)
	}
	for name, engine := range map[string]*StreamEngine{"client": client, "landing": landing} {
		if got := engine.txLedger.Snapshot(time.Now()).OutstandingFrames; got != 0 {
			t.Errorf("%s TX outstanding after empty coordinated drain=%d", name, got)
		}
		stats := engine.Snapshot()
		if stats.RxPendingFrames != 0 || stats.RxPendingBytes != 0 {
			t.Errorf("%s RX pending after empty coordinated drain=%d/%d", name, stats.RxPendingFrames, stats.RxPendingBytes)
		}
	}
}

func TestStreamDrainRefusesLegacyPeerWithoutClosingCarrier(t *testing.T) {
	left, app := NewStreamEngine(testStreamConfig())
	defer left.Close()
	defer app.Close()
	pairs := []engineLegPair{newEngineLegPair(), newEngineLegPair()}
	defer pairs[0].right.Close()
	defer pairs[1].right.Close()
	for i := range pairs {
		if err := left.AttachLeg(LegID(i), pairs[i].left, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := left.DrainLeg(ctx, 1); !errors.Is(err, ErrStreamDrainUnsupported) {
		t.Fatalf("legacy peer DrainLeg error=%v, want unsupported", err)
	}
	if !left.HasLeg(0) || !left.HasLeg(1) || left.LegCount() != 2 {
		t.Fatal("unsupported planned drain closed or removed a legacy Carrier")
	}
}

func TestPhase36ApplicationFullDuplexDrainSettlesReverseOutstandingDATA(t *testing.T) {
	clientCfg := testStreamConfig()
	clientCfg.ChunkSize = 32 * 1024
	clientCfg.MaxInflightFrames = 8
	clientCfg.RetransmitTimeout = 5 * time.Second
	clientCfg.SchedulerMode = StreamSchedulerAggregation
	clientCfg.CapacityMode = StreamCapacityDynamic
	clientCfg.BandwidthMbps = []uint32{100, 100}
	clientCfg.BenchmarkFixedRatio = true
	clientCfg.PathRole = StreamPathRoleClient
	landingCfg := clientCfg
	landingCfg.PathRole = StreamPathRoleServer
	client, clientApp := NewStreamEngine(clientCfg)
	landing, landingApp := NewStreamEngine(landingCfg)
	defer client.Close()
	defer landing.Close()
	defer clientApp.Close()
	defer landingApp.Close()
	phase36AttachDrainLegPair(t, client, landing, 0)
	phase36AttachDrainLegPair(t, client, landing, 1)

	forward := bytes.Repeat([]byte("phase36-client-to-landing-"), 500)
	reverse := bytes.Repeat([]byte("phase36-landing-to-client-"), 1350)
	writeResults := make(chan error, 2)
	go func() {
		n, err := clientApp.Write(forward)
		if err == nil && n != len(forward) {
			err = io.ErrShortWrite
		}
		writeResults <- err
	}()
	go func() {
		n, err := landingApp.Write(reverse)
		if err == nil && n != len(reverse) {
			err = io.ErrShortWrite
		}
		writeResults <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-writeResults:
			if err != nil {
				t.Fatalf("full-duplex application write: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("full-duplex application writes did not finish")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for landing.txLedger.outstandingForLeg(1) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := landing.txLedger.outstandingForLeg(1); got == 0 {
		t.Fatal("reproduction did not place reverse DATA on target Leg 1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drainResult := make(chan error, 1)
	go func() { drainResult <- client.DrainLeg(ctx, 1) }()
	for time.Now().Before(deadline.Add(2 * time.Second)) {
		clientLeg, landingLeg := client.getLeg(1), landing.getLeg(1)
		if clientLeg != nil && landingLeg != nil && clientLeg.draining.Load() && landingLeg.draining.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !client.getLeg(1).draining.Load() || !landing.getLeg(1).draining.Load() {
		t.Fatal("both endpoints did not establish the DATA allocation barrier")
	}

	gotForward, gotReverse := make([]byte, len(forward)), make([]byte, len(reverse))
	readResults := make(chan error, 2)
	go func() { _, err := io.ReadFull(landingApp, gotForward); readResults <- err }()
	go func() { _, err := io.ReadFull(clientApp, gotReverse); readResults <- err }()
	for i := 0; i < 2; i++ {
		select {
		case err := <-readResults:
			if err != nil {
				t.Fatalf("full-duplex application read: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("full-duplex application delivery did not finish: client={%s} landing={%s}",
				phase36DrainDebugSnapshot(client, 1), phase36DrainDebugSnapshot(landing, 1))
		}
	}
	select {
	case err := <-drainResult:
		if err != nil {
			t.Fatalf("coordinated full-duplex drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("coordinated drain did not finish after both directions were read and ACKed")
	}
	if sha256.Sum256(gotForward) != sha256.Sum256(forward) || sha256.Sum256(gotReverse) != sha256.Sum256(reverse) {
		t.Fatal("full-duplex payload SHA-256 changed during coordinated drain")
	}
	settleDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(settleDeadline) {
		settled := true
		for _, engine := range []*StreamEngine{client, landing} {
			if engine.txLedger.Snapshot(time.Now()).OutstandingFrames != 0 {
				settled = false
			}
			stats := engine.Snapshot()
			if stats.RxPendingFrames != 0 || stats.RxPendingBytes != 0 {
				settled = false
			}
		}
		if settled {
			break
		}
		time.Sleep(time.Millisecond)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (client.HasLeg(1) || landing.HasLeg(1)) {
		time.Sleep(time.Millisecond)
	}
	if client.HasLeg(1) || landing.HasLeg(1) || client.LegCount() != 1 || landing.LegCount() != 1 {
		t.Fatalf("both endpoints did not retire the same carrier: client=%d/%t landing=%d/%t", client.LegCount(), client.HasLeg(1), landing.LegCount(), landing.HasLeg(1))
	}
	if client.rxDeliveredBytes.Load() != uint64(len(reverse)) || landing.rxDeliveredBytes.Load() != uint64(len(forward)) {
		t.Fatalf("application delivery bytes differ: client=%d/%d Landing=%d/%d", client.rxDeliveredBytes.Load(), len(reverse), landing.rxDeliveredBytes.Load(), len(forward))
	}
	for name, engine := range map[string]*StreamEngine{"client": client, "landing": landing} {
		tx := engine.txLedger.Snapshot(time.Now())
		stats := engine.Snapshot()
		if tx.OutstandingFrames != 0 || tx.AckedNext != engine.txLedger.NextSequence() {
			t.Errorf("%s TX ledger not settled: outstanding=%d acked=%d next=%d", name, tx.OutstandingFrames, tx.AckedNext, engine.txLedger.NextSequence())
		}
		if stats.RxPendingFrames != 0 || stats.RxPendingBytes != 0 {
			t.Errorf("%s RX pending not cleared: %d/%d", name, stats.RxPendingFrames, stats.RxPendingBytes)
		}
		var unique uint64
		for i := range engine.rxUniqueBytes {
			unique += engine.rxUniqueBytes[i].Load()
		}
		want := uint64(len(forward))
		if name == "client" {
			want = uint64(len(reverse))
		}
		if unique != want {
			t.Errorf("%s unique RX bytes=%d, want exactly %d", name, unique, want)
		}
	}
}

func phase36DrainDebugSnapshot(engine *StreamEngine, legID uint8) string {
	tx := engine.txLedger.Snapshot(time.Now())
	leg := engine.getLeg(legID)
	legState := "absent"
	if leg != nil {
		legState = fmt.Sprintf("generation=%d closed=%t draining=%t send=%d rescue=%d writer=%d",
			leg.generation, leg.closed.Load(), leg.draining.Load(), len(leg.send), len(leg.rescue), leg.debugWriterState.Load())
	}
	engine.drainMu.Lock()
	op := engine.drainOps[legID]
	opState := "absent"
	if op != nil {
		opState = fmt.Sprintf("peer_quiesced=%t local_ready=%t ready_sent=%t peer_ready=%t commit_sent=%t commit_requested=%t commit_ack=%t peer_generation=%d peer_outstanding=%d/%d terminal=%v",
			op.peerQuiesced, op.localReady, op.readySent, op.peerReady, op.commitSent, op.commitRequested,
			op.commitAck, op.peerGeneration, op.peerOutstandingFrames, op.peerOutstandingBytes, op.terminalErr)
	}
	engine.drainMu.Unlock()
	return fmt.Sprintf("tx=%d/%d acked_next=%d rx_pending=%d/%d leg1={%s} drain={%s}",
		tx.OutstandingFrames, tx.OutstandingBytes, tx.AckedNext, engine.rxPendingFrames.Load(),
		engine.rxPendingBytes.Load(), legState, opState)
}

func TestStreamDrainTimeoutRestoresSchedulingEligibility(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	peers := []net.Conn{
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 0),
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 1),
	}
	defer peers[0].Close()
	defer peers[1].Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := engine.DrainLeg(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DrainLeg timeout=%v, want deadline exceeded", err)
	}
	leg := engine.getLeg(1)
	if leg == nil || leg.draining.Load() {
		t.Fatal("timed-out coordinated drain did not restore the live leg")
	}
}

func TestStreamDrainAbortRestoresPeerBarrierBeforeLocalResume(t *testing.T) {
	clientCfg := testStreamConfig()
	clientCfg.PathRole = StreamPathRoleClient
	clientCfg.MaxActiveLegs = 2
	landingCfg := clientCfg
	landingCfg.PathRole = StreamPathRoleServer
	client, clientApp := NewStreamEngine(clientCfg)
	landing, landingApp := NewStreamEngine(landingCfg)
	defer client.Close()
	defer landing.Close()
	defer clientApp.Close()
	defer landingApp.Close()
	phase36AttachDrainLegPair(t, client, landing, 0)
	phase36AttachDrainLegPair(t, client, landing, 1)

	target := client.getLeg(1)
	op := &streamDrainOperation{
		target: target, targetBinding: target.drainBinding, operationID: [16]byte{0x36},
		counter: 1, originRole: StreamPathRoleClient, initiator: true,
		changed: make(chan struct{}, 1), lastControlSent: time.Now(),
	}
	client.drainMu.Lock()
	target.dispatchMu.Lock()
	target.draining.Store(true)
	target.dispatchMu.Unlock()
	client.drainOps[target.id] = op
	client.drainOperationCounter.Store(1)
	client.drainMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.sendDrainMessage(ctx, op, StreamDrainIntent); err != nil {
		t.Fatalf("send initial drain intent: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		peerLeg := landing.getLeg(1)
		if peerLeg != nil && peerLeg.draining.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	peerLeg := landing.getLeg(1)
	if peerLeg == nil || !peerLeg.draining.Load() {
		t.Fatal("peer did not establish its quiesce barrier after Intent")
	}

	client.abortDrain(op, context.Canceled, true)
	if target.draining.Load() {
		t.Fatal("initiator did not restore its target barrier after Abort")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && peerLeg.draining.Load() {
		time.Sleep(time.Millisecond)
	}
	if peerLeg.draining.Load() {
		t.Fatal("peer kept its target barrier after receiving authenticated Abort")
	}
	client.drainMu.Lock()
	clientOpActive := client.drainOps[target.id] != nil
	client.drainMu.Unlock()
	landing.drainMu.Lock()
	landingOpActive := landing.drainOps[peerLeg.id] != nil
	landing.drainMu.Unlock()
	if clientOpActive || landingOpActive {
		t.Fatal("aborted coordinated drain left an operation registered")
	}
}

func TestStreamDrainConcurrentRemovalsCannotRetireLastCarrier(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	peers := []net.Conn{
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 0),
		phase36AttachDrainLegToUnresponsivePeer(t, engine, 1),
	}
	defer peers[0].Close()
	defer peers[1].Close()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	results := make(chan error, 2)
	for id := LegID(0); id < 2; id++ {
		go func(id LegID) { results <- engine.DrainLeg(ctx, id) }(id)
	}
	var allRejected = true
	var errorsSeen []error
	for i := 0; i < 2; i++ {
		err := <-results
		errorsSeen = append(errorsSeen, err)
		allRejected = allRejected && err != nil
	}
	if !allRejected {
		t.Fatalf("concurrent planned removals unexpectedly succeeded: %v", errorsSeen)
	}
	if engine.LegCount() < 1 {
		t.Fatal("concurrent planned removals retired every Carrier")
	}
}

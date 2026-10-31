package smp3core

import (
	"io"
	"net"
	"testing"
)

func TestStreamV7CommitAckWaitsForPeerClosedLegRetirement(t *testing.T) {
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
	if target == nil {
		t.Fatal("missing target leg")
	}
	op := &streamDrainOperation{
		target: target, initiator: true, commitSent: true, commitAck: true,
		changed: make(chan struct{}, 1),
	}
	client.drainMu.Lock()
	client.drainOps[target.id] = op
	client.drainMu.Unlock()
	client.cfg.drainBeforeRetireHook = func(leg *streamLeg) {
		if leg != target {
			t.Errorf("retirement hook received another generation")
			return
		}
		// Model the peer closing immediately after its authenticated CommitAck,
		// before the read-loop failure handler removes this generation from legs.
		leg.closed.Store(true)
	}

	done, err := client.completeAcknowledgedDrain(op)
	if err != nil || done {
		t.Fatalf("drain completed before closed target left the active registry: done=%t err=%v", done, err)
	}
	if client.getLeg(target.id) != target || op.finished {
		t.Fatal("drain did not wait for same-generation carrier cleanup")
	}

	// The normal Carrier failure path moves the closed generation to retiring.
	client.cfg.drainBeforeRetireHook = nil
	client.handleLegFailure(target, io.EOF)
	done, err = client.completeAcknowledgedDrain(op)
	if err != nil || !done {
		t.Fatalf("authenticated CommitAck did not complete after carrier cleanup: done=%t err=%v", done, err)
	}
	if client.HasLeg(1) || client.txLedger.outstandingForLeg(target.id) != 0 {
		t.Fatal("retired generation remained active or owned TX ledger entries")
	}
}

func TestStreamV7CommitAckAcceptedAfterTargetLeavesActiveRegistry(t *testing.T) {
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
	_, landingControlAuth := phase36AttachDrainLegPair(t, client, landing, 0)
	phase36AttachDrainLegPair(t, client, landing, 1)
	target := client.getLeg(1)
	survivor := client.getLeg(0)
	if target == nil || survivor == nil {
		t.Fatal("missing target or surviving leg")
	}
	opID := [16]byte{1, 2, 3, 4}
	op := &streamDrainOperation{
		target: target, operationID: opID, targetBinding: target.drainBinding, counter: 1,
		peerGeneration: target.generation, initiator: true, commitSent: true,
		changed: make(chan struct{}, 1),
	}
	client.drainMu.Lock()
	client.drainOps[target.id] = op
	client.drainMu.Unlock()

	// The peer may close its target after writing CommitAck on the survivor.
	// The local failure handler can retire this generation before the ACK frame
	// is read from the surviving TCP carrier.
	client.handleLegFailure(target, io.EOF)
	if client.getLeg(target.id) == target {
		t.Fatal("test did not remove the closed target from the active registry")
	}
	hello := phase36V7Hello(0, false)
	message := StreamDrainMessage{
		Kind: StreamDrainCommitAck, SenderRole: StreamPathRoleServer,
		SessionID: hello.SessionID, TargetLeg: LegID(target.id), Generation: target.generation,
		TargetBinding: target.drainBinding, OperationID: opID, Counter: 1,
	}
	staleGeneration := message
	staleGeneration.Generation++
	staleWire, err := landingControlAuth.EncodeDrainMessage(staleGeneration)
	if err != nil {
		t.Fatalf("encode stale-generation CommitAck: %v", err)
	}
	if err := client.handleDrainControlFrame(survivor, staleWire); err != nil {
		t.Fatalf("handle stale-generation CommitAck: %v", err)
	}
	client.drainMu.Lock()
	staleAcknowledged := op.commitAck
	client.drainMu.Unlock()
	if staleAcknowledged {
		t.Fatal("CommitAck from a stale peer generation was accepted")
	}
	wire, err := landingControlAuth.EncodeDrainMessage(message)
	if err != nil {
		t.Fatalf("encode authenticated CommitAck: %v", err)
	}
	if err := client.handleDrainControlFrame(survivor, wire); err != nil {
		t.Fatalf("handle authenticated CommitAck after target retirement: %v", err)
	}
	client.drainMu.Lock()
	acknowledged := op.commitAck
	client.drainMu.Unlock()
	if !acknowledged {
		t.Fatal("authenticated CommitAck was ignored after target left the active registry")
	}
}

func TestStreamV7StaleCommitAckCannotAffectRejoinedTargetGeneration(t *testing.T) {
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
	_, landingControlAuth := phase36AttachDrainLegPair(t, client, landing, 0)
	phase36AttachDrainLegPair(t, client, landing, 1)
	target := client.getLeg(1)
	survivor := client.getLeg(0)
	if target == nil || survivor == nil {
		t.Fatal("missing target or surviving leg")
	}
	opID := [16]byte{4, 3, 2, 1}
	op := &streamDrainOperation{
		target: target, operationID: opID, targetBinding: target.drainBinding, counter: 1,
		peerGeneration: target.generation, initiator: true, commitSent: true,
		changed: make(chan struct{}, 1),
	}
	client.drainMu.Lock()
	client.drainOps[target.id] = op
	client.drainMu.Unlock()
	client.handleLegFailure(target, io.EOF)

	replacementConn, replacementPeer := net.Pipe()
	defer replacementPeer.Close()
	replacementHello := phase36V7Hello(1, false)
	replacementClientAuth, _ := phase36V7AuthPair(t, replacementHello, StreamCapabilityCoordinatedDrain)
	if err := client.AttachLegWithProtocolAuth(LegID(1), replacementConn, nil, replacementClientAuth); err != nil {
		t.Fatalf("attach replacement generation: %v", err)
	}
	replacement := client.getLeg(1)
	if replacement == nil || replacement == target || replacement.generation == target.generation {
		t.Fatal("test did not attach a distinct target generation")
	}

	message := StreamDrainMessage{
		Kind: StreamDrainCommitAck, SenderRole: StreamPathRoleServer,
		SessionID: replacementHello.SessionID, TargetLeg: LegID(target.id), Generation: target.generation,
		TargetBinding: target.drainBinding, OperationID: opID, Counter: 1,
	}
	wire, err := landingControlAuth.EncodeDrainMessage(message)
	if err != nil {
		t.Fatalf("encode stale CommitAck: %v", err)
	}
	if err := client.handleDrainControlFrame(survivor, wire); err != nil {
		t.Fatalf("handle stale CommitAck: %v", err)
	}
	client.drainMu.Lock()
	acknowledged := op.commitAck
	client.drainMu.Unlock()
	if acknowledged {
		t.Fatal("CommitAck for the retired generation affected the rejoined generation")
	}
	if client.getLeg(1) != replacement {
		t.Fatal("stale CommitAck changed the active replacement binding")
	}
}

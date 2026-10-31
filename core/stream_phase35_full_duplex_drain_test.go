//go:build phase35baseline
// +build phase35baseline

package smp3core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type phase35CausalEvent struct {
	RunID             string                   `json:"run_id"`
	SessionID         string                   `json:"session_id"`
	Endpoint          string                   `json:"endpoint"`
	Direction         string                   `json:"direction"`
	DataIdentity      string                   `json:"data_identity,omitempty"`
	DataSeqStart      *uint64                  `json:"data_seq_start,omitempty"`
	DataSeqEnd        *uint64                  `json:"data_seq_end,omitempty"`
	DataLength        int                      `json:"data_length,omitempty"`
	AckFrontierBefore *uint64                  `json:"ack_frontier_before,omitempty"`
	AckFrontierAfter  *uint64                  `json:"ack_frontier_after,omitempty"`
	EvidenceStatus    string                   `json:"evidence_status"`
	IdentityStatus    string                   `json:"identity_status,omitempty"`
	Event             streamRecoveryTraceEvent `json:"event"`
}

type phase35Payload struct {
	Direction string
	Payload   []byte
}

type phase35PayloadDescriptor struct {
	Direction string `json:"direction"`
	Bytes     int    `json:"payload_bytes"`
	SHA256    string `json:"sha256"`
}

type phase35TraceCollector struct {
	mu           sync.Mutex
	notify       chan struct{}
	runID        string
	sessionID    string
	ackFrontiers map[string]uint64
	events       []phase35CausalEvent
}

func newPhase35TraceCollector() *phase35TraceCollector {
	runID := os.Getenv("SMP3_PHASE35_RUN_ID")
	if runID == "" {
		runID = fmt.Sprintf("phase35-local-%d", time.Now().UnixNano())
	}
	return &phase35TraceCollector{notify: make(chan struct{}, 1), runID: runID, sessionID: "phase35-session-full-duplex-drain", ackFrontiers: make(map[string]uint64)}
}

func phase35IsDataEvent(kind, frameType string) bool {
	if frameType == "data" {
		return true
	}
	switch kind {
	case "tx_ordinary_assigned", "tx_mark_transit", "tx_mark_rescue_transit", "tx_mark_restricted_rescue_transit",
		"tx_mark_attempt_sent_commit", "tx_mark_rescue_queued", "tx_owner_invalidated",
		"send_queue_accepted", "send_queue_rejected", "send_queue_aborted", "writer_dequeue",
		"core_data_write_enter", "core_data_write_dispatch", "core_data_write_return", "retry_candidate_selected",
		"rescue_queue_accepted", "rescue_queue_rejected", "rescue_queue_aborted",
		"rx_data_received", "rx_contiguous_delivery", "rx_app_pipe_write_return",
		"tx_ledger_reconciled", "rx_ledger_reconciled":
		return true
	default:
		return false
	}
}

func phase35IsInboundDataEvent(kind string) bool {
	switch kind {
	case "rx_data_received", "rx_contiguous_delivery", "rx_app_pipe_write_return", "rx_ledger_reconciled":
		return true
	default:
		return false
	}
}

func (c *phase35TraceCollector) add(endpoint string, event streamRecoveryTraceEvent) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if event.Endpoint == "" {
		event.Endpoint = endpoint
	}
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now()
	}
	idx := uint64(len(c.events) + 1)
	event.EventIndex = idx
	direction := "control"
	if event.Kind == "ack_frontier_advanced" {
		if endpoint == "client" {
			direction = "client_to_landing"
		} else if endpoint == "server" {
			direction = "landing_to_client"
		}
	} else if phase35IsInboundDataEvent(event.Kind) {
		if endpoint == "client" {
			direction = "landing_to_client"
		} else if endpoint == "server" {
			direction = "client_to_landing"
		}
	} else if phase35IsDataEvent(event.Kind, event.FrameType) {
		if endpoint == "client" {
			direction = "client_to_landing"
		} else if endpoint == "server" {
			direction = "landing_to_client"
		}
	}
	entry := phase35CausalEvent{RunID: c.runID, SessionID: c.sessionID, Endpoint: endpoint, Direction: direction, EvidenceStatus: "OBSERVED", Event: event}
	if event.Kind == "ack_frontier_advanced" {
		before := c.ackFrontiers[direction]
		after := event.AckedNext
		entry.AckFrontierBefore, entry.AckFrontierAfter = &before, &after
		if after > before {
			start, end := before, after
			entry.DataSeqStart, entry.DataSeqEnd = &start, &end
			entry.DataLength = event.WriteResultBytes
			entry.DataIdentity = fmt.Sprintf("%s/%s/%d-%d/%d", c.sessionID, direction, start, end, entry.DataLength)
			entry.IdentityStatus = "INFERRED_FROM_CUMULATIVE_ACK_FRONTIER"
			c.ackFrontiers[direction] = after
		}
	} else if phase35IsDataEvent(event.Kind, event.FrameType) {
		start := event.RecordSequence
		if event.Kind == "tx_mark_transit" || event.Kind == "tx_ordinary_assigned" {
			start = event.RecordSequence
		}
		end := start + 1
		entry.DataSeqStart, entry.DataSeqEnd = &start, &end
		entry.DataLength = event.PayloadBytes
		entry.DataIdentity = fmt.Sprintf("%s/%s/%d-%d/%d", c.sessionID, direction, start, end, event.PayloadBytes)
		entry.IdentityStatus = "INFERRED_FROM_SESSION_DIRECTION_FRAME_SEQUENCE_AND_LENGTH"
	}
	c.events = append(c.events, entry)
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return idx
}

func (c *phase35TraceCollector) synthetic(endpoint, direction, kind, action string, legID int16, generation, seq uint64, payloadBytes int, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	event := streamRecoveryTraceEvent{Kind: kind, Endpoint: endpoint, LegID: legID, LegGeneration: generation, RecordSequence: seq, FrameSequence: seq, Action: action, Applied: true, ObservedAt: time.Now()}
	if direction != "control" {
		event.FrameType = "data"
		event.PayloadBytes = payloadBytes
	}
	idx := uint64(len(c.events) + 1)
	event.EventIndex = idx
	entry := phase35CausalEvent{RunID: c.runID, SessionID: c.sessionID, Endpoint: endpoint, Direction: direction, EvidenceStatus: status, Event: event}
	if direction != "control" {
		start, end := seq, seq+1
		entry.DataSeqStart, entry.DataSeqEnd = &start, &end
		entry.DataLength = payloadBytes
		entry.DataIdentity = fmt.Sprintf("%s/%s/%d-%d/%d", c.sessionID, direction, start, end, payloadBytes)
		entry.IdentityStatus = "INFERRED_FROM_SESSION_DIRECTION_FRAME_SEQUENCE_AND_LENGTH"
	}
	c.events = append(c.events, entry)
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func phase35AttachTCPLegPair(t *testing.T, left, right *StreamEngine, id LegID) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for loopback TCP Leg %d: %v", id, err)
	}
	defer listener.Close()
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		accepted <- acceptResult{conn: conn, err: acceptErr}
	}()
	clientConn, err := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial loopback TCP Leg %d: %v", id, err)
	}
	var serverConn net.Conn
	select {
	case result := <-accepted:
		if result.err != nil {
			_ = clientConn.Close()
			t.Fatalf("accept loopback TCP Leg %d: %v", id, result.err)
		}
		serverConn = result.conn
	case <-time.After(2 * time.Second):
		_ = clientConn.Close()
		t.Fatalf("accept loopback TCP Leg %d timed out", id)
	}
	if err := left.AttachLeg(id, clientConn, nil); err != nil {
		_ = clientConn.Close()
		_ = serverConn.Close()
		t.Fatalf("attach client TCP Leg %d: %v", id, err)
	}
	if err := right.AttachLeg(id, serverConn, nil); err != nil {
		_ = serverConn.Close()
		t.Fatalf("attach Landing TCP Leg %d: %v", id, err)
	}
}

func phase35ReadAndVerifyPayload(t *testing.T, reader io.Reader, want []byte) []byte {
	t.Helper()
	got := make([]byte, len(want))
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(reader, got)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read exact application payload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out reading application payload")
	}
	if !bytes.Equal(got, want) {
		t.Fatal("application payload bytes differ from original DATA")
	}
	if sha256.Sum256(got) != sha256.Sum256(want) {
		t.Fatal("application payload SHA-256 differs from original DATA")
	}
	return got
}

func phase35QueueOriginalData(t *testing.T, engine *StreamEngine, trace *phase35TraceCollector, endpoint, peerEndpoint string, legID LegID, payload []byte) *StreamTXRecord {
	t.Helper()
	createdAt := time.Now()
	record := engine.txLedger.Add(payload, createdAt)
	leg := engine.getLeg(uint8(legID))
	if leg == nil {
		t.Fatalf("%s Leg %d is not attached", endpoint, legID)
	}
	if !engine.txLedger.assignOrdinary(record, uint8(legID), createdAt) {
		t.Fatalf("%s could not assign DATA sequence %d to Leg %d", endpoint, record.Sequence(), legID)
	}
	engine.txLedger.setOrigin(record, leg.generation, createdAt)
	if !engine.tryQueue(leg, record, false) {
		engine.txLedger.mu.Lock()
		owner, transitLeg, inTransit, invalidated := record.ordinaryOwner, record.transitLeg, record.inTransit, record.invalidated
		engine.txLedger.mu.Unlock()
		claimed := false
		for _, event := range trace.snapshot() {
			if event.Endpoint == endpoint && event.Event.Kind == "tx_mark_transit" && event.Event.RecordSequence == record.Sequence() && event.Event.LegID == int16(legID) && event.Event.InTransit {
				claimed = true
				break
			}
		}
		if !claimed || !inTransit || transitLeg != legID || owner != int16(legID) || invalidated {
			t.Fatalf("%s original DATA did not queue on Leg %d: claimed=%t owner=%d transit_leg=%d in_transit=%t invalidated=%t", endpoint, legID, claimed, owner, transitLeg, inTransit, invalidated)
		}
		trace.synthetic(endpoint, "control", "initial_data_claimed_concurrently", "tryQueue returned false while the DATA already had a live in-transit claim on its original Leg; queue/write completion remains the hard gate", int16(legID), leg.generation, record.Sequence(), len(payload), "OBSERVED_STATE_WITH_CALLER_UNRESOLVED")
	}
	initialWrite := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == endpoint && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == record.Sequence() && e.Event.LegID == int16(legID) && e.Event.Applied
	})
	if initialWrite.Event.CarrierAcceptedPayloadBytes != uint64(len(payload)) {
		t.Fatalf("%s original DATA Carrier-accepted payload bytes=%d, want %d", endpoint, initialWrite.Event.CarrierAcceptedPayloadBytes, len(payload))
	}
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == peerEndpoint && e.Event.Kind == "rx_data_received" && e.Event.RecordSequence == record.Sequence() && e.Event.LegID == int16(legID) && e.Event.Action == "unique_contiguous"
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == peerEndpoint && e.Event.Kind == "rx_contiguous_delivery" && e.Event.RecordSequence == record.Sequence()
	})
	return record
}

func (c *phase35TraceCollector) snapshot() []phase35CausalEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]phase35CausalEvent(nil), c.events...)
}

func phase35WaitEvent(t *testing.T, c *phase35TraceCollector, timeout time.Duration, match func(phase35CausalEvent) bool) phase35CausalEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for _, event := range c.snapshot() {
			if match(event) {
				return event
			}
		}
		select {
		case <-c.notify:
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatalf("timed out waiting for Phase35 event; recorded=%d", len(c.snapshot()))
		}
	}
}

func phase35WriteLedger(t *testing.T, c *phase35TraceCollector, payloads ...phase35Payload) {
	t.Helper()
	path := os.Getenv("SMP3_PHASE35_LEDGER_JSONL")
	if path == "" {
		ledgerDir := os.Getenv("SMP3_PHASE35_LEDGER_DIR")
		if ledgerDir == "" {
			return
		}
		if err := os.MkdirAll(ledgerDir, 0o700); err != nil {
			t.Fatalf("create per-run causal ledger directory: %v", err)
		}
		fileName := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(c.runID)
		path = filepath.Join(ledgerDir, fileName+".jsonl")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create per-DATA evidence without overwrite: %v", err)
	}
	defer f.Close()
	payloadManifest := make([]phase35PayloadDescriptor, 0, len(payloads))
	for _, payload := range payloads {
		hash := sha256.Sum256(payload.Payload)
		payloadManifest = append(payloadManifest, phase35PayloadDescriptor{Direction: payload.Direction, Bytes: len(payload.Payload), SHA256: fmt.Sprintf("%x", hash)})
	}
	manifest := struct {
		RecordType     string                     `json:"record_type"`
		RunID          string                     `json:"run_id"`
		SessionID      string                     `json:"session_id"`
		SessionIDScope string                     `json:"session_id_scope"`
		Payloads       []phase35PayloadDescriptor `json:"payloads"`
		Plaintext      bool                       `json:"application_payload_persisted"`
	}{RecordType: "run", RunID: c.runID, SessionID: c.sessionID, SessionIDScope: "test_synthetic_not_production_session_id", Payloads: payloadManifest, Plaintext: false}
	manifestLine, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(manifestLine, '\n')); err != nil {
		t.Fatal(err)
	}
	for _, event := range c.snapshot() {
		b, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestPhase35FullDuplexGracefulDrainInvalidatesReverseOutstandingDATA(t *testing.T) {
	trace := newPhase35TraceCollector()
	clientCfg := testStreamConfig()
	clientCfg.ChunkSize = 32 * 1024
	clientCfg.MaxInflightFrames = 8
	clientCfg.RetransmitTimeout = time.Second // isolated causal test; no timer-based retry is expected
	clientCfg.PathRole = StreamPathRoleClient
	clientCfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 { return trace.add("client", event) }
	landingCfg := clientCfg
	landingCfg.PathRole = StreamPathRoleServer
	landingCfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 { return trace.add("server", event) }
	client, clientApp := NewStreamEngine(clientCfg)
	landing, landingApp := NewStreamEngine(landingCfg)
	defer client.Close()
	defer landing.Close()
	defer clientApp.Close()
	defer landingApp.Close()
	phase35AttachTCPLegPair(t, client, landing, 0)
	phase35AttachTCPLegPair(t, client, landing, 1)

	reversePayload := bytes.Repeat([]byte("phase35-landing-to-client-reverse-data-"), 900)
	forwardPayload := bytes.Repeat([]byte("phase35-client-to-landing-forward-data-"), 300)
	defer phase35WriteLedger(t, trace,
		phase35Payload{Direction: "landing_to_client", Payload: reversePayload},
		phase35Payload{Direction: "client_to_landing", Payload: forwardPayload},
	)
	reverseRecord := phase35QueueOriginalData(t, landing, trace, "server", "client", 1, reversePayload)
	forwardRecord := phase35QueueOriginalData(t, client, trace, "client", "server", 0, forwardPayload)
	if got := landing.txLedger.outstandingForLeg(1); got != 1 {
		t.Fatalf("reverse DATA outstanding on Landing Leg 1=%d, want 1", got)
	}
	if got := client.txLedger.outstandingForLeg(0); got != 1 {
		t.Fatalf("forward DATA outstanding on Client Leg 0=%d, want 1", got)
	}
	if got := client.txLedger.outstandingForLeg(1); got != 0 {
		t.Fatalf("Client local TX ledger reports %d outstanding records on draining Leg 1, want 0", got)
	}
	if got := landing.txLedger.outstandingForLeg(1); got != 1 {
		t.Fatalf("reverse outstanding owned by Landing Leg 1=%d, want 1", got)
	}
	if got := client.ackNext.Load(); got != 0 {
		t.Fatalf("Client RX ACK frontier before application read=%d, want 0", got)
	}
	if got := landing.ackNext.Load(); got != 0 {
		t.Fatalf("Landing RX ACK frontier before application read=%d, want 0", got)
	}
	clientLeg1 := client.getLeg(1)
	if clientLeg1 == nil {
		t.Fatal("Client Leg 1 disappeared before DrainLeg")
	}
	trace.synthetic("client", "control", "drain_requested", fmt.Sprintf("DrainLeg(1); local TX outstanding on Leg1=0; client-to-landing seq=%d remains outstanding on Leg0; remote landing-to-client seq=%d remains outstanding on Leg1", forwardRecord.Sequence(), reverseRecord.Sequence()), 1, clientLeg1.generation, 0, 0, "OBSERVED_CALL_AND_BOTH_LOCAL_LEDGER_SNAPSHOTS")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DrainLeg(ctx, 1); err != nil {
		t.Fatalf("client DrainLeg: %v", err)
	}
	trace.synthetic("client", "control", "drain_committed", "client DrainLeg returned after local queue/TX owner check; full-duplex TCP Carrier retired while reverse Landing TX remained outstanding", 1, clientLeg1.generation, 0, 0, "OBSERVED_RETURN_AND_REGISTRY")
	if client.HasLeg(1) {
		t.Fatal("client retained Leg 1 after successful DrainLeg")
	}
	peerRetireDeadline := time.Now().Add(2 * time.Second)
	for landing.HasLeg(1) && time.Now().Before(peerRetireDeadline) {
		time.Sleep(time.Millisecond)
	}
	if landing.HasLeg(1) {
		t.Fatal("Landing still considers the peer-closed Carrier active")
	}
	retireCtx, retireCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := landing.WaitLegRetired(retireCtx, 1); err != nil {
		retireCancel()
		t.Fatalf("Landing worker retirement: %v", err)
	}
	retireCancel()
	landing.txLedger.mu.Lock()
	owner, originGeneration, invalidated, priorSendCount := reverseRecord.ordinaryOwner, reverseRecord.originGeneration, reverseRecord.invalidated, reverseRecord.sendCount
	landing.txLedger.mu.Unlock()
	if owner != 1 || originGeneration != 1 || !invalidated || priorSendCount < 1 {
		t.Fatalf("peer failure did not preserve invalidated original ownership: owner=%d origin_generation=%d invalidated=%t send_count=%d", owner, originGeneration, invalidated, priorSendCount)
	}
	ownerInvalidation := phase35WaitEvent(t, trace, time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "tx_owner_invalidated" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 1 && e.Event.LegGeneration == 1 && e.Event.RecordGeneration == originGeneration && e.Event.Applied
	})
	if ownerInvalidation.Event.AckedNext != 0 || ownerInvalidation.Event.ReasonAfter == "" || !strings.Contains(ownerInvalidation.Event.StateBefore, "invalidated=false") || !strings.Contains(ownerInvalidation.Event.StateAfter, "invalidated=true") {
		t.Fatalf("owner invalidation event lacks causal state/reason: %+v", ownerInvalidation.Event)
	}
	retryDecision := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "retry_candidate_selected" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 0 && e.Event.LegGeneration == 1 && e.Event.RecordGeneration == originGeneration && e.Event.AckedNext == 0 && e.Event.ReasonBefore == "original_owner_invalidated"
	})
	retryTransit := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "tx_mark_transit" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 0 && e.Event.SendCount >= 1
	})
	retryQueue := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "send_queue_accepted" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 0 && e.Event.LegGeneration == 1 && e.Event.Applied
	})
	retryWrite := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 0 && e.Event.Applied
	})
	if !(ownerInvalidation.Event.EventIndex < retryDecision.Event.EventIndex && retryDecision.Event.EventIndex < retryTransit.Event.EventIndex && retryTransit.Event.EventIndex < retryQueue.Event.EventIndex && retryQueue.Event.EventIndex < retryWrite.Event.EventIndex) {
		t.Fatalf("owner invalidation/retry causal order violated: invalidate=%d decision=%d transit=%d queue=%d write=%d", ownerInvalidation.Event.EventIndex, retryDecision.Event.EventIndex, retryTransit.Event.EventIndex, retryQueue.Event.EventIndex, retryWrite.Event.EventIndex)
	}
	if retryWrite.Event.CarrierAcceptedPayloadBytes != uint64(len(reversePayload)) {
		t.Fatalf("retry Carrier-accepted payload bytes=%d, want %d", retryWrite.Event.CarrierAcceptedPayloadBytes, len(reversePayload))
	}
	gotReverse := phase35ReadAndVerifyPayload(t, clientApp, reversePayload)
	gotForward := phase35ReadAndVerifyPayload(t, landingApp, forwardPayload)
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Event.Kind == "rx_app_pipe_write_return" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.Applied
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "rx_app_pipe_write_return" && e.Event.RecordSequence == forwardRecord.Sequence() && e.Event.Applied
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Event.Kind == "rx_data_received" && e.Event.RecordSequence == reverseRecord.Sequence() && e.Event.LegID == 0 && e.Event.Action == "duplicate"
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "rx_data_received" && e.Event.RecordSequence == forwardRecord.Sequence() && e.Event.LegID == 0 && e.Event.Action == "unique_contiguous"
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "ack_frontier_advanced" && e.Event.AckedNext == reverseRecord.Sequence()+1
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Event.Kind == "ack_frontier_advanced" && e.Event.AckedNext == forwardRecord.Sequence()+1
	})
	if got := client.rxDeliveredBytes.Load(); got != uint64(len(reversePayload)) {
		t.Fatalf("Client application-delivered reverse bytes=%d, want exactly %d", got, len(reversePayload))
	}
	if got := landing.rxDeliveredBytes.Load(); got != uint64(len(forwardPayload)) {
		t.Fatalf("Landing application-delivered forward bytes=%d, want exactly %d", got, len(forwardPayload))
	}
	if got := client.ackNext.Load(); got != reverseRecord.Sequence()+1 {
		t.Fatalf("Client RX ACK frontier=%d, want %d", got, reverseRecord.Sequence()+1)
	}
	if got := landing.ackNext.Load(); got != forwardRecord.Sequence()+1 {
		t.Fatalf("Landing RX ACK frontier=%d, want %d", got, forwardRecord.Sequence()+1)
	}
	deadline := time.Now().Add(2 * time.Second)
	for (client.txLedger.Snapshot(time.Now()).OutstandingFrames != 0 || landing.txLedger.Snapshot(time.Now()).OutstandingFrames != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clientTX := client.txLedger.Snapshot(time.Now())
	landingTX := landing.txLedger.Snapshot(time.Now())
	clientRX := client.Snapshot()
	landingRX := landing.Snapshot()
	if clientTX.OutstandingFrames != 0 || clientTX.AckedNext != forwardRecord.Sequence()+1 {
		t.Fatalf("Client TX ledger did not reconcile: outstanding=%d acked_next=%d", clientTX.OutstandingFrames, clientTX.AckedNext)
	}
	if landingTX.OutstandingFrames != 0 || landingTX.AckedNext != reverseRecord.Sequence()+1 {
		t.Fatalf("Landing TX ledger did not reconcile: outstanding=%d acked_next=%d", landingTX.OutstandingFrames, landingTX.AckedNext)
	}
	if clientRX.RxPendingFrames != 0 || clientRX.RxPendingBytes != 0 || landingRX.RxPendingFrames != 0 || landingRX.RxPendingBytes != 0 {
		t.Fatalf("RX pending not cleared: client=%d/%d landing=%d/%d", clientRX.RxPendingFrames, clientRX.RxPendingBytes, landingRX.RxPendingFrames, landingRX.RxPendingBytes)
	}
	clientLeg0Dispatches := 0
	for _, event := range trace.snapshot() {
		if event.Endpoint == "client" && event.Direction == "client_to_landing" && event.Event.Kind == "core_data_write_dispatch" && event.Event.RecordSequence == forwardRecord.Sequence() && event.Event.LegID == 0 {
			clientLeg0Dispatches++
		}
	}
	if clientLeg0Dispatches != 1 {
		t.Fatalf("unaffected client-to-Landing DATA dispatches=%d, want 1", clientLeg0Dispatches)
	}
	trace.add("client", streamRecoveryTraceEvent{Kind: "tx_ledger_reconciled", Endpoint: "client", RecordSequence: forwardRecord.Sequence(), PayloadBytes: len(forwardPayload), AckedNext: clientTX.AckedNext, RetiredFrames: 1, StateAfter: fmt.Sprintf("outstanding_frames=%d; outstanding_bytes=%d; acked_next=%d", clientTX.OutstandingFrames, clientTX.OutstandingBytes, clientTX.AckedNext), Action: "client-to-Landing original DATA final TX ledger snapshot", Applied: clientTX.OutstandingFrames == 0, ObservedAt: time.Now()})
	trace.add("server", streamRecoveryTraceEvent{Kind: "tx_ledger_reconciled", Endpoint: "server", RecordSequence: reverseRecord.Sequence(), PayloadBytes: len(reversePayload), AckedNext: landingTX.AckedNext, RetiredFrames: 1, StateAfter: fmt.Sprintf("outstanding_frames=%d; outstanding_bytes=%d; acked_next=%d", landingTX.OutstandingFrames, landingTX.OutstandingBytes, landingTX.AckedNext), Action: "Landing-to-client original DATA final TX ledger snapshot", Applied: landingTX.OutstandingFrames == 0, ObservedAt: time.Now()})
	trace.add("client", streamRecoveryTraceEvent{Kind: "rx_ledger_reconciled", Endpoint: "client", RecordSequence: reverseRecord.Sequence(), PayloadBytes: len(reversePayload), AckedNext: clientRX.RxAckNext, StateAfter: fmt.Sprintf("rx_ack_next=%d; delivered_bytes=%d; pending_frames=%d; pending_bytes=%d", clientRX.RxAckNext, clientRX.RxDeliveredBytes, clientRX.RxPendingFrames, clientRX.RxPendingBytes), Action: "Landing-to-client final RX/application snapshot", Applied: clientRX.RxPendingFrames == 0 && clientRX.RxPendingBytes == 0, ObservedAt: time.Now()})
	trace.add("server", streamRecoveryTraceEvent{Kind: "rx_ledger_reconciled", Endpoint: "server", RecordSequence: forwardRecord.Sequence(), PayloadBytes: len(forwardPayload), AckedNext: landingRX.RxAckNext, StateAfter: fmt.Sprintf("rx_ack_next=%d; delivered_bytes=%d; pending_frames=%d; pending_bytes=%d", landingRX.RxAckNext, landingRX.RxDeliveredBytes, landingRX.RxPendingFrames, landingRX.RxPendingBytes), Action: "client-to-Landing final RX/application snapshot", Applied: landingRX.RxPendingFrames == 0 && landingRX.RxPendingBytes == 0, ObservedAt: time.Now()})

	t.Logf("Phase35 run_id=%s session_id=%s carrier=loopback_tcp; reverse_seq=%s reverse_bytes=%d reverse_sha256=%x forward_seq=%s forward_bytes=%d forward_sha256=%x events=%s", trace.runID, trace.sessionID, strconv.FormatUint(reverseRecord.Sequence(), 10), len(gotReverse), sha256.Sum256(gotReverse), strconv.FormatUint(forwardRecord.Sequence(), 10), len(gotForward), sha256.Sum256(gotForward), strconv.Itoa(len(trace.snapshot())))
}

func TestPhase35ApplicationFullDuplexDrainRetriesReverseOutstandingDATA(t *testing.T) {
	trace := newPhase35TraceCollector()
	clientCfg := testStreamConfig()
	clientCfg.ChunkSize = 32 * 1024
	clientCfg.MaxInflightFrames = 8
	clientCfg.RetransmitTimeout = 5 * time.Second // suppress timer-driven rescue in this drain-only causality test
	clientCfg.SchedulerMode = StreamSchedulerAggregation
	clientCfg.CapacityMode = StreamCapacityDynamic
	clientCfg.BandwidthMbps = []uint32{100, 100}
	clientCfg.BenchmarkFixedRatio = true // deterministic owner selection, production policy unchanged
	clientCfg.PathRole = StreamPathRoleClient
	clientCfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 { return trace.add("client", event) }
	landingCfg := clientCfg
	landingCfg.PathRole = StreamPathRoleServer
	landingCfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 { return trace.add("server", event) }
	client, clientApp := NewStreamEngine(clientCfg)
	landing, landingApp := NewStreamEngine(landingCfg)
	defer client.Close()
	defer landing.Close()
	defer clientApp.Close()
	defer landingApp.Close()
	phase35AttachTCPLegPair(t, client, landing, 0)
	phase35AttachTCPLegPair(t, client, landing, 1)

	reversePayload := bytes.Repeat([]byte("phase35-application-landing-to-client-"), 900)
	forwardPayload := bytes.Repeat([]byte("phase35-application-client-to-landing-"), 300)
	defer phase35WriteLedger(t, trace,
		phase35Payload{Direction: "landing_to_client", Payload: reversePayload},
		phase35Payload{Direction: "client_to_landing", Payload: forwardPayload},
	)

	startWrites := make(chan struct{})
	writeResults := make(chan error, 2)
	go func() {
		<-startWrites
		n, err := clientApp.Write(forwardPayload)
		if err == nil && n != len(forwardPayload) {
			err = io.ErrShortWrite
		}
		writeResults <- err
	}()
	go func() {
		<-startWrites
		n, err := landingApp.Write(reversePayload)
		if err == nil && n != len(reversePayload) {
			err = io.ErrShortWrite
		}
		writeResults <- err
	}()
	close(startWrites)
	for i := 0; i < 2; i++ {
		select {
		case err := <-writeResults:
			if err != nil {
				t.Fatalf("application write: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent application writes")
		}
	}
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Direction == "landing_to_client" && e.Event.Kind == "send_queue_accepted" && e.Event.RecordSequence == 0 && e.Event.LegID == 0 && e.Event.LegGeneration == 1
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Direction == "landing_to_client" && e.Event.Kind == "send_queue_accepted" && e.Event.RecordSequence == 1 && e.Event.LegID == 1 && e.Event.LegGeneration == 1
	})
	forwardWrite := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Direction == "client_to_landing" && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == 0 && e.Event.LegID == 0 && e.Event.Applied
	})
	if forwardWrite.Event.CarrierAcceptedPayloadBytes != uint64(len(forwardPayload)) {
		t.Fatalf("forward Carrier accepted bytes=%d, want %d", forwardWrite.Event.CarrierAcceptedPayloadBytes, len(forwardPayload))
	}
	reverseFirstWrite := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Direction == "landing_to_client" && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == 0 && e.Event.LegID == 0 && e.Event.Applied
	})
	reverseRetryOriginal := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Direction == "landing_to_client" && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == 1 && e.Event.LegID == 1 && e.Event.Applied
	})
	if reverseFirstWrite.Event.CarrierAcceptedPayloadBytes+reverseRetryOriginal.Event.CarrierAcceptedPayloadBytes != uint64(len(reversePayload)) {
		t.Fatalf("reverse original Carrier accepted bytes=%d+%d, want %d", reverseFirstWrite.Event.CarrierAcceptedPayloadBytes, reverseRetryOriginal.Event.CarrierAcceptedPayloadBytes, len(reversePayload))
	}
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Direction == "landing_to_client" && e.Event.Kind == "rx_data_received" && e.Event.RecordSequence == 0 && e.Event.LegID == 0 && e.Event.Action == "unique_contiguous"
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Direction == "client_to_landing" && e.Event.Kind == "rx_data_received" && e.Event.RecordSequence == 0 && e.Event.LegID == 0 && e.Event.Action == "unique_contiguous"
	})
	if client.ackNext.Load() != 0 || landing.ackNext.Load() != 0 {
		t.Fatalf("ACK advanced before destination applications read: client=%d landing=%d", client.ackNext.Load(), landing.ackNext.Load())
	}
	if got := client.txLedger.outstandingForLeg(1); got != 0 {
		t.Fatalf("Client has %d local TX records on draining Leg 1, want 0", got)
	}
	if got := landing.txLedger.outstandingForLeg(1); got != 1 {
		t.Fatalf("Landing reverse TX records on Leg 1=%d, want one", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DrainLeg(ctx, 1); err != nil {
		t.Fatalf("application-path Client DrainLeg: %v", err)
	}
	if client.HasLeg(1) {
		t.Fatal("Client retained removed application-path Leg 1")
	}
	deadline := time.Now().Add(2 * time.Second)
	for landing.HasLeg(1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if landing.HasLeg(1) {
		t.Fatal("Landing retained full-duplex TCP Leg 1 after peer drain")
	}
	ownerInvalidation := phase35WaitEvent(t, trace, time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "tx_owner_invalidated" && e.Event.RecordSequence == 1 && e.Event.LegID == 1 && e.Event.LegGeneration == 1 && e.Event.RecordGeneration == 1 && e.Event.Applied
	})
	landing.txLedger.mu.Lock()
	original := landing.txLedger.outstanding[1]
	var owner int16
	var generation uint64
	var invalidated bool
	if original != nil {
		owner, generation, invalidated = original.ordinaryOwner, original.originGeneration, original.invalidated
	}
	landing.txLedger.mu.Unlock()
	if original == nil {
		t.Fatal("Landing reverse sequence 1 disappeared before its ACK")
	}
	if owner != 1 || generation != 1 || !invalidated {
		t.Fatalf("Landing reverse DATA owner not invalidated: owner=%d generation=%d invalidated=%t", owner, generation, invalidated)
	}
	if ownerInvalidation.Event.AckedNext != 0 || ownerInvalidation.Event.ReasonAfter == "" {
		t.Fatalf("application-path owner invalidation evidence incomplete: %+v", ownerInvalidation.Event)
	}
	retryDecision := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "retry_candidate_selected" && e.Event.RecordSequence == 1 && e.Event.LegID == 0 && e.Event.LegGeneration == 1 && e.Event.AckedNext == 0 && e.Event.ReasonBefore == "original_owner_invalidated"
	})
	retryWrite := phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "core_data_write_return" && e.Event.RecordSequence == 1 && e.Event.LegID == 0 && e.Event.Applied
	})
	if ownerInvalidation.Event.EventIndex >= retryDecision.Event.EventIndex || retryDecision.Event.EventIndex >= retryWrite.Event.EventIndex {
		t.Fatalf("application-path retry causal order invalidated=%d decision=%d write=%d", ownerInvalidation.Event.EventIndex, retryDecision.Event.EventIndex, retryWrite.Event.EventIndex)
	}
	if retryWrite.Event.CarrierAcceptedPayloadBytes != uint64(len(reversePayload)-clientCfg.ChunkSize) {
		t.Fatalf("retry accepted payload bytes=%d, want second frame size %d", retryWrite.Event.CarrierAcceptedPayloadBytes, len(reversePayload)-clientCfg.ChunkSize)
	}
	gotReverse := phase35ReadAndVerifyPayload(t, clientApp, reversePayload)
	gotForward := phase35ReadAndVerifyPayload(t, landingApp, forwardPayload)
	if len(gotReverse) != len(reversePayload) || len(gotForward) != len(forwardPayload) {
		t.Fatal("application payload lengths changed")
	}
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "client" && e.Event.Kind == "rx_app_pipe_write_return" && e.Event.RecordSequence == 1 && e.Event.Applied
	})
	phase35WaitEvent(t, trace, 2*time.Second, func(e phase35CausalEvent) bool {
		return e.Endpoint == "server" && e.Event.Kind == "rx_app_pipe_write_return" && e.Event.RecordSequence == 0 && e.Event.Applied
	})
	rxSeq1 := make([]phase35CausalEvent, 0, 2)
	for _, event := range trace.snapshot() {
		if event.Endpoint == "client" && event.Direction == "landing_to_client" && event.Event.Kind == "rx_data_received" && event.Event.RecordSequence == 1 {
			rxSeq1 = append(rxSeq1, event)
		}
	}
	if len(rxSeq1) != 2 {
		t.Fatalf("reverse seq=1 receiver observations=%d, want original plus retry copy; events=%+v", len(rxSeq1), rxSeq1)
	}
	duplicates := 0
	for _, event := range rxSeq1 {
		if event.Event.Action == "duplicate" || event.Event.Action == "buffered_duplicate" {
			duplicates++
		}
	}
	if duplicates != 1 {
		t.Fatalf("reverse seq=1 duplicate dispositions=%d, want exactly one duplicate copy; events=%+v", duplicates, rxSeq1)
	}
	if client.ackNext.Load() != 2 || landing.ackNext.Load() != 1 {
		t.Fatalf("final directional ACK frontiers: client=%d landing=%d, want 2/1", client.ackNext.Load(), landing.ackNext.Load())
	}
	deadline = time.Now().Add(2 * time.Second)
	for (client.txLedger.Snapshot(time.Now()).OutstandingFrames != 0 || landing.txLedger.Snapshot(time.Now()).OutstandingFrames != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if client.txLedger.Snapshot(time.Now()).OutstandingFrames != 0 || landing.txLedger.Snapshot(time.Now()).OutstandingFrames != 0 {
		t.Fatal("TX ledgers did not clear after application delivery and ACK")
	}
	clientRX, landingRX := client.Snapshot(), landing.Snapshot()
	if clientRX.RxPendingFrames != 0 || clientRX.RxPendingBytes != 0 || landingRX.RxPendingFrames != 0 || landingRX.RxPendingBytes != 0 {
		t.Fatalf("RX pending remains: client=%d/%d landing=%d/%d", clientRX.RxPendingFrames, clientRX.RxPendingBytes, landingRX.RxPendingFrames, landingRX.RxPendingBytes)
	}
	t.Logf("Phase35 application-path run_id=%s session_id=%s carrier=loopback_tcp forward_bytes=%d reverse_bytes=%d reverse_retry_sequence=1 retry_target=leg0/generation1 retry_accepted_bytes=%d",
		trace.runID, trace.sessionID, len(forwardPayload), len(reversePayload), retryWrite.Event.CarrierAcceptedPayloadBytes)
}

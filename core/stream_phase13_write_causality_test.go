package smp3core

import (
	"io"
	"sync"
	"testing"
	"time"
)

const streamRecoveryTraceLimit = 32768

type v27RecoveryTraceMode string

const (
	v27RecoveryTraceModeFull     = v27RecoveryTraceMode("full")
	v27RecoveryTraceModeLite     = v27RecoveryTraceMode("lite")
	v27RecoveryTraceModeDecision = v27RecoveryTraceMode("decision")
)

func v27ResolveRecoveryTraceMode(modeEnv, legacyEnv string) (v27RecoveryTraceMode, bool) {
	mode := v27RecoveryTraceMode(modeEnv)
	if mode == "" {
		if legacyEnv == "1" {
			mode = v27RecoveryTraceModeFull
		} else {
			mode = "off"
		}
	}
	switch mode {
	case "off", v27RecoveryTraceModeFull, v27RecoveryTraceModeLite, v27RecoveryTraceModeDecision:
		return mode, true
	default:
		return mode, false
	}
}

// v27RecoveryCausalityTrace provides one total event order across Core hooks
// and the blackhole fixture's actual net.Conn.Write boundary. ElapsedNS is
// sampled from time.Now's monotonic clock while holding the same mutex that
// assigns EventIndex, so the exported ordering is explicit and bounded.
type v27RecoveryCausalityTrace struct {
	mu      sync.Mutex
	origin  time.Time
	next    int
	count   int
	seq     uint64
	dropped uint64
	events  []streamRecoveryTraceEvent
	mode    v27RecoveryTraceMode
}

func newV27RecoveryCausalityTrace() *v27RecoveryCausalityTrace {
	return newV27RecoveryCausalityTraceMode(v27RecoveryTraceModeFull)
}

func newV27RecoveryCausalityTraceMode(mode v27RecoveryTraceMode) *v27RecoveryCausalityTrace {
	return &v27RecoveryCausalityTrace{origin: time.Now(), events: make([]streamRecoveryTraceEvent, streamRecoveryTraceLimit), mode: mode}
}

func v27RecoveryTraceLiteCaptures(kind string) bool {
	switch kind {
	case "probe_prewrite_timeout", "probe_timeout_decision", "probe_timeout_action",
		"probe_unique_service_low_decision", "probe_unique_service_low_action",
		"send_queue_accepted", "send_queue_rejected", "send_queue_aborted",
		"rescue_queue_accepted", "rescue_queue_rejected", "rescue_queue_aborted",
		"writer_dequeue", "recovery_probe_write_started", "core_data_write_dispatch",
		"carrier_data_write_enter", "carrier_data_write_return",
		"carrier_socket_write_start",
		"tx_mark_write_started", "tx_mark_attempt_sent_commit",
		"core_data_write_enter", "core_data_write_return",
		"carrier_conn_write_enter", "carrier_conn_write_return", "carrier_conn_write_suppressed":
		return true
	default:
		return false
	}
}

func v27RecoveryTraceDecisionCaptures(kind string) bool {
	switch kind {
	case "probe_timeout_decision", "probe_timeout_action",
		"probe_unique_service_low_decision", "probe_unique_service_low_action":
		return true
	default:
		return false
	}
}

func (r *v27RecoveryCausalityTrace) record(event streamRecoveryTraceEvent) uint64 {
	if r == nil {
		return 0
	}
	// Sparse diagnostic mode keeps only recovery decisions and writer/queue
	// boundaries. Filter before taking the recorder mutex or sampling a clock.
	if r.mode == v27RecoveryTraceModeLite && !v27RecoveryTraceLiteCaptures(event.Kind) {
		return 0
	}
	if r.mode == v27RecoveryTraceModeDecision && !v27RecoveryTraceDecisionCaptures(event.Kind) {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	event.EventIndex = r.seq
	event.ObservedAt = time.Now()
	event.ElapsedNS = event.ObservedAt.Sub(r.origin).Nanoseconds()
	if r.count < len(r.events) {
		index := (r.next + r.count) % len(r.events)
		r.events[index] = event
		r.count++
	} else {
		r.events[r.next] = event
		r.next = (r.next + 1) % len(r.events)
		r.dropped++
	}
	return event.EventIndex
}

func (r *v27RecoveryCausalityTrace) snapshot() ([]streamRecoveryTraceEvent, uint64, uint64) {
	if r == nil {
		return nil, 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]streamRecoveryTraceEvent, r.count)
	for i := range result {
		result[i] = r.events[(r.next+i)%len(r.events)]
	}
	return result, r.seq, r.dropped
}

func TestPhase13FirstWriteAtLedgerFallbackUsesTransitStart(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("frontier"), base)
	if !ledger.MarkTransit(record, 1, base) {
		t.Fatal("MarkTransit failed")
	}
	// The ledger-only path models a successful commit whose caller did not
	// report the actual carrier write start. MarkAttemptSent's compatibility
	// fallback intentionally uses the earlier transit claim timestamp.
	commitAt := base.Add(5 * time.Second)
	if result := ledger.MarkAttemptSent(record, 1, false, commitAt); !result.Applied {
		t.Fatal("MarkAttemptSent failed")
	}
	if !record.firstWriteAt.Equal(base) {
		t.Fatalf("ledger fallback firstWriteAt=%s, want transit start %s", record.firstWriteAt, base)
	}
	if record.firstWriteAt.Equal(commitAt) {
		t.Fatal("ledger fallback unexpectedly represents successful-write commit time")
	}
}

func TestPhase13FirstWriteAtPrefersObservedCarrierWriteStart(t *testing.T) {
	base := time.Unix(1_800_000_100, 0)
	ledger := NewStreamTXLedger()
	record := ledger.Add([]byte("frontier"), base)
	if !ledger.MarkTransit(record, 1, base) {
		t.Fatal("MarkTransit failed")
	}
	writeStart := base.Add(5 * time.Second)
	ledger.markWriteStarted(record, 1, false, writeStart)
	commitAt := writeStart.Add(5 * time.Second)
	if result := ledger.MarkAttemptSent(record, 1, false, commitAt); !result.Applied {
		t.Fatal("MarkAttemptSent failed")
	}
	if !record.firstWriteAt.Equal(writeStart) {
		t.Fatalf("Core writer firstWriteAt=%s, want observed write start %s", record.firstWriteAt, writeStart)
	}
	if record.firstWriteAt.Equal(base) || record.firstWriteAt.Equal(commitAt) {
		t.Fatal("Core writer did not preserve the actual observed write-start timestamp")
	}
}

func TestPhase13RecoveryTraceUsesOneMonotonicEventOrder(t *testing.T) {
	trace := newV27RecoveryCausalityTrace()
	ledger := NewStreamTXLedger()
	ledger.recoveryTraceHook = trace.record
	base := time.Now()
	record := ledger.Add([]byte("trace"), base)
	if !ledger.MarkTransit(record, 0, base) {
		t.Fatal("MarkTransit failed")
	}
	writeStart := base.Add(time.Millisecond)
	ledger.markWriteStarted(record, 0, false, writeStart)
	if result := ledger.MarkAttemptSent(record, 0, false, writeStart.Add(time.Millisecond)); !result.Applied {
		t.Fatal("MarkAttemptSent failed")
	}
	events, count, dropped := trace.snapshot()
	if count != 3 || dropped != 0 || len(events) != 3 {
		t.Fatalf("trace count=%d dropped=%d events=%d, want 3/0/3", count, dropped, len(events))
	}
	wantKinds := []string{"tx_mark_transit", "tx_mark_write_started", "tx_mark_attempt_sent_commit"}
	for i, event := range events {
		if event.EventIndex != uint64(i+1) || event.Kind != wantKinds[i] {
			t.Fatalf("event[%d]=index:%d kind:%s, want index:%d kind:%s", i, event.EventIndex, event.Kind, i+1, wantKinds[i])
		}
		if i > 0 && event.ElapsedNS < events[i-1].ElapsedNS {
			t.Fatalf("event timestamps regressed: %d then %d", events[i-1].ElapsedNS, event.ElapsedNS)
		}
	}
}

func TestPhase13RecoveryTraceLiteCapturesOnlyDecisionAndWriterEvents(t *testing.T) {
	trace := newV27RecoveryCausalityTraceMode(v27RecoveryTraceModeLite)
	kinds := []string{
		"tx_mark_transit",
		"send_queue_rejected",
		"probe_timeout_decision",
		"core_data_write_enter",
		"tx_mark_write_started",
		"carrier_socket_write_start",
		"carrier_conn_write_return",
		"tx_mark_rescue_queued",
		"probe_unique_service_low_action",
	}
	for _, kind := range kinds {
		trace.record(streamRecoveryTraceEvent{Kind: kind})
	}
	events, count, dropped := trace.snapshot()
	wantKinds := []string{
		"send_queue_rejected",
		"probe_timeout_decision",
		"core_data_write_enter",
		"tx_mark_write_started",
		"carrier_socket_write_start",
		"carrier_conn_write_return",
		"probe_unique_service_low_action",
	}
	if count != uint64(len(wantKinds)) || dropped != 0 || len(events) != len(wantKinds) {
		t.Fatalf("lite trace count=%d dropped=%d events=%d, want %d/0/%d", count, dropped, len(events), len(wantKinds), len(wantKinds))
	}
	for i, event := range events {
		if event.Kind != wantKinds[i] || event.EventIndex != uint64(i+1) {
			t.Fatalf("lite event[%d]=(%s,%d), want (%s,%d)", i, event.Kind, event.EventIndex, wantKinds[i], i+1)
		}
	}
}

func TestPhase13RecoveryTraceDecisionCapturesOnlyRecoveryDecisions(t *testing.T) {
	trace := newV27RecoveryCausalityTraceMode(v27RecoveryTraceModeDecision)
	kinds := []string{
		"send_queue_rejected",
		"probe_timeout_decision",
		"core_data_write_enter",
		"probe_timeout_action",
		"carrier_conn_write_return",
		"probe_unique_service_low_decision",
		"probe_unique_service_low_action",
	}
	for _, kind := range kinds {
		trace.record(streamRecoveryTraceEvent{Kind: kind})
	}
	events, count, dropped := trace.snapshot()
	wantKinds := []string{
		"probe_timeout_decision",
		"probe_timeout_action",
		"probe_unique_service_low_decision",
		"probe_unique_service_low_action",
	}
	if count != uint64(len(wantKinds)) || dropped != 0 || len(events) != len(wantKinds) {
		t.Fatalf("decision trace count=%d dropped=%d events=%d, want %d/0/%d", count, dropped, len(events), len(wantKinds), len(wantKinds))
	}
	for i, event := range events {
		if event.Kind != wantKinds[i] || event.EventIndex != uint64(i+1) {
			t.Fatalf("decision trace event[%d]=(%s,%d), want (%s,%d)", i, event.Kind, event.EventIndex, wantKinds[i], i+1)
		}
	}
}

func TestPhase13RecoveryTraceModeResolution(t *testing.T) {
	for _, test := range []struct {
		name     string
		modeEnv  string
		legacy   string
		wantMode v27RecoveryTraceMode
		wantOK   bool
	}{
		{name: "default off", wantMode: "off", wantOK: true},
		{name: "legacy full", legacy: "1", wantMode: v27RecoveryTraceModeFull, wantOK: true},
		{name: "explicit full", modeEnv: "full", legacy: "0", wantMode: v27RecoveryTraceModeFull, wantOK: true},
		{name: "explicit lite", modeEnv: "lite", wantMode: v27RecoveryTraceModeLite, wantOK: true},
		{name: "explicit decision", modeEnv: "decision", wantMode: v27RecoveryTraceModeDecision, wantOK: true},
		{name: "explicit off overrides legacy", modeEnv: "off", legacy: "1", wantMode: "off", wantOK: true},
		{name: "unknown mode rejected", modeEnv: "minimal", wantMode: "minimal", wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotMode, gotOK := v27ResolveRecoveryTraceMode(test.modeEnv, test.legacy)
			if gotMode != test.wantMode || gotOK != test.wantOK {
				t.Fatalf("resolve(%q,%q)=(%q,%v), want (%q,%v)", test.modeEnv, test.legacy, gotMode, gotOK, test.wantMode, test.wantOK)
			}
		})
	}
}

type phase13GatedWriterLeg struct {
	controlEntered chan struct{}
	dataEntered    chan struct{}
	releaseControl chan struct{}
	releaseOnce    sync.Once
	controlOnce    sync.Once
	dataOnce       sync.Once
}

func newPhase13GatedWriterLeg() *phase13GatedWriterLeg {
	return &phase13GatedWriterLeg{
		controlEntered: make(chan struct{}),
		dataEntered:    make(chan struct{}),
		releaseControl: make(chan struct{}),
	}
}

func (l *phase13GatedWriterLeg) Read([]byte) (int, error) { return 0, io.EOF }

func (l *phase13GatedWriterLeg) Write(p []byte) (int, error) {
	if len(p) == frameHeaderSize {
		switch StreamFrameType(p[0]) {
		case StreamFrameACK:
			l.controlOnce.Do(func() { close(l.controlEntered) })
			<-l.releaseControl
		case StreamFrameData:
			l.dataOnce.Do(func() { close(l.dataEntered) })
		}
	}
	return len(p), nil
}

func (l *phase13GatedWriterLeg) Close() error {
	l.releaseOnce.Do(func() { close(l.releaseControl) })
	return nil
}

func TestPhase13WriterControlBlockConsumesUnsentFrontierBudget(t *testing.T) {
	c := slowLegUnitEngine()
	c.cfg.RetransmitTimeout = time.Second
	c.done = make(chan struct{})
	trace := newV27RecoveryCausalityTrace()
	c.cfg.recoveryTraceHook = trace.record
	c.txLedger.recoveryTraceHook = trace.record

	gate := newPhase13GatedWriterLeg()
	leg := &streamLeg{
		id:         1,
		generation: 1,
		conn:       gate,
		send:       make(chan txSendAttempt, 1),
		rescue:     make(chan txSendAttempt, 1),
		control:    make(chan legControl, 1),
		ackWake:    make(chan struct{}, 1),
		done:       make(chan struct{}),
		retired:    make(chan struct{}),
	}
	c.legs[1] = leg
	c.legGeneration[1].Store(1)
	base := time.Now().Add(-time.Second)
	c.admission.legs[1] = streamAdmissionLeg{
		restricted: true, probePending: true, probePendingAt: base,
		since: base, limit: 1, byteLimit: uint64(c.cfg.ChunkSize),
		reason: "probe_pending_write",
	}
	leg.control <- legControl{typ: frameTypeAck, value: 1}

	writerExited := make(chan struct{})
	go func() {
		defer close(writerExited)
		c.legWriteLoop(leg)
	}()
	cleanup := func() {
		_ = gate.Close()
		leg.close(io.ErrClosedPipe)
		close(c.done)
		select {
		case <-writerExited:
		case <-time.After(time.Second):
			t.Error("writer did not exit after releasing the deterministic gate")
		}
	}
	defer cleanup()

	select {
	case <-gate.controlEntered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter the gated control Write")
	}

	record := c.txLedger.Add(make([]byte, c.cfg.ChunkSize), base)
	if !c.txLedger.assignOrdinary(record, 1, base) {
		t.Fatal("failed to assign recovery frontier")
	}
	c.admission.legs[1].probeSequence = record.Sequence()
	c.admission.legs[1].probeSequenceSet = true
	c.txLedger.setOrigin(record, 1, base)
	if !c.tryQueue(leg, record, false) {
		t.Fatal("frontier was not accepted into the blocked writer's send queue")
	}
	if got := len(leg.send); got != 1 {
		t.Fatalf("queued frontier count=%d, want 1 while control Write is blocked", got)
	}

	decisionAt := record.frontierAt.Add(streamProbeBudget + time.Nanosecond)
	candidate := c.txLedger.FrontierCandidate(decisionAt, c.cfg.RetransmitTimeout)
	candidate = c.protectFrontier(candidate, decisionAt)
	state := c.admission.legs[1]
	if !candidate.overdue || !state.restricted || state.recovering || state.probePending || state.reason != "probe_write_start_timeout" {
		t.Fatalf("blocked unsent frontier did not expire at the existing budget: candidate=%+v state=%+v", candidate, c.admission.legs[1])
	}
	ledger := c.txLedger.probeReferenceSnapshot(record, true)
	if !ledger.FirstWriteAt.IsZero() || !ledger.InTransit || len(leg.send) != 1 {
		t.Fatalf("fixture did not hold DATA before writer start: ledger=%+v queued=%d", ledger, len(leg.send))
	}
	select {
	case <-gate.dataEntered:
		t.Fatal("DATA Write began before releasing the control-frame gate")
	default:
	}

	events, _, dropped := trace.snapshot()
	if dropped != 0 {
		t.Fatalf("trace dropped %d events", dropped)
	}
	var decision *streamRecoveryTraceEvent
	for i := range events {
		if events[i].Kind == "probe_prewrite_timeout" && events[i].RecordSequence == record.Sequence() {
			decision = &events[i]
			break
		}
	}
	if decision == nil || !decision.FirstWriteAt.IsZero() || decision.Age < streamProbeBudget {
		t.Fatalf("missing pre-write timeout decision before DATA Write: %+v", decision)
	}

	gate.releaseOnce.Do(func() { close(gate.releaseControl) })
	select {
	case <-gate.dataEntered:
	case <-time.After(time.Second):
		t.Fatal("DATA writer did not resume after the control-frame gate was released")
	}
	ledger = c.txLedger.probeReferenceSnapshot(record, true)
	if ledger.FirstWriteAt.IsZero() {
		t.Fatal("frontier writer start was not recorded after releasing the gate")
	}
	state = c.admission.legs[1]
	if !state.restricted || state.recovering || state.reason != "probe_write_start_timeout" {
		t.Fatalf("late DATA Write incorrectly started recovery after the independent pre-write timeout: %+v", state)
	}
	t.Logf("frontier=%d send_queue_waited_while_control_write_blocked=true prewrite_timeout=true late_data_write_not_recovery=true age=%s budget=%s firstWriteAt=%s", record.Sequence(), decision.Age, decision.Budget, ledger.FirstWriteAt)
}

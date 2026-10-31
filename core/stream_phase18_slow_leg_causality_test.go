package smp3core

import (
	"encoding/hex"
	"encoding/json"
	mathrand "math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// streamPhase18RecoveryTraceRecorder gives opt-in Core events one shared index
// across both endpoints. It keeps the state, decision, writer, rescue and ACK
// events needed for sequence-level causality, while counting omitted event
// kinds so a bounded event filter cannot be mistaken for a complete trace.
type streamPhase18RecoveryTraceRecorder struct {
	mu       sync.Mutex
	started  time.Time
	next     uint64
	limit    int
	dropped  uint64
	events   []streamRecoveryTraceEvent
	filtered map[string]uint64
}

const streamPhase18RecoveryTraceEventLimit = 250000

func newStreamPhase18RecoveryTraceRecorder(started time.Time) *streamPhase18RecoveryTraceRecorder {
	return &streamPhase18RecoveryTraceRecorder{
		started:  started,
		limit:    streamPhase18RecoveryTraceEventLimit,
		filtered: make(map[string]uint64),
	}
}

func streamPhase18TraceKindNeeded(kind string) bool {
	switch kind {
	case "tx_ordinary_assigned", "tx_mark_transit", "tx_mark_rescue_transit",
		"tx_mark_restricted_rescue_transit", "tx_mark_rescue_queued",
		"tx_mark_attempt_sent_commit", "tx_mark_write_started", "send_queue_accepted",
		"send_queue_rejected", "writer_dequeue", "core_data_write_enter",
		"core_data_write_dispatch", "core_data_write_return", "rescue_queue_accepted",
		"rescue_queue_rejected", "frontier_rescue_decision", "slow_leg_frontier_decision",
		"ack_frontier_advanced", "recovery_probe_write_started", "probe_prewrite_timeout",
		"probe_timeout_decision", "probe_timeout_action", "probe_unique_service_low_decision",
		"probe_unique_service_low_action", "rx_data_received", "rx_contiguous_delivery",
		"rx_app_pipe_write_return", "phase19_policy_a_shadow_evaluation",
		"phase19_policy_a_rescue_suppressed":
		return true
	default:
		return false
	}
}

func (r *streamPhase18RecoveryTraceRecorder) record(event streamRecoveryTraceEvent) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now()
	}
	event.EventIndex = r.next
	event.ElapsedNS = event.ObservedAt.Sub(r.started).Nanoseconds()
	if !streamPhase18TraceKindNeeded(event.Kind) {
		r.filtered[event.Kind]++
		return event.EventIndex
	}
	if len(r.events) >= r.limit {
		r.dropped++
		return event.EventIndex
	}
	r.events = append(r.events, event)
	return event.EventIndex
}

func (r *streamPhase18RecoveryTraceRecorder) snapshot() []streamRecoveryTraceEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]streamRecoveryTraceEvent(nil), r.events...)
}

func (r *streamPhase18RecoveryTraceRecorder) counts() (uint64, map[string]uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	filtered := make(map[string]uint64, len(r.filtered))
	for kind, count := range r.filtered {
		filtered[kind] = count
	}
	return r.next, filtered
}

func (r *streamPhase18RecoveryTraceRecorder) droppedEvents() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

func TestStreamPhase18TraceRecorderKeepsCausalEventsAndCountsFiltering(t *testing.T) {
	started := time.Now()
	recorder := newStreamPhase18RecoveryTraceRecorder(started)
	first := recorder.record(streamRecoveryTraceEvent{Kind: "slow_leg_frontier_decision", ObservedAt: started.Add(time.Millisecond)})
	second := recorder.record(streamRecoveryTraceEvent{Kind: "unrelated_telemetry_event", ObservedAt: started.Add(2 * time.Millisecond)})
	third := recorder.record(streamRecoveryTraceEvent{Kind: "core_data_write_dispatch", ObservedAt: started.Add(3 * time.Millisecond)})
	fourth := recorder.record(streamRecoveryTraceEvent{Kind: "rx_contiguous_delivery", ObservedAt: started.Add(4 * time.Millisecond)})
	events := recorder.snapshot()
	total, filtered := recorder.counts()
	if first != 1 || second != 2 || third != 3 || fourth != 4 {
		t.Fatalf("trace hook event indexes = %d, %d, %d, %d; want 1, 2, 3, 4", first, second, third, fourth)
	}
	if total != 4 || filtered["unrelated_telemetry_event"] != 1 {
		t.Fatalf("trace counts = total %d filtered %+v", total, filtered)
	}
	if len(events) != 3 || events[0].EventIndex != 1 || events[1].EventIndex != 3 || events[2].EventIndex != 4 {
		t.Fatalf("retained causal events = %+v", events)
	}
}

func TestStreamPhase18TraceRecorderBoundsRetainedEventsAndCountsDrops(t *testing.T) {
	started := time.Now()
	recorder := newStreamPhase18RecoveryTraceRecorder(started)
	recorder.limit = 1
	recorder.record(streamRecoveryTraceEvent{Kind: "frontier_rescue_decision", ObservedAt: started.Add(time.Millisecond)})
	recorder.record(streamRecoveryTraceEvent{Kind: "rx_contiguous_delivery", ObservedAt: started.Add(2 * time.Millisecond)})
	total, filtered := recorder.counts()
	if total != 2 || len(recorder.snapshot()) != 1 || recorder.droppedEvents() != 1 {
		t.Fatalf("bounded trace total=%d retained=%d dropped=%d", total, len(recorder.snapshot()), recorder.droppedEvents())
	}
	if filtered["rx_contiguous_delivery"] != 0 {
		t.Fatalf("buffer overflow should not be classified as kind filtering: %+v", filtered)
	}
}

// TestStreamPhase18HealthySlowLegCausality is an opt-in diagnostic. It uses
// the shared Core over calibrated virtual serialization and loopback TCP; it
// is not a physical Carrier or WAN measurement. The strict zero-rescue gate
// remains in force. Any retry is logged with decision and frame evidence and
// then fails this test rather than being reclassified as a pass.
func TestStreamPhase18HealthySlowLegCausality(t *testing.T) {
	if os.Getenv("SMP3_PHASE18_SLOW_LEG") != "1" {
		t.Skip("set SMP3_PHASE18_SLOW_LEG=1 to run the isolated healthy 100/1 Mbps low-leg causality diagnostic")
	}

	sourceSHA := os.Getenv("SMP3_PHASE18_CORE_SOURCE_SHA256")
	if len(sourceSHA) != 64 {
		t.Fatal("SMP3_PHASE18_CORE_SOURCE_SHA256 must contain the frozen Core Go source tree SHA-256")
	}
	if _, err := hex.DecodeString(sourceSHA); err != nil {
		t.Fatalf("invalid SMP3_PHASE18_CORE_SOURCE_SHA256: %v", err)
	}
	binarySHA, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	bytes := 128 << 20
	if raw := os.Getenv("SMP3_PHASE18_BYTES"); raw != "" {
		bytes, err = strconv.Atoi(raw)
		if err != nil || bytes < 16<<20 || bytes > 256<<20 {
			t.Fatalf("SMP3_PHASE18_BYTES must be in [16777216,268435456], got %q", raw)
		}
	}
	seed := int64(20261025)
	if raw := os.Getenv("SMP3_PHASE18_ORDER_SEED"); raw != "" {
		seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SMP3_PHASE18_ORDER_SEED %q: %v", raw, err)
		}
	}
	timeout := 120 * time.Second
	if raw := os.Getenv("SMP3_PHASE18_SAMPLE_TIMEOUT_MS"); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms < 1000 || ms > 15*60*1000 {
			t.Fatalf("SMP3_PHASE18_SAMPLE_TIMEOUT_MS must be in [1000,900000], got %q", raw)
		}
		timeout = time.Duration(ms) * time.Millisecond
	}

	binary, _ := json.Marshal(map[string]any{
		"phase":              "18 healthy slow-leg rescue causality diagnostic",
		"source_core_sha256": sourceSHA, "test_binary_sha256": binarySHA,
		"git_head": os.Getenv("SMP3_PHASE18_GIT_HEAD"), "git_branch": os.Getenv("SMP3_PHASE18_BRANCH"),
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"gomaxprocs": runtime.GOMAXPROCS(0), "race_instrumented": os.Getenv("SMP3_PHASE18_RACE") == "1",
		"carrier":        "p0cABCarrier over loopback TCP sockets with virtual serialization and 20ms RTT; NOT physical Carrier/WAN",
		"bandwidth_mbps": []int64{100, 1}, "payload_bytes": bytes, "sample_timeout_ms": timeout.Milliseconds(),
		"capacity_mode": "dynamic", "scheduler_mode": "aggregation", "chunk_bytes": 64 << 10,
		"queue_frames": 256, "max_inflight_frames": 1024, "max_reorder_frames": 4096,
		"ack_interval_ms": 20, "retransmit_timeout_ms": 1500,
		"path_aware_modes":   []string{"off", "ready using synthetic RTT measured by virtual Carrier echoes"},
		"trace_modes":        []string{"carrier frame trace and opt-in Core recovery trace both off", "both on"},
		"randomization_seed": seed, "strict_zero_rescue_gate": true,
	})
	t.Logf("SMP3_PHASE18_IDENTITY %s", binary)

	type mode struct {
		name      string
		pathAware bool
		waitRTT   bool
		trace     bool
	}
	modes := []mode{
		{name: "path_aware_off_trace_off"},
		{name: "path_aware_off_trace_on", trace: true},
		{name: "path_aware_ready_trace_off", pathAware: true, waitRTT: true},
		{name: "path_aware_ready_trace_on", pathAware: true, waitRTT: true, trace: true},
	}
	rng := mathrand.New(mathrand.NewSource(seed))
	rng.Shuffle(len(modes), func(i, j int) { modes[i], modes[j] = modes[j], modes[i] })
	modeNames := make([]string, len(modes))
	for i := range modes {
		modeNames[i] = modes[i].name
	}
	encodedOrder, _ := json.Marshal(modeNames)
	t.Logf("SMP3_PHASE18_ORDER %s", encodedOrder)

	gateFailures := 0
	for index, selected := range modes {
		qoeMode := streamPathQOEMode{name: selected.name, pathAware: selected.pathAware, waitForRTT: selected.waitRTT}
		job := streamPathQOEJob{
			rtt:              [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond},
			carrierRatesMbps: [2]int64{100, 1}, size: bytes, run: index + 1,
			mode: qoeMode, orderSeed: seed, orderIndex: index + 1,
			blockID:     "healthy_slow_leg_100_1/loopback_tcp/" + strconv.Itoa(index+1),
			traceFrames: selected.trace, captureCoreRecoveryTrace: selected.trace,
			collectCoreTelemetry: true, tcpCarrier: true, dynamicCapacity: true, timeout: timeout,
		}
		sample := runStreamPathQoESample(t, job)
		retryBytes := sample.CoreTxRetryWrittenBytesByLeg[0] + sample.CoreTxRetryWrittenBytesByLeg[1]
		rescueBytes := sample.CoreRescueAttemptBytesByLeg[0] + sample.CoreRescueAttemptBytesByLeg[1]
		retransmitFrames := sample.CoreDataTxRetransmitFramesByLeg[0] + sample.CoreDataTxRetransmitFramesByLeg[1]
		duplicateBytes := sample.ReceiverDuplicateBytesByLeg[0] + sample.ReceiverDuplicateBytesByLeg[1]
		gatePass := sample.Pass && sample.SentBytesByLeg[1] > 0 && retryBytes == 0 && rescueBytes == 0 && retransmitFrames == 0 && duplicateBytes == 0
		line, marshalErr := json.Marshal(map[string]any{
			"source_core_sha256": sourceSHA, "test_binary_sha256": binarySHA,
			"mode": selected.name, "order_index": index + 1, "randomization_seed": seed,
			"path_aware": selected.pathAware, "core_and_carrier_trace": selected.trace,
			"strict_zero_retry_gate": gatePass, "core_retry_bytes": retryBytes, "core_rescue_bytes": rescueBytes,
			"core_retransmit_frames": retransmitFrames, "receiver_duplicate_bytes": duplicateBytes,
			"sample": sample,
		})
		if marshalErr != nil {
			t.Errorf("encode sample %s: %v", selected.name, marshalErr)
		} else {
			t.Logf("SMP3_PHASE18_SAMPLE %s", line)
		}
		if !sample.Pass {
			t.Errorf("transfer correctness failed for %s: %s", selected.name, sample.Error)
		}
		if sample.SentBytesByLeg[1] == 0 {
			t.Errorf("the healthy 1 Mbps leg was not exercised in %s", selected.name)
		}
		if retryBytes != 0 || rescueBytes != 0 || retransmitFrames != 0 || duplicateBytes != 0 {
			gateFailures++
			t.Errorf("strict no-fault zero-retry gate failed for %s: retry=%d rescue=%d retransmit_frames=%d rx_duplicate=%d",
				selected.name, retryBytes, rescueBytes, retransmitFrames, duplicateBytes)
		}
	}
	t.Logf("SMP3_PHASE18_GATE_SUMMARY samples=%d strict_zero_retry_failures=%d race_instrumented=%t", len(modes), gateFailures, os.Getenv("SMP3_PHASE18_RACE") == "1")
}

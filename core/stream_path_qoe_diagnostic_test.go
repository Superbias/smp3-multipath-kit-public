package smp3core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type streamPathQoESample struct {
	Scenario                                    string                     `json:"scenario"`
	CarrierRatesMbps                            [2]int64                   `json:"carrier_rates_mbps"`
	TraceCaptureEnabled                         bool                       `json:"trace_capture_enabled"`
	CoreTelemetryEnabled                        bool                       `json:"core_telemetry_enabled"`
	CarrierModel                                string                     `json:"carrier_model"`
	TCPRetransmissionEvidence                   string                     `json:"tcp_retransmission_evidence"`
	TCPInfoBefore                               map[string]any             `json:"tcp_info_before,omitempty"`
	TCPInfoAfter                                map[string]any             `json:"tcp_info_after,omitempty"`
	Mode                                        string                     `json:"mode"`
	RTTLeg0MS                                   int                        `json:"rtt_leg0_ms"`
	RTTLeg1MS                                   int                        `json:"rtt_leg1_ms"`
	Bytes                                       int                        `json:"bytes"`
	Run                                         int                        `json:"run"`
	OrderSeed                                   int64                      `json:"order_seed"`
	OrderIndex                                  int                        `json:"order_index"`
	BlockID                                     string                     `json:"block_id"`
	FeedbackStateAtStart                        string                     `json:"feedback_state_at_start"`
	ActiveLegs                                  int                        `json:"active_legs"`
	SetupFeedbackWaitMS                         float64                    `json:"setup_feedback_wait_ms"`
	FeedbackReadyAtStart                        bool                       `json:"feedback_ready_at_start"`
	FeedbackRTTMS                               [2]float64                 `json:"feedback_rtt_ms"`
	PathPenaltyScore                            [2]float64                 `json:"path_penalty_score"`
	EngineActiveAtStart                         bool                       `json:"engine_active_at_start"`
	ServerFirstRequestByteMS                    float64                    `json:"server_first_request_byte_ms,omitempty"`
	RequestUploadEOFAndAckDrainMS               float64                    `json:"request_upload_eof_and_ack_drain_ms,omitempty"`
	ClientFirstResponseTTFBMS                   float64                    `json:"client_first_response_ttfb_ms,omitempty"`
	ServerRequestCompleteMS                     float64                    `json:"server_request_complete_ms,omitempty"`
	ServerFirstResponseWriteMS                  float64                    `json:"server_first_response_write_ms,omitempty"`
	ClientResponseEOFMS                         float64                    `json:"client_response_eof_from_request_start_ms,omitempty"`
	ResponseEOFToBothDirectionAckDrainMS        float64                    `json:"response_eof_to_both_direction_ack_drain_ms,omitempty"`
	RequestResponseCompletionMS                 float64                    `json:"request_start_to_response_eof_and_ack_drain_ms,omitempty"`
	SenderSHA256                                string                     `json:"sender_sha256"`
	ReceiverSHA256                              string                     `json:"receiver_sha256"`
	SenderBytes                                 int                        `json:"sender_bytes"`
	ReceiverBytes                               int                        `json:"receiver_bytes"`
	UniqueAckBytes                              uint64                     `json:"request_unique_ack_bytes"`
	ResponseSHA256                              string                     `json:"response_sha256,omitempty"`
	ResponseReceiverSHA256                      string                     `json:"response_receiver_sha256,omitempty"`
	ResponseBytes                               int                        `json:"response_bytes,omitempty"`
	ResponseReceivedBytes                       int                        `json:"response_received_bytes,omitempty"`
	ResponseUniqueAckBytes                      uint64                     `json:"response_unique_ack_bytes"`
	OutstandingFrames                           int                        `json:"outstanding_frames"`
	OutstandingBytes                            uint64                     `json:"outstanding_bytes"`
	RxPendingFrames                             int                        `json:"rx_pending_frames"`
	RxPendingBytes                              uint64                     `json:"rx_pending_bytes"`
	ResponseOutstandingFrames                   int                        `json:"response_outstanding_frames"`
	ResponseOutstandingBytes                    uint64                     `json:"response_outstanding_bytes"`
	ResponseRxPendingFrames                     int                        `json:"response_rx_pending_frames"`
	ResponseRxPendingBytes                      uint64                     `json:"response_rx_pending_bytes"`
	SentBytesByLeg                              [2]uint64                  `json:"sent_bytes_by_leg"`
	CoreTxRetryWrittenBytesByLeg                [2]uint64                  `json:"core_tx_retry_written_bytes_by_leg"`
	CoreDataTxAttemptFramesByLeg                [2]uint64                  `json:"core_data_tx_attempt_frames_by_leg"`
	CoreDataTxRetransmitFramesByLeg             [2]uint64                  `json:"core_data_tx_retransmit_frames_by_leg"`
	CoreRescueAttemptFramesByLeg                [2]uint64                  `json:"core_rescue_attempt_frames_by_leg"`
	CoreRescueAttemptBytesByLeg                 [2]uint64                  `json:"core_rescue_attempt_bytes_by_leg"`
	CoreCarrierWriteAttemptFramesByLeg          [2]uint64                  `json:"core_carrier_write_attempt_frames_by_leg"`
	CoreCarrierWriteAttemptBytesByLeg           [2]uint64                  `json:"core_carrier_write_attempt_bytes_by_leg"`
	CoreDataTxRetryWriteAttemptFramesByLeg      [2]uint64                  `json:"core_data_tx_retry_write_attempt_frames_by_leg"`
	CoreDataTxRescueWriteAttemptFramesByLeg     [2]uint64                  `json:"core_data_tx_rescue_write_attempt_frames_by_leg"`
	CoreDataTxRescueWriteAttemptBytesByLeg      [2]uint64                  `json:"core_data_tx_rescue_write_attempt_bytes_by_leg"`
	CoreDataTxCarrierWriteErrorFramesByLeg      [2]uint64                  `json:"core_data_tx_carrier_write_error_frames_by_leg"`
	CoreDataTxCarrierWireBytesByLeg             [2]uint64                  `json:"core_data_tx_carrier_wire_bytes_by_leg"`
	ResponseTxRetryWrittenBytesByLeg            [2]uint64                  `json:"response_tx_retry_written_bytes_by_leg,omitempty"`
	ResponseDataTxAttemptFramesByLeg            [2]uint64                  `json:"response_data_tx_attempt_frames_by_leg,omitempty"`
	ResponseDataTxRetransmitFramesByLeg         [2]uint64                  `json:"response_data_tx_retransmit_frames_by_leg,omitempty"`
	ResponseRescueAttemptFramesByLeg            [2]uint64                  `json:"response_rescue_attempt_frames_by_leg,omitempty"`
	ResponseRescueAttemptBytesByLeg             [2]uint64                  `json:"response_rescue_attempt_bytes_by_leg,omitempty"`
	ResponseCarrierWriteAttemptFramesByLeg      [2]uint64                  `json:"response_carrier_write_attempt_frames_by_leg,omitempty"`
	ResponseCarrierWriteAttemptBytesByLeg       [2]uint64                  `json:"response_carrier_write_attempt_bytes_by_leg,omitempty"`
	ResponseRetryWriteAttemptFramesByLeg        [2]uint64                  `json:"response_retry_write_attempt_frames_by_leg,omitempty"`
	ResponseDataTxRescueWriteAttemptFramesByLeg [2]uint64                  `json:"response_data_tx_rescue_write_attempt_frames_by_leg,omitempty"`
	ResponseDataTxRescueWriteAttemptBytesByLeg  [2]uint64                  `json:"response_data_tx_rescue_write_attempt_bytes_by_leg,omitempty"`
	ResponseDataTxCarrierWriteErrorFramesByLeg  [2]uint64                  `json:"response_data_tx_carrier_write_error_frames_by_leg,omitempty"`
	ResponseDataTxCarrierWireBytesByLeg         [2]uint64                  `json:"response_data_tx_carrier_wire_bytes_by_leg,omitempty"`
	BoundedRescueTX                             StreamBoundedRescueStats   `json:"bounded_rescue_tx"`
	BoundedRescueRX                             StreamBoundedRescueStats   `json:"bounded_rescue_rx"`
	CoreRescueEvents                            []streamPathQOERescueEvent `json:"core_rescue_events,omitempty"`
	RescueTelemetryConsistent                   bool                       `json:"rescue_telemetry_consistent,omitempty"`
	RescueTelemetryCheck                        string                     `json:"rescue_telemetry_check,omitempty"`
	ReceiverDuplicateFramesByLeg                [2]uint64                  `json:"receiver_duplicate_frames_by_leg"`
	ReceiverDuplicateBytesByLeg                 [2]uint64                  `json:"receiver_duplicate_bytes_by_leg"`
	ResponseSentBytesByLeg                      [2]uint64                  `json:"response_sent_bytes_by_leg,omitempty"`
	Phase19PolicyARescueSuppressions            uint64                     `json:"phase19_policy_a_rescue_suppressions"`
	CoreTxAckedUsefulByLeg                      [2]uint64                  `json:"core_tx_acked_useful_by_leg"`
	CoreRxUniqueBytesByLeg                      [2]uint64                  `json:"core_rx_unique_bytes_by_leg"`
	ResponseReceiverDuplicateFramesByLeg        [2]uint64                  `json:"response_receiver_duplicate_frames_by_leg,omitempty"`
	ResponseReceiverDuplicateBytesByLeg         [2]uint64                  `json:"response_receiver_duplicate_bytes_by_leg,omitempty"`
	CoreResponseTxAckedUsefulByLeg              [2]uint64                  `json:"core_response_tx_acked_useful_by_leg,omitempty"`
	CoreResponseRxUniqueBytesByLeg              [2]uint64                  `json:"core_response_rx_unique_bytes_by_leg,omitempty"`
	ApplicationReadWaitEvents                   []streamPathQoEAppReadWait `json:"application_read_wait_events,omitempty"`
	ApplicationFirstByteTTFBMS                  float64                    `json:"application_first_byte_ttfb_ms,omitempty"`
	ApplicationCompletionMS                     float64                    `json:"application_completion_ms,omitempty"`
	ApplicationGoodputMbps                      float64                    `json:"application_goodput_mbps,omitempty"`
	CarrierFrameEvents                          []streamPathQoEFrameEvent  `json:"carrier_frame_events,omitempty"`
	CoreRecoveryTraceEnabled                    bool                       `json:"core_recovery_trace_enabled,omitempty"`
	CoreRecoveryTraceStartedAt                  time.Time                  `json:"core_recovery_trace_started_at,omitempty"`
	CoreRecoveryTraceEvents                     []streamRecoveryTraceEvent `json:"core_recovery_trace_events,omitempty"`
	CoreRecoveryTraceEventCount                 uint64                     `json:"core_recovery_trace_event_count,omitempty"`
	CoreRecoveryTraceDropped                    uint64                     `json:"core_recovery_trace_dropped,omitempty"`
	CoreRecoveryTraceFilteredByKind             map[string]uint64          `json:"core_recovery_trace_filtered_by_kind,omitempty"`
	FlowStartedAt                               time.Time                  `json:"flow_started_at,omitempty"`
	SampleTimeoutMS                             int64                      `json:"sample_timeout_ms,omitempty"`
	Termination                                 string                     `json:"termination"`
	Error                                       string                     `json:"error,omitempty"`
	Pass                                        bool                       `json:"pass"`
}

type streamPathQoEAppReadWait struct {
	Endpoint       string  `json:"endpoint"`
	ElapsedMS      float64 `json:"elapsed_ms"`
	WaitMS         float64 `json:"wait_ms"`
	InterarrivalMS float64 `json:"interarrival_ms,omitempty"`
	Bytes          int     `json:"bytes"`
	Termination    string  `json:"termination,omitempty"`
}

type streamPathQOERescueEvent struct {
	Endpoint string            `json:"endpoint"`
	Event    StreamRescueEvent `json:"event"`
}

type streamPathQOETestBlock struct {
	rtt  [2]time.Duration
	size int
	run  int
}

func streamPathQOEBuildJobs(rtts [][2]time.Duration, sizes []int, modes []streamPathQOEMode, repetitions int, seed int64, randomized bool) []streamPathQOEJob {
	blocks := make([]streamPathQOETestBlock, 0, len(rtts)*len(sizes)*repetitions)
	for _, rtt := range rtts {
		for _, size := range sizes {
			for run := 1; run <= repetitions; run++ {
				blocks = append(blocks, streamPathQOETestBlock{rtt: rtt, size: size, run: run})
			}
		}
	}
	rng := mathrand.New(mathrand.NewSource(seed))
	if randomized {
		rng.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
	}
	jobs := make([]streamPathQOEJob, 0, len(blocks)*len(modes))
	order := 0
	for _, block := range blocks {
		blockModes := append([]streamPathQOEMode(nil), modes...)
		if randomized {
			rng.Shuffle(len(blockModes), func(i, j int) { blockModes[i], blockModes[j] = blockModes[j], blockModes[i] })
		}
		blockID := fmt.Sprintf("%d/%d:%d:%d", block.rtt[0].Milliseconds(), block.rtt[1].Milliseconds(), block.size, block.run)
		for _, mode := range blockModes {
			order++
			jobs = append(jobs, streamPathQOEJob{
				rtt: block.rtt, size: block.size, run: block.run, mode: mode,
				orderSeed: seed, orderIndex: order, blockID: blockID,
			})
		}
	}
	return jobs
}

func streamPathQOEModeNames(modes []streamPathQOEMode) []string {
	names := make([]string, 0, len(modes))
	for _, mode := range modes {
		names = append(names, mode.name)
	}
	return names
}

func streamPathQOETestBinarySHA256() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	contents, err := os.ReadFile(executable)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:]), nil
}

func TestStreamPathQOERandomizedScheduleIsRepeatableAndPaired(t *testing.T) {
	rtts := [][2]time.Duration{{20 * time.Millisecond, 20 * time.Millisecond}, {20 * time.Millisecond, 100 * time.Millisecond}, {20 * time.Millisecond, 400 * time.Millisecond}}
	sizes := []int{100 << 10}
	modes := []streamPathQOEMode{
		{name: "off"}, {name: "single", singleLeg: true}, {name: "cold", pathAware: true}, {name: "missing", pathAware: true, holdRTTEcho: true}, {name: "ready", pathAware: true, waitForRTT: true},
	}
	first := streamPathQOEBuildJobs(rtts, sizes, modes, 30, 20261022, true)
	second := streamPathQOEBuildJobs(rtts, sizes, modes, 30, 20261022, true)
	if len(first) != 3*30*len(modes) || len(second) != len(first) {
		t.Fatalf("unexpected randomized schedule sizes: %d/%d", len(first), len(second))
	}
	seenBlocks := make(map[string]map[string]bool)
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("same seed produced different job at order %d", i+1)
		}
		if first[i].orderIndex != i+1 || first[i].orderSeed != 20261022 {
			t.Fatalf("job order metadata mismatch at %d: %+v", i+1, first[i])
		}
		if seenBlocks[first[i].blockID] == nil {
			seenBlocks[first[i].blockID] = make(map[string]bool)
		}
		if seenBlocks[first[i].blockID][first[i].mode.name] {
			t.Fatalf("duplicate mode in paired block %s: %s", first[i].blockID, first[i].mode.name)
		}
		seenBlocks[first[i].blockID][first[i].mode.name] = true
	}
	if len(seenBlocks) != 3*30 {
		t.Fatalf("got %d paired blocks, want 90", len(seenBlocks))
	}
	for block, got := range seenBlocks {
		if len(got) != len(modes) {
			t.Fatalf("block %s has %d modes, want %d", block, len(got), len(modes))
		}
		for _, mode := range modes {
			if !got[mode.name] {
				t.Fatalf("block %s missing mode %s", block, mode.name)
			}
		}
	}
	otherSeed := streamPathQOEBuildJobs(rtts, sizes, modes, 30, 7, true)
	orderChanged := false
	for i := range first {
		if first[i].blockID != otherSeed[i].blockID || first[i].mode.name != otherSeed[i].mode.name {
			orderChanged = true
			break
		}
	}
	if !orderChanged {
		t.Fatal("different seed did not change the job order")
	}
}

// streamPathQOEFrameGate suppresses only the selected complete PATH_RTT message
// test fixture. DATA, ACK, CLOSE, and PATH_RTT Probe writes retain the Carrier
// service model; this isolates the no-feedback scheduler fallback.
type streamPathQOEFrameGate struct {
	net.Conn
	writeMu  sync.Mutex
	mu       sync.Mutex
	held     [][]byte
	released bool
	changed  chan struct{}
	kind     StreamRTTMessageKind
}

func newStreamPathQOEEchoGate(conn net.Conn) *streamPathQOEFrameGate {
	return newStreamPathQOEFrameGate(conn, StreamRTTEcho)
}

func newStreamPathQOEProbeGate(conn net.Conn) *streamPathQOEFrameGate {
	return newStreamPathQOEFrameGate(conn, StreamRTTProbe)
}

func newStreamPathQOEFrameGate(conn net.Conn, kind StreamRTTMessageKind) *streamPathQOEFrameGate {
	return &streamPathQOEFrameGate{Conn: conn, changed: make(chan struct{}, 1), kind: kind}
}

func (g *streamPathQOEFrameGate) Write(p []byte) (int, error) {
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	if len(p) == StreamFrameHeaderSize+StreamRTTMessageSize && p[0] == byte(StreamFramePathRTT) && StreamRTTMessageKind(p[StreamFrameHeaderSize+9]) == g.kind {
		g.mu.Lock()
		if !g.released {
			g.held = append(g.held, append([]byte(nil), p...))
			g.mu.Unlock()
			select {
			case g.changed <- struct{}{}:
			default:
			}
			return len(p), nil
		}
		g.mu.Unlock()
	}
	return g.Conn.Write(p)
}

func (g *streamPathQOEFrameGate) heldCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
}

func (g *streamPathQOEFrameGate) release() error {
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	g.mu.Lock()
	g.released = true
	held := g.held
	g.held = nil
	g.mu.Unlock()
	for _, frame := range held {
		if err := writeAll(g.Conn, frame); err != nil {
			return err
		}
	}
	return nil
}

func (g *streamPathQOEFrameGate) waitForHeld(count int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for g.heldCount() < count {
		select {
		case <-g.changed:
		case <-timer.C:
			return g.heldCount() >= count
		}
	}
	return true
}

type streamPathQOEMode struct {
	name                string
	pathAware           bool
	waitForRTT          bool
	seedHandshakeRTT    bool
	holdRTTEcho         bool
	singleLeg           bool
	boundedRescue       *StreamBoundedRescueConfig
	captureRescueEvents bool
}

type streamPathQOEJob struct {
	rtt                      [2]time.Duration
	carrierRatesMbps         [2]int64
	size                     int
	run                      int
	mode                     streamPathQOEMode
	orderSeed                int64
	orderIndex               int
	blockID                  string
	requestResponse          bool
	traceFrames              bool
	captureCoreRecoveryTrace bool
	collectCoreTelemetry     bool
	tcpCarrier               bool
	dynamicCapacity          bool
	timeout                  time.Duration
}

// TestStreamPathQoEDiagnostic is opt-in. It measures the actual shared Core
// with a calibrated virtual Carrier; it is not a Native/Standalone or WAN test.
func TestStreamPathQoEDiagnostic(t *testing.T) {
	if os.Getenv("SMP3_PATH_QOE_DIAGNOSTIC") != "1" {
		t.Skip("set SMP3_PATH_QOE_DIAGNOSTIC=1 to run virtual-Carrier path QoE diagnostics")
	}
	repetitions := 5
	rtts := [][2]time.Duration{
		{20 * time.Millisecond, 20 * time.Millisecond},
		{20 * time.Millisecond, 100 * time.Millisecond},
		{20 * time.Millisecond, 400 * time.Millisecond},
	}
	sizes := []int{10 << 10, 100 << 10, 1 << 20}
	modes := []streamPathQOEMode{
		{name: "baseline", pathAware: false},
		{name: "path_aware_cold", pathAware: true},
		{name: "path_aware_feedback_missing", pathAware: true, holdRTTEcho: true},
		{name: "path_aware_configured_rtt_seed_diagnostic", pathAware: true, seedHandshakeRTT: true},
		{name: "path_aware_feedback_ready", pathAware: true, waitForRTT: true},
	}
	formal := os.Getenv("SMP3_PATH_QOE_FORMAL") == "1"
	var orderSeed int64
	if formal {
		if os.Getenv("SMP3_PATH_QOE_ONLY") != "" {
			t.Fatal("SMP3_PATH_QOE_ONLY is not allowed in formal mode; it would remove paired samples")
		}
		seedText := os.Getenv("SMP3_PATH_QOE_ORDER_SEED")
		if seedText == "" {
			t.Fatal("formal mode requires SMP3_PATH_QOE_ORDER_SEED")
		}
		var err error
		orderSeed, err = strconv.ParseInt(seedText, 10, 64)
		if err != nil {
			t.Fatalf("parse SMP3_PATH_QOE_ORDER_SEED: %v", err)
		}
		if sourceDigest := os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256"); len(sourceDigest) != sha256.Size*2 {
			t.Fatal("formal mode requires SMP3_PATH_QOE_SOURCE_SHA256 with the frozen source-tree SHA-256")
		} else if _, err := hex.DecodeString(sourceDigest); err != nil {
			t.Fatalf("parse SMP3_PATH_QOE_SOURCE_SHA256: %v", err)
		}
		repetitions = 30
		sizes = []int{100 << 10}
		modes = []streamPathQOEMode{
			{name: "baseline_path_aware_off"},
			{name: "healthy_single_leg_reference", singleLeg: true},
			{name: "path_aware_cold_attachment_no_feedback", pathAware: true},
			{name: "path_aware_feedback_missing", pathAware: true, holdRTTEcho: true},
			{name: "path_aware_feedback_ready", pathAware: true, waitForRTT: true},
		}
	}
	jobs := streamPathQOEBuildJobs(rtts, sizes, modes, repetitions, orderSeed, formal)
	filter := os.Getenv("SMP3_PATH_QOE_ONLY")
	if filter != "" {
		filtered := jobs[:0]
		for _, job := range jobs {
			if filter == fmt.Sprintf("%d/%d:%d:%s", job.rtt[0].Milliseconds(), job.rtt[1].Milliseconds(), job.size, job.mode.name) {
				filtered = append(filtered, job)
			}
		}
		jobs = filtered
		if len(jobs) == 0 {
			t.Fatalf("SMP3_PATH_QOE_ONLY %q did not match any configured job", filter)
		}
	}
	binaryDigest := ""
	if formal {
		var err error
		binaryDigest, err = streamPathQOETestBinarySHA256()
		if err != nil {
			t.Fatalf("hash executing test binary: %v", err)
		}
	}
	results := make([]streamPathQoESample, 0, len(jobs))
	for _, job := range jobs {
		sample := runStreamPathQoESample(t, job)
		sample.OrderSeed = job.orderSeed
		sample.OrderIndex = job.orderIndex
		sample.BlockID = job.blockID
		encodedSample, err := json.Marshal(sample)
		if err != nil {
			t.Fatalf("encode sample at order %d: %v", job.orderIndex, err)
		}
		t.Logf("SMP3_PATH_QOE_SAMPLE %s", encodedSample)
		results = append(results, sample)
		if !sample.Pass {
			t.Fatalf("virtual Carrier correctness failed: %+v", sample)
		}
	}
	scope := "shared Core + calibrated virtual Carrier; no Native/Standalone process or WAN"
	var identity any
	identity = nil
	if formal {
		scope += "; formal mode is a randomized paired Core diagnostic, not a production or WAN claim"
		identity = map[string]any{
			"source_sha256":         os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256"),
			"test_binary_sha256":    binaryDigest,
			"go_version":            runtime.Version(),
			"goos":                  runtime.GOOS,
			"goarch":                runtime.GOARCH,
			"gomaxprocs":            runtime.GOMAXPROCS(0),
			"data_frame_bytes":      64 << 10,
			"carrier_mbps":          []int{100, 100},
			"ack_interval_ms":       20,
			"retransmit_timeout_ms": 1500,
			"queue_frames":          256,
			"max_inflight_frames":   1024,
			"max_reorder_frames":    4096,
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"scope":                           scope,
		"formal_randomized_paired_design": formal,
		"order_seed":                      orderSeed,
		"repetitions_per_cell":            repetitions,
		"rtt_ms":                          "20/20,20/100,20/400",
		"sizes_bytes":                     sizes,
		"modes":                           streamPathQOEModeNames(modes),
		"percentile_method":               fmt.Sprintf("nearest-rank; p95/p99 with n=%d per cell", repetitions),
		"identity_and_configuration":      identity,
		"results":                         results,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SMP3_PATH_QOE_DIAGNOSTIC %s", encoded)
	for _, rtt := range rtts {
		for _, size := range sizes {
			for _, mode := range modes {
				var firstRequestByte, uploadCompletion []float64
				for _, sample := range results {
					if sample.RTTLeg0MS == int(rtt[0].Milliseconds()) && sample.RTTLeg1MS == int(rtt[1].Milliseconds()) && sample.Bytes == size && sample.Mode == mode.name {
						firstRequestByte = append(firstRequestByte, sample.ServerFirstRequestByteMS)
						uploadCompletion = append(uploadCompletion, sample.RequestUploadEOFAndAckDrainMS)
					}
				}
				t.Logf("SMP3_PATH_QOE_UPLOAD_SUMMARY mode=%s rtt_ms=%d/%d bytes=%d server_first_request_byte_ms=%v request_upload_eof_and_ack_drain_ms=%v",
					mode.name, rtt[0].Milliseconds(), rtt[1].Milliseconds(), size,
					streamPathQoEPercentiles(firstRequestByte), streamPathQoEPercentiles(uploadCompletion))
			}
		}
	}
}

// TestPhase22BoundedRescueQoETreatmentSample is an opt-in, single-process
// request/response sample used by a runner that randomizes paired B0/B1 order.
// It always uses the same test binary; only the explicit per-engine config differs.
func TestPhase22BoundedRescueQoETreatmentSample(t *testing.T) {
	if os.Getenv("SMP3_PHASE22_QOE_TREATMENT") != "1" {
		t.Skip("set SMP3_PHASE22_QOE_TREATMENT=1 for a Phase 22 B0/B1 QoE sample")
	}
	treatment := os.Getenv("SMP3_PHASE22_TREATMENT")
	if treatment != "B0" && treatment != "B1" {
		t.Fatalf("SMP3_PHASE22_TREATMENT must be B0 or B1, got %q", treatment)
	}
	parseInt := func(name string) int {
		t.Helper()
		raw := os.Getenv(name)
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			t.Fatalf("invalid %s=%q", name, raw)
		}
		return value
	}
	sourceSHA := os.Getenv("SMP3_PHASE22_SOURCE_SHA256")
	if len(sourceSHA) != sha256.Size*2 {
		t.Fatal("SMP3_PHASE22_SOURCE_SHA256 must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(sourceSHA); err != nil {
		t.Fatalf("parse SMP3_PHASE22_SOURCE_SHA256: %v", err)
	}
	pairID := os.Getenv("SMP3_PHASE22_PAIR_ID")
	if pairID == "" {
		t.Fatal("SMP3_PHASE22_PAIR_ID is required")
	}
	run := parseInt("SMP3_PHASE22_RUN")
	order := parseInt("SMP3_PHASE22_ORDER")
	rtt0, rtt1 := parseInt("SMP3_PHASE22_RTT0_MS"), parseInt("SMP3_PHASE22_RTT1_MS")
	rate0, rate1 := parseInt("SMP3_PHASE22_RATE0_MBPS"), parseInt("SMP3_PHASE22_RATE1_MBPS")
	bytes := parseInt("SMP3_PHASE22_BYTES")
	basisPoints, burstBytes := uint64(0), uint64(0)
	if raw := os.Getenv("SMP3_PHASE22_BUDGET_BASIS_POINTS"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 16)
		if err != nil {
			t.Fatalf("parse SMP3_PHASE22_BUDGET_BASIS_POINTS: %v", err)
		}
		basisPoints = parsed
	}
	if raw := os.Getenv("SMP3_PHASE22_BURST_BYTES"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse SMP3_PHASE22_BURST_BYTES: %v", err)
		}
		burstBytes = parsed
	}
	var bounded *StreamBoundedRescueConfig
	if treatment == "B0" {
		if basisPoints != 0 || burstBytes != 0 {
			t.Fatal("B0 must not receive a bounded-rescue budget")
		}
	} else {
		if !validBoundedRescueBasisPoints(uint16(basisPoints)) {
			t.Fatalf("unsupported bounded-rescue budget basis points %d", basisPoints)
		}
		if burstBytes > 64<<10 {
			t.Fatalf("burst allowance exceeds one 64KiB DATA chunk: %d", burstBytes)
		}
		bounded = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: uint16(basisPoints), BurstBytes: burstBytes}
	}
	binarySHA, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	captureRescueEvents := os.Getenv("SMP3_PHASE22_CAPTURE_RESCUE_EVENTS") == "1"
	job := streamPathQOEJob{
		rtt:              [2]time.Duration{time.Duration(rtt0) * time.Millisecond, time.Duration(rtt1) * time.Millisecond},
		carrierRatesMbps: [2]int64{int64(rate0), int64(rate1)},
		size:             bytes, run: run,
		mode:      streamPathQOEMode{name: "phase22_" + treatment, boundedRescue: bounded, captureRescueEvents: captureRescueEvents},
		orderSeed: 20261026, orderIndex: order, blockID: pairID,
		requestResponse: true, collectCoreTelemetry: true, tcpCarrier: true, dynamicCapacity: true,
		timeout: 120 * time.Second,
	}
	sample := runStreamPathQoESample(t, job)
	sample.OrderIndex = order
	sample.BlockID = pairID
	identity := map[string]any{
		"phase": "22", "treatment": treatment, "pair_id": pairID, "order": order,
		"source_sha256": sourceSHA, "test_binary_sha256": binarySHA,
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"gomaxprocs": runtime.GOMAXPROCS(0), "carrier": "actual loopback TCP with virtual serialization/delay; not WAN",
		"rates_mbps": []int{rate0, rate1}, "rtt_ms": []int{rtt0, rtt1}, "bytes_each_direction": bytes,
		"chunk_bytes": 64 << 10, "queue_frames": 256, "max_inflight_frames": 1024,
		"ack_interval_ms": 20, "retransmit_timeout_ms": 1500,
		"bounded_rescue": bounded, "capture_rescue_events": captureRescueEvents,
	}
	encoded, err := json.Marshal(map[string]any{"identity": identity, "sample": sample})
	if err != nil {
		t.Fatalf("encode Phase 22 treatment sample: %v", err)
	}
	t.Logf("SMP3_PHASE22_TREATMENT_SAMPLE %s", encoded)
	if !sample.Pass {
		t.Fatalf("Phase 22 treatment sample failed data/EOF/ACK/ledger correctness: %+v", sample)
	}
	if treatment == "B1" && (!sample.BoundedRescueTX.Enabled || sample.BoundedRescueTX.ConfigError != "") {
		t.Fatalf("B1 bounded rescue was not active: %+v", sample.BoundedRescueTX)
	}
	if treatment == "B0" && sample.BoundedRescueTX.Enabled {
		t.Fatalf("B0 unexpectedly enabled bounded rescue: %+v", sample.BoundedRescueTX)
	}
}

func runStreamPathQoESample(t *testing.T, job streamPathQOEJob) (result streamPathQoESample) {
	t.Helper()
	result = streamPathQoESample{
		Scenario:             "request_upload_only",
		TraceCaptureEnabled:  job.traceFrames,
		CoreTelemetryEnabled: job.collectCoreTelemetry,
		Mode:                 job.mode.name, RTTLeg0MS: int(job.rtt[0].Milliseconds()), RTTLeg1MS: int(job.rtt[1].Milliseconds()), Bytes: job.size, Run: job.run,
		OrderSeed: job.orderSeed, OrderIndex: job.orderIndex, BlockID: job.blockID,
	}
	var coreTrace *streamPhase18RecoveryTraceRecorder
	if job.captureCoreRecoveryTrace {
		result.CoreRecoveryTraceEnabled = true
		result.CoreRecoveryTraceStartedAt = time.Now()
		coreTrace = newStreamPhase18RecoveryTraceRecorder(result.CoreRecoveryTraceStartedAt)
		defer func() {
			result.CoreRecoveryTraceEvents = coreTrace.snapshot()
			result.CoreRecoveryTraceEventCount, result.CoreRecoveryTraceFilteredByKind = coreTrace.counts()
			result.CoreRecoveryTraceDropped = coreTrace.droppedEvents()
		}()
	}
	if job.tcpCarrier {
		result.CarrierModel = "p0cABCarrier over actual 127.0.0.1 TCP sockets; virtual serialization and one-way delay"
		if runtime.GOOS == "linux" {
			result.TCPRetransmissionEvidence = "Linux TCP_INFO snapshots captured on each loopback socket"
		} else {
			result.TCPRetransmissionEvidence = "NOT_AVAILABLE_ON_THIS_PLATFORM: loopback TCP socket counters are Linux-only"
		}
	} else {
		result.CarrierModel = "p0cABCarrier over net.Pipe; virtual serialization and one-way delay"
		result.TCPRetransmissionEvidence = "NOT_AVAILABLE: net.Pipe is not a TCP socket"
	}
	if job.requestResponse {
		result.Scenario = "request_response"
	}
	rateMbps := job.carrierRatesMbps
	for leg := range rateMbps {
		if rateMbps[leg] <= 0 {
			rateMbps[leg] = 100
		}
	}
	var sessionID SessionID
	sessionID[0] = byte(job.run)
	sessionID[1] = byte(job.size)
	sessionID[2] = byte(job.rtt[1].Milliseconds())
	sessionID[3] = byte(job.orderIndex)
	capacityMode := StreamCapacityFixed
	if job.dynamicCapacity {
		capacityMode = StreamCapacityDynamic
	}
	clientCfg := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: capacityMode,
		ChunkSize: 64 << 10, QueueFrames: 256, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
		AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond,
		ThresholdBytesPS: 0, ExperimentalPathAware: job.mode.pathAware, PathRole: StreamPathRoleClient,
		BandwidthMbps: []uint32{uint32(rateMbps[0]), uint32(rateMbps[1])},
	}
	if job.mode.boundedRescue != nil {
		clientCfg.ExperimentalBoundedRescue = job.mode.boundedRescue
	}
	var rescueEventsMu sync.Mutex
	var rescueEvents []streamPathQOERescueEvent
	rescueHook := func(endpoint string) func(StreamRescueEvent) {
		return func(event StreamRescueEvent) {
			rescueEventsMu.Lock()
			rescueEvents = append(rescueEvents, streamPathQOERescueEvent{Endpoint: endpoint, Event: event})
			rescueEventsMu.Unlock()
		}
	}
	if job.mode.captureRescueEvents {
		clientCfg.rescueEventHook = rescueHook("client")
	}
	if job.collectCoreTelemetry {
		clientCfg.Telemetry = &StreamTelemetry{}
	}
	serverCfg := clientCfg
	serverCfg.PathRole = StreamPathRoleServer
	if job.mode.captureRescueEvents {
		serverCfg.rescueEventHook = rescueHook("server")
	}
	if job.collectCoreTelemetry {
		serverCfg.Telemetry = &StreamTelemetry{}
	}
	if coreTrace != nil {
		clientCfg.recoveryTraceHook = coreTrace.record
		serverCfg.recoveryTraceHook = coreTrace.record
	}
	defer func() {
		rescueEventsMu.Lock()
		result.CoreRescueEvents = append([]streamPathQOERescueEvent(nil), rescueEvents...)
		rescueEventsMu.Unlock()
		if job.mode.captureRescueEvents {
			result.RescueTelemetryConsistent, result.RescueTelemetryCheck = streamPathQoERescueTelemetryCheck(result, job)
			result.Pass = result.Pass && result.RescueTelemetryConsistent
		}
	}()
	client, clientApp := NewStreamEngine(clientCfg)
	server, serverApp := NewStreamEngine(serverCfg)
	var outbound, inbound [2]*p0cABCarrier
	var clientEchoGate, serverEchoGate [2]*streamPathQOEFrameGate
	var frameRecorder *streamPathQoEFrameRecorder
	if job.traceFrames {
		frameRecorder = &streamPathQoEFrameRecorder{}
	}
	legCount := 2
	if job.mode.singleLeg {
		legCount = 1
	}
	result.ActiveLegs = legCount
	for leg := 0; leg < legCount; leg++ {
		clock := &p0cABClock{changed: make(chan struct{})}
		clock.rate.Store(rateMbps[leg] * 1_000_000)
		result.CarrierRatesMbps[leg] = rateMbps[leg]
		if job.tcpCarrier {
			outbound[leg], inbound[leg] = p0cABTCPPair(t, clock, job.rtt[leg])
		} else {
			outbound[leg], inbound[leg] = p0cABPair(clock, job.rtt[leg])
		}
		if frameRecorder != nil {
			outbound[leg].enableFrameTrace("client", "client_to_server", leg, frameRecorder)
			inbound[leg].enableFrameTrace("server", "server_to_client", leg, frameRecorder)
		}
		if job.mode.pathAware {
			clientAuth, serverAuth := testPathAuthPair(t, sessionID, LegID(leg), byte(job.run+leg+1))
			if job.mode.seedHandshakeRTT {
				// This controlled diagnostic supplies the configured virtual Carrier
				// RTT as the measured handshake sample. It characterizes Core response
				// to a fresh seed; it is not a real host or WAN RTT measurement.
				var err error
				clientAuth, err = clientAuth.WithCarrierHandshakeRTT(job.rtt[leg])
				if err != nil {
					_ = client.Close()
					_ = server.Close()
					t.Fatalf("seed virtual handshake RTT leg%d: %v", leg, err)
				}
			}
			var clientTransport net.Conn = outbound[leg]
			var serverTransport net.Conn = inbound[leg]
			if frameRecorder != nil {
				clientTransport = newStreamPathQoETraceConn(clientTransport, leg, "client_to_server", frameRecorder)
				serverTransport = newStreamPathQoETraceConn(serverTransport, leg, "server_to_client", frameRecorder)
			}
			if job.mode.holdRTTEcho {
				clientEchoGate[leg] = newStreamPathQOEEchoGate(clientTransport)
				serverEchoGate[leg] = newStreamPathQOEEchoGate(serverTransport)
				clientTransport = clientEchoGate[leg]
				serverTransport = serverEchoGate[leg]
			}
			clientCarrier := net.Conn(&p0cdDataFrameCoalescer{Conn: clientTransport})
			serverCarrier := net.Conn(&p0cdDataFrameCoalescer{Conn: serverTransport})
			if err := client.AttachLegWithPathAuth(LegID(leg), clientCarrier, nil, clientAuth); err != nil {
				_ = client.Close()
				_ = server.Close()
				t.Fatalf("attach client leg%d: %v", leg, err)
			}
			if err := server.AttachLegWithPathAuth(LegID(leg), serverCarrier, nil, serverAuth); err != nil {
				_ = client.Close()
				_ = server.Close()
				t.Fatalf("attach server leg%d: %v", leg, err)
			}
		} else {
			var clientTransport net.Conn = outbound[leg]
			var serverTransport net.Conn = inbound[leg]
			if frameRecorder != nil {
				clientTransport = newStreamPathQoETraceConn(clientTransport, leg, "client_to_server", frameRecorder)
				serverTransport = newStreamPathQoETraceConn(serverTransport, leg, "server_to_client", frameRecorder)
			}
			if err := client.AttachLeg(LegID(leg), net.Conn(&p0cdDataFrameCoalescer{Conn: clientTransport}), nil); err != nil {
				_ = client.Close()
				_ = server.Close()
				t.Fatalf("attach client leg%d: %v", leg, err)
			}
			if err := server.AttachLeg(LegID(leg), net.Conn(&p0cdDataFrameCoalescer{Conn: serverTransport}), nil); err != nil {
				_ = client.Close()
				_ = server.Close()
				t.Fatalf("attach server leg%d: %v", leg, err)
			}
		}
	}
	if job.tcpCarrier {
		result.TCPInfoBefore = streamPhase17TCPInfo(outbound, inbound)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
		for leg := 0; leg < 2; leg++ {
			if outbound[leg] != nil {
				_ = outbound[leg].Close()
			}
			if inbound[leg] != nil {
				_ = inbound[leg].Close()
			}
		}
	}()
	if job.mode.holdRTTEcho {
		result.FeedbackStateAtStart = "unavailable"
		warmStarted := time.Now()
		for leg := 0; leg < 2; leg++ {
			if !clientEchoGate[leg].waitForHeld(1, 2*time.Second) || !serverEchoGate[leg].waitForHeld(1, 2*time.Second) {
				result.Error = fmt.Sprintf("leg%d did not produce both withheld RTT echoes", leg)
				result.Termination = "not_started"
				return result
			}
		}
		result.SetupFeedbackWaitMS = float64(time.Since(warmStarted).Microseconds()) / 1000
		for leg := 0; leg < 2; leg++ {
			if _, ok := client.PathFeedback(LegID(leg)); ok {
				result.Error = fmt.Sprintf("client leg%d feedback became ready despite the echo gate", leg)
				result.Termination = "not_started"
				return result
			}
			if _, ok := server.PathFeedback(LegID(leg)); ok {
				result.Error = fmt.Sprintf("server leg%d feedback became ready despite the echo gate", leg)
				result.Termination = "not_started"
				return result
			}
		}
	}
	if job.mode.waitForRTT {
		warmStarted := time.Now()
		deadline := warmStarted.Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ready := true
			for leg := 0; leg < 2; leg++ {
				clientLeg := client.getLeg(uint8(leg))
				if clientLeg == nil {
					ready = false
					break
				}
				if _, ok := client.pathRTTCache.PathFeedback(clientLeg.pathIdentity); !ok {
					ready = false
					break
				}
			}
			if ready {
				result.SetupFeedbackWaitMS = float64(time.Since(warmStarted).Microseconds()) / 1000
				result.FeedbackReadyAtStart = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !result.FeedbackReadyAtStart {
			result.Error = "path feedback did not become ready before 2s"
			result.Termination = "not_started"
			return result
		}
		result.FeedbackStateAtStart = "ready"
	} else if job.mode.pathAware {
		result.FeedbackReadyAtStart = true
		for leg := 0; leg < 2; leg++ {
			clientLeg := client.getLeg(uint8(leg))
			if clientLeg == nil {
				result.FeedbackReadyAtStart = false
				break
			}
			if _, ok := client.pathRTTCache.PathFeedback(clientLeg.pathIdentity); !ok {
				result.FeedbackReadyAtStart = false
				break
			}
		}
		if result.FeedbackReadyAtStart {
			result.FeedbackStateAtStart = "ready_at_cold_start"
		} else if result.FeedbackStateAtStart == "" {
			result.FeedbackStateAtStart = "not_ready_at_cold_start"
		}
	} else {
		result.FeedbackStateAtStart = "path_aware_off"
	}
	result.EngineActiveAtStart = client.Active()
	if job.mode.pathAware {
		for legID := 0; legID < 2; legID++ {
			leg := client.getLeg(uint8(legID))
			if leg == nil {
				continue
			}
			if feedback, ok := client.pathRTTCache.PathFeedback(leg.pathIdentity); ok {
				result.FeedbackRTTMS[legID] = float64(feedback.RTT.Microseconds()) / 1000
				result.PathPenaltyScore[legID] = client.pathRTTScorePenalty(leg)
			}
		}
	}
	if job.requestResponse {
		return runStreamPathQoERequestResponseSample(t, job, result, client, server, clientApp, serverApp, frameRecorder)
	}

	payload := make([]byte, job.size)
	for i := range payload {
		payload[i] = byte((i*31 + 7) % 251)
	}
	sourceHash := sha256.Sum256(payload)
	start := time.Now()
	result.FlowStartedAt = start
	flowTimeout := job.timeout
	if flowTimeout <= 0 {
		flowTimeout = 8 * time.Second
	}
	result.SampleTimeoutMS = flowTimeout.Milliseconds()
	if frameRecorder != nil {
		frameRecorder.setStart(start)
	}
	_ = clientApp.SetDeadline(start.Add(flowTimeout))
	_ = serverApp.SetDeadline(start.Add(flowTimeout))
	type senderResult struct {
		bytes int
		hash  string
		err   error
	}
	type receiverResult struct {
		bytes          int
		hash           string
		firstByteAt    time.Time
		termination    string
		readWaitEvents []streamPathQoEAppReadWait
		err            error
	}
	events := make(chan any, 2)
	go func() {
		written := 0
		for written < len(payload) {
			end := written + 64<<10
			if end > len(payload) {
				end = len(payload)
			}
			n, err := clientApp.Write(payload[written:end])
			written += n
			if err != nil {
				events <- senderResult{bytes: written, hash: hex.EncodeToString(sourceHash[:]), err: err}
				return
			}
			if n == 0 {
				events <- senderResult{bytes: written, hash: hex.EncodeToString(sourceHash[:]), err: io.ErrNoProgress}
				return
			}
		}
		closeErr := clientApp.Close()
		events <- senderResult{bytes: written, hash: hex.EncodeToString(sourceHash[:]), err: closeErr}
	}()
	go func() {
		hash := sha256.New()
		buf := make([]byte, 32<<10)
		var received int
		var firstByteAt, previousReadAt time.Time
		var readWaitEvents []streamPathQoEAppReadWait
		for {
			readStarted := time.Now()
			n, err := serverApp.Read(buf)
			readReturned := time.Now()
			termination := ""
			if errors.Is(err, io.EOF) {
				termination = "io.EOF"
			} else if err != nil {
				termination = err.Error()
			}
			interarrival := 0.0
			if n > 0 && !previousReadAt.IsZero() {
				interarrival = float64(readReturned.Sub(previousReadAt).Microseconds()) / 1000
			}
			readWaitEvents = append(readWaitEvents, streamPathQoEAppReadWait{
				Endpoint: "server_request_reader", ElapsedMS: float64(readReturned.Sub(start).Microseconds()) / 1000,
				WaitMS: float64(readReturned.Sub(readStarted).Microseconds()) / 1000, InterarrivalMS: interarrival, Bytes: n, Termination: termination,
			})
			if n > 0 {
				previousReadAt = readReturned
				if firstByteAt.IsZero() {
					firstByteAt = time.Now()
				}
				_, _ = hash.Write(buf[:n])
				received += n
			}
			if err != nil {
				termination := err.Error()
				if errors.Is(err, io.EOF) {
					termination = "io.EOF"
				}
				events <- receiverResult{bytes: received, hash: hex.EncodeToString(hash.Sum(nil)), firstByteAt: firstByteAt, termination: termination, readWaitEvents: readWaitEvents, err: func() error {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}()}
				return
			}
		}
	}()

	var sender senderResult
	var receiver receiverResult
	senderDone, receiverDone := false, false
	deadline := time.NewTimer(flowTimeout)
	defer deadline.Stop()
	for !senderDone || !receiverDone {
		select {
		case item := <-events:
			switch value := item.(type) {
			case senderResult:
				sender = value
				senderDone = true
			case receiverResult:
				receiver = value
				receiverDone = true
			}
		case <-deadline.C:
			result.Error = fmt.Sprintf("virtual Carrier flow exceeded %s diagnostic timeout", flowTimeout)
			result.Termination = "timeout"
			return result
		}
	}

	drainDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(drainDeadline) {
		txStats, rxStats := client.Snapshot(), server.Snapshot()
		if txStats.OutstandingFrames == 0 && txStats.OutstandingBytes == 0 && rxStats.RxPendingFrames == 0 && rxStats.RxPendingBytes == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if job.mode.holdRTTEcho {
		for leg := 0; leg < 2; leg++ {
			if _, ok := client.PathFeedback(LegID(leg)); ok {
				result.Error = fmt.Sprintf("client leg%d acquired RTT feedback during the withheld-feedback sample", leg)
				result.Termination = "feedback_gate_failed"
				return result
			}
			if _, ok := server.PathFeedback(LegID(leg)); ok {
				result.Error = fmt.Sprintf("server leg%d acquired RTT feedback during the withheld-feedback sample", leg)
				result.Termination = "feedback_gate_failed"
				return result
			}
		}
	}
	txStats, rxStats := client.Snapshot(), server.Snapshot()
	result.SenderSHA256 = sender.hash
	result.ReceiverSHA256 = receiver.hash
	result.SenderBytes = sender.bytes
	result.ReceiverBytes = receiver.bytes
	result.UniqueAckBytes = txStats.TxAckedUsefulTotal
	result.OutstandingFrames = txStats.OutstandingFrames
	result.OutstandingBytes = txStats.OutstandingBytes
	result.RxPendingFrames = rxStats.RxPendingFrames
	result.RxPendingBytes = rxStats.RxPendingBytes
	result.SentBytesByLeg = txStats.TxWrittenBytesByLeg
	result.CoreTxAckedUsefulByLeg = txStats.TxAckedUsefulByLeg
	result.Phase19PolicyARescueSuppressions = txStats.Phase19PolicyARescueSuppressions
	result.CoreRxUniqueBytesByLeg = rxStats.RxUniqueBytesByLeg
	result.ApplicationReadWaitEvents = receiver.readWaitEvents
	result.CoreTxRetryWrittenBytesByLeg = txStats.TxRetryWrittenBytesByLeg
	result.CoreDataTxAttemptFramesByLeg = txStats.Telemetry.DataTxAttemptFrames
	result.CoreDataTxRetransmitFramesByLeg = txStats.Telemetry.DataTxRetransmitFrames
	result.CoreRescueAttemptFramesByLeg = txStats.Telemetry.RescueAttemptFrames
	result.CoreRescueAttemptBytesByLeg = txStats.Telemetry.RescueAttemptBytes
	result.CoreCarrierWriteAttemptFramesByLeg = txStats.Telemetry.DataTxCarrierWriteAttemptFrames
	result.CoreCarrierWriteAttemptBytesByLeg = txStats.Telemetry.DataTxAttemptBytes
	result.CoreDataTxRetryWriteAttemptFramesByLeg = txStats.Telemetry.DataTxRetryWriteAttemptFrames
	result.CoreDataTxRescueWriteAttemptFramesByLeg = txStats.Telemetry.DataTxRescueWriteAttemptFrames
	result.CoreDataTxRescueWriteAttemptBytesByLeg = txStats.Telemetry.DataTxRescueWriteAttemptBytes
	result.CoreDataTxCarrierWriteErrorFramesByLeg = txStats.Telemetry.DataTxCarrierWriteErrorFrames
	result.CoreDataTxCarrierWireBytesByLeg = txStats.Telemetry.DataTxCarrierWireBytes
	result.ReceiverDuplicateFramesByLeg = rxStats.Telemetry.DataRxDuplicateFrames
	result.ReceiverDuplicateBytesByLeg = rxStats.Telemetry.DataRxDuplicateBytes
	if job.tcpCarrier {
		// Capture final TCP_INFO before Core teardown closes the loopback sockets.
		_ = client.Close()
		_ = server.Close()
		for leg := 0; leg < 2; leg++ {
			if outbound[leg] != nil {
				_ = outbound[leg].Close()
			}
			if inbound[leg] != nil {
				_ = inbound[leg].Close()
			}
		}
		result.TCPInfoAfter = streamPhase17TCPInfo(outbound, inbound)
	}
	result.Termination = receiver.termination
	if !receiver.firstByteAt.IsZero() {
		result.ServerFirstRequestByteMS = float64(receiver.firstByteAt.Sub(start).Microseconds()) / 1000
		result.ApplicationFirstByteTTFBMS = result.ServerFirstRequestByteMS
	}
	if !receiver.firstByteAt.IsZero() {
		result.RequestUploadEOFAndAckDrainMS = float64(time.Since(start).Microseconds()) / 1000
		result.ApplicationCompletionMS = result.RequestUploadEOFAndAckDrainMS
		if result.ApplicationCompletionMS > 0 {
			result.ApplicationGoodputMbps = float64(receiver.bytes*8) / (result.ApplicationCompletionMS * 1000)
		}
	}
	if frameRecorder != nil {
		result.CarrierFrameEvents = frameRecorder.snapshot()
	}
	if sender.err != nil {
		result.Error = sender.err.Error()
	} else if receiver.err != nil {
		result.Error = receiver.err.Error()
	}
	result.Pass = sender.err == nil && receiver.err == nil && receiver.termination == "io.EOF" &&
		sender.bytes == job.size && receiver.bytes == job.size && sender.hash == receiver.hash &&
		result.UniqueAckBytes == uint64(job.size) && result.OutstandingFrames == 0 && result.OutstandingBytes == 0 &&
		result.RxPendingFrames == 0 && result.RxPendingBytes == 0
	return result
}

func streamPathQoEPercentiles(values []float64) map[string]float64 {
	if len(values) == 0 {
		return map[string]float64{}
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	nearestRank := func(percent float64) float64 {
		index := int(percent*float64(len(ordered))+0.999999) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(ordered) {
			index = len(ordered) - 1
		}
		return ordered[index]
	}
	return map[string]float64{
		"n": float64(len(ordered)), "p50": nearestRank(0.50), "p95": nearestRank(0.95), "p99": nearestRank(0.99),
	}
}

package smp3core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const phase27LiveEventLimit = 100_000

type phase27LiveSample struct {
	ElapsedNS           int64                    `json:"elapsed_ns"`
	TxWrittenByLeg      [2]uint64                `json:"tx_written_by_leg"`
	TxRetryWrittenByLeg [2]uint64                `json:"tx_retry_written_by_leg"`
	RxUniqueByLeg       [2]uint64                `json:"rx_unique_by_leg"`
	RxDelivered         uint64                   `json:"rx_delivered"`
	OutstandingFrames   int                      `json:"outstanding_frames"`
	OutstandingBytes    uint64                   `json:"outstanding_bytes"`
	RxPendingFrames     int                      `json:"rx_pending_frames"`
	RxPendingBytes      uint64                   `json:"rx_pending_bytes"`
	AssignmentShare     [2]float64               `json:"assignment_share"`
	AdmissionState      [2]string                `json:"admission_state"`
	RecoveryEpoch       [2]uint64                `json:"recovery_epoch"`
	WriterQueueDepth    [2]int                   `json:"writer_queue_depth"`
	WriterQueueCapacity [2]int                   `json:"writer_queue_capacity"`
	WriterState         [2]int32                 `json:"writer_state"`
	WriterQueueWaitNS   [2]uint64                `json:"writer_queue_wait_ns"`
	WriterBusyNS        [2]uint64                `json:"writer_busy_ns"`
	WriterWriteCount    [2]uint64                `json:"writer_write_count"`
	BoundedRescue       StreamBoundedRescueStats `json:"bounded_rescue"`
}

type phase27CandidateLink struct {
	CandidateID              uint64            `json:"candidate_id"`
	Sequence                 uint64            `json:"sequence"`
	ByteStart                uint64            `json:"byte_start"`
	ByteEnd                  uint64            `json:"byte_end"`
	OwnerLeg                 int16             `json:"owner_leg"`
	TargetLeg                uint8             `json:"target_leg"`
	OriginGeneration         uint64            `json:"origin_generation"`
	TargetGeneration         uint64            `json:"target_generation"`
	RecoveryEpoch            uint64            `json:"recovery_epoch"`
	Cause                    StreamRescueCause `json:"cause"`
	FrontierOverdue          bool              `json:"frontier_overdue"`
	OriginalWriteObserved    bool              `json:"original_write_observed"`
	OriginalWriteInTransit   bool              `json:"original_write_in_transit"`
	OwnerLive                bool              `json:"owner_live"`
	TargetAvailable          bool              `json:"target_available"`
	TargetGenerationMatches  bool              `json:"target_generation_matches"`
	GenerationMatchesOrigin  bool              `json:"generation_matches_origin"`
	RecoveryEpochCurrent     bool              `json:"recovery_epoch_current"`
	Class                    StreamRescueClass `json:"class"`
	Decision                 string            `json:"decision"`
	Reason                   string            `json:"reason"`
	Reserved                 bool              `json:"reserved"`
	FrontierDecisionNS       int64             `json:"frontier_decision_elapsed_ns"`
	OriginalWriteDispatchNS  int64             `json:"original_write_dispatch_elapsed_ns"`
	OriginalWriteReturnNS    int64             `json:"original_write_return_elapsed_ns"`
	CandidateAtNS            int64             `json:"candidate_elapsed_ns"`
	WriteStartNS             int64             `json:"rescue_write_start_elapsed_ns"`
	WriteSettleNS            int64             `json:"rescue_write_settled_elapsed_ns"`
	AckNS                    int64             `json:"ack_elapsed_ns"`
	ApplicationReadElapsedNS int64             `json:"application_read_elapsed_ns"`
	WriteStart               bool              `json:"write_start"`
	WriteSettle              bool              `json:"write_settled"`
	Acked                    bool              `json:"acked"`
	AppRangeOK               bool              `json:"application_range_matches"`
	Complete                 bool              `json:"complete_causal_chain"`
}

type phase27AppReadProgress struct {
	ElapsedNS int64  `json:"elapsed_ns"`
	BytesRead uint64 `json:"bytes_read"`
}

type phase27AppReadResult struct {
	data     []byte
	err      error
	progress []phase27AppReadProgress
}

type phase27LiveResult struct {
	Classification       string                      `json:"classification"`
	Policy               StreamRuntimePolicyIdentity `json:"runtime_policy"`
	PolicyVariant        string                      `json:"policy_variant"`
	SchedulerMode        string                      `json:"scheduler_mode"`
	CapacityMode         string                      `json:"capacity_mode"`
	ConfiguredMbps       [2]uint32                   `json:"configured_mbps"`
	RelayOneWayDelayNS   int64                       `json:"relay_one_way_delay_ns"`
	RelayServiceBitsPS   uint64                      `json:"relay_service_bits_per_second"`
	CalibratedRelayRTTNS int64                       `json:"calibrated_relay_rtt_ns"`
	RelayCalibrationN    int                         `json:"relay_calibration_samples"`
	RelayRTTMeasurement  string                      `json:"relay_rtt_measurement_source"`
	MeasuredUniqueMbps   [2]float64                  `json:"measured_receiver_unique_mbps_by_leg"`
	ChunkBytes           int                         `json:"chunk_bytes"`
	PayloadBytes         int                         `json:"payload_bytes"`
	PayloadSHA256        string                      `json:"payload_sha256"`
	ApplicationBytes     int                         `json:"application_bytes"`
	ApplicationSHA256    string                      `json:"application_sha256"`
	ApplicationEOF       string                      `json:"application_eof"`
	WriteDurationNS      int64                       `json:"write_duration_ns"`
	CompletionDurationNS int64                       `json:"completion_duration_ns"`
	TxWrittenByLeg       [2]uint64                   `json:"tx_written_by_leg"`
	TxRetryWrittenByLeg  [2]uint64                   `json:"tx_retry_written_by_leg"`
	RxUniqueByLeg        [2]uint64                   `json:"rx_unique_by_leg"`
	TxAckedUseful        uint64                      `json:"tx_acked_useful"`
	RxDelivered          uint64                      `json:"rx_delivered"`
	TxOutstandingFrames  int                         `json:"tx_outstanding_frames"`
	TxOutstandingBytes   uint64                      `json:"tx_outstanding_bytes"`
	RxPendingFrames      int                         `json:"rx_pending_frames"`
	RxPendingBytes       uint64                      `json:"rx_pending_bytes"`
	CandidateEvents      []StreamRescueEvent         `json:"candidate_events"`
	CoreTraceEvents      []streamRecoveryTraceEvent  `json:"core_trace_events"`
	CandidateLinks       []phase27CandidateLink      `json:"candidate_links"`
	EventDrops           uint64                      `json:"event_drops"`
	CoreTraceDrops       uint64                      `json:"core_trace_drops"`
	M1CompleteChains     int                         `json:"m1_complete_causal_chains"`
	BoundedRescueFinal   StreamBoundedRescueStats    `json:"bounded_rescue_final"`
	Samples              []phase27LiveSample         `json:"samples"`
	MeasuredRateBasis    string                      `json:"measured_rate_basis"`
	SocketBufferNote     string                      `json:"socket_buffer_note"`
}

func phase27ReadToEOFWithProgress(conn net.Conn, started time.Time) phase27AppReadResult {
	var result phase27AppReadResult
	buffer := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			result.data = append(result.data, buffer[:n]...)
			result.progress = append(result.progress, phase27AppReadProgress{
				ElapsedNS: time.Since(started).Nanoseconds(), BytesRead: uint64(len(result.data)),
			})
		}
		if err != nil {
			result.err = err
			return result
		}
	}
}

func phase27CalibrateRelayRTT(t *testing.T, oneWayDelay time.Duration, bitsPerSecond uint64) time.Duration {
	t.Helper()
	relay, client, server := phase25NewRelayPair(t, oneWayDelay, bitsPerSecond, false)
	defer relay.Close()
	var samples []time.Duration
	for i := 0; i < 7; i++ {
		value := []byte{byte(i + 1)}
		started := time.Now()
		if err := writeAll(client, value); err != nil {
			t.Fatalf("relay RTT calibration request %d: %v", i, err)
		}
		var received [1]byte
		if _, err := io.ReadFull(server, received[:]); err != nil || received[0] != value[0] {
			t.Fatalf("relay RTT calibration server read %d: value=%d err=%v", i, received[0], err)
		}
		if err := writeAll(server, received[:]); err != nil {
			t.Fatalf("relay RTT calibration response %d: %v", i, err)
		}
		if _, err := io.ReadFull(client, received[:]); err != nil || received[0] != value[0] {
			t.Fatalf("relay RTT calibration client read %d: value=%d err=%v", i, received[0], err)
		}
		samples = append(samples, time.Since(started))
	}
	return phase25Percentile(samples, .50)
}

func phase27BuildCandidateLinks(events []StreamRescueEvent, coreTrace []streamRecoveryTraceEvent, appProgress []phase27AppReadProgress, expected, delivered []byte, chunkBytes int, runStarted time.Time) []phase27CandidateLink {
	byCandidate := make(map[uint64]*phase27CandidateLink)
	for _, event := range events {
		if event.CandidateID == 0 {
			continue
		}
		link := byCandidate[event.CandidateID]
		if link == nil {
			link = &phase27CandidateLink{CandidateID: event.CandidateID, Sequence: event.Evidence.Sequence}
			byCandidate[event.CandidateID] = link
		}
		if event.Kind == "rescue_reserved" || event.Kind == "rescue_rejected" {
			link.Sequence = event.Evidence.Sequence
			link.OwnerLeg = event.Evidence.OwnerLeg
			link.TargetLeg = event.Evidence.TargetLeg
			link.OriginGeneration = event.Evidence.OriginGeneration
			link.TargetGeneration = event.Evidence.TargetGeneration
			link.RecoveryEpoch = event.Evidence.RecoveryEpoch
			link.Cause = event.Evidence.Cause
			link.FrontierOverdue = event.Evidence.FrontierOverdue
			link.OriginalWriteObserved = event.Evidence.OriginalWriteObserved
			link.OriginalWriteInTransit = event.Evidence.OriginalWriteInTransit
			link.OwnerLive = event.Evidence.OwnerLive
			link.TargetAvailable = event.Evidence.TargetAvailable
			link.TargetGenerationMatches = event.Evidence.TargetGenerationMatches
			link.GenerationMatchesOrigin = event.Evidence.GenerationMatchesOrigin
			link.RecoveryEpochCurrent = event.Evidence.RecoveryEpochCurrent
			link.Class = event.Class
			link.Decision = event.FinalDecision
			link.Reason = event.FinalDecisionReason
			link.Reserved = event.Kind == "rescue_reserved"
			link.CandidateAtNS = event.At.Sub(runStarted).Nanoseconds()
		}
		if event.Kind == "carrier_write_started" {
			link.WriteStart = true
			link.WriteStartNS = event.At.Sub(runStarted).Nanoseconds()
		}
		if event.Kind == "carrier_write_settled" {
			link.WriteSettle = true
			link.WriteSettleNS = event.At.Sub(runStarted).Nanoseconds()
		}
		if event.Kind == "candidate_ack_outcome" && event.FinalDecision == "data_acked" {
			link.Acked = true
			link.AckNS = event.At.Sub(runStarted).Nanoseconds()
		}
	}
	links := make([]phase27CandidateLink, 0, len(byCandidate))
	for _, link := range byCandidate {
		link.ByteStart = link.Sequence * uint64(chunkBytes)
		link.ByteEnd = link.ByteStart + uint64(chunkBytes)
		if link.ByteEnd > uint64(len(expected)) {
			link.ByteEnd = uint64(len(expected))
		}
		link.AppRangeOK = link.ByteStart < uint64(len(expected)) &&
			link.ByteEnd <= uint64(len(expected)) && link.ByteEnd <= uint64(len(delivered)) &&
			link.ByteStart < link.ByteEnd &&
			bytes.Equal(expected[link.ByteStart:link.ByteEnd], delivered[link.ByteStart:link.ByteEnd])
		for _, progress := range appProgress {
			if progress.BytesRead >= link.ByteEnd {
				link.ApplicationReadElapsedNS = progress.ElapsedNS
				break
			}
		}
		for _, event := range coreTrace {
			if event.RecordSequence != link.Sequence || event.FrameType != "data" {
				continue
			}
			if event.Kind == "frontier_rescue_decision" && event.LegID == link.OwnerLeg {
				link.FrontierDecisionNS = event.ElapsedNS
			}
			if !event.Rescue && event.LegID == link.OwnerLeg {
				switch event.Kind {
				case "core_data_write_dispatch":
					link.OriginalWriteDispatchNS = event.ElapsedNS
				case "core_data_write_return":
					link.OriginalWriteReturnNS = event.ElapsedNS
				}
			}
		}
		link.Complete = link.Reserved && link.Class == StreamRescueHealthyHOL &&
			link.Decision == "admitted" && link.Cause == StreamRescueCauseFrontier && link.FrontierOverdue &&
			link.OriginalWriteObserved && link.OwnerLive && link.TargetAvailable &&
			link.TargetGenerationMatches && link.GenerationMatchesOrigin && link.RecoveryEpochCurrent &&
			link.OriginGeneration != 0 && link.TargetGeneration != 0 && link.RecoveryEpoch != 0 &&
			link.FrontierDecisionNS > 0 &&
			link.OriginalWriteDispatchNS > 0 && link.OriginalWriteReturnNS > 0 &&
			link.WriteStart && link.WriteSettle && link.Acked && link.AppRangeOK &&
			link.ApplicationReadElapsedNS > 0
		links = append(links, *link)
	}
	return links
}

func TestPhase27LiveHealthyHOLCandidateTCP(t *testing.T) {
	const (
		defaultSlowRateBitsPS = uint64(1_000_000)
		defaultPayloadBytes   = 32 << 20
		defaultRelayOneWayMS  = 2
	)
	relayOneWay := time.Duration(defaultRelayOneWayMS) * time.Millisecond
	if value := os.Getenv("SMP3_PHASE27_RELAY_ONEWAY_MS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || (parsed != 2 && parsed != 20 && parsed != 50 && parsed != 100) {
			t.Fatalf("unsupported SMP3_PHASE27_RELAY_ONEWAY_MS %q; supported values are 2, 20, 50, and 100", value)
		}
		relayOneWay = time.Duration(parsed) * time.Millisecond
	}
	slowRateBitsPS := defaultSlowRateBitsPS
	if value := os.Getenv("SMP3_PHASE27_SLOW_RATE_BPS"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || (parsed != 1_000_000 && parsed != 10_000_000 && parsed != 100_000_000) {
			t.Fatalf("unsupported SMP3_PHASE27_SLOW_RATE_BPS %q; supported values are 1000000, 10000000, and 100000000", value)
		}
		slowRateBitsPS = parsed
	}
	payloadBytes := defaultPayloadBytes
	if value := os.Getenv("SMP3_PHASE27_PAYLOAD_BYTES"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || (parsed != 32<<20 && parsed != 128<<20) {
			t.Fatalf("unsupported SMP3_PHASE27_PAYLOAD_BYTES %q; supported values are 33554432 and 134217728", value)
		}
		payloadBytes = parsed
	}
	variant := os.Getenv("SMP3_PHASE27_VARIANT")
	if variant == "" {
		variant = "CR"
	}
	var factorMode boundedRescueFactorMode
	switch variant {
	case "CR":
		factorMode = boundedRescueFactorCR
	case "F":
		factorMode = boundedRescueFactorFull
	default:
		t.Fatalf("unsupported SMP3_PHASE27_VARIANT %q; expected CR or F", variant)
	}

	calibratedRTT := phase27CalibrateRelayRTT(t, relayOneWay, slowRateBitsPS)
	cfg := phase25IdentityConfig()
	cfg.BandwidthMbps = []uint32{100, uint32(slowRateBitsPS / 1_000_000)}
	cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: 300, factorMode: factorMode,
	}
	var eventsMu sync.Mutex
	var events []StreamRescueEvent
	var eventDrops uint64
	var coreTraceMu sync.Mutex
	var coreTrace []streamRecoveryTraceEvent
	var coreTraceDrops uint64
	var runStarted time.Time
	cfg.rescueEventHook = func(event StreamRescueEvent) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		if len(events) >= phase27LiveEventLimit {
			eventDrops++
			return
		}
		events = append(events, event)
	}
	cfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 {
		coreTraceMu.Lock()
		defer coreTraceMu.Unlock()
		if len(coreTrace) >= phase27LiveEventLimit {
			coreTraceDrops++
			return 0
		}
		index := uint64(len(coreTrace) + 1)
		event.EventIndex = index
		if !runStarted.IsZero() && !event.ObservedAt.IsZero() {
			event.ElapsedNS = event.ObservedAt.Sub(runStarted).Nanoseconds()
		}
		coreTrace = append(coreTrace, event)
		return index
	}
	client, clientApp, err := NewStreamEngineForCandidate(cfg, StreamPolicyB1)
	if err != nil {
		t.Fatalf("construct B1-CR client: %v", err)
	}
	serverCfg := cfg
	serverCfg.ExperimentalBoundedRescue = nil
	serverCfg.rescueEventHook = nil
	server, serverApp, err := NewStreamEngineForCandidate(serverCfg, StreamPolicyB0)
	if err != nil {
		_ = client.Close()
		_ = clientApp.Close()
		t.Fatalf("construct B0 receiver: %v", err)
	}
	defer client.Close()
	defer clientApp.Close()
	defer server.Close()
	defer serverApp.Close()

	fastClient, fastServer := phase25LoopbackTCPPair(t)
	if err := client.AttachLeg(0, fastClient, nil); err != nil {
		t.Fatalf("attach direct TCP Leg 0 client: %v", err)
	}
	if err := server.AttachLeg(0, fastServer, nil); err != nil {
		t.Fatalf("attach direct TCP Leg 0 server: %v", err)
	}
	slowRelay, slowClient, slowServer := phase25NewRelayPair(t, relayOneWay, slowRateBitsPS, false)
	defer slowRelay.Close()
	if err := client.AttachLeg(1, slowClient, nil); err != nil {
		t.Fatalf("attach shaped healthy TCP Leg 1 client: %v", err)
	}
	if err := server.AttachLeg(1, slowServer, nil); err != nil {
		t.Fatalf("attach shaped healthy TCP Leg 1 server: %v", err)
	}
	started := time.Now()
	runStarted = started
	// Activate is the public host callback seam; no test-only state, ACK,
	// frontier, restriction, or recovery state is modified.
	client.Activate()
	server.Activate()

	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i*31 + i/251)
	}
	wantSHA := sha256.Sum256(payload)
	serverRead := make(chan phase27AppReadResult, 1)
	clientRead := make(chan phase25TCPReadResult, 1)
	go func() { serverRead <- phase27ReadToEOFWithProgress(serverApp, started) }()
	go func() { clientRead <- phase25ReadToEOF(clientApp) }()

	writeDone := make(chan error, 1)
	go func() { writeDone <- writeAll(clientApp, payload) }()
	samples := make([]phase27LiveSample, 0, 16)
	sampleDone := make(chan struct{})
	sampleStopped := make(chan struct{})
	var stopSamplesOnce sync.Once
	stopSamples := func() {
		stopSamplesOnce.Do(func() {
			close(sampleDone)
			<-sampleStopped
		})
	}
	defer stopSamples()
	go func() {
		defer close(sampleStopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				clientStats := client.TelemetrySnapshot()
				serverStats := server.TelemetrySnapshot()
				sample := phase27LiveSample{
					ElapsedNS: time.Since(started).Nanoseconds(), TxWrittenByLeg: clientStats.TxWrittenBytesByLeg,
					TxRetryWrittenByLeg: clientStats.TxRetryWrittenBytesByLeg,
					RxUniqueByLeg:       serverStats.RxUniqueBytesByLeg, RxDelivered: serverStats.RxDeliveredBytes,
					OutstandingFrames: clientStats.OutstandingFrames, OutstandingBytes: clientStats.OutstandingBytes,
					RxPendingFrames: serverStats.RxPendingFrames, RxPendingBytes: serverStats.RxPendingBytes,
					AssignmentShare: clientStats.CapacityAssignmentShare,
					AdmissionState:  clientStats.OrdinaryAdmissionState,
					RecoveryEpoch:   clientStats.OrdinaryRecoveryEpoch,
					BoundedRescue:   clientStats.BoundedRescue,
				}
				for legID := 0; legID < 2; legID++ {
					if leg := client.getLeg(uint8(legID)); leg != nil {
						sample.WriterQueueDepth[legID] = len(leg.send)
						sample.WriterQueueCapacity[legID] = cap(leg.send)
						sample.WriterState[legID] = leg.debugWriterState.Load()
						sample.WriterQueueWaitNS[legID] = leg.debugQueueWaitNs.Load()
						sample.WriterBusyNS[legID] = leg.debugWriteBusyNs.Load()
						sample.WriterWriteCount[legID] = leg.debugWriteCount.Load()
					}
				}
				samples = append(samples, sample)
			case <-sampleDone:
				return
			}
		}
	}()

	select {
	case err := <-writeDone:
		if err != nil {
			stopSamples()
			t.Fatalf("application write: %v", err)
		}
	case <-time.After(90 * time.Second):
		stopSamples()
		t.Fatal("application writer did not finish within 90 seconds")
	}
	writeDuration := time.Since(started)
	client.StartGracefulClose(io.EOF)
	var received phase27AppReadResult
	select {
	case received = <-serverRead:
	case <-time.After(90 * time.Second):
		stopSamples()
		t.Fatal("server application did not reach EOF within 90 seconds")
	}
	if received.err != io.EOF {
		stopSamples()
		t.Fatalf("server application termination=%T %v, want io.EOF", received.err, received.err)
	}
	select {
	case got := <-clientRead:
		if got.err != io.EOF || len(got.data) != 0 {
			stopSamples()
			t.Fatalf("client peer termination bytes=%d err=%T %v, want empty io.EOF", len(got.data), got.err, got.err)
		}
	case <-time.After(90 * time.Second):
		stopSamples()
		t.Fatal("client application did not receive peer EOF within 90 seconds")
	}
	stopSamples()
	completionDuration := time.Since(started)
	if len(received.data) != len(payload) {
		t.Fatalf("application received %d bytes, want %d", len(received.data), len(payload))
	}
	gotSHA := sha256.Sum256(received.data)
	if gotSHA != wantSHA || !bytes.Equal(received.data, payload) {
		t.Fatalf("application payload SHA/content mismatch: got=%x want=%x", gotSHA, wantSHA)
	}

	tx := client.Snapshot()
	rx := server.Snapshot()
	eventsMu.Lock()
	eventCopy := append([]StreamRescueEvent(nil), events...)
	drops := eventDrops
	eventsMu.Unlock()
	coreTraceMu.Lock()
	coreTraceCopy := append([]streamRecoveryTraceEvent(nil), coreTrace...)
	coreTraceDropped := coreTraceDrops
	coreTraceMu.Unlock()
	links := phase27BuildCandidateLinks(eventCopy, coreTraceCopy, received.progress, payload, received.data, cfg.ChunkSize, started)
	completeChains := 0
	for _, link := range links {
		if link.Complete {
			completeChains++
		}
	}
	measuredUniqueMbps := [2]float64{}
	if elapsedSeconds := completionDuration.Seconds(); elapsedSeconds > 0 {
		for legID, uniqueBytes := range rx.RxUniqueBytesByLeg {
			measuredUniqueMbps[legID] = float64(uniqueBytes*8) / elapsedSeconds / 1_000_000
		}
	}
	result := phase27LiveResult{
		Classification: "REAL_TCP_LOOPBACK_CONTROLLED_RELAY; this is not kernel TCP blackhole or WAN",
		Policy:         client.RuntimePolicyIdentity(), PolicyVariant: cfg.ExperimentalBoundedRescue.factorMode.String(),
		SchedulerMode: "aggregation", CapacityMode: "dynamic",
		ConfiguredMbps: [2]uint32{100, uint32(slowRateBitsPS / 1_000_000)}, RelayOneWayDelayNS: relayOneWay.Nanoseconds(),
		RelayServiceBitsPS: slowRateBitsPS, CalibratedRelayRTTNS: calibratedRTT.Nanoseconds(), RelayCalibrationN: 7,
		RelayRTTMeasurement: "median of seven application byte request/echo round trips on a separate TCP pair using the same user-space relay settings; not a sample from the active SMP3 Carrier",
		MeasuredUniqueMbps:  measuredUniqueMbps,
		ChunkBytes:          cfg.ChunkSize, PayloadBytes: len(payload), PayloadSHA256: toSHA256Hex(wantSHA),
		ApplicationBytes: len(received.data), ApplicationSHA256: toSHA256Hex(gotSHA), ApplicationEOF: "io.EOF",
		WriteDurationNS: writeDuration.Nanoseconds(), CompletionDurationNS: completionDuration.Nanoseconds(),
		TxWrittenByLeg: tx.TxWrittenBytesByLeg, TxRetryWrittenByLeg: tx.TxRetryWrittenBytesByLeg,
		RxUniqueByLeg: rx.RxUniqueBytesByLeg, TxAckedUseful: tx.TxAckedUsefulTotal, RxDelivered: rx.RxDeliveredBytes,
		TxOutstandingFrames: tx.OutstandingFrames, TxOutstandingBytes: tx.OutstandingBytes,
		RxPendingFrames: rx.RxPendingFrames, RxPendingBytes: rx.RxPendingBytes,
		CandidateEvents: eventCopy, CoreTraceEvents: coreTraceCopy, CandidateLinks: links,
		EventDrops: drops, CoreTraceDrops: coreTraceDropped, M1CompleteChains: completeChains,
		BoundedRescueFinal: tx.BoundedRescue,
		Samples:            append([]phase27LiveSample(nil), samples...),
		MeasuredRateBasis:  "Core receiver unique payload bytes per leg divided by full measured EOF-completion duration. This is application-relevant unique DATA delivery on the actual TCP read/decode path, not local socket Write acceptance.",
		SocketBufferNote:   "No socket buffer override was set; OS defaults used. Portable Go harness did not query OS buffer sizes.",
	}

	if tx.TxAckedUsefulTotal != uint64(len(payload)) || rx.RxDeliveredBytes != uint64(len(payload)) ||
		tx.OutstandingFrames != 0 || tx.OutstandingBytes != 0 || rx.RxPendingFrames != 0 || rx.RxPendingBytes != 0 {
		t.Fatalf("transport/accounting mismatch: tx_ack=%d rx_delivered=%d tx_out=%d/%d rx_pending=%d/%d",
			tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, tx.OutstandingFrames, tx.OutstandingBytes, rx.RxPendingFrames, rx.RxPendingBytes)
	}
	if tx.TxWrittenBytesByLeg[0] == 0 || tx.TxWrittenBytesByLeg[1] == 0 || rx.RxUniqueBytesByLeg[0] == 0 || rx.RxUniqueBytesByLeg[1] == 0 {
		t.Fatalf("both real TCP legs must contribute unique DATA: tx=%v rx=%v", tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg)
	}
	if drops != 0 || coreTraceDropped != 0 || tx.BoundedRescue.CandidateAckLinkDropped != 0 {
		t.Fatalf("candidate trace incomplete: candidate_event_drops=%d core_trace_drops=%d candidate_ack_link_dropped=%d", drops, coreTraceDropped, tx.BoundedRescue.CandidateAckLinkDropped)
	}
	if resultPath := os.Getenv("SMP3_PHASE27_RESULT_PATH"); resultPath != "" {
		encoded, marshalErr := json.MarshalIndent(result, "", "  ")
		if marshalErr != nil {
			t.Fatalf("marshal Phase27 result: %v", marshalErr)
		}
		if writeErr := os.WriteFile(resultPath, append(encoded, '\n'), 0o600); writeErr != nil {
			t.Fatalf("write Phase27 result %s: %v", resultPath, writeErr)
		}
	}
	t.Logf("phase27_m1_pilot real_tcp=2 policy_variant=%s configured_mbps=%v relay_rate_configured=%d separate_relay_rtt_calibrator=%s payload=%d sha256=%x ack=%d delivered=%d tx_by_leg=%v rx_unique_by_leg=%v measured_unique_mbps_by_leg=%v candidates=%d healthy_hol=%d complete_m1_chains=%d core_trace_events=%d ack_link_drops=%d eof=io.EOF tx_ledger=0 rx_pending=0 write_duration=%s completion=%s",
		cfg.ExperimentalBoundedRescue.factorMode.String(), cfg.BandwidthMbps, slowRateBitsPS, calibratedRTT, len(payload), gotSHA, tx.TxAckedUsefulTotal, rx.RxDeliveredBytes,
		tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg, measuredUniqueMbps, len(links), tx.BoundedRescue.EligibleCount,
		completeChains, len(coreTraceCopy), tx.BoundedRescue.CandidateAckLinkDropped, writeDuration, completionDuration)
}

func toSHA256Hex(sum [32]byte) string { return fmt.Sprintf("%x", sum) }

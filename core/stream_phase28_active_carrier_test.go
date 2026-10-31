package smp3core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type phase28ActiveRTTSnapshot struct {
	LegID      int                `json:"leg_id"`
	Endpoint   string             `json:"endpoint"`
	Identity   StreamPathIdentity `json:"identity"`
	RTTNS      int64              `json:"rtt_ns"`
	Samples    uint32             `json:"samples"`
	ObservedAt time.Time          `json:"observed_at"`
}

type phase28ActiveCarrierResult struct {
	Classification              string                              `json:"classification"`
	Variant                     string                              `json:"variant"`
	PathAware                   bool                                `json:"path_aware"`
	ConfiguredMbps              [2]uint32                           `json:"configured_mbps"`
	MeasuredRateBasis           string                              `json:"measured_rate_basis"`
	RelayOneWayDelayNS          int64                               `json:"relay_one_way_delay_ns"`
	RelaySlowRateBPS            uint64                              `json:"relay_slow_rate_bps"`
	ActiveRTT                   []phase28ActiveRTTSnapshot          `json:"active_carrier_rtt"`
	RelayDirections             [2][2]phase25RelayDirectionSnapshot `json:"relay_direction_metrics"`
	PayloadBytes                int                                 `json:"payload_bytes"`
	PayloadSHA256               string                              `json:"payload_sha256"`
	ApplicationBytes            int                                 `json:"application_bytes"`
	ApplicationSHA256           string                              `json:"application_sha256"`
	ApplicationEOF              string                              `json:"application_eof"`
	WriteDurationNS             int64                               `json:"write_duration_ns"`
	CompletionDurationNS        int64                               `json:"completion_duration_ns"`
	UniquePayloadMbps           [2]float64                          `json:"unique_payload_mbps_by_leg"`
	TxWrittenByLeg              [2]uint64                           `json:"tx_written_by_leg"`
	TxRetryWrittenByLeg         [2]uint64                           `json:"tx_retry_written_by_leg"`
	RxUniqueByLeg               [2]uint64                           `json:"rx_unique_by_leg"`
	TxAckedUseful               uint64                              `json:"tx_acked_useful"`
	RxDelivered                 uint64                              `json:"rx_delivered"`
	TxOutstandingFrames         int                                 `json:"tx_outstanding_frames"`
	TxOutstandingBytes          uint64                              `json:"tx_outstanding_bytes"`
	RxPendingFrames             int                                 `json:"rx_pending_frames"`
	RxPendingBytes              uint64                              `json:"rx_pending_bytes"`
	BoundedRescue               StreamBoundedRescueStats            `json:"bounded_rescue"`
	ObserverEnabled             bool                                `json:"observer_enabled"`
	TxFrontierRescueAttempts    uint64                              `json:"tx_frontier_rescue_attempts"`
	TxRetryWriteAttemptFrames   [2]uint64                           `json:"tx_retry_write_attempt_frames"`
	TxRescueWriteAttemptFrames  [2]uint64                           `json:"tx_rescue_write_attempt_frames"`
	TxRescueWriteAttemptBytes   [2]uint64                           `json:"tx_rescue_write_attempt_bytes"`
	RxDuplicateFrames           [2]uint64                           `json:"rx_duplicate_frames"`
	RxDuplicateBytes            [2]uint64                           `json:"rx_duplicate_bytes"`
	FirstApplicationByteNS      int64                               `json:"first_application_byte_ns"`
	DualLegContributionVerified bool                                `json:"dual_leg_contribution_verified"`
	RestrictedSampledNS         [2]int64                            `json:"restricted_sampled_ns_by_leg"`
	RecoveringSampledNS         [2]int64                            `json:"recovering_sampled_ns_by_leg"`
	StateSampleIntervalNS       int64                               `json:"state_sample_interval_ns"`
	CandidateEventCount         int                                 `json:"candidate_event_count"`
	EventDrops                  uint64                              `json:"event_drops"`
	CandidateEvents             []StreamRescueEvent                 `json:"candidate_events,omitempty"`
}

type phase29FirstByteConn struct {
	net.Conn
	firstByteNS atomic.Int64
}

func (c *phase29FirstByteConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.firstByteNS.CompareAndSwap(0, time.Now().UnixNano())
	}
	return n, err
}

type phase29AdmissionSample struct {
	Restricted [2]time.Duration
	Recovering [2]time.Duration
	Interval   time.Duration
}

func phase29StartAdmissionSampler(engine *StreamEngine, interval time.Duration) func(time.Time) phase29AdmissionSample {
	if engine == nil || interval <= 0 {
		return func(time.Time) phase29AdmissionSample { return phase29AdmissionSample{} }
	}
	started := time.Now()
	initial := engine.TelemetrySnapshot().OrdinaryAdmissionState
	type sampleState struct {
		mu         sync.Mutex
		lastAt     time.Time
		last       [2]string
		restricted [2]time.Duration
		recovering [2]time.Duration
	}
	state := &sampleState{lastAt: started, last: initial}
	stop := make(chan struct{})
	done := make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				current := engine.TelemetrySnapshot().OrdinaryAdmissionState
				state.mu.Lock()
				elapsed := now.Sub(state.lastAt)
				for leg := 0; leg < 2; leg++ {
					switch state.last[leg] {
					case "restricted":
						state.restricted[leg] += elapsed
					case "recovering":
						state.recovering[leg] += elapsed
					}
				}
				state.lastAt, state.last = now, current
				state.mu.Unlock()
			}
		}
	}()
	return func(end time.Time) phase29AdmissionSample {
		close(stop)
		<-done
		state.mu.Lock()
		defer state.mu.Unlock()
		if end.After(state.lastAt) {
			elapsed := end.Sub(state.lastAt)
			for leg := 0; leg < 2; leg++ {
				switch state.last[leg] {
				case "restricted":
					state.restricted[leg] += elapsed
				case "recovering":
					state.recovering[leg] += elapsed
				}
			}
		}
		return phase29AdmissionSample{Restricted: state.restricted, Recovering: state.recovering, Interval: interval}
	}
}

func phase28EnvInt(t *testing.T, key string, fallback int, allowed ...int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, value, err)
	}
	for _, candidate := range allowed {
		if parsed == candidate {
			return parsed
		}
	}
	t.Fatalf("unsupported %s=%d; allowed=%v", key, parsed, allowed)
	return 0
}

func phase28WaitForActivePathSamples(t *testing.T, engines [2]*StreamEngine, minSamples uint32) []phase28ActiveRTTSnapshot {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for endpoint, engine := range engines {
			for legID := 0; legID < 2; legID++ {
				feedback, ok := engine.PathFeedback(LegID(legID))
				if !ok || feedback.Samples < minSamples {
					allReady = false
					continue
				}
				leg := engine.getLeg(uint8(legID))
				if leg == nil || feedback.Identity.Generation != leg.generation || feedback.Identity.CarrierID != leg.pathIdentity.CarrierID || feedback.Identity.LegID != LegID(legID) {
					t.Fatalf("endpoint %d leg %d feedback identity mismatch: feedback=%+v leg=%+v", endpoint, legID, feedback, leg)
				}
				wantDirection := StreamRTTClientToServer
				if endpoint == 1 {
					wantDirection = StreamRTTServerToClient
				}
				if feedback.Identity.Direction != wantDirection {
					t.Fatalf("endpoint %d leg %d feedback direction=%v, want=%v", endpoint, legID, feedback.Identity.Direction, wantDirection)
				}
			}
		}
		if allReady {
			out := make([]phase28ActiveRTTSnapshot, 0, 4)
			for endpoint, engine := range engines {
				endpointName := "client"
				if endpoint == 1 {
					endpointName = "server"
				}
				for legID := 0; legID < 2; legID++ {
					feedback, _ := engine.PathFeedback(LegID(legID))
					out = append(out, phase28ActiveRTTSnapshot{
						LegID: legID, Endpoint: endpointName, Identity: feedback.Identity,
						RTTNS: feedback.RTT.Nanoseconds(), Samples: feedback.Samples, ObservedAt: feedback.ObservedAt,
					})
				}
			}
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	diagnostics := make([]string, 0, 4)
	for endpoint, engine := range engines {
		for legID := 0; legID < 2; legID++ {
			leg := engine.getLeg(uint8(legID))
			feedback, ok := engine.PathFeedback(LegID(legID))
			cache := engine.pathRTTCache
			cache.mu.Lock()
			pending := len(cache.pending)
			binding := cache.binding[legID]
			sampleCount := len(cache.samples[legID][int(leg.pathIdentity.Direction-1)])
			cache.mu.Unlock()
			diagnostics = append(diagnostics, fmt.Sprintf("endpoint=%d leg=%d ok=%t feedback=%+v identity=%+v binding=%+v samples=%d pending=%d", endpoint, legID, ok, feedback, leg.pathIdentity, binding, sampleCount, pending))
		}
	}
	t.Fatalf("active Carrier RTT did not produce %d fresh samples on both legs and endpoints: %s", minSamples, strings.Join(diagnostics, " | "))
	return nil
}

// TestPhase28ActiveCarrierHOLPilotTCP measures same-connection authenticated
// RTT feedback and application delivery over active TCP Carrier sockets.
// By default Leg 0 is direct loopback and Leg 1 is a rate-limited TCP relay;
// SMP3_PHASE28_FAST_RELAY=1 also rate-limits Leg 0 through a second relay.
// Neither mode is a kernel shaper, public WAN, or natural 300 bp rejection test.
func TestPhase28ActiveCarrierHOLPilotTCP(t *testing.T) {
	variant := strings.ToUpper(os.Getenv("SMP3_PHASE28_VARIANT"))
	if variant == "" {
		variant = "B1-CR"
	}
	payloadBytes := phase28EnvInt(t, "SMP3_PHASE28_PAYLOAD_BYTES", 16<<20, 4<<10, 64<<10, 100<<10, 1<<20, 16<<20, 32<<20, 128<<20)
	slowRateMbps := phase28EnvInt(t, "SMP3_PHASE28_SLOW_RATE_MBPS", 10, 1, 10, 50, 100)
	oneWayDelayMS := phase28EnvInt(t, "SMP3_PHASE28_ONEWAY_DELAY_MS", 10, 10, 25, 50, 100)
	fastRelay := os.Getenv("SMP3_PHASE28_FAST_RELAY") == "1"

	cfg := phase25IdentityConfig()
	cfg.BandwidthMbps = []uint32{100, uint32(slowRateMbps)}
	cfg.ExperimentalPathAware = true
	cfg.PathRole = StreamPathRoleClient
	candidate := StreamPolicyB0
	switch variant {
	case "B0":
		cfg.ExperimentalBoundedRescue = nil
	case "B1-C", "B1-CR", "B1-F":
		candidate = StreamPolicyB1
		factor := boundedRescueFactorFull
		if variant == "B1-C" {
			factor = boundedRescueFactorCOnly
		} else if variant == "B1-CR" {
			factor = boundedRescueFactorCR
		}
		cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300, factorMode: factor}
	default:
		t.Fatalf("unsupported SMP3_PHASE28_VARIANT=%q; want B0, B1-C, B1-CR, or B1-F", variant)
	}

	var eventMu sync.Mutex
	var events []StreamRescueEvent
	var eventDrops uint64
	observerEnabled := os.Getenv("SMP3_PHASE29_OBSERVER") == "1"
	if observerEnabled {
		cfg.rescueEventHook = func(event StreamRescueEvent) {
			eventMu.Lock()
			defer eventMu.Unlock()
			if len(events) >= phase27LiveEventLimit {
				eventDrops++
				return
			}
			events = append(events, event)
		}
	}
	client, clientApp, err := NewStreamEngineForCandidate(cfg, candidate)
	if err != nil {
		t.Fatalf("construct %s client: %v", variant, err)
	}
	serverCfg := cfg
	serverCfg.PathRole = StreamPathRoleServer
	serverCfg.ExperimentalBoundedRescue = nil
	serverCfg.rescueEventHook = nil
	server, serverApp, err := NewStreamEngineForCandidate(serverCfg, StreamPolicyB0)
	if err != nil {
		_ = client.Close()
		_ = clientApp.Close()
		t.Fatalf("construct server: %v", err)
	}
	defer client.Close()
	defer clientApp.Close()
	defer server.Close()
	defer serverApp.Close()

	var sessionID SessionID
	sessionID[0] = 0x28
	clientFailures := make(chan error, 8)
	serverFailures := make(chan error, 8)
	var relays [2]*phase25TCPRelay
	for legID := 0; legID < 2; legID++ {
		clientAuth, serverAuth := testPathAuthPair(t, sessionID, LegID(legID), byte(0x70+legID))
		var clientConn, serverConn net.Conn
		if legID == 0 && !fastRelay {
			relays[legID] = nil
			clientConn, serverConn = phase25LoopbackTCPPair(t)
		} else {
			rateMbps := slowRateMbps
			delay := time.Duration(oneWayDelayMS) * time.Millisecond
			if legID == 0 {
				rateMbps = 100
				delay = 0
			}
			relay, cConn, sConn := phase25NewRelayPair(t, delay, uint64(rateMbps)*1_000_000, false)
			relays[legID], clientConn, serverConn = relay, cConn, sConn
		}
		if err := client.AttachLegWithPathAuth(LegID(legID), clientConn, func(err error) { clientFailures <- err }, clientAuth); err != nil {
			t.Fatalf("attach client Leg %d: %v", legID, err)
		}
		if err := server.AttachLegWithPathAuth(LegID(legID), serverConn, func(err error) { serverFailures <- err }, serverAuth); err != nil {
			t.Fatalf("attach server Leg %d: %v", legID, err)
		}
	}
	activeRTT := phase28WaitForActivePathSamples(t, [2]*StreamEngine{client, server}, 1)
	client.Activate()
	server.Activate()

	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i*31 + i/251)
	}
	wantSHA := sha256.Sum256(payload)
	started := time.Now()
	var finishAdmissionSample func(time.Time) phase29AdmissionSample
	if os.Getenv("SMP3_PHASE29_STATE_SAMPLE") == "1" {
		finishAdmissionSample = phase29StartAdmissionSampler(client, 10*time.Millisecond)
	}
	_ = clientApp.SetDeadline(started.Add(2 * time.Minute))
	_ = serverApp.SetDeadline(started.Add(2 * time.Minute))
	firstByteConn := &phase29FirstByteConn{Conn: serverApp}
	serverRead := make(chan phase25TCPReadResult, 1)
	clientRead := make(chan phase25TCPReadResult, 1)
	go func() { serverRead <- phase25ReadToEOF(firstByteConn) }()
	go func() { clientRead <- phase25ReadToEOF(clientApp) }()
	writeDone := make(chan error, 1)
	go func() { writeDone <- writeAll(clientApp, payload) }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("application write: %v", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("application writer exceeded 120 s")
	}
	writeDuration := time.Since(started)
	client.StartGracefulClose(io.EOF)
	var received phase25TCPReadResult
	select {
	case received = <-serverRead:
	case <-time.After(120 * time.Second):
		t.Fatal("server application did not receive EOF within 120 s")
	}
	if received.err != io.EOF {
		t.Fatalf("server application termination=%T %v, want io.EOF", received.err, received.err)
	}
	select {
	case peer := <-clientRead:
		if peer.err != io.EOF || len(peer.data) != 0 {
			t.Fatalf("client peer EOF bytes=%d err=%T %v", len(peer.data), peer.err, peer.err)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("client application did not observe peer EOF within 120 s")
	}
	completionDuration := time.Since(started)
	admissionSample := phase29AdmissionSample{}
	if finishAdmissionSample != nil {
		admissionSample = finishAdmissionSample(time.Now())
	}
	gotSHA := sha256.Sum256(received.data)
	if len(received.data) != len(payload) || gotSHA != wantSHA || !bytes.Equal(received.data, payload) {
		t.Fatalf("application payload mismatch bytes=%d/%d sha=%x want=%x", len(received.data), len(payload), gotSHA, wantSHA)
	}
	tx, rx := client.Snapshot(), server.Snapshot()
	if tx.TxAckedUsefulTotal != uint64(payloadBytes) || rx.RxDeliveredBytes != uint64(payloadBytes) || tx.OutstandingFrames != 0 || tx.OutstandingBytes != 0 || rx.RxPendingFrames != 0 || rx.RxPendingBytes != 0 {
		t.Fatalf("ACK/ledger mismatch ack=%d delivered=%d tx=%d/%d rx=%d/%d", tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, tx.OutstandingFrames, tx.OutstandingBytes, rx.RxPendingFrames, rx.RxPendingBytes)
	}
	dualLegContribution := tx.TxWrittenBytesByLeg[0] > 0 && tx.TxWrittenBytesByLeg[1] > 0 && rx.RxUniqueBytesByLeg[0] > 0 && rx.RxUniqueBytesByLeg[1] > 0
	requireDualLeg := os.Getenv("SMP3_PHASE29_REQUIRE_DUAL_LEG") == "1" || payloadBytes >= 16<<20
	if requireDualLeg && !dualLegContribution {
		t.Fatalf("two-Leg aggregation not observed: tx=%v rx=%v", tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg)
	}

	var copiedEvents []StreamRescueEvent
	eventMu.Lock()
	copiedEvents = append(copiedEvents, events...)
	drops := eventDrops
	eventMu.Unlock()
	rate := [2]float64{}
	if seconds := completionDuration.Seconds(); seconds > 0 {
		for i := range rx.RxUniqueBytesByLeg {
			rate[i] = float64(rx.RxUniqueBytesByLeg[i]*8) / seconds / 1_000_000
		}
	}
	var relayStats [2][2]phase25RelayDirectionSnapshot
	for legID, relay := range relays {
		if relay != nil {
			relayStats[legID] = relay.snapshotStats()
		}
	}
	classification := "REAL_LOOPBACK_TCP_USERSPACE_RELAY; active RTT is authenticated same-Carrier probe feedback; not kernel shaping or WAN"
	measuredRateBasis := "receiver Core unique DATA payload by Leg divided by complete application EOF duration; direct Leg0 is unshaped loopback, relay Leg1 is user-space shaped; local Write acceptance is not remote delivery"
	if fastRelay {
		classification = "REAL_LOOPBACK_TCP_USERSPACE_RELAY_BOTH_LEGS; active RTT is authenticated same-Carrier probe feedback; not kernel shaping or WAN"
		measuredRateBasis = "receiver Core unique DATA payload by Leg divided by complete application EOF duration; both Legs use independently measured user-space TCP relay service; local Write acceptance is not remote delivery"
	}
	firstByteNS := int64(0)
	if observedAt := firstByteConn.firstByteNS.Load(); observedAt != 0 {
		firstByteNS = time.Unix(0, observedAt).Sub(started).Nanoseconds()
	}
	result := phase28ActiveCarrierResult{
		Classification: classification,
		Variant:        variant, PathAware: true, ConfiguredMbps: [2]uint32{100, uint32(slowRateMbps)},
		MeasuredRateBasis:  measuredRateBasis,
		RelayOneWayDelayNS: int64(time.Duration(oneWayDelayMS) * time.Millisecond), RelaySlowRateBPS: uint64(slowRateMbps) * 1_000_000,
		ActiveRTT: activeRTT, RelayDirections: relayStats, PayloadBytes: payloadBytes,
		PayloadSHA256: hex.EncodeToString(wantSHA[:]), ApplicationBytes: len(received.data), ApplicationSHA256: hex.EncodeToString(gotSHA[:]),
		ApplicationEOF: "io.EOF", WriteDurationNS: writeDuration.Nanoseconds(), CompletionDurationNS: completionDuration.Nanoseconds(),
		UniquePayloadMbps: rate, TxWrittenByLeg: tx.TxWrittenBytesByLeg, TxRetryWrittenByLeg: tx.TxRetryWrittenBytesByLeg,
		RxUniqueByLeg: rx.RxUniqueBytesByLeg, TxAckedUseful: tx.TxAckedUsefulTotal, RxDelivered: rx.RxDeliveredBytes,
		TxOutstandingFrames: tx.OutstandingFrames, TxOutstandingBytes: tx.OutstandingBytes, RxPendingFrames: rx.RxPendingFrames, RxPendingBytes: rx.RxPendingBytes,
		BoundedRescue: tx.BoundedRescue, ObserverEnabled: observerEnabled,
		TxFrontierRescueAttempts:   tx.FrontierRescueAttempts,
		TxRetryWriteAttemptFrames:  tx.Telemetry.DataTxRetryWriteAttemptFrames,
		TxRescueWriteAttemptFrames: tx.Telemetry.DataTxRescueWriteAttemptFrames,
		TxRescueWriteAttemptBytes:  tx.Telemetry.DataTxRescueWriteAttemptBytes,
		RxDuplicateFrames:          rx.Telemetry.DataRxDuplicateFrames, RxDuplicateBytes: rx.Telemetry.DataRxDuplicateBytes,
		FirstApplicationByteNS:      firstByteNS,
		DualLegContributionVerified: dualLegContribution,
		RestrictedSampledNS:         [2]int64{admissionSample.Restricted[0].Nanoseconds(), admissionSample.Restricted[1].Nanoseconds()},
		RecoveringSampledNS:         [2]int64{admissionSample.Recovering[0].Nanoseconds(), admissionSample.Recovering[1].Nanoseconds()},
		StateSampleIntervalNS:       admissionSample.Interval.Nanoseconds(),
		CandidateEventCount:         len(copiedEvents), EventDrops: drops, CandidateEvents: copiedEvents,
	}
	resultPath := os.Getenv("SMP3_PHASE29_RESULT_PATH")
	if resultPath == "" {
		resultPath = os.Getenv("SMP3_PHASE28_RESULT_PATH")
	}
	if resultPath != "" {
		encoded, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatalf("marshal Phase28 result: %v", err)
		}
		if err := os.WriteFile(resultPath, append(encoded, '\n'), 0o600); err != nil {
			t.Fatalf("write Phase28 result: %v", err)
		}
	}
	topology := "direct-loopback+user-relay"
	if fastRelay {
		topology = "user-relay-100Mbps+user-relay-slow"
	}
	t.Logf("phase28_active_tcp variant=%s topology=%s active_rtt=%+v payload=%d sha=%s unique_mbps=%v tx=%v rx_unique=%v ack=%d candidates=%d eof=%s completion=%s relay_leg0=%+v relay_leg1=%+v", variant, topology, activeRTT, payloadBytes, hex.EncodeToString(wantSHA[:]), rate, tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg, tx.TxAckedUsefulTotal, len(copiedEvents), received.err, completionDuration, relayStats[0], relayStats[1])
}

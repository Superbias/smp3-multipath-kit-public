package smp3core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type phase19LongRunPoint struct {
	ElapsedSeconds      int         `json:"elapsed_seconds"`
	ApplicationTxBytes  uint64      `json:"application_tx_bytes"`
	ApplicationRxBytes  uint64      `json:"application_rx_bytes"`
	Client              StreamStats `json:"client"`
	Server              StreamStats `json:"server"`
	HeapAllocBytes      uint64      `json:"heap_alloc_bytes"`
	HeapSysBytes        uint64      `json:"heap_sys_bytes"`
	NumGC               uint32      `json:"num_gc"`
	Goroutines          int         `json:"goroutines"`
	ObservedWallTime    time.Time   `json:"observed_wall_time"`
	ProcessCPUSeconds   float64     `json:"process_cpu_seconds,omitempty"`
	ProcessCPUAvailable bool        `json:"process_cpu_available"`
}

type phase19LongRunStateTransition struct {
	ElapsedSeconds int    `json:"elapsed_seconds"`
	Leg            int    `json:"leg"`
	FromState      string `json:"from_state"`
	FromReason     string `json:"from_reason"`
	ToState        string `json:"to_state"`
	ToReason       string `json:"to_reason"`
}

// TestStreamPhase19ContinuousLongRun performs a real-time, continuous, single
// application stream over two loopback TCP Carrier sockets with virtual
// per-leg serialization and delay. It is an efficiency diagnostic, not a WAN
// or native-host test. Policy selection is fixed by the separately built
// source variant; environment variables only provide the run identity/config.
func TestStreamPhase19ContinuousLongRun(t *testing.T) {
	if os.Getenv("SMP3_PHASE19_LONGRUN") != "1" {
		t.Skip("set SMP3_PHASE19_LONGRUN=1 for a continuous Phase 19 stream")
	}
	intEnv := func(name string, fallback int) int {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback
		}
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			t.Fatalf("invalid %s=%q", name, raw)
		}
		return v
	}
	policy := os.Getenv("SMP3_PHASE19_POLICY_LABEL")
	if policy != "policy_a_experimental" && policy != "policy_b_phase18" {
		t.Fatalf("invalid policy label %q", policy)
	}
	phase22Treatment := os.Getenv("SMP3_PHASE22_LONGRUN_TREATMENT")
	var boundedRescue *StreamBoundedRescueConfig
	if phase22Treatment != "" {
		if policy != "policy_b_phase18" {
			t.Fatalf("Phase 22 treatment requires frozen Policy B, got %q", policy)
		}
		basisPoints, burstBytes := uint64(0), uint64(0)
		if raw := os.Getenv("SMP3_PHASE22_BUDGET_BASIS_POINTS"); raw != "" {
			parsed, parseErr := strconv.ParseUint(raw, 10, 16)
			if parseErr != nil {
				t.Fatalf("parse Phase 22 budget: %v", parseErr)
			}
			basisPoints = parsed
		}
		if raw := os.Getenv("SMP3_PHASE22_BURST_BYTES"); raw != "" {
			parsed, parseErr := strconv.ParseUint(raw, 10, 64)
			if parseErr != nil {
				t.Fatalf("parse Phase 22 burst: %v", parseErr)
			}
			burstBytes = parsed
		}
		switch phase22Treatment {
		case "B0":
			if basisPoints != 0 || burstBytes != 0 {
				t.Fatal("B0 long run must not receive bounded budget")
			}
		case "B1":
			if !validBoundedRescueBasisPoints(uint16(basisPoints)) || burstBytes > 64<<10 {
				t.Fatalf("invalid B1 long-run configuration: bp=%d burst=%d", basisPoints, burstBytes)
			}
			boundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: uint16(basisPoints), BurstBytes: burstBytes}
		default:
			t.Fatalf("invalid Phase 22 long-run treatment %q", phase22Treatment)
		}
	}
	sourceSHA := os.Getenv("SMP3_PHASE19_SOURCE_SHA256")
	if len(sourceSHA) != sha256.Size*2 {
		t.Fatal("SMP3_PHASE19_SOURCE_SHA256 must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(sourceSHA); err != nil {
		t.Fatalf("invalid source digest: %v", err)
	}
	binarySHA, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash test binary: %v", err)
	}
	duration := time.Duration(intEnv("SMP3_PHASE19_LONGRUN_SECONDS", 1800)) * time.Second
	rate0, rate1 := intEnv("SMP3_PHASE19_RATE0_MBPS", 100), intEnv("SMP3_PHASE19_RATE1_MBPS", 1)
	rtt0, rtt1 := intEnv("SMP3_PHASE19_RTT0_MS", 20), intEnv("SMP3_PHASE19_RTT1_MS", 20)
	chunkSize := 64 << 10

	clientTelemetry, serverTelemetry := &StreamTelemetry{}, &StreamTelemetry{}
	clientCfg := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic,
		ChunkSize: chunkSize, QueueFrames: 256, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
		AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond,
		BandwidthMbps:             []uint32{uint32(rate0), uint32(rate1)},
		Telemetry:                 clientTelemetry,
		ExperimentalBoundedRescue: boundedRescue,
	}
	serverCfg := clientCfg
	serverCfg.PathRole, serverCfg.Telemetry = StreamPathRoleServer, serverTelemetry
	clientCfg.PathRole = StreamPathRoleClient
	var client, server *StreamEngine
	var clientApp, serverApp net.Conn
	if phase22Treatment == "" {
		client, clientApp = NewStreamEngine(clientCfg)
		server, serverApp = NewStreamEngine(serverCfg)
	} else {
		candidate := StreamPolicyB0
		if phase22Treatment == "B1" {
			candidate = StreamPolicyB1
		}
		var err error
		client, clientApp, err = NewStreamEngineForCandidate(clientCfg, candidate)
		if err != nil {
			t.Fatalf("construct %s client: %v", candidate, err)
		}
		server, serverApp, err = NewStreamEngineForCandidate(serverCfg, candidate)
		if err != nil {
			_ = client.Close()
			_ = clientApp.Close()
			t.Fatalf("construct %s server: %v", candidate, err)
		}
		if got := client.RuntimePolicyIdentity().Candidate; got != candidate {
			t.Fatalf("client runtime candidate=%s want=%s", got, candidate)
		}
		if got := server.RuntimePolicyIdentity().Candidate; got != candidate {
			t.Fatalf("server runtime candidate=%s want=%s", got, candidate)
		}
	}
	var outbound, inbound [2]*p0cABCarrier
	for leg, cfg := range []struct {
		rate int
		rtt  int
	}{{rate0, rtt0}, {rate1, rtt1}} {
		clock := &p0cABClock{changed: make(chan struct{})}
		clock.rate.Store(int64(cfg.rate) * 1_000_000)
		outbound[leg], inbound[leg] = p0cABTCPPair(t, clock, time.Duration(cfg.rtt)*time.Millisecond)
		if err := client.AttachLeg(LegID(leg), net.Conn(&p0cdDataFrameCoalescer{Conn: outbound[leg]}), nil); err != nil {
			t.Fatalf("attach client leg%d: %v", leg, err)
		}
		if err := server.AttachLeg(LegID(leg), net.Conn(&p0cdDataFrameCoalescer{Conn: inbound[leg]}), nil); err != nil {
			t.Fatalf("attach server leg%d: %v", leg, err)
		}
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
		for leg := 0; leg < 2; leg++ {
			_ = outbound[leg].Close()
			_ = inbound[leg].Close()
		}
	}()

	buf := make([]byte, chunkSize)
	const patternMod = 251
	start := time.Now()
	stopAt := start.Add(duration)
	t.Logf("SMP3_PHASE19_LONGRUN_IDENTITY %s", mustJSON(map[string]any{
		"phase": "SMP3 v2.7 Phase 19 continuous long run", "policy": policy,
		"source_core_sha256": sourceSHA, "test_binary_sha256": binarySHA,
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"carrier":          "loopback TCP with virtual per-leg serialization and one-way delay; not physical WAN",
		"duration_seconds": int(duration.Seconds()), "rates_mbps": []int{rate0, rate1},
		"phase22_treatment": phase22Treatment, "phase22_bounded_rescue": boundedRescue,
		"rtt_ms": []int{rtt0, rtt1}, "path_aware": false, "trace": false,
		"chunk_bytes": chunkSize, "queue_frames": 256, "max_inflight_frames": 1024,
		"ack_interval_ms": 20, "retransmit_timeout_ms": 1500,
	}))

	type ioResult struct {
		bytes uint64
		hash  string
		err   error
	}
	serverDone := make(chan ioResult, 1)
	var applicationTxBytes atomic.Uint64
	var applicationRxBytes atomic.Uint64
	go func() {
		h := sha256.New()
		readBuf := make([]byte, chunkSize)
		var total uint64
		for {
			n, readErr := serverApp.Read(readBuf)
			if n > 0 {
				_, _ = h.Write(readBuf[:n])
				total += uint64(n)
				applicationRxBytes.Add(uint64(n))
			}
			if readErr != nil {
				if readErr == io.EOF {
					serverDone <- ioResult{bytes: total, hash: hex.EncodeToString(h.Sum(nil))}
				} else {
					serverDone <- ioResult{bytes: total, hash: hex.EncodeToString(h.Sum(nil)), err: readErr}
				}
				return
			}
		}
	}()

	txDone := make(chan ioResult, 1)
	go func() {
		h := sha256.New()
		var total uint64
		var offset uint64
		for time.Now().Before(stopAt) {
			for i := range buf {
				buf[i] = byte(((offset+uint64(i))*31 + 7) % patternMod)
			}
			n, writeErr := clientApp.Write(buf)
			if n > 0 {
				_, _ = h.Write(buf[:n])
				total += uint64(n)
				offset += uint64(n)
				applicationTxBytes.Add(uint64(n))
			}
			if writeErr != nil {
				txDone <- ioResult{bytes: total, hash: hex.EncodeToString(h.Sum(nil)), err: writeErr}
				return
			}
			if n != len(buf) {
				txDone <- ioResult{bytes: total, hash: hex.EncodeToString(h.Sum(nil)), err: io.ErrShortWrite}
				return
			}
		}
		closeErr := clientApp.Close()
		txDone <- ioResult{bytes: total, hash: hex.EncodeToString(h.Sum(nil)), err: closeErr}
	}()

	type stateKey struct{ state, reason string }
	var transitions []phase19LongRunStateTransition
	var stateSeconds [2]map[string]float64
	stateSeconds[0], stateSeconds[1] = map[string]float64{}, map[string]float64{}
	var previous [2]stateKey
	var previousAt time.Time
	var points []phase19LongRunPoint
	nextPointSecond := 60
	startPeriodic := time.NewTicker(100 * time.Millisecond)
	minuteTicker := time.NewTicker(time.Minute)
	defer startPeriodic.Stop()
	defer minuteTicker.Stop()
	var txResult, rxResult ioResult
	txFinished, rxFinished := false, false
	for !txFinished || !rxFinished {
		select {
		case txResult = <-txDone:
			txFinished = true
		case rxResult = <-serverDone:
			rxFinished = true
		case <-startPeriodic.C:
			now := time.Now()
			stats := [2]StreamStats{client.TelemetrySnapshot(), server.TelemetrySnapshot()}
			elapsed := int(now.Sub(start).Seconds())
			for leg := 0; leg < 2; leg++ {
				key := stateKey{stats[0].OrdinaryAdmissionState[leg], stats[0].OrdinaryAdmissionReason[leg]}
				if key.state == "" {
					key.state = "normal"
				}
				if !previousAt.IsZero() && previous[leg].state != "" {
					stateSeconds[leg][previous[leg].state] += now.Sub(previousAt).Seconds()
					if previous[leg] != key {
						transitions = append(transitions, phase19LongRunStateTransition{
							ElapsedSeconds: elapsed, Leg: leg, FromState: previous[leg].state,
							FromReason: previous[leg].reason, ToState: key.state, ToReason: key.reason,
						})
					}
				}
				previous[leg] = key
			}
			previousAt = now
			if elapsed >= nextPointSecond {
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				points = append(points, phase19LongRunPoint{
					ElapsedSeconds: elapsed, ApplicationTxBytes: applicationTxBytes.Load(),
					ApplicationRxBytes: applicationRxBytes.Load(), Client: stats[0], Server: stats[1],
					HeapAllocBytes: mem.HeapAlloc, HeapSysBytes: mem.HeapSys, NumGC: mem.NumGC,
					Goroutines: runtime.NumGoroutine(), ObservedWallTime: now,
					ProcessCPUSeconds: phase19LongRunCPUSeconds(), ProcessCPUAvailable: phase19LongRunCPUAvailable(),
				})
				encoded, _ := json.Marshal(map[string]any{"point": points[len(points)-1], "state_transitions_so_far": len(transitions)})
				// Go testing buffers t.Log output until this test exits. Write the
				// same immutable minute snapshot directly so a 30-minute runner can
				// persist and display progress while the test is still running.
				_, _ = fmt.Fprintf(os.Stdout, "SMP3_PHASE19_LONGRUN_MINUTE %s\n", encoded)
				t.Logf("SMP3_PHASE19_LONGRUN_MINUTE %s", encoded)
				nextPointSecond += 60
			}
		case <-minuteTicker.C:
			// Minute output is emitted from the 100ms ticker to share one coherent
			// snapshot point with resource metrics.
		}
	}
	finalObservedAt := time.Now()
	if !previousAt.IsZero() {
		for leg := 0; leg < 2; leg++ {
			stateSeconds[leg][previous[leg].state] += finalObservedAt.Sub(previousAt).Seconds()
		}
	}

	// Wait for both ledgers to drain after normal EOF; sample at bounded intervals.
	drainDeadline := time.Now().Add(10 * time.Second)
	var clientStats, serverStats StreamStats
	for time.Now().Before(drainDeadline) {
		clientStats, serverStats = client.Snapshot(), server.Snapshot()
		if clientStats.OutstandingFrames == 0 && clientStats.OutstandingBytes == 0 &&
			serverStats.OutstandingFrames == 0 && serverStats.OutstandingBytes == 0 &&
			clientStats.RxPendingFrames == 0 && clientStats.RxPendingBytes == 0 &&
			serverStats.RxPendingFrames == 0 && serverStats.RxPendingBytes == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	clientStats, serverStats = client.Snapshot(), server.Snapshot()
	result := map[string]any{
		"policy": policy, "source_core_sha256": sourceSHA, "test_binary_sha256": binarySHA,
		"phase22_treatment": phase22Treatment, "phase22_bounded_rescue": boundedRescue,
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"carrier":          "loopback TCP with virtual per-leg serialization and one-way delay; not physical WAN",
		"duration_seconds": time.Since(start).Seconds(), "requested_duration_seconds": duration.Seconds(),
		"rates_mbps": []int{rate0, rate1}, "rtt_ms": []int{rtt0, rtt1},
		"application_tx_bytes": txResult.bytes, "application_rx_bytes": rxResult.bytes,
		"tx_sha256": txResult.hash, "rx_sha256": rxResult.hash,
		"tx_error": errorString(txResult.err), "rx_error": errorString(rxResult.err),
		"strict_eof": rxResult.err == nil, "data_integrity": txResult.bytes == rxResult.bytes && txResult.hash == rxResult.hash,
		"client_final": clientStats, "server_final": serverStats,
		"state_seconds_by_leg": stateSeconds, "state_transitions_sampled_100ms": transitions,
		"minute_points": points, "sampling_resolution_ms": 100,
		"path_aware": false, "trace": false,
	}
	t.Logf("SMP3_PHASE19_LONGRUN_RESULT %s", mustJSON(result))
	if txResult.err != nil || rxResult.err != nil || rxResult.bytes != txResult.bytes || rxResult.hash != txResult.hash {
		t.Fatalf("continuous stream correctness failed: tx=%+v rx=%+v", txResult, rxResult)
	}
	if clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 || serverStats.OutstandingFrames != 0 || serverStats.OutstandingBytes != 0 ||
		clientStats.RxPendingFrames != 0 || clientStats.RxPendingBytes != 0 || serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
		t.Fatalf("final accounting did not drain: client=%+v server=%+v", clientStats, serverStats)
	}
}

func phase19LongRunCPUSeconds() float64 {
	var sample = []metrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64 {
		return 0
	}
	return sample[0].Value.Float64()
}

func phase19LongRunCPUAvailable() bool {
	var sample = []metrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}}
	metrics.Read(sample)
	return sample[0].Value.Kind() == metrics.KindFloat64
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"json_error":"` + err.Error() + `"}`
	}
	return string(b)
}

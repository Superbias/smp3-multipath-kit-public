package smp3core

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	mathrand "math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

type streamPhase17EfficiencyScenario struct {
	name  string
	rtt   [2]time.Duration
	rate  [2]int64
	bytes int
	run   int
	block string
}

func p0cABTCPPair(t *testing.T, clock *p0cABClock, rtt time.Duration) (*p0cABCarrier, *p0cABCarrier) {
	t.Helper()
	client, server, _ := v27BlackholeTCPPair(t)
	reverse := &p0cABClock{changed: make(chan struct{})}
	reverse.rate.Store(10_000_000_000)
	return p0cABWrap(client, clock, rtt/2), p0cABWrap(server, reverse, rtt/2)
}

func streamPhase17TCPInfo(outbound, inbound [2]*p0cABCarrier) map[string]any {
	result := make(map[string]any, 4)
	for leg := 0; leg < 2; leg++ {
		if outbound[leg] != nil {
			result[fmt.Sprintf("leg%d_client_tx", leg)] = outbound[leg].phase17TCPInfoSnapshot()
		}
		if inbound[leg] != nil {
			result[fmt.Sprintf("leg%d_server_rx", leg)] = inbound[leg].phase17TCPInfoSnapshot()
		}
	}
	return result
}

// TestStreamPhase17RetransmissionEfficiency is an opt-in matched diagnostic.
// It exercises the shared Core over a calibrated p0cABCarrier backed by either
// net.Pipe or real loopback TCP sockets. RTT and serialization remain virtual;
// loopback TCP_INFO is only evidence about those local TCP sockets, not WAN.
func TestStreamPhase17RetransmissionEfficiency(t *testing.T) {
	if os.Getenv("SMP3_PHASE17_EFFICIENCY") != "1" {
		t.Skip("set SMP3_PHASE17_EFFICIENCY=1 to run the Phase 17 virtual-Carrier efficiency diagnostic")
	}
	sourceSHA := os.Getenv("SMP3_PHASE17_CORE_SOURCE_SHA256")
	if len(sourceSHA) != 64 {
		t.Fatal("SMP3_PHASE17_CORE_SOURCE_SHA256 must contain the frozen core Go source tree SHA-256")
	}
	if _, err := hex.DecodeString(sourceSHA); err != nil {
		t.Fatalf("invalid SMP3_PHASE17_CORE_SOURCE_SHA256: %v", err)
	}
	binarySHA, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	repetitions := 3
	if raw := os.Getenv("SMP3_PHASE17_REPETITIONS"); raw != "" {
		repetitions, err = strconv.Atoi(raw)
		if err != nil || repetitions < 1 || repetitions > 20 {
			t.Fatalf("SMP3_PHASE17_REPETITIONS must be in [1,20], got %q", raw)
		}
	}
	seed := int64(17017)
	if raw := os.Getenv("SMP3_PHASE17_ORDER_SEED"); raw != "" {
		seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("invalid SMP3_PHASE17_ORDER_SEED %q: %v", raw, err)
		}
	}
	size := 512 << 10
	if raw := os.Getenv("SMP3_PHASE17_BYTES"); raw != "" {
		size, err = strconv.Atoi(raw)
		if err != nil || size < 64<<10 || size > 16<<20 {
			t.Fatalf("SMP3_PHASE17_BYTES must be in [65536,16777216], got %q", raw)
		}
	}
	lowSpeedSize := 16 << 20
	if raw := os.Getenv("SMP3_PHASE17_LOW_SPEED_BYTES"); raw != "" {
		lowSpeedSize, err = strconv.Atoi(raw)
		if err != nil || lowSpeedSize < 1<<20 || lowSpeedSize > 32<<20 {
			t.Fatalf("SMP3_PHASE17_LOW_SPEED_BYTES must be in [1048576,33554432], got %q", raw)
		}
	}
	identity, err := json.Marshal(map[string]any{
		"source_core_sha256": sourceSHA, "test_binary_sha256": binarySHA,
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"gomaxprocs": runtime.GOMAXPROCS(0), "repetitions_per_cell": repetitions,
		"payload_bytes_default": size, "low_speed_payload_bytes": lowSpeedSize, "order_seed": seed,
		"carrier_models": []string{
			"p0cABCarrier over net.Pipe with virtual serialization and one-way delay",
			"p0cABCarrier over 127.0.0.1 TCP sockets with virtual serialization and one-way delay; TCP_INFO captured on Linux",
		},
		"physical_carrier_or_wan": "NOT TESTED",
		"core_config":             map[string]any{"capacity_mode": "dynamic", "bandwidth_mbps_by_scenario": [][]int64{{100, 100}, {100, 1}}, "chunk_bytes": 64 << 10, "queue_frames": 256, "max_inflight_frames": 1024, "max_reorder_frames": 4096, "ack_interval_ms": 20, "retransmit_timeout_ms": 1500},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SMP3_PHASE17_EFFICIENCY_IDENTITY %s", identity)

	scenarios := []streamPhase17EfficiencyScenario{
		{name: "healthy_dual_20_20_100_100", rtt: [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}, rate: [2]int64{100, 100}, bytes: size},
		{name: "healthy_dual_20_100_100_100", rtt: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}, rate: [2]int64{100, 100}, bytes: size},
		{name: "healthy_low_speed_leg_20_20_100_1", rtt: [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}, rate: [2]int64{100, 1}, bytes: lowSpeedSize},
	}
	modes := []streamPathQOEMode{
		{name: "path_aware_off_trace_off"},
		{name: "path_aware_off_trace_on"},
		{name: "path_aware_ready_trace_off", pathAware: true, waitForRTT: true},
		{name: "path_aware_ready_trace_on", pathAware: true, waitForRTT: true},
	}
	type block struct {
		scenario streamPhase17EfficiencyScenario
		run      int
		tcp      bool
	}
	blocks := make([]block, 0, len(scenarios)*repetitions*2)
	for _, tcpCarrier := range []bool{false, true} {
		for _, scenario := range scenarios {
			for run := 1; run <= repetitions; run++ {
				item := scenario
				item.run = run
				transport := "net_pipe"
				if tcpCarrier {
					transport = "loopback_tcp"
				}
				item.block = scenario.name + "/" + transport + "/" + strconv.Itoa(run)
				blocks = append(blocks, block{scenario: item, run: run, tcp: tcpCarrier})
			}
		}
	}
	rng := mathrand.New(mathrand.NewSource(seed))
	rng.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })

	order := 0
	for _, block := range blocks {
		blockModes := append([]streamPathQOEMode(nil), modes...)
		rng.Shuffle(len(blockModes), func(i, j int) { blockModes[i], blockModes[j] = blockModes[j], blockModes[i] })
		for _, mode := range blockModes {
			order++
			experimentScenario := block.scenario.name + "_net_pipe"
			if block.tcp {
				experimentScenario = block.scenario.name + "_loopback_tcp"
			}
			job := streamPathQOEJob{
				rtt: block.scenario.rtt, carrierRatesMbps: block.scenario.rate, size: block.scenario.bytes,
				run: block.run, mode: mode, orderSeed: seed, orderIndex: order,
				blockID:              block.scenario.block,
				traceFrames:          mode.name == "path_aware_off_trace_on" || mode.name == "path_aware_ready_trace_on",
				collectCoreTelemetry: true,
				tcpCarrier:           block.tcp,
				dynamicCapacity:      true,
			}
			sample := runStreamPathQoESample(t, job)
			encoded, err := json.Marshal(map[string]any{
				"experiment_scenario": experimentScenario,
				"source_core_sha256":  sourceSHA,
				"test_binary_sha256":  binarySHA,
				"sample":              sample,
			})
			if err != nil {
				t.Fatalf("encode Phase17 sample %d: %v", order, err)
			}
			t.Logf("SMP3_PHASE17_EFFICIENCY_SAMPLE %s", encoded)
			if !sample.Pass {
				t.Errorf("scenario=%s mode=%s run=%d transfer correctness failed: %s", experimentScenario, mode.name, block.run, sample.Error)
				continue
			}
			if block.scenario.name == "healthy_dual_20_20_100_100" && (sample.SentBytesByLeg[0] == 0 || sample.SentBytesByLeg[1] == 0) {
				t.Errorf("healthy dual-leg case did not use both legs: mode=%s run=%d sent=%v", mode.name, block.run, sample.SentBytesByLeg)
			}
			if block.scenario.name == "healthy_low_speed_leg_20_20_100_1" && sample.SentBytesByLeg[1] == 0 {
				t.Errorf("healthy low-speed leg was not exercised: mode=%s run=%d sent=%v", mode.name, block.run, sample.SentBytesByLeg)
			}
			if sample.CoreTxRetryWrittenBytesByLeg != [2]uint64{} || sample.CoreDataTxRetransmitFramesByLeg != [2]uint64{} || sample.CoreRescueAttemptBytesByLeg != [2]uint64{} || sample.ReceiverDuplicateBytesByLeg != [2]uint64{} {
				t.Errorf("no-fault healthy case observed DATA retry/rescue/duplicate activity: scenario=%s mode=%s run=%d retry=%v retransmit_frames=%v rescue=%v rx_duplicate=%v",
					experimentScenario, mode.name, block.run, sample.CoreTxRetryWrittenBytesByLeg, sample.CoreDataTxRetransmitFramesByLeg, sample.CoreRescueAttemptBytesByLeg, sample.ReceiverDuplicateBytesByLeg)
			}
			if block.tcp && runtime.GOOS == "linux" {
				for _, endpoint := range []map[string]any{sample.TCPInfoBefore, sample.TCPInfoAfter} {
					for leg := 0; leg < 2; leg++ {
						for _, side := range []string{"client_tx", "server_rx"} {
							key := fmt.Sprintf("leg%d_%s", leg, side)
							info, ok := endpoint[key].(map[string]any)
							if !ok || info["available"] != true {
								t.Errorf("loopback TCP_INFO unavailable for %s: %+v", key, endpoint[key])
								continue
							}
							if _, ok := info["total_retrans"].(uint32); !ok {
								t.Errorf("loopback TCP_INFO total_retrans unavailable for %s: %+v", key, info)
							}
						}
					}
				}
			}
		}
	}
}

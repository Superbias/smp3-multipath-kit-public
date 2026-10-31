package smp3core

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestReleaseMatchedConfig runs the same finite fault schedule used by the
// Native runner through the real StreamEngine/Core path. The carrier model is
// deterministic and is not a substitute for either host process test.
func TestReleaseMatchedConfig(t *testing.T) {
	if os.Getenv("SMP3_RELEASE_MATCHED_CONFIG") != "1" {
		t.Skip("explicit matched Core integration audit")
	}
	rttMS, err := strconv.Atoi(os.Getenv("SMP3_RELEASE_RTT_MS"))
	if err != nil || rttMS < 0 {
		t.Fatalf("SMP3_RELEASE_RTT_MS must be a nonnegative integer, got %q", os.Getenv("SMP3_RELEASE_RTT_MS"))
	}
	scenario := os.Getenv("SMP3_RELEASE_SCENARIO")
	switch scenario {
	case "single", "healthy", "slow", "blackhole", "slow-recovery", "blackhole-recovery":
	default:
		t.Fatalf("unsupported SMP3_RELEASE_SCENARIO %q", scenario)
	}
	duration := 70 * time.Second
	if raw := os.Getenv("SMP3_RELEASE_DURATION"); raw != "" {
		duration, err = time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			t.Fatalf("invalid SMP3_RELEASE_DURATION %q", raw)
		}
	}
	var requiredB float64
	if raw := os.Getenv("SMP3_RELEASE_MATCHED_B_MBPS"); raw != "" {
		requiredB, err = strconv.ParseFloat(raw, 64)
		if err != nil || requiredB <= 0 {
			t.Fatalf("invalid SMP3_RELEASE_MATCHED_B_MBPS %q", raw)
		}
	}
	clocks := releaseClocks(100e6, 100e6)
	legs := []int{0, 1}
	if scenario == "single" {
		legs = []int{0}
	}
	baseConfig := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic,
		ActivationMode: StreamActivationLegacy, ThresholdBytesPS: 10_000_000, ActivationWindow: time.Second,
		BandwidthMbps: []uint32{128, 500}, ChunkSize: 64 << 10,
		QueueFrames: 256, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
		AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond, RecoveryTimeout: 15 * time.Second,
	}
	serverConfig := baseConfig
	serverConfig.StartupPolicy = StreamStartupFirstReady
	clientConfig := baseConfig
	clientConfig.ActivationWindow = 10 * time.Second
	clientConfig.StartupPolicy = StreamStartupPreferred
	clientConfig.StartupPreferredLeg = 0
	clientConfig.StartupGrace = 500 * time.Millisecond
	f := releaseStartWithConfigs(t, clocks, time.Duration(rttMS)*time.Millisecond, legs, serverConfig, clientConfig)
	defer f.close(t)
	releaseLog(t, map[string]any{
		"scenario_start": scenario, "rtt_ms": rttMS, "fault_interval_seconds": []int{5, 45},
		"carrier_rates_bps": []int64{100_000_000, 100_000_000}, "server_core_config": serverConfig, "client_core_config": clientConfig,
		"runner_scope": "real StreamEngine over deterministic in-process carriers; not Native or Standalone process evidence",
	})
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	samples := map[int]uint64{0: f.sink.bytes.Load()}
	var last uint64
	for now := range tick.C {
		sec := int(now.Sub(start).Seconds())
		if sec == 5 {
			switch scenario {
			case "slow", "slow-recovery":
				clocks[1].rate.Store(1_000_000)
			case "blackhole", "blackhole-recovery":
				clocks[1].rate.Store(0)
			}
		}
		if sec == 45 && (scenario == "slow-recovery" || scenario == "blackhole-recovery") {
			clocks[1].rate.Store(100_000_000)
		}
		n := f.sink.bytes.Load()
		samples[sec] = n
		releaseLog(t, map[string]any{
			"second": sec, "scenario": scenario, "rtt_ms": rttMS, "delivered": n,
			"goodput_mbps": float64(n-last) * 8 / 1e6, "tx": releaseStats(f.tx), "rx": releaseStats(f.rx),
		})
		last = n
		if now.Sub(start) >= duration {
			break
		}
	}
	result := map[string]any{"scenario_result": scenario, "rtt_ms": rttMS, "duration_seconds": duration.Seconds()}
	if bytes55, ok55 := samples[55]; ok55 {
		if bytes65, ok65 := samples[65]; ok65 {
			window := float64(bytes65-bytes55) * 8 / 10 / 1e6
			result["fixed_55_65_mbps"] = window
			if scenario == "slow-recovery" || scenario == "blackhole-recovery" {
				result["recovery_threshold_mbps"] = 1.5 * requiredB
				if requiredB > 0 && window < 1.5*requiredB {
					t.Errorf("fixed 55-65s recovery window %.3f Mbps is below 1.5B=%.3f Mbps", window, 1.5*requiredB)
				}
			}
		}
	}
	releaseLog(t, result)
	if f.sink.bytes.Load() < 2<<20 {
		t.Errorf("insufficient useful progress: %d bytes", f.sink.bytes.Load())
	}
}

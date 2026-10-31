package smp3core

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	v27BlackholePolicyEnv        = "SMP3_V27_KERNEL_BLACKHOLE_RESCUE_POLICY"
	v27BlackholeBudgetBPSEnv     = "SMP3_V27_KERNEL_BLACKHOLE_RESCUE_BPS"
	v27BlackholeBurstBytesEnv    = "SMP3_V27_KERNEL_BLACKHOLE_RESCUE_BURST_BYTES"
	v27BlackholeRequireBudgetEnv = "SMP3_V27_KERNEL_BLACKHOLE_REQUIRE_BUDGET_EXHAUSTED"
)

type v27BlackholeRescuePolicy struct {
	Name   string
	Config *StreamBoundedRescueConfig
}

func v27BlackholeRescuePolicyFromEnv(getenv func(string) string) (v27BlackholeRescuePolicy, error) {
	if getenv == nil {
		return v27BlackholeRescuePolicy{}, fmt.Errorf("nil environment reader")
	}
	name := strings.ToLower(strings.TrimSpace(getenv(v27BlackholePolicyEnv)))
	if name == "" {
		name = "b0"
	}
	bpsRaw := strings.TrimSpace(getenv(v27BlackholeBudgetBPSEnv))
	burstRaw := strings.TrimSpace(getenv(v27BlackholeBurstBytesEnv))
	switch name {
	case "b0":
		if bpsRaw != "" || burstRaw != "" {
			return v27BlackholeRescuePolicy{}, fmt.Errorf("B0 must not receive B1 budget inputs")
		}
		return v27BlackholeRescuePolicy{Name: "b0"}, nil
	case "b1":
		if bpsRaw == "" {
			return v27BlackholeRescuePolicy{}, fmt.Errorf("B1 requires %s", v27BlackholeBudgetBPSEnv)
		}
		bps, err := strconv.ParseUint(bpsRaw, 10, 16)
		if err != nil || !validBoundedRescueBasisPoints(uint16(bps)) {
			return v27BlackholeRescuePolicy{}, fmt.Errorf("invalid B1 budget basis points %q", bpsRaw)
		}
		var burst uint64
		if burstRaw != "" {
			burst, err = strconv.ParseUint(burstRaw, 10, 64)
			if err != nil || burst > v27BlackholeChunkSize {
				return v27BlackholeRescuePolicy{}, fmt.Errorf("invalid B1 burst bytes %q", burstRaw)
			}
		}
		return v27BlackholeRescuePolicy{
			Name: "b1",
			Config: &StreamBoundedRescueConfig{
				Enabled: true, BudgetBasisPoints: uint16(bps), BurstBytes: burst,
			},
		}, nil
	default:
		return v27BlackholeRescuePolicy{}, fmt.Errorf("invalid rescue policy %q; want b0 or b1", name)
	}
}

func v27ValidateBlackholePolicyBinding(engine *StreamEngine, policy v27BlackholeRescuePolicy) error {
	if engine == nil {
		return fmt.Errorf("nil StreamEngine")
	}
	if policy.Name == "b0" {
		if policy.Config != nil || engine.boundedRescue != nil || engine.boundedRescueConfigError != "" {
			return fmt.Errorf("B0 policy mismatch: config=%+v manager=%v config_error=%q", policy.Config, engine.boundedRescue != nil, engine.boundedRescueConfigError)
		}
		if engine.TelemetrySnapshot().BoundedRescue.Enabled {
			return fmt.Errorf("B0 Core unexpectedly reports bounded rescue enabled")
		}
		return nil
	}
	if policy.Name != "b1" || policy.Config == nil || !policy.Config.Enabled {
		return fmt.Errorf("invalid expected policy binding: %+v", policy)
	}
	if engine.boundedRescue == nil || engine.boundedRescueConfigError != "" {
		return fmt.Errorf("B1 Core failed to initialize budget manager: manager=%v config_error=%q", engine.boundedRescue != nil, engine.boundedRescueConfigError)
	}
	stats := engine.TelemetrySnapshot().BoundedRescue
	if !stats.Enabled || stats.BudgetBasisPoints != policy.Config.BudgetBasisPoints || stats.BurstBytes != policy.Config.BurstBytes {
		return fmt.Errorf("B1 Core config mismatch: got enabled=%v bps=%d burst=%d, want enabled=true bps=%d burst=%d",
			stats.Enabled, stats.BudgetBasisPoints, stats.BurstBytes, policy.Config.BudgetBasisPoints, policy.Config.BurstBytes)
	}
	return nil
}

func v27BlackholeBudgetExhausted(stats StreamBoundedRescueStats) bool {
	if !stats.Enabled || stats.WindowBaseQuotaBytes == 0 || stats.WindowBudgetDebtBytes != 0 || stats.ReservedBytes != 0 || stats.BudgetRejected == 0 {
		return false
	}
	if stats.WindowCarrierAcceptedBytes >= stats.WindowBaseQuotaBytes {
		return true
	}
	return stats.ReservedBaseBytes >= stats.WindowBaseQuotaBytes-stats.WindowCarrierAcceptedBytes
}

type v27BlackholeRescueEventRecorder struct {
	mu           sync.Mutex
	total        uint64
	dropped      uint64
	kindCounts   map[string]uint64
	classCounts  map[string]uint64
	reasonCounts map[string]uint64
	samples      []StreamRescueEvent
}

type v27BlackholeRescueEventSnapshot struct {
	Total        uint64
	Dropped      uint64
	KindCounts   map[string]uint64
	ClassCounts  map[string]uint64
	ReasonCounts map[string]uint64
	Samples      []StreamRescueEvent
}

func newV27BlackholeRescueEventRecorder() *v27BlackholeRescueEventRecorder {
	return &v27BlackholeRescueEventRecorder{
		kindCounts: make(map[string]uint64), classCounts: make(map[string]uint64),
		reasonCounts: make(map[string]uint64), samples: make([]StreamRescueEvent, 0, 128),
	}
}

func (r *v27BlackholeRescueEventRecorder) record(event StreamRescueEvent) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.total++
	if event.Kind != "" {
		r.kindCounts[event.Kind]++
	}
	if class := event.Class.String(); class != "unknown" {
		r.classCounts[class]++
	}
	if event.Reason != "" {
		r.reasonCounts[event.Reason]++
	}
	if len(r.samples) < cap(r.samples) {
		r.samples = append(r.samples, event)
	} else {
		r.dropped++
	}
}

func (r *v27BlackholeRescueEventRecorder) snapshot() v27BlackholeRescueEventSnapshot {
	if r == nil {
		return v27BlackholeRescueEventSnapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return v27BlackholeRescueEventSnapshot{
		Total: r.total, Dropped: r.dropped, KindCounts: cloneStringUint64Map(r.kindCounts),
		ClassCounts: cloneStringUint64Map(r.classCounts), ReasonCounts: cloneStringUint64Map(r.reasonCounts),
		Samples: append([]StreamRescueEvent(nil), r.samples...),
	}
}

func cloneStringUint64Map(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func TestV27BlackholeRescuePolicyEnv(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
		bps  uint16
		bad  bool
	}{
		{name: "default B0", want: "b0"},
		{name: "explicit B0", env: map[string]string{v27BlackholePolicyEnv: "B0"}, want: "b0"},
		{name: "B1 300bp", env: map[string]string{v27BlackholePolicyEnv: "b1", v27BlackholeBudgetBPSEnv: "300", v27BlackholeBurstBytesEnv: "1024"}, want: "b1", bps: 300},
		{name: "reject B0 budget", env: map[string]string{v27BlackholePolicyEnv: "b0", v27BlackholeBudgetBPSEnv: "300"}, bad: true},
		{name: "reject missing B1 budget", env: map[string]string{v27BlackholePolicyEnv: "b1"}, bad: true},
		{name: "reject unsupported B1 budget", env: map[string]string{v27BlackholePolicyEnv: "b1", v27BlackholeBudgetBPSEnv: "301"}, bad: true},
		{name: "reject overlarge burst", env: map[string]string{v27BlackholePolicyEnv: "b1", v27BlackholeBudgetBPSEnv: "300", v27BlackholeBurstBytesEnv: "65537"}, bad: true},
		{name: "reject unknown policy", env: map[string]string{v27BlackholePolicyEnv: "automatic"}, bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := v27BlackholeRescuePolicyFromEnv(func(key string) string { return tc.env[key] })
			if tc.bad {
				if err == nil {
					t.Fatalf("accepted invalid configuration: %+v", policy)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if policy.Name != tc.want {
				t.Fatalf("policy=%q, want %q", policy.Name, tc.want)
			}
			if policy.Config != nil && policy.Config.BudgetBasisPoints != tc.bps {
				t.Fatalf("B1 budget=%d, want %d", policy.Config.BudgetBasisPoints, tc.bps)
			}
		})
	}
}

func TestV27BlackholePolicyMustMatchInitializedCore(t *testing.T) {
	cases := []struct {
		name   string
		policy v27BlackholeRescuePolicy
	}{
		{name: "B0", policy: v27BlackholeRescuePolicy{Name: "b0"}},
		{name: "B1", policy: v27BlackholeRescuePolicy{Name: "b1", Config: &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300, BurstBytes: 1024}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := StreamConfig{
				SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic,
				BandwidthMbps: []uint32{100, 100}, ChunkSize: v27BlackholeChunkSize,
				QueueFrames: 8, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
				AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond,
				RecoveryTimeout: 15 * time.Second, ExperimentalBoundedRescue: tc.policy.Config,
			}
			engine, app := NewStreamEngine(config)
			defer func() {
				_ = app.Close()
				engine.Close()
			}()
			if err := v27ValidateBlackholePolicyBinding(engine, tc.policy); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV27BlackholeBudgetExhaustionRequiresObservedRejection(t *testing.T) {
	stats := StreamBoundedRescueStats{
		Enabled: true, WindowBaseQuotaBytes: 100, WindowCarrierAcceptedBytes: 100,
		BudgetRejected: 1,
	}
	if !v27BlackholeBudgetExhausted(stats) {
		t.Fatal("full quota plus an observed rejection must count as exhausted")
	}
	stats.BudgetRejected = 0
	if v27BlackholeBudgetExhausted(stats) {
		t.Fatal("full quota without a rejected attempt is not evidence of exhaustion")
	}
	stats.BudgetRejected = 1
	stats.ReservedBytes = 1
	if v27BlackholeBudgetExhausted(stats) {
		t.Fatal("outstanding budget reservation must prevent an exhausted result")
	}
}

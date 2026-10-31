package smp3core

import (
	"errors"
	"testing"
	"time"
)

func phase25IdentityConfig() StreamConfig {
	return StreamConfig{
		SchedulerMode:     StreamSchedulerAggregation,
		CapacityMode:      StreamCapacityDynamic,
		MaxActiveLegs:     2,
		BandwidthMbps:     []uint32{100, 10},
		ChunkSize:         64 * 1024,
		QueueFrames:       256,
		MaxInflightFrames: 128,
		MaxReorderFrames:  256,
		AckInterval:       20 * time.Millisecond,
		RetransmitTimeout: 1500 * time.Millisecond,
		RecoveryTimeout:   15 * time.Second,
	}
}

func TestPhase25RuntimePolicyIdentityB0B1AndSessionIsolation(t *testing.T) {
	b0Cfg := phase25IdentityConfig()
	b0, b0App, err := NewStreamEngineForCandidate(b0Cfg, StreamPolicyB0)
	if err != nil {
		t.Fatal(err)
	}
	if got := b0.RuntimePolicyIdentity(); got.Candidate != StreamPolicyB0 || got.B1Enabled || got.ConfigError != "" {
		t.Fatalf("B0 runtime identity = %+v", got)
	}
	_ = b0.Close()
	_ = b0App.Close()

	b1Cfg := phase25IdentityConfig()
	b1Cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{
		Enabled: true, BudgetBasisPoints: 300, BurstBytes: 0,
	}
	b1, b1App, err := NewStreamEngineForCandidate(b1Cfg, StreamPolicyB1)
	if err != nil {
		t.Fatal(err)
	}
	defer b1.Close()
	defer b1App.Close()
	got := b1.RuntimePolicyIdentity()
	if got.Candidate != StreamPolicyB1 || !got.B1Enabled || got.BudgetBasisPoints != 300 || got.BudgetWindow != 60*time.Second || got.BurstBytes != 0 {
		t.Fatalf("B1 request was not actually applied: %+v", got)
	}
	if stats := b1.TelemetrySnapshot().BoundedRescue; !stats.Enabled || stats.BudgetBasisPoints != 300 || stats.Window != 60*time.Second {
		t.Fatalf("B1 telemetry does not match runtime identity: %+v", stats)
	}

	// A fresh B0 session must not inherit B1 state through globals or reused
	// budget managers.
	b0Again, b0AgainApp, err := NewStreamEngineForCandidate(phase25IdentityConfig(), StreamPolicyB0)
	if err != nil {
		t.Fatal(err)
	}
	defer b0Again.Close()
	defer b0AgainApp.Close()
	if got := b0Again.RuntimePolicyIdentity(); got.Candidate != StreamPolicyB0 || got.B1Enabled || got.BudgetBasisPoints != 0 {
		t.Fatalf("B1 state leaked into later B0 session: %+v", got)
	}
}

func TestPhase25RuntimePolicyIdentityFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		candidate StreamPolicyCandidate
		configure func(*StreamConfig)
	}{
		{name: "B1_missing_config", candidate: StreamPolicyB1},
		{name: "B1_disabled_config", candidate: StreamPolicyB1, configure: func(c *StreamConfig) {
			c.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: false, BudgetBasisPoints: 300}
		}},
		{name: "B1_unsupported_budget", candidate: StreamPolicyB1, configure: func(c *StreamConfig) {
			c.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 301}
		}},
		{name: "B1_burst_exceeds_chunk", candidate: StreamPolicyB1, configure: func(c *StreamConfig) {
			c.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300, BurstBytes: uint64(c.ChunkSize + 1)}
		}},
		{name: "B0_with_B1_config", candidate: StreamPolicyB0, configure: func(c *StreamConfig) {
			c.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300}
		}},
		{name: "unknown_candidate", candidate: "B2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := phase25IdentityConfig()
			if tc.configure != nil {
				tc.configure(&cfg)
			}
			engine, app, err := NewStreamEngineForCandidate(cfg, tc.candidate)
			if err == nil {
				if engine != nil {
					_ = engine.Close()
				}
				if app != nil {
					_ = app.Close()
				}
				t.Fatalf("invalid request unexpectedly constructed candidate %q", tc.candidate)
			}
			if !errors.Is(err, errStreamPolicyCandidate) {
				t.Fatalf("error %v does not identify a fail-closed candidate error", err)
			}
		})
	}
}

func TestPhase25RuntimePolicyIdentityRejectsLegacySilentFallback(t *testing.T) {
	cfg := phase25IdentityConfig()
	cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 301}
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	identity := engine.RuntimePolicyIdentity()
	if identity.Candidate != "INVALID" || identity.B1Enabled || identity.ConfigError == "" {
		t.Fatalf("invalid legacy B1 config was mislabeled as B0: %+v", identity)
	}
	if _, _, err := NewStreamEngineForCandidate(cfg, StreamPolicyB1); err == nil {
		t.Fatal("checked B1 constructor silently fell back to B0")
	}
}

package smp3core

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// StreamPolicyCandidate is an explicit per-engine experiment selection. It is
// deliberately not process-global, so one test session cannot enable B1 for a
// later B0 session.
type StreamPolicyCandidate string

const (
	StreamPolicyB0 StreamPolicyCandidate = "B0"
	StreamPolicyB1 StreamPolicyCandidate = "B1"
)

// StreamRuntimePolicyIdentity describes the policy actually instantiated by
// a StreamEngine, rather than merely echoing the caller's requested config.
type StreamRuntimePolicyIdentity struct {
	Candidate         StreamPolicyCandidate `json:"candidate"`
	B1Enabled         bool                  `json:"b1_enabled"`
	BudgetWindow      time.Duration         `json:"budget_window_ns,omitempty"`
	BudgetBasisPoints uint16                `json:"budget_basis_points,omitempty"`
	BurstBytes        uint64                `json:"burst_bytes,omitempty"`
	ConfigError       string                `json:"config_error,omitempty"`
}

var errStreamPolicyCandidate = errors.New("invalid SMP3 stream policy candidate/configuration")

// NewStreamEngineForCandidate is the fail-closed constructor for B0/B1
// experiments. Ordinary production callers should continue to use
// NewStreamEngine with a nil ExperimentalBoundedRescue field; that preserves
// the pre-existing default behavior. B1 experiments must use this constructor
// and verify RuntimePolicyIdentity before accepting samples.
func NewStreamEngineForCandidate(cfg StreamConfig, candidate StreamPolicyCandidate) (*StreamEngine, net.Conn, error) {
	switch candidate {
	case StreamPolicyB0:
		if cfg.ExperimentalBoundedRescue != nil {
			return nil, nil, fmt.Errorf("%w: B0 requires a nil bounded-rescue config", errStreamPolicyCandidate)
		}
	case StreamPolicyB1:
		if cfg.ExperimentalBoundedRescue == nil || !cfg.ExperimentalBoundedRescue.Enabled {
			return nil, nil, fmt.Errorf("%w: B1 requires explicit Enabled=true", errStreamPolicyCandidate)
		}
		chunkSize := cfg.ChunkSize
		if chunkSize <= 0 {
			chunkSize = 64 * 1024
		}
		// Validate through the same constructor used by the engine, then discard
		// the temporary manager. The live manager below is per-engine and gets a
		// fresh session identity; no shared or process-global budget is involved.
		validator, err := newStreamBoundedRescueBudget(*cfg.ExperimentalBoundedRescue, uint64(chunkSize), cfg.boundedRescueClock, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", errStreamPolicyCandidate, err)
		}
		validator.Close()
	default:
		return nil, nil, fmt.Errorf("%w: unknown candidate %q", errStreamPolicyCandidate, candidate)
	}

	engine, app := NewStreamEngine(cfg)
	identity := engine.RuntimePolicyIdentity()
	if identity.ConfigError != "" || identity.Candidate != candidate || identity.B1Enabled != (candidate == StreamPolicyB1) {
		_ = engine.Close()
		_ = app.Close()
		return nil, nil, fmt.Errorf("%w: requested %s but instantiated %+v", errStreamPolicyCandidate, candidate, identity)
	}
	if candidate == StreamPolicyB1 {
		requested := cfg.ExperimentalBoundedRescue
		if identity.BudgetBasisPoints != requested.BudgetBasisPoints || identity.BurstBytes != requested.BurstBytes || identity.BudgetWindow != streamBoundedRescueWindow {
			_ = engine.Close()
			_ = app.Close()
			return nil, nil, fmt.Errorf("%w: B1 runtime budget does not match request: requested=%+v actual=%+v", errStreamPolicyCandidate, requested, identity)
		}
	}
	return engine, app, nil
}

// RuntimePolicyIdentity returns observed per-engine policy state. Invalid or
// partially configured experimental requests are reported as INVALID rather
// than being mislabeled as B0.
func (c *StreamEngine) RuntimePolicyIdentity() StreamRuntimePolicyIdentity {
	if c == nil {
		return StreamRuntimePolicyIdentity{ConfigError: "nil StreamEngine"}
	}
	if c.boundedRescue == nil {
		if c.cfg.ExperimentalBoundedRescue != nil || c.boundedRescueConfigError != "" {
			errText := c.boundedRescueConfigError
			if errText == "" {
				errText = "bounded-rescue config was supplied but no manager is active"
			}
			return StreamRuntimePolicyIdentity{Candidate: "INVALID", ConfigError: errText}
		}
		return StreamRuntimePolicyIdentity{Candidate: StreamPolicyB0}
	}
	stats := c.boundedRescue.Snapshot(c.boundedRescueNow())
	if !stats.Enabled || stats.ConfigError != "" {
		return StreamRuntimePolicyIdentity{Candidate: "INVALID", ConfigError: stats.ConfigError}
	}
	return StreamRuntimePolicyIdentity{
		Candidate:         StreamPolicyB1,
		B1Enabled:         true,
		BudgetWindow:      stats.Window,
		BudgetBasisPoints: stats.BudgetBasisPoints,
		BurstBytes:        stats.BurstBytes,
	}
}

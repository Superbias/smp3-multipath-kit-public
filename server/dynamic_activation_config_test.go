package server

import (
	"testing"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestDynamicActivationConfigMatrix(t *testing.T) {
	cfg := testConfig()
	cfg.Stream.SchedulerMode = "aggregation"
	cfg.Stream.ActivationMode = "dynamic"
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := streamActivationMode(cfg.Stream.ActivationMode); got != smp3core.StreamActivationDynamic {
		t.Fatalf("activation mode=%v, want dynamic", got)
	}
	defaults := testConfig()
	if err := defaults.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := streamActivationMode(defaults.Stream.ActivationMode); got != smp3core.StreamActivationDynamic {
		t.Fatalf("default activation mode=%v, want dynamic", got)
	}
	invalid := testConfig()
	invalid.Stream.SchedulerMode = "adaptive"
	invalid.Stream.ActivationMode = "dynamic"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("dynamic activation without aggregation was accepted")
	}
}

func TestHOLCompletionConfigMatrix(t *testing.T) {
	cfg := testConfig()
	cfg.Stream.SchedulerMode = "aggregation"
	cfg.Stream.HOLMode = "completion"
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := streamHOLMode(cfg.Stream.HOLMode); got != smp3core.StreamHOLCompletion {
		t.Fatalf("HOL mode=%v, want completion", got)
	}
	defaults := testConfig()
	if err := defaults.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := streamHOLMode(defaults.Stream.HOLMode); got != smp3core.StreamHOLCompletion {
		t.Fatalf("default HOL mode=%v, want completion", got)
	}
	invalid := testConfig()
	invalid.Stream.SchedulerMode = "adaptive"
	invalid.Stream.HOLMode = "completion"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("HOL completion without aggregation was accepted")
	}
}

func TestServerDefaultsEnableLatestFeaturesInCanonicalCore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Password = "test-bounded-rescue-defaults"
	cfg.Stream.MaxActiveLegs = 4
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Stream.SchedulerMode != "aggregation" || cfg.Stream.CapacityMode != "dynamic" ||
		cfg.Stream.ActivationMode != "dynamic" || cfg.Stream.HOLMode != "completion" ||
		!cfg.Stream.ExperimentalPathAware || cfg.Stream.BoundedRescueMode != "bounded" ||
		cfg.Stream.BoundedRescueBudgetBasisPoints != 300 || cfg.Stream.BoundedRescueBurstBytes != 0 {
		t.Fatalf("server defaults do not enable the latest stream features: %+v", cfg.Stream)
	}
	server, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	session := server.newSession(smp3core.Hello{Mode: smp3core.ModeStream}, "127.0.0.1:8080")
	defer session.stream.Close()
	defer session.streamApp.Close()
	identity := session.stream.RuntimePolicyIdentity()
	if identity.ConfigError != "" || identity.Candidate != smp3core.StreamPolicyB1 || identity.BudgetBasisPoints != 300 {
		t.Fatalf("server session did not use canonical Bounded Rescue: %+v", identity)
	}
	if stats := session.stream.Snapshot().BoundedRescue; len(stats.AcceptedByTargetLegN) != 4 {
		t.Fatalf("server Core budget did not cover N active legs: %+v", stats)
	}
}

package client

import (
	"testing"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestDynamicActivationConfigMatrix(t *testing.T) {
	cfg := validTestConfig()
	cfg.SMP3.Stream.SchedulerMode = "aggregation"
	cfg.SMP3.Stream.ActivationMode = "dynamic"
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.SMP3.streamConfig(nil, nil).ActivationMode; got != smp3core.StreamActivationDynamic {
		t.Fatalf("activation mode=%v, want dynamic", got)
	}
	defaults := validTestConfig()
	if err := defaults.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := defaults.SMP3.streamConfig(nil, nil).ActivationMode; got != smp3core.StreamActivationDynamic {
		t.Fatalf("default activation mode=%v, want dynamic", got)
	}
	invalid := validTestConfig()
	invalid.SMP3.Stream.SchedulerMode = "adaptive"
	invalid.SMP3.Stream.ActivationMode = "dynamic"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("dynamic activation without aggregation was accepted")
	}
}

func TestHOLCompletionConfigMatrix(t *testing.T) {
	cfg := validTestConfig()
	cfg.SMP3.Stream.SchedulerMode = "aggregation"
	cfg.SMP3.Stream.HOLMode = "completion"
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.SMP3.streamConfig(nil, nil).HOLMode; got != smp3core.StreamHOLCompletion {
		t.Fatalf("HOL mode=%v, want completion", got)
	}
	defaults := validTestConfig()
	if err := defaults.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := defaults.SMP3.streamConfig(nil, nil).HOLMode; got != smp3core.StreamHOLCompletion {
		t.Fatalf("default HOL mode=%v, want completion", got)
	}
	invalid := validTestConfig()
	invalid.SMP3.Stream.SchedulerMode = "adaptive"
	invalid.SMP3.Stream.HOLMode = "completion"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("HOL completion without aggregation was accepted")
	}
}

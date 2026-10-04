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
	legacy := validTestConfig()
	if err := legacy.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := legacy.SMP3.streamConfig(nil, nil).ActivationMode; got != smp3core.StreamActivationLegacy {
		t.Fatalf("omitted activation mode=%v, want legacy", got)
	}
	invalid := validTestConfig()
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
	legacy := validTestConfig()
	if err := legacy.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := legacy.SMP3.streamConfig(nil, nil).HOLMode; got != smp3core.StreamHOLLegacy {
		t.Fatalf("omitted HOL mode=%v, want legacy", got)
	}
	invalid := validTestConfig()
	invalid.SMP3.Stream.HOLMode = "completion"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("HOL completion without aggregation was accepted")
	}
}

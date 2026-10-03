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
	legacy := testConfig()
	if err := legacy.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := streamActivationMode(legacy.Stream.ActivationMode); got != smp3core.StreamActivationLegacy {
		t.Fatalf("omitted activation mode=%v, want legacy", got)
	}
	invalid := testConfig()
	invalid.Stream.ActivationMode = "dynamic"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("dynamic activation without aggregation was accepted")
	}
}

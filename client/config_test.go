package client

import (
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestLoadConfigDefaultsAndStrictValidation(t *testing.T) {
	config := `{
  "listen": "127.0.0.1:18080",
  "upstream_socks": {"address": "127.0.0.1:7898"},
  "smp3": {
    "password": "test-password",
    "routes": {"leg0": "127.0.0.1:24441", "leg1": "127.0.0.1:24442", "leg1_fallback": "127.0.0.1:24443"}
  }
}`
	cfg, err := LoadConfig(strings.NewReader(config))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:18080" || cfg.UpstreamSocks.ConnectTimeout.Time() != 10*time.Second || cfg.SMP3.CarrierReadyTimeout.Time() != 5*time.Second || cfg.SMP3.Stream.ActivationThresholdMbps != 80 || cfg.SMP3.Stream.StartupPolicy != "first-ready" || cfg.SMP3.UDP.MaxDatagramSize != 16384 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	withTimeout := strings.Replace(config, `"address": "127.0.0.1:7898"`, `"address": "127.0.0.1:7898", "connect_timeout": "250ms"`, 1)
	custom, err := LoadConfig(strings.NewReader(withTimeout))
	if err != nil {
		t.Fatal(err)
	}
	if custom.UpstreamSocks.ConnectTimeout.Time() != 250*time.Millisecond {
		t.Fatalf("connect timeout = %s", custom.UpstreamSocks.ConnectTimeout.Time())
	}
	zeroGrace := strings.Replace(config, `"password": "test-password",`, `"password": "test-password", "stream": {"startup_policy": "preferred", "startup_grace": "0s"},`, 1)
	zero, err := LoadConfig(strings.NewReader(zeroGrace))
	if err != nil {
		t.Fatal(err)
	}
	if zero.SMP3.Stream.StartupPolicy != "preferred" || zero.SMP3.Stream.StartupGrace.Time() != 0 {
		t.Fatalf("zero startup grace = %+v", zero.SMP3.Stream)
	}

	for name, invalid := range map[string]string{
		"unknown-field":        strings.Replace(config, "\n}", ",\n  \"unknown\": true\n}", 1),
		"non-loopback":         strings.Replace(config, "127.0.0.1:18080", "0.0.0.0:18080", 1),
		"fallback-equals-leg0": strings.Replace(config, "127.0.0.1:24443", "127.0.0.1:24441", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(strings.NewReader(invalid)); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestEqualRouteTargetsRemainIndependentAndValid(t *testing.T) {
	const sameTarget = "127.0.0.1:24445"

	config := DefaultConfig()
	config.SMP3.Password = "test-password"
	config.SMP3.Routes = RouteOptions{Leg0: sameTarget, Leg1: sameTarget}
	if err := config.NormalizeAndValidate(); err != nil {
		t.Fatalf("equal leg targets rejected: %v", err)
	}
	if config.SMP3.Routes.Leg0 != sameTarget || config.SMP3.Routes.Leg1 != sameTarget {
		t.Fatalf("equal route targets were normalized: %+v", config.SMP3.Routes)
	}

	config.SMP3.Routes.Leg1Fallback = "127.0.0.1:24446"
	if err := config.NormalizeAndValidate(); err != nil {
		t.Fatalf("equal leg targets with distinct fallback rejected: %v", err)
	}

	for name, routes := range map[string]RouteOptions{
		"fallback-equals-leg0": {Leg0: sameTarget, Leg1: "127.0.0.1:24446", Leg1Fallback: sameTarget},
		"fallback-equals-leg1": {Leg0: sameTarget, Leg1: "127.0.0.1:24446", Leg1Fallback: "127.0.0.1:24446"},
		"missing-leg0":         {Leg1: sameTarget},
		"missing-leg1":         {Leg0: sameTarget},
		"malformed-leg0":       {Leg0: "127.0.0.1", Leg1: sameTarget},
		"invalid-leg1-port":    {Leg0: sameTarget, Leg1: "127.0.0.1:0"},
		"malformed-fallback":   {Leg0: sameTarget, Leg1: sameTarget, Leg1Fallback: "not-an-endpoint"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRoutes(routes); err == nil {
				t.Fatal("invalid route set was accepted")
			}
		})
	}

	distinct := RouteOptions{Leg0: "127.0.0.1:24445", Leg1: "127.0.0.1:24446", Leg1Fallback: "127.0.0.1:24447"}
	if err := validateRoutes(distinct); err != nil {
		t.Fatalf("distinct route regression failed: %v", err)
	}
}

func TestStartupConfigMapsPreferredCorePolicy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SMP3.Password = "test-password"
	cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
	cfg.SMP3.Stream.StartupPolicy = "preferred"
	cfg.SMP3.Stream.StartupPreferredLeg = 1
	cfg.SMP3.Stream.StartupGrace = NonNegativeDuration(40 * time.Millisecond)
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	stream := cfg.SMP3.streamConfig(nil, nil)
	if stream.StartupPolicy != smp3core.StreamStartupPreferred || stream.StartupPreferredLeg != 1 || stream.StartupGrace != 40*time.Millisecond {
		t.Fatalf("stream startup config=%+v", stream)
	}

	invalid := cfg
	invalid.SMP3.Stream.StartupPolicy = "invalid"
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("invalid startup policy accepted")
	}
	invalid = cfg
	invalid.SMP3.Stream.StartupPreferredLeg = 2
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("invalid startup preferred leg accepted")
	}
	invalid = cfg
	invalid.SMP3.Stream.StartupGrace = NonNegativeDuration(-time.Millisecond)
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("negative startup grace accepted")
	}
	invalid = cfg
	invalid.SMP3.Stream.StartupGrace = NonNegativeDuration(smp3core.MaxStreamStartupGrace + time.Nanosecond)
	if err := invalid.NormalizeAndValidate(); err == nil {
		t.Fatal("startup grace above Core maximum accepted")
	}
}

func TestR1411StartupCompatibilityMatrix(t *testing.T) {
	retained := `{
  "listen": "127.0.0.1:18080",
  "upstream_socks": {"address": "127.0.0.1:17898"},
  "smp3": {
    "password": "test-password",
    "routes": {"leg0": "10.66.66.1:24445", "leg1": "10.66.66.1:24445"},
    "stream": {
      "startup_policy": "preferred",
      "startup_preferred_leg": 0,
      "startup_grace": "500ms",
      "activation_threshold_mbps": 80,
      "activation_window": "1s"
    }
  }
}`
	cfg, err := LoadConfig(strings.NewReader(retained))
	if err != nil {
		t.Fatalf("retained production config: %v", err)
	}
	if cfg.SMP3.Stream.StartupPolicy != "preferred" || cfg.SMP3.Stream.StartupPreferredLeg != 0 || cfg.SMP3.Stream.StartupGrace.Time() != 500*time.Millisecond || cfg.SMP3.Stream.ActivationThresholdMbps != 80 || cfg.SMP3.Stream.ActivationWindow.Time() != time.Second {
		t.Fatalf("retained startup contract changed: %+v", cfg.SMP3.Stream)
	}
	if got := cfg.effectiveUpstream(0).Address; got != "127.0.0.1:17898" {
		t.Fatalf("retained leg0 upstream = %q", got)
	}
	if got := cfg.effectiveUpstream(1).Address; got != "127.0.0.1:17898" {
		t.Fatalf("retained leg1 upstream = %q", got)
	}

	intended := strings.Replace(retained, `"address": "127.0.0.1:17898"`, `"address": "127.0.0.1:17898", "leg0": {"address": "127.0.0.1:17898"}, "leg1": {"address": "127.0.0.1:17899"}`, 1)
	qualified, err := LoadConfig(strings.NewReader(intended))
	if err != nil {
		t.Fatalf("R14 production-shaped config: %v", err)
	}
	if got := qualified.effectiveUpstream(0).Address; got != "127.0.0.1:17898" {
		t.Fatalf("R14 leg0 upstream = %q", got)
	}
	if got := qualified.effectiveUpstream(1).Address; got != "127.0.0.1:17899" {
		t.Fatalf("R14 leg1 upstream = %q", got)
	}
	if qualified.SMP3.Routes.Leg0 != "10.66.66.1:24445" || qualified.SMP3.Routes.Leg1 != "10.66.66.1:24445" {
		t.Fatalf("same-sidecar routes changed: %+v", qualified.SMP3.Routes)
	}
	stream := qualified.SMP3.streamConfig(nil, nil)
	if stream.StartupPolicy != smp3core.StreamStartupPreferred || stream.StartupPreferredLeg != 0 || stream.StartupGrace != 500*time.Millisecond || stream.ThresholdBytesPS != 10_000_000 || stream.ActivationWindow != time.Second {
		t.Fatalf("runtime startup contract changed: %+v", stream)
	}
}

func TestPerLegUpstreamOverrideValidation(t *testing.T) {
	base := validTestConfig()
	for name, override := range map[string]*UpstreamSocksOverride{
		"empty-address": {Address: ""},
		"invalid-port":  {Address: "127.0.0.1:0"},
		"long-timeout":  {Address: "127.0.0.1:17899", ConnectTimeout: Duration(61 * time.Second)},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.UpstreamSocks.Leg1 = override
			if err := cfg.NormalizeAndValidate(); err == nil {
				t.Fatal("invalid per-leg upstream was accepted")
			}
		})
	}
}

func TestStreamSchedulerModes(t *testing.T) {
	for _, mode := range []string{"adaptive", "static", "aggregation"} {
		cfg := validTestConfig()
		cfg.SMP3.Stream.SchedulerMode = mode
		if err := cfg.NormalizeAndValidate(); err != nil {
			t.Fatalf("scheduler mode %q rejected: %v", mode, err)
		}
	}
	dynamic := validTestConfig()
	dynamic.SMP3.Stream.SchedulerMode = "aggregation"
	dynamic.SMP3.Stream.CapacityMode = "dynamic"
	if err := dynamic.NormalizeAndValidate(); err != nil {
		t.Fatalf("dynamic aggregation rejected: %v", err)
	}
	invalidCapacity := validTestConfig()
	invalidCapacity.SMP3.Stream.CapacityMode = "dynamic"
	if err := invalidCapacity.NormalizeAndValidate(); err == nil {
		t.Fatal("dynamic capacity without aggregation was accepted")
	}
	badCapacity := validTestConfig()
	badCapacity.SMP3.Stream.CapacityMode = "unknown"
	if err := badCapacity.NormalizeAndValidate(); err == nil {
		t.Fatal("invalid capacity mode accepted")
	}
	cfg := validTestConfig()
	cfg.SMP3.Stream.SchedulerMode = "invalid"
	if err := cfg.NormalizeAndValidate(); err == nil {
		t.Fatal("invalid scheduler mode accepted")
	}
	if got := streamSchedulerMode("aggregation"); got != smp3core.StreamSchedulerAggregation {
		t.Fatalf("aggregation mapping = %v", got)
	}
}

func TestLegUpstreamAddressesExposeOnlySafeBindingIdentities(t *testing.T) {
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = "127.0.0.1:10001"
	cfg.UpstreamSocks.Username = "hidden-user"
	cfg.UpstreamSocks.Password = "hidden-password"
	cfg.UpstreamSocks.Leg1 = &UpstreamSocksOverride{
		Address:  "127.0.0.1:10002",
		Username: "hidden-leg-user",
		Password: "hidden-leg-password",
	}
	addresses := cfg.LegUpstreamAddresses()
	if addresses != [2]string{"127.0.0.1:10001", "127.0.0.1:10002"} {
		t.Fatalf("safe leg upstream addresses = %v", addresses)
	}
}

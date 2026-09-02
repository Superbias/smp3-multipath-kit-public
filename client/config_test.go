package client

import (
	"strings"
	"testing"
	"time"
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
	if cfg.Listen != "127.0.0.1:18080" || cfg.UpstreamSocks.ConnectTimeout.Time() != 10*time.Second || cfg.SMP3.CarrierReadyTimeout.Time() != 5*time.Second || cfg.SMP3.Stream.ActivationThresholdMbps != 80 || cfg.SMP3.UDP.MaxDatagramSize != 16384 {
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

	for name, invalid := range map[string]string{
		"unknown-field": strings.Replace(config, "\n}", ",\n  \"unknown\": true\n}", 1),
		"non-loopback":  strings.Replace(config, "127.0.0.1:18080", "0.0.0.0:18080", 1),
		"same-routes":   strings.Replace(config, "127.0.0.1:24442", "127.0.0.1:24441", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(strings.NewReader(invalid)); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

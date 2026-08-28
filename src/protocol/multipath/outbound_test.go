package multipath

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func TestLegacyMultipathOptionsKeepAdaptiveDisabled(t *testing.T) {
	var options option.MultipathOutboundOptions
	if err := json.Unmarshal([]byte(`{
		"type":"multipath",
		"outbounds":["line-path","public-path"],
		"server":"10.66.66.1",
		"server_port":24444,
		"password":"placeholder"
	}`), &options); err != nil {
		t.Fatal(err)
	}
	if options.Leg1Fallback != "" || options.Leg1Adaptive != nil {
		t.Fatalf("legacy options unexpectedly enabled adaptive mode: %+v", options)
	}
}

func TestAdaptiveMultipathOptionsParseDefaultsAndOverrides(t *testing.T) {
	var options option.MultipathOutboundOptions
	if err := json.Unmarshal([]byte(`{
		"type":"multipath",
		"outbounds":["line-path","public-hy2"],
		"leg1_fallback":"public-snell",
		"leg1_adaptive":{"enabled":true,"evaluation_interval":"2s","goodput_degrade_ratio":0.35},
		"server":"10.66.66.1",
		"server_port":24444,
		"password":"placeholder"
	}`), &options); err != nil {
		t.Fatal(err)
	}
	if options.Leg1Adaptive == nil || !options.Leg1Adaptive.Enabled || options.Leg1Fallback != "public-snell" {
		t.Fatalf("adaptive options did not parse: %+v", options)
	}
	settings, err := makeAdaptiveSettings(*options.Leg1Adaptive)
	if err != nil {
		t.Fatal(err)
	}
	if settings.EvaluationInterval != 2*time.Second || settings.GoodputDegradeRatio != 0.35 ||
		settings.MinCanaryUsefulBytes != 1<<20 || settings.MinCanaryActiveWindows != 3 ||
		settings.InitialFailureThreshold != 3 || settings.InitialFailureWindow != 30*time.Second {
		t.Fatalf("unexpected adaptive settings: %+v", settings)
	}
}

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

func TestR11SchedulerAndDatagramOptionsParse(t *testing.T) {
	var options option.MultipathOutboundOptions
	if err := json.Unmarshal([]byte(`{
		"type":"multipath",
		"outbounds":["line-path","public-hy2"],
		"scheduler_mode":"adaptive",
		"bootstrap_fallback_delay":"250ms",
		"udp_multipath":{
			"enabled":true,
			"mode":"adaptive",
			"queue_frames":256,
			"dedup_window":4096,
			"adaptive_queue_delay":"120ms"
		},
		"server":"10.66.66.1",
		"server_port":24444,
		"password":"placeholder"
	}`), &options); err != nil {
		t.Fatal(err)
	}
	if options.SchedulerMode != "adaptive" || time.Duration(options.BootstrapFallbackDelay) != 250*time.Millisecond {
		t.Fatalf("r11 scheduler/bootstrap options did not parse: %+v", options)
	}
	if options.UDPMultipath == nil || !options.UDPMultipath.Enabled || options.UDPMultipath.Mode != "adaptive" {
		t.Fatalf("r11 datagram options did not parse: %+v", options.UDPMultipath)
	}
	cfg, err := makeDatagramConfig(options.UDPMultipath, []uint32{100, 500}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != datagramModeAdaptive || cfg.QueueFrames != 256 || cfg.DedupWindow != 4096 || cfg.AdaptiveQueueDelay != 120*time.Millisecond {
		t.Fatalf("unexpected r11 datagram config: %+v", cfg)
	}
}

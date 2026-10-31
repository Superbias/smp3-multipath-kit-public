package client

import (
	"fmt"
	"testing"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestPhase32NLegConfigBoundsAndLegacyCompatibility(t *testing.T) {
	for _, count := range []int{1, 2, 3, 4, 8, 16, smp3core.MaxStreamLegIDCount} {
		t.Run(fmt.Sprintf("legs-%d", count), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.SMP3.Password = "phase32-test"
			cfg.SMP3.Routes = RouteOptions{Legs: make([]RouteLegOptions, count)}
			cfg.SMP3.Stream.MaxActiveLegs = count
			cfg.SMP3.Stream.SchedulerMode = "aggregation"
			cfg.SMP3.Stream.CapacityMode = "dynamic"
			cfg.SMP3.Stream.BandwidthMbps = make([]uint32, count)
			for id := range cfg.SMP3.Routes.Legs {
				cfg.SMP3.Routes.Legs[id] = RouteLegOptions{ID: uint8(id), Name: fmt.Sprintf("leg-%d", id), Address: fmt.Sprintf("127.0.0.1:%d", 20000+id)}
				cfg.SMP3.Stream.BandwidthMbps[id] = 100
			}
			if err := cfg.NormalizeAndValidate(); err != nil {
				t.Fatalf("N=%d config rejected: %v", count, err)
			}
			if got := len(cfg.SMP3.Routes.entries()); got != count {
				t.Fatalf("route count=%d want %d", got, count)
			}
			if got := len(cfg.LegUpstreamAddressesN()); got != count {
				t.Fatalf("per-leg upstream count=%d want %d", got, count)
			}
		})
	}

	legacy := DefaultConfig()
	legacy.SMP3.Password = "phase32-test"
	legacy.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
	if err := legacy.NormalizeAndValidate(); err != nil {
		t.Fatalf("Phase31 dual-route config rejected: %v", err)
	}
	if got := len(legacy.SMP3.Routes.entries()); got != 2 {
		t.Fatalf("legacy two-leg config resolved to %d legs", got)
	}
}

func TestPhase32NLegConfigRejectsZeroOverCapAndUnsupportedUDP(t *testing.T) {
	base := DefaultConfig()
	base.SMP3.Password = "phase32-test"
	base.SMP3.Stream.SchedulerMode = "aggregation"
	base.SMP3.Stream.MaxActiveLegs = 2
	base.SMP3.Routes = RouteOptions{Legs: []RouteLegOptions{
		{ID: 0, Address: "127.0.0.1:24441"},
		{ID: 1, Address: "127.0.0.1:24442"},
		{ID: 2, Address: "127.0.0.1:24443"},
	}}
	if err := base.NormalizeAndValidate(); err == nil {
		t.Fatal("configured leg count above max_active_legs was accepted")
	}

	invalidIDs := DefaultConfig()
	invalidIDs.SMP3.Password = "phase32-test"
	invalidIDs.SMP3.Routes = RouteOptions{Legs: []RouteLegOptions{
		{ID: 0, Address: "127.0.0.1:24441"},
		{ID: 2, Address: "127.0.0.1:24442"},
	}}
	if err := invalidIDs.NormalizeAndValidate(); err == nil {
		t.Fatal("non-contiguous protocol Leg IDs were accepted")
	}

	udp := DefaultConfig()
	udp.SMP3.Password = "phase32-test"
	udp.SMP3.Stream.MaxActiveLegs = 3
	udp.SMP3.Routes = RouteOptions{Legs: []RouteLegOptions{
		{ID: 0, Address: "127.0.0.1:24441"},
		{ID: 1, Address: "127.0.0.1:24442"},
		{ID: 2, Address: "127.0.0.1:24443"},
	}}
	udp.SMP3.UDP.Enabled = true
	if err := udp.NormalizeAndValidate(); err == nil {
		t.Fatal("N>2 UDP configuration was not explicitly rejected")
	}
}

package outbound

import (
	"fmt"
	"testing"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestPhase32MihomoNLegConfigAndSharedCoreLimit(t *testing.T) {
	for _, count := range []int{1, 2, 3, 4, 8, 16, smp3core.MaxStreamLegIDCount} {
		t.Run(fmt.Sprintf("legs-%d", count), func(t *testing.T) {
			legs := make([]SMP3LegOption, count)
			for id := range legs {
				legs[id].Proxy = fmt.Sprintf("carrier-%d", id)
			}
			max := count
			adapter, err := NewSMP3(SMP3Option{Name: "mp", Server: "127.0.0.1", Port: 24444, Password: "phase32-test", Legs: legs, MaxActiveLegs: max, SchedulerMode: "aggregation", CapacityMode: "dynamic"})
			if err != nil {
				t.Fatalf("N=%d Native adapter rejected: %v", count, err)
			}
			if adapter.streamConfig.MaxActiveLegs != count || adapter.streamConfig.SchedulerMode != smp3core.StreamSchedulerAggregation {
				t.Fatalf("adapter did not configure canonical Core N=%d: %+v", count, adapter.streamConfig)
			}
		})
	}

	legs := []SMP3LegOption{{Proxy: "a"}, {Proxy: "b"}, {Proxy: "c"}}
	if _, err := NewSMP3(SMP3Option{Name: "mp", Server: "127.0.0.1", Port: 24444, Password: "phase32-test", Legs: legs, MaxActiveLegs: 2}); err == nil {
		t.Fatal("configured legs above max-active-legs were accepted")
	}
	if _, err := NewSMP3(SMP3Option{Name: "mp", Server: "127.0.0.1", Port: 24444, Password: "phase32-test", Legs: legs, MaxActiveLegs: 3, UDP: SMP3UDPOption{Enabled: true}}); err == nil {
		t.Fatal("N>2 UDP configuration was not explicitly rejected")
	}
}

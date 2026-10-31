package smp3core

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestClosureSharedRecovery(t *testing.T) {
	if os.Getenv("SMP3_CLOSURE_SHARED") != "1" {
		t.Skip("explicit calibrated short recovery")
	}
	for _, count := range []int{2, 4, 8} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			clocks := releaseClocks(100e6, 100e6)
			registry := NewCarrierCapacityRegistry()
			flows := make([]*releaseFlow, count)
			for i := range flows {
				flows[i] = releaseStart(t, clocks, 20*time.Millisecond, []int{0, 1}, registry, []uint32{100, 100})
			}
			defer func() {
				for _, f := range flows {
					f.close(t)
				}
			}()
			var prior, total, base uint64
			last := make([]uint64, count)
			maxPause := make([]int, count)
			pause := make([]int, count)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for sec := 1; sec <= 30; sec++ {
				<-tick.C
				total = 0
				for i, f := range flows {
					n := f.sink.bytes.Load()
					total += n
					if n == last[i] {
						pause[i]++
					} else {
						pause[i] = 0
					}
					if pause[i] > maxPause[i] {
						maxPause[i] = pause[i]
					}
					last[i] = n
				}
				if sec == 3 {
					clocks[1].rate.Store(1e6)
				}
				if sec == 7 {
					clocks[1].rate.Store(0)
				}
				if sec == 12 {
					clocks[1].rate.Store(100e6)
				}
				if sec == 20 {
					base = total
				}
				releaseLog(t, map[string]any{"flows": count, "second": sec, "goodput": float64(total-prior) * 8 / 1e6})
				prior = total
			}
			goodput := float64(total-base) * 8 / 10 / 1e6
			states := []any{}
			for _, f := range flows {
				states = append(states, releaseStats(f.tx))
				if f.tx.Snapshot().OrdinaryAdmissionState[1] != "normal" {
					t.Error("demanded flow did not exit recovery")
				}
			}
			releaseLog(t, map[string]any{"flows": count, "window_20_30_mbps": goodput, "max_per_flow_zero_seconds": maxPause, "states": states})
			if goodput < 149.97 {
				t.Errorf("recovery %.3f below unchanged 1.5B", goodput)
			}
		})
	}
}

// Compress repeated application churn without changing the full 1800s oracle.
// Restore at20s; the fixed30-40s window retains the original10s recovery offset.
func TestClosureRepeatedSharedRecovery(t *testing.T) {
	if os.Getenv("SMP3_CLOSURE_REPEAT") != "1" {
		t.Skip("explicit repeated calibrated Core recovery")
	}
	clocks := releaseClocks(100e6, 100e6)
	registry := NewCarrierCapacityRegistry()
	var flows []*releaseFlow
	var retired uint64
	add := func() {
		flows = append(flows, releaseStart(t, clocks, 20*time.Millisecond, []int{0, 1}, registry, []uint32{100, 100}))
	}
	add()
	add()
	defer func() {
		for _, f := range flows {
			f.close(t)
		}
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for cycle := 0; cycle < 6; cycle++ {
		var base, total uint64
		for sec := 1; sec <= 45; sec++ {
			<-tick.C
			if sec == 2 {
				add()
				add()
			}
			if sec == 8 {
				clocks[1].rate.Store(1e6)
			}
			if sec == 15 {
				clocks[1].rate.Store(0)
			}
			if sec == 20 {
				clocks[1].rate.Store(100e6)
			}
			total = retired
			for _, f := range flows {
				total += f.sink.bytes.Load()
			}
			if sec == 30 {
				base = total
			}
			if sec == 40 {
				rate := float64(total-base) * 8 / 10 / 1e6
				states := []any{}
				for _, f := range flows {
					states = append(states, releaseStats(f.tx))
					if f.tx.Snapshot().OrdinaryAdmissionState != [2]string{"normal", "normal"} {
						t.Error("demanded flow did not recover")
					}
				}
				releaseLog(t, map[string]any{"cycle": cycle + 1, "fixed_recovery_30_40_mbps": rate, "states": states})
				if rate < 149.97 {
					t.Errorf("cycle%d fixed recovery %.3f below original1.5B", cycle+1, rate)
				}
			}
			if sec == 42 {
				for _, f := range flows[2:] {
					f.close(t)
					retired += f.sink.bytes.Load()
				}
				flows = flows[:2]
			}
		}
	}
	for _, f := range flows {
		f.close(t)
	}
	flows = nil
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, e := range registry.entries {
		if e.refs != 0 || e.backlog != 0 {
			t.Error("final shared reference/backlog not released")
		}
	}
}

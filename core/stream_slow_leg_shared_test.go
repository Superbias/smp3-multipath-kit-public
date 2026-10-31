package smp3core

import (
	"io"
	"testing"
	"time"
)

func TestSlowLegSharedPhysicalBudget(t *testing.T) {
	for _, count := range []int{1, 2, 4, 8} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			registry := NewCarrierCapacityRegistry()
			budgets := [2]*slowTestPhysicalBudget{{rate: 100e6}, {rate: 1e6}}
			var engines []*StreamEngine
			var sinks []*slowTestSink
			var apps []io.Closer
			var exits []chan error
			defer func() {
				for _, engine := range engines {
					engine.Close()
				}
			}()
			for index := 0; index < count; index++ {
				cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128, MaxReorderFrames: 256, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second}
				cfg.CapacityProvider = registry.Provider([2]string{"fixture|down|physical0", "fixture|down|physical1"}, cfg.BandwidthMbps, cfg.ChunkSize)
				sender, app := NewStreamEngine(cfg)
				cfg.CapacityProvider = nil
				receiver, out := NewStreamEngine(cfg)
				engines = append(engines, sender, receiver)
				apps = append(apps, app)
				for id := 0; id < 2; id++ {
					a, b := newSlowTestCarrier(budgets[id].rate, budgets[id])
					if err := sender.AttachLeg(LegID(id), a, nil); err != nil {
						t.Fatal(err)
					}
					if err := receiver.AttachLeg(LegID(id), b, nil); err != nil {
						t.Fatal(err)
					}
				}
				sink := &slowTestSink{}
				sinks = append(sinks, sink)
				readDone, writeDone := make(chan error, 1), make(chan error, 1)
				exits = append(exits, readDone, writeDone)
				go func() { _, err := io.Copy(sink, out); readDone <- err }()
				go func() { _, err := io.Copy(app, &slowTestSource{}); writeDone <- err }()
			}
			start := time.Now()
			timer := time.NewTimer(6 * time.Second)
			<-timer.C
			var total uint64
			for _, sink := range sinks {
				total += sink.bytes.Load()
			}
			rate := float64(total) * 8 / time.Since(start).Seconds() / 1e6
			t.Logf("streams=%d aggregate_goodput=%.3f physical_cap=101Mbps payload=verified bytes=%d", count, rate, total)
			if rate > 105 {
				t.Fatal("shared fixture invented physical capacity")
			}
			if total < 2<<20 {
				t.Fatal("shared physical carrier failed useful progress")
			}
			for _, app := range apps {
				app.Close()
			}
			for _, engine := range engines {
				engine.Close()
			}
			for _, exit := range exits {
				select {
				case err := <-exit:
					if err != nil && err != io.ErrClosedPipe && err != io.EOF {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("shared fixture worker leaked")
				}
			}
			registry.mu.Lock()
			defer registry.mu.Unlock()
			for _, entry := range registry.entries {
				if entry.refs != 0 {
					t.Fatal("provider reference leak")
				}
			}
		})
	}
}

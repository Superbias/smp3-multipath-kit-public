package smp3core

import (
	"sync/atomic"
	"testing"
	"time"
)

type closeProbeCapacityProvider struct{ closes atomic.Int32 }

func (p *closeProbeCapacityProvider) Reset(uint8, time.Time)               {}
func (p *closeProbeCapacityProvider) Admit(uint8, uint64, bool, time.Time) {}
func (p *closeProbeCapacityProvider) Ack(uint8, uint64, time.Time)         {}
func (p *closeProbeCapacityProvider) Weight(_ uint8, base float64) float64 { return base }
func (p *closeProbeCapacityProvider) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	return
}
func (p *closeProbeCapacityProvider) Close() { p.closes.Add(1) }

func TestStreamEngineFailureReleasesCapacityProvider(t *testing.T) {
	probe := &closeProbeCapacityProvider{}
	cfg := testStreamConfig()
	cfg.SchedulerMode = StreamSchedulerAggregation
	cfg.CapacityMode = StreamCapacityDynamic
	cfg.CapacityProvider = probe
	engine, _ := NewStreamEngine(cfg)
	engine.fail(ErrStreamClosed)
	select {
	case <-engine.Done():
	case <-time.After(time.Second):
		t.Fatal("stream engine did not terminate after failure")
	}
	if got := probe.closes.Load(); got != 1 {
		t.Fatalf("capacity provider Close calls=%d, want 1", got)
	}
	_ = engine.Close()
	if got := probe.closes.Load(); got != 1 {
		t.Fatalf("capacity provider Close calls after repeated close=%d, want 1", got)
	}
}

func TestCarrierCapacityRegistrySharesCarrierAndIsolatesKeys(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	keys := [2]string{"owner-a|out|carrier-0", "owner-a|out|carrier-1"}
	p1 := r.Provider(keys, []uint32{100, 200}, 1024)
	p2 := r.Provider(keys, []uint32{100, 200}, 1024)
	defer p1.(interface{ Close() }).Close()
	defer p2.(interface{ Close() }).Close()
	start := time.Unix(100, 0)
	// Both logical streams contribute to one aggregate window. The first ACK
	// stays inside the window; the second closes it with both streams included.
	p1.Admit(0, 4096, true, start)
	p2.Admit(0, 4096, true, start.Add(100*time.Millisecond))
	p1.Ack(0, 4096, start.Add(500*time.Millisecond))
	p2.Ack(0, 4096, start.Add(1100*time.Millisecond))
	raw, estimate, _, valid, _, _ := p1.Telemetry()
	if !valid[0] || raw[0] <= 0 || estimate[0] <= 0 {
		t.Fatalf("shared carrier did not produce aggregate sample: raw=%v estimate=%v valid=%v", raw, estimate, valid)
	}
	if got := len(r.entries); got != 2 {
		t.Fatalf("registry entries=%d, want two isolated physical legs", got)
	}
	other := r.Provider([2]string{"owner-b|out|carrier-0", "owner-b|out|carrier-1"}, []uint32{100, 200}, 1024)
	defer other.(interface{ Close() }).Close()
	if other == p1 {
		t.Fatal("different owner unexpectedly reused provider")
	}
}

func TestCarrierCapacityRegistryWarmReuseAndBoundedPrune(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	p := r.Provider([2]string{"warm-0", "warm-1"}, []uint32{100, 100}, 1024)
	p.(interface{ Close() }).Close()
	r.mu.Lock()
	r.entries["expired"] = &carrierCapacityEntry{estimator: newStreamCapacityEstimator(nil, 1024), lastUsed: time.Now().Add(-carrierCapacityIdleTTL - time.Second)}
	r.mu.Unlock()
	_ = r.Provider([2]string{"fresh-0", "fresh-1"}, []uint32{100, 100}, 1024)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries["expired"]; ok {
		t.Fatal("expired idle carrier state was not pruned")
	}
}

func TestCarrierCapacityRegistryThousandLifecycleBound(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	for i := 0; i < 1200; i++ {
		key0 := "churn|out|carrier-0|" + string(rune(i))
		key1 := "churn|out|carrier-1|" + string(rune(i))
		p := r.Provider([2]string{key0, key1}, []uint32{100, 100}, 1024)
		p.(interface{ Close() }).Close()
	}
	r.mu.Lock()
	entries := len(r.entries)
	r.mu.Unlock()
	if entries > carrierCapacityMaxEntries {
		t.Fatalf("registry grew beyond bound: %d", entries)
	}
	t.Logf("churn lifecycles=1200 registry_entries=%d cap=%d", entries, carrierCapacityMaxEntries)
}

func TestCarrierCapacityRegistryConcurrentStreamCounts(t *testing.T) {
	for _, count := range []int{1, 2, 4, 8} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			r := NewCarrierCapacityRegistry()
			providers := make([]StreamCapacityProvider, count)
			keys := [2]string{"matrix|out|leg0", "matrix|out|leg1"}
			for i := range providers {
				providers[i] = r.Provider(keys, []uint32{100, 100}, 1024)
			}
			start := time.Unix(200, 0)
			perStreamBytes := uint64(12500000 / count)
			for window := 0; window < 5; window++ {
				base := start.Add(time.Duration(window) * 1100 * time.Millisecond)
				for i, p := range providers {
					p.Admit(0, perStreamBytes, true, base.Add(time.Duration(i)*10*time.Millisecond))
					p.Ack(0, perStreamBytes, base.Add(200*time.Millisecond+time.Duration(i)*10*time.Millisecond))
				}
				// Trigger rollover without materially changing the aggregate
				// useful-byte total for the completed window.
				providers[0].Ack(0, 1, base.Add(1050*time.Millisecond))
			}
			_, _, _, valid, _, _ := providers[0].Telemetry()
			if !valid[0] {
				t.Fatalf("%d-stream aggregate did not converge", count)
			}
			raw, estimate, _, _, _, _ := providers[0].Telemetry()
			t.Logf("streams=%d configured_mbps=100 aggregate_useful_mbps=%.1f per_stream_useful_mbps=%.1f global_estimate_mbps=%.1f estimate_error_pct=%.1f active_streams=%d", count, float64(perStreamBytes*uint64(count)*8)/1e6, float64(perStreamBytes*8)/1e6, estimate[0]*8/1e6, (estimate[0]-100*1e6/8)/(100*1e6/8)*100, count)
			_ = raw
			for _, p := range providers {
				p.(interface{ Close() }).Close()
			}
		})
	}
}

func TestCarrierCapacityRegistryAsymmetricFiftyTwoHundred(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	keys := [2]string{"asym|out|50", "asym|out|200"}
	p := r.Provider(keys, []uint32{50, 200}, 1024)
	defer p.(interface{ Close() }).Close()
	start := time.Unix(400, 0)
	leg0Bytes := uint64(6250000 / 4)
	leg1Bytes := uint64(25000000 / 4)
	for i := 0; i < 4; i++ {
		p.Admit(0, leg0Bytes, true, start.Add(time.Duration(i)*20*time.Millisecond))
		p.Admit(1, leg1Bytes, true, start.Add(time.Duration(i)*20*time.Millisecond))
		p.Ack(0, leg0Bytes, start.Add(300*time.Millisecond+time.Duration(i)*20*time.Millisecond))
		p.Ack(1, leg1Bytes, start.Add(300*time.Millisecond+time.Duration(i)*20*time.Millisecond))
	}
	p.Ack(0, leg0Bytes, start.Add(1300*time.Millisecond))
	p.Ack(1, leg1Bytes, start.Add(1300*time.Millisecond))
	raw, estimate, _, valid, _, _ := p.Telemetry()
	if !valid[0] || !valid[1] {
		t.Fatalf("asymmetric carrier did not converge: valid=%v raw=%v", valid, raw)
	}
	share0 := estimate[0] / (estimate[0] + estimate[1])
	t.Logf("configured_mbps=[50 200] global_estimate_mbps=[%.1f %.1f] aggregate_assignment_share=[%.3f %.3f] total_useful_bytes=%d", estimate[0]*8/1e6, estimate[1]*8/1e6, share0, 1-share0, 4*(leg0Bytes+leg1Bytes))
}

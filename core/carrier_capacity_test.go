package smp3core

import (
	"testing"
	"time"
)

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
			for i, p := range providers {
				p.Admit(0, 4096, true, start.Add(time.Duration(i)*10*time.Millisecond))
				p.Ack(0, 4096, start.Add(200*time.Millisecond+time.Duration(i)*10*time.Millisecond))
			}
			providers[0].Ack(0, 4096, start.Add(1200*time.Millisecond))
			_, _, _, valid, _, _ := providers[0].Telemetry()
			if !valid[0] {
				t.Fatalf("%d-stream aggregate did not converge", count)
			}
			for _, p := range providers {
				p.(interface{ Close() }).Close()
			}
		})
	}
}

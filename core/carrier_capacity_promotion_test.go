package smp3core

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// feedCarrierWindow supplies a fixed aggregate useful service window shared
// among logical providers. Virtual timestamps test estimator causality, not
// application throughput or host CPU.
func feedCarrierWindow(ps []StreamCapacityProvider, shares []float64, mbps float64, saturated bool, at time.Time) {
	for i, p := range ps {
		n := uint64(mbps * 1e6 / 8 * shares[i])
		if n == 0 {
			continue
		}
		p.Admit(0, n, saturated, at)
		p.Ack(0, n, at.Add(500*time.Millisecond))
	}
	ps[0].Ack(0, 1, at.Add(time.Second))
}
func carrierMbps(p StreamCapacityProvider) float64 {
	_, e, _, _, _, _ := p.Telemetry()
	return e[0] * 8 / 1e6
}
func requireCarrierMbps(t *testing.T, p StreamCapacityProvider, want, tolerance float64) {
	t.Helper()
	got := carrierMbps(p)
	if math.Abs(got-want) > tolerance {
		t.Fatalf("estimate %.4f Mbps, want %.4f +/- %.4f", got, want, tolerance)
	}
}
func TestGlobalCarrierPromotionMixedJoinLeaveWarmSteps(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	keys := [2]string{"promotion|out|200", "promotion|out|secondary"}
	makeProvider := func() StreamCapacityProvider { return r.Provider(keys, []uint32{200, 50}, 1024) }
	p := makeProvider()
	defer p.(*carrierCapacityProvider).Close()
	at := time.Unix(20000, 0)
	for i := 0; i < 6; i++ {
		feedCarrierWindow([]StreamCapacityProvider{p}, []float64{1}, 200, true, at)
		at = at.Add(time.Second)
	}
	requireCarrierMbps(t, p, 200, 0.01)
	ps := []StreamCapacityProvider{p, makeProvider(), makeProvider(), makeProvider()}
	defer func() {
		for _, q := range ps[1:] {
			q.(*carrierCapacityProvider).Close()
		}
	}()
	// Bulk, medium, low-rate and intermittent demand. Aggregate saturation is
	// asserted only for a window whose bulk contribution is >=80% of demand.
	for i := 0; i < 5; i++ {
		shares := []float64{0.875, 0.10, 0.025, 0}
		if i%2 == 0 {
			shares = []float64{0.85, 0.10, 0.025, 0.025}
		}
		for j, q := range ps {
			n := uint64(200e6 / 8 * shares[j])
			if n == 0 {
				continue
			}
			q.Admit(0, n, j == 0, at)
			q.Ack(0, n, at.Add(500*time.Millisecond))
		}
		ps[0].Ack(0, 1, at.Add(time.Second))
		at = at.Add(time.Second)
	}
	for _, q := range ps {
		requireCarrierMbps(t, q, 200, 0.01)
	}
	t.Logf("mixed demand and join streams=4 global_mbps=%.4f", carrierMbps(p))
	for _, q := range ps[1:] {
		q.(*carrierCapacityProvider).Close()
	}
	feedCarrierWindow([]StreamCapacityProvider{p}, []float64{1}, 200, true, at)
	at = at.Add(time.Second)
	requireCarrierMbps(t, p, 200, 0.01)
	p.(*carrierCapacityProvider).Close()
	warm := makeProvider()
	defer warm.(*carrierCapacityProvider).Close()
	requireCarrierMbps(t, warm, 200, 0.01)
	before := warm.Weight(0, 200)
	warm.Reset(0, at.Add(100*time.Millisecond))
	if warm.Weight(0, 200) != before {
		t.Fatal("warm attach discarded shared knowledge")
	}
	// Ten low-demand streams: 5 Mbps each on a previously trained 200 Mbps carrier.
	low := make([]StreamCapacityProvider, 10)
	shares := make([]float64, 10)
	for i := range low {
		low[i] = makeProvider()
		shares[i] = 0.1
		defer low[i].(*carrierCapacityProvider).Close()
	}
	for i := 0; i < 5; i++ {
		feedCarrierWindow(low, shares, 50, false, at)
		at = at.Add(time.Second)
	}
	requireCarrierMbps(t, warm, 200, 0.01)
	t.Logf("leave/warm/demand-limited streams=10 demand_mbps=50 estimate_mbps=%.4f", carrierMbps(warm))
	// Every live provider sees the same step, and has the same weight.
	for _, target := range []float64{80, 200} {
		for i := 0; i < 7; i++ {
			feedCarrierWindow(low, shares, target, true, at)
			at = at.Add(time.Second)
			if i == 0 {
				t.Logf("step target=%.0f first_window_mbps=%.4f delay=1s", target, carrierMbps(warm))
			}
		}
		requireCarrierMbps(t, warm, target, 0.25)
		for _, q := range low {
			if q.Weight(0, 200) != warm.Weight(0, 200) {
				t.Fatal("shared weights diverged")
			}
		}
		t.Logf("step target=%.0f settled_mbps=%.4f settling_windows=7", target, carrierMbps(warm))
	}
	// Stale policy: estimator retains capacity knowledge; idle registry pruning
	// evicts only after all references have been released (tested below).
	_, _, _, _, _, sampled := warm.Telemetry()
	feedCarrierWindow([]StreamCapacityProvider{warm}, []float64{1}, 0.01, false, at.Add(time.Hour))
	_, _, _, _, _, after := warm.Telemetry()
	if after[0] != sampled[0] {
		t.Fatal("underload refreshed stale valid sample")
	}
}
func TestGlobalCarrierPromotionIsolationRetryAndPrune(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	at := time.Unix(30000, 0)
	keys := [][2]string{{"ownerA|out|50", "ownerA|out|200"}, {"ownerB|out|100", "ownerB|out|100b"}, {"ownerA|in|50", "ownerA|in|200"}}
	ps := make([]StreamCapacityProvider, len(keys))
	for i, k := range keys {
		ps[i] = r.Provider(k, []uint32{200, 200}, 1024)
	}
	feedCarrierWindow(ps[:1], []float64{1}, 50, true, at)
	requireCarrierMbps(t, ps[0], 50, 0.01)
	requireCarrierMbps(t, ps[1], 200, 0.01)
	requireCarrierMbps(t, ps[2], 200, 0.01)
	// Physical repair does not enter Ack. Extra admissions with a single useful
	// retirement must not inflate the capacity numerator.
	ps[0].Admit(0, 6250000*3, true, at.Add(time.Second))
	ps[0].Ack(0, 6250000, at.Add(2*time.Second))
	requireCarrierMbps(t, ps[0], 50, 0.01)
	for _, p := range ps {
		p.(*carrierCapacityProvider).Close()
	}
	r.mu.Lock()
	for _, e := range r.entries {
		if e.refs != 0 {
			t.Fatal("reference leak")
		}
		e.lastUsed = time.Now().Add(-carrierCapacityIdleTTL - time.Second)
	}
	r.pruneLocked(time.Now())
	n := len(r.entries)
	r.mu.Unlock()
	if n != 0 {
		t.Fatalf("stale idle registry not pruned: %d", n)
	}
	t.Log("owner/direction/group isolation, repair accounting, stale idle prune PASS")
}
func TestGlobalCarrierPromotionConcurrentLifecycle(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				p := r.Provider([2]string{"stress|out|shared", fmt.Sprintf("stress|out|secondary%d", worker)}, []uint32{200, 50}, 1024)
				at := time.Now()
				p.Reset(0, at)
				p.Admit(0, 1<<20, true, at)
				p.Ack(0, 1<<20, at.Add(time.Second))
				p.Weight(0, 200)
				p.Telemetry()
				p.(StreamActivationProvider).ObserveActivationDemand(1024, 1024, at)
				p.(*carrierCapacityProvider).Close()
				p.(*carrierCapacityProvider).Close()
				r.mu.Lock()
				r.pruneLocked(time.Now())
				r.mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		if e.refs != 0 {
			t.Fatalf("refs=%d", e.refs)
		}
	}
	if len(r.entries) > carrierCapacityMaxEntries {
		t.Fatal("registry bound exceeded")
	}
	t.Logf("concurrent lifecycles=1200 entries=%d refs=0", len(r.entries))
}

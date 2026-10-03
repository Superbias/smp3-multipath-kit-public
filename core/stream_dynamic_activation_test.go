package smp3core

import (
	"net"
	"sync"
	"testing"
	"time"
)

type activationTestProvider struct {
	mu       sync.Mutex
	capacity float64
	demand   float64
	backlog  uint64
	valid    bool
	conf     float64
}

func (p *activationTestProvider) Reset(uint8, time.Time)               {}
func (p *activationTestProvider) Admit(uint8, uint64, bool, time.Time) {}
func (p *activationTestProvider) Ack(uint8, uint64, time.Time)         {}
func (p *activationTestProvider) Weight(_ uint8, base float64) float64 { return base }
func (p *activationTestProvider) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	estimate[0], confidence[0], valid[0] = p.capacity, p.conf, p.valid
	return
}
func (p *activationTestProvider) ActivationCapacity() (float64, float64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.capacity, p.conf, p.valid
}
func (p *activationTestProvider) ObserveActivationDemand(produced, retired uint64, _ time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backlog += produced
	if retired >= p.backlog {
		p.backlog = 0
	} else {
		p.backlog -= retired
	}
}
func (p *activationTestProvider) ActivationDemand(time.Time) (float64, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.demand, p.backlog
}
func (p *activationTestProvider) Close() {}

func newActivationTestEngine(t *testing.T, provider StreamCapacityProvider, onActivate func()) (*StreamEngine, net.Conn) {
	t.Helper()
	engine, app := NewStreamEngine(StreamConfig{
		SchedulerMode:    StreamSchedulerAggregation,
		CapacityMode:     StreamCapacityFixed,
		ActivationMode:   StreamActivationDynamic,
		CapacityProvider: provider,
		BandwidthMbps:    []uint32{50, 200},
		ActivationWindow: 100 * time.Millisecond,
		QueueFrames:      32,
		ChunkSize:        1024,
		OnActivate:       onActivate,
	})
	left, right := net.Pipe()
	if err := engine.AttachLeg(0, left, nil); err != nil {
		_ = left.Close()
		_ = right.Close()
		engine.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = right.Close()
		_ = engine.Close()
		_ = app.Close()
	})
	return engine, right
}

func TestDynamicActivationRelativeToCapacity(t *testing.T) {
	cases := []struct {
		name    string
		capMbps float64
		demand  float64
		want    bool
	}{
		{name: "low demand", capMbps: 100, demand: 20, want: false},
		{name: "near capacity", capMbps: 100, demand: 105, want: false},
		{name: "just above", capMbps: 50, demand: 60, want: true},
		{name: "strong overload", capMbps: 50, demand: 150, want: true},
		{name: "high capacity primary", capMbps: 200, demand: 100, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			activated := make(chan struct{}, 1)
			p := &activationTestProvider{capacity: tc.capMbps * 1e6 / 8, demand: tc.demand * 1e6 / 8, valid: true, conf: 1}
			engine, peer := newActivationTestEngine(t, p, func() { activated <- struct{}{} })
			_ = peer
			select {
			case <-activated:
				if !tc.want {
					t.Fatalf("unexpected activation: stats=%+v", engine.Snapshot())
				}
			case <-time.After(500 * time.Millisecond):
				if tc.want {
					t.Fatalf("expected activation: stats=%+v", engine.Snapshot())
				}
			}
		})
	}
}

func TestCarrierActivationDemandAggregatesStreams(t *testing.T) {
	r := NewCarrierCapacityRegistry()
	keys := [2]string{"activation-shared-leg0", "activation-shared-leg1"}
	p1 := r.Provider(keys, []uint32{100, 100}, 1024)
	p2 := r.Provider(keys, []uint32{100, 100}, 1024)
	defer p1.(interface{ Close() }).Close()
	defer p2.(interface{ Close() }).Close()
	a := p1.(StreamActivationProvider)
	b := p2.(StreamActivationProvider)
	now := time.Now()
	a.ObserveActivationDemand(40<<20/8, 0, now)
	b.ObserveActivationDemand(40<<20/8, 0, now)
	rate, backlog := a.ActivationDemand(now.Add(100 * time.Millisecond))
	if rate < 79<<20/8 || backlog != 80<<20/8 {
		t.Fatalf("aggregate demand rate=%v backlog=%d, want about 80Mbps and 10MiB", rate, backlog)
	}
	// A third stream pushes the shared physical carrier over its capacity.
	b.ObserveActivationDemand(50<<20/8, 0, now)
	rate, _ = a.ActivationDemand(now.Add(100 * time.Millisecond))
	if rate < 120<<20/8 {
		t.Fatalf("aggregate step-up rate=%v, want at least 120Mbps", rate)
	}
}

func TestDynamicActivationCapacityDropAndStepUp(t *testing.T) {
	activated := make(chan struct{}, 1)
	p := &activationTestProvider{capacity: 100e6 / 8, demand: 70e6 / 8, valid: true, conf: 1}
	engine, _ := newActivationTestEngine(t, p, func() { activated <- struct{}{} })
	_ = engine
	select {
	case <-activated:
		t.Fatal("secondary activated while 70 Mbps fit within 100 Mbps")
	case <-time.After(150 * time.Millisecond):
	}
	p.mu.Lock()
	p.capacity = 40e6 / 8
	p.mu.Unlock()
	select {
	case <-activated:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("capacity drop did not activate secondary")
	}
}

func TestDynamicActivationSuppressesShortBurst(t *testing.T) {
	activated := make(chan struct{}, 1)
	p := &activationTestProvider{capacity: 100e6 / 8, demand: 150e6 / 8, valid: true, conf: 1}
	engine, _ := newActivationTestEngine(t, p, func() { activated <- struct{}{} })
	_ = engine
	select {
	case <-activated:
		t.Fatal("short burst activated before the evidence window")
	case <-time.After(80 * time.Millisecond):
	}
	p.mu.Lock()
	p.demand = 0
	p.mu.Unlock()
	select {
	case <-activated:
		t.Fatal("short burst activated after demand had ended")
	case <-time.After(350 * time.Millisecond):
	}
}

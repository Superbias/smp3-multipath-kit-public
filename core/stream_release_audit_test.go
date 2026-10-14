package smp3core

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test-only calibrated clock: cumulative deadlines retain sub-millisecond
// timing error. At most one chunk of late credit, shared across all streams.
type releaseClock struct {
	mu   sync.Mutex
	next time.Time
	rate atomic.Int64
}

func (b *releaseClock) reserve(n int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.rate.Load()
	if r <= 0 {
		return time.Second
	}
	d := time.Duration(float64(n) * 8 / float64(r) * float64(time.Second))
	now := time.Now()
	// DATA headers are tiny units. Limiting late credit to the current header
	// would reset the clock every frame and reintroduce timer bias. The bound
	// is one fixed source chunk, including across shared flows.
	credit := time.Duration(float64(64<<10) * 8 / float64(r) * float64(time.Second))
	if b.next.Before(now.Add(-credit)) {
		b.next = now
	}
	b.next = b.next.Add(d)
	return time.Until(b.next)
}

type releaseCarrier struct {
	net.Conn
	tx       chan []byte
	delivery chan benchmarkDelivery
	done     chan struct{}
	once     sync.Once
	workers  sync.WaitGroup
	clock    *releaseClock
	delay    time.Duration
}

func releasePair(clock *releaseClock, rtt time.Duration) (*releaseCarrier, *releaseCarrier) {
	a, b := net.Pipe()
	reverse := &releaseClock{}
	reverse.rate.Store(10_000_000_000)
	return releaseWrap(a, clock, rtt/2), releaseWrap(b, reverse, rtt/2)
}
func releaseWrap(conn net.Conn, clock *releaseClock, delay time.Duration) *releaseCarrier {
	c := &releaseCarrier{Conn: conn, clock: clock, delay: delay, tx: make(chan []byte, 2), delivery: make(chan benchmarkDelivery, 1024), done: make(chan struct{})}
	c.workers.Add(2)
	go func() {
		defer c.workers.Done()
		for {
			var p []byte
			select {
			case <-c.done:
				return
			case p = <-c.tx:
			}
			for clock.rate.Load() == 0 {
				if !c.wait(20 * time.Millisecond) {
					return
				}
			}
			if !c.wait(clock.reserve(len(p))) {
				return
			}
			select {
			case <-c.done:
				return
			case c.delivery <- benchmarkDelivery{data: p, ready: time.Now().Add(delay)}:
			}
		}
	}()
	go func() {
		defer c.workers.Done()
		for {
			select {
			case <-c.done:
				return
			case item := <-c.delivery:
				if !c.wait(time.Until(item.ready)) {
					return
				}
				if writeAll(c.Conn, item.data) != nil {
					return
				}
			}
		}
	}()
	return c
}
func (c *releaseCarrier) wait(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-c.done:
		return false
	case <-timer.C:
		return true
	}
}
func (c *releaseCarrier) Write(p []byte) (int, error) {
	if len(p) > 1<<20 {
		return 0, fmt.Errorf("fixture unit too large")
	}
	b := append([]byte(nil), p...)
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case c.tx <- b:
		return len(p), nil
	}
}
func (c *releaseCarrier) Close() error {
	c.once.Do(func() { close(c.done); c.Conn.Close(); c.workers.Wait() })
	return nil
}

type releaseFlow struct {
	tx, rx   *StreamEngine
	app, out net.Conn
	sink     *slowTestSink
	exits    [2]chan error
}

func releaseStart(t *testing.T, clocks [2]*releaseClock, rtt time.Duration, legs []int, registry *CarrierCapacityRegistry, weights []uint32) *releaseFlow {
	t.Helper()
	cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: weights, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128, MaxReorderFrames: 256, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second}
	return releaseStartWithConfig(t, clocks, rtt, legs, registry, cfg)
}

func releaseStartWithConfig(t *testing.T, clocks [2]*releaseClock, rtt time.Duration, legs []int, registry *CarrierCapacityRegistry, cfg StreamConfig) *releaseFlow {
	t.Helper()
	txConfig, rxConfig := cfg, cfg
	if registry != nil {
		txConfig.CapacityProvider = registry.Provider([2]string{"release|down|0", "release|down|1"}, cfg.BandwidthMbps, cfg.ChunkSize)
	}
	rxConfig.CapacityProvider = nil
	return releaseStartWithConfigs(t, clocks, rtt, legs, txConfig, rxConfig)
}

func releaseStartWithConfigs(t *testing.T, clocks [2]*releaseClock, rtt time.Duration, legs []int, txConfig, rxConfig StreamConfig) *releaseFlow {
	t.Helper()
	tx, app := NewStreamEngine(txConfig)
	rx, out := NewStreamEngine(rxConfig)
	f := &releaseFlow{tx: tx, rx: rx, app: app, out: out, sink: &slowTestSink{}, exits: [2]chan error{make(chan error, 1), make(chan error, 1)}}
	for _, id := range legs {
		a, b := releasePair(clocks[id], rtt)
		if err := tx.AttachLeg(LegID(id), a, nil); err != nil {
			t.Fatal(err)
		}
		if err := rx.AttachLeg(LegID(id), b, nil); err != nil {
			t.Fatal(err)
		}
	}
	go func() { _, e := io.Copy(f.sink, out); f.exits[0] <- e }()
	go func() { _, e := io.Copy(app, &slowTestSource{}); f.exits[1] <- e }()
	return f
}
func (f *releaseFlow) close(t *testing.T) {
	t.Helper()
	f.app.Close()
	f.out.Close()
	f.tx.Close()
	f.rx.Close()
	for _, ch := range f.exits {
		select {
		case e := <-ch:
			if e != nil && e != net.ErrClosed && e != io.ErrClosedPipe && e != io.EOF {
				t.Errorf("flow exit: %v", e)
			}
		case <-time.After(5 * time.Second):
			t.Error("flow close timeout")
		}
	}
}
func releaseStats(e *StreamEngine) map[string]any {
	b, _ := json.Marshal(e.Snapshot())
	var all map[string]any
	json.Unmarshal(b, &all)
	m := map[string]any{}
	for _, k := range []string{"CapacityWeight", "CapacityPending", "OrdinaryProbeLimit", "OrdinaryProbeByteLimit", "OrdinaryProofBytes", "OrdinaryProbeServiceBPS", "OrdinarySessionServiceBPS", "OrdinaryFeedbackEnvelopeNS", "OrdinaryAdmissionReason", "OrdinaryAdmissionState", "OrdinaryOutstandingFrames", "OrdinaryOutstandingBytes", "OrdinaryAssignedBytes", "TxWrittenBytesByLeg", "TxRetryWrittenBytesByLeg", "TxAckedUsefulTotal", "OutstandingFrames", "FrontierRescueAttempts", "RxPendingFrames", "LegGeneration"} {
		if v, ok := all[k]; ok {
			m[k] = v
		}
	}
	e.admission.mu.Lock()
	m["opening_rate"] = [2]float64{e.admission.legs[0].openingRate, e.admission.legs[1].openingRate}
	m["feedback_min"] = [2]time.Duration{e.admission.legs[0].feedback, e.admission.legs[1].feedback}
	e.admission.mu.Unlock()
	m["byte_budget"] = [2]uint64{e.ordinaryByteBudget(0), e.ordinaryByteBudget(1)}
	return m
}
func releaseProcessResources() map[string]any {
	m := map[string]any{"scope": "current Core test process"}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			for _, key := range []string{"VmRSS", "VmSize", "Threads"} {
				if strings.HasPrefix(line, key+":") {
					m[key] = strings.TrimSpace(strings.TrimPrefix(line, key+":"))
				}
			}
		}
	} else {
		m["status"] = "NOT AVAILABLE"
	}
	if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
		m["fds"] = len(fds)
	}
	return m
}
func releaseLog(t *testing.T, v any) { b, _ := json.Marshal(v); t.Log("AUDIT " + string(b)) }
func releaseClocks(a, b int64) [2]*releaseClock {
	c := [2]*releaseClock{{}, {}}
	c[0].rate.Store(a)
	c[1].rate.Store(b)
	return c
}

func TestReleaseRTTMatrix(t *testing.T) {
	if os.Getenv("SMP3_RELEASE_MATRIX") != "1" {
		t.Skip("explicit release audit")
	}
	duration := 40 * time.Second
	for _, rtt := range []time.Duration{20, 100, 200, 400} {
		for _, kind := range []string{"single", "healthy", "slow", "recovery"} {
			t.Run(fmt.Sprintf("%dms_%s", rtt, kind), func(t *testing.T) {
				clocks := releaseClocks(100e6, 100e6)
				legs := []int{0, 1}
				if kind == "single" {
					legs = []int{0}
				}
				if kind == "slow" || kind == "recovery" {
					clocks[1].rate.Store(1e6)
				}
				f := releaseStart(t, clocks, rtt*time.Millisecond, legs, nil, []uint32{100, 100})
				defer f.close(t)
				start := time.Now()
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				var last, base uint64
				var windows []float64
				for now := range ticker.C {
					sec := int(now.Sub(start).Seconds())
					n := f.sink.bytes.Load()
					releaseLog(t, map[string]any{"kind": kind, "rtt_ms": rtt, "second": sec, "delivered": n, "goodput_mbps": float64(n-last) * 8 / 1e6, "tx": releaseStats(f.tx), "rx": releaseStats(f.rx)})
					last = n
					if sec%10 == 0 {
						windows = append(windows, float64(n-base)*8/10/1e6)
						base = n
					}
					if kind == "recovery" && sec == 20 {
						clocks[1].rate.Store(100e6)
					}
					if now.Sub(start) >= duration {
						break
					}
				}
				releaseLog(t, map[string]any{"result": "prefix_verified", "kind": kind, "rtt_ms": rtt, "windows": windows})
				if f.sink.bytes.Load() < 2<<20 {
					t.Error("no useful progress")
				}
			})
		}
	}
}

func TestReleaseCarrierCalibration(t *testing.T) {
	if os.Getenv("SMP3_RELEASE_CALIBRATION") != "1" {
		t.Skip("explicit release audit")
	}
	for _, kind := range []string{"raw50", "raw200", "raw_both", "core50", "core200", "core_both"} {
		t.Run(kind, func(t *testing.T) {
			clocks := releaseClocks(50e6, 200e6)
			start := time.Now()
			var sinks []*slowTestSink
			var flows []*releaseFlow
			var closeRaw []func()
			if kind[:3] == "raw" {
				ids := []int{0, 1}
				if kind == "raw50" {
					ids = []int{0}
				}
				if kind == "raw200" {
					ids = []int{1}
				}
				for _, id := range ids {
					a, b := releasePair(clocks[id], 20*time.Millisecond)
					s := &slowTestSink{}
					sinks = append(sinks, s)
					go io.Copy(a, &slowTestSource{})
					go io.Copy(s, b)
					closeRaw = append(closeRaw, func() { a.Close(); b.Close() })
				}
			} else {
				legs := []int{0, 1}
				if kind == "core50" {
					legs = []int{0}
				}
				if kind == "core200" {
					legs = []int{1}
				}
				f := releaseStart(t, clocks, 20*time.Millisecond, legs, nil, []uint32{50, 200})
				flows = append(flows, f)
				sinks = append(sinks, f.sink)
			}
			defer func() {
				for _, fn := range closeRaw {
					fn()
				}
				for _, f := range flows {
					f.close(t)
				}
			}()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			var base uint64
			var windows []float64
			for now := range ticker.C {
				sec := int(now.Sub(start).Seconds())
				var n uint64
				for _, s := range sinks {
					n += s.bytes.Load()
				}
				if sec%10 == 0 {
					windows = append(windows, float64(n-base)*8/10/1e6)
					base = n
				}
				if now.Sub(start) >= 40*time.Second {
					break
				}
			}
			releaseLog(t, map[string]any{"kind": kind, "windows": windows, "payload": "prefix_verified"})
			for _, f := range flows {
				releaseLog(t, map[string]any{"kind": kind, "tx": releaseStats(f.tx)})
			}
		})
	}
}

func TestReleaseSharedSoak(t *testing.T) {
	value := os.Getenv("SMP3_RELEASE_SOAK")
	if value == "" {
		t.Skip("explicit long audit")
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		t.Fatal(err)
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
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last, windowBase uint64
	var pause, maxPause int
	for now := range tick.C {
		sec := int(now.Sub(start).Seconds())
		phase := sec % 120
		if phase == 10 {
			add()
			add()
		}
		if phase == 30 {
			clocks[1].rate.Store(1e6)
		}
		if phase == 60 {
			clocks[1].rate.Store(0)
		}
		if phase == 90 {
			clocks[1].rate.Store(100e6)
		}
		if phase == 115 && len(flows) > 2 {
			for _, f := range flows[2:] {
				f.close(t)
				releaseLog(t, map[string]any{"second": sec, "event": "application_exit_cancel", "closed_tx": releaseStats(f.tx)})
				retired += f.sink.bytes.Load()
			}
			flows = flows[:2]
		}
		n := retired
		for _, f := range flows {
			n += f.sink.bytes.Load()
		}
		delta := n - last
		last = n
		if delta == 0 {
			pause++
		} else {
			pause = 0
		}
		if pause > maxPause {
			maxPause = pause
		}
		if sec%10 == 0 {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			registry.mu.Lock()
			refs := 0
			backlog := uint64(0)
			for _, entry := range registry.entries {
				refs += entry.refs
				backlog += entry.backlog
			}
			entries := len(registry.entries)
			registry.mu.Unlock()
			stats := []any{}
			for _, f := range flows {
				stats = append(stats, releaseStats(f.tx))
			}
			releaseLog(t, map[string]any{"second": sec, "phase": phase, "streams": len(flows), "provider_refs": refs, "entries": entries, "provider_backlog": backlog, "goodput_last_second": float64(delta) * 8 / 1e6, "goodput_fixed_10s": float64(n-windowBase) * 8 / 10 / 1e6, "max_zero_second_pause": maxPause, "delivered": n, "goroutines": runtime.NumGoroutine(), "heap_alloc": mem.HeapAlloc, "heap_inuse": mem.HeapInuse, "heap_objects": mem.HeapObjects, "process_resources": releaseProcessResources(), "tx": stats})
			windowBase = n
			if refs != 2*len(flows) {
				t.Error("provider reference mismatch")
			}
			if entries != 2 {
				t.Error("registry grew")
			}
		}
		if now.Sub(start) >= duration {
			break
		}
	}
	for _, f := range flows {
		f.close(t)
		releaseLog(t, map[string]any{"event": "final_cancel", "closed_tx": releaseStats(f.tx)})
	}
	flows = nil
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, entry := range registry.entries {
		if entry.refs != 0 || entry.backlog != 0 {
			t.Errorf("final provider leak refs=%d backlog=%d", entry.refs, entry.backlog)
		}
	}
	releaseLog(t, map[string]any{"result": "prefix_verified_closed", "seconds": duration.Seconds(), "goroutines": runtime.NumGoroutine()})
}

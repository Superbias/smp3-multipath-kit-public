package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type telemetryFixture struct {
	mu     sync.Mutex
	status rawStatus
	legs   rawLegs
}

func (f *telemetryFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/v1/status":
		_ = json.NewEncoder(w).Encode(f.status)
	case "/api/v1/legs":
		_ = json.NewEncoder(w).Encode(f.legs)
	default:
		http.NotFound(w, r)
	}
}

func newFixtureMonitor(t *testing.T, fixture *telemetryFixture, path string) (*Monitor, func()) {
	t.Helper()
	server := httptest.NewServer(fixture)
	cfg := DefaultConfig()
	cfg.TelemetryURL = server.URL
	cfg.PersistentPath = path
	cfg.PersistentInterval = time.Second
	cfg.Probe = func(target ListenerTarget) ListenerHealth {
		return ListenerHealth{ListenerTarget: target, ListenerUp: true, ProcessUp: true, Probe: "test"}
	}
	monitor, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return monitor, server.Close
}

func setFixture(fixture *telemetryFixture, at time.Time, totalTx, totalRx uint64, legs [2]rawLeg) {
	fixture.mu.Lock()
	fixture.status = rawStatus{SnapshotAt: at.UTC().Format(time.RFC3339Nano), ActiveLegs: 2, WireTxBytes: totalTx, WireRxBytes: totalRx}
	fixture.legs = rawLegs{SnapshotAt: fixture.status.SnapshotAt, Items: []rawLeg{legs[0], legs[1]}}
	fixture.mu.Unlock()
}

func TestCounterDeltaRatesSharesAndReset(t *testing.T) {
	fixture := &telemetryFixture{}
	monitor, closeServer := newFixtureMonitor(t, fixture, "")
	defer closeServer()
	first := time.Unix(100, 0)
	setFixture(fixture, first, 100, 200, [2]rawLeg{{LegID: 0, ActiveConnections: 1, WireTxBytes: 100, WireRxBytes: 50}, {LegID: 1, ActiveConnections: 1, WireTxBytes: 300, WireRxBytes: 150}})
	if err := monitor.CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := first.Add(time.Second)
	setFixture(fixture, second, 600, 400, [2]rawLeg{{LegID: 0, ActiveConnections: 1, WireTxBytes: 200, WireRxBytes: 100, UsefulACKBytes: 10, RXUniqueBytes: 12}, {LegID: 1, ActiveConnections: 1, WireTxBytes: 500, WireRxBytes: 250, UsefulACKBytes: 30, RXUniqueBytes: 48}})
	if err := monitor.CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := monitor.Latest()
	if !ok {
		t.Fatal("latest snapshot missing")
	}
	if snapshot.Legs[0].WireTxRateBPS == nil || *snapshot.Legs[0].WireTxRateBPS != 800 {
		t.Fatalf("leg0 tx rate=%v", snapshot.Legs[0].WireTxRateBPS)
	}
	if snapshot.Traffic.WireTx.Leg0 == nil || *snapshot.Traffic.WireTx.Leg0 != 1.0/3.0 {
		t.Fatalf("tx share=%v", snapshot.Traffic.WireTx)
	}
	if snapshot.Legs[1].UsefulACKRateBPS == nil || *snapshot.Legs[1].UsefulACKRateBPS != 240 {
		t.Fatalf("ack rate=%v", snapshot.Legs[1].UsefulACKRateBPS)
	}
	third := second.Add(time.Second)
	setFixture(fixture, third, 50, 500, [2]rawLeg{{LegID: 0, ActiveConnections: 1, WireTxBytes: 10, WireRxBytes: 100}, {LegID: 1, ActiveConnections: 1, WireTxBytes: 20, WireRxBytes: 250}})
	if err := monitor.CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = monitor.Latest()
	if snapshot.Legs[0].WireTxRateBPS != nil || !snapshot.Legs[0].CounterReset {
		t.Fatalf("reset not qualified: %+v", snapshot.Legs[0])
	}
	if snapshot.Traffic.WireTx.Leg0 != nil || snapshot.Traffic.WireTx.Leg1 != nil {
		t.Fatalf("reset share should be N/A: %+v", snapshot.Traffic.WireTx)
	}
}

func TestHistoryIsBoundedAndPersistent(t *testing.T) {
	fixture := &telemetryFixture{}
	path := filepath.Join(t.TempDir(), "history.jsonl")
	monitor, closeServer := newFixtureMonitor(t, fixture, path)
	defer closeServer()
	monitor.cfg.MemoryRetention = 2 * time.Second
	start := time.Now().UTC().Truncate(time.Second)
	for index, offset := range []time.Duration{0, time.Second, 3 * time.Second} {
		setFixture(fixture, start.Add(offset), uint64(index), uint64(index), [2]rawLeg{{LegID: 0}, {LegID: 1}})
		if err := monitor.CollectOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	memory := monitor.History("memory", 10)
	if len(memory.Items) != 2 || !memory.Items[0].At.Equal(start.Add(time.Second)) {
		t.Fatalf("memory=%+v", memory.Items)
	}
	persistent := monitor.History("persistent", 10)
	if len(persistent.Items) != 3 {
		t.Fatalf("persistent=%d", len(persistent.Items))
	}
	reloaded, err := New(monitor.Config())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(reloaded.History("persistent", 10).Items); got != 3 {
		t.Fatalf("reloaded persistent=%d", got)
	}
	reloaded.Close()
}

func TestMonitorRejectsNonLoopbackEndpoints(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Listen = "0.0.0.0:24600"
	if _, err := New(cfg); err == nil {
		t.Fatal("wildcard panel bind accepted")
	}
	cfg = DefaultConfig()
	cfg.TelemetryURL = "http://localhost:24500"
	if _, err := New(cfg); err == nil {
		t.Fatal("hostname telemetry accepted")
	}
}

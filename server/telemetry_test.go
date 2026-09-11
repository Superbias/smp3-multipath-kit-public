package server

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestCountedConnIncludesPreAdmissionHelloBytes(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	counted, counters := newCountedConn(right)
	writeDone := make(chan error, 1)
	go func() {
		_, err := left.Write([]byte("HELLO"))
		writeDone <- err
	}()
	got := make([]byte, 5)
	if _, err := io.ReadFull(counted, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "HELLO" {
		t.Fatalf("read=%q", got)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 3)
		_, err := io.ReadFull(left, buffer)
		if err == nil && string(buffer) != "ACK" {
			err = io.ErrUnexpectedEOF
		}
		readDone <- err
	}()
	if _, err := counted.Write([]byte("ACK")); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}

	gotCounters := counters.snapshot()
	if gotCounters.RxBytes != 5 || gotCounters.TxBytes != 3 {
		t.Fatalf("wire counters=%+v, want rx=5 tx=3", gotCounters)
	}
}

func TestTelemetryShareZeroDenominatorIsInvalid(t *testing.T) {
	share := calculateTelemetryShare(0, 0)
	if share.Valid {
		t.Fatalf("zero denominator produced a valid share: %+v", share)
	}
}

func TestTelemetryRegistryBoundsPrivacyAndImmutableSnapshot(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{
		Enabled:           true,
		MaxActiveSessions: 1,
		MaxEvents:         1,
		SnapshotInterval:  time.Hour,
		HMACSecret:        []byte("r13-m2a-test-secret"),
	})
	defer registry.Close()

	first := smp3core.SessionID{1, 2, 3, 4}
	second := smp3core.SessionID{5, 6, 7, 8}
	created := time.Unix(100, 0)
	attached := created.Add(time.Second)
	data := attached.Add(time.Second)
	if !registry.RegisterSession(first, "stream", created) {
		t.Fatal("first session was not registered")
	}
	if registry.RegisterSession(second, "stream", created) {
		t.Fatal("bounded registry admitted a second session")
	}
	registry.AttachLeg(first, 0, 7, nil, attached)
	registry.MarkFirstData(first, 0, data)

	snapshot := registry.Snapshot()
	if len(snapshot.Sessions) != 1 || snapshot.ActiveSessions != 1 || snapshot.ActiveLegs != 1 {
		t.Fatalf("unexpected bounded snapshot: %+v", snapshot)
	}
	row := snapshot.Sessions[0]
	if row.DisplaySessionID == "01020304" || row.DisplaySessionID == "01020304000000000000000000000000" {
		t.Fatalf("snapshot leaked raw session ID: %q", row.DisplaySessionID)
	}
	if row.Legs[0].AttachAt != attached || row.Legs[0].Generation != 7 || row.FirstDataSeenAt != data {
		t.Fatalf("lifecycle snapshot=%+v", row)
	}
	snapshot.Sessions[0].DisplaySessionID = "mutated"
	if registry.Snapshot().Sessions[0].DisplaySessionID == "mutated" {
		t.Fatal("snapshot was not immutable")
	}
	if snapshot.TelemetryDroppedEvents == 0 {
		t.Fatal("bounded admission did not report dropped telemetry")
	}
}

func TestTelemetryRegistryAggregatesWireShares(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 4, HMACSecret: []byte("share-test")})
	defer registry.Close()
	first := smp3core.SessionID{1}
	second := smp3core.SessionID{2}
	if !registry.RegisterSession(first, "stream", time.Unix(1, 0)) || !registry.RegisterSession(second, "stream", time.Unix(1, 0)) {
		t.Fatal("failed to register share fixtures")
	}
	firstCounters := &WireCounters{}
	firstCounters.txBytes.Store(100)
	firstCounters.rxBytes.Store(50)
	secondCounters := &WireCounters{}
	secondCounters.txBytes.Store(50)
	secondCounters.rxBytes.Store(50)
	registry.AttachLeg(first, 0, 1, firstCounters, time.Unix(2, 0))
	registry.AttachLeg(second, 1, 1, secondCounters, time.Unix(2, 0))
	snapshot := registry.Snapshot()
	if snapshot.TotalWireTx != 150 || snapshot.TotalWireRx != 100 {
		t.Fatalf("wire totals=%d/%d", snapshot.TotalWireTx, snapshot.TotalWireRx)
	}
	if !snapshot.Legs[0].WireShare.Valid || snapshot.Legs[0].WireShare.Value != 0.6 {
		t.Fatalf("leg0 wire share=%+v", snapshot.Legs[0].WireShare)
	}
	if !snapshot.Legs[1].WireShare.Valid || snapshot.Legs[1].WireShare.Value != 0.4 {
		t.Fatalf("leg1 wire share=%+v", snapshot.Legs[1].WireShare)
	}
	if !snapshot.Legs[0].WireTxShare.Valid || snapshot.Legs[0].WireTxShare.Value != (2.0/3.0) {
		t.Fatalf("leg0 tx share=%+v", snapshot.Legs[0].WireTxShare)
	}
}

func TestTelemetryRegistryDisabledIsNoOp(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{})
	defer registry.Close()
	var id smp3core.SessionID
	if !registry.RegisterSession(id, "stream", time.Now()) {
		t.Fatal("disabled registry rejected fail-open registration")
	}
	if snapshot := registry.Snapshot(); snapshot.ActiveSessions != 0 || snapshot.TotalSessions != 0 || len(snapshot.Sessions) != 0 {
		t.Fatalf("disabled registry produced telemetry: %+v", snapshot)
	}
}

func TestTelemetryRegistryConcurrentSnapshotAndLifecycle(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 64, MaxEvents: 64, HMACSecret: []byte("concurrency-test")})
	defer registry.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				var id smp3core.SessionID
				id[0] = byte(worker)
				id[1] = byte(i)
				registry.RegisterSession(id, "stream", time.Now())
				registry.AttachLeg(id, uint8(i%2), uint64(i+1), &WireCounters{}, time.Now())
				registry.MarkFirstData(id, uint8(i%2), time.Now())
				if i%3 == 0 {
					registry.RemoveSession(id, time.Now())
				}
				_ = registry.Snapshot()
			}
		}(worker)
	}
	wg.Wait()
}

func BenchmarkTelemetryRegistrySnapshot(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		registry := NewTelemetryRegistry(TelemetryConfig{})
		defer registry.Close()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = registry.Snapshot()
		}
	})
	b.Run("enabled", func(b *testing.B) {
		registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 256, HMACSecret: []byte("benchmark-secret")})
		defer registry.Close()
		for i := 0; i < 32; i++ {
			var id smp3core.SessionID
			id[0] = byte(i)
			registry.RegisterSession(id, "stream", time.Now())
			registry.AttachLeg(id, 0, 1, &WireCounters{}, time.Now())
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = registry.Snapshot()
		}
	})
}

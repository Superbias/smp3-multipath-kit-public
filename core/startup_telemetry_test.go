package smp3core

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func startupTelemetryConfig(preferred uint8, grace time.Duration) StreamConfig {
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = LegID(preferred)
	cfg.StartupGrace = grace
	cfg.Telemetry = &StreamTelemetry{}
	cfg.RetransmitTimeout = 10 * time.Millisecond
	cfg.RecoveryTimeout = time.Second
	return cfg
}

func assertStartupMetadata(t *testing.T, snapshot StreamStartupTelemetrySnapshot, preferred uint8, grace time.Duration) {
	t.Helper()
	if !snapshot.Applied || snapshot.Policy != StreamStartupPreferred || snapshot.PreferredLeg != int8(preferred) || snapshot.Grace != grace {
		t.Fatalf("startup metadata=%+v", snapshot)
	}
}

func waitStartupTx(t *testing.T, engine *StreamEngine) StreamStats {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := engine.TelemetrySnapshot()
		if (!stats.Telemetry.FirstTxDataAt[0].IsZero() || !stats.Telemetry.FirstTxDataAt[1].IsZero()) &&
			(!stats.Telemetry.Startup.Applied || stats.Telemetry.Startup.FirstDataLeg >= 0) {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("successful DATA telemetry timeout")
	return StreamStats{}
}

func TestStartupTelemetryFirstReadyDoesNotFakeStartupEvent(t *testing.T) {
	cfg := testStreamConfig()
	cfg.Telemetry = &StreamTelemetry{}
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	leg := attachStartupTestLeg(t, engine, 0)
	writeStartupTestByte(t, app, 'a')
	readStartupTestData(t, leg)
	stats := waitStartupTx(t, engine)
	startup := stats.Telemetry.Startup
	if startup.Applied || startup.Policy != StreamStartupFirstReady || startup.PreferredLeg != -1 || startup.FirstDataLeg != -1 {
		t.Fatalf("first-ready startup=%+v", startup)
	}
	if !startup.FirstDataWaitingAt.IsZero() || !startup.ReleasedAt.IsZero() || startup.ReleaseReason != StartupReleaseNone {
		t.Fatalf("first-ready fabricated transition=%+v", startup)
	}
	if stats.Telemetry.FirstTxDataAt[0].IsZero() {
		t.Fatal("generic FirstTxDataAt was not preserved")
	}
}

func TestStartupTelemetryPreferredAlreadyAttachedAndBeforeNonPreferred(t *testing.T) {
	t.Run("already-attached", func(t *testing.T) {
		engine, app := NewStreamEngine(startupTelemetryConfig(0, time.Second))
		defer engine.Close()
		defer app.Close()
		_ = attachStartupTestLeg(t, engine, 1)
		leg0 := attachStartupTestLeg(t, engine, 0)
		writeStartupTestByte(t, app, 'a')
		readStartupTestData(t, leg0)
		stats := waitStartupTx(t, engine)
		startup := stats.Telemetry.Startup
		assertStartupMetadata(t, startup, 0, time.Second)
		if startup.ReleaseReason != StartupReleasePreferredAlreadyAttached || startup.ReleasedAt.IsZero() || !startup.GraceStartedAt.IsZero() || startup.FirstDataLeg != 0 {
			t.Fatalf("already-attached startup=%+v telemetry=%+v", startup, engine.TelemetrySnapshot().Telemetry)
		}
		if startup.PreferredAttachedAt.IsZero() || startup.NonPreferredAttachedAt.IsZero() || startup.FirstDataWaitingAt.IsZero() {
			t.Fatalf("already-attached timestamps=%+v", startup)
		}
		firstWaiting := startup.FirstDataWaitingAt
		writeStartupTestByte(t, app, 'b')
		if got := engine.TelemetrySnapshot().Telemetry.Startup.FirstDataWaitingAt; !got.Equal(firstWaiting) {
			t.Fatalf("first data waiting changed: %v -> %v", firstWaiting, got)
		}
	})

	t.Run("preferred-before-nonpreferred", func(t *testing.T) {
		engine, app := NewStreamEngine(startupTelemetryConfig(0, time.Second))
		defer engine.Close()
		defer app.Close()
		leg0 := attachStartupTestLeg(t, engine, 0)
		writeStartupTestByte(t, app, 'a')
		readStartupTestData(t, leg0)
		stats := waitStartupTx(t, engine)
		startup := stats.Telemetry.Startup
		if startup.ReleaseReason != StartupReleasePreferredAttachedBeforeNonPreferred || startup.FirstDataLeg != 0 {
			t.Fatalf("preferred-before-nonpreferred startup=%+v", startup)
		}
		attachStartupTestLeg(t, engine, 1)
		if got := engine.TelemetrySnapshot().Telemetry.Startup.ReleaseReason; got != StartupReleasePreferredAttachedBeforeNonPreferred {
			t.Fatalf("late nonpreferred changed reason=%v", got)
		}
	})
}

func TestStartupTelemetryGraceAndTerminalReasons(t *testing.T) {
	t.Run("zero-grace", func(t *testing.T) {
		factory, created, _ := manualStreamStartupTimerFactory()
		cfg := startupTelemetryConfig(0, 0)
		cfg.startupTimerFactory = factory
		engine, app := NewStreamEngine(cfg)
		defer engine.Close()
		defer app.Close()
		leg1 := attachStartupTestLeg(t, engine, 1)
		writeStartupTestByte(t, app, 'z')
		readStartupTestData(t, leg1)
		startup := waitStartupTx(t, engine).Telemetry.Startup
		if startup.ReleaseReason != StartupReleaseGraceExpired || !startup.GraceStartedAt.IsZero() || startup.ReleasedAt.IsZero() {
			t.Fatalf("zero-grace startup=%+v", startup)
		}
		select {
		case <-created:
			t.Fatal("zero grace created a timer")
		default:
		}
	})

	t.Run("within-grace", func(t *testing.T) {
		factory, created, _ := manualStreamStartupTimerFactory()
		cfg := startupTelemetryConfig(0, 40*time.Millisecond)
		cfg.startupTimerFactory = factory
		engine, app := NewStreamEngine(cfg)
		defer engine.Close()
		defer app.Close()
		leg1 := attachStartupTestLeg(t, engine, 1)
		writeDone := make(chan error, 1)
		go func() { _, err := app.Write([]byte{'a'}); writeDone <- err }()
		<-created
		leg0 := attachStartupTestLeg(t, engine, 0)
		readStartupTestData(t, leg0)
		if err := <-writeDone; err != nil {
			t.Fatal(err)
		}
		_ = leg1
		stats := waitStartupTx(t, engine)
		startup := stats.Telemetry.Startup
		if startup.ReleaseReason != StartupReleasePreferredArrivedWithinGrace || startup.GraceStartedAt.IsZero() || startup.ReleasedAt.IsZero() || startup.FirstDataLeg != 0 {
			t.Fatalf("within-grace startup=%+v", startup)
		}
	})

	t.Run("grace-expired-and-late-attach", func(t *testing.T) {
		factory, created, _ := manualStreamStartupTimerFactory()
		cfg := startupTelemetryConfig(0, 40*time.Millisecond)
		cfg.startupTimerFactory = factory
		engine, app := NewStreamEngine(cfg)
		defer engine.Close()
		defer app.Close()
		leg1 := attachStartupTestLeg(t, engine, 1)
		writeStartupTestByte(t, app, 'a')
		timer := <-created
		timer.Fire()
		readStartupTestData(t, leg1)
		before := waitStartupTx(t, engine).Telemetry.Startup
		if before.ReleaseReason != StartupReleaseGraceExpired || before.FirstDataLeg != 1 || before.ReleasedAt.IsZero() {
			t.Fatalf("grace-expired startup=%+v", before)
		}
		leg0 := attachStartupTestLeg(t, engine, 0)
		after := engine.TelemetrySnapshot().Telemetry.Startup
		if after.PreferredAttachedAt.IsZero() || after.ReleasedAt != before.ReleasedAt || after.ReleaseReason != before.ReleaseReason {
			t.Fatalf("late preferred mutated release: before=%+v after=%+v", before, after)
		}
		writeStartupTestByte(t, app, 'b')
		readStartupTestData(t, leg0)
	})

	t.Run("terminal", func(t *testing.T) {
		factory, created, _ := manualStreamStartupTimerFactory()
		cfg := startupTelemetryConfig(0, time.Second)
		cfg.startupTimerFactory = factory
		engine, app := NewStreamEngine(cfg)
		defer engine.Close()
		defer app.Close()
		leg1 := attachStartupTestLeg(t, engine, 1)
		writeStartupTestByte(t, app, 'a')
		<-created
		if !engine.MarkStartupLegTerminalUnavailable(0) {
			t.Fatal("terminal preferred signal was rejected")
		}
		readStartupTestData(t, leg1)
		stats := waitStartupTx(t, engine)
		startup := stats.Telemetry.Startup
		if startup.ReleaseReason != StartupReleasePreferredTerminalUnavailable || startup.ReleasedAt.IsZero() || startup.FirstDataLeg != 1 {
			t.Fatalf("terminal startup=%+v", startup)
		}
	})
}

func TestStartupTelemetryCloseBeforeReleaseAndDisabled(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := startupTelemetryConfig(0, time.Second)
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	attachStartupTestLeg(t, engine, 1)
	writeDone := make(chan error, 1)
	go func() { _, err := app.Write([]byte{'a'}); writeDone <- err }()
	<-created
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	startup := engine.TelemetrySnapshot().Telemetry.Startup
	if !startup.ReleasedAt.IsZero() || startup.ReleaseReason != StartupReleaseSessionClosedBeforeRelease {
		t.Fatalf("close-before-release startup=%+v", startup)
	}
	_ = app.Close()
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("close did not stop waiting DATA")
	}

	disabled := testStreamConfig()
	disabled.StartupPolicy = StreamStartupPreferred
	closedEngine, closedApp := NewStreamEngine(disabled)
	defer closedEngine.Close()
	defer closedApp.Close()
	if startup := closedEngine.TelemetrySnapshot().Telemetry.Startup; startup.Applied || startup.PreferredLeg != -1 || startup.FirstDataLeg != -1 {
		t.Fatalf("disabled startup telemetry=%+v", startup)
	}
}

type startupFailDataLeg struct {
	closed chan struct{}
	once   sync.Once
}

func (l *startupFailDataLeg) Read([]byte) (int, error) {
	<-l.closed
	return 0, io.EOF
}

func (l *startupFailDataLeg) Write(p []byte) (int, error) {
	if len(p) > 0 && p[0] == byte(StreamFrameData) {
		return 0, errors.New("deterministic DATA write failure")
	}
	return len(p), nil
}

func (l *startupFailDataLeg) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func readUntilStartupData(t *testing.T, leg *startupCaptureLeg) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case frame := <-leg.frames:
			header, err := ReadStreamFrameHeader(frameReader(frame))
			if err != nil {
				t.Fatal(err)
			}
			if header.Type == StreamFrameData {
				return
			}
		case <-deadline:
			t.Fatal("successful DATA frame timeout")
		}
	}
}

type byteFrameReader struct {
	data   []byte
	offset int
}

func frameReader(data []byte) *byteFrameReader { return &byteFrameReader{data: data} }
func (r *byteFrameReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func TestStartupTelemetryFirstDataUsesSuccessfulWriteAndIgnoresRX(t *testing.T) {
	cfg := startupTelemetryConfig(0, time.Second)
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	failing := &startupFailDataLeg{closed: make(chan struct{})}
	if err := engine.AttachLeg(0, failing, nil); err != nil {
		t.Fatal(err)
	}
	leg1 := newStartupCaptureLeg()
	if err := engine.AttachLeg(1, leg1, nil); err != nil {
		t.Fatal(err)
	}
	writeStartupTestByte(t, app, 'a')
	readUntilStartupData(t, leg1)
	stats := waitStartupTx(t, engine)
	startup := stats.Telemetry.Startup
	if startup.FirstDataLeg != 1 || !stats.Telemetry.FirstTxDataAt[0].IsZero() || stats.Telemetry.FirstTxDataAt[1].IsZero() {
		t.Fatalf("successful-write attribution startup=%+v telemetry=%+v", startup, stats)
	}
	if startup.FirstDataLeg != 1 {
		t.Fatal("initial failed write selected the startup FirstDataLeg")
	}
}

func TestStartupTelemetryRXBeforeTXDoesNotSetFirstDataLeg(t *testing.T) {
	engine, app := NewStreamEngine(startupTelemetryConfig(0, time.Second))
	defer engine.Close()
	defer app.Close()
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	if err := engine.AttachLeg(0, local, nil); err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, StreamFrameHeaderSize+1)
	if err := EncodeStreamFrameHeader(frame, StreamFrameHeader{Type: StreamFrameData, Value: 99, Length: 1}); err != nil {
		t.Fatal(err)
	}
	_, _ = peer.Write(frame)
	deadline := time.Now().Add(time.Second)
	for engine.TelemetrySnapshot().Telemetry.FirstRxUniqueDataAt[0].IsZero() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if engine.TelemetrySnapshot().Telemetry.Startup.FirstDataLeg != -1 {
		t.Fatal("RX activity set startup FirstDataLeg before local TX")
	}
}

func TestStartupTelemetrySnapshotImmutableAndStress(t *testing.T) {
	for i := 0; i < 500; i++ {
		telemetry := &StreamTelemetry{}
		telemetry.configureStartup(StreamConfig{StartupPolicy: StreamStartupPreferred, StartupPreferredLeg: LegID(i % 2), StartupGrace: time.Millisecond})
		var wg sync.WaitGroup
		wg.Add(6)
		go func() { defer wg.Done(); telemetry.markStartupFirstDataWaiting(time.Now()) }()
		go func() { defer wg.Done(); telemetry.markStartupAttached(0, time.Now()) }()
		go func() { defer wg.Done(); telemetry.markStartupAttached(1, time.Now()) }()
		go func() { defer wg.Done(); telemetry.markStartupGraceStarted(time.Now()) }()
		go func() { defer wg.Done(); telemetry.markStartupReleased(time.Now(), StartupReleaseGraceExpired) }()
		go func() { defer wg.Done(); telemetry.markStartupFirstDataLeg(uint8(i % 2)) }()
		wg.Wait()
		snapshot := telemetry.snapshot().Startup
		if !snapshot.Applied || snapshot.FirstDataLeg < 0 || snapshot.ReleaseReason != StartupReleaseGraceExpired || snapshot.ReleasedAt.IsZero() {
			t.Fatalf("stress iteration=%d startup=%+v", i, snapshot)
		}
	}

	telemetry := &StreamTelemetry{}
	telemetry.configureStartup(StreamConfig{StartupPolicy: StreamStartupPreferred, StartupPreferredLeg: 0, StartupGrace: time.Second})
	snapshotA := telemetry.snapshot().Startup
	telemetry.markStartupAttached(0, time.Now())
	snapshotB := telemetry.snapshot().Startup
	if !snapshotA.PreferredAttachedAt.IsZero() || snapshotB.PreferredAttachedAt.IsZero() {
		t.Fatalf("immutable snapshot changed: A=%+v B=%+v", snapshotA, snapshotB)
	}
}

func BenchmarkStartupTelemetryDisabledSteadyState(b *testing.B) {
	var telemetry *StreamTelemetry
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		telemetry.markDataTx(uint8(i&1), false, false, 1)
	}
}

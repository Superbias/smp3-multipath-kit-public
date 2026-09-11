package smp3core

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type startupCaptureLeg struct {
	mu        sync.Mutex
	buffer    []byte
	frames    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newStartupCaptureLeg() *startupCaptureLeg {
	return &startupCaptureLeg{
		frames: make(chan []byte, 16),
		closed: make(chan struct{}),
	}
}

func (l *startupCaptureLeg) Read([]byte) (int, error) {
	<-l.closed
	return 0, io.EOF
}

func (l *startupCaptureLeg) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.buffer = append(l.buffer, p...)
	for len(l.buffer) >= StreamFrameHeaderSize {
		header, err := ReadStreamFrameHeader(bytes.NewReader(l.buffer))
		if err != nil {
			break
		}
		total := StreamFrameHeaderSize + int(header.Length)
		if len(l.buffer) < total {
			break
		}
		frame := append([]byte(nil), l.buffer[:total]...)
		l.buffer = l.buffer[total:]
		l.frames <- frame
	}
	l.mu.Unlock()
	return len(p), nil
}

func (l *startupCaptureLeg) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func readCapturedStartupFrame(t *testing.T, leg *startupCaptureLeg) (uint64, byte) {
	t.Helper()
	select {
	case frame := <-leg.frames:
		header, err := ReadStreamFrameHeader(bytes.NewReader(frame))
		if err != nil || header.Type != StreamFrameData || header.Length != 1 {
			t.Fatalf("captured frame header=%+v err=%v", header, err)
		}
		return header.Value, frame[StreamFrameHeaderSize]
	case <-time.After(time.Second):
		t.Fatal("captured DATA frame timeout")
		return 0, 0
	}
}

type manualStreamStartupTimer struct {
	ch       chan time.Time
	stopOnce sync.Once
	stopped  chan struct{}
}

func (t *manualStreamStartupTimer) C() <-chan time.Time { return t.ch }
func (t *manualStreamStartupTimer) Stop() bool {
	stopped := false
	t.stopOnce.Do(func() {
		close(t.stopped)
		stopped = true
	})
	return stopped
}
func (t *manualStreamStartupTimer) Fire() { t.ch <- time.Now() }

func manualStreamStartupTimerFactory() (func(time.Duration) streamStartupTimer, <-chan *manualStreamStartupTimer, <-chan time.Duration) {
	created := make(chan *manualStreamStartupTimer, 4)
	durations := make(chan time.Duration, 4)
	return func(duration time.Duration) streamStartupTimer {
		timer := &manualStreamStartupTimer{ch: make(chan time.Time, 1), stopped: make(chan struct{})}
		durations <- duration
		created <- timer
		return timer
	}, created, durations
}

func TestStreamStartupPreferredLeg0ReadyImmediately(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = time.Second
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	leg0 := attachStartupTestLeg(t, engine, 0)
	attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	if sequence, payload := readStartupTestData(t, leg0); sequence != 0 || payload != 'a' {
		t.Fatalf("sequence=%d payload=%q", sequence, payload)
	}
	select {
	case <-created:
		t.Fatal("preferred-ready path created a grace timer")
	default:
	}
}

func attachStartupTestLeg(t *testing.T, engine *StreamEngine, id LegID) net.Conn {
	t.Helper()
	local, peer := net.Pipe()
	if err := engine.AttachLeg(id, local, nil); err != nil {
		_ = local.Close()
		_ = peer.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

func writeStartupTestByte(t *testing.T, app net.Conn, value byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := app.Write([]byte{value})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("application write was not accepted")
	}
}

type startupFrameResult struct {
	sequence uint64
	payload  byte
	err      error
}

func readStartupTestDataResult(peer net.Conn) startupFrameResult {
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	header, err := ReadStreamFrameHeader(peer)
	if err != nil {
		return startupFrameResult{err: err}
	}
	if header.Type != StreamFrameData || header.Length != 1 {
		return startupFrameResult{err: errors.New("unexpected startup frame")}
	}
	var payload [1]byte
	if _, err := io.ReadFull(peer, payload[:]); err != nil {
		return startupFrameResult{err: err}
	}
	return startupFrameResult{sequence: header.Value, payload: payload[0]}
}

func readStartupTestData(t *testing.T, peer net.Conn) (uint64, byte) {
	t.Helper()
	result := readStartupTestDataResult(peer)
	if result.err != nil {
		t.Fatal(result.err)
	}
	return result.sequence, result.payload
}

func TestStreamStartupFirstReadyLeg1Immediate(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	if engine.startup != nil {
		t.Fatal("default first-ready policy allocated a startup gate")
	}
	leg1 := attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	if sequence, payload := readStartupTestData(t, leg1); sequence != 0 || payload != 'a' {
		t.Fatalf("sequence=%d payload=%q", sequence, payload)
	}
}

func TestStreamStartupPreferredLeg1OneShotSelection(t *testing.T) {
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 1
	cfg.StartupGrace = time.Second
	cfg.RetransmitTimeout = time.Hour
	cfg.ThresholdBytesPS = 1 << 60
	cfg.ActivationWindow = time.Hour
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	leg0 := newStartupCaptureLeg()
	leg1 := newStartupCaptureLeg()
	if err := engine.AttachLeg(0, leg0, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.AttachLeg(1, leg1, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leg0.Close(); _ = leg1.Close() })

	writeStartupTestByte(t, app, 'a')
	select {
	case frame := <-leg0.frames:
		header, _ := ReadStreamFrameHeader(bytes.NewReader(frame))
		t.Fatalf("first DATA selected leg0: header=%+v", header)
	case frame := <-leg1.frames:
		header, err := ReadStreamFrameHeader(bytes.NewReader(frame))
		if err != nil || header.Type != StreamFrameData || header.Value != 0 || header.Length != 1 || frame[StreamFrameHeaderSize] != 'a' {
			t.Fatalf("first captured frame header=%+v err=%v", header, err)
		}
	case <-time.After(time.Second):
		t.Fatal("first preferred DATA timeout")
	}
	writeStartupTestByte(t, app, 'b')
	if sequence, payload := readCapturedStartupFrame(t, leg0); sequence != 1 || payload != 'b' {
		t.Fatalf("first sequence=%d payload=%q", sequence, payload)
	}
}

func TestStreamStartupPreferredWaitsBeforeLedgerUntilPreferredAttaches(t *testing.T) {
	factory, created, durations := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = 40 * time.Millisecond
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	timer := <-created
	if duration := <-durations; duration != cfg.StartupGrace {
		t.Fatalf("timer duration=%v", duration)
	}
	if next := engine.TXLedger().NextSequence(); next != 0 {
		t.Fatalf("sequence allocated during grace: %d", next)
	}
	if engine.TXLedger().ProgressSnapshot().Outstanding != 0 || engine.FrontierRescueAttempts() != 0 {
		t.Fatal("retry/rescue state started during startup grace")
	}
	leg0 := attachStartupTestLeg(t, engine, 0)
	if sequence, payload := readStartupTestData(t, leg0); sequence != 0 || payload != 'a' {
		t.Fatalf("sequence=%d payload=%q", sequence, payload)
	}
	select {
	case <-timer.stopped:
	case <-time.After(time.Second):
		t.Fatal("grace timer was not stopped after preferred attach")
	}
}

func TestStreamStartupIdleDoesNotCreateOrBurnGrace(t *testing.T) {
	factory, created, durations := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = 40 * time.Millisecond
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	attachStartupTestLeg(t, engine, 1)
	select {
	case <-created:
		t.Fatal("idle session created startup grace timer")
	default:
	}
	writeStartupTestByte(t, app, 'a')
	if timer := <-created; timer == nil {
		t.Fatal("DATA did not create startup grace timer")
	}
	if duration := <-durations; duration != cfg.StartupGrace {
		t.Fatalf("timer duration=%v", duration)
	}
}

func TestStreamStartupGraceExpiryAndLatePreferredDoNotReopen(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = 40 * time.Millisecond
	cfg.RetransmitTimeout = time.Hour
	cfg.ThresholdBytesPS = 1 << 60
	cfg.ActivationWindow = time.Hour
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	leg1 := attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	timer := <-created
	timer.Fire()
	readStartupTestData(t, leg1)
	if !engine.startup.released.Load() {
		t.Fatal("startup was not terminal after grace expiry")
	}
	leg0 := attachStartupTestLeg(t, engine, 0)
	writeStartupTestByte(t, app, 'b')
	if sequence, payload := readStartupTestData(t, leg0); sequence != 1 || payload != 'b' {
		t.Fatalf("late preferred normal scheduling sequence=%d payload=%q", sequence, payload)
	}
	select {
	case <-created:
		t.Fatal("late preferred reopened startup grace")
	default:
	}
}

func TestStreamStartupPreferredHintFallsBackWhenLegDisappears(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	defer engine.Close()
	defer app.Close()
	attachStartupTestLeg(t, engine, 0)
	attachStartupTestLeg(t, engine, 1)
	if !engine.ReplaceLeg(0, errors.New("preferred disappeared")) {
		t.Fatal("preferred leg was not present")
	}
	if leg := engine.chooseLegWithStartupHint(false, -1, 0); leg == nil || leg.id != 1 {
		t.Fatalf("fallback leg=%v", leg)
	}
}

func TestStreamStartupConfigurationIsBounded(t *testing.T) {
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 9
	cfg.StartupGrace = MaxStreamStartupGrace + time.Second
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	if engine.startup == nil || engine.startup.preferred != 0 || engine.startup.grace != MaxStreamStartupGrace {
		t.Fatalf("normalized startup=%+v", engine.startup)
	}

	cfg.StartupGrace = -time.Second
	negative, negativeApp := NewStreamEngine(cfg)
	defer negative.Close()
	defer negativeApp.Close()
	if negative.startup.grace != 0 {
		t.Fatalf("negative grace=%v", negative.startup.grace)
	}
}

func TestStreamStartupDataBeforeLegStartsGraceOnNonPreferredAttach(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = 40 * time.Millisecond
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	writeStartupTestByte(t, app, 'a')
	if next := engine.TXLedger().NextSequence(); next != 0 {
		t.Fatalf("sequence allocated before any leg: %d", next)
	}
	select {
	case <-created:
		t.Fatal("grace started without a usable non-preferred leg")
	default:
	}
	leg1 := attachStartupTestLeg(t, engine, 1)
	timer := <-created
	timer.Fire()
	if sequence, payload := readStartupTestData(t, leg1); sequence != 0 || payload != 'a' {
		t.Fatalf("sequence=%d payload=%q", sequence, payload)
	}
}

func TestStreamStartupTerminalPreferredFailureReleasesImmediately(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = time.Second
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	leg1 := attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	<-created
	if !engine.MarkStartupLegTerminalUnavailable(0) {
		t.Fatal("terminal preferred failure was not accepted")
	}
	if sequence, payload := readStartupTestData(t, leg1); sequence != 0 || payload != 'a' {
		t.Fatalf("sequence=%d payload=%q", sequence, payload)
	}
}

func TestStreamStartupCloseCancelsNoLegWait(t *testing.T) {
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = time.Second
	engine, app := NewStreamEngine(cfg)
	writeStartupTestByte(t, app, 'a')
	if next := engine.TXLedger().NextSequence(); next != 0 {
		t.Fatalf("sequence allocated before close: %d", next)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-engine.txStopped:
	case <-time.After(time.Second):
		t.Fatal("startup wait did not stop with engine close")
	}
	if got := len(engine.inflight); got != 0 {
		t.Fatalf("startup cancellation retained %d inflight token", got)
	}
	_ = app.Close()
}

func TestStreamStartupCloseStopsActiveGraceTimer(t *testing.T) {
	factory, created, _ := manualStreamStartupTimerFactory()
	cfg := testStreamConfig()
	cfg.StartupPolicy = StreamStartupPreferred
	cfg.StartupPreferredLeg = 0
	cfg.StartupGrace = time.Second
	cfg.startupTimerFactory = factory
	engine, app := NewStreamEngine(cfg)
	attachStartupTestLeg(t, engine, 1)
	writeStartupTestByte(t, app, 'a')
	timer := <-created
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-timer.stopped:
	case <-time.After(time.Second):
		t.Fatal("active startup timer was not stopped on close")
	}
	select {
	case <-engine.txStopped:
	case <-time.After(time.Second):
		t.Fatal("startup grace did not stop with engine close")
	}
	_ = app.Close()
}

func BenchmarkStreamStartupPolicyOverhead(b *testing.B) {
	b.Run("first-ready-construction", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if gate := newStreamStartupGate(StreamConfig{}); gate != nil {
				b.Fatal("first-ready allocated startup gate")
			}
		}
	})
	b.Run("released-preferred-check", func(b *testing.B) {
		gate := newStreamStartupGate(StreamConfig{StartupPolicy: StreamStartupPreferred})
		gate.released.Store(true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if gate != nil && !gate.released.Load() {
				b.Fatal("released gate reopened")
			}
		}
	})
}

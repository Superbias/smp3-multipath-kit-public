package smp3core

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func p0eNewEngine(t *testing.T) (*StreamEngine, net.Conn, net.Conn) {
	t.Helper()
	cfg := testStreamConfig()
	cfg.RecoveryTimeout = 100 * time.Millisecond
	engine, app := NewStreamEngine(cfg)
	localLeg, peerLeg := net.Pipe()
	if err := engine.AttachLeg(0, localLeg, nil); err != nil {
		_ = localLeg.Close()
		_ = peerLeg.Close()
		t.Fatalf("attach test leg: %v", err)
	}
	t.Cleanup(func() {
		_ = peerLeg.Close()
		_ = app.Close()
		_ = engine.Close()
	})
	return engine, app, peerLeg
}

func p0eWriteFrame(conn net.Conn, typ StreamFrameType, value uint64, payload []byte) error {
	header := StreamFrameHeader{Type: typ, Value: value, Length: uint32(len(payload))}
	var wire [StreamFrameHeaderSize]byte
	if err := EncodeStreamFrameHeader(wire[:], header); err != nil {
		return err
	}
	if err := writeAll(conn, wire[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		return writeAll(conn, payload)
	}
	return nil
}

func p0eWaitFrame(t *testing.T, conn net.Conn, accept func(StreamFrameHeader) bool) StreamFrameHeader {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	defer conn.SetReadDeadline(time.Time{})
	for {
		header, err := ReadStreamFrameHeader(conn)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if header.Type == StreamFrameData {
			if _, err := io.CopyN(io.Discard, conn, int64(header.Length)); err != nil {
				t.Fatalf("drain unexpected DATA frame: %v", err)
			}
		}
		if accept(header) {
			return header
		}
	}
}

func p0eWaitDone(t *testing.T, engine *StreamEngine) {
	t.Helper()
	select {
	case <-engine.done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream engine did not finish after invalid close or leg failure")
	}
}

type p0eAppReadResult struct {
	n   int
	err error
}

func p0eStartAppRead(app net.Conn) <-chan p0eAppReadResult {
	started := make(chan struct{})
	result := make(chan p0eAppReadResult, 1)
	go func() {
		close(started)
		var one [1]byte
		n, err := app.Read(one[:])
		result <- p0eAppReadResult{n: n, err: err}
	}()
	<-started
	// net.Pipe does not expose whether Read has reached its internal wait state.
	// Give it a short scheduling grace here; the separately gated test below
	// establishes deterministic call ordering without relying on this delay.
	time.Sleep(10 * time.Millisecond)
	return result
}

type p0eGatedReadConn struct {
	net.Conn
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func (c *p0eGatedReadConn) Read(p []byte) (int, error) {
	c.enteredOnce.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Read(p)
}

func p0eAssertAppDidNotSeeEOF(t *testing.T, engine *StreamEngine, app net.Conn, readResult <-chan p0eAppReadResult) {
	t.Helper()
	closeErr := engine.CloseError()
	if closeErr == nil || errors.Is(closeErr, io.EOF) {
		t.Fatalf("invalid termination was recorded as normal EOF: %v", closeErr)
	}
	select {
	case result := <-readResult:
		if result.n != 0 || result.err == nil || errors.Is(result.err, io.EOF) {
			t.Fatalf("invalid termination produced normal EOF: n=%d err=%v", result.n, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application Read remained blocked after abnormal termination")
	}
	var one [1]byte
	n, err := app.Read(one[:])
	if n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("later application Read after abnormal termination returned n=%d err=%v", n, err)
	}
}

func TestP0EValidCloseReturnsApplicationEOF(t *testing.T) {
	engine, app, peer := p0eNewEngine(t)
	payload := []byte("p0e-small-payload-before-valid-close")
	payloadRead := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(app, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		payloadRead <- err
	}()
	if err := p0eWriteFrame(peer, StreamFrameData, 0, payload); err != nil {
		t.Fatalf("send DATA: %v", err)
	}
	select {
	case err := <-payloadRead:
		if err != nil {
			t.Fatalf("read DATA before CLOSE: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application did not receive DATA before CLOSE")
	}
	ack := p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool {
		return h.Type == StreamFrameACK && h.Value >= 1
	})
	if ack.Value != 1 {
		t.Fatalf("ACK frontier=%d, want 1", ack.Value)
	}

	readResult := p0eStartAppRead(app)
	if err := p0eWriteFrame(peer, StreamFrameClose, 1, nil); err != nil {
		t.Fatalf("send valid CLOSE: %v", err)
	}
	closeFrame := p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool {
		return h.Type == StreamFrameClose
	})
	if closeFrame.Value != 0 {
		t.Fatalf("reciprocal CLOSE sequence=%d, want 0", closeFrame.Value)
	}
	select {
	case result := <-readResult:
		if result.n != 0 || !errors.Is(result.err, io.EOF) {
			t.Fatalf("application Read after valid CLOSE returned n=%d err=%v, want io.EOF", result.n, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application Read did not finish after valid CLOSE")
	}
	p0eWaitDone(t, engine)
	if err := engine.CloseError(); !errors.Is(err, io.EOF) {
		t.Fatalf("valid CLOSE engine error=%v, want io.EOF", err)
	}
}

func TestP0EValidCloseReadStartedBeforeCloseReturnsEOF(t *testing.T) {
	engine, app, peer := p0eNewEngine(t)
	payload := []byte("p0e-gated-read-payload")
	payloadRead := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(app, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		payloadRead <- err
	}()
	if err := p0eWriteFrame(peer, StreamFrameData, 0, payload); err != nil {
		t.Fatalf("send DATA: %v", err)
	}
	select {
	case err := <-payloadRead:
		if err != nil {
			t.Fatalf("read DATA before CLOSE: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application did not receive DATA before CLOSE")
	}
	ack := p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool {
		return h.Type == StreamFrameACK && h.Value >= 1
	})
	if ack.Value != 1 {
		t.Fatalf("ACK frontier=%d, want 1", ack.Value)
	}

	gated := &p0eGatedReadConn{Conn: app, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(gated.release) }) }
	t.Cleanup(releaseRead)
	readResult := make(chan p0eAppReadResult, 1)
	go func() {
		var one [1]byte
		n, err := gated.Read(one[:])
		readResult <- p0eAppReadResult{n: n, err: err}
	}()
	<-gated.entered

	if err := p0eWriteFrame(peer, StreamFrameClose, 1, nil); err != nil {
		t.Fatalf("send valid CLOSE: %v", err)
	}
	_ = p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool { return h.Type == StreamFrameClose })
	p0eWaitDone(t, engine)
	releaseRead()
	select {
	case result := <-readResult:
		if result.n != 0 || !errors.Is(result.err, io.EOF) {
			t.Fatalf("application Read started before CLOSE returned n=%d err=%v, want io.EOF", result.n, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("application Read did not finish after valid CLOSE")
	}
}

func TestP0EExplicitCloseBeforeApplicationReadIsNotEOF(t *testing.T) {
	engine, app, _ := p0eNewEngine(t)
	if err := engine.Close(); err != nil {
		t.Fatalf("explicit engine close: %v", err)
	}
	if closeErr := engine.CloseError(); closeErr == nil || errors.Is(closeErr, io.EOF) {
		t.Fatalf("explicit engine close was recorded as normal EOF: %v", closeErr)
	}
	var one [1]byte
	n, err := app.Read(one[:])
	if n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("application Read after explicit Close returned n=%d err=%v, want io.ErrClosedPipe", n, err)
	}
}

func TestP0EExplicitCloseAfterGracefulCompletionPreservesEOF(t *testing.T) {
	engine, app, peer := p0eNewEngine(t)
	if err := p0eWriteFrame(peer, StreamFrameClose, 0, nil); err != nil {
		t.Fatalf("send valid CLOSE: %v", err)
	}
	_ = p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool { return h.Type == StreamFrameClose })
	p0eWaitDone(t, engine)
	if err := engine.Close(); err != nil {
		t.Fatalf("host cleanup after graceful completion: %v", err)
	}
	var one [1]byte
	if n, err := app.Read(one[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("application Read after post-graceful cleanup returned n=%d err=%v, want io.EOF", n, err)
	}
	if stats := engine.TelemetrySnapshot(); stats.OutstandingFrames != 0 || stats.RxPendingFrames != 0 || stats.RxPendingBytes != 0 {
		t.Fatalf("gracefully closed Core retained ledger state: outstanding=%d rxFrames=%d rxBytes=%d", stats.OutstandingFrames, stats.RxPendingFrames, stats.RxPendingBytes)
	}
	if got := len(engine.inflight); got != 0 {
		t.Fatalf("gracefully closed Core retained %d inflight tokens", got)
	}
}

type p0ePausedAtomicMax struct {
	value       atomic.Uint64
	loads       atomic.Int32
	firstLoaded chan struct{}
	release     chan struct{}
}

func (a *p0ePausedAtomicMax) Load() uint64 {
	value := a.value.Load()
	if a.loads.Add(1) == 1 {
		close(a.firstLoaded)
		<-a.release
	}
	return value
}

func (a *p0ePausedAtomicMax) CompareAndSwap(old, next uint64) bool {
	return a.value.CompareAndSwap(old, next)
}

func TestP0EAckFrontierCannotRegressAfterStaleLoad(t *testing.T) {
	target := &p0ePausedAtomicMax{firstLoaded: make(chan struct{}), release: make(chan struct{})}
	olderDone := make(chan struct{})
	go func() {
		advanceAtomicUint64Max(target, 1)
		close(olderDone)
	}()
	select {
	case <-target.firstLoaded:
	case <-time.After(2 * time.Second):
		t.Fatal("older ACK did not pause after its initial frontier load")
	}
	advanceAtomicUint64Max(target, 2)
	close(target.release)
	select {
	case <-olderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("older ACK did not finish after the newer ACK advanced the frontier")
	}
	if got := target.value.Load(); got != 2 {
		t.Fatalf("ACK frontier regressed to %d after stale callback completed; want 2", got)
	}
}

type p0eWriteReturnGate struct {
	net.Conn
	payloadSize int
	entered     chan struct{}
	release     chan struct{}
	returned    chan struct{}
	once        sync.Once
}

func (c *p0eWriteReturnGate) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && len(p) == c.payloadSize {
		c.once.Do(func() {
			close(c.entered)
			<-c.release
			close(c.returned)
		})
	}
	return n, err
}

func TestP0EAckBeforeCarrierWriteReturnDoesNotResurrectRecord(t *testing.T) {
	engine, app := NewStreamEngine(testStreamConfig())
	local, peer := net.Pipe()
	payload := []byte("first DATA: ACK arrives before Carrier Write returns")
	secondPayload := []byte("second DATA remains outstanding after duplicate ACK")
	gate := &p0eWriteReturnGate{
		Conn: local, payloadSize: len(payload),
		entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}),
	}
	if err := engine.AttachLeg(0, gate, nil); err != nil {
		t.Fatal(err)
	}
	engine.activate()
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(func() {
		releaseWrite()
		_ = peer.Close()
		_ = app.Close()
		_ = engine.Close()
	})

	readPayload := func(want []byte) <-chan error {
		result := make(chan error, 1)
		go func() {
			header, err := ReadStreamFrameHeader(peer)
			if err == nil && header.Type != StreamFrameData {
				err = errors.New("peer received a non-DATA frame")
			}
			if err == nil {
				got := make([]byte, int(header.Length))
				_, err = io.ReadFull(peer, got)
				if err == nil && !bytes.Equal(got, want) {
					err = errors.New("peer received different DATA payload")
				}
			}
			result <- err
		}()
		return result
	}
	writePayload := func(data []byte) {
		t.Helper()
		result := make(chan error, 1)
		go func() { _, err := app.Write(data); result <- err }()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("application Write: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("application Write did not enter Core")
		}
	}
	waitUntil := func(description string, predicate func() bool) {
		t.Helper()
		deadline := time.NewTimer(2 * time.Second)
		ticker := time.NewTicker(time.Millisecond)
		defer deadline.Stop()
		defer ticker.Stop()
		for !predicate() {
			select {
			case <-deadline.C:
				t.Fatalf("timed out waiting for %s", description)
			case <-ticker.C:
			}
		}
	}

	firstFrameRead := readPayload(payload)
	writePayload(payload)
	select {
	case err := <-firstFrameRead:
		if err != nil {
			t.Fatalf("read first DATA at peer: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not receive first DATA")
	}
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Carrier Write did not pause after peer received DATA")
	}
	waitUntil("first outstanding record and next-read token", func() bool {
		return engine.txLedger.NextSequence() == 1 && len(engine.inflight) == 2
	})

	if err := peer.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := p0eWriteFrame(peer, StreamFrameACK, 1, nil); err != nil {
		t.Fatalf("send ACK while Carrier Write is still blocked: %v", err)
	}
	_ = peer.SetWriteDeadline(time.Time{})
	waitUntil("ACK retirement and unique byte accounting before Carrier Write returns", func() bool {
		progress := engine.txLedger.ProgressSnapshot()
		return progress.AckedNext == 1 && progress.Outstanding == 0 && engine.txAckedTotal.Load() == uint64(len(payload))
	})
	if got := engine.txAckedTotal.Load(); got != uint64(len(payload)) {
		t.Fatalf("unique ACK bytes=%d, want %d", got, len(payload))
	}

	secondFrameRead := readPayload(secondPayload)
	writePayload(secondPayload)
	waitUntil("second outstanding record and read token", func() bool {
		return engine.txLedger.NextSequence() == 2 && engine.txLedger.ProgressSnapshot().Outstanding == 1 && len(engine.inflight) == 2
	})
	if err := engine.handleAck(1); err != nil {
		t.Fatalf("duplicate ACK: %v", err)
	}
	if progress := engine.txLedger.ProgressSnapshot(); progress.AckedNext != 1 || progress.Outstanding != 1 {
		t.Fatalf("duplicate ACK changed ledger: %+v", progress)
	}
	if got := len(engine.inflight); got != 2 {
		t.Fatalf("duplicate ACK released another frame's token: inflight=%d, want 2", got)
	}

	releaseWrite()
	select {
	case <-gate.returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Carrier Write did not return after release")
	}
	select {
	case err := <-secondFrameRead:
		if err != nil {
			t.Fatalf("read second DATA at peer: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not receive second DATA after first Write returned")
	}
	waitUntil("first Carrier Write bookkeeping", func() bool {
		return engine.getLeg(0).debugWriteCount.Load() >= 1
	})
	if progress := engine.txLedger.ProgressSnapshot(); progress.Outstanding != 1 || progress.AckedNext != 1 {
		t.Fatalf("late Write completion changed ACKed record state: %+v", progress)
	}
	if err := peer.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := p0eWriteFrame(peer, StreamFrameACK, 2, nil); err != nil {
		t.Fatalf("ACK second DATA: %v", err)
	}
	_ = peer.SetWriteDeadline(time.Time{})
	waitUntil("second ACK retirement", func() bool {
		progress := engine.txLedger.ProgressSnapshot()
		return progress.AckedNext == 2 && progress.Outstanding == 0
	})
	waitUntil("second ACK token release", func() bool { return len(engine.inflight) == 1 })
	if err := p0eWriteFrame(peer, StreamFrameClose, 0, nil); err != nil {
		t.Fatalf("send peer CLOSE: %v", err)
	}
	closeFrame := p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool { return h.Type == StreamFrameClose })
	if closeFrame.Value != 2 {
		t.Fatalf("reciprocal CLOSE sequence=%d, want 2", closeFrame.Value)
	}
	p0eWaitDone(t, engine)
	if got := len(engine.inflight); got != 0 {
		t.Fatalf("closed Core retained %d inflight tokens", got)
	}
	if stats := engine.TelemetrySnapshot(); stats.OutstandingFrames != 0 || stats.RxPendingFrames != 0 || stats.RxPendingBytes != 0 {
		t.Fatalf("closed Core retained ledger state: outstanding=%d rxFrames=%d rxBytes=%d", stats.OutstandingFrames, stats.RxPendingFrames, stats.RxPendingBytes)
	}
	var one [1]byte
	if n, err := app.Read(one[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("application Read after graceful close returned n=%d err=%v, want EOF", n, err)
	}
}

func TestP0EInvalidCloseAndLegFailureDoNotReturnEOF(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *StreamEngine, net.Conn, net.Conn)
		finish  func(*testing.T, *StreamEngine, net.Conn)
	}{
		{
			name: "premature close beyond empty receive frontier",
			finish: func(t *testing.T, _ *StreamEngine, peer net.Conn) {
				if err := p0eWriteFrame(peer, StreamFrameClose, 1, nil); err != nil {
					t.Fatalf("send premature CLOSE: %v", err)
				}
			},
		},
		{
			name: "close sequence beyond acknowledged data",
			prepare: func(t *testing.T, _ *StreamEngine, app, peer net.Conn) {
				payload := []byte("one-acknowledged-frame")
				read := make(chan error, 1)
				go func() {
					got := make([]byte, len(payload))
					_, err := io.ReadFull(app, got)
					if err == nil && !bytes.Equal(got, payload) {
						err = io.ErrUnexpectedEOF
					}
					read <- err
				}()
				if err := p0eWriteFrame(peer, StreamFrameData, 0, payload); err != nil {
					t.Fatalf("send DATA: %v", err)
				}
				select {
				case err := <-read:
					if err != nil {
						t.Fatalf("read DATA before wrong-sequence CLOSE: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("application did not receive DATA before wrong-sequence CLOSE")
				}
			},
			finish: func(t *testing.T, _ *StreamEngine, peer net.Conn) {
				_ = p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool { return h.Type == StreamFrameACK && h.Value >= 1 })
				if err := p0eWriteFrame(peer, StreamFrameClose, 2, nil); err != nil {
					t.Fatalf("send out-of-range CLOSE: %v", err)
				}
			},
		},
		{
			name: "close sequence behind acknowledged data",
			prepare: func(t *testing.T, _ *StreamEngine, app, peer net.Conn) {
				payload := []byte("one-acknowledged-frame")
				read := make(chan error, 1)
				go func() {
					got := make([]byte, len(payload))
					_, err := io.ReadFull(app, got)
					if err == nil && !bytes.Equal(got, payload) {
						err = io.ErrUnexpectedEOF
					}
					read <- err
				}()
				if err := p0eWriteFrame(peer, StreamFrameData, 0, payload); err != nil {
					t.Fatalf("send DATA: %v", err)
				}
				select {
				case err := <-read:
					if err != nil {
						t.Fatalf("read DATA before wrong-sequence CLOSE: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("application did not receive DATA before wrong-sequence CLOSE")
				}
				ack := p0eWaitFrame(t, peer, func(h StreamFrameHeader) bool {
					return h.Type == StreamFrameACK && h.Value >= 1
				})
				if ack.Value != 1 {
					t.Fatalf("ACK frontier=%d, want 1", ack.Value)
				}
			},
			finish: func(t *testing.T, _ *StreamEngine, peer net.Conn) {
				if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				defer peer.SetReadDeadline(time.Time{})
				if err := p0eWriteFrame(peer, StreamFrameClose, 0, nil); err != nil {
					t.Fatalf("send stale CLOSE: %v", err)
				}
				h, err := ReadStreamFrameHeader(peer)
				if err == nil {
					t.Fatalf("invalid stale CLOSE was accepted; received response frame type=%d value=%d", h.Type, h.Value)
				}
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					t.Fatalf("stale CLOSE did not retire the carrier promptly: %v", err)
				}
			},
		},
		{
			name: "duplicate premature close",
			finish: func(t *testing.T, _ *StreamEngine, peer net.Conn) {
				if err := p0eWriteFrame(peer, StreamFrameClose, 1, nil); err != nil {
					t.Fatalf("send first premature CLOSE: %v", err)
				}
				// The first invalid CLOSE must retire the leg. A racing duplicate may
				// either fail locally or be consumed before retirement; neither makes it valid.
				_ = p0eWriteFrame(peer, StreamFrameClose, 1, nil)
			},
		},
		{
			name: "abrupt carrier loss",
			finish: func(t *testing.T, _ *StreamEngine, peer net.Conn) {
				_ = peer.Close()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine, app, peer := p0eNewEngine(t)
			if tc.prepare != nil {
				tc.prepare(t, engine, app, peer)
			}
			readResult := p0eStartAppRead(app)
			if tc.finish != nil {
				tc.finish(t, engine, peer)
			}
			p0eWaitDone(t, engine)
			p0eAssertAppDidNotSeeEOF(t, engine, app, readResult)
		})
	}
}

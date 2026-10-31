package smp3core

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type phase20WriteReturnGate struct {
	net.Conn
	totalFrameBytes int64
	written         atomic.Int64
	entered         chan struct{}
	release         chan struct{}
	enteredOnce     sync.Once
	releaseOnce     sync.Once
}

type phase20FixedWriteCarrier struct {
	n      int
	err    error
	writeN atomic.Int32
}

func (c *phase20FixedWriteCarrier) Read([]byte) (int, error) { return 0, io.EOF }
func (c *phase20FixedWriteCarrier) Write(p []byte) (int, error) {
	c.writeN.Add(1)
	n := c.n
	if n > len(p) {
		n = len(p)
	}
	return n, c.err
}
func (c *phase20FixedWriteCarrier) Close() error { return nil }

func (c *phase20WriteReturnGate) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil || n == 0 {
		return n, err
	}
	if c.written.Add(int64(n)) >= c.totalFrameBytes {
		c.enteredOnce.Do(func() {
			close(c.entered)
			<-c.release
		})
	}
	return n, nil
}

func (c *phase20WriteReturnGate) releaseWrite() {
	c.releaseOnce.Do(func() { close(c.release) })
}

// Force a real DATA frame across a net.Pipe and let the receiver return its
// cumulative ACK, while the sender's successful Carrier Write is held just
// before Write returns to the Core writer. This uses an actual peer ACK and
// avoids sleep-based ordering.
func TestPhase20TelemetryCountsSuccessfulWriteAfterACKRetiresLedger(t *testing.T) {
	payload := bytes.Repeat([]byte("phase20-ack-before-write-return/"), 8)
	telemetry := &StreamTelemetry{}
	ackApplied := make(chan struct{}, 1)
	senderCfg := testStreamConfig()
	senderCfg.ChunkSize = len(payload)
	senderCfg.Telemetry = telemetry
	senderCfg.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 {
		if event.Kind == "ack_frontier_advanced" {
			select {
			case ackApplied <- struct{}{}:
			default:
			}
		}
		return 0
	}
	sender, senderApp := NewStreamEngine(senderCfg)
	receiver, receiverApp := NewStreamEngine(testStreamConfig())
	defer sender.Close()
	defer receiver.Close()
	defer senderApp.Close()
	defer receiverApp.Close()

	local, remote := net.Pipe()
	gate := &phase20WriteReturnGate{
		Conn: local, totalFrameBytes: int64(frameHeaderSize + len(payload)),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	defer gate.releaseWrite()
	if err := sender.AttachLeg(0, gate, nil); err != nil {
		t.Fatal(err)
	}
	if err := receiver.AttachLeg(0, remote, nil); err != nil {
		t.Fatal(err)
	}

	received := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(receiverApp, received)
		readDone <- err
	}()
	writeDone := make(chan error, 1)
	go func() {
		_, err := senderApp.Write(payload)
		writeDone <- err
	}()

	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("DATA carrier write did not reach the post-delivery return barrier")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not deliver DATA before write-return barrier")
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("receiver payload mismatch")
	}
	select {
	case <-ackApplied:
	case <-time.After(2 * time.Second):
		t.Fatal("receiver's real cumulative ACK did not retire sender ledger before Write returned")
	}
	if sender.txLedger.HasOutstanding() {
		t.Fatal("sender ledger still outstanding at the controlled ACK-before-Write-return point")
	}
	gate.releaseWrite()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sender application Write did not finish after releasing carrier return")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := sender.Snapshot()
		if stats.TxWrittenBytesByLeg[0] == uint64(len(payload)) && stats.Telemetry.DataTxAttemptFrames[0] == 1 {
			if stats.Telemetry.DataTxCarrierWriteAttemptFrames[0] != 1 || stats.Telemetry.DataTxAttemptBytes[0] != uint64(len(payload)) {
				t.Fatalf("Carrier attempt telemetry mismatch: %+v", stats.Telemetry)
			}
			if stats.Telemetry.DataTxCarrierWireBytes[0] != uint64(frameHeaderSize+len(payload)) ||
				stats.Telemetry.DataTxCarrierWriteErrorFrames[0] != 0 || stats.Telemetry.DataTxPartialWireBytes[0] != 0 {
				t.Fatalf("Carrier completion telemetry mismatch: %+v", stats.Telemetry)
			}
			if stats.TxAckedUsefulByLeg[0] != uint64(len(payload)) {
				t.Fatalf("unique ACK accounting changed: got=%v payload=%d", stats.TxAckedUsefulByLeg, len(payload))
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	stats := sender.Snapshot()
	t.Fatalf("successful Carrier write accounting did not settle: written=%v telemetry=%+v", stats.TxWrittenBytesByLeg, stats.Telemetry)
}

func TestPhase20TelemetryDistinguishesFailedAndPartialCarrierWrites(t *testing.T) {
	writeErr := io.ErrUnexpectedEOF
	for _, tc := range []struct {
		name        string
		written     int
		wantWire    uint64
		wantPartial uint64
		wantRescue  uint64
		wantRetry   uint64
	}{
		{name: "failed-before-wire-bytes", written: 0},
		{name: "partial-rescue-write", written: 3, wantWire: 3, wantPartial: 3, wantRescue: 1, wantRetry: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			telemetry := &StreamTelemetry{}
			cfg := testStreamConfig()
			cfg.Telemetry = telemetry
			engine, app := NewStreamEngine(cfg)
			defer engine.Close()
			defer app.Close()

			payload := []byte("partial carrier frame payload")
			carrier := &phase20FixedWriteCarrier{n: tc.written, err: writeErr}
			wireBytes, started, err := writeDataFrameWithStart(carrier, dataFrame{seq: 1, data: payload}, func() {
				engine.telemetry.markDataCarrierWriteAttempt(0, tc.wantRetry > 0, tc.wantRescue > 0, uint64(len(payload)))
			})
			if err == nil || !started {
				t.Fatalf("expected attempted write failure, started=%t err=%v", started, err)
			}
			engine.telemetry.finishDataCarrierWrite(0, uint64(wireBytes), uint64(frameHeaderSize+len(payload)), started, err)

			stats := engine.TelemetrySnapshot().Telemetry
			if stats.DataTxCarrierWriteAttemptFrames[0] != 1 || stats.DataTxAttemptBytes[0] != uint64(len(payload)) {
				t.Fatalf("attempt counts mismatch: %+v", stats)
			}
			if stats.DataTxCarrierWriteErrorFrames[0] != 1 || stats.DataTxCarrierWireBytes[0] != tc.wantWire || stats.DataTxPartialWireBytes[0] != tc.wantPartial {
				t.Fatalf("wire/error counts mismatch: %+v", stats)
			}
			if stats.DataTxRescueWriteAttemptFrames[0] != tc.wantRescue || stats.DataTxRescueWriteAttemptBytes[0] != uint64(tc.wantRescue)*uint64(len(payload)) || stats.DataTxRetryWriteAttemptFrames[0] != tc.wantRetry {
				t.Fatalf("retry/rescue attempt classification mismatch: %+v", stats)
			}
			if stats.DataTxAttemptFrames[0] != 0 || engine.Snapshot().TxWrittenBytesByLeg[0] != 0 || engine.txLedger.HasOutstanding() {
				t.Fatalf("failed write was counted as complete or changed ledger: telemetry=%+v stats=%+v", stats, engine.Snapshot())
			}
		})
	}
}

func TestPhase20TelemetryCountsCarrierCloseRacingWrite(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	local, peer := net.Pipe()
	defer local.Close()
	startedCh := make(chan struct{})
	resultCh := make(chan struct {
		written int64
		started bool
		err     error
	}, 1)
	payload := []byte("close-write-race")
	go func() {
		written, started, err := writeDataFrameWithStart(local, dataFrame{seq: 1, data: payload}, func() {
			engine.telemetry.markDataCarrierWriteAttempt(0, false, false, uint64(len(payload)))
			close(startedCh)
		})
		resultCh <- struct {
			written int64
			started bool
			err     error
		}{written: written, started: started, err: err}
	}()
	<-startedCh
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	result := <-resultCh
	engine.telemetry.finishDataCarrierWrite(0, uint64(result.written), uint64(frameHeaderSize+len(payload)), result.started, result.err)
	if !result.started || result.err == nil {
		t.Fatalf("expected close to interrupt an entered Carrier write: %+v", result)
	}
	stats := engine.TelemetrySnapshot().Telemetry
	if stats.DataTxCarrierWriteAttemptFrames[0] != 1 || stats.DataTxAttemptBytes[0] != uint64(len(payload)) || stats.DataTxCarrierWriteErrorFrames[0] != 1 {
		t.Fatalf("close/write attempt accounting mismatch: %+v", stats)
	}
	if stats.DataTxAttemptFrames[0] != 0 || stats.DataTxCarrierWireBytes[0] != 0 || stats.DataTxPartialWireBytes[0] != 0 {
		t.Fatalf("interrupted write incorrectly counted as complete/partial: %+v", stats)
	}
}

func TestPhase20TelemetryCountsWriteAndACKLockRaceExactlyOnce(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	const iterations = 256
	for i := 0; i < iterations; i++ {
		payload := []byte{byte(i)}
		record := engine.txLedger.Add(payload, time.Now())
		if !engine.txLedger.MarkTransit(record, LegID(0), time.Now()) {
			t.Fatalf("iteration %d: failed to mark transit", i)
		}
		start := make(chan struct{})
		writeResult := make(chan StreamTXAttemptResult, 1)
		ackResult := make(chan error, 1)
		go func() {
			<-start
			writeResult <- engine.commitSuccessfulDataWrite(txSendAttempt{record: record, ordinary: true}, 0)
		}()
		go func() {
			<-start
			ackResult <- engine.HandleACKForTest(record.Sequence() + 1)
		}()
		close(start)
		<-writeResult
		if err := <-ackResult; err != nil {
			t.Fatalf("iteration %d: ACK error: %v", i, err)
		}
		if engine.txLedger.IsOutstanding(record) {
			t.Fatalf("iteration %d: ledger did not retire after ACK", i)
		}
	}

	stats := engine.Snapshot()
	if got := stats.Telemetry.DataTxAttemptFrames[0]; got != iterations {
		t.Fatalf("write/ACK race telemetry count=%d want=%d", got, iterations)
	}
	if got := stats.Telemetry.DataTxRetransmitFrames[0]; got != 0 {
		t.Fatalf("ordinary writes misclassified as retransmits: %d", got)
	}
	if got := stats.TxAckedUsefulByLeg[0]; got != iterations {
		t.Fatalf("unique ACK bytes=%d want=%d", got, iterations)
	}
}

func TestPhase20TelemetryCountsRescueWriteAfterOriginalACKOnce(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	payload := []byte("race original ACK against rescue write")
	record := engine.txLedger.Add(payload, time.Now())
	if !engine.txLedger.MarkTransit(record, LegID(0), time.Now()) {
		t.Fatal("failed to mark original DATA transit")
	}
	if result := engine.commitSuccessfulDataWrite(txSendAttempt{record: record, ordinary: true}, 0); !result.Applied {
		t.Fatalf("original DATA write result=%+v", result)
	}
	if _, ok := engine.txLedger.MarkRescueTransit(record, LegID(1), time.Now()); !ok {
		t.Fatal("failed to mark rescue DATA transit")
	}
	if err := engine.HandleACKForTest(record.Sequence() + 1); err != nil {
		t.Fatal(err)
	}
	if result := engine.commitSuccessfulDataWrite(txSendAttempt{record: record, rescue: true}, 1); result.Applied {
		t.Fatalf("ACK-retired rescue ledger unexpectedly applied: %+v", result)
	}
	if err := engine.HandleACKForTest(record.Sequence() + 1); err != nil {
		t.Fatal(err)
	}
	stats := engine.Snapshot()
	if stats.Telemetry.DataTxAttemptFrames != [2]uint64{1, 1} || stats.Telemetry.DataTxRetransmitFrames != [2]uint64{0, 1} {
		t.Fatalf("write/rescue telemetry mismatch: %+v", stats.Telemetry)
	}
	if stats.Telemetry.RescueAttemptFrames != [2]uint64{0, 1} || stats.Telemetry.RescueAttemptBytes[1] != uint64(len(payload)) {
		t.Fatalf("rescue telemetry mismatch: %+v", stats.Telemetry)
	}
	if stats.TxAckedUsefulTotal != uint64(len(payload)) {
		t.Fatalf("duplicate ACK or rescue double-counted useful bytes: %d", stats.TxAckedUsefulTotal)
	}
}

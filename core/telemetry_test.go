package smp3core

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestStreamTelemetryDisabledIsZeroAndNonIntrusive(t *testing.T) {
	engine, app := NewStreamEngine(StreamConfig{Telemetry: nil})
	defer engine.Close()
	defer app.Close()

	stats := engine.Snapshot()
	if stats.Telemetry.DataTxAttemptFrames != [2]uint64{} {
		t.Fatalf("disabled telemetry has DATA attempt frames: %+v", stats.Telemetry.DataTxAttemptFrames)
	}
	if stats.Telemetry.DataRxUniqueFrames != [2]uint64{} {
		t.Fatalf("disabled telemetry has DATA RX frames: %+v", stats.Telemetry.DataRxUniqueFrames)
	}
	if stats.Telemetry.FirstTxDataAt[0].IsZero() == false || stats.Telemetry.FirstTxDataAt[1].IsZero() == false {
		t.Fatalf("disabled telemetry has first-TX timestamps: %+v", stats.Telemetry.FirstTxDataAt)
	}
}

func TestStreamTelemetryCountsDataAndControl(t *testing.T) {
	leftTelemetry := &StreamTelemetry{}
	rightTelemetry := &StreamTelemetry{}
	leftCfg := testStreamConfig()
	leftCfg.Telemetry = leftTelemetry
	rightCfg := testStreamConfig()
	rightCfg.Telemetry = rightTelemetry
	left, leftApp := NewStreamEngine(leftCfg)
	right, rightApp := NewStreamEngine(rightCfg)
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()
	attachEngineLegPair(t, left, right, 0)

	payload := bytes.Repeat([]byte("telemetry-data"), 80)
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(rightApp, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		readDone <- err
	}()
	writeDone := make(chan error, 1)
	go func() { _, err := leftApp.Write(payload); writeDone <- err }()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for telemetry payload")
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leftStats := left.Snapshot()
		rightStats := right.Snapshot()
		if leftStats.Telemetry.DataTxAttemptFrames[0] > 0 &&
			rightStats.Telemetry.DataRxUniqueFrames[0] > 0 &&
			leftStats.Telemetry.AckRxFrames[0] > 0 &&
			rightStats.Telemetry.AckTxFrames[0] > 0 {
			if leftStats.Telemetry.FirstTxDataAt[0].IsZero() || rightStats.Telemetry.FirstRxUniqueDataAt[0].IsZero() {
				t.Fatal("data counters advanced without first-data timestamps")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("telemetry did not observe data/control: left=%+v right=%+v", left.Snapshot().Telemetry, right.Snapshot().Telemetry)
}

func TestStreamTelemetryCountsUniqueAndDuplicateRXFrames(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	firstRead := make(chan error, 1)
	go func() {
		buffer := make([]byte, 3)
		_, err := io.ReadFull(app, buffer)
		firstRead <- err
	}()
	engine.InjectFrameForTest(0, []byte("one"), 0)
	if err := <-firstRead; err != nil {
		t.Fatal(err)
	}
	engine.InjectFrameForTest(0, []byte("one"), 0)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := engine.Snapshot().Telemetry
		if stats.DataRxUniqueFrames[0] == 1 && stats.DataRxDuplicateFrames[0] == 1 && stats.DataRxDuplicateBytes[0] == 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("unexpected RX telemetry=%+v", engine.Snapshot().Telemetry)
}

func TestStreamTelemetryFirstDataFromLeg1(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 3)
		_, err := io.ReadFull(app, buffer)
		readDone <- err
	}()
	engine.InjectFrameForTest(0, []byte("one"), 1)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := engine.Snapshot().Telemetry
		if !stats.FirstRxUniqueDataAt[1].IsZero() {
			if !stats.FirstRxUniqueDataAt[0].IsZero() {
				t.Fatalf("leg0 was incorrectly first: %+v", stats.FirstRxUniqueDataAt)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("leg1 first-data timestamp missing: %+v", engine.Snapshot().Telemetry)
}

func BenchmarkStreamTelemetryCounterPath(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		b.ReportAllocs()
		var telemetry *StreamTelemetry
		for i := 0; i < b.N; i++ {
			if telemetry != nil {
				telemetry.markDataTx(0, false, false, 128)
			}
		}
	})
	b.Run("enabled", func(b *testing.B) {
		b.ReportAllocs()
		telemetry := &StreamTelemetry{}
		for i := 0; i < b.N; i++ {
			telemetry.markDataTx(0, false, false, 128)
		}
	})
}

func TestStreamTelemetryAttributesSuccessfulRescueAttempt(t *testing.T) {
	telemetry := &StreamTelemetry{}
	cfg := testStreamConfig()
	cfg.Telemetry = telemetry
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()

	record := engine.txLedger.Add([]byte("rescue"), time.Now().Add(-time.Second))
	if !engine.txLedger.MarkTransit(record, LegID(0), time.Now()) {
		t.Fatal("failed to mark original attempt")
	}
	if result := engine.markAttemptSent(record, 0, false); !result.Applied {
		t.Fatalf("original attempt result=%+v", result)
	}
	started := time.Now()
	if _, ok := engine.txLedger.MarkRescueTransit(record, LegID(1), started); !ok {
		t.Fatal("failed to mark rescue attempt")
	}
	result := engine.markAttemptSent(record, 1, true)
	if !result.Applied || !result.Retransmit {
		t.Fatalf("mark attempt result=%+v", result)
	}
	stats := engine.Snapshot()
	if stats.Telemetry.DataTxAttemptFrames[1] != 1 || stats.Telemetry.DataTxRetransmitFrames[1] != 1 || stats.Telemetry.RescueAttemptFrames[1] != 1 || stats.Telemetry.RescueAttemptBytes[1] != uint64(len("rescue")) {
		t.Fatalf("rescue telemetry=%+v", stats.Telemetry)
	}
	if stats.TxAckedUsefulByLeg != [2]uint64{} {
		t.Fatalf("rescue unexpectedly changed logical useful ACK bytes before ACK=%v", stats.TxAckedUsefulByLeg)
	}
	if err := engine.HandleACKForTest(record.Sequence() + 1); err != nil {
		t.Fatal(err)
	}
	stats = engine.Snapshot()
	if stats.TxAckedUsefulByLeg[0]+stats.TxAckedUsefulByLeg[1] != uint64(len("rescue")) {
		t.Fatalf("logical useful ACK bytes double-counted=%v", stats.TxAckedUsefulByLeg)
	}
}

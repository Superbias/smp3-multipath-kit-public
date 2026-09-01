package smp3core

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestStreamActivationRateTXOnly(t *testing.T) {
	if got := streamActivationRateBytesPS(7500000, 0, time.Second); got != 7500000 {
		t.Fatalf("TX activation rate=%d, want 7500000 bytes/s", got)
	}
}

func TestStreamActivationRateRXOnly(t *testing.T) {
	if got := streamActivationRateBytesPS(0, 7500000, time.Second); got != 7500000 {
		t.Fatalf("RX activation rate=%d, want 7500000 bytes/s", got)
	}
}

func TestStreamActivationRateBelowThreshold(t *testing.T) {
	if streamActivationEligible(3750000, 2500000, time.Second, 6250000) {
		t.Fatal("below-threshold directional rates activated the booster")
	}
}

func TestStreamActivationRateUsesMaximumNotSum(t *testing.T) {
	const threshold = 6250000 // 50 Mbps in application payload bytes/s.
	if got := streamActivationRateBytesPS(3750000, 3750000, time.Second); got != 3750000 {
		t.Fatalf("activation rate=%d, want max directional rate 3750000", got)
	}
	if streamActivationEligible(3750000, 3750000, time.Second, threshold) {
		t.Fatal("TX+RX was incorrectly used to cross the activation threshold")
	}
}

func TestStreamActivationRateAtThresholdUsesInclusiveBoundary(t *testing.T) {
	if !streamActivationEligible(6250000, 0, time.Second, 6250000) {
		t.Fatal("TX rate exactly at threshold did not activate")
	}
	if !streamActivationEligible(0, 6250000, time.Second, 6250000) {
		t.Fatal("RX rate exactly at threshold did not activate")
	}
}

func runStreamEnginePayloadActivation(t *testing.T, activateOnRX bool) {
	t.Helper()
	activated := make(chan struct{}, 2)
	activeConfig := StreamConfig{
		ChunkSize:         1024,
		QueueFrames:       32,
		MaxReorderFrames:  64,
		MaxInflightFrames: 64,
		ActivationWindow:  100 * time.Millisecond,
		ThresholdBytesPS:  125000, // 1 Mbps in application payload bytes/s.
		OnActivate:        func() { activated <- struct{}{} },
	}
	inactiveConfig := activeConfig
	inactiveConfig.ThresholdBytesPS = 1 << 60
	inactiveConfig.OnActivate = nil

	txConfig, rxConfig := inactiveConfig, inactiveConfig
	if activateOnRX {
		rxConfig = activeConfig
	} else {
		txConfig = activeConfig
	}
	tx, txApp := NewStreamEngine(txConfig)
	rx, rxApp := NewStreamEngine(rxConfig)
	defer tx.Close()
	defer rx.Close()
	defer txApp.Close()
	defer rxApp.Close()

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if err := tx.AttachLeg(0, left, nil); err != nil {
		t.Fatal(err)
	}
	if err := rx.AttachLeg(0, right, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("rx-activation"), 32*1024)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, rxApp, int64(len(payload)))
		readDone <- err
	}()
	writeDone := make(chan error, 1)
	go func() {
		_, err := txApp.Write(payload)
		writeDone <- err
	}()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RX payload")
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out writing activation payload")
	}

	select {
	case <-activated:
	case <-time.After(2 * time.Second):
		t.Fatal("application RX payload did not activate the booster")
	}
	time.Sleep(20 * time.Millisecond)
	if len(activated) != 0 {
		t.Fatal("activation callback fired more than once")
	}
}

func TestStreamEngineTXPayloadActivatesBooster(t *testing.T) {
	runStreamEnginePayloadActivation(t, false)
}

func TestStreamEngineRXPayloadActivatesBooster(t *testing.T) {
	runStreamEnginePayloadActivation(t, true)
}

func TestStreamEngineActivationCallbackIsOneShot(t *testing.T) {
	activated := make(chan struct{}, 2)
	engine, app := NewStreamEngine(StreamConfig{
		ThresholdBytesPS: 1,
		OnActivate:       func() { activated <- struct{}{} },
	})
	defer engine.Close()
	defer app.Close()

	engine.activate()
	engine.activate()
	if len(activated) != 1 {
		t.Fatalf("activation callback count=%d, want 1", len(activated))
	}
}

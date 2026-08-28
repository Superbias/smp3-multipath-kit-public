package multipath

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

const testGoodputUnit = uint64(10 * 1024 * 1024)

func adaptiveTestStats(now time.Time, rx, tx, leg1Rx, leg1Tx uint64) coreStats {
	return coreStats{
		RxDeliveredBytes:   rx,
		TxAckedUsefulByLeg: [2]uint64{tx - leg1Tx, leg1Tx},
		RxUniqueBytesByLeg: [2]uint64{rx - leg1Rx, leg1Rx},
		LastAckProgress:    now,
	}
}

func adaptiveTestController() (*adaptiveController, time.Time) {
	settings := defaultAdaptiveSettings()
	settings.Warmup = 0
	settings.SuspectWindow = 8 * time.Second
	settings.MinRxReorder = 64
	settings.RxGapStall = 1500 * time.Millisecond
	settings.MinTxOutstanding = 64
	settings.TxAckStall = 2 * time.Second
	readyAt := time.Unix(1000, 0)
	return newAdaptiveController(settings, readyAt), readyAt
}

func establishDirectionalBaseline(controller *adaptiveController, start time.Time) {
	controller.observe(start, adaptiveTestStats(start, 0, 0, 0, 0))
	controller.observe(start.Add(time.Second), adaptiveTestStats(start.Add(time.Second), testGoodputUnit, testGoodputUnit, testGoodputUnit, testGoodputUnit))
	controller.observe(start.Add(2*time.Second), adaptiveTestStats(start.Add(2*time.Second), 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit))
}

func TestAdaptiveHealthyGoodputIgnoresShortReorder(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	stats := adaptiveTestStats(start.Add(3*time.Second), 3*testGoodputUnit, 3*testGoodputUnit, 3*testGoodputUnit, 3*testGoodputUnit)
	stats.RxPendingFrames = 96
	stats.RxGapAge = 2 * time.Second
	decision := controller.observe(start.Add(3*time.Second), stats)
	if decision.State != adaptiveHealthy || decision.Fallback {
		t.Fatalf("short reorder changed carrier state: %+v", decision)
	}
}

func TestAdaptiveRXBadTXGoodFallsBack(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	for _, elapsed := range []time.Duration{3 * time.Second, 6 * time.Second, 11 * time.Second} {
		stats := adaptiveTestStats(start.Add(elapsed), 2*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit)
		stats.RxPendingFrames = 256
		stats.RxGapAge = 3 * time.Second
		decision := controller.observe(start.Add(elapsed), stats)
		if elapsed == 3*time.Second && decision.State != adaptiveSuspect {
			t.Fatalf("RX degradation did not enter SUSPECT: %+v", decision)
		}
		if elapsed == 11*time.Second && (!decision.Fallback || decision.Reason != "rx_reorder") {
			t.Fatalf("RX degradation did not fallback while TX stayed healthy: %+v", decision)
		}
	}
}

func TestAdaptiveTXBadRXGoodFallsBack(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	for _, elapsed := range []time.Duration{3 * time.Second, 6 * time.Second, 14 * time.Second} {
		stats := adaptiveTestStats(start.Add(elapsed), uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit)
		stats.OutstandingFrames = 96
		stats.OutstandingFramesByLeg[1] = 96
		stats.OutstandingBytes = 96 * 64 * 1024
		stats.OldestOutstandingAge = elapsed
		stats.AckFrontierValid = true
		stats.AckFrontierLeg = 1
		stats.AckFrontierAge = elapsed
		stats.LastAckProgress = start
		decision := controller.observe(start.Add(elapsed), stats)
		if elapsed == 3*time.Second && decision.State != adaptiveHealthy {
			t.Fatalf("single leg1-frontier sample entered SUSPECT too early: %+v", decision)
		}
		if elapsed == 6*time.Second && decision.State != adaptiveSuspect {
			t.Fatalf("stable TX degradation did not enter SUSPECT: %+v", decision)
		}
		if elapsed == 14*time.Second && (!decision.Fallback || decision.Reason != "tx_ack_stall") {
			t.Fatalf("TX degradation did not fallback while RX stayed healthy: %+v", decision)
		}
	}
}

func TestAdaptiveTXBlockedByLeg0FrontierDoesNotBlameHy2(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	for _, elapsed := range []time.Duration{3 * time.Second, 6 * time.Second, 11 * time.Second, 40 * time.Second} {
		// Model the R7 live failure: global cumulative ACK progress is old and many
		// leg1 frames are outstanding, but the oldest unacked sequence is a slow
		// leg0 frame. Later Hy2 frames cannot retire until that earlier gap closes.
		stats := adaptiveTestStats(start.Add(elapsed), uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit)
		stats.OutstandingFrames = 128
		stats.OutstandingFramesByLeg[0] = 32
		stats.OutstandingFramesByLeg[1] = 96
		stats.OutstandingBytes = 128 * 64 * 1024
		stats.OldestOutstandingAge = elapsed
		stats.AckFrontierValid = true
		stats.AckFrontierLeg = 0
		stats.AckFrontierAge = elapsed
		stats.LastAckProgress = start
		decision := controller.observe(start.Add(elapsed), stats)
		if decision.State != adaptiveHealthy || decision.Fallback || decision.TxPressure {
			t.Fatalf("slow leg0 cumulative-ACK frontier blamed Hy2 at %s: %+v", elapsed, decision)
		}
	}
}

func TestAdaptiveConcurrentFrontierRescueDoesNotBlameHy2(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	for _, elapsed := range []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second} {
		stats := adaptiveTestStats(start.Add(elapsed), uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, 2*testGoodputUnit)
		stats.OutstandingFrames = 128
		stats.OutstandingFramesByLeg = [2]int{64, 96}
		stats.AckFrontierValid = true
		stats.AckFrontierLeg = 1 // newest diagnostic attempt is the rescue
		stats.AckFrontierMultiPath = true
		stats.AckFrontierAge = elapsed
		stats.LastAckProgress = start
		decision := controller.observe(start.Add(elapsed), stats)
		if decision.TxPressure || decision.State != adaptiveHealthy || decision.Fallback {
			t.Fatalf("concurrent leg0+leg1 frontier rescue blamed Hy2 at %s: %+v", elapsed, decision)
		}
	}
}

func TestAdaptiveTransientRXIssueRecoversWithIdleTX(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	stalled := adaptiveTestStats(start.Add(3*time.Second), 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit)
	stalled.RxPendingFrames = 128
	stalled.RxGapAge = 2 * time.Second
	if decision := controller.observe(start.Add(3*time.Second), stalled); decision.State != adaptiveSuspect {
		t.Fatalf("expected RX SUSPECT, got %+v", decision)
	}
	recovered := adaptiveTestStats(start.Add(4*time.Second), 3*testGoodputUnit, 2*testGoodputUnit, 3*testGoodputUnit, 2*testGoodputUnit)
	decision := controller.observe(start.Add(4*time.Second), recovered)
	if !decision.Recovered || decision.State != adaptiveHealthy || decision.Fallback {
		t.Fatalf("RX transient did not recover: %+v", decision)
	}
}

func TestAdaptiveTransientTXIssueRecoversWithIdleRX(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	stalled := adaptiveTestStats(start.Add(3*time.Second), 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit)
	stalled.OutstandingFrames = 96
	stalled.OutstandingFramesByLeg[1] = 96
	stalled.OldestOutstandingAge = 3 * time.Second
	stalled.AckFrontierValid = true
	stalled.AckFrontierLeg = 1
	stalled.AckFrontierAge = 3 * time.Second
	stalled.LastAckProgress = start
	if decision := controller.observe(start.Add(3*time.Second), stalled); decision.State != adaptiveHealthy {
		t.Fatalf("single TX attribution sample should remain HEALTHY, got %+v", decision)
	}
	stalled2 := stalled
	stalled2.OldestOutstandingAge = 6 * time.Second
	stalled2.AckFrontierAge = 6 * time.Second
	if decision := controller.observe(start.Add(6*time.Second), stalled2); decision.State != adaptiveSuspect {
		t.Fatalf("stable TX stall did not enter SUSPECT, got %+v", decision)
	}
	recovered := adaptiveTestStats(start.Add(7*time.Second), 2*testGoodputUnit, 3*testGoodputUnit, 2*testGoodputUnit, 3*testGoodputUnit)
	decision := controller.observe(start.Add(7*time.Second), recovered)
	if !decision.Recovered || decision.State != adaptiveHealthy || decision.Fallback {
		t.Fatalf("TX transient did not recover: %+v", decision)
	}
}

func TestAdaptiveLeg0PressureDoesNotBlameHealthyHy2(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	for _, elapsed := range []time.Duration{3 * time.Second, 6 * time.Second, 11 * time.Second, 40 * time.Second} {
		stats := adaptiveTestStats(start.Add(elapsed), uint64(elapsed/time.Second)*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit, uint64(elapsed/time.Second)*testGoodputUnit)
		stats.RxPendingFrames = 256
		stats.RxGapAge = 3 * time.Second
		decision := controller.observe(start.Add(elapsed), stats)
		if decision.Fallback || decision.State != adaptiveHealthy {
			t.Fatalf("leg0-only pressure blamed healthy Hy2 at %s: %+v", elapsed, decision)
		}
	}
}

func TestAdaptiveIdleDoesNotTrainBaselineOrFallback(t *testing.T) {
	controller, start := adaptiveTestController()
	establishDirectionalBaseline(controller, start)
	rxBaseline := controller.rxBaselineGoodputBPS
	txBaseline := controller.txBaselineGoodputBPS
	decision := controller.observe(start.Add(30*time.Second), adaptiveTestStats(start.Add(30*time.Second), 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit, 2*testGoodputUnit))
	if decision.Demand || decision.Fallback || decision.State != adaptiveHealthy {
		t.Fatalf("idle connection changed state: %+v", decision)
	}
	if controller.rxBaselineGoodputBPS != rxBaseline || controller.txBaselineGoodputBPS != txBaseline {
		t.Fatalf("idle window retrained directional baselines: rx=%f/%f tx=%f/%f", controller.rxBaselineGoodputBPS, rxBaseline, controller.txBaselineGoodputBPS, txBaseline)
	}
}

func TestAdaptiveBaselineDecaysSlowlyAndFreezesInSuspect(t *testing.T) {
	controller, start := adaptiveTestController()
	controller.observe(start, adaptiveTestStats(start, 0, 0, 0, 0))
	var total uint64
	for i, rate := range []uint64{300, 220, 180, 130, 100} {
		total += rate
		stats := adaptiveTestStats(start.Add(time.Duration(i+1)*time.Second), total, 0, total, 0)
		controller.observe(start.Add(time.Duration(i+1)*time.Second), stats)
	}
	baseline := controller.rxBaselineGoodputBPS
	if baseline <= 150 {
		t.Fatalf("RX baseline decayed too quickly: %f", baseline)
	}
	pressure := adaptiveTestStats(start.Add(6*time.Second), total, 0, total, 0)
	pressure.RxPendingFrames = 256
	pressure.RxGapAge = 3 * time.Second
	controller.observe(start.Add(6*time.Second), pressure)
	frozen := controller.rxBaselineGoodputBPS
	pressure.RxGapAge = 4 * time.Second
	controller.observe(start.Add(7*time.Second), pressure)
	if controller.rxBaselineGoodputBPS != frozen {
		t.Fatalf("RX baseline changed in SUSPECT: before=%f after=%f", frozen, controller.rxBaselineGoodputBPS)
	}
}

func TestAdaptiveLongNormalTrafficAndShortGapsDoNotFallback(t *testing.T) {
	controller, start := adaptiveTestController()
	controller.observe(start, adaptiveTestStats(start, 0, 0, 0, 0))
	var total uint64
	for second := 1; second <= 60; second++ {
		total += testGoodputUnit
		stats := adaptiveTestStats(start.Add(time.Duration(second)*time.Second), total, total, total, total)
		stats.RxPendingFrames = 8
		stats.RxGapAge = 200 * time.Millisecond
		decision := controller.observe(start.Add(time.Duration(second)*time.Second), stats)
		if decision.Fallback || decision.State != adaptiveHealthy {
			t.Fatalf("normal traffic fell back at second %d: %+v", second, decision)
		}
	}
}

func TestRxGapTrackerContinuousPendingStartsNewTimerForEachExpectedGap(t *testing.T) {
	start := time.Unix(3000, 0)
	tracker := rxGapTracker{}
	pending := map[uint64]dataFrame{103: {seq: 103, receivedAt: start}}
	tracker.refresh(100, pending, start)
	if tracker.gapExpectedSeq != 100 || !tracker.since.Equal(start) {
		t.Fatalf("initial gap=%+v", tracker)
	}

	// The pending map never becomes empty. Each repaired expected sequence still
	// starts a fresh timer for the next unresolved gap.
	for i, gapAge := range []time.Duration{200 * time.Millisecond, 150 * time.Millisecond, 300 * time.Millisecond} {
		now := start.Add(30*time.Second + time.Duration(i)*time.Second)
		expected := uint64(100 + i + 1)
		pending = map[uint64]dataFrame{expected + 2: {seq: expected + 2, receivedAt: now}}
		tracker.refresh(expected, pending, now)
		if tracker.gapExpectedSeq != expected || !tracker.since.Equal(now) {
			t.Fatalf("gap %d retained stale timer: %+v", i, tracker)
		}
		if now.Add(gapAge).Sub(tracker.since) != gapAge {
			t.Fatalf("gap %d age calculation is not current-gap based", i)
		}
	}
	pending = map[uint64]dataFrame{}
	tracker.refresh(104, pending, start.Add(33*time.Second))
	if !tracker.since.IsZero() {
		t.Fatalf("continuous pending tracker did not clear: %+v", tracker)
	}
}

func TestGlobalInitialHy2FailureLearningUsesWindow(t *testing.T) {
	settings := defaultAdaptiveSettings()
	settings.InitialFailureThreshold = 3
	settings.InitialFailureWindow = 30 * time.Second
	start := time.Unix(4000, 0)
	health := hy2GlobalHealth{}
	if health.noteInitialFailure(start, settings) || health.cooldownActive(start) {
		t.Fatal("one initial failure entered global cooldown")
	}
	if health.noteInitialFailure(start.Add(10*time.Second), settings) || health.cooldownActive(start.Add(10*time.Second)) {
		t.Fatal("below-threshold initial failures entered global cooldown")
	}
	if !health.noteInitialFailure(start.Add(20*time.Second), settings) {
		t.Fatal("threshold initial failures did not request cooldown")
	}
	if !health.cooldownActive(start.Add(20 * time.Second)) {
		t.Fatal("threshold initial failures did not enter cooldown")
	}

	expiring := hy2GlobalHealth{}
	if expiring.noteInitialFailure(start, settings) {
		t.Fatal("first expiring failure unexpectedly crossed threshold")
	}
	if expiring.noteInitialFailure(start.Add(31*time.Second), settings) {
		t.Fatal("expired initial failure was counted")
	}
	if expiring.noteInitialFailure(start.Add(32*time.Second), settings) {
		t.Fatal("two failures inside window unexpectedly crossed threshold")
	}
	expiring.noteActiveSuccess()
	if expiring.noteInitialFailure(start.Add(33*time.Second), settings) {
		t.Fatal("successful active period did not clear initial failure history")
	}
}

func probationConnection(t *testing.T, start time.Time) (*adaptiveConn, *hy2GlobalHealth, adaptiveSettings) {
	t.Helper()
	settings := defaultAdaptiveSettings()
	health := &hy2GlobalHealth{}
	health.noteFallback(start, settings, false)
	selection := health.selectCarrier(start.Add(settings.Cooldown))
	if selection.carrier != carrierHy2 || !selection.probation {
		t.Fatalf("expected Hy2 probation selection, got %+v", selection)
	}
	conn := newAdaptiveConn(true, settings, health, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	conn.probation = true
	return conn, health, settings
}

func TestProbationRequiresHy2UsefulBytesAndActiveWindows(t *testing.T) {
	start := time.Unix(5000, 0)
	conn, health, settings := probationConnection(t, start)
	for second := 0; second <= 20; second++ {
		if conn.observeCanary(start.Add(time.Duration(second)*time.Second), adaptiveDecision{State: adaptiveHealthy, Demand: true}) {
			t.Fatal("zero Hy2 useful bytes became successful probation")
		}
	}
	if next := health.selectCarrier(start.Add(settings.Cooldown + time.Second)); next.carrier != carrierSnell {
		t.Fatalf("zero-useful canary released global owner unexpectedly: %+v", next)
	}

	conn, health, settings = probationConnection(t, start.Add(100*time.Second))
	for second := 0; second <= 20; second++ {
		useful := uint64(0)
		if second < 3 {
			useful = 256 * 1024
		}
		if conn.observeCanary(start.Add(100*time.Second+time.Duration(second)*time.Second), adaptiveDecision{
			State:             adaptiveHealthy,
			Demand:            useful > 0,
			Leg1RxUsefulBytes: useful,
		}) {
			t.Fatal("below-minimum useful canary became successful probation")
		}
	}
	if next := health.selectCarrier(start.Add(121 * time.Second)); next.carrier != carrierSnell {
		t.Fatalf("below-minimum canary released global owner unexpectedly: %+v", next)
	}
}

func TestProbationRecoveryRequiresRealHy2Traffic(t *testing.T) {
	start := time.Unix(6000, 0)
	conn, health, settings := probationConnection(t, start)
	recovered := false
	for second := 0; second <= 20; second++ {
		recovered = conn.observeCanary(start.Add(time.Duration(second)*time.Second), adaptiveDecision{
			State:             adaptiveHealthy,
			Demand:            true,
			Leg1RxUsefulBytes: 512 * 1024,
		})
		if recovered {
			break
		}
	}
	if !recovered || conn.probation {
		t.Fatalf("real Hy2 useful traffic did not complete probation: recovered=%v probation=%v", recovered, conn.probation)
	}
	health.noteRecovery()
	if next := health.selectCarrier(start.Add(settings.Cooldown + time.Second)); next.carrier != carrierHy2 || next.probation {
		t.Fatalf("successful canary did not restore available Hy2: %+v", next)
	}
}

func TestAdaptiveSingleProbationCanaryWith100ConcurrentSessions(t *testing.T) {
	settings := defaultAdaptiveSettings()
	start := time.Unix(7000, 0)
	health := hy2GlobalHealth{}
	health.noteFallback(start, settings, false)
	var wg sync.WaitGroup
	results := make(chan adaptiveSelection, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- health.selectCarrier(start.Add(settings.Cooldown))
		}()
	}
	wg.Wait()
	close(results)
	canaries := 0
	for result := range results {
		if result.carrier == carrierHy2 && result.probation {
			canaries++
		} else if result.carrier != carrierSnell {
			t.Fatalf("non-owner selected unexpected carrier: %+v", result)
		}
	}
	if canaries != 1 {
		t.Fatalf("got %d concurrent Hy2 canaries, want exactly one", canaries)
	}
}

func TestAdaptiveProbationFailureBacksOffAndNormalCloseReleasesOwner(t *testing.T) {
	settings := defaultAdaptiveSettings()
	start := time.Unix(8000, 0)
	health := hy2GlobalHealth{}
	health.noteFallback(start, settings, false)
	canary := health.selectCarrier(start.Add(settings.Cooldown))
	if canary.carrier != carrierHy2 || !canary.probation {
		t.Fatalf("expected probation canary, got %+v", canary)
	}
	health.releaseProbation()
	if next := health.selectCarrier(start.Add(settings.Cooldown)); next.carrier != carrierHy2 || !next.probation {
		t.Fatalf("normal close did not release canary owner: %+v", next)
	}
	health.noteFallback(start.Add(settings.Cooldown), settings, true)
	if next := health.selectCarrier(start.Add(settings.Cooldown + time.Second)); next.carrier != carrierSnell {
		t.Fatalf("probation failure did not re-enter cooldown: %+v", next)
	}
	if got := health.cooldownPenalty; got != 180*time.Second {
		t.Fatalf("probation failure did not back off cooldown: %s", got)
	}
}

func TestAdaptiveCarrierSwitchKeepsLogicalCoreAndPayload(t *testing.T) {
	leftCore, leftApp := newCore(testCoreConfig())
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		t.Fatal(err)
	}
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		t.Fatal(err)
	}

	settings := defaultAdaptiveSettings()
	health := &hy2GlobalHealth{}
	adaptive := newAdaptiveConn(true, settings, health, leftCore)
	adaptive.selected = true
	adaptive.carrier = carrierHy2
	var fallbacks atomic.Int32
	adaptive.onFallback = func(string, bool, bool) {
		fallbacks.Add(1)
		if !leftCore.replaceLeg(1, errors.New("fake Hy2 degradation")) {
			t.Error("left leg1 was not replaced")
		}
		_ = rightCore.replaceLeg(1, errors.New("fake Hy2 degradation"))
	}
	if !adaptive.switchToSnell("fake_hy2_degradation", true) {
		t.Fatal("adaptive carrier did not switch")
	}
	if adaptive.currentCarrier() != carrierSnell || fallbacks.Load() != 1 {
		t.Fatalf("unexpected fallback state: carrier=%s callbacks=%d", adaptive.currentCarrier(), fallbacks.Load())
	}
	if leftCore.hasLeg(1) || !leftCore.hasLeg(0) || rightCore.hasLeg(1) {
		t.Fatal("carrier replacement did not preserve leg0 and remove old leg1")
	}

	newA1, newB1 := net.Pipe()
	if err := leftCore.addLeg(1, newA1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, newB1, nil); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("same-session-payload-"), 4000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("payload did not survive carrier switch")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload changed across carrier replacement")
	}
}

func TestAdaptiveCooldownUsesBackoffAndRecovery(t *testing.T) {
	settings := defaultAdaptiveSettings()
	health := hy2GlobalHealth{}
	start := time.Unix(9000, 0)
	if got := health.noteFallback(start, settings, false); got != 90*time.Second {
		t.Fatalf("first cooldown=%s, want 90s", got)
	}
	if got := health.selectCarrier(start.Add(time.Second)).carrier; got != carrierSnell {
		t.Fatalf("cooldown selected %s, want snell", got)
	}
	if got := health.noteFallback(start.Add(90*time.Second), settings, true); got != 180*time.Second {
		t.Fatalf("second cooldown=%s, want 180s", got)
	}
	health.noteRecovery()
	if got := health.selectCarrier(start.Add(1000 * time.Second)).carrier; got != carrierHy2 {
		t.Fatalf("recovered health selected %s, want hysteria2", got)
	}
}

func TestAdaptiveNoBaselineSevereRXBlackholeFallsBack(t *testing.T) {
	controller, start := adaptiveTestController()
	controller.observe(start, adaptiveTestStats(start, 0, 0, 0, 0))

	first := adaptiveTestStats(start.Add(time.Second), 0, 0, 0, 0)
	first.RxPendingFrames = 2 * controller.settings.MinRxReorder
	first.RxGapAge = 6 * time.Second
	decision := controller.observe(start.Add(time.Second), first)
	if decision.State != adaptiveSuspect || decision.Fallback {
		t.Fatalf("no-baseline RX blackhole did not enter SUSPECT conservatively: %+v", decision)
	}

	later := first
	later.RxGapAge = 15 * time.Second
	decision = controller.observe(start.Add(10*time.Second), later)
	if !decision.Fallback || decision.Reason != "rx_reorder" {
		t.Fatalf("persistent no-baseline RX blackhole did not fallback: %+v", decision)
	}
}

func TestAdaptiveNoBaselineSevereTXBlackholeFallsBack(t *testing.T) {
	controller, start := adaptiveTestController()
	controller.observe(start, adaptiveTestStats(start, 0, 0, 0, 0))

	first := adaptiveTestStats(start.Add(6*time.Second), 0, 0, 0, 0)
	first.OutstandingFrames = 2 * controller.settings.MinTxOutstanding
	first.OutstandingFramesByLeg[1] = controller.settings.MinTxOutstanding
	first.OldestOutstandingAge = 6 * time.Second
	first.AckFrontierValid = true
	first.AckFrontierLeg = 1
	first.AckFrontierAge = 6 * time.Second
	first.LastAckProgress = start
	decision := controller.observe(start.Add(6*time.Second), first)
	if decision.State != adaptiveHealthy || decision.Fallback {
		t.Fatalf("first no-baseline TX sample should only arm stable attribution: %+v", decision)
	}

	second := first
	second.OldestOutstandingAge = 11 * time.Second
	second.AckFrontierAge = 11 * time.Second
	decision = controller.observe(start.Add(11*time.Second), second)
	if decision.State != adaptiveSuspect || decision.Fallback {
		t.Fatalf("stable no-baseline TX blackhole did not enter SUSPECT conservatively: %+v", decision)
	}

	later := second
	later.OldestOutstandingAge = 19 * time.Second
	later.AckFrontierAge = 19 * time.Second
	decision = controller.observe(start.Add(19*time.Second), later)
	if !decision.Fallback || decision.Reason != "tx_ack_stall" {
		t.Fatalf("persistent no-baseline TX blackhole did not fallback: %+v", decision)
	}
}

func TestProbationIdleTimeDoesNotCountTowardStableWindow(t *testing.T) {
	start := time.Unix(10000, 0)
	conn, _, settings := probationConnection(t, start)
	if conn.observeCanary(start, adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("canary recovered on first useful window")
	}
	if conn.observeCanary(start.Add(time.Second), adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("canary recovered before stable window")
	}
	for second := 2; second <= 20; second++ {
		if conn.observeCanary(start.Add(time.Duration(second)*time.Second), adaptiveDecision{State: adaptiveHealthy, Demand: true}) {
			t.Fatalf("idle time counted toward stable canary window at second %d", second)
		}
	}
	if conn.observeCanary(start.Add(21*time.Second), adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 1}) {
		t.Fatal("long idle gap plus one byte incorrectly completed probation")
	}
	if conn.healthyActiveFrom.Before(start.Add(20 * time.Second)) {
		t.Fatalf("stable window was not restarted after idle gap: %s", conn.healthyActiveFrom)
	}
	if conn.canaryUsefulBytes >= settings.MinCanaryUsefulBytes {
		t.Fatalf("old canary bytes survived continuity break: %d", conn.canaryUsefulBytes)
	}
}

func TestInitialFailureBurstOnlyAdvancesCooldownOnce(t *testing.T) {
	settings := defaultAdaptiveSettings()
	settings.InitialFailureThreshold = 3
	settings.InitialFailureWindow = 30 * time.Second
	start := time.Unix(11000, 0)
	health := hy2GlobalHealth{}
	for i := 0; i < 5; i++ {
		health.noteInitialFailure(start.Add(time.Duration(i)*time.Millisecond), settings)
	}
	if health.state != hy2Cooldown {
		t.Fatalf("failure burst did not enter cooldown: state=%v", health.state)
	}
	if health.cooldownPenalty != settings.Cooldown {
		t.Fatalf("single concurrent failure burst multiplied cooldown: got=%s want=%s", health.cooldownPenalty, settings.Cooldown)
	}
	if len(health.initialFailures) != 0 {
		t.Fatalf("consumed failure burst was retained: %d", len(health.initialFailures))
	}
}

func TestActiveSuccessRequiresContinuousRealHy2Contribution(t *testing.T) {
	settings := defaultAdaptiveSettings()
	settings.Warmup = 0
	settings.RecoveryStableWindow = 3 * time.Second
	settings.MinCanaryUsefulBytes = 1 << 20
	settings.MinCanaryActiveWindows = 3
	start := time.Unix(12000, 0)
	conn := newAdaptiveConn(true, settings, &hy2GlobalHealth{}, nil)
	conn.selected = true
	conn.carrier = carrierHy2

	for second := 0; second <= 5; second++ {
		if conn.observeActiveSuccess(start.Add(time.Duration(second)*time.Second), start, adaptiveDecision{State: adaptiveHealthy, Demand: true}) {
			t.Fatalf("leg0-only logical demand cleared Hy2 failure history at second %d", second)
		}
	}
	if conn.activeSuccessSeen {
		t.Fatal("active success was recorded without Hy2 useful contribution")
	}

	if conn.observeActiveSuccess(start.Add(10*time.Second), start, adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("active success recorded too early")
	}
	if conn.observeActiveSuccess(start.Add(11*time.Second), start, adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("active success recorded before stable duration")
	}
	if conn.observeActiveSuccess(start.Add(12*time.Second), start, adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("active success recorded before stable duration")
	}
	if !conn.observeActiveSuccess(start.Add(13*time.Second), start, adaptiveDecision{State: adaptiveHealthy, Leg1RxUsefulBytes: 512 * 1024}) {
		t.Fatal("continuous real Hy2 contribution did not record active success")
	}
}

func TestShortSessionEOFWithoutHy2ContributionIsNotCarrierFailure(t *testing.T) {
	settings := defaultAdaptiveSettings()
	conn := newAdaptiveConn(true, settings, &hy2GlobalHealth{}, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	conn.legReadyRxUseful = 0
	conn.legReadyTxUseful = 0
	stats := coreStats{
		LegUp:             [2]bool{true, false},
		RxDeliveredBytes:  settings.MinCanaryUsefulBytes / 4,
		LastAckProgress:   time.Now(),
		RxPendingFrames:   0,
		OutstandingFrames: 0,
	}
	if conn.shouldRecordCarrierFailure(errors.New("snell: server error 101: Remote EOF"), stats, true) {
		t.Fatal("small healthy logical session EOF was misclassified as Hy2 carrier failure")
	}
}

func TestLargeUnpressuredZeroUsefulEOFFirstOccurrenceIsAmbiguous(t *testing.T) {
	settings := defaultAdaptiveSettings()
	conn := newAdaptiveConn(true, settings, &hy2GlobalHealth{}, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	stats := coreStats{
		LegUp:            [2]bool{true, false},
		RxDeliveredBytes: 64 * settings.MinCanaryUsefulBytes,
		LastAckProgress:  time.Now(),
	}
	if conn.shouldRecordCarrierFailure(io.EOF, stats, true) {
		t.Fatal("first zero-useful EOF on healthy unpressured leg0 should remain ambiguous even on a large logical session")
	}
}

func TestRepeatedAmbiguousEOFOnSameLiveSessionFallsBack(t *testing.T) {
	settings := defaultAdaptiveSettings()
	health := &hy2GlobalHealth{}
	conn := newAdaptiveConn(true, settings, health, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	var fallbackReason string
	conn.onFallback = func(reason string, cooldown bool, probation bool) {
		fallbackReason = reason
		if cooldown {
			health.noteFallback(time.Unix(12500, 0), settings, probation)
		}
	}
	if conn.noteAmbiguousCarrierEOF() {
		t.Fatal("first ambiguous EOF should not fallback")
	}
	if !conn.noteAmbiguousCarrierEOF() {
		t.Fatal("repeated ambiguous EOF on same live session did not fallback")
	}
	if fallbackReason != "repeated_zero_useful_eof" {
		t.Fatalf("unexpected fallback reason: %q", fallbackReason)
	}
	if health.cooldownPenalty != settings.Cooldown {
		t.Fatalf("repeated ambiguous EOF did not open base cooldown: %s", health.cooldownPenalty)
	}
}

func TestEOFWithUsefulHy2ContributionCountsAsCarrierFailure(t *testing.T) {
	settings := defaultAdaptiveSettings()
	conn := newAdaptiveConn(true, settings, &hy2GlobalHealth{}, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	conn.legReadyRxUseful = 100
	conn.legReadyTxUseful = 200
	stats := coreStats{
		LegUp:              [2]bool{true, false},
		RxUniqueBytesByLeg: [2]uint64{0, 101},
		TxAckedUsefulByLeg: [2]uint64{0, 200},
		LastAckProgress:    time.Now(),
		RxDeliveredBytes:   101,
		OutstandingFrames:  0,
		RxPendingFrames:    0,
	}
	if !conn.shouldRecordCarrierFailure(io.EOF, stats, true) {
		t.Fatal("EOF after real Hy2 useful contribution was ignored")
	}
}

func TestEOFUnderLogicalPressureCountsAsCarrierFailure(t *testing.T) {
	settings := defaultAdaptiveSettings()
	conn := newAdaptiveConn(true, settings, &hy2GlobalHealth{}, nil)
	conn.selected = true
	conn.carrier = carrierHy2
	now := time.Now()
	stats := coreStats{
		LegUp:             [2]bool{true, false},
		OutstandingFrames: settings.MinTxOutstanding,
		LastAckProgress:   now.Add(-settings.TxAckStall - time.Second),
	}
	if !conn.shouldRecordCarrierFailure(io.EOF, stats, true) {
		t.Fatal("EOF during logical TX pressure was ignored")
	}
}

func TestConcurrentEstablishedFallbackBurstKeepsSingleCooldownPenalty(t *testing.T) {
	settings := defaultAdaptiveSettings()
	start := time.Unix(13000, 0)
	health := hy2GlobalHealth{}
	if got := health.noteFallback(start, settings, false); got != settings.Cooldown {
		t.Fatalf("first established failure cooldown=%s want=%s", got, settings.Cooldown)
	}
	firstUntil := health.cooldownUntil
	for i := 1; i <= 20; i++ {
		if got := health.noteFallback(start.Add(time.Duration(i)*time.Millisecond), settings, false); got != settings.Cooldown {
			t.Fatalf("concurrent failure %d multiplied cooldown: %s", i, got)
		}
	}
	if health.cooldownPenalty != settings.Cooldown {
		t.Fatalf("concurrent established burst penalty=%s want=%s", health.cooldownPenalty, settings.Cooldown)
	}
	if !health.cooldownUntil.Equal(firstUntil) {
		t.Fatalf("concurrent established burst extended cooldown: first=%s final=%s", firstUntil, health.cooldownUntil)
	}

	selection := health.selectCarrier(firstUntil)
	if selection.carrier != carrierHy2 || !selection.probation {
		t.Fatalf("expected single probation canary after cooldown, got %+v", selection)
	}
	if got := health.noteFallback(firstUntil.Add(time.Second), settings, true); got != 2*settings.Cooldown {
		t.Fatalf("probation failure did not advance backoff: got=%s", got)
	}
}

func TestHundredShortSessionEOFsDoNotPoisonGlobalHy2Health(t *testing.T) {
	settings := defaultAdaptiveSettings()
	health := &hy2GlobalHealth{}
	for i := 0; i < 100; i++ {
		conn := newAdaptiveConn(true, settings, health, nil)
		conn.selected = true
		conn.carrier = carrierHy2
		stats := coreStats{
			LegUp:            [2]bool{true, false},
			RxDeliveredBytes: uint64(i+1) * 1024,
			LastAckProgress:  time.Now(),
		}
		if conn.shouldRecordCarrierFailure(io.EOF, stats, true) {
			t.Fatalf("short session %d was misclassified as carrier failure", i)
		}
	}
	if health.state != hy2Available || health.cooldownPenalty != 0 || len(health.initialFailures) != 0 {
		t.Fatalf("short-session EOF burst poisoned global Hy2 health: state=%v penalty=%s failures=%d", health.state, health.cooldownPenalty, len(health.initialFailures))
	}
}

func TestAdaptiveCarrierReplacementErrorPreservesCarrier(t *testing.T) {
	err := &adaptiveCarrierReplacementError{from: carrierHy2, reason: "tx_ack_stall"}
	if err.from != carrierHy2 {
		t.Fatalf("replacement error lost original carrier: %v", err.from)
	}
	if got := err.Error(); got != "multipath adaptive carrier replacement: tx_ack_stall" {
		t.Fatalf("unexpected replacement error text: %q", got)
	}
}

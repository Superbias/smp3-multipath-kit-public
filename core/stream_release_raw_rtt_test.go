package smp3core

import (
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestReleaseRawRTTCalibration isolates the timer/rate/delay carrier model
// from Core admission. It must pass the configured raw floor before its
// throughput is used to diagnose a Core performance result.
func TestReleaseRawRTTCalibration(t *testing.T) {
	if os.Getenv("SMP3_RELEASE_RAW_CALIBRATION") != "1" {
		t.Skip("explicit raw carrier calibration")
	}
	parseInt := func(name string, fallback int) int {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid %s=%q", name, raw)
		}
		return value
	}
	rttMS := parseInt("SMP3_RELEASE_RTT_MS", 400)
	rateMbps := parseInt("SMP3_RELEASE_RAW_RATE_MBPS", 100)
	duration := time.Duration(parseInt("SMP3_RELEASE_RAW_SECONDS", 20)) * time.Second
	minimum := 0.0
	if raw := os.Getenv("SMP3_RELEASE_RAW_MIN_MBPS"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || value <= 0 {
			t.Fatalf("invalid SMP3_RELEASE_RAW_MIN_MBPS=%q", raw)
		}
		minimum = value
	}
	clock := &releaseClock{}
	clock.rate.Store(int64(rateMbps) * 1_000_000)
	a, b := releasePair(clock, time.Duration(rttMS)*time.Millisecond)
	sink := &slowTestSink{}
	sendDone, recvDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := io.Copy(a, &slowTestSource{}); sendDone <- err }()
	go func() { _, err := io.Copy(sink, b); recvDone <- err }()
	defer func() { a.Close(); b.Close() }()
	releaseLog(t, map[string]any{
		"raw_calibration_start": true, "rtt_ms": rttMS, "rate_mbps": rateMbps,
		"duration_seconds": duration.Seconds(), "carrier_model": "releaseCarrier without StreamEngine/Core",
	})
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last, windowBase uint64
	var windows []float64
	for now := range tick.C {
		sec := int(now.Sub(start).Seconds())
		n := sink.bytes.Load()
		goodput := float64(n-last) * 8 / 1e6
		releaseLog(t, map[string]any{"second": sec, "goodput_mbps": goodput, "delivered": n})
		last = n
		if sec%10 == 0 {
			window := float64(n-windowBase) * 8 / 10 / 1e6
			windows = append(windows, window)
			releaseLog(t, map[string]any{"fixed_window_end_second": sec, "goodput_mbps": window})
			windowBase = n
			if minimum > 0 && window < minimum {
				t.Errorf("raw fixed window %.3f Mbps below configured floor %.3f Mbps", window, minimum)
			}
		}
		if now.Sub(start) >= duration {
			break
		}
	}
	releaseLog(t, map[string]any{"raw_calibration_result": true, "windows_mbps": windows})
	a.Close()
	b.Close()
	for name, result := range map[string]<-chan error{"sender": sendDone, "receiver": recvDone} {
		select {
		case err := <-result:
			if err != nil && err != net.ErrClosed && err != io.ErrClosedPipe {
				t.Errorf("%s exit: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s did not exit after carrier close", name)
		}
	}
}

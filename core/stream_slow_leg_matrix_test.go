package smp3core

import (
	"io"
	"net"
	"os"
	"testing"
	"time"
)

type slowTestDelayedACK struct {
	net.Conn
	delay time.Duration
}

func (c slowTestDelayedACK) Write(p []byte) (int, error) { time.Sleep(c.delay); return c.Conn.Write(p) }

type slowTestLimitedSink struct {
	sink  *slowTestSink
	pause time.Duration
}

func (s slowTestLimitedSink) Write(p []byte) (int, error) {
	time.Sleep(s.pause)
	return s.sink.Write(p)
}

// Supplemental transport matrix. Explicit opt-in because the quantitative
// experiment is intentionally separate from race and normal fast unit tests.
func TestSlowLegSupplementalMatrix(t *testing.T) {
	if os.Getenv("SMP3_SLOW_LEG_MATRIX") != "1" {
		t.Skip("set SMP3_SLOW_LEG_MATRIX=1 for transport fault matrix")
	}
	for _, tc := range []struct {
		name                       string
		rates                      [2]int64
		prior                      []uint32
		hot, recover, jitter, wave bool
		ack, read                  time.Duration
	}{
		{name: "80_1_equal_prior", rates: [2]int64{80e6, 1e6}, prior: []uint32{100, 100}},
		{name: "100_1_high_slow_prior", rates: [2]int64{100e6, 1e6}, prior: []uint32{100, 500}},
		{name: "100_1_true_prior", rates: [2]int64{100e6, 1e6}, prior: []uint32{100, 1}},
		{name: "100_01", rates: [2]int64{100e6, 100000}, prior: []uint32{100, 100}},
		{name: "100_05", rates: [2]int64{100e6, 500000}, prior: []uint32{100, 100}},
		{name: "100_5", rates: [2]int64{100e6, 5e6}, prior: []uint32{100, 100}},
		{name: "hot_drop_recovery", rates: [2]int64{100e6, 100e6}, prior: []uint32{100, 100}, hot: true, recover: true},
		{name: "wave_01_05_1_5", rates: [2]int64{100e6, 1e6}, prior: []uint32{100, 100}, wave: true},
		{name: "short_500ms", rates: [2]int64{100e6, 100e6}, prior: []uint32{100, 100}, jitter: true},
		{name: "both_slow", rates: [2]int64{500000, 1e6}, prior: []uint32{100, 100}},
		{name: "healthy_100_100", rates: [2]int64{100e6, 100e6}, prior: []uint32{100, 100}},
		{name: "healthy_50_200", rates: [2]int64{50e6, 200e6}, prior: []uint32{50, 200}},
		{name: "ACK_delay_300ms", rates: [2]int64{100e6, 100e6}, prior: []uint32{100, 100}, ack: 300 * time.Millisecond},
		{name: "application_slow_read", rates: [2]int64{100e6, 100e6}, prior: []uint32{100, 100}, read: 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: tc.prior, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128, MaxReorderFrames: 256, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second}
			sender, app := NewStreamEngine(cfg)
			receiver, out := NewStreamEngine(cfg)
			defer sender.Close()
			defer receiver.Close()
			var links [2]*slowTestCarrier
			for id := 0; id < 2; id++ {
				a, b := newSlowTestCarrier(tc.rates[id])
				links[id] = a
				var reverse StreamLeg = b
				if tc.ack > 0 {
					reverse = slowTestDelayedACK{Conn: b, delay: tc.ack}
				}
				if err := sender.AttachLeg(LegID(id), a, nil); err != nil {
					t.Fatal(err)
				}
				if err := receiver.AttachLeg(LegID(id), reverse, nil); err != nil {
					t.Fatal(err)
				}
			}
			sink := &slowTestSink{}
			readDone, writeDone := make(chan error, 1), make(chan error, 1)
			go func() { _, err := io.Copy(slowTestLimitedSink{sink: sink, pause: tc.read}, out); readDone <- err }()
			go func() { _, err := io.Copy(app, &slowTestSource{}); writeDone <- err }()
			timers := []*time.Timer{}
			if tc.hot {
				timers = append(timers, time.AfterFunc(time.Second, func() { links[1].setRate(1e6) }))
			}
			if tc.recover {
				timers = append(timers, time.AfterFunc(4*time.Second, func() { links[1].setRate(100e6) }))
			}
			if tc.jitter {
				timers = append(timers, time.AfterFunc(time.Second, func() { links[1].setRate(0) }), time.AfterFunc(1500*time.Millisecond, func() { links[1].setRate(100e6) }))
			}
			if tc.wave {
				for i, rate := range []int64{100000, 500000, 1e6, 5e6} {
					r := rate
					timers = append(timers, time.AfterFunc(time.Duration(i+1)*time.Second, func() { links[1].setRate(r) }))
				}
			}
			time.Sleep(8 * time.Second)
			for _, timer := range timers {
				timer.Stop()
			}
			stats := sender.Snapshot()
			t.Logf("bytes=%d goodput=%.3f state=%v weight=%v ordinary=%v rescues=%d pending=%v reorder_peak=%d", sink.bytes.Load(), float64(sink.bytes.Load())/1e6, stats.OrdinaryAdmissionState, stats.CapacityWeight, stats.OrdinaryAssignedBytes, stats.FrontierRescueAttempts, stats.CapacityPending, receiver.Snapshot().RxMaxPendingFrames)
			if sink.bytes.Load() < 512<<10 {
				t.Fatal("transport lost useful progress")
			}
			if tc.ack > 0 && stats.OrdinaryAdmissionState != [2]string{"normal", "normal"} {
				t.Fatal("reverse ACK delay falsely restricted downlink")
			}
			if tc.jitter && stats.OrdinaryAdmissionState != [2]string{"normal", "normal"} {
				t.Fatal("short jitter left long restriction")
			}
			sender.Close()
			receiver.Close()
			select {
			case err := <-readDone:
				if err != nil && err != io.EOF && err != io.ErrClosedPipe {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("reader leaked")
			}
			select {
			case <-writeDone:
			case <-time.After(time.Second):
				t.Fatal("writer leaked")
			}
		})
	}
}

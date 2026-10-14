package smp3core

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A reliable, bounded, unidirectional carrier. No bytes are discarded. Reverse
// ACKs use the underlying pipe without the downlink rate limiter. There are
// two ingress units and eight delivery units, at most ten chunks of buffering.
type slowTestCarrier struct {
	net.Conn
	rate     atomic.Int64
	tx       chan []byte
	delivery chan benchmarkDelivery
	done     chan struct{}
	changed  chan struct{}
	once     sync.Once
	workers  sync.WaitGroup
	shared   *slowTestPhysicalBudget
}

type slowTestPhysicalBudget struct {
	mu   sync.Mutex
	next time.Time
	rate int64
}

func (b *slowTestPhysicalBudget) reserve(bytes int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.next.Before(now) {
		b.next = now
	}
	b.next = b.next.Add(time.Duration(float64(bytes*8) / float64(b.rate) * float64(time.Second)))
	return time.Until(b.next)
}

func newSlowTestCarrier(rate int64, shared ...*slowTestPhysicalBudget) (*slowTestCarrier, net.Conn) {
	a, b := net.Pipe()
	c := &slowTestCarrier{Conn: a, tx: make(chan []byte, 2), delivery: make(chan benchmarkDelivery, 8), done: make(chan struct{}), changed: make(chan struct{}, 1)}
	c.rate.Store(rate)
	if len(shared) > 0 {
		c.shared = shared[0]
	}
	c.workers.Add(2)
	go func() {
		defer c.workers.Done()
		for {
			var p []byte
			select {
			case <-c.done:
				return
			case p = <-c.tx:
			}
			for c.rate.Load() == 0 {
				select {
				case <-c.done:
					return
				case <-c.changed:
				}
			}
			duration := time.Duration(float64(len(p)*8) / float64(c.rate.Load()) * float64(time.Second))
			if c.shared != nil {
				duration = c.shared.reserve(len(p))
			}
			if !c.wait(duration) {
				return
			}
			select {
			case <-c.done:
				return
			case c.delivery <- benchmarkDelivery{data: p, ready: time.Now().Add(20 * time.Millisecond)}:
			}
		}
	}()
	go func() {
		defer c.workers.Done()
		for {
			select {
			case <-c.done:
				return
			case item := <-c.delivery:
				if !c.wait(time.Until(item.ready)) {
					return
				}
				if err := writeAll(c.Conn, item.data); err != nil {
					return
				}
			}
		}
	}()
	return c, b
}
func (c *slowTestCarrier) wait(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-c.done:
		return false
	case <-timer.C:
		return true
	}
}
func (c *slowTestCarrier) Write(p []byte) (int, error) {
	copyBytes := append([]byte(nil), p...)
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case c.tx <- copyBytes:
		return len(p), nil
	}
}
func (c *slowTestCarrier) setRate(rate int64) {
	c.rate.Store(rate)
	select {
	case c.changed <- struct{}{}:
	default:
	}
}
func (c *slowTestCarrier) Close() error {
	c.once.Do(func() { close(c.done); c.Conn.Close(); c.workers.Wait() })
	return nil
}

type slowTestSink struct {
	bytes  atomic.Uint64
	offset uint64
}
type slowTestSource struct{ offset uint64 }

func (s *slowTestSource) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte((s.offset + uint64(i)) % 251)
	}
	s.offset += uint64(len(p))
	return len(p), nil
}

func (s *slowTestSource) WriteTo(w io.Writer) (int64, error) {
	buffer := make([]byte, 64<<10)
	var total int64
	for {
		s.Read(buffer)
		n, err := w.Write(buffer)
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n != len(buffer) {
			return total, io.ErrShortWrite
		}
	}
}
func (s *slowTestSink) Write(p []byte) (int, error) {
	for i, b := range p {
		if b != byte((s.offset+uint64(i))%251) {
			return i, fmt.Errorf("payload mismatch at %d", s.offset+uint64(i))
		}
	}
	s.offset += uint64(len(p))
	s.bytes.Add(uint64(len(p)))
	return len(p), nil
}

// Default runs a short correctness regression. Explicit duration=40 produces
// fixed 0-10/10-20/20-30/30-40s windows and independent three-run evidence.
func TestSlowLegReliableStream(t *testing.T) {
	duration := 8 * time.Second
	if value := os.Getenv("SMP3_SLOW_LEG_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name    string
		slow    int
		rate    int64
		recover bool
		healthy int64
		hot     bool
		wave    bool
	}{
		{"single", -1, 100e6, false, 100e6, false, false},
		{"cold_100_1", 1, 1e6, false, 100e6, false, false},
		{"swapped_1_100", 0, 1e6, false, 100e6, false, false},
		{"blackhole_recovery", 1, 0, true, 100e6, false, false},
	}
	if os.Getenv("SMP3_SLOW_LEG_EXTENDED") == "1" {
		cases = []struct {
			name    string
			slow    int
			rate    int64
			recover bool
			healthy int64
			hot     bool
			wave    bool
		}{
			{"single_80", -1, 80e6, false, 80e6, false, false},
			{"cold_80_1", 1, 1e6, false, 80e6, false, false},
			{"hot_100_1", 1, 1e6, false, 100e6, true, false},
			{"hot_blackhole", 1, 0, false, 100e6, true, false},
			{"wave_01_05_1_5", 1, 100000, false, 100e6, false, true},
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 128, MaxReorderFrames: 256, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second}
			sender, app := NewStreamEngine(cfg)
			receiver, out := NewStreamEngine(cfg)
			defer sender.Close()
			defer receiver.Close()
			var links [2]*slowTestCarrier
			for id := 0; id < 2; id++ {
				if tc.slow < 0 && id == 1 {
					continue
				}
				rate := tc.healthy
				if id == tc.slow && !tc.hot {
					rate = tc.rate
				}
				a, b := newSlowTestCarrier(rate)
				links[id] = a
				if err := sender.AttachLeg(LegID(id), a, nil); err != nil {
					t.Fatal(err)
				}
				if err := receiver.AttachLeg(LegID(id), b, nil); err != nil {
					t.Fatal(err)
				}
			}
			sink := &slowTestSink{}
			readDone := make(chan error, 1)
			writeDone := make(chan error, 1)
			go func() { _, err := io.Copy(sink, out); readDone <- err }()
			go func() { _, err := io.Copy(app, &slowTestSource{}); writeDone <- err }()
			start := time.Now()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			var last uint64
			var windows []float64
			var windowBase uint64
			var maxPending, maxReorder int
			recovered := false
			faulted := false
			for now := range ticker.C {
				elapsed := now.Sub(start)
				if tc.hot && !faulted && elapsed >= 5*time.Second {
					links[tc.slow].setRate(tc.rate)
					faulted = true
				}
				if tc.wave {
					rates := [...]int64{100000, 500000, 1000000, 5000000}
					links[tc.slow].setRate(rates[(int(elapsed.Seconds())/5)%len(rates)])
				}
				stats := sender.Snapshot()
				rx := receiver.Snapshot()
				for _, n := range stats.CapacityPending {
					if int(n) > maxPending {
						maxPending = int(n)
					}
				}
				if rx.RxPendingFrames > maxReorder {
					maxReorder = rx.RxPendingFrames
				}
				current := sink.bytes.Load()
				secondRate := float64(current-last) * 8 / 1e6
				last = current
				t.Logf("second=%d goodput=%.3f states=%v outstanding=%v weight=%v assigned=%v rescue=%d", int(elapsed.Seconds()), secondRate, stats.OrdinaryAdmissionState, stats.OrdinaryOutstandingFrames, stats.CapacityWeight, stats.OrdinaryAssignedBytes, stats.FrontierRescueAttempts)
				if int(elapsed.Seconds())%10 == 0 {
					windows = append(windows, float64(current-windowBase)*8/10/1e6)
					windowBase = current
				}
				if tc.recover && !recovered && elapsed >= duration/2 {
					links[tc.slow].setRate(100e6)
					recovered = true
				}
				if elapsed >= duration {
					break
				}
			}
			stats := sender.Snapshot()
			t.Logf("RESULT seconds=%g delivered=%d windows=%v rescue_bytes=%v pending_peak=%d reorder_peak=%d payload=PASS states=%v", duration.Seconds(), sink.bytes.Load(), windows, stats.TxRetransmitBytesByLeg, maxPending, maxReorder, stats.OrdinaryAdmissionState)
			t.Logf("WRITE_COUNTERS successful=%v retry=%v useful_ack=%d", stats.TxWrittenBytesByLeg, stats.TxRetryWrittenBytesByLeg, stats.TxAckedUsefulTotal)
			if sink.bytes.Load() < 2<<20 {
				t.Errorf("healthy path failed to sustain useful progress: bytes=%d", sink.bytes.Load())
			}
			if stats.LegGeneration[0] > 1 || stats.LegGeneration[1] > 1 {
				t.Error("unexpected reconnect")
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

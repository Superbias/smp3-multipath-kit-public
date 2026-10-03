package smp3core

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func promotionConfig() StreamConfig {
	cfg := StreamConfig{ChunkSize: 32 << 10, QueueFrames: 256, MaxReorderFrames: 4096,
		MaxInflightFrames: 2048, AckInterval: 5 * time.Millisecond, RetransmitTimeout: time.Second,
		RecoveryTimeout: 5 * time.Second, BandwidthMbps: []uint32{50, 200}, SchedulerMode: StreamSchedulerStatic,
		BenchmarkAssignedDecoupled: true, BenchmarkPendingFrames: 32,
		benchmarkEpochRebase: os.Getenv("SMP3_BENCHMARK_EPOCH_REBASE") != "",
		frontierProbe:        &benchmarkFrontierProbe{grace: true}, Telemetry: &StreamTelemetry{}}
	if os.Getenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY") != "" {
		cfg.SchedulerMode = StreamSchedulerAggregation
		cfg.CapacityMode = StreamCapacityDynamic
	}
	return cfg
}

func promotionWait(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func promotionState(label string, c *StreamEngine) {
	b, _ := c.assignedSnapshot()
	bp := c.BackpressureSnapshot()
	fmt.Printf("PROMOTION_STATE %s assigned=%v normalized=[%.2f,%.2f] pending_bytes=[%d,%d] physical=%v ledger=%d active=[%v,%v] frontier=%d\n",
		label, b, float64(b[0])/50, float64(b[1])/200, len(c.pending[0])*c.cfg.ChunkSize, len(c.pending[1])*c.cfg.ChunkSize,
		c.Snapshot().TxSentBytesByLeg, bp.LedgerOutstanding, bp.Legs[0].Present, bp.Legs[1].Present, bp.ACKFrontier)
}

func promotionShutdown(t *testing.T, c *StreamEngine) {
	t.Helper()
	c.Close()
	for _, done := range c.decoupledStats.feedersDone {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("pending feeder failed to stop")
		}
	}
	select {
	case <-c.txStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("TX failed to stop")
	}
}

func TestBenchmarkPromotionReconnect(t *testing.T) {
	if os.Getenv("SMP3_PROMOTION_GATE") == "" {
		t.Skip("set SMP3_PROMOTION_GATE=1")
	}
	for _, failedID := range []uint8{1, 0} {
		t.Run(fmt.Sprintf("failed_leg_%d", failedID), func(t *testing.T) {
			cfg := promotionConfig()
			trace := &benchmarkAssignmentTrace{}
			cfg.assignmentTrace = trace
			left, app := NewStreamEngine(cfg)
			rcfg := cfg
			rcfg.frontierProbe = nil
			rcfg.assignmentTrace = nil
			right, peer := NewStreamEngine(rcfg)
			defer promotionShutdown(t, left)
			defer promotionShutdown(t, right)
			attach := func(id uint8) {
				a, b := benchmarkLinkPair(10*time.Millisecond, [2]int64{50_000_000, 200_000_000}[id], benchmarkFault{})
				if err := left.AttachLeg(LegID(id), a, nil); err != nil {
					t.Fatal(err)
				}
				if err := right.AttachLeg(LegID(id), b, nil); err != nil {
					t.Fatal(err)
				}
			}
			attach(0)
			attach(1)
			left.SetActiveForTest(true)
			const total = 24 << 20
			var received atomic.Int64
			readDone := make(chan error, 1)
			go func() {
				_, err := io.CopyN(benchmarkCountingWriter{Writer: benchmarkPayloadChecker{}, bytes: &received}, peer, total)
				readDone <- err
			}()
			write := func(n int64) {
				if _, err := io.CopyN(app, benchmarkPayloadReader{}, n); err != nil {
					t.Fatal(err)
				}
			}
			drain := func(n int64) {
				promotionWait(t, "payload and ledger drain", func() bool { return received.Load() >= n && !left.hasOutstanding() })
			}
			write(4 << 20)
			drain(4 << 20)
			promotionState("T0", left)
			// Byte-triggered failure while a second healthy segment is in flight.
			write(4 << 20)
			left.ReplaceLeg(LegID(failedID), io.ErrUnexpectedEOF)
			right.ReplaceLeg(LegID(failedID), io.ErrUnexpectedEOF)
			promotionState("T1", left)
			write(4 << 20)
			drain(12 << 20)
			promotionState("T2", left)
			before, _ := left.assignedSnapshot()
			left.weightedMu.Lock()
			trace.armed = true
			left.weightedMu.Unlock()
			attach(failedID)
			promotionState("T3", left)
			write(12 << 20)
			drain(total)
			promotionState("T4", left)
			select {
			case err := <-readDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("reader did not finish")
			}
			left.weightedMu.Lock()
			samples := append([]benchmarkAssignedSample(nil), trace.samples...)
			left.weightedMu.Unlock()
			counts := [2]int{}
			last := -1
			streak := 0
			maxStreak := [2]int{}
			for i, s := range samples {
				counts[s.Leg]++
				if last == int(s.Leg) {
					streak++
				} else {
					last = int(s.Leg)
					streak = 1
				}
				if streak > maxStreak[s.Leg] {
					maxStreak[s.Leg] = streak
				}
				for _, n := range []int{32, 64, 128, 256, 512, 1024} {
					if i+1 == n {
						fmt.Printf("RECONNECT_PREFIX failed=%d n=%d selections=%v share=%.2f/%.2f lifetime=%v debt=%.2f\n", failedID, n, counts, 100*float64(counts[0])/float64(n), 100*float64(counts[1])/float64(n), s.Bytes, s.Debt)
					}
				}
			}
			after, _ := left.assignedSnapshot()
			ss := left.Snapshot()
			fmt.Printf("RECONNECT_RESULT failed=%d before=%v after=%v max_streak=%v rescue=%d retransmit=%v ledger=%d pending=[%d,%d] payload=PASS\n", failedID, before, after, maxStreak, ss.FrontierRescueAttempts, ss.TxRetransmitBytesByLeg, ss.OutstandingFrames, len(left.pending[0]), len(left.pending[1]))
			if ss.OutstandingFrames != 0 || len(left.pending[0])+len(left.pending[1]) != 0 {
				t.Fatal("undrained state")
			}
		})
	}
}

// Reuses the existing streaming sample and virtual serializer, without holding
// the payload in RAM or repeating single-leg controls for a one-GiB gate.
func TestBenchmarkPromotionLongFlow(t *testing.T) {
	if os.Getenv("SMP3_PROMOTION_GATE") == "" {
		t.Skip("set SMP3_PROMOTION_GATE=1")
	}
	size := benchmarkIntEnv("SMP3_BENCHMARK_BYTES", 1<<30)
	mode := StreamSchedulerStatic
	if os.Getenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY") != "" {
		mode = StreamSchedulerAggregation
	}
	elapsed, s, err := runStreamAggregationSampleWithMode(size, 2, 0, [2]int64{50_000_000, 200_000_000}, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}, [2]benchmarkFault{}, mode)
	if err != nil {
		t.Fatal(err)
	}
	physical := s.TxSentBytesByLeg[0] + s.TxSentBytesByLeg[1]
	mbps := float64(size) * 8 / elapsed.Seconds() / 1e6
	rescueBytes := s.Telemetry.RescueAttemptBytes
	fmt.Printf("PROMOTION_LONG bytes=%d elapsed=%s useful_mbps=%.2f physical_raw=%.4f amp=%.6f physical=%v retry=%v rescue=%d rescue_bytes=%v ledger=%d payload=PASS\n", size, elapsed, mbps, mbps*float64(physical)/float64(size)/250, float64(physical)/float64(size), s.TxSentBytesByLeg, s.TxRetransmitBytesByLeg, s.FrontierRescueAttempts, rescueBytes, s.OutstandingFrames)
	if s.OutstandingFrames != 0 {
		t.Fatal("ledger did not retire")
	}
	if s.FrontierRescueAttempts > 8 || float64(physical)/float64(size) > 1.01 {
		t.Fatalf("healthy long stream entered runaway recovery: rescue=%d retransmit=%v amplification=%.6f", s.FrontierRescueAttempts, s.TxRetransmitBytesByLeg, float64(physical)/float64(size))
	}
}

func promotionMonitor(left, right *StreamEngine, size int) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		var pendingHist [2][33]int64
		var ledgerHist [2049]int64
		maxLedger := 0
		maxPending := [2]int{}
		quarter := 1
		for {
			select {
			case <-stop:
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				b, _ := left.assignedSnapshot()
				fmt.Printf("PROMOTION_MEMORY pct=100 heap_alloc=%d heap_inuse=%d heap_sys=%d goroutines=%d ledger=%d pending=[%d,%d] assigned=%v\n", m.HeapAlloc, m.HeapInuse, m.HeapSys, runtime.NumGoroutine(), left.Snapshot().OutstandingFrames, len(left.pending[0]), len(left.pending[1]), b)
				p95 := func(h []int64) int {
					var n, sum int64
					for _, v := range h {
						n += v
					}
					for i, v := range h {
						sum += v
						if sum*100 >= n*95 {
							return i
						}
					}
					return 0
				}
				fmt.Printf("PROMOTION_BOUNDS pending_p95=[%d,%d] pending_max=%v ledger_p95=%d ledger_max=%d pending_final=[%d,%d] ledger_final=%d\n", p95(pendingHist[0][:]), p95(pendingHist[1][:]), maxPending, p95(ledgerHist[:]), maxLedger, len(left.pending[0]), len(left.pending[1]), left.Snapshot().OutstandingFrames)
				return
			case <-tick.C:
				s := left.Snapshot()
				l := s.OutstandingFrames
				if l > maxLedger {
					maxLedger = l
				}
				if l < len(ledgerHist) {
					ledgerHist[l]++
				}
				for id := 0; id < 2; id++ {
					p := len(left.pending[id])
					if p > maxPending[id] {
						maxPending[id] = p
					}
					if p < len(pendingHist[id]) {
						pendingHist[id][p]++
					}
				}
				if size > 0 && right.rxDeliveredBytes.Load() >= uint64(size)*uint64(quarter)/4 && quarter <= 4 {
					runtime.GC()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					b, _ := left.assignedSnapshot()
					fmt.Printf("PROMOTION_MEMORY pct=%d heap_alloc=%d heap_sys=%d goroutines=%d ledger=%d pending=[%d,%d] assigned=%v\n", quarter*25, m.HeapAlloc, m.HeapSys, runtime.NumGoroutine(), l, len(left.pending[0]), len(left.pending[1]), b)
					quarter++
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}

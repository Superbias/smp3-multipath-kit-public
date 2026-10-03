package smp3core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchmarkLink serializes bytes at the configured rate, then delivers them
// after a one-way delay. Serialization and propagation run in separate bounded
// stages, so RTT does not become a per-frame throughput cap.
type benchmarkLink struct {
	net.Conn
	tx                  chan []byte
	done                chan struct{}
	once                sync.Once
	latency             time.Duration
	bps                 int64
	fault               benchmarkFault
	started             time.Time
	bytes               atomic.Int64
	servicedBytes       atomic.Int64
	writes              atomic.Int64
	dataWrites          atomic.Int64
	droppedUnits        atomic.Int64
	serializerIdleNs    atomic.Int64
	serializerBusyNs    atomic.Int64
	waitCount           atomic.Int64
	busyYields          atomic.Int64
	immediateSend       atomic.Int64
	sleepRequestedNs    atomic.Int64
	sleepActualNs       atomic.Int64
	busyYieldCount      atomic.Int64
	deadlineLateNs      atomic.Int64
	serviceIterations   atomic.Int64
	serviceTimerFires   atomic.Int64
	serviceNoCredit     atomic.Int64
	serviceMu           sync.Mutex
	serviceEvents       []benchmarkServiceEvent
	deliveryLatenessNs  []int64
	actualDeliveryTimes []time.Time
	timerFireTimes      []time.Time
	phaseOffset         time.Duration
	stalled             atomic.Bool
	pendingHeader       []byte
}

type benchmarkServiceEvent struct {
	creditBefore, creditAfter, bytes, gapNs int64
	afterTimer                              bool
	at                                      time.Time
}

var benchmarkCreditPhaseSeq atomic.Int64
var benchmarkHandoffTrace atomic.Pointer[StreamHandoffTelemetry]

type benchmarkFault struct {
	degradeAfter       time.Duration
	degradeEpoch       *time.Time
	degradeAfterBytes  int64
	logicalBytes       *atomic.Int64
	degradedBPS        int64
	recoverAfter       time.Duration
	recoveredBPS       int64
	stallAfterBytes    int64
	stallDuration      time.Duration
	extraEvery         int64
	extraDelay         time.Duration
	dropEvery          int64
	hardFailAfterBytes int64
	hardFailBeforeData bool
}

type benchmarkDelivery struct {
	data      []byte
	ready     time.Time
	scheduled time.Time
}

type benchmarkCountingWriter struct {
	io.Writer
	bytes *atomic.Int64
}

func (w benchmarkCountingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

func newBenchmarkLink(conn net.Conn, latency time.Duration, bps int64, fault benchmarkFault) *benchmarkLink {
	l := &benchmarkLink{Conn: conn, tx: make(chan []byte, 256), done: make(chan struct{}), latency: latency, bps: bps, fault: fault, started: time.Now()}
	if fault.hardFailBeforeData {
		go l.Close()
	}
	if os.Getenv("SMP3_BENCHMARK_PACER") == "serializer" {
		startSerializerBenchmarkLink(l)
		return l
	}
	if os.Getenv("SMP3_BENCHMARK_PACER") == "credit" {
		if raw := os.Getenv("SMP3_CREDIT_PHASE_FRACTION"); raw != "" && benchmarkCreditPhaseSeq.Add(1)%2 == 0 {
			if fraction, err := strconv.ParseFloat(raw, 64); err == nil && fraction >= 0 && fraction < 1 {
				quantum := benchmarkIntEnv("SMP3_CREDIT_QUANTUM", 64*1024)
				period := time.Duration(float64(quantum*8) / float64(bps) * float64(time.Second))
				l.phaseOffset = time.Duration(float64(period) * fraction)
			}
		}
		startCreditBenchmarkLink(l)
		return l
	}
	serialized := make(chan benchmarkDelivery, 256)
	go func() {
		defer close(serialized)
		var nextSend time.Time
		for {
			select {
			case <-l.done:
				return
			case p := <-l.tx:
				if len(p) == 0 {
					continue
				}
				written := l.bytes.Add(int64(len(p)))
				if l.fault.hardFailAfterBytes > 0 && written >= l.fault.hardFailAfterBytes {
					l.Close()
					return
				}
				l.writes.Add(1)
				dataCount := int64(0)
				if len(p) > 1024 {
					dataCount = l.dataWrites.Add(1)
				}
				if fault.dropEvery > 0 && dataCount > 0 && dataCount%fault.dropEvery == 0 {
					continue
				}
				if fault.stallAfterBytes > 0 && written >= fault.stallAfterBytes && l.stalled.CompareAndSwap(false, true) {
					time.Sleep(fault.stallDuration)
				}
				if fault.extraEvery > 0 && dataCount > 0 && dataCount%fault.extraEvery == 0 {
					time.Sleep(fault.extraDelay)
				}
				rate := bps
				degradeSince := time.Since(l.started)
				if fault.degradeEpoch != nil {
					degradeSince = time.Since(*fault.degradeEpoch)
				}
				degraded := fault.degradeAfter > 0 && degradeSince >= fault.degradeAfter
				progressBytes := written
				if fault.logicalBytes != nil {
					progressBytes = fault.logicalBytes.Load()
				}
				if fault.degradeAfterBytes > 0 && progressBytes >= fault.degradeAfterBytes {
					degraded = true
				}
				if degraded && fault.degradedBPS > 0 {
					rate = fault.degradedBPS
				}
				if fault.recoverAfter > 0 && degradeSince >= fault.recoverAfter && fault.recoveredBPS > 0 {
					rate = fault.recoveredBPS
				}
				if rate > 0 && len(p) > 1024 {
					duration := time.Duration(float64(len(p)*8) / float64(rate) * float64(time.Second))
					if nextSend.IsZero() {
						nextSend = time.Now()
					}
					if nextSend.After(time.Now()) {
						l.waitCount.Add(1)
					} else {
						l.immediateSend.Add(1)
					}
					waitBenchmarkUntilMeasured(l, nextSend)
					nextSend = nextSend.Add(duration)
					if nextSend.Before(time.Now()) {
						if os.Getenv("SMP3_BENCHMARK_PACER") == "credit" {
							// Benchmark-only bounded service credit.
							window := benchmarkDurationEnv("SMP3_CREDIT_WINDOW", 16*time.Millisecond)
							floor := time.Now().Add(-window)
							if nextSend.Before(floor) {
								nextSend = floor
							}
						} else {
							nextSend = time.Now()
						}
					}
				}
				select {
				case <-l.done:
					return
				case serialized <- benchmarkDelivery{data: p, ready: time.Now().Add(latency)}:
				}
			}
		}
	}()
	go func() {
		for packet := range serialized {
			if wait := time.Until(packet.ready); wait > 0 {
				time.Sleep(wait)
			}
			if err := writeAll(conn, packet.data); err != nil {
				l.Close()
				return
			}
			l.servicedBytes.Add(int64(len(packet.data)))
		}
	}()
	return l
}

// startSerializerBenchmarkLink is a benchmark-only virtual serializer. It
// schedules fixed physical quanta from producer arrival timestamps; no credit
// refill, periodic timer, or per-quantum goroutine participates in pacing.
func startSerializerBenchmarkLink(l *benchmarkLink) {
	serialized := make(chan benchmarkDelivery, 256)
	quantum := benchmarkIntEnv("SMP3_SERIALIZER_QUANTUM", 16*1024)
	go func() {
		defer close(serialized)
		var nextAvailable time.Time
		for {
			select {
			case <-l.done:
				return
			case p := <-l.tx:
				if len(p) == 0 {
					continue
				}
				if len(p) > 0 && p[0] == byte(StreamFrameData) {
					if len(p) >= StreamFrameHeaderSize {
						if trace := benchmarkHandoffTrace.Load(); trace != nil {
							trace.markSerializerArrival(binary.BigEndian.Uint64(p[1:9]), time.Now().UnixNano())
						}
					}
					dataCount := l.dataWrites.Add(1)
					if l.fault.dropEvery > 0 && dataCount%l.fault.dropEvery == 0 && l.droppedUnits.Add(1) <= int64(benchmarkIntEnv("SMP3_DROP_MAX", 16)) {
						continue
					}
				}
				for len(p) > 0 {
					n := min(len(p), quantum)
					unit := append([]byte(nil), p[:n]...)
					p = p[n:]
					arrival := time.Now()
					start := arrival
					if nextAvailable.After(start) {
						start = nextAvailable
					} else if !nextAvailable.IsZero() {
						l.serializerIdleNs.Add(arrival.Sub(nextAvailable).Nanoseconds())
					}
					rate := l.bps
					elapsed := time.Since(l.started)
					if l.fault.degradeEpoch != nil {
						elapsed = time.Since(*l.fault.degradeEpoch)
					}
					if l.fault.degradeAfter > 0 && elapsed >= l.fault.degradeAfter && l.fault.degradedBPS > 0 {
						rate = l.fault.degradedBPS
					}
					if l.fault.recoverAfter > 0 && elapsed >= l.fault.recoverAfter && l.fault.recoveredBPS > 0 {
						rate = l.fault.recoveredBPS
					}
					finish := start.Add(time.Duration(float64(n*8) / float64(rate) * float64(time.Second)))
					nextAvailable = finish
					l.serializerBusyNs.Add(finish.Sub(start).Nanoseconds())
					l.serviceMu.Lock()
					gap := int64(0)
					if len(l.serviceEvents) > 0 {
						gap = finish.Sub(l.serviceEvents[len(l.serviceEvents)-1].at).Nanoseconds()
					}
					l.serviceEvents = append(l.serviceEvents, benchmarkServiceEvent{bytes: int64(n), gapNs: gap, at: finish})
					l.serviceMu.Unlock()
					l.bytes.Add(int64(n))
					if l.fault.hardFailAfterBytes > 0 && l.bytes.Load() >= l.fault.hardFailAfterBytes {
						l.Close()
						return
					}
					l.writes.Add(1)
					if l.fault.stallAfterBytes > 0 && l.bytes.Load() >= l.fault.stallAfterBytes && l.stalled.CompareAndSwap(false, true) {
						time.Sleep(l.fault.stallDuration)
					}
					select {
					case <-l.done:
						return
					case serialized <- benchmarkDelivery{data: unit, ready: finish.Add(l.latency), scheduled: finish.Add(l.latency)}:
					}
				}
			}
		}
	}()
	go func() {
		for packet := range serialized {
			if wait := time.Until(packet.ready); wait > 0 {
				time.Sleep(wait)
			}
			actual := time.Now()
			l.serviceMu.Lock()
			l.deliveryLatenessNs = append(l.deliveryLatenessNs, actual.Sub(packet.scheduled).Nanoseconds())
			l.actualDeliveryTimes = append(l.actualDeliveryTimes, actual)
			l.serviceMu.Unlock()
			if err := writeAll(l.Conn, packet.data); err != nil {
				l.Close()
				return
			}
			l.servicedBytes.Add(int64(len(packet.data)))
		}
	}()
}

// startCreditBenchmarkLink is a benchmark-only byte service clock. A bounded
// channel holds caller data; one reusable timer wakes the service loop when
// credit may have accrued. Late wakeups earn only the configured credit window.
func startCreditBenchmarkLink(l *benchmarkLink) {
	serialized := make(chan benchmarkDelivery, 256)
	go func() {
		defer close(serialized)
		window := benchmarkDurationEnv("SMP3_CREDIT_WINDOW", 16*time.Millisecond)
		quantum := benchmarkIntEnv("SMP3_CREDIT_QUANTUM", 64*1024)
		if l.phaseOffset > 0 {
			time.Sleep(l.phaseOffset)
		} else if offset := benchmarkDurationEnv("SMP3_CREDIT_PHASE_OFFSET", 0); offset > 0 {
			time.Sleep(offset)
		}
		maxCredit := float64(l.bps) / 8 * window.Seconds()
		credit := float64(0)
		last := time.Now()
		var current []byte
		var lastService time.Time
		afterTimer := false
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()
		for {
			l.serviceIterations.Add(1)
			now := time.Now()
			credit += now.Sub(last).Seconds() * float64(l.bps) / 8
			if credit > maxCredit {
				credit = maxCredit
			}
			last = now
			if len(current) == 0 {
				select {
				case <-l.done:
					return
				case current = <-l.tx:
					if len(current) == 0 {
						continue
					}
				default:
					select {
					case <-l.done:
						return
					case current = <-l.tx:
						continue
					}
				}
			}
			creditBefore := credit
			target := min(len(current), quantum)
			serviceable := min(target, int(credit))
			if os.Getenv("SMP3_CREDIT_WHOLE_QUANTUM") == "1" && len(current) >= quantum && credit < float64(target) {
				serviceable = 0
			}
			if serviceable == 0 {
				l.serviceNoCredit.Add(1)
				deficit := 1.0 - credit
				if os.Getenv("SMP3_CREDIT_WHOLE_QUANTUM") == "1" {
					deficit = float64(target) - credit
				}
				wait := time.Duration(deficit * 8 / float64(l.bps) * float64(time.Second))
				if wait < time.Millisecond {
					wait = time.Millisecond
				}
				timer.Reset(wait)
				select {
				case <-l.done:
					return
				case <-timer.C:
					l.serviceTimerFires.Add(1)
					l.serviceMu.Lock()
					l.timerFireTimes = append(l.timerFireTimes, time.Now())
					l.serviceMu.Unlock()
					afterTimer = true
				}
				continue
			}
			piece := append([]byte(nil), current[:serviceable]...)
			current = current[serviceable:]
			credit -= float64(serviceable)
			serviceNow := time.Now()
			gap := int64(0)
			if !lastService.IsZero() {
				gap = serviceNow.Sub(lastService).Nanoseconds()
			}
			l.serviceMu.Lock()
			l.serviceEvents = append(l.serviceEvents, benchmarkServiceEvent{creditBefore: int64(creditBefore), creditAfter: int64(credit), bytes: int64(serviceable), gapNs: gap, afterTimer: afterTimer, at: serviceNow})
			l.serviceMu.Unlock()
			lastService = serviceNow
			afterTimer = false
			l.bytes.Add(int64(serviceable))
			l.writes.Add(1)
			select {
			case <-l.done:
				return
			case serialized <- benchmarkDelivery{data: piece, ready: time.Now().Add(l.latency)}:
			}
		}
	}()
	go func() {
		for packet := range serialized {
			if wait := time.Until(packet.ready); wait > 0 {
				time.Sleep(wait)
			}
			if err := writeAll(l.Conn, packet.data); err != nil {
				l.Close()
				return
			}
			l.servicedBytes.Add(int64(len(packet.data)))
		}
	}()
}

// Windows timer granularity can turn a 1-5 ms Sleep into roughly 15 ms. Keep
// the carrier's pacing deadline accurate for benchmark rates without changing
// production scheduling or adding a dependency on OS timer resolution.
func waitBenchmarkUntil(deadline time.Time) {
	for {
		wait := time.Until(deadline)
		if wait <= 0 {
			return
		}
		if wait > 2*time.Millisecond {
			time.Sleep(wait - time.Millisecond)
			continue
		}
		runtime.Gosched()
	}
}

func waitBenchmarkUntilMeasured(l *benchmarkLink, deadline time.Time) {
	for {
		wait := time.Until(deadline)
		if wait <= 0 {
			if late := -wait; late > 0 {
				l.deadlineLateNs.Add(int64(late))
			}
			return
		}
		if os.Getenv("SMP3_BENCHMARK_BUSY_ALL") != "" {
			l.busyYieldCount.Add(1)
			runtime.Gosched()
			continue
		}
		if os.Getenv("SMP3_BENCHMARK_PACER") == "hybrid" {
			// 8 ms is derived from the measured multi-millisecond Sleep
			// overshoot (roughly 7.5 ms at the 100 Mbps/64 KiB point). The
			// guard is benchmark-only and bounded; it is not production policy.
			const guard = 8 * time.Millisecond
			if wait > guard {
				started := time.Now()
				l.sleepRequestedNs.Add(int64(wait - guard))
				time.Sleep(wait - guard)
				l.sleepActualNs.Add(int64(time.Since(started)))
				continue
			}
			l.busyYieldCount.Add(1)
			runtime.Gosched()
			continue
		}
		if wait > 2*time.Millisecond {
			started := time.Now()
			l.sleepRequestedNs.Add(int64(wait - time.Millisecond))
			time.Sleep(wait - time.Millisecond)
			l.sleepActualNs.Add(int64(time.Since(started)))
			continue
		}
		l.busyYieldCount.Add(1)
		runtime.Gosched()
	}
}

func (l *benchmarkLink) Write(p []byte) (int, error) {
	originalLength := len(p)
	if len(p) == StreamFrameHeaderSize && p[0] == byte(StreamFrameData) {
		l.pendingHeader = append(l.pendingHeader[:0], p...)
		return len(p), nil
	}
	if len(l.pendingHeader) > 0 {
		combined := make([]byte, 0, len(l.pendingHeader)+len(p))
		combined = append(combined, l.pendingHeader...)
		combined = append(combined, p...)
		l.pendingHeader = nil
		p = combined
	}
	if (os.Getenv("SMP3_BENCHMARK_PACER") == "service-clock" || os.Getenv("SMP3_BENCHMARK_PACER") == "credit") && len(p) > 16*1024 {
		// Benchmark-only service quantum. The paced carrier sees fixed-size
		// service units independent of caller Write granularity.
		quantum := 16 * 1024
		if os.Getenv("SMP3_BENCHMARK_PACER") == "credit" {
			quantum = benchmarkIntEnv("SMP3_CREDIT_QUANTUM", 64*1024)
		}
		for offset := 0; offset < len(p); {
			end := offset + quantum
			if end > len(p) {
				end = len(p)
			}
			chunk := append([]byte(nil), p[offset:end]...)
			select {
			case <-l.done:
				return 0, net.ErrClosed
			case l.tx <- chunk:
			}
			offset = end
		}
		return originalLength, nil
	}
	copyOfP := append([]byte(nil), p...)
	select {
	case <-l.done:
		return 0, net.ErrClosed
	case l.tx <- copyOfP:
		return originalLength, nil
	}
}

func (l *benchmarkLink) Close() error {
	l.once.Do(func() { close(l.done); _ = l.Conn.Close() })
	return nil
}

func benchmarkLinkPair(latency time.Duration, bps int64, fault benchmarkFault) (StreamLeg, StreamLeg) {
	if os.Getenv("SMP3_BENCHMARK_NETPIPE") != "" {
		left, right := net.Pipe()
		return newBenchmarkLink(left, latency/2, bps, fault), right
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	left, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		panic(err)
	}
	right := <-accepted
	_ = listener.Close()
	if tcp, ok := left.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if tcp, ok := right.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return newBenchmarkLink(left, latency/2, bps, fault), newBenchmarkLink(right, latency/2, bps, fault)
}

func benchmarkIntEnv(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func benchmarkDurationEnv(name string, fallback time.Duration) time.Duration {
	if value := os.Getenv(name); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

type benchmarkFeedingSamples struct {
	count             int64
	queueDepthSum     [2]int64
	queueEmpty        [2]int64
	inflight          []int64
	maxInflight       int64
	atInflightCeiling int64
}

type benchmarkPayloadReader struct {
	rateBPS int64
}

func (r benchmarkPayloadReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	if r.rateBPS > 0 {
		delay := time.Duration(float64(len(p)*8) / float64(r.rateBPS) * float64(time.Second))
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	return len(p), nil
}

type benchmarkPayloadChecker struct{}

func (benchmarkPayloadChecker) Write(p []byte) (int, error) {
	for _, b := range p {
		if b != 'x' {
			return 0, errors.New("payload mismatch")
		}
	}
	return len(p), nil
}

func runStreamAggregationSample(size int, legCount int, firstLeg int, rates [2]int64, latencies [2]time.Duration, faults [2]benchmarkFault) (time.Duration, StreamStats, error) {
	mode := StreamSchedulerAdaptive
	if strings.EqualFold(os.Getenv("SMP3_BENCHMARK_SCHEDULER"), "static") {
		mode = StreamSchedulerStatic
	}
	return runStreamAggregationSampleWithMode(size, legCount, firstLeg, rates, latencies, faults, mode)
}

func runStreamAggregationSampleWithMode(size int, legCount int, firstLeg int, rates [2]int64, latencies [2]time.Duration, faults [2]benchmarkFault, mode StreamSchedulerMode) (time.Duration, StreamStats, error) {
	if os.Getenv("SMP3_PRODUCTION_AGGREGATION") != "" {
		mode = StreamSchedulerAggregation
	}
	if id := benchmarkIntEnv("SMP3_BENCHMARK_HARD_FAIL_LEG", -1); legCount == 2 && id >= 0 && id < 2 {
		faults[id].hardFailAfterBytes = int64(benchmarkIntEnv("SMP3_BENCHMARK_HARD_FAIL_BYTES", 0))
		faults[id].hardFailBeforeData = os.Getenv("SMP3_BENCHMARK_HARD_FAIL_BEFORE_DATA") != ""
	}
	var frontier *benchmarkFrontierProbe
	if os.Getenv("SMP3_FRONTIER_EXPOSURE_TELEMETRY") != "" || os.Getenv("SMP3_BENCHMARK_FRONTIER_EXPOSURE_GRACE") != "" {
		frontier = &benchmarkFrontierProbe{grace: os.Getenv("SMP3_BENCHMARK_FRONTIER_EXPOSURE_GRACE") != ""}
	}
	var handoff *StreamHandoffTelemetry
	if os.Getenv("SMP3_HANDOFF_TELEMETRY") != "" {
		handoff = &StreamHandoffTelemetry{}
		benchmarkHandoffTrace.Store(handoff)
		defer benchmarkHandoffTrace.Store(nil)
	}
	var wakeTelemetry *streamWakeTelemetry
	if os.Getenv("SMP3_WAKE_TELEMETRY") != "" {
		wakeTelemetry = &streamWakeTelemetry{}
	}
	var loadProbe *streamLoadProbe
	if os.Getenv("SMP3_LOAD_ACCOUNTING_TELEMETRY") != "" || os.Getenv("SMP3_BENCHMARK_INSERVICE_SCORE") != "" {
		loadProbe = &streamLoadProbe{inServiceScore: os.Getenv("SMP3_BENCHMARK_INSERVICE_SCORE") != ""}
	}
	bandwidth := []uint32{uint32(rates[0] / 1_000_000), uint32(rates[1] / 1_000_000)}
	if raw := os.Getenv("SMP3_BENCHMARK_BASELINE_RATES"); raw != "" {
		parts := strings.Split(raw, ",")
		if len(parts) == 2 {
			if a, errA := strconv.Atoi(strings.TrimSpace(parts[0])); errA == nil {
				if b, errB := strconv.Atoi(strings.TrimSpace(parts[1])); errB == nil && a > 0 && b > 0 {
					bandwidth = []uint32{uint32(a), uint32(b)}
				}
			}
		}
	}
	cfg := StreamConfig{
		frontierProbe: frontier,
		ChunkSize:     benchmarkIntEnv("SMP3_BENCHMARK_CHUNK", 64*1024), QueueFrames: benchmarkIntEnv("SMP3_BENCHMARK_QUEUE", 256), MaxReorderFrames: 4096,
		MaxInflightFrames: benchmarkIntEnv("SMP3_BENCHMARK_INFLIGHT", 2048), AckInterval: 5 * time.Millisecond,
		// Match the v2.5 client/server production default. A one-second
		// benchmark-only timeout overstates frontier loss on the 250 Mbps,
		// 10 ms virtual path and creates artificial rescue traffic.
		RetransmitTimeout: 1500 * time.Millisecond, RecoveryTimeout: 5 * time.Second,
		BandwidthMbps: bandwidth, SchedulerMode: mode,
		BenchmarkFixedRatio:        os.Getenv("SMP3_BENCHMARK_FIXED_RATIO") != "",
		BenchmarkReadyFallback:     os.Getenv("SMP3_BENCHMARK_READY_FALLBACK") != "",
		BenchmarkWeightedDeficit:   os.Getenv("SMP3_BENCHMARK_WEIGHTED_DEFICIT") != "",
		BenchmarkWeightedCommit:    os.Getenv("SMP3_BENCHMARK_WEIGHTED_COMMIT") != "",
		BenchmarkAssignedCommit:    os.Getenv("SMP3_BENCHMARK_ASSIGNED_COMMIT") != "",
		BenchmarkAssignedService:   os.Getenv("SMP3_BENCHMARK_ASSIGNED_FALLBACK") != "",
		BenchmarkAssignedReady:     os.Getenv("SMP3_BENCHMARK_ASSIGNED_READY") != "",
		BenchmarkAssignedDecoupled: os.Getenv("SMP3_BENCHMARK_ASSIGNED_DECOUPLED") != "",
		BenchmarkPendingFrames:     benchmarkIntEnv("SMP3_BENCHMARK_PENDING", 32),
		HandoffTelemetry:           handoff,
		wakeTelemetry:              wakeTelemetry,
		loadProbe:                  loadProbe,
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SMP3_BENCHMARK_HOL_MODE"))) {
	case "completion":
		cfg.HOLMode = StreamHOLCompletion
	case "", "legacy", "disabled":
		cfg.HOLMode = StreamHOLLegacy
	default:
		return 0, StreamStats{}, fmt.Errorf("invalid SMP3_BENCHMARK_HOL_MODE=%q", os.Getenv("SMP3_BENCHMARK_HOL_MODE"))
	}
	if mode == StreamSchedulerAggregation && os.Getenv("SMP3_BENCHMARK_DYNAMIC_CAPACITY") != "" {
		cfg.CapacityMode = StreamCapacityDynamic
	}
	if os.Getenv("SMP3_BENCHMARK_DISABLE_RESCUE") != "" {
		cfg.RetransmitTimeout = time.Hour
	} else {
		cfg.RetransmitTimeout = benchmarkDurationEnv("SMP3_BENCHMARK_RESCUE_TIMEOUT", cfg.RetransmitTimeout)
	}
	if os.Getenv("SMP3_RESCUE_ATTRIBUTION") != "" {
		cfg.Telemetry = &StreamTelemetry{}
	}
	left, leftApp := NewStreamEngine(cfg)
	receiverCfg := cfg
	receiverCfg.frontierProbe = nil
	right, rightApp := NewStreamEngine(receiverCfg)
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()
	var benchmarkEpoch time.Time
	var logicalBytes atomic.Int64
	var telemetryLinks [2]*benchmarkLink
	for i := range faults {
		if faults[i].degradeAfter > 0 || faults[i].recoverAfter > 0 {
			faults[i].degradeEpoch = &benchmarkEpoch
		}
		if faults[i].degradeAfterBytes > 0 {
			faults[i].logicalBytes = &logicalBytes
		}
	}
	for offset := 0; offset < legCount; offset++ {
		id := firstLeg + offset
		leftLeg, rightLeg := benchmarkLinkPair(latencies[id], rates[id], faults[id])
		if bl, ok := leftLeg.(*benchmarkLink); ok {
			telemetryLinks[id] = bl
		}
		if err := left.AttachLeg(LegID(id), leftLeg, nil); err != nil {
			return 0, StreamStats{}, err
		}
		if err := right.AttachLeg(LegID(id), rightLeg, nil); err != nil {
			return 0, StreamStats{}, err
		}
	}
	benchmarkEpoch = time.Now()
	left.SetActiveForTest(true)
	if os.Getenv("SMP3_PROMOTION_MEMORY_MONITOR") != "" {
		stopMonitor := promotionMonitor(left, right, size)
		defer stopMonitor()
	}
	readErr := make(chan error, 1)
	go func() { _, err := io.CopyN(benchmarkPayloadChecker{}, rightApp, int64(size)); readErr <- err }()
	started := time.Now()
	var feeding benchmarkFeedingSamples
	stopFeeding := make(chan struct{})
	feedingDone := make(chan struct{})
	if os.Getenv("SMP3_POST_RESCUE_TELEMETRY") != "" {
		go func() {
			defer close(feedingDone)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopFeeding:
					return
				case <-ticker.C:
					bp := left.BackpressureSnapshot()
					feeding.count++
					n := int64(bp.Inflight)
					feeding.inflight = append(feeding.inflight, n)
					if n > feeding.maxInflight {
						feeding.maxInflight = n
					}
					if n >= int64(bp.InflightMax) {
						feeding.atInflightCeiling++
					}
					for id, link := range telemetryLinks {
						if link == nil {
							continue
						}
						depth := int64(len(link.tx))
						feeding.queueDepthSum[id] += depth
						if depth == 0 {
							feeding.queueEmpty[id]++
						}
					}
				}
			}
		}()
	} else {
		close(feedingDone)
	}
	writeErr := make(chan error, 1)
	var appWriter io.Writer = leftApp
	if faults[0].logicalBytes != nil || faults[1].logicalBytes != nil {
		appWriter = benchmarkCountingWriter{Writer: leftApp, bytes: &logicalBytes}
	}
	payloadRate := int64(benchmarkIntEnv("SMP3_BENCHMARK_APP_RATE_BPS", 0))
	go func() {
		_, err := io.CopyN(appWriter, benchmarkPayloadReader{rateBPS: payloadRate}, int64(size))
		writeErr <- err
	}()
	if os.Getenv("SMP3_BACKPRESSURE_EVIDENCE") != "" {
		deadline := time.NewTimer(benchmarkDurationEnv("SMP3_EVIDENCE_TIMEOUT", 180*time.Second))
		defer deadline.Stop()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		writeDone, readDone := false, false
		for !writeDone || !readDone {
			select {
			case err := <-writeErr:
				if err != nil {
					return 0, StreamStats{}, err
				}
				writeDone = true
				writeErr = nil
			case err := <-readErr:
				if err != nil {
					return 0, StreamStats{}, err
				}
				readDone = true
				readErr = nil
			case <-ticker.C:
				printBenchmarkBackpressure("TIMELINE", started, left, right)
			case <-deadline.C:
				printBenchmarkBackpressure("BACKPRESSURE SNAPSHOT TIMEOUT", started, left, right)
				stack := make([]byte, 1<<20)
				n := runtime.Stack(stack, true)
				fmt.Fprintf(os.Stderr, "BACKPRESSURE GOROUTINES\n%s\n", stack[:n])
				return 0, StreamStats{}, fmt.Errorf("backpressure evidence timeout after %s", time.Since(started))
			}
		}
	} else {
		if err := <-writeErr; err != nil {
			return 0, StreamStats{}, err
		}
		if err := <-readErr; err != nil {
			return 0, StreamStats{}, err
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for left.Snapshot().OutstandingFrames > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Attribute wall-clock throughput only after the sender's logical records
	// have drained. Measuring at application-copy completion can make a dual
	// run appear faster than the sum of its single-leg controls while DATA is
	// still queued or awaiting ACK retirement.
	elapsed := time.Since(started)
	close(stopFeeding)
	<-feedingDone
	if feeding.count > 0 {
		bp := left.BackpressureSnapshot()
		fmt.Fprintf(os.Stderr, "POST_RESCUE_FEEDING samples=%d queue_mean=[%.2f,%.2f] queue_empty=[%.3f,%.3f] inflight_p95=%d inflight_max=%d inflight_ceiling_fraction=%.4f ordinary_dispatches=[%d,%d] rescue_dispatches=[%d,%d] serializer_idle_ns=[%d,%d] serializer_busy_ns=[%d,%d]\n", feeding.count, float64(feeding.queueDepthSum[0])/float64(feeding.count), float64(feeding.queueDepthSum[1])/float64(feeding.count), float64(feeding.queueEmpty[0])/float64(feeding.count), float64(feeding.queueEmpty[1])/float64(feeding.count), percentileInt64(feeding.inflight, .95), feeding.maxInflight, float64(feeding.atInflightCeiling)/float64(feeding.count), bp.Legs[0].OrdinaryDispatches, bp.Legs[1].OrdinaryDispatches, bp.Legs[0].RescueDispatches, bp.Legs[1].RescueDispatches, func() int64 {
			if telemetryLinks[0] != nil {
				return telemetryLinks[0].serializerIdleNs.Load()
			}
			return 0
		}(), func() int64 {
			if telemetryLinks[1] != nil {
				return telemetryLinks[1].serializerIdleNs.Load()
			}
			return 0
		}(), func() int64 {
			if telemetryLinks[0] != nil {
				return telemetryLinks[0].serializerBusyNs.Load()
			}
			return 0
		}(), func() int64 {
			if telemetryLinks[1] != nil {
				return telemetryLinks[1].serializerBusyNs.Load()
			}
			return 0
		}())
	}
	if os.Getenv("SMP3_PRINT_WEIGHTS") != "" {
		s := left.BackpressureSnapshot()
		fmt.Fprintf(os.Stderr, "FINAL PATH TELEMETRY leg0=%+v leg1=%+v\n", s.Legs[0], s.Legs[1])
	}
	if os.Getenv("SMP3_DYNAMIC_CAPACITY_TELEMETRY") != "" {
		ss := left.Snapshot()
		fmt.Fprintf(os.Stderr, "DYNAMIC_CAPACITY raw_bps=%v smoothed_bps=%v confidence=%v valid=%v demand=%v sample_time=%v weight=%v assignment_share=%v pending=%v acked=%v retry=%v rescue=%d\n", ss.CapacityRawBPS, ss.CapacitySmoothedBPS, ss.CapacityConfidence, ss.CapacityValid, ss.CapacityDemand, ss.CapacityLastSample, ss.CapacityWeight, ss.CapacityAssignmentShare, ss.CapacityPending, ss.TxAckedUsefulByLeg, ss.TxRetransmitBytesByLeg, ss.FrontierRescueAttempts)
	}
	if os.Getenv("SMP3_S1_SERIALIZER_PILOT") != "" && os.Getenv("SMP3_BENCHMARK_PACER") == "serializer" {
		for id, link := range telemetryLinks {
			if link == nil {
				continue
			}
			pm := serviceMetrics(link)
			_, ap, ax, lm, lp, l99, lx := serializerDeliveryMetrics(link)
			fmt.Fprintf(os.Stderr, "S1_SERIALIZER_TIMING leg=%d scheduled_gap_p95=%dns actual_gap_p95=%dns actual_gap_max=%dns lateness_median=%dns lateness_p95=%dns lateness_p99=%dns lateness_max=%dns scheduled_bursts=%d\n", id, pm.p95GapNs, ap, ax, lm, lp, l99, lx, len(link.serviceEvents))
		}
	}
	if os.Getenv("SMP3_RESCUE_ATTRIBUTION") != "" {
		ss := left.Snapshot()
		tx := ss.Telemetry
		bp := left.BackpressureSnapshot()
		rx := right.Snapshot().Telemetry
		totalRetransmit := ss.TxRetransmitBytesByLeg[0] + ss.TxRetransmitBytesByLeg[1]
		rescueBytes := tx.RescueAttemptBytes[0] + tx.RescueAttemptBytes[1]
		fmt.Fprintf(os.Stderr, "S1_RESCUE_ACCOUNTING physical=%d useful=%d overhead=%d total_retransmit=%d frontier_rescue=%d non_rescue_retransmit=%d\n", ss.TxSentBytesByLeg[0]+ss.TxSentBytesByLeg[1], size, int64(ss.TxSentBytesByLeg[0]+ss.TxSentBytesByLeg[1])-int64(size), totalRetransmit, rescueBytes, int64(totalRetransmit)-int64(rescueBytes))
		fmt.Fprintf(os.Stderr, "S1_RESCUE_ATTRIBUTION physical=%v retransmit=%v rescue_bytes=%v rescue_frames=%v retry_dispatches=%d rescue_dispatches=%d rescue_scheduled=%d rx_duplicate_bytes=%v rx_duplicate_frames=%v ack_advances=%d ack_retired_records=%d ack_retired_bytes=%d ledger_outstanding=%d inflight=%d/%d\n", ss.TxSentBytesByLeg, ss.TxRetransmitBytesByLeg, tx.RescueAttemptBytes, tx.RescueAttemptFrames, bp.RetryDispatches, bp.RescueDispatches, bp.RescueScheduled, rx.DataRxDuplicateBytes, rx.DataRxDuplicateFrames, bp.ACKAdvances, bp.ACKRetiredRecords, bp.ACKRetiredBytes, bp.LedgerOutstanding, bp.Inflight, bp.InflightMax)
	}
	if handoff != nil {
		samples, gaps := handoff.snapshot()
		fmt.Fprintf(os.Stderr, "HANDOFF_CADENCE records=%d gaps=%s\n", len(samples), handoffPercentiles(gaps))
		if os.Getenv("SMP3_SCHEDULER_STALL_TELEMETRY") != "" {
			buckets := handoffGapBuckets(gaps)
			fmt.Fprintf(os.Stderr, "SCHEDULER_BRANCH primary_success=%d primary_failure=%d fallback_attempts=%d fallback_success=%d spacewake_entries=%d spacewake_wait_ns=%d gap_buckets=%v\n", left.debugPrimarySuccess.Load(), left.debugPrimaryFailure.Load(), left.debugFallbackAttempts.Load(), left.debugFallbackSuccess.Load(), left.debugSpaceWakeEntries.Load(), left.debugSpaceWakeWaitNs.Load(), buckets)
		}
		for leg := 0; leg < 2; leg++ {
			fmt.Fprintf(os.Stderr, "HANDOFF_LEG leg=%d sched_wait=%s decision=%s handoff=%s enqueue_wait=%s queue_wait=%s writer_serializer=%s selected_serializer=%s\n", leg,
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT1Ns - s.traceT0Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT2Ns - s.traceT1Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT3Ns - s.traceT2Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT4Ns - s.traceT3Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT5Ns - s.traceT4Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT7Ns - s.traceT5Ns })),
				handoffPercentiles(handoffDurations(samples, leg, func(s *StreamTXRecord) int64 { return s.traceT7Ns - s.traceT2Ns })))
		}
	}
	if wakeTelemetry != nil {
		waits, signals, dropped := wakeTelemetry.snapshot()
		fmt.Fprintf(os.Stderr, "SPACEWAKE_CAUSAL waits=%d dropped=%d signals=%v %s\n", len(waits), dropped, signals, wakeWaitSummary(waits))
	}
	if loadProbe != nil {
		s, d, lh, lc, ln, hn, assigned, counts := loadProbe.snapshot()
		fmt.Fprintf(os.Stderr, "LOAD_ACCOUNTING samples=%d divergence=%d divergence_fraction=%.4f low_hole_select=%d low_clear_select=%d hole_time_ns=[%d,%d] assigned=[%d,%d] dispatches=[%d,%d] candidate=%v\n", s, d, float64(d)/float64(maxUint64(s, 1)), lh, lc, ln, hn, assigned[0], assigned[1], counts[0], counts[1], loadProbe.inServiceScore)
	}
	if os.Getenv("SMP3_BENCHMARK_ASSIGNED_COMMIT") != "" || os.Getenv("SMP3_BENCHMARK_ASSIGNED_FALLBACK") != "" || os.Getenv("SMP3_BENCHMARK_ASSIGNED_READY") != "" || os.Getenv("SMP3_BENCHMARK_ASSIGNED_DECOUPLED") != "" {
		bytes, records := left.assignedSnapshot()
		fmt.Fprintf(os.Stderr, "ASSIGNED_SERVICE bytes=[%d,%d] records=[%d,%d] total=%d\n", bytes[0], bytes[1], records[0], records[1], bytes[0]+bytes[1])
		fmt.Fprintf(os.Stderr, "ASSIGNED_READY decisions=[both:%d low_only:%d high_only:%d none:%d] race_failures=%d\n", left.readyStateDecisions[0].Load(), left.readyStateDecisions[1].Load(), left.readyStateDecisions[2].Load(), left.readyStateDecisions[3].Load(), left.readyRaceFailures.Load())
		if os.Getenv("SMP3_BENCHMARK_ASSIGNED_DECOUPLED") != "" {
			fmt.Fprintf(os.Stderr, "DECOUPLED pending_depth=[%d,%d] pending_max=[%d,%d] planner_wait_ns=%d feeder_wait_ns=[%d,%d]\n", left.pendingDepth[0].Load(), left.pendingDepth[1].Load(), left.pendingMax[0].Load(), left.pendingMax[1].Load(), left.plannerPendingWaitNs.Load(), left.feederBlockedWaitNs[0].Load(), left.feederBlockedWaitNs[1].Load())
			fmt.Fprintf(os.Stderr, "ASSIGNMENT_FAILURE initial=%d rollback=%d reassigned=%d reassignment_count=%d admitted=[%d,%d]\n", left.decoupledStats.initial.Load(), left.decoupledStats.rollback.Load(), left.decoupledStats.reassigned.Load(), left.decoupledStats.reassignments.Load(), left.decoupledStats.admitted[0].Load(), left.decoupledStats.admitted[1].Load())
		}
	}
	if frontier != nil {
		printFrontierProbe(frontier)
	}
	return elapsed, left.Snapshot(), nil
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func wakeWaitSummary(waits []*streamWakeWait) string {
	var total, stale int64
	var first [4]int
	for _, w := range waits {
		if w.W4.IsZero() {
			continue
		}
		total += w.W4.Sub(w.W0).Nanoseconds()
		if !w.W1.IsZero() {
			stale += w.W3.Sub(w.W1).Nanoseconds()
		}
		if w.FirstLeg == 0 {
			first[0]++
		} else if w.FirstLeg == 1 {
			first[1]++
		} else if w.FirstLeg == 2 {
			first[2]++
		} else {
			first[3]++
		}
	}
	frac := float64(0)
	if total > 0 {
		frac = float64(stale) / float64(total)
	}
	return fmt.Sprintf("total_wait_ns=%d stale_after_writable_ns=%d stale_fraction=%.4f first_low=%d first_high=%d both=%d neither=%d", total, stale, frac, first[0], first[1], first[2], first[3])
}

func handoffDurations(samples []*StreamTXRecord, leg int, f func(*StreamTXRecord) int64) []int64 {
	values := make([]int64, 0, len(samples))
	for _, s := range samples {
		if int(s.traceLeg) == leg {
			if v := f(s); v >= 0 {
				values = append(values, v)
			}
		}
	}
	return values
}

func handoffPercentiles(values []int64) string {
	if len(values) == 0 {
		return "n/a"
	}
	copyValues := append([]int64(nil), values...)
	return fmt.Sprintf("p50=%dns,p95=%dns,p99=%dns,max=%dns,n=%d", percentileInt64(copyValues, .50), percentileInt64(copyValues, .95), percentileInt64(copyValues, .99), maxInt64(copyValues), len(copyValues))
}

func maxInt64(values []int64) int64 {
	var max int64
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

func handoffGapBuckets(gaps []int64) [8]struct {
	Count   int
	TotalNs int64
} {
	var out [8]struct {
		Count   int
		TotalNs int64
	}
	for _, gap := range gaps {
		idx := 0
		switch {
		case gap < int64(time.Millisecond):
			idx = 0
		case gap < int64(2*time.Millisecond):
			idx = 1
		case gap < int64(5*time.Millisecond):
			idx = 2
		case gap < int64(10*time.Millisecond):
			idx = 3
		case gap < int64(50*time.Millisecond):
			idx = 4
		case gap < int64(100*time.Millisecond):
			idx = 5
		case gap < int64(250*time.Millisecond):
			idx = 6
		default:
			idx = 7
		}
		out[idx].Count++
		out[idx].TotalNs += gap
	}
	return out
}

func printBenchmarkBackpressure(label string, started time.Time, left, right *StreamEngine) {
	fmt.Fprintf(os.Stderr, "%s T+%s sender=%+v receiver=%+v\n", label, time.Since(started).Round(time.Second), left.BackpressureSnapshot(), right.BackpressureSnapshot())
}

// TestPostRescueAggregationBaseline measures healthy path asymmetry with the
// accepted one-second rescue default. It is benchmark-only and does not alter
// scheduler policy.
func TestPostRescueAggregationBaseline(t *testing.T) {
	if os.Getenv("SMP3_POST_RESCUE_BASELINE") == "" {
		t.Skip("set SMP3_POST_RESCUE_BASELINE=1")
	}
	if os.Getenv("SMP3_BENCHMARK_PACER") != "serializer" {
		t.Fatal("post-rescue baseline requires serializer")
	}
	size := benchmarkIntEnv("SMP3_POST_RESCUE_BYTES", 32<<20)
	reps := benchmarkIntEnv("SMP3_POST_RESCUE_REPS", 1)
	cases := []struct {
		name  string
		rates [2]int64
		rtts  [2]time.Duration
	}{
		{"symmetric_100_100_20ms", [2]int64{100e6, 100e6}, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}},
		{"rtt_20_40", [2]int64{100e6, 100e6}, [2]time.Duration{10 * time.Millisecond, 20 * time.Millisecond}},
		{"rtt_20_60", [2]int64{100e6, 100e6}, [2]time.Duration{10 * time.Millisecond, 30 * time.Millisecond}},
		{"rtt_20_100", [2]int64{100e6, 100e6}, [2]time.Duration{10 * time.Millisecond, 50 * time.Millisecond}},
		{"bw_50_100", [2]int64{50e6, 100e6}, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}},
		{"bw_50_150", [2]int64{50e6, 150e6}, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}},
		{"bw_50_200", [2]int64{50e6, 200e6}, [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}},
		{"combined_20ms50_60ms150", [2]int64{50e6, 150e6}, [2]time.Duration{10 * time.Millisecond, 30 * time.Millisecond}},
		{"combined_20ms50_100ms200", [2]int64{50e6, 200e6}, [2]time.Duration{10 * time.Millisecond, 50 * time.Millisecond}},
	}
	selected := os.Getenv("SMP3_POST_RESCUE_CASE")
	for _, tc := range cases {
		if selected != "" && selected != tc.name {
			continue
		}
		for rep := 1; rep <= reps; rep++ {
			var elapsed [3]time.Duration
			var stats [3]StreamStats
			for path := 0; path < 3; path++ {
				legs, first := 1, path
				if path == 2 {
					legs, first = 2, 0
				}
				var err error
				elapsed[path], stats[path], err = runStreamAggregationSample(size, legs, first, tc.rates, tc.rtts, [2]benchmarkFault{})
				if err != nil {
					t.Fatal(tc.name, err)
				}
			}
			mbps := func(d time.Duration) float64 { return float64(size) * 8 / d.Seconds() / 1e6 }
			s0, s1, dual := mbps(elapsed[0]), mbps(elapsed[1]), mbps(elapsed[2])
			physical := stats[2].TxSentBytesByLeg[0] + stats[2].TxSentBytesByLeg[1]
			raw := float64(tc.rates[0]+tc.rates[1]) / 1e6
			physicalMbps := float64(physical) * 8 / elapsed[2].Seconds() / 1e6
			bdp0 := float64(tc.rates[0]) * tc.rtts[0].Seconds() / 4
			bdp1 := float64(tc.rates[1]) * tc.rtts[1].Seconds() / 4
			t.Logf("POST_RESCUE case=%s rep=%d raw=%.2f single=[%.2f,%.2f] dual=%.2f useful_raw=%.2f%% physical_raw=%.2f%% payload_eff=%.2f%% physical_bytes=[%d,%d] leg_util=[%.2f%%,%.2f%%] share=[%.2f%%,%.2f%%] amp=%.3fx retransmit=%d rescue=%d bdp=[%.0f,%.0f] inflight=%d/%d reorder=%d/%d ledger=%d", tc.name, rep, raw, s0, s1, dual, 100*dual/raw, 100*physicalMbps/raw, 100*dual/physicalMbps, stats[2].TxSentBytesByLeg[0], stats[2].TxSentBytesByLeg[1], 100*float64(stats[2].TxSentBytesByLeg[0])*8/elapsed[2].Seconds()/1e6/(float64(tc.rates[0])/1e6), 100*float64(stats[2].TxSentBytesByLeg[1])*8/elapsed[2].Seconds()/1e6/(float64(tc.rates[1])/1e6), 100*float64(stats[2].TxSentBytesByLeg[0])/float64(physical), 100*float64(stats[2].TxSentBytesByLeg[1])/float64(physical), float64(physical)/float64(size), stats[2].TxRetransmitBytesByLeg[0]+stats[2].TxRetransmitBytesByLeg[1], stats[2].FrontierRescueAttempts, bdp0, bdp1, stats[2].OutstandingFrames, 2048, stats[2].RxPendingFrames, stats[2].RxMaxPendingFrames, stats[2].OutstandingFrames)
		}
	}
}

func TestChooseLegSequenceAudit(t *testing.T) {
	if os.Getenv("SMP3_CHOOSELEG_AUDIT") == "" {
		t.Skip("set SMP3_CHOOSELEG_AUDIT=1")
	}
	c := &StreamEngine{cfg: StreamConfig{SchedulerMode: StreamSchedulerStatic, ChunkSize: 32 * 1024, BandwidthMbps: []uint32{50, 200}}, legs: make(map[uint8]*streamLeg)}
	c.legs[0] = &streamLeg{id: 0, send: make(chan txSendAttempt, 256), done: make(chan struct{})}
	c.legs[1] = &streamLeg{id: 1, send: make(chan txSendAttempt, 256), done: make(chan struct{})}
	prefixes := []int{32, 64, 128, 256, 512, 1024, 2048}
	counts := [2]int{}
	nextPrefix := 0
	for i := 1; i <= 2048; i++ {
		leg := c.chooseLeg(true, -1)
		counts[leg.id]++
		c.txSentBytes[leg.id].Add(uint64(c.cfg.ChunkSize))
		if nextPrefix < len(prefixes) && i == prefixes[nextPrefix] {
			t.Logf("prefix=%d low=%d high=%d share=%.2f/%.2f", i, counts[0], counts[1], 100*float64(counts[0])/float64(i), 100*float64(counts[1])/float64(i))
			nextPrefix++
		}
	}
	t.Logf("pure chooseLeg final low=%d high=%d share=%.2f/%.2f weights=%.0f/%.0f", counts[0], counts[1], 100*float64(counts[0])/2048, 100*float64(counts[1])/2048, c.effectiveSchedulerWeight(c.legs[0]), c.effectiveSchedulerWeight(c.legs[1]))
	if os.Getenv("SMP3_ASSIGNED_SEQUENCE_AUDIT") == "" {
		return
	}
	assigned := &StreamEngine{cfg: StreamConfig{SchedulerMode: StreamSchedulerStatic, BenchmarkAssignedCommit: true, ChunkSize: 32 * 1024, BandwidthMbps: []uint32{50, 200}}, legs: make(map[uint8]*streamLeg)}
	assigned.legs[0] = &streamLeg{id: 0, send: make(chan txSendAttempt, 256), done: make(chan struct{})}
	assigned.legs[1] = &streamLeg{id: 1, send: make(chan txSendAttempt, 256), done: make(chan struct{})}
	for i := 1; i <= 2048; i++ {
		leg := assigned.chooseAssignedLeg(-1, assigned.cfg.ChunkSize)
		assigned.assignedService[leg.id] += uint64(assigned.cfg.ChunkSize)
		if i == 32 || i == 64 || i == 128 || i == 256 || i == 512 || i == 1024 || i == 2048 {
			t.Logf("assigned_prefix=%d low=%d high=%d share=%.2f/%.2f", i, assigned.assignedService[0]/uint64(assigned.cfg.ChunkSize), assigned.assignedService[1]/uint64(assigned.cfg.ChunkSize), 100*float64(assigned.assignedService[0])/float64(i*assigned.cfg.ChunkSize), 100*float64(assigned.assignedService[1])/float64(i*assigned.cfg.ChunkSize))
		}
	}
}

// TestStreamSerializerS1Pilot is the first corrected S1 measurement. It is
// opt-in because it transfers 256 MiB and is benchmark-only.
func TestStreamSerializerS1Pilot(t *testing.T) {
	if os.Getenv("SMP3_S1_SERIALIZER_PILOT") == "" {
		t.Skip("set SMP3_S1_SERIALIZER_PILOT=1")
	}
	if os.Getenv("SMP3_BENCHMARK_PACER") != "serializer" {
		t.Fatal("S1 pilot requires SMP3_BENCHMARK_PACER=serializer")
	}
	size := benchmarkIntEnv("SMP3_S1_BYTES", 256<<20)
	rates := [2]int64{100_000_000, 100_000_000}
	latencies := [2]time.Duration{10 * time.Millisecond, 10 * time.Millisecond}
	faults := [2]benchmarkFault{}
	if drop := benchmarkIntEnv("SMP3_S1_DROP_EVERY", 0); drop > 0 {
		faults[1].dropEvery = int64(drop)
	}
	if stall := benchmarkIntEnv("SMP3_S1_STALL_AFTER_BYTES", 0); stall > 0 {
		faults[0].stallAfterBytes = int64(stall)
		faults[0].stallDuration = benchmarkDurationEnv("SMP3_S1_STALL_DURATION", 2*time.Second)
	}
	var elapsed [3]time.Duration
	var stats [3]StreamStats
	for path := 0; path < 3; path++ {
		legs, first := 1, path
		if path == 2 {
			legs, first = 2, 0
		}
		var err error
		elapsed[path], stats[path], err = runStreamAggregationSample(size, legs, first, rates, latencies, faults)
		if err != nil {
			t.Fatal(err)
		}
	}
	mbps := func(d time.Duration) float64 { return float64(size) * 8 / d.Seconds() / 1e6 }
	single0, single1, dual := mbps(elapsed[0]), mbps(elapsed[1]), mbps(elapsed[2])
	physical := stats[2].TxSentBytesByLeg[0] + stats[2].TxSentBytesByLeg[1]
	retry := stats[2].TxRetransmitBytesByLeg[0] + stats[2].TxRetransmitBytesByLeg[1]
	rescue := stats[2].FrontierRescueAttempts
	t.Logf("S1_SERIALIZER_PILOT size=%d single0=%.2fMbps single1=%.2fMbps dual=%.2fMbps control_eff=%.2f%% raw_aggregate=196.23Mbps useful_capacity_eff=%.2f%% physical_data_util=%.2f%% physical_bytes=[%d,%d] leg_share=[%.2f%%,%.2f%%] amplification=%.3fx total_retransmit_bytes=%d rescue_attempts=%d reorder=%d/%d gap_age=%s max_gap_age=%s", size, single0, single1, dual, 100*dual/(single0+single1), 100*dual/196.23, 100*((float64(physical)*8/elapsed[2].Seconds()/1e6)/196.23), stats[2].TxSentBytesByLeg[0], stats[2].TxSentBytesByLeg[1], 100*float64(stats[2].TxSentBytesByLeg[0])/float64(physical), 100*float64(stats[2].TxSentBytesByLeg[1])/float64(physical), float64(physical)/float64(size), retry, rescue, stats[2].RxPendingFrames, stats[2].RxMaxPendingFrames, stats[2].RxGapAge, stats[2].RxMaxGapAge)
}

func TestStreamBackpressureEvidence(t *testing.T) {
	if os.Getenv("SMP3_BACKPRESSURE_EVIDENCE") == "" {
		t.Skip("set SMP3_BACKPRESSURE_EVIDENCE=1")
	}
	size := benchmarkIntEnv("SMP3_BENCHMARK_BYTES", 256<<20)
	faults := [2]benchmarkFault{}
	if os.Getenv("SMP3_EVIDENCE_LOSS") != "" {
		faults[1].dropEvery = 100
	}
	elapsed, stats, err := runStreamAggregationSample(size, 2, 0,
		[2]int64{100_000_000, 500_000_000},
		[2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}, faults)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("size=%d elapsed=%s sent=%v rescues=%d", size, elapsed, stats.TxSentBytesByLeg, stats.FrontierRescueAttempts)
}

func TestStreamContinuousEvidence(t *testing.T) {
	if os.Getenv("SMP3_CONTINUOUS_EVIDENCE") == "" {
		t.Skip("set SMP3_CONTINUOUS_EVIDENCE=1")
	}
	duration := benchmarkDurationEnv("SMP3_CONTINUOUS_DURATION", 5*time.Minute)
	cfg := StreamConfig{ChunkSize: 64 * 1024, QueueFrames: 256, MaxReorderFrames: 4096,
		MaxInflightFrames: 2048, AckInterval: 5 * time.Millisecond,
		RetransmitTimeout: 1 * time.Second, RecoveryTimeout: 5 * time.Second,
		BandwidthMbps: []uint32{100, 500}, SchedulerMode: StreamSchedulerAdaptive}
	left, leftApp := NewStreamEngine(cfg)
	right, rightApp := NewStreamEngine(cfg)
	defer left.Close()
	defer right.Close()
	defer leftApp.Close()
	defer rightApp.Close()
	for id := 0; id < 2; id++ {
		a, b := benchmarkLinkPair([2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}[id], [2]int64{100_000_000, 500_000_000}[id], benchmarkFault{})
		if err := left.AttachLeg(LegID(id), a, nil); err != nil {
			t.Fatal(err)
		}
		if err := right.AttachLeg(LegID(id), b, nil); err != nil {
			t.Fatal(err)
		}
	}
	left.SetActiveForTest(true)
	var written, read atomic.Uint64
	errs := make(chan error, 2)
	go func() {
		buf := make([]byte, 32*1024)
		for i := range buf {
			buf[i] = 'x'
		}
		for {
			n, err := leftApp.Write(buf)
			written.Add(uint64(n))
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := rightApp.Read(buf)
			for _, b := range buf[:n] {
				if b != 'x' {
					errs <- errors.New("continuous payload mismatch")
					return
				}
			}
			read.Add(uint64(n))
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	start := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	lastBytes := uint64(0)
	for {
		select {
		case err := <-errs:
			t.Fatalf("continuous stream ended early: %v", err)
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			s := left.BackpressureSnapshot()
			got := read.Load()
			t.Logf("T+%s written=%d read=%d heap=%d heap_inuse=%d inflight=%d/%d ledger=%d ack=%d retry=%d rescue=%d reorder=%d", time.Since(start).Round(time.Second), written.Load(), got, m.HeapAlloc, m.HeapInuse, s.Inflight, s.InflightMax, s.LedgerOutstanding, s.ACKFrontier, s.RetryDispatches, s.RescueDispatches, right.BackpressureSnapshot().RXReorder)
			if got == lastBytes {
				t.Fatal("no application delivery progress in 30 seconds")
			}
			lastBytes = got
		case <-timer.C:
			t.Logf("continuous stream completed duration=%s written=%d read=%d", time.Since(start), written.Load(), read.Load())
			return
		}
	}
}

func TestStreamAggregationBaseline(t *testing.T) {
	if os.Getenv("SMP3_BENCHMARK") == "" {
		t.Skip("set SMP3_BENCHMARK=1 to run the asymmetric-link baseline")
	}
	size := benchmarkIntEnv("SMP3_BENCHMARK_BYTES", 64<<20)
	cases := []struct {
		name      string
		rates     [2]int64
		latencies [2]time.Duration
		faults    [2]benchmarkFault
	}{
		{name: "A_20ms_100_plus_60ms_500", rates: [2]int64{100_000_000, 500_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}},
		{name: "B_20ms_100_plus_100ms_500", rates: [2]int64{100_000_000, 500_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}},
		{name: "C_leg1_one_percent_loss", rates: [2]int64{100_000_000, 500_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}, faults: [2]benchmarkFault{{}, {dropEvery: 100}}},
		{name: "D_leg1_degrades_after_30s", rates: [2]int64{100_000_000, 500_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}, faults: [2]benchmarkFault{{}, {degradeAfter: benchmarkDurationEnv("SMP3_BENCHMARK_DEGRADE_AFTER", 30*time.Second), degradedBPS: 50_000_000}}},
		{name: "D_steady_degraded_100_plus_50", rates: [2]int64{100_000_000, 50_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}},
		{name: "E_leg0_two_second_stall", rates: [2]int64{100_000_000, 500_000_000}, latencies: [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}, faults: [2]benchmarkFault{{stallAfterBytes: 256 * 1024, stallDuration: 2 * time.Second}, {}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var results [3]time.Duration
			var stats [3]StreamStats
			for mode := 0; mode < 3; mode++ {
				legCount, firstLeg := 1, mode
				if mode == 2 {
					legCount, firstLeg = 2, 0
				}
				elapsed, snapshot, err := runStreamAggregationSample(size, legCount, firstLeg, tc.rates, tc.latencies, tc.faults)
				if err != nil {
					t.Fatal(err)
				}
				results[mode], stats[mode] = elapsed, snapshot
			}
			rate0 := float64(size) * 8 / results[0].Seconds() / 1e6
			rate1 := float64(size) * 8 / results[1].Seconds() / 1e6
			dual := float64(size) * 8 / results[2].Seconds() / 1e6
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			physical := stats[2].TxSentBytesByLeg[0] + stats[2].TxSentBytesByLeg[1]
			amplification := float64(physical) / float64(size)
			t.Logf("size=%d leg0=%.2fMbps leg1=%.2fMbps dual=%.2fMbps efficiency=%.1f%% bytes=[%d,%d] amplification=%.3fx retransmit=%d rescues=%d reorder=%d heap=%d", size, rate0, rate1, dual, dual/(rate0+rate1)*100, stats[2].TxSentBytesByLeg[0], stats[2].TxSentBytesByLeg[1], amplification, stats[2].TxRetransmitBytesByLeg[0]+stats[2].TxRetransmitBytesByLeg[1], stats[2].FrontierRescueAttempts, stats[2].RxPendingFrames, mem.HeapAlloc)
		})
	}
}

// TestStreamAggregationPairedBaseline alternates scheduler order within each
// round and uses fresh single-leg controls for each dual-leg measurement.
func TestStreamAggregationPairedBaseline(t *testing.T) {
	if os.Getenv("SMP3_PAIRED_BASELINE") == "" {
		t.Skip("set SMP3_PAIRED_BASELINE=1")
	}
	size := benchmarkIntEnv("SMP3_BENCHMARK_BYTES", 256<<20)
	rounds := benchmarkIntEnv("SMP3_PAIRED_ROUNDS", 3)
	rates := [2]int64{100_000_000, 500_000_000}
	latencies := [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}
	for round := 0; round < rounds; round++ {
		modes := []StreamSchedulerMode{StreamSchedulerStatic, StreamSchedulerAdaptive}
		if os.Getenv("SMP3_PAIRED_CAPACITY_FIRST") != "" {
			modes = append(modes, StreamSchedulerCapacityFirst)
		}
		if selected := os.Getenv("SMP3_PAIRED_MODES"); selected != "" {
			modes = modes[:0]
			for _, name := range strings.Split(selected, ",") {
				switch strings.TrimSpace(strings.ToLower(name)) {
				case "static":
					modes = append(modes, StreamSchedulerStatic)
				case "adaptive":
					modes = append(modes, StreamSchedulerAdaptive)
				case "capacity-first":
					modes = append(modes, StreamSchedulerCapacityFirst)
				default:
					t.Fatalf("unknown SMP3_PAIRED_MODES entry %q", name)
				}
			}
		}
		if round%2 == 1 && len(modes) > 1 {
			modes[0], modes[1] = modes[1], modes[0]
		}
		for _, mode := range modes {
			faults := [2]benchmarkFault{}
			if raw := os.Getenv("SMP3_PAIRED_DEGRADE_BYTES"); raw != "" {
				faults[1] = benchmarkFault{degradeAfterBytes: int64(benchmarkIntEnv("SMP3_PAIRED_DEGRADE_BYTES", 4<<20)), degradedBPS: int64(benchmarkIntEnv("SMP3_PAIRED_DEGRADED_BPS", 50_000_000))}
			} else if raw := os.Getenv("SMP3_PAIRED_DEGRADE_AFTER"); raw != "" {
				faults[1] = benchmarkFault{degradeAfter: benchmarkDurationEnv("SMP3_PAIRED_DEGRADE_AFTER", 1*time.Second), degradedBPS: int64(benchmarkIntEnv("SMP3_PAIRED_DEGRADED_BPS", 50_000_000))}
			}
			var elapsed [3]time.Duration
			var stats [3]StreamStats
			for path := 0; path < 3; path++ {
				t.Logf("PAIRED start round=%d mode=%d path=%d size=%d", round+1, mode, path, size)
				legs, first := 1, path
				if path == 2 {
					legs, first = 2, 0
				}
				var err error
				elapsed[path], stats[path], err = runStreamAggregationSampleWithMode(size, legs, first, rates, latencies, faults, mode)
				if err != nil {
					t.Fatalf("round=%d mode=%d path=%d: %v", round+1, mode, path, err)
				}
			}
			leg0 := float64(size) * 8 / elapsed[0].Seconds() / 1e6
			leg1 := float64(size) * 8 / elapsed[1].Seconds() / 1e6
			dual := float64(size) * 8 / elapsed[2].Seconds() / 1e6
			physical := stats[2].TxSentBytesByLeg[0] + stats[2].TxSentBytesByLeg[1]
			t.Logf("PAIRED round=%d mode=%d size=%d leg0=%.2f leg1=%.2f dual=%.2f efficiency=%.1f%% amplification=%.3fx rescues=%d retransmit=%d reorder=%d/%d gap_age=%s max_gap_age=%s bytes=[%d,%d]", round+1, mode, size, leg0, leg1, dual, dual/(leg0+leg1)*100, float64(physical)/float64(size), stats[2].FrontierRescueAttempts, stats[2].TxRetransmitBytesByLeg[0]+stats[2].TxRetransmitBytesByLeg[1], stats[2].RxPendingFrames, stats[2].RxMaxPendingFrames, stats[2].RxGapAge.Round(time.Millisecond), stats[2].RxMaxGapAge.Round(time.Millisecond), stats[2].TxSentBytesByLeg[0], stats[2].TxSentBytesByLeg[1])
		}
	}
}

func BenchmarkStreamAggregationScheduler(b *testing.B) {
	size := benchmarkIntEnv("SMP3_BENCHMARK_BYTES", 8<<20)
	elapsed, stats, err := runStreamAggregationSample(size, 2, 0, [2]int64{100_000_000, 500_000_000}, [2]time.Duration{20 * time.Millisecond, 60 * time.Millisecond}, [2]benchmarkFault{})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(size))
	b.ReportMetric(float64(size)*8/elapsed.Seconds()/1e6, "Mbps")
	b.ReportMetric(float64(stats.TxSentBytesByLeg[1])/float64(size)*100, "leg1_pct")
	b.ReportMetric(float64(stats.FrontierRescueAttempts), "rescues")
}

var _ StreamLeg = (*benchmarkLink)(nil)

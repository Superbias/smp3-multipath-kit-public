package smp3core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const p0cABFrameBytes = 64 << 10
const p0cABFramesPerLeg = 4096
const p0cABBytesPerLeg = uint64(p0cABFrameBytes * p0cABFramesPerLeg)
const p0cABDeadline = 60 * time.Second

type p0cABClock struct {
	mu              sync.Mutex
	changed         chan struct{}
	rate            atomic.Int64
	serviceDeadline time.Time
}

func (c *p0cABClock) current() (int64, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.rate.Load(), c.changed
}

func (c *p0cABClock) setRate(rate int64) {
	c.mu.Lock()
	if c.changed == nil {
		c.changed = make(chan struct{})
	} else {
		close(c.changed)
		c.changed = make(chan struct{})
	}
	c.rate.Store(rate)
	// A deliberate rate transition starts a new service interval. Do not let
	// idle time before the transition accumulate as transmission credit.
	c.serviceDeadline = time.Now()
	c.mu.Unlock()
}

// serialize applies a per-leg byte service rate using cumulative monotonic
// deadlines. A late timer wake makes following frame deadlines immediately
// eligible instead of charging the wake overshoot again for every frame. The
// bounded Carrier queues preserve backpressure. A zero rate pauses service
// until the next explicit rate change. If a rate changes mid-frame, elapsed
// service is charged at the old rate and the remaining bytes use the new rate.
func (c *p0cABClock) serialize(n int, abort <-chan struct{}) (time.Duration, time.Duration, bool) {
	remainingBits := float64(n) * 8
	started := time.Now()
	var requested time.Duration
	for remainingBits > 0 {
		c.mu.Lock()
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		rate, changed := c.rate.Load(), c.changed
		if rate <= 0 {
			c.mu.Unlock()
			select {
			case <-abort:
				return requested, time.Since(started), false
			case <-changed:
				continue
			}
		}
		wait := time.Duration(remainingBits / float64(rate) * float64(time.Second))
		if wait < time.Nanosecond {
			wait = time.Nanosecond
		}
		base := c.serviceDeadline
		if base.IsZero() {
			base = time.Now()
		}
		target := base.Add(wait)
		c.serviceDeadline = target
		c.mu.Unlock()
		requested += wait
		delay := time.Until(target)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			return requested, time.Since(started), true
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			servedFor := time.Since(base)
			if servedFor < 0 {
				servedFor = 0
			}
			servedBits := float64(servedFor) * float64(rate) / float64(time.Second)
			if servedBits > remainingBits {
				servedBits = remainingBits
			}
			remainingBits -= servedBits
			c.mu.Lock()
			c.serviceDeadline = time.Now()
			c.mu.Unlock()
		case <-abort:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return requested, time.Since(started), false
		}
	}
	return requested, time.Since(started), true
}

type p0cABWaitSample struct {
	kind      string
	requested time.Duration
	actual    time.Duration
}

type p0cABWaitRecorder struct {
	mu      sync.Mutex
	samples []p0cABWaitSample
}

func (r *p0cABWaitRecorder) record(kind string, requested, actual time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, p0cABWaitSample{kind: kind, requested: requested, actual: actual})
	r.mu.Unlock()
}

func (r *p0cABWaitRecorder) summary(kind string) map[string]any {
	r.mu.Lock()
	samples := append([]p0cABWaitSample(nil), r.samples...)
	r.mu.Unlock()
	requested, actual, overshoot := make([]int64, 0), make([]int64, 0), make([]int64, 0)
	for _, sample := range samples {
		if sample.kind != kind || sample.requested <= 0 {
			continue
		}
		want, got := sample.requested.Nanoseconds(), sample.actual.Nanoseconds()
		requested = append(requested, want)
		actual = append(actual, got)
		overshoot = append(overshoot, got-want)
	}
	return map[string]any{
		"kind": kind, "timer_wait_count": len(actual),
		"requested_ns": p0cABDurationSummary(requested),
		"actual_ns":    p0cABDurationSummary(actual),
		"overshoot_ns": p0cABDurationSummary(overshoot),
	}
}

func p0cABDurationSummary(values []int64) map[string]any {
	if len(values) == 0 {
		return map[string]any{"count": 0}
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var total int64
	for _, value := range values {
		total += value
	}
	quantile := func(percent float64) int64 {
		return ordered[int(float64(len(ordered)-1)*percent)]
	}
	return map[string]any{
		"count": len(values), "sum": total, "mean": total / int64(len(values)),
		"p50": quantile(.50), "p95": quantile(.95), "p99": quantile(.99),
		"max": ordered[len(ordered)-1],
	}
}

type p0cABWriteRequest struct {
	data       []byte
	frames     []StreamFrameHeader
	serialized chan error
}

type p0cABDelivery struct {
	data            []byte
	frames          []StreamFrameHeader
	serviceComplete time.Time
	ready           time.Time
}

type p0cABCarrier struct {
	net.Conn
	tx               chan p0cABWriteRequest
	delivery         chan p0cABDelivery
	abort            chan struct{}
	abortOnce        sync.Once
	closeOnce        sync.Once
	writeMu          sync.Mutex
	closed           bool
	workers          sync.WaitGroup
	workersDone      chan struct{}
	clock            *p0cABClock
	delay            time.Duration
	waits            *p0cABWaitRecorder
	traceRecorder    *streamPathQoEFrameRecorder
	traceEndpoint    string
	traceDirection   string
	traceLeg         int
	traceWriteParser streamPathQoEFrameParser
	tcpInfoMu        sync.Mutex
	tcpInfoAtClose   map[string]any
}

func p0cABPair(clock *p0cABClock, rtt time.Duration) (*p0cABCarrier, *p0cABCarrier) {
	left, right := net.Pipe()
	reverse := &p0cABClock{changed: make(chan struct{})}
	reverse.rate.Store(10_000_000_000)
	return p0cABWrap(left, clock, rtt/2), p0cABWrap(right, reverse, rtt/2)
}

func p0cABWrap(conn net.Conn, clock *p0cABClock, delay time.Duration) *p0cABCarrier {
	c := &p0cABCarrier{
		Conn: conn, tx: make(chan p0cABWriteRequest, 2), delivery: make(chan p0cABDelivery, 2),
		abort: make(chan struct{}), workersDone: make(chan struct{}), clock: clock, delay: delay,
		waits: &p0cABWaitRecorder{},
	}
	c.workers.Add(2)
	go func() {
		defer c.workers.Done()
		defer close(c.delivery)
		for request := range c.tx {
			serviceStarted := time.Now()
			c.recordTrace("virtual_service_start", request.frames, serviceStarted, 0, nil)
			requested, actual, ok := c.clock.serialize(len(request.data), c.abort)
			c.waits.record("serialization", requested, actual)
			if !ok {
				c.recordTrace("virtual_service_aborted", request.frames, time.Now(), actual, net.ErrClosed)
				request.serialized <- net.ErrClosed
				return
			}
			completed := time.Now()
			c.recordTrace("virtual_service_complete", request.frames, completed, actual, nil)
			select {
			case c.delivery <- p0cABDelivery{data: request.data, frames: request.frames, serviceComplete: completed, ready: completed.Add(c.delay)}:
				c.recordTrace("virtual_delivery_enqueued", request.frames, time.Now(), 0, nil)
				request.serialized <- nil
			case <-c.abort:
				request.serialized <- net.ErrClosed
				return
			}
		}
	}()
	go func() {
		defer c.workers.Done()
		var pending *p0cABDelivery
		for {
			if pending == nil {
				item, ok := <-c.delivery
				if !ok {
					return
				}
				pending = &item
				c.recordTrace("virtual_propagation_start", item.frames, time.Now(), 0, nil)
			}
			if !c.wait(time.Until(pending.ready), "propagation") {
				c.recordTrace("virtual_propagation_aborted", pending.frames, time.Now(), time.Since(pending.serviceComplete), net.ErrClosed)
				return
			}
			item := *pending
			pending = nil
			c.recordTrace("virtual_propagation_ready", item.frames, time.Now(), time.Since(item.serviceComplete), nil)
			c.waits.record("propagation_observed", c.delay, time.Since(item.serviceComplete))
			pipeWriteStarted := time.Now()
			c.recordTrace("virtual_pipe_write_start", item.frames, pipeWriteStarted, 0, nil)
			if err := p0cABWriteAll(c.Conn, item.data); err != nil {
				c.recordTrace("virtual_pipe_write_return", item.frames, time.Now(), time.Since(pipeWriteStarted), err)
				c.abortOnce.Do(func() {
					c.recordLifecycleTrace("virtual_carrier_abort", time.Now(), err)
					close(c.abort)
					c.captureTCPInfoOnClose()
					_ = c.Conn.Close()
				})
				return
			}
			c.recordTrace("virtual_pipe_write_return", item.frames, time.Now(), time.Since(pipeWriteStarted), nil)
			// After a late timer wake, consume the next queued frame without
			// restarting a full relative delay. Frames not yet due remain paced
			// by their original absolute propagation deadline.
			select {
			case next, ok := <-c.delivery:
				if !ok {
					return
				}
				pending = &next
				c.recordTrace("virtual_propagation_start", next.frames, time.Now(), 0, nil)
			default:
			}
		}
	}()
	go func() {
		c.workers.Wait()
		close(c.workersDone)
	}()
	return c
}

func (c *p0cABCarrier) captureTCPInfoOnClose() {
	snapshot := v27BlackholeTCPInfoSnapshot(c.Conn)
	c.tcpInfoMu.Lock()
	if c.tcpInfoAtClose == nil {
		c.tcpInfoAtClose = snapshot
	}
	c.tcpInfoMu.Unlock()
}

func (c *p0cABCarrier) phase17TCPInfoSnapshot() map[string]any {
	c.tcpInfoMu.Lock()
	snapshot := c.tcpInfoAtClose
	c.tcpInfoMu.Unlock()
	if snapshot != nil {
		return snapshot
	}
	return v27BlackholeTCPInfoSnapshot(c.Conn)
}

func (c *p0cABCarrier) enableFrameTrace(endpoint, direction string, leg int, recorder *streamPathQoEFrameRecorder) {
	c.traceEndpoint = endpoint
	c.traceDirection = direction
	c.traceLeg = leg
	c.traceRecorder = recorder
}

func (c *p0cABCarrier) recordTrace(action string, frames []StreamFrameHeader, at time.Time, duration time.Duration, err error) {
	if c.traceRecorder == nil {
		return
	}
	for _, frame := range frames {
		event := streamPathQoEFrameEvent{
			Endpoint: c.traceEndpoint, Direction: c.traceDirection, Leg: c.traceLeg,
			Action: action, Type: frame.Type.String(), Value: frame.Value, PayloadBytes: int(frame.Length),
			WriteCallMS: float64(duration.Microseconds()) / 1000,
		}
		if err != nil {
			event.Error = err.Error()
		}
		c.traceRecorder.record(event, at)
	}
}

func (c *p0cABCarrier) recordLifecycleTrace(action string, at time.Time, err error) {
	if c.traceRecorder == nil {
		return
	}
	event := streamPathQoEFrameEvent{
		Endpoint: c.traceEndpoint, Direction: c.traceDirection, Leg: c.traceLeg,
		Action: action, Type: "CARRIER",
	}
	if err != nil {
		event.Error = err.Error()
	}
	c.traceRecorder.record(event, at)
}
func (c *p0cABCarrier) wait(duration time.Duration, kind string) bool {
	if duration <= 0 {
		c.waits.record(kind, duration, 0)
		return true
	}
	started := time.Now()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-c.abort:
		return false
	case <-timer.C:
		c.waits.record(kind, duration, time.Since(started))
		return true
	}
}

func (c *p0cABCarrier) Write(payload []byte) (int, error) {
	if len(payload) > 1<<20 {
		return 0, errors.New("test Carrier write exceeds 1MiB")
	}
	if len(payload) == 0 {
		return 0, nil
	}
	request := p0cABWriteRequest{data: append([]byte(nil), payload...), serialized: make(chan error, 1)}
	c.writeMu.Lock()
	if c.closed {
		c.writeMu.Unlock()
		return 0, net.ErrClosed
	}
	request.frames = c.traceWriteParser.feed(payload)
	select {
	case <-c.abort:
		c.writeMu.Unlock()
		return 0, net.ErrClosed
	case c.tx <- request:
		c.writeMu.Unlock()
	}
	if err := <-request.serialized; err != nil {
		return 0, err
	}
	return len(payload), nil
}

func (c *p0cABCarrier) Close() error {
	c.closeOnce.Do(func() {
		c.captureTCPInfoOnClose()
		c.recordLifecycleTrace("virtual_carrier_close_begin", time.Now(), nil)
		c.writeMu.Lock()
		c.closed = true
		close(c.tx)
		c.writeMu.Unlock()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-c.workersDone:
		case <-timer.C:
			c.abortOnce.Do(func() { close(c.abort) })
			_ = c.Conn.Close()
			<-c.workersDone
		}
		c.recordLifecycleTrace("virtual_carrier_close_connection", time.Now(), nil)
		_ = c.Conn.Close()
	})
	return nil
}
func p0cABWriteAll(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		n, err := conn.Write(payload)
		if n > 0 {
			payload = payload[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func p0cABNewLegs() ([2]*p0cABCarrier, [2]*p0cABCarrier, [2]*p0cABClock) {
	clocks := [2]*p0cABClock{
		{changed: make(chan struct{})},
		{changed: make(chan struct{})},
	}
	clocks[0].rate.Store(100_000_000)
	clocks[1].rate.Store(100_000_000)
	var outbound, inbound [2]*p0cABCarrier
	for leg := range outbound {
		outbound[leg], inbound[leg] = p0cABPair(clocks[leg], 20*time.Millisecond)
	}
	return outbound, inbound, clocks
}

func p0cABStartFaultTimeline(t *testing.T, start time.Time, clocks [2]*p0cABClock) string {
	t.Helper()
	mode := os.Getenv("SMP3_P0C_AB_FAULT")
	if mode == "" {
		mode = "none"
	}
	if mode == "none" {
		return "none"
	}
	if mode != "standard" {
		t.Fatalf("SMP3_P0C_AB_FAULT must be none or standard, got %q", mode)
		return ""
	}
	go func() {
		phases := []struct {
			at   time.Duration
			rate int64
			name string
		}{
			{at: time.Second, rate: 1_000_000, name: "leg1_rate_1Mbps"},
			{at: 3 * time.Second, rate: 0, name: "leg1_service_pause_0Mbps"},
			{at: 5 * time.Second, rate: 100_000_000, name: "leg1_restored_100Mbps"},
		}
		for _, phase := range phases {
			if wait := time.Until(start.Add(phase.at)); wait > 0 {
				timer := time.NewTimer(wait)
				<-timer.C
				timer.Stop()
			}
			clocks[1].setRate(phase.rate)
			t.Logf("P0C_FAULT event=%s elapsed_ms=%d leg1_rate_mbps=%g", phase.name, time.Since(start).Milliseconds(), float64(phase.rate)/1e6)
		}
	}()
	return "t+1s leg1=1Mbps; t+3s leg1=0Mbps service pause; t+5s leg1=100Mbps"
}

func TestP0CCalibratedRawDev4Candidate(t *testing.T) {
	role := os.Getenv("SMP3_P0C_RUN_VARIANT")
	if role != "raw" && role != "core-single-leg" && role != "core-dual-leg" {
		t.Skip("explicit P0-C calibrated 512MiB Raw/Core topology comparison")
	}
	var result map[string]any
	var complete bool
	if role == "raw" {
		result, complete = p0cABRunRaw(t)
	} else {
		result, complete = p0cABRunCore(t, role)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("P0C_CALIBRATED_AB_RESULT %s", encoded)
	if !complete {
		t.Errorf("%s raw/Core 512MiB transfer did not meet the unchanged 60s completion, EOF, SHA and ledger checks", role)
	}
}

func p0cABRunRaw(t *testing.T) (map[string]any, bool) {
	t.Helper()
	outbound, inbound, clocks := p0cABNewLegs()
	defer func() {
		for leg := range outbound {
			_ = outbound[leg].Close()
			_ = inbound[leg].Close()
		}
	}()
	var accepted [2]atomic.Uint64
	var delivered [2]atomic.Uint64
	type done struct {
		leg      int
		kind     string
		sender   p0cABSenderResult
		receiver p0cABReceiverResult
	}
	events := make(chan done, 4)
	payload := p0cABPayload()
	startGate := make(chan struct{})
	for leg := range outbound {
		go func(leg int) {
			<-startGate
			hash := sha256.New()
			var written uint64
			for frame := 0; frame < p0cABFramesPerLeg; frame++ {
				n, err := outbound[leg].Write(payload)
				if n > 0 {
					_, _ = hash.Write(payload[:n])
					written += uint64(n)
				}
				if err == nil && n != len(payload) {
					err = io.ErrShortWrite
				}
				if err != nil {
					events <- done{leg: leg, kind: "writer", sender: p0cABSenderResult{bytes: written, sha256: hex.EncodeToString(hash.Sum(nil)), err: err.Error()}}
					return
				}
				accepted[leg].Add(1)
			}
			events <- done{leg: leg, kind: "writer", sender: p0cABSenderResult{bytes: written, sha256: hex.EncodeToString(hash.Sum(nil))}}
		}(leg)
		go func(leg int) {
			<-startGate
			hash := sha256.New()
			buffer := make([]byte, p0cABFrameBytes)
			var total uint64
			for total < p0cABBytesPerLeg {
				need := p0cABBytesPerLeg - total
				if need > uint64(len(buffer)) {
					need = uint64(len(buffer))
				}
				n, err := io.ReadFull(inbound[leg], buffer[:int(need)])
				if n > 0 {
					_, _ = hash.Write(buffer[:n])
					total += uint64(n)
					delivered[leg].Add(uint64(n))
				}
				if err != nil {
					events <- done{leg: leg, kind: "reader", receiver: p0cABReceiverResult{bytes: total, sha256: hex.EncodeToString(hash.Sum(nil)), termination: err.Error(), err: err.Error()}}
					return
				}
			}
			if err := outbound[leg].Close(); err != nil {
				events <- done{leg: leg, kind: "reader", receiver: p0cABReceiverResult{bytes: total, sha256: hex.EncodeToString(hash.Sum(nil)), err: err.Error()}}
				return
			}
			var extra [1]byte
			n, err := inbound[leg].Read(extra[:])
			termination := ""
			errString := ""
			if n != 0 {
				errString = "raw receiver observed bytes beyond target"
			} else if errors.Is(err, io.EOF) {
				termination = "io.EOF"
			} else if err != nil {
				termination = err.Error()
				errString = err.Error()
			} else {
				errString = "raw receiver read made no progress before EOF"
			}
			events <- done{leg: leg, kind: "reader", receiver: p0cABReceiverResult{bytes: total, sha256: hex.EncodeToString(hash.Sum(nil)), termination: termination, err: errString}}
		}(leg)
	}
	started := time.Now()
	close(startGate)
	faultTimeline := p0cABStartFaultTimeline(t, started, clocks)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.NewTimer(p0cABDeadline)
	defer deadline.Stop()
	var senders [2]p0cABSenderResult
	var receivers [2]p0cABReceiverResult
	var senderDone, receiverDone [2]bool
	remaining := 4
	timedOut, failed := false, false
	for remaining > 0 {
		select {
		case event := <-events:
			remaining--
			if event.kind == "writer" {
				senders[event.leg] = event.sender
				senderDone[event.leg] = true
				failed = failed || event.sender.err != ""
			} else {
				receivers[event.leg] = event.receiver
				receiverDone[event.leg] = true
				failed = failed || event.receiver.err != ""
			}
			if failed {
				remaining = 0
			}
		case <-ticker.C:
			t.Logf("P0C_CALIBRATED_AB_PROGRESS role=raw elapsed_ms=%d path_rate_mbps=[%g,%g] accepted_frames=%v delivered_bytes=%v",
				time.Since(started).Milliseconds(),
				float64(clocks[0].rate.Load())/1e6, float64(clocks[1].rate.Load())/1e6,
				[2]uint64{accepted[0].Load(), accepted[1].Load()},
				[2]uint64{delivered[0].Load(), delivered[1].Load()})
		case <-deadline.C:
			timedOut = true
			remaining = 0
		}
	}
	for leg := range outbound {
		_ = outbound[leg].Close()
		_ = inbound[leg].Close()
	}
	cleanupDeadline := time.NewTimer(3 * time.Second)
	defer cleanupDeadline.Stop()
	for leg := range outbound {
		if !senderDone[leg] || !receiverDone[leg] {
			select {
			case event := <-events:
				if event.kind == "writer" {
					senders[event.leg] = event.sender
					senderDone[event.leg] = true
				} else {
					receivers[event.leg] = event.receiver
					receiverDone[event.leg] = true
				}
			case <-cleanupDeadline.C:
				break
			}
		}
	}
	elapsed := time.Since(started)
	legs := make([]map[string]any, 2)
	complete := !timedOut && !failed
	var acceptedTotal, deliveredTotal uint64
	for leg := range legs {
		sourceHashMatches := senders[leg].sha256 != "" && senders[leg].sha256 == receivers[leg].sha256
		legComplete := senderDone[leg] && receiverDone[leg] && senders[leg].err == "" &&
			receivers[leg].err == "" && receivers[leg].termination == "io.EOF" &&
			senders[leg].bytes == p0cABBytesPerLeg && receivers[leg].bytes == p0cABBytesPerLeg && sourceHashMatches
		complete = complete && legComplete && accepted[leg].Load() == p0cABFramesPerLeg && delivered[leg].Load() == p0cABBytesPerLeg
		acceptedTotal += senders[leg].bytes
		deliveredTotal += receivers[leg].bytes
		legs[leg] = map[string]any{
			"leg": leg, "target_bytes": p0cABBytesPerLeg,
			"accepted_frames": accepted[leg].Load(), "accepted_bytes": senders[leg].bytes,
			"delivered_bytes": receivers[leg].bytes,
			"goodput_mbps":    float64(receivers[leg].bytes*8) / elapsed.Seconds() / 1e6,
			"source_sha256":   senders[leg].sha256, "receiver_sha256": receivers[leg].sha256,
			"sha256_match": sourceHashMatches, "termination": receivers[leg].termination,
			"sender_error": senders[leg].err, "receiver_error": receivers[leg].err,
			"serialization_waits":  outbound[leg].waits.summary("serialization"),
			"propagation_waits":    outbound[leg].waits.summary("propagation"),
			"observed_propagation": outbound[leg].waits.summary("propagation_observed"),
		}
	}
	return map[string]any{
		"role": "raw", "complete": complete, "timed_out_at_60s": timedOut,
		"elapsed_ms": elapsed.Milliseconds(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"rate_mbps_per_leg": 100, "rtt_ms": 20, "fault_timeline": faultTimeline,
		"frame_bytes": p0cABFrameBytes, "frames_per_leg": p0cABFramesPerLeg,
		"total_payload_bytes": p0cABBytesPerLeg * 2,
		"accepted_bytes":      acceptedTotal, "delivered_bytes": deliveredTotal,
		"strict_eof": complete, "sha256_match": complete, "legs": legs,
	}, complete
}

type p0cABSenderResult struct {
	bytes  uint64
	sha256 string
	err    string
}

type p0cABReceiverResult struct {
	bytes       uint64
	sha256      string
	termination string
	err         string
}

func p0cABPayload() []byte {
	payload := make([]byte, p0cABFrameBytes)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}

func p0cABWriteApplication(app net.Conn, accepted *atomic.Uint64) p0cABSenderResult {
	payload := p0cABPayload()
	hash := sha256.New()
	var written uint64
	for written < p0cABBytesPerLeg*2 {
		chunkBytes := uint64(len(payload))
		if remaining := p0cABBytesPerLeg*2 - written; remaining < chunkBytes {
			chunkBytes = remaining
		}
		offset := 0
		for offset < int(chunkBytes) {
			n, err := app.Write(payload[offset:int(chunkBytes)])
			if n > 0 {
				_, _ = hash.Write(payload[offset : offset+n])
				offset += n
				written += uint64(n)
				accepted.Store(written)
			}
			if err != nil {
				return p0cABSenderResult{bytes: written, sha256: hex.EncodeToString(hash.Sum(nil)), err: err.Error()}
			}
			if n == 0 {
				return p0cABSenderResult{bytes: written, sha256: hex.EncodeToString(hash.Sum(nil)), err: io.ErrNoProgress.Error()}
			}
		}
	}
	closeErr := app.Close()
	errString := ""
	if closeErr != nil {
		errString = closeErr.Error()
	}
	return p0cABSenderResult{bytes: written, sha256: hex.EncodeToString(hash.Sum(nil)), err: errString}
}

func p0cABReadApplication(out net.Conn, delivered *atomic.Uint64) p0cABReceiverResult {
	hash := sha256.New()
	buffer := make([]byte, 128<<10)
	var total uint64
	for {
		n, err := out.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
			total += uint64(n)
			delivered.Store(total)
		}
		if err != nil {
			termination := err.Error()
			if errors.Is(err, io.EOF) {
				termination = "io.EOF"
			}
			errString := ""
			if !errors.Is(err, io.EOF) {
				errString = err.Error()
			}
			return p0cABReceiverResult{bytes: total, sha256: hex.EncodeToString(hash.Sum(nil)), termination: termination, err: errString}
		}
	}
}

func p0cABRunCore(t *testing.T, variant string) (map[string]any, bool) {
	t.Helper()
	outbound, inbound, clocks := p0cABNewLegs()
	activeLegCount := 2
	if variant == "core-single-leg" {
		activeLegCount = 1
	}
	base := StreamConfig{
		SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic,
		ActivationMode: StreamActivationLegacy, ThresholdBytesPS: 10_000_000,
		ActivationWindow: time.Second, BandwidthMbps: []uint32{100, 100},
		ChunkSize: 64 << 10, QueueFrames: 256, MaxInflightFrames: 1024, MaxReorderFrames: 4096,
		AckInterval: 20 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond,
		RecoveryTimeout: 15 * time.Second,
	}
	if activeLegCount == 1 {
		base.BandwidthMbps = []uint32{100}
	}
	txConfig := base
	txConfig.StartupPolicy = StreamStartupFirstReady
	rxConfig := base
	rxConfig.ActivationWindow = 10 * time.Second
	rxConfig.StartupPolicy = StreamStartupPreferred
	rxConfig.StartupPreferredLeg = 0
	rxConfig.StartupGrace = 500 * time.Millisecond
	tx, app := NewStreamEngine(txConfig)
	rx, out := NewStreamEngine(rxConfig)
	for leg := 0; leg < activeLegCount; leg++ {
		txCarrier := net.Conn(&p0cdDataFrameCoalescer{Conn: outbound[leg]})
		if err := tx.AttachLeg(LegID(leg), txCarrier, nil); err != nil {
			t.Fatal(err)
		}
		if err := rx.AttachLeg(LegID(leg), inbound[leg], nil); err != nil {
			t.Fatal(err)
		}
	}

	var sourceAccepted, receiverDelivered atomic.Uint64
	type event struct {
		sender   *p0cABSenderResult
		receiver *p0cABReceiverResult
	}
	events := make(chan event, 2)
	started := time.Now()
	faultTimeline := p0cABStartFaultTimeline(t, started, clocks)
	go func() {
		result := p0cABWriteApplication(app, &sourceAccepted)
		events <- event{sender: &result}
	}()
	go func() {
		result := p0cABReadApplication(out, &receiverDelivered)
		events <- event{receiver: &result}
	}()

	var senderResult p0cABSenderResult
	var receiverResult p0cABReceiverResult
	senderDone, receiverDone := false, false
	timedOut := false
	ledgerClear := false
	deadline := time.NewTimer(p0cABDeadline)
	defer deadline.Stop()
	progress := time.NewTicker(time.Second)
	defer progress.Stop()
	drain := time.NewTicker(20 * time.Millisecond)
	defer drain.Stop()
wait:
	for {
		if senderDone && receiverDone {
			txStats, rxStats := tx.Snapshot(), rx.Snapshot()
			ledgerClear = txStats.OutstandingFrames == 0 && txStats.OutstandingBytes == 0 &&
				rxStats.RxPendingFrames == 0 && rxStats.RxPendingBytes == 0
			if ledgerClear {
				break
			}
		}
		select {
		case item := <-events:
			if item.sender != nil {
				senderResult = *item.sender
				senderDone = true
			}
			if item.receiver != nil {
				receiverResult = *item.receiver
				receiverDone = true
			}
		case <-progress.C:
			txStats, rxStats := tx.Snapshot(), rx.Snapshot()
			t.Logf("P0C_CALIBRATED_AB_PROGRESS role=%s elapsed_ms=%d path_rate_mbps=[%g,%g] app_accepted=%d rx_delivered=%d unique_ack=%d outstanding_frames=%d outstanding_bytes=%d rx_pending_frames=%d rx_pending_bytes=%d sent_by_leg=%v",
				variant, time.Since(started).Milliseconds(), float64(clocks[0].rate.Load())/1e6, float64(clocks[1].rate.Load())/1e6,
				sourceAccepted.Load(), receiverDelivered.Load(), txStats.TxAckedUsefulTotal,
				txStats.OutstandingFrames, txStats.OutstandingBytes, rxStats.RxPendingFrames,
				rxStats.RxPendingBytes, txStats.TxWrittenBytesByLeg)
		case <-drain.C:
		case <-deadline.C:
			timedOut = true
			break wait
		}
	}
	elapsed := time.Since(started)
	txStats, rxStats := tx.Snapshot(), rx.Snapshot()
	ledgerClear = txStats.OutstandingFrames == 0 && txStats.OutstandingBytes == 0 &&
		rxStats.RxPendingFrames == 0 && rxStats.RxPendingBytes == 0
	for _, carrier := range outbound {
		_ = carrier.Close()
	}
	for _, carrier := range inbound {
		_ = carrier.Close()
	}
	_ = app.Close()
	_ = out.Close()
	_ = tx.Close()
	_ = rx.Close()
	if !senderDone {
		select {
		case item := <-events:
			if item.sender != nil {
				senderResult = *item.sender
				senderDone = true
			}
			if item.receiver != nil {
				receiverResult = *item.receiver
				receiverDone = true
			}
		case <-time.After(3 * time.Second):
			senderResult.err = "writer did not exit within 3s after cleanup"
		}
	}
	if !receiverDone {
		select {
		case item := <-events:
			if item.sender != nil {
				senderResult = *item.sender
				senderDone = true
			}
			if item.receiver != nil {
				receiverResult = *item.receiver
				receiverDone = true
			}
		case <-time.After(3 * time.Second):
			receiverResult.err = "reader did not exit within 3s after cleanup"
		}
	}
	sourceHashMatches := senderResult.sha256 != "" && senderResult.sha256 == receiverResult.sha256
	complete := !timedOut && senderDone && receiverDone &&
		senderResult.err == "" && receiverResult.err == "" && receiverResult.termination == "io.EOF" &&
		senderResult.bytes == p0cABBytesPerLeg*2 && receiverResult.bytes == p0cABBytesPerLeg*2 &&
		sourceHashMatches && txStats.TxAckedUsefulTotal == p0cABBytesPerLeg*2 && ledgerClear
	legs := make([]map[string]any, 2)
	for leg := range legs {
		legs[leg] = map[string]any{
			"leg":                             leg,
			"serialization_waits":             outbound[leg].waits.summary("serialization"),
			"propagation_waits":               outbound[leg].waits.summary("propagation"),
			"observed_propagation":            outbound[leg].waits.summary("propagation_observed"),
			"reverse_ack_serialization_waits": inbound[leg].waits.summary("serialization"),
			"reverse_ack_propagation_waits":   inbound[leg].waits.summary("propagation"),
		}
	}
	return map[string]any{
		"role": variant, "complete": complete, "timed_out_at_60s": timedOut,
		"elapsed_ms": elapsed.Milliseconds(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"rate_mbps_per_leg": 100, "rtt_ms": 20, "fault_timeline": faultTimeline,
		"payload_bytes":             p0cABBytesPerLeg * 2,
		"active_leg_count":          activeLegCount,
		"configured_bandwidth_mbps": base.BandwidthMbps,
		"data_frame_write_model":    "Core DATA header and payload coalesced into one Carrier.Write",
		"app_accepted_bytes":        senderResult.bytes, "rx_delivered_bytes": receiverResult.bytes,
		"application_termination": receiverResult.termination,
		"source_sha256":           senderResult.sha256, "receiver_sha256": receiverResult.sha256,
		"sha256_match": sourceHashMatches, "sender_error": senderResult.err, "receiver_error": receiverResult.err,
		"unique_ack_bytes":   txStats.TxAckedUsefulTotal,
		"outstanding_frames": txStats.OutstandingFrames, "outstanding_bytes": txStats.OutstandingBytes,
		"rx_pending_frames": rxStats.RxPendingFrames, "rx_pending_bytes": rxStats.RxPendingBytes,
		"ledger_clear": ledgerClear, "tx_sent_bytes_by_leg": txStats.TxWrittenBytesByLeg,
		"rx_delivered_bytes_core": rxStats.RxDeliveredBytes, "legs": legs,
	}, complete
}

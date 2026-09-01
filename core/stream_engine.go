package smp3core

import (
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameTypeData     byte = byte(StreamFrameData)
	frameTypeActivate byte = byte(StreamFrameActivate)
	frameTypeAck      byte = byte(StreamFrameACK)
	frameTypeClose    byte = byte(StreamFrameClose)
	frameHeaderSize        = StreamFrameHeaderSize
	maxFramePayload        = MaxStreamFramePayload
)

var ErrStreamClosed = errors.New("multipath core closed")

type StreamSchedulerMode uint8

const (
	StreamSchedulerStatic StreamSchedulerMode = iota
	StreamSchedulerAdaptive
)

const (
	schedulerStatic   = StreamSchedulerStatic
	schedulerAdaptive = StreamSchedulerAdaptive
)

type streamLegPerf struct {
	mu           sync.Mutex
	writeBPS     float64
	writeLatency time.Duration
	ackedBPS     float64
	lastAckAt    time.Time
}

func (p *streamLegPerf) observeWrite(bytes int, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Microsecond
	}
	bps := float64(bytes) / elapsed.Seconds()
	p.mu.Lock()
	if p.writeBPS == 0 {
		p.writeBPS = bps
	} else {
		p.writeBPS = p.writeBPS*0.85 + bps*0.15
	}
	if p.writeLatency == 0 {
		p.writeLatency = elapsed
	} else {
		p.writeLatency = time.Duration(float64(p.writeLatency)*0.85 + float64(elapsed)*0.15)
	}
	p.mu.Unlock()
}

func (p *streamLegPerf) observeAck(bytes int, now time.Time) {
	if bytes <= 0 {
		return
	}
	p.mu.Lock()
	if !p.lastAckAt.IsZero() {
		elapsed := now.Sub(p.lastAckAt)
		if elapsed > 0 {
			bps := float64(bytes) / elapsed.Seconds()
			if p.ackedBPS == 0 {
				p.ackedBPS = bps
			} else {
				p.ackedBPS = p.ackedBPS*0.8 + bps*0.2
			}
		}
	}
	p.lastAckAt = now
	p.mu.Unlock()
}

func (p *streamLegPerf) snapshot() (writeBPS, ackedBPS float64, latency time.Duration) {
	p.mu.Lock()
	writeBPS, ackedBPS, latency = p.writeBPS, p.ackedBPS, p.writeLatency
	p.mu.Unlock()
	return
}

type StreamConfig struct {
	SchedulerMode        StreamSchedulerMode
	ChunkSize            int
	QueueFrames          int
	ThresholdBytesPS     uint64
	ActivationWindow     time.Duration
	BandwidthMbps        []uint32
	MaxReorderFrames     int
	MaxInflightFrames    int
	AckInterval          time.Duration
	RetransmitTimeout    time.Duration
	RecoveryTimeout      time.Duration
	OnActivate           func()
	OnLegDown            func(uint8, error)
	OnFutureAck          func(next, max, count uint64)
	NotifyPeerOnActivate bool
}

// StreamStats is a point-in-time, logical-stream view of a core. Counters are
// intentionally based on DATA accepted by SMP3, cumulative ACK retirement,
// and bytes delivered to the application; carrier wire bytes are not used by
// the adaptive controller.
type StreamStats struct {
	TxSentBytesByLeg       [2]uint64
	TxAckedUsefulByLeg     [2]uint64
	TxRetransmitBytesByLeg [2]uint64
	FrontierRescueAttempts uint64
	OutstandingFrames      int
	OutstandingBytes       uint64
	OutstandingFramesByLeg [2]int
	LastAckProgress        time.Time
	OldestOutstandingAge   time.Duration
	OldestOutstandingByLeg [2]time.Duration
	// AckFrontier identifies the oldest cumulative-ACK blocker (ackedNext).
	// With a cumulative ACK wire format, later leg1 frames cannot retire while
	// an earlier leg0 frame is missing. Adaptive health must therefore blame
	// tx_ack_stall on leg1 only when the ACK frontier itself is currently owned
	// by leg1, rather than using aggregate outstanding age alone.
	AckFrontierValid     bool
	AckFrontierLeg       uint8
	AckFrontierMultiPath bool
	AckFrontierAge       time.Duration

	RxUniqueBytesByLeg [2]uint64
	RxDeliveredBytes   uint64
	RxPendingFrames    int
	RxPendingBytes     uint64
	RxGapAge           time.Duration

	LegUp [2]bool
}

type dataFrame struct {
	seq  uint64
	data []byte
	leg  uint8
}

type txSendAttempt struct {
	record *StreamTXRecord
	rescue bool
}

type legControl struct {
	typ   byte
	value uint64
	done  chan error
}

type streamLeg struct {
	id      uint8
	conn    StreamLeg
	send    chan txSendAttempt
	rescue  chan txSendAttempt
	control chan legControl
	ackWake chan struct{}
	// ackPending is cumulative and monotonic. ACK producers only publish the
	// newest value and wake this leg's writer; a blocked carrier can therefore
	// never hold up ACK delivery on another carrier.
	ackPending atomic.Uint64
	ackForce   atomic.Bool
	onClose    func(error)
	once       sync.Once
	closed     atomic.Bool
	done       chan struct{}
	workers    sync.WaitGroup
	retired    chan struct{}
	perf       streamLegPerf
}

func (l *streamLeg) close(err error) {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.done)
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

type wireFrame struct {
	typ  byte
	seq  uint64
	data []byte
}

type StreamEngine struct {
	cfg      StreamConfig
	appConn  net.Conn
	pipeConn net.Conn

	// lifecycleMu serializes the transition into FINALIZING/DONE with leg
	// attachment. ACTIVE and graceful CLOSING still accept transport repair;
	// FINALIZING/DONE never do. Lock order is lifecycleMu -> legsMu.
	lifecycleMu sync.Mutex
	legsMu      sync.RWMutex
	legs        map[uint8]*streamLeg
	retiring    map[uint8]*streamLeg

	incoming  chan dataFrame
	done      chan struct{}
	txStopped chan struct{}
	closeErr  atomic.Value
	closeOne  sync.Once

	closing      atomic.Bool
	finalizing   atomic.Bool
	gracefulOnce sync.Once
	gracefulCh   chan struct{}

	txLedger *StreamTXLedger
	inflight chan struct{}
	retryCh  chan struct{}
	// frontierRescueCh is a coalesced O(1) wakeup for the cumulative-ACK
	// frontier. R10 uses it when ACK progress exposes a new overdue head so the
	// next repair does not have to wait for the periodic retransmit ticker.
	frontierRescueCh chan struct{}

	ingressBytes atomic.Uint64
	active       atomic.Bool
	activeCh     chan struct{}
	activateOnce sync.Once

	ackNext   atomic.Uint64
	ackForce  atomic.Bool
	ackWakeCh chan struct{}

	txSentBytes            [2]atomic.Uint64
	txAckedUseful          [2]atomic.Uint64
	txRetransmitBytes      [2]atomic.Uint64
	frontierRescueAttempts atomic.Uint64
	rxUniqueBytes          [2]atomic.Uint64
	rxDeliveredBytes       atomic.Uint64
	rxPendingFrames        atomic.Int64
	rxPendingBytes         atomic.Uint64
	rxGapSinceUnixNs       atomic.Int64
	lastAckProgressNs      atomic.Int64

	// recoveryEpoch invalidates stale "no legs" recovery timers. A temporary
	// recovery followed by a later outage must get a fresh RecoveryTimeout, not
	// inherit the deadline from the earlier outage.
	recoveryEpoch atomic.Uint64

	bufferPool sync.Pool // RX-only buffers
}

func NewStreamEngine(cfg StreamConfig) (*StreamEngine, net.Conn) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = time.Second
	}
	if cfg.MaxReorderFrames <= 0 {
		cfg.MaxReorderFrames = 2048
	}
	if cfg.MaxInflightFrames <= 0 {
		cfg.MaxInflightFrames = 1024
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = 20 * time.Millisecond
	}
	if cfg.RetransmitTimeout <= 0 {
		cfg.RetransmitTimeout = 1500 * time.Millisecond
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 15 * time.Second
	}
	appConn, pipeConn := net.Pipe()
	c := &StreamEngine{
		cfg:              cfg,
		appConn:          appConn,
		pipeConn:         pipeConn,
		legs:             make(map[uint8]*streamLeg),
		retiring:         make(map[uint8]*streamLeg),
		incoming:         make(chan dataFrame, cfg.QueueFrames*2),
		done:             make(chan struct{}),
		txStopped:        make(chan struct{}),
		gracefulCh:       make(chan struct{}),
		activeCh:         make(chan struct{}),
		txLedger:         NewStreamTXLedger(),
		inflight:         make(chan struct{}, cfg.MaxInflightFrames),
		retryCh:          make(chan struct{}, 1),
		frontierRescueCh: make(chan struct{}, 1),
		ackWakeCh:        make(chan struct{}, 1),
	}
	c.bufferPool.New = func() any {
		return make([]byte, cfg.ChunkSize)
	}
	if cfg.ThresholdBytesPS == 0 {
		c.activate()
	}
	go c.txLoop()
	go c.rxLoop()
	go c.activationLoop()
	go c.ackLoop()
	go c.retransmitLoop()
	return c, appConn
}

func (c *StreamEngine) AttachLeg(id LegID, conn StreamLeg, onClose func(error)) error {
	id8 := uint8(id)
	for {
		c.lifecycleMu.Lock()
		if c.finalizing.Load() {
			c.lifecycleMu.Unlock()
			return ErrStreamClosed
		}
		select {
		case <-c.done:
			c.lifecycleMu.Unlock()
			return ErrStreamClosed
		default:
		}

		c.legsMu.Lock()
		if _, exists := c.legs[id8]; exists {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return errors.New("duplicate multipath leg")
		}
		if retiring := c.retiring[id8]; retiring != nil {
			retired := retiring.retired
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			// A dead same-ID leg is not replaceable until both of its worker
			// goroutines have exited. Otherwise an old worker can clear inTransit
			// state that already belongs to the new generation.
			select {
			case <-retired:
				continue
			case <-c.done:
				return ErrStreamClosed
			}
		}

		leg := &streamLeg{
			id:      id8,
			conn:    conn,
			send:    make(chan txSendAttempt, c.cfg.QueueFrames),
			rescue:  make(chan txSendAttempt, 8),
			control: make(chan legControl, 8),
			ackWake: make(chan struct{}, 1),
			onClose: onClose,
			done:    make(chan struct{}),
			retired: make(chan struct{}),
		}
		leg.workers.Add(2)
		c.legs[id8] = leg
		c.legsMu.Unlock()
		c.lifecycleMu.Unlock()

		// Any successful join/rejoin cancels an older zero-leg recovery deadline.
		c.recoveryEpoch.Add(1)
		go func() {
			defer leg.workers.Done()
			c.legWriteLoop(leg)
		}()
		go func() {
			defer leg.workers.Done()
			c.legReadLoop(leg)
		}()
		c.kickRetry()
		c.forceAck()
		return nil
	}
}

// ReplaceLeg intentionally tears down one carrier while keeping the logical
// core, session ID, sequence numbers, ACK state, and outstanding records
// intact. The normal handleLegFailure callback remains the single owner of
// reconnect scheduling.
func (c *StreamEngine) ReplaceLeg(id LegID, err error) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

func (c *StreamEngine) AppConn() net.Conn     { return c.appConn }
func (c *StreamEngine) Done() <-chan struct{} { return c.done }

func (c *StreamEngine) Snapshot() StreamStats {
	now := time.Now()
	stats := StreamStats{
		TxSentBytesByLeg:       [2]uint64{c.txSentBytes[0].Load(), c.txSentBytes[1].Load()},
		TxAckedUsefulByLeg:     [2]uint64{c.txAckedUseful[0].Load(), c.txAckedUseful[1].Load()},
		TxRetransmitBytesByLeg: [2]uint64{c.txRetransmitBytes[0].Load(), c.txRetransmitBytes[1].Load()},
		FrontierRescueAttempts: c.frontierRescueAttempts.Load(),
		RxUniqueBytesByLeg:     [2]uint64{c.rxUniqueBytes[0].Load(), c.rxUniqueBytes[1].Load()},
		RxDeliveredBytes:       c.rxDeliveredBytes.Load(),
		RxPendingFrames:        int(c.rxPendingFrames.Load()),
		RxPendingBytes:         c.rxPendingBytes.Load(),
	}
	if timestamp := c.lastAckProgressNs.Load(); timestamp != 0 {
		stats.LastAckProgress = time.Unix(0, timestamp)
	}
	if gapSince := c.rxGapSinceUnixNs.Load(); gapSince != 0 {
		stats.RxGapAge = now.Sub(time.Unix(0, gapSince))
		if stats.RxGapAge < 0 {
			stats.RxGapAge = 0
		}
	}

	txSnapshot := c.txLedger.Snapshot(now)
	stats.OutstandingFrames = txSnapshot.OutstandingFrames
	stats.OutstandingBytes = txSnapshot.OutstandingBytes
	stats.OutstandingFramesByLeg = txSnapshot.OutstandingFramesByLeg
	stats.OldestOutstandingAge = txSnapshot.OldestOutstandingAge
	stats.OldestOutstandingByLeg = txSnapshot.OldestOutstandingByLeg
	stats.AckFrontierValid = txSnapshot.AckFrontierValid
	stats.AckFrontierLeg = uint8(txSnapshot.AckFrontierLeg)
	stats.AckFrontierMultiPath = txSnapshot.AckFrontierMultiPath
	stats.AckFrontierAge = txSnapshot.AckFrontierAge

	for id := range stats.LegUp {
		stats.LegUp[id] = c.hasLeg(uint8(id))
	}
	if stats.LastAckProgress.IsZero() && stats.OutstandingFrames > 0 {
		stats.LastAckProgress = now.Add(-stats.OldestOutstandingAge)
	}
	return stats
}

func (c *StreamEngine) Close() error {
	// Explicit core shutdown is not a carrier-health signal. Mark it before
	// closing the done channel so host-side carrier health cannot turn normal
	// teardown into a global cooldown.
	c.closing.Store(true)
	c.fail(io.EOF)
	return nil
}

// StartGracefulClose terminates a logical stream without discarding payload that
// has already been accepted from the local application. The TX loop is stopped,
// outstanding DATA is allowed to drain while ACK progress continues, and a
// logical CLOSE control frame is emitted before the transport legs are torn down.
//
// This is intentionally different from fail(): fail is for fatal/shutdown paths
// and may drop outstanding payload; StartGracefulClose is for ordinary routed TCP
// EOF/close where already-written bytes must reach the peer first.
func (c *StreamEngine) StartGracefulClose(err error) {
	if err == nil {
		err = io.EOF
	}
	c.closing.Store(true)
	c.gracefulOnce.Do(func() {
		close(c.gracefulCh)
		// Closing the application side wakes txLoop if the router/application has
		// not already done so. Successful net.Pipe writes are synchronous, so once
		// txLoop stops, every byte accepted from appConn is represented in TX state.
		_ = c.appConn.Close()
		go func(closeErr error) {
			select {
			case <-c.done:
				return
			case <-c.txStopped:
			}
			if drainErr := c.drainOutstandingOnClose(); drainErr != nil {
				if !errors.Is(drainErr, ErrStreamClosed) {
					c.fail(drainErr)
				}
				return
			}

			// From this point on, leg loss is part of normal final teardown and must
			// not start another reconnect/recovery cycle. CLOSE is broadcast because
			// it is the only logical EOF marker and should survive loss of one leg.
			if !c.beginFinalizing() {
				return
			}
			c.sendCloseFrame(c.txLedger.NextSequence())
			c.fail(closeErr)
		}(err)
	})
}

func (c *StreamEngine) beginFinalizing() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	select {
	case <-c.done:
		return false
	default:
	}
	c.finalizing.Store(true)
	return true
}

func (c *StreamEngine) fail(err error) {
	if err == nil {
		err = ErrStreamClosed
	}
	c.closeOne.Do(func() {
		// This lock makes FINALIZING/DONE atomic with AttachLeg(). A leg is either
		// installed before shutdown snapshots transports, or rejected after the
		// lifecycle transition; it can never appear in between.
		c.lifecycleMu.Lock()
		c.finalizing.Store(true)
		c.closeErr.Store(err)
		close(c.done)
		_ = c.pipeConn.Close()
		_ = c.appConn.Close()
		c.legsMu.RLock()
		legs := make([]*streamLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		c.lifecycleMu.Unlock()
		for _, leg := range legs {
			leg.close(err)
		}
	})
}

func (c *StreamEngine) getBuffer() []byte { return c.bufferPool.Get().([]byte) }
func (c *StreamEngine) putBuffer(buffer []byte) {
	if cap(buffer) < c.cfg.ChunkSize {
		return
	}
	c.bufferPool.Put(buffer[:c.cfg.ChunkSize])
}

func (c *StreamEngine) acquireInflight() bool {
	select {
	case c.inflight <- struct{}{}:
		return true
	case <-c.gracefulCh:
		return false
	case <-c.done:
		return false
	}
}

func (c *StreamEngine) releaseInflight(count int) {
	for range count {
		select {
		case <-c.inflight:
		default:
			return
		}
	}
}

func (c *StreamEngine) txLoop() {
	defer close(c.txStopped)
	for {
		if !c.acquireInflight() {
			return
		}
		buffer := make([]byte, c.cfg.ChunkSize)
		n, err := c.pipeConn.Read(buffer)
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			record := c.txLedger.Add(buffer[:n], time.Now())
			if enqueueErr := c.enqueueRecord(record, -1); enqueueErr != nil {
				c.fail(enqueueErr)
				return
			}
		} else {
			c.releaseInflight(1)
		}
		if err != nil {
			// A local application/router close is not a transport failure. Preserve
			// every frame already accepted from appConn and finish the stream only
			// after cumulative ACKs have drained the outstanding window.
			c.StartGracefulClose(err)
			return
		}
	}
}

// drainOutstandingOnClose has no fixed total deadline. A large/slow tail is
// allowed to take arbitrarily long as long as cumulative ACKs keep making
// progress. RecoveryTimeout is instead used as a *stall* timeout: if no ACK/TX
// retirement progress occurs for that long, the graceful close is considered
// wedged and the logical stream fails. This avoids alpha2/alpha2.1's 5-second
// absolute cap truncating large responses that were still draining normally.
func (c *StreamEngine) drainOutstandingOnClose() error {
	stallTimeout := c.cfg.RecoveryTimeout
	if stallTimeout <= 0 {
		stallTimeout = 15 * time.Second
	}
	if stallTimeout < 100*time.Millisecond {
		stallTimeout = 100 * time.Millisecond
	}

	// A timer channel is only a wake-up mechanism here, never proof that progress
	// actually stalled. Under scheduler pressure an ACK can retire DATA after the
	// timer becomes readable but before this goroutine runs; treating the stale
	// timer event as authoritative caused false graceful-drain failures. Poll the
	// protected ACK/outstanding state first and measure the stall interval from the
	// last state we observed making progress.
	pollInterval := 10 * time.Millisecond
	if quarter := stallTimeout / 4; quarter > 0 && quarter < pollInterval {
		pollInterval = quarter
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastProgress := time.Now()
	lastRemaining := -1
	var lastAck uint64
	for {
		progress := c.txLedger.ProgressSnapshot()
		remaining := progress.Outstanding
		acked := progress.AckedNext
		if remaining == 0 {
			return nil
		}

		now := time.Now()
		if lastRemaining < 0 || remaining < lastRemaining || acked > lastAck {
			lastProgress = now
		}
		lastRemaining = remaining
		lastAck = acked
		if now.Sub(lastProgress) >= stallTimeout {
			return errors.New("multipath graceful drain stalled waiting for ACK progress")
		}

		select {
		case <-c.done:
			return ErrStreamClosed
		case <-ticker.C:
		}
	}
}

func streamActivationRateBytesPS(txDelta, rxDelta uint64, elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	txRate := uint64(float64(txDelta) / elapsed.Seconds())
	rxRate := uint64(float64(rxDelta) / elapsed.Seconds())
	if rxRate > txRate {
		return rxRate
	}
	return txRate
}

func streamActivationEligible(txDelta, rxDelta uint64, elapsed time.Duration, thresholdBytesPS uint64) bool {
	return streamActivationRateBytesPS(txDelta, rxDelta, elapsed) >= thresholdBytesPS
}

func (c *StreamEngine) activationLoop() {
	if c.active.Load() {
		return
	}
	interval := c.cfg.ActivationWindow / 10
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	windowStart := time.Now()
	txWindowBase := c.ingressBytes.Load()
	rxWindowBase := c.rxDeliveredBytes.Load()
	var queueHighSince time.Time
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			if c.active.Load() {
				return
			}
			// A logical stream that has already entered graceful CLOSING and has
			// no TX tail left must not wake a lazy booster from stale ingress/queue
			// history. If outstanding DATA remains, activation is still allowed so
			// transport repair can finish the graceful drain.
			if c.closing.Load() && !c.hasOutstanding() {
				return
			}
			if now.Sub(windowStart) >= c.cfg.ActivationWindow {
				txBytesNow := c.ingressBytes.Load()
				rxBytesNow := c.rxDeliveredBytes.Load()
				elapsed := now.Sub(windowStart)
				if streamActivationEligible(txBytesNow-txWindowBase, rxBytesNow-rxWindowBase, elapsed, c.cfg.ThresholdBytesPS) {
					c.activate()
					return
				}
				windowStart = now
				txWindowBase = txBytesNow
				rxWindowBase = rxBytesNow
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			if len(primary.send)*5 >= cap(primary.send)*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					c.activate()
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func (c *StreamEngine) activate() {
	c.activateOnce.Do(func() {
		c.active.Store(true)
		close(c.activeCh)
		if c.cfg.NotifyPeerOnActivate {
			go c.notifyPeerActivate()
		}
		if c.cfg.OnActivate != nil {
			c.cfg.OnActivate()
		}
	})
}

func (c *StreamEngine) notifyPeerActivate() { c.sendControlFrame(frameTypeActivate, 0) }

func (c *StreamEngine) getLeg(id uint8) *streamLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *StreamEngine) hasLeg(id uint8) bool { return c.getLeg(id) != nil }

func (c *StreamEngine) availableLegs() []*streamLeg {
	c.legsMu.RLock()
	legs := make([]*streamLeg, 0, len(c.legs))
	for _, leg := range c.legs {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *StreamEngine) legCount() int {
	c.legsMu.RLock()
	n := len(c.legs)
	c.legsMu.RUnlock()
	return n
}

func (c *StreamEngine) hasOutstanding() bool {
	return c.txLedger.HasOutstanding()
}

func (c *StreamEngine) weightFor(id uint8) uint32 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *StreamEngine) effectiveSchedulerWeight(leg *streamLeg) float64 {
	base := float64(c.weightFor(leg.id))
	if c.cfg.SchedulerMode != StreamSchedulerAdaptive {
		return base
	}
	writeBPS, ackedBPS, latency := leg.perf.snapshot()
	observed := ackedBPS
	if observed <= 0 {
		observed = writeBPS
	}
	if observed > 0 {
		observedMbps := observed * 8 / 1e6
		if observedMbps < 1 {
			observedMbps = 1
		}
		if base > 1 {
			// Static bandwidth is a prior; useful ACK/write feedback gradually
			// reshapes it without letting one short sample dominate scheduling.
			base = math.Sqrt(base * observedMbps)
		} else {
			base = observedMbps
		}
	}
	if latency > 0 {
		// A leg that takes longer to drain a frame should receive fewer early
		// sequence numbers. This directly reduces slow-path HOL pressure before
		// frontier rescue is needed.
		penaltyBase := c.cfg.RetransmitTimeout / 8
		if penaltyBase < 20*time.Millisecond {
			penaltyBase = 20 * time.Millisecond
		}
		base /= 1 + float64(latency)/float64(penaltyBase)
	}
	if base < 0.1 {
		base = 0.1
	}
	return base
}

func (c *StreamEngine) chooseLeg(active bool, avoid int16) *streamLeg {
	if !active {
		if primary := c.getLeg(0); primary != nil && int16(primary.id) != avoid {
			return primary
		}
	}
	legs := c.availableLegs()
	var best *streamLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		if len(legs) > 1 && int16(leg.id) == avoid {
			continue
		}
		weight := c.effectiveSchedulerWeight(leg)
		score := float64(len(leg.send)+1) / weight
		if best == nil || score < bestScore {
			best = leg
			bestScore = score
		}
	}
	if best == nil && len(legs) > 0 {
		return legs[0]
	}
	return best
}

func (c *StreamEngine) markTransit(record *StreamTXRecord, legID uint8) bool {
	return c.txLedger.MarkTransit(record, LegID(legID), time.Now())
}

func (c *StreamEngine) markRescueTransit(record *StreamTXRecord, legID uint8) (time.Time, bool) {
	return c.txLedger.MarkRescueTransit(record, LegID(legID), time.Now())
}

func (c *StreamEngine) markRescueQueued(record *StreamTXRecord, legID uint8, started time.Time) bool {
	if !c.txLedger.MarkRescueQueued(record, LegID(legID), started) {
		return false
	}
	c.frontierRescueAttempts.Add(1)
	return true
}

func (c *StreamEngine) clearAttempt(record *StreamTXRecord, legID uint8, rescue bool) {
	c.txLedger.ClearAttempt(record, LegID(legID), rescue)
}

func (c *StreamEngine) attemptCurrent(record *StreamTXRecord, legID uint8, rescue bool) bool {
	return c.txLedger.AttemptCurrent(record, LegID(legID), rescue)
}

func (c *StreamEngine) markAttemptSent(record *StreamTXRecord, legID uint8, rescue bool) {
	result := c.txLedger.MarkAttemptSent(record, LegID(legID), rescue, time.Now())
	if !result.Applied || legID >= uint8(len(c.txSentBytes)) {
		return
	}
	if result.Retransmit {
		c.txRetransmitBytes[legID].Add(uint64(result.Bytes))
	}
	c.txSentBytes[legID].Add(uint64(result.Bytes))
}

func (c *StreamEngine) isOutstanding(record *StreamTXRecord) bool {
	return c.txLedger.IsOutstanding(record)
}

func (c *StreamEngine) tryQueue(leg *streamLeg, record *StreamTXRecord) bool {
	if leg.closed.Load() {
		return false
	}
	if !c.markTransit(record, leg.id) {
		return !c.isOutstanding(record)
	}
	attempt := txSendAttempt{record: record}
	select {
	case <-leg.done:
		c.clearAttempt(record, leg.id, false)
		return false
	case leg.send <- attempt:
		// A buffered send can win the select while the leg is closing. Recheck
		// after enqueue so a record never remains pinned to a retired writer.
		if leg.closed.Load() {
			c.clearAttempt(record, leg.id, false)
			return false
		}
		return true
	default:
		c.clearAttempt(record, leg.id, false)
		return false
	}
}

func (c *StreamEngine) tryQueueRescue(leg *streamLeg, record *StreamTXRecord) bool {
	if leg == nil || leg.closed.Load() {
		return false
	}
	started, marked := c.markRescueTransit(record, leg.id)
	if !marked {
		already := !c.txLedger.IsOutstanding(record) || c.txLedger.RescueInTransit(record)
		return already
	}
	attempt := txSendAttempt{record: record, rescue: true}
	select {
	case <-leg.done:
		c.clearAttempt(record, leg.id, true)
		return false
	case leg.rescue <- attempt:
		// Commit cooldown/diagnostic state only after the priority queue accepted
		// this attempt. A full queue therefore remains immediately retryable.
		c.markRescueQueued(record, leg.id, started)
		if leg.closed.Load() {
			c.clearAttempt(record, leg.id, true)
			return false
		}
		return true
	default:
		c.clearAttempt(record, leg.id, true)
		return false
	}
}

func (c *StreamEngine) enqueueRecord(record *StreamTXRecord, avoid int16) error {
	for {
		select {
		case <-c.done:
			return ErrStreamClosed
		default:
		}
		if !c.isOutstanding(record) {
			return nil
		}
		active := c.active.Load()
		leg := c.chooseLeg(active, avoid)
		if leg == nil {
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.retryCh:
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if c.tryQueue(leg, record) {
			return nil
		}
		if !active && leg != nil && leg.id == 0 && cap(leg.send) > 0 && len(leg.send) >= cap(leg.send) {
			// A saturated preferred queue is direct evidence that the single-path data
			// plane cannot currently absorb ingress. Do not wait an entire activation
			// sampling window before waking the booster; this also improves short burst
			// and speed-test flows while preserving lazy leg1 startup for light traffic.
			c.activate()
			active = true
		}
		if active {
			for _, other := range c.availableLegs() {
				if other == leg || (c.legCount() > 1 && int16(other.id) == avoid) {
					continue
				}
				if c.tryQueue(other, record) {
					return nil
				}
			}
		}
		select {
		case <-c.done:
			return ErrStreamClosed
		case <-time.After(time.Millisecond):
		}
	}
}

// tryEnqueueRecord makes one non-blocking ordinary retry scheduling pass.
// Application TX still uses enqueueRecord because it must apply backpressure
// to the application. The retransmit scheduler, however, must not wait for a
// retry queue: frontier rescue is an independent repair decision and must be
// evaluated in the same scheduler turn even when ordinary retry capacity is
// temporarily unavailable.
func (c *StreamEngine) tryEnqueueRecord(record *StreamTXRecord, avoid int16) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	if !c.isOutstanding(record) {
		return true
	}
	active := c.active.Load()
	leg := c.chooseLeg(active, avoid)
	if leg == nil {
		return false
	}
	if c.tryQueue(leg, record) {
		return true
	}
	if !active && leg.id == 0 && cap(leg.send) > 0 && len(leg.send) >= cap(leg.send) {
		c.activate()
		active = true
	}
	if active {
		for _, other := range c.availableLegs() {
			if other == leg || (c.legCount() > 1 && int16(other.id) == avoid) {
				continue
			}
			if c.tryQueue(other, record) {
				return true
			}
		}
	}
	return false
}

func (c *StreamEngine) enqueueRescue(record *StreamTXRecord, avoid int16) error {
	select {
	case <-c.done:
		return ErrStreamClosed
	default:
	}
	if !c.isOutstanding(record) {
		return nil
	}
	legs := c.availableLegs()
	if len(legs) < 2 {
		return nil
	}
	leg := c.chooseLeg(true, avoid)
	if leg == nil || int16(leg.id) == avoid {
		return nil
	}
	_ = c.tryQueueRescue(leg, record)
	return nil
}

func (c *StreamEngine) legWriteLoop(leg *streamLeg) {
	var ackSent uint64

	writeControl := func(control legControl) bool {
		header := encodeControlFrame(control.typ, control.value)
		err := writeAll(leg.conn, header[:])
		if control.done != nil {
			control.done <- err
		}
		if err != nil {
			c.handleLegFailure(leg, err)
			return false
		}
		return true
	}

	writePendingAck := func() bool {
		next := leg.ackPending.Load()
		force := leg.ackForce.Swap(false)
		if next == 0 || (next <= ackSent && !force) {
			return true
		}
		if !writeControl(legControl{typ: frameTypeAck, value: next}) {
			return false
		}
		if next > ackSent {
			ackSent = next
		}
		return true
	}

	writeAttempt := func(attempt txSendAttempt) bool {
		record := attempt.record
		if record == nil {
			return true
		}
		if !c.attemptCurrent(record, leg.id, attempt.rescue) {
			return true
		}
		writeStarted := time.Now()
		if err := writeDataFrame(leg.conn, dataFrame{seq: record.Sequence(), data: record.Payload()}); err != nil {
			c.clearAttempt(record, leg.id, attempt.rescue)
			c.handleLegFailure(leg, err)
			return false
		}
		leg.perf.observeWrite(len(record.Payload()), time.Since(writeStarted))
		c.markAttemptSent(record, leg.id, attempt.rescue)
		return true
	}

	for {
		// ACKs are cumulative, cheap to coalesce, and latency-sensitive. Publish the
		// latest ACK before selecting another DATA frame. This is priority at frame
		// boundaries; an already-started stream write cannot be preempted safely.
		if !writePendingAck() {
			return
		}

		// Explicit control is next. Frontier-rescue DATA is also a priority lane:
		// it exists specifically to repair a cumulative-ACK hole and must not wait
		// behind an ordinary per-leg DATA queue that may already be backpressured.
		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case <-leg.ackWake:
			continue
		case control := <-leg.control:
			if !writeControl(control) {
				return
			}
			continue
		default:
		}
		select {
		case rescue := <-leg.rescue:
			if !writeAttempt(rescue) {
				return
			}
			continue
		default:
		}

		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case <-leg.ackWake:
			continue
		case control := <-leg.control:
			if !writeControl(control) {
				return
			}
		case rescue := <-leg.rescue:
			if !writeAttempt(rescue) {
				return
			}
		case attempt := <-leg.send:
			// ACK/control/rescue wakes can race with ordinary DATA becoming selectable.
			// Recheck those lanes before starting a normal DATA frame.
			if !writePendingAck() {
				return
			}
			select {
			case control := <-leg.control:
				if !writeControl(control) {
					return
				}
			default:
			}
			select {
			case rescue := <-leg.rescue:
				if !writeAttempt(rescue) {
					return
				}
			default:
			}
			if !writeAttempt(attempt) {
				return
			}
		}
	}
}

func (c *StreamEngine) legReadLoop(leg *streamLeg) {
	for {
		frame, err := readWireFrame(leg.conn, c)
		if err != nil {
			c.handleLegFailure(leg, err)
			return
		}
		switch frame.typ {
		case frameTypeActivate:
			c.activate()
		case frameTypeAck:
			if err := c.handleAck(frame.seq); err != nil {
				c.handleLegFailure(leg, err)
				return
			}
		case frameTypeClose:
			// CLOSE carries the sender's final next-sequence value. The sender only
			// emits it after our cumulative ACK has retired all of those DATA frames,
			// so a value beyond ackNext is inconsistent and must not truncate data.
			if frame.seq > c.ackNext.Load() {
				c.handleLegFailure(leg, errors.New("multipath CLOSE beyond received sequence"))
				return
			}
			c.StartGracefulClose(io.EOF)
			return
		case frameTypeData:
			select {
			case c.incoming <- dataFrame{seq: frame.seq, data: frame.data, leg: leg.id}:
			case <-c.done:
				c.putBuffer(frame.data)
				return
			}
		}
	}
}

func (c *StreamEngine) handleLegFailure(leg *streamLeg, err error) {
	leg.close(err)
	removed := false
	c.legsMu.Lock()
	if current := c.legs[leg.id]; current == leg {
		delete(c.legs, leg.id)
		c.retiring[leg.id] = leg
		removed = true
	}
	remaining := len(c.legs)
	c.legsMu.Unlock()
	if !removed {
		return
	}

	// Any record queued/writing on the dead leg must become retryable. A frame
	// that actually arrived before the failure is harmlessly deduplicated by seq.
	c.txLedger.InvalidateLeg(LegID(leg.id))

	// Do not allow a same-ID replacement to attach until *both* old worker
	// goroutines have exited. The old read/write loops use only an 8-bit leg ID
	// in TX bookkeeping, so overlap between generations would let a stale worker
	// clear state belonging to the replacement.
	go func() {
		leg.workers.Wait()
		c.legsMu.Lock()
		if current := c.retiring[leg.id]; current == leg {
			delete(c.retiring, leg.id)
		}
		c.legsMu.Unlock()
		close(leg.retired)
		c.kickRetry()
	}()

	// If the logical core is already closing, this is ordinary transport
	// teardown. Do not re-activate, emit leg-down callbacks, or start recovery
	// timers from the read/write goroutines racing with Close/fail.
	select {
	case <-c.done:
		return
	default:
	}
	if c.finalizing.Load() {
		return
	}
	// Graceful CLOSING still permits transport repair while DATA remains
	// outstanding. Once the TX tail is fully ACKed, however, a racing carrier EOF
	// is ordinary teardown and must not wake the booster, schedule reconnects, or
	// start a no-leg recovery timer.
	if c.closing.Load() && !c.hasOutstanding() {
		return
	}

	// Primary loss before the threshold is an emergency activation: leg 1 is
	// allowed to come up immediately so the logical stream has a chance to live.
	if leg.id == 0 && !c.active.Load() {
		c.activate()
	}
	if c.cfg.OnLegDown != nil {
		go c.cfg.OnLegDown(leg.id, err)
	}
	c.kickRetry()
	c.forceAck()
	if remaining == 0 {
		epoch := c.recoveryEpoch.Add(1)
		go c.failIfNoLegRecovers(epoch)
	}
}

func (c *StreamEngine) failIfNoLegRecovers(epoch uint64) {
	timer := time.NewTimer(c.cfg.RecoveryTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return
	case <-timer.C:
		if c.recoveryEpoch.Load() == epoch && c.legCount() == 0 {
			c.fail(errors.New("multipath recovery timeout: no transport leg available"))
		}
	}
}

func (c *StreamEngine) kickRetry() {
	select {
	case c.retryCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) kickFrontierRescue() {
	select {
	case c.frontierRescueCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) kickAck() {
	select {
	case c.ackWakeCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) forceAck() {
	c.ackForce.Store(true)
	c.kickAck()
}

func (c *StreamEngine) retransmitLoop() {
	interval := c.cfg.RetransmitTimeout / 4
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.scheduleRetries()
			c.scheduleFrontierRescue()
		case <-c.retryCh:
			c.scheduleRetries()
			c.scheduleFrontierRescue()
		case <-c.frontierRescueCh:
			// ACK progress only needs an O(1) check of outstanding[ackedNext].
			// Do not rescan the entire outstanding map on every cumulative ACK.
			c.scheduleFrontierRescue()
		}
	}
}

type retryCandidate struct {
	record *StreamTXRecord
	avoid  int16
}

func streamLegAvailability(legs []*streamLeg) StreamLegAvailability {
	var available StreamLegAvailability
	for _, leg := range legs {
		if leg.id < uint8(len(available)) {
			available[leg.id] = true
		}
	}
	return available
}

func (c *StreamEngine) scheduleRetries() {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return
	}
	for _, candidate := range c.txLedger.PlanRetries(streamLegAvailability(legs)) {
		// A full ordinary retry queue is deferred to the next scheduler turn.
		// Do not let it delay frontier rescue below.
		_ = c.tryEnqueueRecord(candidate.Record, candidate.Avoid)
	}
}

// scheduleFrontierRescue performs the R10 ACK-paced repair check. SMP3 v4 only
// has a cumulative ACK, so the sender can prove exactly one blocking sequence:
// outstanding[ackedNext]. We intentionally rescue only that record instead of
// guessing at later frames that may already be buffered by the receiver.
//
// The periodic retransmit loop remains a safety net for a frontier that never
// gets any ACK activity. The fast path is handleAck(): whenever cumulative ACK
// progress exposes a new head, it coalesces a frontierRescueCh wake so an old,
// already-overdue blocker can be repaired immediately rather than waiting up to
// another retransmit-loop interval.
func (c *StreamEngine) scheduleFrontierRescue() {
	now := time.Now()
	var live StreamLegAvailability
	live[0] = c.hasLeg(0)
	live[1] = c.hasLeg(1)
	plan := DecideStreamTXFrontierRepair(c.txLedger.FrontierCandidate(now, c.cfg.RetransmitTimeout), live)
	switch plan.Action {
	case StreamTXFrontierNeedActivation:
		if !c.active.Load() {
			c.activate()
		}
	case StreamTXFrontierRescue:
		if err := c.enqueueRescue(plan.Record, plan.Avoid); err != nil && !errors.Is(err, ErrStreamClosed) {
			c.fail(err)
		}
	}
}

func (c *StreamEngine) handleAck(next uint64) error {
	result := c.txLedger.ApplyACK(next, time.Now())
	if result.Disposition == StreamTXACKFuture {
		// This ACK cannot safely prove delivery of any local payload. A live
		// alpha2 test observed such an anomaly around transport rejoin; treating
		// it as fatal caused an otherwise recoverable leg to enter a reconnect
		// storm. Ignore it without advancing TX state and let valid cumulative
		// ACKs/retransmission drive progress.
		count := result.FutureCount
		if c.cfg.OnFutureAck != nil && (count == 1 || count&(count-1) == 0) {
			c.cfg.OnFutureAck(next, result.Max, count)
		}
		return nil
	}
	if result.Disposition == StreamTXACKNoProgress {
		return nil
	}
	if result.Released > 0 {
		now := result.LastACKProgress
		c.lastAckProgressNs.Store(now.UnixNano())
		for legID, bytes := range result.AckedBytesByLeg {
			if bytes <= 0 {
				continue
			}
			c.txAckedUseful[legID].Add(bytes)
			if leg := c.getLeg(uint8(legID)); leg != nil {
				leg.perf.observeAck(int(bytes), now)
			}
		}
	}
	c.releaseInflight(result.Released)
	// R10: cumulative ACK progress can expose another already-overdue blocker.
	// Wake the O(1) frontier checker immediately instead of waiting for the next
	// retransmit ticker. Duplicate/non-progress ACKs returned above and do not
	// create repair work.
	c.kickFrontierRescue()
	return nil
}

func (c *StreamEngine) requestAck(next uint64) {
	for {
		old := c.ackNext.Load()
		if next <= old || c.ackNext.CompareAndSwap(old, next) {
			break
		}
	}
	c.kickAck()
}

func (c *StreamEngine) ackLoop() {
	ticker := time.NewTicker(c.cfg.AckInterval)
	defer ticker.Stop()
	var sent uint64
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		case <-c.ackWakeCh:
			// Wake quickly after recovery/leg join, but coalescing still happens
			// because ackNext is cumulative.
		}
		next := c.ackNext.Load()
		force := c.ackForce.Swap(false)
		if next == 0 || (next <= sent && !force) || c.legCount() == 0 {
			if force && c.legCount() == 0 {
				c.ackForce.Store(true)
			}
			continue
		}
		if c.sendAckFrame(next, force) {
			sent = next
		} else if force {
			c.ackForce.Store(true)
		}
	}
}

func (c *StreamEngine) queueAck(leg *streamLeg, value uint64, force bool) bool {
	if leg == nil || leg.closed.Load() {
		return false
	}
	updated := false
	for {
		old := leg.ackPending.Load()
		if value <= old {
			break
		}
		if leg.ackPending.CompareAndSwap(old, value) {
			updated = true
			break
		}
	}
	if force {
		leg.ackForce.Store(true)
	}
	select {
	case <-leg.done:
		return false
	default:
	}
	if updated || force {
		select {
		case leg.ackWake <- struct{}{}:
		default:
		}
	}
	return true
}

func (c *StreamEngine) sendAckFrame(value uint64, force bool) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}
	// Never synchronously write ACKs while iterating over carriers. Each leg has
	// its own writer and cumulative ACK slot, so one slow/black-holed transport
	// cannot head-of-line block ACK progress on a healthy transport.
	scheduled := false
	for _, leg := range legs {
		if c.queueAck(leg, value, force) {
			scheduled = true
		}
	}
	return scheduled
}

func (c *StreamEngine) queueControl(leg *streamLeg, typ byte, value uint64, wait bool) (chan error, bool) {
	if leg == nil || leg.closed.Load() {
		return nil, false
	}
	var done chan error
	if wait {
		done = make(chan error, 1)
	}
	control := legControl{typ: typ, value: value, done: done}
	select {
	case <-c.done:
		return nil, false
	case <-leg.done:
		return nil, false
	case leg.control <- control:
		return done, true
	}
}

func (c *StreamEngine) sendCloseFrame(value uint64) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}

	// Queue CLOSE on every live leg first. Then wait only until one carrier has
	// actually written it; a blocked carrier must not prevent a healthy carrier
	// from delivering the logical EOF before fail() tears transports down.
	waiters := make([]struct {
		leg  *streamLeg
		done chan error
	}, 0, len(legs))
	for _, leg := range legs {
		if done, ok := c.queueControl(leg, frameTypeClose, value, true); ok {
			waiters = append(waiters, struct {
				leg  *streamLeg
				done chan error
			}{leg: leg, done: done})
		}
	}
	if len(waiters) == 0 {
		return false
	}

	result := make(chan bool, len(waiters))
	for _, waiter := range waiters {
		go func(leg *streamLeg, done chan error) {
			select {
			case err := <-done:
				result <- err == nil
			case <-leg.done:
				result <- false
			case <-c.done:
				result <- false
			}
		}(waiter.leg, waiter.done)
	}
	for range waiters {
		if <-result {
			return true
		}
	}
	return false
}

func (c *StreamEngine) sendControlFrame(typ byte, value uint64) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}
	// Prefer leg 0 for one-shot control traffic, otherwise use any surviving leg.
	// The per-leg writer serializes this ahead of queued DATA at the next frame
	// boundary, without taking a global/cross-leg write lock.
	leg := c.getLeg(0)
	if leg == nil {
		leg = legs[0]
	}
	_, ok := c.queueControl(leg, typ, value, false)
	return ok
}

func (c *StreamEngine) syncRXStatsFromWindow(window *StreamRXWindow) {
	c.rxPendingFrames.Store(int64(window.PendingFrames()))
	c.rxPendingBytes.Store(window.PendingBytes())
	gapSince := window.GapSince()
	if gapSince.IsZero() {
		c.rxGapSinceUnixNs.Store(0)
	} else {
		c.rxGapSinceUnixNs.Store(gapSince.UnixNano())
	}
}

func (c *StreamEngine) rxLoop() {
	window := NewStreamRXWindow(c.cfg.MaxReorderFrames)
	defer func() {
		for _, frame := range window.DrainPending() {
			c.putBuffer(frame.Payload)
		}
		c.syncRXStatsFromWindow(window)
	}()
	for {
		select {
		case <-c.done:
			return
		case frame, open := <-c.incoming:
			if !open {
				return
			}
			rxFrame := StreamRXFrame{
				Sequence: frame.seq,
				Leg:      LegID(frame.leg),
				Payload:  frame.data,
			}
			disposition, err := window.Insert(rxFrame, time.Now())
			if err != nil {
				c.putBuffer(frame.data)
				c.fail(err)
				return
			}
			switch disposition {
			case StreamRXDuplicate:
				c.putBuffer(frame.data)
				c.forceAck()
				continue
			case StreamRXBufferedDuplicate:
				c.putBuffer(frame.data)
				continue
			case StreamRXBuffered:
				if frame.leg < uint8(len(c.rxUniqueBytes)) {
					c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
				}
				c.syncRXStatsFromWindow(window)
				continue
			}
			if frame.leg < uint8(len(c.rxUniqueBytes)) {
				c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
			}
			for {
				if err := writeAll(c.pipeConn, rxFrame.Payload); err != nil {
					c.putBuffer(rxFrame.Payload)
					if !c.closing.Load() {
						c.fail(err)
					}
					return
				}
				c.rxDeliveredBytes.Add(uint64(len(rxFrame.Payload)))
				c.putBuffer(rxFrame.Payload)
				window.CommitReady(time.Now())
				next, exists := window.PopContiguous()
				c.syncRXStatsFromWindow(window)
				if !exists {
					break
				}
				rxFrame = next
			}
			c.requestAck(window.Expected())
		}
	}
}

func encodeControlFrame(typ byte, value uint64) [frameHeaderSize]byte {
	var header [frameHeaderSize]byte
	_ = EncodeStreamFrameHeader(header[:], StreamFrameHeader{
		Type:  StreamFrameType(typ),
		Value: value,
	})
	return header
}

func writeDataFrame(conn StreamLeg, frame dataFrame) error {
	var header [frameHeaderSize]byte
	if err := EncodeStreamFrameHeader(header[:], StreamFrameHeader{
		Type:   StreamFrameData,
		Value:  frame.seq,
		Length: uint32(len(frame.data)),
	}); err != nil {
		return err
	}
	buffers := net.Buffers{header[:], frame.data}
	_, err := buffers.WriteTo(conn)
	return err
}

func readWireFrame(conn StreamLeg, core *StreamEngine) (wireFrame, error) {
	header, err := ReadStreamFrameHeader(conn)
	if err != nil {
		return wireFrame{}, err
	}
	frame := wireFrame{typ: byte(header.Type), seq: header.Value}
	if header.Type != StreamFrameData {
		return frame, nil
	}
	length := int(header.Length)
	if length <= core.cfg.ChunkSize {
		frame.data = core.getBuffer()[:length]
	} else {
		frame.data = make([]byte, length)
	}
	if _, err := io.ReadFull(conn, frame.data); err != nil {
		core.putBuffer(frame.data)
		return wireFrame{}, err
	}
	return frame, nil
}

func writeAll(writer io.Writer, buffer []byte) error {
	for len(buffer) > 0 {
		n, err := writer.Write(buffer)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		buffer = buffer[n:]
	}
	return nil
}

// Activate forces the stream booster state and is intentionally a small host callback seam.
func (c *StreamEngine) Activate() { c.activate() }

// HasLeg reports whether a concrete leg generation is currently attached.
func (c *StreamEngine) HasLeg(id LegID) bool { return c.hasLeg(uint8(id)) }

// LegCount reports the number of currently attached legs.
func (c *StreamEngine) LegCount() int { return c.legCount() }

// Active reports whether the stream booster has been activated.
func (c *StreamEngine) Active() bool { return c.active.Load() }

// Closing reports whether application close has started while allowing live
// carrier repair to continue during the graceful drain phase.
func (c *StreamEngine) Closing() bool { return c.closing.Load() }

// Finalizing reports whether the logical stream has entered terminal teardown.
func (c *StreamEngine) Finalizing() bool { return c.finalizing.Load() }

// BeginFinalizingForTest exposes the existing lifecycle transition to package
// tests without exposing the lifecycle mutex or transport registry.
func (c *StreamEngine) BeginFinalizingForTest() bool { return c.beginFinalizing() }

// SetClosingForTest is reserved for compatibility tests that exercise repair
// behavior while a graceful close is in progress.
func (c *StreamEngine) SetClosingForTest(closing bool) { c.closing.Store(closing) }

// TXLedger returns the engine-owned ledger for the legacy semantic test
// adapter. Production callers should use Snapshot and not mutate the ledger.
func (c *StreamEngine) TXLedger() *StreamTXLedger { return c.txLedger }

// FrontierRescueAttempts returns the current repair-attempt counter.
func (c *StreamEngine) FrontierRescueAttempts() uint64 { return c.frontierRescueAttempts.Load() }

// SetActiveForTest is reserved for compatibility tests that exercise scheduler
// transitions without a real activation threshold.
func (c *StreamEngine) SetActiveForTest(active bool) { c.active.Store(active) }

// InjectFrameForTest feeds a decoded DATA frame into the existing RX worker.
// It is used only by package-level semantic tests; wire decoding remains in the engine.
func (c *StreamEngine) InjectFrameForTest(sequence uint64, payload []byte, leg LegID) {
	c.incoming <- dataFrame{seq: sequence, data: payload, leg: uint8(leg)}
}

// CloseError returns the terminal error after the engine has been finalized.
func (c *StreamEngine) CloseError() error {
	if value := c.closeErr.Load(); value != nil {
		return value.(error)
	}
	return nil
}

// InflightForTest exposes the bounded ingress token channel to legacy semantic
// tests; normal callers use the application pipe and never need this hook.
func (c *StreamEngine) InflightForTest() chan struct{} { return c.inflight }

// SetOnActivateForTest updates the activation callback for adapter tests that
// construct an engine before installing an observation hook.
func (c *StreamEngine) SetOnActivateForTest(callback func()) { c.cfg.OnActivate = callback }

// ScheduleFrontierRescueForTest invokes the existing policy orchestration for
// package-level semantic tests without exposing queues or worker state.
func (c *StreamEngine) ScheduleFrontierRescueForTest() { c.scheduleFrontierRescue() }

// HandleACKForTest applies an ACK through the production orchestration path.
func (c *StreamEngine) HandleACKForTest(next uint64) error { return c.handleAck(next) }

// ScheduleRetriesForTest runs the ordinary retry orchestration once.
func (c *StreamEngine) ScheduleRetriesForTest() { c.scheduleRetries() }

// DrainOutstandingForTest exercises the existing graceful-drain implementation.
func (c *StreamEngine) DrainOutstandingForTest() error { return c.drainOutstandingOnClose() }

// HandleLegFailureForTest routes a synthetic failure through the normal
// generation/invalidation path.
func (c *StreamEngine) HandleLegFailureForTest(id LegID, err error) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

// ChooseLegForTest returns the selected logical leg ID without exposing the
// concrete worker or carrier object.
func (c *StreamEngine) ChooseLegForTest(active bool, avoid int16) (LegID, bool) {
	leg := c.chooseLeg(active, avoid)
	if leg == nil {
		return 0, false
	}
	return LegID(leg.id), true
}

// SetLegPerformanceForTest seeds the existing scheduler observation state.
func (c *StreamEngine) SetLegPerformanceForTest(id LegID, writeBPS, ackedBPS float64, latency time.Duration) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	leg.perf.mu.Lock()
	leg.perf.writeBPS = writeBPS
	leg.perf.ackedBPS = ackedBPS
	leg.perf.writeLatency = latency
	leg.perf.mu.Unlock()
	return true
}

// SendACKFrameForTest schedules a cumulative ACK through per-leg writers.
func (c *StreamEngine) SendACKFrameForTest(value uint64, force bool) bool {
	return c.sendAckFrame(value, force)
}

// SendCloseFrameForTest schedules the logical CLOSE control through all live legs.
func (c *StreamEngine) SendCloseFrameForTest(value uint64) bool { return c.sendCloseFrame(value) }

// QueueDataAttemptForTest places one existing TX record on a concrete leg.
// It is used to preserve the writer-priority semantic test without exporting
// the worker queue or attempt representation as part of the production API.
func (c *StreamEngine) QueueDataAttemptForTest(record *StreamTXRecord, id LegID) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil || leg.closed.Load() {
		return false
	}
	select {
	case <-leg.done:
		return false
	case leg.send <- txSendAttempt{record: record}:
		return true
	default:
		return false
	}
}

// IsRetiringForTest reports whether a same-ID generation is waiting for its old
// workers to exit.
func (c *StreamEngine) IsRetiringForTest(id LegID) bool {
	c.legsMu.RLock()
	_, exists := c.retiring[uint8(id)]
	c.legsMu.RUnlock()
	return exists
}

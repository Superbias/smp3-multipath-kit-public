package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameTypeData     byte = 1
	frameTypeActivate byte = 2
	frameTypeAck      byte = 3
	frameTypeClose    byte = 4
	frameHeaderSize        = 13 // type(1) + seq/ack(8) + len(4)
	maxFramePayload        = 1 << 20
)

var errCoreClosed = errors.New("multipath core closed")

type schedulerMode uint8

const (
	schedulerStatic schedulerMode = iota
	schedulerAdaptive
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

type coreConfig struct {
	SchedulerMode        schedulerMode
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

// coreStats is a point-in-time, logical-stream view of a core. Counters are
// intentionally based on DATA accepted by SMP3, cumulative ACK retirement,
// and bytes delivered to the application; carrier wire bytes are not used by
// the adaptive controller.
type coreStats struct {
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
	seq        uint64
	data       []byte
	leg        uint8
	receivedAt time.Time
}

// rxGapTracker describes the currently unresolved sequence gap. It is kept
// separately from the pending-frame count because a later gap must not inherit
// the age of an earlier, already-repaired gap. The timer starts when the
// current expected sequence becomes unresolved, not when the oldest pending
// frame originally arrived.
type rxGapTracker struct {
	gapExpectedSeq uint64
	since          time.Time
}

func (g *rxGapTracker) refresh(expected uint64, pending map[uint64]dataFrame, now time.Time) {
	if len(pending) == 0 {
		g.gapExpectedSeq = expected
		g.since = time.Time{}
		return
	}
	if g.gapExpectedSeq != expected || g.since.IsZero() {
		g.gapExpectedSeq = expected
		g.since = now
	}
}

type txRecord struct {
	seq uint64
	// data is immutable until the record is cumulatively ACKed. We deliberately
	// do not return TX payloads to a pool because stale retransmit queue entries
	// may still reference them after an ACK races with a queued retry.
	data []byte

	createdAt         time.Time
	lastSentAt        time.Time
	lastSentLeg       int16 // -1 until the first successful write
	lastSentAttemptAt time.Time
	inTransit         bool
	transitLeg        uint8
	transitSince      time.Time
	rescueInTransit   bool
	rescueLeg         uint8
	rescueSince       time.Time
	lastRescueAt      time.Time
	sendCount         uint32
}

type txSendAttempt struct {
	record *txRecord
	rescue bool
}

type legControl struct {
	typ   byte
	value uint64
	done  chan error
}

type mpLeg struct {
	id      uint8
	conn    net.Conn
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

func (l *mpLeg) close(err error) {
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

type mpCore struct {
	cfg      coreConfig
	appConn  net.Conn
	pipeConn net.Conn

	// lifecycleMu serializes the transition into FINALIZING/DONE with leg
	// attachment. ACTIVE and graceful CLOSING still accept transport repair;
	// FINALIZING/DONE never do. Lock order is lifecycleMu -> legsMu.
	lifecycleMu sync.Mutex
	legsMu      sync.RWMutex
	legs        map[uint8]*mpLeg
	retiring    map[uint8]*mpLeg

	incoming  chan dataFrame
	done      chan struct{}
	txStopped chan struct{}
	closeErr  atomic.Value
	closeOne  sync.Once

	closing      atomic.Bool
	finalizing   atomic.Bool
	gracefulOnce sync.Once
	gracefulCh   chan struct{}

	txSeq          atomic.Uint64
	futureAckCount atomic.Uint64

	txMu        sync.Mutex
	outstanding map[uint64]*txRecord
	ackedNext   uint64
	inflight    chan struct{}
	retryCh     chan struct{}
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

func newCore(cfg coreConfig) (*mpCore, net.Conn) {
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
	c := &mpCore{
		cfg:              cfg,
		appConn:          appConn,
		pipeConn:         pipeConn,
		legs:             make(map[uint8]*mpLeg),
		retiring:         make(map[uint8]*mpLeg),
		incoming:         make(chan dataFrame, cfg.QueueFrames*2),
		done:             make(chan struct{}),
		txStopped:        make(chan struct{}),
		gracefulCh:       make(chan struct{}),
		activeCh:         make(chan struct{}),
		outstanding:      make(map[uint64]*txRecord),
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

func (c *mpCore) addLeg(id uint8, conn net.Conn, onClose func(error)) error {
	for {
		c.lifecycleMu.Lock()
		if c.finalizing.Load() {
			c.lifecycleMu.Unlock()
			return errCoreClosed
		}
		select {
		case <-c.done:
			c.lifecycleMu.Unlock()
			return errCoreClosed
		default:
		}

		c.legsMu.Lock()
		if _, exists := c.legs[id]; exists {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return errors.New("duplicate multipath leg")
		}
		if retiring := c.retiring[id]; retiring != nil {
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
				return errCoreClosed
			}
		}

		leg := &mpLeg{
			id:      id,
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
		c.legs[id] = leg
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

// replaceLeg intentionally tears down one carrier while keeping the logical
// core, session ID, sequence numbers, ACK state, and outstanding records
// intact. The normal handleLegFailure callback remains the single owner of
// reconnect scheduling.
func (c *mpCore) replaceLeg(id uint8, err error) bool {
	leg := c.getLeg(id)
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

func (c *mpCore) AppConn() net.Conn     { return c.appConn }
func (c *mpCore) Done() <-chan struct{} { return c.done }

func (c *mpCore) snapshotStats() coreStats {
	now := time.Now()
	stats := coreStats{
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

	c.txMu.Lock()
	for _, record := range c.outstanding {
		stats.OutstandingFrames++
		stats.OutstandingBytes += uint64(len(record.data))
		age := now.Sub(record.createdAt)
		if record.createdAt.IsZero() {
			age = 0
		}
		if age > stats.OldestOutstandingAge {
			stats.OldestOutstandingAge = age
		}
		var owners [2]bool
		if record.lastSentLeg >= 0 && record.lastSentLeg < int16(len(owners)) {
			owners[uint8(record.lastSentLeg)] = true
		}
		if record.inTransit && record.transitLeg < uint8(len(owners)) {
			owners[record.transitLeg] = true
		}
		if record.rescueInTransit && record.rescueLeg < uint8(len(owners)) {
			owners[record.rescueLeg] = true
		}
		for legID, owned := range owners {
			if !owned {
				continue
			}
			stats.OutstandingFramesByLeg[legID]++
			if age > stats.OldestOutstandingByLeg[legID] {
				stats.OldestOutstandingByLeg[legID] = age
			}
		}
	}
	if frontier, exists := c.outstanding[c.ackedNext]; exists {
		var owners [2]bool
		var ownerCount int
		markOwner := func(id int16) {
			if id < 0 || id >= int16(len(owners)) || owners[id] {
				return
			}
			owners[id] = true
			ownerCount++
		}
		markOwner(frontier.lastSentLeg)
		if frontier.inTransit {
			markOwner(int16(frontier.transitLeg))
		}
		if frontier.rescueInTransit {
			markOwner(int16(frontier.rescueLeg))
		}
		if ownerCount > 0 {
			stats.AckFrontierValid = true
			stats.AckFrontierMultiPath = ownerCount > 1
			// Prefer the newest attempt for diagnostics. Adaptive attribution refuses
			// to blame one carrier while MultiPath is true.
			legID := frontier.lastSentLeg
			latest := frontier.lastSentAttemptAt
			if frontier.inTransit && (latest.IsZero() || frontier.transitSince.After(latest)) {
				legID = int16(frontier.transitLeg)
				latest = frontier.transitSince
			}
			if frontier.rescueInTransit && (latest.IsZero() || frontier.rescueSince.After(latest)) {
				legID = int16(frontier.rescueLeg)
			}
			if legID < 0 {
				for id, owned := range owners {
					if owned {
						legID = int16(id)
						break
					}
				}
			}
			stats.AckFrontierLeg = uint8(legID)
			if !frontier.createdAt.IsZero() {
				stats.AckFrontierAge = now.Sub(frontier.createdAt)
				if stats.AckFrontierAge < 0 {
					stats.AckFrontierAge = 0
				}
			}
		}
	}
	c.txMu.Unlock()

	for id := range stats.LegUp {
		stats.LegUp[id] = c.hasLeg(uint8(id))
	}
	if stats.LastAckProgress.IsZero() && stats.OutstandingFrames > 0 {
		stats.LastAckProgress = now.Add(-stats.OldestOutstandingAge)
	}
	return stats
}

func (c *mpCore) Close() error {
	// Explicit core shutdown is not a carrier-health signal. Mark it before
	// closing the done channel so adaptive probation cannot turn normal teardown
	// into a global Hy2 cooldown.
	c.closing.Store(true)
	c.fail(io.EOF)
	return nil
}

// startGracefulClose terminates a logical stream without discarding payload that
// has already been accepted from the local application. The TX loop is stopped,
// outstanding DATA is allowed to drain while ACK progress continues, and a
// logical CLOSE control frame is emitted before the transport legs are torn down.
//
// This is intentionally different from fail(): fail is for fatal/shutdown paths
// and may drop outstanding payload; startGracefulClose is for ordinary routed TCP
// EOF/close where already-written bytes must reach the peer first.
func (c *mpCore) startGracefulClose(err error) {
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
				if !errors.Is(drainErr, errCoreClosed) {
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
			c.sendCloseFrame(c.txSeq.Load())
			c.fail(closeErr)
		}(err)
	})
}

func (c *mpCore) beginFinalizing() bool {
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

func (c *mpCore) fail(err error) {
	if err == nil {
		err = errCoreClosed
	}
	c.closeOne.Do(func() {
		// This lock makes FINALIZING/DONE atomic with addLeg(). A leg is either
		// installed before shutdown snapshots transports, or rejected after the
		// lifecycle transition; it can never appear in between.
		c.lifecycleMu.Lock()
		c.finalizing.Store(true)
		c.closeErr.Store(err)
		close(c.done)
		_ = c.pipeConn.Close()
		_ = c.appConn.Close()
		c.legsMu.RLock()
		legs := make([]*mpLeg, 0, len(c.legs))
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

func (c *mpCore) getBuffer() []byte { return c.bufferPool.Get().([]byte) }
func (c *mpCore) putBuffer(buffer []byte) {
	if cap(buffer) < c.cfg.ChunkSize {
		return
	}
	c.bufferPool.Put(buffer[:c.cfg.ChunkSize])
}

func (c *mpCore) acquireInflight() bool {
	select {
	case c.inflight <- struct{}{}:
		return true
	case <-c.gracefulCh:
		return false
	case <-c.done:
		return false
	}
}

func (c *mpCore) releaseInflight(count int) {
	for range count {
		select {
		case <-c.inflight:
		default:
			return
		}
	}
}

func (c *mpCore) txLoop() {
	defer close(c.txStopped)
	for {
		if !c.acquireInflight() {
			return
		}
		buffer := make([]byte, c.cfg.ChunkSize)
		n, err := c.pipeConn.Read(buffer)
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			record := &txRecord{
				seq:         c.txSeq.Add(1) - 1,
				data:        buffer[:n],
				createdAt:   time.Now(),
				lastSentLeg: -1,
			}
			c.txMu.Lock()
			c.outstanding[record.seq] = record
			c.txMu.Unlock()
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
			c.startGracefulClose(err)
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
func (c *mpCore) drainOutstandingOnClose() error {
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
		c.txMu.Lock()
		remaining := len(c.outstanding)
		acked := c.ackedNext
		c.txMu.Unlock()
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
			return errCoreClosed
		case <-ticker.C:
		}
	}
}

func (c *mpCore) activationLoop() {
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
	windowBase := c.ingressBytes.Load()
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
				bytesNow := c.ingressBytes.Load()
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				if elapsed > 0 && uint64(float64(delta)/elapsed.Seconds()) >= c.cfg.ThresholdBytesPS {
					c.activate()
					return
				}
				windowStart = now
				windowBase = bytesNow
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

func (c *mpCore) activate() {
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

func (c *mpCore) notifyPeerActivate() { c.sendControlFrame(frameTypeActivate, 0) }

func (c *mpCore) getLeg(id uint8) *mpLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *mpCore) hasLeg(id uint8) bool { return c.getLeg(id) != nil }

func (c *mpCore) availableLegs() []*mpLeg {
	c.legsMu.RLock()
	legs := make([]*mpLeg, 0, len(c.legs))
	for _, leg := range c.legs {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *mpCore) legCount() int {
	c.legsMu.RLock()
	n := len(c.legs)
	c.legsMu.RUnlock()
	return n
}

func (c *mpCore) hasOutstanding() bool {
	c.txMu.Lock()
	has := len(c.outstanding) > 0
	c.txMu.Unlock()
	return has
}

func (c *mpCore) weightFor(id uint8) uint32 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *mpCore) effectiveSchedulerWeight(leg *mpLeg) float64 {
	base := float64(c.weightFor(leg.id))
	if c.cfg.SchedulerMode != schedulerAdaptive {
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

func (c *mpCore) chooseLeg(active bool, avoid int16) *mpLeg {
	if !active {
		if primary := c.getLeg(0); primary != nil && int16(primary.id) != avoid {
			return primary
		}
	}
	legs := c.availableLegs()
	var best *mpLeg
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

func (c *mpCore) markTransit(record *txRecord, legID uint8) bool {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if current := c.outstanding[record.seq]; current != record || record.inTransit {
		return false
	}
	record.inTransit = true
	record.transitLeg = legID
	record.transitSince = time.Now()
	return true
}

func (c *mpCore) markRescueTransit(record *txRecord, legID uint8) (time.Time, bool) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if current := c.outstanding[record.seq]; current != record || record.rescueInTransit {
		return time.Time{}, false
	}
	// A rescue must add path diversity. Never queue a duplicate attempt onto the
	// same carrier that is already holding the normal attempt/current ownership.
	if record.inTransit && record.transitLeg == legID {
		return time.Time{}, false
	}
	if !record.inTransit && record.lastSentLeg == int16(legID) {
		return time.Time{}, false
	}
	now := time.Now()
	record.rescueInTransit = true
	record.rescueLeg = legID
	record.rescueSince = now
	// Do not consume the rescue cooldown here. The priority queue may be full or
	// the leg may already be retiring. lastRescueAt is committed only after the
	// attempt is actually accepted by leg.rescue.
	return now, true
}

func (c *mpCore) markRescueQueued(record *txRecord, legID uint8, started time.Time) bool {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.outstanding[record.seq] != record {
		return false
	}
	// The writer can race the enqueueing goroutine. A valid queued rescue is
	// either still the current rescue attempt, or it has already completed and
	// markAttemptSent recorded the same attempt start timestamp. A write failure
	// clears rescueInTransit without advancing lastSentAttemptAt, so it does not
	// accidentally consume the cooldown.
	pending := record.rescueInTransit && record.rescueLeg == legID && record.rescueSince.Equal(started)
	sent := record.lastSentAttemptAt.Equal(started)
	if !pending && !sent {
		return false
	}
	record.lastRescueAt = started
	c.frontierRescueAttempts.Add(1)
	return true
}

func (c *mpCore) clearAttempt(record *txRecord, legID uint8, rescue bool) {
	c.txMu.Lock()
	if current := c.outstanding[record.seq]; current == record {
		if rescue {
			if record.rescueInTransit && record.rescueLeg == legID {
				// Failed/abandoned rescue attempts must not delay the next ACK-paced
				// repair. If this exact attempt had already committed its queue time,
				// roll the cooldown back before clearing the transient state.
				if record.lastRescueAt.Equal(record.rescueSince) {
					record.lastRescueAt = time.Time{}
				}
				record.rescueInTransit = false
				record.rescueSince = time.Time{}
			}
		} else if record.inTransit && record.transitLeg == legID {
			record.inTransit = false
			record.transitSince = time.Time{}
		}
	}
	c.txMu.Unlock()
}

func (c *mpCore) attemptCurrent(record *txRecord, legID uint8, rescue bool) bool {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.outstanding[record.seq] != record {
		return false
	}
	if rescue {
		return record.rescueInTransit && record.rescueLeg == legID
	}
	return record.inTransit && record.transitLeg == legID
}

func (c *mpCore) markAttemptSent(record *txRecord, legID uint8, rescue bool) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.outstanding[record.seq] != record {
		return
	}

	var attemptStarted time.Time
	if rescue {
		if !record.rescueInTransit || record.rescueLeg != legID {
			return
		}
		attemptStarted = record.rescueSince
		record.rescueInTransit = false
		record.rescueSince = time.Time{}
	} else {
		if !record.inTransit || record.transitLeg != legID {
			return
		}
		attemptStarted = record.transitSince
		record.inTransit = false
		record.transitSince = time.Time{}
	}

	now := time.Now()
	if attemptStarted.IsZero() {
		attemptStarted = now
	}
	// A slow old attempt can finish after a newer rescue. Do not let that stale
	// completion steal ACK-frontier ownership back from the newer attempt.
	if record.lastSentAttemptAt.IsZero() || !attemptStarted.Before(record.lastSentAttemptAt) {
		record.lastSentAttemptAt = attemptStarted
		record.lastSentAt = now
		record.lastSentLeg = int16(legID)
	}
	if rescue || record.sendCount > 0 {
		c.txRetransmitBytes[legID].Add(uint64(len(record.data)))
	}
	c.txSentBytes[legID].Add(uint64(len(record.data)))
	record.sendCount++
}

func (c *mpCore) isOutstanding(record *txRecord) bool {
	c.txMu.Lock()
	ok := c.outstanding[record.seq] == record
	c.txMu.Unlock()
	return ok
}

func (c *mpCore) tryQueue(leg *mpLeg, record *txRecord) bool {
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

func (c *mpCore) tryQueueRescue(leg *mpLeg, record *txRecord) bool {
	if leg == nil || leg.closed.Load() {
		return false
	}
	started, marked := c.markRescueTransit(record, leg.id)
	if !marked {
		c.txMu.Lock()
		already := c.outstanding[record.seq] != record || record.rescueInTransit
		c.txMu.Unlock()
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

func (c *mpCore) enqueueRecord(record *txRecord, avoid int16) error {
	for {
		select {
		case <-c.done:
			return errCoreClosed
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
				return errCoreClosed
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
			return errCoreClosed
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *mpCore) enqueueRescue(record *txRecord, avoid int16) error {
	select {
	case <-c.done:
		return errCoreClosed
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

func (c *mpCore) legWriteLoop(leg *mpLeg) {
	var ackSent uint64

	writeControl := func(control legControl) bool {
		var header [frameHeaderSize]byte
		header[0] = control.typ
		binary.BigEndian.PutUint64(header[1:9], control.value)
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
		if err := writeDataFrame(leg.conn, dataFrame{seq: record.seq, data: record.data}); err != nil {
			c.clearAttempt(record, leg.id, attempt.rescue)
			c.handleLegFailure(leg, err)
			return false
		}
		leg.perf.observeWrite(len(record.data), time.Since(writeStarted))
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

func (c *mpCore) legReadLoop(leg *mpLeg) {
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
			c.startGracefulClose(io.EOF)
			return
		case frameTypeData:
			select {
			case c.incoming <- dataFrame{seq: frame.seq, data: frame.data, leg: leg.id, receivedAt: time.Now()}:
			case <-c.done:
				c.putBuffer(frame.data)
				return
			}
		}
	}
}

func (c *mpCore) handleLegFailure(leg *mpLeg, err error) {
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
	c.txMu.Lock()
	for _, record := range c.outstanding {
		if record.inTransit && record.transitLeg == leg.id {
			record.inTransit = false
			record.transitSince = time.Time{}
		}
		if record.rescueInTransit && record.rescueLeg == leg.id {
			if record.lastRescueAt.Equal(record.rescueSince) {
				record.lastRescueAt = time.Time{}
			}
			record.rescueInTransit = false
			record.rescueSince = time.Time{}
		}
		// lastSentLeg is an 8-bit logical leg id, not a transport-generation id.
		// Once this concrete leg instance dies, ownership by that id must be
		// invalidated so a same-ID replacement is allowed to replay old unacked
		// DATA instead of being mistaken for the original writer.
		if record.lastSentLeg == int16(leg.id) {
			record.lastSentLeg = -1
		}
	}
	c.txMu.Unlock()

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

func (c *mpCore) failIfNoLegRecovers(epoch uint64) {
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

func (c *mpCore) kickRetry() {
	select {
	case c.retryCh <- struct{}{}:
	default:
	}
}

func (c *mpCore) kickFrontierRescue() {
	select {
	case c.frontierRescueCh <- struct{}{}:
	default:
	}
}

func (c *mpCore) kickAck() {
	select {
	case c.ackWakeCh <- struct{}{}:
	default:
	}
}

func (c *mpCore) forceAck() {
	c.ackForce.Store(true)
	c.kickAck()
}

func (c *mpCore) retransmitLoop() {
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
	record *txRecord
	avoid  int16
}

func (c *mpCore) scheduleRetries() {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return
	}
	available := make(map[uint8]struct{}, len(legs))
	for _, leg := range legs {
		available[leg.id] = struct{}{}
	}
	var retry []retryCandidate
	c.txMu.Lock()
	// Frames with no live attempt need ordinary replay. A separate rescue attempt
	// is treated as live work too; do not enqueue a second normal copy while it is
	// already repairing the record.
	for _, record := range c.outstanding {
		if record.inTransit || record.rescueInTransit {
			continue
		}
		if record.lastSentLeg < 0 {
			retry = append(retry, retryCandidate{record: record, avoid: -1})
			continue
		}
		lastID := uint8(record.lastSentLeg)
		if _, lastAlive := available[lastID]; !lastAlive {
			retry = append(retry, retryCandidate{record: record, avoid: record.lastSentLeg})
		}
	}
	c.txMu.Unlock()

	// Cumulative ACK makes the oldest outstanding sequence the only proven HOL
	// blocker. Go map iteration is deliberately unordered, so always replay from
	// the ACK frontier forward rather than letting future DATA fill the surviving
	// leg queue ahead of the frame that can advance cumulative ACK.
	sort.Slice(retry, func(i, j int) bool {
		return retry[i].record.seq < retry[j].record.seq
	})
	for _, candidate := range retry {
		if err := c.enqueueRecord(candidate.record, candidate.avoid); err != nil && !errors.Is(err, errCoreClosed) {
			c.fail(err)
			return
		}
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
func (c *mpCore) scheduleFrontierRescue() {
	now := time.Now()
	var rescue *txRecord
	var owner int16 = -1

	// Healthy ACK progress is the hot path. Decide whether the current frontier
	// is actually overdue before snapshotting legs or allocating an availability
	// map. enqueueRescue revalidates the record/leg state before queueing, so it is
	// safe for transport membership to change after this short TX snapshot.
	c.txMu.Lock()
	head := c.outstanding[c.ackedNext]
	if head == nil || head.rescueInTransit {
		c.txMu.Unlock()
		return
	}
	if head.inTransit {
		owner = int16(head.transitLeg)
	} else if head.lastSentLeg >= 0 {
		owner = head.lastSentLeg
	}
	reference := head.createdAt
	for _, candidate := range []time.Time{head.transitSince, head.lastSentAt, head.lastRescueAt} {
		if candidate.After(reference) {
			reference = candidate
		}
	}
	if reference.IsZero() || now.Sub(reference) < c.cfg.RetransmitTimeout {
		c.txMu.Unlock()
		return
	}
	rescue = head
	c.txMu.Unlock()

	legs := c.availableLegs()
	if len(legs) == 0 {
		return
	}
	if len(legs) == 1 {
		if !c.active.Load() {
			c.activate()
		}
		return
	}
	if owner < 0 {
		return
	}
	ownerAlive := false
	for _, leg := range legs {
		if int16(leg.id) == owner {
			ownerAlive = true
			break
		}
	}
	if !ownerAlive {
		// Ordinary retry/rejoin recovery owns dead-generation replay. Rescue is
		// only for adding diversity to a still-live current owner.
		return
	}
	if err := c.enqueueRescue(rescue, owner); err != nil && !errors.Is(err, errCoreClosed) {
		c.fail(err)
	}
}

func (c *mpCore) handleAck(next uint64) error {
	max := c.txSeq.Load()
	if next > max {
		// This ACK cannot safely prove delivery of any local payload. A live
		// alpha2 test observed such an anomaly around transport rejoin; treating
		// it as fatal caused an otherwise recoverable leg to enter a reconnect
		// storm. Ignore it without advancing TX state and let valid cumulative
		// ACKs/retransmission drive progress.
		count := c.futureAckCount.Add(1)
		if c.cfg.OnFutureAck != nil && (count == 1 || count&(count-1) == 0) {
			c.cfg.OnFutureAck(next, max, count)
		}
		return nil
	}
	c.txMu.Lock()
	if next <= c.ackedNext {
		c.txMu.Unlock()
		return nil
	}
	released := 0
	var ackedBytesByLeg [2]int
	for seq := c.ackedNext; seq < next; seq++ {
		if record, exists := c.outstanding[seq]; exists {
			legID := record.lastSentLeg
			// A peer can ACK immediately after the frame write completes, before
			// legWriteLoop gets to markSent. transitLeg still identifies the
			// successful carrier in that narrow race, so useful TX goodput must not
			// lose the frame's attribution.
			if legID < 0 && record.inTransit {
				legID = int16(record.transitLeg)
			}
			if legID >= 0 && legID < int16(len(c.txAckedUseful)) {
				c.txAckedUseful[legID].Add(uint64(len(record.data)))
				ackedBytesByLeg[legID] += len(record.data)
			}
			delete(c.outstanding, seq)
			released++
		}
	}
	c.ackedNext = next
	c.txMu.Unlock()
	if released > 0 {
		now := time.Now()
		c.lastAckProgressNs.Store(now.UnixNano())
		for legID, bytes := range ackedBytesByLeg {
			if bytes <= 0 {
				continue
			}
			if leg := c.getLeg(uint8(legID)); leg != nil {
				leg.perf.observeAck(bytes, now)
			}
		}
	}
	c.releaseInflight(released)
	// R10: cumulative ACK progress can expose another already-overdue blocker.
	// Wake the O(1) frontier checker immediately instead of waiting for the next
	// retransmit ticker. Duplicate/non-progress ACKs returned above and do not
	// create repair work.
	c.kickFrontierRescue()
	return nil
}

func (c *mpCore) requestAck(next uint64) {
	for {
		old := c.ackNext.Load()
		if next <= old || c.ackNext.CompareAndSwap(old, next) {
			break
		}
	}
	c.kickAck()
}

func (c *mpCore) ackLoop() {
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

func (c *mpCore) queueAck(leg *mpLeg, value uint64, force bool) bool {
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

func (c *mpCore) sendAckFrame(value uint64, force bool) bool {
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

func (c *mpCore) queueControl(leg *mpLeg, typ byte, value uint64, wait bool) (chan error, bool) {
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

func (c *mpCore) sendCloseFrame(value uint64) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}

	// Queue CLOSE on every live leg first. Then wait only until one carrier has
	// actually written it; a blocked carrier must not prevent a healthy carrier
	// from delivering the logical EOF before fail() tears transports down.
	waiters := make([]struct {
		leg  *mpLeg
		done chan error
	}, 0, len(legs))
	for _, leg := range legs {
		if done, ok := c.queueControl(leg, frameTypeClose, value, true); ok {
			waiters = append(waiters, struct {
				leg  *mpLeg
				done chan error
			}{leg: leg, done: done})
		}
	}
	if len(waiters) == 0 {
		return false
	}

	result := make(chan bool, len(waiters))
	for _, waiter := range waiters {
		go func(leg *mpLeg, done chan error) {
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

func (c *mpCore) sendControlFrame(typ byte, value uint64) bool {
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

func (c *mpCore) rxLoop() {
	expected := uint64(0)
	pending := make(map[uint64]dataFrame)
	var gap rxGapTracker
	for {
		select {
		case <-c.done:
			for _, frame := range pending {
				c.putBuffer(frame.data)
			}
			return
		case frame := <-c.incoming:
			if frame.receivedAt.IsZero() {
				frame.receivedAt = time.Now()
			}
			if frame.seq < expected {
				c.putBuffer(frame.data)
				c.forceAck()
				continue
			}
			if frame.seq > expected {
				if _, exists := pending[frame.seq]; exists {
					c.putBuffer(frame.data)
					continue
				}
				if len(pending) >= c.cfg.MaxReorderFrames {
					c.putBuffer(frame.data)
					c.fail(errors.New("multipath reorder buffer exceeded"))
					return
				}
				pending[frame.seq] = frame
				if frame.leg < uint8(len(c.rxUniqueBytes)) {
					c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
				}
				c.rxPendingFrames.Add(1)
				c.rxPendingBytes.Add(uint64(len(frame.data)))
				gap.refresh(expected, pending, time.Now())
				if gap.since.IsZero() {
					c.rxGapSinceUnixNs.Store(0)
				} else {
					c.rxGapSinceUnixNs.Store(gap.since.UnixNano())
				}
				continue
			}
			if frame.leg < uint8(len(c.rxUniqueBytes)) {
				c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
			}
			for {
				if err := writeAll(c.pipeConn, frame.data); err != nil {
					c.putBuffer(frame.data)
					if !c.closing.Load() {
						c.fail(err)
					}
					for _, pendingFrame := range pending {
						c.putBuffer(pendingFrame.data)
					}
					return
				}
				c.rxDeliveredBytes.Add(uint64(len(frame.data)))
				c.putBuffer(frame.data)
				expected++
				next, exists := pending[expected]
				if !exists {
					gap.refresh(expected, pending, time.Now())
					if gap.since.IsZero() {
						c.rxGapSinceUnixNs.Store(0)
					} else {
						c.rxGapSinceUnixNs.Store(gap.since.UnixNano())
					}
					break
				}
				delete(pending, expected)
				c.rxPendingFrames.Add(-1)
				c.rxPendingBytes.Add(uint64(0) - uint64(len(next.data)))
				frame = next
			}
			c.requestAck(expected)
		}
	}
}

func writeDataFrame(conn net.Conn, frame dataFrame) error {
	var header [frameHeaderSize]byte
	header[0] = frameTypeData
	binary.BigEndian.PutUint64(header[1:9], frame.seq)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(frame.data)))
	buffers := net.Buffers{header[:], frame.data}
	_, err := buffers.WriteTo(conn)
	return err
}

func readWireFrame(conn net.Conn, core *mpCore) (wireFrame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return wireFrame{}, err
	}
	frame := wireFrame{typ: header[0], seq: binary.BigEndian.Uint64(header[1:9])}
	switch frame.typ {
	case frameTypeActivate, frameTypeAck, frameTypeClose:
		if binary.BigEndian.Uint32(header[9:13]) != 0 {
			return wireFrame{}, errors.New("invalid multipath control frame length")
		}
		return frame, nil
	case frameTypeData:
	default:
		return wireFrame{}, errors.New("unknown multipath frame type")
	}
	length := int(binary.BigEndian.Uint32(header[9:13]))
	if length <= 0 || length > maxFramePayload {
		return wireFrame{}, errors.New("invalid multipath frame length")
	}
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

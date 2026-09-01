package smp3core

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DatagramFrameType      byte = 5
	MaxDatagramPayload          = 16 * 1024
	MaxDatagramAddressSize      = 512
)

var (
	ErrDatagramClosed   = errors.New("multipath datagram core closed")
	ErrDatagramTooLarge = errors.New("multipath datagram exceeds max_datagram_size")
)

type DatagramMode uint8

const (
	DatagramStripe DatagramMode = iota
	DatagramDuplicate
	DatagramAdaptive
)

func (m DatagramMode) String() string {
	switch m {
	case DatagramDuplicate:
		return "duplicate"
	case DatagramAdaptive:
		return "adaptive"
	default:
		return "stripe"
	}
}

type DatagramConfig struct {
	Mode                       DatagramMode
	QueueFrames                int
	MaxDatagramSize            int
	DedupWindow                uint64
	IdleTimeout                time.Duration
	RecoveryTimeout            time.Duration
	AdaptiveQueueDelay         time.Duration
	AdaptiveDuplicateThreshold int
	BandwidthMbps              []uint32
	OnLegDown                  func(LegID, error)
	OnLegUseful                func(LegID, int)
}

type datagramFrame struct {
	id       uint64
	address  string
	data     []byte
	queuedAt time.Time
}

type datagramLegPerf struct {
	mu              sync.Mutex
	ewmaBytesPerSec float64
	ewmaDelay       time.Duration
	lastSuccess     time.Time
}

func (p *datagramLegPerf) observe(bytes int, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Microsecond
	}
	bps := float64(bytes) / elapsed.Seconds()
	p.mu.Lock()
	if p.ewmaBytesPerSec == 0 {
		p.ewmaBytesPerSec = bps
	} else {
		p.ewmaBytesPerSec = p.ewmaBytesPerSec*0.8 + bps*0.2
	}
	if p.ewmaDelay == 0 {
		p.ewmaDelay = elapsed
	} else {
		p.ewmaDelay = time.Duration(float64(p.ewmaDelay)*0.8 + float64(elapsed)*0.2)
	}
	p.lastSuccess = time.Now()
	p.mu.Unlock()
}

func (p *datagramLegPerf) snapshot() (float64, time.Duration, time.Time) {
	p.mu.Lock()
	bps, delay, last := p.ewmaBytesPerSec, p.ewmaDelay, p.lastSuccess
	p.mu.Unlock()
	return bps, delay, last
}

type datagramLeg struct {
	id          uint8
	leg         DatagramLeg
	send        chan datagramFrame
	done        chan struct{}
	retired     chan struct{}
	onClose     func(error)
	once        sync.Once
	closed      atomic.Bool
	workers     sync.WaitGroup
	perf        datagramLegPerf
	queuedBytes atomic.Int64
}

func (l *datagramLeg) close(err error) {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.done)
		_ = l.leg.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

type DatagramStats struct {
	Mode            DatagramMode
	LegUp           [2]bool
	QueueDepth      [2]int
	QueueBytes      [2]int64
	TxBytes         [2]uint64
	RxUniqueBytes   [2]uint64
	DuplicateTx     uint64
	DuplicateRxDrop uint64
	AdaptiveWeight  [2]float64
}

type DatagramEngine struct {
	cfg DatagramConfig

	legsMu   sync.RWMutex
	legs     map[uint8]*datagramLeg
	retiring map[uint8]*datagramLeg

	incoming chan datagramFrame
	done     chan struct{}
	closeOne sync.Once

	txSeq atomic.Uint64

	seenMu  sync.Mutex
	seen    map[uint64]struct{}
	maxSeen uint64

	txBytes       [2]atomic.Uint64
	rxUniqueBytes [2]atomic.Uint64
	duplicateTx   atomic.Uint64
	duplicateDrop atomic.Uint64
	lastActivity  atomic.Int64
	recoveryEpoch atomic.Uint64
}

func NewDatagramEngine(cfg DatagramConfig) *DatagramEngine {
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.MaxDatagramSize <= 0 {
		cfg.MaxDatagramSize = MaxDatagramPayload
	}
	if cfg.MaxDatagramSize > MaxStreamFramePayload-2-512 {
		cfg.MaxDatagramSize = MaxStreamFramePayload - 2 - 512
	}
	if cfg.DedupWindow == 0 {
		cfg.DedupWindow = 4096
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 2 * time.Minute
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 15 * time.Second
	}
	if cfg.AdaptiveQueueDelay <= 0 {
		cfg.AdaptiveQueueDelay = 120 * time.Millisecond
	}
	c := &DatagramEngine{
		cfg:      cfg,
		legs:     make(map[uint8]*datagramLeg),
		retiring: make(map[uint8]*datagramLeg),
		incoming: make(chan datagramFrame, cfg.QueueFrames*2),
		done:     make(chan struct{}),
		seen:     make(map[uint64]struct{}),
	}
	c.lastActivity.Store(time.Now().UnixNano())
	go c.idleLoop()
	return c
}

func (c *DatagramEngine) Done() <-chan struct{} { return c.done }

func (c *DatagramEngine) Close() error {
	c.closeOne.Do(func() {
		close(c.done)
		c.legsMu.RLock()
		legs := make([]*datagramLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			leg.close(ErrDatagramClosed)
		}
	})
	return nil
}

func (c *DatagramEngine) touch() { c.lastActivity.Store(time.Now().UnixNano()) }

func (c *DatagramEngine) idleLoop() {
	interval := c.cfg.IdleTimeout / 4
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			last := c.lastActivity.Load()
			if last != 0 && now.Sub(time.Unix(0, last)) >= c.cfg.IdleTimeout {
				_ = c.Close()
				return
			}
		}
	}
}

func (c *DatagramEngine) AttachLeg(id LegID, leg DatagramLeg, onClose func(error)) error {
	id8 := uint8(id)
	if id8 > 1 {
		return errors.New("invalid datagram leg id")
	}
	for {
		select {
		case <-c.done:
			return ErrDatagramClosed
		default:
		}
		c.legsMu.Lock()
		if _, exists := c.legs[id8]; exists {
			c.legsMu.Unlock()
			return errors.New("duplicate datagram leg")
		}
		if retiring := c.retiring[id8]; retiring != nil {
			retired := retiring.retired
			c.legsMu.Unlock()
			select {
			case <-retired:
				continue
			case <-c.done:
				return ErrDatagramClosed
			}
		}
		leg := &datagramLeg{
			id: id8, leg: leg, send: make(chan datagramFrame, c.cfg.QueueFrames),
			done: make(chan struct{}), retired: make(chan struct{}), onClose: onClose,
		}
		leg.workers.Add(2)
		c.legs[id8] = leg
		c.legsMu.Unlock()
		c.recoveryEpoch.Add(1)
		go func() { defer leg.workers.Done(); c.legWriteLoop(leg) }()
		go func() { defer leg.workers.Done(); c.legReadLoop(leg) }()
		return nil
	}
}

// ReplaceLeg retires one currently attached carrier while preserving the
// datagram engine, ID allocator, dedup window, and other leg generations.
func (c *DatagramEngine) ReplaceLeg(id LegID, err error) bool {
	c.legsMu.RLock()
	leg := c.legs[uint8(id)]
	c.legsMu.RUnlock()
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

func (c *DatagramEngine) hasLeg(id uint8) bool {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg != nil
}

func (c *DatagramEngine) availableLegs() []*datagramLeg {
	c.legsMu.RLock()
	legs := make([]*datagramLeg, 0, len(c.legs))
	for _, leg := range c.legs {
		if !leg.closed.Load() {
			legs = append(legs, leg)
		}
	}
	c.legsMu.RUnlock()
	sort.Slice(legs, func(i, j int) bool { return legs[i].id < legs[j].id })
	return legs
}

func (c *DatagramEngine) staticWeight(id uint8) float64 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return float64(c.cfg.BandwidthMbps[id])
	}
	return 1
}

func (c *DatagramEngine) effectiveWeight(leg *datagramLeg) float64 {
	base := c.staticWeight(leg.id)
	if c.cfg.Mode != DatagramAdaptive {
		return base
	}
	bps, delay, _ := leg.perf.snapshot()
	if bps > 0 {
		dynamicMbps := bps * 8 / 1e6
		if dynamicMbps < 1 {
			dynamicMbps = 1
		}
		if base > 1 {
			base = math.Sqrt(base * dynamicMbps)
		} else {
			base = dynamicMbps
		}
	}
	if delay >= c.cfg.AdaptiveQueueDelay*2 {
		base *= 0.15
	} else if delay >= c.cfg.AdaptiveQueueDelay {
		base *= 0.4
	}
	if base < 0.1 {
		base = 0.1
	}
	return base
}

func (c *DatagramEngine) chooseLeg(legs []*datagramLeg, packetSize int) *datagramLeg {
	var best *datagramLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		weight := c.effectiveWeight(leg)
		queuedBytes := leg.queuedBytes.Load()
		if queuedBytes < 0 {
			queuedBytes = 0
		}
		score := float64(queuedBytes+int64(packetSize)+1) / weight
		if c.cfg.Mode == DatagramAdaptive {
			_, delay, _ := leg.perf.snapshot()
			if delay > 0 {
				score *= 1 + float64(delay)/float64(c.cfg.AdaptiveQueueDelay)
			}
		}
		if best == nil || score < bestScore {
			best, bestScore = leg, score
		}
	}
	return best
}

func (c *DatagramEngine) shouldDuplicate(size int, legs []*datagramLeg) bool {
	if len(legs) < 2 {
		return false
	}
	if c.cfg.Mode == DatagramDuplicate {
		return true
	}
	return c.cfg.Mode == DatagramAdaptive && c.cfg.AdaptiveDuplicateThreshold > 0 && size <= c.cfg.AdaptiveDuplicateThreshold
}

func (c *DatagramEngine) enqueue(leg *datagramLeg, packet datagramFrame, deadline time.Time, nonBlocking bool) bool {
	if leg == nil || leg.closed.Load() {
		return false
	}
	// Reserve byte pressure before publishing to the channel. The writer can receive
	// immediately after a successful send, so accounting after the send would race
	// its dequeue subtraction and transiently produce a negative backlog. Failed or
	// timed-out reservations are rolled back here.
	reserved := int64(len(packet.data))
	leg.queuedBytes.Add(reserved)
	queued := false
	defer func() {
		if !queued {
			leg.queuedBytes.Add(-reserved)
		}
	}()
	if nonBlocking {
		select {
		case <-leg.done:
			return false
		case leg.send <- packet:
			queued = true
			return true
		default:
			return false
		}
	}
	if deadline.IsZero() {
		select {
		case <-c.done:
			return false
		case <-leg.done:
			return false
		case leg.send <- packet:
			queued = true
			return true
		}
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-c.done:
		return false
	case <-leg.done:
		return false
	case <-timer.C:
		return false
	case leg.send <- packet:
		queued = true
		return true
	}
}

func (c *DatagramEngine) Send(data []byte, address string, deadline time.Time) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > c.cfg.MaxDatagramSize {
		return ErrDatagramTooLarge
	}
	if address == "" || len(address) > MaxDatagramAddressSize {
		return errors.New("invalid multipath datagram address")
	}
	legs := c.availableLegs()
	if len(legs) == 0 {
		return errors.New("no multipath datagram leg available")
	}
	packet := datagramFrame{
		id: c.txSeq.Add(1) - 1, address: address,
		data: append([]byte(nil), data...), queuedAt: time.Now(),
	}
	c.touch()
	if c.shouldDuplicate(len(data), legs) {
		queued := 0
		for _, leg := range legs {
			if c.enqueue(leg, packet, deadline, true) {
				queued++
			}
		}
		if queued > 0 {
			if queued > 1 {
				c.duplicateTx.Add(uint64(queued - 1))
			}
			return nil
		}
	}

	best := c.chooseLeg(legs, len(data))
	if c.enqueue(best, packet, deadline, false) {
		return nil
	}
	// A selected leg can retire while the packet is being queued. Try the other
	// live generation before surfacing an error; UDP itself still remains
	// unreliable and this does not retransmit a packet already written on wire.
	for _, leg := range c.availableLegs() {
		if best != nil && leg.id == best.id {
			continue
		}
		if c.enqueue(leg, packet, deadline, true) {
			return nil
		}
	}
	return errors.New("multipath datagram queue unavailable")
}

func (c *DatagramEngine) legWriteLoop(leg *datagramLeg) {
	for {
		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case packet := <-leg.send:
			leg.queuedBytes.Add(-int64(len(packet.data)))
			start := packet.queuedAt
			if start.IsZero() {
				start = time.Now()
			}
			if err := WriteDatagramFrame(leg.leg, packet.id, packet.address, packet.data); err != nil {
				c.handleLegFailure(leg, err)
				return
			}
			elapsed := time.Since(start)
			leg.perf.observe(len(packet.data), elapsed)
			c.txBytes[leg.id].Add(uint64(len(packet.data)))
			if c.cfg.OnLegUseful != nil {
				c.cfg.OnLegUseful(LegID(leg.id), len(packet.data))
			}
			c.touch()
		}
	}
}

func (c *DatagramEngine) acceptDatagramID(id uint64, size int) bool {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	// Only datagrams that this sender mode can actually replicate use the stale
	// lower-bound rule. Pure stripe traffic has one wire copy per ID, so an old ID
	// arriving from a very slow path may still be a unique UDP datagram and must not
	// be discarded merely because newer IDs arrived first. Duplicate mode replicates
	// every packet; adaptive mode replicates only latency-sensitive packets at or
	// below AdaptiveDuplicateThreshold.
	mayBeReplicated := c.cfg.Mode == DatagramDuplicate ||
		(c.cfg.Mode == DatagramAdaptive && c.cfg.AdaptiveDuplicateThreshold > 0 && size <= c.cfg.AdaptiveDuplicateThreshold)
	if mayBeReplicated && len(c.seen) > 0 && c.maxSeen > c.cfg.DedupWindow {
		floor := c.maxSeen - c.cfg.DedupWindow
		if id < floor {
			return false
		}
	}
	if _, exists := c.seen[id]; exists {
		return false
	}
	c.seen[id] = struct{}{}
	if id > c.maxSeen || len(c.seen) == 1 {
		c.maxSeen = id
	}
	if c.maxSeen > c.cfg.DedupWindow && uint64(len(c.seen)) > c.cfg.DedupWindow*2 {
		floor := c.maxSeen - c.cfg.DedupWindow
		for seq := range c.seen {
			if seq < floor {
				delete(c.seen, seq)
			}
		}
	}
	return true
}

func (c *DatagramEngine) legReadLoop(leg *datagramLeg) {
	for {
		id, address, data, err := ReadDatagramFrame(leg.leg, c.cfg.MaxDatagramSize)
		if err != nil {
			c.handleLegFailure(leg, err)
			return
		}
		if !c.acceptDatagramID(id, len(data)) {
			c.duplicateDrop.Add(1)
			continue
		}
		packet := datagramFrame{id: id, address: address, data: data}
		packet.queuedAt = time.Now()
		c.rxUniqueBytes[leg.id].Add(uint64(len(packet.data)))
		c.touch()
		select {
		case <-c.done:
			return
		case c.incoming <- packet:
		}
	}
}

func (c *DatagramEngine) handleLegFailure(leg *datagramLeg, err error) {
	select {
	case <-c.done:
		// Core shutdown is intentional logical teardown, not a path-health signal.
		leg.close(err)
		return
	default:
	}
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
	go func() {
		leg.workers.Wait()
		c.legsMu.Lock()
		if c.retiring[leg.id] == leg {
			delete(c.retiring, leg.id)
		}
		c.legsMu.Unlock()
		close(leg.retired)
	}()
	if c.cfg.OnLegDown != nil {
		go c.cfg.OnLegDown(LegID(leg.id), err)
	}
	if remaining == 0 {
		epoch := c.recoveryEpoch.Add(1)
		go c.failIfNoLegRecovers(epoch)
	}
}

func (c *DatagramEngine) failIfNoLegRecovers(epoch uint64) {
	timer := time.NewTimer(c.cfg.RecoveryTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return
	case <-timer.C:
		if c.recoveryEpoch.Load() == epoch && len(c.availableLegs()) == 0 {
			_ = c.Close()
		}
	}
}

type Datagram struct {
	Address string
	Payload []byte
}

func (c *DatagramEngine) Receive(deadline time.Time) (Datagram, error) {
	var timer *time.Timer
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		delay := time.Until(deadline)
		if delay <= 0 {
			return Datagram{}, ErrDatagramTimeout
		}
		timer = time.NewTimer(delay)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case <-c.done:
		return Datagram{}, ErrDatagramClosed
	case <-timeout:
		return Datagram{}, ErrDatagramTimeout
	case packet := <-c.incoming:
		return Datagram{Address: packet.address, Payload: packet.data}, nil
	}
}

func (c *DatagramEngine) Snapshot() DatagramStats {
	stats := DatagramStats{Mode: c.cfg.Mode, DuplicateTx: c.duplicateTx.Load(), DuplicateRxDrop: c.duplicateDrop.Load()}
	for _, leg := range c.availableLegs() {
		stats.LegUp[leg.id] = true
		stats.QueueDepth[leg.id] = len(leg.send)
		stats.QueueBytes[leg.id] = leg.queuedBytes.Load()
		stats.AdaptiveWeight[leg.id] = c.effectiveWeight(leg)
	}
	for i := range 2 {
		stats.TxBytes[i] = c.txBytes[i].Load()
		stats.RxUniqueBytes[i] = c.rxUniqueBytes[i].Load()
	}
	return stats
}

func WriteDatagramFrame(conn io.Writer, id uint64, address string, data []byte) error {
	addressBytes := []byte(address)
	if len(addressBytes) == 0 || len(addressBytes) > MaxDatagramAddressSize {
		return errors.New("invalid datagram address")
	}
	payloadLength := 2 + len(addressBytes) + len(data)
	if payloadLength > MaxStreamFramePayload {
		return errors.New("datagram frame too large")
	}
	var header [StreamFrameHeaderSize]byte
	header[0] = DatagramFrameType
	binary.BigEndian.PutUint64(header[1:9], id)
	binary.BigEndian.PutUint32(header[9:13], uint32(payloadLength))
	var addressLength [2]byte
	binary.BigEndian.PutUint16(addressLength[:], uint16(len(addressBytes)))
	return writeDatagramParts(conn, header[:], addressLength[:], addressBytes, data)
}

func ReadDatagramFrame(conn io.Reader, maxDatagram int) (uint64, string, []byte, error) {
	var header [StreamFrameHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, "", nil, err
	}
	if header[0] != DatagramFrameType {
		return 0, "", nil, errors.New("unexpected frame on multipath datagram leg")
	}
	id := binary.BigEndian.Uint64(header[1:9])
	payloadLength := int(binary.BigEndian.Uint32(header[9:13]))
	maxPayloadLength := 2 + MaxDatagramAddressSize + maxDatagram
	if payloadLength < 3 || payloadLength > maxPayloadLength || payloadLength > MaxStreamFramePayload {
		return 0, "", nil, errors.New("invalid multipath datagram frame length")
	}
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, "", nil, err
	}
	addressLength := int(binary.BigEndian.Uint16(payload[:2]))
	if addressLength <= 0 || addressLength > MaxDatagramAddressSize || 2+addressLength > len(payload) {
		return 0, "", nil, errors.New("invalid multipath datagram address length")
	}
	address := string(payload[2 : 2+addressLength])
	data := payload[2+addressLength:]
	if len(data) > maxDatagram {
		return 0, "", nil, errors.New("multipath datagram exceeds negotiated maximum")
	}
	return id, address, data, nil
}

var ErrDatagramTimeout = errors.New("multipath datagram receive timeout")

func writeDatagramParts(writer io.Writer, parts ...[]byte) error {
	for _, part := range parts {
		for len(part) > 0 {
			n, err := writer.Write(part)
			if err != nil {
				return err
			}
			if n <= 0 {
				return io.ErrUnexpectedEOF
			}
			part = part[n:]
		}
	}
	return nil
}

// TxSequenceForTest exposes the next wire ID to package-level semantic tests;
// application callers never observe wire IDs.
func (c *DatagramEngine) TxSequenceForTest() uint64 { return c.txSeq.Load() }

// AcceptDatagramIDForTest preserves the existing dedup-window test seam.
func (c *DatagramEngine) AcceptDatagramIDForTest(id uint64, size int) bool {
	return c.acceptDatagramID(id, size)
}

// SeenContainsForTest checks a concrete dedup entry without exposing the map.
func (c *DatagramEngine) SeenContainsForTest(id uint64) bool {
	c.seenMu.Lock()
	_, exists := c.seen[id]
	c.seenMu.Unlock()
	return exists
}

// MaxSeenForTest returns the newest observed wire ID for dedup tests.
func (c *DatagramEngine) MaxSeenForTest() uint64 {
	c.seenMu.Lock()
	max := c.maxSeen
	c.seenMu.Unlock()
	return max
}

// InjectDatagramForTest feeds an already decoded packet to the adapter seam.
func (c *DatagramEngine) InjectDatagramForTest(address string, payload []byte) {
	c.incoming <- datagramFrame{address: address, data: payload}
}

// HasLeg reports whether a concrete datagram leg is attached.
func (c *DatagramEngine) HasLeg(id LegID) bool { return c.hasLeg(uint8(id)) }

// HandleLegFailureForTest routes a synthetic failure through normal leg
// retirement and recovery handling.
func (c *DatagramEngine) HandleLegFailureForTest(id LegID, err error) bool {
	c.legsMu.RLock()
	leg := c.legs[uint8(id)]
	c.legsMu.RUnlock()
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

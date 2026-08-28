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
	frameTypeDatagram      byte = 5
	maxRoutedDatagramSize       = 16 * 1024
	maxDatagramAddressSize      = 512
)

var (
	errDatagramClosed   = errors.New("multipath datagram core closed")
	errDatagramTooLarge = errors.New("multipath datagram exceeds max_datagram_size")
)

type datagramMode uint8

const (
	datagramModeStripe datagramMode = iota
	datagramModeDuplicate
	datagramModeAdaptive
)

func (m datagramMode) String() string {
	switch m {
	case datagramModeDuplicate:
		return "duplicate"
	case datagramModeAdaptive:
		return "adaptive"
	default:
		return "stripe"
	}
}

type datagramConfig struct {
	Mode                       datagramMode
	QueueFrames                int
	MaxDatagramSize            int
	DedupWindow                uint64
	IdleTimeout                time.Duration
	RecoveryTimeout            time.Duration
	AdaptiveQueueDelay         time.Duration
	AdaptiveDuplicateThreshold int
	BandwidthMbps              []uint32
	OnLegDown                  func(uint8, error)
	OnLegUseful                func(uint8, int)
}

type datagramPacket struct {
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
	conn        net.Conn
	send        chan datagramPacket
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
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

type datagramStats struct {
	Mode            datagramMode
	LegUp           [2]bool
	QueueDepth      [2]int
	QueueBytes      [2]int64
	TxBytes         [2]uint64
	RxUniqueBytes   [2]uint64
	DuplicateTx     uint64
	DuplicateRxDrop uint64
	AdaptiveWeight  [2]float64
}

type mpDatagramCore struct {
	cfg datagramConfig

	legsMu   sync.RWMutex
	legs     map[uint8]*datagramLeg
	retiring map[uint8]*datagramLeg

	incoming chan datagramPacket
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

	appConn *datagramPacketConn
}

func newDatagramCore(cfg datagramConfig) (*mpDatagramCore, *datagramPacketConn) {
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.MaxDatagramSize <= 0 {
		cfg.MaxDatagramSize = maxRoutedDatagramSize
	}
	if cfg.MaxDatagramSize > maxFramePayload-2-512 {
		cfg.MaxDatagramSize = maxFramePayload - 2 - 512
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
	c := &mpDatagramCore{
		cfg:      cfg,
		legs:     make(map[uint8]*datagramLeg),
		retiring: make(map[uint8]*datagramLeg),
		incoming: make(chan datagramPacket, cfg.QueueFrames*2),
		done:     make(chan struct{}),
		seen:     make(map[uint64]struct{}),
	}
	c.lastActivity.Store(time.Now().UnixNano())
	pc := &datagramPacketConn{core: c}
	c.appConn = pc
	go c.idleLoop()
	return c, pc
}

func (c *mpDatagramCore) Done() <-chan struct{} { return c.done }

func (c *mpDatagramCore) Close() error {
	c.closeOne.Do(func() {
		close(c.done)
		c.legsMu.RLock()
		legs := make([]*datagramLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			leg.close(errDatagramClosed)
		}
	})
	return nil
}

func (c *mpDatagramCore) touch() { c.lastActivity.Store(time.Now().UnixNano()) }

func (c *mpDatagramCore) idleLoop() {
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

func (c *mpDatagramCore) addLeg(id uint8, conn net.Conn, onClose func(error)) error {
	if id > 1 {
		return errors.New("invalid datagram leg id")
	}
	for {
		select {
		case <-c.done:
			return errDatagramClosed
		default:
		}
		c.legsMu.Lock()
		if _, exists := c.legs[id]; exists {
			c.legsMu.Unlock()
			return errors.New("duplicate datagram leg")
		}
		if retiring := c.retiring[id]; retiring != nil {
			retired := retiring.retired
			c.legsMu.Unlock()
			select {
			case <-retired:
				continue
			case <-c.done:
				return errDatagramClosed
			}
		}
		leg := &datagramLeg{
			id: id, conn: conn, send: make(chan datagramPacket, c.cfg.QueueFrames),
			done: make(chan struct{}), retired: make(chan struct{}), onClose: onClose,
		}
		leg.workers.Add(2)
		c.legs[id] = leg
		c.legsMu.Unlock()
		c.recoveryEpoch.Add(1)
		go func() { defer leg.workers.Done(); c.legWriteLoop(leg) }()
		go func() { defer leg.workers.Done(); c.legReadLoop(leg) }()
		return nil
	}
}

func (c *mpDatagramCore) hasLeg(id uint8) bool {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg != nil
}

func (c *mpDatagramCore) availableLegs() []*datagramLeg {
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

func (c *mpDatagramCore) staticWeight(id uint8) float64 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return float64(c.cfg.BandwidthMbps[id])
	}
	return 1
}

func (c *mpDatagramCore) effectiveWeight(leg *datagramLeg) float64 {
	base := c.staticWeight(leg.id)
	if c.cfg.Mode != datagramModeAdaptive {
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

func (c *mpDatagramCore) chooseLeg(legs []*datagramLeg, packetSize int) *datagramLeg {
	var best *datagramLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		weight := c.effectiveWeight(leg)
		queuedBytes := leg.queuedBytes.Load()
		if queuedBytes < 0 {
			queuedBytes = 0
		}
		score := float64(queuedBytes+int64(packetSize)+1) / weight
		if c.cfg.Mode == datagramModeAdaptive {
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

func (c *mpDatagramCore) shouldDuplicate(size int, legs []*datagramLeg) bool {
	if len(legs) < 2 {
		return false
	}
	if c.cfg.Mode == datagramModeDuplicate {
		return true
	}
	return c.cfg.Mode == datagramModeAdaptive && c.cfg.AdaptiveDuplicateThreshold > 0 && size <= c.cfg.AdaptiveDuplicateThreshold
}

func (c *mpDatagramCore) enqueue(leg *datagramLeg, packet datagramPacket, deadline time.Time, nonBlocking bool) bool {
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

func (c *mpDatagramCore) sendDatagram(data []byte, address string, deadline time.Time) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > c.cfg.MaxDatagramSize {
		return errDatagramTooLarge
	}
	if address == "" || len(address) > maxDatagramAddressSize {
		return errors.New("invalid multipath datagram address")
	}
	legs := c.availableLegs()
	if len(legs) == 0 {
		return errors.New("no multipath datagram leg available")
	}
	packet := datagramPacket{
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

func (c *mpDatagramCore) legWriteLoop(leg *datagramLeg) {
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
			if err := writeDatagramFrame(leg.conn, packet); err != nil {
				c.handleLegFailure(leg, err)
				return
			}
			elapsed := time.Since(start)
			leg.perf.observe(len(packet.data), elapsed)
			c.txBytes[leg.id].Add(uint64(len(packet.data)))
			if c.cfg.OnLegUseful != nil {
				c.cfg.OnLegUseful(leg.id, len(packet.data))
			}
			c.touch()
		}
	}
}

func (c *mpDatagramCore) acceptDatagramID(id uint64, size int) bool {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	// Only datagrams that this sender mode can actually replicate use the stale
	// lower-bound rule. Pure stripe traffic has one wire copy per ID, so an old ID
	// arriving from a very slow path may still be a unique UDP datagram and must not
	// be discarded merely because newer IDs arrived first. Duplicate mode replicates
	// every packet; adaptive mode replicates only latency-sensitive packets at or
	// below AdaptiveDuplicateThreshold.
	mayBeReplicated := c.cfg.Mode == datagramModeDuplicate ||
		(c.cfg.Mode == datagramModeAdaptive && c.cfg.AdaptiveDuplicateThreshold > 0 && size <= c.cfg.AdaptiveDuplicateThreshold)
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

func (c *mpDatagramCore) legReadLoop(leg *datagramLeg) {
	for {
		packet, err := readDatagramFrame(leg.conn, c.cfg.MaxDatagramSize)
		if err != nil {
			c.handleLegFailure(leg, err)
			return
		}
		if !c.acceptDatagramID(packet.id, len(packet.data)) {
			c.duplicateDrop.Add(1)
			continue
		}
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

func (c *mpDatagramCore) handleLegFailure(leg *datagramLeg, err error) {
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
		go c.cfg.OnLegDown(leg.id, err)
	}
	if remaining == 0 {
		epoch := c.recoveryEpoch.Add(1)
		go c.failIfNoLegRecovers(epoch)
	}
}

func (c *mpDatagramCore) failIfNoLegRecovers(epoch uint64) {
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

func (c *mpDatagramCore) snapshotStats() datagramStats {
	stats := datagramStats{Mode: c.cfg.Mode, DuplicateTx: c.duplicateTx.Load(), DuplicateRxDrop: c.duplicateDrop.Load()}
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

func writeDatagramFrame(conn net.Conn, packet datagramPacket) error {
	address := []byte(packet.address)
	if len(address) == 0 || len(address) > maxDatagramAddressSize {
		return errors.New("invalid datagram address")
	}
	payloadLength := 2 + len(address) + len(packet.data)
	if payloadLength > maxFramePayload {
		return errors.New("datagram frame too large")
	}
	var header [frameHeaderSize]byte
	header[0] = frameTypeDatagram
	binary.BigEndian.PutUint64(header[1:9], packet.id)
	binary.BigEndian.PutUint32(header[9:13], uint32(payloadLength))
	var addressLength [2]byte
	binary.BigEndian.PutUint16(addressLength[:], uint16(len(address)))
	buffers := net.Buffers{header[:], addressLength[:], address, packet.data}
	_, err := buffers.WriteTo(conn)
	return err
}

func readDatagramFrame(conn net.Conn, maxDatagram int) (datagramPacket, error) {
	var packet datagramPacket
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return packet, err
	}
	if header[0] != frameTypeDatagram {
		return packet, errors.New("unexpected frame on multipath datagram leg")
	}
	packet.id = binary.BigEndian.Uint64(header[1:9])
	payloadLength := int(binary.BigEndian.Uint32(header[9:13]))
	maxPayloadLength := 2 + maxDatagramAddressSize + maxDatagram
	if payloadLength < 3 || payloadLength > maxPayloadLength || payloadLength > maxFramePayload {
		return packet, errors.New("invalid multipath datagram frame length")
	}
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return packet, err
	}
	addressLength := int(binary.BigEndian.Uint16(payload[:2]))
	if addressLength <= 0 || addressLength > maxDatagramAddressSize || 2+addressLength > len(payload) {
		return packet, errors.New("invalid multipath datagram address length")
	}
	packet.address = string(payload[2 : 2+addressLength])
	packet.data = payload[2+addressLength:]
	if len(packet.data) > maxDatagram {
		return packet, errors.New("multipath datagram exceeds negotiated maximum")
	}
	return packet, nil
}

type smp3PacketAddr string

func (a smp3PacketAddr) Network() string { return "udp" }
func (a smp3PacketAddr) String() string  { return string(a) }

type datagramPacketConn struct {
	core          *mpDatagramCore
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
}

func (c *datagramPacketConn) readDatagram() (datagramPacket, error) {
	deadline := deadlineFromAtomic(&c.readDeadline)
	var timer *time.Timer
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return datagramPacket{}, timeoutError{}
		}
		timer = time.NewTimer(d)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case <-c.core.done:
		return datagramPacket{}, net.ErrClosed
	case <-timeout:
		return datagramPacket{}, timeoutError{}
	case packet := <-c.core.incoming:
		return packet, nil
	}
}

func (c *datagramPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	packet, err := c.readDatagram()
	if err != nil {
		return 0, nil, err
	}
	n := copy(p, packet.data)
	return n, smp3PacketAddr(packet.address), nil
}

func (c *datagramPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, errors.New("nil datagram destination")
	}
	deadline := deadlineFromAtomic(&c.writeDeadline)
	if err := c.core.sendDatagram(p, addr.String(), deadline); err != nil {
		// An oversize UDP datagram is a packet-level rejection.  Report it as
		// consumed so a PacketConn copy loop keeps the association alive for the
		// next datagram; the core has already refused to enqueue or fragment it.
		if errors.Is(err, errDatagramTooLarge) {
			return len(p), nil
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, timeoutError{}
		}
		return 0, err
	}
	return len(p), nil
}

func (c *datagramPacketConn) Close() error        { return c.core.Close() }
func (c *datagramPacketConn) LocalAddr() net.Addr { return smp3PacketAddr("0.0.0.0:0") }
func (c *datagramPacketConn) SetDeadline(t time.Time) error {
	c.storeDeadline(&c.readDeadline, t)
	c.storeDeadline(&c.writeDeadline, t)
	return nil
}
func (c *datagramPacketConn) SetReadDeadline(t time.Time) error {
	c.storeDeadline(&c.readDeadline, t)
	return nil
}
func (c *datagramPacketConn) SetWriteDeadline(t time.Time) error {
	c.storeDeadline(&c.writeDeadline, t)
	return nil
}
func (c *datagramPacketConn) storeDeadline(v *atomic.Int64, t time.Time) {
	if t.IsZero() {
		v.Store(0)
	} else {
		v.Store(t.UnixNano())
	}
}

func deadlineFromAtomic(v *atomic.Int64) time.Time {
	ns := v.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.PacketConn = (*datagramPacketConn)(nil)

package smp3core

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StreamRTTProbePendingLimit = 8
	StreamRTTProbeTimeout      = 2 * time.Second
	StreamRTTSampleWindow      = 30 * time.Second
	streamRTTProbeInterval     = time.Second
	streamRTTMaxSamples        = 5
)

var (
	ErrStreamRTTCacheClosed     = errors.New("stream RTT probe cache is closed")
	ErrStreamRTTStaleGeneration = errors.New("stale stream RTT carrier generation")
	ErrStreamRTTGenerationReuse = errors.New("stream RTT carrier generation reused")
	ErrStreamRTTPendingFull     = errors.New("stream RTT probe limit reached")
	ErrStreamRTTTokenExhausted  = errors.New("stream RTT probe token space exhausted")
	ErrStreamRTTProbeExpired    = errors.New("stream RTT probe expired")
	ErrStreamRTTNoPending       = errors.New("stream RTT echo has no matching pending probe")
	ErrStreamRTTReplay          = errors.New("stream RTT probe replay rejected")
)

// StreamPathFeedback is an immutable scheduler-facing RTT snapshot. Samples
// counts only observations still inside StreamRTTSampleWindow.
type StreamPathFeedback struct {
	Identity   StreamPathIdentity
	RTT        time.Duration
	Samples    uint32
	ObservedAt time.Time
}

// StreamPathFeedbackProvider is safe for scheduler reads. Implementations must
// not perform I/O, wait on channels, or take a lock in PathFeedback.
type StreamPathFeedbackProvider interface {
	PathFeedback(StreamPathIdentity) (StreamPathFeedback, bool)
}

type streamRTTPending struct {
	identity StreamPathIdentity
	started  time.Time
}

type streamRTTSample struct {
	rtt time.Duration
	at  time.Time
}

type streamRTTPublished struct {
	identity StreamPathIdentity
	samples  [streamRTTMaxSamples]streamRTTSample
	count    int
}

// streamRTTProbeCache owns one session's per-leg probe sequence, pending set,
// generation high-water marks, and fresh feedback snapshots. Mutations are
// serialized; the provider read path is bounded atomic-load and small-array work.
type streamRTTProbeCache struct {
	mu sync.Mutex

	maxLegs       int
	closed        bool
	highWater     []uint64
	active        []bool
	binding       []StreamPathBinding
	probeKey      [][sha256.Size]byte
	lastPeerToken [][2]uint64
	nextToken     uint64
	pending       map[uint64]streamRTTPending
	samples       [][2][]streamRTTSample

	activeView []atomic.Pointer[StreamPathBinding]
	feedback   [][2]atomic.Pointer[streamRTTPublished]
}

var _ StreamPathFeedbackProvider = (*streamRTTProbeCache)(nil)

func newStreamRTTProbeCache() *streamRTTProbeCache {
	return newStreamRTTProbeCacheForLegs(2)
}

func newStreamRTTProbeCacheForLegs(maxLegs int) *streamRTTProbeCache {
	if maxLegs < 1 || maxLegs > MaxStreamLegIDCount {
		maxLegs = 2
	}
	return &streamRTTProbeCache{
		maxLegs:       maxLegs,
		highWater:     make([]uint64, maxLegs),
		active:        make([]bool, maxLegs),
		binding:       make([]StreamPathBinding, maxLegs),
		probeKey:      make([][sha256.Size]byte, maxLegs),
		lastPeerToken: make([][2]uint64, maxLegs),
		pending:       make(map[uint64]streamRTTPending),
		samples:       make([][2][]streamRTTSample, maxLegs),
		activeView:    make([]atomic.Pointer[StreamPathBinding], maxLegs),
		feedback:      make([][2]atomic.Pointer[streamRTTPublished], maxLegs),
	}
}

// BindLeg installs the authenticated result of one Version6 READY on a Core-
// owned generation. A generation can be rebound only idempotently; replacement
// requires a strictly higher Core generation. Auth must come directly from an
// authenticated Version6 READY codec; hosts cannot supply capabilities or keys.
func (c *streamRTTProbeCache) bindLeg(binding StreamPathBinding, auth StreamPathAuth) error {
	if c == nil {
		return ErrStreamRTTCacheClosed
	}
	if err := validateStreamPathBinding(binding); err != nil {
		return err
	}
	if int(binding.LegID) >= c.maxLegs {
		return ErrStreamPathMessage
	}
	if !auth.validFor(binding) {
		return ErrStreamPathNegotiation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrStreamRTTCacheClosed
	}
	id := int(binding.LegID)
	if binding.Generation < c.highWater[id] {
		return ErrStreamRTTStaleGeneration
	}
	if binding.Generation == c.highWater[id] {
		if c.active[id] && c.binding[id] == binding && hmac.Equal(c.probeKey[id][:], auth.probeKey[:]) {
			return nil
		}
		return ErrStreamRTTGenerationReuse
	}

	c.clearLegLocked(id)
	c.highWater[id] = binding.Generation
	c.binding[id] = binding
	c.probeKey[id] = auth.probeKey
	c.active[id] = true
	view := binding
	c.activeView[id].Store(&view)
	return nil
}

// recordHandshakeSample seeds only the authenticated client's complete
// HELLO/READY round trip. Server READY write time is not an RTT sample.
func (c *streamRTTProbeCache) recordHandshakeSample(binding StreamPathBinding, identity StreamPathIdentity, auth StreamPathAuth) bool {
	rtt, observedAt := auth.carrierHandshakeSample()
	if c == nil || int(binding.LegID) >= c.maxLegs || identity.Direction != StreamRTTClientToServer || rtt <= 0 || rtt >= StreamRTTProbeTimeout || observedAt.IsZero() || !auth.validFor(binding) || identity.binding() != binding {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := int(binding.LegID)
	if c.closed || !c.active[id] || c.binding[id] != binding {
		return false
	}
	age := time.Since(observedAt)
	if age < 0 || age > StreamRTTSampleWindow {
		return false
	}
	c.recordSampleLocked(identity, rtt, observedAt)
	return true
}

// InvalidateLeg cancels all pending work and feedback for exactly the active
// carrier. It retains the generation high-water mark to reject delayed binds.
func (c *streamRTTProbeCache) invalidateLeg(binding StreamPathBinding) bool {
	if c == nil || int(binding.LegID) >= c.maxLegs {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := int(binding.LegID)
	if c.closed || !c.active[id] || c.binding[id] != binding {
		return false
	}
	c.activeView[id].Store(nil)
	c.clearLegLocked(id)
	return true
}

// StartProbe creates an authenticated probe with a cache-wide monotonically
// increasing token. Expired pending probes are reclaimed before the per-leg
// limit is checked, so a blackholed Carrier cannot permanently starve probes.
func (c *streamRTTProbeCache) startProbe(identity StreamPathIdentity) (uint64, []byte, error) {
	return c.startProbeAt(identity, time.Now())
}

func (c *streamRTTProbeCache) startProbeAt(identity StreamPathIdentity, now time.Time) (uint64, []byte, error) {
	if c == nil || !validStreamRTTDirection(identity.Direction) || int(identity.LegID) >= c.maxLegs {
		return 0, nil, ErrStreamPathMessage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, nil, ErrStreamRTTCacheClosed
	}
	id := int(identity.LegID)
	if !c.active[id] || c.binding[id] != identity.binding() {
		return 0, nil, ErrStreamRTTStaleGeneration
	}
	c.expirePendingLocked(now)
	pendingForLeg := 0
	for _, pending := range c.pending {
		if pending.identity.LegID == identity.LegID {
			pendingForLeg++
		}
	}
	if pendingForLeg >= StreamRTTProbePendingLimit {
		return 0, nil, ErrStreamRTTPendingFull
	}
	if c.nextToken == math.MaxUint64 {
		return 0, nil, ErrStreamRTTTokenExhausted
	}
	token := c.nextToken + 1
	message := StreamRTTMessage{Kind: StreamRTTProbe, Identity: identity, Token: token}
	wire, err := encodeStreamRTTMessage(message, c.probeKey[id])
	if err != nil {
		return 0, nil, err
	}
	c.nextToken = token
	c.pending[token] = streamRTTPending{identity: identity, started: now}
	return token, wire, nil
}

// CancelProbe retires a pending token without reusing it.
func (c *streamRTTProbeCache) cancelProbe(identity StreamPathIdentity, token uint64) bool {
	if c == nil || int(identity.LegID) >= c.maxLegs || token == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.pending[token]
	if !ok || pending.identity != identity {
		return false
	}
	delete(c.pending, token)
	return true
}

// ExpirePending immediately reclaims probes whose 2-second response deadline
// has elapsed. StartProbe also invokes the same cleanup before admission.
func (c *streamRTTProbeCache) expirePending() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.expirePendingLocked(time.Now())
}

// AcceptProbe authenticates a peer probe for the expected protocol direction
// and returns an authenticated echo. The caller must write the echo on the same
// StreamLeg that delivered the probe.
func (c *streamRTTProbeCache) acceptProbe(binding StreamPathBinding, expectedDirection StreamRTTDirection, wire []byte) ([]byte, error) {
	if c == nil || int(binding.LegID) >= c.maxLegs || !validStreamRTTDirection(expectedDirection) {
		return nil, ErrStreamPathMessage
	}
	id := int(binding.LegID)
	c.mu.Lock()
	if c.closed || !c.active[id] || c.binding[id] != binding {
		c.mu.Unlock()
		return nil, ErrStreamRTTStaleGeneration
	}
	key := c.probeKey[id]
	c.mu.Unlock()
	message, err := decodeStreamRTTMessage(wire, key, binding)
	if err != nil {
		return nil, err
	}
	if message.Kind != StreamRTTProbe || message.Identity.Direction != expectedDirection {
		return nil, ErrStreamPathMessage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.active[id] || c.binding[id] != binding {
		return nil, ErrStreamRTTStaleGeneration
	}
	directionIndex := int(message.Identity.Direction - 1)
	if message.Token <= c.lastPeerToken[id][directionIndex] {
		return nil, ErrStreamRTTReplay
	}
	echo := message
	echo.Kind = StreamRTTEcho
	echoWire, err := encodeStreamRTTMessage(echo, c.probeKey[id])
	if err != nil {
		return nil, err
	}
	c.lastPeerToken[id][directionIndex] = message.Token
	return echoWire, nil
}

// AcceptEcho authenticates and retires one pending probe, then publishes its
// locally measured round-trip time. A late, replayed, old-generation, or
// cross-direction echo cannot change the feedback snapshot.
func (c *streamRTTProbeCache) acceptEcho(identity StreamPathIdentity, wire []byte) error {
	return c.acceptEchoAt(identity, wire, time.Now())
}

func (c *streamRTTProbeCache) acceptEchoAt(identity StreamPathIdentity, wire []byte, now time.Time) error {
	if c == nil || int(identity.LegID) >= c.maxLegs || !validStreamRTTDirection(identity.Direction) {
		return ErrStreamPathMessage
	}
	id := int(identity.LegID)
	c.mu.Lock()
	if c.closed || !c.active[id] || c.binding[id] != identity.binding() {
		c.mu.Unlock()
		return ErrStreamRTTStaleGeneration
	}
	key := c.probeKey[id]
	c.mu.Unlock()
	message, err := decodeStreamRTTMessage(wire, key, identity.binding())
	if err != nil {
		return err
	}
	if message.Kind != StreamRTTEcho || message.Identity.Direction != identity.Direction {
		return ErrStreamPathMessage
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.active[id] || c.binding[id] != identity.binding() {
		return ErrStreamRTTStaleGeneration
	}
	pending, ok := c.pending[message.Token]
	if !ok || pending.identity != identity {
		return ErrStreamRTTNoPending
	}
	delete(c.pending, message.Token)
	if now.Before(pending.started) || now.Sub(pending.started) >= StreamRTTProbeTimeout {
		return ErrStreamRTTProbeExpired
	}
	rtt := now.Sub(pending.started)
	if rtt <= 0 {
		// Some host clocks quantize a same-process Carrier round trip to zero.
		// Retire the token but do not treat an unmeasurable sample as a protocol
		// failure: a later fresh probe can produce a usable RTT.
		return nil
	}
	c.recordSampleLocked(identity, rtt, now)
	return nil
}

// PathFeedback reads a fresh immutable snapshot without taking the mutation
// mutex. At most five sample timestamps are filtered and sorted per call.
func (c *streamRTTProbeCache) PathFeedback(identity StreamPathIdentity) (StreamPathFeedback, bool) {
	return c.pathFeedbackAt(identity, time.Now())
}

func (c *streamRTTProbeCache) pathFeedbackAt(identity StreamPathIdentity, now time.Time) (StreamPathFeedback, bool) {
	if c == nil || int(identity.LegID) >= c.maxLegs || !validStreamRTTDirection(identity.Direction) {
		return StreamPathFeedback{}, false
	}
	id := int(identity.LegID)
	active := c.activeView[id].Load()
	if active == nil || *active != identity.binding() {
		return StreamPathFeedback{}, false
	}
	snapshot := c.feedback[id][int(identity.Direction-1)].Load()
	if snapshot == nil || snapshot.identity != identity {
		return StreamPathFeedback{}, false
	}
	var recent [streamRTTMaxSamples]time.Duration
	count := 0
	var newest time.Time
	for i := 0; i < snapshot.count; i++ {
		sample := snapshot.samples[i]
		age := now.Sub(sample.at)
		if age < 0 || age > StreamRTTSampleWindow {
			continue
		}
		recent[count] = sample.rtt
		count++
		if sample.at.After(newest) {
			newest = sample.at
		}
	}
	if count == 0 || c.activeView[id].Load() != active {
		return StreamPathFeedback{}, false
	}
	for i := 1; i < count; i++ {
		value := recent[i]
		j := i
		for j > 0 && recent[j-1] > value {
			recent[j] = recent[j-1]
			j--
		}
		recent[j] = value
	}
	median := recent[count/2]
	if count%2 == 0 {
		low := recent[count/2-1]
		median = low + (recent[count/2]-low)/2
	}
	return StreamPathFeedback{Identity: identity, RTT: median, Samples: uint32(count), ObservedAt: newest}, true
}

func (c *streamRTTProbeCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for id := range c.active {
		c.activeView[id].Store(nil)
		c.clearLegLocked(id)
	}
	c.pending = nil
}

func (c *streamRTTProbeCache) recordSampleLocked(identity StreamPathIdentity, rtt time.Duration, now time.Time) {
	id := int(identity.LegID)
	if id >= c.maxLegs {
		return
	}
	direction := int(identity.Direction - 1)
	current := c.samples[id][direction]
	// A handshake observation can be published after later probe replies. Use
	// the newest observation as the window edge so a delayed sample cannot
	// make newer samples look like future data and erase them.
	windowEnd := now
	for _, sample := range current {
		if sample.at.After(windowEnd) {
			windowEnd = sample.at
		}
	}
	recent := make([]streamRTTSample, 0, streamRTTMaxSamples)
	for _, sample := range current {
		age := windowEnd.Sub(sample.at)
		if age >= 0 && age <= StreamRTTSampleWindow {
			recent = append(recent, sample)
		}
	}
	if age := windowEnd.Sub(now); age <= StreamRTTSampleWindow {
		recent = append(recent, streamRTTSample{rtt: rtt, at: now})
	}
	for i := 1; i < len(recent); i++ {
		value := recent[i]
		j := i
		for j > 0 && recent[j-1].at.After(value.at) {
			recent[j] = recent[j-1]
			j--
		}
		recent[j] = value
	}
	if len(recent) > streamRTTMaxSamples {
		recent = recent[len(recent)-streamRTTMaxSamples:]
	}
	c.samples[id][direction] = recent
	published := &streamRTTPublished{identity: identity, count: len(recent)}
	copy(published.samples[:], recent)
	c.feedback[id][direction].Store(published)
}

func (c *streamRTTProbeCache) expirePendingLocked(now time.Time) int {
	removed := 0
	for token, pending := range c.pending {
		if !now.Before(pending.started) && now.Sub(pending.started) >= StreamRTTProbeTimeout {
			delete(c.pending, token)
			removed++
		}
	}
	return removed
}

func (c *streamRTTProbeCache) clearLegLocked(id int) {
	for token, pending := range c.pending {
		if int(pending.identity.LegID) == id {
			delete(c.pending, token)
		}
	}
	c.active[id] = false
	c.binding[id] = StreamPathBinding{}
	c.probeKey[id] = [sha256.Size]byte{}
	c.lastPeerToken[id] = [2]uint64{}
	for direction := 0; direction < 2; direction++ {
		c.samples[id][direction] = nil
		c.feedback[id][direction].Store(nil)
	}
}

func validStreamRTTDirection(direction StreamRTTDirection) bool {
	return direction == StreamRTTClientToServer || direction == StreamRTTServerToClient
}

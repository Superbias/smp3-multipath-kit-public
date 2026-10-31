package smp3core

import (
	"sync"
	"time"
)

// StreamCapacityProvider is the shared observation surface consumed by a
// dynamic aggregation scheduler. StreamEngine calls Ack only for logical
// cumulative-ACK retirement, so retransmits and rescue attempts are not
// counted as useful service twice.
type StreamCapacityProvider interface {
	Reset(id uint8, now time.Time)
	Admit(id uint8, bytes uint64, saturated bool, now time.Time)
	Ack(id uint8, bytes uint64, now time.Time)
	Weight(id uint8, baseMbps float64) float64
	Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time)
}

// StreamCapacityLegTelemetry is the N-leg capacity snapshot. Legacy providers
// may implement only StreamCapacityProvider; the Core mirrors the first two
// legs into the original fixed-size telemetry API in that case.
type StreamCapacityLegTelemetry struct {
	LegID                           uint8
	RawBPS, EstimateBPS, Confidence float64
	Valid, Demand                   bool
	SampledAt                       time.Time
	SampleReason                    string
}

type StreamCapacityProviderN interface {
	TelemetryByLeg() []StreamCapacityLegTelemetry
	SampleReasonByLeg() []string
}

// StreamActivationProvider is an optional extension used by capacity-relative
// leg activation. It is deliberately separate from StreamCapacityProvider so
// existing provider implementations and compatibility callers remain valid.
type StreamActivationProvider interface {
	ActivationCapacity() (bytesPerSecond, confidence float64, valid bool)
	ObserveActivationDemand(produced, retired uint64, now time.Time)
	ActivationDemand(now time.Time) (bytesPerSecond float64, backlog uint64)
}

// CarrierCapacityRegistry owns shared state for one ownership domain (usually
// one standalone client process). It is deliberately instantiated by a host,
// rather than package-global, so independent clients and servers cannot mix.
// Keys supplied by the host must include owner, direction, and physical
// carrier identity.
type CarrierCapacityRegistry struct {
	mu      sync.Mutex
	entries map[string]*carrierCapacityEntry
}

const carrierCapacityIdleTTL = 10 * time.Minute
const carrierCapacityMaxEntries = 1024

type carrierCapacityEntry struct {
	estimator   *streamCapacityEstimator
	refs        int
	lastUsed    time.Time
	demandStart time.Time
	demandAt    time.Time
	demandBytes uint64
	backlog     uint64
}

func NewCarrierCapacityRegistry() *CarrierCapacityRegistry {
	return &CarrierCapacityRegistry{entries: make(map[string]*carrierCapacityEntry)}
}

// Provider returns a two-leg view backed by the registry. An empty key creates
// a private estimator for that leg and is useful for compatibility callers.
func (r *CarrierCapacityRegistry) Provider(keys [2]string, weights []uint32, chunk int) StreamCapacityProvider {
	return r.ProviderForLegs(keys[:], weights, chunk, 2)
}

// ProviderForLegs returns capacity state for a configured N-leg stream. Keys
// and weights are indexed by protocol LegID and must describe contiguous legs.
func (r *CarrierCapacityRegistry) ProviderForLegs(keys []string, weights []uint32, chunk, legCount int) StreamCapacityProvider {
	if legCount < 1 || legCount > MaxStreamLegIDCount {
		legCount = 2
	}
	if len(keys) < legCount {
		padded := make([]string, legCount)
		copy(padded, keys)
		keys = padded
	}
	if r == nil {
		return newStreamCapacityEstimatorForLegs(weights, chunk, legCount)
	}
	states := make([]*streamCapacityEstimator, legCount)
	keyCopy := append([]string(nil), keys[:legCount]...)
	r.mu.Lock()
	r.pruneLocked(time.Now())
	for id, key := range keyCopy {
		if key == "" {
			states[id] = newStreamCapacityEstimatorForLegs(weightsForLeg(weights, id), chunk, 1)
			continue
		}
		entry := r.entries[key]
		if entry == nil {
			entry = &carrierCapacityEntry{estimator: newStreamCapacityEstimatorForLegs(weightsForLeg(weights, id), chunk, 1)}
			r.entries[key] = entry
		}
		entry.refs++
		entry.lastUsed = time.Now()
		states[id] = entry.estimator
	}
	r.pruneLocked(time.Now())
	r.mu.Unlock()
	return &carrierCapacityProvider{states: states, registry: r, keys: keyCopy}
}

func (r *CarrierCapacityRegistry) pruneLocked(now time.Time) {
	for key, entry := range r.entries {
		if entry.refs == 0 && !entry.lastUsed.IsZero() && now.Sub(entry.lastUsed) > carrierCapacityIdleTTL {
			delete(r.entries, key)
		}
	}
	if len(r.entries) <= carrierCapacityMaxEntries {
		return
	}
	for key, entry := range r.entries {
		if entry.refs == 0 {
			delete(r.entries, key)
			if len(r.entries) <= carrierCapacityMaxEntries {
				break
			}
		}
	}
}

func weightsForLeg(weights []uint32, id int) []uint32 {
	if id < 0 || id >= len(weights) || weights[id] == 0 {
		return []uint32{1}
	}
	return []uint32{weights[id]}
}

type carrierCapacityProvider struct {
	mu         sync.Mutex // lifecycle and this provider's unfinished logical demand
	closed     bool
	unfinished uint64
	states     []*streamCapacityEstimator
	registry   *CarrierCapacityRegistry
	keys       []string
	once       sync.Once
}

func (p *carrierCapacityProvider) demandEntry() *carrierCapacityEntry {
	if p == nil || p.registry == nil || p.keys[0] == "" {
		return nil
	}
	p.registry.mu.Lock()
	entry := p.registry.entries[p.keys[0]]
	p.registry.mu.Unlock()
	return entry
}

func (p *carrierCapacityProvider) ActivationCapacity() (bytesPerSecond, confidence float64, valid bool) {
	if p == nil || p.states[0] == nil {
		return
	}
	_, estimate, confidenceValues, validValues, _, _ := p.states[0].telemetry()
	return estimate[0], confidenceValues[0], validValues[0]
}

func (p *carrierCapacityProvider) ObserveActivationDemand(produced, retired uint64, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	entry := p.demandEntry()
	if entry == nil || (produced == 0 && retired == 0) {
		return
	}
	p.registry.mu.Lock()
	defer p.registry.mu.Unlock()
	// Re-resolve under the registry lock; pruning can race with a stream that
	// has just released its final reference.
	entry = p.registry.entries[p.keys[0]]
	if entry == nil {
		return
	}
	if entry.demandStart.IsZero() {
		entry.demandStart = now
	}
	if !entry.demandAt.IsZero() && now.Sub(entry.demandAt) > 2*time.Second {
		entry.demandStart, entry.demandBytes = now, 0
	}
	entry.demandAt = now
	entry.demandBytes += produced
	if retired > p.unfinished {
		retired = p.unfinished
	}
	p.unfinished = p.unfinished - retired + produced
	if retired >= entry.backlog {
		entry.backlog = 0
	} else {
		entry.backlog -= retired
	}
	entry.backlog += produced
}

func (p *carrierCapacityProvider) ActivationDemand(now time.Time) (bytesPerSecond float64, backlog uint64) {
	entry := p.demandEntry()
	if entry == nil {
		return
	}
	p.registry.mu.Lock()
	defer p.registry.mu.Unlock()
	entry = p.registry.entries[p.keys[0]]
	if entry == nil {
		return
	}
	backlog = entry.backlog
	if entry.demandAt.IsZero() || now.Sub(entry.demandAt) > 500*time.Millisecond {
		return 0, backlog
	}
	if entry.demandStart.IsZero() || entry.demandBytes == 0 {
		return 0, backlog
	}
	elapsed := now.Sub(entry.demandStart)
	if elapsed <= 0 {
		return 0, backlog
	}
	if elapsed > 2*time.Second {
		return 0, backlog
	}
	return float64(entry.demandBytes) / elapsed.Seconds(), backlog
}

func (p *carrierCapacityProvider) Reset(id uint8, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if int(id) < len(p.states) && p.states[id] != nil {
		state := p.states[id]
		state.mu.Lock()
		if state.legs[0].windowStart.IsZero() {
			state.legs[0].windowStart = now
			state.legs[0].sampleReason = "cold_start"
		}
		state.mu.Unlock()
	}
}

func (p *carrierCapacityProvider) Admit(id uint8, bytes uint64, saturated bool, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if int(id) < len(p.states) && p.states[id] != nil {
		p.states[id].admit(0, bytes, saturated, now)
	}
}

func (p *carrierCapacityProvider) Ack(id uint8, bytes uint64, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if int(id) < len(p.states) && p.states[id] != nil {
		p.states[id].ack(0, bytes, now)
	}
}

func (p *carrierCapacityProvider) Weight(id uint8, base float64) float64 {
	if int(id) >= len(p.states) || p.states[id] == nil {
		return base
	}
	return p.states[id].weight(0, base)
}

func (p *carrierCapacityProvider) Recover(id uint8, seedMbps float64, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if int(id) < len(p.states) && p.states[id] != nil {
		p.states[id].Recover(0, seedMbps, now)
	}
}

func (p *carrierCapacityProvider) SampleReason() (reason [2]string) {
	for id, state := range p.states {
		if id >= len(reason) {
			break
		}
		if state != nil {
			reason[id] = state.SampleReason()[0]
		}
	}
	return
}

func (p *carrierCapacityProvider) SampleReasonByLeg() []string {
	if p == nil {
		return nil
	}
	reason := make([]string, len(p.states))
	for id, state := range p.states {
		if state != nil {
			reason[id] = state.SampleReason()[0]
		}
	}
	return reason
}

func (p *carrierCapacityProvider) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	for id, state := range p.states {
		if id >= len(raw) {
			break
		}
		if state == nil {
			continue
		}
		r, e, c, v, d, s := state.telemetry()
		raw[id], estimate[id], confidence[id], valid[id], demand[id], sampled[id] = r[0], e[0], c[0], v[0], d[0], s[0]
	}
	return
}

func (p *carrierCapacityProvider) TelemetryByLeg() []StreamCapacityLegTelemetry {
	if p == nil {
		return nil
	}
	rows := make([]StreamCapacityLegTelemetry, len(p.states))
	for id, state := range p.states {
		rows[id].LegID = uint8(id)
		if state == nil {
			continue
		}
		r, e, c, v, d, s := state.telemetry()
		rows[id].RawBPS, rows[id].EstimateBPS, rows[id].Confidence = r[0], e[0], c[0]
		rows[id].Valid, rows[id].Demand, rows[id].SampledAt = v[0], d[0], s[0]
		rows[id].SampleReason = state.SampleReason()[0]
	}
	return rows
}

// Close drops stream references while retaining the estimate for warm reuse.
// Registry state is bounded by the configured carrier set; hosts may recreate
// a registry to discard all learned state.
func (p *carrierCapacityProvider) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.closed = true
		if p.registry == nil {
			return
		}
		p.registry.mu.Lock()
		defer p.registry.mu.Unlock()
		if entry := p.registry.entries[p.keys[0]]; entry != nil {
			if p.unfinished >= entry.backlog {
				entry.backlog = 0
			} else {
				entry.backlog -= p.unfinished
			}
		}
		p.unfinished = 0
		for _, key := range p.keys {
			if key == "" {
				continue
			}
			if entry := p.registry.entries[key]; entry != nil {
				if entry.refs > 0 {
					entry.refs--
				}
				entry.lastUsed = time.Now()
			}
		}
	})
}

func (e *streamCapacityEstimator) windowStartEmpty() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.legs[0].windowStart.IsZero()
}

var _ StreamCapacityProvider = (*streamCapacityEstimator)(nil)
var _ StreamCapacityProvider = (*carrierCapacityProvider)(nil)
var _ StreamActivationProvider = (*carrierCapacityProvider)(nil)

func (e *streamCapacityEstimator) Reset(id uint8, now time.Time) { e.reset(id, now) }
func (e *streamCapacityEstimator) Admit(id uint8, bytes uint64, saturated bool, now time.Time) {
	e.admit(id, bytes, saturated, now)
}
func (e *streamCapacityEstimator) Ack(id uint8, bytes uint64, now time.Time) { e.ack(id, bytes, now) }
func (e *streamCapacityEstimator) Weight(id uint8, base float64) float64     { return e.weight(id, base) }
func (e *streamCapacityEstimator) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	return e.telemetry()
}

func (e *streamCapacityEstimator) TelemetryByLeg() []StreamCapacityLegTelemetry {
	return e.telemetryByLeg()
}
func (e *streamCapacityEstimator) SampleReasonByLeg() []string { return e.sampleReasonByLeg() }

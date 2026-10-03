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
	estimator *streamCapacityEstimator
	refs      int
	lastUsed  time.Time
}

func NewCarrierCapacityRegistry() *CarrierCapacityRegistry {
	return &CarrierCapacityRegistry{entries: make(map[string]*carrierCapacityEntry)}
}

// Provider returns a two-leg view backed by the registry. An empty key creates
// a private estimator for that leg and is useful for compatibility callers.
func (r *CarrierCapacityRegistry) Provider(keys [2]string, weights []uint32, chunk int) StreamCapacityProvider {
	if r == nil {
		return newStreamCapacityEstimator(weights, chunk)
	}
	states := [2]*streamCapacityEstimator{}
	r.mu.Lock()
	r.pruneLocked(time.Now())
	for id, key := range keys {
		if key == "" {
			states[id] = newStreamCapacityEstimator(weightsForLeg(weights, id), chunk)
			continue
		}
		entry := r.entries[key]
		if entry == nil {
			entry = &carrierCapacityEntry{estimator: newStreamCapacityEstimator(weightsForLeg(weights, id), chunk)}
			r.entries[key] = entry
		}
		entry.refs++
		entry.lastUsed = time.Now()
		states[id] = entry.estimator
	}
	r.pruneLocked(time.Now())
	r.mu.Unlock()
	return &carrierCapacityProvider{states: states, registry: r, keys: keys}
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
	states   [2]*streamCapacityEstimator
	registry *CarrierCapacityRegistry
	keys     [2]string
	once     sync.Once
}

func (p *carrierCapacityProvider) Reset(id uint8, now time.Time) {
	if id < 2 && p.states[id] != nil && p.states[id].windowStartEmpty() {
		p.states[id].reset(0, now)
	}
}

func (p *carrierCapacityProvider) Admit(id uint8, bytes uint64, saturated bool, now time.Time) {
	if id < 2 && p.states[id] != nil {
		p.states[id].admit(0, bytes, saturated, now)
	}
}

func (p *carrierCapacityProvider) Ack(id uint8, bytes uint64, now time.Time) {
	if id < 2 && p.states[id] != nil {
		p.states[id].ack(0, bytes, now)
	}
}

func (p *carrierCapacityProvider) Weight(id uint8, base float64) float64 {
	if id >= 2 || p.states[id] == nil {
		return base
	}
	return p.states[id].weight(0, base)
}

func (p *carrierCapacityProvider) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	for id, state := range p.states {
		if state == nil {
			continue
		}
		r, e, c, v, d, s := state.telemetry()
		raw[id], estimate[id], confidence[id], valid[id], demand[id], sampled[id] = r[0], e[0], c[0], v[0], d[0], s[0]
	}
	return
}

// Close drops stream references while retaining the estimate for warm reuse.
// Registry state is bounded by the configured carrier set; hosts may recreate
// a registry to discard all learned state.
func (p *carrierCapacityProvider) Close() {
	p.once.Do(func() {
		if p.registry == nil {
			return
		}
		p.registry.mu.Lock()
		defer p.registry.mu.Unlock()
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

func (e *streamCapacityEstimator) Reset(id uint8, now time.Time) { e.reset(id, now) }
func (e *streamCapacityEstimator) Admit(id uint8, bytes uint64, saturated bool, now time.Time) {
	e.admit(id, bytes, saturated, now)
}
func (e *streamCapacityEstimator) Ack(id uint8, bytes uint64, now time.Time) { e.ack(id, bytes, now) }
func (e *streamCapacityEstimator) Weight(id uint8, base float64) float64     { return e.weight(id, base) }
func (e *streamCapacityEstimator) Telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	return e.telemetry()
}

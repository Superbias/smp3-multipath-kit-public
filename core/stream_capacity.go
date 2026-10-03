package smp3core

import (
	"math"
	"sync"
	"time"
)

// StreamCapacityMode controls the source of aggregation weights. Fixed is the
// compatibility default; Dynamic is explicit opt-in and only has an effect in
// StreamSchedulerAggregation.
type StreamCapacityMode uint8

const (
	StreamCapacityFixed StreamCapacityMode = iota
	StreamCapacityDynamic
)

type streamCapacityLeg struct {
	baseline, raw, estimate float64 // bytes per second
	confidence              float64
	windowStart, lastValid  time.Time
	windowBytes             uint64
	windowAdmittedBytes     uint64
	windowSaturatedBytes    uint64
	demand                  bool
	valid                   bool
	samples                 uint64
}

type streamCapacityEstimator struct {
	mu       sync.Mutex
	legs     [2]streamCapacityLeg
	minBytes uint64
	window   time.Duration
}

func newStreamCapacityEstimator(weights []uint32, chunk int) *streamCapacityEstimator {
	e := &streamCapacityEstimator{window: time.Second}
	if chunk <= 0 {
		chunk = 64 * 1024
	}
	e.minBytes = uint64(chunk * 4)
	for i := range e.legs {
		mbps := uint32(1)
		if i < len(weights) && weights[i] > 0 {
			mbps = weights[i]
		}
		e.legs[i].baseline = float64(mbps) * 1e6 / 8
		e.legs[i].estimate = e.legs[i].baseline
	}
	return e
}

func (e *streamCapacityEstimator) reset(id uint8, now time.Time) {
	if e == nil || id >= 2 {
		return
	}
	e.mu.Lock()
	leg := &e.legs[id]
	leg.raw, leg.estimate, leg.confidence = 0, leg.baseline, 0
	leg.windowStart, leg.lastValid = now, time.Time{}
	leg.windowBytes, leg.windowAdmittedBytes, leg.windowSaturatedBytes = 0, 0, 0
	leg.demand, leg.valid, leg.samples = false, false, 0
	e.mu.Unlock()
}

func (e *streamCapacityEstimator) admit(id uint8, bytes uint64, saturated bool, now time.Time) {
	if e == nil || id >= 2 || bytes == 0 {
		return
	}
	e.mu.Lock()
	leg := &e.legs[id]
	if leg.windowStart.IsZero() {
		leg.windowStart = now
	}
	leg.windowAdmittedBytes += bytes
	if saturated {
		leg.demand = true
		leg.windowSaturatedBytes += bytes
	}
	e.mu.Unlock()
}

func (e *streamCapacityEstimator) ack(id uint8, bytes uint64, now time.Time) {
	if e == nil || id >= 2 || bytes == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	leg := &e.legs[id]
	if leg.windowStart.IsZero() {
		leg.windowStart = now
	}
	leg.windowBytes += bytes
	if now.Sub(leg.windowStart) < e.window {
		return
	}
	// A single full queue observation is not enough to declare a path
	// capacity-limited. Require meaningful saturated service opportunity in the
	// same window; this prevents scheduler under-service from teaching the
	// estimator an artificially low capacity.
	saturatedEnough := leg.windowSaturatedBytes >= e.minBytes &&
		(leg.windowAdmittedBytes == 0 || leg.windowSaturatedBytes*5 >= leg.windowAdmittedBytes*4)
	if leg.demand && saturatedEnough && leg.windowBytes >= e.minBytes {
		raw := float64(leg.windowBytes) / now.Sub(leg.windowStart).Seconds()
		if raw > 0 && math.IsInf(raw, 0) == false && math.IsNaN(raw) == false {
			leg.raw = raw
			if leg.samples == 0 {
				leg.estimate = raw
			} else {
				leg.estimate = leg.estimate*0.4 + raw*0.6
			}
			leg.confidence += (1 - leg.confidence) * 0.45
			if leg.confidence > 1 {
				leg.confidence = 1
			}
			leg.lastValid, leg.valid = now, true
			leg.samples++
		}
	} else if leg.valid && !leg.demand && leg.windowBytes >= e.minBytes {
		// Under-demand alone is not evidence for a higher capacity. Reopen a
		// previously reduced estimate with a slow passive probe when the observed
		// ACK rate is not materially below it. A recovered path can be scheduled
		// below its true capacity, so waiting for raw > estimate*1.05 forever
		// would permanently strand it at the old estimate. Saturated windows
		// remain authoritative and can pull the estimate down again.
		raw := float64(leg.windowBytes) / now.Sub(leg.windowStart).Seconds()
		if raw > 0 && raw >= leg.estimate*0.8 {
			leg.raw = raw
			// Require a substantial fraction of the configured prior before
			// reopening. A persistently 80 Mbps path with a 200 Mbps prior must
			// not drift upward merely because its reduced entitlement is no
			// longer queue-saturated, while a recovered 200 Mbps path can probe.
			if leg.baseline > leg.estimate && raw >= leg.baseline*0.70 {
				// Probe at most 15% of the remaining prior gap per window.
				leg.estimate += (leg.baseline - leg.estimate) * 0.15
			} else if raw > leg.estimate*1.05 {
				if raw > leg.baseline {
					raw = leg.baseline
				}
				leg.estimate = leg.estimate*0.4 + raw*0.6
			}
		}
	}
	// Demand-limited and idle windows deliberately freeze the previous estimate.
	leg.windowStart, leg.windowBytes, leg.windowAdmittedBytes, leg.windowSaturatedBytes, leg.demand = now, 0, 0, 0, false
}

func (e *streamCapacityEstimator) weight(id uint8, base float64) float64 {
	if e == nil || id >= 2 {
		return base
	}
	e.mu.Lock()
	leg := e.legs[id]
	w := leg.baseline*(1-leg.confidence) + leg.estimate*leg.confidence
	e.mu.Unlock()
	if w <= 0 || math.IsNaN(w) || math.IsInf(w, 0) {
		return base
	}
	// Keep the estimator from making an abrupt scheduler jump on one sample.
	baseBPS := base * 1e6 / 8
	if baseBPS > 0 {
		if w > baseBPS*4 {
			w = baseBPS * 4
		}
	}
	if w < 0.1*1e6/8 {
		w = 0.1 * 1e6 / 8
	}
	return w * 8 / 1e6
}

func (e *streamCapacityEstimator) snapshot() (raw, estimate, confidence [2]float64, valid [2]bool) {
	if e == nil {
		return
	}
	e.mu.Lock()
	for i, leg := range e.legs {
		raw[i], estimate[i], confidence[i], valid[i] = leg.raw, leg.estimate, leg.confidence, leg.valid
	}
	e.mu.Unlock()
	return
}

func (e *streamCapacityEstimator) telemetry() (raw, estimate, confidence [2]float64, valid, demand [2]bool, sampled [2]time.Time) {
	if e == nil {
		return
	}
	e.mu.Lock()
	for i, leg := range e.legs {
		raw[i], estimate[i], confidence[i], valid[i] = leg.raw, leg.estimate, leg.confidence, leg.valid
		demand[i], sampled[i] = leg.demand, leg.lastValid
	}
	e.mu.Unlock()
	return
}

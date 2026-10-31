package smp3core

import (
	"sync"
	"time"
)

type streamLoadProbe struct {
	inServiceScore            bool
	mu                        sync.Mutex
	queued                    []int64
	inService                 []int64
	assigned                  []uint64
	counts                    []uint64
	samples                   uint64
	divergence                uint64
	lowWhenHole, lowWhenClear uint64
	holeTimeNs                []int64
	holeSince                 []time.Time
}

func (p *streamLoadProbe) ensureLegCapacityLocked(size int) {
	if size <= len(p.queued) {
		return
	}
	queued := make([]int64, size)
	copy(queued, p.queued)
	p.queued = queued
	inService := make([]int64, size)
	copy(inService, p.inService)
	p.inService = inService
	assigned := make([]uint64, size)
	copy(assigned, p.assigned)
	p.assigned = assigned
	counts := make([]uint64, size)
	copy(counts, p.counts)
	p.counts = counts
	holeTimeNs := make([]int64, size)
	copy(holeTimeNs, p.holeTimeNs)
	p.holeTimeNs = holeTimeNs
	holeSince := make([]time.Time, size)
	copy(holeSince, p.holeSince)
	p.holeSince = holeSince
}

func (p *streamLoadProbe) assignedRecord(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureLegCapacityLocked(int(id) + 1)
	p.queued[id] += int64(bytes)
	p.assigned[id] += uint64(bytes)
	p.counts[id]++
}

func (p *streamLoadProbe) dequeue(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureLegCapacityLocked(int(id) + 1)
	p.queued[id] -= int64(bytes)
	p.inService[id] += int64(bytes)
	if p.inService[id] > 0 && p.holeSince[id].IsZero() {
		p.holeSince[id] = time.Now()
	}
}
func (p *streamLoadProbe) complete(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureLegCapacityLocked(int(id) + 1)
	p.inService[id] -= int64(bytes)
	if p.inService[id] <= 0 {
		if !p.holeSince[id].IsZero() {
			d := time.Since(p.holeSince[id])
			p.holeTimeNs[id] += d.Nanoseconds()
		}
		p.holeSince[id] = time.Time{}
	}
}

func (p *streamLoadProbe) choose(c *StreamEngine, payload int) *streamLeg {
	legs := c.availableLegs()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureLegCapacityLocked(c.maxActiveLegs)
	var best, counter *streamLeg
	bestScore, counterScore := 1e300, 1e300
	for _, leg := range legs {
		weight := c.effectiveSchedulerWeight(leg)
		if weight <= 0 {
			continue
		}
		currentLoad := float64(c.txSentBytes[leg.id].Load()) + float64((len(leg.send)+1)*c.cfg.ChunkSize)
		observed := float64(c.txSentBytes[leg.id].Load()) + float64(p.queued[leg.id]+p.inService[leg.id]+int64(payload))
		if currentLoad/weight < bestScore {
			bestScore = currentLoad / weight
			best = leg
		}
		if observed/weight < counterScore {
			counterScore = observed / weight
			counter = leg
		}
	}
	p.samples++
	if best != counter {
		p.divergence++
		if best != nil && counter != nil && best.id == 0 && counter.id == 1 {
			p.lowWhenHole++
		}
	}
	if best != nil && p.inService[best.id] > 0 {
		if best.id == 0 {
			p.lowWhenHole++
		}
	} else {
		p.lowWhenClear++
	}
	if p.inServiceScore {
		return counter
	}
	return best
}

func (p *streamLoadProbe) snapshotByLeg() (samples, divergence, lowHole, lowClear uint64, holeTimeNs []int64, assigned, counts []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.samples, p.divergence, p.lowWhenHole, p.lowWhenClear,
		append([]int64(nil), p.holeTimeNs...), append([]uint64(nil), p.assigned...), append([]uint64(nil), p.counts...)
}

// snapshot preserves the historic two-leg benchmark fields as a compatibility
// view. New N-leg diagnostics should use snapshotByLeg.
func (p *streamLoadProbe) snapshot() (samples, divergence, lowHole, lowClear uint64, lowNs, highNs int64, assigned [2]uint64, counts [2]uint64) {
	samples, divergence, lowHole, lowClear, holeTimeNs, assignedByLeg, countsByLeg := p.snapshotByLeg()
	if len(holeTimeNs) > 0 {
		lowNs = holeTimeNs[0]
	}
	for id, value := range holeTimeNs {
		if id > 0 {
			highNs += value
		}
	}
	copy(assigned[:], assignedByLeg)
	copy(counts[:], countsByLeg)
	return samples, divergence, lowHole, lowClear, lowNs, highNs, assigned, counts
}

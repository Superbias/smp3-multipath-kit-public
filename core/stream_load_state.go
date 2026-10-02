package smp3core

import (
	"sync"
	"time"
)

type streamLoadProbe struct {
	inServiceScore                bool
	mu                            sync.Mutex
	queued                        [2]int64
	inService                     [2]int64
	assigned                      [2]uint64
	counts                        [2]uint64
	samples                       uint64
	divergence                    uint64
	lowWhenHole, lowWhenClear     uint64
	lowHoleTimeNs, highHoleTimeNs int64
	holeSince                     [2]time.Time
}

func (p *streamLoadProbe) assignedRecord(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queued[id] += int64(bytes)
	p.assigned[id] += uint64(bytes)
	p.counts[id]++
}

func (p *streamLoadProbe) dequeue(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queued[id] -= int64(bytes)
	p.inService[id] += int64(bytes)
	if p.inService[id] > 0 && p.holeSince[id].IsZero() {
		p.holeSince[id] = time.Now()
	}
}
func (p *streamLoadProbe) complete(id uint8, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inService[id] -= int64(bytes)
	if p.inService[id] <= 0 {
		if !p.holeSince[id].IsZero() {
			d := time.Since(p.holeSince[id])
			if id == 0 {
				p.lowHoleTimeNs += d.Nanoseconds()
			} else {
				p.highHoleTimeNs += d.Nanoseconds()
			}
		}
		p.holeSince[id] = time.Time{}
	}
}

func (p *streamLoadProbe) choose(c *StreamEngine, payload int) *streamLeg {
	legs := c.availableLegs()
	p.mu.Lock()
	defer p.mu.Unlock()
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

func (p *streamLoadProbe) snapshot() (samples, divergence, lowHole, lowClear uint64, lowNs, highNs int64, assigned [2]uint64, counts [2]uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.samples, p.divergence, p.lowWhenHole, p.lowWhenClear, p.lowHoleTimeNs, p.highHoleTimeNs, p.assigned, p.counts
}

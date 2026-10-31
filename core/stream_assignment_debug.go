package smp3core

import (
	"math"
	"sync/atomic"
	"time"
)

// benchmarkAssignment belongs to one existing ledger record. Its pointer is
// published atomically because ACK/retry can inspect a record before the planner
// assigns it. All owner/accounting transitions use weightedMu; admitted is the
// conservative boundary beyond which recovery belongs to the existing ledger.
type benchmarkAssignment struct {
	owner    *streamLeg // weightedMu
	admitted atomic.Bool
}

// Bounded reconnect trace, guarded by weightedMu with the assigned counters.
type benchmarkAssignmentTrace struct {
	armed   bool
	samples []benchmarkAssignedSample
}

type benchmarkAssignedSample struct {
	Leg   uint8
	Bytes [2]uint64
	Debt  float64
}

type benchmarkAssignmentStats struct {
	initial       atomic.Uint64
	rollback      atomic.Uint64
	reassigned    atomic.Uint64
	reassignments atomic.Uint64
	admitted      []atomic.Uint64
	feederHeld    []atomic.Int64
	feedersDone   []chan struct{}
}

const (
	holFrontierDistance    = 8
	holCandidateStride     = 8
	holMaxCorrectionStreak = 4
	holMinimumLead         = 2 * time.Millisecond
	holRelativeLead        = 0.10
)

// completionEstimate is deliberately a bounded, event-driven estimate. It
// uses only bytes already visible to this stream (pending queue plus sent but
// not cumulatively ACKed bytes), the current normalized service weight, and
// the existing smoothed write latency as a transport-latency proxy.
func (c *StreamEngine) completionEstimate(leg *streamLeg, payload int) float64 {
	if leg == nil {
		return math.MaxFloat64
	}
	weight := c.effectiveSchedulerWeight(leg)
	if weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return math.MaxFloat64
	}
	bps := weight * 1e6 / 8
	if bps <= 0 {
		return math.MaxFloat64
	}
	pending := c.pendingDepth[leg.id].Load()
	if pending < 0 {
		pending = 0
	}
	queued := uint64(pending) * uint64(c.cfg.ChunkSize)
	sent := c.txSentBytes[leg.id].Load()
	acked := c.txAckedUseful[leg.id].Load()
	inflight := uint64(0)
	if sent > acked {
		inflight = sent - acked
	}
	writeBPS, ackedBPS, latency := leg.perf.snapshot()
	if latency <= 0 {
		// A short-lived leg may not have a write sample yet. The configured
		// retransmit budget is a conservative bounded proxy, never a timer.
		if writeBPS <= 0 && ackedBPS <= 0 {
			latency = 0
		}
	}
	if payload < 0 {
		payload = 0
	}
	return float64(queued+inflight+uint64(payload))/bps + latency.Seconds()
}

func (c *StreamEngine) holCorrection(r *StreamTXRecord, baseline *streamLeg, legs []*streamLeg) *streamLeg {
	if c.cfg.HOLMode != StreamHOLCompletion || r == nil || baseline == nil || len(legs) < 2 {
		return baseline
	}
	frontierSequence := c.txAckedNext.Load()
	if r.Sequence() < frontierSequence {
		return baseline
	}
	if r.Sequence()-frontierSequence > holFrontierDistance {
		return baseline
	}
	c.holFrontierCandidates.Add(1)
	baseEstimate := c.completionEstimate(baseline, len(r.Payload()))
	best := baseline
	bestEstimate := baseEstimate
	for _, leg := range legs {
		if leg == nil || leg.closed.Load() || leg == baseline {
			continue
		}
		estimate := c.completionEstimate(leg, len(r.Payload()))
		if estimate < bestEstimate {
			best, bestEstimate = leg, estimate
		}
	}
	if best == baseline || baseEstimate-bestEstimate < holMinimumLead.Seconds() || bestEstimate >= baseEstimate*(1-holRelativeLead) {
		if c.holCorrectionStreak.Load() != 0 {
			c.holCorrectionStreak.Store(0)
		}
		return baseline
	}
	c.holPredictedEarlier.Add(1)
	streak := c.holCorrectionStreak.Load()
	if streak >= holMaxCorrectionStreak {
		return baseline
	}
	streak++
	c.holCorrectionStreak.Store(streak)
	for {
		old := c.holMaxCorrectionStreak.Load()
		if streak <= old || c.holMaxCorrectionStreak.CompareAndSwap(old, streak) {
			break
		}
	}
	c.holCorrections.Add(1)
	return best
}

func (c *StreamEngine) benchmarkPendingOnly(r *StreamTXRecord) bool {
	if !c.assignedDecoupledEnabled() || r == nil {
		return false
	}
	// A freshly added record can be observed before assignment publication.
	a := r.benchmarkAssignment.Load()
	return a == nil || !a.admitted.Load()
}

func (c *StreamEngine) wakeDecoupled() {
	if !c.assignedDecoupledEnabled() {
		return
	}
	for _, wake := range c.decoupledWake {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// assignBenchmark commits ownership and accounting together. A vanished or
// closed owner is a hard failure; a full physical queue is never considered here.
func (c *StreamEngine) assignBenchmark(r *StreamTXRecord, a *benchmarkAssignment) *streamLeg {
	// Read feedback before taking weightedMu. Key it by the concrete leg pointer
	// so a concurrent replacement cannot transfer an old generation's RTT to a
	// new Carrier with the same leg ID.
	feedbackLegs := make([]*streamLeg, c.maxActiveLegs)
	pathPenalty := make([]float64, c.maxActiveLegs)
	for index := 0; index < len(feedbackLegs); index++ {
		id := uint8(index)
		leg := c.getLeg(id)
		feedbackLegs[id] = leg
		if leg != nil {
			pathPenalty[id] = c.pathRTTScorePenalty(leg)
		}
	}
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	counts := make([]int, c.maxActiveLegs)
	if c.slowLegProtection() {
		copy(counts, c.txLedger.OrdinaryFramesByLeg())
	}
	now := time.Now()
	if a.owner != nil && !a.owner.closed.Load() && (a.admitted.Load() || c.ordinaryOwnerAllowed(a.owner.id) || c.pendingRecoveryProbeOwner(a.owner.id, r.Sequence())) {
		return a.owner
	}
	var best *streamLeg
	score := math.MaxFloat64
	legs := c.availableLegs()
	selectable := make([]bool, c.maxActiveLegs)
	for i := range selectable {
		selectable[i] = true
	}
	if c.slowLegProtection() {
		for _, leg := range legs {
			count := counts[leg.id]
			if a.owner == leg {
				count--
			}
			selectable[leg.id] = c.ordinaryEligible(leg.id, count, now)
		}
	}
	weights := make([]float64, c.maxActiveLegs)
	for _, leg := range legs {
		weights[leg.id] = c.effectiveSchedulerWeight(leg)
	}
	if c.cfg.benchmarkEpochRebase {
		membershipChanged := false
		for _, leg := range legs {
			// Temporary budget fullness is readiness, not a membership epoch.
			// Rebase only a true restriction/rejoin, never every queue/ACK turn.
			if !leg.closed.Load() && (c.ordinaryOwnerAllowed(leg.id) || selectable[leg.id]) {
				if !c.assignedEpochReady[leg.id] {
					membershipChanged = true
				}
			} else if c.assignedEpochReady[leg.id] {
				membershipChanged = true
			}
		}
		if membershipChanged {
			for _, leg := range legs {
				if weight := weights[leg.id]; weight > 0 {
					c.assignedEpochOrigin[leg.id] = float64(c.assignedService[leg.id]) / weight
					c.assignedEpochWeight[leg.id] = weight
				}
			}
			for _, leg := range legs {
				c.assignedEpochReady[leg.id] = !leg.closed.Load() && (c.ordinaryOwnerAllowed(leg.id) || selectable[leg.id])
			}
			var mask uint8
			for id := 0; id < 2 && id < len(c.assignedEpochReady); id++ {
				if c.assignedEpochReady[id] {
					mask |= 1 << id
				}
			}
			c.assignedEpochMask = mask
		}
	}
	if c.capacity != nil || c.capacityProvider != nil {
		// Changing capacity changes future entitlement, not historical service.
		// Preserve each leg's normalized finish across a weight update so a
		// long-lived stream does not acquire a catch-up burst or stall.
		for _, leg := range legs {
			id := leg.id
			old, next := c.assignedEpochWeight[id], weights[id]
			if old > 0 && next > 0 && old != next {
				finish := float64(c.assignedService[id])/old - c.assignedEpochOrigin[id]
				c.assignedEpochOrigin[id] = float64(c.assignedService[id])/next - finish
			}
			c.assignedEpochWeight[id] = next
		}
	}
	for _, leg := range legs {
		if leg.closed.Load() || !selectable[leg.id] {
			continue
		}
		weight := weights[leg.id]
		if weight <= 0 {
			continue
		}
		next := float64(c.assignedService[leg.id]+uint64(len(r.Payload())))/weight - c.assignedEpochOrigin[leg.id]
		if int(leg.id) < len(feedbackLegs) && feedbackLegs[leg.id] == leg {
			next += pathPenalty[leg.id]
		}
		if best == nil || next < score || (next == score && leg.id < best.id) {
			best, score = leg, next
		}
	}
	if best == nil {
		return nil // retain ownership until a path recovers, or normal recovery expires
	}
	// Sampling the frontier at a bounded sequence stride keeps the opt-in
	// correction out of the hot path for the bulk of records while still
	// giving each advancing frontier a nearby candidate.
	if c.cfg.HOLMode == StreamHOLCompletion && r.Sequence()%holCandidateStride == 0 {
		eligible := make([]*streamLeg, 0, 2)
		for _, leg := range legs {
			if selectable[leg.id] {
				eligible = append(eligible, leg)
			}
		}
		best = c.holCorrection(r, best, eligible)
	}
	n := uint64(len(r.Payload()))
	if a.admitted.Load() {
		a.owner = best
		return best
	}
	if c.slowLegProtection() && !c.txLedger.assignOrdinary(r, best.id, now) {
		return nil
	}
	if a.owner != nil && a.owner != best {
		c.cancelPendingRecoveryProbe(a.owner.id, r.Sequence(), now)
	}
	c.ordinaryAssignedRecord(best.id, r, now, len(r.Payload()))
	if c.slowLegProtection() {
		c.admission.mu.Lock()
		epoch := c.admission.legs[best.id].since
		c.admission.mu.Unlock()
		c.txLedger.setOrigin(r, c.legGeneration[best.id].Load(), epoch)
	}
	// An expired cooldown can reselect the same pending owner. Starting its
	// bounded probe is valid, but no ownership or virtual service moved.
	if a.owner == best {
		return best
	}
	if a.owner != nil {
		c.assignedService[a.owner.id] -= n
		c.assignedRecords[a.owner.id]--
		c.decoupledStats.rollback.Add(n)
		c.decoupledStats.reassigned.Add(n)
		c.decoupledStats.reassignments.Add(1)
	} else {
		c.decoupledStats.initial.Add(n)
		c.ordinaryAssignedBytes[best.id].Add(n)
	}
	a.owner = best
	c.assignedService[best.id] += n
	c.assignedRecords[best.id]++
	if trace := c.cfg.assignmentTrace; trace != nil && trace.armed && len(trace.samples) < 1024 {
		var legacyBytes [2]uint64
		copy(legacyBytes[:], c.assignedService)
		weight0, weight1 := float64(c.weightFor(0)), float64(c.weightFor(1))
		trace.samples = append(trace.samples, benchmarkAssignedSample{Leg: best.id, Bytes: legacyBytes,
			Debt: float64(c.assignedService[0])/weight0 - float64(c.assignedService[1])/weight1})
	}
	return best
}

func (c *StreamEngine) reassignFailedPending(id uint8) {
	if !c.assignedDecoupledEnabled() || int(id) >= len(c.pending) {
		return
	}
	for {
		select {
		case r := <-c.pending[id]:
			c.pendingDepth[id].Store(int64(len(c.pending[id])))
			a := r.benchmarkAssignment.Load()
			if a == nil {
				continue
			}
			leg := c.assignBenchmark(r, a)
			if leg == nil {
				return
			}
			for {
				select {
				case c.pending[leg.id] <- r:
					c.pendingDepth[leg.id].Store(int64(len(c.pending[leg.id])))
					c.decoupledStats.reassignments.Add(1)
					leg = nil
				case <-c.done:
					return
				case <-c.decoupledWake[leg.id]:
				}
				if leg == nil {
					break
				}
			}
		default:
			return
		}
	}
}

func (c *StreamEngine) enqueueAssignedDecoupled(r *StreamTXRecord, avoid int16) error {
	defer c.inputBlocked.Store(false)
	a := &benchmarkAssignment{}
	r.benchmarkAssignment.Store(a)
	var leg *streamLeg
	for leg == nil {
		if !c.isOutstanding(r) {
			return nil
		}
		leg = c.assignBenchmark(r, a)
		if leg == nil {
			c.inputBlocked.Store(true)
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.decoupledWake[2]:
			}
		}
	}
	// A hard failure while the planner is waiting for this bounded queue is
	// handled by its feeder. No second queue push or second initial assignment.
	for {
		if !c.isOutstanding(r) {
			return nil
		}
		select {
		case c.pending[leg.id] <- r:
			goto queued
		default:
		}
		c.inputBlocked.Store(true)
		started := time.Now()
		select {
		case <-c.done:
			return ErrStreamClosed
		case <-c.decoupledWake[2]:
		}
		wait := uint64(time.Since(started))
		c.pendingFullWaitNs[leg.id].Add(wait)
		c.plannerPendingWaitNs.Add(wait)
		if next := c.assignBenchmark(r, a); next != nil {
			leg = next
		}
	}
queued:
	depth := int64(len(c.pending[leg.id]))
	c.pendingDepth[leg.id].Store(depth)
	for {
		old := c.pendingMax[leg.id].Load()
		if depth <= old || c.pendingMax[leg.id].CompareAndSwap(old, depth) {
			break
		}
	}
	return nil
}

func (c *StreamEngine) pendingFeeder(id uint8) {
	defer close(c.decoupledStats.feedersDone[id])
	for {
		select {
		case <-c.done:
			return
		case r := <-c.pending[id]:
			c.wakeDecoupled()
			c.pendingDepth[id].Store(int64(len(c.pending[id])))
			c.decoupledStats.feederHeld[id].Store(1)
			for c.isOutstanding(r) {
				a := r.benchmarkAssignment.Load()
				leg := c.assignBenchmark(r, a)
				if leg != nil && c.tryQueue(leg, r, false) {
					break
				}
				// Once the send channel accepted the record, even a concurrent
				// close is post-admission failure. InvalidateLeg + ordinary retry
				// owns it; never roll back uncertain physical delivery.
				if a.admitted.Load() {
					break
				}
				started := time.Now()
				select {
				case <-c.done:
					return
				case <-c.decoupledWake[id]:
				}
				c.feederBlockedWaitNs[id].Add(uint64(time.Since(started)))
			}
			c.decoupledStats.feederHeld[id].Store(0)
		}
	}
}

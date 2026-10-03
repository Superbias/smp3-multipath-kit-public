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
	admitted      [2]atomic.Uint64
	feederHeld    [2]atomic.Int64
	feedersDone   [2]chan struct{}
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
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	if a.owner != nil && !a.owner.closed.Load() {
		return a.owner
	}
	var best *streamLeg
	score := math.MaxFloat64
	legs := c.availableLegs()
	var weights [2]float64
	for _, leg := range legs {
		weights[leg.id] = c.effectiveSchedulerWeight(leg)
	}
	if c.cfg.benchmarkEpochRebase {
		var mask uint8
		for _, leg := range legs {
			if !leg.closed.Load() {
				mask |= 1 << leg.id
			}
		}
		if mask != c.assignedEpochMask {
			for _, leg := range legs {
				if weight := weights[leg.id]; weight > 0 {
					c.assignedEpochOrigin[leg.id] = float64(c.assignedService[leg.id]) / weight
					c.assignedEpochWeight[leg.id] = weight
				}
			}
			c.assignedEpochMask = mask
		}
	}
	if c.capacity != nil {
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
		if leg.closed.Load() {
			continue
		}
		weight := weights[leg.id]
		if weight <= 0 {
			continue
		}
		next := float64(c.assignedService[leg.id]+uint64(len(r.Payload())))/weight - c.assignedEpochOrigin[leg.id]
		if best == nil || next < score || (next == score && leg.id < best.id) {
			best, score = leg, next
		}
	}
	if best == nil {
		return nil // retain ownership until a path recovers, or normal recovery expires
	}
	n := uint64(len(r.Payload()))
	if a.admitted.Load() {
		a.owner = best
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
	}
	a.owner = best
	c.assignedService[best.id] += n
	c.assignedRecords[best.id]++
	if trace := c.cfg.assignmentTrace; trace != nil && trace.armed && len(trace.samples) < 1024 {
		trace.samples = append(trace.samples, benchmarkAssignedSample{Leg: best.id, Bytes: c.assignedService,
			Debt: float64(c.assignedService[0])/float64(c.cfg.BandwidthMbps[0]) - float64(c.assignedService[1])/float64(c.cfg.BandwidthMbps[1])})
	}
	return best
}

func (c *StreamEngine) reassignFailedPending(id uint8) {
	if !c.assignedDecoupledEnabled() || id >= 2 {
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
	a := &benchmarkAssignment{}
	r.benchmarkAssignment.Store(a)
	var leg *streamLeg
	for leg == nil {
		leg = c.assignBenchmark(r, a)
		if leg == nil {
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.decoupledWake[2]:
			}
		}
	}
	// A hard failure while the planner is waiting for this bounded queue is
	// handled by its feeder. No second queue push or second initial assignment.
	select {
	case c.pending[leg.id] <- r:
	default:
		started := time.Now()
		select {
		case <-c.done:
			return ErrStreamClosed
		case c.pending[leg.id] <- r:
		}
		wait := uint64(time.Since(started))
		c.pendingFullWaitNs[leg.id].Add(wait)
		c.plannerPendingWaitNs.Add(wait)
	}
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

package smp3core

import "time"

// StreamBackpressureSnapshot is a point-in-time diagnostic view. It is queried
// by benchmarks; normal traffic does not allocate or format debug output.
type StreamBackpressureSnapshot struct {
	At                                                            time.Time
	Done, TXStopped, Closing, Finalizing                          bool
	Inflight, InflightMax                                         int
	LedgerOutstanding                                             int
	TXNext, ACKFrontier, OldestUnacked, HighestOutstanding        uint64
	OldestUnackedAge, LastACKProgressAge, LastACKFrameAge         time.Duration
	LastAdmissionAge, LastDispatchAge                             time.Duration
	LastACKValue, ACKAdvances, ACKRetiredRecords, ACKRetiredBytes uint64
	NewDispatches, RetryScheduled, RetryDispatches, RetryRejects  uint64
	RescueScheduled, RescueDispatches, RescueRejects              uint64
	RescueChecks, RescueOverdue, RescuePlans                      uint64
	Legs                                                          [2]StreamBackpressureLeg
	RXExpected, RXHighest                                         uint64
	RXReorder, RXReorderMax                                       int
}

type StreamBackpressureLeg struct {
	Present                                                                                bool
	Send, SendMax, Rescue, RescueMax, Control, ControlMax                                  int
	WriterState                                                                            string
	LastDequeueAge, LastWriteAge, LastWriteDuration                                        time.Duration
	WriteBPS, AckedBPS, EffectiveWeight                                                    float64
	EWMAWriteLatency                                                                       time.Duration
	QueueWaitTotal, WriteBusyTotal                                                         time.Duration
	WriteCount                                                                             uint64
	OrdinaryDispatches, RescueDispatches, QueueFullRejects, RescueFullRejects, WriteErrors uint64
}

func debugAge(now time.Time, timestamp int64) time.Duration {
	if timestamp == 0 {
		return 0
	}
	age := now.Sub(time.Unix(0, timestamp))
	if age < 0 {
		return 0
	}
	return age
}

func (c *StreamEngine) BackpressureSnapshot() StreamBackpressureSnapshot {
	now := time.Now()
	tx := c.txLedger.Snapshot(now)
	s := StreamBackpressureSnapshot{
		At: now, Closing: c.closing.Load(), Finalizing: c.finalizing.Load(), Inflight: len(c.inflight), InflightMax: cap(c.inflight),
		LedgerOutstanding: tx.OutstandingFrames, TXNext: tx.NextSequence,
		ACKFrontier: tx.AckedNext, OldestUnacked: tx.AckedNext,
		OldestUnackedAge:   tx.OldestOutstandingAge,
		LastACKProgressAge: debugAge(now, c.lastAckProgressNs.Load()),
		LastACKFrameAge:    debugAge(now, c.debugLastAckFrameNs.Load()),
		LastAdmissionAge:   debugAge(now, c.debugLastAdmissionNs.Load()),
		LastDispatchAge:    debugAge(now, c.debugLastDispatchNs.Load()),
		LastACKValue:       c.debugLastAckValue.Load(), ACKAdvances: c.debugAckAdvances.Load(),
		ACKRetiredRecords: c.debugAckRetiredRecords.Load(), ACKRetiredBytes: c.debugAckRetiredBytes.Load(),
		NewDispatches: c.debugNewDispatches.Load(), RetryScheduled: c.debugRetryScheduled.Load(),
		RetryDispatches: c.debugRetryDispatches.Load(), RetryRejects: c.debugRetryRejects.Load(),
		RescueScheduled: c.frontierRescueAttempts.Load(), RescueDispatches: c.debugRescueDispatches.Load(),
		RescueChecks: c.debugRescueChecks.Load(), RescueOverdue: c.debugRescueOverdue.Load(), RescuePlans: c.debugRescuePlans.Load(),
		RescueRejects: c.debugRescueRejects.Load(),
		RXExpected:    c.debugRXExpected.Load(), RXHighest: c.debugRXHighest.Load(),
		RXReorder: int(c.rxPendingFrames.Load()), RXReorderMax: c.cfg.MaxReorderFrames,
	}
	select {
	case <-c.done:
		s.Done = true
	default:
	}
	select {
	case <-c.txStopped:
		s.TXStopped = true
	default:
	}
	if tx.NextSequence > 0 {
		s.HighestOutstanding = tx.NextSequence - 1
	}
	for _, leg := range c.availableLegs() {
		if leg.id >= 2 {
			continue
		}
		state := "idle"
		switch leg.debugWriterState.Load() {
		case 1:
			state = "waiting"
		case 2:
			state = "writing"
		}
		writeBPS, ackedBPS, writeLatency := leg.perf.snapshot()
		s.Legs[leg.id] = StreamBackpressureLeg{
			Present: true, Send: len(leg.send), SendMax: cap(leg.send),
			Rescue: len(leg.rescue), RescueMax: cap(leg.rescue),
			Control: len(leg.control), ControlMax: cap(leg.control), WriterState: state,
			LastDequeueAge:    debugAge(now, leg.debugLastDequeueNs.Load()),
			LastWriteAge:      debugAge(now, leg.debugLastWriteNs.Load()),
			LastWriteDuration: time.Duration(leg.debugLastWriteDurationNs.Load()),
			WriteBPS:          writeBPS, AckedBPS: ackedBPS, EffectiveWeight: c.effectiveSchedulerWeight(leg), EWMAWriteLatency: writeLatency,
			QueueWaitTotal: time.Duration(leg.debugQueueWaitNs.Load()), WriteBusyTotal: time.Duration(leg.debugWriteBusyNs.Load()), WriteCount: leg.debugWriteCount.Load(),
			OrdinaryDispatches: leg.debugOrdinaryDispatches.Load(),
			RescueDispatches:   leg.debugRescueDispatches.Load(),
			QueueFullRejects:   leg.debugQueueFullRejects.Load(),
			RescueFullRejects:  leg.debugRescueFullRejects.Load(),
			WriteErrors:        leg.debugWriteErrors.Load(),
		}
	}
	return s
}

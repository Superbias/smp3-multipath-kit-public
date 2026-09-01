package smp3core

import (
	"sort"
	"time"
)

// StreamLegAvailability is the current fixed two-leg live mask. It contains
// no carrier object and deliberately does not generalize the wire to N legs.
type StreamLegAvailability [2]bool

func (a StreamLegAvailability) Has(id LegID) bool { return id < 2 && a[id] }
func (a StreamLegAvailability) Count() int {
	n := 0
	for _, live := range a {
		if live {
			n++
		}
	}
	return n
}

// StreamTXRetryCandidate is a candidate, not a send authorization. The host
// must revalidate it with IsOutstanding and MarkTransit before queueing.
type StreamTXRetryCandidate struct {
	Record *StreamTXRecord
	Avoid  int16
}

// PlanRetries applies the ordinary retry policy without using RetransmitTimeout.
func (l *StreamTXLedger) PlanRetries(live StreamLegAvailability) []StreamTXRetryCandidate {
	states := l.retrySnapshot()
	candidates := make([]StreamTXRetryCandidate, 0, len(states))
	for _, state := range states {
		if state.InTransit || state.RescueInTransit {
			continue
		}
		if state.LastSentLeg < 0 {
			candidates = append(candidates, StreamTXRetryCandidate{Record: state.Record, Avoid: -1})
			continue
		}
		if !live.Has(LegID(state.LastSentLeg)) {
			candidates = append(candidates, StreamTXRetryCandidate{Record: state.Record, Avoid: state.LastSentLeg})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Record.Sequence() < candidates[j].Record.Sequence() })
	return candidates
}

type StreamTXFrontierCandidate struct {
	record                           *StreamTXRecord
	owner                            int16
	reference                        time.Time
	overdue, exists, rescueInTransit bool
}

type StreamTXFrontierAction uint8

const (
	StreamTXFrontierNone StreamTXFrontierAction = iota
	StreamTXFrontierNeedActivation
	StreamTXFrontierRescue
)

type StreamTXFrontierPlan struct {
	Action StreamTXFrontierAction
	Record *StreamTXRecord
	Avoid  int16
}

func (l *StreamTXLedger) FrontierCandidate(now time.Time, timeout time.Duration) StreamTXFrontierCandidate {
	snapshot := l.frontierSnapshot()
	if !snapshot.Exists {
		return StreamTXFrontierCandidate{}
	}
	return StreamTXFrontierCandidate{record: snapshot.Record, owner: snapshot.Owner, reference: snapshot.ReferenceTime, overdue: !snapshot.ReferenceTime.IsZero() && now.Sub(snapshot.ReferenceTime) >= timeout, exists: true, rescueInTransit: snapshot.RescueInTransit}
}

func DecideStreamTXFrontierRepair(candidate StreamTXFrontierCandidate, live StreamLegAvailability) StreamTXFrontierPlan {
	if !candidate.exists || candidate.rescueInTransit || !candidate.overdue {
		return StreamTXFrontierPlan{}
	}
	if live.Count() == 1 {
		return StreamTXFrontierPlan{Action: StreamTXFrontierNeedActivation}
	}
	if live.Count() < 2 || candidate.owner < 0 || !live.Has(LegID(candidate.owner)) {
		return StreamTXFrontierPlan{}
	}
	return StreamTXFrontierPlan{Action: StreamTXFrontierRescue, Record: candidate.record, Avoid: candidate.owner}
}

package smp3core

import (
	"sort"
	"time"
)

// StreamLegAvailability is a protocol-ID-indexed live mask. LegID remains a
// byte on the wire, while each StreamEngine independently caps the number of
// IDs accepted for a session.
type StreamLegAvailability [MaxStreamLegIDCount]bool

func (a StreamLegAvailability) Has(id LegID) bool { return int(id) < len(a) && a[id] }
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
		if state.LocalEnqueue || state.InTransit || state.RescueInTransit {
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
	if live.Count() == 0 {
		return StreamTXFrontierPlan{}
	}
	if live.Count() == 1 {
		// A lost frontier must still be retransmitted when the session has only
		// one live leg. Activation is optional; withholding repair here can leave
		// a cumulative ACK stream permanently stalled after a single loss.
		for id, present := range live {
			if present {
				return StreamTXFrontierPlan{Action: StreamTXFrontierRescue, Record: candidate.record, Avoid: int16(id) - 1}
			}
		}
	}
	if candidate.owner < 0 || !live.Has(LegID(candidate.owner)) {
		return StreamTXFrontierPlan{}
	}
	return StreamTXFrontierPlan{Action: StreamTXFrontierRescue, Record: candidate.record, Avoid: candidate.owner}
}

// RestrictedFrontierBatch is a bounded old-owner prefix, not a second retry
// ledger. Only a proven overdue frontier starts it. Snapshot candidates are
// revalidated by MarkRestrictedRescueTransit before queueing.
func (l *StreamTXLedger) RestrictedFrontierBatch(frontier *StreamTXRecord, owner int16, now, cutoff time.Time, cooldown time.Duration, byteBudget int) []*StreamTXRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	if frontier == nil || l.outstanding[l.ackedNext] != frontier || frontier.ordinaryOwner != owner {
		return nil
	}
	var prefix [8]*StreamTXRecord
	n := 0
	for _, record := range l.outstanding {
		if record.ordinaryOwner != owner || record.createdAt.After(cutoff) || record.rescueInTransit || (!record.lastRescueAt.IsZero() && now.Sub(record.lastRescueAt) < cooldown) {
			continue
		}
		position := 0
		for position < n && prefix[position].sequence < record.sequence {
			position++
		}
		if position >= len(prefix) {
			continue
		}
		if n < len(prefix) {
			n++
		}
		for i := n - 1; i > position; i-- {
			prefix[i] = prefix[i-1]
		}
		prefix[position] = record
	}
	bytes := 0
	count := 0
	for count < n {
		size := len(prefix[count].payload)
		if count > 0 && bytes+size > byteBudget {
			break
		}
		bytes += size
		count++
	}
	result := make([]*StreamTXRecord, count)
	copy(result, prefix[:count])
	return result
}

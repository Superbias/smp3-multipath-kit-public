package smp3core

import (
	"sync"
	"time"
)

// Benchmark-only blocker clock. ACK and ledger creation own exposure updates;
// repeated rescue checks never refresh an unchanged frontier.
type benchmarkFrontierProbe struct {
	mu            sync.Mutex
	grace         bool
	active        bool
	sequence      uint64
	since         time.Time
	transitions   uint64
	failedAt      time.Time
	failedMax     uint64
	failed        map[uint64]bool
	retries       map[uint64]time.Time
	exposures     map[uint64]time.Time
	samples       []benchmarkFrontierRescue
	suppressed    uint64
	firstACK      time.Time
	passedFailure time.Time
}

type benchmarkFrontierRescue struct {
	Sequence        uint64
	Bytes           int
	At              time.Time
	CreatedAgeNs    int64
	RecordBaseAgeNs int64
	AttemptAgeNs    int64
	LastRescueAgeNs int64
	ExposureAgeNs   int64
	FailedSet       bool
	HadRetry        bool
	RetryDelayNs    int64
	ACKAt           time.Time
}

func (p *benchmarkFrontierProbe) observe(s StreamTXFrontierSnapshot, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !s.Exists {
		p.active = false
		return
	}
	seq := s.Record.Sequence()
	if !p.active || seq != p.sequence {
		// A delayed observer may read an older ledger snapshot. Never rewind.
		if p.active && seq < p.sequence {
			return
		}
		p.active, p.sequence, p.since = true, seq, now
		p.transitions++
		if p.exposures == nil {
			p.exposures = make(map[uint64]time.Time)
		}
		if len(p.exposures) < 16384 {
			p.exposures[seq] = now
		}
	}
}

func (p *benchmarkFrontierProbe) apply(candidate StreamTXFrontierCandidate, now time.Time, timeout time.Duration) StreamTXFrontierCandidate {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !candidate.exists || !p.active || p.sequence != candidate.record.Sequence() {
		return candidate
	}
	if p.grace && p.since.After(candidate.reference) {
		candidate.reference = p.since
		candidate.overdue = now.Sub(candidate.reference) >= timeout
		if !candidate.overdue {
			p.suppressed++
		}
	}
	return candidate
}

func (p *benchmarkFrontierProbe) failure(c *StreamEngine, id uint8, now time.Time) {
	// Snapshot real ledger ownership before InvalidateLeg clears it.
	c.txLedger.mu.Lock()
	set := make(map[uint64]bool)
	var max uint64
	for seq, r := range c.txLedger.outstanding {
		if (r.inTransit && uint8(r.transitLeg) == id) || r.lastSentLeg == int16(id) {
			set[seq] = true
			if seq > max {
				max = seq
			}
		}
	}
	c.txLedger.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failedAt.IsZero() {
		p.failedAt, p.failed, p.failedMax = now, set, max
	}
}

func (p *benchmarkFrontierProbe) retry(r *StreamTXRecord, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retries == nil {
		p.retries = make(map[uint64]time.Time)
	}
	if len(p.retries) < 16384 {
		p.retries[r.Sequence()] = now
	}
}

func (p *benchmarkFrontierProbe) rescue(c *StreamEngine, r *StreamTXRecord, now time.Time) {
	c.txLedger.mu.Lock()
	created, attempt, rescue := r.createdAt, r.lastSentAt, r.lastRescueAt
	base := created
	for _, t := range []time.Time{r.transitSince, attempt, rescue} {
		if t.After(base) {
			base = t
		}
	}
	c.txLedger.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.samples) >= 16384 {
		return
	}
	exposure := p.exposures[r.Sequence()]
	age := func(t time.Time) int64 {
		if t.IsZero() {
			return -1
		}
		return now.Sub(t).Nanoseconds()
	}
	_, retried := p.retries[r.Sequence()]
	p.samples = append(p.samples, benchmarkFrontierRescue{Sequence: r.Sequence(), Bytes: len(r.Payload()), At: now,
		CreatedAgeNs: age(created), RecordBaseAgeNs: age(base), AttemptAgeNs: age(attempt), LastRescueAgeNs: age(rescue),
		ExposureAgeNs: age(exposure), FailedSet: p.failed[r.Sequence()], HadRetry: retried, RetryDelayNs: age(p.retries[r.Sequence()])})
}

func (p *benchmarkFrontierProbe) ack(next uint64, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.failedAt.IsZero() {
		if p.firstACK.IsZero() {
			p.firstACK = now
		}
		if next > p.failedMax && p.passedFailure.IsZero() {
			p.passedFailure = now
		}
	}
	for i := range p.samples {
		if p.samples[i].Sequence < next && p.samples[i].ACKAt.IsZero() {
			p.samples[i].ACKAt = now
		}
	}
}

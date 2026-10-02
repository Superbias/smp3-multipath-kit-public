package smp3core

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func printFrontierProbe(p *benchmarkFrontierProbe) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var ages, ackDelay []int64
	preaged, prebytes, allbytes, failed, retried := 0, 0, 0, 0, 0
	for _, s := range p.samples {
		ages = append(ages, s.ExposureAgeNs)
		allbytes += s.Bytes
		if s.RecordBaseAgeNs >= int64(time.Second) && s.ExposureAgeNs >= 0 && s.ExposureAgeNs < int64(time.Second) {
			preaged++
			prebytes += s.Bytes
		}
		if s.FailedSet {
			failed++
		}
		if s.HadRetry {
			retried++
		}
		if !s.ACKAt.IsZero() {
			ackDelay = append(ackDelay, s.ACKAt.Sub(s.At).Nanoseconds())
		}
	}
	fraction := 0.0
	if len(p.samples) > 0 {
		fraction = float64(preaged) / float64(len(p.samples))
	}
	failureDelay := func(t time.Time) int64 {
		if p.failedAt.IsZero() || t.IsZero() {
			return -1
		}
		return t.Sub(p.failedAt).Nanoseconds()
	}
	fmt.Fprintf(os.Stderr, "FRONTIER_EXPOSURE grace=%v transitions=%d rescue_dispatches=%d preaged=%d preaged_bytes=%d fraction=%.4f all_rescue_bytes=%d failed_set=%d had_retry=%d exposure=%s rescue_ack=%s failure_first_ack_ns=%d failure_pass_range_ns=%d suppressed=%d\n", p.grace, p.transitions, len(p.samples), preaged, prebytes, fraction, allbytes, failed, retried, handoffPercentiles(ages), handoffPercentiles(ackDelay), failureDelay(p.firstACK), failureDelay(p.passedFailure), p.suppressed)
	if os.Getenv("SMP3_FRONTIER_TRACE") != "" {
		data, _ := json.Marshal(p.samples)
		fmt.Fprintf(os.Stderr, "FRONTIER_SEQUENCE_TRACE %s\n", data)
	}
}

func TestBenchmarkFrontierExposureClock(t *testing.T) {
	start := time.Now()
	r := &StreamTXRecord{sequence: 0, createdAt: start.Add(-2 * time.Second)}
	p := &benchmarkFrontierProbe{grace: true}
	s := StreamTXFrontierSnapshot{Exists: true, Record: r, ReferenceTime: r.createdAt}
	p.observe(s, start)
	p.observe(s, start.Add(500*time.Millisecond))
	c := StreamTXFrontierCandidate{exists: true, record: r, reference: r.createdAt, overdue: true}
	if p.apply(c, start.Add(900*time.Millisecond), time.Second).overdue {
		t.Fatal("new frontier rescued before grace")
	}
	if !p.apply(c, start.Add(time.Second), time.Second).overdue {
		t.Fatal("unchanged frontier clock reset by repeated checks")
	}
	r2 := &StreamTXRecord{sequence: 1, createdAt: r.createdAt}
	p.observe(StreamTXFrontierSnapshot{Exists: true, Record: r2}, start.Add(time.Second))
	c.record = r2
	if p.apply(c, start.Add(1500*time.Millisecond), time.Second).overdue {
		t.Fatal("new second lost frontier did not receive grace")
	}
	if !p.apply(c, start.Add(2*time.Second), time.Second).overdue {
		t.Fatal("second lost frontier can never recover")
	}
}

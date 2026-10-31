package smp3core

import (
	"testing"
	"time"
)

func TestClosureStaleRescueRejectCannotClearNewAttempt(t *testing.T) {
	l := NewStreamTXLedger()
	r := l.Add([]byte("one logical payload"), time.Now())
	l.assignOrdinary(r, 0, time.Now())
	leg := &streamLeg{id: 1, done: make(chan struct{}), rescue: make(chan txSendAttempt)}
	var fresh time.Time
	c := &StreamEngine{txLedger: l, done: make(chan struct{}), cfg: StreamConfig{rescueRejectHook: func(r *StreamTXRecord, old time.Time) {
		l.InvalidateLeg(1)
		var ok bool
		fresh, ok = l.MarkRescueTransit(r, 1, old.Add(time.Second))
		if !ok {
			t.Fatal("new-generation attempt did not start")
		}
		if !l.MarkRescueQueued(r, 1, fresh) {
			t.Fatal("new-generation queue commit failed")
		}
	}}}
	if c.tryQueueRescue(leg, r) {
		t.Fatal("old full queue unexpectedly accepted")
	}
	if !l.RescueInTransit(r) || !l.LastRescueAt(r).Equal(fresh) {
		t.Fatal("old queue-rejection cleanup erased the new-generation rescue lease/cooldown")
	}
}

func TestClosureStaleNormalCleanupCannotClearNewAttempt(t *testing.T) {
	l := NewStreamTXLedger()
	old := time.Now()
	r := l.Add([]byte("one payload"), old)
	if !l.MarkTransit(r, 1, old) {
		t.Fatal("old attempt")
	}
	l.InvalidateLeg(1)
	fresh := old.Add(time.Second)
	if !l.MarkTransit(r, 1, fresh) {
		t.Fatal("new attempt")
	}
	l.clearAttemptAt(r, 1, false, old)
	if !l.AttemptCurrent(r, 1, false) {
		t.Fatal("old normal cleanup erased new attempt")
	}
	l.clearAttemptAt(r, 1, false, fresh)
	if l.AttemptCurrent(r, 1, false) {
		t.Fatal("matching cleanup did not clear")
	}
}

package smp3core

import (
	"sync"
	"time"
)

// Diagnostic metadata travels with the existing capacity-one buffered wake.
// Zero-valued tokens retain the production channel's coalescing semantics.
type streamSpaceNotification struct {
	source string
	leg    int
	at     time.Time
}

type streamWakeState struct {
	Depth, Capacity [2]int
	Eligible        [2]bool
	Writer          [2]int32
}

type streamWakeWait struct {
	W0, W1, W2, W3, W4    time.Time
	FirstLeg, SelectedLeg int
	Source                string
	State                 [4]streamWakeState
}

type streamWakeTelemetry struct {
	mu               sync.Mutex
	legs             [2]*streamLeg
	waits            []*streamWakeWait
	active           *streamWakeWait
	pending          []*streamWakeWait
	signals          map[string][2]int
	dispatchTimes    []time.Time
	counts, bytes    [2]uint64
	minSize, maxSize int
	dropped          int
}

func (t *streamWakeTelemetry) snapshot() (waits []*streamWakeWait, signals map[string][2]int, dropped int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	waits = append(waits, t.waits...)
	signals = make(map[string][2]int, len(t.signals))
	for k, v := range t.signals {
		signals[k] = v
	}
	return waits, signals, t.dropped
}

func (t *streamWakeTelemetry) state() (s streamWakeState) {
	for id, leg := range t.legs {
		if leg == nil {
			continue
		}
		s.Depth[id], s.Capacity[id] = len(leg.send), cap(leg.send)
		s.Eligible[id] = !leg.closed.Load()
		s.Writer[id] = leg.debugWriterState.Load()
	}
	return
}

func (t *streamWakeTelemetry) begin(c *StreamEngine, selected *streamLeg) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.waits) >= 65536 {
		t.dropped++
		return
	}
	w := &streamWakeWait{W0: time.Now(), FirstLeg: -1, SelectedLeg: int(selected.id)}
	w.State[0] = t.state()
	for id := range t.legs {
		if w.State[0].Eligible[id] && w.State[0].Depth[id] < w.State[0].Capacity[id] {
			if w.FirstLeg == -1 {
				w.FirstLeg = id
			} else {
				w.FirstLeg = 2
			}
			w.W1 = w.W0
		}
	}
	t.waits = append(t.waits, w)
	t.active = w
}

func (t *streamWakeTelemetry) released(leg *streamLeg) {
	t.mu.Lock()
	defer t.mu.Unlock()
	w := t.active
	if w != nil && w.W1.IsZero() && !leg.closed.Load() && len(leg.send) < cap(leg.send) {
		w.W1 = time.Now()
		w.FirstLeg = int(leg.id)
		w.State[1] = t.state()
	}
}

func (t *streamWakeTelemetry) signal(source string, emitted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.signals == nil {
		t.signals = make(map[string][2]int)
	}
	n := t.signals[source]
	if emitted {
		n[0]++
	} else {
		n[1]++
	}
	t.signals[source] = n
}

func (t *streamWakeTelemetry) received(n streamSpaceNotification) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w := t.active; w != nil {
		w.W2, w.W3, w.Source = n.at, time.Now(), n.source
		w.State[2], w.State[3] = t.state(), t.state()
		// A release racing with wait registration can precede the recorded W0.
		if w.W1.IsZero() && n.source == "QUEUE_SPACE_RELEASE" && n.leg >= 0 {
			w.W1 = n.at
			if w.W1.Before(w.W0) {
				w.W1 = w.W0
			}
			w.FirstLeg = n.leg
		}
		t.pending = append(t.pending, w)
		t.active = nil
	}
}

func (t *streamWakeTelemetry) dispatched(r *StreamTXRecord, leg *streamLeg) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if len(t.pending) > 0 {
		t.pending[0].W4 = now
		t.pending = t.pending[1:]
	}
	t.counts[leg.id]++
	t.bytes[leg.id] += uint64(len(r.Payload()))
	size := len(r.Payload())
	if t.minSize == 0 || size < t.minSize {
		t.minSize = size
	}
	if size > t.maxSize {
		t.maxSize = size
	}
	if len(t.dispatchTimes) < 65536 {
		t.dispatchTimes = append(t.dispatchTimes, now)
	}
}

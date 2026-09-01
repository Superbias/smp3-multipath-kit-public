package smp3core

import (
	"errors"
	"time"
)

// ErrStreamRXReorderLimit preserves the legacy logical-session failure when a
// new future DATA frame would exceed the configured pending-frame limit.
var ErrStreamRXReorderLimit = errors.New("multipath reorder buffer exceeded")

// StreamRXDisposition describes how StreamRXWindow handled an inserted frame.
type StreamRXDisposition uint8

const (
	// StreamRXReady means Sequence equals Expected. The window does not retain
	// Payload; the caller owns it and must CommitReady only after delivery.
	StreamRXReady StreamRXDisposition = iota
	// StreamRXBuffered means the window retained the future frame and assumed
	// ownership of Payload until PopContiguous or DrainPending returns it.
	StreamRXBuffered
	// StreamRXDuplicate means the frame is older than Expected. The window does
	// not retain Payload. Legacy callers use this disposition to force an ACK.
	StreamRXDuplicate
	// StreamRXBufferedDuplicate means the same future sequence is already
	// retained. The window does not retain the newly supplied Payload and the
	// legacy path does not force an ACK for this case.
	StreamRXBufferedDuplicate
)

// StreamRXFrame is a protocol-owned ordered DATA value. Sequence is a DATA
// frame ordinal, not a byte offset. Payload allocation and release remain the
// caller's responsibility according to the disposition ownership contract.
type StreamRXFrame struct {
	Sequence uint64
	Leg      LegID
	Payload  []byte
}

// StreamRXWindow is a single-owner stream reorder state machine. It is not
// safe for concurrent use and intentionally contains no mutex or buffer pool.
type StreamRXWindow struct {
	expected         uint64
	pending          map[uint64]StreamRXFrame
	pendingBytes     uint64
	gapExpected      uint64
	gapSince         time.Time
	maxReorderFrames int
	readyPending     bool
}

// NewStreamRXWindow creates an empty RX window. Production passes the already
// normalized core MaxReorderFrames value.
func NewStreamRXWindow(maxReorderFrames int) *StreamRXWindow {
	return &StreamRXWindow{maxReorderFrames: maxReorderFrames}
}

// Insert classifies frame without copying Payload.
//
// Ownership rules:
//   - StreamRXReady: caller retains Payload and must deliver then CommitReady.
//   - StreamRXBuffered: the window owns Payload.
//   - duplicate dispositions or error: caller retains Payload.
func (w *StreamRXWindow) Insert(frame StreamRXFrame, now time.Time) (StreamRXDisposition, error) {
	if frame.Sequence < w.expected {
		return StreamRXDuplicate, nil
	}
	if frame.Sequence == w.expected {
		w.readyPending = true
		return StreamRXReady, nil
	}
	if _, exists := w.pending[frame.Sequence]; exists {
		return StreamRXBufferedDuplicate, nil
	}
	if len(w.pending) >= w.maxReorderFrames {
		return StreamRXReady, ErrStreamRXReorderLimit
	}
	if w.pending == nil {
		w.pending = make(map[uint64]StreamRXFrame)
	}
	w.pending[frame.Sequence] = frame
	w.pendingBytes += uint64(len(frame.Payload))
	w.refreshGap(now)
	return StreamRXBuffered, nil
}

// CommitReady advances the frame-ordinal frontier after the caller has
// successfully delivered the READY frame returned by Insert or PopContiguous.
func (w *StreamRXWindow) CommitReady(now time.Time) {
	if !w.readyPending {
		return
	}
	w.readyPending = false
	w.expected++
	if _, contiguous := w.pending[w.expected]; !contiguous {
		w.refreshGap(now)
	}
}

// PopContiguous transfers ownership of the next retained frame to the caller.
// The caller must deliver it and call CommitReady before asking for another.
func (w *StreamRXWindow) PopContiguous() (StreamRXFrame, bool) {
	if w.readyPending {
		return StreamRXFrame{}, false
	}
	frame, exists := w.pending[w.expected]
	if !exists {
		return StreamRXFrame{}, false
	}
	delete(w.pending, w.expected)
	w.pendingBytes -= uint64(len(frame.Payload))
	w.readyPending = true
	return frame, true
}

// Expected returns the next DATA frame ordinal required for delivery.
func (w *StreamRXWindow) Expected() uint64 { return w.expected }

// PendingFrames returns the number of future DATA frames retained by window.
func (w *StreamRXWindow) PendingFrames() int { return len(w.pending) }

// PendingBytes returns the total payload bytes retained by window.
func (w *StreamRXWindow) PendingBytes() uint64 { return w.pendingBytes }

// GapSince returns when the current missing expected ordinal became observable.
func (w *StreamRXWindow) GapSince() time.Time { return w.gapSince }

// DrainPending transfers ownership of every retained Payload back to the
// caller. READY payloads are never retained and therefore are not returned.
func (w *StreamRXWindow) DrainPending() []StreamRXFrame {
	frames := make([]StreamRXFrame, 0, len(w.pending))
	for _, frame := range w.pending {
		frames = append(frames, frame)
	}
	w.pending = nil
	w.pendingBytes = 0
	w.gapExpected = w.expected
	w.gapSince = time.Time{}
	w.readyPending = false
	return frames
}

func (w *StreamRXWindow) refreshGap(now time.Time) {
	if len(w.pending) == 0 {
		w.gapExpected = w.expected
		w.gapSince = time.Time{}
		return
	}
	if w.gapExpected != w.expected || w.gapSince.IsZero() {
		w.gapExpected = w.expected
		w.gapSince = now
	}
}

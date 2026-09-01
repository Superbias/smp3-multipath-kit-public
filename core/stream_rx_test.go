package smp3core

import (
	"bytes"
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

type streamRXTimedFrame struct {
	frame StreamRXFrame
	now   time.Time
}

type streamRXTrace struct {
	dispositions []StreamRXDisposition
	delivered    []StreamRXFrame
	expected     uint64
	pending      int
	pendingBytes uint64
	gapSince     time.Time
	forceACKs    int
	err          string
}

type legacyStreamRXReference struct {
	expected         uint64
	pending          map[uint64]StreamRXFrame
	pendingBytes     uint64
	gapExpected      uint64
	gapSince         time.Time
	maxReorderFrames int
}

func (r *legacyStreamRXReference) refreshGap(now time.Time) {
	if len(r.pending) == 0 {
		r.gapExpected = r.expected
		r.gapSince = time.Time{}
		return
	}
	if r.gapExpected != r.expected || r.gapSince.IsZero() {
		r.gapExpected = r.expected
		r.gapSince = now
	}
}

func runLegacyStreamRX(input []streamRXTimedFrame, maxReorderFrames int) streamRXTrace {
	reference := legacyStreamRXReference{
		pending:          make(map[uint64]StreamRXFrame),
		maxReorderFrames: maxReorderFrames,
	}
	var trace streamRXTrace
	for _, item := range input {
		frame := item.frame
		if frame.Sequence < reference.expected {
			trace.dispositions = append(trace.dispositions, StreamRXDuplicate)
			trace.forceACKs++
			continue
		}
		if frame.Sequence > reference.expected {
			if _, exists := reference.pending[frame.Sequence]; exists {
				trace.dispositions = append(trace.dispositions, StreamRXBufferedDuplicate)
				continue
			}
			if len(reference.pending) >= reference.maxReorderFrames {
				trace.dispositions = append(trace.dispositions, StreamRXReady)
				trace.err = ErrStreamRXReorderLimit.Error()
				break
			}
			reference.pending[frame.Sequence] = frame
			reference.pendingBytes += uint64(len(frame.Payload))
			reference.refreshGap(item.now)
			trace.dispositions = append(trace.dispositions, StreamRXBuffered)
			continue
		}

		trace.dispositions = append(trace.dispositions, StreamRXReady)
		for {
			trace.delivered = append(trace.delivered, frame)
			reference.expected++
			next, exists := reference.pending[reference.expected]
			if !exists {
				reference.refreshGap(item.now)
				break
			}
			delete(reference.pending, reference.expected)
			reference.pendingBytes -= uint64(len(next.Payload))
			frame = next
		}
	}
	trace.expected = reference.expected
	trace.pending = len(reference.pending)
	trace.pendingBytes = reference.pendingBytes
	trace.gapSince = reference.gapSince
	return trace
}

func runCoreStreamRX(input []streamRXTimedFrame, maxReorderFrames int) streamRXTrace {
	window := NewStreamRXWindow(maxReorderFrames)
	var trace streamRXTrace
	for _, item := range input {
		disposition, err := window.Insert(item.frame, item.now)
		trace.dispositions = append(trace.dispositions, disposition)
		if err != nil {
			trace.err = err.Error()
			break
		}
		switch disposition {
		case StreamRXDuplicate:
			trace.forceACKs++
		case StreamRXReady:
			frame := item.frame
			for {
				trace.delivered = append(trace.delivered, frame)
				window.CommitReady(item.now)
				next, ok := window.PopContiguous()
				if !ok {
					break
				}
				frame = next
			}
		}
	}
	trace.expected = window.Expected()
	trace.pending = window.PendingFrames()
	trace.pendingBytes = window.PendingBytes()
	trace.gapSince = window.GapSince()
	return trace
}

func assertStreamRXParity(t *testing.T, input []streamRXTimedFrame, maxReorderFrames int) streamRXTrace {
	t.Helper()
	legacy := runLegacyStreamRX(input, maxReorderFrames)
	core := runCoreStreamRX(input, maxReorderFrames)
	if !reflect.DeepEqual(core, legacy) {
		t.Fatalf("legacy/Core mismatch: legacy=%+v core=%+v", legacy, core)
	}
	return core
}

func streamRXInput(order []uint64, sizes []int, duplicates map[int]uint64) []streamRXTimedFrame {
	start := time.Unix(1700000000, 0)
	input := make([]streamRXTimedFrame, 0, len(order)+len(duplicates))
	for index, sequence := range order {
		size := sizes[int(sequence)%len(sizes)]
		payload := bytes.Repeat([]byte{byte(sequence + 1)}, size)
		input = append(input, streamRXTimedFrame{
			frame: StreamRXFrame{Sequence: sequence, Leg: LegID(index % 2), Payload: payload},
			now:   start.Add(time.Duration(len(input)) * time.Millisecond),
		})
		if duplicate, ok := duplicates[index]; ok {
			dupPayload := bytes.Repeat([]byte{0xee}, sizes[int(duplicate)%len(sizes)])
			input = append(input, streamRXTimedFrame{
				frame: StreamRXFrame{Sequence: duplicate, Leg: LegID((index + 1) % 2), Payload: dupPayload},
				now:   start.Add(time.Duration(len(input)) * time.Millisecond),
			})
		}
	}
	return input
}

func deliveredSequences(frames []StreamRXFrame) []uint64 {
	sequences := make([]uint64, len(frames))
	for index, frame := range frames {
		sequences[index] = frame.Sequence
	}
	return sequences
}

func TestStreamRXWindowDeterministicDifferential(t *testing.T) {
	tests := []struct {
		name       string
		order      []uint64
		duplicates map[int]uint64
		want       []uint64
	}{
		{name: "fully_in_order", order: []uint64{0, 1, 2, 3}, want: []uint64{0, 1, 2, 3}},
		{name: "one_gap", order: []uint64{0, 2, 3, 1}, want: []uint64{0, 1, 2, 3}},
		{name: "reverse_small_window", order: []uint64{3, 2, 1, 0}, want: []uint64{0, 1, 2, 3}},
		{name: "duplicate_before_delivery", order: []uint64{0, 2, 1}, duplicates: map[int]uint64{1: 2}, want: []uint64{0, 1, 2}},
		{name: "old_duplicate_after_delivery", order: []uint64{0, 1, 2}, duplicates: map[int]uint64{2: 1}, want: []uint64{0, 1, 2}},
		{name: "gap_fill_promotes_multiple", order: []uint64{0, 3, 2, 1}, want: []uint64{0, 1, 2, 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := assertStreamRXParity(t, streamRXInput(test.order, []int{1, 7, 1200, 4096}, test.duplicates), 8)
			if got := deliveredSequences(trace.delivered); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("delivery=%v want=%v", got, test.want)
			}
		})
	}
}

func TestStreamRXWindowSequenceIsFrameOrdinal(t *testing.T) {
	window := NewStreamRXWindow(4)
	for sequence, size := range []int{1, 16384, 3, 65536} {
		frame := StreamRXFrame{Sequence: uint64(sequence), Payload: make([]byte, size)}
		disposition, err := window.Insert(frame, time.Time{})
		if err != nil || disposition != StreamRXReady {
			t.Fatalf("sequence %d disposition=%d err=%v", sequence, disposition, err)
		}
		window.CommitReady(time.Time{})
		if window.Expected() != uint64(sequence+1) {
			t.Fatalf("after payload size %d expected=%d want=%d", size, window.Expected(), sequence+1)
		}
	}
}

func TestStreamRXWindowReorderLimitBoundary(t *testing.T) {
	start := time.Unix(1800000000, 0)
	window := NewStreamRXWindow(2)
	for _, sequence := range []uint64{1, 2} {
		disposition, err := window.Insert(StreamRXFrame{Sequence: sequence, Payload: []byte{byte(sequence)}}, start)
		if err != nil || disposition != StreamRXBuffered {
			t.Fatalf("sequence %d disposition=%d err=%v", sequence, disposition, err)
		}
	}
	if window.PendingFrames() != 2 {
		t.Fatalf("pending=%d want=2", window.PendingFrames())
	}
	current := []byte("caller-owned")
	disposition, err := window.Insert(StreamRXFrame{Sequence: 3, Payload: current}, start.Add(time.Second))
	if !errors.Is(err, ErrStreamRXReorderLimit) || disposition != StreamRXReady {
		t.Fatalf("limit+1 disposition=%d err=%v", disposition, err)
	}
	drained := window.DrainPending()
	if len(drained) != 2 {
		t.Fatalf("drained=%d want=2", len(drained))
	}
	for _, frame := range drained {
		if len(frame.Payload) > 0 && &frame.Payload[0] == &current[0] {
			t.Fatal("rejected payload was retained by the window")
		}
	}
}

func TestStreamRXWindowOwnershipAndDrain(t *testing.T) {
	window := NewStreamRXWindow(8)
	readyPayload := []byte("ready")
	if disposition, err := window.Insert(StreamRXFrame{Sequence: 0, Payload: readyPayload}, time.Time{}); err != nil || disposition != StreamRXReady {
		t.Fatalf("READY disposition=%d err=%v", disposition, err)
	}
	if drained := window.DrainPending(); len(drained) != 0 {
		t.Fatalf("READY payload retained: %+v", drained)
	}

	window = NewStreamRXWindow(8)
	bufferedPayload := []byte("buffered")
	if disposition, err := window.Insert(StreamRXFrame{Sequence: 2, Payload: bufferedPayload}, time.Unix(1, 0)); err != nil || disposition != StreamRXBuffered {
		t.Fatalf("BUFFERED disposition=%d err=%v", disposition, err)
	}
	duplicatePayload := []byte("duplicate")
	if disposition, err := window.Insert(StreamRXFrame{Sequence: 2, Payload: duplicatePayload}, time.Unix(2, 0)); err != nil || disposition != StreamRXBufferedDuplicate {
		t.Fatalf("future duplicate disposition=%d err=%v", disposition, err)
	}
	drained := window.DrainPending()
	if len(drained) != 1 || &drained[0].Payload[0] != &bufferedPayload[0] {
		t.Fatalf("BUFFERED ownership not returned: %+v", drained)
	}
	if &drained[0].Payload[0] == &duplicatePayload[0] {
		t.Fatal("duplicate payload was retained")
	}
	if window.PendingFrames() != 0 || window.PendingBytes() != 0 || !window.GapSince().IsZero() {
		t.Fatalf("drain did not reset window: pending=%d bytes=%d gap=%s", window.PendingFrames(), window.PendingBytes(), window.GapSince())
	}
}

func TestStreamRXWindowGapResetsForEachExpectedGap(t *testing.T) {
	start := time.Unix(3000, 0)
	window := NewStreamRXWindow(8)
	if disposition, err := window.Insert(StreamRXFrame{Sequence: 3, Payload: []byte("d")}, start); err != nil || disposition != StreamRXBuffered {
		t.Fatalf("initial buffered disposition=%d err=%v", disposition, err)
	}
	if !window.GapSince().Equal(start) {
		t.Fatalf("initial gap=%s want=%s", window.GapSince(), start)
	}
	for sequence := uint64(0); sequence < 2; sequence++ {
		now := start.Add(time.Duration(sequence+1) * time.Second)
		disposition, err := window.Insert(StreamRXFrame{Sequence: sequence, Payload: []byte{byte(sequence)}}, now)
		if err != nil || disposition != StreamRXReady {
			t.Fatalf("sequence=%d disposition=%d err=%v", sequence, disposition, err)
		}
		window.CommitReady(now)
		if !window.GapSince().Equal(now) {
			t.Fatalf("sequence=%d gap=%s want=%s", sequence, window.GapSince(), now)
		}
	}
}

func TestStreamRXWindowRandomizedDifferential5000(t *testing.T) {
	rng := rand.New(rand.NewSource(0x534d50335258))
	sizes := []int{1, 7, 1200, 4096, 16384, 65535}
	const cases = 5000
	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		count := 1 + rng.Intn(20)
		canonical := make([]StreamRXFrame, count)
		for index := range canonical {
			size := sizes[rng.Intn(len(sizes))]
			payload := bytes.Repeat([]byte{byte(index + 1)}, size)
			canonical[index] = StreamRXFrame{Sequence: uint64(index), Leg: LegID(rng.Intn(2)), Payload: payload}
		}
		arrival := append([]StreamRXFrame(nil), canonical...)
		for index := 0; index < len(arrival)-1; index++ {
			window := len(arrival) - index
			if window > 5 {
				window = 5
			}
			swap := index + rng.Intn(window)
			arrival[index], arrival[swap] = arrival[swap], arrival[index]
		}
		withDuplicates := make([]StreamRXFrame, 0, len(arrival)*2)
		for _, frame := range arrival {
			withDuplicates = append(withDuplicates, frame)
			if rng.Intn(4) == 0 {
				duplicate := frame
				duplicate.Leg = LegID(rng.Intn(2))
				duplicate.Payload = append([]byte(nil), frame.Payload...)
				withDuplicates = append(withDuplicates, duplicate)
			}
		}
		start := time.Unix(1900000000+int64(caseIndex), 0)
		input := make([]streamRXTimedFrame, len(withDuplicates))
		for index, frame := range withDuplicates {
			input[index] = streamRXTimedFrame{frame: frame, now: start.Add(time.Duration(index) * time.Microsecond)}
		}
		maxReorderFrames := rng.Intn(count + 3)
		assertStreamRXParity(t, input, maxReorderFrames)
	}
}

func TestStreamRXWindowInOrderAllocations(t *testing.T) {
	window := NewStreamRXWindow(8)
	payload := make([]byte, 1200)
	var sequence uint64
	allocations := testing.AllocsPerRun(1000, func() {
		disposition, err := window.Insert(StreamRXFrame{Sequence: sequence, Payload: payload}, time.Time{})
		if err != nil || disposition != StreamRXReady {
			t.Fatalf("sequence=%d disposition=%d err=%v", sequence, disposition, err)
		}
		window.CommitReady(time.Time{})
		sequence++
	})
	if allocations != 0 {
		t.Fatalf("in-order allocations/op=%f want=0", allocations)
	}
}

func BenchmarkStreamRXWindowInOrder(b *testing.B) {
	window := NewStreamRXWindow(8)
	payload := make([]byte, 1200)
	frame := StreamRXFrame{Payload: payload}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		frame.Sequence = uint64(index)
		disposition, err := window.Insert(frame, time.Time{})
		if err != nil || disposition != StreamRXReady {
			b.Fatalf("disposition=%d err=%v", disposition, err)
		}
		window.CommitReady(time.Time{})
	}
}

func BenchmarkStreamRXWindowReorder(b *testing.B) {
	window := NewStreamRXWindow(8)
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		base := uint64(index * 2)
		if disposition, err := window.Insert(StreamRXFrame{Sequence: base + 1, Payload: payload}, time.Time{}); err != nil || disposition != StreamRXBuffered {
			b.Fatalf("future disposition=%d err=%v", disposition, err)
		}
		if disposition, err := window.Insert(StreamRXFrame{Sequence: base, Payload: payload}, time.Time{}); err != nil || disposition != StreamRXReady {
			b.Fatalf("ready disposition=%d err=%v", disposition, err)
		}
		window.CommitReady(time.Time{})
		if _, ok := window.PopContiguous(); !ok {
			b.Fatal("contiguous frame not promoted")
		}
		window.CommitReady(time.Time{})
	}
}

func TestStreamRXWindowDrainOrderIsNotPartOfContract(t *testing.T) {
	window := NewStreamRXWindow(8)
	for _, sequence := range []uint64{4, 2, 3} {
		if _, err := window.Insert(StreamRXFrame{Sequence: sequence, Payload: []byte{byte(sequence)}}, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	drained := window.DrainPending()
	sequences := deliveredSequences(drained)
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	if !reflect.DeepEqual(sequences, []uint64{2, 3, 4}) {
		t.Fatalf("drained sequences=%v", sequences)
	}
}

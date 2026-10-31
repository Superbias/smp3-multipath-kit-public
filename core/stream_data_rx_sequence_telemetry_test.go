package smp3core

import (
	"testing"
	"time"
)

func TestDataRXSequenceTelemetryIsOptInAndBounded(t *testing.T) {
	var telemetry StreamTelemetry
	now := time.Now()
	event := StreamDataRXSequenceEvent{
		CarrierReadAt:  now,
		CoreAcceptedAt: now.Add(time.Microsecond),
		Sequence:       42,
		LegID:          1,
		LegGeneration:  7,
		PayloadBytes:   65536,
		Disposition:    "unique_contiguous",
		Unique:         true,
	}

	telemetry.recordDataRXSequence(event)
	if got := telemetry.snapshot(); len(got.DataRXSequenceEvents) != 0 {
		t.Fatalf("sequence event recorded while disabled: %+v", got.DataRXSequenceEvents)
	}

	telemetry.EnableDataRXSequenceEvents()
	for i := 0; i < StreamDataRXSequenceEventLimit+3; i++ {
		event.Sequence = uint64(i)
		telemetry.recordDataRXSequence(event)
	}

	got := telemetry.snapshot()
	if len(got.DataRXSequenceEvents) != StreamDataRXSequenceEventLimit {
		t.Fatalf("event ring length=%d, want %d", len(got.DataRXSequenceEvents), StreamDataRXSequenceEventLimit)
	}
	if got.DroppedDataRXSequenceEvents != 3 {
		t.Fatalf("dropped=%d, want 3", got.DroppedDataRXSequenceEvents)
	}
	first := got.DataRXSequenceEvents[0]
	last := got.DataRXSequenceEvents[len(got.DataRXSequenceEvents)-1]
	if first.Sequence != 3 || first.EventIndex != 4 || first.LegID != 1 || first.LegGeneration != 7 || first.PayloadBytes != 65536 || !first.Unique {
		t.Fatalf("unexpected oldest retained event: %+v", first)
	}
	if last.Sequence != uint64(StreamDataRXSequenceEventLimit+2) || last.EventIndex != StreamDataRXSequenceEventLimit+3 {
		t.Fatalf("unexpected newest event: %+v", last)
	}

	got.DataRXSequenceEvents[0].Sequence = 999
	if telemetry.snapshot().DataRXSequenceEvents[0].Sequence == 999 {
		t.Fatal("snapshot event slice aliases telemetry ring")
	}
}

package server

import (
	"encoding/json"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestStreamTransportTelemetryDetailAndClone(t *testing.T) {
	nLegs := make([]TelemetryStreamLegTransport, 16)
	for id := range nLegs {
		nLegs[id] = TelemetryStreamLegTransport{
			LegID: uint8(id), Generation: uint64(id + 1), Up: true,
			CapacityEstimateBPS: float64(id+1) * 1e6, CapacityValid: true,
			DataRXUniqueFrames: uint64(id + 1), CarrierWireBytes: uint64(id+1) * 100,
			AdmissionState: "normal", RecoveryEpoch: uint64(id),
		}
	}
	row := TelemetrySessionSnapshot{DisplaySessionID: "local-test", Transport: &TelemetryStreamTransport{Direction: "local_tx", AdmissionState: [2]string{"normal", "restricted"}, AdmissionReason: [2]string{"", "sustained_frontier"}, OrdinaryBytes: [2]uint64{100, 1}, WrittenBytes: [2]uint64{110, 1}, RetryBytes: [2]uint64{10, 0}, Generation: [2]uint64{1, 1}, NLegs: nLegs, DataRXSequenceEvents: []smp3core.StreamDataRXSequenceEvent{{EventIndex: 1, Sequence: 7, LegID: 1, LegGeneration: 4, PayloadBytes: 65536, Disposition: "unique_contiguous", Unique: true}}}}
	original := TelemetrySnapshot{Sessions: []TelemetrySessionSnapshot{row}}
	clone := cloneTelemetrySnapshot(original)
	clone.Sessions[0].Transport.AdmissionState[1] = "normal"
	if original.Sessions[0].Transport.AdmissionState[1] != "restricted" {
		t.Fatal("mutable snapshot alias")
	}
	clone.Sessions[0].Transport.DataRXSequenceEvents[0].Sequence = 99
	if original.Sessions[0].Transport.DataRXSequenceEvents[0].Sequence != 7 {
		t.Fatal("sequence event snapshot aliases original")
	}
	clone.Sessions[0].Transport.NLegs[15].CapacityEstimateBPS = 0
	if original.Sessions[0].Transport.NLegs[15].CapacityEstimateBPS != 16e6 {
		t.Fatal("N-leg transport snapshot aliases original")
	}
	body, err := json.Marshal(sessionDetailJSON(row, time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err = json.Unmarshal(body, &parsed); err != nil {
		t.Fatal(err)
	}
	transport := parsed["stream_transport"].(map[string]any)
	if transport["direction"] != "local_tx" || transport["admission_state"].([]any)[1] != "restricted" {
		t.Fatal("missing session/leg protection telemetry")
	}
	events := transport["data_rx_sequence_events"].([]any)
	event := events[0].(map[string]any)
	if event["sequence"] != float64(7) || event["leg_id"] != float64(1) || event["leg_generation"] != float64(4) || event["disposition"] != "unique_contiguous" {
		t.Fatalf("unexpected DATA sequence event in session detail: %+v", event)
	}
	transportLegs := transport["n_legs"].([]any)
	if len(transportLegs) != 16 {
		t.Fatalf("dynamic transport telemetry has %d legs; want 16", len(transportLegs))
	}
	lastLeg := transportLegs[15].(map[string]any)
	if lastLeg["leg_id"] != float64(15) || lastLeg["generation"] != float64(16) || lastLeg["capacity_estimate_bps"] != 16e6 || lastLeg["data_rx_unique_frames"] != float64(16) || lastLeg["carrier_wire_bytes"] != float64(1600) {
		t.Fatalf("last N-leg transport details were truncated or misbound: %+v", lastLeg)
	}
}

func TestDataSequenceTelemetryRequiresTelemetryEnabled(t *testing.T) {
	options := TelemetryOptions{DataSequenceEvents: true}
	if err := options.NormalizeAndValidate(); err == nil {
		t.Fatal("data_sequence_events accepted while telemetry is disabled")
	}
	options.Enabled = true
	options.Listen = "127.0.0.1:0"
	if err := options.NormalizeAndValidate(); err != nil {
		t.Fatalf("enabled sequence telemetry rejected: %v", err)
	}
}

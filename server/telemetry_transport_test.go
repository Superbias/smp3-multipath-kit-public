package server

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStreamTransportTelemetryDetailAndClone(t *testing.T) {
	row := TelemetrySessionSnapshot{DisplaySessionID: "local-test", Transport: &TelemetryStreamTransport{Direction: "local_tx", AdmissionState: [2]string{"normal", "restricted"}, AdmissionReason: [2]string{"", "sustained_frontier"}, OrdinaryBytes: [2]uint64{100, 1}, WrittenBytes: [2]uint64{110, 1}, RetryBytes: [2]uint64{10, 0}, Generation: [2]uint64{1, 1}}}
	original := TelemetrySnapshot{Sessions: []TelemetrySessionSnapshot{row}}
	clone := cloneTelemetrySnapshot(original)
	clone.Sessions[0].Transport.AdmissionState[1] = "normal"
	if original.Sessions[0].Transport.AdmissionState[1] != "restricted" {
		t.Fatal("mutable snapshot alias")
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
}

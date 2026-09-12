package monitor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestTelemetrySSELifecycleIsReducedToSafeEvent(t *testing.T) {
	monitor, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()
	body := "event: lifecycle\nid: 7\ndata: {\"kind\":\"leg_attached\",\"mode\":\"stream\",\"leg_id\":1,\"display_session_id\":\"RAW_SESSION_SHOULD_NOT_APPEAR\"}\n\n"
	response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
	if err := consumeTelemetrySSE(context.Background(), response, monitor); err != nil {
		t.Fatal(err)
	}
	events := monitor.Events(10).Items
	if len(events) != 1 || events[0].Type != "lifecycle_leg_attached" || !strings.Contains(events[0].Message, "leg 1") {
		t.Fatalf("events=%+v", events)
	}
	if strings.Contains(events[0].Message, "RAW_SESSION_SHOULD_NOT_APPEAR") {
		t.Fatal("raw session alias leaked")
	}
}

func TestOperationalEventsAreDeduplicatedAndHealthClassified(t *testing.T) {
	var down bool
	monitor, err := New(Config{Probe: func(target ListenerTarget) ListenerHealth {
		if down && target.Name == "carrier-b" {
			return ListenerHealth{ListenerTarget: target, Probe: "test"}
		}
		return ListenerHealth{ListenerTarget: target, ListenerUp: true, ProcessUp: true, Probe: "test"}
	}})
	if err != nil {
		t.Fatal(err)
	}
	first := monitor.health(0, 0)
	if first.Overall != "IDLE" {
		t.Fatalf("first health=%+v", first)
	}
	_ = monitor.health(0, 0)
	if got := len(monitor.Events(10).Items); got != 0 {
		t.Fatalf("duplicate idle events=%d", got)
	}
	down = true
	degraded := monitor.health(1, 2)
	if degraded.Overall != "DEGRADED" {
		t.Fatalf("degraded health=%+v", degraded)
	}
	if got := len(monitor.Events(10).Items); got != 2 {
		t.Fatalf("health transition events=%d", got)
	}
	monitor.addEvent(time.Now(), "test", "info", "test", "deduplicated")
	monitor.addEvent(time.Now().Add(time.Second), "test", "info", "test", "deduplicated")
	if got := len(monitor.Events(10).Items); got != 3 {
		t.Fatalf("dedup events=%d", got)
	}
}

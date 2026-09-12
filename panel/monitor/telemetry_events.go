package monitor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type telemetryLifecycle struct {
	Kind  string `json:"kind"`
	Mode  string `json:"mode"`
	LegID *int   `json:"leg_id"`
}

func (m *Monitor) runTelemetryEvents() {
	defer close(m.eventDone)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-m.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.TelemetryURL+"/api/v1/events", nil)
		if err == nil {
			request.Header.Set("Accept", "text/event-stream")
		}
		if err == nil {
			response, requestErr := m.sseClient.Do(request)
			if requestErr == nil {
				if response.StatusCode == http.StatusOK {
					_ = consumeTelemetrySSE(ctx, response, m)
				} else {
					_ = response.Body.Close()
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeTelemetrySSE(ctx context.Context, response *http.Response, monitor *Monitor) error {
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 1024), 128<<10)
	var eventName string
	var data strings.Builder
	flush := func() {
		if eventName == "" || data.Len() == 0 {
			eventName, data = "", strings.Builder{}
			return
		}
		payload := data.String()
		switch eventName {
		case "lifecycle":
			var lifecycle telemetryLifecycle
			if json.Unmarshal([]byte(payload), &lifecycle) == nil {
				monitor.RecordLifecycle(lifecycle.Kind, lifecycle.Mode, lifecycle.LegID)
			}
		case "reset":
			monitor.addEvent(monitor.now().UTC(), "telemetry_reset", "warning", "telemetry", "telemetry history reset")
		}
		eventName, data = "", strings.Builder{}
	}
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			continue
		}
		if strings.HasPrefix(line, "id:") {
			_, _ = strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64)
		}
	}
	flush()
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

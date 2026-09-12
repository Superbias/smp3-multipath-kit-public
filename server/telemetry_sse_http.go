package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (h *telemetryHTTPServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !h.registry.Enabled() {
		writeTelemetryJSON(w, http.StatusServiceUnavailable, telemetryError{Error: telemetryErrorBody{"TELEMETRY_DISABLED", "telemetry is disabled"}}, false)
		return
	}
	afterID, hasAfter, err := parseLastEventID(r.Header.Get("Last-Event-ID"))
	if err != nil {
		writeTelemetryJSON(w, http.StatusBadRequest, telemetryError{Error: telemetryErrorBody{"INVALID_LAST_EVENT_ID", "invalid Last-Event-ID"}}, false)
		return
	}
	subscriber, replay, stale, limited := h.registry.SubscribeSSE(afterID, hasAfter)
	if limited {
		writeTelemetryJSON(w, http.StatusServiceUnavailable, telemetryError{Error: telemetryErrorBody{"SSE_CLIENT_LIMIT", "SSE client limit reached"}}, false)
		return
	}
	defer h.registry.UnsubscribeSSE(subscriber)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Connection", "keep-alive")
	if _, ok := w.(http.Flusher); !ok {
		return
	}
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	if stale {
		if err := writeSSEReset(w); err != nil {
			return
		}
	}
	for _, event := range replay {
		if err := writeSSELifecycle(w, event); err != nil {
			return
		}
	}
	if err := h.writeSSESnapshot(w, h.registry.LatestSnapshot()); err != nil {
		return
	}
	if events, snapshot, activationStale, closed := h.registry.activateSSE(subscriber); !closed {
		if activationStale {
			if err := writeSSEReset(w); err != nil {
				return
			}
			if err := h.writeSSESnapshot(w, h.registry.LatestSnapshot()); err != nil {
				return
			}
		}
		if err := writeSSELifecycleBatch(w, events); err != nil {
			return
		}
		if snapshot {
			if err := h.writeSSESnapshot(w, h.registry.LatestSnapshot()); err != nil {
				return
			}
		}
	} else {
		return
	}

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-subscriber.ch:
			if !ok {
				return
			}
			events, snapshot, _, closed := h.registry.drainSSE(subscriber)
			if closed {
				return
			}
			if err := writeSSELifecycleBatch(w, events); err != nil {
				return
			}
			if snapshot {
				if err := h.writeSSESnapshot(w, h.registry.LatestSnapshot()); err != nil {
					return
				}
			}
		case <-keepalive.C:
			if err := writeSSEKeepalive(w); err != nil {
				return
			}
		}
	}
}

func writeSSELifecycleBatch(w http.ResponseWriter, events []telemetryLifecycleEvent) error {
	for _, event := range events {
		if err := writeSSELifecycle(w, event); err != nil {
			return err
		}
	}
	return nil
}

func parseLastEventID(raw string) (uint64, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		return 0, false, fmt.Errorf("invalid event id")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func writeSSELifecycle(w http.ResponseWriter, event telemetryLifecycleEvent) error {
	data := map[string]any{
		"display_session_id": event.DisplaySessionID,
		"mode":               event.Mode,
		"timestamp":          event.At.UTC().Format(time.RFC3339Nano),
	}
	if event.LegID != nil {
		data["leg_id"] = *event.LegID
	}
	data["kind"] = event.Kind
	return writeSSEFrame(w, "lifecycle", event.ID, data, true)
}

func writeSSEReset(w http.ResponseWriter) error {
	return writeSSEFrame(w, "reset", 0, map[string]any{"reason": "history_expired"}, false)
}

func (h *telemetryHTTPServer) writeSSESnapshot(w http.ResponseWriter, snapshot TelemetrySnapshot) error {
	h.accountSnapshot(snapshot)
	var traffic *TrafficReport
	if h.accounting != nil {
		report := h.trafficReport("today", snapshot)
		traffic = &report
	}
	return writeSSEAggregate(w, telemetrySSEAggregate{
		SnapshotAt:             snapshot.Timestamp,
		Generation:             h.generation,
		WindowDuration:         snapshot.WindowDuration,
		ActiveSessions:         snapshot.ActiveSessions,
		TotalSessions:          snapshot.TotalSessions,
		ActiveLegs:             snapshot.ActiveLegs,
		TotalWireTx:            snapshot.TotalWireTx,
		TotalWireRx:            snapshot.TotalWireRx,
		CurrentWireTxRate:      snapshot.CurrentWireTxRate,
		CurrentWireRxRate:      snapshot.CurrentWireRxRate,
		TelemetryDroppedEvents: snapshot.TelemetryDroppedEvents,
		Legs:                   snapshot.Legs,
		Roles:                  snapshot.Roles,
		Traffic:                traffic,
	})
}

func writeSSESnapshot(w http.ResponseWriter, snapshot TelemetrySnapshot) error {
	return writeSSEAggregate(w, telemetrySSEAggregate{
		SnapshotAt:             snapshot.Timestamp,
		WindowDuration:         snapshot.WindowDuration,
		ActiveSessions:         snapshot.ActiveSessions,
		TotalSessions:          snapshot.TotalSessions,
		ActiveLegs:             snapshot.ActiveLegs,
		TotalWireTx:            snapshot.TotalWireTx,
		TotalWireRx:            snapshot.TotalWireRx,
		CurrentWireTxRate:      snapshot.CurrentWireTxRate,
		CurrentWireRxRate:      snapshot.CurrentWireRxRate,
		TelemetryDroppedEvents: snapshot.TelemetryDroppedEvents,
		Legs:                   snapshot.Legs,
	})
}

func writeSSEAggregate(w http.ResponseWriter, aggregate telemetrySSEAggregate) error {
	legs := make([]map[string]any, 2)
	for leg := range aggregate.Legs {
		a := aggregate.Legs[leg]
		legs[leg] = map[string]any{
			"leg_id":                     leg,
			"ingress_role":               legIngressRoleFromAggregate(aggregate.Roles, leg),
			"active_sessions":            a.ActiveSessions,
			"active_connections":         a.ActiveConnections,
			"wire_tx_bytes":              a.WireTxBytes,
			"wire_rx_bytes":              a.WireRxBytes,
			"wire_tx_rate_bps":           a.WireTxRate,
			"wire_rx_rate_bps":           a.WireRxRate,
			"data_sent_bytes":            a.DataSentBytes,
			"data_sent_rate_bps":         a.DataSentRate,
			"wire_share":                 shareValue(a.WireShare),
			"wire_tx_share":              shareValue(a.WireTxShare),
			"wire_rx_share":              shareValue(a.WireRxShare),
			"logical_tx_acked_bytes":     a.LogicalTxAckedBytes,
			"logical_tx_acked_rate_bps":  a.LogicalTxAckedRate,
			"logical_rx_unique_bytes":    a.LogicalRxUniqueBytes,
			"logical_rx_unique_rate_bps": a.LogicalRxUniqueRate,
			"logical_tx_share":           shareValue(a.LogicalTxShare),
			"logical_rx_share":           shareValue(a.LogicalRxShare),
			"retransmit_bytes":           a.RetransmitBytes,
			"retransmit_frames":          a.RetransmitFrames,
			"rescue_frames":              a.RescueAttemptFrames,
			"rescue_bytes":               a.RescueAttemptBytes,
			"data_attempt_frames":        a.DataAttemptFrames,
		}
	}
	payload := map[string]any{
		"snapshot_at":              nullableTimeString(aggregate.SnapshotAt),
		"telemetry_generation":     aggregate.Generation,
		"window_ms":                aggregate.WindowDuration.Milliseconds(),
		"active_sessions":          aggregate.ActiveSessions,
		"total_sessions":           aggregate.TotalSessions,
		"active_legs":              aggregate.ActiveLegs,
		"wire_tx_bytes":            aggregate.TotalWireTx,
		"wire_rx_bytes":            aggregate.TotalWireRx,
		"wire_tx_rate_bps":         aggregate.CurrentWireTxRate,
		"wire_rx_rate_bps":         aggregate.CurrentWireRxRate,
		"telemetry_dropped_events": aggregate.TelemetryDroppedEvents,
		"legs":                     legs,
		"role_stats":               roleStatsJSON(TelemetrySnapshot{Roles: aggregate.Roles}),
	}
	if aggregate.Traffic != nil {
		payload["traffic"] = aggregate.Traffic
	}
	return writeSSEFrame(w, "snapshot", 0, payload, false)
}

func legIngressRoleFromAggregate(roles [4]TelemetryRoleAggregate, leg int) string {
	seen := make(map[string]struct{}, 2)
	for _, role := range roles {
		if role.Role == TelemetryIngressUnknown || role.Role == TelemetryIngressMixed {
			continue
		}
		value := role.Legs[leg]
		if value.DataSentBytes > 0 || value.LogicalTxAckedBytes > 0 || value.LogicalRxUniqueBytes > 0 || role.ActiveLegs > 0 {
			seen[role.Role] = struct{}{}
		}
	}
	if len(seen) == 1 {
		for role := range seen {
			return role
		}
	}
	if len(seen) > 1 {
		return TelemetryIngressMixed
	}
	return TelemetryIngressUnknown
}

func writeSSEKeepalive(w http.ResponseWriter) error {
	return writeSSEBytes(w, []byte(": keepalive\n\n"))
}

func writeSSEFrame(w http.ResponseWriter, event string, id uint64, payload any, withID bool) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	buffer := make([]byte, 0, len(data)+64)
	buffer = append(buffer, "event: "...)
	buffer = append(buffer, event...)
	buffer = append(buffer, '\n')
	if withID {
		buffer = append(buffer, "id: "...)
		buffer = strconv.AppendUint(buffer, id, 10)
		buffer = append(buffer, '\n')
	}
	buffer = append(buffer, "data: "...)
	buffer = append(buffer, data...)
	buffer = append(buffer, '\n', '\n')
	return writeSSEBytes(w, buffer)
}

func writeSSEBytes(w http.ResponseWriter, data []byte) error {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := w.Write(data)
	if err == nil {
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	_ = controller.SetWriteDeadline(time.Time{})
	return err
}

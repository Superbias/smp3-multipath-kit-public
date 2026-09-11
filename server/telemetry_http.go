package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	telemetryDefaultPageSize = 50
	telemetryMaxPageSize     = 200
	telemetrySnapshotCache   = 8
)

type telemetryHTTPServer struct {
	registry  *TelemetryRegistry
	version   string
	startedAt time.Time

	cacheMu sync.Mutex
	cache   map[string]TelemetrySnapshot
	order   []string

	server   *http.Server
	listener net.Listener
	closeOne sync.Once
}

func newTelemetryHTTPServer(registry *TelemetryRegistry, startedAt time.Time) *telemetryHTTPServer {
	return &telemetryHTTPServer{
		registry:  registry,
		version:   Version,
		startedAt: startedAt,
		cache:     make(map[string]TelemetrySnapshot),
	}
}

func (h *telemetryHTTPServer) Handler() http.Handler { return h }

func (h *telemetryHTTPServer) Start(listen string) error {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	h.listener = listener
	h.server = &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		// REST writes set per-response deadlines; SSE uses per-event deadlines
		// and must not inherit a fixed connection lifetime.
		WriteTimeout:   0,
		IdleTimeout:    30 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
	go func() { _ = h.server.Serve(listener) }()
	return nil
}

func (h *telemetryHTTPServer) Addr() net.Addr {
	if h == nil || h.listener == nil {
		return nil
	}
	return h.listener.Addr()
}

func (h *telemetryHTTPServer) Close() error {
	if h == nil {
		return nil
	}
	var err error
	h.closeOne.Do(func() {
		if h.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = h.server.Shutdown(ctx)
			cancel()
		} else if h.listener != nil {
			err = h.listener.Close()
		}
	})
	return err
}

func (h *telemetryHTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	head := r.Method == http.MethodHead
	if r.Method != http.MethodGet && !head {
		writeTelemetryJSON(w, http.StatusMethodNotAllowed, telemetryError{Error: telemetryErrorBody{"METHOD_NOT_ALLOWED", "method not allowed"}}, false)
		return
	}
	switch {
	case r.URL.Path == "/api/v1/events":
		if r.Method != http.MethodGet {
			writeTelemetryJSON(w, http.StatusMethodNotAllowed, telemetryError{Error: telemetryErrorBody{"METHOD_NOT_ALLOWED", "method not allowed"}}, false)
			return
		}
		h.handleEvents(w, r)
	case r.URL.Path == "/api/v1/status":
		h.handleStatus(w, head)
	case r.URL.Path == "/api/v1/legs":
		h.handleLegs(w, head)
	case r.URL.Path == "/api/v1/sessions":
		h.handleSessions(w, r, head)
	case strings.HasPrefix(r.URL.Path, "/api/v1/sessions/"):
		h.handleSessionDetail(w, r, head)
	case r.URL.Path == "/" || r.URL.Path == "/dashboard.js" || r.URL.Path == "/dashboard.css":
		h.handleDashboardAsset(w, r.URL.Path, head)
	default:
		writeTelemetryJSON(w, http.StatusNotFound, telemetryError{Error: telemetryErrorBody{"NOT_FOUND", "resource not found"}}, head)
	}
}

type telemetryError struct {
	Error telemetryErrorBody `json:"error"`
}

type telemetryErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeTelemetryJSON(w http.ResponseWriter, status int, value any, head bool) {
	body, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":{"code":"INTERNAL_SNAPSHOT_ERROR","message":"snapshot serialization failed"}}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	w.WriteHeader(status)
	if !head {
		_, _ = w.Write(body)
	}
	_ = controller.SetWriteDeadline(time.Time{})
}

func (h *telemetryHTTPServer) captureSnapshot() TelemetrySnapshot {
	snapshot := h.registry.LatestSnapshot()
	key := snapshot.Timestamp.UTC().Format(time.RFC3339Nano)
	h.cacheMu.Lock()
	if key != "0001-01-01T00:00:00Z" {
		if _, exists := h.cache[key]; !exists {
			h.order = append(h.order, key)
		}
		h.cache[key] = snapshot
		for len(h.order) > telemetrySnapshotCache {
			delete(h.cache, h.order[0])
			h.order = h.order[1:]
		}
	}
	h.cacheMu.Unlock()
	return snapshot
}

func (h *telemetryHTTPServer) snapshotForCursor(cursor string) (TelemetrySnapshot, *telemetryErrorBody) {
	current := h.captureSnapshot()
	if cursor == "" {
		return current, nil
	}
	var value telemetryCursor
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || json.Unmarshal(data, &value) != nil || value.Generation == "" {
		e := telemetryErrorBody{Code: "INVALID_CURSOR", Message: "invalid cursor"}
		return TelemetrySnapshot{}, &e
	}
	h.cacheMu.Lock()
	snapshot, ok := h.cache[value.Generation]
	h.cacheMu.Unlock()
	if !ok {
		e := telemetryErrorBody{Code: "STALE_CURSOR", Message: "cursor snapshot is no longer available"}
		return TelemetrySnapshot{}, &e
	}
	return snapshot, nil
}

func (h *telemetryHTTPServer) handleStatus(w http.ResponseWriter, head bool) {
	snapshot := h.captureSnapshot()
	uptime := int64(0)
	if !h.startedAt.IsZero() {
		uptime = time.Since(h.startedAt).Milliseconds()
		if uptime < 0 {
			uptime = 0
		}
	}
	value := map[string]any{
		"version":                  h.version,
		"uptime_ms":                uptime,
		"snapshot_at":              nullableTimeString(snapshot.Timestamp),
		"window_ms":                snapshot.WindowDuration.Milliseconds(),
		"active_sessions":          snapshot.ActiveSessions,
		"total_sessions":           snapshot.TotalSessions,
		"active_legs":              snapshot.ActiveLegs,
		"wire_tx_bytes":            snapshot.TotalWireTx,
		"wire_rx_bytes":            snapshot.TotalWireRx,
		"wire_tx_rate_bps":         snapshot.CurrentWireTxRate,
		"wire_rx_rate_bps":         snapshot.CurrentWireRxRate,
		"telemetry_dropped_events": snapshot.TelemetryDroppedEvents,
	}
	writeTelemetryJSON(w, http.StatusOK, value, head)
}

func (h *telemetryHTTPServer) handleLegs(w http.ResponseWriter, head bool) {
	snapshot := h.captureSnapshot()
	items := make([]map[string]any, 2)
	for leg := range snapshot.Legs {
		a := snapshot.Legs[leg]
		items[leg] = map[string]any{
			"leg_id":                  leg,
			"active_sessions":         a.ActiveSessions,
			"active_connections":      a.ActiveConnections,
			"wire_tx_bytes":           a.WireTxBytes,
			"wire_rx_bytes":           a.WireRxBytes,
			"wire_tx_rate_bps":        a.WireTxRate,
			"wire_rx_rate_bps":        a.WireRxRate,
			"wire_share":              shareValue(a.WireShare),
			"wire_tx_share":           shareValue(a.WireTxShare),
			"wire_rx_share":           shareValue(a.WireRxShare),
			"logical_tx_acked_bytes":  a.LogicalTxAckedBytes,
			"logical_rx_unique_bytes": a.LogicalRxUniqueBytes,
			"logical_tx_share":        shareValue(a.LogicalTxShare),
			"logical_rx_share":        shareValue(a.LogicalRxShare),
			"retransmit_bytes":        a.RetransmitBytes,
			"retransmit_frames":       a.RetransmitFrames,
			"rescue_frames":           a.RescueAttemptFrames,
			"rescue_bytes":            a.RescueAttemptBytes,
			"data_attempt_frames":     a.DataAttemptFrames,
			"first_data_count":        firstDataCount(snapshot, leg),
		}
	}
	writeTelemetryJSON(w, http.StatusOK, map[string]any{"snapshot_at": nullableTimeString(snapshot.Timestamp), "items": items}, head)
}

type telemetryCursor struct {
	Generation string `json:"generation"`
	LastKey    string `json:"last_key"`
}

func (h *telemetryHTTPServer) handleSessions(w http.ResponseWriter, r *http.Request, head bool) {
	limit, err := parseTelemetryLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeTelemetryJSON(w, http.StatusBadRequest, telemetryError{Error: *err}, head)
		return
	}
	snapshot, cursorErr := h.snapshotForCursor(r.URL.Query().Get("cursor"))
	if cursorErr != nil {
		writeTelemetryJSON(w, http.StatusBadRequest, telemetryError{Error: *cursorErr}, head)
		return
	}
	rows := append([]TelemetrySessionSnapshot(nil), snapshot.Sessions...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].DisplaySessionID < rows[j].DisplaySessionID })
	lastKey := ""
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		data, _ := base64.RawURLEncoding.DecodeString(cursor)
		var value telemetryCursor
		_ = json.Unmarshal(data, &value)
		lastKey = value.LastKey
	}
	start := 0
	for start < len(rows) && lastKey != "" && rows[start].DisplaySessionID <= lastKey {
		start++
	}
	end := start + limit
	if end > len(rows) {
		end = len(rows)
	}
	items := make([]any, 0, end-start)
	for _, row := range rows[start:end] {
		items = append(items, sessionCompactJSON(row, snapshot.Timestamp))
	}
	var next *string
	if end < len(rows) {
		value := telemetryCursor{Generation: snapshot.Timestamp.UTC().Format(time.RFC3339Nano), LastKey: rows[end-1].DisplaySessionID}
		data, _ := json.Marshal(value)
		encoded := base64.RawURLEncoding.EncodeToString(data)
		next = &encoded
	}
	writeTelemetryJSON(w, http.StatusOK, map[string]any{"snapshot_at": nullableTimeString(snapshot.Timestamp), "items": items, "next_cursor": next}, head)
}

func (h *telemetryHTTPServer) handleSessionDetail(w http.ResponseWriter, r *http.Request, head bool) {
	displayID := strings.TrimPrefix(r.URL.Path, "/api/v1/sessions/")
	if displayID == "" || strings.Contains(displayID, "/") {
		writeTelemetryJSON(w, http.StatusNotFound, telemetryError{Error: telemetryErrorBody{"NOT_FOUND", "resource not found"}}, head)
		return
	}
	snapshot := h.captureSnapshot()
	for _, row := range snapshot.Sessions {
		if row.DisplaySessionID == displayID {
			writeTelemetryJSON(w, http.StatusOK, map[string]any{"snapshot_at": nullableTimeString(snapshot.Timestamp), "session": sessionDetailJSON(row, snapshot.Timestamp)}, head)
			return
		}
	}
	writeTelemetryJSON(w, http.StatusNotFound, telemetryError{Error: telemetryErrorBody{"NOT_FOUND", "resource not found"}}, head)
}

func parseTelemetryLimit(raw string) (int, *telemetryErrorBody) {
	if raw == "" {
		return telemetryDefaultPageSize, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > telemetryMaxPageSize {
		e := telemetryErrorBody{Code: "INVALID_LIMIT", Message: "limit must be between 1 and 200"}
		return 0, &e
	}
	return value, nil
}

func firstDataCount(snapshot TelemetrySnapshot, leg int) int {
	count := 0
	for _, row := range snapshot.Sessions {
		if !row.Legs[leg].FirstDataAt.IsZero() {
			count++
		}
	}
	return count
}

func shareValue(share TelemetryShare) any {
	if !share.Valid {
		return nil
	}
	return share.Value
}

func nullableTimeString(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func sessionCompactJSON(row TelemetrySessionSnapshot, snapshotAt time.Time) map[string]any {
	result := map[string]any{
		"display_session_id":    row.DisplaySessionID,
		"mode":                  row.Mode,
		"created_at":            nullableTimeString(row.CreatedAt),
		"duration_ms":           durationAt(row.CreatedAt, row.ClosedAt, snapshotAt),
		"leg0_state":            row.Legs[0].State,
		"leg1_state":            row.Legs[1].State,
		"leg0_attach_at":        nullableTimeString(row.Legs[0].AttachAt),
		"leg1_attach_at":        nullableTimeString(row.Legs[1].AttachAt),
		"leg0_wire_tx_bytes":    row.Legs[0].WireTxBytes,
		"leg0_wire_rx_bytes":    row.Legs[0].WireRxBytes,
		"leg1_wire_tx_bytes":    row.Legs[1].WireTxBytes,
		"leg1_wire_rx_bytes":    row.Legs[1].WireRxBytes,
		"leg0_logical_tx_bytes": row.Legs[0].LogicalTxAckedBytes,
		"leg0_logical_rx_bytes": row.Legs[0].LogicalRxUniqueBytes,
		"leg1_logical_tx_bytes": row.Legs[1].LogicalTxAckedBytes,
		"leg1_logical_rx_bytes": row.Legs[1].LogicalRxUniqueBytes,
		"leg0_wire_share":       shareValue(sessionWireShare(row, 0)),
		"leg1_wire_share":       shareValue(sessionWireShare(row, 1)),
		"leg0_logical_tx_share": shareValue(sessionLogicalShare(row, 0, true)),
		"leg1_logical_tx_share": shareValue(sessionLogicalShare(row, 1, true)),
		"first_leg_ready_at":    nullableTimeString(row.FirstLegReadyAt),
		"first_data_seen_at":    nullableTimeString(row.FirstDataSeenAt),
		"leg0_first_data_at":    nullableTimeString(row.Legs[0].FirstDataAt),
		"leg1_first_data_at":    nullableTimeString(row.Legs[1].FirstDataAt),
		"startup_released_at":   nullableTimeString(row.StartupReleasedAt),
		"startup_result":        nullableString(row.StartupResult),
	}
	return result
}

func sessionDetailJSON(row TelemetrySessionSnapshot, snapshotAt time.Time) map[string]any {
	result := sessionCompactJSON(row, snapshotAt)
	result["startup"] = startupJSON(row.Startup)
	return result
}

func startupJSON(startup *TelemetryStartupSnapshot) any {
	if startup == nil {
		return nil
	}
	preferredLeg := any(nil)
	grace := any(nil)
	firstDataLeg := any(nil)
	if startup.Applied {
		preferredLeg = int(startup.PreferredLeg)
		grace = startup.Grace.Milliseconds()
	}
	if startup.FirstDataLeg >= 0 && startup.FirstDataLeg <= 1 {
		firstDataLeg = int(startup.FirstDataLeg)
	}
	policy := "first-ready"
	if startup.Policy == smp3core.StreamStartupPreferred {
		policy = "preferred"
	}
	var releaseReason any
	if value := startup.ReleaseReason.String(); value != "" {
		releaseReason = value
	}
	return map[string]any{
		"applied":                  startup.Applied,
		"policy":                   policy,
		"preferred_leg":            preferredLeg,
		"grace_ms":                 grace,
		"first_data_waiting_at":    nullableTimeString(startup.FirstDataWaitingAt),
		"grace_started_at":         nullableTimeString(startup.GraceStartedAt),
		"preferred_attached_at":    nullableTimeString(startup.PreferredAttachedAt),
		"nonpreferred_attached_at": nullableTimeString(startup.NonPreferredAttachedAt),
		"released_at":              nullableTimeString(startup.ReleasedAt),
		"release_reason":           releaseReason,
		"first_data_leg":           firstDataLeg,
	}
}

func durationAt(created, closed, snapshot time.Time) int64 {
	if created.IsZero() {
		return 0
	}
	end := snapshot
	if !closed.IsZero() {
		end = closed
	}
	if end.Before(created) {
		return 0
	}
	return end.Sub(created).Milliseconds()
}

func sessionWireShare(row TelemetrySessionSnapshot, leg int) TelemetryShare {
	total := uint64(0)
	for _, item := range row.Legs {
		total += item.WireTxBytes + item.WireRxBytes
	}
	value := row.Legs[leg].WireTxBytes + row.Legs[leg].WireRxBytes
	return calculateTelemetryShare(value, total)
}

func sessionLogicalShare(row TelemetrySessionSnapshot, leg int, tx bool) TelemetryShare {
	var total, value uint64
	for index, item := range row.Legs {
		if tx {
			value = row.Legs[leg].LogicalTxAckedBytes
			total += item.LogicalTxAckedBytes
		} else {
			value = row.Legs[leg].LogicalRxUniqueBytes
			total += item.LogicalRxUniqueBytes
		}
		if index == leg && !tx {
			value = item.LogicalRxUniqueBytes
		}
	}
	return calculateTelemetryShare(value, total)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

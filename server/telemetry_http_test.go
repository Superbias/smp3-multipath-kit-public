package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestTelemetryConfigAcceptsOnlyLiteralLoopback(t *testing.T) {
	accepted := []string{"127.0.0.1:24500", "[::1]:24500"}
	for _, listen := range accepted {
		cfg := testConfig()
		cfg.Telemetry.Enabled = true
		cfg.Telemetry.Listen = listen
		if err := cfg.NormalizeAndValidate(); err != nil {
			t.Fatalf("loopback %q rejected: %v", listen, err)
		}
	}
	for _, listen := range []string{"localhost:24500", "0.0.0.0:24500", "[::]:24500", "192.168.1.2:24500"} {
		cfg := testConfig()
		cfg.Telemetry.Enabled = true
		cfg.Telemetry.Listen = listen
		if err := cfg.NormalizeAndValidate(); err == nil {
			t.Fatalf("non-literal/non-loopback %q accepted", listen)
		}
	}
}

func TestTelemetryHTTPStatusAndHEADShareGETHeaders(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("http-test")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x42
	registry.RegisterSession(id, "stream", time.Now())
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if get.Code != http.StatusOK || get.Body.Len() == 0 {
		t.Fatalf("GET status=%d body=%q", get.Code, get.Body.String())
	}
	if get.Header().Get("Content-Type") == "" || get.Header().Get("Cache-Control") != "no-store" || get.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("GET headers=%v", get.Header())
	}

	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/api/v1/status", nil))
	if head.Code != get.Code || head.Body.Len() != 0 {
		t.Fatalf("HEAD status/body=%d/%d", head.Code, head.Body.Len())
	}
	for _, name := range []string{"Content-Type", "Cache-Control", "X-Content-Type-Options"} {
		if head.Header().Get(name) != get.Header().Get(name) {
			t.Fatalf("HEAD header %s=%q GET=%q", name, head.Header().Get(name), get.Header().Get(name))
		}
	}
}

func TestTelemetryHTTPTrafficAndHistoryExposeAuthoritativeAccounting(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("traffic-http-test")})
	defer registry.Close()
	httpServer := newTelemetryHTTPServer(registry, time.Now())
	at := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	httpServer.accounting.Collect(accountingTestSnapshot(at, accountingTestSession("session-a", TelemetryIngressStandalone, 100, 80, 200, 160)))
	httpServer.accounting.Collect(accountingTestSnapshot(at.Add(time.Second), accountingTestSession("session-a", TelemetryIngressStandalone, 1100, 880, 2200, 1760)))

	traffic := httptest.NewRecorder()
	httpServer.ServeHTTP(traffic, httptest.NewRequest(http.MethodGet, "/api/v1/traffic?period=all_recorded", nil))
	if traffic.Code != http.StatusOK || !strings.Contains(traffic.Body.String(), `"combined_carrier_bytes":3000`) || !strings.Contains(traffic.Body.String(), `"accounting_status":"COMPLETE"`) {
		t.Fatalf("traffic response=%d %q", traffic.Code, traffic.Body.String())
	}

	history := httptest.NewRecorder()
	httpServer.ServeHTTP(history, httptest.NewRequest(http.MethodGet, "/api/v1/traffic/history?resolution=hour", nil))
	if history.Code != http.StatusOK || !strings.Contains(history.Body.String(), `"resolution":"hour"`) || !strings.Contains(history.Body.String(), `"items":[`) {
		t.Fatalf("history response=%d %q", history.Code, history.Body.String())
	}

	status := httptest.NewRecorder()
	httpServer.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	for _, marker := range []string{"telemetry_generation", "role_stats", "traffic"} {
		if !strings.Contains(status.Body.String(), marker) {
			t.Fatalf("status missing %q: %q", marker, status.Body.String())
		}
	}
}

func TestTelemetryHTTPAPIsShareOneAuthoritativeSnapshot(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, SnapshotInterval: time.Hour, HMACSecret: []byte("api-consistency-test")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x73
	created := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	if !registry.RegisterSession(id, "stream", created) {
		t.Fatal("RegisterSession failed")
	}
	nativeCounters := &WireCounters{}
	standaloneCounters := &WireCounters{}
	registry.AttachLegWithRole(id, 0, 11, nativeCounters, TelemetryIngressNative, created.Add(time.Millisecond))
	registry.AttachLegWithRole(id, 1, 22, standaloneCounters, TelemetryIngressStandalone, created.Add(2*time.Millisecond))
	nativeCounters.txBytes.Store(101)
	nativeCounters.rxBytes.Store(11)
	standaloneCounters.txBytes.Store(202)
	standaloneCounters.rxBytes.Store(22)
	registry.mu.Lock()
	registry.sessions[id].legs[0].dataSentBytes = 1000
	registry.sessions[id].legs[0].logicalTxAckedBytes = 900
	registry.sessions[id].legs[0].logicalRxUniqueBytes = 800
	registry.sessions[id].legs[1].dataSentBytes = 2000
	registry.sessions[id].legs[1].logicalTxAckedBytes = 1800
	registry.sessions[id].legs[1].logicalRxUniqueBytes = 1600
	registry.mu.Unlock()

	h := newTelemetryHTTPServer(registry, created)
	defer h.Close()
	expected := registry.Snapshot()
	handler := h.Handler()

	var status struct {
		Generation     string `json:"telemetry_generation"`
		ActiveSessions uint64 `json:"active_sessions"`
		ActiveLegs     uint64 `json:"active_legs"`
		WireTxBytes    uint64 `json:"wire_tx_bytes"`
		RoleStats      []struct {
			Role          string `json:"role"`
			ActiveLegs    uint64 `json:"active_legs"`
			DataSentBytes uint64 `json:"data_sent_bytes"`
			UsefulBytes   uint64 `json:"logical_tx_acked_bytes"`
			Legs          []struct {
				DataSentBytes uint64 `json:"data_sent_bytes"`
				UsefulBytes   uint64 `json:"logical_tx_acked_bytes"`
			} `json:"legs"`
		} `json:"role_stats"`
	}
	statusRecorder := httptest.NewRecorder()
	handler.ServeHTTP(statusRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if statusRecorder.Code != http.StatusOK || json.Unmarshal(statusRecorder.Body.Bytes(), &status) != nil {
		t.Fatalf("status response=%d %q", statusRecorder.Code, statusRecorder.Body.String())
	}
	if status.Generation != h.generation || status.ActiveSessions != expected.ActiveSessions || status.ActiveLegs != expected.ActiveLegs || status.WireTxBytes != expected.TotalWireTx {
		t.Fatalf("status drift: got=%+v expected generation=%s sessions=%d legs=%d wire_tx=%d", status, h.generation, expected.ActiveSessions, expected.ActiveLegs, expected.TotalWireTx)
	}
	if len(status.RoleStats) != len(expected.Roles) {
		t.Fatalf("role count=%d expected=%d", len(status.RoleStats), len(expected.Roles))
	}
	for index, role := range expected.Roles {
		got := status.RoleStats[index]
		if got.Role != role.Role || got.ActiveLegs != role.ActiveLegs || got.DataSentBytes != role.DataSentBytes || got.UsefulBytes != role.LogicalTxAckedBytes {
			t.Fatalf("role[%d] drift: got=%+v expected=%+v", index, got, role)
		}
		for leg := range role.Legs {
			if len(got.Legs) <= leg || got.Legs[leg].DataSentBytes != role.Legs[leg].DataSentBytes || got.Legs[leg].UsefulBytes != role.Legs[leg].LogicalTxAckedBytes {
				t.Fatalf("role[%d] leg[%d] drift: got=%+v expected=%+v", index, leg, got.Legs, role.Legs)
			}
		}
	}

	var legs struct {
		Items []struct {
			LegID         int    `json:"leg_id"`
			IngressRole   string `json:"ingress_role"`
			DataSentBytes uint64 `json:"data_sent_bytes"`
			UsefulBytes   uint64 `json:"logical_tx_acked_bytes"`
		} `json:"items"`
	}
	legsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(legsRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/legs", nil))
	if legsRecorder.Code != http.StatusOK || json.Unmarshal(legsRecorder.Body.Bytes(), &legs) != nil || len(legs.Items) != len(expected.Legs) {
		t.Fatalf("legs response=%d %q", legsRecorder.Code, legsRecorder.Body.String())
	}
	for index, expectedLeg := range expected.Legs {
		got := legs.Items[index]
		if got.LegID != index || got.IngressRole != legIngressRole(expected, index) || got.DataSentBytes != expectedLeg.DataSentBytes || got.UsefulBytes != expectedLeg.LogicalTxAckedBytes {
			t.Fatalf("leg[%d] drift: got=%+v expected=%+v", index, got, expectedLeg)
		}
	}

	var sessions struct {
		Items []map[string]any `json:"items"`
	}
	sessionsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(sessionsRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=50", nil))
	if sessionsRecorder.Code != http.StatusOK || json.Unmarshal(sessionsRecorder.Body.Bytes(), &sessions) != nil || len(sessions.Items) != len(expected.Sessions) {
		t.Fatalf("sessions response=%d %q", sessionsRecorder.Code, sessionsRecorder.Body.String())
	}
}

func TestTelemetryHTTPRejectsMutationAndRedactsSentinel(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("http-test")})
	defer registry.Close()
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(method, "/api/v1/status", nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method %s status=%d", method, recorder.Code)
		}
	}
	errorRecorder := httptest.NewRecorder()
	handler.ServeHTTP(errorRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/does-not-exist", nil))
	if errorRecorder.Code != http.StatusNotFound || strings.Contains(errorRecorder.Body.String(), "SECRET_SHOULD_NOT_APPEAR") {
		t.Fatalf("sanitized error status/body=%d/%q", errorRecorder.Code, errorRecorder.Body.String())
	}
}

func TestTelemetryHTTPSessionsPaginationBindsSnapshot(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 16, HMACSecret: []byte("pagination-test")})
	defer registry.Close()
	for i := 0; i < 3; i++ {
		var id smp3core.SessionID
		id[0] = byte(i + 1)
		registry.RegisterSession(id, "stream", time.Now())
	}
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=2", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first page status=%d body=%q", first.Code, first.Body.String())
	}
	var page1 struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page1); err != nil {
		t.Fatal(err)
	}
	if len(page1.Items) != 2 || page1.NextCursor == nil || *page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=2&cursor="+*page1.NextCursor, nil))
	if second.Code != http.StatusOK {
		t.Fatalf("second page status=%d body=%q", second.Code, second.Body.String())
	}
	var page2 struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &page2); err != nil {
		t.Fatal(err)
	}
	if len(page2.Items) != 1 {
		t.Fatalf("page2=%+v", page2)
	}
	if page1.Items[0]["display_session_id"] == page2.Items[0]["display_session_id"] || page1.Items[1]["display_session_id"] == page2.Items[0]["display_session_id"] {
		t.Fatalf("pagination repeated a row: page1=%v page2=%v", page1.Items, page2.Items)
	}

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?cursor=not-valid", nil))
	if invalid.Code != http.StatusBadRequest || !strings.Contains(invalid.Body.String(), "INVALID_CURSOR") {
		t.Fatalf("invalid cursor status/body=%d/%q", invalid.Code, invalid.Body.String())
	}
}

func TestTelemetryHTTPSessionsRejectsStaleCursor(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 16, SnapshotInterval: 5 * time.Millisecond, HMACSecret: []byte("stale-cursor")})
	defer registry.Close()
	for i := 0; i < 3; i++ {
		var id smp3core.SessionID
		id[0] = byte(i + 1)
		registry.RegisterSession(id, "stream", time.Now())
	}
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=2", nil))
	var page struct {
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == nil {
		t.Fatalf("first page=%q err=%v", first.Body.String(), err)
	}
	for i := 0; i < telemetrySnapshotCache+4; i++ {
		time.Sleep(15 * time.Millisecond)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	}
	stale := httptest.NewRecorder()
	handler.ServeHTTP(stale, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?cursor="+*page.NextCursor, nil))
	if stale.Code != http.StatusBadRequest || !strings.Contains(stale.Body.String(), "STALE_CURSOR") {
		t.Fatalf("stale cursor status/body=%d/%q", stale.Code, stale.Body.String())
	}
}

func TestTelemetryHTTPLegsAndSessionDetailUsePrivacySafeSnapshot(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("detail-test")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 99
	registry.RegisterSession(id, "stream", time.Now())
	registry.AttachLeg(id, 0, 1, &WireCounters{}, time.Now())
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=1", nil))
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
		t.Fatalf("session list=%q err=%v", list.Body.String(), err)
	}
	displayID, ok := page.Items[0]["display_session_id"].(string)
	if !ok || displayID == "" {
		t.Fatalf("display ID missing: %+v", page.Items[0])
	}

	legs := httptest.NewRecorder()
	handler.ServeHTTP(legs, httptest.NewRequest(http.MethodGet, "/api/v1/legs", nil))
	if legs.Code != http.StatusOK || !strings.Contains(legs.Body.String(), `"wire_share":null`) {
		t.Fatalf("legs response=%d %q", legs.Code, legs.Body.String())
	}

	detail := httptest.NewRecorder()
	handler.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+displayID, nil))
	if detail.Code != http.StatusOK || strings.Contains(detail.Body.String(), "target") || strings.Contains(detail.Body.String(), "SECRET_SHOULD_NOT_APPEAR") {
		t.Fatalf("detail response=%d %q", detail.Code, detail.Body.String())
	}
}

func TestTelemetryHTTPListenerDisabledAndEnabledLifecycle(t *testing.T) {
	disabled := testConfig()
	disabled.Telemetry.Enabled = false
	disabledServer, err := New(disabled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := disabledServer.Start(); err != nil {
		t.Fatal(err)
	}
	if disabledServer.TelemetryHTTPAddr() != nil {
		t.Fatal("disabled telemetry started an HTTP listener")
	}
	_ = disabledServer.Close()

	enabled := testConfig()
	enabled.Telemetry.Enabled = true
	enabled.Telemetry.Listen = "127.0.0.1:0"
	enabledServer, err := New(enabled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := enabledServer.Start(); err != nil {
		t.Fatal(err)
	}
	defer enabledServer.Close()
	if enabledServer.TelemetryHTTPAddr() == nil {
		t.Fatal("enabled telemetry did not start HTTP listener")
	}
	response, err := http.Get("http://" + enabledServer.TelemetryHTTPAddr().String() + "/api/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status code=%d", response.StatusCode)
	}
}

func TestTelemetryHTTPEnabledBindFailureFailsStartup(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	cfg := testConfig()
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.Listen = occupied.Addr().String()
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err == nil {
		_ = instance.Close()
		t.Fatal("enabled telemetry bind collision did not fail startup")
	}
	_ = instance.Close()
}

func TestTelemetryHTTPShutdownClosesSSE(t *testing.T) {
	cfg := testConfig()
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.Listen = "127.0.0.1:0"
	instance := startTestServer(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + instance.TelemetryHTTPAddr().String() + "/api/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(response.Body)
		readDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("SSE body did not close during server shutdown")
	}
	_ = response.Body.Close()
}

type blockingTelemetryWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
	status  int
}

func (w *blockingTelemetryWriter) Header() http.Header    { return w.header }
func (w *blockingTelemetryWriter) WriteHeader(status int) { w.status = status }
func (w *blockingTelemetryWriter) Write(data []byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.release
	return len(data), nil
}

func TestTelemetryHTTPSlowWriterDoesNotHoldRegistry(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("slow-writer")})
	defer registry.Close()
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	writer := &blockingTelemetryWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
		close(done)
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("HTTP writer did not block")
	}
	var id smp3core.SessionID
	id[0] = 77
	registered := make(chan bool, 1)
	go func() { registered <- registry.RegisterSession(id, "stream", time.Now()) }()
	select {
	case <-registered:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("slow HTTP writer blocked registry lifecycle")
	}
	close(writer.release)
	<-done
}

func BenchmarkTelemetryHTTPStatus(b *testing.B) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 128, HMACSecret: []byte("http-bench")})
	defer registry.Close()
	for i := 0; i < 50; i++ {
		var id smp3core.SessionID
		id[0] = byte(i)
		registry.RegisterSession(id, "stream", time.Now())
	}
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	}
}

func BenchmarkTelemetryHTTPSessions50(b *testing.B) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxActiveSessions: 128, HMACSecret: []byte("http-bench")})
	defer registry.Close()
	for i := 0; i < 50; i++ {
		var id smp3core.SessionID
		id[0] = byte(i)
		registry.RegisterSession(id, "stream", time.Now())
	}
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=50", nil))
	}
}

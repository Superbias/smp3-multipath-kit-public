package panel

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Superbias/smp3-multipath-kit-public/panel/monitor"
)

type panelTelemetry struct {
	at time.Time
	tx uint64
}

func (f *panelTelemetry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	at := f.at.UTC().Format(time.RFC3339Nano)
	switch r.URL.Path {
	case "/api/v1/status":
		_ = json.NewEncoder(w).Encode(map[string]any{"snapshot_at": at, "active_sessions": 1, "total_sessions": 1, "active_legs": 2, "wire_tx_bytes": f.tx, "wire_rx_bytes": f.tx, "telemetry_dropped_events": 0})
	case "/api/v1/legs":
		_ = json.NewEncoder(w).Encode(map[string]any{"snapshot_at": at, "items": []any{
			map[string]any{"leg_id": 0, "active_connections": 1, "wire_tx_bytes": f.tx / 2, "wire_rx_bytes": f.tx / 2, "logical_tx_acked_bytes": f.tx / 4, "logical_rx_unique_bytes": f.tx / 4},
			map[string]any{"leg_id": 1, "active_connections": 1, "wire_tx_bytes": f.tx / 2, "wire_rx_bytes": f.tx / 2, "logical_tx_acked_bytes": f.tx / 4, "logical_rx_unique_bytes": f.tx / 4},
		}})
	case "/api/v1/events":
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		http.NotFound(w, r)
	}
}

func newPanelForTest(t *testing.T, telemetry *panelTelemetry) (*Server, func()) {
	t.Helper()
	upstream := httptest.NewServer(telemetry)
	cfg := monitor.DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.TelemetryURL = upstream.URL
	cfg.Probe = func(target monitor.ListenerTarget) monitor.ListenerHealth {
		return monitor.ListenerHealth{ListenerTarget: target, ListenerUp: true, ProcessUp: true, Probe: "test"}
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return server, func() { server.Close(); upstream.Close() }
}

func TestMonitorRESTAndHEADContract(t *testing.T) {
	telemetry := &panelTelemetry{at: time.Unix(300, 0), tx: 100}
	server, cleanup := newPanelForTest(t, telemetry)
	defer cleanup()
	if err := server.Monitor().CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/monitor/status", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("GET status=%d headers=%v", get.Code, get.Header())
	}
	if strings.Contains(get.Body.String(), "SessionID") || strings.Contains(get.Body.String(), "destination") || strings.Contains(get.Body.String(), "target") {
		t.Fatalf("privacy leak: %s", get.Body.String())
	}
	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/api/monitor/status", nil))
	if head.Code != get.Code || head.Body.Len() != 0 || head.Header().Get("Content-Type") != get.Header().Get("Content-Type") || head.Header().Get("Cache-Control") != get.Header().Get("Cache-Control") || head.Header().Get("X-Content-Type-Options") != get.Header().Get("X-Content-Type-Options") {
		t.Fatalf("HEAD differs: get=%v head=%v body=%d", get.Header(), head.Header(), head.Body.Len())
	}
	for _, path := range []string{"/api/monitor/history?scope=memory", "/api/monitor/events?limit=10"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, recorder.Code)
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(method, "/api/monitor/status", nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status=%d", method, recorder.Code)
		}
	}
}

func TestMonitorSSESnapshotAndAssetIsolation(t *testing.T) {
	telemetry := &panelTelemetry{at: time.Unix(400, 0), tx: 100}
	server, cleanup := newPanelForTest(t, telemetry)
	defer cleanup()
	if err := server.Monitor().CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	response, err := http.Get(httpServer.URL + "/api/monitor/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("SSE status/headers=%d/%v", response.StatusCode, response.Header)
	}
	reader := bufio.NewReader(response.Body)
	seenSnapshot := false
	for i := 0; i < 5; i++ {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.TrimSpace(line) == "event: snapshot" {
			seenSnapshot = true
			break
		}
	}
	if !seenSnapshot {
		t.Fatal("initial snapshot missing")
	}
	telemetry.at = telemetry.at.Add(time.Second)
	telemetry.tx = 300
	if err := server.Monitor().CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.TrimSpace(line) == "event: snapshot" {
			return
		}
	}
	t.Fatal("updated snapshot missing")
}

func TestMonitorAssetsCSPAndNoExternalResource(t *testing.T) {
	server, cleanup := newPanelForTest(t, &panelTelemetry{at: time.Unix(500, 0), tx: 100})
	defer cleanup()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	body, _ := io.ReadAll(recorder.Result().Body)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("asset response=%d headers=%v", recorder.Code, recorder.Header())
	}
	text := string(body)
	if strings.Contains(text, "http://") || strings.Contains(text, "https://") || strings.Contains(text, "onclick=") || strings.Contains(text, "style=") {
		t.Fatalf("external or inline asset detected: %s", text)
	}
	for _, path := range []string{"/monitor.js", "/monitor.css"} {
		asset := httptest.NewRecorder()
		server.Handler().ServeHTTP(asset, httptest.NewRequest(http.MethodGet, path, nil))
		assetBody, _ := io.ReadAll(asset.Result().Body)
		assetText := string(assetBody)
		if asset.Code != http.StatusOK || strings.Contains(assetText, "http://") || strings.Contains(assetText, "https://") || strings.Contains(assetText, "fetch(\"http") || strings.Contains(assetText, "POST") || strings.Contains(assetText, "PUT") || strings.Contains(assetText, "DELETE") || strings.Contains(assetText, "target") || strings.Contains(assetText, "SessionID") {
			t.Fatalf("unsafe asset %s: %s", path, assetText)
		}
	}
	traversal := httptest.NewRecorder()
	server.Handler().ServeHTTP(traversal, httptest.NewRequest(http.MethodGet, "/../monitor.js", nil))
	if traversal.Code == http.StatusOK {
		t.Fatal("asset allowlist accepted traversal")
	}
}

func TestPanelServerStartAndClose(t *testing.T) {
	telemetry := &panelTelemetry{at: time.Now().UTC(), tx: 100}
	server, cleanup := newPanelForTest(t, telemetry)
	defer cleanup()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if server.Addr() == nil {
		t.Fatal("panel listener missing")
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + server.Addr().String() + "/api/monitor/status")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable && response.StatusCode != http.StatusOK {
		t.Fatalf("startup status=%d", response.StatusCode)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

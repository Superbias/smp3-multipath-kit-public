package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDashboardStaticAllowlistAndAPIPriority(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("dashboard-test")})
	defer registry.Close()
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	for _, path := range []string{"/", "/dashboard.js", "/dashboard.css"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("static path %s status=%d body=%q", path, recorder.Code, recorder.Body.String())
		}
	}
	api := httptest.NewRecorder()
	handler.ServeHTTP(api, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if api.Code != http.StatusOK || !strings.Contains(api.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("API route was swallowed by static handler: status=%d content-type=%q", api.Code, api.Header().Get("Content-Type"))
	}
	traversal := httptest.NewRecorder()
	handler.ServeHTTP(traversal, httptest.NewRequest(http.MethodGet, "/../dashboard.js", nil))
	if traversal.Code == http.StatusOK {
		t.Fatal("static handler accepted traversal path")
	}
}

func TestDashboardCSPAndReadOnlyPrivacyContract(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("dashboard-test")})
	defer registry.Close()
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	if page.Header().Get("Content-Security-Policy") == "" || !strings.Contains(page.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Fatalf("missing self-only CSP: %q", page.Header().Get("Content-Security-Policy"))
	}
	if strings.Contains(page.Body.String(), "SECRET_SHOULD_NOT_APPEAR") || strings.Contains(page.Body.String(), "target") {
		t.Fatalf("dashboard HTML privacy leak: %q", page.Body.String())
	}

	js := httptest.NewRecorder()
	handler.ServeHTTP(js, httptest.NewRequest(http.MethodGet, "/dashboard.js", nil))
	jsBody := js.Body.String()
	if strings.Contains(jsBody, "style=") || strings.Contains(page.Body.String(), "<script>") || strings.Contains(page.Body.String(), "<style>") {
		t.Fatal("dashboard used inline script/style")
	}
	if strings.Contains(jsBody, "method: 'POST'") || strings.Contains(jsBody, "method: 'PUT'") || strings.Contains(jsBody, "method: 'PATCH'") || strings.Contains(jsBody, "method: 'DELETE'") {
		t.Fatalf("dashboard JS contains mutation request: %q", jsBody)
	}
	if strings.Contains(jsBody, "SECRET_SHOULD_NOT_APPEAR") || strings.Contains(jsBody, "raw_session_id") || strings.Contains(jsBody, "target") {
		t.Fatalf("dashboard JS privacy leak: %q", jsBody)
	}
	if strings.Contains(jsBody, "http://") || strings.Contains(jsBody, "https://") || strings.Contains(jsBody, "EventSource") == false {
		t.Fatalf("dashboard JS external resource or missing SSE contract: %q", jsBody)
	}

	css := httptest.NewRecorder()
	handler.ServeHTTP(css, httptest.NewRequest(http.MethodGet, "/dashboard.css", nil))
	cssBody, _ := io.ReadAll(css.Body)
	if strings.Contains(string(cssBody), "url(http") || strings.Contains(string(cssBody), "fonts.googleapis") {
		t.Fatalf("dashboard CSS external resource: %q", cssBody)
	}
}

func TestDashboardVisualStructureAndAccessibilityContract(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("visual-test")})
	defer registry.Close()
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	html := page.Body.String()
	for _, required := range []string{"dashboard-sidebar", "overview", "legs", "sessions", "traffic", "traffic-period", "detail-drawer", "detail-close", "aria-label"} {
		if !strings.Contains(html, required) {
			t.Fatalf("dashboard HTML missing visual/accessibility marker %q", required)
		}
	}
	js := httptest.NewRecorder()
	handler.ServeHTTP(js, httptest.NewRequest(http.MethodGet, "/dashboard.js", nil))
	jsBody := js.Body.String()
	for _, required := range []string{"Escape", "detail-close", "sessions-next", "sessions-prev", "renderStartup", "renderTrafficVolume", "traffic-period", "combined_carrier_bytes", "preferred_arrived_within_grace", "Session closed before release", "Startup Status"} {
		if !strings.Contains(jsBody, required) {
			t.Fatalf("dashboard JS missing UX behavior marker %q", required)
		}
	}
	css := httptest.NewRecorder()
	handler.ServeHTTP(css, httptest.NewRequest(http.MethodGet, "/dashboard.css", nil))
	cssBody := css.Body.String()
	for _, required := range []string{"font-variant-numeric: tabular-nums", "@media (max-width: 1024px)", "overflow-x: auto", "prefers-reduced-motion", ".startup-card", ".late-marker", ".accounting-status", ".traffic-columns"} {
		if !strings.Contains(cssBody, required) {
			t.Fatalf("dashboard CSS missing visual marker %q", required)
		}
	}
}

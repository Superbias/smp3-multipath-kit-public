package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDashboardBrowserSmoke(t *testing.T) {
	chrome := ""
	candidates := []string{"chromium", "chromium-browser", "google-chrome"}
	if runtime.GOOS == "windows" {
		candidates = []string{
			"chrome.exe",
			`C:\Program Files\Google\Chrome\Application\chrome.exe`,
			`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
		}
	}
	for _, candidate := range candidates {
		if path, err := exec.LookPath(candidate); err == nil {
			chrome = path
			break
		}
		if _, err := exec.Command(candidate, "--version").Output(); err == nil {
			chrome = candidate
			break
		}
	}
	if chrome == "" {
		t.Skip("Chrome is not installed")
	}

	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, SnapshotInterval: 25 * time.Millisecond, HMACSecret: []byte("browser-test")})
	defer registry.Close()
	h := newTelemetryHTTPServer(registry, time.Now())
	base := h.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			base.ServeHTTP(w, r)
			return
		}
		body, err := dashboardAssets.ReadFile("dashboard/index.html")
		if err != nil {
			http.Error(w, "dashboard asset unavailable", http.StatusInternalServerError)
			return
		}
		// The real page intentionally keeps its EventSource open. A dump-dom
		// browser test needs the same REST rendering without a reconnect loop.
		stub := []byte(`<script>window.EventSource=function(){this.readyState=2;this.addEventListener=function(){};};window.EventSource.CLOSED=2;</script>`)
		body = bytes.Replace(body, []byte(`<script src="/dashboard.js" defer></script>`), append(stub, []byte(`<script src="/dashboard.js" defer></script>`)...), 1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	defer h.Close()

	profile := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, chrome,
		"--headless=new", "--disable-gpu", "--no-sandbox",
		"--user-data-dir="+filepath.Join(profile, "profile"),
		"--virtual-time-budget=1800", "--dump-dom", server.URL+"/")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("headless browser failed: %v\n%s", err, output)
	}
	dom := string(output)
	for _, marker := range []string{"System overview", "Traffic volume", "Native vs Standalone", "METRICS INFO"} {
		if !strings.Contains(dom, marker) {
			t.Fatalf("browser DOM missing %q", marker)
		}
	}
}

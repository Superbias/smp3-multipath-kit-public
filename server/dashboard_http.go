package server

import (
	"net/http"
	"strconv"
)

const dashboardCSP = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"

func (h *telemetryHTTPServer) handleDashboardAsset(w http.ResponseWriter, path string, head bool) {
	asset := map[string]struct {
		name        string
		contentType string
	}{
		"/":              {name: "dashboard/index.html", contentType: "text/html; charset=utf-8"},
		"/dashboard.js":  {name: "dashboard/dashboard.js", contentType: "text/javascript; charset=utf-8"},
		"/dashboard.css": {name: "dashboard/dashboard.css", contentType: "text/css; charset=utf-8"},
	}[path]
	if asset.name == "" {
		writeTelemetryJSON(w, http.StatusNotFound, telemetryError{Error: telemetryErrorBody{"NOT_FOUND", "resource not found"}}, head)
		return
	}
	body, err := dashboardAssets.ReadFile(asset.name)
	if err != nil {
		writeTelemetryJSON(w, http.StatusInternalServerError, telemetryError{Error: telemetryErrorBody{"INTERNAL_ASSET_ERROR", "asset unavailable"}}, head)
		return
	}
	w.Header().Set("Content-Type", asset.contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", dashboardCSP)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if !head {
		_, _ = w.Write(body)
	}
}

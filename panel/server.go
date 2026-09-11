package panel

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Superbias/smp3-multipath-kit-public/panel/monitor"
)

//go:embed web/index.html web/monitor.js web/monitor.css
var webAssets embed.FS

type Server struct {
	cfg      monitor.Config
	monitor  *monitor.Monitor
	server   *http.Server
	listener net.Listener
	closeOne sync.Once
}

func New(cfg monitor.Config) (*Server, error) {
	m, err := monitor.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Server{cfg: m.Config(), monitor: m}, nil
}

func (s *Server) Monitor() *monitor.Monitor { return s.monitor }
func (s *Server) Handler() http.Handler     { return s.handler() }
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) Start() error {
	if s.listener != nil {
		return errors.New("panel already started")
	}
	listener, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen panel: %w", err)
	}
	s.listener = listener
	s.monitor.Start()
	s.server = &http.Server{Handler: s.handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 0, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	go func() { _ = s.server.Serve(listener) }()
	return nil
}

func (s *Server) Close() error {
	var result error
	s.closeOne.Do(func() {
		if s.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			result = s.server.Shutdown(ctx)
			cancel()
		} else if s.listener != nil {
			result = s.listener.Close()
		}
		s.monitor.Close()
	})
	return result
}

const monitorCSP = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"

type apiError struct {
	Error apiErrorBody `json:"error"`
}
type apiErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *Server) handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	head := r.Method == http.MethodHead
	if r.Method != http.MethodGet && !head {
		writeJSON(w, http.StatusMethodNotAllowed, apiError{apiErrorBody{"METHOD_NOT_ALLOWED", "method not allowed"}}, false)
		return
	}
	switch {
	case r.URL.Path == "/api/monitor/stream":
		if head {
			writeJSON(w, http.StatusMethodNotAllowed, apiError{apiErrorBody{"METHOD_NOT_ALLOWED", "method not allowed"}}, true)
			return
		}
		s.handleStream(w, r)
	case r.URL.Path == "/api/monitor/status":
		s.handleStatus(w, head)
	case r.URL.Path == "/api/monitor/history":
		s.handleHistory(w, r, head)
	case r.URL.Path == "/api/monitor/events":
		s.handleEvents(w, r, head)
	case r.URL.Path == "/" || r.URL.Path == "/monitor.js" || r.URL.Path == "/monitor.css":
		s.handleAsset(w, r.URL.Path, head)
	default:
		writeJSON(w, http.StatusNotFound, apiError{apiErrorBody{"NOT_FOUND", "resource not found"}}, head)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, head bool) {
	snapshot, ok := s.monitor.Latest()
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, apiError{apiErrorBody{"TELEMETRY_UNAVAILABLE", "telemetry is unavailable"}}, head)
		return
	}
	writeJSON(w, http.StatusOK, snapshot, head)
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request, head bool) {
	limit, err := parseLimit(r.URL.Query().Get("limit"), 3600)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{*err}, head)
		return
	}
	scope := r.URL.Query().Get("scope")
	if scope != "" && scope != "memory" && scope != "persistent" && scope != "all" {
		writeJSON(w, http.StatusBadRequest, apiError{apiErrorBody{"INVALID_SCOPE", "invalid history scope"}}, head)
		return
	}
	writeJSON(w, http.StatusOK, s.monitor.History(scope, limit), head)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request, head bool) {
	limit, err := parseLimit(r.URL.Query().Get("limit"), 256)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{*err}, head)
		return
	}
	writeJSON(w, http.StatusOK, s.monitor.Events(limit), head)
}

func parseLimit(raw string, maximum int) (int, *apiErrorBody) {
	if raw == "" {
		return maximum, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maximum {
		return 0, &apiErrorBody{"INVALID_LIMIT", "limit is out of range"}
	}
	return value, nil
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	updates, unsubscribe, accepted := s.monitor.Subscribe()
	if !accepted {
		writeJSON(w, http.StatusServiceUnavailable, apiError{apiErrorBody{"SSE_CLIENT_LIMIT", "SSE client limit reached"}}, false)
		return
	}
	defer unsubscribe()
	snapshot, ok := s.monitor.Latest()
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, apiError{apiErrorBody{"TELEMETRY_UNAVAILABLE", "telemetry is unavailable"}}, false)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, apiError{apiErrorBody{"SSE_UNSUPPORTED", "stream unavailable"}}, false)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if err := writeSSE(w, "snapshot", 0, snapshot); err != nil {
		return
	}
	flusher.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case message, open := <-updates:
			if !open {
				return
			}
			if message.Snapshot != nil {
				if err := writeSSE(w, "snapshot", 0, *message.Snapshot); err != nil {
					return
				}
				flushSSE(w, flusher)
			}
			if message.Event != nil {
				if err := writeSSE(w, "event", message.Event.ID, *message.Event); err != nil {
					return
				}
				flushSSE(w, flusher)
			}
		case <-keepalive.C:
			controller := http.NewResponseController(w)
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := w.Write([]byte(": keepalive\n\n"))
			_ = controller.SetWriteDeadline(time.Time{})
			if err != nil {
				return
			}
			flushSSE(w, flusher)
		}
	}
}

func flushSSE(w http.ResponseWriter, flusher http.Flusher) {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	flusher.Flush()
	_ = controller.SetWriteDeadline(time.Time{})
}

func writeSSE(w http.ResponseWriter, name string, id uint64, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var builder strings.Builder
	builder.WriteString("event: ")
	builder.WriteString(name)
	builder.WriteByte('\n')
	if id != 0 {
		builder.WriteString("id: ")
		builder.WriteString(strconv.FormatUint(id, 10))
		builder.WriteByte('\n')
	}
	builder.WriteString("data: ")
	builder.Write(data)
	builder.WriteString("\n\n")
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = w.Write([]byte(builder.String()))
	_ = controller.SetWriteDeadline(time.Time{})
	return err
}

func writeJSON(w http.ResponseWriter, status int, value any, head bool) {
	body, err := json.Marshal(value)
	if err != nil {
		status, body = http.StatusInternalServerError, []byte(`{"error":{"code":"INTERNAL_ERROR","message":"response unavailable"}}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if !head {
		_, _ = w.Write(body)
	}
}

func (s *Server) handleAsset(w http.ResponseWriter, path string, head bool) {
	assets := map[string]struct{ name, contentType string }{
		"/":            {"web/index.html", "text/html; charset=utf-8"},
		"/monitor.js":  {"web/monitor.js", "text/javascript; charset=utf-8"},
		"/monitor.css": {"web/monitor.css", "text/css; charset=utf-8"},
	}
	asset, ok := assets[path]
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{apiErrorBody{"NOT_FOUND", "resource not found"}}, head)
		return
	}
	body, err := webAssets.ReadFile(asset.name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{apiErrorBody{"INTERNAL_ASSET_ERROR", "asset unavailable"}}, head)
		return
	}
	w.Header().Set("Content-Type", asset.contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", monitorCSP)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if !head {
		_, _ = w.Write(body)
	}
}

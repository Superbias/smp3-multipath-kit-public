package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type telemetryTestLeg struct {
	closed chan struct{}
	once   sync.Once
}

func newTelemetryTestLeg() *telemetryTestLeg {
	return &telemetryTestLeg{closed: make(chan struct{})}
}

func (l *telemetryTestLeg) Read([]byte) (int, error) {
	<-l.closed
	return 0, io.EOF
}

func (l *telemetryTestLeg) Write(p []byte) (int, error) { return len(p), nil }

func (l *telemetryTestLeg) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func newTelemetryTestStream(t *testing.T, preferred uint8, grace time.Duration) (*smp3core.StreamEngine, net.Conn, [2]*telemetryTestLeg) {
	t.Helper()
	telemetry := &smp3core.StreamTelemetry{}
	engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
		StartupPolicy:       smp3core.StreamStartupPreferred,
		StartupPreferredLeg: smp3core.LegID(preferred),
		StartupGrace:        grace,
		Telemetry:           telemetry,
		QueueFrames:         32,
		MaxInflightFrames:   64,
		RecoveryTimeout:     time.Second,
	})
	legs := [2]*telemetryTestLeg{newTelemetryTestLeg(), newTelemetryTestLeg()}
	t.Cleanup(func() {
		_ = engine.Close()
		_ = app.Close()
		_ = legs[0].Close()
		_ = legs[1].Close()
	})
	return engine, app, legs
}

func waitTelemetrySession(t *testing.T, registry *TelemetryRegistry, id smp3core.SessionID, predicate func(*TelemetrySessionSnapshot) bool) TelemetrySessionSnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := registry.Snapshot()
		for _, row := range snapshot.Sessions {
			if row.DisplaySessionID == registry.displayID(id) && predicate(&row) {
				return row
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("telemetry session predicate timed out")
	return TelemetrySessionSnapshot{}
}

func registerTelemetryStream(t *testing.T, registry *TelemetryRegistry, id smp3core.SessionID, engine *smp3core.StreamEngine) {
	t.Helper()
	if !registry.RegisterSession(id, "stream", time.Now()) {
		t.Fatal("RegisterSession failed")
	}
	registry.BindStream(id, engine)
}

func TestTelemetryRegistryCopiesCoreStartupAndKeepsSnapshotImmutable(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("startup-registry")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x31
	engine, app, legs := newTelemetryTestStream(t, 0, time.Second)
	registerTelemetryStream(t, registry, id, engine)
	if err := engine.AttachLeg(1, legs[1], nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.AttachLeg(0, legs[0], nil); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Write([]byte("startup")); err != nil {
		t.Fatal(err)
	}
	row := waitTelemetrySession(t, registry, id, func(row *TelemetrySessionSnapshot) bool {
		return row.Startup != nil && row.Startup.Applied && row.Startup.FirstDataLeg == 0 && row.Startup.ReleasedAt.IsZero() == false
	})
	first := *row.Startup
	if first.PreferredLeg != 0 || first.NonPreferredAttachedAt.IsZero() || first.PreferredAttachedAt.IsZero() {
		t.Fatalf("startup copy=%+v", first)
	}
	snapshotA := registry.Snapshot()
	var lateID smp3core.SessionID
	lateID[0] = 0x32
	registry.RegisterSession(lateID, "stream", time.Now())
	before := registry.LatestSnapshot()
	if err := engine.AttachLeg(0, newTelemetryTestLeg(), nil); err == nil {
		t.Fatal("duplicate leg unexpectedly attached")
	}
	if snapshotA.Sessions[0].Startup == nil || before.Sessions[0].Startup == nil {
		t.Fatal("startup snapshot missing")
	}
	if snapshotA.Sessions[0].Startup != row.Startup && snapshotA.Sessions[0].Startup.ReleasedAt != first.ReleasedAt {
		t.Fatal("snapshot A changed after later registry activity")
	}
}

func TestTelemetryRESTStartupDetailListHEADAndDatagramNull(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("startup-rest")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x41
	engine, app, legs := newTelemetryTestStream(t, 1, time.Second)
	registerTelemetryStream(t, registry, id, engine)
	if err := engine.AttachLeg(0, legs[0], nil); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Write([]byte("preferred")); err != nil {
		t.Fatal(err)
	}
	if err := engine.AttachLeg(1, legs[1], nil); err != nil {
		t.Fatal(err)
	}
	row := waitTelemetrySession(t, registry, id, func(row *TelemetrySessionSnapshot) bool {
		return row.Startup != nil && row.Startup.FirstDataLeg == 1
	})
	_ = row

	var datagramID smp3core.SessionID
	datagramID[0] = 0x42
	registry.RegisterSession(datagramID, "datagram", time.Now())
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	displayID := registry.displayID(id)

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=10", nil))
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), `"startup"`) {
		t.Fatalf("session list changed: status=%d body=%s", list.Code, list.Body.String())
	}

	detail := httptest.NewRecorder()
	handler.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+displayID, nil))
	if detail.Code != http.StatusOK || strings.Contains(detail.Body.String(), "target") || strings.Contains(detail.Body.String(), "SECRET_SHOULD_NOT_APPEAR") {
		t.Fatalf("detail status/body=%d/%s", detail.Code, detail.Body.String())
	}
	var payload struct {
		Session map[string]any `json:"session"`
	}
	if err := json.Unmarshal(detail.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	startup, ok := payload.Session["startup"].(map[string]any)
	if !ok || startup["applied"] != true || startup["preferred_leg"] != float64(1) || startup["first_data_leg"] != float64(1) {
		t.Fatalf("startup detail=%v", payload.Session["startup"])
	}
	if payload.Session["startup_result"] != startup["release_reason"] || payload.Session["startup_released_at"] == nil {
		t.Fatalf("legacy startup aliases are not Core-backed: session=%v startup=%v", payload.Session, startup)
	}

	head := httptest.NewRecorder()
	handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/api/v1/sessions/"+displayID, nil))
	if head.Code != detail.Code || head.Body.Len() != 0 || head.Header().Get("Content-Length") != detail.Header().Get("Content-Length") {
		t.Fatalf("HEAD parity status/body/length=%d/%d/%s GET=%s", head.Code, head.Body.Len(), head.Header().Get("Content-Length"), detail.Header().Get("Content-Length"))
	}

	datagramDetail := httptest.NewRecorder()
	datagramDisplay := registry.displayID(datagramID)
	if snapshot := registry.Snapshot(); len(snapshot.Sessions) < 2 {
		t.Fatalf("datagram session was not retained: %+v", snapshot.Sessions)
	}
	handler.ServeHTTP(datagramDetail, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+datagramDisplay, nil))
	if datagramDetail.Code != http.StatusOK || !strings.Contains(datagramDetail.Body.String(), `"startup":null`) {
		t.Fatalf("datagram startup=%d %s", datagramDetail.Code, datagramDetail.Body.String())
	}
}

func TestTelemetryRESTFirstReadyStartupIsNullTransitions(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("first-ready-rest")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x51
	telemetry := &smp3core.StreamTelemetry{}
	engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{Telemetry: telemetry})
	defer engine.Close()
	defer app.Close()
	registerTelemetryStream(t, registry, id, engine)
	displayID := registry.displayID(id)
	handler := newTelemetryHTTPServer(registry, time.Now()).Handler()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+displayID, nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"applied":false`) || !strings.Contains(recorder.Body.String(), `"policy":"first-ready"`) {
		t.Fatalf("first-ready detail=%d %s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), `"release_reason":"`) || strings.Contains(recorder.Body.String(), `"first_data_leg":0`) {
		t.Fatalf("first-ready fabricated startup=%s", recorder.Body.String())
	}
}

func TestTelemetryRegistryCloseKeepsStartupSummary(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("close-startup")})
	defer registry.Close()
	var id smp3core.SessionID
	id[0] = 0x61
	engine, app, legs := newTelemetryTestStream(t, 0, time.Second)
	registerTelemetryStream(t, registry, id, engine)
	if err := engine.AttachLeg(1, legs[1], nil); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Write([]byte("close-summary")); err != nil {
		t.Fatal(err)
	}
	row := waitTelemetrySession(t, registry, id, func(row *TelemetrySessionSnapshot) bool {
		return row.Startup != nil && row.Startup.ReleaseReason != smp3core.StartupReleaseNone
	})
	before := *row.Startup
	registry.RemoveSession(id, time.Now())
	closed := registry.Snapshot()
	if len(closed.Sessions) != 1 || closed.Sessions[0].Startup == nil || closed.Sessions[0].Startup.ReleaseReason != before.ReleaseReason {
		t.Fatalf("closed startup summary=%+v", closed.Sessions)
	}
}

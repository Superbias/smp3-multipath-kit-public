package server

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func activateTestSSESubscriber(t *testing.T, registry *TelemetryRegistry, subscriber *telemetrySSESubscriber) {
	t.Helper()
	_, _, stale, closed := registry.activateSSE(subscriber)
	if stale || closed {
		t.Fatalf("subscriber activation stale=%v closed=%v", stale, closed)
	}
}

func drainTestSSENotification(t *testing.T, registry *TelemetryRegistry, subscriber *telemetrySSESubscriber) ([]telemetryLifecycleEvent, bool) {
	t.Helper()
	select {
	case message, ok := <-subscriber.ch:
		if !ok {
			t.Fatal("subscriber notification channel closed")
		}
		if message.Kind != "notify" || message.Lifecycle.ID != 0 {
			t.Fatalf("notification=%+v, want payload-free notify", message)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber notification missing")
	}
	events, snapshot, stale, closed := registry.drainSSE(subscriber)
	if stale || closed {
		t.Fatalf("subscriber drain stale=%v closed=%v", stale, closed)
	}
	return events, snapshot
}

func TestTelemetrySSEInitialSnapshotAndLifecycle(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, SnapshotInterval: time.Second, HMACSecret: []byte("sse-test")})
	defer registry.Close()
	server := httptest.NewServer(newTelemetryHTTPServer(registry, time.Now()).Handler())
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.URL + "/api/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("SSE status/headers=%d/%v", response.StatusCode, response.Header)
	}
	reader := bufio.NewReader(response.Body)
	seenSnapshot := false
	for i := 0; i < 12; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(line) == "event: snapshot" {
			seenSnapshot = true
			break
		}
	}
	if !seenSnapshot {
		t.Fatal("initial snapshot event missing")
	}

	var id smp3core.SessionID
	id[0] = 0x71
	registry.RegisterSession(id, "stream", time.Now())
	seenLifecycle := false
	for i := 0; i < 20; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(line) == "event: lifecycle" {
			seenLifecycle = true
			break
		}
	}
	if !seenLifecycle {
		t.Fatal("session_created lifecycle event missing")
	}
	dataLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(dataLine, "SECRET_SHOULD_NOT_APPEAR") || strings.Contains(dataLine, "71000000000000000000000000000000") {
		t.Fatalf("SSE privacy leak: %q", dataLine)
	}
}

func TestTelemetrySSERejectsInvalidIDAndMutation(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("sse-test")})
	defer registry.Close()
	server := httptest.NewServer(newTelemetryHTTPServer(registry, time.Now()).Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Last-Event-ID", "abc")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "INVALID_LAST_EVENT_ID") {
		t.Fatalf("invalid ID status/body=%d/%q", response.StatusCode, body)
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d", response.StatusCode)
	}
}

func TestTelemetrySSEReplayStaleCursorAndClientBound(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 2, MaxActiveSessions: 64, HMACSecret: []byte("replay-test")})
	defer registry.Close()
	first, _, stale, limited := registry.SubscribeSSE(0, false)
	if stale || limited {
		t.Fatal("initial subscriber rejected")
	}
	activateTestSSESubscriber(t, registry, first)
	var id1 smp3core.SessionID
	id1[0] = 1
	registry.RegisterSession(id1, "stream", time.Now())
	events, _ := drainTestSSENotification(t, registry, first)
	if len(events) != 1 || events[0].ID == 0 {
		t.Fatalf("first lifecycle=%+v", events)
	}
	lastID := events[0].ID
	var id2 smp3core.SessionID
	id2[0] = 2
	registry.RegisterSession(id2, "stream", time.Now())
	replay, replayEvents, stale, limited := registry.SubscribeSSE(lastID, true)
	if stale || limited || len(replayEvents) != 1 || replayEvents[0].ID <= lastID {
		t.Fatalf("recent replay stale=%v limited=%v events=%+v", stale, limited, replayEvents)
	}
	registry.UnsubscribeSSE(replay)
	for i := byte(3); i < 6; i++ {
		var id smp3core.SessionID
		id[0] = i
		registry.RegisterSession(id, "stream", time.Now())
	}
	staleSub, _, stale, limited := registry.SubscribeSSE(lastID, true)
	if !stale || limited {
		t.Fatalf("old cursor stale=%v limited=%v", stale, limited)
	}
	registry.UnsubscribeSSE(staleSub)
	registry.UnsubscribeSSE(first)

	subs := make([]*telemetrySSESubscriber, 0, telemetryMaxSSEClients)
	for i := 0; i < telemetryMaxSSEClients; i++ {
		sub, _, _, limited := registry.SubscribeSSE(0, false)
		if limited {
			t.Fatalf("subscriber %d unexpectedly limited", i)
		}
		subs = append(subs, sub)
	}
	_, _, _, limited = registry.SubscribeSSE(0, false)
	if !limited {
		t.Fatal("SSE client bound did not reject overflow")
	}
	for _, sub := range subs {
		registry.UnsubscribeSSE(sub)
	}
}

func TestTelemetrySSESlowLifecycleDisconnectAndSnapshotCoalesce(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 2, MaxActiveSessions: 64, HMACSecret: []byte("slow-test")})
	defer registry.Close()
	slow, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("slow subscriber rejected")
	}
	activateTestSSESubscriber(t, registry, slow)
	for i := byte(1); i < 6; i++ {
		var id smp3core.SessionID
		id[0] = i
		registry.RegisterSession(id, "stream", time.Now())
	}
	select {
	case _, ok := <-slow.ch:
		if !ok {
			t.Fatal("slow subscriber closed before cursor drain")
		}
	case <-time.After(time.Second):
		t.Fatal("slow subscriber notification missing")
	}
	_, _, stale, closed := registry.drainSSE(slow)
	if !stale || !closed || registry.sseSlowDrops.Load() == 0 {
		t.Fatalf("slow lifecycle client was not disconnected: drops=%d", registry.sseSlowDrops.Load())
	}

	fast, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("fast subscriber rejected")
	}
	activateTestSSESubscriber(t, registry, fast)
	for i := 0; i < 20; i++ {
		registry.publishSnapshot(TelemetrySnapshot{Timestamp: time.Now()})
	}
	select {
	case message := <-fast.ch:
		if message.Kind != "notify" {
			t.Fatalf("coalesced message=%+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced snapshot was not delivered")
	}
	_, snapshot, stale, closed := registry.drainSSE(fast)
	if !snapshot || stale || closed {
		t.Fatalf("coalesced snapshot drain snapshot=%v stale=%v closed=%v", snapshot, stale, closed)
	}
	registry.UnsubscribeSSE(fast)
}

func TestTelemetrySSENotificationBurstDoesNotFalseDisconnect(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 64, MaxActiveSessions: 64, HMACSecret: []byte("notification-burst")})
	defer registry.Close()
	subscriber, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("subscriber rejected")
	}
	if cap(subscriber.ch) != 1 {
		t.Fatalf("notification capacity=%d, want 1", cap(subscriber.ch))
	}
	for i := byte(1); i < 32; i++ {
		var id smp3core.SessionID
		id[0] = i
		registry.RegisterSession(id, "stream", time.Now())
	}
	select {
	case message, ok := <-subscriber.ch:
		if !ok {
			t.Fatal("subscriber was falsely disconnected")
		}
		if message.Kind != "notify" || message.Lifecycle.ID != 0 {
			t.Fatalf("notification=%+v, want payload-free notify", message)
		}
	default:
		t.Fatal("notification was not delivered")
	}
	registry.UnsubscribeSSE(subscriber)
}

func TestTelemetrySSETwoClientsDisconnectAndShutdown(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 64, MaxActiveSessions: 128, HMACSecret: []byte("two-client")})
	first, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("first client rejected")
	}
	activateTestSSESubscriber(t, registry, first)
	second, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("second client rejected")
	}
	activateTestSSESubscriber(t, registry, second)
	var id smp3core.SessionID
	id[0] = 0x44
	registry.RegisterSession(id, "stream", time.Now())
	for index, subscriber := range []*telemetrySSESubscriber{first, second} {
		events, _ := drainTestSSENotification(t, registry, subscriber)
		if len(events) != 1 || events[0].ID == 0 {
			t.Fatalf("client %d events=%+v", index, events)
		}
	}
	registry.UnsubscribeSSE(first)
	for i := 0; i < 100; i++ {
		sub, _, _, limited := registry.SubscribeSSE(0, false)
		if limited {
			t.Fatal("temporary client rejected")
		}
		registry.UnsubscribeSSE(sub)
	}
	registry.Close()
	select {
	case _, ok := <-second.ch:
		if ok {
			for range second.ch {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close SSE subscriber")
	}
}

func TestTelemetrySSESlowClientDoesNotAffectFastClient(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 64, MaxActiveSessions: 64, HMACSecret: []byte("fast-slow")})
	defer registry.Close()
	slow, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("slow client rejected")
	}
	fast, _, _, limited := registry.SubscribeSSE(0, false)
	if limited {
		t.Fatal("fast client rejected")
	}
	activateTestSSESubscriber(t, registry, fast)
	for i := byte(1); i < 32; i++ {
		var id smp3core.SessionID
		id[0] = i
		registry.RegisterSession(id, "stream", time.Now())
	}
	events, _ := drainTestSSENotification(t, registry, fast)
	if len(events) != 31 {
		t.Fatalf("fast client received %d lifecycle events", len(events))
	}
	if registry.sseSlowDrops.Load() != 0 {
		t.Fatalf("fast/slow pair produced slow disconnects=%d", registry.sseSlowDrops.Load())
	}
	registry.UnsubscribeSSE(fast)
	registry.UnsubscribeSSE(slow)
}

func readSSEFrame(reader *bufio.Reader) (string, uint64, bool, error) {
	event := ""
	var id uint64
	hasID := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return event, id, hasID, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event != "" {
				return event, id, hasID, nil
			}
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			event = strings.TrimPrefix(line, "event: ")
		}
		if strings.HasPrefix(line, "id: ") {
			value, err := strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if err != nil {
				return "", 0, false, err
			}
			id, hasID = value, true
		}
	}
}

func TestTelemetrySSEHTTPRecentReplayAndStaleReset(t *testing.T) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, MaxEvents: 2, MaxActiveSessions: 64, SnapshotInterval: time.Hour, HMACSecret: []byte("http-replay")})
	defer registry.Close()
	server := httptest.NewServer(newTelemetryHTTPServer(registry, time.Now()).Handler())
	defer server.Close()
	client := &http.Client{Timeout: 2 * time.Second}

	firstResponse, err := client.Get(server.URL + "/api/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	firstReader := bufio.NewReader(firstResponse.Body)
	event, _, _, err := readSSEFrame(firstReader)
	if err != nil || event != "snapshot" {
		t.Fatalf("initial SSE event=%q err=%v", event, err)
	}
	var firstID smp3core.SessionID
	firstID[0] = 1
	registry.RegisterSession(firstID, "stream", time.Now())
	event, lastID, hasID, err := readSSEFrame(firstReader)
	if err != nil || event != "lifecycle" || !hasID {
		t.Fatalf("first lifecycle event=%q id=%d has=%v err=%v", event, lastID, hasID, err)
	}
	_ = firstResponse.Body.Close()

	var secondID smp3core.SessionID
	secondID[0] = 2
	registry.RegisterSession(secondID, "stream", time.Now())
	reconnect, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	reconnect.Header.Set("Last-Event-ID", strconv.FormatUint(lastID, 10))
	secondResponse, err := client.Do(reconnect)
	if err != nil {
		t.Fatal(err)
	}
	secondReader := bufio.NewReader(secondResponse.Body)
	event, replayID, hasID, err := readSSEFrame(secondReader)
	if err != nil || event != "lifecycle" || !hasID || replayID <= lastID {
		t.Fatalf("recent replay event=%q id=%d has=%v err=%v", event, replayID, hasID, err)
	}
	event, _, _, err = readSSEFrame(secondReader)
	if err != nil || event != "snapshot" {
		t.Fatalf("fresh snapshot after replay event=%q err=%v", event, err)
	}
	_ = secondResponse.Body.Close()

	for i := byte(3); i < 7; i++ {
		var id smp3core.SessionID
		id[0] = i
		registry.RegisterSession(id, "stream", time.Now())
	}
	staleRequest, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	staleRequest.Header.Set("Last-Event-ID", strconv.FormatUint(lastID, 10))
	staleResponse, err := client.Do(staleRequest)
	if err != nil {
		t.Fatal(err)
	}
	staleReader := bufio.NewReader(staleResponse.Body)
	event, _, _, err = readSSEFrame(staleReader)
	if err != nil || event != "reset" {
		t.Fatalf("stale reset event=%q err=%v", event, err)
	}
	event, _, _, err = readSSEFrame(staleReader)
	if err != nil || event != "snapshot" {
		t.Fatalf("stale fresh snapshot event=%q err=%v", event, err)
	}
	_ = staleResponse.Body.Close()
}

func BenchmarkTelemetrySSESnapshotSerialization(b *testing.B) {
	registry := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("sse-bench")})
	defer registry.Close()
	snapshot := registry.Snapshot()
	server := newTelemetryHTTPServer(registry, time.Now())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		if err := writeSSESnapshot(recorder, snapshot); err != nil {
			b.Fatal(err)
		}
	}
	_ = server
}

func BenchmarkTelemetrySSELifecycleSerialization(b *testing.B) {
	event := telemetryLifecycleEvent{ID: 1, Kind: "leg_attached", At: time.Now(), DisplaySessionID: "abcd1234", Mode: "stream"}
	server := NewTelemetryRegistry(TelemetryConfig{Enabled: true, HMACSecret: []byte("sse-bench")})
	defer server.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		if err := writeSSELifecycle(recorder, event); err != nil {
			b.Fatal(err)
		}
	}
}

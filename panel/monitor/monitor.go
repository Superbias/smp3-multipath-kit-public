package monitor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultEventDedupWindow = 30 * time.Second
	defaultHistoryLimit     = 3600
	maxHistoryLimit         = 3600
	maxEventLimit           = 256
	maxTelemetryBody        = 2 << 20
)

type rawStatus struct {
	SnapshotAt       string `json:"snapshot_at"`
	ActiveSessions   uint64 `json:"active_sessions"`
	TotalSessions    uint64 `json:"total_sessions"`
	ActiveLegs       uint64 `json:"active_legs"`
	WireTxBytes      uint64 `json:"wire_tx_bytes"`
	WireRxBytes      uint64 `json:"wire_rx_bytes"`
	TelemetryDropped uint64 `json:"telemetry_dropped_events"`
}

type rawLegs struct {
	SnapshotAt string   `json:"snapshot_at"`
	Items      []rawLeg `json:"items"`
}

type rawLeg struct {
	LegID             int    `json:"leg_id"`
	ActiveSessions    uint64 `json:"active_sessions"`
	ActiveConnections uint64 `json:"active_connections"`
	WireTxBytes       uint64 `json:"wire_tx_bytes"`
	WireRxBytes       uint64 `json:"wire_rx_bytes"`
	UsefulACKBytes    uint64 `json:"logical_tx_acked_bytes"`
	RXUniqueBytes     uint64 `json:"logical_rx_unique_bytes"`
}

type rawSample struct {
	At               time.Time
	ActiveSessions   uint64
	TotalSessions    uint64
	ActiveLegs       uint64
	WireTxBytes      uint64
	WireRxBytes      uint64
	TelemetryDropped uint64
	Legs             [2]rawLeg
}

type previousSample struct {
	raw rawSample
	at  time.Time
}

type subscriber struct {
	id uint64
	ch chan Message
}

// Message is one read-only update delivered to a Panel SSE subscriber.
type Message struct {
	Snapshot *Snapshot
	Event    *Event
}

// Monitor owns only Panel-side state. It never exposes or mutates the SMP3
// server's live registry; telemetry is fetched through its read-only HTTP API.
type Monitor struct {
	cfg       Config
	client    *http.Client
	sseClient *http.Client
	now       func() time.Time
	probe     ListenerProbe
	targets   []ListenerTarget

	mu          sync.RWMutex
	latestValue Snapshot
	haveLatest  bool
	telemetryUp bool
	memory      []Snapshot
	persistent  []Snapshot

	rateMu    sync.Mutex
	previous  *previousSample
	collectMu sync.Mutex

	eventMu       sync.Mutex
	events        []Event
	nextEventID   uint64
	lastEventAt   map[string]time.Time
	lastHealth    string
	lastListeners map[string]bool

	subMu       sync.Mutex
	subs        map[uint64]*subscriber
	nextSubID   uint64
	streamDrops atomic.Uint64

	startOnce sync.Once
	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
	eventDone chan struct{}
}

func DefaultConfig() Config { return defaultConfig() }

func (m *Monitor) Config() Config { return m.cfg }

func New(cfg Config) (*Monitor, error) {
	if err := normalizeConfig(&cfg); err != nil {
		return nil, err
	}
	m := &Monitor{
		cfg:           cfg,
		client:        cfg.HTTPClient,
		now:           cfg.Now,
		probe:         cfg.Probe,
		targets:       defaultTargets(),
		lastEventAt:   make(map[string]time.Time),
		lastListeners: make(map[string]bool),
		subs:          make(map[uint64]*subscriber),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		eventDone:     make(chan struct{}),
	}
	if m.client == nil {
		m.client = &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	} else {
		client := *m.client
		if client.Timeout <= 0 {
			client.Timeout = 5 * time.Second
		}
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
		m.client = &client
	}
	m.sseClient = &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	if err := m.loadPersistent(); err != nil {
		return nil, err
	}
	return m, nil
}

func normalizeConfig(cfg *Config) error {
	defaults := defaultConfig()
	if cfg.Listen == "" {
		cfg.Listen = defaults.Listen
	}
	if err := validateLoopbackListen(cfg.Listen); err != nil {
		return err
	}
	if cfg.TelemetryURL == "" {
		cfg.TelemetryURL = defaults.TelemetryURL
	}
	parsed, err := url.Parse(cfg.TelemetryURL)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("invalid telemetry URL")
	}
	host, _, err := net.SplitHostPort(parsed.Host)
	if err != nil || host != "127.0.0.1" && host != "::1" {
		return fmt.Errorf("telemetry URL must use literal loopback")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	cfg.TelemetryURL = strings.TrimRight(parsed.String(), "/")
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	if cfg.MemoryRetention <= 0 {
		cfg.MemoryRetention = defaults.MemoryRetention
	}
	if cfg.PersistentInterval <= 0 {
		cfg.PersistentInterval = defaults.PersistentInterval
	}
	if cfg.PersistentRetention <= 0 {
		cfg.PersistentRetention = defaults.PersistentRetention
	}
	if cfg.EventRetention <= 0 || cfg.EventRetention > maxEventLimit {
		cfg.EventRetention = defaults.EventRetention
	}
	if cfg.MaxSSEClients <= 0 || cfg.MaxSSEClients > 64 {
		cfg.MaxSSEClients = defaults.MaxSSEClients
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Probe == nil {
		cfg.Probe = tcpProbe
	}
	return nil
}

func validateLoopbackListen(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" && host != "::1" {
		return fmt.Errorf("panel listen must use literal loopback")
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 0 || value > 65535 {
		return fmt.Errorf("invalid panel listen port")
	}
	return nil
}

func defaultTargets() []ListenerTarget {
	return []ListenerTarget{
		{Name: "smp3-client", Address: "127.0.0.1:18080", Port: 18080},
		{Name: "carrier-a", Address: "127.0.0.1:17898", Port: 17898},
		{Name: "carrier-b", Address: "127.0.0.1:17899", Port: 17899},
	}
}

func tcpProbe(target ListenerTarget) ListenerHealth {
	conn, err := net.DialTimeout("tcp", target.Address, 350*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return ListenerHealth{ListenerTarget: target, ListenerUp: true, ProcessUp: true, Probe: "tcp"}
	}
	return ListenerHealth{ListenerTarget: target, Probe: "tcp"}
}

func (m *Monitor) Start() {
	m.startOnce.Do(func() {
		go m.run()
		go m.runTelemetryEvents()
	})
}

func (m *Monitor) run() {
	defer close(m.done)
	_ = m.CollectOnce(context.Background())
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			m.closeSubscribers()
			return
		case <-ticker.C:
			_ = m.CollectOnce(context.Background())
		}
	}
}

func (m *Monitor) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
		m.startOnce.Do(func() { close(m.done); close(m.eventDone) })
		<-m.done
		<-m.eventDone
		m.closeSubscribers()
	})
}

// CollectOnce performs one bounded, read-only telemetry poll. It is exported
// for deterministic integration tests and does not start a background loop.
func (m *Monitor) CollectOnce(ctx context.Context) error {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	status, err := m.fetchStatus(ctx)
	if err != nil {
		m.telemetryFailure()
		return err
	}
	legs, err := m.fetchLegs(ctx)
	if err != nil {
		m.telemetryFailure()
		return err
	}
	at := parseTelemetryTime(status.SnapshotAt)
	if at.IsZero() {
		at = parseTelemetryTime(legs.SnapshotAt)
	}
	if at.IsZero() {
		at = m.now().UTC()
	}
	raw := rawSample{
		At:               at,
		ActiveSessions:   status.ActiveSessions,
		TotalSessions:    status.TotalSessions,
		ActiveLegs:       status.ActiveLegs,
		WireTxBytes:      status.WireTxBytes,
		WireRxBytes:      status.WireRxBytes,
		TelemetryDropped: status.TelemetryDropped,
	}
	for _, leg := range legs.Items {
		if leg.LegID >= 0 && leg.LegID < len(raw.Legs) {
			raw.Legs[leg.LegID] = leg
		}
	}
	snapshot, resets := m.buildSnapshot(raw)
	snapshot.Health = m.health(snapshot.ActiveSessions, snapshot.ActiveLegs)
	m.telemetryRecovery()
	m.recordSnapshot(snapshot)
	if resets > 0 {
		m.addEvent(at, "counter_reset", "warning", "telemetry", "counter reset detected")
	}
	if err := m.persistSnapshot(snapshot); err != nil {
		m.addEvent(at, "history_persist_error", "warning", "history", "persistent history unavailable")
	}
	m.broadcastSnapshot(snapshot)
	return nil
}

func (m *Monitor) fetchStatus(ctx context.Context) (rawStatus, error) {
	var result rawStatus
	if err := m.getJSON(ctx, "/api/v1/status", &result); err != nil {
		return result, err
	}
	return result, nil
}

func (m *Monitor) fetchLegs(ctx context.Context) (rawLegs, error) {
	var result rawLegs
	if err := m.getJSON(ctx, "/api/v1/legs", &result); err != nil {
		return result, err
	}
	return result, nil
}

func (m *Monitor) getJSON(ctx context.Context, path string, destination any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.TelemetryURL+path, nil)
	if err != nil {
		return errors.New("telemetry request unavailable")
	}
	request.Header.Set("Accept", "application/json")
	response, err := m.client.Do(request)
	if err != nil {
		return errors.New("telemetry request unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("telemetry request unavailable")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxTelemetryBody))
	if err := decoder.Decode(destination); err != nil {
		return errors.New("telemetry response unavailable")
	}
	return nil
}

func parseTelemetryTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func (m *Monitor) buildSnapshot(raw rawSample) (Snapshot, int) {
	snapshot := Snapshot{
		At:               raw.At,
		ActiveSessions:   raw.ActiveSessions,
		TotalSessions:    raw.TotalSessions,
		ActiveLegs:       raw.ActiveLegs,
		TotalWireTxBytes: raw.WireTxBytes,
		TotalWireRxBytes: raw.WireRxBytes,
		TelemetryDropped: raw.TelemetryDropped,
	}
	m.rateMu.Lock()
	previous := m.previous
	m.previous = &previousSample{raw: raw, at: raw.At}
	m.rateMu.Unlock()
	if previous == nil {
		for leg := range snapshot.Legs {
			snapshot.Legs[leg] = makeLegSnapshot(raw.Legs[leg], nil, false)
		}
		return snapshot, 0
	}
	seconds := raw.At.Sub(previous.at).Seconds()
	if seconds <= 0 {
		seconds = 0
	}
	resetCount := 0
	if value, valid, reset := deltaRate(raw.WireTxBytes, previous.raw.WireTxBytes, seconds); valid {
		snapshot.TotalWireTxRateBPS = &value
	} else if reset {
		resetCount++
	}
	if value, valid, reset := deltaRate(raw.WireRxBytes, previous.raw.WireRxBytes, seconds); valid {
		snapshot.TotalWireRxRateBPS = &value
	} else if reset {
		resetCount++
	}
	for leg := range snapshot.Legs {
		current := raw.Legs[leg]
		prior := previous.raw.Legs[leg]
		legReset := false
		value, valid, reset := deltaRate(current.WireTxBytes, prior.WireTxBytes, seconds)
		if reset {
			resetCount++
			legReset = true
		}
		wireTxRate := optionalRate(value, valid)
		value, valid, reset = deltaRate(current.WireRxBytes, prior.WireRxBytes, seconds)
		if reset {
			resetCount++
			legReset = true
		}
		wireRxRate := optionalRate(value, valid)
		value, valid, reset = deltaRate(current.UsefulACKBytes, prior.UsefulACKBytes, seconds)
		if reset {
			resetCount++
			legReset = true
		}
		ackRate := optionalRate(value, valid)
		value, valid, reset = deltaRate(current.RXUniqueBytes, prior.RXUniqueBytes, seconds)
		if reset {
			resetCount++
			legReset = true
		}
		rxRate := optionalRate(value, valid)
		snapshot.Legs[leg] = makeLegSnapshot(current, &legRates{wireTxRate, wireRxRate, ackRate, rxRate}, legReset)
	}
	snapshot.Traffic = trafficShares(raw, previous.raw)
	return snapshot, resetCount
}

type legRates struct {
	wireTx, wireRx, ack, rx *float64
}

func makeLegSnapshot(raw rawLeg, rates *legRates, reset bool) LegSnapshot {
	result := LegSnapshot{
		LegID:             raw.LegID,
		State:             "down",
		ActiveSessions:    raw.ActiveSessions,
		ActiveConnections: raw.ActiveConnections,
		WireTxBytes:       raw.WireTxBytes,
		WireRxBytes:       raw.WireRxBytes,
		UsefulACKBytes:    raw.UsefulACKBytes,
		RXUniqueBytes:     raw.RXUniqueBytes,
		CounterReset:      reset,
	}
	if raw.ActiveConnections > 0 {
		result.State = "up"
	}
	if rates != nil {
		result.WireTxRateBPS = rates.wireTx
		result.WireRxRateBPS = rates.wireRx
		result.UsefulACKRateBPS = rates.ack
		result.RXUniqueRateBPS = rates.rx
	}
	return result
}

func optionalRate(value float64, valid bool) *float64 {
	if !valid {
		return nil
	}
	return &value
}

func deltaRate(current, previous uint64, seconds float64) (float64, bool, bool) {
	if seconds <= 0 {
		return 0, false, false
	}
	if current < previous {
		return 0, false, true
	}
	return float64(current-previous) * 8 / seconds, true, false
}

func trafficShares(current, previous rawSample) Traffic {
	return Traffic{
		WireTx:    shareDelta(current.Legs[0].WireTxBytes, previous.Legs[0].WireTxBytes, current.Legs[1].WireTxBytes, previous.Legs[1].WireTxBytes),
		WireRx:    shareDelta(current.Legs[0].WireRxBytes, previous.Legs[0].WireRxBytes, current.Legs[1].WireRxBytes, previous.Legs[1].WireRxBytes),
		UsefulACK: shareDelta(current.Legs[0].UsefulACKBytes, previous.Legs[0].UsefulACKBytes, current.Legs[1].UsefulACKBytes, previous.Legs[1].UsefulACKBytes),
		RXUnique:  shareDelta(current.Legs[0].RXUniqueBytes, previous.Legs[0].RXUniqueBytes, current.Legs[1].RXUniqueBytes, previous.Legs[1].RXUniqueBytes),
	}
}

func shareDelta(current0, previous0, current1, previous1 uint64) TrafficShare {
	if current0 < previous0 || current1 < previous1 {
		return TrafficShare{}
	}
	return makeTrafficShare(current0-previous0, current1-previous1)
}

func makeTrafficShare(left, right uint64) TrafficShare {
	total := left + right
	if total == 0 {
		return TrafficShare{}
	}
	a := float64(left) / float64(total)
	b := float64(right) / float64(total)
	return TrafficShare{Leg0: &a, Leg1: &b}
}

func (m *Monitor) health(activeSessions, activeLegs uint64) Health {
	listeners := make([]ListenerHealth, len(m.targets))
	up := 0
	for index, target := range m.targets {
		listeners[index] = m.probe(target)
		if listeners[index].ListenerUp && listeners[index].ProcessUp {
			up++
		}
	}
	overall := "HEALTHY"
	if up == 0 {
		overall = "DOWN"
	} else if up != len(listeners) || activeSessions > 0 && activeLegs < 2 {
		overall = "DEGRADED"
	} else if activeSessions == 0 {
		overall = "IDLE"
	}
	result := Health{Overall: overall, Telemetry: "UP", Listeners: listeners}
	m.recordHealthEvents(result)
	return result
}

func (m *Monitor) recordHealthEvents(health Health) {
	now := m.now().UTC()
	if m.lastHealth != "" && m.lastHealth != health.Overall {
		m.addEvent(now, "health_changed", severityForHealth(health.Overall), "monitor", "overall health changed to "+health.Overall)
	}
	m.lastHealth = health.Overall
	for _, listener := range health.Listeners {
		key := listener.Name
		current := listener.ListenerUp && listener.ProcessUp
		if previous, exists := m.lastListeners[key]; exists && previous != current {
			state := "down"
			severity := "warning"
			if current {
				state, severity = "up", "info"
			}
			m.addEvent(now, "listener_"+state, severity, key, key+" listener is "+state)
		}
		m.lastListeners[key] = current
	}
}

func severityForHealth(value string) string {
	if value == "HEALTHY" || value == "IDLE" {
		return "info"
	}
	return "warning"
}

func (m *Monitor) recordSnapshot(snapshot Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestValue = cloneSnapshot(snapshot)
	m.haveLatest = true
	cutoff := snapshot.At.Add(-m.cfg.MemoryRetention)
	m.memory = append(m.memory, cloneSnapshot(snapshot))
	first := 0
	for first < len(m.memory) && m.memory[first].At.Before(cutoff) {
		first++
	}
	if first > 0 {
		m.memory = append([]Snapshot(nil), m.memory[first:]...)
	}
}

func (m *Monitor) telemetryFailure() {
	m.mu.Lock()
	wasUp := m.telemetryUp
	m.telemetryUp = false
	var stale Snapshot
	haveStale := false
	if m.haveLatest {
		m.latestValue.Health = Health{Overall: "UNKNOWN", Telemetry: "DOWN"}
		stale = cloneSnapshot(m.latestValue)
		haveStale = true
	}
	m.mu.Unlock()
	if wasUp {
		m.addEvent(m.now().UTC(), "telemetry_down", "warning", "telemetry", "telemetry endpoint unavailable")
		m.addEvent(m.now().UTC(), "health_changed", "warning", "monitor", "overall health changed to UNKNOWN")
		if haveStale {
			m.broadcastSnapshot(stale)
		}
	}
}

func (m *Monitor) telemetryRecovery() {
	m.mu.Lock()
	wasDown := m.haveLatest && !m.telemetryUp
	m.telemetryUp = true
	m.mu.Unlock()
	if wasDown {
		m.addEvent(m.now().UTC(), "telemetry_up", "info", "telemetry", "telemetry endpoint recovered")
	}
}

func (m *Monitor) Latest() (Snapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.haveLatest {
		return Snapshot{}, false
	}
	return cloneSnapshot(m.latestValue), true
}

func (m *Monitor) History(scope string, limit int) HistoryResponse {
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}
	if scope != "memory" && scope != "persistent" && scope != "all" {
		scope = "memory"
	}
	m.mu.RLock()
	var items []Snapshot
	switch scope {
	case "persistent":
		items = append(items, m.persistent...)
	case "all":
		items = append(items, m.persistent...)
		items = append(items, m.memory...)
	default:
		items = append(items, m.memory...)
	}
	m.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].At.Before(items[j].At) })
	if scope == "all" {
		dedup := make([]Snapshot, 0, len(items))
		var last time.Time
		for _, item := range items {
			if !last.IsZero() && item.At.Equal(last) {
				continue
			}
			dedup = append(dedup, item)
			last = item.At
		}
		items = dedup
	}
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return HistoryResponse{Scope: scope, Items: items}
}

func (m *Monitor) Events(limit int) EventsResponse {
	if limit <= 0 {
		limit = maxEventLimit
	}
	if limit > maxEventLimit {
		limit = maxEventLimit
	}
	m.eventMu.Lock()
	items := append([]Event(nil), m.events...)
	m.eventMu.Unlock()
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return EventsResponse{Items: items}
}

func (m *Monitor) addEvent(at time.Time, typ, severity, source, message string) {
	key := typ + "|" + severity + "|" + source + "|" + message
	m.eventMu.Lock()
	if previous, exists := m.lastEventAt[key]; exists && at.Sub(previous) < defaultEventDedupWindow {
		m.eventMu.Unlock()
		return
	}
	m.nextEventID++
	event := Event{ID: m.nextEventID, At: at.UTC(), Type: typ, Severity: severity, Source: source, Message: message}
	m.lastEventAt[key] = at
	m.events = append(m.events, event)
	if len(m.events) > m.cfg.EventRetention {
		m.events = append([]Event(nil), m.events[len(m.events)-m.cfg.EventRetention:]...)
	}
	if len(m.lastEventAt) > m.cfg.EventRetention*4 {
		var oldestKey string
		var oldest time.Time
		for candidate, candidateAt := range m.lastEventAt {
			if oldestKey == "" || candidateAt.Before(oldest) {
				oldestKey, oldest = candidate, candidateAt
			}
		}
		if oldestKey != "" {
			delete(m.lastEventAt, oldestKey)
		}
	}
	m.eventMu.Unlock()
	m.broadcastEvent(event)
}

// RecordLifecycle records only the safe operational shape of a telemetry
// lifecycle notification; it intentionally discards session aliases and data.
func (m *Monitor) RecordLifecycle(kind, mode string, legID *int) {
	message := "lifecycle event"
	if mode != "" {
		message = safeToken(mode) + " lifecycle event"
	}
	if legID != nil && (*legID == 0 || *legID == 1) {
		message += " on leg " + strconv.Itoa(*legID)
	}
	m.addEvent(m.now().UTC(), "lifecycle_"+safeToken(kind), "info", "telemetry", message)
}

func safeToken(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			builder.WriteRune(r)
			if builder.Len() == 32 {
				break
			}
		}
	}
	if builder.Len() == 0 {
		return "update"
	}
	return builder.String()
}

func (m *Monitor) Subscribe() (<-chan Message, func(), bool) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	if len(m.subs) >= m.cfg.MaxSSEClients {
		return nil, func() {}, false
	}
	m.nextSubID++
	sub := &subscriber{id: m.nextSubID, ch: make(chan Message, 8)}
	m.subs[sub.id] = sub
	return sub.ch, func() { m.unsubscribe(sub.id) }, true
}

func (m *Monitor) unsubscribe(id uint64) {
	m.subMu.Lock()
	if sub := m.subs[id]; sub != nil {
		delete(m.subs, id)
		close(sub.ch)
	}
	m.subMu.Unlock()
}

func (m *Monitor) closeSubscribers() {
	m.subMu.Lock()
	for id, sub := range m.subs {
		delete(m.subs, id)
		close(sub.ch)
	}
	m.subMu.Unlock()
}

func (m *Monitor) broadcastSnapshot(snapshot Snapshot) {
	m.broadcast(Message{Snapshot: ptrSnapshot(snapshot)})
}

func (m *Monitor) broadcastEvent(event Event) {
	m.broadcast(Message{Event: ptrEvent(event)})
}

func (m *Monitor) broadcast(message Message) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	for _, sub := range m.subs {
		select {
		case sub.ch <- message:
		default:
			m.streamDrops.Add(1)
		}
	}
}

func (m *Monitor) StreamDrops() uint64 { return m.streamDrops.Load() }

func (m *Monitor) loadPersistent() error {
	if m.cfg.PersistentPath == "" {
		return nil
	}
	file, err := os.Open(m.cfg.PersistentPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return nil
	}
	cutoff := m.now().Add(-m.cfg.PersistentRetention)
	needsCompact := false
	scanner := bufio.NewScanner(io.LimitReader(file, 64<<20))
	scanner.Buffer(make([]byte, 4096), maxTelemetryBody)
	for scanner.Scan() {
		var sample Snapshot
		if json.Unmarshal(scanner.Bytes(), &sample) != nil || sample.At.IsZero() || sample.At.Before(cutoff) {
			needsCompact = true
			continue
		}
		m.persistent = append(m.persistent, sample)
	}
	if len(m.persistent) > maxHistoryLimit*17 {
		m.persistent = m.persistent[len(m.persistent)-maxHistoryLimit*17:]
		needsCompact = true
	}
	_ = file.Close()
	if needsCompact {
		_ = m.compactPersistent(m.persistent)
	}
	return nil
}

func (m *Monitor) persistSnapshot(snapshot Snapshot) error {
	if m.cfg.PersistentPath == "" {
		return nil
	}
	m.mu.Lock()
	last := time.Time{}
	if len(m.persistent) > 0 {
		last = m.persistent[len(m.persistent)-1].At
	}
	if !last.IsZero() && snapshot.At.Sub(last) < m.cfg.PersistentInterval {
		m.mu.Unlock()
		return nil
	}
	m.persistent = append(m.persistent, cloneSnapshot(snapshot))
	cutoff := snapshot.At.Add(-m.cfg.PersistentRetention)
	first := 0
	for first < len(m.persistent) && m.persistent[first].At.Before(cutoff) {
		first++
	}
	if first > 0 {
		m.persistent = append([]Snapshot(nil), m.persistent[first:]...)
	}
	items := append([]Snapshot(nil), m.persistent...)
	m.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(m.cfg.PersistentPath), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(m.cfg.PersistentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Compact whenever retention removed records; this keeps the physical file
	// bounded without rewriting it for every one-second sample.
	if first > 0 {
		return m.compactPersistent(items)
	}
	return nil
}

func (m *Monitor) compactPersistent(items []Snapshot) error {
	temporary := m.cfg.PersistentPath + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			_ = file.Close()
			_ = os.Remove(temporary)
			return err
		}
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, m.cfg.PersistentPath); err != nil {
		// Windows does not replace an existing file with Rename. The fallback is
		// still local to the monitor history file and never touches production
		// configuration or runtime state.
		if removeErr := os.Remove(m.cfg.PersistentPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			_ = os.Remove(temporary)
			return err
		}
		if retryErr := os.Rename(temporary, m.cfg.PersistentPath); retryErr != nil {
			_ = os.Remove(temporary)
			return retryErr
		}
	}
	return nil
}

func cloneSnapshot(source Snapshot) Snapshot {
	result := source
	result.Legs = [2]LegSnapshot{cloneLeg(source.Legs[0]), cloneLeg(source.Legs[1])}
	result.Traffic = Traffic{WireTx: cloneShare(source.Traffic.WireTx), WireRx: cloneShare(source.Traffic.WireRx), UsefulACK: cloneShare(source.Traffic.UsefulACK), RXUnique: cloneShare(source.Traffic.RXUnique)}
	result.Health.Listeners = append([]ListenerHealth(nil), source.Health.Listeners...)
	return result
}

func cloneLeg(source LegSnapshot) LegSnapshot {
	result := source
	result.WireTxRateBPS = cloneFloat(source.WireTxRateBPS)
	result.WireRxRateBPS = cloneFloat(source.WireRxRateBPS)
	result.UsefulACKRateBPS = cloneFloat(source.UsefulACKRateBPS)
	result.RXUniqueRateBPS = cloneFloat(source.RXUniqueRateBPS)
	result.WireTxShare = cloneFloat(source.WireTxShare)
	result.WireRxShare = cloneFloat(source.WireRxShare)
	result.UsefulACKShare = cloneFloat(source.UsefulACKShare)
	result.RXUniqueShare = cloneFloat(source.RXUniqueShare)
	return result
}

func cloneShare(source TrafficShare) TrafficShare {
	return TrafficShare{Leg0: cloneFloat(source.Leg0), Leg1: cloneFloat(source.Leg1)}
}
func cloneFloat(source *float64) *float64 {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}
func ptrSnapshot(source Snapshot) *Snapshot { copy := cloneSnapshot(source); return &copy }
func ptrEvent(source Event) *Event          { copy := source; return &copy }

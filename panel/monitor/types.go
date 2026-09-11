package monitor

import (
	"net/http"
	"time"
)

// Config controls the Panel-side read-only monitor. The default telemetry URL
// is literal loopback so the browser never needs access to the telemetry port.
type Config struct {
	Listen              string
	TelemetryURL        string
	PollInterval        time.Duration
	MemoryRetention     time.Duration
	PersistentPath      string
	PersistentInterval  time.Duration
	PersistentRetention time.Duration
	EventRetention      int
	MaxSSEClients       int
	HTTPClient          *http.Client
	Probe               ListenerProbe
	Now                 func() time.Time
}

// ListenerTarget identifies one local process/listener health probe.
type ListenerTarget struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type ListenerHealth struct {
	ListenerTarget
	ListenerUp bool   `json:"listener_up"`
	ProcessUp  bool   `json:"process_up"`
	Probe      string `json:"probe"`
}

type Health struct {
	Overall   string           `json:"overall"`
	Telemetry string           `json:"telemetry"`
	Listeners []ListenerHealth `json:"listeners"`
}

type LegSnapshot struct {
	LegID             int      `json:"leg_id"`
	State             string   `json:"state"`
	ActiveSessions    uint64   `json:"active_sessions"`
	ActiveConnections uint64   `json:"active_connections"`
	WireTxBytes       uint64   `json:"wire_tx_bytes"`
	WireRxBytes       uint64   `json:"wire_rx_bytes"`
	WireTxRateBPS     *float64 `json:"wire_tx_rate_bps"`
	WireRxRateBPS     *float64 `json:"wire_rx_rate_bps"`
	UsefulACKBytes    uint64   `json:"useful_ack_bytes"`
	UsefulACKRateBPS  *float64 `json:"useful_ack_rate_bps"`
	RXUniqueBytes     uint64   `json:"rx_unique_bytes"`
	RXUniqueRateBPS   *float64 `json:"rx_unique_rate_bps"`
	WireTxShare       *float64 `json:"wire_tx_share"`
	WireRxShare       *float64 `json:"wire_rx_share"`
	UsefulACKShare    *float64 `json:"useful_ack_share"`
	RXUniqueShare     *float64 `json:"rx_unique_share"`
	CounterReset      bool     `json:"counter_reset"`
}

type TrafficShare struct {
	Leg0 *float64 `json:"leg0"`
	Leg1 *float64 `json:"leg1"`
}

type Traffic struct {
	WireTx    TrafficShare `json:"wire_tx"`
	WireRx    TrafficShare `json:"wire_rx"`
	UsefulACK TrafficShare `json:"useful_ack"`
	RXUnique  TrafficShare `json:"rx_unique"`
}

// Snapshot is the browser-facing aggregate. It contains no raw SessionID,
// destination, target, payload, credential, or live server object.
type Snapshot struct {
	At                 time.Time      `json:"at"`
	ActiveSessions     uint64         `json:"active_sessions"`
	TotalSessions      uint64         `json:"total_sessions"`
	ActiveLegs         uint64         `json:"active_legs"`
	TotalWireTxBytes   uint64         `json:"total_wire_tx_bytes"`
	TotalWireRxBytes   uint64         `json:"total_wire_rx_bytes"`
	TotalWireTxRateBPS *float64       `json:"total_wire_tx_rate_bps"`
	TotalWireRxRateBPS *float64       `json:"total_wire_rx_rate_bps"`
	TelemetryDropped   uint64         `json:"telemetry_dropped_events"`
	Legs               [2]LegSnapshot `json:"legs"`
	Traffic            Traffic        `json:"traffic"`
	Health             Health         `json:"health"`
}

type Event struct {
	ID       uint64    `json:"id"`
	At       time.Time `json:"at"`
	Type     string    `json:"type"`
	Severity string    `json:"severity"`
	Source   string    `json:"source"`
	Message  string    `json:"message"`
}

type HistoryResponse struct {
	Scope string     `json:"scope"`
	Items []Snapshot `json:"items"`
}

type EventsResponse struct {
	Items []Event `json:"items"`
}

type ListenerProbe func(target ListenerTarget) ListenerHealth

func defaultConfig() Config {
	return Config{
		Listen:              "127.0.0.1:24600",
		TelemetryURL:        "http://127.0.0.1:24500",
		PollInterval:        time.Second,
		MemoryRetention:     time.Hour,
		PersistentInterval:  10 * time.Second,
		PersistentRetention: 7 * 24 * time.Hour,
		EventRetention:      256,
		MaxSSEClients:       32,
	}
}

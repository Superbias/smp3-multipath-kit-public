package option

import "github.com/sagernet/sing/common/json/badoption"

type MultipathEndpointOptions struct {
	Server     string `json:"server"`
	ServerPort uint16 `json:"server_port"`
}

// MultipathAdaptiveOptions controls the optional logical-stream health
// controller for the public leg. It deliberately lives inside the multipath
// outbound rather than being a generic sing-box outbound so it can observe
// SMP3 sequence, ACK, reorder, and outstanding state.
type MultipathAdaptiveOptions struct {
	Enabled                 bool               `json:"enabled,omitempty"`
	EvaluationInterval      badoption.Duration `json:"evaluation_interval,omitempty"`
	Warmup                  badoption.Duration `json:"warmup,omitempty"`
	SuspectWindow           badoption.Duration `json:"suspect_window,omitempty"`
	HardFailureThreshold    uint32             `json:"hard_failure_threshold,omitempty"`
	HardFailureWindow       badoption.Duration `json:"hard_failure_window,omitempty"`
	Cooldown                badoption.Duration `json:"cooldown,omitempty"`
	MaxCooldown             badoption.Duration `json:"max_cooldown,omitempty"`
	RecoveryStableWindow    badoption.Duration `json:"recovery_stable_window,omitempty"`
	MinTxOutstandingFrames  uint32             `json:"min_tx_outstanding_frames,omitempty"`
	TxAckStall              badoption.Duration `json:"tx_ack_stall,omitempty"`
	MinRxReorderFrames      uint32             `json:"min_rx_reorder_frames,omitempty"`
	RxGapStall              badoption.Duration `json:"rx_gap_stall,omitempty"`
	GoodputDegradeRatio     float64            `json:"goodput_degrade_ratio,omitempty"`
	MinCanaryUsefulBytes    uint64             `json:"min_canary_useful_bytes,omitempty"`
	MinCanaryActiveWindows  uint32             `json:"min_canary_active_windows,omitempty"`
	InitialFailureThreshold uint32             `json:"initial_failure_threshold,omitempty"`
	InitialFailureWindow    badoption.Duration `json:"initial_failure_window,omitempty"`
}

// MultipathOutboundOptions combines exactly two existing reliable outbounds
// into one logical, ordered TCP byte stream. UDP is intentionally not
// aggregated and is delegated to UDPOutbound (or Preferred / first child).
type MultipathOutboundOptions struct {
	Outbounds   []string `json:"outbounds" reference:"outbound"`
	Preferred   string   `json:"preferred,omitempty" reference:"outbound"`
	UDPOutbound string   `json:"udp_outbound,omitempty" reference:"outbound"`
	// Endpoints optionally supplies a distinct aggregation address for each child path.
	// When omitted, Server/ServerPort is used for both paths for compatibility.
	Endpoints               []MultipathEndpointOptions `json:"endpoints,omitempty"`
	Server                  string                     `json:"server,omitempty"`
	ServerPort              uint16                     `json:"server_port,omitempty"`
	Password                string                     `json:"password"`
	Leg1Fallback            string                     `json:"leg1_fallback,omitempty" reference:"outbound"`
	Leg1Adaptive            *MultipathAdaptiveOptions  `json:"leg1_adaptive,omitempty"`
	ActivationThresholdMbps uint32                     `json:"activation_threshold_mbps,omitempty"`
	ActivationWindow        badoption.Duration         `json:"activation_window,omitempty"`
	ChunkSize               uint32                     `json:"chunk_size,omitempty"`
	QueueFrames             uint32                     `json:"queue_frames,omitempty"`
	BandwidthMbps           []uint32                   `json:"bandwidth_mbps,omitempty"`
	MaxReorderFrames        uint32                     `json:"max_reorder_frames,omitempty"`
	MaxInflightFrames       uint32                     `json:"max_inflight_frames,omitempty"`
	AckInterval             badoption.Duration         `json:"ack_interval,omitempty"`
	RetransmitTimeout       badoption.Duration         `json:"retransmit_timeout,omitempty"`
	RecoveryTimeout         badoption.Duration         `json:"recovery_timeout,omitempty"`
	RedialInterval          badoption.Duration         `json:"redial_interval,omitempty"`
}

type MultipathInboundOptions struct {
	ListenOptions
	Password                string             `json:"password"`
	ActivationThresholdMbps uint32             `json:"activation_threshold_mbps,omitempty"`
	ActivationWindow        badoption.Duration `json:"activation_window,omitempty"`
	ChunkSize               uint32             `json:"chunk_size,omitempty"`
	QueueFrames             uint32             `json:"queue_frames,omitempty"`
	BandwidthMbps           []uint32           `json:"bandwidth_mbps,omitempty"`
	MaxReorderFrames        uint32             `json:"max_reorder_frames,omitempty"`
	MaxInflightFrames       uint32             `json:"max_inflight_frames,omitempty"`
	AckInterval             badoption.Duration `json:"ack_interval,omitempty"`
	RetransmitTimeout       badoption.Duration `json:"retransmit_timeout,omitempty"`
	RecoveryTimeout         badoption.Duration `json:"recovery_timeout,omitempty"`
}

package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

// Version is the public SMP3 Standalone client release version. Official builds
// inject the same value from the repository VERSION file with -ldflags.
var Version = "2.4.0"

type Duration time.Duration

type NonNegativeDuration time.Duration

func (d *NonNegativeDuration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("duration must be a string such as 0s or 20ms")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return fmt.Errorf("invalid duration %q", value)
	}
	*d = NonNegativeDuration(parsed)
	return nil
}

func (d NonNegativeDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d NonNegativeDuration) Time() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("duration must be a string such as 1s or 20ms")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fmt.Errorf("invalid duration %q", value)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
func (d Duration) Time() time.Duration          { return time.Duration(d) }

type Config struct {
	Listen        string               `json:"listen"`
	UpstreamSocks UpstreamSocksOptions `json:"upstream_socks"`
	SMP3          SMP3Options          `json:"smp3"`
}

type UpstreamSocksOptions struct {
	Address        string                 `json:"address"`
	Username       string                 `json:"username,omitempty"`
	Password       string                 `json:"password,omitempty"`
	ConnectTimeout Duration               `json:"connect_timeout"`
	Leg0           *UpstreamSocksOverride `json:"leg0,omitempty"`
	Leg1           *UpstreamSocksOverride `json:"leg1,omitempty"`
}

// LegUpstreamAddresses returns only the safe endpoint identities used by each
// logical leg. It intentionally excludes upstream credentials and other
// protocol-specific settings so callers can log or display binding state
// without exposing secrets.
func (c Config) LegUpstreamAddresses() [2]string {
	return [2]string{
		c.effectiveUpstream(0).Address,
		c.effectiveUpstream(1).Address,
	}
}

// UpstreamSocksOverride selects a carrier SOCKS5 endpoint for one logical
// SMP3 leg. It deliberately does not contain another override, so selection
// remains a single, immutable lookup at carrier connection establishment.
type UpstreamSocksOverride struct {
	Address        string   `json:"address"`
	Username       string   `json:"username,omitempty"`
	Password       string   `json:"password,omitempty"`
	ConnectTimeout Duration `json:"connect_timeout"`
}

type SMP3Options struct {
	Password            string             `json:"password"`
	CarrierMode         string             `json:"carrier_mode,omitempty"`
	HostCarrier         HostCarrierOptions `json:"host_carrier,omitempty"`
	Routes              RouteOptions       `json:"routes"`
	CarrierReadyTimeout Duration           `json:"carrier_ready_timeout"`
	Stream              StreamOptions      `json:"stream"`
	UDP                 UDPOptions         `json:"udp"`
}

type HostCarrierOptions struct {
	ControlAddress string   `json:"control_address,omitempty"`
	CarrierHandle  string   `json:"carrier_handle,omitempty"`
	Leg0Handle     string   `json:"leg0_handle,omitempty"`
	Leg1Handle     string   `json:"leg1_handle,omitempty"`
	TargetHandle   string   `json:"target_handle,omitempty"`
	ConnectTimeout Duration `json:"connect_timeout,omitempty"`
}

type StreamOptions struct {
	SchedulerMode           string              `json:"scheduler_mode"`
	StartupPolicy           string              `json:"startup_policy,omitempty"`
	StartupPreferredLeg     uint8               `json:"startup_preferred_leg,omitempty"`
	StartupGrace            NonNegativeDuration `json:"startup_grace,omitempty"`
	ActivationThresholdMbps uint32              `json:"activation_threshold_mbps"`
	ActivationWindow        Duration            `json:"activation_window"`
	ChunkSize               int                 `json:"chunk_size"`
	QueueFrames             int                 `json:"queue_frames"`
	BandwidthMbps           []uint32            `json:"bandwidth_mbps"`
	MaxReorderFrames        int                 `json:"max_reorder_frames"`
	MaxInflightFrames       int                 `json:"max_inflight_frames"`
	AckInterval             Duration            `json:"ack_interval"`
	RetransmitTimeout       Duration            `json:"retransmit_timeout"`
	RecoveryTimeout         Duration            `json:"recovery_timeout"`
	RedialInterval          Duration            `json:"redial_interval"`
}

type RouteOptions struct {
	Leg0         string `json:"leg0"`
	Leg1         string `json:"leg1"`
	Leg1Fallback string `json:"leg1_fallback,omitempty"`
}

type UDPOptions struct {
	Enabled                    bool     `json:"enabled"`
	Mode                       string   `json:"mode"`
	QueueFrames                int      `json:"queue_frames"`
	MaxDatagramSize            int      `json:"max_datagram_size"`
	DedupWindow                uint64   `json:"dedup_window"`
	IdleTimeout                Duration `json:"idle_timeout"`
	AdaptiveQueueDelay         Duration `json:"adaptive_queue_delay"`
	AdaptiveDuplicateThreshold int      `json:"adaptive_duplicate_threshold"`
}

func DefaultConfig() Config {
	return Config{
		Listen:        "127.0.0.1:18080",
		UpstreamSocks: UpstreamSocksOptions{Address: "127.0.0.1:7898", ConnectTimeout: Duration(10 * time.Second)},
		SMP3: SMP3Options{
			CarrierReadyTimeout: Duration(5 * time.Second),
			Stream: StreamOptions{
				SchedulerMode:           "adaptive",
				ActivationThresholdMbps: 80,
				ActivationWindow:        Duration(time.Second),
				ChunkSize:               64 * 1024,
				QueueFrames:             256,
				BandwidthMbps:           []uint32{128, 500},
				MaxReorderFrames:        4096,
				MaxInflightFrames:       1024,
				AckInterval:             Duration(20 * time.Millisecond),
				RetransmitTimeout:       Duration(1500 * time.Millisecond),
				RecoveryTimeout:         Duration(15 * time.Second),
				RedialInterval:          Duration(time.Second),
			},
			UDP: UDPOptions{
				Mode:               "adaptive",
				QueueFrames:        256,
				MaxDatagramSize:    16384,
				DedupWindow:        4096,
				IdleTimeout:        Duration(2 * time.Minute),
				AdaptiveQueueDelay: Duration(120 * time.Millisecond),
			},
		},
	}
}

func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return LoadConfig(bytes.NewReader(data))
}

func LoadConfig(reader io.Reader) (Config, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	config := DefaultConfig()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, errors.New("config contains more than one JSON value")
		}
		return Config{}, fmt.Errorf("decode trailing config data: %w", err)
	}
	if err := config.NormalizeAndValidate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c *Config) NormalizeAndValidate() error {
	if err := validateLoopbackListen(c.Listen); err != nil {
		return err
	}
	if c.UpstreamSocks.Address == "" {
		c.UpstreamSocks.Address = "127.0.0.1:7898"
	}
	if err := validateEndpoint(c.UpstreamSocks.Address, "upstream_socks.address", false); err != nil {
		return err
	}
	if c.UpstreamSocks.ConnectTimeout.Time() <= 0 {
		c.UpstreamSocks.ConnectTimeout = Duration(10 * time.Second)
	}
	if c.UpstreamSocks.ConnectTimeout.Time() > time.Minute {
		return errors.New("invalid upstream_socks.connect_timeout: must be at most 1m")
	}
	if err := validateUpstreamOverride(c.UpstreamSocks.Leg0, "upstream_socks.leg0"); err != nil {
		return err
	}
	if err := validateUpstreamOverride(c.UpstreamSocks.Leg1, "upstream_socks.leg1"); err != nil {
		return err
	}
	if c.SMP3.Password == "" {
		return errors.New("empty smp3.password")
	}
	if c.SMP3.CarrierMode == "" {
		c.SMP3.CarrierMode = "legacy_socks"
	}
	if c.SMP3.CarrierMode != "legacy_socks" && c.SMP3.CarrierMode != "host_bridge" {
		return fmt.Errorf("invalid smp3.carrier_mode %q", c.SMP3.CarrierMode)
	}
	if c.SMP3.CarrierMode == "host_bridge" {
		if err := validateLoopbackListen(c.SMP3.HostCarrier.ControlAddress); err != nil {
			return fmt.Errorf("invalid smp3.host_carrier.control_address: %w", err)
		}
		if c.SMP3.HostCarrier.Leg0Handle == "" {
			c.SMP3.HostCarrier.Leg0Handle = c.SMP3.HostCarrier.CarrierHandle
		}
		if c.SMP3.HostCarrier.Leg1Handle == "" {
			c.SMP3.HostCarrier.Leg1Handle = c.SMP3.HostCarrier.CarrierHandle
		}
		if c.SMP3.HostCarrier.Leg0Handle == "" || c.SMP3.HostCarrier.Leg1Handle == "" || c.SMP3.HostCarrier.TargetHandle == "" {
			return errors.New("smp3.host_carrier handles are required")
		}
		if c.SMP3.HostCarrier.ConnectTimeout.Time() <= 0 {
			c.SMP3.HostCarrier.ConnectTimeout = Duration(5 * time.Second)
		}
	}
	if c.SMP3.CarrierReadyTimeout.Time() <= 0 {
		c.SMP3.CarrierReadyTimeout = Duration(5 * time.Second)
	}
	if c.SMP3.CarrierReadyTimeout.Time() > time.Minute {
		return errors.New("invalid smp3.carrier_ready_timeout: must be at most 1m")
	}
	if err := validateRoutes(c.SMP3.Routes); err != nil {
		return err
	}
	if err := validateStream(&c.SMP3.Stream); err != nil {
		return err
	}
	return validateUDP(&c.SMP3.UDP)
}

func validateUpstreamOverride(override *UpstreamSocksOverride, field string) error {
	if override == nil {
		return nil
	}
	if err := validateEndpoint(override.Address, field+".address", false); err != nil {
		return err
	}
	if override.ConnectTimeout.Time() <= 0 {
		override.ConnectTimeout = Duration(10 * time.Second)
	}
	if override.ConnectTimeout.Time() > time.Minute {
		return fmt.Errorf("invalid %s.connect_timeout: must be at most 1m", field)
	}
	return nil
}

// effectiveUpstream returns the immutable connection-level carrier selection
// for a logical leg. A missing override preserves the original global
// upstream_socks behavior. It intentionally does not inspect the other leg,
// so one leg can never silently fall back to the other leg's carrier.
func (c Config) effectiveUpstream(leg uint8) UpstreamSocksOptions {
	override := c.UpstreamSocks.Leg0
	if leg == 1 {
		override = c.UpstreamSocks.Leg1
	}
	if override == nil {
		return c.UpstreamSocks
	}
	return UpstreamSocksOptions{
		Address:        override.Address,
		Username:       override.Username,
		Password:       override.Password,
		ConnectTimeout: override.ConnectTimeout,
	}
}

func validateLoopbackListen(address string) error {
	if address == "" {
		return errors.New("invalid listen: address is empty")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || (host != "127.0.0.1" && host != "::1") {
		return fmt.Errorf("invalid listen %q: only 127.0.0.1 and ::1 are allowed", address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("invalid listen port %q", portText)
	}
	return nil
}

func validateEndpoint(address, field string, allowZero bool) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("invalid %s %q: expected host:port", field, address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || (!allowZero && port == 0) {
		return fmt.Errorf("invalid %s port %q", field, portText)
	}
	return nil
}

func validateRoutes(routes RouteOptions) error {
	if routes.Leg0 == "" || routes.Leg1 == "" {
		return errors.New("smp3.routes.leg0 and smp3.routes.leg1 are required")
	}
	if err := validateEndpoint(routes.Leg0, "smp3.routes.leg0", false); err != nil {
		return err
	}
	if err := validateEndpoint(routes.Leg1, "smp3.routes.leg1", false); err != nil {
		return err
	}
	if routes.Leg1Fallback != "" {
		if err := validateEndpoint(routes.Leg1Fallback, "smp3.routes.leg1_fallback", false); err != nil {
			return err
		}
		if routes.Leg1Fallback == routes.Leg0 || routes.Leg1Fallback == routes.Leg1 {
			return fmt.Errorf("duplicate route target %q in smp3.routes.leg1_fallback and leg route", routes.Leg1Fallback)
		}
	}
	return nil
}

func validateStream(c *StreamOptions) error {
	if c.SchedulerMode == "" {
		c.SchedulerMode = "adaptive"
	}
	if c.SchedulerMode != "adaptive" && c.SchedulerMode != "static" && c.SchedulerMode != "aggregation" {
		return fmt.Errorf("invalid smp3.scheduler_mode %q", c.SchedulerMode)
	}
	if c.StartupPolicy == "" {
		c.StartupPolicy = "first-ready"
	}
	if c.StartupPolicy != "first-ready" && c.StartupPolicy != "preferred" {
		return fmt.Errorf("invalid smp3.startup_policy %q", c.StartupPolicy)
	}
	if c.StartupPreferredLeg > 1 {
		return errors.New("invalid smp3.startup_preferred_leg: must be 0 or 1")
	}
	if c.StartupGrace.Time() < 0 || c.StartupGrace.Time() > smp3core.MaxStreamStartupGrace {
		return fmt.Errorf("invalid smp3.startup_grace: must be between 0 and %s", smp3core.MaxStreamStartupGrace)
	}
	if c.ActivationThresholdMbps == 0 {
		c.ActivationThresholdMbps = 80
	}
	if c.ActivationWindow.Time() <= 0 {
		c.ActivationWindow = Duration(time.Second)
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = 64 * 1024
	}
	if c.ChunkSize < 1024 || c.ChunkSize > 1<<20 {
		return errors.New("invalid smp3.chunk_size")
	}
	if c.QueueFrames == 0 {
		c.QueueFrames = 256
	}
	if c.QueueFrames < 8 || c.QueueFrames > 4096 {
		return errors.New("invalid smp3.queue_frames")
	}
	if len(c.BandwidthMbps) != 0 && len(c.BandwidthMbps) != 2 {
		return errors.New("invalid smp3.bandwidth_mbps: expected zero or two entries")
	}
	for _, value := range c.BandwidthMbps {
		if value == 0 {
			return errors.New("invalid smp3.bandwidth_mbps: entries must be positive")
		}
	}
	if c.MaxReorderFrames == 0 {
		c.MaxReorderFrames = 4096
	}
	if c.MaxReorderFrames < 1 || c.MaxReorderFrames > 1<<20 {
		return errors.New("invalid smp3.max_reorder_frames")
	}
	if c.MaxInflightFrames == 0 {
		c.MaxInflightFrames = 1024
	}
	if c.MaxInflightFrames < 1 || c.MaxInflightFrames > 1<<20 {
		return errors.New("invalid smp3.max_inflight_frames")
	}
	if c.AckInterval.Time() <= 0 {
		c.AckInterval = Duration(20 * time.Millisecond)
	}
	if c.RetransmitTimeout.Time() <= 0 {
		c.RetransmitTimeout = Duration(1500 * time.Millisecond)
	}
	if c.RecoveryTimeout.Time() <= 0 {
		c.RecoveryTimeout = Duration(15 * time.Second)
	}
	if c.RedialInterval.Time() <= 0 {
		c.RedialInterval = Duration(time.Second)
	}
	return nil
}

func validateUDP(c *UDPOptions) error {
	if c.Mode == "" {
		c.Mode = "adaptive"
	}
	if c.Mode != "adaptive" && c.Mode != "stripe" && c.Mode != "duplicate" {
		return fmt.Errorf("invalid smp3.udp.mode %q", c.Mode)
	}
	if c.QueueFrames == 0 {
		c.QueueFrames = 256
	}
	if c.QueueFrames < 8 || c.QueueFrames > 4096 {
		return errors.New("invalid smp3.udp.queue_frames")
	}
	if c.MaxDatagramSize == 0 {
		c.MaxDatagramSize = 16384
	}
	if c.MaxDatagramSize < 512 || c.MaxDatagramSize > smp3core.MaxDatagramPayload {
		return errors.New("invalid smp3.udp.max_datagram_size")
	}
	if c.DedupWindow == 0 {
		c.DedupWindow = 4096
	}
	if c.DedupWindow < 64 || c.DedupWindow > 1<<20 {
		return errors.New("invalid smp3.udp.dedup_window")
	}
	if c.IdleTimeout.Time() <= 0 {
		c.IdleTimeout = Duration(2 * time.Minute)
	}
	if c.AdaptiveQueueDelay.Time() <= 0 {
		c.AdaptiveQueueDelay = Duration(120 * time.Millisecond)
	}
	if c.AdaptiveDuplicateThreshold < 0 || c.AdaptiveDuplicateThreshold > c.MaxDatagramSize {
		return errors.New("invalid smp3.udp.adaptive_duplicate_threshold")
	}
	return nil
}

func (c SMP3Options) streamConfig(onActivate func(), onLegDown func(uint8, error)) smp3core.StreamConfig {
	startupPolicy := smp3core.StreamStartupFirstReady
	if c.Stream.StartupPolicy == "preferred" {
		startupPolicy = smp3core.StreamStartupPreferred
	}
	return smp3core.StreamConfig{
		SchedulerMode:       streamSchedulerMode(c.Stream.SchedulerMode),
		ChunkSize:           c.Stream.ChunkSize,
		QueueFrames:         c.Stream.QueueFrames,
		ThresholdBytesPS:    uint64(c.Stream.ActivationThresholdMbps) * 1000 * 1000 / 8,
		ActivationWindow:    c.Stream.ActivationWindow.Time(),
		BandwidthMbps:       append([]uint32(nil), c.Stream.BandwidthMbps...),
		MaxReorderFrames:    c.Stream.MaxReorderFrames,
		MaxInflightFrames:   c.Stream.MaxInflightFrames,
		AckInterval:         c.Stream.AckInterval.Time(),
		RetransmitTimeout:   c.Stream.RetransmitTimeout.Time(),
		RecoveryTimeout:     c.Stream.RecoveryTimeout.Time(),
		OnActivate:          onActivate,
		OnLegDown:           onLegDown,
		StartupPolicy:       startupPolicy,
		StartupPreferredLeg: smp3core.LegID(c.Stream.StartupPreferredLeg),
		StartupGrace:        c.Stream.StartupGrace.Time(),
	}
}

func streamSchedulerMode(value string) smp3core.StreamSchedulerMode {
	if value == "static" {
		return smp3core.StreamSchedulerStatic
	}
	if value == "aggregation" {
		return smp3core.StreamSchedulerAggregation
	}
	return smp3core.StreamSchedulerAdaptive
}

func (c UDPOptions) datagramConfig(onLegDown func(smp3core.LegID, error), onLegUseful func(smp3core.LegID, int)) smp3core.DatagramConfig {
	return smp3core.DatagramConfig{
		Mode:                       datagramMode(c.Mode),
		QueueFrames:                c.QueueFrames,
		MaxDatagramSize:            c.MaxDatagramSize,
		DedupWindow:                c.DedupWindow,
		IdleTimeout:                c.IdleTimeout.Time(),
		RecoveryTimeout:            time.Duration(15 * time.Second),
		AdaptiveQueueDelay:         c.AdaptiveQueueDelay.Time(),
		AdaptiveDuplicateThreshold: c.AdaptiveDuplicateThreshold,
		OnLegDown:                  onLegDown,
		OnLegUseful:                onLegUseful,
	}
}

func datagramMode(value string) smp3core.DatagramMode {
	switch value {
	case "stripe":
		return smp3core.DatagramStripe
	case "duplicate":
		return smp3core.DatagramDuplicate
	default:
		return smp3core.DatagramAdaptive
	}
}

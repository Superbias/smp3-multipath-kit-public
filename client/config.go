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
	"strings"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

// Version is the public SMP3 Standalone client release version. Official builds
// inject the same value from the repository VERSION file with -ldflags.
var Version = "2.7.0-selfuse.4"

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
	LogLevel      string               `json:"log_level,omitempty"`
	UpstreamSocks UpstreamSocksOptions `json:"upstream_socks"`
	SMP3          SMP3Options          `json:"smp3"`
}

type UpstreamSocksOptions struct {
	Address        string                  `json:"address"`
	Username       string                  `json:"username,omitempty"`
	Password       string                  `json:"password,omitempty"`
	ConnectTimeout Duration                `json:"connect_timeout"`
	Leg0           *UpstreamSocksOverride  `json:"leg0,omitempty"`
	Leg1           *UpstreamSocksOverride  `json:"leg1,omitempty"`
	Legs           []UpstreamSocksOverride `json:"legs,omitempty"`
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

// LegUpstreamAddressesN returns the upstream carrier address for every
// configured stream route. The legacy fixed-size accessor above remains a
// compatibility view for existing callers.
func (c Config) LegUpstreamAddressesN() []string {
	routes := c.SMP3.Routes.entries()
	addresses := make([]string, len(routes))
	for id := range routes {
		addresses[id] = c.effectiveUpstream(uint8(id)).Address
	}
	return addresses
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
	CarrierTLS          CarrierTLSOptions  `json:"carrier_tls,omitempty"`
	HostCarrier         HostCarrierOptions `json:"host_carrier,omitempty"`
	Routes              RouteOptions       `json:"routes"`
	CarrierReadyTimeout Duration           `json:"carrier_ready_timeout"`
	Stream              StreamOptions      `json:"stream"`
	UDP                 UDPOptions         `json:"udp"`
}

// CarrierTLSOptions enables end-to-end TLS from the standalone client to the
// SMP3 server over the selected TCP carrier. TLS is disabled by default.
// Host-bridge carriers own their transport security and cannot be wrapped here.
type CarrierTLSOptions struct {
	Enabled    bool   `json:"enabled,omitempty"`
	ServerName string `json:"server_name,omitempty"`
	RootCAFile string `json:"root_ca_file,omitempty"`
}

type HostCarrierOptions struct {
	ControlAddress string   `json:"control_address,omitempty"`
	CarrierHandle  string   `json:"carrier_handle,omitempty"`
	Leg0Handle     string   `json:"leg0_handle,omitempty"`
	Leg1Handle     string   `json:"leg1_handle,omitempty"`
	LegHandles     []string `json:"leg_handles,omitempty"`
	TargetHandle   string   `json:"target_handle,omitempty"`
	ConnectTimeout Duration `json:"connect_timeout,omitempty"`
}

type StreamOptions struct {
	SchedulerMode         string `json:"scheduler_mode"`
	CapacityMode          string `json:"capacity_mode,omitempty"`
	ActivationMode        string `json:"activation_mode,omitempty"`
	HOLMode               string `json:"hol_mode,omitempty"`
	ExperimentalPathAware bool   `json:"experimental_path_aware,omitempty"`
	// AllowV7LegacyFallback permits retrying an older HELLO version on a new
	// Carrier after an ambiguous V7 transport failure. It is disabled by
	// default because an unauthenticated timeout/reset is not proof that a peer
	// lacks V7 support.
	AllowV7LegacyFallback bool `json:"allow_v7_legacy_fallback,omitempty"`
	// AllowPathRTTV4Fallback is the master opt-in for V4 fallback.
	AllowPathRTTV4Fallback bool `json:"allow_path_rtt_v4_fallback,omitempty"`
	// AllowPathRTTV4FallbackOnTransportError permits a fresh V4 connection
	// after an ambiguous V6 handshake transport failure. It does not mean the
	// peer explicitly rejected V6. Both fallback options must be enabled.
	AllowPathRTTV4FallbackOnTransportError bool                `json:"allow_path_rtt_v4_fallback_on_transport_error,omitempty"`
	StartupPolicy                          string              `json:"startup_policy,omitempty"`
	StartupPreferredLeg                    uint8               `json:"startup_preferred_leg,omitempty"`
	StartupGrace                           NonNegativeDuration `json:"startup_grace,omitempty"`
	ActivationThresholdMbps                uint32              `json:"activation_threshold_mbps"`
	ActivationWindow                       Duration            `json:"activation_window"`
	ChunkSize                              int                 `json:"chunk_size"`
	QueueFrames                            int                 `json:"queue_frames"`
	BandwidthMbps                          []uint32            `json:"bandwidth_mbps"`
	MaxActiveLegs                          int                 `json:"max_active_legs,omitempty"`
	MaxReorderFrames                       int                 `json:"max_reorder_frames"`
	MaxInflightFrames                      int                 `json:"max_inflight_frames"`
	AckInterval                            Duration            `json:"ack_interval"`
	RetransmitTimeout                      Duration            `json:"retransmit_timeout"`
	RecoveryTimeout                        Duration            `json:"recovery_timeout"`
	RedialInterval                         Duration            `json:"redial_interval"`
}

type RouteOptions struct {
	Leg0         string            `json:"leg0"`
	Leg1         string            `json:"leg1"`
	Leg1Fallback string            `json:"leg1_fallback,omitempty"`
	Legs         []RouteLegOptions `json:"legs,omitempty"`
}

type RouteLegOptions struct {
	ID       uint8  `json:"id"`
	Name     string `json:"name,omitempty"`
	Address  string `json:"address"`
	Fallback string `json:"fallback,omitempty"`
}

func (r RouteOptions) entries() []RouteLegOptions {
	if len(r.Legs) > 0 {
		return append([]RouteLegOptions(nil), r.Legs...)
	}
	return []RouteLegOptions{{ID: 0, Name: "leg0", Address: r.Leg0}, {ID: 1, Name: "leg1", Address: r.Leg1, Fallback: r.Leg1Fallback}}
}

func (r RouteOptions) leg(id uint8) (RouteLegOptions, bool) {
	entries := r.entries()
	if int(id) >= len(entries) || entries[id].ID != id {
		return RouteLegOptions{}, false
	}
	return entries[id], true
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
				CapacityMode:            "fixed",
				ActivationThresholdMbps: 80,
				ActivationWindow:        Duration(time.Second),
				ChunkSize:               64 * 1024,
				QueueFrames:             256,
				BandwidthMbps:           []uint32{128, 500},
				MaxActiveLegs:           smp3core.DefaultMaxActiveStreamLegs,
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
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	c.LogLevel = strings.ToLower(c.LogLevel)
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("invalid log_level %q", c.LogLevel)
	}
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
	if c.SMP3.CarrierTLS.Enabled {
		if c.SMP3.CarrierMode != "legacy_socks" {
			return errors.New("smp3.carrier_tls is only supported with legacy_socks; host_bridge owns its transport security")
		}
		if strings.TrimSpace(c.SMP3.CarrierTLS.ServerName) == "" {
			return errors.New("smp3.carrier_tls.server_name is required when carrier TLS is enabled")
		}
	}
	if !c.SMP3.CarrierTLS.Enabled && (c.SMP3.CarrierTLS.ServerName != "" || c.SMP3.CarrierTLS.RootCAFile != "") {
		return errors.New("smp3.carrier_tls server_name/root_ca_file require enabled=true")
	}
	if c.SMP3.CarrierMode == "host_bridge" {
		if err := validateLoopbackListen(c.SMP3.HostCarrier.ControlAddress); err != nil {
			return fmt.Errorf("invalid smp3.host_carrier.control_address: %w", err)
		}
		routes := c.SMP3.Routes.entries()
		if len(c.SMP3.HostCarrier.LegHandles) > len(routes) {
			return errors.New("smp3.host_carrier.leg_handles cannot exceed configured route count")
		}
		if c.SMP3.HostCarrier.Leg0Handle == "" {
			c.SMP3.HostCarrier.Leg0Handle = c.SMP3.HostCarrier.CarrierHandle
		}
		if c.SMP3.HostCarrier.Leg1Handle == "" {
			c.SMP3.HostCarrier.Leg1Handle = c.SMP3.HostCarrier.CarrierHandle
		}
		if len(c.SMP3.HostCarrier.LegHandles) == 0 {
			c.SMP3.HostCarrier.LegHandles = make([]string, len(routes))
		}
		for id := range routes {
			if c.SMP3.HostCarrier.LegHandles[id] != "" {
				continue
			}
			switch id {
			case 0:
				c.SMP3.HostCarrier.LegHandles[id] = c.SMP3.HostCarrier.Leg0Handle
			case 1:
				c.SMP3.HostCarrier.LegHandles[id] = c.SMP3.HostCarrier.Leg1Handle
			default:
				c.SMP3.HostCarrier.LegHandles[id] = c.SMP3.HostCarrier.CarrierHandle
			}
			if c.SMP3.HostCarrier.LegHandles[id] == "" {
				return fmt.Errorf("smp3.host_carrier.leg_handles[%d] is required", id)
			}
		}
		if c.SMP3.HostCarrier.TargetHandle == "" {
			return errors.New("smp3.host_carrier.target_handle is required")
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
	if err := validateRoutesWithUpstreams(c.SMP3.Routes, c.effectiveUpstream); err != nil {
		return err
	}
	routeCount := len(c.SMP3.Routes.entries())
	if len(c.UpstreamSocks.Legs) > routeCount {
		return errors.New("upstream_socks.legs cannot exceed configured smp3.routes.legs")
	}
	for id := range c.UpstreamSocks.Legs {
		override := &c.UpstreamSocks.Legs[id]
		if err := validateUpstreamOverride(override, fmt.Sprintf("upstream_socks.legs[%d]", id)); err != nil {
			return err
		}
	}
	if err := validateStream(&c.SMP3.Stream); err != nil {
		return err
	}
	if routeCount > c.SMP3.Stream.MaxActiveLegs {
		return fmt.Errorf("smp3.routes.legs has %d entries, exceeding stream.max_active_legs %d", routeCount, c.SMP3.Stream.MaxActiveLegs)
	}
	if int(c.SMP3.Stream.StartupPreferredLeg) >= routeCount {
		return fmt.Errorf("invalid smp3.stream.startup_preferred_leg: must reference a configured leg in [0,%d)", routeCount)
	}
	if c.SMP3.UDP.Enabled && routeCount > 2 {
		return errors.New("smp3.udp currently supports at most two configured legs; disable UDP or configure at most two legs")
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
	if int(leg) < len(c.UpstreamSocks.Legs) {
		override := c.UpstreamSocks.Legs[leg]
		return UpstreamSocksOptions{Address: override.Address, Username: override.Username, Password: override.Password, ConnectTimeout: override.ConnectTimeout}
	}
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
	return validateRoutesWithUpstreams(routes, nil)
}

func validateRoutesWithUpstreams(routes RouteOptions, upstreamForLeg func(uint8) UpstreamSocksOptions) error {
	if len(routes.Legs) > 0 {
		if routes.Leg0 != "" || routes.Leg1 != "" || routes.Leg1Fallback != "" {
			return errors.New("smp3.routes must use either legacy leg0/leg1 fields or legs, not both")
		}
		if len(routes.Legs) > smp3core.MaxStreamLegIDCount {
			return fmt.Errorf("smp3.routes.legs exceeds protocol maximum %d", smp3core.MaxStreamLegIDCount)
		}
		type binding struct{ upstream UpstreamSocksOptions }
		seen := make(map[string][]binding, len(routes.Legs)*2)
		for index, leg := range routes.Legs {
			if int(leg.ID) != index {
				return fmt.Errorf("smp3.routes.legs[%d].id must equal its contiguous protocol leg ID %d", index, index)
			}
			if err := validateEndpoint(leg.Address, fmt.Sprintf("smp3.routes.legs[%d].address", index), false); err != nil {
				return err
			}
			upstream := UpstreamSocksOptions{}
			if upstreamForLeg != nil {
				upstream = upstreamForLeg(uint8(index))
			}
			for _, prior := range seen[leg.Address] {
				if upstreamForLeg == nil || sameUpstreamCarrier(prior.upstream, upstream) {
					return fmt.Errorf("duplicate SMP3 route target %q on the same upstream carrier", leg.Address)
				}
			}
			seen[leg.Address] = append(seen[leg.Address], binding{upstream: upstream})
			if leg.Fallback != "" {
				if err := validateEndpoint(leg.Fallback, fmt.Sprintf("smp3.routes.legs[%d].fallback", index), false); err != nil {
					return err
				}
				for _, prior := range seen[leg.Fallback] {
					if upstreamForLeg == nil || sameUpstreamCarrier(prior.upstream, upstream) {
						return fmt.Errorf("duplicate SMP3 fallback target %q on the same upstream carrier", leg.Fallback)
					}
				}
				seen[leg.Fallback] = append(seen[leg.Fallback], binding{upstream: upstream})
			}
		}
		return nil
	}
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

func sameUpstreamCarrier(left, right UpstreamSocksOptions) bool {
	return left.Address == right.Address && left.Username == right.Username && left.Password == right.Password
}

func validateStream(c *StreamOptions) error {
	if c.MaxActiveLegs == 0 {
		c.MaxActiveLegs = smp3core.DefaultMaxActiveStreamLegs
	}
	if c.MaxActiveLegs < 1 || c.MaxActiveLegs > smp3core.MaxStreamLegIDCount {
		return fmt.Errorf("invalid smp3.stream.max_active_legs: must be between 1 and %d", smp3core.MaxStreamLegIDCount)
	}
	if c.SchedulerMode == "" {
		c.SchedulerMode = "adaptive"
	}
	if c.SchedulerMode != "adaptive" && c.SchedulerMode != "static" && c.SchedulerMode != "aggregation" {
		return fmt.Errorf("invalid smp3.scheduler_mode %q", c.SchedulerMode)
	}
	if c.CapacityMode == "" {
		c.CapacityMode = "fixed"
	}
	if c.CapacityMode != "fixed" && c.CapacityMode != "dynamic" {
		return fmt.Errorf("invalid smp3.stream.capacity_mode %q", c.CapacityMode)
	}
	if c.CapacityMode == "dynamic" && c.SchedulerMode != "aggregation" {
		return errors.New("smp3.stream.capacity_mode dynamic requires scheduler_mode aggregation")
	}
	if c.ActivationMode == "" {
		c.ActivationMode = "legacy"
	}
	if c.ActivationMode != "legacy" && c.ActivationMode != "dynamic" {
		return fmt.Errorf("invalid smp3.activation_mode %q", c.ActivationMode)
	}
	if c.ActivationMode == "dynamic" && c.SchedulerMode != "aggregation" {
		return errors.New("smp3.stream.activation_mode dynamic requires scheduler_mode aggregation")
	}
	if c.HOLMode == "" {
		c.HOLMode = "legacy"
	}
	if c.HOLMode != "legacy" && c.HOLMode != "disabled" && c.HOLMode != "completion" {
		return fmt.Errorf("invalid smp3.stream.hol_mode %q", c.HOLMode)
	}
	if c.HOLMode == "completion" && c.SchedulerMode != "aggregation" {
		return errors.New("smp3.stream.hol_mode completion requires scheduler_mode aggregation")
	}
	if c.StartupPolicy == "" {
		c.StartupPolicy = "first-ready"
	}
	if c.StartupPolicy != "first-ready" && c.StartupPolicy != "preferred" {
		return fmt.Errorf("invalid smp3.startup_policy %q", c.StartupPolicy)
	}
	if int(c.StartupPreferredLeg) >= c.MaxActiveLegs {
		return fmt.Errorf("invalid smp3.startup_preferred_leg: must be less than max_active_legs (%d)", c.MaxActiveLegs)
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
	if len(c.BandwidthMbps) > c.MaxActiveLegs {
		return fmt.Errorf("invalid smp3.bandwidth_mbps: cannot exceed max_active_legs (%d)", c.MaxActiveLegs)
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
		SchedulerMode:         streamSchedulerMode(c.Stream.SchedulerMode),
		CapacityMode:          streamCapacityMode(c.Stream.CapacityMode),
		ActivationMode:        streamActivationMode(c.Stream.ActivationMode),
		HOLMode:               streamHOLMode(c.Stream.HOLMode),
		ExperimentalPathAware: c.Stream.ExperimentalPathAware,
		PathRole:              smp3core.StreamPathRoleClient,
		ChunkSize:             c.Stream.ChunkSize,
		QueueFrames:           c.Stream.QueueFrames,
		ThresholdBytesPS:      uint64(c.Stream.ActivationThresholdMbps) * 1000 * 1000 / 8,
		ActivationWindow:      c.Stream.ActivationWindow.Time(),
		BandwidthMbps:         append([]uint32(nil), c.Stream.BandwidthMbps...),
		MaxReorderFrames:      c.Stream.MaxReorderFrames,
		MaxInflightFrames:     c.Stream.MaxInflightFrames,
		AckInterval:           c.Stream.AckInterval.Time(),
		RetransmitTimeout:     c.Stream.RetransmitTimeout.Time(),
		RecoveryTimeout:       c.Stream.RecoveryTimeout.Time(),
		OnActivate:            onActivate,
		OnLegDown:             onLegDown,
		StartupPolicy:         startupPolicy,
		StartupPreferredLeg:   smp3core.LegID(c.Stream.StartupPreferredLeg),
		MaxActiveLegs:         c.Stream.MaxActiveLegs,
		StartupGrace:          c.Stream.StartupGrace.Time(),
	}
}

func streamActivationMode(value string) smp3core.StreamActivationMode {
	if value == "dynamic" {
		return smp3core.StreamActivationDynamic
	}
	return smp3core.StreamActivationLegacy
}

func streamHOLMode(value string) smp3core.StreamHOLMode {
	if value == "completion" {
		return smp3core.StreamHOLCompletion
	}
	return smp3core.StreamHOLLegacy
}

func streamCapacityMode(value string) smp3core.StreamCapacityMode {
	if value == "dynamic" {
		return smp3core.StreamCapacityDynamic
	}
	return smp3core.StreamCapacityFixed
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

package server

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
)

const Version = "2.0.0"

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	}
	var nanos int64
	if err := json.Unmarshal(data, &nanos); err != nil {
		return errors.New("duration must be a string such as 10s or an integer nanosecond value")
	}
	*d = Duration(nanos)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
func (d Duration) Time() time.Duration          { return time.Duration(d) }

type Config struct {
	Listen           string        `json:"listen"`
	Password         string        `json:"password"`
	HelloReadTimeout Duration      `json:"hello_read_timeout"`
	RecoveryTimeout  Duration      `json:"recovery_timeout"`
	Stream           StreamOptions `json:"stream"`
	UDP              UDPOptions    `json:"udp"`
}

type StreamOptions struct {
	SchedulerMode           string   `json:"scheduler_mode"`
	ActivationThresholdMbps uint32   `json:"activation_threshold_mbps"`
	ActivationWindow        Duration `json:"activation_window"`
	ChunkSize               int      `json:"chunk_size"`
	QueueFrames             int      `json:"queue_frames"`
	BandwidthMbps           []uint32 `json:"bandwidth_mbps"`
	MaxReorderFrames        int      `json:"max_reorder_frames"`
	MaxInflightFrames       int      `json:"max_inflight_frames"`
	AckInterval             Duration `json:"ack_interval"`
	RetransmitTimeout       Duration `json:"retransmit_timeout"`
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
		Listen:           "0.0.0.0:24444",
		HelloReadTimeout: Duration(10 * time.Second),
		RecoveryTimeout:  Duration(15 * time.Second),
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
		},
		UDP: UDPOptions{
			Mode:                       "adaptive",
			QueueFrames:                256,
			MaxDatagramSize:            16384,
			DedupWindow:                4096,
			IdleTimeout:                Duration(2 * time.Minute),
			AdaptiveQueueDelay:         Duration(120 * time.Millisecond),
			AdaptiveDuplicateThreshold: 0,
		},
	}
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
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
	if c.Listen == "" {
		return errors.New("invalid listen: address is empty")
	}
	host, portText, err := net.SplitHostPort(c.Listen)
	if err != nil || host == "" && !strings.HasPrefix(c.Listen, ":") {
		return fmt.Errorf("invalid listen %q: expected host:port", c.Listen)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("invalid listen port %q", portText)
	}
	if c.Password == "" {
		return errors.New("empty password")
	}
	if c.HelloReadTimeout.Time() <= 0 {
		c.HelloReadTimeout = Duration(10 * time.Second)
	}
	if c.RecoveryTimeout.Time() <= 0 {
		c.RecoveryTimeout = Duration(15 * time.Second)
	}
	if err := validateStream(&c.Stream); err != nil {
		return err
	}
	if c.UDP.Enabled {
		if err := validateUDP(&c.UDP); err != nil {
			return err
		}
	}
	return nil
}

func validateStream(c *StreamOptions) error {
	if c.SchedulerMode == "" {
		c.SchedulerMode = "adaptive"
	}
	if c.SchedulerMode != "adaptive" && c.SchedulerMode != "static" {
		return fmt.Errorf("invalid scheduler_mode %q", c.SchedulerMode)
	}
	if c.ActivationWindow.Time() <= 0 {
		c.ActivationWindow = Duration(time.Second)
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = 64 * 1024
	}
	if c.ChunkSize < 1024 || c.ChunkSize > 1<<20 {
		return errors.New("invalid chunk_size: must be between 1024 and 1048576")
	}
	if c.QueueFrames == 0 {
		c.QueueFrames = 256
	}
	if c.QueueFrames < 8 || c.QueueFrames > 4096 {
		return errors.New("invalid queue_frames: must be between 8 and 4096")
	}
	if len(c.BandwidthMbps) != 0 && len(c.BandwidthMbps) != 2 {
		return errors.New("invalid bandwidth_mbps: must be empty or contain two entries")
	}
	for _, bandwidth := range c.BandwidthMbps {
		if bandwidth == 0 {
			return errors.New("invalid bandwidth_mbps: entries must be positive")
		}
	}
	if c.MaxReorderFrames == 0 {
		c.MaxReorderFrames = 4096
	}
	if c.MaxReorderFrames < 1 || c.MaxReorderFrames > 1<<20 {
		return errors.New("invalid max_reorder_frames")
	}
	if c.MaxInflightFrames == 0 {
		c.MaxInflightFrames = 1024
	}
	if c.MaxInflightFrames < 1 || c.MaxInflightFrames > 1<<20 {
		return errors.New("invalid max_inflight_frames")
	}
	if c.AckInterval.Time() <= 0 {
		c.AckInterval = Duration(20 * time.Millisecond)
	}
	if c.RetransmitTimeout.Time() <= 0 {
		c.RetransmitTimeout = Duration(1500 * time.Millisecond)
	}
	if c.AckInterval.Time() <= 0 || c.RetransmitTimeout.Time() <= 0 {
		return errors.New("invalid stream timeout")
	}
	return nil
}

func validateUDP(c *UDPOptions) error {
	if c.Mode == "" {
		c.Mode = "adaptive"
	}
	if c.Mode != "adaptive" && c.Mode != "stripe" && c.Mode != "duplicate" {
		return fmt.Errorf("invalid udp mode %q", c.Mode)
	}
	if c.QueueFrames == 0 {
		c.QueueFrames = 256
	}
	if c.QueueFrames < 8 || c.QueueFrames > 4096 {
		return errors.New("invalid udp queue_frames: must be between 8 and 4096")
	}
	if c.MaxDatagramSize == 0 {
		c.MaxDatagramSize = 16384
	}
	if c.MaxDatagramSize < 512 || c.MaxDatagramSize > 16384 {
		return errors.New("invalid max_datagram_size: must be between 512 and 16384")
	}
	if c.DedupWindow == 0 {
		c.DedupWindow = 4096
	}
	if c.DedupWindow < 64 || c.DedupWindow > 1<<20 {
		return errors.New("invalid dedup_window")
	}
	if c.IdleTimeout.Time() <= 0 {
		c.IdleTimeout = Duration(2 * time.Minute)
	}
	if c.IdleTimeout.Time() < 5*time.Second || c.IdleTimeout.Time() > time.Hour {
		return errors.New("invalid idle_timeout: must be between 5s and 1h")
	}
	if c.AdaptiveQueueDelay.Time() <= 0 {
		c.AdaptiveQueueDelay = Duration(120 * time.Millisecond)
	}
	if c.AdaptiveQueueDelay.Time() < time.Millisecond || c.AdaptiveQueueDelay.Time() > 5*time.Second {
		return errors.New("invalid adaptive_queue_delay")
	}
	if c.AdaptiveDuplicateThreshold < 0 || c.AdaptiveDuplicateThreshold > c.MaxDatagramSize {
		return errors.New("invalid adaptive_duplicate_threshold")
	}
	return nil
}

package front

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

var Version = "2.7.0"

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	}
	var nanos int64
	if err := json.Unmarshal(data, &nanos); err != nil {
		return errors.New("duration must be a string such as 10s")
	}
	*d = Duration(nanos)
	return nil
}

func (d Duration) Time() time.Duration          { return time.Duration(d) }
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Config struct {
	Listen                  string   `json:"listen"`
	LandingTarget           string   `json:"landing_target"`
	AllowedSources          []string `json:"allowed_sources,omitempty"`
	ConnectTimeout          Duration `json:"connect_timeout"`
	IdleTimeout             Duration `json:"idle_timeout"`
	BufferBytes             int      `json:"buffer_bytes"`
	MaxConnections          int      `json:"max_connections"`
	GracefulShutdownTimeout Duration `json:"graceful_shutdown_timeout"`
	LogLevel                string   `json:"log_level"`
}

func DefaultConfig() Config {
	return Config{
		Listen: "127.0.0.1:8444", ConnectTimeout: Duration(10 * time.Second),
		IdleTimeout: Duration(10 * time.Minute), BufferBytes: 32 * 1024,
		MaxConnections: 512, GracefulShutdownTimeout: Duration(30 * time.Second), LogLevel: "info",
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
		return Config{}, err
	}
	cfg := DefaultConfig()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Config{}, errors.New("config must contain exactly one JSON value")
	}
	if err := cfg.NormalizeAndValidate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) NormalizeAndValidate() error {
	if err := validateAddress(c.Listen, "listen"); err != nil {
		return err
	}
	if err := validateAddress(c.LandingTarget, "landing_target"); err != nil {
		return err
	}
	listenHost, _, _ := net.SplitHostPort(c.Listen)
	if !isLoopbackHost(listenHost) && len(c.AllowedSources) == 0 {
		return errors.New("allowed_sources is required when Front listens outside loopback")
	}
	for _, source := range c.AllowedSources {
		if _, _, err := net.ParseCIDR(source); err != nil {
			return fmt.Errorf("invalid allowed_sources entry %q: expected CIDR", source)
		}
	}
	if c.ConnectTimeout.Time() <= 0 || c.ConnectTimeout.Time() > time.Minute {
		return errors.New("connect_timeout must be greater than 0 and at most 1m")
	}
	if c.IdleTimeout.Time() < 0 {
		return errors.New("idle_timeout must not be negative")
	}
	if c.BufferBytes < 4096 || c.BufferBytes > 1<<20 {
		return errors.New("buffer_bytes must be between 4096 and 1048576")
	}
	if c.MaxConnections < 1 || c.MaxConnections > 1<<20 {
		return errors.New("max_connections must be between 1 and 1048576")
	}
	if c.GracefulShutdownTimeout.Time() <= 0 || c.GracefulShutdownTimeout.Time() > 10*time.Minute {
		return errors.New("graceful_shutdown_timeout must be greater than 0 and at most 10m")
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	c.LogLevel = strings.ToLower(c.LogLevel)
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("invalid log_level %q", c.LogLevel)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateAddress(address, field string) error {
	host, portText, err := net.SplitHostPort(address)
	port, portErr := strconv.Atoi(portText)
	minimum := 1
	if field == "listen" {
		minimum = 0
	}
	if err != nil || host == "" || portErr != nil || port < minimum || port > 65535 {
		return fmt.Errorf("invalid %s %q: expected host:port", field, address)
	}
	return nil
}

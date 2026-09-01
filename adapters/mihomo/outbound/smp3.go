package outbound

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

type SMP3LegOption struct {
	Proxy string `proxy:"proxy"`
}

type SMP3UDPOption struct {
	Enabled                    bool   `proxy:"enabled,omitempty"`
	Mode                       string `proxy:"mode,omitempty"`
	MaxDatagramSize            int    `proxy:"max-datagram-size,omitempty"`
	IdleTimeout                string `proxy:"idle-timeout,omitempty"`
	AdaptiveDuplicateThreshold int    `proxy:"adaptive-duplicate-threshold,omitempty"`
}

// SMP3Option is the Mihomo stream/datagram schema. UDP is advertised only when
// the existing udp.enabled option is set.
type SMP3Option struct {
	BasicOption
	Name          string          `proxy:"name"`
	Server        string          `proxy:"server"`
	Port          int             `proxy:"port"`
	Password      string          `proxy:"password,omitempty"`
	Legs          []SMP3LegOption `proxy:"legs"`
	Leg1Fallback  string          `proxy:"leg1-fallback,omitempty"`
	SchedulerMode string          `proxy:"scheduler-mode,omitempty"`

	ActivationThresholdMbps uint64   `proxy:"activation-threshold-mbps,omitempty"`
	ActivationWindow        string   `proxy:"activation-window,omitempty"`
	ChunkSize               int      `proxy:"chunk-size,omitempty"`
	QueueFrames             int      `proxy:"queue-frames,omitempty"`
	BandwidthMbps           []uint32 `proxy:"bandwidth-mbps,omitempty"`
	MaxReorderFrames        int      `proxy:"max-reorder-frames,omitempty"`
	MaxInflightFrames       int      `proxy:"max-inflight-frames,omitempty"`
	AckInterval             string   `proxy:"ack-interval,omitempty"`
	RetransmitTimeout       string   `proxy:"retransmit-timeout,omitempty"`
	RecoveryTimeout         string   `proxy:"recovery-timeout,omitempty"`
	RedialInterval          string   `proxy:"redial-interval,omitempty"`

	UDP SMP3UDPOption `proxy:"udp,omitempty"`
}

// SMP3 is a stream/datagram adapter. Child proxies dial only reliable carrier
// streams to the aggregate endpoint; logical engines live in canonical Core.
type SMP3 struct {
	*Base
	option         SMP3Option
	streamConfig   smp3core.StreamConfig
	redialInterval time.Duration
	lookup         func(string) (C.Proxy, bool)
	sessions       sync.Map
	udpSessions    sync.Map
}

type smp3Session struct {
	owner       *SMP3
	engine      *smp3core.StreamEngine
	ctx         context.Context
	cancel      context.CancelFunc
	sessionID   smp3core.SessionID
	destination string

	mu      sync.Mutex
	joining [2]bool
}

func NewSMP3(option SMP3Option) (*SMP3, error) {
	if option.Name == "" {
		return nil, fmt.Errorf("smp3: missing name")
	}
	if option.Server == "" || option.Port <= 0 || option.Port > 65535 {
		return nil, fmt.Errorf("smp3: invalid server or port")
	}
	if option.Password == "" {
		return nil, fmt.Errorf("smp3: missing password")
	}
	if len(option.Legs) != 2 {
		return nil, fmt.Errorf("smp3: exactly two legs are required")
	}
	if option.Legs[0].Proxy == "" || option.Legs[1].Proxy == "" {
		return nil, fmt.Errorf("smp3: both leg proxy names are required")
	}
	if option.Legs[0].Proxy == option.Legs[1].Proxy {
		return nil, fmt.Errorf("smp3: leg proxy names must be different")
	}
	for _, leg := range option.Legs {
		if leg.Proxy == option.Name {
			return nil, fmt.Errorf("smp3: recursive child reference %q is unsupported", leg.Proxy)
		}
	}
	if option.Leg1Fallback == option.Name || option.Leg1Fallback == option.Legs[0].Proxy || option.Leg1Fallback == option.Legs[1].Proxy {
		return nil, fmt.Errorf("smp3: leg1 fallback must be a separate child proxy")
	}

	streamConfig, err := makeStreamConfig(option)
	if err != nil {
		return nil, err
	}
	redialInterval, err := parseSMP3Duration(option.RedialInterval, time.Second)
	if err != nil {
		return nil, fmt.Errorf("smp3: redial-interval: %w", err)
	}
	if redialInterval < 100*time.Millisecond || redialInterval > 30*time.Second {
		return nil, fmt.Errorf("smp3: redial-interval must be between 100ms and 30s")
	}

	return &SMP3{
		Base: NewBase(BaseOption{
			Name:         option.Name,
			Addr:         net.JoinHostPort(option.Server, strconv.Itoa(option.Port)),
			Type:         C.Relay,
			ProviderName: option.ProviderName,
			UDP:          option.UDP.Enabled,
			TFO:          option.TFO,
			MPTCP:        option.MPTCP,
			Interface:    option.Interface,
			RoutingMark:  option.RoutingMark,
			Prefer:       option.IPVersion,
		}),
		option:         option,
		streamConfig:   streamConfig,
		redialInterval: redialInterval,
	}, nil
}

func (s *SMP3) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if metadata == nil || !metadata.Valid() || metadata.DstPort == 0 {
		return nil, fmt.Errorf("smp3: invalid application destination")
	}
	destination := metadata.RemoteAddress()
	var sessionID smp3core.SessionID
	if _, err := rand.Read(sessionID[:]); err != nil {
		return nil, fmt.Errorf("smp3: generate session id: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	session := &smp3Session{
		owner:       s,
		ctx:         sessionCtx,
		cancel:      cancel,
		sessionID:   sessionID,
		destination: destination,
	}
	s.sessions.Store(session, struct{}{})
	defer func() {
		if session.engine == nil {
			s.sessions.Delete(session)
			cancel()
		}
	}()

	leg0, carrierName, err := session.dialCarrier(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("smp3: leg0 bootstrap: %w", err)
	}

	config := s.streamConfig
	config.OnActivate = func() { session.ensureLeg(1) }
	config.OnLegDown = func(id uint8, legErr error) { session.scheduleLeg(id, legErr) }
	engine, appConn := smp3core.NewStreamEngine(config)
	session.engine = engine
	go func() {
		<-engine.Done()
		cancel()
		s.sessions.Delete(session)
	}()
	if err := engine.AttachLeg(0, leg0, nil); err != nil {
		_ = leg0.Close()
		_ = engine.Close()
		return nil, fmt.Errorf("smp3: attach leg0: %w", err)
	}
	log.Infoln("[SMP3] %s session bootstrapped via leg 0 / %s to %s", s.Name(), carrierName, destination)
	if engine.Active() {
		session.ensureLeg(1)
	}
	return NewConn(appConn, s), nil
}

// ListenPacketContext delegates the production UDP lifecycle to the dual-leg
// DatagramEngine boundary.
func (s *SMP3) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return s.listenPacketContext(ctx, metadata)
}

func (s *SMP3) Close() error {
	s.sessions.Range(func(key, _ any) bool {
		session := key.(*smp3Session)
		session.cancel()
		if session.engine != nil {
			_ = session.engine.Close()
		}
		return true
	})
	s.udpSessions.Range(func(key, _ any) bool {
		_ = key.(*smp3UDPDualSession).Close()
		return true
	})
	return s.Base.Close()
}

func (s *SMP3) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"type": "smp3", "name": s.Name(), "addr": s.Addr()})
}

func (s *SMP3) child(name string) (C.Proxy, error) {
	lookup := s.lookup
	if lookup == nil {
		lookup = func(name string) (C.Proxy, bool) { proxy, ok := tunnel.Proxies()[name]; return proxy, ok }
	}
	proxy, ok := lookup(name)
	if !ok || proxy == nil {
		return nil, fmt.Errorf("smp3: child proxy not found: %s", name)
	}
	if proxy.Name() == s.Name() || isSMP3Proxy(proxy) {
		return nil, fmt.Errorf("smp3: recursive child proxy is unsupported: %s", name)
	}
	return proxy, nil
}

func (s *SMP3) endpointMetadata() (*C.Metadata, error) {
	metadata := &C.Metadata{NetWork: C.TCP}
	if err := metadata.SetRemoteAddress(net.JoinHostPort(s.option.Server, strconv.Itoa(s.option.Port))); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (ss *smp3Session) dialCarrier(ctx context.Context, id uint8) (C.Conn, string, error) {
	if id > 1 {
		return nil, "", fmt.Errorf("invalid leg id %d", id)
	}
	names := []string{ss.owner.option.Legs[id].Proxy}
	if id == 1 && ss.owner.option.Leg1Fallback != "" {
		names = append(names, ss.owner.option.Leg1Fallback)
	}
	endpoint, err := ss.owner.endpointMetadata()
	if err != nil {
		return nil, "", err
	}
	var causes []error
	for _, name := range names {
		child, childErr := ss.owner.child(name)
		if childErr != nil {
			causes = append(causes, childErr)
			continue
		}
		conn, dialErr := child.DialContext(ctx, endpoint)
		if dialErr != nil {
			causes = append(causes, fmt.Errorf("%s: %w", name, dialErr))
			continue
		}
		if helloErr := writeSMP3Hello(conn, ss.sessionID, smp3core.LegID(id), ss.destination, ss.owner.option.Password); helloErr != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s HELLO: %w", name, helloErr))
			continue
		}
		return conn, name, nil
	}
	return nil, "", errors.Join(causes...)
}

func (ss *smp3Session) ensureLeg(id uint8) {
	if id > 1 {
		return
	}
	ss.mu.Lock()
	if ss.engine == nil || ss.engine.HasLeg(smp3core.LegID(id)) || ss.joining[id] {
		ss.mu.Unlock()
		return
	}
	ss.joining[id] = true
	ss.mu.Unlock()
	go func() {
		defer func() {
			ss.mu.Lock()
			ss.joining[id] = false
			ss.mu.Unlock()
		}()
		for {
			select {
			case <-ss.ctx.Done():
				return
			default:
			}
			if ss.engine.HasLeg(smp3core.LegID(id)) {
				return
			}
			conn, carrierName, err := ss.dialCarrier(ss.ctx, id)
			if err == nil {
				if err = ss.engine.AttachLeg(smp3core.LegID(id), conn, nil); err == nil {
					log.Infoln("[SMP3] %s leg %d ready/rejoined via %s to %s", ss.owner.Name(), id, carrierName, ss.destination)
					return
				}
				_ = conn.Close()
			}
			timer := time.NewTimer(ss.owner.redialInterval)
			select {
			case <-ss.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (ss *smp3Session) scheduleLeg(id uint8, legErr error) {
	if id > 1 {
		return
	}
	log.Warnln("[SMP3] %s leg %d down for %s: %v", ss.owner.Name(), id, ss.destination, legErr)
	time.AfterFunc(ss.owner.redialInterval, func() { ss.ensureLeg(id) })
}

func writeSMP3Hello(conn net.Conn, sessionID smp3core.SessionID, legID smp3core.LegID, destination string, password string) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	header, dest, mac, err := smp3core.EncodeHelloParts(smp3core.Hello{
		Version:     smp3core.Version4,
		SessionID:   sessionID,
		LegID:       legID,
		Mode:        smp3core.ModeStream,
		Timestamp:   time.Now().Unix(),
		Nonce:       nonce,
		Destination: destination,
	}, []byte(password))
	if err != nil {
		return err
	}
	buffers := net.Buffers{header, dest, mac}
	_, err = buffers.WriteTo(conn)
	return err
}

func makeStreamConfig(option SMP3Option) (smp3core.StreamConfig, error) {
	activationWindow, err := parseSMP3Duration(option.ActivationWindow, time.Second)
	if err != nil {
		return smp3core.StreamConfig{}, fmt.Errorf("smp3: activation-window: %w", err)
	}
	ackInterval, err := parseSMP3Duration(option.AckInterval, 20*time.Millisecond)
	if err != nil {
		return smp3core.StreamConfig{}, fmt.Errorf("smp3: ack-interval: %w", err)
	}
	retransmitTimeout, err := parseSMP3Duration(option.RetransmitTimeout, 1500*time.Millisecond)
	if err != nil {
		return smp3core.StreamConfig{}, fmt.Errorf("smp3: retransmit-timeout: %w", err)
	}
	recoveryTimeout, err := parseSMP3Duration(option.RecoveryTimeout, 15*time.Second)
	if err != nil {
		return smp3core.StreamConfig{}, fmt.Errorf("smp3: recovery-timeout: %w", err)
	}
	mode := smp3core.StreamSchedulerAdaptive
	switch option.SchedulerMode {
	case "", "adaptive":
	case "static":
		mode = smp3core.StreamSchedulerStatic
	default:
		return smp3core.StreamConfig{}, fmt.Errorf("smp3: scheduler-mode must be adaptive or static")
	}
	threshold := option.ActivationThresholdMbps
	if threshold == 0 {
		threshold = 80
	}
	return smp3core.StreamConfig{
		SchedulerMode:     mode,
		ChunkSize:         option.ChunkSize,
		QueueFrames:       option.QueueFrames,
		ThresholdBytesPS:  threshold * 1000 * 1000 / 8,
		ActivationWindow:  activationWindow,
		BandwidthMbps:     append([]uint32(nil), option.BandwidthMbps...),
		MaxReorderFrames:  option.MaxReorderFrames,
		MaxInflightFrames: option.MaxInflightFrames,
		AckInterval:       ackInterval,
		RetransmitTimeout: retransmitTimeout,
		RecoveryTimeout:   recoveryTimeout,
	}, nil
}

func parseSMP3Duration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	return duration, nil
}

func isSMP3Proxy(proxy C.Proxy) bool {
	data, err := proxy.MarshalJSON()
	if err != nil {
		return false
	}
	var mapping map[string]any
	if json.Unmarshal(data, &mapping) != nil {
		return false
	}
	typ, _ := mapping["type"].(string)
	return strings.EqualFold(typ, "smp3")
}

// ValidateSMP3Config is called by the pinned Mihomo config parser before any
// proxy is constructed, so missing/duplicate/recursive child names fail at
// config load instead of silently falling back to DIRECT.
func ValidateSMP3Config(mapping map[string]any, available map[string]struct{}) error {
	typ, _ := mapping["type"].(string)
	if typ != "smp3" {
		return nil
	}
	name, _ := mapping["name"].(string)
	if name == "" {
		return fmt.Errorf("smp3: missing name")
	}
	names, err := smp3LegNames(mapping["legs"])
	if err != nil {
		return err
	}
	if len(names) != 2 {
		return fmt.Errorf("smp3 %s: exactly two legs are required", name)
	}
	if names[0] == "" || names[1] == "" {
		return fmt.Errorf("smp3 %s: both leg proxy names are required", name)
	}
	if names[0] == names[1] {
		return fmt.Errorf("smp3 %s: leg proxy names must be different", name)
	}
	for _, child := range names {
		if child == name {
			return fmt.Errorf("smp3 %s: recursive child reference %q is unsupported", name, child)
		}
		if _, ok := available[child]; !ok {
			return fmt.Errorf("smp3 %s: child proxy not found: %s", name, child)
		}
	}
	if fallback, _ := mapping["leg1-fallback"].(string); fallback != "" {
		if fallback == name || fallback == names[0] || fallback == names[1] {
			return fmt.Errorf("smp3 %s: leg1 fallback must be a separate child proxy", name)
		}
		if _, ok := available[fallback]; !ok {
			return fmt.Errorf("smp3 %s: fallback proxy not found: %s", name, fallback)
		}
	}
	return nil
}

func smp3LegNames(value any) ([]string, error) {
	var names []string
	switch legs := value.(type) {
	case []map[string]any:
		for _, leg := range legs {
			name, _ := leg["proxy"].(string)
			names = append(names, name)
		}
	case []any:
		for _, raw := range legs {
			leg, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("smp3: invalid leg entry")
			}
			name, _ := leg["proxy"].(string)
			names = append(names, name)
		}
	default:
		return nil, fmt.Errorf("smp3: legs must be a list")
	}
	return names, nil
}

var _ C.ProxyAdapter = (*SMP3)(nil)

package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

var (
	ErrReloadRequiresRestart = errors.New("configuration change requires a client restart")
	ErrReloadPartial         = errors.New("runtime reload partially applied; see result for compensation state")
)

type clientRuntimeConfig struct {
	cfg         Config
	carrierTLS  *tls.Config
	hostCarrier hostCarrierOpener
	version     uint64
}

// currentRuntime returns the immutable committed snapshot. Before the first
// reload it deliberately reads the legacy fields so existing embedders that
// construct a Client in tests can still install a HostCarrier before use.
func (c *Client) currentRuntime() *clientRuntimeConfig {
	if c == nil {
		return nil
	}
	if snapshot := c.runtime.Load(); snapshot != nil {
		return snapshot
	}
	return &clientRuntimeConfig{cfg: c.cfg, carrierTLS: c.carrierTLS, hostCarrier: c.hostCarrier}
}

func (s *streamSession) currentRuntime() *clientRuntimeConfig {
	if s == nil {
		return nil
	}
	if snapshot := s.runtime.Load(); snapshot != nil {
		return snapshot
	}
	if s.client != nil {
		return s.client.currentRuntime()
	}
	return nil
}

type RuntimeReloadOptions struct {
	// ForceRemove closes removed carriers immediately and lets Core recover
	// outstanding DATA through surviving legs. Graceful removal is the default.
	ForceRemove bool
	// DrainTimeout bounds every graceful per-session leg drain. A non-positive
	// value uses 10 seconds.
	DrainTimeout time.Duration
}

type RuntimeReloadResult struct {
	OperationID    string                     `json:"operation_id"`
	StartedAt      time.Time                  `json:"started_at"`
	FinishedAt     time.Time                  `json:"finished_at"`
	ConfigVersion  uint64                     `json:"config_version"`
	AddedLegIDs    []uint8                    `json:"added_leg_ids"`
	RemovedLegIDs  []uint8                    `json:"removed_leg_ids"`
	SessionCount   int                        `json:"session_count"`
	Committed      bool                       `json:"committed"`
	Partial        bool                       `json:"partial"`
	ForceRemove    bool                       `json:"force_remove"`
	AppliedActions []string                   `json:"applied_actions"`
	Compensations  []string                   `json:"compensations"`
	BeforeSessions []RuntimeSessionLegBinding `json:"before_sessions,omitempty"`
	AfterSessions  []RuntimeSessionLegBinding `json:"after_sessions,omitempty"`
	Events         []RuntimeReloadEvent       `json:"events,omitempty"`
	Error          string                     `json:"error,omitempty"`
}

type RuntimeSessionLegBinding struct {
	SessionID string              `json:"session_id"`
	Legs      []RuntimeLegBinding `json:"legs"`
}

type RuntimeLegBinding struct {
	LegID      uint8  `json:"leg_id"`
	Generation uint64 `json:"generation"`
}

type appliedRuntimeLeg struct {
	session *streamSession
	id      uint8
}

type RuntimeReloadEvent struct {
	At          time.Time `json:"at"`
	OperationID string    `json:"operation_id"`
	SessionID   string    `json:"session_id,omitempty"`
	LegID       uint8     `json:"leg_id,omitempty"`
	Generation  uint64    `json:"generation,omitempty"`
	Action      string    `json:"action"`
	Result      string    `json:"result"`
	Error       string    `json:"error,omitempty"`
}

func cloneRuntimeConfig(config Config, version uint64) (*clientRuntimeConfig, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("encode candidate config: %w", err)
	}
	copyConfig, err := LoadConfig(bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("validate candidate config: %w", err)
	}
	tlsConfig, err := newCarrierTLSConfig(copyConfig.SMP3.CarrierTLS)
	if err != nil {
		return nil, err
	}
	var host hostCarrierOpener
	if copyConfig.SMP3.CarrierMode == "host_bridge" {
		host = newHostCarrierOpener(copyConfig.SMP3.HostCarrier)
	}
	return &clientRuntimeConfig{cfg: copyConfig, carrierTLS: tlsConfig, hostCarrier: host, version: version}, nil
}

// ConfigReloadRequest is a local, one-shot trigger. It contains no config or
// credentials; the running process always reads its already-selected config
// path after consuming the marker.
type ConfigReloadRequest struct {
	RequestID   string    `json:"request_id"`
	ForceRemove bool      `json:"force_remove"`
	RequestedAt time.Time `json:"requested_at"`
}

func RequestConfigReload(configPath string, force bool) (ConfigReloadRequest, error) {
	if configPath == "" {
		return ConfigReloadRequest{}, errors.New("empty config path")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return ConfigReloadRequest{}, err
	}
	request := ConfigReloadRequest{RequestID: hex.EncodeToString(random[:]), ForceRemove: force, RequestedAt: time.Now().UTC()}
	data, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return ConfigReloadRequest{}, err
	}
	marker := configPath + ".reload.request"
	if _, err := os.Stat(marker); err == nil {
		return ConfigReloadRequest{}, errors.New("a reload request is already pending")
	} else if !errors.Is(err, os.ErrNotExist) {
		return ConfigReloadRequest{}, err
	}
	tmp := marker + ".tmp-" + request.RequestID
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return ConfigReloadRequest{}, err
	}
	if err := os.Rename(tmp, marker); err != nil {
		_ = os.Remove(tmp)
		return ConfigReloadRequest{}, err
	}
	return request, nil
}

// WatchConfigReloadMarker polls a local marker instead of watching config-file
// writes. Saving JSON alone never changes a running client. The request file is
// consumed atomically and a credential-free result is written beside it.
func (c *Client) WatchConfigReloadMarker(ctx context.Context, configPath string, interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			marker := configPath + ".reload.request"
			processing := marker + ".processing"
			if err := os.Rename(marker, processing); err != nil {
				continue
			}
			var request ConfigReloadRequest
			data, readErr := os.ReadFile(processing)
			if readErr == nil {
				readErr = json.Unmarshal(data, &request)
			}
			result := RuntimeReloadResult{OperationID: request.RequestID, StartedAt: time.Now().UTC(), ForceRemove: request.ForceRemove}
			var reloadErr error
			if readErr == nil && request.RequestID == "" {
				readErr = errors.New("reload marker has an empty request_id")
			}
			if readErr != nil {
				reloadErr = fmt.Errorf("invalid local reload marker: %w", readErr)
			} else {
				reloadCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				result, reloadErr = c.ReloadFromFile(reloadCtx, configPath, RuntimeReloadOptions{ForceRemove: request.ForceRemove})
				cancel()
				result.OperationID = request.RequestID
			}
			result.FinishedAt = time.Now().UTC()
			if reloadErr != nil {
				result.Error = reloadErr.Error()
			}
			out, _ := json.MarshalIndent(result, "", "  ")
			_ = os.WriteFile(configPath+".reload.result.json", out, 0600)
			_ = os.Remove(processing)
		}
	}()
}

func (c *Client) ReloadFromFile(ctx context.Context, configPath string, options RuntimeReloadOptions) (RuntimeReloadResult, error) {
	result := RuntimeReloadResult{StartedAt: time.Now().UTC(), ForceRemove: options.ForceRemove}
	candidate, err := LoadConfigFile(configPath)
	if err != nil {
		sessions := c.activeStreamSessions()
		result.SessionCount = len(sessions)
		result.BeforeSessions = snapshotRuntimeSessionBindings(sessions)
		result.AfterSessions = snapshotRuntimeSessionBindings(c.activeStreamSessions())
		result.FinishedAt = time.Now().UTC()
		result.Error = err.Error()
		return result, err
	}
	return c.Reload(ctx, candidate, options)
}

func newReloadOperationID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("reload-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func (c *Client) activeStreamSessions() []*streamSession {
	c.sessionsMu.RLock()
	result := make([]*streamSession, 0, len(c.sessions))
	for session := range c.sessions {
		result = append(result, session)
	}
	c.sessionsMu.RUnlock()
	return result
}

func (c *Client) registerStreamSession(session *streamSession) {
	if c == nil || session == nil {
		return
	}
	c.sessionsMu.Lock()
	if c.sessions == nil {
		c.sessions = make(map[*streamSession]struct{})
	}
	c.sessions[session] = struct{}{}
	c.sessionsMu.Unlock()
}

func (c *Client) unregisterStreamSession(session *streamSession) {
	if c == nil || session == nil {
		return
	}
	c.sessionsMu.Lock()
	delete(c.sessions, session)
	c.sessionsMu.Unlock()
}

// Reload applies only a tail append/removal of stream Legs plus display-name
// changes. Identity, listener, scheduler/recovery policy, TLS, UDP, upstream
// changes to existing Legs and max_active_legs require restart. The operation
// is staged per Session and compensated on failure; it is not a distributed
// atomic transaction across every live application Session.
func (c *Client) Reload(ctx context.Context, candidate Config, options RuntimeReloadOptions) (result RuntimeReloadResult, returnErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.DrainTimeout <= 0 {
		options.DrainTimeout = 10 * time.Second
	}
	c.reloadMu.Lock()
	defer c.reloadMu.Unlock()

	old := c.currentRuntime()
	result = RuntimeReloadResult{OperationID: newReloadOperationID(), StartedAt: time.Now().UTC(), ForceRemove: options.ForceRemove}
	defer func() {
		if result.FinishedAt.IsZero() {
			result.FinishedAt = time.Now().UTC()
		}
		if returnErr != nil && result.Error == "" {
			result.Error = returnErr.Error()
		}
		result.AfterSessions = snapshotRuntimeSessionBindings(c.activeStreamSessions())
	}()
	if c.isClosed() {
		return result, ErrClientClosed
	}
	sessions := c.activeStreamSessions()
	result.SessionCount = len(sessions)
	result.BeforeSessions = snapshotRuntimeSessionBindings(sessions)
	next, err := cloneRuntimeConfig(candidate, old.version+1)
	if err != nil {
		return result, err
	}
	added, removed, err := validateReloadDelta(old.cfg, next.cfg)
	if err != nil {
		return result, err
	}
	result.AddedLegIDs = append([]uint8(nil), added...)
	result.RemovedLegIDs = append([]uint8(nil), removed...)
	result.ConfigVersion = next.version

	addedLegs := make([]appliedRuntimeLeg, 0)
	for _, session := range sessions {
		if session.isClosed() {
			continue
		}
		if session.startupInFlight() {
			return result, errors.New("reload rejected while a stream session is completing initial carrier startup")
		}
	}
	for _, session := range sessions {
		if session.isClosed() {
			continue
		}
		for _, id := range added {
			session.setDesiredLeg(id, true)
			if err := session.addLegWithRuntime(ctx, id, next); err != nil {
				result.Error = fmt.Sprintf("add leg %d to session %x: %v", id, session.id, err)
				compensationFailed := false
				for _, applied := range addedLegs {
					applied.session.setDesiredLeg(applied.id, false)
					if removeErr := removeLegForced(ctx, applied.session, applied.id); removeErr != nil {
						compensationFailed = true
						result.Compensations = append(result.Compensations, fmt.Sprintf("failed to force-remove staged Leg %d from Session %x: %v", applied.id, applied.session.id, removeErr))
					} else {
						result.Compensations = append(result.Compensations, fmt.Sprintf("force-removed staged Leg %d from Session %x", applied.id, applied.session.id))
					}
				}
				session.setDesiredLeg(id, false)
				result.Partial = compensationFailed
				if compensationFailed {
					return result, fmt.Errorf("%w: %s", ErrReloadPartial, result.Error)
				}
				return result, errors.New(result.Error)
			}
			addedLegs = append(addedLegs, appliedRuntimeLeg{session: session, id: id})
			result.AppliedActions = append(result.AppliedActions, fmt.Sprintf("added Leg %d to Session %x generation %d", id, session.id, session.legGeneration(id)))
			result.Events = append(result.Events, RuntimeReloadEvent{At: time.Now().UTC(), OperationID: result.OperationID, SessionID: fmt.Sprintf("%x", session.id[:]), LegID: id, Generation: session.legGeneration(id), Action: "add", Result: "staged"})
		}
	}

	// Publish desired routes before removal callbacks can request automatic
	// repair. New SOCKS sessions are excluded by reloadMu until commit finishes.
	c.runtime.Store(next)
	for _, session := range sessions {
		if session.isClosed() {
			continue
		}
		session.runtime.Store(next)
		for _, id := range removed {
			session.setDesiredLeg(id, false)
		}
	}

	for _, session := range sessions {
		if session.isClosed() {
			continue
		}
		for _, id := range removed {
			generation := session.legGeneration(id)
			removeCtx, cancel := context.WithTimeout(ctx, options.DrainTimeout)
			removeErr := error(nil)
			if options.ForceRemove {
				removeErr = removeLegForced(removeCtx, session, id)
			} else {
				removeErr = session.engine.DrainLeg(removeCtx, smp3core.LegID(id))
			}
			cancel()
			if removeErr != nil {
				result.Error = fmt.Sprintf("remove leg %d from session %x: %v", id, session.id, removeErr)
				// Restore the prior desired route set and reconcile already removed
				// Legs. Any failure is surfaced as partial convergence.
				c.runtime.Store(old)
				compensationFailed := false
				for _, target := range sessions {
					if target.isClosed() {
						continue
					}
					target.runtime.Store(old)
					for _, removedID := range removed {
						target.setDesiredLeg(removedID, true)
						if !target.engine.HasLeg(smp3core.LegID(removedID)) {
							if err := target.addLegWithRuntime(ctx, removedID, old); err != nil {
								compensationFailed = true
								result.Compensations = append(result.Compensations, fmt.Sprintf("failed to restore Leg %d to Session %x: %v", removedID, target.id, err))
							} else {
								result.Compensations = append(result.Compensations, fmt.Sprintf("restored Leg %d to Session %x", removedID, target.id))
							}
						}
					}
				}
				for _, applied := range addedLegs {
					if applied.session.engine.HasLeg(smp3core.LegID(applied.id)) {
						applied.session.setDesiredLeg(applied.id, false)
						if err := removeLegForced(ctx, applied.session, applied.id); err != nil {
							compensationFailed = true
							result.Compensations = append(result.Compensations, fmt.Sprintf("failed to remove staged Leg %d from Session %x: %v", applied.id, applied.session.id, err))
						} else {
							result.Compensations = append(result.Compensations, fmt.Sprintf("removed staged Leg %d from Session %x", applied.id, applied.session.id))
						}
					}
				}
				result.Partial = compensationFailed
				result.FinishedAt = time.Now().UTC()
				if compensationFailed {
					return result, fmt.Errorf("%w: %s", ErrReloadPartial, result.Error)
				}
				return result, errors.New(result.Error)
			}
			session.setDesiredLeg(id, false)
			result.AppliedActions = append(result.AppliedActions, fmt.Sprintf("removed Leg %d from Session %x", id, session.id))
			result.Events = append(result.Events, RuntimeReloadEvent{At: time.Now().UTC(), OperationID: result.OperationID, SessionID: fmt.Sprintf("%x", session.id[:]), LegID: id, Generation: generation, Action: "remove", Result: "retired"})
		}
	}

	for _, session := range sessions {
		if session.isClosed() {
			continue
		}
		session.runtime.Store(next)
		for id := range next.cfg.SMP3.Routes.entries() {
			session.setDesiredLeg(uint8(id), true)
			if prior := bandwidthPrior(next.cfg, id); prior != 0 {
				session.engine.SetLegBandwidthPrior(smp3core.LegID(id), prior)
			}
			if !session.engine.HasLeg(smp3core.LegID(id)) {
				session.scheduleRepair(uint8(id))
			}
		}
	}
	result.Committed = true
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func snapshotRuntimeSessionBindings(sessions []*streamSession) []RuntimeSessionLegBinding {
	bindings := make([]RuntimeSessionLegBinding, 0, len(sessions))
	for _, session := range sessions {
		if session == nil || session.isClosed() || session.engine == nil {
			continue
		}
		binding := RuntimeSessionLegBinding{SessionID: fmt.Sprintf("%x", session.id[:]), Legs: make([]RuntimeLegBinding, 0)}
		for id := 0; id < smp3core.MaxStreamLegIDCount; id++ {
			if !session.engine.HasLeg(smp3core.LegID(id)) {
				continue
			}
			generation, ok := session.engine.LegGeneration(smp3core.LegID(id))
			if ok {
				binding.Legs = append(binding.Legs, RuntimeLegBinding{LegID: uint8(id), Generation: generation})
			}
		}
		bindings = append(bindings, binding)
	}
	return bindings
}

func removeLegForced(ctx context.Context, session *streamSession, id uint8) error {
	if session == nil || session.isClosed() {
		return nil
	}
	if !session.engine.HasLeg(smp3core.LegID(id)) {
		return nil
	}
	if !session.engine.ReplaceLeg(smp3core.LegID(id), errors.New("runtime reload forced leg removal")) {
		return nil
	}
	return session.engine.WaitLegRetired(ctx, smp3core.LegID(id))
}

func validateReloadDelta(old, next Config) (added, removed []uint8, err error) {
	oldRoutes := old.SMP3.Routes.entries()
	nextRoutes := next.SMP3.Routes.entries()
	if len(oldRoutes) == 0 || len(nextRoutes) == 0 {
		return nil, nil, errors.New("reload requires at least one active stream Leg")
	}
	if old.SMP3.Stream.MaxActiveLegs != next.SMP3.Stream.MaxActiveLegs {
		return nil, nil, fmt.Errorf("%w: max_active_legs is fixed when Core Sessions are created", ErrReloadRequiresRestart)
	}
	if len(nextRoutes) > next.SMP3.Stream.MaxActiveLegs {
		return nil, nil, fmt.Errorf("configured %d Legs exceeds max_active_legs %d", len(nextRoutes), next.SMP3.Stream.MaxActiveLegs)
	}
	common := len(oldRoutes)
	if len(nextRoutes) < common {
		common = len(nextRoutes)
	}
	for id := 0; id < common; id++ {
		if oldRoutes[id].Address != nextRoutes[id].Address || oldRoutes[id].Fallback != nextRoutes[id].Fallback {
			return nil, nil, fmt.Errorf("%w: endpoint change for existing Leg %d requires explicit carrier replacement/restart", ErrReloadRequiresRestart, id)
		}
		if !reflect.DeepEqual(old.effectiveUpstream(uint8(id)), next.effectiveUpstream(uint8(id))) {
			return nil, nil, fmt.Errorf("%w: upstream change for existing Leg %d is not hot-applied", ErrReloadRequiresRestart, id)
		}
		if old.SMP3.CarrierMode == "host_bridge" && next.SMP3.CarrierMode == "host_bridge" {
			if int(id) < len(old.SMP3.HostCarrier.LegHandles) && int(id) < len(next.SMP3.HostCarrier.LegHandles) && old.SMP3.HostCarrier.LegHandles[id] != next.SMP3.HostCarrier.LegHandles[id] {
				return nil, nil, fmt.Errorf("%w: host carrier handle change for existing Leg %d", ErrReloadRequiresRestart, id)
			}
		}
	}
	oldFixed, nextFixed := old, next
	oldFixed.SMP3.Routes, nextFixed.SMP3.Routes = RouteOptions{}, RouteOptions{}
	oldFixed.SMP3.Stream.BandwidthMbps, nextFixed.SMP3.Stream.BandwidthMbps = nil, nil
	oldFixed.UpstreamSocks.Leg0, nextFixed.UpstreamSocks.Leg0 = nil, nil
	oldFixed.UpstreamSocks.Leg1, nextFixed.UpstreamSocks.Leg1 = nil, nil
	oldFixed.UpstreamSocks.Legs, nextFixed.UpstreamSocks.Legs = nil, nil
	oldFixed.SMP3.HostCarrier.LegHandles, nextFixed.SMP3.HostCarrier.LegHandles = nil, nil
	if !reflect.DeepEqual(oldFixed, nextFixed) {
		return nil, nil, fmt.Errorf("%w: only route-count/name changes and per-Leg bandwidth prior changes are hot-applied", ErrReloadRequiresRestart)
	}
	if (old.SMP3.UDP.Enabled || next.SMP3.UDP.Enabled) && len(oldRoutes) != len(nextRoutes) {
		return nil, nil, fmt.Errorf("%w: route-count reload is not supported while UDP is enabled", ErrReloadRequiresRestart)
	}
	if len(nextRoutes) > len(oldRoutes) {
		for id := len(oldRoutes); id < len(nextRoutes); id++ {
			added = append(added, uint8(id))
		}
	} else if len(nextRoutes) < len(oldRoutes) {
		for id := len(oldRoutes) - 1; id >= len(nextRoutes); id-- {
			removed = append(removed, uint8(id))
		}
	}
	return added, removed, nil
}

func bandwidthPrior(config Config, id int) uint32 {
	if id >= 0 && id < len(config.SMP3.Stream.BandwidthMbps) {
		return config.SMP3.Stream.BandwidthMbps[id]
	}
	return 0
}

func (s *streamSession) addLegWithRuntime(ctx context.Context, id uint8, runtime *clientRuntimeConfig) error {
	if s == nil || s.isClosed() {
		return ErrClientClosed
	}
	if runtime == nil || int(id) >= len(runtime.cfg.SMP3.Routes.entries()) {
		return errors.New("Leg is not present in candidate runtime config")
	}
	if s.engine.HasLeg(smp3core.LegID(id)) {
		return nil
	}
	if err := s.engine.WaitLegRetired(ctx, smp3core.LegID(id)); err != nil {
		return fmt.Errorf("wait for prior Leg %d generation: %w", id, err)
	}
	conn, auth, err := s.dialLegWithRuntime(id, runtime)
	if err != nil {
		return err
	}
	prior := bandwidthPrior(runtime.cfg, int(id))
	if err := attachStreamLegWithPrior(s.engine, smp3core.LegID(id), conn, auth, prior); err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}

func (c *Client) ReloadResultPath(configPath string) string {
	return filepath.Clean(configPath + ".reload.result.json")
}

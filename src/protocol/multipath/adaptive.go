package multipath

import (
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

type leg1Carrier uint8

const (
	carrierHy2 leg1Carrier = iota
	carrierSnell
)

func (c leg1Carrier) String() string {
	if c == carrierSnell {
		return "snell"
	}
	return "hysteria2"
}

// adaptiveCarrierReplacementError preserves the carrier being intentionally
// retired. The adaptive state is switched before core.replaceLeg() runs, so
// reading currentCarrier() from the later OnLegDown callback would otherwise
// misattribute the old carrier's teardown to the newly-selected carrier.
type adaptiveCarrierReplacementError struct {
	from   leg1Carrier
	reason string
}

func (e *adaptiveCarrierReplacementError) Error() string {
	return "multipath adaptive carrier replacement: " + e.reason
}

type adaptiveState uint8

const (
	adaptiveHealthy adaptiveState = iota
	adaptiveSuspect
	adaptiveFallback
)

func (s adaptiveState) String() string {
	switch s {
	case adaptiveSuspect:
		return "suspect"
	case adaptiveFallback:
		return "fallback"
	default:
		return "healthy"
	}
}

type adaptiveSettings struct {
	EvaluationInterval      time.Duration
	Warmup                  time.Duration
	SuspectWindow           time.Duration
	HardFailureThreshold    uint32
	HardFailureWindow       time.Duration
	Cooldown                time.Duration
	MaxCooldown             time.Duration
	RecoveryStableWindow    time.Duration
	MinTxOutstanding        int
	TxAckStall              time.Duration
	MinRxReorder            int
	RxGapStall              time.Duration
	GoodputDegradeRatio     float64
	MinCanaryUsefulBytes    uint64
	MinCanaryActiveWindows  int
	InitialFailureThreshold uint32
	InitialFailureWindow    time.Duration
}

func defaultAdaptiveSettings() adaptiveSettings {
	return adaptiveSettings{
		EvaluationInterval:      time.Second,
		Warmup:                  5 * time.Second,
		SuspectWindow:           8 * time.Second,
		HardFailureThreshold:    2,
		HardFailureWindow:       15 * time.Second,
		Cooldown:                90 * time.Second,
		MaxCooldown:             5 * time.Minute,
		RecoveryStableWindow:    20 * time.Second,
		MinTxOutstanding:        64,
		TxAckStall:              2 * time.Second,
		MinRxReorder:            64,
		RxGapStall:              1500 * time.Millisecond,
		GoodputDegradeRatio:     0.40,
		MinCanaryUsefulBytes:    1 << 20,
		MinCanaryActiveWindows:  3,
		InitialFailureThreshold: 3,
		InitialFailureWindow:    30 * time.Second,
	}
}

type adaptiveDecision struct {
	State                adaptiveState
	StateChanged         bool
	Reason               string
	Fallback             bool
	Recovered            bool
	Demand               bool
	RxPressure           bool
	TxPressure           bool
	RxLogicalGoodputBPS  float64
	TxLogicalGoodputBPS  float64
	RxBaselineGoodputBPS float64
	TxBaselineGoodputBPS float64
	Leg1RxGoodputBPS     float64
	Leg1TxGoodputBPS     float64
	Leg1RxBaselineBPS    float64
	Leg1TxBaselineBPS    float64
	Leg1RxUsefulBytes    uint64
	Leg1TxUsefulBytes    uint64
	Leg1Contribution     float64
	SuspectFor           time.Duration
}

type adaptiveController struct {
	settings adaptiveSettings
	state    adaptiveState
	readyAt  time.Time

	initialized            bool
	lastSampleAt           time.Time
	lastRxDelivered        uint64
	lastTxAckedUsefulByLeg [2]uint64
	lastRxUniqueByLeg      [2]uint64
	rxBaselineGoodputBPS   float64
	txBaselineGoodputBPS   float64
	leg1RxBaselineBPS      float64
	leg1TxBaselineBPS      float64
	txFrontierLeg1Since    time.Time
	suspectSince           time.Time
}

type adaptiveSelection struct {
	carrier   leg1Carrier
	probation bool
}

type adaptiveConn struct {
	mu       sync.Mutex
	enabled  bool
	settings adaptiveSettings
	global   *hy2GlobalHealth
	core     *mpCore

	selected  bool
	carrier   leg1Carrier
	probation bool
	state     adaptiveState
	readyAt   time.Time
	ctrl      *adaptiveController
	running   bool

	hy2FailureTimes      []time.Time
	healthyActiveFrom    time.Time
	recoveryReported     bool
	activeSuccessSeen    bool
	canaryUsefulBytes    uint64
	canaryActiveWindows  int
	lastCanaryUsefulAt   time.Time
	activeUsefulBytes    uint64
	activeUsefulWindows  int
	lastActiveUsefulAt   time.Time
	legReadyRxUseful     uint64
	legReadyTxUseful     uint64
	ambiguousEOFFailures uint32

	onSelected         func(leg1Carrier, bool)
	onHealth           func(adaptiveDecision, coreStats)
	onFallback         func(reason string, cooldown bool, probation bool)
	onRecovery         func()
	onActiveSuccess    func()
	onProbationRelease func()
}

func newAdaptiveConn(enabled bool, settings adaptiveSettings, global *hy2GlobalHealth, core *mpCore) *adaptiveConn {
	return &adaptiveConn{
		enabled:  enabled,
		settings: settings,
		global:   global,
		core:     core,
		state:    adaptiveHealthy,
	}
}

func (s *adaptiveConn) carrierForLeg1(now time.Time) leg1Carrier {
	s.mu.Lock()
	if s.selected {
		carrier := s.carrier
		s.mu.Unlock()
		return carrier
	}
	selection := adaptiveSelection{carrier: carrierHy2}
	if s.enabled {
		selection = s.global.selectCarrier(now)
	}
	s.carrier = selection.carrier
	s.probation = selection.probation
	s.selected = true
	onSelected := s.onSelected
	s.mu.Unlock()
	if onSelected != nil && s.enabled {
		onSelected(selection.carrier, selection.probation)
	}
	return selection.carrier
}

func (s *adaptiveConn) currentCarrier() leg1Carrier {
	s.mu.Lock()
	carrier := s.carrier
	s.mu.Unlock()
	return carrier
}

func (s *adaptiveConn) completeProbationRecovery() bool {
	s.mu.Lock()
	if !s.enabled || s.carrier != carrierHy2 || !s.probation {
		s.mu.Unlock()
		return false
	}
	s.probation = false
	s.state = adaptiveHealthy
	s.recoveryReported = true
	onRecovery := s.onRecovery
	s.mu.Unlock()
	if onRecovery != nil {
		onRecovery()
	}
	return true
}

func (s *adaptiveConn) releaseUnusedProbation() {
	s.mu.Lock()
	if !s.enabled || s.carrier != carrierHy2 || !s.probation {
		s.mu.Unlock()
		return
	}
	s.probation = false
	onRelease := s.onProbationRelease
	s.mu.Unlock()
	if onRelease != nil {
		onRelease()
	} else if s.global != nil {
		s.global.releaseProbation()
	}
}

func ackProgressAge(stats coreStats) time.Duration {
	if stats.LastAckProgress.IsZero() {
		return stats.OldestOutstandingAge
	}
	age := time.Since(stats.LastAckProgress)
	if age < 0 {
		return 0
	}
	return age
}

func (s *adaptiveConn) markLegReady() {
	now := time.Now()
	var stats coreStats
	if s.core != nil {
		stats = s.core.snapshotStats()
	}
	s.mu.Lock()
	s.legReadyRxUseful = stats.RxUniqueBytesByLeg[1]
	s.legReadyTxUseful = stats.TxAckedUsefulByLeg[1]
	if !s.enabled || s.carrier != carrierHy2 || s.ctrl != nil {
		s.mu.Unlock()
		return
	}
	s.readyAt = now
	s.healthyActiveFrom = time.Time{}
	s.recoveryReported = false
	s.activeSuccessSeen = false
	s.canaryUsefulBytes = 0
	s.canaryActiveWindows = 0
	s.lastCanaryUsefulAt = time.Time{}
	s.activeUsefulBytes = 0
	s.activeUsefulWindows = 0
	s.lastActiveUsefulAt = time.Time{}
	s.ctrl = newAdaptiveController(s.settings, now)
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	core := s.core
	s.mu.Unlock()
	go s.healthLoop(core)
}

func isEOFLikeCarrierError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "eof")
}

// shouldRecordCarrierFailure distinguishes an actual Hy2 carrier failure from
// the common secondary-leg teardown race at the end of a short logical TCP
// session. SMP3 v4 has no explicit server-side JOIN acknowledgement, so a local
// writeHello/addLeg success only proves that the carrier accepted bytes; the
// server may legitimately close the secondary because the logical session
// finished just before that hello was processed.
//
// An EOF with zero useful leg1 contribution is therefore treated as ambiguous
// while leg0 is healthy and the logical stream is unpressured. The first such
// EOF does not poison global health. If useful demand persists, the health loop
// repairs leg1; repeated zero-useful EOFs on that same logical session are then
// strong evidence of a real carrier problem. Useful Hy2 contribution, logical
// pressure, or simultaneous leg0 loss make an EOF immediately actionable.
func (s *adaptiveConn) shouldRecordCarrierFailure(err error, stats coreStats, legWasReady bool) bool {
	if !isEOFLikeCarrierError(err) {
		return true
	}
	s.mu.Lock()
	settings := s.settings
	readyRx := s.legReadyRxUseful
	readyTx := s.legReadyTxUseful
	s.mu.Unlock()

	if legWasReady {
		leg1Useful := counterDelta(stats.RxUniqueBytesByLeg[1], readyRx) + counterDelta(stats.TxAckedUsefulByLeg[1], readyTx)
		if leg1Useful > 0 {
			return true
		}
	}
	if !stats.LegUp[0] {
		return true
	}
	rxPressure := stats.RxPendingFrames >= settings.MinRxReorder && stats.RxGapAge >= settings.RxGapStall
	txPressure := stats.OutstandingFrames >= settings.MinTxOutstanding && ackProgressAge(stats) >= settings.TxAckStall
	if rxPressure || txPressure {
		return true
	}
	return false
}

// noteAmbiguousCarrierEOF tracks zero-useful EOFs only within one logical
// session. A second occurrence is meaningful because ambiguous EOFs do not
// trigger immediate repair: another attempt only happens after the health loop
// observes continuing useful demand. This prevents a burst of unrelated short
// HTTP/TLS sessions from poisoning global Hy2 health while still detecting a
// carrier that repeatedly accepts and drops a live long-running session.
func (s *adaptiveConn) noteAmbiguousCarrierEOF() bool {
	s.mu.Lock()
	if !s.enabled || s.carrier != carrierHy2 {
		s.mu.Unlock()
		return false
	}
	s.ambiguousEOFFailures++
	count := s.ambiguousEOFFailures
	threshold := s.settings.HardFailureThreshold
	s.mu.Unlock()
	if threshold == 0 {
		threshold = 2
	}
	if count >= threshold {
		return s.switchToSnell("repeated_zero_useful_eof", true)
	}
	return false
}

func (s *adaptiveConn) recordCarrierFailure(established bool) bool {
	s.mu.Lock()
	if !s.enabled || s.carrier != carrierHy2 {
		s.mu.Unlock()
		return false
	}
	probation := s.probation
	now := time.Now()
	if !established || probation {
		s.mu.Unlock()
		if probation {
			return s.switchToSnell("probation_carrier_failure", true)
		}
		if s.global != nil {
			// The global manager owns the threshold transition. The callback only
			// switches this logical connection to Snell and must not double the
			// newly-created cooldown penalty.
			s.global.noteInitialFailure(now, s.settings)
		}
		return s.switchToSnell("initial_carrier_failure", false)
	}
	cutoff := now.Add(-s.settings.HardFailureWindow)
	kept := s.hy2FailureTimes[:0]
	for _, failureAt := range s.hy2FailureTimes {
		if !failureAt.Before(cutoff) {
			kept = append(kept, failureAt)
		}
	}
	s.hy2FailureTimes = append(kept, now)
	count := len(s.hy2FailureTimes)
	threshold := int(s.settings.HardFailureThreshold)
	s.mu.Unlock()
	if count >= threshold {
		return s.switchToSnell("repeated_carrier_failure", true)
	}
	return false
}

func (s *adaptiveConn) switchToSnell(reason string, cooldown bool) bool {
	s.mu.Lock()
	if !s.enabled || s.carrier != carrierHy2 {
		s.mu.Unlock()
		return false
	}
	probation := s.probation
	s.carrier = carrierSnell
	s.probation = false
	s.state = adaptiveFallback
	s.ctrl = nil
	s.ambiguousEOFFailures = 0
	s.healthyActiveFrom = time.Time{}
	onFallback := s.onFallback
	s.mu.Unlock()
	if onFallback != nil {
		onFallback(reason, cooldown, probation)
	}
	return true
}

// continuityGrace allows a couple of empty sampling windows without declaring
// a healthy carrier inactive. Longer idle gaps must not count toward a
// continuous recovery/active-success window.
func (s *adaptiveConn) continuityGrace() time.Duration {
	grace := 3 * s.settings.EvaluationInterval
	if grace < 2*time.Second {
		grace = 2 * time.Second
	}
	return grace
}

// observeCanary records one health window for a probation connection. It is
// intentionally separate from dial/handshake success: recovery requires real
// leg1 useful bytes, not merely logical demand carried by leg0. Idle gaps longer
// than continuityGrace reset the stable window so wall-clock idle time cannot
// make a weak/unused Hy2 canary look recovered.
func (s *adaptiveConn) observeCanary(now time.Time, decision adaptiveDecision) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.probation || s.recoveryReported {
		return false
	}
	useful := decision.Leg1RxUsefulBytes + decision.Leg1TxUsefulBytes
	healthy := decision.State == adaptiveHealthy && !decision.RxPressure && !decision.TxPressure
	if healthy && useful > 0 {
		if !s.lastCanaryUsefulAt.IsZero() && now.Sub(s.lastCanaryUsefulAt) > s.continuityGrace() {
			s.healthyActiveFrom = time.Time{}
			s.canaryUsefulBytes = 0
			s.canaryActiveWindows = 0
		}
		if s.healthyActiveFrom.IsZero() {
			s.healthyActiveFrom = now
		}
		s.lastCanaryUsefulAt = now
		s.canaryUsefulBytes += useful
		s.canaryActiveWindows++
	} else if !healthy {
		s.healthyActiveFrom = time.Time{}
		s.lastCanaryUsefulAt = time.Time{}
		s.canaryUsefulBytes = 0
		s.canaryActiveWindows = 0
	} else if !s.lastCanaryUsefulAt.IsZero() && now.Sub(s.lastCanaryUsefulAt) > s.continuityGrace() {
		s.healthyActiveFrom = time.Time{}
		s.lastCanaryUsefulAt = time.Time{}
		s.canaryUsefulBytes = 0
		s.canaryActiveWindows = 0
	}
	if !s.healthyActiveFrom.IsZero() &&
		now.Sub(s.healthyActiveFrom) >= s.settings.RecoveryStableWindow &&
		s.canaryUsefulBytes >= s.settings.MinCanaryUsefulBytes &&
		s.canaryActiveWindows >= s.settings.MinCanaryActiveWindows {
		s.recoveryReported = true
		s.probation = false
		return true
	}
	return false
}

func (s *adaptiveConn) observeActiveSuccess(now time.Time, readyAt time.Time, decision adaptiveDecision) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.probation || s.activeSuccessSeen || s.carrier != carrierHy2 {
		return false
	}
	useful := decision.Leg1RxUsefulBytes + decision.Leg1TxUsefulBytes
	healthy := decision.State == adaptiveHealthy && !decision.RxPressure && !decision.TxPressure && now.Sub(readyAt) >= s.settings.Warmup
	if healthy && useful > 0 {
		if !s.lastActiveUsefulAt.IsZero() && now.Sub(s.lastActiveUsefulAt) > s.continuityGrace() {
			s.healthyActiveFrom = time.Time{}
			s.activeUsefulBytes = 0
			s.activeUsefulWindows = 0
		}
		if s.healthyActiveFrom.IsZero() {
			s.healthyActiveFrom = now
		}
		s.lastActiveUsefulAt = now
		s.activeUsefulBytes += useful
		s.activeUsefulWindows++
	} else if !healthy {
		s.healthyActiveFrom = time.Time{}
		s.lastActiveUsefulAt = time.Time{}
		s.activeUsefulBytes = 0
		s.activeUsefulWindows = 0
	} else if !s.lastActiveUsefulAt.IsZero() && now.Sub(s.lastActiveUsefulAt) > s.continuityGrace() {
		s.healthyActiveFrom = time.Time{}
		s.lastActiveUsefulAt = time.Time{}
		s.activeUsefulBytes = 0
		s.activeUsefulWindows = 0
	}
	if !s.healthyActiveFrom.IsZero() &&
		now.Sub(s.healthyActiveFrom) >= s.settings.RecoveryStableWindow &&
		s.activeUsefulBytes >= s.settings.MinCanaryUsefulBytes &&
		s.activeUsefulWindows >= s.settings.MinCanaryActiveWindows {
		s.activeSuccessSeen = true
		return true
	}
	return false
}

func (s *adaptiveConn) finishProbation(core *mpCore) {
	s.mu.Lock()
	probation := s.probation && !s.recoveryReported && s.carrier == carrierHy2
	onRelease := s.onProbationRelease
	s.mu.Unlock()
	if !probation || s.global == nil {
		return
	}
	if core.closing.Load() || core.finalizing.Load() {
		s.global.releaseProbation()
		return
	}
	// An abnormal logical-core death before useful canary traffic is a carrier
	// failure. Ordinary logical close is marked closing and only releases owner.
	s.global.noteFallback(time.Now(), s.settings, true)
	if onRelease != nil {
		onRelease()
	}
}

func (s *adaptiveConn) healthLoop(core *mpCore) {
	interval := s.settings.EvaluationInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer s.finishProbation(core)
	for {
		select {
		case <-core.Done():
			return
		case now := <-ticker.C:
			if core.closing.Load() || core.finalizing.Load() {
				return
			}
			stats := core.snapshotStats()
			s.mu.Lock()
			if s.carrier != carrierHy2 || s.ctrl == nil {
				s.mu.Unlock()
				return
			}
			decision := s.ctrl.observe(now, stats)
			readyAt := s.readyAt
			onHealth := s.onHealth
			probation := s.probation
			onRecovery := s.onRecovery
			onActiveSuccess := s.onActiveSuccess
			s.mu.Unlock()

			canaryRecovered := false
			if probation {
				canaryRecovered = s.observeCanary(now, decision)
			} else if !s.observeActiveSuccess(now, readyAt, decision) {
				onActiveSuccess = nil
			}
			if canaryRecovered && onRecovery != nil {
				onRecovery()
			}
			if onActiveSuccess != nil {
				onActiveSuccess()
			}
			if onHealth != nil {
				onHealth(decision, stats)
			}
			if decision.Fallback && !core.closing.Load() && !core.finalizing.Load() {
				if s.switchToSnell(decision.Reason, true) {
					return
				}
			}
		}
	}
}

type hy2HealthState uint8

const (
	hy2Available hy2HealthState = iota
	hy2Cooldown
	hy2Probation
)

type hy2GlobalHealth struct {
	mu                sync.Mutex
	state             hy2HealthState
	cooldownUntil     time.Time
	cooldownPenalty   time.Duration
	probationInFlight bool
	initialFailures   []time.Time
}

func (h *hy2GlobalHealth) selectCarrier(now time.Time) adaptiveSelection {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == hy2Cooldown && now.Before(h.cooldownUntil) {
		return adaptiveSelection{carrier: carrierSnell}
	}
	if h.state == hy2Cooldown {
		h.state = hy2Probation
	}
	if h.state == hy2Probation {
		if h.probationInFlight {
			return adaptiveSelection{carrier: carrierSnell}
		}
		h.probationInFlight = true
		return adaptiveSelection{carrier: carrierHy2, probation: true}
	}
	return adaptiveSelection{carrier: carrierHy2}
}

func (h *hy2GlobalHealth) noteInitialFailure(now time.Time, settings adaptiveSettings) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Once one failure burst has already opened a cooldown, late failures from
	// concurrently in-flight dials belong to the same event. They must not
	// repeatedly double the backoff. A later probation failure is what advances
	// the exponential penalty.
	if h.state == hy2Cooldown {
		return true
	}
	cutoff := now.Add(-settings.InitialFailureWindow)
	kept := h.initialFailures[:0]
	for _, failureAt := range h.initialFailures {
		if !failureAt.Before(cutoff) {
			kept = append(kept, failureAt)
		}
	}
	h.initialFailures = append(kept, now)
	if uint32(len(h.initialFailures)) < settings.InitialFailureThreshold {
		return false
	}
	// Consume this burst before entering cooldown so residual in-flight failures
	// cannot be counted again after the threshold transition.
	h.initialFailures = nil
	h.noteFallbackLocked(now, settings)
	return true
}

func (h *hy2GlobalHealth) noteActiveSuccess() {
	h.mu.Lock()
	h.initialFailures = nil
	h.mu.Unlock()
}

func (h *hy2GlobalHealth) noteFallback(now time.Time, settings adaptiveSettings, probation bool) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Multiple logical connections can observe the same physical Hy2 outage at
	// nearly the same time. Once the first one opens a live cooldown, residual
	// failures from that burst must share the existing penalty instead of
	// multiplying 90s -> 180s -> 300s. Only a later probation failure is allowed
	// to advance the exponential backoff.
	if h.state == hy2Cooldown && now.Before(h.cooldownUntil) && !probation {
		return h.cooldownPenalty
	}
	return h.noteFallbackLocked(now, settings)
}

func (h *hy2GlobalHealth) noteFallbackLocked(now time.Time, settings adaptiveSettings) time.Duration {
	penalty := settings.Cooldown
	if h.cooldownPenalty > 0 {
		penalty = h.cooldownPenalty * 2
	}
	if penalty > settings.MaxCooldown {
		penalty = settings.MaxCooldown
	}
	h.cooldownPenalty = penalty
	h.cooldownUntil = now.Add(penalty)
	h.state = hy2Cooldown
	h.probationInFlight = false
	return penalty
}

func (h *hy2GlobalHealth) noteRecovery() {
	h.mu.Lock()
	h.state = hy2Available
	h.cooldownUntil = time.Time{}
	h.cooldownPenalty = 0
	h.probationInFlight = false
	h.initialFailures = nil
	h.mu.Unlock()
}

func (h *hy2GlobalHealth) releaseProbation() {
	h.mu.Lock()
	if h.state == hy2Probation {
		h.probationInFlight = false
	}
	h.mu.Unlock()
}

func (h *hy2GlobalHealth) cooldownActive(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state == hy2Cooldown && now.Before(h.cooldownUntil)
}

func newAdaptiveController(settings adaptiveSettings, readyAt time.Time) *adaptiveController {
	return &adaptiveController{settings: settings, state: adaptiveHealthy, readyAt: readyAt}
}

func (c *adaptiveController) stateValue() adaptiveState { return c.state }
func (c *adaptiveController) baseline() float64         { return c.rxBaselineGoodputBPS }

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func updateGoodputBaseline(baseline, current float64) float64 {
	if current <= 0 {
		return baseline
	}
	if baseline == 0 {
		return current
	}
	// Capacity rises quickly, but decays slowly. Baselines are only updated in
	// HEALTHY with directional demand; SUSPECT and idle windows freeze them.
	alpha := 0.03
	if current > baseline {
		alpha = 0.20
	}
	return baseline*(1-alpha) + current*alpha
}

func (c *adaptiveController) observe(now time.Time, stats coreStats) adaptiveDecision {
	decision := adaptiveDecision{
		State:                c.state,
		RxBaselineGoodputBPS: c.rxBaselineGoodputBPS,
		TxBaselineGoodputBPS: c.txBaselineGoodputBPS,
		Leg1RxBaselineBPS:    c.leg1RxBaselineBPS,
		Leg1TxBaselineBPS:    c.leg1TxBaselineBPS,
	}
	if !c.initialized {
		c.initialized = true
		c.lastSampleAt = now
		c.lastRxDelivered = stats.RxDeliveredBytes
		c.lastTxAckedUsefulByLeg = stats.TxAckedUsefulByLeg
		c.lastRxUniqueByLeg = stats.RxUniqueBytesByLeg
		return decision
	}
	elapsed := now.Sub(c.lastSampleAt)
	if elapsed <= 0 {
		return decision
	}
	c.lastSampleAt = now
	rxDelta := counterDelta(stats.RxDeliveredBytes, c.lastRxDelivered)
	txDeltaByLeg := [2]uint64{
		counterDelta(stats.TxAckedUsefulByLeg[0], c.lastTxAckedUsefulByLeg[0]),
		counterDelta(stats.TxAckedUsefulByLeg[1], c.lastTxAckedUsefulByLeg[1]),
	}
	rxDeltaByLeg := [2]uint64{
		counterDelta(stats.RxUniqueBytesByLeg[0], c.lastRxUniqueByLeg[0]),
		counterDelta(stats.RxUniqueBytesByLeg[1], c.lastRxUniqueByLeg[1]),
	}
	c.lastRxDelivered = stats.RxDeliveredBytes
	c.lastTxAckedUsefulByLeg = stats.TxAckedUsefulByLeg
	c.lastRxUniqueByLeg = stats.RxUniqueBytesByLeg
	txDelta := txDeltaByLeg[0] + txDeltaByLeg[1]
	rxGoodput := float64(rxDelta) / elapsed.Seconds()
	txGoodput := float64(txDelta) / elapsed.Seconds()
	leg1RxGoodput := float64(rxDeltaByLeg[1]) / elapsed.Seconds()
	leg1TxGoodput := float64(txDeltaByLeg[1]) / elapsed.Seconds()
	decision.RxLogicalGoodputBPS = rxGoodput
	decision.TxLogicalGoodputBPS = txGoodput
	decision.Leg1RxGoodputBPS = leg1RxGoodput
	decision.Leg1TxGoodputBPS = leg1TxGoodput
	decision.Leg1RxUsefulBytes = rxDeltaByLeg[1]
	decision.Leg1TxUsefulBytes = txDeltaByLeg[1]
	if rxDelta+txDelta > 0 {
		decision.Leg1Contribution = float64(rxDeltaByLeg[1]+txDeltaByLeg[1]) / float64(rxDelta+txDelta)
	}
	rxDemand := rxDelta > 0 || stats.RxPendingFrames > 0
	txDemand := txDelta > 0 || stats.OutstandingFrames > 0
	decision.Demand = rxDemand || txDemand

	if c.state == adaptiveFallback || now.Sub(c.readyAt) < c.settings.Warmup {
		return decision
	}

	ackProgressAge := time.Duration(0)
	if !stats.LastAckProgress.IsZero() {
		ackProgressAge = now.Sub(stats.LastAckProgress)
		if ackProgressAge < 0 {
			ackProgressAge = 0
		}
	}
	rxPressure := stats.RxPendingFrames >= c.settings.MinRxReorder && stats.RxGapAge >= c.settings.RxGapStall
	// SMP3 v4 has one cumulative ACK frontier. R9 requires exclusive, stable
	// leg1 ownership before blaming Hy2: a rescue can temporarily put the same seq
	// on both carriers, and a single sampling instant can flip ownership while ACK
	// progress is otherwise recovering. Neither case is evidence of a Hy2 fault.
	frontierLeg1Exclusive := stats.AckFrontierValid && !stats.AckFrontierMultiPath && stats.AckFrontierLeg == 1
	if frontierLeg1Exclusive {
		if c.txFrontierLeg1Since.IsZero() {
			c.txFrontierLeg1Since = now
		}
	} else {
		c.txFrontierLeg1Since = time.Time{}
	}
	frontierLeg1StableFor := time.Duration(0)
	if !c.txFrontierLeg1Since.IsZero() {
		frontierLeg1StableFor = now.Sub(c.txFrontierLeg1Since)
		if frontierLeg1StableFor < 0 {
			frontierLeg1StableFor = 0
		}
	}
	txPressure := stats.OutstandingFrames >= c.settings.MinTxOutstanding &&
		stats.OutstandingFramesByLeg[1] >= c.settings.MinTxOutstanding &&
		frontierLeg1Exclusive && frontierLeg1StableFor >= c.settings.TxAckStall &&
		ackProgressAge >= c.settings.TxAckStall &&
		stats.AckFrontierAge >= c.settings.TxAckStall
	decision.RxPressure = rxPressure
	decision.TxPressure = txPressure

	rxLogicalDegraded := c.rxBaselineGoodputBPS > 0 && rxGoodput < c.rxBaselineGoodputBPS*c.settings.GoodputDegradeRatio
	txLogicalDegraded := c.txBaselineGoodputBPS > 0 && txGoodput < c.txBaselineGoodputBPS*c.settings.GoodputDegradeRatio
	leg1RxDegraded := c.leg1RxBaselineBPS > 0 && leg1RxGoodput < c.leg1RxBaselineBPS*c.settings.GoodputDegradeRatio
	leg1TxDegraded := c.leg1TxBaselineBPS > 0 && leg1TxGoodput < c.leg1TxBaselineBPS*c.settings.GoodputDegradeRatio

	// A carrier can be unusable from its first active sample, before a healthy
	// leg1 baseline exists. Do not let that state remain HEALTHY forever. The
	// no-baseline path is intentionally conservative: it only recognizes a
	// near-black-hole where logical useful progress is zero, leg1 contributes
	// nothing, pressure is at least twice the ordinary threshold, and the same
	// gap/ACK stall has already persisted for a long time. It still enters
	// SUSPECT first and must survive SuspectWindow before fallback.
	severeStall := 5 * time.Second
	if candidate := 2 * c.settings.RxGapStall; candidate > severeStall {
		severeStall = candidate
	}
	if candidate := 2 * c.settings.TxAckStall; candidate > severeStall {
		severeStall = candidate
	}
	rxNoBaselineSevere := c.leg1RxBaselineBPS == 0 &&
		rxGoodput == 0 && leg1RxGoodput == 0 &&
		stats.RxPendingFrames >= 2*c.settings.MinRxReorder &&
		stats.RxGapAge >= severeStall
	txNoBaselineSevere := c.leg1TxBaselineBPS == 0 &&
		txGoodput == 0 && leg1TxGoodput == 0 &&
		stats.OutstandingFrames >= 2*c.settings.MinTxOutstanding &&
		stats.OutstandingFramesByLeg[1] >= c.settings.MinTxOutstanding &&
		frontierLeg1Exclusive && frontierLeg1StableFor >= severeStall &&
		ackProgressAge >= severeStall && stats.AckFrontierAge >= severeStall

	rxImpact := rxDemand && rxPressure && ((leg1RxDegraded && rxLogicalDegraded) || rxNoBaselineSevere)
	txImpact := txDemand && txPressure && ((leg1TxDegraded && txLogicalDegraded) || txNoBaselineSevere)
	impact := rxImpact || txImpact
	decision.Reason = pressureReason(rxImpact, txImpact)

	if c.state == adaptiveHealthy {
		if impact {
			c.state = adaptiveSuspect
			c.suspectSince = now
			decision.State = c.state
			decision.StateChanged = true
			return decision
		}
		if rxDemand {
			c.rxBaselineGoodputBPS = updateGoodputBaseline(c.rxBaselineGoodputBPS, rxGoodput)
			if rxDeltaByLeg[1] > 0 {
				c.leg1RxBaselineBPS = updateGoodputBaseline(c.leg1RxBaselineBPS, leg1RxGoodput)
			}
		}
		if txDemand {
			c.txBaselineGoodputBPS = updateGoodputBaseline(c.txBaselineGoodputBPS, txGoodput)
			if txDeltaByLeg[1] > 0 {
				c.leg1TxBaselineBPS = updateGoodputBaseline(c.leg1TxBaselineBPS, leg1TxGoodput)
			}
		}
		decision.RxBaselineGoodputBPS = c.rxBaselineGoodputBPS
		decision.TxBaselineGoodputBPS = c.txBaselineGoodputBPS
		decision.Leg1RxBaselineBPS = c.leg1RxBaselineBPS
		decision.Leg1TxBaselineBPS = c.leg1TxBaselineBPS
		return decision
	}

	decision.SuspectFor = now.Sub(c.suspectSince)
	if !impact {
		c.state = adaptiveHealthy
		c.suspectSince = time.Time{}
		decision.State = c.state
		decision.StateChanged = true
		decision.Recovered = true
		return decision
	}
	if decision.Demand && decision.SuspectFor >= c.settings.SuspectWindow {
		c.state = adaptiveFallback
		decision.State = c.state
		decision.StateChanged = true
		decision.Fallback = true
	}
	return decision
}

func pressureReason(rxPressure, txPressure bool) string {
	if rxPressure {
		return "rx_reorder"
	}
	if txPressure {
		return "tx_ack_stall"
	}
	return ""
}

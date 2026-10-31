package smp3core

import (
	"math"
	"sync"
	"time"
)

const streamProbeBudget = 200 * time.Millisecond

// Cumulative ACK can wait behind the healthy carrier's bounded ordinary
// pipeline. A proof may arrive later than the frontier rescue budget, but is
// accepted only if no rescue attempt was claimed for that record.
const streamProofBudget = streamProbeBudget + 500*time.Millisecond
const streamRestrictionCooldown = 5 * time.Second
const streamOrdinaryMaxBytes = 16 << 20

// This is a directional, per-session safety limit, independent of a shared
// physical capacity estimate. No carrier is closed or redialed by this policy.
type streamAdmissionLeg struct {
	restricted               bool
	recovering               bool
	probePending             bool
	probeSequence            uint64
	probeSequenceSet         bool
	probeExpired             bool
	probeExpiredSequence     uint64
	probePendingAt           time.Time
	since, nextProbe         time.Time
	limit, proofs            int
	proofAfter               uint64
	byteLimit                uint64
	proofBytes               uint64
	feedback                 time.Duration
	feedbackAt               time.Time
	feedbackMax              time.Duration
	feedbackMaxAt            time.Time
	probeWindow              time.Time
	probeBytes               uint64
	probeRate                float64
	serviceWindow, serviceAt time.Time
	serviceBytes             uint64
	serviceRate              float64
	openingRate              float64 // probation hypothesis from this session alternate, not a measurement
	seedMbps                 float64
	reason                   string
	recoveryEpoch            uint64 // monotonically increments on each new restricted cycle
	suspectSince, suspectAt  time.Time
}
type streamAdmission struct {
	mu   sync.Mutex
	legs []streamAdmissionLeg
}

func (c *StreamEngine) validAdmissionLeg(id uint8) bool {
	return c != nil && int(id) < len(c.admission.legs) && int(id) < len(c.legGeneration)
}

func (c *StreamEngine) admissionLegIDs() []uint8 {
	legs := c.availableLegs()
	ids := make([]uint8, 0, len(legs))
	for _, leg := range legs {
		if leg != nil && !leg.closed.Load() && c.validAdmissionLeg(leg.id) {
			ids = append(ids, leg.id)
		}
	}
	return ids
}

// alternateIDLocked selects the best currently usable companion from the
// active Leg set. For two legs this is exactly the historical 1-id mapping;
// for N legs it prefers a normal Leg and then the highest known session rate.
// admission.mu must be held.
func (c *StreamEngine) alternateIDLocked(id uint8, active []uint8) (uint8, bool) {
	var best uint8
	bestHealthy := false
	bestRate := -1.0
	found := false
	for _, candidate := range active {
		if candidate == id || !c.validAdmissionLeg(candidate) {
			continue
		}
		state := c.admission.legs[candidate]
		healthy := !state.restricted && !state.recovering
		rate := state.serviceRate
		if state.openingRate > rate {
			rate = state.openingRate
		}
		if rate <= 0 {
			rate = float64(c.weightFor(candidate)) * 1e6 / 8
		}
		if !found || healthy && !bestHealthy || healthy == bestHealthy && rate > bestRate || healthy == bestHealthy && rate == bestRate && candidate < best {
			best, bestHealthy, bestRate, found = candidate, healthy, rate, true
		}
	}
	return best, found
}

func (c *StreamEngine) slowLegProtection() bool {
	return c.txLedger != nil && c.cfg.SchedulerMode == StreamSchedulerAggregation && c.cfg.CapacityMode == StreamCapacityDynamic
}

func (c *StreamEngine) ordinaryBudget(id uint8) int {
	weight := float64(c.weightFor(id))
	if c.capacityProvider != nil {
		weight = c.capacityProvider.Weight(id, weight)
	}
	return c.frameBudget(weight)
}

func (c *StreamEngine) ordinaryByteBudget(id uint8) uint64 {
	weight := float64(c.weightFor(id))
	if c.capacityProvider != nil {
		weight = c.capacityProvider.Weight(id, weight)
	}
	active := c.admissionLegIDs()
	c.admission.mu.Lock()
	self := c.admission.legs[id]
	var alternate streamAdmissionLeg
	if peer, ok := c.alternateIDLocked(id, active); ok {
		alternate = c.admission.legs[peer]
	}
	service := self.serviceRate
	sampled := self.serviceAt
	if service == 0 {
		service = self.openingRate
		sampled = self.since
	}
	if sampled.IsZero() {
		sampled = self.since
	}
	// While the alternate is under probation, its captured peer service is a
	// bounded pipeline hypothesis. HOL-delayed ACKs must not shrink the healthy
	// pipeline underneath its previously validated service.
	if alternate.recovering || alternate.restricted {
		if alternate.openingRate > service {
			service = alternate.openingRate
			sampled = time.Now()
		}
		if service*8/1e6 > weight {
			weight = service * 8 / 1e6
		}
	}
	c.admission.mu.Unlock()
	// This is this session's ACK service, not a shared physical estimate.
	if service > 0 && time.Since(sampled) < 30*time.Second && service*8/1e6*1.25 < weight {
		weight = service * 8 / 1e6 * 1.25
	}
	horizon := .25 + c.feedbackHorizon(id, time.Now()).Seconds()
	if !self.restricted && !self.recovering && !alternate.recovering && !alternate.restricted && self.feedbackMax > 0 && time.Since(self.feedbackMaxAt) < 30*time.Second {
		// Normal cumulative ACK feedback includes ordering. Its observed bounded
		// envelope must cover the outstanding pipeline that produced the sample.
		observed := self.feedbackMax
		if observed > 2*time.Second {
			observed = 2 * time.Second
		}
		if h := .25 + observed.Seconds(); h > horizon {
			horizon = h
		}
	}
	if alternate.recovering {
		// Drain extra local queue while preserving this leg's known feedback
		// pipeline. Cumulative ACK must not keep a probation probe behind a large
		// healthy tail. The feedback includes ordering, and is not physical RTT.
		base := self.feedback.Seconds()
		if time.Since(self.feedbackAt) > 30*time.Second {
			base = 0
		}
		smaller := base + c.cfg.AckInterval.Seconds() + .05
		if smaller < horizon {
			horizon = smaller
		}
	}
	budget := weight * 1e6 / 8 * horizon
	minimum := float64(2 * c.cfg.ChunkSize)
	if budget < minimum {
		budget = minimum
	}
	maxBytes := c.ordinaryHardBytes()
	if budget > float64(maxBytes) {
		budget = float64(maxBytes)
	}
	return uint64(budget)
}

func (c *StreamEngine) frameBudget(weight float64) int {
	chunk := c.cfg.ChunkSize
	if chunk <= 0 {
		chunk = 64 << 10
	}
	// A quarter-second of estimated service, bounded by both frame granularity
	// and existing global resources. This is not an extra queue.
	n := int(math.Ceil(weight * 1e6 / 8 * .25 / float64(chunk)))
	max := c.cfg.MaxInflightFrames
	if max < 2 {
		max = 2
	}
	byteFrames := streamOrdinaryMaxBytes / chunk
	if byteFrames < 2 {
		byteFrames = 2
	}
	if max > byteFrames {
		max = byteFrames
	}
	if n < 2 {
		n = 2
	}
	if n > max {
		n = max
	}
	return n
}

func (c *StreamEngine) ordinaryEligible(id uint8, count int, now time.Time) bool {
	if !c.slowLegProtection() || !c.validAdmissionLeg(id) {
		return true
	}
	bytes, _ := c.txLedger.OrdinaryAdmissionBytesByLeg()
	budget := c.ordinaryByteBudget(id)
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	s := &c.admission.legs[id]
	if s.restricted {
		return !s.probePending && count == 0 && !now.Before(s.nextProbe)
	}
	if s.recovering {
		return count < s.limit && (s.byteLimit == 0 || bytes[id] < s.byteLimit)
	}
	return count < c.cfg.MaxInflightFrames && int(id) < len(bytes) && bytes[id] < budget
}

func (c *StreamEngine) maintainAdmission(now time.Time) {
	if !c.slowLegProtection() {
		return
	}
	counts := c.txLedger.OrdinaryFramesByLeg()
	wake := false
	// The pre-write recovery-probe budget is independent of whichever record
	// currently owns the cumulative ACK frontier. A queued probe can sit behind
	// earlier work on the other leg, so expire pending probes per leg here rather
	// than relying only on protectFrontier observing that exact record.
	for id := range c.admission.legs {
		c.admission.mu.Lock()
		state := c.admission.legs[id]
		c.admission.mu.Unlock()
		if !state.probePending || state.probePendingAt.IsZero() || now.Sub(state.probePendingAt) < streamProbeBudget {
			continue
		}
		if !c.expirePendingRecoveryProbe(uint8(id), state.probeSequence, now) {
			continue
		}
		wake = true
		if c.cfg.recoveryTraceHook != nil {
			c.traceRecoveryEvent(streamRecoveryTraceEvent{
				Kind: "probe_prewrite_timeout", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: state.probeSequence, LegID: int16(id), LegGeneration: c.legGeneration[id].Load(),
				ReferenceAt: state.probePendingAt, PolicyAt: now, Age: now.Sub(state.probePendingAt),
				Budget: streamProbeBudget, StateBefore: "restricted", ReasonBefore: state.reason,
				StateAfter: "restricted", ReasonAfter: "probe_write_start_timeout",
				Action: "expire_prewrite_probe", Applied: true,
			})
		}
	}
	c.admission.mu.Lock()
	for id, state := range c.admission.legs {
		if state.restricted && counts[id] == 0 && !now.Before(state.nextProbe) {
			wake = true
		}
	}
	c.admission.mu.Unlock()
	if wake {
		c.wakeDecoupled()
	}
}

func (c *StreamEngine) ordinaryAssigned(id uint8, now time.Time, actualBytes ...int) {
	c.ordinaryAssignedRecord(id, nil, now, actualBytes...)
}

func (c *StreamEngine) ordinaryAssignedRecord(id uint8, record *StreamTXRecord, now time.Time, actualBytes ...int) {
	if !c.slowLegProtection() || !c.validAdmissionLeg(id) {
		return
	}
	probeBytes := c.cfg.ChunkSize
	if len(actualBytes) > 0 && actualBytes[0] > 0 {
		probeBytes = actualBytes[0]
	}
	frontier := c.txLedger.ProgressSnapshot().AckedNext
	active := c.admissionLegIDs()
	seed := float64(c.weightFor(id))
	for _, candidate := range active {
		if candidate == id {
			continue
		}
		if other := c.getLeg(candidate); other != nil {
			if weight := c.effectiveSchedulerWeight(other); weight > seed {
				seed = weight
			}
		}
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	if s.restricted && !s.probePending {
		// Keep the leg restricted while the recovery record waits in the Core
		// queues. The bounded pre-write timer is independent of the RTT/proof
		// window, which starts only when the underlying DATA Write begins.
		s.probePending, s.probePendingAt = true, now
		if record != nil {
			record.recoveryProbeWriteStartedAt.Store(nil)
		}
		s.probeSequence = 0
		s.probeSequenceSet = record != nil
		if record != nil {
			s.probeSequence = record.Sequence()
			if !s.probeExpired || s.probeExpiredSequence != record.Sequence() {
				s.probeExpired = false
				s.probeExpiredSequence = 0
			}
		} else {
			s.probeExpired = false
			s.probeExpiredSequence = 0
		}
		s.recovering, s.limit, s.proofs = false, 1, 0
		s.since, s.reason, s.proofAfter = now, "probe_pending_write", frontier
		s.seedMbps = seed
		s.openingRate = 0
		if peerID, ok := c.alternateIDLocked(id, active); ok {
			s.openingRate = c.admission.legs[peerID].serviceRate
		}
		// Normal ACK service belongs to the retired admission phase. A stale
		// nonzero meter must not bypass the fresh bounded probe hypothesis.
		s.serviceRate = 0
		s.serviceAt = time.Time{}
		s.serviceWindow = time.Time{}
		s.serviceBytes = 0
		s.byteLimit = uint64(probeBytes)
		s.proofBytes = 0
		s.probeWindow = time.Time{}
		s.probeBytes = 0
		s.probeRate = 0
	}
	if s.probePending && s.byteLimit == 0 {
		s.byteLimit = uint64(probeBytes)
	}
	c.admission.mu.Unlock()
}

// recoveryProbeWriteStarted moves a pending recovery probe into recovering at
// the instant the Core writer is about to call the underlying DATA Write. A
// pending probe which already consumed its independent pre-write budget is
// not promoted into recovery here; the existing frontier repair path can then
// rescue it. A writer which already dequeued the record still completes its I/O.
func (c *StreamEngine) recoveryProbeWriteStarted(id uint8, record *StreamTXRecord, at time.Time) bool {
	if !c.slowLegProtection() || !c.validAdmissionLeg(id) || record == nil {
		return true
	}
	ackedNext := c.txLedger.ProgressSnapshot().AckedNext
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	return c.recoveryProbeWriteStartedLocked(id, record, at, ackedNext)
}

// recoveryProbeWriteStartedAtWriteBoundary is called immediately before the
// DATA buffers are handed to the underlying carrier. It samples time only once
// admission locking has completed, so queue or lock wait cannot be mistaken for
// time spent recovering.
func (c *StreamEngine) recoveryProbeWriteStartedAtWriteBoundary(id uint8, record *StreamTXRecord) (time.Time, bool) {
	if !c.slowLegProtection() || !c.validAdmissionLeg(id) || record == nil {
		return time.Now(), true
	}
	ackedNext := c.txLedger.ProgressSnapshot().AckedNext
	c.admission.mu.Lock()
	at := time.Now()
	s := &c.admission.legs[id]
	pendingAt := s.probePendingAt
	pendingProbe := s.probePending && (!s.probeSequenceSet || s.probeSequence == record.Sequence())
	started := c.recoveryProbeWriteStartedLocked(id, record, at, ackedNext)
	c.admission.mu.Unlock()
	if pendingProbe && started && c.cfg.recoveryTraceHook != nil {
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "recovery_probe_write_started", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
			LegID: int16(id), LegGeneration: c.legGeneration[id].Load(), RecordGeneration: c.txLedger.originGenerationSnapshot(record),
			PayloadBytes: len(record.Payload()), ReferenceAt: pendingAt, ActionAt: at,
			Age: at.Sub(pendingAt), Budget: streamProbeBudget,
			StateBefore: "restricted", ReasonBefore: "probe_pending_write",
			StateAfter: "recovering", ReasonAfter: "unique_probe", Applied: true,
		})
	}
	return at, started
}

// recoveryProbeWriteStartedLocked applies the bounded pre-write check and
// transition. c.admission.mu must be held by the caller.
func (c *StreamEngine) recoveryProbeWriteStartedLocked(id uint8, record *StreamTXRecord, at time.Time, ackedNext uint64) bool {
	s := &c.admission.legs[id]
	if s.probeExpired && s.probeExpiredSequence == record.Sequence() {
		return false
	}
	if !s.probePending || (s.probeSequenceSet && s.probeSequence != record.Sequence()) {
		return true
	}
	if !s.probePendingAt.IsZero() && at.Sub(s.probePendingAt) >= streamProbeBudget {
		s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
		s.probePendingAt = time.Time{}
		s.probeExpired = true
		s.probeExpiredSequence = record.Sequence()
		advanceRecoveryEpochLocked(s)
		s.restricted, s.recovering = true, false
		s.since, s.nextProbe, s.reason = at, at.Add(streamRestrictionCooldown), "probe_write_start_timeout"
		s.proofs, s.proofBytes = 0, 0
		return false
	}
	s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
	s.probePendingAt = time.Time{}
	s.probeExpired = false
	s.probeExpiredSequence = 0
	startedAt := at
	record.recoveryProbeWriteStartedAt.Store(&startedAt)
	s.restricted, s.recovering = false, true
	s.since, s.reason, s.proofAfter = at, "unique_probe", ackedNext
	return true
}

func (c *StreamEngine) cancelPendingRecoveryProbe(id uint8, sequence uint64, now time.Time) {
	if !c.validAdmissionLeg(id) {
		return
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	if s.probePending && (!s.probeSequenceSet || s.probeSequence == sequence) {
		s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
		s.probePendingAt = time.Time{}
		s.probeExpired = true
		s.probeExpiredSequence = sequence
		s.reason, s.since = "probe_reassigned_before_write", now
	}
	c.admission.mu.Unlock()
}

func (c *StreamEngine) expirePendingRecoveryProbe(id uint8, sequence uint64, now time.Time) bool {
	if !c.validAdmissionLeg(id) {
		return false
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	expired := s.probePending && (!s.probeSequenceSet || s.probeSequence == sequence) &&
		!s.probePendingAt.IsZero() && now.Sub(s.probePendingAt) >= streamProbeBudget
	if expired {
		s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
		s.probePendingAt = time.Time{}
		s.probeExpired = true
		s.probeExpiredSequence = sequence
		advanceRecoveryEpochLocked(s)
		s.restricted, s.recovering = true, false
		s.since, s.nextProbe, s.reason = now, now.Add(streamRestrictionCooldown), "probe_write_start_timeout"
		s.proofs, s.proofBytes = 0, 0
	}
	c.admission.mu.Unlock()
	return expired
}

// Rejoining a logical leg must not retain proof collected in its old workers.
// The transport retirement barrier handles stale attempts; this timestamp also
// rejects a clean ACK result delayed on the surviving leg across that barrier.
func (c *StreamEngine) admissionRejoined(id uint8, generation uint64, now time.Time) {
	if !c.validAdmissionLeg(id) {
		return
	}
	var frontier uint64
	if c.slowLegProtection() {
		frontier = c.txLedger.ProgressSnapshot().AckedNext
	}
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	// Publish a replacement generation under the same lock used by recovery
	// timeout revalidation, so a stale timeout cannot cross the generation reset.
	c.legGeneration[id].Store(generation)
	if !c.slowLegProtection() {
		return
	}
	s := &c.admission.legs[id]
	s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
	s.probeExpired = false
	s.probeExpiredSequence = 0
	s.probePendingAt = time.Time{}
	s.feedback = 0
	s.feedbackAt = time.Time{}
	s.feedbackMax = 0
	s.feedbackMaxAt = time.Time{}
	s.probeWindow = time.Time{}
	s.probeBytes = 0
	s.probeRate = 0
	s.serviceRate = 0
	s.serviceWindow = time.Time{}
	s.serviceBytes = 0
	s.openingRate = 0
	if s.recovering {
		s.limit, s.proofs, s.since, s.proofAfter = 1, 0, now, frontier
		s.reason = "generation_reprobe"
		s.byteLimit = 0
		s.proofBytes = 0
		s.probeWindow = time.Time{}
		s.probeBytes = 0
		s.probeRate = 0
		s.feedbackMax = 0
		s.feedback = 0
		s.feedbackAt = time.Time{}
	}
}

func (c *StreamEngine) ordinaryOwnerAllowed(id uint8) bool {
	if !c.slowLegProtection() {
		return true
	}
	if !c.validAdmissionLeg(id) {
		return false
	}
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	return !c.admission.legs[id].restricted
}

// pendingRecoveryProbeOwner keeps the one record selected as a bounded
// recovery probe on its restricted Carrier until it reaches the underlying
// DATA Write or the independent pre-write budget expires. This is deliberately
// record-specific: other unadmitted records remain ineligible for the leg.
func (c *StreamEngine) pendingRecoveryProbeOwner(id uint8, sequence uint64) bool {
	if !c.slowLegProtection() || !c.validAdmissionLeg(id) {
		return false
	}
	c.admission.mu.Lock()
	s := c.admission.legs[id]
	pending := s.restricted && s.probePending && s.probeSequenceSet && s.probeSequence == sequence &&
		(!s.probeExpired || s.probeExpiredSequence != sequence)
	c.admission.mu.Unlock()
	return pending
}

func (c *StreamEngine) restrictOrdinary(id uint8, now time.Time, reason string) bool {
	if !c.validAdmissionLeg(id) {
		return false
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	applied := restrictOrdinaryLocked(s, now, reason)
	c.admission.mu.Unlock()
	c.wakeDecoupled()
	return applied
}

func restrictOrdinaryLocked(s *streamAdmissionLeg, now time.Time, reason string) bool {
	if s.restricted {
		return false
	}
	advanceRecoveryEpochLocked(s)
	s.restricted, s.recovering = true, false
	s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
	s.probeExpired = false
	s.probeExpiredSequence = 0
	s.probePendingAt = time.Time{}
	s.since, s.nextProbe, s.reason = now, now.Add(streamRestrictionCooldown), reason
	s.proofs = 0
	s.proofBytes = 0
	s.probeWindow = time.Time{}
	s.probeBytes = 0
	s.probeRate = 0
	s.feedbackMax = 0
	s.feedback = 0
	s.feedbackAt = time.Time{}
	return true
}

func advanceRecoveryEpochLocked(s *streamAdmissionLeg) {
	if s == nil || s.recoveryEpoch == ^uint64(0) {
		return
	}
	s.recoveryEpoch++
}

// restrictOverdueRecoveryFrontier revalidates the frontier and recovery epoch
// under both owning locks before applying a timeout. The ledger lock is acquired
// before admission.mu; admission paths release their lock before calling into
// the TX ledger.
func (c *StreamEngine) restrictOverdueRecoveryFrontier(id uint8, record *StreamTXRecord, now time.Time, budget time.Duration, expectedSince time.Time, expectedGeneration uint64) (eligible, applied bool) {
	if !c.validAdmissionLeg(id) || record == nil {
		return false, false
	}
	l := c.txLedger
	l.mu.Lock()
	if l.outstanding[l.ackedNext] != record {
		l.mu.Unlock()
		return false, false
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	reference, limit := record.frontierAt, budget
	if startedAt := record.recoveryProbeWriteStartedAt.Load(); startedAt != nil {
		reference = *startedAt
	} else {
		if reference.IsZero() {
			reference = record.createdAt
		}
		if record.firstWriteAt.After(reference) {
			reference = record.firstWriteAt
		}
	}
	if s.probePending && (!s.probeSequenceSet || s.probeSequence == record.sequence) && !s.probePendingAt.IsZero() {
		reference, limit = s.probePendingAt, streamProbeBudget
	}
	eligible = !reference.IsZero() && !now.Before(reference) && now.Sub(reference) >= limit
	if eligible && s.recovering && !s.restricted && s.since.Equal(expectedSince) && c.legGeneration[id].Load() == expectedGeneration {
		applied = restrictOrdinaryLocked(s, now, "probe_timeout")
	}
	c.admission.mu.Unlock()
	l.mu.Unlock()
	if applied {
		c.wakeDecoupled()
	}
	return eligible, applied
}

func (c *StreamEngine) admissionACK(result StreamTXACKResult) {
	if !c.slowLegProtection() {
		return
	}
	active := c.admissionLegIDs()
	budgets := make([]uint64, len(c.admission.legs))
	for id := range budgets {
		budgets[id] = c.ordinaryByteBudget(uint8(id))
	}
	recovered := make([]bool, len(c.admission.legs))
	seeds := make([]float64, len(c.admission.legs))
	c.admission.mu.Lock()
	for id := range c.admission.legs {
		s := &c.admission.legs[id]
		if s.probeExpired && result.AckedNext > s.probeExpiredSequence {
			s.probeExpired = false
			s.probeExpiredSequence = 0
		}
		if s.probePending && s.probeSequenceSet && result.AckedNext > s.probeSequence {
			s.probePending, s.probeSequence, s.probeSequenceSet = false, 0, false
			s.probePendingAt = time.Time{}
			s.since, s.nextProbe = result.LastACKProgress, result.LastACKProgress.Add(streamRestrictionCooldown)
			s.reason = "probe_retired_before_write"
		}
	}
	for id := range c.admission.legs {
		legResult := result.legResult(id)
		n := legResult.UniqueTimely
		s := &c.admission.legs[id]
		sampleFresh := legResult.SampleAssignedMin.IsZero() || !legResult.SampleAssignedMin.Before(s.since)
		if gen := legResult.UniqueGeneration; gen != 0 && gen != c.legGeneration[id].Load() {
			continue
		}
		if sample := legResult.FeedbackMin; !s.recovering && !s.restricted && sampleFresh && !result.LastACKProgress.Before(s.since) && sample > 0 && sample <= 2*time.Second {
			if s.feedback == 0 || result.LastACKProgress.Sub(s.feedbackAt) > 30*time.Second || sample < s.feedback {
				s.feedback = sample
				s.feedbackAt = result.LastACKProgress
			}
		}
		if !s.restricted && !s.recovering && sampleFresh && !result.LastACKProgress.Before(s.since) {
			if maximum := legResult.FeedbackMax; maximum > 0 && maximum <= 2*time.Second {
				if s.feedbackMax == 0 || result.LastACKProgress.Sub(s.feedbackMaxAt) > 30*time.Second {
					s.feedbackMax = maximum
				} else {
					s.feedbackMax = (s.feedbackMax + maximum) / 2
				}
				s.feedbackMaxAt = result.LastACKProgress
			}
		}
		if !s.restricted && !s.recovering && sampleFresh && !result.LastACKProgress.Before(s.since) && legResult.SampleBytes > 0 {
			if s.serviceWindow.IsZero() || result.LastACKProgress.Sub(s.serviceWindow) > 2*time.Second {
				s.serviceWindow = result.LastACKProgress
				s.serviceBytes = 0
			}
			s.serviceBytes += legResult.SampleBytes
			if elapsed := result.LastACKProgress.Sub(s.serviceWindow); elapsed >= 250*time.Millisecond {
				rate := float64(s.serviceBytes) / elapsed.Seconds()
				if s.serviceRate == 0 {
					s.serviceRate = rate
				} else {
					s.serviceRate = .5*s.serviceRate + .5*rate
				}
				s.serviceAt = result.LastACKProgress
				s.openingRate = 0
				s.serviceWindow = result.LastACKProgress
				s.serviceBytes = 0
			}
		}
		n += legResult.UniqueOrdered
		// Retirement may precede a failure/cooldown while its ACK callback is
		// delayed on another worker/provider. Proof belongs to its original epoch.
		// The cumulative frontier disambiguates equal/coarse wall-clock timestamps.
		if !s.recovering || n == 0 || result.LastACKProgress.Before(s.since) || result.AckedNext <= s.proofAfter {
			continue
		}
		s.proofAfter = result.AckedNext
		peerID, hasPeer := c.alternateIDLocked(uint8(id), active)
		if hasPeer && s.openingRate == 0 && c.admission.legs[peerID].serviceRate > 0 {
			s.openingRate = c.admission.legs[peerID].serviceRate
		}
		if legResult.UniqueBytes > 0 {
			if s.probeWindow.IsZero() || result.LastACKProgress.Sub(s.probeWindow) > 2*time.Second {
				s.probeWindow = result.LastACKProgress
				s.probeBytes = 0
				s.probeRate = 0 // discard pre-window batch
			} else {
				s.probeBytes += legResult.UniqueBytes
				if elapsed := result.LastACKProgress.Sub(s.probeWindow); elapsed >= 250*time.Millisecond {
					s.probeRate = float64(s.probeBytes) / elapsed.Seconds()
				}
			}
		}
		// A few unique small copies prove reachability only. Require a lower
		// bound of useful service relative to this session's alternate, not prior.
		target := s.openingRate * .1
		var peer streamAdmissionLeg
		if hasPeer {
			peer = c.admission.legs[peerID]
		}
		envelope := time.Duration(0)
		envelopeSource := ""
		if peer.feedbackMax > envelope {
			envelope = peer.feedbackMax
			envelopeSource = "peer_feedback_max"
		}
		if peer.feedback > envelope {
			envelope = peer.feedback
			envelopeSource = "peer_feedback"
		}
		if envelope < streamProbeBudget {
			envelope = streamProbeBudget
			envelopeSource = "stream_probe_budget"
		}
		if envelope > 2*time.Second {
			envelope = 2 * time.Second
			envelopeSource = "two_second_cap"
		}
		meaningful := uint64(target * envelope.Seconds())
		if hasPeer && target > 0 && s.probeRate > 0 && s.probeRate < target && legResult.BeforeOrdinaryBytes >= meaningful && legResult.BeforeOrdinaryFrames >= s.limit {
			peerState := "normal"
			if peer.restricted {
				peerState = "restricted"
			} else if peer.recovering {
				peerState = "recovering"
			}
			probeWindow := time.Duration(0)
			if !s.probeWindow.IsZero() {
				probeWindow = result.LastACKProgress.Sub(s.probeWindow)
			}
			if c.telemetry != nil || c.cfg.recoveryTraceHook != nil {
				decisionAt := time.Now()
				snapshot := StreamProbeUniqueServiceLowSnapshot{
					ObservedAt:             decisionAt,
					AckProgressAt:          result.LastACKProgress,
					ProbeWindowStart:       s.probeWindow,
					ProbeWindow:            probeWindow,
					LegID:                  uint8(id),
					PeerLegID:              peerID,
					StateBefore:            "recovering",
					ReasonBefore:           s.reason,
					PeerState:              peerState,
					PeerReason:             peer.reason,
					StateAfter:             "restricted",
					ReasonAfter:            "probe_unique_service_low",
					LegGeneration:          c.legGeneration[id].Load(),
					AckGeneration:          legResult.UniqueGeneration,
					PeerGeneration:         c.legGeneration[peerID].Load(),
					OpeningRateBPS:         s.openingRate,
					PeerServiceRateBPS:     peer.serviceRate,
					PeerServiceAt:          peer.serviceAt,
					ProbeRateBPS:           s.probeRate,
					ProbeBytes:             s.probeBytes,
					UniqueAckBytes:         legResult.UniqueBytes,
					UniqueTimelyRecords:    legResult.UniqueTimely,
					UniqueOrderedRecords:   legResult.UniqueOrdered,
					AckFeedbackMin:         legResult.FeedbackMin,
					AckFeedbackMax:         legResult.FeedbackMax,
					AckedNext:              result.AckedNext,
					BeforeOrdinaryBytes:    legResult.BeforeOrdinaryBytes,
					BeforeOrdinaryFrames:   legResult.BeforeOrdinaryFrames,
					ProbeFrameLimit:        s.limit,
					TargetRateBPS:          target,
					PeerFeedback:           peer.feedback,
					PeerFeedbackAt:         peer.feedbackAt,
					PeerFeedbackMax:        peer.feedbackMax,
					PeerFeedbackMaxAt:      peer.feedbackMaxAt,
					FeedbackEnvelope:       envelope,
					FeedbackEnvelopeSource: envelopeSource,
					MeaningfulDemandBytes:  meaningful,
				}
				if c.telemetry != nil {
					snapshot.EventIndex = c.telemetry.recordProbeUniqueServiceLow(snapshot)
				}
				var decisionEventIndex uint64
				if c.cfg.recoveryTraceHook != nil {
					decisionEventIndex = c.traceRecoveryEvent(streamRecoveryTraceEvent{
						Kind: "probe_unique_service_low_decision", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
						LegID: int16(id), PeerLegID: int16(peerID), LegGeneration: snapshot.LegGeneration,
						RecordGeneration: snapshot.AckGeneration, AckedNext: result.AckedNext,
						ObservedAt: decisionAt, PolicyAt: decisionAt, StateBefore: snapshot.StateBefore, ReasonBefore: snapshot.ReasonBefore,
						Action: "restrict_ordinary_probe_unique_service_low", ProbeServiceLow: &snapshot,
					})
				}
				if !s.restricted {
					advanceRecoveryEpochLocked(s)
				}
				s.restricted, s.recovering = true, false
				s.since = result.LastACKProgress
				s.nextProbe = s.since.Add(streamRestrictionCooldown)
				s.proofs = 0
				s.proofBytes = 0
				s.reason = "probe_unique_service_low"
				if c.cfg.recoveryTraceHook != nil {
					c.traceRecoveryEvent(streamRecoveryTraceEvent{
						Kind: "probe_unique_service_low_action", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
						LegID: int16(id), PeerLegID: int16(peerID), LegGeneration: snapshot.LegGeneration,
						RecordGeneration: snapshot.AckGeneration, AckedNext: result.AckedNext,
						PolicyAt: decisionAt, ActionAt: time.Now(), StateBefore: snapshot.StateBefore,
						ReasonBefore: snapshot.ReasonBefore, StateAfter: "restricted", ReasonAfter: s.reason,
						DecisionEventIndex: decisionEventIndex, Action: "restrict_ordinary_probe_unique_service_low",
						Applied: s.restricted && !s.recovering, ProbeServiceLow: &snapshot,
					})
				}
			} else {
				if !s.restricted {
					advanceRecoveryEpochLocked(s)
				}
				s.restricted, s.recovering = true, false
				s.since = result.LastACKProgress
				s.nextProbe = s.since.Add(streamRestrictionCooldown)
				s.proofs = 0
				s.proofBytes = 0
				s.reason = "probe_unique_service_low"
			}
			continue
		}
		serviceReady := target == 0 || s.probeRate >= target
		if (s.byteLimit >= budgets[id] || s.limit >= c.cfg.MaxInflightFrames) && serviceReady {
			s.recovering = false
			s.serviceRate = 0
			s.serviceWindow = time.Time{}
			s.serviceBytes = 0
			s.reason = "unique_ack_recovered"
			s.since = result.LastACKProgress
			recovered[id] = true
			seeds[id] = s.seedMbps
			continue
		}
		s.proofs += n
		if s.proofs > 3 {
			s.proofs = 3
		}
		s.proofBytes += legResult.UniqueBytes
		if s.proofBytes > s.byteLimit {
			s.proofBytes = s.byteLimit
		}
		// One stage per ACK. Each newly expanded byte allowance needs fresh
		// confirmed payload bytes; short records cannot manufacture a large window.
		if s.proofs >= 3 && s.byteLimit > 0 && s.proofBytes >= s.byteLimit {
			s.proofs, s.proofBytes = 0, 0
			if s.limit < c.cfg.MaxInflightFrames {
				s.limit *= 2
				if s.limit > c.cfg.MaxInflightFrames {
					s.limit = c.cfg.MaxInflightFrames
				}
			}
			if s.byteLimit < c.ordinaryHardBytes() {
				s.byteLimit *= 2
				if s.byteLimit > c.ordinaryHardBytes() {
					s.byteLimit = c.ordinaryHardBytes()
				}
			}
			s.probeWindow = time.Time{}
			s.probeBytes = 0
			s.probeRate = 0
			s.reason = "unique_ack_ramp"
			if (s.byteLimit >= budgets[id] || s.limit >= c.cfg.MaxInflightFrames) && serviceReady {
				s.recovering, s.reason = false, "unique_ack_recovered"
				s.serviceRate = 0
				s.serviceWindow = time.Time{}
				s.serviceBytes = 0
				s.since = result.LastACKProgress
				recovered[id] = true
				seeds[id] = s.seedMbps
			}
		}
	}
	// Probation ACK ordering may depress the surviving leg's measured service.
	// Restart both normal cohorts once at graduation, retaining only the captured
	// per-session pipeline hypothesis until newly assigned normal data is sampled.
	for id, done := range recovered {
		if !done {
			continue
		}
		own := &c.admission.legs[id]
		own.serviceAt = time.Time{}
		for _, peerID := range active {
			if int(peerID) == id {
				continue
			}
			peer := &c.admission.legs[peerID]
			if !peer.restricted && !peer.recovering {
				floor := own.openingRate
				if peer.serviceRate > floor {
					floor = peer.serviceRate
				}
				peer.serviceRate = 0
				peer.serviceWindow = time.Time{}
				peer.serviceBytes = 0
				peer.serviceAt = time.Time{}
				peer.openingRate = floor
				peer.since = result.LastACKProgress
			}
		}
	}
	c.admission.mu.Unlock()
	if provider, ok := c.capacityProvider.(interface {
		Recover(uint8, float64, time.Time)
	}); ok {
		for id, done := range recovered {
			if done {
				provider.Recover(uint8(id), seeds[id], result.LastACKProgress)
			}
		}
	}
	graduated := false
	for _, done := range recovered {
		graduated = graduated || done
	}
	if graduated {
		// Probation's deliberately limited allocation is not an entitlement debt
		// that may be repaid after graduation. Preserve cumulative counters, start
		// a new normalized service epoch once, not on ordinary queue fullness.
		c.weightedMu.Lock()
		for _, leg := range c.availableLegs() {
			weight := c.effectiveSchedulerWeight(leg)
			if weight > 0 {
				c.assignedEpochOrigin[leg.id] = float64(c.assignedService[leg.id]) / weight
				c.assignedEpochWeight[leg.id] = weight
			}
		}
		c.weightedMu.Unlock()
	}
	c.wakeDecoupled()
}

// Reuses the existing cancellable maintenance loop. An overdue frontier proves
// a blocker; any subsequent repair is bounded to its old ordinary-owner prefix.
func (c *StreamEngine) protectFrontier(candidate StreamTXFrontierCandidate, now time.Time) StreamTXFrontierCandidate {
	if !c.slowLegProtection() || !candidate.exists || candidate.owner < 0 || candidate.owner >= int16(len(c.admission.legs)) {
		return candidate
	}
	id := uint8(candidate.owner)
	if owner := c.txLedger.OrdinaryOwner(candidate.record); owner >= 0 && owner < int16(len(c.admission.legs)) {
		id = uint8(owner)
		candidate.owner = owner
	}
	active := c.admissionLegIDs()
	c.admission.mu.Lock()
	s := c.admission.legs[id]
	stateObservedAt := time.Time{}
	if c.cfg.recoveryTraceHook != nil {
		stateObservedAt = time.Now()
	}
	c.admission.mu.Unlock()
	age := now.Sub(candidate.reference)
	if s.restricted || s.recovering {
		budget := c.feedbackHorizon(id, now)
		for _, otherID := range active {
			if otherID == id {
				continue
			}
			if other := c.feedbackHorizon(otherID, now); other > budget {
				budget = other
			}
		}
		if budget < streamProbeBudget {
			budget = streamProbeBudget
		}
		// A recovering probe can only provide cumulative-ACK evidence after it
		// becomes the frontier. Do not charge time spent behind an earlier
		// record as probe exposure, regardless of the measured RTT.
		pendingRecord := s.probePending && (!s.probeSequenceSet || s.probeSequence == candidate.record.Sequence())
		var ledgerSnapshot streamTXProbeReferenceSnapshot
		if c.cfg.recoveryTraceHook != nil {
			ledgerSnapshot = c.txLedger.probeReferenceSnapshot(candidate.record, s.recovering)
			if !ledgerSnapshot.ReferenceAt.IsZero() {
				if pendingRecord && !s.probePendingAt.IsZero() {
					candidate.reference = s.probePendingAt
				} else {
					candidate.reference = ledgerSnapshot.ReferenceAt
				}
				age = now.Sub(ledgerSnapshot.ReferenceAt)
				if pendingRecord && !s.probePendingAt.IsZero() {
					age = now.Sub(s.probePendingAt)
				}
			}
		} else if ref := c.txLedger.probeReference(candidate.record, s.recovering); !ref.IsZero() {
			if pendingRecord && !s.probePendingAt.IsZero() {
				candidate.reference = s.probePendingAt
				age = now.Sub(s.probePendingAt)
			} else {
				candidate.reference = ref
				age = now.Sub(ref)
			}
		}
		prewriteAge := time.Duration(0)
		if s.probePending && !s.probePendingAt.IsZero() {
			prewriteAge = now.Sub(s.probePendingAt)
		}
		if pendingRecord {
			candidate.overdue = age >= streamProbeBudget
		} else {
			candidate.overdue = age >= budget
		}
		if s.probePending && prewriteAge >= streamProbeBudget && c.expirePendingRecoveryProbe(id, s.probeSequence, now) && c.cfg.recoveryTraceHook != nil {
			event := streamRecoveryTraceEvent{
				Kind: "probe_prewrite_timeout", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: s.probeSequence, LegID: int16(id), LegGeneration: c.legGeneration[id].Load(),
				ReferenceAt: s.probePendingAt, PolicyAt: now, Age: prewriteAge,
				Budget: streamProbeBudget, StateBefore: "restricted", ReasonBefore: s.reason,
				StateAfter: "restricted", ReasonAfter: "probe_write_start_timeout",
				Action: "expire_prewrite_probe",
			}
			if pendingRecord {
				event.RecordGeneration = ledgerSnapshot.OriginGeneration
				event.PayloadBytes = len(candidate.record.Payload())
				event.FirstWriteAt, event.SendCount = ledgerSnapshot.FirstWriteAt, ledgerSnapshot.SendCount
				event.AckedNext = ledgerSnapshot.AckedNext
			}
			c.traceRecoveryEvent(event)
		}
		if candidate.overdue && s.recovering {
			expectedSince := s.since
			expectedGeneration := c.legGeneration[id].Load()
			var decisionEventIndex uint64
			if c.cfg.recoveryTraceHook != nil {
				decisionEventIndex = c.traceRecoveryEvent(streamRecoveryTraceEvent{
					Kind: "probe_timeout_decision", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
					RecordSequence: candidate.record.Sequence(), LegID: int16(id),
					LegGeneration: c.legGeneration[id].Load(), RecordGeneration: ledgerSnapshot.OriginGeneration,
					PayloadBytes: len(candidate.record.Payload()), ReferenceAt: candidate.reference,
					PolicyAt: now, StateObservedAt: stateObservedAt, Age: age, Budget: budget,
					StateBefore: "recovering", ReasonBefore: s.reason,
					LedgerObservedAt: ledgerSnapshot.LedgerObservedAt, LedgerPresent: ledgerSnapshot.Exists,
					AckedNext: ledgerSnapshot.AckedNext, InTransit: ledgerSnapshot.InTransit,
					TransitLeg: ledgerSnapshot.TransitLeg, TransitSince: ledgerSnapshot.TransitSince,
					RescueInTransit: ledgerSnapshot.RescueInTransit, RescueLeg: ledgerSnapshot.RescueLeg,
					RescueSince: ledgerSnapshot.RescueSince, FirstWriteAt: ledgerSnapshot.FirstWriteAt,
					LastSentAt: ledgerSnapshot.LastSentAt, FrontierAt: ledgerSnapshot.FrontierAt,
					SendCount: ledgerSnapshot.SendCount, Action: "restrict_ordinary_probe_timeout",
				})
			}
			eligible, applied := c.restrictOverdueRecoveryFrontier(id, candidate.record, now, budget, expectedSince, expectedGeneration)
			if !eligible || !applied {
				// The candidate or recovery epoch changed after it was observed. Do not
				// let this stale maintenance pass trigger a rescue under the old state;
				// the next pass will calculate policy from the current frontier.
				candidate.overdue = false
			}
			if c.cfg.recoveryTraceHook != nil {
				c.admission.mu.Lock()
				after := c.admission.legs[id]
				actionAt := time.Now()
				c.admission.mu.Unlock()
				stateAfter := "normal"
				if after.restricted {
					stateAfter = "restricted"
				} else if after.recovering {
					stateAfter = "recovering"
				}
				stateBefore := "normal"
				if s.restricted {
					stateBefore = "restricted"
				} else if s.recovering {
					stateBefore = "recovering"
				}
				c.traceRecoveryEvent(streamRecoveryTraceEvent{
					Kind: "probe_timeout_action", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
					RecordSequence: candidate.record.Sequence(), LegID: int16(id),
					LegGeneration: c.legGeneration[id].Load(), RecordGeneration: ledgerSnapshot.OriginGeneration,
					ReferenceAt: candidate.reference, PolicyAt: now, ActionAt: actionAt, StateObservedAt: stateObservedAt,
					Age: age, Budget: budget, StateBefore: stateBefore, ReasonBefore: s.reason,
					StateAfter: stateAfter, ReasonAfter: after.reason,
					DecisionEventIndex: decisionEventIndex, Action: "restrict_ordinary_probe_timeout",
					Applied:          applied,
					LedgerObservedAt: ledgerSnapshot.LedgerObservedAt, LedgerPresent: ledgerSnapshot.Exists,
					AckedNext: ledgerSnapshot.AckedNext, InTransit: ledgerSnapshot.InTransit,
					TransitLeg: ledgerSnapshot.TransitLeg, TransitSince: ledgerSnapshot.TransitSince,
					FirstWriteAt: ledgerSnapshot.FirstWriteAt, SendCount: ledgerSnapshot.SendCount,
				})
			}
		}
		return candidate
	}
	age = now.Sub(candidate.record.createdAt)
	threshold := 2 * time.Second
	if c.cfg.RetransmitTimeout > threshold {
		threshold = c.cfg.RetransmitTimeout
	}
	var other *streamLeg
	var otherID uint8
	var otherWeight float64
	for _, candidateLeg := range c.availableLegs() {
		if candidateLeg.id == id {
			continue
		}
		weight := c.effectiveSchedulerWeight(candidateLeg)
		if other == nil || weight > otherWeight {
			other, otherID, otherWeight = candidateLeg, candidateLeg.id, weight
		}
	}
	if age >= streamProbeBudget && other != nil {
		otherWriteBPS, _, latency := other.perf.snapshot()
		own := c.getLeg(id)
		if own == nil {
			return candidate
		}
		ownWriteBPS, _, ownLatency := own.perf.snapshot()
		started := own.writeStartedNs.Load()
		blocked := started != 0 && now.Sub(time.Unix(0, started)) >= threshold
		var writeStartedAt time.Time
		if started != 0 {
			writeStartedAt = time.Unix(0, started)
		}
		otherStarted := other.writeStartedNs.Load()
		alternateBlocked := otherStarted != 0 && now.Sub(time.Unix(0, otherStarted)) >= threshold
		writeSlow := blocked || (ownLatency >= 50*time.Millisecond && ownLatency > latency*4)
		weight := c.effectiveSchedulerWeight(own)
		knownSlow := weight > 0 && weight*4 < otherWeight
		c.admission.mu.Lock()
		alternateNormal := !c.admission.legs[otherID].restricted && !c.admission.legs[otherID].recovering
		state := &c.admission.legs[id]
		persistentLow := false
		if knownSlow && alternateNormal {
			if state.suspectSince.IsZero() || now.Sub(state.suspectAt) > time.Second {
				state.suspectSince = now
			}
			state.suspectAt = now
			persistentLow = now.Sub(state.suspectSince) >= threshold
		}
		suspectSince, suspectObservedAt := state.suspectSince, state.suspectAt
		c.admission.mu.Unlock()
		// This is a HOL safety restriction, not a claim about physical capacity.
		// Sustained frontier ownership plus independent alternate write progress is
		// required. A single delayed ACK or idle stream cannot trigger it.
		trigger := ""
		if blocked {
			trigger = "write_blocked"
		} else if writeSlow && age >= threshold {
			trigger = "slow_write_and_frontier_age"
		} else if persistentLow {
			trigger = "persistent_low_service"
		}
		otherWriteCount := other.debugWriteCount.Load()
		restrictionEligible := trigger != "" && !alternateBlocked && alternateNormal && latency > 0 && latency < threshold/8 && otherWriteCount >= 3
		restrictionApplied := false
		if restrictionEligible {
			restrictionApplied = c.restrictOrdinary(id, now, "sustained_frontier")
			candidate.overdue = now.Sub(candidate.reference) >= streamProbeBudget
		}
		if c.cfg.recoveryTraceHook != nil && (trigger != "" || candidate.overdue) {
			ledger := c.txLedger.probeReferenceSnapshot(candidate.record, false)
			c.admission.mu.Lock()
			stateAfter := c.admission.legs[id]
			c.admission.mu.Unlock()
			stateBeforeName, stateAfterName := "normal", "normal"
			if s.restricted {
				stateBeforeName = "restricted"
			} else if s.recovering {
				stateBeforeName = "recovering"
			}
			if stateAfter.restricted {
				stateAfterName = "restricted"
			} else if stateAfter.recovering {
				stateAfterName = "recovering"
			}
			predictedRemaining := time.Duration(0)
			estimateBasis := "unavailable"
			if started != 0 && ownWriteBPS > 0 && len(candidate.record.Payload()) > 0 {
				estimatedFullWrite := time.Duration(float64(len(candidate.record.Payload())) / ownWriteBPS * float64(time.Second))
				predictedRemaining = estimatedFullWrite - now.Sub(writeStartedAt)
				if predictedRemaining < 0 {
					predictedRemaining = 0
				}
				estimateBasis = "completed_write_bps_ewma_minus_current_write_age"
			}
			action := "retain"
			if restrictionEligible {
				action = "restrict_sustained_frontier"
				if !restrictionApplied {
					action = "restriction_not_applied"
				}
			}
			c.traceRecoveryEvent(streamRecoveryTraceEvent{
				Kind: "slow_leg_frontier_decision", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: candidate.record.Sequence(), FrameSequence: candidate.record.Sequence(), FrameType: "data",
				LegID: int16(id), PeerLegID: int16(otherID), LegGeneration: c.legGeneration[id].Load(),
				RecordGeneration: ledger.OriginGeneration, PayloadBytes: len(candidate.record.Payload()),
				ReferenceAt: candidate.reference, PolicyAt: now, Age: age, Budget: threshold,
				StateBefore: stateBeforeName, ReasonBefore: s.reason, StateAfter: stateAfterName, ReasonAfter: stateAfter.reason,
				LedgerObservedAt: ledger.LedgerObservedAt, LedgerPresent: ledger.Exists, AckedNext: ledger.AckedNext,
				InTransit: ledger.InTransit, TransitLeg: ledger.TransitLeg, TransitSince: ledger.TransitSince,
				RescueInTransit: ledger.RescueInTransit, RescueLeg: ledger.RescueLeg, RescueSince: ledger.RescueSince,
				FirstWriteAt: ledger.FirstWriteAt, LastSentAt: ledger.LastSentAt, FrontierAt: ledger.FrontierAt,
				SendCount: ledger.SendCount, AttemptStartedAt: writeStartedAt,
				Action: action, Applied: restrictionApplied,
				Decision: &streamRecoveryDecisionSnapshot{
					Trigger: trigger, BlockedWrite: blocked, WriteSlow: writeSlow, PersistentLow: persistentLow,
					KnownSlow: knownSlow, AlternateBlocked: alternateBlocked, AlternateNormal: alternateNormal,
					FrontierOverdue: candidate.overdue, AlternateWriteCount: otherWriteCount,
					OwnWriteBPS: ownWriteBPS, AlternateWriteBPS: otherWriteBPS,
					OwnWriteLatency: ownLatency, AlternateWriteLatency: latency,
					OwnEffectiveWeight: weight, AlternateEffectiveWeight: otherWeight,
					WriteStartedAt: writeStartedAt, PredictedServiceRemaining: predictedRemaining,
					PredictedServiceEstimateBasis: estimateBasis, SuspectSince: suspectSince,
					SuspectObservedAt: suspectObservedAt, AdmissionAge: age,
					FrontierAge: now.Sub(candidate.reference), RestrictionEligible: restrictionEligible,
				},
			})
		}
	}
	return candidate
}

// Demand is one blocked local producer, not an unlimited ingress pre-read.
// A delivered ACK resets the stall clock. Normal idle/outstanding-only tails
// do not activate a companion solely because a rate threshold was not met.
func (c *StreamEngine) stalledActivationReady(now time.Time) bool {
	if c.legCount() != 1 {
		return false
	}
	snap := c.txLedger.Snapshot(now)
	if snap.OutstandingFrames == 0 {
		return false
	}
	required := c.cfg.ActivationWindow
	if required < 500*time.Millisecond {
		required = 500 * time.Millisecond
	}
	if c.cfg.RetransmitTimeout > required {
		required = c.cfg.RetransmitTimeout
	}
	if !c.inputBlocked.Load() && !c.txLedger.sentTailStalled(now, required, uint64(2*c.cfg.ChunkSize)) {
		return false
	}
	if !snap.LastACKProgress.IsZero() {
		return now.Sub(snap.LastACKProgress) >= required
	}
	return snap.OldestOutstandingAge >= required
}

func (c *StreamEngine) ordinaryHardBytes() uint64 {
	max := uint64(streamOrdinaryMaxBytes)
	for _, frames := range []int{c.cfg.MaxInflightFrames, c.cfg.MaxReorderFrames} {
		if frames > 0 {
			n := uint64(frames) * uint64(c.cfg.ChunkSize)
			if n < max {
				max = n
			}
		}
	}
	return max
}
func (c *StreamEngine) feedbackHorizon(id uint8, now time.Time) time.Duration {
	c.admission.mu.Lock()
	s := c.admission.legs[id]
	c.admission.mu.Unlock()
	if s.feedback <= 0 || now.Sub(s.feedbackAt) > 30*time.Second {
		return 0
	}
	margin := s.feedback / 4
	queueMargin := c.cfg.AckInterval + streamProbeBudget/4
	if margin < queueMargin {
		margin = queueMargin
	}
	horizon := s.feedback + margin
	if horizon > 2*time.Second {
		horizon = 2 * time.Second
	}
	return horizon
}

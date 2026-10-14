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
	suspectSince, suspectAt  time.Time
}
type streamAdmission struct {
	mu   sync.Mutex
	legs [2]streamAdmissionLeg
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
	c.admission.mu.Lock()
	self := c.admission.legs[id]
	alternate := c.admission.legs[1-id]
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
	if !c.slowLegProtection() {
		return true
	}
	bytes, _ := c.txLedger.OrdinaryAdmissionBytes()
	budget := c.ordinaryByteBudget(id)
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	s := &c.admission.legs[id]
	if s.restricted {
		return count == 0 && !now.Before(s.nextProbe)
	}
	if s.recovering {
		return count < s.limit && (s.byteLimit == 0 || bytes[id] < s.byteLimit)
	}
	return count < c.cfg.MaxInflightFrames && bytes[id] < budget
}

func (c *StreamEngine) maintainAdmission(now time.Time) {
	if !c.slowLegProtection() {
		return
	}
	counts := c.txLedger.OrdinaryFrames()
	wake := false
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
	if !c.slowLegProtection() {
		return
	}
	probeBytes := c.cfg.ChunkSize
	if len(actualBytes) > 0 && actualBytes[0] > 0 {
		probeBytes = actualBytes[0]
	}
	frontier := c.txLedger.ProgressSnapshot().AckedNext
	seed := float64(c.weightFor(id))
	if other := c.getLeg(1 - id); other != nil {
		if weight := c.effectiveSchedulerWeight(other); weight > seed {
			seed = weight
		}
	}
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	if s.restricted {
		s.restricted, s.recovering, s.limit, s.proofs = false, true, 1, 0
		s.since, s.reason, s.proofAfter = now, "unique_probe", frontier
		s.seedMbps = seed
		s.openingRate = c.admission.legs[1-id].serviceRate
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
	if s.recovering && s.byteLimit == 0 {
		s.byteLimit = uint64(probeBytes)
	}
	c.admission.mu.Unlock()
}

// Rejoining a logical leg must not retain proof collected in its old workers.
// The transport retirement barrier handles stale attempts; this timestamp also
// rejects a clean ACK result delayed on the surviving leg across that barrier.
func (c *StreamEngine) admissionRejoined(id uint8, now time.Time) {
	if !c.slowLegProtection() || id >= 2 {
		return
	}
	frontier := c.txLedger.ProgressSnapshot().AckedNext
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	s := &c.admission.legs[id]
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
	c.admission.mu.Lock()
	defer c.admission.mu.Unlock()
	return !c.admission.legs[id].restricted
}

func (c *StreamEngine) restrictOrdinary(id uint8, now time.Time, reason string) {
	c.admission.mu.Lock()
	s := &c.admission.legs[id]
	if !s.restricted {
		s.restricted, s.recovering = true, false
		s.since, s.nextProbe, s.reason = now, now.Add(streamRestrictionCooldown), reason
		s.proofs = 0
		s.proofBytes = 0
		s.probeWindow = time.Time{}
		s.probeBytes = 0
		s.probeRate = 0
		s.feedbackMax = 0
		s.feedback = 0
		s.feedbackAt = time.Time{}
	}
	c.admission.mu.Unlock()
	c.wakeDecoupled()
}

func (c *StreamEngine) admissionACK(result StreamTXACKResult) {
	if !c.slowLegProtection() {
		return
	}
	var budgets [2]uint64
	for id := range budgets {
		budgets[id] = c.ordinaryByteBudget(uint8(id))
	}
	var recovered [2]bool
	var seeds [2]float64
	c.admission.mu.Lock()
	for id, n := range result.UniqueTimelyByLeg {
		s := &c.admission.legs[id]
		sampleFresh := result.SampleAssignedMinByLeg[id].IsZero() || !result.SampleAssignedMinByLeg[id].Before(s.since)
		if gen := result.UniqueGeneration[id]; gen != 0 && gen != c.legGeneration[id].Load() {
			continue
		}
		if sample := result.FeedbackMinByLeg[id]; !s.recovering && !s.restricted && sampleFresh && !result.LastACKProgress.Before(s.since) && sample > 0 && sample <= 2*time.Second {
			if s.feedback == 0 || result.LastACKProgress.Sub(s.feedbackAt) > 30*time.Second || sample < s.feedback {
				s.feedback = sample
				s.feedbackAt = result.LastACKProgress
			}
		}
		if !s.restricted && !s.recovering && sampleFresh && !result.LastACKProgress.Before(s.since) {
			if maximum := result.FeedbackMaxByLeg[id]; maximum > 0 && maximum <= 2*time.Second {
				if s.feedbackMax == 0 || result.LastACKProgress.Sub(s.feedbackMaxAt) > 30*time.Second {
					s.feedbackMax = maximum
				} else {
					s.feedbackMax = (s.feedbackMax + maximum) / 2
				}
				s.feedbackMaxAt = result.LastACKProgress
			}
		}
		if !s.restricted && !s.recovering && sampleFresh && !result.LastACKProgress.Before(s.since) && result.SampleBytesByLeg[id] > 0 {
			if s.serviceWindow.IsZero() || result.LastACKProgress.Sub(s.serviceWindow) > 2*time.Second {
				s.serviceWindow = result.LastACKProgress
				s.serviceBytes = 0
			}
			s.serviceBytes += result.SampleBytesByLeg[id]
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
		n += result.UniqueOrderedByLeg[id]
		// Retirement may precede a failure/cooldown while its ACK callback is
		// delayed on another worker/provider. Proof belongs to its original epoch.
		// The cumulative frontier disambiguates equal/coarse wall-clock timestamps.
		if !s.recovering || n == 0 || result.LastACKProgress.Before(s.since) || result.AckedNext <= s.proofAfter {
			continue
		}
		s.proofAfter = result.AckedNext
		if s.openingRate == 0 && c.admission.legs[1-id].serviceRate > 0 {
			s.openingRate = c.admission.legs[1-id].serviceRate
		}
		if result.UniqueBytesByLeg[id] > 0 {
			if s.probeWindow.IsZero() || result.LastACKProgress.Sub(s.probeWindow) > 2*time.Second {
				s.probeWindow = result.LastACKProgress
				s.probeBytes = 0
				s.probeRate = 0 // discard pre-window batch
			} else {
				s.probeBytes += result.UniqueBytesByLeg[id]
				if elapsed := result.LastACKProgress.Sub(s.probeWindow); elapsed >= 250*time.Millisecond {
					s.probeRate = float64(s.probeBytes) / elapsed.Seconds()
				}
			}
		}
		// A few unique small copies prove reachability only. Require a lower
		// bound of useful service relative to this session's alternate, not prior.
		target := s.openingRate * .1
		peer := c.admission.legs[1-id]
		envelope := peer.feedbackMax
		if envelope < peer.feedback {
			envelope = peer.feedback
		}
		if envelope < streamProbeBudget {
			envelope = streamProbeBudget
		}
		if envelope > 2*time.Second {
			envelope = 2 * time.Second
		}
		meaningful := uint64(target * envelope.Seconds())
		if target > 0 && s.probeRate > 0 && s.probeRate < target && result.BeforeOrdinaryBytes[id] >= meaningful && result.BeforeOrdinaryFrames[id] >= s.limit {
			s.restricted, s.recovering = true, false
			s.since = result.LastACKProgress
			s.nextProbe = s.since.Add(streamRestrictionCooldown)
			s.proofs = 0
			s.proofBytes = 0
			s.reason = "probe_unique_service_low"
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
		s.proofBytes += result.UniqueBytesByLeg[id]
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
		peer := &c.admission.legs[1-id]
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
	if recovered[0] || recovered[1] {
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
	if !c.slowLegProtection() || !candidate.exists || candidate.owner < 0 || candidate.owner >= 2 {
		return candidate
	}
	id := uint8(candidate.owner)
	if owner := c.txLedger.OrdinaryOwner(candidate.record); owner >= 0 && owner < 2 {
		id = uint8(owner)
		candidate.owner = owner
	}
	c.admission.mu.Lock()
	s := c.admission.legs[id]
	c.admission.mu.Unlock()
	age := now.Sub(candidate.reference)
	if s.restricted || s.recovering {
		budget := c.feedbackHorizon(id, now)
		if other := c.feedbackHorizon(1-id, now); other > budget {
			budget = other
		}
		if budget < streamProbeBudget {
			budget = streamProbeBudget
		}
		// A recovering probe can only provide cumulative-ACK evidence after it
		// becomes the frontier. Do not charge time spent behind an earlier
		// record as probe exposure, regardless of the measured RTT.
		if ref := c.txLedger.probeReference(candidate.record, s.recovering); !ref.IsZero() {
			candidate.reference = ref
			age = now.Sub(ref)
		}
		candidate.overdue = age >= budget
		if candidate.overdue && s.recovering {
			c.restrictOrdinary(id, now, "probe_timeout")
		}
		return candidate
	}
	age = now.Sub(candidate.record.createdAt)
	threshold := 2 * time.Second
	if c.cfg.RetransmitTimeout > threshold {
		threshold = c.cfg.RetransmitTimeout
	}
	other := c.getLeg(1 - id)
	if age >= streamProbeBudget && other != nil {
		_, _, latency := other.perf.snapshot()
		own := c.getLeg(id)
		if own == nil {
			return candidate
		}
		_, _, ownLatency := own.perf.snapshot()
		started := own.writeStartedNs.Load()
		blocked := started != 0 && now.Sub(time.Unix(0, started)) >= threshold
		otherStarted := other.writeStartedNs.Load()
		alternateBlocked := otherStarted != 0 && now.Sub(time.Unix(0, otherStarted)) >= threshold
		writeSlow := blocked || (ownLatency >= 50*time.Millisecond && ownLatency > latency*4)
		weight, otherWeight := c.effectiveSchedulerWeight(own), c.effectiveSchedulerWeight(other)
		knownSlow := weight > 0 && weight*4 < otherWeight
		c.admission.mu.Lock()
		alternateNormal := !c.admission.legs[1-id].restricted && !c.admission.legs[1-id].recovering
		state := &c.admission.legs[id]
		persistentLow := false
		if knownSlow && alternateNormal {
			if state.suspectSince.IsZero() || now.Sub(state.suspectAt) > time.Second {
				state.suspectSince = now
			}
			state.suspectAt = now
			persistentLow = now.Sub(state.suspectSince) >= threshold
		}
		c.admission.mu.Unlock()
		// This is a HOL safety restriction, not a claim about physical capacity.
		// Sustained frontier ownership plus independent alternate write progress is
		// required. A single delayed ACK or idle stream cannot trigger it.
		if (blocked || (writeSlow && age >= threshold) || persistentLow) && !alternateBlocked && alternateNormal && latency > 0 && latency < threshold/8 && other.debugWriteCount.Load() >= 3 {
			c.restrictOrdinary(id, now, "sustained_frontier")
			candidate.overdue = now.Sub(candidate.reference) >= streamProbeBudget
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

package smp3core

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// StreamDataRXSequenceEvent is an opt-in, payload-free receive trace. It binds
// the decoded DATA sequence to the receiving leg generation and records the
// Core reorder-window disposition. It does not claim application delivery.
type StreamDataRXSequenceEvent struct {
	EventIndex     uint64    `json:"event_index"`
	CarrierReadAt  time.Time `json:"carrier_read_at"`
	CoreAcceptedAt time.Time `json:"core_accepted_at"`
	Sequence       uint64    `json:"sequence"`
	LegID          uint8     `json:"leg_id"`
	LegGeneration  uint64    `json:"leg_generation"`
	PayloadBytes   uint32    `json:"payload_bytes"`
	Disposition    string    `json:"disposition"`
	Unique         bool      `json:"unique"`
}

const StreamDataRXSequenceEventLimit = 4096

type StreamStartupReleaseReason uint8

const (
	StartupReleaseNone StreamStartupReleaseReason = iota
	StartupReleasePreferredAlreadyAttached
	StartupReleasePreferredAttachedBeforeNonPreferred
	StartupReleasePreferredArrivedWithinGrace
	StartupReleaseGraceExpired
	StartupReleasePreferredTerminalUnavailable
	StartupReleaseSessionClosedBeforeRelease
)

func (r StreamStartupReleaseReason) String() string {
	switch r {
	case StartupReleasePreferredAlreadyAttached:
		return "preferred_already_attached"
	case StartupReleasePreferredAttachedBeforeNonPreferred:
		return "preferred_attached_before_nonpreferred"
	case StartupReleasePreferredArrivedWithinGrace:
		return "preferred_arrived_within_grace"
	case StartupReleaseGraceExpired:
		return "grace_expired"
	case StartupReleasePreferredTerminalUnavailable:
		return "preferred_terminal_unavailable"
	case StartupReleaseSessionClosedBeforeRelease:
		return "session_closed_before_release"
	default:
		return ""
	}
}

type StreamStartupTelemetrySnapshot struct {
	Applied                bool
	Policy                 StreamStartupPolicy
	PreferredLeg           int16
	Grace                  time.Duration
	FirstDataWaitingAt     time.Time
	GraceStartedAt         time.Time
	PreferredAttachedAt    time.Time
	NonPreferredAttachedAt time.Time
	ReleasedAt             time.Time
	ReleaseReason          StreamStartupReleaseReason
	FirstDataLeg           int16
}

// StreamProbeUniqueServiceLowSnapshot captures the immutable inputs at the
// exact admission transition where low unique-ACK service restricts a leg.
// It is diagnostic only; the policy decision remains in stream_admission.go.
type StreamProbeUniqueServiceLowSnapshot struct {
	EventIndex             uint64
	ObservedAt             time.Time
	AckProgressAt          time.Time
	ProbeWindowStart       time.Time
	ProbeWindow            time.Duration
	LegID                  uint8
	PeerLegID              uint8
	StateBefore            string
	ReasonBefore           string
	PeerState              string
	PeerReason             string
	StateAfter             string
	ReasonAfter            string
	LegGeneration          uint64
	AckGeneration          uint64
	PeerGeneration         uint64
	OpeningRateBPS         float64
	PeerServiceRateBPS     float64
	PeerServiceAt          time.Time
	ProbeRateBPS           float64
	ProbeBytes             uint64
	UniqueAckBytes         uint64
	UniqueTimelyRecords    int
	UniqueOrderedRecords   int
	AckFeedbackMin         time.Duration
	AckFeedbackMax         time.Duration
	AckedNext              uint64
	BeforeOrdinaryBytes    uint64
	BeforeOrdinaryFrames   int
	ProbeFrameLimit        int
	TargetRateBPS          float64
	PeerFeedback           time.Duration
	PeerFeedbackAt         time.Time
	PeerFeedbackMax        time.Duration
	PeerFeedbackMaxAt      time.Time
	FeedbackEnvelope       time.Duration
	FeedbackEnvelopeSource string
	MeaningfulDemandBytes  uint64
}

const streamProbeUniqueServiceLowHistoryLimit = 32

// StreamTelemetryStats contains only counters/events that are not already
// represented by the authoritative StreamStats fields. Logical byte totals
// remain in StreamStats; this avoids a second accounting domain.
type StreamTelemetryStats struct {
	// DataTxAttemptFrames is the historical count of complete DATA frame writes
	// whose Carrier Write returned successfully. Use CarrierWriteAttemptFrames
	// and DataTxCarrierWriteAttemptFrames/DataTxAttemptBytes for actual frame-
	// level Carrier Write attempts.
	DataTxAttemptFrames                [2]uint64
	DataTxRetransmitFrames             [2]uint64
	DataTxCarrierWriteAttemptFrames    [2]uint64
	DataTxAttemptBytes                 [2]uint64
	DataTxRetryWriteAttemptFrames      [2]uint64
	DataTxRescueWriteAttemptFrames     [2]uint64
	DataTxRescueWriteAttemptBytes      [2]uint64
	DataTxCarrierWriteErrorFrames      [2]uint64
	DataTxCarrierWireBytes             [2]uint64
	DataTxPartialWireBytes             [2]uint64
	DataRxUniqueFrames                 [2]uint64
	DataRxDuplicateFrames              [2]uint64
	DataRxDuplicateBytes               [2]uint64
	AckTxFrames                        [2]uint64
	AckRxFrames                        [2]uint64
	ActivateTxFrames                   [2]uint64
	ActivateRxFrames                   [2]uint64
	CloseTxFrames                      [2]uint64
	CloseRxFrames                      [2]uint64
	RescueAttemptFrames                [2]uint64
	RescueAttemptBytes                 [2]uint64
	FirstTxDataAt                      [2]time.Time
	FirstRxUniqueDataAt                [2]time.Time
	Startup                            StreamStartupTelemetrySnapshot
	ProbeUniqueServiceLowEvents        []StreamProbeUniqueServiceLowSnapshot
	DroppedProbeUniqueServiceLowEvents uint64
	DataRXSequenceEvents               []StreamDataRXSequenceEvent
	DroppedDataRXSequenceEvents        uint64
	// Legs includes all observed leg counters. The fixed arrays above remain
	// compatibility mirrors for leg 0 and leg 1.
	Legs []StreamLegTelemetryStats
}

type StreamLegTelemetryStats struct {
	LegID                                                                 uint8
	DataTxAttemptFrames, DataTxRetransmitFrames                           uint64
	DataTxCarrierWriteAttemptFrames, DataTxAttemptBytes                   uint64
	DataTxRetryWriteAttemptFrames, DataTxRescueWriteAttemptFrames         uint64
	DataTxRescueWriteAttemptBytes, DataTxCarrierWriteErrorFrames          uint64
	DataTxCarrierWireBytes, DataTxPartialWireBytes                        uint64
	DataRxUniqueFrames, DataRxDuplicateFrames, DataRxDuplicateBytes       uint64
	AckTxFrames, AckRxFrames, ActivateTxFrames, ActivateRxFrames          uint64
	CloseTxFrames, CloseRxFrames, RescueAttemptFrames, RescueAttemptBytes uint64
	FirstTxDataAt, FirstRxUniqueDataAt                                    time.Time
}

type streamTelemetryLegCounters struct {
	dataTxAttemptFrames, dataTxRetransmitFrames                           atomic.Uint64
	carrierWriteAttemptFrames, dataTxAttemptBytes                         atomic.Uint64
	dataTxRetryWriteAttemptFrames, dataTxRescueWriteAttemptFrames         atomic.Uint64
	dataTxRescueWriteAttemptBytes, dataTxCarrierWriteErrorFrames          atomic.Uint64
	dataTxCarrierWireBytes, dataTxPartialWireBytes                        atomic.Uint64
	dataRxUniqueFrames, dataRxDuplicateFrames, dataRxDuplicateBytes       atomic.Uint64
	ackTxFrames, ackRxFrames, activateTxFrames, activateRxFrames          atomic.Uint64
	closeTxFrames, closeRxFrames, rescueAttemptFrames, rescueAttemptBytes atomic.Uint64
	firstTxDataAt, firstRxUniqueDataAt                                    atomic.Int64
}

type streamStartupTelemetryState struct {
	StreamStartupTelemetrySnapshot
}

// StreamTelemetry is optional instrumentation owned by the host telemetry
// plane. A nil pointer is the disabled, allocation-free path.
type StreamTelemetry struct {
	dataTxAttemptFrames                [2]atomic.Uint64
	dataTxRetransmitFrames             [2]atomic.Uint64
	carrierWriteAttemptFrames          [2]atomic.Uint64
	dataTxAttemptBytes                 [2]atomic.Uint64
	dataTxRetryWriteAttemptFrames      [2]atomic.Uint64
	dataTxRescueWriteAttemptFrames     [2]atomic.Uint64
	dataTxRescueWriteAttemptBytes      [2]atomic.Uint64
	dataTxCarrierWriteErrorFrames      [2]atomic.Uint64
	dataTxCarrierWireBytes             [2]atomic.Uint64
	dataTxPartialWireBytes             [2]atomic.Uint64
	dataRxUniqueFrames                 [2]atomic.Uint64
	dataRxDuplicateFrames              [2]atomic.Uint64
	dataRxDuplicateBytes               [2]atomic.Uint64
	ackTxFrames                        [2]atomic.Uint64
	ackRxFrames                        [2]atomic.Uint64
	activateTxFrames                   [2]atomic.Uint64
	activateRxFrames                   [2]atomic.Uint64
	closeTxFrames                      [2]atomic.Uint64
	closeRxFrames                      [2]atomic.Uint64
	rescueAttemptFrames                [2]atomic.Uint64
	rescueAttemptBytes                 [2]atomic.Uint64
	firstTxDataAt                      [2]atomic.Int64
	firstRxUniqueDataAt                [2]atomic.Int64
	extraLegs                          sync.Map // uint8 -> *streamTelemetryLegCounters; leg 0/1 use inline counters above
	startup                            atomic.Pointer[streamStartupTelemetryState]
	probeUniqueServiceLowMu            sync.Mutex
	probeUniqueServiceLowEvents        []StreamProbeUniqueServiceLowSnapshot
	probeUniqueServiceLowEventCount    uint64
	droppedProbeUniqueServiceLowEvents uint64
	rxSequenceEventsEnabled            atomic.Bool
	rxSequenceMu                       sync.Mutex
	rxSequenceEvents                   []StreamDataRXSequenceEvent
	rxSequenceEventCount               uint64
	droppedRXSequenceEvents            uint64
}

func (t *StreamTelemetry) extraLegCounters(leg uint8) *streamTelemetryLegCounters {
	if t == nil || leg < 2 {
		return nil
	}
	if counters, ok := t.extraLegs.Load(leg); ok {
		return counters.(*streamTelemetryLegCounters)
	}
	counters := new(streamTelemetryLegCounters)
	actual, _ := t.extraLegs.LoadOrStore(leg, counters)
	return actual.(*streamTelemetryLegCounters)
}

// EnableDataRXSequenceEvents enables bounded, payload-free DATA receive
// instrumentation. Call it before constructing/starting the StreamEngine.
func (t *StreamTelemetry) EnableDataRXSequenceEvents() {
	if t != nil {
		t.rxSequenceEventsEnabled.Store(true)
	}
}

func (t *StreamTelemetry) recordDataRXSequence(event StreamDataRXSequenceEvent) {
	if t == nil || !t.rxSequenceEventsEnabled.Load() {
		return
	}
	t.rxSequenceMu.Lock()
	t.rxSequenceEventCount++
	event.EventIndex = t.rxSequenceEventCount
	if len(t.rxSequenceEvents) < StreamDataRXSequenceEventLimit {
		t.rxSequenceEvents = append(t.rxSequenceEvents, event)
	} else {
		copy(t.rxSequenceEvents, t.rxSequenceEvents[1:])
		t.rxSequenceEvents[len(t.rxSequenceEvents)-1] = event
		t.droppedRXSequenceEvents++
	}
	t.rxSequenceMu.Unlock()
}

func (t *StreamTelemetry) configureStartup(cfg StreamConfig) {
	if t == nil {
		return
	}
	state := StreamStartupTelemetrySnapshot{
		Policy:       cfg.StartupPolicy,
		PreferredLeg: -1,
		FirstDataLeg: -1,
		Grace:        0,
	}
	if cfg.StartupPolicy == StreamStartupPreferred {
		state.Applied = true
		state.PreferredLeg = int16(cfg.StartupPreferredLeg)
		state.Grace = cfg.StartupGrace
	}
	t.startup.Store(&streamStartupTelemetryState{StreamStartupTelemetrySnapshot: state})
}

func (t *StreamTelemetry) updateStartup(update func(*StreamStartupTelemetrySnapshot) bool) {
	if t == nil {
		return
	}
	for {
		current := t.startup.Load()
		if current == nil {
			return
		}
		next := *current
		if !update(&next.StreamStartupTelemetrySnapshot) {
			return
		}
		if t.startup.CompareAndSwap(current, &next) {
			return
		}
	}
}

func (t *StreamTelemetry) markStartupFirstDataWaiting(now time.Time) {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied || !state.FirstDataWaitingAt.IsZero() {
			return false
		}
		state.FirstDataWaitingAt = now
		return true
	})
}

func (t *StreamTelemetry) markStartupAttached(leg uint8, now time.Time) {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied {
			return false
		}
		if int16(leg) == state.PreferredLeg {
			if !state.PreferredAttachedAt.IsZero() {
				return false
			}
			state.PreferredAttachedAt = now
			return true
		}
		if !state.NonPreferredAttachedAt.IsZero() {
			return false
		}
		state.NonPreferredAttachedAt = now
		return true
	})
}

func (t *StreamTelemetry) markStartupGraceStarted(now time.Time) {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied || !state.GraceStartedAt.IsZero() {
			return false
		}
		state.GraceStartedAt = now
		return true
	})
}

func (t *StreamTelemetry) markStartupReleased(now time.Time, reason StreamStartupReleaseReason) {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied || !state.ReleasedAt.IsZero() || state.ReleaseReason != StartupReleaseNone {
			return false
		}
		state.ReleasedAt = now
		state.ReleaseReason = reason
		return true
	})
}

func (t *StreamTelemetry) markStartupClosedBeforeRelease() {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied || state.FirstDataWaitingAt.IsZero() || !state.ReleasedAt.IsZero() || state.ReleaseReason != StartupReleaseNone {
			return false
		}
		state.ReleaseReason = StartupReleaseSessionClosedBeforeRelease
		return true
	})
}

func (t *StreamTelemetry) markStartupFirstDataLeg(leg uint8) {
	t.updateStartup(func(state *StreamStartupTelemetrySnapshot) bool {
		if !state.Applied || state.FirstDataLeg >= 0 {
			return false
		}
		state.FirstDataLeg = int16(leg)
		return true
	})
}

func (t *StreamTelemetry) snapshot() StreamTelemetryStats {
	if t == nil {
		return StreamTelemetryStats{Startup: StreamStartupTelemetrySnapshot{PreferredLeg: -1, FirstDataLeg: -1}}
	}
	var result StreamTelemetryStats
	for leg := range 2 {
		result.DataTxAttemptFrames[leg] = t.dataTxAttemptFrames[leg].Load()
		result.DataTxRetransmitFrames[leg] = t.dataTxRetransmitFrames[leg].Load()
		result.DataTxCarrierWriteAttemptFrames[leg] = t.carrierWriteAttemptFrames[leg].Load()
		result.DataTxAttemptBytes[leg] = t.dataTxAttemptBytes[leg].Load()
		result.DataTxRetryWriteAttemptFrames[leg] = t.dataTxRetryWriteAttemptFrames[leg].Load()
		result.DataTxRescueWriteAttemptFrames[leg] = t.dataTxRescueWriteAttemptFrames[leg].Load()
		result.DataTxRescueWriteAttemptBytes[leg] = t.dataTxRescueWriteAttemptBytes[leg].Load()
		result.DataTxCarrierWriteErrorFrames[leg] = t.dataTxCarrierWriteErrorFrames[leg].Load()
		result.DataTxCarrierWireBytes[leg] = t.dataTxCarrierWireBytes[leg].Load()
		result.DataTxPartialWireBytes[leg] = t.dataTxPartialWireBytes[leg].Load()
		result.DataRxUniqueFrames[leg] = t.dataRxUniqueFrames[leg].Load()
		result.DataRxDuplicateFrames[leg] = t.dataRxDuplicateFrames[leg].Load()
		result.DataRxDuplicateBytes[leg] = t.dataRxDuplicateBytes[leg].Load()
		result.AckTxFrames[leg] = t.ackTxFrames[leg].Load()
		result.AckRxFrames[leg] = t.ackRxFrames[leg].Load()
		result.ActivateTxFrames[leg] = t.activateTxFrames[leg].Load()
		result.ActivateRxFrames[leg] = t.activateRxFrames[leg].Load()
		result.CloseTxFrames[leg] = t.closeTxFrames[leg].Load()
		result.CloseRxFrames[leg] = t.closeRxFrames[leg].Load()
		result.RescueAttemptFrames[leg] = t.rescueAttemptFrames[leg].Load()
		result.RescueAttemptBytes[leg] = t.rescueAttemptBytes[leg].Load()
		result.FirstTxDataAt[leg] = atomicTime(t.firstTxDataAt[leg].Load())
		result.FirstRxUniqueDataAt[leg] = atomicTime(t.firstRxUniqueDataAt[leg].Load())
	}
	for leg := range 2 {
		result.Legs = append(result.Legs, telemetryRowFromCompatibility(uint8(leg), &result))
	}
	t.extraLegs.Range(func(key, value any) bool {
		result.Legs = append(result.Legs, telemetryRowFromExtra(key.(uint8), value.(*streamTelemetryLegCounters)))
		return true
	})
	sort.Slice(result.Legs, func(i, j int) bool { return result.Legs[i].LegID < result.Legs[j].LegID })
	if startup := t.startup.Load(); startup != nil {
		result.Startup = startup.StreamStartupTelemetrySnapshot
	} else {
		result.Startup.PreferredLeg = -1
		result.Startup.FirstDataLeg = -1
	}
	t.probeUniqueServiceLowMu.Lock()
	result.ProbeUniqueServiceLowEvents = append([]StreamProbeUniqueServiceLowSnapshot(nil), t.probeUniqueServiceLowEvents...)
	result.DroppedProbeUniqueServiceLowEvents = t.droppedProbeUniqueServiceLowEvents
	t.probeUniqueServiceLowMu.Unlock()
	if t.rxSequenceEventsEnabled.Load() {
		t.rxSequenceMu.Lock()
		result.DataRXSequenceEvents = append([]StreamDataRXSequenceEvent(nil), t.rxSequenceEvents...)
		result.DroppedDataRXSequenceEvents = t.droppedRXSequenceEvents
		t.rxSequenceMu.Unlock()
	}
	return result
}

func telemetryRowFromCompatibility(leg uint8, stats *StreamTelemetryStats) StreamLegTelemetryStats {
	i := int(leg)
	return StreamLegTelemetryStats{
		LegID:               leg,
		DataTxAttemptFrames: stats.DataTxAttemptFrames[i], DataTxRetransmitFrames: stats.DataTxRetransmitFrames[i],
		DataTxCarrierWriteAttemptFrames: stats.DataTxCarrierWriteAttemptFrames[i], DataTxAttemptBytes: stats.DataTxAttemptBytes[i],
		DataTxRetryWriteAttemptFrames: stats.DataTxRetryWriteAttemptFrames[i], DataTxRescueWriteAttemptFrames: stats.DataTxRescueWriteAttemptFrames[i],
		DataTxRescueWriteAttemptBytes: stats.DataTxRescueWriteAttemptBytes[i], DataTxCarrierWriteErrorFrames: stats.DataTxCarrierWriteErrorFrames[i],
		DataTxCarrierWireBytes: stats.DataTxCarrierWireBytes[i], DataTxPartialWireBytes: stats.DataTxPartialWireBytes[i],
		DataRxUniqueFrames: stats.DataRxUniqueFrames[i], DataRxDuplicateFrames: stats.DataRxDuplicateFrames[i],
		DataRxDuplicateBytes: stats.DataRxDuplicateBytes[i], AckTxFrames: stats.AckTxFrames[i], AckRxFrames: stats.AckRxFrames[i],
		ActivateTxFrames: stats.ActivateTxFrames[i], ActivateRxFrames: stats.ActivateRxFrames[i],
		CloseTxFrames: stats.CloseTxFrames[i], CloseRxFrames: stats.CloseRxFrames[i],
		RescueAttemptFrames: stats.RescueAttemptFrames[i], RescueAttemptBytes: stats.RescueAttemptBytes[i],
		FirstTxDataAt: stats.FirstTxDataAt[i], FirstRxUniqueDataAt: stats.FirstRxUniqueDataAt[i],
	}
}

func telemetryRowFromExtra(leg uint8, c *streamTelemetryLegCounters) StreamLegTelemetryStats {
	return StreamLegTelemetryStats{
		LegID:               leg,
		DataTxAttemptFrames: c.dataTxAttemptFrames.Load(), DataTxRetransmitFrames: c.dataTxRetransmitFrames.Load(),
		DataTxCarrierWriteAttemptFrames: c.carrierWriteAttemptFrames.Load(), DataTxAttemptBytes: c.dataTxAttemptBytes.Load(),
		DataTxRetryWriteAttemptFrames: c.dataTxRetryWriteAttemptFrames.Load(), DataTxRescueWriteAttemptFrames: c.dataTxRescueWriteAttemptFrames.Load(),
		DataTxRescueWriteAttemptBytes: c.dataTxRescueWriteAttemptBytes.Load(), DataTxCarrierWriteErrorFrames: c.dataTxCarrierWriteErrorFrames.Load(),
		DataTxCarrierWireBytes: c.dataTxCarrierWireBytes.Load(), DataTxPartialWireBytes: c.dataTxPartialWireBytes.Load(),
		DataRxUniqueFrames: c.dataRxUniqueFrames.Load(), DataRxDuplicateFrames: c.dataRxDuplicateFrames.Load(), DataRxDuplicateBytes: c.dataRxDuplicateBytes.Load(),
		AckTxFrames: c.ackTxFrames.Load(), AckRxFrames: c.ackRxFrames.Load(), ActivateTxFrames: c.activateTxFrames.Load(), ActivateRxFrames: c.activateRxFrames.Load(),
		CloseTxFrames: c.closeTxFrames.Load(), CloseRxFrames: c.closeRxFrames.Load(), RescueAttemptFrames: c.rescueAttemptFrames.Load(), RescueAttemptBytes: c.rescueAttemptBytes.Load(),
		FirstTxDataAt: atomicTime(c.firstTxDataAt.Load()), FirstRxUniqueDataAt: atomicTime(c.firstRxUniqueDataAt.Load()),
	}
}

func (t *StreamTelemetry) recordProbeUniqueServiceLow(event StreamProbeUniqueServiceLowSnapshot) uint64 {
	if t == nil {
		return 0
	}
	t.probeUniqueServiceLowMu.Lock()
	t.probeUniqueServiceLowEventCount++
	event.EventIndex = t.probeUniqueServiceLowEventCount
	if len(t.probeUniqueServiceLowEvents) < streamProbeUniqueServiceLowHistoryLimit {
		t.probeUniqueServiceLowEvents = append(t.probeUniqueServiceLowEvents, event)
	} else {
		copy(t.probeUniqueServiceLowEvents, t.probeUniqueServiceLowEvents[1:])
		t.probeUniqueServiceLowEvents[len(t.probeUniqueServiceLowEvents)-1] = event
		t.droppedProbeUniqueServiceLowEvents++
	}
	t.probeUniqueServiceLowMu.Unlock()
	return event.EventIndex
}

func atomicTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value)
}

func (t *StreamTelemetry) markDataTx(leg uint8, retransmit, rescue bool, bytes uint64) {
	if t == nil {
		return
	}
	if leg < 2 {
		t.dataTxAttemptFrames[leg].Add(1)
		if retransmit {
			t.dataTxRetransmitFrames[leg].Add(1)
		}
		if rescue {
			t.rescueAttemptFrames[leg].Add(1)
			t.rescueAttemptBytes[leg].Add(bytes)
		}
		markFirstTimestampAt(&t.firstTxDataAt[leg], time.Now())
	} else {
		c := t.extraLegCounters(leg)
		c.dataTxAttemptFrames.Add(1)
		if retransmit {
			c.dataTxRetransmitFrames.Add(1)
		}
		if rescue {
			c.rescueAttemptFrames.Add(1)
			c.rescueAttemptBytes.Add(bytes)
		}
		markFirstTimestampAt(&c.firstTxDataAt, time.Now())
	}
	t.markStartupFirstDataLeg(leg)
}

// markDataCarrierWriteAttempt records one DATA frame entering the Carrier's
// first Write call. Payload bytes count once per frame attempt; it does not
// include the SMP3 frame header or imply that the write completed.
func (t *StreamTelemetry) markDataCarrierWriteAttempt(leg uint8, retry, rescue bool, payloadBytes uint64) {
	if t == nil {
		return
	}
	if leg < 2 {
		t.carrierWriteAttemptFrames[leg].Add(1)
		t.dataTxAttemptBytes[leg].Add(payloadBytes)
		if retry {
			t.dataTxRetryWriteAttemptFrames[leg].Add(1)
		}
		if rescue {
			t.dataTxRescueWriteAttemptFrames[leg].Add(1)
			t.dataTxRescueWriteAttemptBytes[leg].Add(payloadBytes)
		}
	} else {
		c := t.extraLegCounters(leg)
		c.carrierWriteAttemptFrames.Add(1)
		c.dataTxAttemptBytes.Add(payloadBytes)
		if retry {
			c.dataTxRetryWriteAttemptFrames.Add(1)
		}
		if rescue {
			c.dataTxRescueWriteAttemptFrames.Add(1)
			c.dataTxRescueWriteAttemptBytes.Add(payloadBytes)
		}
	}
}

// finishDataCarrierWrite records bytes reported by the frame writer and
// classifies returned errors. PartialWireBytes is a subset of CarrierWireBytes
// and counts bytes reported before an error/short write, including headers.
func (t *StreamTelemetry) finishDataCarrierWrite(leg uint8, wireBytes, expectedWireBytes uint64, attempted bool, err error) {
	if t == nil || !attempted {
		return
	}
	if leg < 2 {
		if wireBytes != 0 {
			t.dataTxCarrierWireBytes[leg].Add(wireBytes)
		}
		if err != nil {
			t.dataTxCarrierWriteErrorFrames[leg].Add(1)
			if wireBytes > 0 && wireBytes < expectedWireBytes {
				t.dataTxPartialWireBytes[leg].Add(wireBytes)
			}
		}
	} else {
		c := t.extraLegCounters(leg)
		if wireBytes != 0 {
			c.dataTxCarrierWireBytes.Add(wireBytes)
		}
		if err != nil {
			c.dataTxCarrierWriteErrorFrames.Add(1)
			if wireBytes > 0 && wireBytes < expectedWireBytes {
				c.dataTxPartialWireBytes.Add(wireBytes)
			}
		}
	}
}

func (t *StreamTelemetry) markDataRxUnique(leg uint8) {
	if t == nil {
		return
	}
	if leg < 2 {
		t.dataRxUniqueFrames[leg].Add(1)
		markFirstTimestamp(&t.firstRxUniqueDataAt[leg])
	} else {
		c := t.extraLegCounters(leg)
		c.dataRxUniqueFrames.Add(1)
		markFirstTimestamp(&c.firstRxUniqueDataAt)
	}
}

func markFirstTimestamp(target *atomic.Int64) {
	markFirstTimestampAt(target, time.Now())
}

func markFirstTimestampAt(target *atomic.Int64, now time.Time) {
	if target.Load() == 0 {
		target.CompareAndSwap(0, now.UnixNano())
	}
}

func (t *StreamTelemetry) markDataRxDuplicate(leg uint8, bytes uint64) {
	if t == nil {
		return
	}
	if leg < 2 {
		t.dataRxDuplicateFrames[leg].Add(1)
		t.dataRxDuplicateBytes[leg].Add(bytes)
	} else {
		c := t.extraLegCounters(leg)
		c.dataRxDuplicateFrames.Add(1)
		c.dataRxDuplicateBytes.Add(bytes)
	}
}

func (t *StreamTelemetry) markControlTx(leg uint8, typ byte) {
	if t == nil {
		return
	}
	var ack, activate, close *atomic.Uint64
	if leg < 2 {
		ack, activate, close = &t.ackTxFrames[leg], &t.activateTxFrames[leg], &t.closeTxFrames[leg]
	} else {
		c := t.extraLegCounters(leg)
		ack, activate, close = &c.ackTxFrames, &c.activateTxFrames, &c.closeTxFrames
	}
	switch typ {
	case frameTypeAck:
		ack.Add(1)
	case frameTypeActivate:
		activate.Add(1)
	case frameTypeClose:
		close.Add(1)
	}
}

func (t *StreamTelemetry) markControlRx(leg uint8, typ byte) {
	if t == nil {
		return
	}
	var ack, activate, close *atomic.Uint64
	if leg < 2 {
		ack, activate, close = &t.ackRxFrames[leg], &t.activateRxFrames[leg], &t.closeRxFrames[leg]
	} else {
		c := t.extraLegCounters(leg)
		ack, activate, close = &c.ackRxFrames, &c.activateRxFrames, &c.closeRxFrames
	}
	switch typ {
	case frameTypeAck:
		ack.Add(1)
	case frameTypeActivate:
		activate.Add(1)
	case frameTypeClose:
		close.Add(1)
	}
}

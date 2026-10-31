package smp3core

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameTypeData     byte = byte(StreamFrameData)
	frameTypeActivate byte = byte(StreamFrameActivate)
	frameTypeAck      byte = byte(StreamFrameACK)
	frameTypeClose    byte = byte(StreamFrameClose)
	frameTypePathRTT  byte = byte(StreamFramePathRTT)
	frameTypeDrain    byte = byte(StreamFrameDrain)
	frameHeaderSize        = StreamFrameHeaderSize
	maxFramePayload        = MaxStreamFramePayload
)

var ErrStreamClosed = errors.New("multipath core closed")
var ErrStreamLegNotFound = errors.New("multipath leg not found")
var ErrStreamLastLeg = errors.New("cannot drain the last active multipath leg")

type StreamSchedulerMode uint8

const (
	StreamSchedulerStatic StreamSchedulerMode = iota
	StreamSchedulerAdaptive
	// StreamSchedulerCapacityFirst keeps adaptive delivery feedback but does
	// not apply the latency penalty. It is an explicit experiment mode; it is
	// not a production default.
	StreamSchedulerCapacityFirst
	// StreamSchedulerAggregation enables normalized assigned-service scheduling
	// with bounded per-leg admission and frontier exposure repair.
	StreamSchedulerAggregation
)

// StreamActivationMode controls when the secondary carrier is requested.
// Legacy is the compatibility default; Dynamic is explicit opt-in.
type StreamActivationMode uint8

const (
	StreamActivationLegacy StreamActivationMode = iota
	StreamActivationDynamic
)

// StreamHOLMode controls the optional short-timescale assignment correction.
// Legacy is the compatibility default; Completion is explicit opt-in.
type StreamHOLMode uint8

const (
	StreamHOLLegacy StreamHOLMode = iota
	StreamHOLCompletion
)

const (
	schedulerStatic   = StreamSchedulerStatic
	schedulerAdaptive = StreamSchedulerAdaptive
)

type streamLegPerf struct {
	mu           sync.Mutex
	writeBPS     float64
	writeLatency time.Duration
	ackedBPS     float64
	lastAckAt    time.Time
}

func (p *streamLegPerf) observeWrite(bytes int, elapsed time.Duration) {
	if elapsed <= 0 {
		elapsed = time.Microsecond
	}
	bps := float64(bytes) / elapsed.Seconds()
	p.mu.Lock()
	if p.writeBPS == 0 {
		p.writeBPS = bps
	} else {
		p.writeBPS = p.writeBPS*0.85 + bps*0.15
	}
	if p.writeLatency == 0 {
		p.writeLatency = elapsed
	} else {
		p.writeLatency = time.Duration(float64(p.writeLatency)*0.85 + float64(elapsed)*0.15)
	}
	p.mu.Unlock()
}

func (p *streamLegPerf) observeAck(bytes int, now time.Time) {
	if bytes <= 0 {
		return
	}
	p.mu.Lock()
	if !p.lastAckAt.IsZero() {
		elapsed := now.Sub(p.lastAckAt)
		if elapsed > 0 {
			bps := float64(bytes) / elapsed.Seconds()
			if p.ackedBPS == 0 {
				p.ackedBPS = bps
			} else {
				p.ackedBPS = p.ackedBPS*0.8 + bps*0.2
			}
		}
	}
	p.lastAckAt = now
	p.mu.Unlock()
}

func (p *streamLegPerf) snapshot() (writeBPS, ackedBPS float64, latency time.Duration) {
	p.mu.Lock()
	writeBPS, ackedBPS, latency = p.writeBPS, p.ackedBPS, p.writeLatency
	p.mu.Unlock()
	return
}

type StreamConfig struct {
	frontierProbe  *benchmarkFrontierProbe
	SchedulerMode  StreamSchedulerMode
	CapacityMode   StreamCapacityMode
	ActivationMode StreamActivationMode
	HOLMode        StreamHOLMode
	// CapacityProvider optionally shares dynamic capacity observations across logical streams.
	CapacityProvider StreamCapacityProvider
	// BenchmarkFixedRatio enables a benchmark-only deterministic 1:4 ordinary
	// dispatch pattern. It is intentionally false by default and is not used by
	// production constructors.
	BenchmarkFixedRatio        bool
	BenchmarkReadyFallback     bool
	BenchmarkWeightedDeficit   bool
	BenchmarkWeightedCommit    bool
	BenchmarkAssignedCommit    bool
	BenchmarkAssignedService   bool
	BenchmarkAssignedReady     bool
	BenchmarkAssignedDecoupled bool
	BenchmarkPendingFrames     int
	benchmarkEpochRebase       bool
	assignmentTrace            *benchmarkAssignmentTrace
	BenchmarkInServiceScore    bool
	loadProbe                  *streamLoadProbe
	HandoffTelemetry           *StreamHandoffTelemetry
	wakeTelemetry              *streamWakeTelemetry
	queueRejectHook            func(*streamLeg, *StreamTXRecord, bool) // deterministic test barrier; nil in production
	rescueRejectHook           func(*StreamTXRecord, time.Time)        // test-only interleaving; nil in production
	activateNotifyHook         func()                                  // deterministic test barrier; nil in production
	closeTraceHook             func(streamCloseTraceEvent)             // deterministic CLOSE lifecycle trace; nil in production
	drainBeforeRetireHook      func(*streamLeg)                        // deterministic coordinated-drain close race; nil in production
	recoveryTraceHook          func(streamRecoveryTraceEvent) uint64   // opt-in Phase 13 causality trace; nil in production
	ChunkSize                  int
	QueueFrames                int
	ThresholdBytesPS           uint64
	ActivationWindow           time.Duration
	BandwidthMbps              []uint32
	// MaxActiveLegs bounds the number of independently attached carriers in one
	// stream session. Zero selects the safe default. LegID remains one byte on
	// the wire, so the protocol address space is at most 256 legs.
	MaxActiveLegs     int
	MaxReorderFrames  int
	MaxInflightFrames int
	AckInterval       time.Duration
	RetransmitTimeout time.Duration
	RecoveryTimeout   time.Duration
	// DrainEventObserver receives payload-free coordinated-drain transitions.
	// It runs synchronously on the Core goroutine and must not block.
	DrainEventObserver    func(StreamDrainEvent)
	ExperimentalPathAware bool
	// ExperimentalBoundedRescue is an opt-in Phase 22 experiment. No production
	// host config maps to it, and nil preserves the frozen Policy B behavior.
	ExperimentalBoundedRescue *StreamBoundedRescueConfig
	rescueEventHook           func(StreamRescueEvent) // experiment/test observer; nil in ordinary operation
	boundedRescueClock        func() time.Time        // deterministic tests only
	PathRole                  StreamPathRole
	OnActivate                func()
	OnLegDown                 func(uint8, error)
	OnFutureAck               func(next, max, count uint64)
	NotifyPeerOnActivate      bool
	Telemetry                 *StreamTelemetry
	StartupPolicy             StreamStartupPolicy
	StartupPreferredLeg       LegID
	StartupGrace              time.Duration
	startupTimerFactory       func(time.Duration) streamStartupTimer
}

const (
	DefaultMaxActiveStreamLegs = 16
	MaxStreamLegIDCount        = 256 // LegID is a uint8 on the wire.
)

func normalizeMaxActiveStreamLegs(configured int) (int, string) {
	if configured == 0 {
		return DefaultMaxActiveStreamLegs, ""
	}
	if configured < 1 || configured > MaxStreamLegIDCount {
		return DefaultMaxActiveStreamLegs, fmt.Sprintf("max_active_legs must be between 1 and %d", MaxStreamLegIDCount)
	}
	return configured, ""
}

type streamCloseTraceStage uint8

const (
	streamCloseWriteResult streamCloseTraceStage = iota + 1
	streamCloseWaitingPreferred
	streamCloseFrameDecoded
	streamCloseSequenceAccepted
)

type streamCloseTraceEvent struct {
	Stage streamCloseTraceStage
	LegID uint8
	Seq   uint64
	Err   error
	At    time.Time
}

// streamRecoveryTraceEvent is a diagnostic-only event shared by Core and
// isolated transport fixtures. The recorder assigns a total event index and
// a monotonic elapsed timestamp. It is intentionally absent unless the test
// configuration supplies recoveryTraceHook.
type streamRecoveryTraceEvent struct {
	EventIndex                  uint64                               `json:"event_index,omitempty"`
	ElapsedNS                   int64                                `json:"elapsed_ns,omitempty"`
	ObservedAt                  time.Time                            `json:"observed_at,omitempty"`
	Kind                        string                               `json:"kind"`
	Endpoint                    string                               `json:"endpoint,omitempty"`
	RecordSequence              uint64                               `json:"record_sequence,omitempty"`
	FrameSequence               uint64                               `json:"frame_sequence,omitempty"`
	FrameType                   string                               `json:"frame_type,omitempty"`
	LegID                       int16                                `json:"leg_id"`
	PeerLegID                   int16                                `json:"peer_leg_id"`
	LegGeneration               uint64                               `json:"leg_generation,omitempty"`
	RecordGeneration            uint64                               `json:"record_generation,omitempty"`
	PayloadBytes                int                                  `json:"payload_bytes,omitempty"`
	WriteBytes                  int                                  `json:"write_bytes,omitempty"`
	WriteResultBytes            int                                  `json:"write_result_bytes,omitempty"`
	CarrierAcceptedPayloadBytes uint64                               `json:"carrier_accepted_payload_bytes,omitempty"`
	AttemptStartedAt            time.Time                            `json:"attempt_started_at,omitempty"`
	ReferenceAt                 time.Time                            `json:"reference_at,omitempty"`
	PolicyAt                    time.Time                            `json:"policy_at,omitempty"`
	ActionAt                    time.Time                            `json:"action_at,omitempty"`
	StateObservedAt             time.Time                            `json:"state_observed_at,omitempty"`
	LedgerObservedAt            time.Time                            `json:"ledger_observed_at,omitempty"`
	Age                         time.Duration                        `json:"age_ns,omitempty"`
	Budget                      time.Duration                        `json:"budget_ns,omitempty"`
	StateBefore                 string                               `json:"state_before,omitempty"`
	ReasonBefore                string                               `json:"reason_before,omitempty"`
	StateAfter                  string                               `json:"state_after,omitempty"`
	ReasonAfter                 string                               `json:"reason_after,omitempty"`
	Action                      string                               `json:"action,omitempty"`
	DecisionEventIndex          uint64                               `json:"decision_event_index,omitempty"`
	LedgerPresent               bool                                 `json:"ledger_present,omitempty"`
	InTransit                   bool                                 `json:"in_transit,omitempty"`
	TransitLeg                  uint8                                `json:"transit_leg,omitempty"`
	TransitSince                time.Time                            `json:"transit_since,omitempty"`
	RescueInTransit             bool                                 `json:"rescue_in_transit,omitempty"`
	RescueLeg                   uint8                                `json:"rescue_leg,omitempty"`
	RescueSince                 time.Time                            `json:"rescue_since,omitempty"`
	FirstWriteAt                time.Time                            `json:"first_write_at,omitempty"`
	LastSentAt                  time.Time                            `json:"last_sent_at,omitempty"`
	FrontierAt                  time.Time                            `json:"frontier_at,omitempty"`
	SendCount                   uint32                               `json:"send_count,omitempty"`
	Rescue                      bool                                 `json:"rescue,omitempty"`
	AckedNext                   uint64                               `json:"acked_next,omitempty"`
	RetiredFrames               int                                  `json:"retired_frames,omitempty"`
	QueueLength                 int                                  `json:"queue_length,omitempty"`
	QueueCapacity               int                                  `json:"queue_capacity,omitempty"`
	Applied                     bool                                 `json:"applied,omitempty"`
	Error                       string                               `json:"error,omitempty"`
	ProbeServiceLow             *StreamProbeUniqueServiceLowSnapshot `json:"probe_unique_service_low,omitempty"`
	Decision                    *streamRecoveryDecisionSnapshot      `json:"decision,omitempty"`
}

// streamRecoveryDecisionSnapshot records inputs to an opt-in slow-leg
// decision. PredictedServiceRemaining is diagnostic only: it is derived from
// the completed-write EWMA and never participates in recovery policy.
type streamRecoveryDecisionSnapshot struct {
	Trigger                       string        `json:"trigger,omitempty"`
	BlockedWrite                  bool          `json:"blocked_write,omitempty"`
	WriteSlow                     bool          `json:"write_slow,omitempty"`
	PersistentLow                 bool          `json:"persistent_low,omitempty"`
	KnownSlow                     bool          `json:"known_slow,omitempty"`
	AlternateBlocked              bool          `json:"alternate_blocked,omitempty"`
	AlternateNormal               bool          `json:"alternate_normal,omitempty"`
	FrontierOverdue               bool          `json:"frontier_overdue,omitempty"`
	CarrierWriteInProgress        bool          `json:"carrier_write_in_progress,omitempty"`
	CarrierWriteLeg               int16         `json:"carrier_write_leg,omitempty"`
	CarrierWriteAge               time.Duration `json:"carrier_write_age_ns,omitempty"`
	AlternateWriteCount           uint64        `json:"alternate_write_count,omitempty"`
	OwnWriteBPS                   float64       `json:"own_write_bps,omitempty"`
	AlternateWriteBPS             float64       `json:"alternate_write_bps,omitempty"`
	OwnWriteLatency               time.Duration `json:"own_write_latency_ns,omitempty"`
	AlternateWriteLatency         time.Duration `json:"alternate_write_latency_ns,omitempty"`
	OwnEffectiveWeight            float64       `json:"own_effective_weight,omitempty"`
	AlternateEffectiveWeight      float64       `json:"alternate_effective_weight,omitempty"`
	WriteStartedAt                time.Time     `json:"write_started_at,omitempty"`
	PredictedServiceRemaining     time.Duration `json:"predicted_service_remaining_ns,omitempty"`
	PredictedServiceEstimateBasis string        `json:"predicted_service_estimate_basis,omitempty"`
	SuspectSince                  time.Time     `json:"suspect_since,omitempty"`
	SuspectObservedAt             time.Time     `json:"suspect_observed_at,omitempty"`
	AdmissionAge                  time.Duration `json:"admission_age_ns,omitempty"`
	FrontierAge                   time.Duration `json:"frontier_age_ns,omitempty"`
	RestrictionEligible           bool          `json:"restriction_eligible,omitempty"`
	RescueAction                  string        `json:"rescue_action,omitempty"`
	RescueAvoidLeg                int16         `json:"rescue_avoid_leg,omitempty"`
}

func (c *StreamEngine) traceRecoveryEvent(event streamRecoveryTraceEvent) uint64 {
	if c == nil || c.cfg.recoveryTraceHook == nil {
		return 0
	}
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now()
	}
	return c.cfg.recoveryTraceHook(event)
}

func streamRecoveryEndpoint(role StreamPathRole) string {
	switch role {
	case StreamPathRoleClient:
		return "client"
	case StreamPathRoleServer:
		return "server"
	default:
		return "unspecified"
	}
}

func (c *StreamEngine) traceCloseEvent(stage streamCloseTraceStage, legID uint8, seq uint64, err error) {
	if hook := c.cfg.closeTraceHook; hook != nil {
		hook(streamCloseTraceEvent{Stage: stage, LegID: legID, Seq: seq, Err: err, At: time.Now()})
	}
}

func (c *StreamEngine) assignedDecoupledEnabled() bool {
	return c.cfg.BenchmarkAssignedDecoupled || c.cfg.SchedulerMode == StreamSchedulerAggregation
}

// StreamHandoffTelemetry is a bounded benchmark diagnostic for ordinary DATA.
// It is nil on all production configurations and has no hot-path allocation
// when disabled.
type StreamHandoffTelemetry struct {
	mu      sync.Mutex
	samples []*StreamTXRecord
	bySeq   map[uint64]*StreamTXRecord
	gapsNs  []int64
}

func (t *StreamHandoffTelemetry) track(r *StreamTXRecord) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.bySeq == nil {
		t.bySeq = make(map[uint64]*StreamTXRecord)
	}
	if len(t.samples) < 8192 {
		t.samples = append(t.samples, r)
		t.bySeq[r.sequence] = r
	}
	t.mu.Unlock()
}

func (t *StreamHandoffTelemetry) markSerializerArrival(seq uint64, at int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if r := t.bySeq[seq]; r != nil {
		atomic.StoreInt64(&r.traceT7Ns, at)
	}
	t.mu.Unlock()
}

func (t *StreamHandoffTelemetry) addGap(gap int64) {
	if t == nil || gap <= 0 {
		return
	}
	t.mu.Lock()
	if len(t.gapsNs) < 8192 {
		t.gapsNs = append(t.gapsNs, gap)
	}
	t.mu.Unlock()
}

func (t *StreamHandoffTelemetry) snapshot() (samples []*StreamTXRecord, gaps []int64) {
	if t == nil {
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	samples = append(samples, t.samples...)
	gaps = append(gaps, t.gapsNs...)
	return
}

// StreamStats is a point-in-time, logical-stream view of a core. Counters are
// intentionally based on DATA accepted by SMP3, cumulative ACK retirement,
// and bytes delivered to the application; carrier wire bytes are not used by
// the adaptive controller.
type StreamStats struct {
	TxAckedUsefulTotal       uint64    // unique retirement, including unattributed rescue-only records
	TxWrittenBytesByLeg      [2]uint64 // successful DATA payload writes, including copies and post-ACK completion
	TxRetryWrittenBytesByLeg [2]uint64 // subset of successful writes: retry/rescue attempts
	TxSentBytesByLeg         [2]uint64
	TxAckedUsefulByLeg       [2]uint64
	TxRetransmitBytesByLeg   [2]uint64
	FrontierRescueAttempts   uint64
	OutstandingFrames        int
	OutstandingBytes         uint64
	OutstandingFramesByLeg   [2]int
	LastAckProgress          time.Time
	OldestOutstandingAge     time.Duration
	OldestOutstandingByLeg   [2]time.Duration
	// AckFrontier identifies the oldest cumulative-ACK blocker (ackedNext).
	// With a cumulative ACK wire format, later leg1 frames cannot retire while
	// an earlier leg0 frame is missing. Adaptive health must therefore blame
	// tx_ack_stall on leg1 only when the ACK frontier itself is currently owned
	// by leg1, rather than using aggregate outstanding age alone.
	AckFrontierValid     bool
	AckFrontierLeg       uint8
	AckFrontierMultiPath bool
	AckFrontierAge       time.Duration

	RxUniqueBytesByLeg [2]uint64
	RxDeliveredBytes   uint64
	RxAckNext          uint64 // next receive DATA sequence advertised by cumulative ACK
	RxPendingFrames    int
	RxPendingBytes     uint64
	RxGapAge           time.Duration
	RxMaxPendingFrames int
	RxMaxGapAge        time.Duration

	LegUp                            [2]bool
	Telemetry                        StreamTelemetryStats
	CapacityRawBPS                   [2]float64
	CapacitySmoothedBPS              [2]float64
	CapacityConfidence               [2]float64
	CapacityValid                    [2]bool
	CapacityDemand                   [2]bool
	CapacityLastSample               [2]time.Time
	CapacitySampleReason             [2]string
	CapacitySampleAge                [2]time.Duration
	OrdinaryEffectiveWeight          [2]float64 // zero when current ordinary admission is not eligible
	CapacityWeight                   [2]float64
	CapacityPending                  [2]int64
	CapacityAssignmentShare          [2]float64
	OrdinaryAssignedBytes            [2]uint64 // cumulative initial ordinary allocations; no epoch reset or copies/migrations
	OrdinaryOutstandingFrames        [2]int
	OrdinaryOutstandingBytes         [2]uint64
	OrdinaryPendingBytes             [2]uint64 // includes planner/feeder held pre-admission records
	OrdinaryProbeLimit               [2]int
	OrdinaryProbeByteLimit           [2]uint64
	OrdinaryProofBytes               [2]uint64
	OrdinaryProbeServiceBPS          [2]float64
	OrdinarySessionServiceBPS        [2]float64
	OrdinaryFeedbackEnvelopeNS       [2]int64
	OrdinaryAdmissionState           [2]string
	OrdinaryAdmissionReason          [2]string
	OrdinaryAdmissionSince           [2]time.Time
	OrdinaryProbeAfter               [2]time.Time
	OrdinaryRecoveryEpoch            [2]uint64
	LegGeneration                    [2]uint64
	ActivationDemandBPS              float64
	ActivationCapacityBPS            float64
	ActivationConfidence             float64
	ActivationOverloadRatio          float64
	ActivationOverloadAge            time.Duration
	ActivationReason                 string
	HOLCorrections                   uint64
	HOLFrontierCandidates            uint64
	HOLPredictedEarlier              uint64
	HOLMaxCorrectionStreak           uint64
	Phase19PolicyARescueSuppressions uint64 // experimental diagnostic; zero in the default candidate
	BoundedRescue                    StreamBoundedRescueStats
	// Legs contains the N-leg view. The fixed arrays above remain compatibility
	// mirrors for the first two protocol leg IDs.
	Legs []StreamLegStats
}

type StreamLegStats struct {
	LegID                                                                                   uint8
	Up                                                                                      bool
	Generation                                                                              uint64
	OutstandingFrames                                                                       int
	OldestOutstanding                                                                       time.Duration
	TxWrittenBytes, TxRetryWrittenBytes, TxSentBytes, TxAckedUsefulBytes, TxRetransmitBytes uint64
	RxUniqueBytes                                                                           uint64
	CapacityRawBPS, CapacitySmoothedBPS, CapacityConfidence                                 float64
	CapacityValid, CapacityDemand                                                           bool
	CapacityLastSample                                                                      time.Time
	CapacitySampleReason                                                                    string
	CapacitySampleAge                                                                       time.Duration
	CapacityWeight, OrdinaryEffectiveWeight                                                 float64
	CapacityPending                                                                         int64
	CapacityAssignmentShare                                                                 float64
	OrdinaryAssignedBytes                                                                   uint64
	OrdinaryOutstandingFrames                                                               int
	OrdinaryOutstandingBytes, OrdinaryPendingBytes                                          uint64
	OrdinaryProbeLimit                                                                      int
	OrdinaryProbeByteLimit, OrdinaryProofBytes                                              uint64
	OrdinaryProbeServiceBPS, OrdinarySessionServiceBPS                                      float64
	OrdinaryFeedbackEnvelopeNS                                                              int64
	OrdinaryAdmissionState, OrdinaryAdmissionReason                                         string
	OrdinaryAdmissionSince, OrdinaryProbeAfter                                              time.Time
	OrdinaryRecoveryEpoch                                                                   uint64
}

type dataFrame struct {
	seq        uint64
	data       []byte
	leg        uint8
	generation uint64
	readAt     time.Time
}

type txSendAttempt struct {
	record                   *StreamTXRecord
	rescue                   bool
	rescueCandidateID        uint64
	queuedNs                 int64
	ordinary                 bool
	started                  time.Time // identity of this ledger attempt, not a generation-wide cancellation
	rescueClass              StreamRescueClass
	boundedRescueReservation uint64
}

const streamRescueCandidateAckLinksPerRecord = 64

type streamRescueCandidateAckLink struct {
	candidateID uint64
	class       StreamRescueClass
}

type legControl struct {
	typ   byte
	value uint64
	data  []byte
	done  chan error
}

type streamLeg struct {
	writeStartedNs atomic.Int64
	activateQueued atomic.Bool
	draining       atomic.Bool
	dispatchMu     sync.RWMutex
	priorMbps      atomic.Uint32
	id             uint8
	generation     uint64
	conn           StreamLeg
	pathBinding    StreamPathBinding
	pathIdentity   StreamPathIdentity
	pathRTTEnabled bool
	drainAuth      StreamPathAuth
	drainBinding   [16]byte
	drainEnabled   bool
	send           chan txSendAttempt
	rescue         chan txSendAttempt
	control        chan legControl
	ackWake        chan struct{}
	// ackPending is cumulative and monotonic. ACK producers only publish the
	// newest value and wake this leg's writer; a blocked carrier can therefore
	// never hold up ACK delivery on another carrier.
	ackPending               atomic.Uint64
	ackForce                 atomic.Bool
	onClose                  func(error)
	once                     sync.Once
	closed                   atomic.Bool
	done                     chan struct{}
	workers                  sync.WaitGroup
	retired                  chan struct{}
	perf                     streamLegPerf
	debugWriterState         atomic.Int32 // 0 idle, 1 waiting, 2 writing
	debugLastDequeueNs       atomic.Int64
	debugLastWriteNs         atomic.Int64
	debugLastWriteDurationNs atomic.Int64
	debugOrdinaryDispatches  atomic.Uint64
	debugRescueDispatches    atomic.Uint64
	debugQueueFullRejects    atomic.Uint64
	debugRescueFullRejects   atomic.Uint64
	debugWriteErrors         atomic.Uint64
	debugQueueWaitNs         atomic.Uint64
	debugWriteBusyNs         atomic.Uint64
	debugWriteCount          atomic.Uint64
}

func (l *streamLeg) close(err error) {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.done)
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

type wireFrame struct {
	typ  byte
	seq  uint64
	data []byte
}

type StreamEngine struct {
	txAckedTotal              atomic.Uint64
	rescueCandidateSequence   atomic.Uint64
	rescueObservationStart    time.Time
	rescueCandidateAckMu      sync.Mutex
	rescueCandidateAckLinks   map[uint64][]streamRescueCandidateAckLink
	rescueCandidateAckDropped atomic.Uint64
	maxActiveLegs             int
	configError               string
	ordinaryAssignedBytes     []atomic.Uint64
	txWrittenBytes            []atomic.Uint64
	txRetryWrittenBytes       []atomic.Uint64
	admission                 streamAdmission
	legGeneration             []atomic.Uint64
	cfg                       StreamConfig
	boundedRescue             *streamBoundedRescueBudget
	boundedRescueConfigError  string
	pathRTTCache              *streamRTTProbeCache
	pathSessionID             SessionID
	pathSessionSet            bool
	drainSessionID            SessionID
	drainSessionSet           bool
	drainMu                   sync.Mutex
	drainOps                  map[uint8]*streamDrainOperation
	drainRemoteHighWater      map[streamDrainReplayKey]uint64
	drainOperationCounter     atomic.Uint64
	telemetry                 *StreamTelemetry
	appConn                   net.Conn
	pipeConn                  net.Conn

	// lifecycleMu serializes the transition into FINALIZING/DONE with leg
	// attachment. ACTIVE and graceful CLOSING still accept transport repair;
	// FINALIZING/DONE never do. Lock order is lifecycleMu -> legsMu.
	lifecycleMu sync.Mutex
	legsMu      sync.RWMutex
	legs        map[uint8]*streamLeg
	retiring    map[uint8]*streamLeg

	incoming          chan dataFrame
	done              chan struct{}
	txStopped         chan struct{}
	closeErr          atomic.Value
	closeOne          sync.Once
	capacityCloseOnce sync.Once

	closing      atomic.Bool
	finalizing   atomic.Bool
	gracefulOnce sync.Once
	gracefulCh   chan struct{}

	txLedger  *StreamTXLedger
	inflight  chan struct{}
	retryCh   chan struct{}
	spaceWake chan streamSpaceNotification
	// frontierRescueCh is a coalesced O(1) wakeup for the cumulative-ACK
	// frontier. R10 uses it when ACK progress exposes a new overdue head so the
	// next repair does not have to wait for the periodic retransmit ticker.
	frontierRescueCh chan struct{}

	ingressBytes            atomic.Uint64
	activationBacklog       atomic.Uint64
	inputBlocked            atomic.Bool
	active                  atomic.Bool
	activeCh                chan struct{}
	activateOnce            sync.Once
	activationOverloadSince atomic.Int64
	activationDemandBPS     atomic.Uint64
	activationCapacityBPS   atomic.Uint64
	activationConfidence    atomic.Uint32
	activationRatio         atomic.Uint64
	activationReason        atomic.Value
	holCorrections          atomic.Uint64
	holFrontierCandidates   atomic.Uint64
	holPredictedEarlier     atomic.Uint64
	holCorrectionStreak     atomic.Uint64
	holMaxCorrectionStreak  atomic.Uint64
	startup                 *streamStartupGate

	ackNext   atomic.Uint64
	ackForce  atomic.Bool
	ackWakeCh chan struct{}

	txSentBytes                      []atomic.Uint64
	txAckedUseful                    []atomic.Uint64
	txAckedNext                      atomic.Uint64
	txRetransmitBytes                []atomic.Uint64
	frontierRescueAttempts           atomic.Uint64
	phase19PolicyARescueSuppressions atomic.Uint64
	debugRescueChecks                atomic.Uint64
	debugRescueOverdue               atomic.Uint64
	debugRescuePlans                 atomic.Uint64
	rxUniqueBytes                    []atomic.Uint64
	rxDeliveredBytes                 atomic.Uint64
	rxPendingFrames                  atomic.Int64
	rxPendingBytes                   atomic.Uint64
	rxGapSinceUnixNs                 atomic.Int64
	rxMaxPendingFrames               atomic.Int64
	rxMaxGapAgeNs                    atomic.Int64
	lastAckProgressNs                atomic.Int64
	debugLastAckFrameNs              atomic.Int64
	debugLastAckValue                atomic.Uint64
	debugAckAdvances                 atomic.Uint64
	debugAckRetiredRecords           atomic.Uint64
	debugAckRetiredBytes             atomic.Uint64
	debugLastAdmissionNs             atomic.Int64
	debugLastDispatchNs              atomic.Int64
	debugNewDispatches               atomic.Uint64
	debugRetryScheduled              atomic.Uint64
	debugRetryDispatches             atomic.Uint64
	debugRetryRejects                atomic.Uint64
	debugRescueDispatches            atomic.Uint64
	debugRescueRejects               atomic.Uint64
	benchmarkFixedDispatch           atomic.Uint64
	debugPrimarySuccess              atomic.Uint64
	debugPrimaryFailure              atomic.Uint64
	debugFallbackAttempts            atomic.Uint64
	debugFallbackSuccess             atomic.Uint64
	debugSpaceWakeEntries            atomic.Uint64
	debugSpaceWakeWaitNs             atomic.Uint64
	weightedMu                       sync.Mutex
	capacity                         *streamCapacityEstimator
	capacityProvider                 StreamCapacityProvider
	weightedFinish                   []float64
	assignedService                  []uint64
	assignedRecords                  []uint64
	assignedEpochReady               []bool
	assignedEpochMask                uint8 // legacy first-two membership mirror for tests/telemetry
	assignedEpochOrigin              []float64
	assignedEpochWeight              []float64
	pending                          []chan *StreamTXRecord
	pendingDepth                     []atomic.Int64
	pendingMax                       []atomic.Int64
	pendingFullWaitNs                []atomic.Uint64
	plannerPendingWaitNs             atomic.Uint64
	feederBlockedWaitNs              []atomic.Uint64
	decoupledWake                    [3]chan struct{}
	decoupledStats                   benchmarkAssignmentStats
	readyStateDecisions              [4]atomic.Uint64 // both, low-only, high-only, none
	readyRaceFailures                atomic.Uint64
	debugRXExpected                  atomic.Uint64
	debugRXHighest                   atomic.Uint64

	// recoveryEpoch invalidates stale "no legs" recovery timers. A temporary
	// recovery followed by a later outage must get a fresh RecoveryTimeout, not
	// inherit the deadline from the earlier outage.
	recoveryEpoch atomic.Uint64

	bufferPool sync.Pool // RX-only buffers
}

func NewStreamEngine(cfg StreamConfig) (*StreamEngine, net.Conn) {
	maxActiveLegs, configError := normalizeMaxActiveStreamLegs(cfg.MaxActiveLegs)
	cfg.MaxActiveLegs = maxActiveLegs
	if int(cfg.StartupPreferredLeg) >= maxActiveLegs {
		configError = fmt.Sprintf("startup_preferred_leg must be less than max_active_legs (%d)", maxActiveLegs)
	}
	if cfg.ExperimentalBoundedRescue != nil && cfg.ExperimentalBoundedRescue.Enabled && maxActiveLegs > 2 {
		configError = "bounded rescue currently supports at most two active legs"
	}
	if cfg.SchedulerMode == StreamSchedulerAggregation {
		// Production aggregation is an explicit mode. Keep the older benchmark
		// switches as internal implementation flags so existing static/adaptive
		// behavior remains byte-for-byte unchanged.
		cfg.benchmarkEpochRebase = true
		if cfg.BenchmarkPendingFrames <= 0 {
			cfg.BenchmarkPendingFrames = 32
		}
		if cfg.frontierProbe == nil {
			cfg.frontierProbe = &benchmarkFrontierProbe{grace: true}
		}
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = time.Second
	}
	if cfg.MaxReorderFrames <= 0 {
		cfg.MaxReorderFrames = 2048
	}
	if cfg.MaxInflightFrames <= 0 {
		cfg.MaxInflightFrames = 1024
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = 20 * time.Millisecond
	}
	if cfg.RetransmitTimeout <= 0 {
		cfg.RetransmitTimeout = 1 * time.Second
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 15 * time.Second
	}
	appConn, pipeConn := net.Pipe()
	c := &StreamEngine{
		cfg:                   cfg,
		maxActiveLegs:         maxActiveLegs,
		configError:           configError,
		ordinaryAssignedBytes: make([]atomic.Uint64, maxActiveLegs),
		txWrittenBytes:        make([]atomic.Uint64, maxActiveLegs),
		txRetryWrittenBytes:   make([]atomic.Uint64, maxActiveLegs),
		legGeneration:         make([]atomic.Uint64, maxActiveLegs),
		txSentBytes:           make([]atomic.Uint64, maxActiveLegs),
		txAckedUseful:         make([]atomic.Uint64, maxActiveLegs),
		txRetransmitBytes:     make([]atomic.Uint64, maxActiveLegs),
		rxUniqueBytes:         make([]atomic.Uint64, maxActiveLegs),
		weightedFinish:        make([]float64, maxActiveLegs),
		assignedService:       make([]uint64, maxActiveLegs),
		assignedRecords:       make([]uint64, maxActiveLegs),
		assignedEpochReady:    make([]bool, maxActiveLegs),
		assignedEpochOrigin:   make([]float64, maxActiveLegs),
		assignedEpochWeight:   make([]float64, maxActiveLegs),
		pending:               make([]chan *StreamTXRecord, maxActiveLegs),
		pendingDepth:          make([]atomic.Int64, maxActiveLegs),
		pendingMax:            make([]atomic.Int64, maxActiveLegs),
		pendingFullWaitNs:     make([]atomic.Uint64, maxActiveLegs),
		feederBlockedWaitNs:   make([]atomic.Uint64, maxActiveLegs),
		decoupledStats: benchmarkAssignmentStats{
			admitted:    make([]atomic.Uint64, maxActiveLegs),
			feederHeld:  make([]atomic.Int64, maxActiveLegs),
			feedersDone: make([]chan struct{}, maxActiveLegs),
		},
		admission:              streamAdmission{legs: make([]streamAdmissionLeg, maxActiveLegs)},
		rescueObservationStart: time.Now(),
		telemetry:              cfg.Telemetry,
		appConn:                appConn,
		pipeConn:               pipeConn,
		legs:                   make(map[uint8]*streamLeg),
		retiring:               make(map[uint8]*streamLeg),
		drainOps:               make(map[uint8]*streamDrainOperation),
		drainRemoteHighWater:   make(map[streamDrainReplayKey]uint64),
		incoming:               make(chan dataFrame, cfg.QueueFrames*2),
		done:                   make(chan struct{}),
		txStopped:              make(chan struct{}),
		gracefulCh:             make(chan struct{}),
		activeCh:               make(chan struct{}),
		txLedger:               newStreamTXLedger(maxActiveLegs),
		inflight:               make(chan struct{}, cfg.MaxInflightFrames),
		retryCh:                make(chan struct{}, 1),
		spaceWake:              make(chan streamSpaceNotification, 1),
		frontierRescueCh:       make(chan struct{}, 1),
		ackWakeCh:              make(chan struct{}, 1),
	}
	if cfg.ExperimentalBoundedRescue != nil && cfg.ExperimentalBoundedRescue.Enabled {
		manager, err := newStreamBoundedRescueBudget(*cfg.ExperimentalBoundedRescue, uint64(cfg.ChunkSize), cfg.boundedRescueClock, cfg.rescueEventHook)
		if err != nil {
			c.boundedRescueConfigError = err.Error()
		} else {
			manager.monotonicStart = c.rescueObservationStart
			c.boundedRescue = manager
		}
	}
	if cfg.recoveryTraceHook != nil {
		c.txLedger.recoveryTraceHook = func(event streamRecoveryTraceEvent) uint64 {
			event.Endpoint = streamRecoveryEndpoint(cfg.PathRole)
			if event.LegID >= 0 && event.LegID < int16(len(c.legGeneration)) {
				event.LegGeneration = c.legGeneration[event.LegID].Load()
			}
			return cfg.recoveryTraceHook(event)
		}
	}
	if cfg.ExperimentalPathAware && (cfg.PathRole == StreamPathRoleClient || cfg.PathRole == StreamPathRoleServer) {
		c.pathRTTCache = newStreamRTTProbeCacheForLegs(maxActiveLegs)
	}
	if cfg.SchedulerMode == StreamSchedulerAggregation && (cfg.CapacityMode == StreamCapacityDynamic || cfg.CapacityProvider != nil) {
		if cfg.CapacityProvider != nil {
			c.capacityProvider = cfg.CapacityProvider
		} else {
			c.capacity = newStreamCapacityEstimatorForLegs(cfg.BandwidthMbps, cfg.ChunkSize, maxActiveLegs)
			c.capacityProvider = c.capacity
		}
		if cfg.CapacityMode == StreamCapacityDynamic {
			if dynamic, ok := c.capacityProvider.(StreamCapacityProviderN); ok {
				rows := dynamic.TelemetryByLeg()
				now := time.Now()
				for _, row := range rows {
					id := int(row.LegID)
					if id >= len(c.admission.legs) {
						continue
					}
					c.admission.legs[id].since = now
					c.admission.legs[id].reason = "initial"
					prior := float64(c.weightFor(uint8(id))) * 1e6 / 8
					if row.Valid && row.Confidence >= .5 && row.EstimateBPS < prior*.25 {
						c.admission.legs[id] = streamAdmissionLeg{restricted: true, since: now, nextProbe: now.Add(streamRestrictionCooldown), reason: "warm_low_capacity", recoveryEpoch: 1}
					}
				}
			} else {
				_, estimate, confidence, valid, _, _ := c.capacityProvider.Telemetry()
				now := time.Now()
				for id := range estimate {
					c.admission.legs[id].since = now
					c.admission.legs[id].reason = "initial"
					prior := float64(c.weightFor(uint8(id))) * 1e6 / 8
					if valid[id] && confidence[id] >= .5 && estimate[id] < prior*.25 {
						c.admission.legs[id] = streamAdmissionLeg{restricted: true, since: now, nextProbe: now.Add(streamRestrictionCooldown), reason: "warm_low_capacity", recoveryEpoch: 1}
					}
				}
			}
		}
	}
	if cfg.BenchmarkAssignedDecoupled || cfg.SchedulerMode == StreamSchedulerAggregation {
		for id := range c.decoupledWake {
			c.decoupledWake[id] = make(chan struct{}, 1)
		}
	}
	c.startup = newStreamStartupGate(cfg)
	if cfg.Telemetry != nil {
		cfg.Telemetry.configureStartup(cfg)
	}
	c.bufferPool.New = func() any {
		return make([]byte, cfg.ChunkSize)
	}
	if cfg.ThresholdBytesPS == 0 && cfg.ActivationMode != StreamActivationDynamic {
		c.activate()
	}
	go c.txLoop()
	go c.rxLoop()
	go c.activationLoop()
	go c.ackLoop()
	go c.retransmitLoop()
	return c, appConn
}

func (c *StreamEngine) emitRescueObservation(event StreamRescueEvent) {
	if c == nil || c.cfg.rescueEventHook == nil {
		return
	}
	if !c.rescueObservationStart.IsZero() {
		event.MonotonicElapsedNS = time.Since(c.rescueObservationStart).Nanoseconds()
		event.MonotonicTimeValid = true
	}
	c.cfg.rescueEventHook(event)
}

func (c *StreamEngine) AttachLeg(id LegID, conn StreamLeg, onClose func(error)) error {
	return c.attachLeg(id, conn, onClose, nil, 0)
}

// AttachLegWithBandwidthPrior attaches a new generation with its configured
// scheduler prior already installed, before the leg becomes visible to the
// assignment loop.
func (c *StreamEngine) AttachLegWithBandwidthPrior(id LegID, conn StreamLeg, onClose func(error), mbps uint32) error {
	return c.attachLeg(id, conn, onClose, nil, mbps)
}

// AttachLegWithPathAuth attaches a stream leg whose Version6 READY exchange
// was authenticated by Core. The handle contains no caller-readable key.
func (c *StreamEngine) AttachLegWithPathAuth(id LegID, conn StreamLeg, onClose func(error), auth StreamPathAuth) error {
	return c.AttachLegWithProtocolAuth(id, conn, onClose, auth)
}

// AttachLegWithProtocolAuth attaches a carrier after an authenticated Version6
// or Version7 READY exchange. RTT and coordinated drain are independent
// negotiated capabilities; each is enabled only when selected by the peer.
func (c *StreamEngine) AttachLegWithProtocolAuth(id LegID, conn StreamLeg, onClose func(error), auth StreamPathAuth) error {
	if auth.PathRTTEnabled() && (!c.cfg.ExperimentalPathAware || c.pathRTTCache == nil) {
		return ErrStreamPathNegotiation
	}
	if !auth.PathRTTEnabled() && !auth.CoordinatedDrainEnabled() {
		return ErrStreamPathNegotiation
	}
	return c.attachLeg(id, conn, onClose, &auth, 0)
}

// AttachLegWithPathAuthAndBandwidthPrior is the authenticated-RTT counterpart
// of AttachLegWithBandwidthPrior. Both the path identity and scheduler prior
// are installed before the generation is published.
func (c *StreamEngine) AttachLegWithPathAuthAndBandwidthPrior(id LegID, conn StreamLeg, onClose func(error), auth StreamPathAuth, mbps uint32) error {
	if auth.PathRTTEnabled() && (!c.cfg.ExperimentalPathAware || c.pathRTTCache == nil) {
		return ErrStreamPathNegotiation
	}
	if !auth.PathRTTEnabled() && !auth.CoordinatedDrainEnabled() {
		return ErrStreamPathNegotiation
	}
	return c.attachLeg(id, conn, onClose, &auth, mbps)
}

func (c *StreamEngine) attachLeg(id LegID, conn StreamLeg, onClose func(error), auth *StreamPathAuth, priorMbps uint32) error {
	id8 := uint8(id)
	if c.configError != "" {
		return errors.New(c.configError)
	}
	if int(id8) >= c.maxActiveLegs {
		return errInvalidLeg
	}
	for {
		c.lifecycleMu.Lock()
		if c.finalizing.Load() {
			c.lifecycleMu.Unlock()
			return ErrStreamClosed
		}
		select {
		case <-c.done:
			c.lifecycleMu.Unlock()
			return ErrStreamClosed
		default:
		}

		c.legsMu.Lock()
		if _, exists := c.legs[id8]; exists {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return errors.New("duplicate multipath leg")
		}
		if retiring := c.retiring[id8]; retiring != nil {
			retired := retiring.retired
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			// A dead same-ID leg is not replaceable until both of its worker
			// goroutines have exited. Otherwise an old worker can clear inTransit
			// state that already belongs to the new generation.
			select {
			case <-retired:
				continue
			case <-c.done:
				return ErrStreamClosed
			}
		}
		previousGeneration := c.legGeneration[id8].Load()
		if previousGeneration == ^uint64(0) {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return ErrStreamRTTGenerationReuse
		}
		generation := previousGeneration + 1
		var pathBinding StreamPathBinding
		var pathIdentity StreamPathIdentity
		pathRTTEnabled := auth != nil && auth.PathRTTEnabled()
		drainEnabled := auth != nil && auth.CoordinatedDrainEnabled()
		if auth != nil && !pathRTTEnabled && !drainEnabled {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return ErrStreamPathNegotiation
		}
		if pathRTTEnabled && (!c.cfg.ExperimentalPathAware || c.pathRTTCache == nil) {
			c.legsMu.Unlock()
			c.lifecycleMu.Unlock()
			return ErrStreamPathNegotiation
		}
		if pathRTTEnabled {
			localDirection, _, ok := c.pathDirections()
			if !ok {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return ErrStreamPathRole
			}
			if c.pathSessionSet && c.pathSessionID != auth.sessionID {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return ErrStreamPathMessage
			}
			pathBinding = StreamPathBinding{
				SessionID: auth.sessionID, LegID: id, CarrierID: fmt.Sprintf("%x/%d/%d", auth.sessionID, id8, generation),
				Generation: generation,
			}
			if !auth.validFor(pathBinding) {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return ErrStreamPathMessage
			}
			if err := c.pathRTTCache.bindLeg(pathBinding, *auth); err != nil {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return err
			}
			c.pathSessionID = auth.sessionID
			c.pathSessionSet = true
			pathIdentity = StreamPathIdentity{
				SessionID: auth.sessionID, LegID: id, CarrierID: pathBinding.CarrierID,
				Generation: generation, Direction: localDirection,
			}
			c.pathRTTCache.recordHandshakeSample(pathBinding, pathIdentity, *auth)
		}
		if drainEnabled {
			if c.drainSessionSet && c.drainSessionID != auth.sessionID {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return ErrStreamDrainMessage
			}
			if auth.drainBinding == ([16]byte{}) {
				c.legsMu.Unlock()
				c.lifecycleMu.Unlock()
				return ErrStreamDrainMessage
			}
			c.drainSessionID = auth.sessionID
			c.drainSessionSet = true
		}
		var drainAuth StreamPathAuth
		var drainBinding [16]byte
		if auth != nil {
			drainAuth = *auth
			drainBinding = auth.drainBinding
		}

		leg := &streamLeg{
			id:             id8,
			generation:     generation,
			conn:           conn,
			pathBinding:    pathBinding,
			pathIdentity:   pathIdentity,
			pathRTTEnabled: pathRTTEnabled,
			drainAuth:      drainAuth,
			drainBinding:   drainBinding,
			drainEnabled:   drainEnabled,
			send:           make(chan txSendAttempt, c.cfg.QueueFrames),
			rescue:         make(chan txSendAttempt, 8),
			control:        make(chan legControl, 8),
			ackWake:        make(chan struct{}, 1),
			onClose:        onClose,
			done:           make(chan struct{}),
			retired:        make(chan struct{}),
		}
		if priorMbps > 0 {
			leg.priorMbps.Store(priorMbps)
		} else if int(id8) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id8] > 0 {
			leg.priorMbps.Store(c.cfg.BandwidthMbps[id8])
		} else {
			leg.priorMbps.Store(1)
		}
		leg.workers.Add(2)
		if pathRTTEnabled {
			leg.workers.Add(1)
		}
		c.legs[id8] = leg
		startFeeder := c.assignedDecoupledEnabled() && c.pending[id8] == nil
		if startFeeder {
			depth := c.cfg.BenchmarkPendingFrames
			if depth <= 0 {
				depth = 32
			}
			c.pending[id8] = make(chan *StreamTXRecord, depth)
			c.decoupledStats.feedersDone[id8] = make(chan struct{})
		}
		c.admissionRejoined(id8, generation, time.Now())
		if c.capacityProvider != nil {
			c.capacityProvider.Reset(id8, time.Now())
		}
		if c.cfg.wakeTelemetry != nil {
			c.cfg.wakeTelemetry.setLeg(id8, leg)
		}
		c.legsMu.Unlock()
		c.lifecycleMu.Unlock()
		if startFeeder {
			go c.pendingFeeder(id8)
		}
		if pathRTTEnabled {
			c.queuePathRTTProbe(leg)
		}

		// Any successful join/rejoin cancels an older zero-leg recovery deadline.
		c.recoveryEpoch.Add(1)
		go func() {
			defer leg.workers.Done()
			c.legWriteLoop(leg)
		}()
		go func() {
			defer leg.workers.Done()
			c.legReadLoop(leg)
		}()
		if pathRTTEnabled {
			go func() {
				defer leg.workers.Done()
				c.pathRTTProbeLoop(leg)
			}()
		}
		if c.telemetry != nil {
			c.telemetry.markStartupAttached(id8, time.Now())
		}
		c.kickRetry()
		c.kickSpaceSource("CONNECTION_ATTACH", -1)
		c.startup.notify(id8)
		c.forceAck()
		if c.active.Load() && c.cfg.NotifyPeerOnActivate {
			c.notifyPeerActivate()
		}
		return nil
	}
}

func (c *StreamEngine) pathDirections() (local, peer StreamRTTDirection, ok bool) {
	switch c.cfg.PathRole {
	case StreamPathRoleClient:
		return StreamRTTClientToServer, StreamRTTServerToClient, true
	case StreamPathRoleServer:
		return StreamRTTServerToClient, StreamRTTClientToServer, true
	default:
		return 0, 0, false
	}
}

func (c *StreamEngine) queuePathRTTProbe(leg *streamLeg) bool {
	if leg == nil || !leg.pathRTTEnabled || c.pathRTTCache == nil {
		return false
	}
	token, wire, err := c.pathRTTCache.startProbe(leg.pathIdentity)
	if err != nil {
		return false
	}
	select {
	case leg.control <- legControl{typ: frameTypePathRTT, data: wire}:
		return true
	case <-leg.done:
		c.pathRTTCache.cancelProbe(leg.pathIdentity, token)
		return false
	default:
		c.pathRTTCache.cancelProbe(leg.pathIdentity, token)
		return false
	}
}

func (c *StreamEngine) pathRTTProbeLoop(leg *streamLeg) {
	ticker := time.NewTicker(streamRTTProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case <-ticker.C:
			c.pathRTTCache.expirePending()
			c.queuePathRTTProbe(leg)
		}
	}
}

func (c *StreamEngine) handlePathRTTFrame(leg *streamLeg, wire []byte) error {
	if !leg.pathRTTEnabled || c.pathRTTCache == nil {
		return ErrStreamPathNegotiation
	}
	_, peerDirection, ok := c.pathDirections()
	if !ok {
		return ErrStreamPathRole
	}
	if len(wire) != StreamRTTMessageSize {
		return ErrStreamPathMessage
	}
	switch StreamRTTMessageKind(wire[9]) {
	case StreamRTTProbe:
		echo, err := c.pathRTTCache.acceptProbe(leg.pathBinding, peerDirection, wire)
		if err != nil {
			return fmt.Errorf("accept same-carrier RTT probe: %w", err)
		}
		select {
		case leg.control <- legControl{typ: frameTypePathRTT, data: echo}:
		default:
			// A full control queue drops only this echo. The sender will time out
			// this unique token and retry with a later, never-reused token.
		}
	case StreamRTTEcho:
		err := c.pathRTTCache.acceptEcho(leg.pathIdentity, wire)
		if errors.Is(err, ErrStreamRTTNoPending) || errors.Is(err, ErrStreamRTTProbeExpired) || errors.Is(err, ErrStreamRTTReplay) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("accept same-carrier RTT echo kind=%d direction=%d expected=%d token=%d: %w", wire[9], wire[10], leg.pathIdentity.Direction, binary.BigEndian.Uint64(wire[28:36]), err)
		}
	default:
		return ErrStreamPathMessage
	}
	return nil
}

// ReplaceLeg intentionally tears down one carrier while keeping the logical
// core, session ID, sequence numbers, ACK state, and outstanding records
// intact. The normal handleLegFailure callback remains the single owner of
// reconnect scheduling.
func (c *StreamEngine) ReplaceLeg(id LegID, err error) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

// WaitLegRetired waits until both workers from a removed generation have
// exited. It is separate from DrainLeg because a successful DrainLeg commits
// logical removal before carrier worker cleanup finishes. Hosts that intend to
// reuse the same Leg ID must cross this barrier before dialing or attaching the
// replacement generation.
func (c *StreamEngine) WaitLegRetired(ctx context.Context, id LegID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || int(id) >= c.maxActiveLegs {
		return ErrStreamLegNotFound
	}
	for {
		c.legsMu.RLock()
		retiring := c.retiring[uint8(id)]
		active := c.legs[uint8(id)]
		c.legsMu.RUnlock()
		if active != nil {
			return errors.New("multipath leg is still active")
		}
		if retiring == nil {
			return nil
		}
		select {
		case <-retiring.retired:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return ErrStreamClosed
		}
	}
}

// SetLegBandwidthPrior updates only the named live carrier's scheduler prior.
// It is used by hosts when a new route is added to an existing Core Session;
// it does not change the Core policy or the configured rescue budget.
func (c *StreamEngine) SetLegBandwidthPrior(id LegID, mbps uint32) bool {
	if c == nil || mbps == 0 {
		return false
	}
	leg := c.getLeg(uint8(id))
	if leg == nil || leg.closed.Load() {
		return false
	}
	leg.priorMbps.Store(mbps)
	return true
}

func (c *StreamEngine) AppConn() net.Conn     { return c.appConn }
func (c *StreamEngine) Done() <-chan struct{} { return c.done }

// PathFeedback returns the immutable, fresh RTT snapshot for the active leg.
// It performs no I/O and is safe for host telemetry and tests.
func (c *StreamEngine) PathFeedback(id LegID) (StreamPathFeedback, bool) {
	if c == nil || c.pathRTTCache == nil || int(id) >= c.maxActiveLegs {
		return StreamPathFeedback{}, false
	}
	leg := c.getLeg(uint8(id))
	if leg == nil || !leg.pathRTTEnabled || leg.closed.Load() {
		return StreamPathFeedback{}, false
	}
	return c.pathRTTCache.PathFeedback(leg.pathIdentity)
}

func (c *StreamEngine) Snapshot() StreamStats {
	now := time.Now()
	stats := StreamStats{
		TxSentBytesByLeg:       [2]uint64{loadAtomicUint64At(c.txSentBytes, 0), loadAtomicUint64At(c.txSentBytes, 1)},
		TxAckedUsefulByLeg:     [2]uint64{loadAtomicUint64At(c.txAckedUseful, 0), loadAtomicUint64At(c.txAckedUseful, 1)},
		TxRetransmitBytesByLeg: [2]uint64{loadAtomicUint64At(c.txRetransmitBytes, 0), loadAtomicUint64At(c.txRetransmitBytes, 1)},
		FrontierRescueAttempts: c.frontierRescueAttempts.Load(),
		RxUniqueBytesByLeg:     [2]uint64{loadAtomicUint64At(c.rxUniqueBytes, 0), loadAtomicUint64At(c.rxUniqueBytes, 1)},
		RxDeliveredBytes:       c.rxDeliveredBytes.Load(),
		RxAckNext:              c.ackNext.Load(),
		RxPendingFrames:        int(c.rxPendingFrames.Load()),
		RxPendingBytes:         c.rxPendingBytes.Load(),
		RxMaxPendingFrames:     int(c.rxMaxPendingFrames.Load()),
		RxMaxGapAge:            time.Duration(c.rxMaxGapAgeNs.Load()),
	}
	stats.Legs = make([]StreamLegStats, c.maxActiveLegs)
	for id := range stats.Legs {
		stats.Legs[id].LegID = uint8(id)
	}
	stats.Telemetry = c.telemetry.snapshot()
	c.capacityStats(&stats)
	if timestamp := c.lastAckProgressNs.Load(); timestamp != 0 {
		stats.LastAckProgress = time.Unix(0, timestamp)
	}
	if gapSince := c.rxGapSinceUnixNs.Load(); gapSince != 0 {
		stats.RxGapAge = now.Sub(time.Unix(0, gapSince))
		if stats.RxGapAge < 0 {
			stats.RxGapAge = 0
		}
	}

	txSnapshot := c.txLedger.Snapshot(now)
	stats.OutstandingFrames = txSnapshot.OutstandingFrames
	stats.OutstandingBytes = txSnapshot.OutstandingBytes
	stats.OutstandingFramesByLeg = txSnapshot.OutstandingFramesByLeg
	stats.OldestOutstandingAge = txSnapshot.OldestOutstandingAge
	stats.OldestOutstandingByLeg = txSnapshot.OldestOutstandingByLeg
	stats.AckFrontierValid = txSnapshot.AckFrontierValid
	stats.AckFrontierLeg = uint8(txSnapshot.AckFrontierLeg)
	stats.AckFrontierMultiPath = txSnapshot.AckFrontierMultiPath
	stats.AckFrontierAge = txSnapshot.AckFrontierAge
	for id, leg := range txSnapshot.PerLeg {
		if id < len(stats.Legs) {
			stats.Legs[id].OutstandingFrames, stats.Legs[id].OldestOutstanding = leg.OutstandingFrames, leg.OldestOutstanding
		}
	}

	for id := range stats.LegUp {
		stats.LegUp[id] = c.hasLeg(uint8(id))
	}
	if stats.LastAckProgress.IsZero() && stats.OutstandingFrames > 0 {
		stats.LastAckProgress = now.Add(-stats.OldestOutstandingAge)
	}
	return stats
}

// TelemetrySnapshot skips the full TX ledger/debug scan. Capacity/admission
// diagnostics use bounded O(1) lock reads alongside atomic protocol counters.
func (c *StreamEngine) TelemetrySnapshot() StreamStats {
	stats := StreamStats{
		FrontierRescueAttempts: c.frontierRescueAttempts.Load(),
		RxPendingFrames:        int(c.rxPendingFrames.Load()),
		RxPendingBytes:         c.rxPendingBytes.Load(),
		RxMaxPendingFrames:     int(c.rxMaxPendingFrames.Load()),
		TxSentBytesByLeg:       [2]uint64{loadAtomicUint64At(c.txSentBytes, 0), loadAtomicUint64At(c.txSentBytes, 1)},
		TxAckedUsefulByLeg:     [2]uint64{loadAtomicUint64At(c.txAckedUseful, 0), loadAtomicUint64At(c.txAckedUseful, 1)},
		TxRetransmitBytesByLeg: [2]uint64{loadAtomicUint64At(c.txRetransmitBytes, 0), loadAtomicUint64At(c.txRetransmitBytes, 1)},
		RxUniqueBytesByLeg:     [2]uint64{loadAtomicUint64At(c.rxUniqueBytes, 0), loadAtomicUint64At(c.rxUniqueBytes, 1)},
		RxDeliveredBytes:       c.rxDeliveredBytes.Load(),
		RxAckNext:              c.ackNext.Load(),
		Telemetry:              c.telemetry.snapshot(),
	}
	stats.Legs = make([]StreamLegStats, c.maxActiveLegs)
	for id := range stats.Legs {
		stats.Legs[id].LegID = uint8(id)
	}
	c.capacityStats(&stats)
	if timestamp := c.lastAckProgressNs.Load(); timestamp != 0 {
		stats.LastAckProgress = time.Unix(0, timestamp)
	}
	for id := range stats.LegUp {
		stats.LegUp[id] = c.hasLeg(uint8(id))
	}
	return stats
}

// capacityStats leaves legacy snapshots free of capacity accounting locks.
func (c *StreamEngine) capacityStats(stats *StreamStats) {
	stats.TxAckedUsefulTotal = c.txAckedTotal.Load()
	if c.boundedRescue != nil {
		stats.BoundedRescue = c.boundedRescue.Snapshot(c.boundedRescueNow())
	} else if c.boundedRescueConfigError != "" {
		stats.BoundedRescue.ConfigError = c.boundedRescueConfigError
	}
	if c.slowLegProtection() {
		framesByLeg := c.txLedger.OrdinaryFramesByLeg()
		outstandingByLeg, pendingByLeg := c.txLedger.OrdinaryAdmissionBytesByLeg()
		for id := 0; id < 2 && id < len(framesByLeg); id++ {
			stats.OrdinaryOutstandingFrames[id] = framesByLeg[id]
			stats.OrdinaryOutstandingBytes[id], stats.OrdinaryPendingBytes[id] = outstandingByLeg[id], pendingByLeg[id]
		}
		c.admission.mu.Lock()
		for id, s := range c.admission.legs {
			if id >= len(stats.OrdinaryAdmissionState) {
				continue
			}
			state := "normal"
			if s.restricted {
				state = "restricted"
			} else if s.recovering {
				state = "recovering"
			}
			stats.OrdinaryAdmissionState[id], stats.OrdinaryAdmissionReason[id] = state, s.reason
			stats.OrdinaryAdmissionSince[id], stats.OrdinaryProbeAfter[id] = s.since, s.nextProbe
			stats.OrdinaryRecoveryEpoch[id] = s.recoveryEpoch
			stats.OrdinaryProbeLimit[id] = s.limit
			stats.OrdinaryProbeByteLimit[id] = s.byteLimit
			stats.OrdinaryProofBytes[id] = s.proofBytes
			stats.OrdinaryProbeServiceBPS[id] = s.probeRate
			stats.OrdinarySessionServiceBPS[id] = s.serviceRate
			stats.OrdinaryFeedbackEnvelopeNS[id] = int64(s.feedbackMax)
		}
		c.admission.mu.Unlock()
	}
	for id := range stats.OrdinaryAssignedBytes {
		if id >= len(c.txWrittenBytes) || id >= len(c.txRetryWrittenBytes) || id >= len(c.ordinaryAssignedBytes) || id >= len(c.legGeneration) {
			continue
		}
		stats.TxWrittenBytesByLeg[id] = c.txWrittenBytes[id].Load()
		stats.TxRetryWrittenBytesByLeg[id] = c.txRetryWrittenBytes[id].Load()
		stats.OrdinaryAssignedBytes[id] = c.ordinaryAssignedBytes[id].Load()
		stats.LegGeneration[id] = c.legGeneration[id].Load()
	}
	if c.capacityProvider != nil {
		stats.CapacityRawBPS, stats.CapacitySmoothedBPS, stats.CapacityConfidence,
			stats.CapacityValid, stats.CapacityDemand, stats.CapacityLastSample = c.capacityProvider.Telemetry()
		if source, ok := c.capacityProvider.(interface{ SampleReason() [2]string }); ok {
			stats.CapacitySampleReason = source.SampleReason()
		}
		if source, ok := c.capacityProvider.(StreamCapacityProviderN); ok {
			for _, sample := range source.TelemetryByLeg() {
				id := int(sample.LegID)
				if id >= len(stats.Legs) {
					continue
				}
				row := &stats.Legs[id]
				row.CapacityRawBPS, row.CapacitySmoothedBPS, row.CapacityConfidence = sample.RawBPS, sample.EstimateBPS, sample.Confidence
				row.CapacityValid, row.CapacityDemand = sample.Valid, sample.Demand
				row.CapacityLastSample, row.CapacitySampleReason = sample.SampledAt, sample.SampleReason
			}
			if reasons := source.SampleReasonByLeg(); len(reasons) > 0 {
				for id, reason := range reasons {
					if id < len(stats.Legs) && stats.Legs[id].CapacitySampleReason == "" {
						stats.Legs[id].CapacitySampleReason = reason
					}
				}
			}
		}
	}
	for id := range stats.CapacityWeight {
		if id >= len(c.pendingDepth) {
			continue
		}
		if leg := c.getLeg(uint8(id)); leg != nil && !leg.closed.Load() {
			stats.CapacityWeight[id] = c.effectiveSchedulerWeight(leg)
			if c.ordinaryEligible(uint8(id), stats.OrdinaryOutstandingFrames[id], time.Now()) {
				stats.OrdinaryEffectiveWeight[id] = stats.CapacityWeight[id]
			}
		}
		if !stats.CapacityLastSample[id].IsZero() {
			age := time.Since(stats.CapacityLastSample[id])
			if age > 0 {
				stats.CapacitySampleAge[id] = age
			}
		}
		stats.CapacityPending[id] = c.pendingDepth[id].Load()
	}
	assigned, _ := c.assignedSnapshotByLeg()
	var assignedTotal uint64
	for _, value := range assigned {
		assignedTotal += value
	}
	if assignedTotal > 0 {
		for id, value := range assigned {
			share := float64(value) / float64(assignedTotal)
			if id < 2 {
				stats.CapacityAssignmentShare[id] = share
			}
			if id < len(stats.Legs) {
				stats.Legs[id].CapacityAssignmentShare = share
			}
		}
	}
	framesByLeg := c.txLedger.OrdinaryFramesByLeg()
	outstandingByLeg, pendingByLeg := c.txLedger.OrdinaryAdmissionBytesByLeg()
	for id := range stats.Legs {
		row := &stats.Legs[id]
		row.Up = c.hasLeg(uint8(id))
		row.Generation = c.legGeneration[id].Load()
		row.TxWrittenBytes, row.TxRetryWrittenBytes = c.txWrittenBytes[id].Load(), c.txRetryWrittenBytes[id].Load()
		row.TxSentBytes, row.TxAckedUsefulBytes = c.txSentBytes[id].Load(), c.txAckedUseful[id].Load()
		row.TxRetransmitBytes, row.RxUniqueBytes = c.txRetransmitBytes[id].Load(), c.rxUniqueBytes[id].Load()
		row.OrdinaryAssignedBytes = c.ordinaryAssignedBytes[id].Load()
		row.CapacityPending = c.pendingDepth[id].Load()
		if id < len(framesByLeg) {
			row.OrdinaryOutstandingFrames = framesByLeg[id]
		}
		if id < len(outstandingByLeg) {
			row.OrdinaryOutstandingBytes, row.OrdinaryPendingBytes = outstandingByLeg[id], pendingByLeg[id]
		}
		if id < len(stats.Legs) {
			if leg := c.getLeg(uint8(id)); leg != nil && !leg.closed.Load() {
				row.CapacityWeight = c.effectiveSchedulerWeight(leg)
				if id < len(framesByLeg) && c.ordinaryEligible(uint8(id), framesByLeg[id], time.Now()) {
					row.OrdinaryEffectiveWeight = row.CapacityWeight
				}
			}
			if row.CapacityLastSample.IsZero() && id < 2 {
				row.CapacityLastSample = stats.CapacityLastSample[id]
			}
			if !row.CapacityLastSample.IsZero() {
				if age := time.Since(row.CapacityLastSample); age > 0 {
					row.CapacitySampleAge = age
				}
			}
			if id < len(c.admission.legs) {
				c.admission.mu.Lock()
				state := c.admission.legs[id]
				c.admission.mu.Unlock()
				row.OrdinaryProbeLimit, row.OrdinaryProbeByteLimit, row.OrdinaryProofBytes = state.limit, state.byteLimit, state.proofBytes
				row.OrdinaryProbeServiceBPS, row.OrdinarySessionServiceBPS = state.probeRate, state.serviceRate
				row.OrdinaryFeedbackEnvelopeNS = int64(state.feedbackMax)
				row.OrdinaryAdmissionState, row.OrdinaryAdmissionReason = "normal", state.reason
				if state.restricted {
					row.OrdinaryAdmissionState = "restricted"
				} else if state.recovering {
					row.OrdinaryAdmissionState = "recovering"
				}
				row.OrdinaryAdmissionSince, row.OrdinaryProbeAfter, row.OrdinaryRecoveryEpoch = state.since, state.nextProbe, state.recoveryEpoch
			}
		}
	}
	stats.ActivationDemandBPS = math.Float64frombits(c.activationDemandBPS.Load())
	stats.ActivationCapacityBPS = math.Float64frombits(c.activationCapacityBPS.Load())
	stats.ActivationConfidence = float64(math.Float32frombits(c.activationConfidence.Load()))
	stats.ActivationOverloadRatio = math.Float64frombits(c.activationRatio.Load())
	if since := c.activationOverloadSince.Load(); since != 0 {
		stats.ActivationOverloadAge = time.Since(time.Unix(0, since))
		if stats.ActivationOverloadAge < 0 {
			stats.ActivationOverloadAge = 0
		}
	}
	if reason := c.activationReason.Load(); reason != nil {
		stats.ActivationReason, _ = reason.(string)
	}
	stats.HOLCorrections = c.holCorrections.Load()
	stats.HOLFrontierCandidates = c.holFrontierCandidates.Load()
	stats.HOLPredictedEarlier = c.holPredictedEarlier.Load()
	stats.HOLMaxCorrectionStreak = c.holMaxCorrectionStreak.Load()
	stats.Phase19PolicyARescueSuppressions = c.phase19PolicyARescueSuppressions.Load()
}

func loadAtomicUint64At(values []atomic.Uint64, index int) uint64 {
	if index < 0 || index >= len(values) {
		return 0
	}
	return values[index].Load()
}

func (c *StreamEngine) Close() error {
	// Explicit Core shutdown is not a carrier-health signal or a protocol EOF.
	// If graceful completion already won the lifecycle race, its pipe close keeps
	// the application's pending Read observable as EOF. Otherwise fail records a
	// forced-close error and closes the application endpoint.
	c.closing.Store(true)
	c.releaseCapacityProvider()
	c.fail(ErrStreamClosed)
	return nil
}

func (c *StreamEngine) releaseCapacityProvider() {
	c.capacityCloseOnce.Do(func() {
		if releaser, ok := c.capacityProvider.(interface{ Close() }); ok {
			releaser.Close()
		}
	})
}

// StartGracefulClose terminates a logical stream without discarding payload that
// has already been accepted from the local application. The TX loop is stopped,
// outstanding DATA is allowed to drain while ACK progress continues, and a
// logical CLOSE control frame is emitted before the transport legs are torn down.
//
// This is intentionally different from fail(): fail is for fatal/shutdown paths
// and may drop outstanding payload; StartGracefulClose is for ordinary routed TCP
// EOF/close where already-written bytes must reach the peer first.
func (c *StreamEngine) StartGracefulClose(err error) {
	if err == nil {
		err = io.EOF
	}
	c.closing.Store(true)
	c.gracefulOnce.Do(func() {
		c.lifecycleMu.Lock()
		select {
		case <-c.done:
			c.lifecycleMu.Unlock()
			return
		default:
		}
		close(c.gracefulCh)
		// Close the Core endpoint of the application pipe. This wakes txLoop but
		// leaves the application's endpoint readable, so protocol EOF is visible
		// as io.EOF instead of ErrClosedPipe.
		_ = c.pipeConn.Close()
		c.lifecycleMu.Unlock()
		go func(closeErr error) {
			select {
			case <-c.done:
				return
			case <-c.txStopped:
			}
			if drainErr := c.drainOutstandingOnClose(); drainErr != nil {
				if !errors.Is(drainErr, ErrStreamClosed) {
					c.fail(drainErr)
				}
				return
			}

			// From this point on, leg loss is part of normal final teardown and must
			// not start another reconnect/recovery cycle. CLOSE is broadcast because
			// it is the only logical EOF marker and should survive loss of one leg.
			if !c.beginFinalizing() {
				return
			}
			if !c.sendCloseFrame(c.txLedger.NextSequence()) {
				c.fail(errors.New("stream CLOSE notification did not complete within recovery bound"))
				return
			}
			c.finishGracefulClose(closeErr)
		}(err)
	})
}

func (c *StreamEngine) beginFinalizing() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	select {
	case <-c.done:
		return false
	default:
	}
	c.finalizing.Store(true)
	return true
}

func (c *StreamEngine) fail(err error) {
	c.failWithAppClose(err, true)
}

func (c *StreamEngine) finishGracefulClose(err error) {
	if err == nil {
		err = io.EOF
	}
	if !errors.Is(err, io.EOF) {
		c.fail(err)
		return
	}
	c.failWithAppClose(err, false)
}

func (c *StreamEngine) failWithAppClose(err error, closeApp bool) {
	if err == nil {
		err = ErrStreamClosed
	}
	c.releaseCapacityProvider()
	if c.boundedRescue != nil {
		c.boundedRescue.Close()
	}
	if c.pathRTTCache != nil {
		c.pathRTTCache.close()
	}
	c.closeOne.Do(func() {
		if c.startup != nil {
			c.startup.markSessionClosed()
		}
		// This lock makes FINALIZING/DONE atomic with AttachLeg(). A leg is either
		// installed before shutdown snapshots transports, or rejected after the
		// lifecycle transition; it can never appear in between.
		c.lifecycleMu.Lock()
		c.finalizing.Store(true)
		c.closeErr.Store(err)
		close(c.done)
		c.rescueCandidateAckMu.Lock()
		c.rescueCandidateAckLinks = nil
		c.rescueCandidateAckMu.Unlock()
		if closeApp {
			// Close the application endpoint first so an in-flight application
			// Read cannot observe the peer-pipe EOF that follows and misclassify
			// an abnormal termination as a normal logical EOF.
			_ = c.appConn.Close()
		}
		_ = c.pipeConn.Close()
		c.legsMu.RLock()
		legs := make([]*streamLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		c.lifecycleMu.Unlock()
		for _, leg := range legs {
			leg.close(err)
		}
	})
}

func (c *StreamEngine) getBuffer() []byte { return c.bufferPool.Get().([]byte) }
func (c *StreamEngine) putBuffer(buffer []byte) {
	if cap(buffer) < c.cfg.ChunkSize {
		return
	}
	c.bufferPool.Put(buffer[:c.cfg.ChunkSize])
}

func (c *StreamEngine) acquireInflight() bool {
	select {
	case c.inflight <- struct{}{}:
		return true
	default:
	}
	c.inputBlocked.Store(true)
	defer c.inputBlocked.Store(false)
	select {
	case c.inflight <- struct{}{}:
		return true
	case <-c.gracefulCh:
		return false
	case <-c.done:
		return false
	}
}

func (c *StreamEngine) releaseInflight(count int) {
	for range count {
		select {
		case <-c.inflight:
		default:
			return
		}
	}
}

func (c *StreamEngine) txLoop() {
	defer close(c.txStopped)
	for {
		if !c.acquireInflight() {
			return
		}
		buffer := make([]byte, c.cfg.ChunkSize)
		n, err := c.pipeConn.Read(buffer)
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			c.activationBacklog.Add(uint64(n))
			if provider, ok := c.capacityProvider.(StreamActivationProvider); ok {
				provider.ObserveActivationDemand(uint64(n), 0, time.Now())
			}
			if c.startup != nil && c.telemetry != nil {
				c.telemetry.markStartupFirstDataWaiting(time.Now())
			}
			startupHint := int16(-1)
			if c.startup != nil && !c.startup.released.Load() {
				var released bool
				startupHint, released = c.startup.await(c)
				if !released {
					c.putBuffer(buffer)
					c.releaseInflight(1)
					return
				}
			}
			record := c.txLedger.addLocal(buffer[:n], time.Now())
			if c.boundedRescue != nil {
				c.boundedRescue.RecordOriginal(record.Sequence(), uint64(n), record.createdAt)
			}
			if p := c.cfg.frontierProbe; p != nil {
				p.observe(c.txLedger.frontierSnapshot(), time.Now())
			}
			if c.cfg.HandoffTelemetry != nil {
				record.trace = c.cfg.HandoffTelemetry
				record.traceT0Ns = record.createdAt.UnixNano()
				c.cfg.HandoffTelemetry.track(record)
			}
			if record.trace != nil {
				record.traceT1Ns = time.Now().UnixNano()
			}
			c.debugLastAdmissionNs.Store(time.Now().UnixNano())
			var enqueueErr error
			if startupHint >= 0 {
				enqueueErr = c.enqueueRecordWithHint(record, -1, startupHint)
			} else {
				enqueueErr = c.enqueueRecord(record, -1)
			}
			c.txLedger.finishLocalEnqueue(record)
			if enqueueErr != nil {
				c.fail(enqueueErr)
				return
			}
		} else {
			c.releaseInflight(1)
		}
		if err != nil {
			// A local application/router close is not a transport failure. Preserve
			// every frame already accepted from appConn and finish the stream only
			// after cumulative ACKs have drained the outstanding window.
			c.StartGracefulClose(err)
			return
		}
	}
}

// drainOutstandingOnClose has no fixed total deadline. A large/slow tail is
// allowed to take arbitrarily long as long as cumulative ACKs keep making
// progress. RecoveryTimeout is instead used as a *stall* timeout: if no ACK/TX
// retirement progress occurs for that long, the graceful close is considered
// wedged and the logical stream fails. This avoids alpha2/alpha2.1's 5-second
// absolute cap truncating large responses that were still draining normally.
func (c *StreamEngine) drainOutstandingOnClose() error {
	stallTimeout := c.cfg.RecoveryTimeout
	if stallTimeout <= 0 {
		stallTimeout = 15 * time.Second
	}
	if stallTimeout < 100*time.Millisecond {
		stallTimeout = 100 * time.Millisecond
	}

	// A timer channel is only a wake-up mechanism here, never proof that progress
	// actually stalled. Under scheduler pressure an ACK can retire DATA after the
	// timer becomes readable but before this goroutine runs; treating the stale
	// timer event as authoritative caused false graceful-drain failures. Poll the
	// protected ACK/outstanding state first and measure the stall interval from the
	// last state we observed making progress.
	pollInterval := 10 * time.Millisecond
	if quarter := stallTimeout / 4; quarter > 0 && quarter < pollInterval {
		pollInterval = quarter
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastProgress := time.Now()
	lastRemaining := -1
	var lastAck uint64
	for {
		progress := c.txLedger.ProgressSnapshot()
		remaining := progress.Outstanding
		acked := progress.AckedNext
		if remaining == 0 {
			return nil
		}

		now := time.Now()
		if lastRemaining < 0 || remaining < lastRemaining || acked > lastAck {
			lastProgress = now
		}
		lastRemaining = remaining
		lastAck = acked
		if now.Sub(lastProgress) >= stallTimeout {
			return errors.New("multipath graceful drain stalled waiting for ACK progress")
		}

		select {
		case <-c.done:
			return ErrStreamClosed
		case <-ticker.C:
		}
	}
}

func streamActivationRateBytesPS(txDelta, rxDelta uint64, elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	txRate := uint64(float64(txDelta) / elapsed.Seconds())
	rxRate := uint64(float64(rxDelta) / elapsed.Seconds())
	if rxRate > txRate {
		return rxRate
	}
	return txRate
}

func streamActivationEligible(txDelta, rxDelta uint64, elapsed time.Duration, thresholdBytesPS uint64) bool {
	return streamActivationRateBytesPS(txDelta, rxDelta, elapsed) >= thresholdBytesPS
}

func (c *StreamEngine) activationLoop() {
	if c.active.Load() {
		return
	}
	interval := c.cfg.ActivationWindow / 10
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	windowStart := time.Now()
	txWindowBase := c.ingressBytes.Load()
	rxWindowBase := c.rxDeliveredBytes.Load()
	var queueHighSince time.Time
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			if c.active.Load() {
				return
			}
			// A logical stream that has already entered graceful CLOSING and has
			// no TX tail left must not wake a lazy booster from stale ingress/queue
			// history. If outstanding DATA remains, activation is still allowed so
			// transport repair can finish the graceful drain.
			if c.closing.Load() && !c.hasOutstanding() {
				return
			}
			if c.stalledActivationReady(now) {
				c.activationReason.Store("blocked_input_no_delivery")
				c.activate()
				return
			}
			if c.cfg.ActivationMode == StreamActivationDynamic {
				if c.dynamicActivationReady(now, &windowStart, txWindowBase, rxWindowBase) {
					c.activate()
					return
				}
			} else if now.Sub(windowStart) >= c.cfg.ActivationWindow {
				txBytesNow := c.ingressBytes.Load()
				rxBytesNow := c.rxDeliveredBytes.Load()
				elapsed := now.Sub(windowStart)
				if streamActivationEligible(txBytesNow-txWindowBase, rxBytesNow-rxWindowBase, elapsed, c.cfg.ThresholdBytesPS) {
					c.activationReason.Store("legacy_threshold")
					c.activate()
					return
				}
				windowStart = now
				txWindowBase = txBytesNow
				rxWindowBase = rxBytesNow
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			if len(primary.send)*5 >= cap(primary.send)*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					c.activationReason.Store("queue_congestion")
					c.activate()
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

const (
	activationSafetyMargin    = 1.10
	activationStrongOverload  = 1.50
	activationStrongEvidence  = 200 * time.Millisecond
	activationBacklogFraction = 0.25
)

func (c *StreamEngine) dynamicActivationReady(now time.Time, windowStart *time.Time, txBase, rxBase uint64) bool {
	demandRate := 0.0
	backlog := c.activationBacklog.Load()
	providerDemand := false
	if provider, ok := c.capacityProvider.(StreamActivationProvider); ok {
		demandRate, backlog = provider.ActivationDemand(now)
		providerDemand = true
	}
	if !providerDemand && demandRate <= 0 {
		elapsed := now.Sub(*windowStart)
		if elapsed > 0 {
			tx := c.ingressBytes.Load() - txBase
			rx := c.rxDeliveredBytes.Load() - rxBase
			if rx > tx {
				tx = rx
			}
			demandRate = float64(tx) / elapsed.Seconds()
		}
	}
	if backlog > 0 {
		window := c.cfg.ActivationWindow
		if window <= 0 {
			window = time.Second
		}
		backlogRate := float64(backlog) / window.Seconds()
		if backlogRate > demandRate {
			demandRate = backlogRate
		}
	}
	capacity := 0.0
	confidence := 0.0
	valid := false
	if provider, ok := c.capacityProvider.(StreamActivationProvider); ok {
		capacity, confidence, valid = provider.ActivationCapacity()
	}
	if capacity <= 0 && len(c.cfg.BandwidthMbps) > 0 && c.cfg.BandwidthMbps[0] > 0 {
		capacity = float64(c.cfg.BandwidthMbps[0]) * 1e6 / 8
	}
	if capacity <= 0 {
		return false
	}
	if !valid {
		confidence = 0
	}
	ratio := demandRate / capacity
	c.activationDemandBPS.Store(math.Float64bits(demandRate))
	c.activationCapacityBPS.Store(math.Float64bits(capacity))
	c.activationConfidence.Store(math.Float32bits(float32(confidence)))
	c.activationRatio.Store(math.Float64bits(ratio))
	if ratio <= activationSafetyMargin {
		c.activationOverloadSince.Store(0)
		c.activationReason.Store("")
		return false
	}
	since := c.activationOverloadSince.Load()
	if since == 0 {
		c.activationOverloadSince.Store(now.UnixNano())
		return false
	}
	required := c.cfg.ActivationWindow
	if required <= 0 {
		required = time.Second
	}
	// Strong, high-confidence overload is allowed to activate promptly. Low
	// confidence estimates still require the complete configured window.
	if valid && confidence >= 0.25 && ratio >= activationStrongOverload && required > activationStrongEvidence {
		required = activationStrongEvidence
	}
	if backlog >= uint64(capacity*required.Seconds()*activationBacklogFraction) && ratio > 1 {
		required = activationStrongEvidence
	}
	c.activationReason.Store("dynamic_capacity_overload")
	return now.Sub(time.Unix(0, since)) >= required
}

func (c *StreamEngine) activate() {
	c.activateOnce.Do(func() {
		c.active.Store(true)
		close(c.activeCh)
		c.kickSpaceSource("ACTIVATION", -1)
		if c.cfg.NotifyPeerOnActivate {
			c.notifyPeerActivate()
		}
		if c.cfg.OnActivate != nil {
			c.cfg.OnActivate()
		}
	})
}

// An active session retains its intent. Each attached generation gets one
// queued notification; a full queue is retried by bounded maintenance.
// Queue acceptance is not wire delivery or peer acknowledgement.
func (c *StreamEngine) notifyPeerActivate() {
	select {
	case <-c.done:
		return
	default:
	}
	if c.cfg.activateNotifyHook != nil {
		defer c.cfg.activateNotifyHook()
	}
	for _, leg := range c.availableLegs() {
		if !leg.activateQueued.CompareAndSwap(false, true) {
			continue
		}
		select {
		case <-c.done:
			leg.activateQueued.Store(false)
		case <-leg.done:
			leg.activateQueued.Store(false)
		case leg.control <- legControl{typ: frameTypeActivate}:
		default:
			leg.activateQueued.Store(false)
		}
	}
}

func (c *StreamEngine) getLeg(id uint8) *streamLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *StreamEngine) hasLeg(id uint8) bool { return c.getLeg(id) != nil }

func (c *StreamEngine) availableLegs() []*streamLeg {
	c.legsMu.RLock()
	legs := make([]*streamLeg, 0, len(c.legs))
	for _, leg := range c.legs {
		if !leg.closed.Load() && !leg.draining.Load() {
			legs = append(legs, leg)
		}
	}
	c.legsMu.RUnlock()
	sort.Slice(legs, func(i, j int) bool { return legs[i].id < legs[j].id })
	return legs
}

// bestAlternateLeg selects a live path for a copy while respecting the
// scheduler's current measured/prior capacity weights. Ties are deterministic
// by protocol LegID.
func (c *StreamEngine) bestAlternateLeg(owner int16) *streamLeg {
	var best *streamLeg
	bestWeight := -1.0
	for _, leg := range c.availableLegs() {
		if int16(leg.id) == owner {
			continue
		}
		weight := c.effectiveSchedulerWeight(leg)
		if best == nil || weight > bestWeight || weight == bestWeight && leg.id < best.id {
			best, bestWeight = leg, weight
		}
	}
	return best
}

func (c *StreamEngine) legCount() int {
	c.legsMu.RLock()
	n := len(c.legs)
	c.legsMu.RUnlock()
	return n
}

func (c *StreamEngine) hasOutstanding() bool {
	return c.txLedger.HasOutstanding()
}

func (c *StreamEngine) weightFor(id uint8) uint32 {
	if leg := c.getLeg(id); leg != nil {
		if prior := leg.priorMbps.Load(); prior > 0 {
			return prior
		}
	}
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *StreamEngine) effectiveSchedulerWeight(leg *streamLeg) float64 {
	if c.slowLegProtection() {
		c.admission.mu.Lock()
		state := c.admission.legs[leg.id]
		c.admission.mu.Unlock()
		if state.recovering && state.seedMbps > 0 {
			return state.seedMbps
		}
		if state.recovering {
			return float64(c.weightFor(leg.id))
		}
	}
	base := float64(c.weightFor(leg.id))
	if c.cfg.SchedulerMode == StreamSchedulerAggregation && c.cfg.CapacityMode == StreamCapacityDynamic {
		if c.capacityProvider != nil {
			return c.capacityProvider.Weight(leg.id, base)
		}
		if c.capacity != nil {
			return c.capacity.weight(leg.id, base)
		}
		return base
	}
	if c.cfg.SchedulerMode != StreamSchedulerAdaptive && c.cfg.SchedulerMode != StreamSchedulerCapacityFirst {
		return base
	}
	writeBPS, ackedBPS, latency := leg.perf.snapshot()
	observed := ackedBPS
	if observed <= 0 {
		observed = writeBPS
	}
	if observed > 0 {
		observedMbps := observed * 8 / 1e6
		if observedMbps < 1 {
			observedMbps = 1
		}
		if base > 1 {
			// Static bandwidth is a prior; useful ACK/write feedback gradually
			// reshapes it without letting one short sample dominate scheduling.
			base = math.Sqrt(base * observedMbps)
		} else {
			base = observedMbps
		}
	}
	if c.cfg.SchedulerMode == StreamSchedulerAdaptive && latency > 0 {
		// A leg that takes longer to drain a frame should receive fewer early
		// sequence numbers. This directly reduces slow-path HOL pressure before
		// frontier rescue is needed.
		penaltyBase := c.cfg.RetransmitTimeout / 8
		if penaltyBase < 20*time.Millisecond {
			penaltyBase = 20 * time.Millisecond
		}
		base /= 1 + float64(latency)/float64(penaltyBase)
	}
	if base < 0.1 {
		base = 0.1
	}
	return base
}

// pathRTTScorePenalty adds a measured completion-time prior only when both
// active legs have fresh, authenticated samples and neither leg is in slow-leg
// restriction/recovery. One score unit is 8 microseconds (the existing score
// uses bytes divided by Mbps), so this preserves capacity weighting while
// biasing short-flow cold start toward the lower-RTT path.
func (c *StreamEngine) pathRTTScorePenalty(leg *streamLeg) float64 {
	if !c.cfg.ExperimentalPathAware || leg == nil || !leg.pathRTTEnabled || c.pathRTTCache == nil {
		return 0
	}
	legs := c.availableLegs()
	if len(legs) < 2 {
		return 0
	}
	if c.slowLegProtection() {
		c.admission.mu.Lock()
		restricted := false
		for _, active := range legs {
			state := c.admission.legs[active.id]
			if state.restricted || state.recovering {
				restricted = true
				break
			}
		}
		c.admission.mu.Unlock()
		if restricted {
			return 0
		}
	}
	var ownRTT time.Duration
	for _, active := range legs {
		if !active.pathRTTEnabled {
			return 0
		}
		feedback, ok := c.pathRTTCache.PathFeedback(active.pathIdentity)
		if !ok || feedback.RTT <= 0 {
			return 0
		}
		if active == leg {
			ownRTT = feedback.RTT
		}
	}
	if ownRTT <= 0 {
		return 0
	}
	return float64(ownRTT.Nanoseconds()) / 8000
}

func (c *StreamEngine) chooseLeg(active bool, avoid int16) *streamLeg {
	if c.cfg.BenchmarkWeightedDeficit {
		legs := c.availableLegs()
		c.weightedMu.Lock()
		defer c.weightedMu.Unlock()
		var best *streamLeg
		bestFinish := math.MaxFloat64
		for _, leg := range legs {
			if len(legs) > 1 && int16(leg.id) == avoid {
				continue
			}
			if c.weightedFinish[leg.id] < bestFinish {
				best, bestFinish = leg, c.weightedFinish[leg.id]
			}
		}
		if best != nil {
			return best
		}
	}
	if c.cfg.BenchmarkFixedRatio {
		// One low-capacity record followed by four high-capacity records. The
		// diagnostic preserves the normal enqueue, writer, ACK and ledger path.
		legs := c.availableLegs()
		if len(legs) >= 2 {
			n := c.benchmarkFixedDispatch.Add(1) - 1
			preferred := uint8(1)
			if n%5 == 0 {
				preferred = 0
			}
			for _, leg := range legs {
				if leg.id == preferred && int16(leg.id) != avoid {
					return leg
				}
			}
		}
	}
	if !active {
		if primary := c.getLeg(0); primary != nil && !primary.draining.Load() && int16(primary.id) != avoid {
			return primary
		}
	}
	legs := c.availableLegs()
	var best *streamLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		if len(legs) > 1 && int16(leg.id) == avoid {
			continue
		}
		weight := c.effectiveSchedulerWeight(leg)
		dispatched := uint64(0)
		if int(leg.id) < len(c.txSentBytes) {
			dispatched = c.txSentBytes[leg.id].Load()
		}
		// A fast writer can make its channel look empty on every scheduler turn.
		// Include the bytes already dispatched on this logical stream so weighted
		// scheduling converges toward usable path capacity instead of selecting the
		// same empty queue indefinitely.
		score := (float64(dispatched) + float64(len(leg.send)+1)*float64(c.cfg.ChunkSize)) / weight
		score += c.pathRTTScorePenalty(leg)
		if best == nil || score < bestScore {
			best = leg
			bestScore = score
		}
	}
	if best == nil && len(legs) > 0 {
		return legs[0]
	}
	return best
}

func (c *StreamEngine) chargeWeightedService(leg *streamLeg, bytes int) {
	if !c.cfg.BenchmarkWeightedDeficit || leg == nil || bytes <= 0 {
		return
	}
	weight := c.effectiveSchedulerWeight(leg)
	if weight <= 0 {
		return
	}
	c.weightedMu.Lock()
	c.weightedFinish[leg.id] += float64(bytes) / weight
	c.weightedMu.Unlock()
}

func (c *StreamEngine) chooseLegWithStartupHint(active bool, avoid, preferred int16) *streamLeg {
	if preferred >= 0 && preferred < int16(c.maxActiveLegs) && preferred != avoid {
		if leg := c.getLeg(uint8(preferred)); leg != nil && !leg.closed.Load() && !leg.draining.Load() {
			return leg
		}
	}
	return c.chooseLeg(active, avoid)
}

func (c *StreamEngine) chooseOrdinaryLeg(active bool, avoid int16, payload int) *streamLeg {
	if c.cfg.BenchmarkAssignedCommit || c.cfg.BenchmarkAssignedService {
		return c.chooseAssignedLeg(avoid, payload)
	}
	if c.cfg.loadProbe != nil {
		return c.cfg.loadProbe.choose(c, payload)
	}
	return c.chooseLeg(active, avoid)
}

func (c *StreamEngine) chooseAssignedLeg(avoid int16, payload int) *streamLeg {
	legs := c.availableLegs()
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	var best *streamLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		if len(legs) > 1 && int16(leg.id) == avoid {
			continue
		}
		weight := c.effectiveSchedulerWeight(leg)
		if weight <= 0 {
			continue
		}
		score := float64(c.assignedService[leg.id]+uint64(payload)) / weight
		score += c.pathRTTScorePenalty(leg)
		if best == nil || score < bestScore {
			best, bestScore = leg, score
		}
	}
	return best
}

func (c *StreamEngine) assignedSnapshot() ([2]uint64, [2]uint64) {
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	var service, records [2]uint64
	copy(service[:], c.assignedService)
	copy(records[:], c.assignedRecords)
	return service, records
}

func (c *StreamEngine) assignedSnapshotByLeg() ([]uint64, []uint64) {
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	return append([]uint64(nil), c.assignedService...), append([]uint64(nil), c.assignedRecords...)
}

func (c *StreamEngine) readyLegs() ([]*streamLeg, uint8) {
	legs := c.availableLegs()
	ready := make([]*streamLeg, 0, len(legs))
	for _, leg := range legs {
		if leg.closed.Load() || cap(leg.send) == 0 || len(leg.send) >= cap(leg.send) {
			continue
		}
		ready = append(ready, leg)
	}
	state := uint8(3)
	if len(ready) >= 2 {
		state = 0
	} else if len(ready) == 1 {
		if ready[0].id == 0 {
			state = 1
		} else {
			state = 2
		}
	}
	c.readyStateDecisions[state].Add(1)
	return ready, state
}

func (c *StreamEngine) chooseAssignedReadyLeg(avoid int16, payload int) (*streamLeg, uint8) {
	ready, state := c.readyLegs()
	c.weightedMu.Lock()
	defer c.weightedMu.Unlock()
	var best *streamLeg
	bestScore := math.MaxFloat64
	for _, leg := range ready {
		if len(ready) > 1 && int16(leg.id) == avoid {
			continue
		}
		weight := c.effectiveSchedulerWeight(leg)
		if weight <= 0 {
			continue
		}
		score := float64(c.assignedService[leg.id]+uint64(payload)) / weight
		if best == nil || score < bestScore {
			best, bestScore = leg, score
		}
	}
	return best, state
}

func (c *StreamEngine) markTransit(record *StreamTXRecord, legID uint8) bool {
	return c.txLedger.MarkTransit(record, LegID(legID), time.Now())
}

func (c *StreamEngine) markRescueTransit(record *StreamTXRecord, legID uint8) (time.Time, bool) {
	return c.txLedger.MarkRescueTransit(record, LegID(legID), time.Now())
}

func (c *StreamEngine) markRescueQueued(record *StreamTXRecord, legID uint8, started time.Time) bool {
	if !c.txLedger.MarkRescueQueued(record, LegID(legID), started) {
		return false
	}
	c.frontierRescueAttempts.Add(1)
	return true
}

func (c *StreamEngine) clearAttempt(record *StreamTXRecord, legID uint8, rescue bool) {
	c.txLedger.ClearAttempt(record, LegID(legID), rescue)
}

func (c *StreamEngine) clearAttemptAt(record *StreamTXRecord, legID uint8, rescue bool, started time.Time) {
	c.txLedger.clearAttemptAt(record, LegID(legID), rescue, started)
}

func (c *StreamEngine) attemptCurrent(record *StreamTXRecord, legID uint8, rescue bool) bool {
	return c.txLedger.AttemptCurrent(record, LegID(legID), rescue)
}

func (c *StreamEngine) markAttemptSent(record *StreamTXRecord, legID uint8, rescue bool) StreamTXAttemptResult {
	result := c.txLedger.MarkAttemptSent(record, LegID(legID), rescue, time.Now())
	if !result.Applied || int(legID) >= len(c.txSentBytes) {
		return result
	}
	if result.Retransmit {
		c.txRetransmitBytes[legID].Add(uint64(result.Bytes))
	}
	c.txSentBytes[legID].Add(uint64(result.Bytes))
	return result
}

// commitSuccessfulDataWrite records ledger effects when the record is still
// outstanding, while recording telemetry for every complete successful
// Carrier DATA write. A cumulative ACK may retire the record after the peer
// receives the frame but before this writer callback commits the ledger.
func (c *StreamEngine) commitSuccessfulDataWrite(attempt txSendAttempt, legID uint8) StreamTXAttemptResult {
	result := c.markAttemptSent(attempt.record, legID, attempt.rescue)
	if c.telemetry != nil && attempt.record != nil {
		retransmit := result.Retransmit || attempt.rescue || !attempt.ordinary
		c.telemetry.markDataTx(legID, retransmit, attempt.rescue, uint64(len(attempt.record.Payload())))
	}
	return result
}

func (c *StreamEngine) isOutstanding(record *StreamTXRecord) bool {
	return c.txLedger.IsOutstanding(record)
}

func (c *StreamEngine) tryQueue(leg *streamLeg, record *StreamTXRecord, retry bool) bool {
	if leg == nil {
		return false
	}
	leg.dispatchMu.RLock()
	defer leg.dispatchMu.RUnlock()
	if leg.closed.Load() || leg.draining.Load() {
		return false
	}
	if !retry && record.trace != nil {
		record.traceLeg = leg.id
		record.traceT3Ns = time.Now().UnixNano()
	}
	started := time.Now()
	if !c.txLedger.MarkTransit(record, LegID(leg.id), started) {
		return !c.isOutstanding(record)
	}
	attempt := txSendAttempt{record: record, queuedNs: time.Now().UnixNano(), ordinary: !retry, started: started}
	saturated := len(leg.send) >= cap(leg.send)-1 || (int(leg.id) < len(c.pendingDepth) && c.pendingDepth[leg.id].Load() > 0)
	select {
	case <-leg.done:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "send_queue_aborted", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()), Error: "leg_closed",
		})
		c.clearAttemptAt(record, leg.id, false, started)
		return false
	case leg.send <- attempt:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "send_queue_accepted", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()),
			QueueLength: len(leg.send), QueueCapacity: cap(leg.send), Applied: true,
		})
		if !retry && c.slowLegProtection() {
			c.txLedger.markOrdinaryAdmitted(record, leg.id)
		}
		if !retry && c.capacityProvider != nil {
			c.capacityProvider.Admit(leg.id, uint64(len(record.Payload())), saturated, time.Now())
		}
		if retry && c.cfg.frontierProbe != nil {
			c.cfg.frontierProbe.retry(record, time.Now())
		}
		if !retry && c.assignedDecoupledEnabled() {
			if assignment := record.benchmarkAssignment.Load(); assignment != nil {
				assignment.admitted.Store(true)
				c.decoupledStats.admitted[leg.id].Add(uint64(len(record.Payload())))
			}
		}
		if !retry && c.cfg.loadProbe != nil {
			c.cfg.loadProbe.assignedRecord(leg.id, len(record.Payload()))
		}
		if !retry {
			if c.cfg.BenchmarkAssignedCommit || c.cfg.BenchmarkAssignedService || c.cfg.BenchmarkAssignedReady {
				c.weightedMu.Lock()
				c.assignedService[leg.id] += uint64(len(record.Payload()))
				c.assignedRecords[leg.id]++
				c.weightedMu.Unlock()
			}
			c.chargeWeightedService(leg, len(record.Payload()))
		}
		if !retry && c.cfg.wakeTelemetry != nil {
			c.cfg.wakeTelemetry.dispatched(record, leg)
		}
		if retry {
			c.debugRetryDispatches.Add(1)
		} else {
			c.debugNewDispatches.Add(1)
		}
		dispatchNow := time.Now().UnixNano()
		previousDispatch := int64(0)
		if !retry {
			previousDispatch = c.debugLastDispatchNs.Swap(dispatchNow)
		} else {
			c.debugLastDispatchNs.Store(dispatchNow)
		}
		leg.debugOrdinaryDispatches.Add(1)
		if !retry && record.trace != nil {
			record.traceT4Ns = dispatchNow
			if previousDispatch > 0 {
				record.trace.addGap(dispatchNow - previousDispatch)
			}
		}
		// A buffered send can win the select while the leg is closing. Recheck
		// after enqueue so a record never remains pinned to a retired writer.
		if leg.closed.Load() {
			c.clearAttemptAt(record, leg.id, false, started)
			return false
		}
		return true
	default:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "send_queue_rejected", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()),
			QueueLength: len(leg.send), QueueCapacity: cap(leg.send), Error: "queue_full",
		})
		leg.debugQueueFullRejects.Add(1)
		if !c.active.Load() && c.legCount() == 1 {
			c.activationReason.Store("queue_congestion")
			c.activate()
		}
		if retry {
			c.debugRetryRejects.Add(1)
		}
		c.clearAttemptAt(record, leg.id, false, started)
		if c.cfg.queueRejectHook != nil {
			c.cfg.queueRejectHook(leg, record, retry)
		}
		return false
	}
}

func (c *StreamEngine) tryQueueRescue(leg *streamLeg, record *StreamTXRecord) bool {
	return c.tryQueueRescueOwned(leg, record, -1)
}

func (c *StreamEngine) tryQueueRescueOwned(leg *streamLeg, record *StreamTXRecord, owner int16) bool {
	return c.tryQueueRescueOwnedAttempt(leg, record, owner, StreamRescueOtherCoreRetry, 0)
}

func (c *StreamEngine) tryQueueBoundedFrontierRescue(leg *streamLeg, record *StreamTXRecord, owner int16, recoveryEpoch uint64, overdue bool) bool {
	if c.boundedRescue == nil {
		return c.tryQueueRescueOwned(leg, record, owner)
	}
	evidence := c.boundedFrontierEvidence(leg, record, owner, recoveryEpoch, overdue)
	decision := c.boundedRescue.Reserve(evidence, uint64(len(record.Payload())), c.boundedRescueNow())
	if !decision.Admitted {
		return false
	}
	return c.tryQueueRescueOwnedAttemptWithCandidateID(leg, record, evidence.OwnerLeg, decision.Class, decision.ReservationID, decision.CandidateID)
}

func (c *StreamEngine) boundedRescueNow() time.Time {
	if c != nil && c.cfg.boundedRescueClock != nil {
		return c.cfg.boundedRescueClock()
	}
	return time.Now()
}

func (c *StreamEngine) boundedFrontierEvidence(leg *streamLeg, record *StreamTXRecord, owner int16, recoveryEpoch uint64, overdue bool) StreamRescueEvidence {
	snapshot := c.txLedger.rescueEvidenceSnapshot(record)
	evidenceOwner := owner
	if evidenceOwner < 0 || evidenceOwner >= int16(len(c.legGeneration)) {
		evidenceOwner = snapshot.OrdinaryOwner
	}
	evidence := StreamRescueEvidence{
		Sequence: record.Sequence(), AckedNext: snapshot.AckedNext, OwnerLeg: evidenceOwner, RecordOwnerLeg: snapshot.OrdinaryOwner,
		TargetLeg: 255, OriginGeneration: snapshot.OriginGeneration, RecoveryEpoch: recoveryEpoch,
		Cause: StreamRescueCauseFrontier, IsData: true, IsRescue: true,
		Outstanding: snapshot.Exists, FrontierOverdue: overdue,
		OriginalWriteObserved: !snapshot.FirstWriteAt.IsZero(), OwnerInvalidated: snapshot.Invalidated,
		OriginalWriteInTransit: snapshot.InTransit, OriginalWriteLeg: uint8(snapshot.TransitLeg),
		GenerationMatchesOrigin: snapshot.OriginGeneration != 0 && evidenceOwner >= 0 && evidenceOwner < int16(len(c.legGeneration)) &&
			c.legGeneration[evidenceOwner].Load() == snapshot.OriginGeneration,
	}
	if leg != nil {
		evidence.TargetLeg = leg.id
		evidence.TargetGeneration = leg.generation
		evidence.TargetAvailable = c.getLeg(leg.id) == leg && !leg.closed.Load()
		evidence.TargetGenerationMatches = c.legGeneration[leg.id].Load() == leg.generation
	}
	if evidenceOwner >= 0 && evidenceOwner < int16(len(c.legGeneration)) {
		id := uint8(evidenceOwner)
		evidence.OwnerGeneration = c.legGeneration[id].Load()
		evidence.OwnerLive = c.hasLeg(id)
		evidence.ProbeData = c.pendingRecoveryProbeOwner(id, record.Sequence())
		c.admission.mu.Lock()
		state := c.admission.legs[id]
		evidence.OwnerRestricted = state.restricted
		if evidence.RecoveryEpoch == 0 {
			evidence.RecoveryEpoch = state.recoveryEpoch
		}
		evidence.RecoveryEpochCurrent = evidence.RecoveryEpoch == state.recoveryEpoch
		c.admission.mu.Unlock()
	}
	return evidence
}

func (c *StreamEngine) revalidateBoundedRescueBeforeWrite(leg *streamLeg, record *StreamTXRecord, reservationID uint64) (StreamRescueClass, uint64, bool) {
	if c == nil || c.boundedRescue == nil || leg == nil || record == nil {
		return StreamRescueDuplicateOrRejected, 0, false
	}
	expected, ok := c.boundedRescue.reservationEvidence(reservationID)
	if !ok {
		return StreamRescueDuplicateOrRejected, 0, false
	}
	if c.getLeg(leg.id) != leg || leg.closed.Load() {
		c.boundedRescue.Release(reservationID, "target_generation_retired_before_carrier_write", c.boundedRescueNow())
		return StreamRescueDuplicateOrRejected, 0, false
	}
	current := c.boundedFrontierEvidence(leg, record, expected.OwnerLeg, expected.RecoveryEpoch, expected.FrontierOverdue)
	return c.boundedRescue.RevalidateBeforeWrite(reservationID, current, c.boundedRescueNow())
}

func (c *StreamEngine) tryQueueRescueOwnedAttempt(leg *streamLeg, record *StreamTXRecord, owner int16, class StreamRescueClass, reservationID uint64) bool {
	return c.tryQueueRescueOwnedAttemptWithCandidateID(leg, record, owner, class, reservationID, 0)
}

func (c *StreamEngine) tryQueueRescueOwnedAttemptWithCandidateID(leg *streamLeg, record *StreamTXRecord, owner int16, class StreamRescueClass, reservationID, candidateID uint64) bool {
	if leg == nil {
		if c.boundedRescue != nil && reservationID != 0 {
			c.boundedRescue.Release(reservationID, "leg_unavailable_before_queue", c.boundedRescueNow())
		}
		return false
	}
	leg.dispatchMu.RLock()
	defer leg.dispatchMu.RUnlock()
	if leg.closed.Load() || leg.draining.Load() {
		if c.boundedRescue != nil && reservationID != 0 {
			c.boundedRescue.Release(reservationID, "leg_unavailable_before_queue", c.boundedRescueNow())
		}
		return false
	}
	var started time.Time
	var marked bool
	if owner >= 0 {
		started, marked = c.txLedger.MarkRestrictedRescueTransit(record, LegID(leg.id), owner, time.Now())
	} else {
		started, marked = c.markRescueTransit(record, leg.id)
	}
	if !marked {
		if c.boundedRescue != nil && reservationID != 0 {
			c.boundedRescue.Release(reservationID, "ledger_claim_rejected", c.boundedRescueNow())
		}
		already := !c.txLedger.IsOutstanding(record) || c.txLedger.RescueInTransit(record)
		return already
	}
	attempt := txSendAttempt{record: record, rescue: true, rescueCandidateID: candidateID, queuedNs: time.Now().UnixNano(), started: started,
		rescueClass: class, boundedRescueReservation: reservationID}
	select {
	case <-leg.done:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "rescue_queue_aborted", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()), Rescue: true, Error: "leg_closed",
		})
		c.clearAttemptAt(record, leg.id, true, started)
		if c.boundedRescue != nil && reservationID != 0 {
			c.boundedRescue.Release(reservationID, "leg_closed_before_queue", c.boundedRescueNow())
		}
		return false
	case leg.rescue <- attempt:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "rescue_queue_accepted", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()), Rescue: true,
			QueueLength: len(leg.rescue), QueueCapacity: cap(leg.rescue), Applied: true,
		})
		if p := c.cfg.frontierProbe; p != nil {
			p.rescue(c, record, time.Now())
		}
		c.debugRescueDispatches.Add(1)
		c.debugLastDispatchNs.Store(time.Now().UnixNano())
		leg.debugRescueDispatches.Add(1)
		// Commit cooldown/diagnostic state only after the priority queue accepted
		// this attempt. A full queue therefore remains immediately retryable.
		c.markRescueQueued(record, leg.id, started)
		if leg.closed.Load() {
			c.clearAttemptAt(record, leg.id, true, started)
			if c.boundedRescue != nil && reservationID != 0 {
				c.boundedRescue.Release(reservationID, "leg_closed_after_queue", c.boundedRescueNow())
			}
			return false
		}
		return true
	default:
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "rescue_queue_rejected", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), LegID: int16(leg.id), LegGeneration: leg.generation,
			AttemptStartedAt: started, PayloadBytes: len(record.Payload()), Rescue: true,
			QueueLength: len(leg.rescue), QueueCapacity: cap(leg.rescue), Error: "queue_full",
		})
		leg.debugRescueFullRejects.Add(1)
		c.debugRescueRejects.Add(1)
		if c.cfg.rescueRejectHook != nil {
			c.cfg.rescueRejectHook(record, started)
		}
		c.clearAttemptAt(record, leg.id, true, started)
		if c.boundedRescue != nil && reservationID != 0 {
			c.boundedRescue.Release(reservationID, "rescue_queue_full", c.boundedRescueNow())
		}
		return false
	}
}

func (c *StreamEngine) enqueueRecord(record *StreamTXRecord, avoid int16) error {
	if c.assignedDecoupledEnabled() {
		return c.enqueueAssignedDecoupled(record, avoid)
	}
	if c.cfg.BenchmarkAssignedReady {
		for {
			select {
			case <-c.done:
				return ErrStreamClosed
			default:
			}
			if !c.isOutstanding(record) {
				return nil
			}
			leg, _ := c.chooseAssignedReadyLeg(avoid, len(record.Payload()))
			if leg == nil {
				select {
				case <-c.done:
					return ErrStreamClosed
				case <-c.spaceWake:
				}
				continue
			}
			if record.trace != nil {
				record.traceT2Ns = time.Now().UnixNano()
			}
			if c.tryQueue(leg, record, false) {
				return nil
			}
			c.readyRaceFailures.Add(1)
			runtime.Gosched()
			// The ready set is advisory. Rebuild it and make a new weighted
			// decision; never reuse a stale fallback chain.
		}
	}
	if c.cfg.BenchmarkAssignedCommit {
		leg := c.chooseAssignedLeg(avoid, len(record.Payload()))
		if record.trace != nil {
			record.traceT2Ns = time.Now().UnixNano()
		}
		for {
			select {
			case <-c.done:
				return ErrStreamClosed
			default:
			}
			if !c.isOutstanding(record) {
				return nil
			}
			if leg != nil && c.tryQueue(leg, record, false) {
				return nil
			}
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.spaceWake:
			}
		}
	}
	if c.cfg.BenchmarkWeightedCommit {
		leg := c.chooseOrdinaryLeg(c.active.Load(), avoid, len(record.Payload()))
		if record.trace != nil {
			record.traceT2Ns = time.Now().UnixNano()
		}
		for {
			select {
			case <-c.done:
				return ErrStreamClosed
			default:
			}
			if !c.isOutstanding(record) {
				return nil
			}
			if leg != nil && c.tryQueue(leg, record, false) {
				return nil
			}
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.spaceWake:
			}
		}
	}
	if c.cfg.BenchmarkFixedRatio {
		// Pin each logical record to the diagnostic assignment until its ordinary
		// enqueue succeeds. This prevents a full preferred queue from turning the
		// intended 1:4 pattern into an accidental fallback ratio.
		leg := c.chooseLeg(c.active.Load(), avoid)
		if record.trace != nil {
			record.traceT2Ns = time.Now().UnixNano()
		}
		for {
			select {
			case <-c.done:
				return ErrStreamClosed
			default:
			}
			if !c.isOutstanding(record) {
				return nil
			}
			if leg != nil && c.tryQueue(leg, record, false) {
				return nil
			}
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.spaceWake:
			case <-c.retryCh:
			}
		}
	}
	for {
		select {
		case <-c.done:
			return ErrStreamClosed
		default:
		}
		if !c.isOutstanding(record) {
			return nil
		}
		active := c.active.Load()
		leg := c.chooseOrdinaryLeg(active, avoid, len(record.Payload()))
		if record.trace != nil {
			record.traceT2Ns = time.Now().UnixNano()
		}
		if leg == nil {
			select {
			case <-c.done:
				return ErrStreamClosed
			case <-c.retryCh:
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if c.tryQueue(leg, record, false) {
			if c.cfg.HandoffTelemetry != nil {
				c.debugPrimarySuccess.Add(1)
			}
			return nil
		}
		if c.cfg.HandoffTelemetry != nil {
			c.debugPrimaryFailure.Add(1)
		}
		if !active && leg != nil && leg.id == 0 && cap(leg.send) > 0 && len(leg.send) >= cap(leg.send) {
			// A saturated preferred queue is direct evidence that the single-path data
			// plane cannot currently absorb ingress. Do not wait an entire activation
			// sampling window before waking the booster; this also improves short burst
			// and speed-test flows while preserving lazy leg1 startup for light traffic.
			c.activate()
			active = true
		}
		if active {
			for _, other := range c.availableLegs() {
				if other == leg || (c.legCount() > 1 && int16(other.id) == avoid) {
					continue
				}
				if c.cfg.HandoffTelemetry != nil {
					c.debugFallbackAttempts.Add(1)
				}
				if c.tryQueue(other, record, false) {
					if c.cfg.HandoffTelemetry != nil {
						c.debugFallbackSuccess.Add(1)
					}
					return nil
				}
			}
		}
		if c.cfg.BenchmarkReadyFallback {
			// Benchmark-only race-window probe: give another healthy queue a few
			// scheduler turns to become writable before entering spaceWake.
			for pass := 0; pass < 4; pass++ {
				runtime.Gosched()
				for _, other := range c.availableLegs() {
					if other == leg || (c.legCount() > 1 && int16(other.id) == avoid) {
						continue
					}
					if c.cfg.HandoffTelemetry != nil {
						c.debugFallbackAttempts.Add(1)
					}
					if c.tryQueue(other, record, false) {
						if c.cfg.HandoffTelemetry != nil {
							c.debugFallbackSuccess.Add(1)
						}
						return nil
					}
				}
			}
		}
		var waitStarted time.Time
		if c.cfg.wakeTelemetry != nil {
			c.cfg.wakeTelemetry.begin(c, leg)
		}
		if c.cfg.HandoffTelemetry != nil {
			waitStarted = time.Now()
		}
		if c.cfg.HandoffTelemetry != nil {
			c.debugSpaceWakeEntries.Add(1)
		}
		select {
		case <-c.done:
			return ErrStreamClosed
		case notification := <-c.spaceWake:
			if c.cfg.wakeTelemetry != nil {
				c.cfg.wakeTelemetry.received(notification)
			}
			if c.cfg.HandoffTelemetry != nil {
				c.debugSpaceWakeWaitNs.Add(uint64(time.Since(waitStarted)))
			}
		}
	}
}

func (c *StreamEngine) enqueueRecordWithHint(record *StreamTXRecord, avoid, preferred int16) error {
	if preferred < 0 || preferred >= int16(c.maxActiveLegs) || preferred == avoid || !c.isOutstanding(record) {
		return c.enqueueRecord(record, avoid)
	}
	active := c.active.Load()
	leg := c.chooseLegWithStartupHint(active, avoid, preferred)
	if leg != nil && c.tryQueue(leg, record, false) {
		return nil
	}
	// The one-shot hint is consumed by this decision. Any unavailable or
	// backpressured preferred leg falls back to the unchanged scheduler.
	return c.enqueueRecord(record, avoid)
}

func (c *StreamEngine) MarkStartupLegTerminalUnavailable(id LegID) bool {
	return c.startup.markTerminalUnavailable(id)
}

// tryEnqueueRecord makes one non-blocking ordinary retry scheduling pass.
// Application TX still uses enqueueRecord because it must apply backpressure
// to the application. The retransmit scheduler, however, must not wait for a
// retry queue: frontier rescue is an independent repair decision and must be
// evaluated in the same scheduler turn even when ordinary retry capacity is
// temporarily unavailable.
func (c *StreamEngine) tryEnqueueRecord(record *StreamTXRecord, avoid int16) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	if !c.isOutstanding(record) {
		return true
	}
	active := c.active.Load()
	leg := c.chooseLeg(active, avoid)
	if leg == nil {
		return false
	}
	if c.cfg.recoveryTraceHook != nil {
		snapshot := c.txLedger.probeReferenceSnapshot(record, false)
		reason := "ordinary_retry_candidate"
		if snapshot.Invalidated {
			reason = "original_owner_invalidated"
		}
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "retry_candidate_selected", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
			LegID: int16(leg.id), LegGeneration: leg.generation, RecordGeneration: snapshot.OriginGeneration,
			PayloadBytes: len(record.Payload()), LedgerObservedAt: snapshot.LedgerObservedAt,
			AckedNext: snapshot.AckedNext, LedgerPresent: snapshot.Exists,
			InTransit: snapshot.InTransit, TransitLeg: snapshot.TransitLeg, TransitSince: snapshot.TransitSince,
			RescueInTransit: snapshot.RescueInTransit, RescueLeg: snapshot.RescueLeg, RescueSince: snapshot.RescueSince,
			FirstWriteAt: snapshot.FirstWriteAt, LastSentAt: snapshot.LastSentAt,
			SendCount: snapshot.SendCount, ReasonBefore: reason, Action: "ordinary_retry_target_selected",
			Applied: true, ObservedAt: time.Now(),
		})
	}
	if c.tryQueue(leg, record, true) {
		return true
	}
	if !active && leg.id == 0 && cap(leg.send) > 0 && len(leg.send) >= cap(leg.send) {
		c.activate()
		active = true
	}
	if active {
		for _, other := range c.availableLegs() {
			if other == leg || (c.legCount() > 1 && int16(other.id) == avoid) {
				continue
			}
			if c.tryQueue(other, record, true) {
				return true
			}
		}
	}
	return false
}

func (c *StreamEngine) enqueueRescue(record *StreamTXRecord, avoid int16) error {
	return c.enqueueRescueWithCandidateID(record, avoid, 0)
}

func (c *StreamEngine) enqueueRescueWithCandidateID(record *StreamTXRecord, avoid int16, candidateID uint64) error {
	select {
	case <-c.done:
		return ErrStreamClosed
	default:
	}
	if !c.isOutstanding(record) {
		return nil
	}
	legs := c.availableLegs()
	if len(legs) == 0 {
		return nil
	}
	var leg *streamLeg
	if len(legs) == 1 {
		// With one live leg, retry the overdue frontier on that same leg. The
		// alternative is an unrecoverable cumulative-ACK stall after loss.
		leg = legs[0]
	} else {
		leg = c.chooseLeg(true, avoid)
	}
	if leg == nil || (len(legs) > 1 && int16(leg.id) == avoid) {
		return nil
	}
	if candidateID != 0 {
		_ = c.tryQueueRescueOwnedAttemptWithCandidateID(leg, record, -1, StreamRescueOtherCoreRetry, 0, candidateID)
	} else {
		_ = c.tryQueueRescue(leg, record)
	}
	return nil
}

func (c *StreamEngine) legWriteLoop(leg *streamLeg) {
	defer c.releaseQueuedBoundedRescues(leg)
	leg.debugWriterState.Store(0)
	defer leg.debugWriterState.Store(0)
	var ackSent uint64

	writeControl := func(control legControl) bool {
		var err error
		if control.typ == frameTypePathRTT {
			err = writePathRTTFrame(leg.conn, control.data)
		} else if control.typ == frameTypeDrain {
			err = writeStreamControlPayload(leg.conn, StreamFrameDrain, control.data)
		} else {
			header := encodeControlFrame(control.typ, control.value)
			err = writeAll(leg.conn, header[:])
		}
		if control.done != nil {
			control.done <- err
		}
		if err != nil {
			c.handleLegFailure(leg, err)
			return false
		}
		if c.telemetry != nil {
			c.telemetry.markControlTx(leg.id, control.typ)
		}
		return true
	}

	writePendingAck := func() bool {
		next := leg.ackPending.Load()
		force := leg.ackForce.Swap(false)
		if next == 0 || (next <= ackSent && !force) {
			return true
		}
		if !writeControl(legControl{typ: frameTypeAck, value: next}) {
			return false
		}
		if next > ackSent {
			ackSent = next
		}
		return true
	}

	writeAttempt := func(attempt txSendAttempt) bool {
		leg.debugLastDequeueNs.Store(time.Now().UnixNano())
		if !attempt.rescue && attempt.record != nil && attempt.record.trace != nil {
			attempt.record.traceT5Ns = time.Now().UnixNano()
		}
		if attempt.queuedNs != 0 {
			queuedAt := time.Unix(0, attempt.queuedNs)
			if wait := time.Since(queuedAt); wait > 0 {
				leg.debugQueueWaitNs.Add(uint64(wait))
			}
		}
		record := attempt.record
		if record == nil {
			return true
		}
		if attempt.queuedNs != 0 && c.cfg.recoveryTraceHook != nil {
			queuedAt := time.Unix(0, attempt.queuedNs)
			c.traceRecoveryEvent(streamRecoveryTraceEvent{
				Kind: "writer_dequeue", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
				LegID: int16(leg.id), LegGeneration: leg.generation, RecordGeneration: c.txLedger.originGenerationSnapshot(record),
				PayloadBytes: len(record.Payload()), AttemptStartedAt: attempt.started,
				ReferenceAt: queuedAt, Age: time.Since(queuedAt), Rescue: attempt.rescue,
				QueueLength: len(leg.send), QueueCapacity: cap(leg.send), Applied: true,
			})
		}
		if !c.attemptCurrent(record, leg.id, attempt.rescue) {
			if c.boundedRescue != nil && attempt.boundedRescueReservation != 0 {
				c.boundedRescue.Release(attempt.boundedRescueReservation, "stale_before_carrier_write", c.boundedRescueNow())
			}
			if attempt.ordinary && c.cfg.loadProbe != nil {
				c.cfg.loadProbe.complete(leg.id, len(record.Payload()))
			}
			return true
		}
		if attempt.ordinary && c.cfg.loadProbe != nil {
			defer c.cfg.loadProbe.complete(leg.id, len(record.Payload()))
		}
		reservationID := attempt.boundedRescueReservation
		leg.debugWriterState.Store(2)
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "core_data_write_enter", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
			LegID: int16(leg.id), LegGeneration: leg.generation, PayloadBytes: len(record.Payload()),
			AttemptStartedAt: attempt.started,
			Rescue:           attempt.rescue,
		})
		// Keep synchronous trace and ledger work ahead of the recovery transition.
		// The transition itself runs in the callback immediately before the DATA
		// buffers are handed to the carrier, after all pre-write queue/lock delay.
		c.txLedger.markWriteStarted(record, leg.id, attempt.rescue, time.Now())
		var writeStarted time.Time
		var b0RescueEvidence StreamRescueEvidence
		var b0RescueWriteObserved bool
		defer leg.writeStartedNs.Store(0)
		wireBytes, carrierWriteAttempted, writeErr := writeDataFrameWithGate(leg.conn, dataFrame{seq: record.Sequence(), data: record.Payload()}, func() error {
			if c.boundedRescue != nil && reservationID != 0 {
				_, currentReservation, valid := c.revalidateBoundedRescueBeforeWrite(leg, record, reservationID)
				if !valid {
					reservationID = 0
					return errBoundedRescueNotWritable
				}
				reservationID = currentReservation
				if reservationID != 0 && !c.txLedger.beginBoundedRescueWrite(record, c.boundedRescue, reservationID, c.boundedRescueNow()) {
					reservationID = 0
					return errBoundedRescueNotWritable
				}
			}
			c.telemetry.markDataCarrierWriteAttempt(leg.id, attempt.rescue || !attempt.ordinary, attempt.rescue, uint64(len(record.Payload())))
			var probeStarted bool
			if attempt.ordinary && !attempt.rescue {
				writeStarted, probeStarted = c.recoveryProbeWriteStartedAtWriteBoundary(leg.id, record)
			} else {
				writeStarted = time.Now()
			}
			if attempt.rescue && c.boundedRescue == nil && c.cfg.rescueEventHook != nil {
				ledger := c.txLedger.rescueEvidenceSnapshot(record)
				b0RescueEvidence = StreamRescueEvidence{
					Sequence: record.Sequence(), AckedNext: ledger.AckedNext, OwnerLeg: ledger.OrdinaryOwner, RecordOwnerLeg: ledger.OrdinaryOwner,
					TargetLeg: leg.id, TargetGeneration: leg.generation, TargetAvailable: c.getLeg(leg.id) == leg && !leg.closed.Load(),
					TargetGenerationMatches: c.legGeneration[leg.id].Load() == leg.generation, OriginGeneration: ledger.OriginGeneration,
					Cause: StreamRescueCauseNone, IsData: true, IsRescue: true,
					Outstanding: ledger.Exists, OriginalWriteInTransit: ledger.InTransit, OriginalWriteLeg: uint8(ledger.TransitLeg),
					OriginalWriteObserved: !ledger.FirstWriteAt.IsZero(), OwnerInvalidated: ledger.Invalidated,
				}
				if ledger.OrdinaryOwner >= 0 && ledger.OrdinaryOwner < int16(len(c.legGeneration)) {
					ownerID := uint8(ledger.OrdinaryOwner)
					b0RescueEvidence.OwnerGeneration = c.legGeneration[ownerID].Load()
					b0RescueEvidence.OwnerLive = c.hasLeg(ownerID)
					b0RescueEvidence.GenerationMatchesOrigin = ledger.OriginGeneration != 0 && b0RescueEvidence.OwnerGeneration == ledger.OriginGeneration
					c.admission.mu.Lock()
					b0RescueEvidence.RecoveryEpoch = c.admission.legs[ownerID].recoveryEpoch
					b0RescueEvidence.RecoveryEpochCurrent = b0RescueEvidence.RecoveryEpoch != 0
					c.admission.mu.Unlock()
				}
				b0RescueWriteObserved = true
				c.emitRescueObservation(StreamRescueEvent{
					At: writeStarted, SessionID: c.pathSessionID, CandidateID: attempt.rescueCandidateID, PolicyVariant: "B0", Kind: "b0_rescue_carrier_write_started",
					Evidence: b0RescueEvidence, Class: attempt.rescueClass, PayloadBytes: uint64(len(record.Payload())),
					Reason: "B0 carrier write boundary; reason not reclassified by this observer", FinalDecision: "write_started",
					FinalDecisionReason: "legacy B0 path dispatched rescue",
				})
			}
			if c.cfg.recoveryTraceHook != nil {
				c.traceRecoveryEvent(streamRecoveryTraceEvent{
					Kind: "core_data_write_dispatch", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
					RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
					LegID: int16(leg.id), LegGeneration: leg.generation, RecordGeneration: c.txLedger.originGenerationSnapshot(record),
					PayloadBytes: len(record.Payload()), AttemptStartedAt: attempt.started,
					ActionAt: writeStarted, Applied: probeStarted, Rescue: attempt.rescue,
				})
			}
			leg.writeStartedNs.Store(writeStarted.UnixNano())
			if !attempt.rescue && record.trace != nil {
				record.traceT6Ns = writeStarted.UnixNano()
			}
			return nil
		})
		c.telemetry.finishDataCarrierWrite(leg.id, uint64(wireBytes), uint64(frameHeaderSize+len(record.Payload())), carrierWriteAttempted, writeErr)
		if b0RescueWriteObserved {
			reason := "carrier_write_complete"
			if writeErr != nil {
				reason = writeErr.Error()
			}
			c.emitRescueObservation(StreamRescueEvent{
				At: time.Now(), SessionID: c.pathSessionID, CandidateID: attempt.rescueCandidateID, PolicyVariant: "B0", Kind: "b0_rescue_carrier_write_settled",
				Evidence: b0RescueEvidence, Class: attempt.rescueClass, PayloadBytes: uint64(len(record.Payload())),
				AcceptedBytes: streamDataPayloadAccepted(wireBytes, len(record.Payload())), Reason: reason,
				FinalDecision: "write_settled", FinalDecisionReason: reason,
			})
		}
		if errors.Is(writeErr, errBoundedRescueNotWritable) {
			leg.debugWriterState.Store(0)
			c.clearAttemptAt(record, leg.id, attempt.rescue, attempt.started)
			return true
		}
		if c.boundedRescue != nil && reservationID != 0 {
			if settleErr := c.boundedRescue.Settle(reservationID, streamDataPayloadAccepted(wireBytes, len(record.Payload())), writeErr, c.boundedRescueNow()); settleErr != nil {
				c.fail(fmt.Errorf("bounded rescue budget settlement: %w", settleErr))
				return false
			}
		}
		c.traceRecoveryEvent(streamRecoveryTraceEvent{
			Kind: "core_data_write_return", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
			RecordSequence: record.Sequence(), FrameSequence: record.Sequence(), FrameType: "data",
			LegID: int16(leg.id), LegGeneration: leg.generation, PayloadBytes: len(record.Payload()),
			WriteBytes: frameHeaderSize + len(record.Payload()), WriteResultBytes: int(wireBytes),
			CarrierAcceptedPayloadBytes: streamDataPayloadAccepted(wireBytes, len(record.Payload())),
			AttemptStartedAt:            attempt.started, Applied: writeErr == nil,
			Rescue: attempt.rescue,
			Error: func() string {
				if writeErr != nil {
					return writeErr.Error()
				}
				return ""
			}(),
		})
		if writeErr != nil {
			leg.debugWriteErrors.Add(1)
			leg.debugWriterState.Store(0)
			c.clearAttemptAt(record, leg.id, attempt.rescue, attempt.started)
			c.handleLegFailure(leg, writeErr)
			return false
		}
		leg.perf.observeWrite(len(record.Payload()), time.Since(writeStarted))
		c.txWrittenBytes[leg.id].Add(uint64(len(record.Payload())))
		if attempt.rescue || !attempt.ordinary {
			c.txRetryWrittenBytes[leg.id].Add(uint64(len(record.Payload())))
		}
		leg.debugWriteBusyNs.Add(uint64(time.Since(writeStarted)))
		leg.debugWriteCount.Add(1)
		leg.debugLastWriteDurationNs.Store(int64(time.Since(writeStarted)))
		leg.debugLastWriteNs.Store(time.Now().UnixNano())
		leg.debugWriterState.Store(0)
		c.commitSuccessfulDataWrite(attempt, leg.id)
		return true
	}

	for {
		// ACKs are cumulative, cheap to coalesce, and latency-sensitive. Publish the
		// latest ACK before selecting another DATA frame. This is priority at frame
		// boundaries; an already-started stream write cannot be preempted safely.
		if !writePendingAck() {
			return
		}

		// Explicit control is next. Frontier-rescue DATA is also a priority lane:
		// it exists specifically to repair a cumulative-ACK hole and must not wait
		// behind an ordinary per-leg DATA queue that may already be backpressured.
		leg.debugWriterState.Store(1)
		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case <-leg.ackWake:
			continue
		case control := <-leg.control:
			if !writeControl(control) {
				return
			}
			continue
		default:
		}
		select {
		case rescue := <-leg.rescue:
			if !writeAttempt(rescue) {
				return
			}
			continue
		default:
		}

		select {
		case <-c.done:
			return
		case <-leg.done:
			return
		case <-leg.ackWake:
			continue
		case control := <-leg.control:
			if !writeControl(control) {
				return
			}
		case rescue := <-leg.rescue:
			if !writeAttempt(rescue) {
				return
			}
		case attempt := <-leg.send:
			if attempt.ordinary && c.cfg.loadProbe != nil {
				c.cfg.loadProbe.dequeue(leg.id, len(attempt.record.Payload()))
			}
			if c.cfg.wakeTelemetry != nil {
				c.cfg.wakeTelemetry.released(leg)
			}
			c.kickSpaceSource("QUEUE_SPACE_RELEASE", int(leg.id))
			// ACK/control/rescue wakes can race with ordinary DATA becoming selectable.
			// Recheck those lanes before starting a normal DATA frame.
			if !writePendingAck() {
				return
			}
			select {
			case control := <-leg.control:
				if !writeControl(control) {
					return
				}
			default:
			}
			select {
			case rescue := <-leg.rescue:
				if !writeAttempt(rescue) {
					return
				}
			default:
			}
			if !writeAttempt(attempt) {
				return
			}
		}
	}
}

func (c *StreamEngine) releaseQueuedBoundedRescues(leg *streamLeg) {
	if leg == nil || c.boundedRescue == nil {
		return
	}
	for {
		select {
		case attempt := <-leg.rescue:
			if attempt.record != nil {
				c.clearAttemptAt(attempt.record, leg.id, true, attempt.started)
			}
			if attempt.boundedRescueReservation != 0 {
				c.boundedRescue.Release(attempt.boundedRescueReservation, "writer_exit_before_carrier_write", c.boundedRescueNow())
			}
		default:
			return
		}
	}
}

func (c *StreamEngine) legReadLoop(leg *streamLeg) {
	for {
		frame, err := readWireFrame(leg.conn, c)
		if err != nil {
			c.handleLegFailure(leg, err)
			return
		}
		switch frame.typ {
		case frameTypeActivate:
			if c.telemetry != nil {
				c.telemetry.markControlRx(leg.id, frame.typ)
			}
			c.activate()
		case frameTypeAck:
			c.debugLastAckFrameNs.Store(time.Now().UnixNano())
			c.debugLastAckValue.Store(frame.seq)
			if c.telemetry != nil {
				c.telemetry.markControlRx(leg.id, frame.typ)
			}
			var ackErr error
			if c.cfg.recoveryTraceHook != nil {
				ackErr = c.handleAckWithSource(frame.seq, int16(leg.id))
			} else {
				ackErr = c.handleAck(frame.seq)
			}
			if ackErr != nil {
				c.handleLegFailure(leg, ackErr)
				return
			}
		case frameTypeClose:
			c.traceCloseEvent(streamCloseFrameDecoded, leg.id, frame.seq, nil)
			if c.telemetry != nil {
				c.telemetry.markControlRx(leg.id, frame.typ)
			}
			// CLOSE carries the sender's final next-sequence value. It must exactly
			// match our cumulative receive frontier; both stale and future values
			// are invalid and cannot terminate the logical stream.
			if frame.seq != c.ackNext.Load() {
				c.handleLegFailure(leg, errors.New("multipath CLOSE sequence does not match receive frontier"))
				return
			}
			c.traceCloseEvent(streamCloseSequenceAccepted, leg.id, frame.seq, nil)
			c.StartGracefulClose(io.EOF)
			return
		case frameTypePathRTT:
			if err := c.handlePathRTTFrame(leg, frame.data); err != nil {
				c.handleLegFailure(leg, err)
				return
			}
		case frameTypeDrain:
			if err := c.handleDrainControlFrame(leg, frame.data); err != nil {
				c.handleLegFailure(leg, err)
				return
			}
		case frameTypeData:
			readAt := time.Now()
			select {
			case c.incoming <- dataFrame{seq: frame.seq, data: frame.data, leg: leg.id, generation: leg.generation, readAt: readAt}:
			case <-c.done:
				c.putBuffer(frame.data)
				return
			}
		}
	}
}

func (c *StreamEngine) handleLegFailure(leg *streamLeg, err error) {
	if leg.pathRTTEnabled && c.pathRTTCache != nil {
		c.pathRTTCache.invalidateLeg(leg.pathBinding)
	}
	leg.close(err)
	removed := false
	c.legsMu.Lock()
	if current := c.legs[leg.id]; current == leg {
		delete(c.legs, leg.id)
		c.retiring[leg.id] = leg
		removed = true
	}
	remaining := len(c.legs)
	c.legsMu.Unlock()
	if !removed {
		return
	}

	// Any record queued/writing on the dead leg must become retryable. A frame
	// that actually arrived before the failure is harmlessly deduplicated by seq.
	if p := c.cfg.frontierProbe; p != nil {
		p.failure(c, leg.id, time.Now())
	}
	reason := "carrier_failure"
	if err != nil {
		reason = err.Error()
	}
	c.txLedger.InvalidateLegWithReason(LegID(leg.id), leg.generation, reason)
	if c.assignedDecoupledEnabled() {
		go c.reassignFailedPending(leg.id)
	}

	// Do not allow a same-ID replacement to attach until *both* old worker
	// goroutines have exited. The old read/write loops use only an 8-bit leg ID
	// in TX bookkeeping, so overlap between generations would let a stale worker
	// clear state belonging to the replacement.
	go func() {
		leg.workers.Wait()
		c.legsMu.Lock()
		if current := c.retiring[leg.id]; current == leg {
			delete(c.retiring, leg.id)
		}
		c.legsMu.Unlock()
		close(leg.retired)
		c.kickRetry()
	}()

	// If the logical core is already closing, this is ordinary transport
	// teardown. Do not re-activate, emit leg-down callbacks, or start recovery
	// timers from the read/write goroutines racing with Close/fail.
	select {
	case <-c.done:
		return
	default:
	}
	if c.finalizing.Load() {
		return
	}
	// Graceful CLOSING still permits transport repair while DATA remains
	// outstanding. Once the TX tail is fully ACKed, however, a racing carrier EOF
	// is ordinary teardown and must not wake the booster, schedule reconnects, or
	// start a no-leg recovery timer.
	if c.closing.Load() && !c.hasOutstanding() {
		return
	}

	// Primary loss before the threshold is an emergency activation: leg 1 is
	// allowed to come up immediately so the logical stream has a chance to live.
	if leg.id == 0 && !c.active.Load() {
		c.activate()
	}
	if c.cfg.OnLegDown != nil {
		go c.cfg.OnLegDown(leg.id, err)
	}
	c.kickRetry()
	c.kickSpaceSource("CONNECTION_FAILURE", -1)
	c.forceAck()
	if remaining == 0 {
		epoch := c.recoveryEpoch.Add(1)
		go c.failIfNoLegRecovers(epoch)
	}
}

func (c *StreamEngine) failIfNoLegRecovers(epoch uint64) {
	timer := time.NewTimer(c.cfg.RecoveryTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return
	case <-timer.C:
		if c.recoveryEpoch.Load() == epoch && c.legCount() == 0 {
			c.fail(errors.New("multipath recovery timeout: no transport leg available"))
		}
	}
}

func (c *StreamEngine) kickRetry() {
	select {
	case c.retryCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) kickSpace() {
	c.kickSpaceSource("OTHER", -1)
}

func (c *StreamEngine) kickSpaceSource(source string, leg int) {
	c.wakeDecoupled()
	notification := streamSpaceNotification{}
	if c.cfg.wakeTelemetry != nil {
		notification = streamSpaceNotification{source: source, leg: leg, at: time.Now()}
	}
	select {
	case c.spaceWake <- notification:
		if c.cfg.wakeTelemetry != nil {
			c.cfg.wakeTelemetry.signal(source, true)
		}
	default:
		if c.cfg.wakeTelemetry != nil {
			c.cfg.wakeTelemetry.signal(source, false)
		}
	}
}

func (c *StreamEngine) kickFrontierRescue() {
	select {
	case c.frontierRescueCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) kickAck() {
	select {
	case c.ackWakeCh <- struct{}{}:
	default:
	}
}

func (c *StreamEngine) forceAck() {
	c.ackForce.Store(true)
	c.kickAck()
}

func (c *StreamEngine) retransmitLoop() {
	interval := c.cfg.RetransmitTimeout / 4
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if c.slowLegProtection() {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			c.maintainAdmission(now)
			if c.active.Load() && c.cfg.NotifyPeerOnActivate {
				c.notifyPeerActivate()
			}
			c.scheduleRetries()
			c.scheduleFrontierRescue()
		case <-c.retryCh:
			c.scheduleRetries()
			c.scheduleFrontierRescue()
		case <-c.frontierRescueCh:
			// ACK progress only needs an O(1) check of outstanding[ackedNext].
			// Do not rescan the entire outstanding map on every cumulative ACK.
			c.scheduleFrontierRescue()
		}
	}
}

type retryCandidate struct {
	record *StreamTXRecord
	avoid  int16
}

func streamLegAvailability(legs []*streamLeg) StreamLegAvailability {
	var available StreamLegAvailability
	for _, leg := range legs {
		if int(leg.id) < len(available) {
			available[leg.id] = true
		}
	}
	return available
}

func (c *StreamEngine) scheduleRetries() {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return
	}
	for _, candidate := range c.txLedger.PlanRetries(streamLegAvailability(legs)) {
		if c.benchmarkPendingOnly(candidate.Record) {
			continue
		}
		c.debugRetryScheduled.Add(1)
		// A full ordinary retry queue is deferred to the next scheduler turn.
		// Do not let it delay frontier rescue below.
		_ = c.tryEnqueueRecord(candidate.Record, candidate.Avoid)
	}
}

// scheduleFrontierRescue performs the R10 ACK-paced repair check. SMP3 v4 only
// has a cumulative ACK, so the sender can prove exactly one blocking sequence:
// outstanding[ackedNext]. We intentionally rescue only that record instead of
// guessing at later frames that may already be buffered by the receiver.
//
// The periodic retransmit loop remains a safety net for a frontier that never
// gets any ACK activity. The fast path is handleAck(): whenever cumulative ACK
// progress exposes a new head, it coalesces a frontierRescueCh wake so an old,
// already-overdue blocker can be repaired immediately rather than waiting up to
// another retransmit-loop interval.
func (c *StreamEngine) scheduleFrontierRescue() {
	c.debugRescueChecks.Add(1)
	now := time.Now()
	var live StreamLegAvailability
	for _, leg := range c.availableLegs() {
		live[LegID(leg.id)] = !leg.closed.Load()
	}
	candidate := c.txLedger.FrontierCandidate(now, c.cfg.RetransmitTimeout)
	if p := c.cfg.frontierProbe; p != nil {
		candidate = p.apply(candidate, now, c.cfg.RetransmitTimeout)
	}
	if c.slowLegProtection() {
		// Admission protection uses the attempt clock, not a fresh exposure
		// grace for each previously queued sequence.
		candidate = c.txLedger.FrontierCandidate(now, c.cfg.RetransmitTimeout)
	}
	candidate = c.protectFrontier(candidate, now)
	// Deciding a frontier repair is pure. Compute the preview before Policy A's
	// optional early return so diagnostic runs can count the exact opportunities
	// Policy A would see without steering the default Policy B action.
	plan := DecideStreamTXFrontierRepair(candidate, live)
	if c.cfg.recoveryTraceHook != nil && plan.Action == StreamTXFrontierRescue && plan.Record != nil {
		c.traceRecoveryEvent(c.phase20PolicyAShadowEvaluation(candidate, now))
	}
	var b0CandidateID uint64
	if c.boundedRescue == nil && c.cfg.rescueEventHook != nil && plan.Action == StreamTXFrontierRescue && plan.Record != nil {
		b0CandidateID = c.rescueCandidateSequence.Add(1)
		owner := candidate.owner
		var target *streamLeg
		if owner >= 0 && int(owner) < c.maxActiveLegs {
			target = c.chooseLeg(c.active.Load(), owner)
		}
		evidence := c.boundedFrontierEvidence(target, plan.Record, owner, 0, candidate.overdue)
		class := ClassifyStreamRescue(evidence)
		c.emitRescueObservation(StreamRescueEvent{
			At: now, SessionID: c.pathSessionID, CandidateID: b0CandidateID, PolicyVariant: "B0",
			Kind: "b0_frontier_candidate", Evidence: evidence, Class: class,
			PayloadBytes: uint64(len(plan.Record.Payload())), FinalDecision: "observed",
			FinalDecisionReason: "B0 legacy decision path continues",
		})
		c.linkB0RescueCandidateToACK(plan.Record.Sequence(), b0CandidateID, class, now)
	}
	if c.benchmarkPendingOnly(candidate.record) && !c.slowLegProtection() {
		return
	}
	if candidate.overdue {
		c.debugRescueOverdue.Add(1)
	}
	if plan.Action == StreamTXFrontierRescue {
		c.debugRescuePlans.Add(1)
		if c.cfg.recoveryTraceHook != nil && plan.Record != nil {
			ledger := c.txLedger.probeReferenceSnapshot(plan.Record, false)
			owner := candidate.owner
			var generation uint64
			var carrierWriteStartedAt time.Time
			var carrierWriteInProgress bool
			var carrierWriteAge time.Duration
			var ownerWriteBPS float64
			state := streamAdmissionLeg{}
			if owner >= 0 && int(owner) < c.maxActiveLegs {
				generation = c.legGeneration[uint8(owner)].Load()
				if ownerLeg := c.getLeg(uint8(owner)); ownerLeg != nil {
					ownerWriteBPS, _, _ = ownerLeg.perf.snapshot()
					if writeStartedNS := ownerLeg.writeStartedNs.Load(); writeStartedNS != 0 &&
						ledger.InTransit && ledger.TransitLeg == uint8(owner) {
						carrierWriteStartedAt = time.Unix(0, writeStartedNS)
						carrierWriteAge = now.Sub(carrierWriteStartedAt)
						carrierWriteInProgress = carrierWriteAge >= 0
					}
				}
				c.admission.mu.Lock()
				state = c.admission.legs[owner]
				c.admission.mu.Unlock()
			}
			stateName := "normal"
			if state.restricted {
				stateName = "restricted"
			} else if state.recovering {
				stateName = "recovering"
			}
			predictedRemaining := time.Duration(0)
			estimateBasis := "unavailable"
			if carrierWriteInProgress && ownerWriteBPS > 0 && len(plan.Record.Payload()) > 0 {
				predictedService := time.Duration(float64(len(plan.Record.Payload())) / ownerWriteBPS * float64(time.Second))
				predictedRemaining = predictedService - carrierWriteAge
				if predictedRemaining < 0 {
					predictedRemaining = 0
				}
				estimateBasis = "owner_completed_write_bps_ewma_minus_current_in_transit_write_age"
			}
			c.traceRecoveryEvent(streamRecoveryTraceEvent{
				Kind: "frontier_rescue_decision", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: plan.Record.Sequence(), FrameSequence: plan.Record.Sequence(), FrameType: "data",
				LegID: owner, PeerLegID: plan.Avoid, LegGeneration: generation, RecordGeneration: ledger.OriginGeneration,
				PayloadBytes: len(plan.Record.Payload()), ReferenceAt: candidate.reference, PolicyAt: now,
				Age: now.Sub(candidate.reference), StateBefore: stateName, ReasonBefore: state.reason,
				LedgerObservedAt: ledger.LedgerObservedAt, LedgerPresent: ledger.Exists,
				AckedNext: ledger.AckedNext, InTransit: ledger.InTransit,
				TransitLeg: ledger.TransitLeg, TransitSince: ledger.TransitSince,
				RescueInTransit: ledger.RescueInTransit, RescueLeg: ledger.RescueLeg, RescueSince: ledger.RescueSince,
				FirstWriteAt: ledger.FirstWriteAt, LastSentAt: ledger.LastSentAt, FrontierAt: ledger.FrontierAt,
				SendCount: ledger.SendCount, Action: "enqueue_frontier_rescue", Applied: true,
				Decision: &streamRecoveryDecisionSnapshot{
					FrontierOverdue: candidate.overdue, RescueAction: "rescue", RescueAvoidLeg: plan.Avoid,
					CarrierWriteInProgress: carrierWriteInProgress, CarrierWriteLeg: owner,
					CarrierWriteAge: carrierWriteAge, WriteStartedAt: carrierWriteStartedAt,
					PredictedServiceRemaining: predictedRemaining, PredictedServiceEstimateBasis: estimateBasis,
				},
			})
		}
	}
	switch plan.Action {
	case StreamTXFrontierNeedActivation:
		if !c.active.Load() {
			c.activate()
		}
	case StreamTXFrontierRescue:
		if c.boundedRescue != nil {
			c.scheduleBoundedFrontierRescue(plan, candidate)
			return
		}
		if c.slowLegProtection() && plan.Avoid >= 0 && int(plan.Avoid) < c.maxActiveLegs {
			id := uint8(plan.Avoid)
			c.admission.mu.Lock()
			state := c.admission.legs[id]
			c.admission.mu.Unlock()
			if state.restricted {
				leg := c.bestAlternateLeg(int16(id))
				if leg != nil {
					budget := 256 << 10
					if c.cfg.ChunkSize > budget {
						budget = c.cfg.ChunkSize
					}
					for _, record := range c.txLedger.RestrictedFrontierBatch(plan.Record, plan.Avoid, now, state.since, c.cfg.RetransmitTimeout, budget) {
						candidateID := uint64(0)
						if b0CandidateID != 0 && record.Sequence() == plan.Record.Sequence() {
							candidateID = b0CandidateID
						}
						queued := false
						if candidateID != 0 {
							queued = c.tryQueueRescueOwnedAttemptWithCandidateID(leg, record, plan.Avoid, StreamRescueOtherCoreRetry, 0, candidateID)
						} else {
							queued = c.tryQueueRescueOwned(leg, record, plan.Avoid)
						}
						if !queued {
							break
						}
					}
					return
				}
			}
		}
		if err := c.enqueueRescueWithCandidateID(plan.Record, plan.Avoid, b0CandidateID); err != nil && !errors.Is(err, ErrStreamClosed) {
			c.fail(err)
		}
	}
}

func (c *StreamEngine) scheduleBoundedFrontierRescue(plan StreamTXFrontierPlan, candidate StreamTXFrontierCandidate) {
	if plan.Record == nil || c.boundedRescue == nil {
		return
	}
	owner := candidate.owner
	if owner < 0 || int(owner) >= c.maxActiveLegs {
		evidence := c.boundedFrontierEvidence(nil, plan.Record, owner, 0, candidate.overdue)
		decision := c.boundedRescue.Reserve(evidence, uint64(len(plan.Record.Payload())), c.boundedRescueNow())
		if decision.Admitted && decision.Exempt {
			legs := c.availableLegs()
			var target *streamLeg
			if len(legs) == 1 {
				target = legs[0]
			} else if len(legs) > 1 {
				target = c.chooseLeg(true, owner)
			}
			c.tryQueueRescueOwnedAttemptWithCandidateID(target, plan.Record, evidence.OwnerLeg, decision.Class, 0, decision.CandidateID)
		}
		// Unclassifiable candidates do not gain a budget exemption. The ordinary
		// retry planner remains independent and may still schedule a necessary retry.
		return
	}
	id := uint8(owner)
	c.admission.mu.Lock()
	state := c.admission.legs[id]
	c.admission.mu.Unlock()
	if state.restricted && c.slowLegProtection() {
		leg := c.bestAlternateLeg(int16(id))
		if leg == nil {
			return
		}
		budget := 256 << 10
		if c.cfg.ChunkSize > budget {
			budget = c.cfg.ChunkSize
		}
		for _, record := range c.txLedger.RestrictedFrontierBatch(plan.Record, int16(id), c.boundedRescueNow(), state.since, c.cfg.RetransmitTimeout, budget) {
			if !c.tryQueueBoundedFrontierRescue(leg, record, int16(id), state.recoveryEpoch, candidate.overdue) {
				// The same session budget covers both legs. Once the head candidate
				// is denied, do not skip it and duplicate later ordered DATA.
				return
			}
		}
		return
	}
	legs := c.availableLegs()
	if len(legs) == 0 {
		return
	}
	var target *streamLeg
	if len(legs) == 1 {
		target = legs[0]
	} else {
		target = c.chooseLeg(true, int16(id))
	}
	if target == nil {
		return
	}
	c.tryQueueBoundedFrontierRescue(target, plan.Record, int16(id), state.recoveryEpoch, candidate.overdue)
}

// linkB0RescueCandidateToACK joins a read-only B0 candidate observation to its
// later cumulative ACK. It is active only when the opt-in observer is present;
// no transport or scheduler decision reads this map.
func (c *StreamEngine) linkB0RescueCandidateToACK(sequence, candidateID uint64, class StreamRescueClass, at time.Time) {
	hook := c.cfg.rescueEventHook
	if hook == nil || candidateID == 0 {
		return
	}
	var event *StreamRescueEvent
	c.rescueCandidateAckMu.Lock()
	select {
	case <-c.done:
		c.rescueCandidateAckMu.Unlock()
		return
	default:
	}
	ackedNext := c.txAckedNext.Load()
	if sequence < ackedNext {
		event = &StreamRescueEvent{
			At: at, SessionID: c.pathSessionID, CandidateID: candidateID, PolicyVariant: "B0",
			Kind: "candidate_ack_outcome", Evidence: StreamRescueEvidence{Sequence: sequence, AckedNext: ackedNext},
			Class: class, AckedAtDecision: true, AckedAtAttempt: true, AckOutcome: "acked_before_link",
			FinalDecision: "data_acked", FinalDecisionReason: "cumulative_ack_advanced_before_candidate_link",
		}
	} else {
		if c.rescueCandidateAckLinks == nil {
			c.rescueCandidateAckLinks = make(map[uint64][]streamRescueCandidateAckLink)
		}
		links := c.rescueCandidateAckLinks[sequence]
		if len(links) >= streamRescueCandidateAckLinksPerRecord {
			c.rescueCandidateAckDropped.Add(1)
			event = &StreamRescueEvent{
				At: at, SessionID: c.pathSessionID, CandidateID: candidateID, PolicyVariant: "B0",
				Kind: "candidate_ack_link_dropped", Evidence: StreamRescueEvidence{Sequence: sequence, AckedNext: ackedNext},
				Class: class, FinalDecision: "link_dropped", FinalDecisionReason: "per_record_ack_link_limit",
			}
		} else {
			c.rescueCandidateAckLinks[sequence] = append(links, streamRescueCandidateAckLink{candidateID: candidateID, class: class})
		}
	}
	c.rescueCandidateAckMu.Unlock()
	if event != nil {
		c.emitRescueObservation(*event)
	}
}

func (c *StreamEngine) emitB0RescueCandidateACKs(ackedNext uint64, at time.Time) {
	hook := c.cfg.rescueEventHook
	if hook == nil {
		return
	}
	var events []StreamRescueEvent
	c.rescueCandidateAckMu.Lock()
	for sequence, links := range c.rescueCandidateAckLinks {
		if sequence >= ackedNext {
			continue
		}
		for _, link := range links {
			events = append(events, StreamRescueEvent{
				At: at, SessionID: c.pathSessionID, CandidateID: link.candidateID, PolicyVariant: "B0",
				Kind: "candidate_ack_outcome", Evidence: StreamRescueEvidence{Sequence: sequence, AckedNext: ackedNext},
				Class: link.class, AckedAtDecision: false, AckedAtAttempt: true, AckOutcome: "acked_after_candidate",
				FinalDecision: "data_acked", FinalDecisionReason: "cumulative_ack_advanced_past_candidate_sequence",
			})
		}
		delete(c.rescueCandidateAckLinks, sequence)
	}
	c.rescueCandidateAckMu.Unlock()
	for _, event := range events {
		c.emitRescueObservation(event)
	}
}

func (c *StreamEngine) handleAck(next uint64) error {
	return c.handleAckWithSource(next, -1)
}

func (c *StreamEngine) handleAckWithSource(next uint64, sourceLeg int16) error {
	result := c.txLedger.ApplyACK(next, time.Now())
	if c.boundedRescue != nil && result.Disposition != StreamTXACKFuture {
		c.boundedRescue.AckThrough(result.AckedNext, result.LastACKProgress)
	}
	// Keep the sender-side cumulative frontier available to the bounded HOL
	// correction without taking the ledger mutex on every new assignment.
	advanceAtomicUint64Max(&c.txAckedNext, result.AckedNext)
	c.emitB0RescueCandidateACKs(result.AckedNext, result.LastACKProgress)
	if result.Disposition == StreamTXACKFuture {
		// This ACK cannot safely prove delivery of any local payload. A live
		// alpha2 test observed such an anomaly around transport rejoin; treating
		// it as fatal caused an otherwise recoverable leg to enter a reconnect
		// storm. Ignore it without advancing TX state and let valid cumulative
		// ACKs/retransmission drive progress.
		count := result.FutureCount
		if c.cfg.OnFutureAck != nil && (count == 1 || count&(count-1) == 0) {
			c.cfg.OnFutureAck(next, result.Max, count)
		}
		return nil
	}
	if result.Disposition == StreamTXACKNoProgress {
		return nil
	}
	if result.Released > 0 {
		if c.cfg.recoveryTraceHook != nil {
			var ackGeneration uint64
			if sourceLeg >= 0 && int(sourceLeg) < len(c.legGeneration) {
				ackGeneration = c.legGeneration[sourceLeg].Load()
			}
			c.traceRecoveryEvent(streamRecoveryTraceEvent{
				Kind: "ack_frontier_advanced", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
				RecordSequence: result.AckedNext, FrameSequence: next, FrameType: "ack",
				LegID: sourceLeg, LegGeneration: ackGeneration, AckedNext: result.AckedNext,
				WriteResultBytes: int(result.RetiredBytes), RetiredFrames: result.Released,
				PolicyAt: result.LastACKProgress, Action: "retire_unique_tx_and_advance_frontier", Applied: true,
			})
		}
		c.txAckedTotal.Add(result.RetiredBytes)
		if p := c.cfg.frontierProbe; p != nil {
			p.ack(result.AckedNext, time.Now())
			p.observe(c.txLedger.frontierSnapshot(), time.Now())
		}
		c.debugAckAdvances.Add(1)
		c.debugAckRetiredRecords.Add(uint64(result.Released))
		for _, legResult := range result.PerLeg {
			c.debugAckRetiredBytes.Add(legResult.AckedBytes)
		}
		now := result.LastACKProgress
		for {
			old := c.activationBacklog.Load()
			remaining := uint64(0)
			if old > result.RetiredBytes {
				remaining = old - result.RetiredBytes
			}
			if c.activationBacklog.CompareAndSwap(old, remaining) {
				break
			}
		}
		if provider, ok := c.capacityProvider.(StreamActivationProvider); ok {
			provider.ObserveActivationDemand(0, result.RetiredBytes, now)
		}
		c.lastAckProgressNs.Store(now.UnixNano())
		for legID, legResult := range result.PerLeg {
			bytes := legResult.AckedBytes
			if bytes <= 0 || legID >= len(c.txAckedUseful) {
				continue
			}
			c.txAckedUseful[legID].Add(bytes)
			if c.capacityProvider != nil {
				sample := bytes
				if c.slowLegProtection() {
					sample = legResult.SampleBytes
				}
				c.capacityProvider.Ack(uint8(legID), sample, now)
			}
			if leg := c.getLeg(uint8(legID)); leg != nil {
				leg.perf.observeAck(int(bytes), now)
			}
		}
	}
	c.releaseInflight(result.Released)
	c.admissionACK(result)
	// ACK retirement releases logical admission capacity. A producer may be
	// asleep in enqueueRecord after all physical queues rejected its attempt;
	// wake it even when no leg writer happened to dequeue another record.
	if result.Released > 0 {
		c.kickSpaceSource("ACK_LEDGER_RELEASE", -1)
	}
	// R10: cumulative ACK progress can expose another already-overdue blocker.
	// Wake the O(1) frontier checker immediately instead of waiting for the next
	// retransmit ticker. Duplicate/non-progress ACKs returned above and do not
	// create repair work.
	c.kickFrontierRescue()
	return nil
}

type atomicUint64MaxTarget interface {
	Load() uint64
	CompareAndSwap(old, next uint64) bool
}

// advanceAtomicUint64Max keeps the cached cumulative frontier monotonic when
// ACK callbacks from multiple legs race. A stale callback must retry its CAS
// against the newest value rather than overwrite it with a later Store.
func advanceAtomicUint64Max(target atomicUint64MaxTarget, next uint64) {
	for {
		old := target.Load()
		if next <= old || target.CompareAndSwap(old, next) {
			return
		}
	}
}

func (c *StreamEngine) requestAck(next uint64) {
	for {
		old := c.ackNext.Load()
		if next <= old || c.ackNext.CompareAndSwap(old, next) {
			break
		}
	}
	c.kickAck()
}

func (c *StreamEngine) ackLoop() {
	ticker := time.NewTicker(c.cfg.AckInterval)
	defer ticker.Stop()
	var sent uint64
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		case <-c.ackWakeCh:
			// Wake quickly after recovery/leg join, but coalescing still happens
			// because ackNext is cumulative.
		}
		next := c.ackNext.Load()
		force := c.ackForce.Swap(false)
		if next == 0 || (next <= sent && !force) || c.legCount() == 0 {
			if force && c.legCount() == 0 {
				c.ackForce.Store(true)
			}
			continue
		}
		if c.sendAckFrame(next, force) {
			sent = next
		} else if force {
			c.ackForce.Store(true)
		}
	}
}

func (c *StreamEngine) queueAck(leg *streamLeg, value uint64, force bool) bool {
	if leg == nil || leg.closed.Load() {
		return false
	}
	updated := false
	for {
		old := leg.ackPending.Load()
		if value <= old {
			break
		}
		if leg.ackPending.CompareAndSwap(old, value) {
			updated = true
			break
		}
	}
	if force {
		leg.ackForce.Store(true)
	}
	select {
	case <-leg.done:
		return false
	default:
	}
	if updated || force {
		select {
		case leg.ackWake <- struct{}{}:
		default:
		}
	}
	return true
}

func (c *StreamEngine) sendAckFrame(value uint64, force bool) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}
	// Never synchronously write ACKs while iterating over carriers. Each leg has
	// its own writer and cumulative ACK slot, so one slow/black-holed transport
	// cannot head-of-line block ACK progress on a healthy transport.
	scheduled := false
	for _, leg := range legs {
		if c.queueAck(leg, value, force) {
			scheduled = true
		}
	}
	return scheduled
}

func (c *StreamEngine) queueControl(leg *streamLeg, typ byte, value uint64, wait bool) (chan error, bool) {
	if leg == nil || leg.closed.Load() {
		return nil, false
	}
	var done chan error
	if wait {
		done = make(chan error, 1)
	}
	control := legControl{typ: typ, value: value, done: done}
	select {
	case <-c.done:
		return nil, false
	case <-leg.done:
		return nil, false
	case leg.control <- control:
		return done, true
	default:
		return nil, false
	}
}

// CLOSE has no wire acknowledgement. Queue and write success remain distinct;
// a stalled/full last carrier must not strand finalization forever.
func (c *StreamEngine) preferredCloseLeg(legs []*streamLeg) (uint8, time.Duration, bool) {
	if !c.cfg.ExperimentalPathAware || c.pathRTTCache == nil {
		return 0, 0, false
	}
	var preferred uint8
	var preferredRTT time.Duration
	var firstRTT time.Duration
	active := 0
	allEqual := true
	for _, leg := range legs {
		if leg == nil || int(leg.id) >= c.maxActiveLegs || leg.closed.Load() {
			continue
		}
		active++
		feedback, found := c.PathFeedback(LegID(leg.id))
		if !found || feedback.RTT <= 0 {
			return 0, 0, false
		}
		if active == 1 {
			preferred, preferredRTT = leg.id, feedback.RTT
			firstRTT = feedback.RTT
		} else {
			if feedback.RTT != firstRTT {
				allEqual = false
			}
			if feedback.RTT < preferredRTT {
				preferred, preferredRTT = leg.id, feedback.RTT
			}
		}
	}
	// A preference is useful only when both active Carriers have a fresh,
	// authenticated sample for this session and generation. Otherwise retain the
	// legacy any-success behavior rather than guessing from leg order or capacity.
	if active < 2 || allEqual {
		return 0, 0, false
	}
	return preferred, preferredRTT, true
}

func (c *StreamEngine) sendCloseFrame(value uint64) bool {
	legs := c.availableLegs()
	if len(legs) == 0 {
		return false
	}
	preferredID, preferredGrace, hasPreferred := c.preferredCloseLeg(legs)
	timeout := c.cfg.RecoveryTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var preferredFallbackTimer *time.Timer
	var preferredFallbackC <-chan time.Time
	defer func() {
		if preferredFallbackTimer != nil {
			preferredFallbackTimer.Stop()
		}
	}()
	retryInterval := 50 * time.Millisecond
	if hasPreferred {
		// Queue pressure is transient, so retry well before the preferred path's
		// RTT fallback expires. A fixed 50 ms retry can miss the entire grace
		// window on a 20 ms path.
		retryInterval = preferredGrace / 8
		if retryInterval < time.Millisecond {
			retryInterval = time.Millisecond
		}
		if retryInterval > 50*time.Millisecond {
			retryInterval = 50 * time.Millisecond
		}
	}
	tick := time.NewTicker(retryInterval)
	defer tick.Stop()
	type closeWriteResult struct {
		leg uint8
		err error
	}
	queued := make([]bool, c.maxActiveLegs)
	results := make(chan closeWriteResult, len(legs))
	var preferredFailed bool
	fallbackWriteSucceeded := false
	preferredCanStillSend := func() bool {
		if !hasPreferred {
			return false
		}
		leg := c.getLeg(preferredID)
		return leg != nil && !leg.closed.Load()
	}
	for {
		live := false
		for _, leg := range legs {
			if leg.closed.Load() {
				continue
			}
			live = true
			if int(leg.id) < len(queued) && !queued[leg.id] {
				if done, ok := c.queueControl(leg, frameTypeClose, value, true); ok {
					queued[leg.id] = true
					go func(id uint8, current *streamLeg, completion <-chan error) {
						select {
						case err := <-completion:
							select {
							case results <- closeWriteResult{leg: id, err: err}:
							case <-c.done:
							}
						case <-current.done:
							select {
							case results <- closeWriteResult{leg: id, err: net.ErrClosed}:
							case <-c.done:
							}
						case <-c.done:
						}
					}(leg.id, leg, done)
				}
			}
		}
		if !live {
			return fallbackWriteSucceeded
		}
		if fallbackWriteSucceeded && (preferredFailed || !preferredCanStillSend()) {
			return true
		}
		select {
		case <-c.done:
			return false
		case <-timer.C:
			// A successful redundant local write is a bounded fallback if the
			// preferred Carrier never completes its local Write.
			return fallbackWriteSucceeded
		case result := <-results:
			id, err := result.leg, result.err
			c.traceCloseEvent(streamCloseWriteResult, id, value, err)
			if err == nil && (!hasPreferred || id == preferredID) {
				return true
			}
			if err != nil && hasPreferred && id == preferredID {
				preferredFailed = true
				if fallbackWriteSucceeded {
					return true
				}
				continue
			}
			if err == nil {
				fallbackWriteSucceeded = true
				if !hasPreferred || preferredFailed || !preferredCanStillSend() {
					return true
				}
				if preferredFallbackTimer == nil {
					preferredFallbackTimer = time.NewTimer(preferredGrace)
					preferredFallbackC = preferredFallbackTimer.C
				}
				c.traceCloseEvent(streamCloseWaitingPreferred, preferredID, value, nil)
			}
		case <-preferredFallbackC:
			if fallbackWriteSucceeded {
				return true
			}
		case <-tick.C:
		}
	}
}

func (c *StreamEngine) syncRXStatsFromWindow(window *StreamRXWindow) {
	pending := int64(window.PendingFrames())
	c.rxPendingFrames.Store(pending)
	for {
		old := c.rxMaxPendingFrames.Load()
		if pending <= old || c.rxMaxPendingFrames.CompareAndSwap(old, pending) {
			break
		}
	}
	c.rxPendingBytes.Store(window.PendingBytes())
	gapSince := window.GapSince()
	if gapSince.IsZero() {
		c.rxGapSinceUnixNs.Store(0)
	} else {
		c.rxGapSinceUnixNs.Store(gapSince.UnixNano())
		age := time.Since(gapSince)
		if age > 0 {
			for {
				old := c.rxMaxGapAgeNs.Load()
				if int64(age) <= old || c.rxMaxGapAgeNs.CompareAndSwap(old, int64(age)) {
					break
				}
			}
		}
	}
}

func (c *StreamEngine) rxLoop() {
	window := NewStreamRXWindow(c.cfg.MaxReorderFrames)
	defer func() {
		for _, frame := range window.DrainPending() {
			c.putBuffer(frame.Payload)
		}
		c.syncRXStatsFromWindow(window)
	}()
	for {
		select {
		case <-c.done:
			return
		case frame, open := <-c.incoming:
			if !open {
				return
			}
			rxFrame := StreamRXFrame{
				Sequence: frame.seq,
				Leg:      LegID(frame.leg),
				Payload:  frame.data,
			}
			if frame.seq > c.debugRXHighest.Load() {
				c.debugRXHighest.Store(frame.seq)
			}
			rxObservedAt := time.Now()
			disposition, err := window.Insert(rxFrame, rxObservedAt)
			if c.telemetry != nil {
				c.telemetry.recordDataRXSequence(StreamDataRXSequenceEvent{
					CarrierReadAt:  frame.readAt,
					CoreAcceptedAt: rxObservedAt,
					Sequence:       frame.seq,
					LegID:          frame.leg,
					LegGeneration:  frame.generation,
					PayloadBytes:   uint32(len(frame.data)),
					Disposition:    streamRXDispositionName(disposition, err),
					Unique:         err == nil && (disposition == StreamRXReady || disposition == StreamRXBuffered),
				})
			}
			c.debugRXExpected.Store(window.Expected())
			if c.cfg.recoveryTraceHook != nil {
				dispositionName := "unique_contiguous"
				switch disposition {
				case StreamRXDuplicate:
					dispositionName = "duplicate"
				case StreamRXBufferedDuplicate:
					dispositionName = "buffered_duplicate"
				case StreamRXBuffered:
					dispositionName = "buffered_out_of_order"
				}
				c.traceRecoveryEvent(streamRecoveryTraceEvent{
					Kind: "rx_data_received", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
					RecordSequence: frame.seq, FrameSequence: frame.seq, FrameType: "data",
					LegID: int16(frame.leg), PayloadBytes: len(frame.data), Action: dispositionName,
					Applied: err == nil, ObservedAt: rxObservedAt,
				})
			}
			if err != nil {
				c.putBuffer(frame.data)
				c.fail(err)
				return
			}
			switch disposition {
			case StreamRXDuplicate:
				if c.telemetry != nil {
					c.telemetry.markDataRxDuplicate(frame.leg, uint64(len(frame.data)))
				}
				c.putBuffer(frame.data)
				c.forceAck()
				continue
			case StreamRXBufferedDuplicate:
				if c.telemetry != nil {
					c.telemetry.markDataRxDuplicate(frame.leg, uint64(len(frame.data)))
				}
				c.putBuffer(frame.data)
				continue
			case StreamRXBuffered:
				if c.telemetry != nil {
					c.telemetry.markDataRxUnique(frame.leg)
				}
				if int(frame.leg) < len(c.rxUniqueBytes) {
					c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
				}
				c.syncRXStatsFromWindow(window)
				continue
			}
			if c.telemetry != nil {
				c.telemetry.markDataRxUnique(frame.leg)
			}
			if int(frame.leg) < len(c.rxUniqueBytes) {
				c.rxUniqueBytes[frame.leg].Add(uint64(len(frame.data)))
			}
			for {
				pipeWriteStartedAt := time.Now()
				if c.cfg.recoveryTraceHook != nil {
					c.traceRecoveryEvent(streamRecoveryTraceEvent{
						Kind: "rx_contiguous_delivery", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
						RecordSequence: rxFrame.Sequence, FrameSequence: rxFrame.Sequence, FrameType: "data",
						LegID: int16(rxFrame.Leg), PayloadBytes: len(rxFrame.Payload), Applied: true,
						ObservedAt: pipeWriteStartedAt,
					})
				}
				pipeWriteErr := writeAll(c.pipeConn, rxFrame.Payload)
				pipeWriteReturnedAt := time.Now()
				if c.cfg.recoveryTraceHook != nil {
					writeResultBytes := 0
					if pipeWriteErr == nil {
						writeResultBytes = len(rxFrame.Payload)
					}
					traceEvent := streamRecoveryTraceEvent{
						Kind: "rx_app_pipe_write_return", Endpoint: streamRecoveryEndpoint(c.cfg.PathRole),
						RecordSequence: rxFrame.Sequence, FrameSequence: rxFrame.Sequence, FrameType: "data",
						LegID: int16(rxFrame.Leg), PayloadBytes: len(rxFrame.Payload),
						WriteBytes: len(rxFrame.Payload), WriteResultBytes: writeResultBytes,
						Applied: pipeWriteErr == nil, ObservedAt: pipeWriteReturnedAt,
					}
					if pipeWriteErr != nil {
						traceEvent.Error = pipeWriteErr.Error()
					}
					c.traceRecoveryEvent(traceEvent)
				}
				if pipeWriteErr != nil {
					c.putBuffer(rxFrame.Payload)
					if !c.closing.Load() {
						c.fail(pipeWriteErr)
					}
					return
				}
				c.rxDeliveredBytes.Add(uint64(len(rxFrame.Payload)))
				c.putBuffer(rxFrame.Payload)
				window.CommitReady(time.Now())
				next, exists := window.PopContiguous()
				c.syncRXStatsFromWindow(window)
				if !exists {
					break
				}
				rxFrame = next
			}
			c.requestAck(window.Expected())
		}
	}
}

func streamRXDispositionName(disposition StreamRXDisposition, err error) string {
	if err != nil {
		return "rejected"
	}
	switch disposition {
	case StreamRXReady:
		return "unique_contiguous"
	case StreamRXBuffered:
		return "unique_buffered"
	case StreamRXDuplicate:
		return "duplicate"
	case StreamRXBufferedDuplicate:
		return "buffered_duplicate"
	default:
		return "unknown"
	}
}

func encodeControlFrame(typ byte, value uint64) [frameHeaderSize]byte {
	var header [frameHeaderSize]byte
	_ = EncodeStreamFrameHeader(header[:], StreamFrameHeader{
		Type:  StreamFrameType(typ),
		Value: value,
	})
	return header
}

func writeDataFrame(conn StreamLeg, frame dataFrame) error {
	_, _, err := writeDataFrameWithStart(conn, frame, nil)
	return err
}

// streamDataWriteStartLeg starts recovery accounting from the first actual
// carrier Write call for a DATA frame, rather than from the earlier point at
// which the frame buffers are prepared for net.Buffers.WriteTo.
type streamDataWriteStartLeg struct {
	StreamLeg
	beforeFirstWrite func() error
	started          bool
}

func (c *streamDataWriteStartLeg) Write(p []byte) (int, error) {
	if !c.started {
		if c.beforeFirstWrite != nil {
			if err := c.beforeFirstWrite(); err != nil {
				return 0, err
			}
		}
		c.started = true
	}
	return c.StreamLeg.Write(p)
}

func writeDataFrameWithStart(conn StreamLeg, frame dataFrame, beforeWrite func()) (int64, bool, error) {
	var beforeFirstWrite func() error
	if beforeWrite != nil {
		beforeFirstWrite = func() error {
			beforeWrite()
			return nil
		}
	}
	return writeDataFrameWithGate(conn, frame, beforeFirstWrite)
}

func writeDataFrameWithGate(conn StreamLeg, frame dataFrame, beforeWrite func() error) (int64, bool, error) {
	var header [frameHeaderSize]byte
	if err := EncodeStreamFrameHeader(header[:], StreamFrameHeader{
		Type:   StreamFrameData,
		Value:  frame.seq,
		Length: uint32(len(frame.data)),
	}); err != nil {
		return 0, false, err
	}
	buffers := net.Buffers{header[:], frame.data}
	writeLeg := &streamDataWriteStartLeg{
		StreamLeg:        conn,
		beforeFirstWrite: beforeWrite,
	}
	written, err := buffers.WriteTo(writeLeg)
	expected := int64(frameHeaderSize + len(frame.data))
	if err == nil && written != expected {
		err = io.ErrShortWrite
	}
	return written, writeLeg.started, err
}

func streamDataPayloadAccepted(wireBytes int64, payloadBytes int) uint64 {
	payload := wireBytes - frameHeaderSize
	if payload <= 0 || payloadBytes <= 0 {
		return 0
	}
	if payload > int64(payloadBytes) {
		payload = int64(payloadBytes)
	}
	return uint64(payload)
}

func readWireFrame(conn StreamLeg, core *StreamEngine) (wireFrame, error) {
	header, err := ReadStreamFrameHeader(conn)
	if err != nil {
		return wireFrame{}, err
	}
	frame := wireFrame{typ: byte(header.Type), seq: header.Value}
	if header.Type == StreamFramePathRTT || header.Type == StreamFrameDrain {
		frame.data = make([]byte, int(header.Length))
		if _, err := io.ReadFull(conn, frame.data); err != nil {
			return wireFrame{}, err
		}
		return frame, nil
	}
	if header.Type != StreamFrameData {
		return frame, nil
	}
	length := int(header.Length)
	if length <= core.cfg.ChunkSize {
		frame.data = core.getBuffer()[:length]
	} else {
		frame.data = make([]byte, length)
	}
	if _, err := io.ReadFull(conn, frame.data); err != nil {
		core.putBuffer(frame.data)
		return wireFrame{}, err
	}
	return frame, nil
}

func writePathRTTFrame(conn StreamLeg, message []byte) error {
	if len(message) != StreamRTTMessageSize {
		return ErrStreamPathMessage
	}
	frame := make([]byte, frameHeaderSize+len(message))
	if err := EncodeStreamFrameHeader(frame[:frameHeaderSize], StreamFrameHeader{
		Type: StreamFramePathRTT, Length: uint32(len(message)),
	}); err != nil {
		return err
	}
	copy(frame[frameHeaderSize:], message)
	return writeAll(conn, frame)
}

func writeStreamControlPayload(conn StreamLeg, typ StreamFrameType, payload []byte) error {
	if len(payload) == 0 || uint64(len(payload)) > uint64(MaxStreamFramePayload) {
		return ErrStreamDrainMessage
	}
	frame := make([]byte, frameHeaderSize+len(payload))
	if err := EncodeStreamFrameHeader(frame[:frameHeaderSize], StreamFrameHeader{Type: typ, Length: uint32(len(payload))}); err != nil {
		return err
	}
	copy(frame[frameHeaderSize:], payload)
	return writeAll(conn, frame)
}

func writeAll(writer io.Writer, buffer []byte) error {
	for len(buffer) > 0 {
		n, err := writer.Write(buffer)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		buffer = buffer[n:]
	}
	return nil
}

// Activate forces the stream booster state and is intentionally a small host callback seam.
func (c *StreamEngine) Activate() { c.activate() }

// HasLeg reports whether a concrete leg generation is currently attached.
func (c *StreamEngine) HasLeg(id LegID) bool { return c.hasLeg(uint8(id)) }

// LegGeneration returns the generation number of the currently attached
// carrier for a logical Leg ID. The value is diagnostic; callers must never
// use it as authentication in place of the READY/Leg-join protocol.
func (c *StreamEngine) LegGeneration(id LegID) (uint64, bool) {
	leg := c.getLeg(uint8(id))
	if leg == nil || leg.closed.Load() {
		return 0, false
	}
	return leg.generation, true
}

// LegCount reports the number of currently attached legs.
func (c *StreamEngine) LegCount() int { return c.legCount() }

// Active reports whether the stream booster has been activated.
func (c *StreamEngine) Active() bool { return c.active.Load() }

// Closing reports whether application close has started while allowing live
// carrier repair to continue during the graceful drain phase.
func (c *StreamEngine) Closing() bool { return c.closing.Load() }

// Finalizing reports whether the logical stream has entered terminal teardown.
func (c *StreamEngine) Finalizing() bool { return c.finalizing.Load() }

// BeginFinalizingForTest exposes the existing lifecycle transition to package
// tests without exposing the lifecycle mutex or transport registry.
func (c *StreamEngine) BeginFinalizingForTest() bool { return c.beginFinalizing() }

// SetClosingForTest is reserved for compatibility tests that exercise repair
// behavior while a graceful close is in progress.
func (c *StreamEngine) SetClosingForTest(closing bool) { c.closing.Store(closing) }

// TXLedger returns the engine-owned ledger for the legacy semantic test
// adapter. Production callers should use Snapshot and not mutate the ledger.
func (c *StreamEngine) TXLedger() *StreamTXLedger { return c.txLedger }

// FrontierRescueAttempts returns the current repair-attempt counter.
func (c *StreamEngine) FrontierRescueAttempts() uint64 { return c.frontierRescueAttempts.Load() }

// SetActiveForTest is reserved for compatibility tests that exercise scheduler
// transitions without a real activation threshold.
func (c *StreamEngine) SetActiveForTest(active bool) { c.active.Store(active) }

// InjectFrameForTest feeds a decoded DATA frame into the existing RX worker.
// It is used only by package-level semantic tests; wire decoding remains in the engine.
func (c *StreamEngine) InjectFrameForTest(sequence uint64, payload []byte, leg LegID) {
	c.incoming <- dataFrame{seq: sequence, data: payload, leg: uint8(leg)}
}

// CloseError returns the terminal error after the engine has been finalized.
func (c *StreamEngine) CloseError() error {
	if value := c.closeErr.Load(); value != nil {
		return value.(error)
	}
	return nil
}

// InflightForTest exposes the bounded ingress token channel to legacy semantic
// tests; normal callers use the application pipe and never need this hook.
func (c *StreamEngine) InflightForTest() chan struct{} { return c.inflight }

// SetOnActivateForTest updates the activation callback for adapter tests that
// construct an engine before installing an observation hook.
func (c *StreamEngine) SetOnActivateForTest(callback func()) { c.cfg.OnActivate = callback }

// ScheduleFrontierRescueForTest invokes the existing policy orchestration for
// package-level semantic tests without exposing queues or worker state.
func (c *StreamEngine) ScheduleFrontierRescueForTest() { c.scheduleFrontierRescue() }

// HandleACKForTest applies an ACK through the production orchestration path.
func (c *StreamEngine) HandleACKForTest(next uint64) error { return c.handleAck(next) }

// ScheduleRetriesForTest runs the ordinary retry orchestration once.
func (c *StreamEngine) ScheduleRetriesForTest() { c.scheduleRetries() }

// DrainOutstandingForTest exercises the existing graceful-drain implementation.
func (c *StreamEngine) DrainOutstandingForTest() error { return c.drainOutstandingOnClose() }

// HandleLegFailureForTest routes a synthetic failure through the normal
// generation/invalidation path.
func (c *StreamEngine) HandleLegFailureForTest(id LegID, err error) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	c.handleLegFailure(leg, err)
	return true
}

// ChooseLegForTest returns the selected logical leg ID without exposing the
// concrete worker or carrier object.
func (c *StreamEngine) ChooseLegForTest(active bool, avoid int16) (LegID, bool) {
	leg := c.chooseLeg(active, avoid)
	if leg == nil {
		return 0, false
	}
	return LegID(leg.id), true
}

// SetLegPerformanceForTest seeds the existing scheduler observation state.
func (c *StreamEngine) SetLegPerformanceForTest(id LegID, writeBPS, ackedBPS float64, latency time.Duration) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil {
		return false
	}
	leg.perf.mu.Lock()
	leg.perf.writeBPS = writeBPS
	leg.perf.ackedBPS = ackedBPS
	leg.perf.writeLatency = latency
	leg.perf.mu.Unlock()
	return true
}

// SendACKFrameForTest schedules a cumulative ACK through per-leg writers.
func (c *StreamEngine) SendACKFrameForTest(value uint64, force bool) bool {
	return c.sendAckFrame(value, force)
}

// SendCloseFrameForTest schedules the logical CLOSE control through all live legs.
func (c *StreamEngine) SendCloseFrameForTest(value uint64) bool { return c.sendCloseFrame(value) }

// QueueDataAttemptForTest places one existing TX record on a concrete leg.
// It is used to preserve the writer-priority semantic test without exporting
// the worker queue or attempt representation as part of the production API.
func (c *StreamEngine) QueueDataAttemptForTest(record *StreamTXRecord, id LegID) bool {
	leg := c.getLeg(uint8(id))
	if leg == nil || leg.closed.Load() {
		return false
	}
	select {
	case <-leg.done:
		return false
	case leg.send <- txSendAttempt{record: record}:
		return true
	default:
		return false
	}
}

// IsRetiringForTest reports whether a same-ID generation is waiting for its old
// workers to exit.
func (c *StreamEngine) IsRetiringForTest(id LegID) bool {
	c.legsMu.RLock()
	_, exists := c.retiring[uint8(id)]
	c.legsMu.RUnlock()
	return exists
}

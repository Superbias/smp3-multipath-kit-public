package smp3core

import (
	"sync/atomic"
	"time"
)

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
	PreferredLeg           int8
	Grace                  time.Duration
	FirstDataWaitingAt     time.Time
	GraceStartedAt         time.Time
	PreferredAttachedAt    time.Time
	NonPreferredAttachedAt time.Time
	ReleasedAt             time.Time
	ReleaseReason          StreamStartupReleaseReason
	FirstDataLeg           int8
}

// StreamTelemetryStats contains only counters/events that are not already
// represented by the authoritative StreamStats fields. Logical byte totals
// remain in StreamStats; this avoids a second accounting domain.
type StreamTelemetryStats struct {
	DataTxAttemptFrames    [2]uint64
	DataTxRetransmitFrames [2]uint64
	DataRxUniqueFrames     [2]uint64
	DataRxDuplicateFrames  [2]uint64
	DataRxDuplicateBytes   [2]uint64
	AckTxFrames            [2]uint64
	AckRxFrames            [2]uint64
	ActivateTxFrames       [2]uint64
	ActivateRxFrames       [2]uint64
	CloseTxFrames          [2]uint64
	CloseRxFrames          [2]uint64
	RescueAttemptFrames    [2]uint64
	RescueAttemptBytes     [2]uint64
	FirstTxDataAt          [2]time.Time
	FirstRxUniqueDataAt    [2]time.Time
	Startup                StreamStartupTelemetrySnapshot
}

type streamStartupTelemetryState struct {
	StreamStartupTelemetrySnapshot
}

// StreamTelemetry is optional instrumentation owned by the host telemetry
// plane. A nil pointer is the disabled, allocation-free path.
type StreamTelemetry struct {
	dataTxAttemptFrames    [2]atomic.Uint64
	dataTxRetransmitFrames [2]atomic.Uint64
	dataRxUniqueFrames     [2]atomic.Uint64
	dataRxDuplicateFrames  [2]atomic.Uint64
	dataRxDuplicateBytes   [2]atomic.Uint64
	ackTxFrames            [2]atomic.Uint64
	ackRxFrames            [2]atomic.Uint64
	activateTxFrames       [2]atomic.Uint64
	activateRxFrames       [2]atomic.Uint64
	closeTxFrames          [2]atomic.Uint64
	closeRxFrames          [2]atomic.Uint64
	rescueAttemptFrames    [2]atomic.Uint64
	rescueAttemptBytes     [2]atomic.Uint64
	firstTxDataAt          [2]atomic.Int64
	firstRxUniqueDataAt    [2]atomic.Int64
	startup                atomic.Pointer[streamStartupTelemetryState]
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
		state.PreferredLeg = int8(cfg.StartupPreferredLeg)
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
		if !state.Applied || leg > 1 {
			return false
		}
		if int8(leg) == state.PreferredLeg {
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
		if !state.Applied || leg > 1 || state.FirstDataLeg >= 0 {
			return false
		}
		state.FirstDataLeg = int8(leg)
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
	if startup := t.startup.Load(); startup != nil {
		result.Startup = startup.StreamStartupTelemetrySnapshot
	} else {
		result.Startup.PreferredLeg = -1
		result.Startup.FirstDataLeg = -1
	}
	return result
}

func atomicTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value)
}

func (t *StreamTelemetry) markDataTx(leg uint8, retransmit, rescue bool, bytes uint64) {
	if t == nil || leg >= 2 {
		return
	}
	t.dataTxAttemptFrames[leg].Add(1)
	if retransmit {
		t.dataTxRetransmitFrames[leg].Add(1)
	}
	if rescue {
		t.rescueAttemptFrames[leg].Add(1)
		t.rescueAttemptBytes[leg].Add(bytes)
	}
	now := time.Now()
	markFirstTimestampAt(&t.firstTxDataAt[leg], now)
	t.markStartupFirstDataLeg(leg)
}

func (t *StreamTelemetry) markDataRxUnique(leg uint8) {
	if t == nil || leg >= 2 {
		return
	}
	t.dataRxUniqueFrames[leg].Add(1)
	markFirstTimestamp(&t.firstRxUniqueDataAt[leg])
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
	if t == nil || leg >= 2 {
		return
	}
	t.dataRxDuplicateFrames[leg].Add(1)
	t.dataRxDuplicateBytes[leg].Add(bytes)
}

func (t *StreamTelemetry) markControlTx(leg uint8, typ byte) {
	if t == nil || leg >= 2 {
		return
	}
	switch typ {
	case frameTypeAck:
		t.ackTxFrames[leg].Add(1)
	case frameTypeActivate:
		t.activateTxFrames[leg].Add(1)
	case frameTypeClose:
		t.closeTxFrames[leg].Add(1)
	}
}

func (t *StreamTelemetry) markControlRx(leg uint8, typ byte) {
	if t == nil || leg >= 2 {
		return
	}
	switch typ {
	case frameTypeAck:
		t.ackRxFrames[leg].Add(1)
	case frameTypeActivate:
		t.activateRxFrames[leg].Add(1)
	case frameTypeClose:
		t.closeRxFrames[leg].Add(1)
	}
}

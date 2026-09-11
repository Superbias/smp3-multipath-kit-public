package smp3core

import (
	"sync"
	"sync/atomic"
	"time"
)

type StreamStartupPolicy uint8

const (
	StreamStartupFirstReady StreamStartupPolicy = iota
	StreamStartupPreferred
)

const MaxStreamStartupGrace = 10 * time.Second

type streamStartupState uint8

const (
	streamStartupNoData streamStartupState = iota
	streamStartupDataWaitingNoLeg
	streamStartupGrace
	streamStartupReleased
)

type streamStartupTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realStreamStartupTimer struct {
	*time.Timer
}

func (t *realStreamStartupTimer) C() <-chan time.Time { return t.Timer.C }

type streamStartupGate struct {
	mu                          sync.Mutex
	preferred                   uint8
	grace                       time.Duration
	state                       streamStartupState
	terminal                    [2]bool
	attached                    [2]bool
	preferredBeforeNonPreferred bool
	released                    atomic.Bool
	wake                        chan struct{}
	timerFactory                func(time.Duration) streamStartupTimer
	graceStarted                bool
	releaseReason               StreamStartupReleaseReason
	telemetry                   *StreamTelemetry
}

func newStreamStartupGate(cfg StreamConfig) *streamStartupGate {
	if cfg.StartupPolicy != StreamStartupPreferred {
		return nil
	}
	preferred := uint8(cfg.StartupPreferredLeg)
	if preferred > 1 {
		preferred = 0
	}
	grace := cfg.StartupGrace
	if grace < 0 {
		grace = 0
	}
	if grace > MaxStreamStartupGrace {
		grace = MaxStreamStartupGrace
	}
	factory := cfg.startupTimerFactory
	if factory == nil {
		factory = func(duration time.Duration) streamStartupTimer {
			return &realStreamStartupTimer{Timer: time.NewTimer(duration)}
		}
	}
	return &streamStartupGate{
		preferred:    preferred,
		grace:        grace,
		state:        streamStartupNoData,
		wake:         make(chan struct{}, 1),
		timerFactory: factory,
		telemetry:    cfg.Telemetry,
	}
}

func (s *streamStartupGate) notify(ids ...uint8) {
	if s == nil || s.released.Load() {
		return
	}
	s.mu.Lock()
	if len(ids) > 0 && ids[0] < 2 && !s.attached[ids[0]] {
		if ids[0] == s.preferred && !s.attached[s.preferred^1] {
			s.preferredBeforeNonPreferred = true
		}
		s.attached[ids[0]] = true
	}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *streamStartupGate) markTerminalUnavailable(id LegID) bool {
	if s == nil || uint8(id) > 1 || s.released.Load() {
		return false
	}
	s.mu.Lock()
	if s.released.Load() || uint8(id) != s.preferred {
		s.mu.Unlock()
		return false
	}
	s.terminal[uint8(id)] = true
	s.mu.Unlock()
	s.notify()
	return true
}

func (s *streamStartupGate) release(reason StreamStartupReleaseReason) {
	released := false
	s.mu.Lock()
	if !s.released.Load() {
		s.state = streamStartupReleased
		s.releaseReason = reason
		s.released.Store(true)
		released = true
	}
	s.mu.Unlock()
	if released && s.telemetry != nil {
		s.telemetry.markStartupReleased(time.Now(), reason)
	}
}

func (s *streamStartupGate) markSessionClosed() {
	if s == nil || s.released.Load() {
		return
	}
	if s.telemetry != nil {
		s.telemetry.markStartupClosedBeforeRelease()
	}
}

func (s *streamStartupGate) preferredReleaseReason() StreamStartupReleaseReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.graceStarted {
		return StartupReleasePreferredArrivedWithinGrace
	}
	if s.preferredBeforeNonPreferred {
		return StartupReleasePreferredAttachedBeforeNonPreferred
	}
	return StartupReleasePreferredAlreadyAttached
}

func (s *streamStartupGate) terminalPreferred() bool {
	s.mu.Lock()
	terminal := s.terminal[s.preferred]
	s.mu.Unlock()
	return terminal
}

func (s *streamStartupGate) setWaitingState(state streamStartupState) {
	graceStarted := false
	s.mu.Lock()
	if !s.released.Load() {
		s.state = state
		if state == streamStartupGrace && !s.graceStarted {
			s.graceStarted = true
			graceStarted = true
		}
	}
	s.mu.Unlock()
	if graceStarted && s.telemetry != nil {
		s.telemetry.markStartupGraceStarted(time.Now())
	}
}

func (s *streamStartupGate) await(engine *StreamEngine) (int16, bool) {
	if s == nil || s.released.Load() {
		return -1, true
	}
	preferred := s.preferred
	nonPreferred := preferred ^ 1
	var timer streamStartupTimer
	var timerC <-chan time.Time
	graceExpired := false
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		if s.released.Load() {
			return -1, true
		}
		preferredReady := engine.hasLeg(preferred)
		nonPreferredReady := engine.hasLeg(nonPreferred)
		if preferredReady {
			s.release(s.preferredReleaseReason())
			return int16(preferred), true
		}
		if nonPreferredReady && s.terminalPreferred() {
			s.release(StartupReleasePreferredTerminalUnavailable)
			return -1, true
		}
		if nonPreferredReady && (s.grace == 0 || graceExpired) {
			s.release(StartupReleaseGraceExpired)
			return -1, true
		}
		if !nonPreferredReady {
			if timer != nil {
				timer.Stop()
				timer = nil
				timerC = nil
			}
			graceExpired = false
			s.setWaitingState(streamStartupDataWaitingNoLeg)
		} else if timer == nil {
			timer = s.timerFactory(s.grace)
			timerC = timer.C()
			s.setWaitingState(streamStartupGrace)
		}

		select {
		case <-engine.done:
			return -1, false
		case <-engine.gracefulCh:
			return -1, false
		case <-s.wake:
		case <-timerC:
			timer = nil
			timerC = nil
			graceExpired = true
		}
	}
}

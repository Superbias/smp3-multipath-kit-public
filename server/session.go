package server

import (
	"errors"
	"fmt"
	"net"
	"sync"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type serverSession struct {
	id          smp3core.SessionID
	mode        smp3core.HelloMode
	destination string

	stream    *smp3core.StreamEngine
	streamApp net.Conn
	dgram     *smp3core.DatagramEngine

	targetMu sync.Mutex
	target   ioCloser
	// hostReleased is guarded by targetMu. Once the logical Core is terminal,
	// no later lazy target creation may resurrect host resources.
	hostReleased bool
	hostRelease  sync.Once

	workerWG      sync.WaitGroup
	closeOne      sync.Once
	legMu         sync.Mutex
	reserved      map[uint8]struct{}
	maxActiveLegs int
	telemetryOnce sync.Once
	telemetry     *telemetrySessionRecord
}

func (s *serverSession) addTargetBytes(tx bool, n int) {
	if s.telemetry == nil || n <= 0 {
		return
	}
	if tx {
		s.telemetry.targetTxBytes.Add(uint64(n))
	} else {
		s.telemetry.targetRxBytes.Add(uint64(n))
	}
}

type ioCloser interface {
	Close() error
}

func (s *serverSession) done() <-chan struct{} {
	if s.mode == smp3core.ModeDatagram {
		return s.dgram.Done()
	}
	return s.stream.Done()
}

func (s *serverSession) attachLeg(id smp3core.LegID, conn net.Conn, onClose func(error)) error {
	return s.attachLegWithPathAuth(id, conn, onClose, nil)
}

func (s *serverSession) attachLegWithPathAuth(id smp3core.LegID, conn net.Conn, onClose func(error), auth *smp3core.StreamPathAuth) error {
	var err error
	if s.mode == smp3core.ModeDatagram {
		if auth != nil {
			return errors.New("path RTT authentication is stream-only")
		}
		err = s.dgram.AttachLeg(id, conn, onClose)
	} else if auth != nil {
		err = s.stream.AttachLegWithPathAuth(id, conn, onClose, *auth)
	} else {
		err = s.stream.AttachLeg(id, conn, onClose)
	}
	if err == nil {
		// The Core has committed the leg. Transfer the admission slot from the
		// pending-handshake reservation to the active-leg count immediately;
		// callers also defer release as an error-path safeguard.
		s.releaseLeg(id)
	}
	return err
}

// reserveLeg closes the admission race between a sidecar HELLO/READY
// handshake and the canonical Core AttachLeg call. A successful attachment
// immediately promotes its reservation into the Core's active-leg count.
func (s *serverSession) reserveLeg(id smp3core.LegID) error {
	s.legMu.Lock()
	defer s.legMu.Unlock()
	limit := s.maxActiveLegs
	if s.mode == smp3core.ModeDatagram {
		limit = 2
	} else if limit == 0 {
		limit = smp3core.DefaultMaxActiveStreamLegs
	}
	if int(id) >= limit {
		return fmt.Errorf("multipath leg id %d exceeds configured limit %d", id, limit)
	}
	if s.reserved == nil {
		s.reserved = make(map[uint8]struct{})
	}
	id8 := uint8(id)
	if _, exists := s.reserved[id8]; exists {
		return errors.New("multipath leg is already being admitted")
	}
	if s.mode == smp3core.ModeDatagram {
		if s.dgram.HasLeg(id) {
			return errors.New("duplicate multipath datagram leg")
		}
	} else if s.stream.HasLeg(id) {
		return errors.New("duplicate multipath stream leg")
	}
	active := 0
	if s.mode == smp3core.ModeDatagram {
		active = s.dgram.LegCount()
	} else {
		active = s.stream.LegCount()
	}
	if active+len(s.reserved) >= limit {
		return fmt.Errorf("maximum multipath leg count %d reached", limit)
	}
	s.reserved[id8] = struct{}{}
	return nil
}

func (s *serverSession) releaseLeg(id smp3core.LegID) {
	s.legMu.Lock()
	delete(s.reserved, uint8(id))
	s.legMu.Unlock()
}

func (s *serverSession) close() {
	s.closeOne.Do(func() {
		if s.mode == smp3core.ModeDatagram {
			_ = s.dgram.Close()
		} else {
			_ = s.stream.Close()
		}
	})
	s.releaseHostResources()
}

// releaseHostResources is separate from logical Core shutdown because the
// Core.Done watcher must be able to release host resources without calling
// session.close and without waiting on its own worker.
func (s *serverSession) releaseHostResources() {
	s.hostRelease.Do(func() {
		s.targetMu.Lock()
		s.hostReleased = true
		target := s.target
		s.target = nil
		s.targetMu.Unlock()
		if target != nil {
			_ = target.Close()
		}
	})
}

func (s *serverSession) setTarget(target ioCloser) bool {
	s.targetMu.Lock()
	defer s.targetMu.Unlock()
	if s.hostReleased {
		return false
	}
	select {
	case <-s.done():
		return false
	default:
	}
	if s.target != nil {
		return false
	}
	s.target = target
	return true
}

func (s *serverSession) goWorker(fn func()) {
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		fn()
	}()
}

func (s *serverSession) waitWorkers() { s.workerWG.Wait() }

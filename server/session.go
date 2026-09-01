package server

import (
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

	workerWG sync.WaitGroup
	closeOne sync.Once
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

func (s *serverSession) attachLeg(id smp3core.LegID, conn net.Conn) error {
	if s.mode == smp3core.ModeDatagram {
		return s.dgram.AttachLeg(id, conn, nil)
	}
	return s.stream.AttachLeg(id, conn, nil)
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

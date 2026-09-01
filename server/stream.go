package server

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func streamSchedulerMode(value string) smp3core.StreamSchedulerMode {
	if value == "static" {
		return smp3core.StreamSchedulerStatic
	}
	return smp3core.StreamSchedulerAdaptive
}

func (s *Server) startStreamHost(session *serverSession) error {
	dialer := net.Dialer{}
	target, err := dialer.DialContext(s.ctx, "tcp", session.destination)
	if err != nil {
		return err
	}
	if !session.setTarget(target) {
		_ = target.Close()
		return errors.New("stream session closed before target setup")
	}

	var completed atomic.Int32
	var firstErrMu sync.Mutex
	var firstErr error
	finish := func(err error) {
		if err != nil {
			firstErrMu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			firstErrMu.Unlock()
		}
		if completed.Add(1) != 2 {
			return
		}
		closeErr := io.EOF
		firstErrMu.Lock()
		if firstErr != nil {
			closeErr = firstErr
		}
		firstErrMu.Unlock()
		s.logger.Debug("multipath stream target bridge finished", "session", sessionLogID(session.id), "error", closeErr)
		session.stream.StartGracefulClose(closeErr)
	}

	session.goWorker(func() {
		err := copyStreamDirection(session.streamApp, target)
		finish(err)
	})
	session.goWorker(func() {
		err := copyStreamDirection(target, session.streamApp)
		finish(err)
	})
	return nil
}

func copyStreamDirection(source, destination net.Conn) error {
	_, err := io.Copy(destination, source)
	if err != nil {
		_ = source.Close()
		_ = destination.Close()
		return err
	}
	if closer, ok := destination.(interface{ CloseWrite() error }); ok {
		if closeErr := closer.CloseWrite(); closeErr != nil {
			_ = source.Close()
			_ = destination.Close()
			return closeErr
		}
		return nil
	}
	_ = destination.Close()
	return nil
}

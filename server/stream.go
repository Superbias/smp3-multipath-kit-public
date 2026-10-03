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
	if value == "aggregation" {
		return smp3core.StreamSchedulerAggregation
	}
	return smp3core.StreamSchedulerAdaptive
}

func streamCapacityMode(value string) smp3core.StreamCapacityMode {
	if value == "dynamic" {
		return smp3core.StreamCapacityDynamic
	}
	return smp3core.StreamCapacityFixed
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
		err := copyStreamDirectionCounted(session.streamApp, target, func(n int) {
			session.addTargetBytes(true, n)
		})
		finish(err)
	})
	session.goWorker(func() {
		err := copyStreamDirectionCounted(target, session.streamApp, func(n int) {
			session.addTargetBytes(false, n)
		})
		finish(err)
	})
	return nil
}

func copyStreamDirection(source, destination net.Conn) error {
	return copyStreamDirectionCounted(source, destination, nil)
}

type telemetryCountingWriter struct {
	writer  io.Writer
	counted func(int)
}

func (w telemetryCountingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 && w.counted != nil {
		w.counted(n)
	}
	return n, err
}

func copyStreamDirectionCounted(source, destination net.Conn, counted func(int)) error {
	writer := io.Writer(destination)
	if counted != nil {
		writer = telemetryCountingWriter{writer: destination, counted: counted}
	}
	_, err := io.Copy(writer, source)
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

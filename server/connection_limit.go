package server

import (
	"log/slog"
	"net"
	"sync"
	"time"
)

// connectionLimitListener enforces the limit before a TLS handshake begins,
// so unauthenticated TCP peers cannot create unbounded TLS handshake work.
type connectionLimitListener struct {
	net.Listener
	slots  chan struct{}
	logger *slog.Logger
}

func (l *connectionLimitListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &connectionSlotConn{Conn: conn, slots: l.slots}, nil
		default:
			if l.logger != nil {
				l.logger.Debug("SMP3 connection limit reached; rejecting carrier")
			}
			_ = conn.Close()
		}
	}
}

type connectionSlotConn struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
}

func (c *connectionSlotConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { <-c.slots })
	return err
}

// activityTimeoutConn treats the configured idle timeout as an inactivity
// timeout: any successful carrier read or write refreshes the full deadline.
// The timeout is disabled by default for compatibility.
type activityTimeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (c *activityTimeoutConn) Read(p []byte) (int, error) {
	deadline := time.Now().Add(c.timeout)
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return 0, err
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	return n, err
}

func (c *activityTimeoutConn) Write(p []byte) (int, error) {
	deadline := time.Now().Add(c.timeout)
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return 0, err
	}
	n, err := c.Conn.Write(p)
	if n > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	return n, err
}

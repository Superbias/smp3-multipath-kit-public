package smp3core

import (
	"io"
	"net"
	"sync"
)

// p0cdDataFrameCoalescer keeps the virtual Carrier's service model aligned
// with a byte-stream transfer: Core's DATA header and payload are one frame
// and must incur one serialization and propagation service interval.
// It is test-only and assumes the Core's per-leg writer serializes writes.
type p0cdDataFrameCoalescer struct {
	net.Conn
	mu            sync.Mutex
	pendingHeader []byte
}

func (c *p0cdDataFrameCoalescer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pendingHeader) > 0 {
		frame := make([]byte, 0, len(c.pendingHeader)+len(p))
		frame = append(frame, c.pendingHeader...)
		frame = append(frame, p...)
		c.pendingHeader = c.pendingHeader[:0]
		n, err := c.Conn.Write(frame)
		if err != nil {
			return 0, err
		}
		if n != len(frame) {
			return 0, io.ErrShortWrite
		}
		return len(p), nil
	}
	if len(p) == StreamFrameHeaderSize && p[0] == byte(StreamFrameData) {
		c.pendingHeader = append(c.pendingHeader[:0], p...)
		return len(p), nil
	}
	return c.Conn.Write(p)
}

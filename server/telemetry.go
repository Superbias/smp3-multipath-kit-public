package server

import (
	"net"
	"sync/atomic"
)

type WireCounterSnapshot struct {
	TxBytes uint64
	RxBytes uint64
}

type WireCounters struct {
	txBytes atomic.Uint64
	rxBytes atomic.Uint64
}

func (c *WireCounters) snapshot() WireCounterSnapshot {
	if c == nil {
		return WireCounterSnapshot{}
	}
	return WireCounterSnapshot{TxBytes: c.txBytes.Load(), RxBytes: c.rxBytes.Load()}
}

// countedConn measures SMP3 protocol bytes at the net.Conn boundary. It is
// installed before HELLO authentication so pre-admission bytes can be bound to
// an admitted session only after authentication succeeds.
type countedConn struct {
	net.Conn
	counters *WireCounters
}

func newCountedConn(conn net.Conn) (*countedConn, *WireCounters) {
	counters := &WireCounters{}
	return &countedConn{Conn: conn, counters: counters}, counters
}

func (c *countedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.counters.rxBytes.Add(uint64(n))
	}
	return n, err
}

func (c *countedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.counters.txBytes.Add(uint64(n))
	}
	return n, err
}

func (c *countedConn) wireCounters() *WireCounters { return c.counters }

type TelemetryShare struct {
	Value float64
	Valid bool
}

func calculateTelemetryShare(numerator, denominator uint64) TelemetryShare {
	if denominator == 0 {
		return TelemetryShare{}
	}
	return TelemetryShare{Value: float64(numerator) / float64(denominator), Valid: true}
}

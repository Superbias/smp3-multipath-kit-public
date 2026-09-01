package smp3core

import "io"

// DatagramLeg is the minimal carrier capability required by the datagram
// engine. PacketConn/address/deadline behavior remains in the host adapter.
type DatagramLeg interface {
	io.Reader
	io.Writer
	io.Closer
}

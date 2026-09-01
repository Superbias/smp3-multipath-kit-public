package smp3core

import "io"

// StreamLeg is the transport capability required by the SMP3 stream engine.
// Carrier implementations may be TCP connections, but the protocol engine
// deliberately depends only on bidirectional byte-stream I/O and close.
type StreamLeg interface {
	io.Reader
	io.Writer
	io.Closer
}

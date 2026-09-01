package smp3core

// SessionID identifies one logical SMP3 session.
type SessionID [16]byte

// LegID identifies one carrier leg in the session.
type LegID uint8

// Version is the HELLO wire version.
type Version uint8

// HelloMode selects the stream or datagram data plane.
type HelloMode uint8

const (
	Version4 Version = 4
	Version5 Version = 5

	ModeStream   HelloMode = 0
	ModeDatagram HelloMode = 1
)

// Hello is the protocol-owned HELLO value. Datagram policy and carrier
// selection remain adapter concerns and are intentionally absent here.
type Hello struct {
	Version     Version
	SessionID   SessionID
	LegID       LegID
	Mode        HelloMode
	Timestamp   int64
	Nonce       [16]byte
	Destination string
}

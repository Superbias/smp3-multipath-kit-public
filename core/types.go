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
	// Version6 is the opt-in stream protocol version that negotiates authenticated
	// same-carrier path feedback. Datagram sessions remain on Version5.
	Version6 Version = 6
	// Version7 adds explicit stream capability negotiation for coordinated,
	// full-duplex leg drain. Version6 wire behavior remains unchanged.
	Version7 Version = 7

	ModeStream   HelloMode = 0
	ModeDatagram HelloMode = 1
)

// StreamCapabilityPathRTT enables authenticated same-carrier RTT probes. It
// is negotiated only by Version6 stream HELLOs and is never enabled implicitly.
const StreamCapabilityPathRTT uint32 = 1 << 0

// StreamCapabilityCoordinatedDrain enables authenticated peer quiesce and
// bilateral ACK settlement before planned carrier removal. It is only valid
// on Version7 stream HELLOs.
const StreamCapabilityCoordinatedDrain uint32 = 1 << 1

const knownStreamCapabilities = StreamCapabilityPathRTT | StreamCapabilityCoordinatedDrain

// Hello is the protocol-owned HELLO value. Datagram policy and carrier
// selection remain adapter concerns and are intentionally absent here.
type Hello struct {
	Version      Version
	SessionID    SessionID
	LegID        LegID
	Mode         HelloMode
	Timestamp    int64
	Nonce        [16]byte
	Capabilities uint32
	Destination  string
}

package smp3core

import (
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"time"
)

const (
	streamReadyV2Magic = "SMP3RDY2"
	streamReadyV3Magic = "SMP3RDY3"
	streamReadyV2Size  = 8 + 16 + 1 + 1 + 4 + 16 + sha256.Size

	StreamRTTMessageSize = 8 + 1 + 1 + 1 + 1 + 16 + 8 + sha256.Size
	streamRTTMessageV1   = 1

	StreamDrainControlMessageSize = 8 + 1 + 1 + 1 + 1 + 16 + 1 + 8 + 16 + 16 + 8 + 8 + 4 + 8 + sha256.Size
)

var (
	ErrStreamPathNegotiation  = errors.New("stream path RTT capability was not negotiated")
	ErrStreamPathMessage      = errors.New("invalid stream path RTT message")
	ErrStreamPathRole         = errors.New("invalid stream path endpoint role")
	ErrStreamDrainNegotiation = errors.New("coordinated stream drain capability was not negotiated")
	ErrStreamDrainMessage     = errors.New("invalid coordinated stream drain message")
)

var streamRTTMessageMagic = [8]byte{'S', 'M', 'P', '3', 'R', 'T', 'T', '1'}
var streamDrainControlMagic = [8]byte{'S', 'M', 'P', '3', 'D', 'R', 'N', '1'}

// StreamReadyV2 is the authenticated result of a Version6 stream HELLO.
// Capabilities contains only features selected by the server.
type StreamReadyV2 struct {
	SessionID    SessionID
	LegID        LegID
	Mode         HelloMode
	Capabilities uint32
	ServerNonce  [16]byte
}

// StreamPathAuth is an opaque capability produced only by Core's authenticated
// Version6 READY codec. Hosts pass it back into Core; they cannot construct or
// edit the underlying probe key or negotiation fields.
type StreamPathAuth struct {
	sessionID     SessionID
	legID         LegID
	capabilities  uint32
	probeKey      [sha256.Size]byte
	drainKey      [sha256.Size]byte
	drainBinding  [16]byte
	authenticated bool
	handshakeRTT  time.Duration
	handshakeAt   time.Time
}

// CoordinatedDrainEnabled reports whether this authenticated carrier
// negotiated the Version7 coordinated-drain capability.
func (a StreamPathAuth) CoordinatedDrainEnabled() bool {
	return a.authenticated && a.capabilities&StreamCapabilityCoordinatedDrain != 0
}

func (a StreamPathAuth) drainCarrierBinding() [16]byte { return a.drainBinding }

func (a StreamPathAuth) PathRTTEnabled() bool {
	return a.authenticated && a.capabilities&StreamCapabilityPathRTT != 0
}

// CarrierHandshakeRTT reports the initial same-Carrier sample when present.
func (a StreamPathAuth) CarrierHandshakeRTT() (time.Duration, bool) {
	if !a.PathRTTEnabled() || a.handshakeRTT <= 0 || a.handshakeRTT >= StreamRTTProbeTimeout || a.handshakeAt.IsZero() {
		return 0, false
	}
	return a.handshakeRTT, true
}

// CarrierHandshakeRTT reports the initial same-Carrier round trip when the
// host measured and attached one. It is absent for servers and legacy sessions.
// WithCarrierHandshakeRTT attaches a client-measured HELLO-to-authenticated-
// READY round trip to this opaque capability. The host must measure one newly
// opened Carrier, from immediately before sending its v6 HELLO through
// validating READY on that same connection. This is an observed Carrier
// service RTT (including peer/runtime processing), not a physical-link RTT.
// Invalid or unavailable measurements do not prevent the authenticated v6
// session from proceeding; the Core can still learn through later probes.
func (a StreamPathAuth) WithCarrierHandshakeRTT(rtt time.Duration) (StreamPathAuth, error) {
	if !a.PathRTTEnabled() {
		return a, ErrStreamPathNegotiation
	}
	if rtt <= 0 || rtt >= StreamRTTProbeTimeout {
		return a, ErrStreamPathMessage
	}
	a.handshakeRTT = rtt
	a.handshakeAt = time.Now()
	return a, nil
}

func (a StreamPathAuth) carrierHandshakeSample() (time.Duration, time.Time) {
	return a.handshakeRTT, a.handshakeAt
}

// StreamPathBinding is local connection identity. Generation and CarrierID
// deliberately stay local; a new connection derives a new probe key from its
// fresh HELLO and READY nonces, while Core compares this binding on every read.
type StreamPathBinding struct {
	SessionID  SessionID
	LegID      LegID
	CarrierID  string
	Generation uint64
}

type StreamRTTMessageKind uint8

const (
	StreamRTTProbe StreamRTTMessageKind = iota + 1
	StreamRTTEcho
)

// StreamRTTDirection names the endpoint that initiated a probe, not the local
// endpoint's inbound/outbound view.
type StreamRTTDirection uint8

const (
	StreamRTTClientToServer StreamRTTDirection = iota + 1
	StreamRTTServerToClient
)

// StreamPathRole is the local protocol endpoint role. It determines which
// direction this Core measures when it initiates same-carrier probes.
type StreamPathRole uint8

const (
	StreamPathRoleClient StreamPathRole = iota + 1
	StreamPathRoleServer
)

// StreamPathIdentity binds feedback to one active Carrier generation and to a
// protocol direction. It is not a physical-interface or one-way latency claim.
type StreamPathIdentity struct {
	SessionID  SessionID
	LegID      LegID
	CarrierID  string
	Generation uint64
	Direction  StreamRTTDirection
}

func (i StreamPathIdentity) binding() StreamPathBinding {
	return StreamPathBinding{SessionID: i.SessionID, LegID: i.LegID, CarrierID: i.CarrierID, Generation: i.Generation}
}

// StreamRTTMessage is a single authenticated probe or echo. Its wire MAC
// covers operation, direction, session, leg, and token. The connection-local
// CarrierID and Generation are checked against the receiving Core binding.
type StreamRTTMessage struct {
	Kind     StreamRTTMessageKind
	Identity StreamPathIdentity
	Token    uint64
}

func EncodeStreamReadyV2(hello Hello, selectedCapabilities uint32, serverNonce [16]byte, password []byte) ([]byte, error) {
	if err := validateStreamReadyHello(hello); err != nil {
		return nil, err
	}
	if len(password) == 0 {
		return nil, errEmptyPassword
	}
	if !validSelectedStreamCapabilities(hello, selectedCapabilities) {
		return nil, errInvalidCapabilities
	}
	ready := make([]byte, streamReadyV2Size)
	copy(ready[:8], readyMagic(hello.Version))
	copy(ready[8:24], hello.SessionID[:])
	ready[24] = byte(hello.LegID)
	ready[25] = byte(hello.Mode)
	binary.BigEndian.PutUint32(ready[26:30], selectedCapabilities)
	copy(ready[30:46], serverNonce[:])
	mac := streamReadyMAC(hello, ready[:46], password)
	copy(ready[46:], mac[:])
	return ready, nil
}

// ReadStreamReadyV2 authenticates the complete session, leg, mode, capability
// selection, client nonce, and server nonce before returning the result.
func ReadStreamReadyV2(r io.Reader, expected Hello, password []byte) (StreamReadyV2, StreamPathAuth, error) {
	var ready StreamReadyV2
	if err := validateStreamReadyHello(expected); err != nil {
		return ready, StreamPathAuth{}, err
	}
	if len(password) == 0 {
		return ready, StreamPathAuth{}, errEmptyPassword
	}
	wire := make([]byte, streamReadyV2Size)
	if _, err := io.ReadFull(r, wire); err != nil {
		return ready, StreamPathAuth{}, err
	}
	want := streamReadyMAC(expected, wire[:46], password)
	if !hmac.Equal(wire[46:], want[:]) {
		return ready, StreamPathAuth{}, errInvalidAuthentication
	}
	if string(wire[:8]) != readyMagic(expected.Version) || !hmac.Equal(wire[8:24], expected.SessionID[:]) ||
		wire[24] != byte(expected.LegID) || wire[25] != byte(expected.Mode) {
		return ready, StreamPathAuth{}, ErrStreamPathMessage
	}
	selected := binary.BigEndian.Uint32(wire[26:30])
	if !validSelectedStreamCapabilities(expected, selected) {
		return ready, StreamPathAuth{}, errInvalidCapabilities
	}
	ready.SessionID = expected.SessionID
	ready.LegID = expected.LegID
	ready.Mode = expected.Mode
	ready.Capabilities = selected
	copy(ready.ServerNonce[:], wire[30:46])
	auth, err := makeStreamPathAuth(expected, ready, password)
	if err != nil {
		return StreamReadyV2{}, StreamPathAuth{}, err
	}
	return ready, auth, nil
}

func deriveStreamPathProbeKey(hello Hello, ready StreamReadyV2, password []byte) ([sha256.Size]byte, error) {
	var key [sha256.Size]byte
	if err := validateStreamPathHello(hello); err != nil {
		return key, err
	}
	if len(password) == 0 {
		return key, errEmptyPassword
	}
	if ready.SessionID != hello.SessionID || ready.LegID != hello.LegID || ready.Mode != ModeStream ||
		!validSelectedStreamCapabilities(hello, ready.Capabilities) {
		return key, ErrStreamPathMessage
	}
	if ready.Capabilities&StreamCapabilityPathRTT == 0 {
		return key, ErrStreamPathNegotiation
	}
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-STREAM-PATH-KEY-V6"))
	_, _ = mac.Write(hello.SessionID[:])
	_, _ = mac.Write([]byte{byte(hello.LegID), byte(hello.Mode)})
	var caps [4]byte
	binary.BigEndian.PutUint32(caps[:], ready.Capabilities)
	_, _ = mac.Write(caps[:])
	_, _ = mac.Write(hello.Nonce[:])
	_, _ = mac.Write(ready.ServerNonce[:])
	copy(key[:], mac.Sum(nil))
	return key, nil
}

// EncodeStreamRTTMessage encodes one fixed-size probe or echo. The authenticated
// message is carried only after Version6 capability negotiation succeeds.
func encodeStreamRTTMessage(message StreamRTTMessage, key [sha256.Size]byte) ([]byte, error) {
	if err := validateStreamRTTMessage(message); err != nil {
		return nil, err
	}
	wire := make([]byte, StreamRTTMessageSize)
	copy(wire[:8], streamRTTMessageMagic[:])
	wire[8] = streamRTTMessageV1
	wire[9] = byte(message.Kind)
	wire[10] = byte(message.Identity.Direction)
	wire[11] = byte(message.Identity.LegID)
	copy(wire[12:28], message.Identity.SessionID[:])
	binary.BigEndian.PutUint64(wire[28:36], message.Token)
	mac := streamRTTMessageMAC(key, wire[:36])
	copy(wire[36:], mac[:])
	return wire, nil
}

// DecodeStreamRTTMessage verifies the message against the local active binding.
// Generation and CarrierID are supplied by that connection's Core state, never
// accepted from the peer as proof of local connection identity.
func decodeStreamRTTMessage(wire []byte, key [sha256.Size]byte, expected StreamPathBinding) (StreamRTTMessage, error) {
	var message StreamRTTMessage
	if err := validateStreamPathBinding(expected); err != nil {
		return message, err
	}
	if len(wire) != StreamRTTMessageSize || string(wire[:8]) != string(streamRTTMessageMagic[:]) || wire[8] != streamRTTMessageV1 {
		return message, ErrStreamPathMessage
	}
	want := streamRTTMessageMAC(key, wire[:36])
	if !hmac.Equal(wire[36:], want[:]) {
		return StreamRTTMessage{}, errInvalidAuthentication
	}
	message.Kind = StreamRTTMessageKind(wire[9])
	direction := StreamRTTDirection(wire[10])
	legID := LegID(wire[11])
	var sessionID SessionID
	copy(sessionID[:], wire[12:28])
	token := binary.BigEndian.Uint64(wire[28:36])
	if (message.Kind != StreamRTTProbe && message.Kind != StreamRTTEcho) ||
		(direction != StreamRTTClientToServer && direction != StreamRTTServerToClient) ||
		legID != expected.LegID || sessionID != expected.SessionID || token == 0 {
		return StreamRTTMessage{}, ErrStreamPathMessage
	}
	message.Identity = StreamPathIdentity{
		SessionID:  expected.SessionID,
		LegID:      expected.LegID,
		CarrierID:  expected.CarrierID,
		Generation: expected.Generation,
		Direction:  direction,
	}
	message.Token = token
	return message, nil
}

func validateStreamV6Hello(hello Hello) error {
	if hello.Version != Version6 || hello.Mode != ModeStream {
		return ErrStreamPathMessage
	}
	if hello.Capabilities&^StreamCapabilityPathRTT != 0 || hello.Capabilities&StreamCapabilityPathRTT == 0 {
		return errInvalidCapabilities
	}
	return nil
}

func validateStreamPathHello(hello Hello) error {
	if hello.Mode != ModeStream || (hello.Version != Version6 && hello.Version != Version7) ||
		hello.Capabilities&StreamCapabilityPathRTT == 0 {
		return ErrStreamPathMessage
	}
	if hello.Version == Version6 && hello.Capabilities&^StreamCapabilityPathRTT != 0 ||
		hello.Version == Version7 && hello.Capabilities&^knownStreamCapabilities != 0 {
		return errInvalidCapabilities
	}
	return nil
}

func validateStreamReadyHello(hello Hello) error {
	if hello.Version == Version6 {
		return validateStreamV6Hello(hello)
	}
	if hello.Version != Version7 || hello.Mode != ModeStream ||
		hello.Capabilities&^knownStreamCapabilities != 0 || hello.Capabilities&StreamCapabilityCoordinatedDrain == 0 {
		return ErrStreamPathMessage
	}
	return nil
}

func validSelectedStreamCapabilities(hello Hello, selected uint32) bool {
	if selected&^hello.Capabilities != 0 || selected&^knownStreamCapabilities != 0 {
		return false
	}
	if hello.Version == Version6 {
		return selected&^StreamCapabilityPathRTT == 0
	}
	return hello.Version == Version7 && hello.Mode == ModeStream
}

func makeStreamPathAuth(hello Hello, ready StreamReadyV2, password []byte) (StreamPathAuth, error) {
	if ready.SessionID != hello.SessionID || ready.LegID != hello.LegID || ready.Mode != ModeStream ||
		!validSelectedStreamCapabilities(hello, ready.Capabilities) {
		return StreamPathAuth{}, ErrStreamPathMessage
	}
	if ready.Capabilities == 0 {
		return StreamPathAuth{}, nil
	}
	var pathKey, drainKey [sha256.Size]byte
	var err error
	if ready.Capabilities&StreamCapabilityPathRTT != 0 {
		pathKey, err = deriveStreamPathProbeKey(hello, ready, password)
		if err != nil {
			return StreamPathAuth{}, err
		}
	}
	var drainBinding [16]byte
	if ready.Capabilities&StreamCapabilityCoordinatedDrain != 0 {
		if hello.Version != Version7 || len(password) == 0 {
			return StreamPathAuth{}, errInvalidCapabilities
		}
		drainKey = deriveStreamDrainControlKey(hello, ready, password)
		bindingMAC := hmac.New(sha256.New, drainKey[:])
		_, _ = bindingMAC.Write([]byte("SMP3-DRAIN-CARRIER-BINDING-V1"))
		_, _ = bindingMAC.Write(hello.SessionID[:])
		bindingMAC.Write([]byte{byte(hello.LegID)})
		copy(drainBinding[:], bindingMAC.Sum(nil)[:16])
	}
	return StreamPathAuth{sessionID: hello.SessionID, legID: hello.LegID, capabilities: ready.Capabilities,
		probeKey: pathKey, drainKey: drainKey, drainBinding: drainBinding, authenticated: true}, nil
}

func (a StreamPathAuth) validFor(binding StreamPathBinding) bool {
	return a.authenticated && a.sessionID == binding.SessionID && a.legID == binding.LegID &&
		a.capabilities&StreamCapabilityPathRTT != 0 && a.capabilities&^knownStreamCapabilities == 0
}

func validateStreamPathBinding(binding StreamPathBinding) error {
	if binding.CarrierID == "" || binding.Generation == 0 {
		return ErrStreamPathMessage
	}
	return nil
}

func validateStreamRTTMessage(message StreamRTTMessage) error {
	if err := validateStreamPathBinding(message.Identity.binding()); err != nil {
		return err
	}
	if message.Identity.Direction != StreamRTTClientToServer && message.Identity.Direction != StreamRTTServerToClient {
		return ErrStreamPathMessage
	}
	if (message.Kind != StreamRTTProbe && message.Kind != StreamRTTEcho) || message.Token == 0 {
		return ErrStreamPathMessage
	}
	return nil
}

func streamReadyV2MAC(hello Hello, prefix, password []byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-SIDECAR-READY-V2"))
	_, _ = mac.Write(hello.Nonce[:])
	_, _ = mac.Write(prefix)
	var out [sha256.Size]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func readyMagic(version Version) string {
	if version == Version7 {
		return streamReadyV3Magic
	}
	return streamReadyV2Magic
}

func streamReadyMAC(hello Hello, prefix, password []byte) [sha256.Size]byte {
	if hello.Version == Version6 {
		return streamReadyV2MAC(hello, prefix, password)
	}
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-SIDECAR-READY-V3"))
	_, _ = mac.Write([]byte{byte(hello.Version)})
	_, _ = mac.Write(hello.Nonce[:])
	_, _ = mac.Write(prefix)
	var out [sha256.Size]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func deriveStreamDrainControlKey(hello Hello, ready StreamReadyV2, password []byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-STREAM-DRAIN-CONTROL-KEY-V1"))
	_, _ = mac.Write([]byte{byte(hello.Version), byte(hello.Mode), byte(hello.LegID)})
	_, _ = mac.Write(hello.SessionID[:])
	var caps [4]byte
	binary.BigEndian.PutUint32(caps[:], ready.Capabilities)
	_, _ = mac.Write(caps[:])
	_, _ = mac.Write(hello.Nonce[:])
	_, _ = mac.Write(ready.ServerNonce[:])
	var out [sha256.Size]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func streamRTTMessageMAC(key [sha256.Size]byte, prefix []byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("SMP3-STREAM-RTT-MESSAGE-V1"))
	_, _ = mac.Write(prefix)
	var out [sha256.Size]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func writeProtocolFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// WriteStreamReadyV2 creates a fresh server nonce, writes the complete
// authenticated response, and only then returns the opaque path capability.
func WriteStreamReadyV2(w io.Writer, hello Hello, selectedCapabilities uint32, password []byte) (StreamPathAuth, error) {
	var serverNonce [16]byte
	if _, err := cryptorand.Read(serverNonce[:]); err != nil {
		return StreamPathAuth{}, err
	}
	return writeStreamReadyV2WithNonce(w, hello, selectedCapabilities, serverNonce, password)
}

func writeStreamReadyV2WithNonce(w io.Writer, hello Hello, selectedCapabilities uint32, serverNonce [16]byte, password []byte) (StreamPathAuth, error) {
	wire, err := EncodeStreamReadyV2(hello, selectedCapabilities, serverNonce, password)
	if err != nil {
		return StreamPathAuth{}, err
	}
	if err := writeProtocolFull(w, wire); err != nil {
		return StreamPathAuth{}, err
	}
	ready := StreamReadyV2{
		SessionID: hello.SessionID, LegID: hello.LegID, Mode: hello.Mode,
		Capabilities: selectedCapabilities, ServerNonce: serverNonce,
	}
	return makeStreamPathAuth(hello, ready, password)
}

// StreamDrainMessageKind is a monotone, idempotent coordinated-drain control
// phase. All messages are authenticated on a surviving negotiated carrier.
type StreamDrainMessageKind uint8

const (
	StreamDrainIntent StreamDrainMessageKind = iota + 1
	StreamDrainQuiesced
	StreamDrainReady
	StreamDrainCommit
	StreamDrainCommitAck
	StreamDrainAbort
)

// StreamDrainMessage is authenticated by the READY-derived key of the carrier
// that transports it. TargetBinding names the exact target carrier generation
// at both endpoints; Generation is the sender's local Core generation and is
// diagnostic because local counters need not be numerically synchronized.
type StreamDrainMessage struct {
	Kind              StreamDrainMessageKind
	SenderRole        StreamPathRole
	SessionID         SessionID
	TargetLeg         LegID
	Generation        uint64
	TargetBinding     [16]byte
	OperationID       [16]byte
	Counter           uint64
	AckedNext         uint64
	OutstandingFrames uint32
	OutstandingBytes  uint64
}

func (a StreamPathAuth) EncodeDrainMessage(message StreamDrainMessage) ([]byte, error) {
	if !a.CoordinatedDrainEnabled() || message.SessionID != a.sessionID || !validStreamDrainMessage(message) {
		return nil, ErrStreamDrainNegotiation
	}
	wire := make([]byte, StreamDrainControlMessageSize)
	copy(wire[:8], streamDrainControlMagic[:])
	wire[8] = 1
	wire[9] = byte(message.Kind)
	wire[10] = byte(message.SenderRole)
	copy(wire[12:28], message.SessionID[:])
	wire[28] = byte(message.TargetLeg)
	binary.BigEndian.PutUint64(wire[29:37], message.Generation)
	copy(wire[37:53], message.TargetBinding[:])
	copy(wire[53:69], message.OperationID[:])
	binary.BigEndian.PutUint64(wire[69:77], message.Counter)
	binary.BigEndian.PutUint64(wire[77:85], message.AckedNext)
	binary.BigEndian.PutUint32(wire[85:89], message.OutstandingFrames)
	binary.BigEndian.PutUint64(wire[89:97], message.OutstandingBytes)
	mac := streamDrainMessageMAC(a.drainKey, wire[:97])
	copy(wire[97:], mac[:])
	return wire, nil
}

func (a StreamPathAuth) DecodeDrainMessage(wire []byte, expectedPeerRole StreamPathRole) (StreamDrainMessage, error) {
	var message StreamDrainMessage
	if !a.CoordinatedDrainEnabled() || len(wire) != StreamDrainControlMessageSize ||
		!hmac.Equal(wire[:8], streamDrainControlMagic[:]) || wire[8] != 1 {
		return message, ErrStreamDrainMessage
	}
	if wire[11] != 0 {
		return message, ErrStreamDrainMessage
	}
	want := streamDrainMessageMAC(a.drainKey, wire[:97])
	if !hmac.Equal(wire[97:], want[:]) {
		return message, errInvalidAuthentication
	}
	message.Kind = StreamDrainMessageKind(wire[9])
	message.SenderRole = StreamPathRole(wire[10])
	copy(message.SessionID[:], wire[12:28])
	message.TargetLeg = LegID(wire[28])
	message.Generation = binary.BigEndian.Uint64(wire[29:37])
	copy(message.TargetBinding[:], wire[37:53])
	copy(message.OperationID[:], wire[53:69])
	message.Counter = binary.BigEndian.Uint64(wire[69:77])
	message.AckedNext = binary.BigEndian.Uint64(wire[77:85])
	message.OutstandingFrames = binary.BigEndian.Uint32(wire[85:89])
	message.OutstandingBytes = binary.BigEndian.Uint64(wire[89:97])
	if message.SessionID != a.sessionID || message.SenderRole != expectedPeerRole || !validStreamDrainMessage(message) {
		return StreamDrainMessage{}, ErrStreamDrainMessage
	}
	return message, nil
}

func validStreamDrainMessage(message StreamDrainMessage) bool {
	if message.Kind < StreamDrainIntent || message.Kind > StreamDrainAbort ||
		(message.SenderRole != StreamPathRoleClient && message.SenderRole != StreamPathRoleServer) ||
		message.Generation == 0 || message.Counter == 0 {
		return false
	}
	var zero [16]byte
	if message.TargetBinding == zero || message.OperationID == zero {
		return false
	}
	if (message.Kind == StreamDrainReady || message.Kind == StreamDrainCommit || message.Kind == StreamDrainCommitAck) &&
		(message.OutstandingFrames != 0 || message.OutstandingBytes != 0) {
		return false
	}
	return true
}

func streamDrainMessageMAC(key [sha256.Size]byte, prefix []byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("SMP3-STREAM-DRAIN-MESSAGE-V1"))
	_, _ = mac.Write(prefix)
	var out [sha256.Size]byte
	copy(out[:], mac.Sum(nil))
	return out
}

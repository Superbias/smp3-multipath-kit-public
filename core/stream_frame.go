package smp3core

import (
	"encoding/binary"
	"errors"
	"io"
)

// StreamFrameHeaderSize is the fixed SMP3 stream-frame header size:
// type(1) + value/sequence(8) + payload length(4).
const StreamFrameHeaderSize = 13

// MaxStreamFramePayload is the protocol safety bound for one DATA payload.
// Queue, inflight, scheduler, and retransmission limits remain host/core policy.
const MaxStreamFramePayload = 1 << 20

// StreamFrameType identifies a frame on an SMP3 stream carrier.
type StreamFrameType uint8

const (
	StreamFrameData     StreamFrameType = 1
	StreamFrameActivate StreamFrameType = 2
	StreamFrameACK      StreamFrameType = 3
	StreamFrameClose    StreamFrameType = 4
)

// StreamFrameHeader is the wire-visible portion of one stream frame. DATA uses
// Value as its sequence number; control frames use it as their control value.
// The codec never allocates or retains the DATA payload.
type StreamFrameHeader struct {
	Type   StreamFrameType
	Value  uint64
	Length uint32
}

var (
	errUnknownStreamFrameType   = errors.New("unknown multipath frame type")
	errInvalidControlFrameSize  = errors.New("invalid multipath control frame length")
	errInvalidStreamFrameLength = errors.New("invalid multipath frame length")
)

func validateStreamFrameHeader(header StreamFrameHeader) error {
	switch header.Type {
	case StreamFrameData:
		if header.Length == 0 || header.Length > MaxStreamFramePayload {
			return errInvalidStreamFrameLength
		}
	case StreamFrameActivate, StreamFrameACK, StreamFrameClose:
		if header.Length != 0 {
			return errInvalidControlFrameSize
		}
	default:
		return errUnknownStreamFrameType
	}
	return nil
}

// EncodeStreamFrameHeader writes one validated stream-frame header to dst.
// It does not inspect, copy, or retain a payload.
func EncodeStreamFrameHeader(dst []byte, header StreamFrameHeader) error {
	if len(dst) < StreamFrameHeaderSize {
		return io.ErrShortBuffer
	}
	if err := validateStreamFrameHeader(header); err != nil {
		return err
	}
	dst[0] = byte(header.Type)
	binary.BigEndian.PutUint64(dst[1:9], header.Value)
	binary.BigEndian.PutUint32(dst[9:StreamFrameHeaderSize], header.Length)
	return nil
}

// ReadStreamFrameHeader reads and validates one stream-frame header. The
// caller owns reading the declared DATA payload and decides its buffer policy.
func ReadStreamFrameHeader(reader io.Reader) (StreamFrameHeader, error) {
	var wire [StreamFrameHeaderSize]byte
	if _, err := io.ReadFull(reader, wire[:]); err != nil {
		return StreamFrameHeader{}, err
	}
	header := StreamFrameHeader{
		Type:   StreamFrameType(wire[0]),
		Value:  binary.BigEndian.Uint64(wire[1:9]),
		Length: binary.BigEndian.Uint32(wire[9:StreamFrameHeaderSize]),
	}
	if err := validateStreamFrameHeader(header); err != nil {
		return StreamFrameHeader{}, err
	}
	return header, nil
}

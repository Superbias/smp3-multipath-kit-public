package smp3core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"time"
)

var (
	errEmptyPassword         = errors.New("empty multipath password")
	errInvalidHello          = errors.New("invalid multipath hello")
	errInvalidVersion        = errors.New("invalid multipath hello version")
	errInvalidLeg            = errors.New("invalid multipath leg id")
	errInvalidMode           = errors.New("invalid multipath hello mode")
	errInvalidV5Mode         = errors.New("invalid multipath v5 hello mode")
	errStaleHello            = errors.New("stale multipath hello")
	errEmptyDestination      = errors.New("empty multipath destination")
	errInvalidDestination    = errors.New("invalid multipath destination")
	errInvalidAuthentication = errors.New("invalid multipath authentication")
	errInvalidCapabilities   = errors.New("invalid multipath stream capabilities")
)

const helloSkew = 90 * time.Second

var helloMagic = [4]byte{'S', 'M', 'P', '3'}

func EncodeHelloParts(hello Hello, password []byte) (header []byte, destination []byte, mac []byte, err error) {
	if len(password) == 0 {
		return nil, nil, nil, errEmptyPassword
	}
	if len(hello.Destination) == 0 || len(hello.Destination) > 65535 {
		return nil, nil, nil, errInvalidDestination
	}
	if hello.Version != Version4 && hello.Version != Version5 && hello.Version != Version6 && hello.Version != Version7 {
		return nil, nil, nil, errInvalidVersion
	}
	if hello.Version == Version4 && hello.Mode != ModeStream {
		return nil, nil, nil, errInvalidMode
	}
	if hello.Version == Version5 && hello.Mode != ModeDatagram {
		return nil, nil, nil, errInvalidMode
	}
	if hello.Version == Version6 && hello.Mode != ModeStream {
		return nil, nil, nil, errInvalidMode
	}
	if hello.Version == Version7 && hello.Mode != ModeStream {
		return nil, nil, nil, errInvalidMode
	}
	if hello.Version != Version6 && hello.Version != Version7 && hello.Capabilities != 0 {
		return nil, nil, nil, errInvalidCapabilities
	}
	if hello.Version == Version6 && (hello.Capabilities&^StreamCapabilityPathRTT != 0 || hello.Capabilities&StreamCapabilityPathRTT == 0) {
		return nil, nil, nil, errInvalidCapabilities
	}
	if hello.Version == Version7 && (hello.Capabilities&^knownStreamCapabilities != 0 || hello.Capabilities&StreamCapabilityCoordinatedDrain == 0) {
		return nil, nil, nil, errInvalidCapabilities
	}

	destination = []byte(hello.Destination)
	if hello.Version == Version5 {
		// v5: magic(4) version(1) leg(1) mode(1) session(16) unix(8)
		//     nonce(16) destlen(2)
		header = make([]byte, 49)
		copy(header[0:4], helloMagic[:])
		header[4] = byte(Version5)
		header[5] = byte(hello.LegID)
		header[6] = byte(hello.Mode)
		copy(header[7:23], hello.SessionID[:])
		binary.BigEndian.PutUint64(header[23:31], uint64(hello.Timestamp))
		copy(header[31:47], hello.Nonce[:])
		binary.BigEndian.PutUint16(header[47:49], uint16(len(destination)))
	} else if hello.Version == Version6 || hello.Version == Version7 {
		// v6/v7 stream: magic(4) version(1) leg(1) mode(1) session(16) unix(8)
		//     nonce(16) capabilities(4) destlen(2)
		header = make([]byte, 53)
		copy(header[0:4], helloMagic[:])
		header[4] = byte(hello.Version)
		header[5] = byte(hello.LegID)
		header[6] = byte(hello.Mode)
		copy(header[7:23], hello.SessionID[:])
		binary.BigEndian.PutUint64(header[23:31], uint64(hello.Timestamp))
		copy(header[31:47], hello.Nonce[:])
		binary.BigEndian.PutUint32(header[47:51], hello.Capabilities)
		binary.BigEndian.PutUint16(header[51:53], uint16(len(destination)))
	} else {
		// v4: magic(4) version(1) leg(1) session(16) unix(8) nonce(16)
		//     destlen(2)
		header = make([]byte, 48)
		copy(header[0:4], helloMagic[:])
		header[4] = byte(Version4)
		header[5] = byte(hello.LegID)
		copy(header[6:22], hello.SessionID[:])
		binary.BigEndian.PutUint64(header[22:30], uint64(hello.Timestamp))
		copy(header[30:46], hello.Nonce[:])
		binary.BigEndian.PutUint16(header[46:48], uint16(len(destination)))
	}

	hasher := hmac.New(sha256.New, password)
	_, _ = hasher.Write(header)
	_, _ = hasher.Write(destination)
	mac = hasher.Sum(nil)
	return header, destination, mac, nil
}

func ReadHelloAt(r io.Reader, password []byte, now time.Time) (Hello, error) {
	var hello Hello
	if len(password) == 0 {
		return hello, errEmptyPassword
	}

	prefix := make([]byte, 6)
	if _, err := io.ReadFull(r, prefix); err != nil {
		return hello, err
	}
	if prefix[0] != helloMagic[0] || prefix[1] != helloMagic[1] || prefix[2] != helloMagic[2] || prefix[3] != helloMagic[3] {
		return hello, errInvalidHello
	}

	version := Version(prefix[4])
	hello.Version = version
	hello.LegID = LegID(prefix[5])

	var header []byte
	var timestamp int64
	var destinationLength int
	switch version {
	case Version4:
		header = make([]byte, 48)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(r, header[6:]); err != nil {
			return hello, err
		}
		hello.Mode = ModeStream
		copy(hello.SessionID[:], header[6:22])
		copy(hello.Nonce[:], header[30:46])
		timestamp = int64(binary.BigEndian.Uint64(header[22:30]))
		destinationLength = int(binary.BigEndian.Uint16(header[46:48]))
	case Version5:
		header = make([]byte, 49)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(r, header[6:]); err != nil {
			return hello, err
		}
		hello.Mode = HelloMode(header[6])
		if hello.Mode != ModeDatagram {
			return hello, errInvalidV5Mode
		}
		copy(hello.SessionID[:], header[7:23])
		copy(hello.Nonce[:], header[31:47])
		timestamp = int64(binary.BigEndian.Uint64(header[23:31]))
		destinationLength = int(binary.BigEndian.Uint16(header[47:49]))
	case Version6:
		fallthrough
	case Version7:
		header = make([]byte, 53)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(r, header[6:]); err != nil {
			return hello, err
		}
		hello.Mode = HelloMode(header[6])
		if hello.Mode != ModeStream {
			return hello, errInvalidMode
		}
		copy(hello.SessionID[:], header[7:23])
		timestamp = int64(binary.BigEndian.Uint64(header[23:31]))
		copy(hello.Nonce[:], header[31:47])
		hello.Capabilities = binary.BigEndian.Uint32(header[47:51])
		if version == Version6 && (hello.Capabilities&^StreamCapabilityPathRTT != 0 || hello.Capabilities&StreamCapabilityPathRTT == 0) {
			return hello, errInvalidCapabilities
		}
		if version == Version7 && (hello.Capabilities&^knownStreamCapabilities != 0 || hello.Capabilities&StreamCapabilityCoordinatedDrain == 0) {
			return hello, errInvalidCapabilities
		}
		destinationLength = int(binary.BigEndian.Uint16(header[51:53]))
	default:
		return hello, errInvalidVersion
	}

	sent := time.Unix(timestamp, 0)
	if sent.Before(now.Add(-helloSkew)) || sent.After(now.Add(helloSkew)) {
		return hello, errStaleHello
	}
	if destinationLength <= 0 {
		return hello, errEmptyDestination
	}
	destination := make([]byte, destinationLength)
	if _, err := io.ReadFull(r, destination); err != nil {
		return hello, err
	}
	gotMAC := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, gotMAC); err != nil {
		return hello, err
	}
	hasher := hmac.New(sha256.New, password)
	_, _ = hasher.Write(header)
	_, _ = hasher.Write(destination)
	if !hmac.Equal(gotMAC, hasher.Sum(nil)) {
		return hello, errInvalidAuthentication
	}
	hello.Timestamp = timestamp
	hello.Destination = string(destination)
	return hello, nil
}

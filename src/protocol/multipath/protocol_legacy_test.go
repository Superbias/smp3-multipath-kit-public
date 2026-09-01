package multipath

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"time"
)

// legacyEncodeHelloParts is the pre-Phase-2 reference implementation. It is
// test-only so the runtime has one HELLO codec after the migration.
func legacyEncodeHelloParts(message helloMessage, password string, timestamp int64, nonce [16]byte) ([]byte, []byte, []byte, error) {
	if err := validateHelloMessage(message, password); err != nil {
		return nil, nil, nil, err
	}
	destination := []byte(message.Destination)
	var header []byte
	if message.Mode == helloModeDatagram {
		header = make([]byte, 49)
		copy(header[0:4], helloMagic[:])
		header[4] = helloVersionDatagram
		header[5] = message.LegID
		header[6] = byte(message.Mode)
		copy(header[7:23], message.Session[:])
		binary.BigEndian.PutUint64(header[23:31], uint64(timestamp))
		copy(header[31:47], nonce[:])
		binary.BigEndian.PutUint16(header[47:49], uint16(len(destination)))
	} else {
		header = make([]byte, 48)
		copy(header[0:4], helloMagic[:])
		header[4] = helloVersion
		header[5] = message.LegID
		copy(header[6:22], message.Session[:])
		binary.BigEndian.PutUint64(header[22:30], uint64(timestamp))
		copy(header[30:46], nonce[:])
		binary.BigEndian.PutUint16(header[46:48], uint16(len(destination)))
	}
	hasher := hmac.New(sha256.New, []byte(password))
	_, _ = hasher.Write(header)
	_, _ = hasher.Write(destination)
	return header, destination, hasher.Sum(nil), nil
}

func legacyReadHelloAt(r io.Reader, password string, now time.Time) (helloMessage, [16]byte, error) {
	var message helloMessage
	var nonce [16]byte
	if password == "" {
		return message, nonce, errors.New("empty multipath password")
	}
	prefix := make([]byte, 6)
	if _, err := io.ReadFull(r, prefix); err != nil {
		return message, nonce, err
	}
	if string(prefix[:4]) != string(helloMagic[:]) {
		return message, nonce, errors.New("invalid multipath hello")
	}
	version := prefix[4]
	message.LegID = prefix[5]
	if message.LegID > 1 {
		return message, nonce, errors.New("invalid multipath leg id")
	}
	var header []byte
	var timestamp int64
	var destinationLength int
	switch version {
	case helloVersion:
		header = make([]byte, 48)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(r, header[6:]); err != nil {
			return message, nonce, err
		}
		message.Mode = helloModeStream
		copy(message.Session[:], header[6:22])
		copy(nonce[:], header[30:46])
		timestamp = int64(binary.BigEndian.Uint64(header[22:30]))
		destinationLength = int(binary.BigEndian.Uint16(header[46:48]))
	case helloVersionDatagram:
		header = make([]byte, 49)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(r, header[6:]); err != nil {
			return message, nonce, err
		}
		message.Mode = helloMode(header[6])
		if message.Mode != helloModeDatagram {
			return message, nonce, errors.New("invalid multipath v5 hello mode")
		}
		copy(message.Session[:], header[7:23])
		copy(nonce[:], header[31:47])
		timestamp = int64(binary.BigEndian.Uint64(header[23:31]))
		destinationLength = int(binary.BigEndian.Uint16(header[47:49]))
	default:
		return message, nonce, errors.New("invalid multipath hello version")
	}
	sent := time.Unix(timestamp, 0)
	if sent.Before(now.Add(-helloSkew)) || sent.After(now.Add(helloSkew)) {
		return message, nonce, errors.New("stale multipath hello")
	}
	if destinationLength <= 0 {
		return message, nonce, errors.New("empty multipath destination")
	}
	destination := make([]byte, destinationLength)
	if _, err := io.ReadFull(r, destination); err != nil {
		return message, nonce, err
	}
	gotMAC := make([]byte, sha256.Size)
	if _, err := io.ReadFull(r, gotMAC); err != nil {
		return message, nonce, err
	}
	hasher := hmac.New(sha256.New, []byte(password))
	_, _ = hasher.Write(header)
	_, _ = hasher.Write(destination)
	if !hmac.Equal(gotMAC, hasher.Sum(nil)) {
		return message, nonce, errors.New("invalid multipath authentication")
	}
	message.Destination = string(destination)
	return message, nonce, nil
}

package multipath

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"
)

var helloMagic = [4]byte{'S', 'M', 'P', '3'}

const (
	// helloVersion remains the ordered stream HELLO used by alpha2.2/alpha2.3-r10.
	// R11 deliberately keeps writing v4 for TCP so an r11 client can still use an
	// r10 server for the stream data plane during a staged upgrade.
	helloVersion byte = 4
	// Datagram mode extends HELLO with one explicit mode byte. Only the new UDP
	// data plane writes v5; an r11 server accepts both v4 stream and v5 datagram.
	helloVersionDatagram byte = 5
)

const helloSkew = 90 * time.Second

type helloMode byte

const (
	helloModeStream helloMode = iota
	helloModeDatagram
)

func (m helloMode) String() string {
	if m == helloModeDatagram {
		return "datagram"
	}
	return "stream"
}

type helloMessage struct {
	Session     [16]byte
	LegID       uint8
	Mode        helloMode
	Destination string
}

func newSessionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func writeHello(conn net.Conn, message helloMessage, password string) error {
	if password == "" {
		return errors.New("empty multipath password")
	}
	if len(message.Destination) == 0 || len(message.Destination) > 65535 {
		return errors.New("invalid multipath destination")
	}
	if message.LegID > 1 {
		return errors.New("invalid multipath leg id")
	}
	if message.Mode != helloModeStream && message.Mode != helloModeDatagram {
		return errors.New("invalid multipath hello mode")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	dest := []byte(message.Destination)

	var header []byte
	if message.Mode == helloModeDatagram {
		// v5: magic(4) version(1) leg(1) mode(1) session(16) unix(8)
		//     nonce(16) destlen(2)
		header = make([]byte, 49)
		copy(header[0:4], helloMagic[:])
		header[4] = helloVersionDatagram
		header[5] = message.LegID
		header[6] = byte(message.Mode)
		copy(header[7:23], message.Session[:])
		binary.BigEndian.PutUint64(header[23:31], uint64(time.Now().Unix()))
		copy(header[31:47], nonce[:])
		binary.BigEndian.PutUint16(header[47:49], uint16(len(dest)))
	} else {
		// v4 is byte-for-byte compatible with r10 stream HELLO.
		// magic(4) version(1) leg(1) session(16) unix(8) nonce(16) destlen(2)
		header = make([]byte, 48)
		copy(header[0:4], helloMagic[:])
		header[4] = helloVersion
		header[5] = message.LegID
		copy(header[6:22], message.Session[:])
		binary.BigEndian.PutUint64(header[22:30], uint64(time.Now().Unix()))
		copy(header[30:46], nonce[:])
		binary.BigEndian.PutUint16(header[46:48], uint16(len(dest)))
	}

	mac := hmac.New(sha256.New, []byte(password))
	mac.Write(header)
	mac.Write(dest)
	sum := mac.Sum(nil)
	buffers := net.Buffers{header, dest, sum}
	_, err := buffers.WriteTo(conn)
	return err
}

func readHello(conn net.Conn, password string) (helloMessage, [16]byte, error) {
	var message helloMessage
	var nonce [16]byte
	if password == "" {
		return message, nonce, errors.New("empty multipath password")
	}

	prefix := make([]byte, 6)
	if _, err := io.ReadFull(conn, prefix); err != nil {
		return message, nonce, err
	}
	if string(prefix[0:4]) != string(helloMagic[:]) {
		return message, nonce, errors.New("invalid multipath hello")
	}
	version := prefix[4]
	message.LegID = prefix[5]
	if message.LegID > 1 {
		return message, nonce, errors.New("invalid multipath leg id")
	}

	var header []byte
	var ts int64
	var length int
	switch version {
	case helloVersion:
		header = make([]byte, 48)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(conn, header[6:]); err != nil {
			return message, nonce, err
		}
		message.Mode = helloModeStream
		copy(message.Session[:], header[6:22])
		copy(nonce[:], header[30:46])
		ts = int64(binary.BigEndian.Uint64(header[22:30]))
		length = int(binary.BigEndian.Uint16(header[46:48]))
	case helloVersionDatagram:
		header = make([]byte, 49)
		copy(header[:6], prefix)
		if _, err := io.ReadFull(conn, header[6:]); err != nil {
			return message, nonce, err
		}
		message.Mode = helloMode(header[6])
		if message.Mode != helloModeDatagram {
			return message, nonce, errors.New("invalid multipath v5 hello mode")
		}
		copy(message.Session[:], header[7:23])
		copy(nonce[:], header[31:47])
		ts = int64(binary.BigEndian.Uint64(header[23:31]))
		length = int(binary.BigEndian.Uint16(header[47:49]))
	default:
		return message, nonce, errors.New("invalid multipath hello version")
	}

	now := time.Now()
	sent := time.Unix(ts, 0)
	if sent.Before(now.Add(-helloSkew)) || sent.After(now.Add(helloSkew)) {
		return message, nonce, errors.New("stale multipath hello")
	}
	if length <= 0 {
		return message, nonce, errors.New("empty multipath destination")
	}
	dest := make([]byte, length)
	if _, err := io.ReadFull(conn, dest); err != nil {
		return message, nonce, err
	}
	got := make([]byte, sha256.Size)
	if _, err := io.ReadFull(conn, got); err != nil {
		return message, nonce, err
	}
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write(header)
	mac.Write(dest)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return message, nonce, errors.New("invalid multipath authentication")
	}
	message.Destination = string(dest)
	return message, nonce, nil
}

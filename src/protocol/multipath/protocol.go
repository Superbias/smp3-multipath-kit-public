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

const helloVersion byte = 4
const helloSkew = 90 * time.Second

type helloMessage struct {
	Session     [16]byte
	LegID       uint8
	Destination string
}

func newSessionID() ([16]byte, error) { var id [16]byte; _, err := rand.Read(id[:]); return id, err }

func writeHello(conn net.Conn, message helloMessage, password string) error {
	if password == "" {
		return errors.New("empty multipath password")
	}
	if len(message.Destination) == 0 || len(message.Destination) > 65535 {
		return errors.New("invalid multipath destination")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	dest := []byte(message.Destination)
	// magic(4) version(1) leg(1) session(16) unix(8) nonce(16) destlen(2)
	header := make([]byte, 48)
	copy(header[0:4], helloMagic[:])
	header[4] = helloVersion
	header[5] = message.LegID
	copy(header[6:22], message.Session[:])
	binary.BigEndian.PutUint64(header[22:30], uint64(time.Now().Unix()))
	copy(header[30:46], nonce[:])
	binary.BigEndian.PutUint16(header[46:48], uint16(len(dest)))
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
	header := make([]byte, 48)
	if _, err := io.ReadFull(conn, header); err != nil {
		return message, nonce, err
	}
	if string(header[0:4]) != string(helloMagic[:]) || header[4] != helloVersion {
		return message, nonce, errors.New("invalid multipath hello")
	}
	message.LegID = header[5]
	copy(message.Session[:], header[6:22])
	copy(nonce[:], header[30:46])
	ts := int64(binary.BigEndian.Uint64(header[22:30]))
	now := time.Now()
	sent := time.Unix(ts, 0)
	if sent.Before(now.Add(-helloSkew)) || sent.After(now.Add(helloSkew)) {
		return message, nonce, errors.New("stale multipath hello")
	}
	length := int(binary.BigEndian.Uint16(header[46:48]))
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

package client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	sidecarReadyV1Magic = "SMP3RDY1"
	sidecarReadyV1Size  = 8 + 8 + 1 + 1 + sha256.Size
)

func encodeSidecarReadyV1(hello smp3core.Hello, password []byte) ([]byte, error) {
	if len(password) == 0 {
		return nil, errors.New("empty sidecar READY password")
	}
	ready := make([]byte, sidecarReadyV1Size)
	copy(ready[:8], sidecarReadyV1Magic)
	binary.BigEndian.PutUint64(ready[8:16], binary.BigEndian.Uint64(hello.SessionID[:8]))
	ready[16] = byte(hello.LegID)
	ready[17] = byte(hello.Mode)
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-SIDECAR-READY-V1"))
	_, _ = mac.Write(hello.Nonce[:])
	_, _ = mac.Write(ready[8:18])
	copy(ready[18:], mac.Sum(nil))
	return ready, nil
}

func readSidecarReadyV1(reader io.Reader, expected smp3core.Hello, password []byte, timeout time.Duration) error {
	if len(password) == 0 {
		return errors.New("empty sidecar READY password")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if conn, ok := reader.(net.Conn); ok {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer conn.SetReadDeadline(time.Time{})
	}
	ready := make([]byte, sidecarReadyV1Size)
	if _, err := io.ReadFull(reader, ready); err != nil {
		return fmt.Errorf("read sidecar READY: %w", err)
	}
	if string(ready[:8]) != sidecarReadyV1Magic {
		return errors.New("invalid sidecar READY magic")
	}
	if binary.BigEndian.Uint64(ready[8:16]) != binary.BigEndian.Uint64(expected.SessionID[:8]) {
		return errors.New("sidecar READY session mismatch")
	}
	if ready[16] != byte(expected.LegID) {
		return errors.New("sidecar READY leg mismatch")
	}
	if ready[17] != byte(expected.Mode) {
		return errors.New("sidecar READY mode mismatch")
	}
	mac := hmac.New(sha256.New, password)
	_, _ = mac.Write([]byte("SMP3-SIDECAR-READY-V1"))
	_, _ = mac.Write(expected.Nonce[:])
	_, _ = mac.Write(ready[8:18])
	if !hmac.Equal(ready[18:], mac.Sum(nil)) {
		return errors.New("invalid sidecar READY authentication")
	}
	return nil
}

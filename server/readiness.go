package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	sidecarReadyV1Magic = "SMP3RDY1"
	sidecarReadyV1Size  = 8 + 8 + 1 + 1 + sha256.Size
)

var errSidecarReadyPassword = errors.New("empty sidecar READY password")

func encodeSidecarReadyV1(hello smp3core.Hello, password []byte) ([]byte, error) {
	if len(password) == 0 {
		return nil, errSidecarReadyPassword
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

func writeSidecarReadyV1(w io.Writer, hello smp3core.Hello, password []byte) error {
	ready, err := encodeSidecarReadyV1(hello, password)
	if err != nil {
		return err
	}
	for len(ready) > 0 {
		n, err := w.Write(ready)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		ready = ready[n:]
	}
	return nil
}

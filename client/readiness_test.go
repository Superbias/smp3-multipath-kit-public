package client

import (
	"bytes"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestSidecarReadyV1RoundTripAndBinding(t *testing.T) {
	hello := readinessTestHello()
	wire, err := encodeSidecarReadyV1(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	if err := readSidecarReadyV1(bytes.NewReader(wire), hello, []byte("password"), time.Second); err != nil {
		t.Fatal(err)
	}
	wrong := hello
	wrong.LegID = 0
	if err := readSidecarReadyV1(bytes.NewReader(wire), wrong, []byte("password"), time.Second); err == nil {
		t.Fatal("READY accepted for wrong leg")
	}
	wrong = hello
	wrong.Mode = smp3core.ModeDatagram
	if err := readSidecarReadyV1(bytes.NewReader(wire), wrong, []byte("password"), time.Second); err == nil {
		t.Fatal("READY accepted for wrong mode")
	}
}

func TestSidecarReadyV1RejectsMalformedAuthentication(t *testing.T) {
	hello := readinessTestHello()
	wire, err := encodeSidecarReadyV1(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte){
		"magic":     func(value []byte) { value[0] ^= 1 },
		"session":   func(value []byte) { value[8] ^= 1 },
		"mac":       func(value []byte) { value[len(value)-1] ^= 1 },
		"truncated": func(value []byte) { value = value[:len(value)-1] },
	} {
		t.Run(name, func(t *testing.T) {
			value := append([]byte(nil), wire...)
			if name == "truncated" {
				value = value[:len(value)-1]
			} else {
				mutate(value)
			}
			if err := readSidecarReadyV1(bytes.NewReader(value), hello, []byte("password"), time.Second); err == nil {
				t.Fatal("malformed READY was accepted")
			}
		})
	}
}

func readinessTestHello() smp3core.Hello {
	var hello smp3core.Hello
	for i := range hello.SessionID {
		hello.SessionID[i] = byte(i + 1)
		hello.Nonce[i] = byte(32 + i)
	}
	hello.Version = smp3core.Version4
	hello.LegID = 1
	hello.Mode = smp3core.ModeStream
	return hello
}

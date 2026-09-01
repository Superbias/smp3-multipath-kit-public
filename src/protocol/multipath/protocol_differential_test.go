package multipath

import (
	"bytes"
	"fmt"
	mathrand "math/rand"
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func coreHelloFromLegacy(message helloMessage, timestamp int64, nonce [16]byte) smp3core.Hello {
	version := smp3core.Version4
	if message.Mode == helloModeDatagram {
		version = smp3core.Version5
	}
	return smp3core.Hello{
		Version:     version,
		SessionID:   smp3core.SessionID(message.Session),
		LegID:       smp3core.LegID(message.LegID),
		Mode:        smp3core.HelloMode(message.Mode),
		Timestamp:   timestamp,
		Nonce:       nonce,
		Destination: message.Destination,
	}
}

func joinHelloParts(header, destination, mac []byte) []byte {
	return append(append(append([]byte(nil), header...), destination...), mac...)
}

func TestCoreHelloDifferentialGoldenVectors(t *testing.T) {
	vectors := []struct {
		name        string
		fixture     string
		version     smp3core.Version
		mode        helloMode
		leg         uint8
		destination string
	}{
		{name: "v4_leg0_ipv4", fixture: "hello_v4/v4_leg0_ipv4.hex", version: smp3core.Version4, mode: helloModeStream, leg: 0, destination: "192.0.2.1:443"},
		{name: "v4_leg1_ipv6", fixture: "hello_v4/v4_leg1_ipv6.hex", version: smp3core.Version4, mode: helloModeStream, leg: 1, destination: "[2001:db8::1]:8443"},
		{name: "v4_leg0_domain", fixture: "hello_v4/v4_leg0_domain.hex", version: smp3core.Version4, mode: helloModeStream, leg: 0, destination: "example.com:443"},
		{name: "v5_leg0_ipv4", fixture: "hello_v5/v5_leg0_ipv4.hex", version: smp3core.Version5, mode: helloModeDatagram, leg: 0, destination: "192.0.2.1:443"},
		{name: "v5_leg1_ipv6", fixture: "hello_v5/v5_leg1_ipv6.hex", version: smp3core.Version5, mode: helloModeDatagram, leg: 1, destination: "[2001:db8::1]:8443"},
		{name: "v5_leg0_domain", fixture: "hello_v5/v5_leg0_domain.hex", version: smp3core.Version5, mode: helloModeDatagram, leg: 0, destination: "example.com:443"},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			message := helloMessage{Session: goldenSession, LegID: vector.leg, Mode: vector.mode, Destination: vector.destination}
			legacyHeader, legacyDestination, legacyMAC, err := legacyEncodeHelloParts(message, goldenPassword, goldenUnixTime, goldenNonce)
			if err != nil {
				t.Fatal(err)
			}
			coreHeader, coreDestination, coreMAC, err := smp3core.EncodeHelloParts(coreHelloFromLegacy(message, goldenUnixTime, goldenNonce), []byte(goldenPassword))
			if err != nil {
				t.Fatal(err)
			}
			legacyWire := joinHelloParts(legacyHeader, legacyDestination, legacyMAC)
			coreWire := joinHelloParts(coreHeader, coreDestination, coreMAC)
			if !bytes.Equal(legacyWire, coreWire) {
				t.Fatalf("legacy/Core wire mismatch:\nlegacy=%x\ncore=%x", legacyWire, coreWire)
			}
			if !bytes.Equal(coreWire, wireFixture(t, vector.fixture)) {
				t.Fatalf("Core wire differs from frozen fixture")
			}

			legacyDecoded, legacyNonce, legacyErr := legacyReadHelloAt(bytes.NewReader(coreWire), goldenPassword, time.Unix(goldenUnixTime, 0))
			coreDecoded, coreErr := smp3core.ReadHelloAt(bytes.NewReader(coreWire), []byte(goldenPassword), time.Unix(goldenUnixTime, 0))
			if legacyErr != nil || coreErr != nil {
				t.Fatalf("decode errors: legacy=%v core=%v", legacyErr, coreErr)
			}
			if legacyDecoded != message || legacyNonce != coreDecoded.Nonce || coreDecoded != coreHelloFromLegacy(message, goldenUnixTime, goldenNonce) {
				t.Fatalf("decode mismatch: legacy=%+v nonce=%x core=%+v", legacyDecoded, legacyNonce, coreDecoded)
			}
			if coreDecoded.Version != vector.version {
				t.Fatalf("version=%d want=%d", coreDecoded.Version, vector.version)
			}
		})
	}
}

func differentialDestination(rng *mathrand.Rand, index int) string {
	switch index % 4 {
	case 0:
		return fmt.Sprintf("192.0.2.%d:%d", 1+rng.Intn(250), 1+rng.Intn(65534))
	case 1:
		return fmt.Sprintf("[2001:db8::%x]:%d", 1+rng.Intn(65534), 1+rng.Intn(65534))
	case 2:
		return fmt.Sprintf("node-%d.example.test:%d", index, 1+rng.Intn(65534))
	default:
		length := 1 + rng.Intn(2048)
		if index == 997 {
			length = 65535
		} else if index == 998 {
			length = 255
		}
		return strings.Repeat("x", length)
	}
}

func TestCoreHelloDifferentialRandomParity(t *testing.T) {
	rng := mathrand.New(mathrand.NewSource(0x534d50332d72616e))
	const cases = 1000
	for i := 0; i < cases; i++ {
		var session [16]byte
		var nonce [16]byte
		_, _ = rng.Read(session[:])
		_, _ = rng.Read(nonce[:])
		mode := helloModeStream
		if rng.Intn(2) == 1 {
			mode = helloModeDatagram
		}
		message := helloMessage{
			Session:     session,
			LegID:       uint8(rng.Intn(2)),
			Mode:        mode,
			Destination: differentialDestination(rng, i),
		}
		timestamp := int64(1600000000 + rng.Intn(500000000))
		legacyHeader, legacyDestination, legacyMAC, err := legacyEncodeHelloParts(message, goldenPassword, timestamp, nonce)
		if err != nil {
			t.Fatalf("case %d legacy encode: %v", i, err)
		}
		coreHeader, coreDestination, coreMAC, err := smp3core.EncodeHelloParts(coreHelloFromLegacy(message, timestamp, nonce), []byte(goldenPassword))
		if err != nil {
			t.Fatalf("case %d Core encode: %v", i, err)
		}
		if !bytes.Equal(joinHelloParts(legacyHeader, legacyDestination, legacyMAC), joinHelloParts(coreHeader, coreDestination, coreMAC)) {
			t.Fatalf("case %d legacy/Core wire mismatch", i)
		}
	}
}

func TestCoreHelloDifferentialInvalidParity(t *testing.T) {
	message := helloMessage{Session: goldenSession, LegID: 0, Mode: helloModeStream, Destination: "example.com:443"}
	header, destination, mac, err := legacyEncodeHelloParts(message, goldenPassword, goldenUnixTime, goldenNonce)
	if err != nil {
		t.Fatal(err)
	}
	valid := joinHelloParts(header, destination, mac)
	invalid := []struct {
		name string
		wire []byte
		now  time.Time
	}{
		{name: "bad_magic", wire: func() []byte { w := append([]byte(nil), valid...); w[0] ^= 1; return w }(), now: time.Unix(goldenUnixTime, 0)},
		{name: "bad_version", wire: func() []byte { w := append([]byte(nil), valid...); w[4] = 6; return w }(), now: time.Unix(goldenUnixTime, 0)},
		{name: "invalid_hmac", wire: func() []byte { w := append([]byte(nil), valid...); w[len(w)-1] ^= 1; return w }(), now: time.Unix(goldenUnixTime, 0)},
		{name: "stale_timestamp", wire: valid, now: time.Unix(goldenUnixTime+91, 0)},
		{name: "malformed_length", wire: func() []byte { w := append([]byte(nil), valid...); w[46] = 0; w[47] = 0; return w }(), now: time.Unix(goldenUnixTime, 0)},
		{name: "truncated_input", wire: valid[:47], now: time.Unix(goldenUnixTime, 0)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, _, legacyErr := legacyReadHelloAt(bytes.NewReader(test.wire), goldenPassword, test.now)
			_, coreErr := smp3core.ReadHelloAt(bytes.NewReader(test.wire), []byte(goldenPassword), test.now)
			if (legacyErr == nil) != (coreErr == nil) {
				t.Fatalf("acceptance mismatch: legacy=%v core=%v", legacyErr, coreErr)
			}
			if legacyErr == nil {
				t.Fatal("invalid HELLO accepted")
			}
		})
	}
}

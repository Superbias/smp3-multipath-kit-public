package smp3core

import (
	"bytes"
	"testing"
	"time"
)

func testHello(version Version, mode HelloMode) Hello {
	var session SessionID
	var nonce [16]byte
	for i := range session {
		session[i] = byte(i + 1)
		nonce[i] = byte(0xf0 - i)
	}
	return Hello{
		Version:     version,
		SessionID:   session,
		LegID:       1,
		Mode:        mode,
		Timestamp:   1700000000,
		Nonce:       nonce,
		Destination: "example.com:443",
	}
}

func TestHelloRoundTripV4(t *testing.T) {
	hello := testHello(Version4, ModeStream)
	header, destination, mac, err := EncodeHelloParts(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadHelloAt(bytes.NewReader(append(append(append([]byte(nil), header...), destination...), mac...)), []byte("password"), time.Unix(hello.Timestamp, 0))
	if err != nil {
		t.Fatal(err)
	}
	if decoded != hello {
		t.Fatalf("decoded=%+v want=%+v", decoded, hello)
	}
}

func TestHelloRoundTripV5(t *testing.T) {
	hello := testHello(Version5, ModeDatagram)
	header, destination, mac, err := EncodeHelloParts(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 49 || len(destination) != len(hello.Destination) || len(mac) != 32 {
		t.Fatalf("unexpected parts: header=%d destination=%d mac=%d", len(header), len(destination), len(mac))
	}
}

func TestHelloRejectsInvalidInputs(t *testing.T) {
	valid := testHello(Version4, ModeStream)
	for _, test := range []struct {
		name  string
		value Hello
	}{
		{name: "version", value: Hello{Version: 6, Mode: ModeStream, Destination: "x"}},
		{name: "mode", value: Hello{Version: Version4, Mode: ModeDatagram, Destination: "x"}},
		{name: "leg", value: Hello{Version: Version4, Mode: ModeStream, LegID: 2, Destination: "x"}},
		{name: "destination", value: Hello{Version: Version4, Mode: ModeStream}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, _, err := EncodeHelloParts(test.value, []byte("password")); err == nil {
				t.Fatal("invalid HELLO was accepted")
			}
		})
	}
	if _, _, _, err := EncodeHelloParts(valid, nil); err == nil {
		t.Fatal("empty password was accepted")
	}
}

func TestHelloFreshnessBoundary(t *testing.T) {
	hello := testHello(Version4, ModeStream)
	header, destination, mac, err := EncodeHelloParts(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	wire := append(append(append([]byte(nil), header...), destination...), mac...)
	if _, err := ReadHelloAt(bytes.NewReader(wire), []byte("password"), time.Unix(hello.Timestamp+90, 0)); err != nil {
		t.Fatalf("boundary HELLO rejected: %v", err)
	}
	if _, err := ReadHelloAt(bytes.NewReader(wire), []byte("password"), time.Unix(hello.Timestamp+91, 0)); err == nil {
		t.Fatal("stale HELLO accepted")
	}
}

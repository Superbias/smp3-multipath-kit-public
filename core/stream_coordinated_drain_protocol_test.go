package smp3core

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func phase36V7Hello(leg LegID, pathRTT bool) Hello {
	hello := testHello(Version7, ModeStream)
	hello.Timestamp = time.Now().Unix()
	hello.LegID = leg
	hello.Capabilities = StreamCapabilityCoordinatedDrain
	if pathRTT {
		hello.Capabilities |= StreamCapabilityPathRTT
	}
	return hello
}

func phase36V7AuthPair(t *testing.T, hello Hello, selected uint32) (StreamPathAuth, StreamPathAuth) {
	t.Helper()
	password := []byte("phase36-test-secret")
	ready := StreamReadyV2{SessionID: hello.SessionID, LegID: hello.LegID, Mode: hello.Mode,
		Capabilities: selected, ServerNonce: [16]byte{1, 2, 3, byte(hello.LegID + 1)}}
	clientAuth, err := makeStreamPathAuth(hello, ready, password)
	if err != nil {
		t.Fatal(err)
	}
	serverAuth, err := makeStreamPathAuth(hello, ready, password)
	if err != nil {
		t.Fatal(err)
	}
	return clientAuth, serverAuth
}

func TestStreamV7HelloReadyAndDrainCapabilityRoundTrip(t *testing.T) {
	password := []byte("phase36-test-secret")
	hello := phase36V7Hello(2, true)
	header, destination, helloMAC, err := EncodeHelloParts(hello, password)
	if err != nil {
		t.Fatal(err)
	}
	if got := Version(header[4]); got != Version7 {
		t.Fatalf("encoded HELLO version=%d, want %d", got, Version7)
	}
	decoded, err := ReadHelloAt(bytes.NewReader(bytes.Join([][]byte{header, destination, helloMAC}, nil)), password, time.Unix(hello.Timestamp, 0))
	if err != nil {
		t.Fatal(err)
	}
	if decoded != hello {
		t.Fatalf("decoded HELLO=%+v, want %+v", decoded, hello)
	}

	selected := StreamCapabilityCoordinatedDrain | StreamCapabilityPathRTT
	serverNonce := [16]byte{9, 8, 7, 6}
	readyWire, err := EncodeStreamReadyV2(hello, selected, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readyWire[:8]); got != streamReadyV3Magic {
		t.Fatalf("V7 READY magic=%q, want %q", got, streamReadyV3Magic)
	}
	_, clientAuth, err := ReadStreamReadyV2(bytes.NewReader(readyWire), hello, password)
	if err != nil {
		t.Fatal(err)
	}
	serverAuth, err := writeStreamReadyV2WithNonce(new(bytes.Buffer), hello, selected, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if !clientAuth.CoordinatedDrainEnabled() || !serverAuth.CoordinatedDrainEnabled() ||
		!clientAuth.PathRTTEnabled() || !serverAuth.PathRTTEnabled() {
		t.Fatal("V7 READY did not enable the selected authenticated capabilities")
	}
	if clientAuth.drainKey != serverAuth.drainKey || clientAuth.drainBinding != serverAuth.drainBinding {
		t.Fatal("V7 endpoints derived different drain key or carrier binding")
	}
}

func TestStreamV7RejectsInvalidHelloCapabilities(t *testing.T) {
	password := []byte("secret")
	hello := phase36V7Hello(0, false)
	if _, _, _, err := EncodeHelloParts(hello, password); err != nil {
		t.Fatalf("valid V7 HELLO: %v", err)
	}
	missingDrain := hello
	missingDrain.Capabilities = StreamCapabilityPathRTT
	if _, _, _, err := EncodeHelloParts(missingDrain, password); !errors.Is(err, errInvalidCapabilities) {
		t.Fatalf("missing drain capability error=%v", err)
	}
	unknown := hello
	unknown.Capabilities |= 1 << 31
	if _, _, _, err := EncodeHelloParts(unknown, password); !errors.Is(err, errInvalidCapabilities) {
		t.Fatalf("unknown capability error=%v", err)
	}
	datagram := hello
	datagram.Mode = ModeDatagram
	if _, _, _, err := EncodeHelloParts(datagram, password); !errors.Is(err, errInvalidMode) {
		t.Fatalf("V7 datagram error=%v", err)
	}
}

func TestStreamDrainControlMessageAuthenticatedAndBoundToCarrier(t *testing.T) {
	carrierHello := phase36V7Hello(1, false)
	clientAuth, serverAuth := phase36V7AuthPair(t, carrierHello, StreamCapabilityCoordinatedDrain)
	targetHello := phase36V7Hello(0, false)
	targetClientAuth, targetServerAuth := phase36V7AuthPair(t, targetHello, StreamCapabilityCoordinatedDrain)
	if targetClientAuth.drainBinding != targetServerAuth.drainBinding {
		t.Fatal("target carrier binding differs across endpoints")
	}
	message := StreamDrainMessage{
		Kind: StreamDrainReady, SenderRole: StreamPathRoleClient, SessionID: carrierHello.SessionID,
		TargetLeg: 0, Generation: 4, TargetBinding: targetClientAuth.drainBinding,
		OperationID: [16]byte{1}, Counter: 3,
	}
	wire, err := clientAuth.EncodeDrainMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	got, err := serverAuth.DecodeDrainMessage(wire, StreamPathRoleClient)
	if err != nil || got != message {
		t.Fatalf("authenticated drain round trip got=%+v err=%v", got, err)
	}
	if _, err := targetServerAuth.DecodeDrainMessage(wire, StreamPathRoleClient); err == nil {
		t.Fatal("message authenticated on a different Carrier")
	}
	if _, err := serverAuth.DecodeDrainMessage(wire, StreamPathRoleServer); err == nil {
		t.Fatal("message accepted with the wrong peer role")
	}

	tampered := append([]byte(nil), wire...)
	tampered[84] ^= 1
	if _, err := serverAuth.DecodeDrainMessage(tampered, StreamPathRoleClient); !errors.Is(err, errInvalidAuthentication) {
		t.Fatalf("tampered message error=%v", err)
	}
	reserved := append([]byte(nil), wire...)
	reserved[11] = 1
	mac := streamDrainMessageMAC(serverAuth.drainKey, reserved[:97])
	copy(reserved[97:], mac[:])
	if _, err := serverAuth.DecodeDrainMessage(reserved, StreamPathRoleClient); !errors.Is(err, ErrStreamDrainMessage) {
		t.Fatalf("nonzero reserved byte error=%v", err)
	}
}

func TestVersion6CannotEncodeCoordinatedDrain(t *testing.T) {
	hello := testStreamV6Hello()
	password := []byte("phase36-test-secret")
	ready := StreamReadyV2{SessionID: hello.SessionID, LegID: hello.LegID, Mode: hello.Mode,
		Capabilities: StreamCapabilityPathRTT, ServerNonce: [16]byte{3}}
	auth, err := makeStreamPathAuth(hello, ready, password)
	if err != nil {
		t.Fatal(err)
	}
	message := StreamDrainMessage{Kind: StreamDrainIntent, SenderRole: StreamPathRoleClient,
		SessionID: hello.SessionID, TargetLeg: 0, Generation: 1, TargetBinding: [16]byte{1},
		OperationID: [16]byte{2}, Counter: 1}
	if _, err := auth.EncodeDrainMessage(message); !errors.Is(err, ErrStreamDrainNegotiation) {
		t.Fatalf("V6 drain encode error=%v", err)
	}
}

package smp3core

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"
)

type shortStreamPathWriter struct {
	bytes.Buffer
	maxWrites int
	writes    int
	failAfter int
}

func (w *shortStreamPathWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.failAfter > 0 && w.writes > w.failAfter {
		return 0, io.ErrClosedPipe
	}
	if w.maxWrites > 0 && len(p) > w.maxWrites {
		p = p[:w.maxWrites]
	}
	return w.Buffer.Write(p)
}

func testStreamV6Hello() Hello {
	hello := testHello(Version4, ModeStream)
	hello.Version = Version6
	hello.Capabilities = StreamCapabilityPathRTT
	return hello
}

func testStreamPathBinding(generation uint64) StreamPathBinding {
	return StreamPathBinding{
		SessionID:  SessionID{1, 2, 3, 4},
		LegID:      1,
		CarrierID:  "carrier-1",
		Generation: generation,
	}
}

func testStreamPathIdentity(generation uint64, direction StreamRTTDirection) StreamPathIdentity {
	binding := testStreamPathBinding(generation)
	return StreamPathIdentity{
		SessionID:  binding.SessionID,
		LegID:      binding.LegID,
		CarrierID:  binding.CarrierID,
		Generation: binding.Generation,
		Direction:  direction,
	}
}

func TestStreamV6HelloAndReadyV2RoundTrip(t *testing.T) {
	hello := testStreamV6Hello()
	password := []byte("correct horse battery staple")
	serverNonce := [16]byte{9, 8, 7, 6, 5, 4, 3, 2, 1}
	header, destination, helloMAC, err := EncodeHelloParts(hello, password)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 53 {
		t.Fatalf("v6 header length=%d, want 53", len(header))
	}
	wire := bytes.Join([][]byte{header, destination, helloMAC}, nil)
	decoded, err := ReadHelloAt(bytes.NewReader(wire), password, time.Unix(hello.Timestamp, 0))
	if err != nil {
		t.Fatal(err)
	}
	if decoded != hello {
		t.Fatalf("decoded HELLO differs: got=%+v want=%+v", decoded, hello)
	}

	readyWire, err := EncodeStreamReadyV2(hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	var serverOutput bytes.Buffer
	serverAuth, err := writeStreamReadyV2WithNonce(&serverOutput, hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(serverOutput.Bytes(), readyWire) {
		t.Fatal("READY write differs from encoded response")
	}
	ready, clientAuth, err := ReadStreamReadyV2(bytes.NewReader(readyWire), hello, password)
	if err != nil {
		t.Fatal(err)
	}
	if ready.SessionID != hello.SessionID || ready.LegID != hello.LegID || ready.Mode != ModeStream ||
		ready.Capabilities != StreamCapabilityPathRTT || ready.ServerNonce != serverNonce {
		t.Fatalf("unexpected READY: %+v", ready)
	}
	if !serverAuth.PathRTTEnabled() || !clientAuth.PathRTTEnabled() {
		t.Fatal("authenticated READY did not produce path RTT capabilities")
	}
	clientAuth, err = clientAuth.WithCarrierHandshakeRTT(400 * time.Millisecond)
	if err != nil {
		t.Fatalf("attach same-Carrier handshake RTT: %v", err)
	}
	if serverAuth.probeKey != clientAuth.probeKey {
		t.Fatal("client and server authenticated probe keys differ")
	}
	otherReady := ready
	otherReady.ServerNonce[0]++
	otherKey, err := deriveStreamPathProbeKey(hello, otherReady, password)
	if err != nil {
		t.Fatal(err)
	}
	if clientAuth.probeKey == otherKey {
		t.Fatal("probe key did not change with server nonce")
	}
}

func TestStreamPathAuthRejectsInvalidHandshakeRTTSamples(t *testing.T) {
	hello := testStreamV6Hello()
	password := []byte("password")
	wire, err := EncodeStreamReadyV2(hello, StreamCapabilityPathRTT, [16]byte{7}, password)
	if err != nil {
		t.Fatal(err)
	}
	_, auth, err := ReadStreamReadyV2(bytes.NewReader(wire), hello, password)
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []time.Duration{0, -time.Millisecond, StreamRTTProbeTimeout} {
		if _, err := auth.WithCarrierHandshakeRTT(sample); !errors.Is(err, ErrStreamPathMessage) {
			t.Errorf("RTT %v error=%v, want invalid sample", sample, err)
		}
	}
	legacy := StreamPathAuth{authenticated: true}
	if _, err := legacy.WithCarrierHandshakeRTT(time.Millisecond); !errors.Is(err, ErrStreamPathNegotiation) {
		t.Fatalf("unnegotiated RTT capability error=%v", err)
	}
}

func TestStreamV6RejectsUnknownCapabilitiesAndInvalidReady(t *testing.T) {
	password := []byte("password")
	hello := testStreamV6Hello()
	unknown := hello
	unknown.Capabilities |= 1 << 31
	if _, _, _, err := EncodeHelloParts(unknown, password); !errors.Is(err, errInvalidCapabilities) {
		t.Fatalf("unknown HELLO capability error=%v", err)
	}
	datagram := hello
	datagram.Mode = ModeDatagram
	if _, _, _, err := EncodeHelloParts(datagram, password); !errors.Is(err, errInvalidMode) {
		t.Fatalf("v6 datagram error=%v", err)
	}

	serverNonce := [16]byte{1}
	readyWire, err := EncodeStreamReadyV2(hello, 0, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deriveStreamPathProbeKey(hello, StreamReadyV2{SessionID: hello.SessionID, LegID: hello.LegID, Mode: ModeStream}, password); !errors.Is(err, ErrStreamPathNegotiation) {
		t.Fatalf("unselected capability error=%v", err)
	}
	readyWire[len(readyWire)-1] ^= 0x80
	if _, _, err := ReadStreamReadyV2(bytes.NewReader(readyWire), hello, password); !errors.Is(err, errInvalidAuthentication) {
		t.Fatalf("tampered READY error=%v", err)
	}

	wrongNonce := hello
	wrongNonce.Nonce[0]++
	readyWire, err = EncodeStreamReadyV2(hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadStreamReadyV2(bytes.NewReader(readyWire), wrongNonce, password); !errors.Is(err, errInvalidAuthentication) {
		t.Fatalf("READY bound to different HELLO nonce error=%v", err)
	}
}

func TestWriteStreamReadyV2RequiresCompleteWriteBeforeReturningAuth(t *testing.T) {
	hello := testStreamV6Hello()
	password := []byte("password")
	serverNonce := [16]byte{1, 2, 3}

	short := &shortStreamPathWriter{maxWrites: 3}
	auth, err := writeStreamReadyV2WithNonce(short, hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatalf("short writes should be completed: %v", err)
	}
	if !auth.PathRTTEnabled() || short.Len() != streamReadyV2Size {
		t.Fatalf("successful READY did not issue auth: auth=%v bytes=%d", auth.PathRTTEnabled(), short.Len())
	}

	failed := &shortStreamPathWriter{maxWrites: 3, failAfter: 2}
	auth, err = writeStreamReadyV2WithNonce(failed, hello, StreamCapabilityPathRTT, serverNonce, password)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("failed READY write error=%v", err)
	}
	if auth.PathRTTEnabled() {
		t.Fatal("failed READY write returned an authenticated path capability")
	}
}

func TestStreamRTTMessageAuthenticatesFieldsAndBinding(t *testing.T) {
	identity := testStreamPathIdentity(4, StreamRTTClientToServer)
	key := sha256.Sum256([]byte("probe key"))
	wire, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTProbe, Identity: identity, Token: 9}, key)
	if err != nil {
		t.Fatal(err)
	}
	message, err := decodeStreamRTTMessage(wire, key, identity.binding())
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != StreamRTTProbe || message.Identity != identity || message.Token != 9 {
		t.Fatalf("decoded message=%+v", message)
	}

	for name, mutate := range map[string]func([]byte){
		"direction": func(b []byte) { b[10] = byte(StreamRTTServerToClient) },
		"leg":       func(b []byte) { b[11] = 0 },
		"session":   func(b []byte) { b[12] ^= 1 },
		"token":     func(b []byte) { b[35] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := append([]byte(nil), wire...)
			mutate(tampered)
			if _, err := decodeStreamRTTMessage(tampered, key, identity.binding()); !errors.Is(err, errInvalidAuthentication) {
				t.Fatalf("tampered message error=%v", err)
			}
		})
	}
	newGenerationKey := sha256.Sum256([]byte("new-generation-key"))
	if _, err := decodeStreamRTTMessage(wire, newGenerationKey, testStreamPathBinding(5)); !errors.Is(err, errInvalidAuthentication) {
		t.Fatalf("old generation key accepted by replacement binding: %v", err)
	}
}

package server

import (
	"bytes"
	"testing"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestSidecarReadyV1EncodingIsAuthenticatedAndBound(t *testing.T) {
	var hello smp3core.Hello
	for i := range hello.SessionID {
		hello.SessionID[i] = byte(i + 1)
		hello.Nonce[i] = byte(32 + i)
	}
	hello.Version = smp3core.Version4
	hello.LegID = 1
	hello.Mode = smp3core.ModeStream
	wire, err := encodeSidecarReadyV1(hello, []byte("password"))
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != sidecarReadyV1Size || !bytes.Equal(wire[:8], []byte(sidecarReadyV1Magic)) {
		t.Fatalf("READY wire length/magic invalid: %d %q", len(wire), wire[:8])
	}
	if _, err := encodeSidecarReadyV1(hello, nil); err == nil {
		t.Fatal("empty password accepted")
	}
}

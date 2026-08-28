package multipath

import (
	"net"
	"testing"
)

func TestHelloWireCompatibilityAndSessionReuse(t *testing.T) {
	if string(helloMagic[:]) != "SMP3" {
		t.Fatalf("hello magic=%q, want SMP3", helloMagic)
	}
	if helloVersion != 4 {
		t.Fatalf("hello version=%d, want 4", helloVersion)
	}
	if frameHeaderSize != 13 {
		t.Fatalf("frame header size=%d, want 13", frameHeaderSize)
	}

	password := "test-password"
	session := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for _, legID := range []uint8{0, 1} {
		client, server := net.Pipe()
		writeErr := make(chan error, 1)
		go func() {
			writeErr <- writeHello(client, helloMessage{
				Session:     session,
				LegID:       legID,
				Destination: "example.com:443",
			}, password)
			_ = client.Close()
		}()

		got, _, err := readHello(server, password)
		_ = server.Close()
		if err != nil {
			t.Fatalf("read leg %d hello: %v", legID, err)
		}
		if err := <-writeErr; err != nil {
			t.Fatalf("write leg %d hello: %v", legID, err)
		}
		if got.Session != session || got.LegID != legID || got.Mode != helloModeStream || got.Destination != "example.com:443" {
			t.Fatalf("decoded leg %d hello=%+v", legID, got)
		}
	}
}

func TestDatagramHelloV5(t *testing.T) {
	if helloVersionDatagram != 5 {
		t.Fatalf("datagram hello version=%d, want 5", helloVersionDatagram)
	}
	password := "test-password"
	session := [16]byte{9, 8, 7, 6, 5, 4, 3, 2, 1}
	client, server := net.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- writeHello(client, helloMessage{
			Session: session, LegID: 1, Mode: helloModeDatagram, Destination: "192.0.2.1:53",
		}, password)
		_ = client.Close()
	}()
	got, _, err := readHello(server, password)
	_ = server.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if got.Session != session || got.LegID != 1 || got.Mode != helloModeDatagram || got.Destination != "192.0.2.1:53" {
		t.Fatalf("decoded datagram hello=%+v", got)
	}
}

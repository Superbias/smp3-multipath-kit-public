package front

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConfigRejectsUnknownFieldsAndInvalidTarget(t *testing.T) {
	if _, err := LoadConfig(strings.NewReader(`{"listen":"127.0.0.1:0","landing_target":"127.0.0.1:8443","udp":true}`)); err == nil {
		t.Fatal("unknown UDP setting was accepted by TCP-only Front")
	}
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.LandingTarget = "udp://127.0.0.1:8443"
	if err := cfg.NormalizeAndValidate(); err == nil {
		t.Fatal("non-TCP target was accepted")
	}
}

func TestPublicListenerRequiresValidSourceAllowlist(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Listen = "0.0.0.0:8444"
	cfg.LandingTarget = "192.0.2.20:8443"
	if err := cfg.NormalizeAndValidate(); err == nil {
		t.Fatal("public Front listener was accepted without allowed_sources")
	}
	cfg.AllowedSources = []string{"not-a-cidr"}
	if err := cfg.NormalizeAndValidate(); err == nil {
		t.Fatal("public Front listener accepted an invalid source CIDR")
	}
	cfg.AllowedSources = []string{"198.51.100.25/32"}
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatalf("public Front listener with a CIDR allowlist was rejected: %v", err)
	}
}

func TestRelayForwardsOpaqueTCPAndHalfClose(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		conn, err := echo.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()

	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.LandingTarget = echo.Addr().String()
	relay, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	client, err := net.Dial("tcp", relay.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	want := []byte("opaque tls carrier bytes")
	if _, err := client.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("forwarded bytes=%q want=%q", got, want)
	}
}

func TestRelayPreservesRequestHalfCloseAndResponse(t *testing.T) {
	landing, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer landing.Close()
	go func() {
		conn, err := landing.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		request, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		_, _ = conn.Write(append([]byte("reply:"), request...))
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()

	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.LandingTarget = landing.Addr().String()
	relay, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	conn, err := net.Dial("tcp", relay.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tcp := conn.(*net.TCPConn)
	defer tcp.Close()
	_ = tcp.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := tcp.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := tcp.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(tcp)
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != "reply:request" {
		t.Fatalf("response=%q want=%q", response, "reply:request")
	}
}

func TestRelayEnforcesConnectionLimitAndShutdown(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.LandingTarget = echo.Addr().String()
	cfg.MaxConnections = 1
	relay, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	first, err := net.Dial("tcp", relay.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for relay.ActiveConnections() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if relay.ActiveConnections() != 1 {
		t.Fatal("first connection was not counted")
	}
	second, err := net.Dial("tcp", relay.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	var byteValue [1]byte
	if _, err := second.Read(byteValue[:]); err == nil {
		t.Fatal("connection limit did not close excess connection")
	}
	_ = second.Close()
	_ = first.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := relay.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

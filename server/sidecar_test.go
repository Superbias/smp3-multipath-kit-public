package server

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func sidecarTestHello(id smp3core.SessionID, leg uint8, mode smp3core.HelloMode, destination string, nonce [16]byte) smp3core.Hello {
	version := smp3core.Version4
	if mode == smp3core.ModeDatagram {
		version = smp3core.Version5
	}
	return smp3core.Hello{
		Version: version, SessionID: id, LegID: smp3core.LegID(leg), Mode: mode,
		Timestamp: time.Now().Unix(), Nonce: nonce, Destination: destination,
	}
}

func connectSidecarAndReadReady(t *testing.T, address, password string, hello smp3core.Hello) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	writeHelloNonce(t, conn, password, hello.SessionID, uint8(hello.LegID), hello.Mode, hello.Destination, hello.Nonce)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	ready := make([]byte, sidecarReadyV1Size)
	if _, err := io.ReadFull(conn, ready); err != nil {
		_ = conn.Close()
		t.Fatalf("read sidecar READY: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	expected, err := encodeSidecarReadyV1(hello, []byte(password))
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if !bytes.Equal(ready, expected) {
		_ = conn.Close()
		t.Fatalf("sidecar READY mismatch: got=%x want=%x", ready, expected)
	}
	return conn
}

func readClosedSidecar(t *testing.T, conn net.Conn) {
	t.Helper()
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("sidecar connection remained open")
	}
}

func TestSidecarConfigCompatibilityAndStrictUnknownFields(t *testing.T) {
	valid := testConfig()
	valid.SidecarListeners = []string{"127.0.0.2:24445", "127.0.0.3:24446"}
	if err := valid.NormalizeAndValidate(); err != nil {
		t.Fatalf("multiple sidecar listeners rejected: %v", err)
	}
	checks := []struct {
		name string
		edit func(*Config)
	}{
		{"duplicate-primary", func(c *Config) { c.Listen = "127.0.0.1:24444"; c.SidecarListeners = []string{c.Listen} }},
		{"duplicate-sidecar", func(c *Config) { c.SidecarListeners = []string{"127.0.0.2:24445", "127.0.0.2:24445"} }},
		{"malformed-sidecar", func(c *Config) { c.SidecarListeners = []string{"not-an-address"} }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			cfg := valid
			check.edit(&cfg)
			if err := cfg.NormalizeAndValidate(); err == nil {
				t.Fatal("invalid sidecar listener config was accepted")
			}
		})
	}

	fixture, err := LoadConfig(filepath.Join("testdata", "legacy_v220_server_config.json"))
	if err != nil {
		t.Fatalf("sanitized legacy v2.2.0 fixture rejected: %v", err)
	}
	if fixture.Listen != "127.0.0.1:24444" || len(fixture.SidecarListeners) != 1 || fixture.SidecarListeners[0] != "127.0.0.1:24445" {
		t.Fatalf("legacy fixture normalization lost listener shape: %+v", fixture)
	}

	unknownPath := filepath.Join(t.TempDir(), "unknown.json")
	unknown := []byte(`{"listen":"127.0.0.1:24444","password":"test-only","unknown_field":true}`)
	if err := os.WriteFile(unknownPath, unknown, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(unknownPath); err == nil || !strings.Contains(err.Error(), `unknown field "unknown_field"`) {
		t.Fatalf("unknown JSON field was not rejected strictly: %v", err)
	}
}

func TestSidecarListenersBindIndependentlyAndEmitExactReady(t *testing.T) {
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.SidecarListeners = []string{"127.0.0.2:0", "127.0.0.3:0"}
	instance := startTestServer(t, cfg)
	addresses := instance.ListenerAddrs()
	if len(addresses) != 3 {
		t.Fatalf("listener count=%d, want 3", len(addresses))
	}
	if addresses[0] != instance.String() || len(instance.sidecarListeners) != 2 {
		t.Fatalf("listener ownership primary=%q sidecars=%d addresses=%v", instance.String(), len(instance.sidecarListeners), addresses)
	}

	var id smp3core.SessionID
	id[0] = 0x31
	var nonce0, nonce1 [16]byte
	nonce0[0], nonce1[0] = 1, 2
	leg0 := connectSidecarAndReadReady(t, addresses[1], cfg.Password, sidecarTestHello(id, 0, smp3core.ModeStream, target, nonce0))
	leg1 := connectSidecarAndReadReady(t, addresses[2], cfg.Password, sidecarTestHello(id, 1, smp3core.ModeStream, target, nonce1))
	defer leg0.Close()
	defer leg1.Close()

	waitFor(t, time.Second, func() bool {
		instance.access.Lock()
		session := instance.sessions[id]
		ready := session != nil && session.stream.HasLeg(0) && session.stream.HasLeg(1)
		instance.access.Unlock()
		return ready
	})
}

func TestSidecarFailureAndDuplicateLegAreLocal(t *testing.T) {
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.SidecarListeners = []string{"127.0.0.2:0"}
	instance := startTestServer(t, cfg)
	address := instance.ListenerAddrs()[1]

	var badID smp3core.SessionID
	badID[0] = 0x41
	badAuth, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	writeHello(t, badAuth, "wrong-password", badID, 0, smp3core.ModeStream, target, 10)
	readClosedSidecar(t, badAuth)
	if instance.SessionCount() != 0 {
		t.Fatalf("bad sidecar auth created a session: %d", instance.SessionCount())
	}

	var id smp3core.SessionID
	id[0] = 0x42
	var nonce [16]byte
	nonce[0] = 20
	first := connectSidecarAndReadReady(t, address, cfg.Password, sidecarTestHello(id, 0, smp3core.ModeStream, target, nonce))
	defer first.Close()

	duplicateNonce := nonce
	duplicateNonce[0]++
	duplicate, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	writeHelloNonce(t, duplicate, cfg.Password, id, 0, smp3core.ModeStream, target, duplicateNonce)
	readClosedSidecar(t, duplicate)

	if instance.SessionCount() != 1 {
		t.Fatalf("local sidecar failures changed session count: %d", instance.SessionCount())
	}
	primary := connectLeg(t, instance, cfg.Password, id, 1, smp3core.ModeStream, target, 31)
	defer primary.Close()
}

func TestSidecarPartialBindAndPendingShutdownCleanup(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	cfg := testConfig()
	cfg.SidecarListeners = []string{"127.0.0.2:0", occupied.Addr().String()}
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err == nil {
		t.Fatal("partial listener bind unexpectedly succeeded")
	}
	instance.access.Lock()
	if instance.listener != nil || len(instance.listeners) != 0 {
		instance.access.Unlock()
		t.Fatal("partial listener bind left server listener state behind")
	}
	instance.access.Unlock()
	_ = instance.Close()

	cfg = testConfig()
	cfg.HelloReadTimeout = Duration(5 * time.Second)
	cfg.SidecarListeners = []string{"127.0.0.2:0"}
	instance = startTestServer(t, cfg)
	pending, err := net.Dial("tcp", instance.ListenerAddrs()[1])
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		instance.pendingMu.Lock()
		count := len(instance.pending)
		instance.pendingMu.Unlock()
		return count > 0
	})
	started := time.Now()
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("pending sidecar shutdown took %v", elapsed)
	}
	readClosedSidecar(t, pending)
}

func TestSidecarSessionAppearsInTelemetryAfterAttach(t *testing.T) {
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.SidecarListeners = []string{"127.0.0.2:0"}
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.Listen = "127.0.0.1:0"
	instance := startTestServer(t, cfg)

	var id smp3core.SessionID
	id[0] = 0x51
	var nonce [16]byte
	nonce[0] = 40
	conn := connectSidecarAndReadReady(t, instance.ListenerAddrs()[1], cfg.Password, sidecarTestHello(id, 0, smp3core.ModeStream, target, nonce))
	defer conn.Close()
	waitFor(t, time.Second, func() bool {
		snapshot := instance.Telemetry().Snapshot()
		return snapshot.ActiveSessions == 1 && snapshot.ActiveLegs == 1 && len(snapshot.Sessions) == 1 && snapshot.Sessions[0].Legs[0].WireRxBytes > 0
	})
}

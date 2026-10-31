package server

import (
	"net"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestPublicListenerRequiresFixedApplicationTarget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Listen = "0.0.0.0:8443"
	cfg.Password = localTestCredential
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("public listener without application_target was accepted")
	}
	cfg.ApplicationTarget = "127.0.0.1:9000"
	cfg.CarrierTLS = CarrierTLSOptions{CertFile: "server.crt", KeyFile: "server.key"}
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatalf("public listener with fixed target and TLS configuration rejected: %v", err)
	}
}

func TestServerRejectsDestinationOutsideApplicationTarget(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	cfg := testConfig()
	cfg.ApplicationTarget = target.Addr().String()
	instance := startTestServer(t, cfg)
	var id smp3core.SessionID
	id[0] = 7
	conn := connectLeg(t, instance, cfg.Password, id, 0, smp3core.ModeStream, "127.0.0.1:9", 88)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var byteValue [1]byte
	if _, err := conn.Read(byteValue[:]); err == nil {
		t.Fatal("disallowed destination was not rejected")
	}
	if got := instance.SessionCount(); got != 0 {
		t.Fatalf("rejected destination created %d sessions", got)
	}
}

func TestServerEnforcesActiveSessionLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSessions = 1
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	var firstID, secondID smp3core.SessionID
	firstID[0] = 1
	secondID[0] = 2
	firstHello := smp3core.Hello{Version: smp3core.Version4, SessionID: firstID, Mode: smp3core.ModeStream}
	first, created, err := instance.admitSession(firstHello, "127.0.0.1:80")
	if err != nil || !created {
		t.Fatalf("first session admission: created=%t err=%v", created, err)
	}
	secondHello := smp3core.Hello{Version: smp3core.Version4, SessionID: secondID, Mode: smp3core.ModeStream}
	if _, _, err := instance.admitSession(secondHello, "127.0.0.1:80"); err != errSessionLimit {
		t.Fatalf("second session error=%v, want %v", err, errSessionLimit)
	}
	first.close()
	deadline := time.Now().Add(time.Second)
	for instance.SessionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if instance.SessionCount() != 0 {
		t.Fatal("closed session did not release its admission slot")
	}
}

func TestServerEnforcesTCPConnectionLimitBeforeHello(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConnections = 1
	instance := startTestServer(t, cfg)
	first, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for len(instance.connectionSlots) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(instance.connectionSlots) != 1 {
		t.Fatal("first TCP connection did not consume a slot")
	}
	second, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	var byteValue [1]byte
	if _, err := second.Read(byteValue[:]); err == nil {
		t.Fatal("excess TCP connection was not closed")
	}
	_ = first.Close()
	deadline = time.Now().Add(time.Second)
	for len(instance.connectionSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(instance.connectionSlots) != 0 {
		t.Fatal("TCP connection slot was not released")
	}
}

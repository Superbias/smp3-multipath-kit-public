package client

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	serverpkg "github.com/Superbias/smp3-multipath-kit-public/server"
)

func TestStreamPerLegCarrierBindingSameSidecarAndIntegrity(t *testing.T) {
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = false
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	addresses := standalone.ListenerAddrs()
	if len(addresses) != 2 {
		t.Fatalf("server listeners = %v", addresses)
	}

	carrierA := newSOCKSForwarder(t)
	defer carrierA.Close()
	carrierB := newSOCKSForwarder(t)
	defer carrierB.Close()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = carrierA.Addr().String()
	cfg.UpstreamSocks.Leg1 = &UpstreamSocksOverride{Address: carrierB.Addr().String()}
	cfg.SMP3.Routes.Leg0 = addresses[1]
	cfg.SMP3.Routes.Leg1 = addresses[1]
	cfg.SMP3.Stream.ChunkSize = 1 << 20
	// Keep this disposable binding fixture independent of the production
	// activation threshold and timing boundary; the assertion is that both
	// configured SOCKS listeners carry the same logical session.
	cfg.SMP3.Stream.ActivationThresholdMbps = 1
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	session, err := newStreamSession(instance, echoListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.ensureLegOnce(1)
	attachDeadline := time.Now().Add(3 * time.Second)
	for !session.engine.Snapshot().LegUp[1] && time.Now().Before(attachDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !session.engine.Snapshot().LegUp[1] {
		t.Fatal("leg1 did not attach before integrity transfer")
	}

	localListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer localListener.Close()
	peer, err := net.Dial("tcp", localListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	local, err := localListener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	go session.run(local, bufio.NewReader(local))

	payload := make([]byte, 32*1024*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := peer.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	writeErr := make(chan error, 1)
	go func() {
		for offset := 0; offset < len(payload); {
			end := offset + 1*1024*1024
			if end > len(payload) {
				end = len(payload)
			}
			if err := writeAll(peer, payload[offset:end]); err != nil {
				writeErr <- err
				return
			}
			offset = end
			time.Sleep(35 * time.Millisecond)
		}
		writeErr <- nil
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	digestBytes := sha256.Sum256(got)
	expectedDigest := sha256.Sum256(payload)
	if digest := hex.EncodeToString(digestBytes[:]); digest != hex.EncodeToString(expectedDigest[:]) {
		t.Fatalf("32 MiB SHA256 = %s, want %s", digest, hex.EncodeToString(expectedDigest[:]))
	}
	prefixDigest := sha256.Sum256(got[:16*1024*1024])
	expectedPrefix := sha256.Sum256(payload[:16*1024*1024])
	if prefixDigest != expectedPrefix {
		t.Fatal("16 MiB deterministic prefix SHA256 mismatch")
	}

	stats := session.engine.Snapshot()
	if stats.TxAckedUsefulByLeg[0] == 0 {
		t.Fatalf("leg0 useful ACK contribution = %v", stats.TxAckedUsefulByLeg)
	}
	// This fixture qualifies deterministic listener binding, not scheduler
	// fairness. A short single-direction transfer may legitimately carry all
	// DATA on one attached leg while the companion still completes SMP3 attach.
	t.Logf("useful ACK contribution by leg = %v; listener binding is asserted below", stats.TxAckedUsefulByLeg)

	if targets := carrierA.waitForTargets(t, 1); targets[0] != addresses[1] {
		t.Fatalf("leg0 target = %v, want %s", targets, addresses[1])
	}
	if targets := carrierB.waitForTargets(t, 1); targets[0] != addresses[1] {
		t.Fatalf("leg1 target = %v, want %s", targets, addresses[1])
	}
	if carrierA.bytes.Load() <= 128 || carrierB.bytes.Load() <= 128 {
		t.Fatalf("carrier wire bytes = A:%d B:%d, want both carriers to carry data", carrierA.bytes.Load(), carrierB.bytes.Load())
	}
}

func TestPerLegCarrierFailureDoesNotCrossToOtherUpstream(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := closed.Addr().String()
	_ = closed.Close()

	carrierB := newSOCKSForwarder(t)
	defer carrierB.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = carrierB.Addr().String()
	cfg.UpstreamSocks.Leg0 = &UpstreamSocksOverride{Address: closedAddress, ConnectTimeout: Duration(100 * time.Millisecond)}
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	session := &streamSession{client: instance, id: id, destination: "127.0.0.1:24445", ctx: instance.ctx}
	if conn, err := session.dialLeg(0); err == nil {
		_ = conn.Close()
		t.Fatal("unavailable leg0 unexpectedly connected")
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		carrierB.mu.Lock()
		count := len(carrierB.targets)
		targets := append([]string(nil), carrierB.targets...)
		carrierB.mu.Unlock()
		if count != 0 {
			t.Fatalf("leg0 failure crossed to leg1 upstream: targets=%v", targets)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDatagramPerLegCarrierBindingUsesOverrides(t *testing.T) {
	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = true
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	addresses := standalone.ListenerAddrs()

	carrierA := newSOCKSForwarder(t)
	defer carrierA.Close()
	carrierB := newSOCKSForwarder(t)
	defer carrierB.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = carrierA.Addr().String()
	cfg.UpstreamSocks.Leg1 = &UpstreamSocksOverride{Address: carrierB.Addr().String()}
	cfg.SMP3.Routes.Leg0 = addresses[1]
	cfg.SMP3.Routes.Leg1 = addresses[1]
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	sid, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	association := &datagramAssociation{client: instance, ctx: instance.ctx}
	leg0, err := association.dialLeg(0, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer leg0.Close()
	leg1, err := association.dialLeg(1, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer leg1.Close()
	if targets := carrierA.waitForTargets(t, 1); targets[0] != addresses[1] {
		t.Fatalf("datagram leg0 target = %v, want %s", targets, addresses[1])
	}
	if targets := carrierB.waitForTargets(t, 1); targets[0] != addresses[1] {
		t.Fatalf("datagram leg1 target = %v, want %s", targets, addresses[1])
	}
}

//go:build standalone_q4

package client

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	serverpkg "github.com/Superbias/smp3-multipath-kit-public/server"
)

type q4LockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *q4LockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *q4LockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var q4SessionPattern = regexp.MustCompile(`session=([^ ]+)`)

func q4WaitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func q4StartEcho(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String()
}

func q4StartServer(t *testing.T, logBuffer *q4LockedBuffer) (*serverpkg.Server, string) {
	t.Helper()
	cfg := serverpkg.DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.SidecarListeners = []string{"127.0.0.1:0"}
	cfg.Password = "q4-disposable-password"
	cfg.UDP.Enabled = false
	cfg.Stream.QueueFrames = 64
	cfg.Stream.BandwidthMbps = []uint32{1, 1}
	instance, err := serverpkg.New(cfg, slog.New(slog.NewTextHandler(logBuffer, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		_ = instance.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	addresses := instance.ListenerAddrs()
	if len(addresses) != 2 {
		t.Fatalf("disposable server listeners = %v, want primary plus one sidecar", addresses)
	}
	return instance, addresses[1]
}

func q4ClientConfig(upstream, target string) Config {
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = upstream
	cfg.SMP3.Password = "q4-disposable-password"
	cfg.SMP3.Routes = RouteOptions{Leg0: target, Leg1: target}
	cfg.SMP3.Stream.ActivationThresholdMbps = 1
	cfg.SMP3.Stream.ActivationWindow = Duration(20 * time.Millisecond)
	cfg.SMP3.Stream.ChunkSize = 16 * 1024
	cfg.SMP3.Stream.QueueFrames = 64
	cfg.SMP3.Stream.BandwidthMbps = []uint32{1, 1}
	return cfg
}

func q4SessionLegs(logs string) (string, bool, bool) {
	var leg0, leg1 string
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "session=") {
			continue
		}
		match := q4SessionPattern.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		if strings.Contains(line, "multipath session created") && strings.Contains(line, "leg=0") {
			leg0 = match[1]
		}
		if strings.Contains(line, "multipath leg joined/rejoined") && strings.Contains(line, "leg=1") {
			leg1 = match[1]
		}
	}
	return leg0, leg0 != "", leg1 != "" && leg1 == leg0
}

func TestQ4SameListenerNormalActivationAndStreamIntegrity(t *testing.T) {
	logBuffer := new(q4LockedBuffer)
	_, sidecar := q4StartServer(t, logBuffer)
	target := q4StartEcho(t)
	forwarder := newSOCKSForwarder(t)
	cfg := q4ClientConfig(forwarder.Addr().String(), sidecar)
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	conn, err := net.Dial("tcp", instance.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := socksConnect(conn, target); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 16<<20)
	for i := range payload {
		payload[i] = byte((i*31 + 7) % 251)
	}
	want := sha256.Sum256(payload)
	if err := writeAll(conn, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != want {
		t.Fatalf("same-listener stream SHA256 mismatch: got %x want %x", sha256.Sum256(got), want)
	}

	targets := forwarder.waitForTargets(t, 2)
	if len(targets) != 2 || targets[0] != sidecar || targets[1] != sidecar {
		t.Fatalf("same-target CONNECT targets = %v, want exactly two %s", targets, sidecar)
	}
	q4WaitFor(t, 5*time.Second, func() bool {
		_, leg0, sameSession := q4SessionLegs(logBuffer.String())
		return leg0 && sameSession
	})
	_, leg0, sameSession := q4SessionLegs(logBuffer.String())
	if !leg0 || !sameSession {
		t.Fatalf("server did not record same-session leg0/leg1 attach: %s", logBuffer.String())
	}
}

func TestQ4SameTargetStressFiveHundredSessions(t *testing.T) {
	logBuffer := new(q4LockedBuffer)
	_, sidecar := q4StartServer(t, logBuffer)
	target := q4StartEcho(t)
	forwarder := newSOCKSForwarder(t)
	cfg := q4ClientConfig(forwarder.Addr().String(), sidecar)
	cfg.SMP3.Stream.ActivationWindow = Duration(5 * time.Millisecond)
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	const sessions = 500
	payload := bytes.Repeat([]byte("q4"), 4096)
	for i := 0; i < sessions; i++ {
		conn, err := net.Dial("tcp", instance.Addr().String())
		if err != nil {
			t.Fatalf("session %d local dial: %v", i, err)
		}
		if err := socksConnect(conn, target); err != nil {
			_ = conn.Close()
			t.Fatalf("session %d SOCKS CONNECT: %v", i, err)
		}
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		if err := writeAll(conn, payload); err != nil {
			_ = conn.Close()
			t.Fatalf("session %d write: %v", i, err)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			_ = conn.Close()
			t.Fatalf("session %d read: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			_ = conn.Close()
			t.Fatalf("session %d payload mismatch", i)
		}
		_ = conn.Close()
		q4WaitFor(t, 10*time.Second, func() bool {
			forwarder.mu.Lock()
			count := len(forwarder.targets)
			forwarder.mu.Unlock()
			return count >= 2*(i+1)
		})
	}

	forwarder.mu.Lock()
	targets := append([]string(nil), forwarder.targets...)
	forwarder.mu.Unlock()
	if len(targets) != 2*sessions {
		t.Fatalf("same-target CONNECT count = %d, want %d", len(targets), 2*sessions)
	}
	for i, got := range targets {
		if got != sidecar {
			t.Fatalf("CONNECT %d target = %s, want %s", i, got, sidecar)
		}
	}
}

func TestQ4Leg1FailureDoesNotUseFallback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requests := make(chan string, 2)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := newQ4SOCKSReader(conn)
				target, err := reader.readConnectTarget()
				if err != nil {
					return
				}
				requests <- target
				_ = writeAll(conn, []byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
			}()
		}
	}()

	cfg := q4ClientConfig(listener.Addr().String(), "127.0.0.1:24445")
	cfg.SMP3.Routes.Leg1Fallback = "127.0.0.1:24446"
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	var id [16]byte
	id[0] = 0x44
	session := &streamSession{client: instance, id: id, destination: "example.com:443", ctx: instance.ctx}
	if conn, err := session.dialLeg(0); err == nil {
		_ = conn.Close()
	} else {
		t.Fatalf("Leg0 direct dial unexpectedly failed: %v", err)
	}
	if _, err := session.dialLeg(1); err == nil {
		t.Fatal("Leg1 failure unexpectedly succeeded")
	}
	select {
	case target := <-requests:
		if target != "127.0.0.1:24445" {
			t.Fatalf("Leg0 target = %s", target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Leg0 SOCKS CONNECT not observed")
	}
	select {
	case target := <-requests:
		if target != "127.0.0.1:24445" {
			t.Fatalf("Leg1 target = %s", target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Leg1 SOCKS CONNECT not observed")
	}
	select {
	case extra := <-requests:
		t.Fatalf("unexpected fallback/retry target %s", extra)
	case <-time.After(150 * time.Millisecond):
	}
}

type q4SOCKSReader struct {
	conn  net.Conn
	reader *bufio.Reader
}

func newQ4SOCKSReader(conn net.Conn) *q4SOCKSReader {
	return &q4SOCKSReader{conn: conn, reader: bufio.NewReader(conn)}
}

func (r *q4SOCKSReader) readConnectTarget() (string, error) {
	var greeting [2]byte
	if _, err := io.ReadFull(r.reader, greeting[:]); err != nil {
		return "", err
	}
	methods := make([]byte, greeting[1])
	if _, err := io.ReadFull(r.reader, methods); err != nil {
		return "", err
	}
	if err := writeAll(r.conn, []byte{5, 0}); err != nil {
		return "", err
	}
	var request [4]byte
	if _, err := io.ReadFull(r.reader, request[:]); err != nil {
		return "", err
	}
	return decodeSocksAddressWithType(r.reader, request[3])
}

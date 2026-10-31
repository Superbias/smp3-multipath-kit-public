package client

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	serverpkg "github.com/Superbias/smp3-multipath-kit-public/server"
)

func TestStandaloneClientCloseTerminatesAppSocketAndReleasesSession(t *testing.T) {
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer targetListener.Close()
	targetAccepted := make(chan net.Conn, 1)
	targetClosed := make(chan error, 1)
	go func() {
		conn, acceptErr := targetListener.Accept()
		if acceptErr != nil {
			targetClosed <- acceptErr
			return
		}
		targetAccepted <- conn
		buffer := make([]byte, 32*1024)
		var readErr error
		for {
			n, err := conn.Read(buffer)
			if n > 0 {
				if _, writeErr := conn.Write(buffer[:n]); writeErr != nil {
					readErr = writeErr
					break
				}
			}
			if err != nil {
				readErr = err
				break
			}
		}
		_ = conn.Close()
		targetClosed <- readErr
	}()

	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0", "127.0.0.1:0"}
	serverConfig.Password = "forced-close-test"
	serverConfig.UDP.Enabled = false
	standaloneServer, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standaloneServer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Logf("standalone server started with carrier listeners %v", standaloneServer.ListenerAddrs())
	t.Logf("server recovery timeout %s", serverConfig.RecoveryTimeout.Time())
	defer standaloneServer.Close()
	serverAddresses := standaloneServer.ListenerAddrs()
	if len(serverAddresses) != 3 {
		t.Fatalf("server listeners = %v, want main plus two carrier listeners", serverAddresses)
	}

	carrier := newForcedCloseSOCKSForwarder(t)
	defer carrier.Close()
	config := validTestConfig()
	config.SMP3.Password = serverConfig.Password
	config.UpstreamSocks.Address = carrier.Addr().String()
	config.SMP3.Routes.Leg0 = serverAddresses[1]
	config.SMP3.Routes.Leg1 = serverAddresses[2]
	config.SMP3.Stream.ActivationThresholdMbps = 1
	config.SMP3.Stream.ActivationWindow = Duration(20 * time.Millisecond)
	config.SMP3.Stream.ChunkSize = 1024
	instance, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	t.Logf("standalone client started at %s", instance.Addr())

	appConn, err := net.Dial("tcp", instance.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer appConn.Close()
	if err := appConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := socksConnect(appConn, targetListener.Addr().String()); err != nil {
		t.Fatalf("local SOCKS CONNECT failed: %v; routes=%v", err, config.SMP3.Routes)
	}
	t.Log("local SOCKS CONNECT established")
	if err := appConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 256*1024)
	for index := range payload {
		payload[index] = byte(index)
	}
	if err := writeAll(appConn, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-targetAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("target did not accept the active stream")
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(appConn, received); err != nil {
		t.Fatalf("active stream did not echo payload before forced close: %v", err)
	}
	if string(received) != string(payload) {
		t.Fatal("target payload mismatch before forced close")
	}
	targets := carrier.waitForTargets(t, 2)
	seenTargets := map[string]bool{}
	for _, target := range targets {
		seenTargets[target] = true
	}
	if !seenTargets[serverAddresses[1]] || !seenTargets[serverAddresses[2]] {
		t.Fatalf("carrier targets = %v, want both server legs %v", targets, serverAddresses[1:])
	}
	waitForStandaloneSessions(t, standaloneServer, 1)

	closeStarted := time.Now()
	if err := instance.Close(); err != nil {
		t.Fatalf("Client.Close(): %v", err)
	}
	t.Logf("Client.Close() completed in %s", time.Since(closeStarted))

	if err := appConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var terminalErr error
	readBuf := make([]byte, 1)
	if n, readErr := appConn.Read(readBuf); n != 0 || readErr == nil {
		t.Fatalf("application read after Client.Close() = (%d, %v), want terminal socket result", n, readErr)
	} else {
		terminalErr = readErr
	}
	t.Logf("application-visible TCP termination after Client.Close(): %T: %v", terminalErr, terminalErr)

	targetDone := false
	var targetErr error
	targetClosedAt := time.Time{}
	deadline := time.NewTimer(20 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for !targetDone || standaloneServer.SessionCount() != 0 {
		select {
		case targetErr = <-targetClosed:
			targetDone = true
			targetClosedAt = time.Now()
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("forced-close cleanup timed out: target_closed=%t server_sessions=%d", targetDone, standaloneServer.SessionCount())
		}
	}
	t.Logf("target-side termination after %s: %T: %v", targetClosedAt.Sub(closeStarted), targetErr, targetErr)
	if count := standaloneServer.SessionCount(); count != 0 {
		t.Fatalf("server active session count after forced close = %d, want 0", count)
	}
	instance.mu.Lock()
	trackedConnections := len(instance.conns)
	instance.mu.Unlock()
	if trackedConnections != 0 {
		t.Fatalf("client tracked connections after Close() = %d, want 0", trackedConnections)
	}
}

func waitForStandaloneSessions(t *testing.T, instance *serverpkg.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := instance.SessionCount(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server active sessions = %d, want %d", instance.SessionCount(), want)
}

// forcedCloseSOCKSForwarder models an underlying Carrier that fully closes
// both directions when either endpoint closes. The general SOCKS test helper
// intentionally waits for both copies and therefore does not model teardown.
type forcedCloseSOCKSForwarder struct {
	listener net.Listener
	mu       sync.Mutex
	closed   bool
	active   map[net.Conn]struct{}
	targets  []string
	wg       sync.WaitGroup
}

func newForcedCloseSOCKSForwarder(t *testing.T) *forcedCloseSOCKSForwarder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	forwarder := &forcedCloseSOCKSForwarder{listener: listener, active: make(map[net.Conn]struct{})}
	go forwarder.acceptLoop()
	return forwarder
}

func (f *forcedCloseSOCKSForwarder) Addr() net.Addr { return f.listener.Addr() }

func (f *forcedCloseSOCKSForwarder) Close() {
	f.mu.Lock()
	f.closed = true
	active := make([]net.Conn, 0, len(f.active))
	for conn := range f.active {
		active = append(active, conn)
	}
	f.mu.Unlock()
	_ = f.listener.Close()
	for _, conn := range active {
		_ = conn.Close()
	}
	f.wg.Wait()
}

func (f *forcedCloseSOCKSForwarder) waitForTargets(t *testing.T, count int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		targets := append([]string(nil), f.targets...)
		f.mu.Unlock()
		if len(targets) >= count {
			return targets
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t.Fatalf("forced-close carrier targets = %v, want at least %d", f.targets, count)
	return nil
}

func (f *forcedCloseSOCKSForwarder) acceptLoop() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			_ = conn.Close()
			return
		}
		f.active[conn] = struct{}{}
		f.wg.Add(1)
		f.mu.Unlock()
		go f.handle(conn)
	}
}

func (f *forcedCloseSOCKSForwarder) track(conn net.Conn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	f.active[conn] = struct{}{}
	return true
}

func (f *forcedCloseSOCKSForwarder) untrack(conn net.Conn) {
	f.mu.Lock()
	delete(f.active, conn)
	f.mu.Unlock()
}

func (f *forcedCloseSOCKSForwarder) handle(conn net.Conn) {
	defer f.wg.Done()
	defer f.untrack(conn)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var greeting [2]byte
	if _, err := io.ReadFull(reader, greeting[:]); err != nil {
		return
	}
	methods := make([]byte, greeting[1])
	if _, err := io.ReadFull(reader, methods); err != nil {
		return
	}
	if err := writeAll(conn, []byte{5, 0}); err != nil {
		return
	}
	var request [4]byte
	if _, err := io.ReadFull(reader, request[:]); err != nil {
		return
	}
	target, err := decodeSocksAddressWithType(reader, request[3])
	if err != nil {
		return
	}
	f.mu.Lock()
	f.targets = append(f.targets, target)
	f.mu.Unlock()
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		_ = writeAll(conn, []byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	if !f.track(upstream) {
		_ = upstream.Close()
		return
	}
	defer f.untrack(upstream)
	defer upstream.Close()
	if err := writeAll(conn, []byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, reader)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(conn, upstream)
		done <- struct{}{}
	}()
	<-done
	_ = conn.Close()
	_ = upstream.Close()
	<-done
}

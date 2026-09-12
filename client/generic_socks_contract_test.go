package client

import (
	"context"
	"net"
	"testing"
	"time"
)

// sameProcessSOCKSManager models one external proxy process that owns two
// deterministic SOCKS5 listeners. The SMP3 client sees only the two listener
// addresses and does not need to know that the listeners share an owner.
type sameProcessSOCKSManager struct {
	leg0 *socksForwarder
	leg1 *socksForwarder
}

func newSameProcessSOCKSManager(t *testing.T) *sameProcessSOCKSManager {
	t.Helper()
	return &sameProcessSOCKSManager{
		leg0: newSOCKSForwarder(t),
		leg1: newSOCKSForwarder(t),
	}
}

func (m *sameProcessSOCKSManager) Close() {
	m.leg0.Close()
	m.leg1.Close()
}

func TestGenericSOCKSSameProcessMultiListenerBinding(t *testing.T) {
	manager := newSameProcessSOCKSManager(t)
	defer manager.Close()

	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer targetListener.Close()
	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			conn, acceptErr := targetListener.Accept()
			if acceptErr != nil {
				return
			}
			accepted <- conn
		}
	}()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = manager.leg0.Addr().String()
	cfg.UpstreamSocks.Leg1 = &UpstreamSocksOverride{Address: manager.leg1.Addr().String()}
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	leg0Conn, err := dialUpstream(ctx, cfg.effectiveUpstream(0), targetListener.Addr().String())
	if err != nil {
		t.Fatalf("leg0 generic SOCKS CONNECT: %v", err)
	}
	defer leg0Conn.Close()
	leg1Conn, err := dialUpstream(ctx, cfg.effectiveUpstream(1), targetListener.Addr().String())
	if err != nil {
		t.Fatalf("leg1 generic SOCKS CONNECT: %v", err)
	}
	defer leg1Conn.Close()

	for range 2 {
		select {
		case conn := <-accepted:
			_ = conn.Close()
		case <-ctx.Done():
			t.Fatal("same-process SOCKS listeners did not reach the target")
		}
	}

	leg0Targets := manager.leg0.waitForTargets(t, 1)
	leg1Targets := manager.leg1.waitForTargets(t, 1)
	if len(leg0Targets) != 1 || leg0Targets[0] != targetListener.Addr().String() {
		t.Fatalf("SOCKS-A targets = %v, want only %s", leg0Targets, targetListener.Addr())
	}
	if len(leg1Targets) != 1 || leg1Targets[0] != targetListener.Addr().String() {
		t.Fatalf("SOCKS-B targets = %v, want only %s", leg1Targets, targetListener.Addr())
	}

	// The manager has two listener identities. A successful connection on each
	// listener is the deterministic evidence that the configured leg binding did
	// not collapse both legs onto one external endpoint.
}

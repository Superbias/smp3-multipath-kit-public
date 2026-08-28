package multipath

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func newTestInboundForSessionCreation() *Inbound {
	cfg := testCoreConfig()
	// Keep tests focused on session creation/join. A positive threshold avoids
	// immediate activation callbacks before a logger is installed.
	cfg.ThresholdBytesPS = 1 << 60
	return &Inbound{
		sessions:   make(map[[16]byte]*serverSession),
		tombstones: make(map[[16]byte]time.Time),
		cfg:        cfg,
	}
}

func TestInboundSecondaryCanCreateSessionBeforePrimary(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	var id [16]byte
	id[0] = 0x42
	destination := M.ParseSocksaddr("example.com:443")

	leg1, peer1 := net.Pipe()
	defer peer1.Close()
	hello1 := helloMessage{Session: id, LegID: 1, Destination: destination.String()}
	session, created, err := inbound.createOrJoinSession(context.Background(), hello1, destination, leg1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.streamCore.Close()
	defer session.streamConn.Close()
	if !created {
		t.Fatal("secondary first arrival did not create session")
	}
	if !session.streamCore.hasLeg(1) || session.streamCore.hasLeg(0) {
		t.Fatalf("unexpected initial legs: leg0=%v leg1=%v", session.streamCore.hasLeg(0), session.streamCore.hasLeg(1))
	}

	leg0, peer0 := net.Pipe()
	defer peer0.Close()
	hello0 := helloMessage{Session: id, LegID: 0, Destination: destination.String()}
	joined, created, err := inbound.createOrJoinSession(context.Background(), hello0, destination, leg0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("primary arrival created a second session")
	}
	if joined != session {
		t.Fatalf("primary joined wrong session: got=%p want=%p", joined, session)
	}
	if !session.streamCore.hasLeg(0) || !session.streamCore.hasLeg(1) {
		t.Fatalf("both legs not present after join: leg0=%v leg1=%v", session.streamCore.hasLeg(0), session.streamCore.hasLeg(1))
	}
}

func TestInboundConcurrentFirstLegsCreateExactlyOneSession(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	var id [16]byte
	id[0] = 0x99
	destination := M.ParseSocksaddr("speed.cloudflare.com:443")

	type result struct {
		session *serverSession
		created bool
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var peers []net.Conn
	var peersMu sync.Mutex
	for legID := uint8(0); legID < 2; legID++ {
		legID := legID
		go func() {
			conn, peer := net.Pipe()
			peersMu.Lock()
			peers = append(peers, peer)
			peersMu.Unlock()
			<-start
			hello := helloMessage{Session: id, LegID: legID, Destination: destination.String()}
			session, created, err := inbound.createOrJoinSession(context.Background(), hello, destination, conn, nil)
			results <- result{session: session, created: created, err: err}
		}()
	}
	close(start)

	var got [2]result
	for idx := range got {
		select {
		case got[idx] = <-results:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent create/join timed out")
		}
		if got[idx].err != nil {
			t.Fatal(got[idx].err)
		}
	}
	defer func() {
		peersMu.Lock()
		defer peersMu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()

	createdCount := 0
	for _, item := range got {
		if item.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("expected exactly one creator, got %d", createdCount)
	}
	if got[0].session == nil || got[1].session == nil || got[0].session != got[1].session {
		t.Fatalf("concurrent legs did not converge on one session: %p %p", got[0].session, got[1].session)
	}
	session := got[0].session
	defer session.streamCore.Close()
	defer session.streamConn.Close()
	if !session.streamCore.hasLeg(0) || !session.streamCore.hasLeg(1) {
		t.Fatalf("concurrent first legs not both attached: leg0=%v leg1=%v", session.streamCore.hasLeg(0), session.streamCore.hasLeg(1))
	}
}

func TestServerSessionRejectsLiveDuplicateLegJoin(t *testing.T) {
	core, appConn := newCore(testCoreConfig())
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	defer b0.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}

	a1, b1 := net.Pipe()
	defer b1.Close()
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}

	session := &serverSession{mode: helloModeStream, streamCore: core}
	replacement, peer := net.Pipe()
	defer peer.Close()
	defer replacement.Close()
	if err := session.attachLeg(1, replacement, nil); err == nil || err.Error() != "duplicate multipath leg" {
		t.Fatalf("expected duplicate leg rejection, got %v", err)
	}
	if !core.hasLeg(0) || !core.hasLeg(1) {
		t.Fatal("duplicate join disturbed existing logical session")
	}
}

func TestInboundCompletedSessionTombstoneRejectsLateLeg(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	var id [16]byte
	id[0] = 0x77
	destination := M.ParseSocksaddr("example.com:443")

	leg0, peer0 := net.Pipe()
	defer peer0.Close()
	session, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 0, Destination: destination.String(),
	}, destination, leg0, nil)
	if err != nil || !created {
		t.Fatalf("create session: created=%v err=%v", created, err)
	}
	_ = session.streamCore.Close()

	deadline := time.Now().Add(time.Second)
	for {
		inbound.access.Lock()
		_, active := inbound.sessions[id]
		expires, retired := inbound.tombstones[id]
		inbound.access.Unlock()
		if !active && retired && time.Now().Before(expires) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed session was not tombstoned")
		}
		time.Sleep(time.Millisecond)
	}

	late, latePeer := net.Pipe()
	defer late.Close()
	defer latePeer.Close()
	got, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 1, Destination: destination.String(),
	}, destination, late, nil)
	if err == nil || err.Error() != "multipath session id is retired" {
		t.Fatalf("late old-session leg was not rejected by tombstone: %v", err)
	}
	if created || got != nil {
		t.Fatalf("late leg resurrected completed session: created=%v session=%p", created, got)
	}
}

func TestInboundExpiredTombstoneAllowsFreshCreate(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	var id [16]byte
	id[0] = 0x78
	inbound.tombstones[id] = time.Now().Add(-time.Second)
	destination := M.ParseSocksaddr("example.net:443")

	leg, peer := net.Pipe()
	defer peer.Close()
	session, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 1, Destination: destination.String(),
	}, destination, leg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.streamCore.Close()
	defer session.streamConn.Close()
	if !created {
		t.Fatal("expired tombstone blocked a fresh create")
	}
}

func TestInboundClosedRejectsNewSession(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	inbound.access.Lock()
	inbound.closed = true
	inbound.access.Unlock()
	var id [16]byte
	id[0] = 0x79
	destination := M.ParseSocksaddr("example.org:443")
	leg, peer := net.Pipe()
	defer leg.Close()
	defer peer.Close()
	got, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 0, Destination: destination.String(),
	}, destination, leg, nil)
	if !errors.Is(err, errCoreClosed) {
		t.Fatalf("closed inbound accepted new session: %v", err)
	}
	if created || got != nil {
		t.Fatalf("closed inbound created session: created=%v session=%p", created, got)
	}
}

func TestInboundDatagramSessionRejectsStreamModeJoin(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	inbound.udpEnabled = true
	inbound.udpCfg = datagramConfig{Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64, IdleTimeout: time.Minute, RecoveryTimeout: time.Second}
	var id [16]byte
	id[0] = 0x81
	destination := M.ParseSocksaddr("192.0.2.1:53")

	leg1, peer1 := net.Pipe()
	defer peer1.Close()
	session, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 1, Mode: helloModeDatagram, Destination: destination.String(),
	}, destination, leg1, nil)
	if err != nil || !created {
		t.Fatalf("create datagram session: created=%v err=%v", created, err)
	}
	defer session.datagramCore.Close()

	streamLeg, streamPeer := net.Pipe()
	defer streamLeg.Close()
	defer streamPeer.Close()
	_, created, err = inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 0, Mode: helloModeStream, Destination: destination.String(),
	}, destination, streamLeg, nil)
	if err == nil || err.Error() != "multipath session mode mismatch" {
		t.Fatalf("cross-mode join was not rejected: %v", err)
	}
	if created {
		t.Fatal("cross-mode join created a second session")
	}
}

func TestInboundDatagramSecondLegJoinsSameSession(t *testing.T) {
	inbound := newTestInboundForSessionCreation()
	inbound.udpEnabled = true
	inbound.udpCfg = datagramConfig{Mode: datagramModeAdaptive, QueueFrames: 16, DedupWindow: 64, IdleTimeout: time.Minute, RecoveryTimeout: time.Second}
	var id [16]byte
	id[0] = 0x82
	destination := M.ParseSocksaddr("203.0.113.9:53")

	leg0, peer0 := net.Pipe()
	defer peer0.Close()
	session, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 0, Mode: helloModeDatagram, Destination: destination.String(),
	}, destination, leg0, nil)
	if err != nil || !created {
		t.Fatalf("create datagram session: created=%v err=%v", created, err)
	}
	defer session.datagramCore.Close()

	leg1, peer1 := net.Pipe()
	defer peer1.Close()
	joined, created, err := inbound.createOrJoinSession(context.Background(), helloMessage{
		Session: id, LegID: 1, Mode: helloModeDatagram, Destination: destination.String(),
	}, destination, leg1, nil)
	if err != nil || created || joined != session {
		t.Fatalf("datagram join failed: created=%v same=%v err=%v", created, joined == session, err)
	}
	if !session.datagramCore.hasLeg(0) || !session.datagramCore.hasLeg(1) {
		t.Fatal("datagram session does not contain both legs")
	}
}

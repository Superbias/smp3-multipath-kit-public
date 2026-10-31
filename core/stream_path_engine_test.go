package smp3core

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestStreamPathRTTIsDisabledByDefault(t *testing.T) {
	engine, _ := NewStreamEngine(StreamConfig{})
	t.Cleanup(func() { _ = engine.Close() })
	if engine.cfg.ExperimentalPathAware || engine.pathRTTCache != nil {
		t.Fatal("path-aware RTT initialized without explicit opt-in")
	}
	var sessionID SessionID
	identity := StreamPathBinding{SessionID: sessionID, LegID: 0, CarrierID: "disabled", Generation: 1}
	if err := engine.AttachLegWithPathAuth(0, nil, nil, testStreamPathAuth(identity, [32]byte{1})); err != ErrStreamPathNegotiation {
		t.Fatalf("disabled path-aware attach error=%v, want negotiation disabled", err)
	}
}

func TestStreamEngineSameCarrierRTTProbeAndGenerationReplacement(t *testing.T) {
	var sessionID SessionID
	sessionID[0] = 0x71
	clientFailures := make(chan error, 8)
	serverFailures := make(chan error, 8)
	client, _ := NewStreamEngine(StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleClient})
	server, _ := NewStreamEngine(StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleServer})
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	clientAuth, serverAuth := testPathAuthPair(t, sessionID, 0, 1)
	clientConn, serverConn := net.Pipe()
	if err := client.AttachLegWithPathAuth(0, delayedPathTestLeg{Conn: clientConn, delay: time.Millisecond}, func(err error) { clientFailures <- err }, clientAuth); err != nil {
		t.Fatal(err)
	}
	if err := server.AttachLegWithPathAuth(0, delayedPathTestLeg{Conn: serverConn, delay: time.Millisecond}, func(err error) { serverFailures <- err }, serverAuth); err != nil {
		t.Fatal(err)
	}
	clientLeg := client.getLeg(0)
	serverLeg := server.getLeg(0)
	waitForEnginePathFeedback(t, client, clientLeg, server, clientFailures)
	waitForEnginePathFeedback(t, server, serverLeg, client, serverFailures)
	if clientLeg.pathIdentity.Direction != StreamRTTClientToServer || serverLeg.pathIdentity.Direction != StreamRTTServerToClient {
		t.Fatalf("endpoint directions disagree: client=%v server=%v", clientLeg.pathIdentity.Direction, serverLeg.pathIdentity.Direction)
	}

	oldIdentity := clientLeg.pathIdentity
	if !client.ReplaceLeg(0, net.ErrClosed) {
		t.Fatal("client leg replacement did not retire old generation")
	}
	waitForPathFeedbackGone(t, client.pathRTTCache, oldIdentity)
	waitForPathFeedbackGone(t, server.pathRTTCache, serverLeg.pathIdentity)

	clientAuth, serverAuth = testPathAuthPair(t, sessionID, 0, 2)
	clientConn, serverConn = net.Pipe()
	if err := client.AttachLegWithPathAuth(0, delayedPathTestLeg{Conn: clientConn, delay: time.Millisecond}, func(err error) { clientFailures <- err }, clientAuth); err != nil {
		t.Fatalf("attach client replacement: %v", err)
	}
	if err := server.AttachLegWithPathAuth(0, delayedPathTestLeg{Conn: serverConn, delay: time.Millisecond}, func(err error) { serverFailures <- err }, serverAuth); err != nil {
		t.Fatalf("attach server replacement: %v", err)
	}
	clientLeg = client.getLeg(0)
	serverLeg = server.getLeg(0)
	if clientLeg.generation <= oldIdentity.Generation || clientLeg.pathIdentity.CarrierID == oldIdentity.CarrierID {
		t.Fatalf("replacement reused old identity: old=%+v new=%+v", oldIdentity, clientLeg.pathIdentity)
	}
	waitForEnginePathFeedback(t, client, clientLeg, server, clientFailures)
	waitForEnginePathFeedback(t, server, serverLeg, client, serverFailures)
	if _, ok := client.pathRTTCache.PathFeedback(oldIdentity); ok {
		t.Fatal("old generation feedback survived Carrier replacement")
	}
}

func TestStreamServerFirstResponseDoesNotWaitForServerToClientRTTFeedback(t *testing.T) {
	var sessionID SessionID
	sessionID[0] = 0x73
	clientFailures := make(chan error, 8)
	serverFailures := make(chan error, 8)
	clientCfg := StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleClient, SchedulerMode: StreamSchedulerAggregation, BandwidthMbps: []uint32{100, 100}}
	serverCfg := clientCfg
	serverCfg.PathRole = StreamPathRoleServer
	client, clientApp := NewStreamEngine(clientCfg)
	server, serverApp := NewStreamEngine(serverCfg)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	var serverProbeGate [2]*streamPathQOEFrameGate
	for id := 0; id < 2; id++ {
		clientAuth, serverAuth := testPathAuthPair(t, sessionID, LegID(id), byte(0x40+id))
		clientConn, serverConn := net.Pipe()
		serverProbeGate[id] = newStreamPathQOEProbeGate(serverConn)
		if err := client.AttachLegWithPathAuth(LegID(id), delayedPathTestLeg{Conn: clientConn, delay: time.Millisecond}, func(err error) { clientFailures <- err }, clientAuth); err != nil {
			t.Fatalf("attach client leg%d: %v", id, err)
		}
		if err := server.AttachLegWithPathAuth(LegID(id), serverProbeGate[id], func(err error) { serverFailures <- err }, serverAuth); err != nil {
			t.Fatalf("attach server leg%d: %v", id, err)
		}
	}

	// Hold only the server's RTT probes. Client-to-server samples may become
	// ready, but the server cannot publish a reverse-direction sample yet.
	for id := 0; id < 2; id++ {
		if !serverProbeGate[id].waitForHeld(1, 2*time.Second) {
			t.Fatalf("server leg%d did not produce an RTT probe", id)
		}
	}
	for id := 0; id < 2; id++ {
		serverLeg := server.getLeg(uint8(id))
		if feedback, ok := server.PathFeedback(LegID(id)); ok {
			t.Fatalf("server leg%d published RTT before its same-Carrier echo was delivered: %+v", id, feedback)
		}
		if serverLeg.pathIdentity.Direction != StreamRTTServerToClient {
			t.Fatalf("server leg%d has wrong local RTT direction: %+v", id, serverLeg.pathIdentity)
		}
	}

	payload := bytes.Repeat([]byte("server-first-response/"), (100<<10)/len("server-first-response/")+1)
	payload = payload[:100<<10]
	_ = serverApp.SetDeadline(time.Now().Add(3 * time.Second))
	_ = clientApp.SetDeadline(time.Now().Add(3 * time.Second))
	written := make(chan error, 1)
	go func() { written <- writeAll(serverApp, payload) }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(clientApp, got); err != nil {
		t.Fatalf("read first server response without reverse RTT feedback: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("write first server response without reverse RTT feedback: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("first server response payload mismatch")
	}
	for id := 0; id < 2; id++ {
		if _, ok := server.PathFeedback(LegID(id)); ok {
			t.Fatalf("server leg%d acquired feedback while client echoes were held", id)
		}
	}

	for id := 0; id < 2; id++ {
		if err := serverProbeGate[id].release(); err != nil {
			t.Fatalf("release server leg%d RTT probes: %v", id, err)
		}
	}
	for id := 0; id < 2; id++ {
		serverLeg := server.getLeg(uint8(id))
		serverFeedback := waitForEnginePathFeedback(t, server, serverLeg, client, serverFailures)
		if serverFeedback.Identity.Direction != StreamRTTServerToClient {
			t.Fatalf("server response sample has wrong direction: %+v", serverFeedback.Identity)
		}
		if serverFeedback.Identity.Generation != serverLeg.generation || serverFeedback.Identity.CarrierID != serverLeg.pathIdentity.CarrierID {
			t.Fatalf("server response feedback is not bound to its active Carrier generation: feedback=%+v leg=%+v", serverFeedback.Identity, serverLeg.pathIdentity)
		}
	}
	t.Logf("server_first_response_bytes=%d completed_while_server_to_client_feedback_missing=true", len(got))
}

func TestStreamEngineSeedsFreshClientFeedbackFromAuthenticatedCarrierHandshake(t *testing.T) {
	var sessionID SessionID
	sessionID[0] = 0x72
	clientAuth, _ := testPathAuthPair(t, sessionID, 0, 0x31)
	clientAuth, err := clientAuth.WithCarrierHandshakeRTT(400 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := NewStreamEngine(StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleClient})
	peer, attached := net.Pipe()
	t.Cleanup(func() {
		_ = engine.Close()
		_ = peer.Close()
	})
	if err := engine.AttachLegWithPathAuth(0, attached, nil, clientAuth); err != nil {
		t.Fatal(err)
	}
	leg := engine.getLeg(0)
	feedback, ok := engine.pathRTTCache.PathFeedback(leg.pathIdentity)
	if !ok || feedback.RTT != 400*time.Millisecond || feedback.Samples != 1 {
		t.Fatalf("fresh handshake RTT was not seeded into current Carrier: feedback=%+v ok=%v", feedback, ok)
	}
	if feedback.Identity.Generation != leg.generation || feedback.Identity.Direction != StreamRTTClientToServer {
		t.Fatalf("seeded feedback is not bound to active client Carrier: feedback=%+v leg=%+v", feedback.Identity, leg.pathIdentity)
	}
	oldIdentity := leg.pathIdentity
	if !engine.ReplaceLeg(0, net.ErrClosed) {
		t.Fatal("seeded Carrier did not enter replacement")
	}
	waitForPathFeedbackGone(t, engine.pathRTTCache, oldIdentity)
	newPeer, newAttached := net.Pipe()
	defer newPeer.Close()
	newAuth, _ := testPathAuthPair(t, sessionID, 0, 0x33)
	newAuth, err = newAuth.WithCarrierHandshakeRTT(100 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AttachLegWithPathAuth(0, newAttached, nil, newAuth); err != nil {
		t.Fatalf("attach replacement Carrier: %v", err)
	}
	newLeg := engine.getLeg(0)
	newFeedback, ok := engine.pathRTTCache.PathFeedback(newLeg.pathIdentity)
	if !ok || newLeg.generation <= oldIdentity.Generation || newFeedback.RTT != 100*time.Millisecond || newFeedback.Identity.CarrierID == oldIdentity.CarrierID {
		t.Fatalf("replacement reused stale handshake feedback: old=%+v new=%+v feedback=%+v ok=%v", oldIdentity, newLeg.pathIdentity, newFeedback, ok)
	}

	// A server-side READY write is not an RTT observation, even if a caller
	// accidentally attaches a duration to the opaque auth capability.
	_, serverAuth := testPathAuthPair(t, sessionID, 1, 0x32)
	serverAuth, err = serverAuth.WithCarrierHandshakeRTT(100 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	serverEngine, _ := NewStreamEngine(StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleServer})
	serverPeer, serverAttached := net.Pipe()
	t.Cleanup(func() {
		_ = serverEngine.Close()
		_ = serverPeer.Close()
	})
	if err := serverEngine.AttachLegWithPathAuth(1, serverAttached, nil, serverAuth); err != nil {
		t.Fatal(err)
	}
	serverLeg := serverEngine.getLeg(1)
	if _, ok := serverEngine.pathRTTCache.PathFeedback(serverLeg.pathIdentity); ok {
		t.Fatal("server READY write duration was published as a round-trip sample")
	}

	staleAuth, _ := testPathAuthPair(t, sessionID, 0, 0x34)
	staleAuth, err = staleAuth.WithCarrierHandshakeRTT(200 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	staleAuth.handshakeAt = time.Now().Add(-StreamRTTSampleWindow - time.Second)
	staleEngine, _ := NewStreamEngine(StreamConfig{ExperimentalPathAware: true, PathRole: StreamPathRoleClient})
	stalePeer, staleAttached := net.Pipe()
	t.Cleanup(func() {
		_ = staleEngine.Close()
		_ = stalePeer.Close()
	})
	if err := staleEngine.AttachLegWithPathAuth(0, staleAttached, nil, staleAuth); err != nil {
		t.Fatal(err)
	}
	if _, ok := staleEngine.PathFeedback(0); ok {
		t.Fatal("expired HELLO/READY RTT sample survived the freshness window")
	}
}

func TestPathAwareColdStartBiasNeedsFreshFeedbackAndPreservesSlowLegPolicy(t *testing.T) {
	engine, peers := newPathAwareSchedulerFixture(t, true, StreamSchedulerStatic, StreamCapacityFixed)
	t.Cleanup(func() {
		_ = engine.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	legs := [2]*streamLeg{engine.getLeg(0), engine.getLeg(1)}
	base := time.Now().Add(-time.Second)
	for i, rtt := range []time.Duration{20 * time.Millisecond, 400 * time.Millisecond} {
		if err := recordStreamRTTSample(engine.pathRTTCache, legs[i].pathIdentity, engine.pathRTTCache.probeKey[i], base, rtt); err != nil {
			t.Fatalf("record leg%d RTT: %v", i, err)
		}
	}
	if got0 := engine.pathRTTScorePenalty(legs[0]); got0 <= 0 {
		t.Fatalf("leg0 path RTT penalty missing: %f", got0)
	}
	if got1 := engine.pathRTTScorePenalty(legs[1]); got1 <= engine.pathRTTScorePenalty(legs[0]) {
		t.Fatalf("slow path penalty did not exceed fast path: fast=%f slow=%f", engine.pathRTTScorePenalty(legs[0]), got1)
	}
	low, high := selectPathAwareFrames(engine, 16)
	if low != 16 || high != 0 {
		t.Fatalf("20/400ms cold start selected low=%d high=%d; want 16/0", low, high)
	}

	baselineEngine, baselinePeers := newPathAwareSchedulerFixture(t, false, StreamSchedulerStatic, StreamCapacityFixed)
	t.Cleanup(func() {
		_ = baselineEngine.Close()
		for _, peer := range baselinePeers {
			_ = peer.Close()
		}
	})
	baseline0, baseline1 := selectPathAwareFrames(baselineEngine, 16)
	if baseline0 < 7 || baseline0 > 9 || baseline1 < 7 || baseline1 > 9 {
		t.Fatalf("disabled experiment changed baseline allocation: %d/%d", baseline0, baseline1)
	}

	slowEngine, slowPeers := newPathAwareSchedulerFixture(t, true, StreamSchedulerAggregation, StreamCapacityDynamic)
	t.Cleanup(func() {
		_ = slowEngine.Close()
		for _, peer := range slowPeers {
			_ = peer.Close()
		}
	})
	slowLegs := [2]*streamLeg{slowEngine.getLeg(0), slowEngine.getLeg(1)}
	slowBase := time.Now().Add(-time.Second)
	for i, rtt := range []time.Duration{20 * time.Millisecond, 400 * time.Millisecond} {
		if err := recordStreamRTTSample(slowEngine.pathRTTCache, slowLegs[i].pathIdentity, slowEngine.pathRTTCache.probeKey[i], slowBase, rtt); err != nil {
			t.Fatalf("record slow-policy leg%d RTT: %v", i, err)
		}
	}
	slowEngine.admission.mu.Lock()
	slowEngine.admission.legs[1].restricted = true
	slowEngine.admission.mu.Unlock()
	if penalty := slowEngine.pathRTTScorePenalty(slowLegs[0]); penalty != 0 {
		t.Fatalf("path-aware penalty modified an active slow-leg restriction: %f", penalty)
	}
}

func TestPathAware20ms20msKeepsWeightedAggregation(t *testing.T) {
	engine, peers := newPathAwareSchedulerFixture(t, true, StreamSchedulerStatic, StreamCapacityFixed)
	t.Cleanup(func() {
		_ = engine.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	legs := [2]*streamLeg{engine.getLeg(0), engine.getLeg(1)}
	base := time.Now().Add(-time.Second)
	for i := range legs {
		if err := recordStreamRTTSample(engine.pathRTTCache, legs[i].pathIdentity, engine.pathRTTCache.probeKey[i], base, 20*time.Millisecond); err != nil {
			t.Fatalf("record leg%d RTT: %v", i, err)
		}
	}
	leg0, leg1 := selectPathAwareFrames(engine, 30)
	if leg0 < 14 || leg0 > 16 || leg1 < 14 || leg1 > 16 {
		t.Fatalf("equal-RTT healthy legs lost weighted aggregation: %d/%d", leg0, leg1)
	}
}

func TestPathAwareAggregationAppliesRTTFeedbackToVirtualServiceAssignment(t *testing.T) {
	engine, peers := newPathAwareSchedulerFixture(t, true, StreamSchedulerAggregation, StreamCapacityFixed)
	t.Cleanup(func() {
		_ = engine.Close()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	legs := [2]*streamLeg{engine.getLeg(0), engine.getLeg(1)}
	base := time.Now().Add(-time.Second)
	for i, rtt := range []time.Duration{20 * time.Millisecond, 400 * time.Millisecond} {
		if err := recordStreamRTTSample(engine.pathRTTCache, legs[i].pathIdentity, engine.pathRTTCache.probeKey[i], base, rtt); err != nil {
			t.Fatalf("record aggregation leg%d RTT: %v", i, err)
		}
	}
	low, high := selectPathAwareAssignedFrames(engine, 16)
	if low != 16 || high != 0 {
		t.Fatalf("20/400ms assigned-ready cold start selected low=%d high=%d; want 16/0", low, high)
	}

	equal, equalPeers := newPathAwareSchedulerFixture(t, true, StreamSchedulerAggregation, StreamCapacityFixed)
	t.Cleanup(func() {
		_ = equal.Close()
		for _, peer := range equalPeers {
			_ = peer.Close()
		}
	})
	equalLegs := [2]*streamLeg{equal.getLeg(0), equal.getLeg(1)}
	for i := range equalLegs {
		if err := recordStreamRTTSample(equal.pathRTTCache, equalLegs[i].pathIdentity, equal.pathRTTCache.probeKey[i], base, 20*time.Millisecond); err != nil {
			t.Fatalf("record equal aggregation leg%d RTT: %v", i, err)
		}
	}
	low, high = selectPathAwareAssignedFrames(equal, 30)
	if low < 14 || low > 16 || high < 14 || high > 16 {
		t.Fatalf("20/20ms assigned-ready healthy aggregation changed: %d/%d", low, high)
	}
}

func newPathAwareSchedulerFixture(t *testing.T, enabled bool, scheduler StreamSchedulerMode, capacity StreamCapacityMode) (*StreamEngine, []net.Conn) {
	t.Helper()
	engine, _ := NewStreamEngine(StreamConfig{
		ExperimentalPathAware: enabled,
		PathRole:              StreamPathRoleClient,
		SchedulerMode:         scheduler,
		CapacityMode:          capacity,
		ChunkSize:             64 * 1024,
		QueueFrames:           256,
		BandwidthMbps:         []uint32{100, 100},
	})
	var sessionID SessionID
	sessionID[0] = 0x72
	peers := make([]net.Conn, 0, 2)
	for id := uint8(0); id < 2; id++ {
		local, peer := net.Pipe()
		peers = append(peers, peer)
		binding := StreamPathBinding{
			SessionID: sessionID, LegID: LegID(id), CarrierID: string(rune('a' + id)), Generation: 1,
		}
		key := [32]byte{byte(id + 1), 0xa5}
		if enabled {
			if err := engine.pathRTTCache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
				t.Fatalf("bind leg %d: %v", id, err)
			}
		}
		identity := StreamPathIdentity{
			SessionID: sessionID, LegID: LegID(id), CarrierID: binding.CarrierID,
			Generation: binding.Generation, Direction: StreamRTTClientToServer,
		}
		engine.legsMu.Lock()
		engine.legs[id] = &streamLeg{
			id: id, generation: 1, conn: local, pathBinding: binding, pathIdentity: identity,
			pathRTTEnabled: enabled, send: make(chan txSendAttempt, 256), rescue: make(chan txSendAttempt, 8),
			control: make(chan legControl, 8), done: make(chan struct{}), retired: make(chan struct{}),
		}
		engine.legGeneration[id].Store(1)
		engine.legsMu.Unlock()
	}
	return engine, peers
}

func selectPathAwareFrames(engine *StreamEngine, count int) (int, int) {
	var selected [2]int
	for i := 0; i < count; i++ {
		leg := engine.chooseLeg(true, -1)
		if leg == nil {
			return selected[0], selected[1]
		}
		selected[leg.id]++
		engine.txSentBytes[leg.id].Add(uint64(engine.cfg.ChunkSize))
	}
	return selected[0], selected[1]
}

func selectPathAwareAssignedFrames(engine *StreamEngine, count int) (int, int) {
	var selected [2]int
	for i := 0; i < count; i++ {
		record := &StreamTXRecord{payload: make([]byte, engine.cfg.ChunkSize)}
		leg := engine.assignBenchmark(record, &benchmarkAssignment{})
		if leg == nil {
			return selected[0], selected[1]
		}
		selected[leg.id]++
	}
	return selected[0], selected[1]
}

func testPathAuthPair(t *testing.T, sessionID SessionID, legID LegID, nonce byte) (StreamPathAuth, StreamPathAuth) {
	t.Helper()
	hello := testStreamV6Hello()
	hello.SessionID = sessionID
	hello.LegID = legID
	hello.Nonce[0] = nonce
	password := []byte("same-carrier-path-test")
	serverNonce := [16]byte{nonce, 0x91}
	wire, err := EncodeStreamReadyV2(hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	var serverOutput bytes.Buffer
	serverAuth, err := writeStreamReadyV2WithNonce(&serverOutput, hello, StreamCapabilityPathRTT, serverNonce, password)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, serverOutput.Bytes()) {
		t.Fatal("deterministic READY helper changed bytes")
	}
	_, clientAuth, err := ReadStreamReadyV2(bytes.NewReader(wire), hello, password)
	if err != nil {
		t.Fatal(err)
	}
	return clientAuth, serverAuth
}

type delayedPathTestLeg struct {
	net.Conn
	delay time.Duration
}

func (leg delayedPathTestLeg) Write(p []byte) (int, error) {
	time.Sleep(leg.delay)
	return leg.Conn.Write(p)
}

func waitForEnginePathFeedback(t *testing.T, engine *StreamEngine, leg *streamLeg, peer *StreamEngine, failures <-chan error) StreamPathFeedback {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if feedback, ok := engine.pathRTTCache.PathFeedback(leg.pathIdentity); ok {
			return feedback
		}
		time.Sleep(time.Millisecond)
	}
	cache := engine.pathRTTCache
	cache.mu.Lock()
	pending := make([]uint64, 0, len(cache.pending))
	for token := range cache.pending {
		pending = append(pending, token)
	}
	active := cache.active[int(leg.id)]
	binding := cache.binding[int(leg.id)]
	samples := len(cache.samples[int(leg.id)][int(leg.pathIdentity.Direction-1)])
	cache.mu.Unlock()
	peerCache := peer.pathRTTCache
	peerCache.mu.Lock()
	peerPending := make([]uint64, 0, len(peerCache.pending))
	for token := range peerCache.pending {
		peerPending = append(peerPending, token)
	}
	peerLastToken := peerCache.lastPeerToken[int(leg.id)]
	peerCache.mu.Unlock()
	var closeErr error
	select {
	case closeErr = <-failures:
	default:
	}
	t.Fatalf("timed out waiting for path feedback identity=%+v; closed=%v closeErr=%v engineLeg=%v peerLeg=%v pending=%v samples=%d active=%v binding=%+v peerPending=%v peerLastTokens=%v", leg.pathIdentity, leg.closed.Load(), closeErr, engine.HasLeg(leg.pathIdentity.LegID), peer.HasLeg(leg.pathIdentity.LegID), pending, samples, active, binding, peerPending, peerLastToken)
	return StreamPathFeedback{}
}

func waitForPathFeedbackGone(t *testing.T, cache *streamRTTProbeCache, identity StreamPathIdentity) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := cache.PathFeedback(identity); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("stale path feedback remained active identity=%+v", identity)
}

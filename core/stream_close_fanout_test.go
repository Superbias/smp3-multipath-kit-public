package smp3core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type closeFanoutAddr string

func (a closeFanoutAddr) Network() string { return "close-fanout-test" }
func (a closeFanoutAddr) String() string  { return string(a) }

type closeFanoutCarrier struct {
	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
	writes  [][]byte
}

func newCloseFanoutCarrier() *closeFanoutCarrier {
	return &closeFanoutCarrier{closeCh: make(chan struct{})}
}

func (c *closeFanoutCarrier) Read([]byte) (int, error) {
	<-c.closeCh
	return 0, net.ErrClosed
}

func (c *closeFanoutCarrier) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	c.writes = append(c.writes, bytes.Clone(p))
	return len(p), nil
}

func (c *closeFanoutCarrier) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.closeCh)
	}
	return nil
}

func (c *closeFanoutCarrier) LocalAddr() net.Addr              { return closeFanoutAddr("local") }
func (c *closeFanoutCarrier) RemoteAddr() net.Addr             { return closeFanoutAddr("remote") }
func (c *closeFanoutCarrier) SetDeadline(time.Time) error      { return nil }
func (c *closeFanoutCarrier) SetReadDeadline(time.Time) error  { return nil }
func (c *closeFanoutCarrier) SetWriteDeadline(time.Time) error { return nil }
func (c *closeFanoutCarrier) firstWrite() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return nil
	}
	return bytes.Clone(c.writes[0])
}

type closeFanoutReadTraceConn struct {
	net.Conn
	onCloseRead func()
	mu          sync.Mutex
	pending     []byte
	seen        bool
}

type closeFanoutReadObservation struct {
	LegID uint8
	At    time.Time
}

func (c *closeFanoutReadTraceConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == 0 {
		return n, err
	}
	c.mu.Lock()
	c.pending = append(c.pending, p[:n]...)
	if !c.seen && len(c.pending) >= StreamFrameHeaderSize {
		header, decodeErr := ReadStreamFrameHeader(bytes.NewReader(c.pending[:StreamFrameHeaderSize]))
		if decodeErr == nil && header.Type == StreamFrameClose {
			c.seen = true
			c.mu.Unlock()
			c.onCloseRead()
			return n, err
		}
		if decodeErr == nil {
			c.pending = c.pending[StreamFrameHeaderSize:]
		}
	}
	c.mu.Unlock()
	return n, err
}

func closeFanoutNewSender(t *testing.T, events chan<- streamCloseTraceEvent) (*StreamEngine, [2]*streamLeg) {
	return closeFanoutNewSenderWithRTTs(t, events, [2]time.Duration{20 * time.Millisecond, 400 * time.Millisecond})
}

func closeFanoutNewSenderWithRTTs(t *testing.T, events chan<- streamCloseTraceEvent, rtts [2]time.Duration) (*StreamEngine, [2]*streamLeg) {
	t.Helper()
	var sessionID SessionID
	sessionID[0] = 0xB7
	cache := newStreamRTTProbeCache()
	engine := &StreamEngine{
		cfg: StreamConfig{
			MaxActiveLegs:         2,
			ExperimentalPathAware: true,
			RecoveryTimeout:       time.Second,
			closeTraceHook: func(event streamCloseTraceEvent) {
				events <- event
			},
		},
		pathRTTCache:  cache,
		maxActiveLegs: 2,
		done:          make(chan struct{}),
		legs:          make(map[uint8]*streamLeg, 2),
	}
	engine.appConn, engine.pipeConn = net.Pipe()
	var legs [2]*streamLeg
	for id, rtt := range rtts {
		legID := LegID(id)
		binding := StreamPathBinding{SessionID: sessionID, LegID: legID, CarrierID: fmt.Sprintf("close-carrier-%d", id), Generation: 1}
		_, auth := testPathAuthPair(t, sessionID, legID, byte(0xB0+id))
		if err := cache.bindLeg(binding, auth); err != nil {
			t.Fatalf("bind test path leg %d: %v", id, err)
		}
		identity := StreamPathIdentity{
			SessionID:  binding.SessionID,
			LegID:      binding.LegID,
			CarrierID:  binding.CarrierID,
			Generation: binding.Generation,
			Direction:  StreamRTTServerToClient,
		}
		cache.mu.Lock()
		cache.recordSampleLocked(identity, rtt, time.Now())
		cache.mu.Unlock()
		legs[id] = &streamLeg{
			id:             uint8(id),
			generation:     1,
			conn:           newCloseFanoutCarrier(),
			pathBinding:    binding,
			pathIdentity:   identity,
			pathRTTEnabled: true,
			control:        make(chan legControl, 1),
			done:           make(chan struct{}),
		}
		engine.legs[uint8(id)] = legs[id]
	}
	return engine, legs
}

func closeFanoutStartSend(engine *StreamEngine) <-chan bool {
	result := make(chan bool, 1)
	go func() { result <- engine.sendCloseFrame(0) }()
	return result
}

func closeFanoutWaitControl(t *testing.T, leg *streamLeg) legControl {
	t.Helper()
	select {
	case control := <-leg.control:
		return control
	case <-time.After(time.Second):
		t.Fatalf("CLOSE was not queued on leg %d", leg.id)
		return legControl{}
	}
}

func closeFanoutWriteControl(t *testing.T, leg *streamLeg, control legControl) []byte {
	t.Helper()
	if control.typ != frameTypeClose {
		t.Fatalf("queued control type=%d, want CLOSE", control.typ)
	}
	header := encodeControlFrame(control.typ, control.value)
	n, err := leg.conn.Write(header[:])
	if err == nil && n != len(header) {
		err = io.ErrShortWrite
	}
	control.done <- err
	if err != nil {
		t.Fatalf("write CLOSE on leg %d: %v", leg.id, err)
	}
	return bytes.Clone(header[:])
}

func closeFanoutWaitEvent(t *testing.T, events <-chan streamCloseTraceEvent, done <-chan struct{}, want streamCloseTraceStage, legID uint8) streamCloseTraceEvent {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	match := func(event streamCloseTraceEvent) bool { return event.Stage == want && event.LegID == legID }
	for {
		select {
		case event := <-events:
			if match(event) {
				return event
			}
		case <-done:
			// The event is emitted before graceful teardown closes Done. Drain
			// already-published events before reporting an ordering failure.
			for {
				select {
				case event := <-events:
					if match(event) {
						return event
					}
				default:
					t.Fatalf("Core finalized before CLOSE event stage=%d leg=%d", want, legID)
				}
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for CLOSE event stage=%d leg=%d", want, legID)
		}
	}
}

func TestStreamCloseDoesNotLetSlowLocalWriteCancelFastCarrier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rtts     [2]time.Duration
		wantFast bool
	}{
		{name: "equal_rtt_20_20_tie_fallback", rtts: [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{name: "asymmetric_20_100", rtts: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}, wantFast: true},
		{name: "asymmetric_20_400", rtts: [2]time.Duration{20 * time.Millisecond, 400 * time.Millisecond}, wantFast: true},
	} {
		t.Run(tc.name, func(t *testing.T) { runStreamCloseEOFProfile(t, tc.rtts, tc.wantFast) })
	}
}

func runStreamCloseEOFProfile(t *testing.T, rtts [2]time.Duration, wantFast bool) {
	t.Helper()
	var eventOrder []string
	serverEvents := make(chan streamCloseTraceEvent, 16)
	server, serverLegs := closeFanoutNewSenderWithRTTs(t, serverEvents, rtts)
	defer func() { _ = server.Close() }()
	serverAppRead := make(chan p0eAppReadResult, 1)
	go func() {
		var one [1]byte
		n, err := server.appConn.Read(one[:])
		serverAppRead <- p0eAppReadResult{n: n, err: err}
	}()
	clientEvents := make(chan streamCloseTraceEvent, 16)
	client, app := NewStreamEngine(StreamConfig{
		closeTraceHook: func(event streamCloseTraceEvent) {
			clientEvents <- event
		},
	})
	clientLeg0, peer0 := net.Pipe()
	clientLeg1, peer1 := net.Pipe()
	carrierRead := make(chan closeFanoutReadObservation, 2)
	if err := client.AttachLeg(0, &closeFanoutReadTraceConn{Conn: clientLeg0, onCloseRead: func() { carrierRead <- closeFanoutReadObservation{LegID: 0, At: time.Now()} }}, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.AttachLeg(1, &closeFanoutReadTraceConn{Conn: clientLeg1, onCloseRead: func() { carrierRead <- closeFanoutReadObservation{LegID: 1, At: time.Now()} }}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = peer0.Close()
		_ = peer1.Close()
		_ = app.Close()
		_ = client.Close()
	})

	appRead := make(chan p0eAppReadResult, 1)
	go func() {
		var one [1]byte
		n, err := app.Read(one[:])
		appRead <- p0eAppReadResult{n: n, err: err}
	}()
	reciprocal := make(chan StreamFrameHeader, 2)
	for _, peer := range []net.Conn{peer0, peer1} {
		go func(peer net.Conn) {
			header, err := ReadStreamFrameHeader(peer)
			if err != nil {
				reciprocal <- StreamFrameHeader{Type: 0}
				return
			}
			reciprocal <- header
		}(peer)
	}

	serverResult := make(chan bool, 1)
	go func() {
		ok := server.sendCloseFrame(0)
		if ok {
			server.finishGracefulClose(io.EOF)
		}
		serverResult <- ok
	}()
	control0 := closeFanoutWaitControl(t, serverLegs[0])
	control1 := closeFanoutWaitControl(t, serverLegs[1])
	slowWire := closeFanoutWriteControl(t, serverLegs[1], control1)
	eventOrder = append(eventOrder, "slow_leg1_local_write_success")
	slowGate := make(chan struct{})
	slowDeliveryStarted := make(chan struct{})
	slowDeliveryResult := make(chan error, 1)
	var releaseSlow sync.Once
	deliverSlow := func() { releaseSlow.Do(func() { close(slowGate) }) }
	t.Cleanup(deliverSlow)
	go func() {
		<-slowGate
		close(slowDeliveryStarted)
		slowDeliveryResult <- writeAll(peer1, slowWire)
	}()

	var deliveryWire []byte
	var deliveryPeer net.Conn
	wantDeliveryLeg := uint8(1)
	if wantFast {
		// The slow Carrier's local Write succeeded, but its propagation gate stays
		// closed. The Core must preserve the authenticated lower-RTT leg's queued
		// CLOSE opportunity instead of treating local Write order as network order.
		_ = closeFanoutWaitEvent(t, serverEvents, server.Done(), streamCloseWaitingPreferred, 0)
		eventOrder = append(eventOrder, "core_waited_for_preferred_leg0_write")
		select {
		case <-server.Done():
			t.Fatal("Core finalized after slow-leg local Write while the fast-leg CLOSE was pending")
		default:
		}
		deliveryWire, deliveryPeer, wantDeliveryLeg = closeFanoutWriteControl(t, serverLegs[0], control0), peer0, 0
		eventOrder = append(eventOrder, "fast_leg0_local_write_success")
		select {
		case ok := <-serverResult:
			if !ok {
				t.Fatal("Core did not complete CLOSE after the preferred fast-leg Write succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("Core did not finish after the preferred fast-leg CLOSE succeeded")
		}
	} else {
		// Equal feedback has no preferred leg, so a successful CLOSE on either
		// Carrier remains sufficient and adds no preference wait.
		select {
		case ok := <-serverResult:
			if !ok {
				t.Fatal("Core did not complete CLOSE with equal RTT feedback")
			}
		case <-time.After(time.Second):
			t.Fatal("Core waited despite equal RTT feedback and a successful CLOSE write")
		}
		eventOrder = append(eventOrder, "core_finalized_after_leg1_local_write_success")
		deliverSlow()
		select {
		case err := <-slowDeliveryResult:
			if err != nil {
				t.Fatalf("deliver equal-RTT CLOSE: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("equal-RTT slow delivery gate did not complete")
		}
		eventOrder = append(eventOrder, "slow_propagation_gate_released")
		deliveryWire, deliveryPeer = slowWire, peer1
	}
	select {
	case result := <-serverAppRead:
		if result.n != 0 || !errors.Is(result.err, io.EOF) {
			t.Fatalf("server application read n=%d err=%v, want io.EOF", result.n, result.err)
		}
		eventOrder = append(eventOrder, "sender_application_eof")
	case <-time.After(time.Second):
		t.Fatal("sending Core did not expose graceful EOF to its application endpoint")
	}

	frameHeader, err := ReadStreamFrameHeader(bytes.NewReader(deliveryWire))
	if err != nil {
		t.Fatalf("parse selected CLOSE: %v", err)
	}
	if frameHeader.Type != StreamFrameClose || frameHeader.Value != 0 {
		t.Fatalf("selected frame=%+v, want CLOSE seq=0", frameHeader)
	}
	if wantFast {
		frameDelivery := make(chan error, 1)
		go func() { frameDelivery <- writeAll(deliveryPeer, deliveryWire) }()
		select {
		case err := <-frameDelivery:
			if err != nil {
				t.Fatalf("deliver CLOSE on selected leg %d: %v", wantDeliveryLeg, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("client Core did not read CLOSE on selected leg %d", wantDeliveryLeg)
		}
	}
	select {
	case observation := <-carrierRead:
		if observation.LegID != wantDeliveryLeg {
			t.Fatalf("Carrier Read observed CLOSE on leg %d, want selected leg %d", observation.LegID, wantDeliveryLeg)
		}
		eventOrder = append(eventOrder, fmt.Sprintf("carrier_read_close_leg%d", observation.LegID))
		_ = closeFanoutWaitEvent(t, clientEvents, client.Done(), streamCloseFrameDecoded, wantDeliveryLeg)
		eventOrder = append(eventOrder, fmt.Sprintf("core_decoded_close_leg%d", wantDeliveryLeg))
		accepted := closeFanoutWaitEvent(t, clientEvents, client.Done(), streamCloseSequenceAccepted, wantDeliveryLeg)
		eventOrder = append(eventOrder, fmt.Sprintf("core_accepted_close_sequence_%d", accepted.Seq))
		select {
		case result := <-appRead:
			if result.n != 0 || !errors.Is(result.err, io.EOF) {
				t.Fatalf("application read n=%d err=%v, want io.EOF", result.n, result.err)
			}
			eventOrder = append(eventOrder, "receiver_application_eof")
		case <-time.After(time.Second):
			t.Fatal("application did not observe EOF after Core accepted CLOSE")
		}
	case <-time.After(time.Second):
		t.Fatal("no Carrier Read boundary was recorded for CLOSE")
	}
	if wantFast {
		select {
		case <-slowDeliveryStarted:
			t.Fatal("slow propagation gate was released before the fast-leg EOF")
		default:
		}
	}
	reciprocalDeadline := time.NewTimer(time.Second)
	defer reciprocalDeadline.Stop()
	for {
		select {
		case header := <-reciprocal:
			if header.Type == StreamFrameClose && header.Value == 0 {
				eventOrder = append(eventOrder, "client_reciprocal_close_wire_observed")
				goto reciprocalSeen
			}
		case <-reciprocalDeadline.C:
			t.Fatal("client Core did not send its reciprocal CLOSE")
		}
	}

reciprocalSeen:
	deliverSlow()
	select {
	case <-slowDeliveryStarted:
	case <-time.After(time.Second):
		t.Fatal("slow propagation gate did not open")
	}
	if wantFast {
		_ = <-slowDeliveryResult // the peer may already have closed its leg after valid EOF.
		eventOrder = append(eventOrder, "slow_propagation_gate_released_after_receiver_eof")
	}
	t.Logf("SMP3_CLOSE_TIMELINE rtt0=%s rtt1=%s selected_leg=%d events=%q sender_eof=true receiver_eof=true", rtts[0], rtts[1], wantDeliveryLeg, eventOrder)
}

func TestStreamClosePrefersOnlyACompleteFreshRTTPair(t *testing.T) {
	cases := []struct {
		name       string
		rtts       [2]time.Duration
		wantLeg    uint8
		wantChosen bool
	}{
		{name: "healthy_equal_20_20", rtts: [2]time.Duration{20 * time.Millisecond, 20 * time.Millisecond}},
		{name: "asymmetric_20_100", rtts: [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond}, wantLeg: 0, wantChosen: true},
		{name: "asymmetric_20_400", rtts: [2]time.Duration{20 * time.Millisecond, 400 * time.Millisecond}, wantLeg: 0, wantChosen: true},
		{name: "reversed_100_20", rtts: [2]time.Duration{100 * time.Millisecond, 20 * time.Millisecond}, wantLeg: 1, wantChosen: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, legs := closeFanoutNewSenderWithRTTs(t, make(chan streamCloseTraceEvent, 8), tc.rtts)
			defer engine.Close()
			preferred, _, ok := engine.preferredCloseLeg([]*streamLeg{legs[0], legs[1]})
			if ok != tc.wantChosen || (ok && preferred != tc.wantLeg) {
				t.Fatalf("preferredCloseLeg()=(%d, _, %v), want (%d, _, %v)", preferred, ok, tc.wantLeg, tc.wantChosen)
			}
		})
	}
}

func TestStreamCloseFallsBackAfterPreferredWriteFailure(t *testing.T) {
	events := make(chan streamCloseTraceEvent, 16)
	engine, legs := closeFanoutNewSender(t, events)
	defer engine.Close()
	result := closeFanoutStartSend(engine)
	control0 := closeFanoutWaitControl(t, legs[0])
	control1 := closeFanoutWaitControl(t, legs[1])

	writeErr := errors.New("preferred fast Carrier write failed")
	control0.done <- writeErr
	legs[0].close(writeErr)
	closeFanoutWaitEvent(t, events, engine.Done(), streamCloseWriteResult, 0)

	_ = closeFanoutWriteControl(t, legs[1], control1)
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("CLOSE did not fall back to the healthy slow Carrier after preferred Write failure")
		}
	case <-time.After(time.Second):
		t.Fatal("bounded fallback did not complete after preferred Write failure")
	}
}

func TestStreamCloseUsesRTTBoundForRedundantFallback(t *testing.T) {
	events := make(chan streamCloseTraceEvent, 16)
	engine, legs := closeFanoutNewSender(t, events)
	defer engine.Close()
	result := closeFanoutStartSend(engine)
	control0 := closeFanoutWaitControl(t, legs[0])
	control1 := closeFanoutWaitControl(t, legs[1])
	started := time.Now()
	_ = closeFanoutWriteControl(t, legs[1], control1)
	closeFanoutWaitEvent(t, events, engine.Done(), streamCloseWaitingPreferred, 0)
	_ = control0 // the preferred leg deliberately never completes its local Write.
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("CLOSE fallback failed after the preferred-write RTT bound")
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("CLOSE fallback took %s, expected a bound near the fresh 20 ms RTT", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CLOSE remained blocked beyond its fresh preferred-path RTT bound")
	}
}

func TestStreamCloseWithNoTrustedRTTKeepsAnySuccessFallback(t *testing.T) {
	events := make(chan streamCloseTraceEvent, 16)
	engine, legs := closeFanoutNewSender(t, events)
	defer engine.Close()
	identity := legs[0].pathIdentity
	if !engine.pathRTTCache.invalidateLeg(identity.binding()) {
		t.Fatal("failed to invalidate leg0 feedback for missing-sample test")
	}
	result := closeFanoutStartSend(engine)
	control0 := closeFanoutWaitControl(t, legs[0])
	control1 := closeFanoutWaitControl(t, legs[1])
	_ = control0
	_ = closeFanoutWriteControl(t, legs[1], control1)
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("CLOSE did not retain any-success fallback without a complete trusted RTT pair")
		}
	case <-time.After(time.Second):
		t.Fatal("CLOSE waited for RTT preference despite missing trusted feedback")
	}
}

func TestStreamCloseFailsWhenBothCarriersAreUnavailable(t *testing.T) {
	events := make(chan streamCloseTraceEvent, 16)
	engine, legs := closeFanoutNewSender(t, events)
	defer engine.Close()
	engine.cfg.RecoveryTimeout = 25 * time.Millisecond
	result := closeFanoutStartSend(engine)
	_ = closeFanoutWaitControl(t, legs[0])
	_ = closeFanoutWaitControl(t, legs[1])
	started := time.Now()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("CLOSE reported normal success when neither Carrier completed its Write")
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("both-unavailable CLOSE exceeded the configured recovery bound: %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("both-unavailable CLOSE did not terminate within its configured recovery bound")
	}
}

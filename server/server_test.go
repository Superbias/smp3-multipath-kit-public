package server

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const localTestCredential = "phase6b-local-fixture"

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.Password = localTestCredential
	cfg.Stream.ActivationThresholdMbps = 0
	cfg.Stream.QueueFrames = 32
	cfg.Stream.BandwidthMbps = []uint32{1, 1}
	cfg.UDP.Enabled = true
	cfg.UDP.QueueFrames = 32
	cfg.UDP.DedupWindow = 64
	return cfg
}

func startTestServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		instance.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	return instance
}

func writeAll(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("zero-length write")
		}
		data = data[n:]
	}
}

func writeHello(t *testing.T, conn net.Conn, password string, id smp3core.SessionID, leg uint8, mode smp3core.HelloMode, destination string, nonce byte) {
	t.Helper()
	var helloNonce [16]byte
	helloNonce[0] = nonce
	writeHelloNonce(t, conn, password, id, leg, mode, destination, helloNonce)
}

func writeHelloNonce(t *testing.T, conn net.Conn, password string, id smp3core.SessionID, leg uint8, mode smp3core.HelloMode, destination string, nonce [16]byte) {
	t.Helper()
	version := smp3core.Version4
	if mode == smp3core.ModeDatagram {
		version = smp3core.Version5
	}
	hello := smp3core.Hello{
		Version: version, SessionID: id, LegID: smp3core.LegID(leg), Mode: mode,
		Timestamp: time.Now().Unix(), Nonce: nonce, Destination: destination,
	}
	header, dest, mac, err := smp3core.EncodeHelloParts(hello, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	writeAll(t, conn, header)
	writeAll(t, conn, dest)
	writeAll(t, conn, mac)
}

func connectLeg(t *testing.T, instance *Server, password string, id smp3core.SessionID, leg uint8, mode smp3core.HelloMode, destination string, nonce byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	writeHello(t, conn, password, id, leg, mode, destination, nonce)
	return conn
}

func connectLegNonce(t *testing.T, instance *Server, password string, id smp3core.SessionID, leg uint8, mode smp3core.HelloMode, destination string, nonce [16]byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	writeHelloNonce(t, conn, password, id, leg, mode, destination, nonce)
	return conn
}

func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
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

func startTCPEcho(t *testing.T) string {
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

func streamClient(t *testing.T) (*smp3core.StreamEngine, net.Conn) {
	t.Helper()
	engine, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
		ChunkSize: 16 * 1024, QueueFrames: 32, MaxReorderFrames: 128,
		MaxInflightFrames: 64, AckInterval: time.Millisecond, RetransmitTimeout: 100 * time.Millisecond,
		RecoveryTimeout: time.Second,
	})
	t.Cleanup(func() { _ = engine.Close(); _ = app.Close() })
	return engine, app
}

func streamRoundTrip(t *testing.T, app net.Conn, payload []byte) {
	t.Helper()
	written := make(chan error, 1)
	go func() {
		_, err := app.Write(payload)
		written <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("stream echo payload mismatch")
	}
}

func TestConfigValidationAndDurationParsing(t *testing.T) {
	valid := testConfig()
	valid.Listen = ":0"
	if err := valid.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		edit func(*Config)
	}{
		{"empty-password", func(c *Config) { c.Password = "" }},
		{"bad-listen", func(c *Config) { c.Listen = "not-an-address" }},
		{"bad-scheduler", func(c *Config) { c.Stream.SchedulerMode = "bad" }},
		{"bad-queue", func(c *Config) { c.Stream.QueueFrames = 7 }},
		{"bad-max-datagram", func(c *Config) { c.UDP.MaxDatagramSize = 16385 }},
		{"bad-bandwidth", func(c *Config) { c.Stream.BandwidthMbps = []uint32{1} }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			cfg := valid
			check.edit(&cfg)
			if err := cfg.NormalizeAndValidate(); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
	var duration Duration
	if err := duration.UnmarshalJSON([]byte(`"10s"`)); err != nil || duration.Time() != 10*time.Second {
		t.Fatalf("duration parse = %v/%v", duration, err)
	}
}

func TestHelloAdmissionRejectsBadAuthAndReplay(t *testing.T) {
	target := startTCPEcho(t)
	instance := startTestServer(t, testConfig())
	var id smp3core.SessionID
	id[0] = 0x55
	badAuth, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	writeHello(t, badAuth, "wrong-password", id, 0, smp3core.ModeStream, target, 200)
	_ = badAuth.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := badAuth.Read(one[:]); err == nil {
		t.Fatal("bad-auth carrier remained open")
	}
	_ = badAuth.Close()

	valid := connectLeg(t, instance, instance.cfg.Password, id, 0, smp3core.ModeStream, target, 201)
	defer valid.Close()
	waitFor(t, time.Second, func() bool { return instance.SessionCount() == 1 })
	replay, err := net.Dial("tcp", instance.String())
	if err != nil {
		t.Fatal(err)
	}
	writeHello(t, replay, instance.cfg.Password, id, 1, smp3core.ModeStream, target, 201)
	_ = replay.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := replay.Read(one[:]); err == nil {
		t.Fatal("replayed HELLO carrier remained open")
	}
	_ = replay.Close()
}

func TestDatagramAdmissionRequiresEnabledConfig(t *testing.T) {
	cfg := testConfig()
	cfg.UDP.Enabled = false
	instance := startTestServer(t, cfg)
	var id smp3core.SessionID
	id[0] = 0x56
	conn := connectLeg(t, instance, cfg.Password, id, 0, smp3core.ModeDatagram, "127.0.0.1:53", 202)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("disabled datagram admission remained open")
	}
}

func TestConcurrentEitherLegFirstCreatesOneSession(t *testing.T) {
	instance, err := New(testConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	var id smp3core.SessionID
	id[0] = 0x61
	target := "127.0.0.1:1"
	start := make(chan struct{})
	type result struct {
		session *serverSession
		created bool
		err     error
		peer    net.Conn
	}
	results := make(chan result, 2)
	for leg := uint8(0); leg < 2; leg++ {
		leg := leg
		go func() {
			carrier, peer := net.Pipe()
			<-start
			session, created, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: smp3core.LegID(leg), Mode: smp3core.ModeStream}, target, carrier)
			results <- result{session: session, created: created, err: err, peer: peer}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	defer first.peer.Close()
	defer second.peer.Close()
	if first.err != nil || second.err != nil || first.session == nil || second.session == nil {
		t.Fatalf("create/join results: %#v %#v", first, second)
	}
	if first.session != second.session || (first.created == second.created) {
		t.Fatalf("sessions did not converge: created=%v/%v session=%p/%p", first.created, second.created, first.session, second.session)
	}
	first.session.close()
}

func TestRegistryRejectsModeDestinationAndDuplicateLeg(t *testing.T) {
	instance, err := New(testConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	var id smp3core.SessionID
	id[0] = 0x62
	streamCarrier, streamPeer := net.Pipe()
	defer streamPeer.Close()
	session, created, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 0, Mode: smp3core.ModeStream}, "example.com:443", streamCarrier)
	if err != nil || !created {
		t.Fatalf("stream create: %v/%v", created, err)
	}
	defer session.close()
	modeCarrier, modePeer := net.Pipe()
	defer modeCarrier.Close()
	defer modePeer.Close()
	if _, _, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 1, Mode: smp3core.ModeDatagram}, "example.com:443", modeCarrier); !errors.Is(err, errSessionMode) {
		t.Fatalf("mode mismatch = %v", err)
	}
	destinationCarrier, destinationPeer := net.Pipe()
	defer destinationCarrier.Close()
	defer destinationPeer.Close()
	if _, _, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 1, Mode: smp3core.ModeStream}, "example.net:443", destinationCarrier); !errors.Is(err, errSessionDestination) {
		t.Fatalf("destination mismatch = %v", err)
	}
	duplicateCarrier, duplicatePeer := net.Pipe()
	defer duplicateCarrier.Close()
	defer duplicatePeer.Close()
	if _, _, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 0, Mode: smp3core.ModeStream}, "example.com:443", duplicateCarrier); err == nil {
		t.Fatal("duplicate live leg was accepted")
	}
}

func TestStreamLocalE2EAndEitherLegFirst(t *testing.T) {
	target := startTCPEcho(t)
	for _, firstLeg := range []uint8{0, 1} {
		t.Run("first-leg-"+string(rune('0'+firstLeg)), func(t *testing.T) {
			instance := startTestServer(t, func() Config {
				cfg := testConfig()
				cfg.UDP.Enabled = false
				return cfg
			}())
			client, app := streamClient(t)
			var id smp3core.SessionID
			id[0] = 0x70 + firstLeg
			first := connectLeg(t, instance, instance.cfg.Password, id, firstLeg, smp3core.ModeStream, target, 1+firstLeg)
			defer first.Close()
			if err := client.AttachLeg(smp3core.LegID(firstLeg), first, nil); err != nil {
				t.Fatal(err)
			}
			secondLeg := firstLeg ^ 1
			second := connectLeg(t, instance, instance.cfg.Password, id, secondLeg, smp3core.ModeStream, target, 3+firstLeg)
			defer second.Close()
			if err := client.AttachLeg(smp3core.LegID(secondLeg), second, nil); err != nil {
				t.Fatal(err)
			}
			streamRoundTrip(t, app, []byte("phase6b-small-stream"))
			streamRoundTrip(t, app, bytes.Repeat([]byte("large"), 64*1024))
		})
	}
}

func TestStreamBothLegsSameIDRejoin(t *testing.T) {
	target := startTCPEcho(t)
	for _, failedLeg := range []uint8{0, 1} {
		t.Run("leg-"+string(rune('0'+failedLeg)), func(t *testing.T) {
			instance := startTestServer(t, func() Config {
				cfg := testConfig()
				cfg.UDP.Enabled = false
				return cfg
			}())
			client, app := streamClient(t)
			var id smp3core.SessionID
			id[0] = 0x80 + failedLeg
			legs := make([]net.Conn, 2)
			for leg := uint8(0); leg < 2; leg++ {
				legs[leg] = connectLeg(t, instance, instance.cfg.Password, id, leg, smp3core.ModeStream, target, 10+failedLeg*2+leg)
				if err := client.AttachLeg(smp3core.LegID(leg), legs[leg], nil); err != nil {
					t.Fatal(err)
				}
			}
			streamRoundTrip(t, app, []byte("before-rejoin"))
			_ = legs[failedLeg].Close()
			waitFor(t, 2*time.Second, func() bool { return !client.Snapshot().LegUp[failedLeg] && instance.SessionCount() == 1 })
			replacement := connectLeg(t, instance, instance.cfg.Password, id, failedLeg, smp3core.ModeStream, target, 20+failedLeg)
			defer replacement.Close()
			if err := client.AttachLeg(smp3core.LegID(failedLeg), replacement, nil); err != nil {
				t.Fatal(err)
			}
			streamRoundTrip(t, app, []byte("after-rejoin"))
			if instance.SessionCount() != 1 {
				t.Fatalf("rejoin created a new session: %d", instance.SessionCount())
			}
			_ = legs[failedLeg^1].Close()
			_ = replacement.Close()
			_ = client.Close()
			_ = app.Close()
		})
	}
}

func startUDPEcho(t *testing.T, network, host string) string {
	t.Helper()
	addr, err := net.ResolveUDPAddr(network, net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		t.Skipf("%s loopback unavailable: %v", network, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buffer := make([]byte, 64*1024)
		for {
			n, peer, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(buffer[:n], peer)
		}
	}()
	return conn.LocalAddr().String()
}

func datagramClient(t *testing.T, mode smp3core.DatagramMode) (*smp3core.DatagramEngine, string) {
	t.Helper()
	client := smp3core.NewDatagramEngine(smp3core.DatagramConfig{
		Mode: mode, QueueFrames: 32, MaxDatagramSize: 16384, DedupWindow: 64,
		IdleTimeout: 2 * time.Minute, RecoveryTimeout: 10 * time.Second,
		AdaptiveQueueDelay: 10 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client, localTestCredential
}

func datagramRoundTrip(t *testing.T, client *smp3core.DatagramEngine, destination string, payload []byte) smp3core.Datagram {
	t.Helper()
	if err := client.Send(payload, destination, time.Now().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	response, err := client.Receive(time.Now().Add(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.Payload, payload) {
		t.Fatal("datagram echo payload mismatch")
	}
	return response
}

func TestDatagramModesAndMultiDestination(t *testing.T) {
	firstTarget := startUDPEcho(t, "udp4", "127.0.0.1")
	secondTarget := startUDPEcho(t, "udp4", "127.0.0.1")
	secondDomain := net.JoinHostPort("localhost", mustPort(t, secondTarget))
	for _, test := range []struct {
		name string
		mode smp3core.DatagramMode
	}{
		{"adaptive", smp3core.DatagramAdaptive},
		{"stripe", smp3core.DatagramStripe},
		{"duplicate", smp3core.DatagramDuplicate},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.UDP.Mode = test.name
			instance := startTestServer(t, cfg)
			client, password := datagramClient(t, test.mode)
			var id smp3core.SessionID
			id[0] = byte(0x90 + test.mode)
			leg0 := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, firstTarget, byte(30+test.mode))
			defer leg0.Close()
			if err := client.AttachLeg(0, leg0, nil); err != nil {
				t.Fatal(err)
			}
			leg1 := connectLeg(t, instance, password, id, 1, smp3core.ModeDatagram, firstTarget, byte(40+test.mode))
			defer leg1.Close()
			if err := client.AttachLeg(1, leg1, nil); err != nil {
				t.Fatal(err)
			}
			first := datagramRoundTrip(t, client, firstTarget, []byte("udp-destination-one"))
			second := datagramRoundTrip(t, client, secondDomain, []byte("udp-destination-two"))
			if first.Address == "" || second.Address == "" {
				t.Fatal("response source address was empty")
			}
			if test.mode == smp3core.DatagramDuplicate {
				waitFor(t, time.Second, func() bool {
					instance.access.Lock()
					session := instance.sessions[id]
					instance.access.Unlock()
					return session != nil && session.dgram.Snapshot().DuplicateRxDrop > 0
				})
			}
		})
	}
}

func TestDatagramEitherLegFirst(t *testing.T) {
	target := startUDPEcho(t, "udp4", "127.0.0.1")
	for _, firstLeg := range []uint8{0, 1} {
		t.Run("first-leg-"+string(rune('0'+firstLeg)), func(t *testing.T) {
			instance := startTestServer(t, testConfig())
			client, password := datagramClient(t, smp3core.DatagramAdaptive)
			var id smp3core.SessionID
			id[0] = 0xe0 + firstLeg
			first := connectLeg(t, instance, password, id, firstLeg, smp3core.ModeDatagram, target, byte(210+firstLeg))
			defer first.Close()
			if err := client.AttachLeg(smp3core.LegID(firstLeg), first, nil); err != nil {
				t.Fatal(err)
			}
			secondLeg := firstLeg ^ 1
			second := connectLeg(t, instance, password, id, secondLeg, smp3core.ModeDatagram, target, byte(220+firstLeg))
			defer second.Close()
			if err := client.AttachLeg(smp3core.LegID(secondLeg), second, nil); err != nil {
				t.Fatal(err)
			}
			datagramRoundTrip(t, client, target, []byte("either-leg-first"))
		})
	}
}

func TestDatagramIPv6AndMappedAddress(t *testing.T) {
	ipv6Target := startUDPEcho(t, "udp6", "::1")
	ipv4Target := startUDPEcho(t, "udp4", "127.0.0.1")
	cfg := testConfig()
	instance := startTestServer(t, cfg)
	client, password := datagramClient(t, smp3core.DatagramAdaptive)
	var id smp3core.SessionID
	id[0] = 0xa1
	leg := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, ipv6Target, 51)
	defer leg.Close()
	if err := client.AttachLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	datagramRoundTrip(t, client, ipv6Target, []byte("ipv6"))
	if host, _, err := net.SplitHostPort(ipv4Target); err == nil {
		mapped := net.JoinHostPort("::ffff:"+host, mustPort(t, ipv4Target))
		datagramRoundTrip(t, client, mapped, []byte("mapped-v4"))
	}
}

func mustPort(t *testing.T, address string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestDatagramBoundaryAndBothLegRejoin(t *testing.T) {
	target := startUDPEcho(t, "udp4", "127.0.0.1")
	for _, failedLeg := range []uint8{0, 1} {
		instance := startTestServer(t, testConfig())
		client, password := datagramClient(t, smp3core.DatagramAdaptive)
		var id smp3core.SessionID
		id[0] = 0xb1 + failedLeg
		legs := make([]net.Conn, 2)
		for leg := uint8(0); leg < 2; leg++ {
			legs[leg] = connectLeg(t, instance, password, id, leg, smp3core.ModeDatagram, target, byte(60+failedLeg*2+leg))
			if err := client.AttachLeg(smp3core.LegID(leg), legs[leg], nil); err != nil {
				t.Fatal(err)
			}
		}
		if failedLeg == 0 {
			datagramRoundTrip(t, client, target, bytes.Repeat([]byte("a"), 16384))
			if err := client.Send(bytes.Repeat([]byte("b"), 16385), target, time.Now().Add(time.Second)); !errors.Is(err, smp3core.ErrDatagramTooLarge) {
				t.Fatalf("oversize error = %v", err)
			}
			datagramRoundTrip(t, client, target, []byte("valid-after-oversize"))
		}
		_ = legs[failedLeg].Close()
		waitFor(t, 2*time.Second, func() bool { return !client.Snapshot().LegUp[failedLeg] && instance.SessionCount() == 1 })
		replacement := connectLeg(t, instance, password, id, failedLeg, smp3core.ModeDatagram, target, byte(70+failedLeg))
		if err := client.AttachLeg(smp3core.LegID(failedLeg), replacement, nil); err != nil {
			t.Fatal(err)
		}
		datagramRoundTrip(t, client, target, []byte("datagram-rejoined"))
		_ = replacement.Close()
		_ = legs[failedLeg^1].Close()
		_ = client.Close()
		_ = instance.Close()
	}
}

func TestDatagramIdleTombstoneAndModeBinding(t *testing.T) {
	cfg := testConfig()
	cfg.UDP.IdleTimeout = Duration(5 * time.Second)
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	var id smp3core.SessionID
	id[0] = 0xc1
	carrier, peer := net.Pipe()
	destination := "127.0.0.1:53"
	session, created, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 0, Mode: smp3core.ModeDatagram}, destination, carrier)
	if err != nil || !created || session == nil {
		t.Fatalf("datagram create: %v/%v", created, err)
	}
	defer peer.Close()
	waitFor(t, 8*time.Second, func() bool { return instance.SessionCount() == 0 && instance.TombstoneCount() == 1 })
	late, latePeer := net.Pipe()
	defer late.Close()
	defer latePeer.Close()
	if _, _, err := instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 1, Mode: smp3core.ModeDatagram}, destination, late); !errors.Is(err, errSessionRetired) {
		t.Fatalf("late datagram was not tombstoned: %v", err)
	}
	newID := id
	newID[0]++
	newCarrier, newPeer := net.Pipe()
	defer newPeer.Close()
	newSession, created, err := instance.createOrJoinSession(smp3core.Hello{SessionID: newID, LegID: 0, Mode: smp3core.ModeDatagram}, destination, newCarrier)
	if err != nil || !created || newSession == nil {
		t.Fatalf("fresh datagram session: %v/%v", created, err)
	}
	newSession.close()
}

type halfCloseConn struct {
	net.Conn
	closed      atomic.Int32
	writeClosed atomic.Int32
}

func (c *halfCloseConn) Close() error {
	c.closed.Add(1)
	return c.Conn.Close()
}

func (c *halfCloseConn) CloseWrite() error {
	c.writeClosed.Add(1)
	return nil
}

func TestStreamHalfCloseDoesNotCloseDestination(t *testing.T) {
	source, sourcePeer := net.Pipe()
	destination, destinationPeer := net.Pipe()
	defer source.Close()
	defer sourcePeer.Close()
	defer destination.Close()
	defer destinationPeer.Close()
	tracked := &halfCloseConn{Conn: destination}
	done := make(chan error, 1)
	go func() { done <- copyStreamDirection(source, tracked) }()
	if _, err := sourcePeer.Write([]byte("half-close")); err != nil {
		t.Fatal(err)
	}
	_ = sourcePeer.Close()
	got := make([]byte, len("half-close"))
	if _, err := io.ReadFull(destinationPeer, got); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if tracked.writeClosed.Load() != 1 || tracked.closed.Load() != 0 {
		t.Fatalf("half-close state write=%d close=%d", tracked.writeClosed.Load(), tracked.closed.Load())
	}
}

func TestServerCloseIsIdempotentAndReleasesListener(t *testing.T) {
	instance := startTestServer(t, testConfig())
	address := instance.String()
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("closed server still accepted connections")
	}
}

func TestCloseDuringConcurrentCreateDoesNotPublish(t *testing.T) {
	instance, err := New(testConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var id smp3core.SessionID
	id[0] = 0xd1
	carrier, peer := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = instance.createOrJoinSession(smp3core.Hello{SessionID: id, LegID: 0, Mode: smp3core.ModeStream}, "127.0.0.1:1", carrier)
	}()
	_ = instance.Close()
	wg.Wait()
	_ = peer.Close()
	if instance.SessionCount() != 0 {
		t.Fatalf("closed server retained sessions: %d", instance.SessionCount())
	}
}

func datagramTargetFor(t *testing.T, instance *Server, id smp3core.SessionID) (*serverSession, *udpTarget) {
	t.Helper()
	var session *serverSession
	var target *udpTarget
	waitFor(t, 2*time.Second, func() bool {
		instance.access.Lock()
		session = instance.sessions[id]
		instance.access.Unlock()
		if session == nil {
			return false
		}
		session.targetMu.Lock()
		value := session.target
		session.targetMu.Unlock()
		var ok bool
		target, ok = value.(*udpTarget)
		return ok
	})
	return session, target
}

func datagramTargetState(target *udpTarget) (closed bool, sockets int) {
	target.access.Lock()
	closed = target.closed
	sockets = len(target.sockets)
	target.access.Unlock()
	return
}

func processFDCounts() (fds, sockets int, supported bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, 0, false
	}
	for _, entry := range entries {
		fds++
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && len(target) >= len("socket:[") && target[:len("socket[")] == "socket[" {
			sockets++
		}
	}
	return fds, sockets, true
}

func TestDatagramDoneReleasesTargetAndRejectsResurrection(t *testing.T) {
	cfg := testConfig()
	cfg.UDP.IdleTimeout = Duration(5 * time.Second)
	cfg.RecoveryTimeout = Duration(100 * time.Millisecond)
	instance := startTestServer(t, cfg)
	targetAddress := startUDPEcho(t, "udp4", "127.0.0.1")
	client, password := datagramClient(t, smp3core.DatagramAdaptive)
	var id smp3core.SessionID
	id[0] = 0xe1
	leg := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, targetAddress, 1)
	defer leg.Close()
	if err := client.AttachLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	datagramRoundTrip(t, client, targetAddress, []byte("target-release"))
	session, target := datagramTargetFor(t, instance, id)
	waitFor(t, time.Second, func() bool {
		_, sockets := datagramTargetState(target)
		return sockets == 1
	})
	waitFor(t, 8*time.Second, func() bool {
		closed, sockets := datagramTargetState(target)
		return closed && sockets == 0 && instance.SessionCount() == 0
	})
	session.waitWorkers()
	closed, sockets := datagramTargetState(target)
	if !closed || sockets != 0 {
		t.Fatalf("target remained live after Core.Done: closed=%t sockets=%d", closed, sockets)
	}
	udpAddress, err := net.ResolveUDPAddr("udp4", targetAddress)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.write([]byte("late"), udpAddress, time.Now().Add(time.Second)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late target write resurrected resources: %v", err)
	}
	closed, sockets = datagramTargetState(target)
	if !closed || sockets != 0 {
		t.Fatalf("late target write changed terminal state: closed=%t sockets=%d", closed, sockets)
	}
}

func TestDatagramDualFamilyAndDomainTargetRelease(t *testing.T) {
	cfg := testConfig()
	cfg.UDP.IdleTimeout = Duration(5 * time.Second)
	cfg.RecoveryTimeout = Duration(100 * time.Millisecond)
	instance := startTestServer(t, cfg)
	ipv4 := startUDPEcho(t, "udp4", "127.0.0.1")
	ipv6 := startUDPEcho(t, "udp6", "::1")
	domain := net.JoinHostPort("localhost", mustPort(t, ipv4))
	client, password := datagramClient(t, smp3core.DatagramAdaptive)
	var id smp3core.SessionID
	id[0] = 0xe2
	leg := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, ipv4, 2)
	defer leg.Close()
	if err := client.AttachLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	datagramRoundTrip(t, client, ipv4, []byte("ipv4"))
	datagramRoundTrip(t, client, domain, []byte("domain"))
	datagramRoundTrip(t, client, ipv6, []byte("ipv6"))
	session, target := datagramTargetFor(t, instance, id)
	waitFor(t, time.Second, func() bool {
		_, sockets := datagramTargetState(target)
		return sockets == 2
	})
	waitFor(t, 8*time.Second, func() bool {
		closed, sockets := datagramTargetState(target)
		return closed && sockets == 0 && instance.SessionCount() == 0
	})
	session.waitWorkers()
	closed, sockets := datagramTargetState(target)
	if !closed || sockets != 0 {
		t.Fatalf("dual-family target cleanup failed: closed=%t sockets=%d", closed, sockets)
	}
}

func TestDatagramTargetCloseCreationRace(t *testing.T) {
	session := &serverSession{}
	target := &udpTarget{server: &Server{logger: loggerOrDefault(nil)}, session: session, maxSize: 16384, sockets: make(map[string]net.PacketConn)}
	address, err := net.ResolveUDPAddr("udp4", "127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if index%2 == 0 {
				_ = target.Close()
				return
			}
			_ = target.write([]byte("race"), address, time.Now().Add(time.Second))
		}(i)
	}
	wg.Wait()
	_ = target.Close()
	session.waitWorkers()
	closed, sockets := datagramTargetState(target)
	if !closed || sockets != 0 {
		t.Fatalf("target close/create race resurrected socket: closed=%t sockets=%d", closed, sockets)
	}
}

func TestDatagramServerCloseReleasesTarget(t *testing.T) {
	instance := startTestServer(t, testConfig())
	targetAddress := startUDPEcho(t, "udp4", "127.0.0.1")
	client, password := datagramClient(t, smp3core.DatagramAdaptive)
	var id smp3core.SessionID
	id[0] = 0xe3
	leg := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, targetAddress, 3)
	defer leg.Close()
	if err := client.AttachLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	datagramRoundTrip(t, client, targetAddress, []byte("server-close"))
	_, target := datagramTargetFor(t, instance, id)
	waitFor(t, time.Second, func() bool {
		_, sockets := datagramTargetState(target)
		return sockets == 1
	})
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	closed, sockets := datagramTargetState(target)
	if !closed || sockets != 0 {
		t.Fatalf("Server.Close did not release target: closed=%t sockets=%d", closed, sockets)
	}
}

func TestDatagramZeroLegRecoveryReleasesTarget(t *testing.T) {
	cfg := testConfig()
	cfg.RecoveryTimeout = Duration(100 * time.Millisecond)
	instance := startTestServer(t, cfg)
	targetAddress := startUDPEcho(t, "udp4", "127.0.0.1")
	client, password := datagramClient(t, smp3core.DatagramAdaptive)
	var id smp3core.SessionID
	id[0] = 0xe4
	leg := connectLeg(t, instance, password, id, 0, smp3core.ModeDatagram, targetAddress, 4)
	if err := client.AttachLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	datagramRoundTrip(t, client, targetAddress, []byte("zero-leg"))
	session, target := datagramTargetFor(t, instance, id)
	waitFor(t, time.Second, func() bool {
		_, sockets := datagramTargetState(target)
		return sockets == 1
	})
	if err := leg.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		closed, sockets := datagramTargetState(target)
		return closed && sockets == 0 && instance.SessionCount() == 0
	})
	session.waitWorkers()
}

func TestDatagramTargetChurnReleasesSockets(t *testing.T) {
	cfg := testConfig()
	cfg.UDP.IdleTimeout = Duration(5 * time.Second)
	cfg.RecoveryTimeout = Duration(20 * time.Millisecond)
	instance := startTestServer(t, cfg)
	targetAddress := startUDPEcho(t, "udp4", "127.0.0.1")
	fdBefore, socketFDsBefore, fdSupported := processFDCounts()
	if fdSupported {
		t.Logf("FD_BEFORE=%d SOCKET_FD_BEFORE=%d", fdBefore, socketFDsBefore)
	}
	for index := 0; index < 1000; index++ {
		client, password := datagramClient(t, smp3core.DatagramAdaptive)
		var id smp3core.SessionID
		binaryID := uint16(index)
		id[0] = 0xf0 + byte(binaryID>>8)
		id[1] = byte(binaryID)
		var nonce [16]byte
		nonce[0] = byte(index)
		nonce[1] = byte(index >> 8)
		leg := connectLegNonce(t, instance, password, id, 0, smp3core.ModeDatagram, targetAddress, nonce)
		if err := client.AttachLeg(0, leg, nil); err != nil {
			t.Fatal(err)
		}
		datagramRoundTrip(t, client, targetAddress, []byte("churn"))
		session, target := datagramTargetFor(t, instance, id)
		waitFor(t, time.Second, func() bool {
			_, sockets := datagramTargetState(target)
			return sockets == 1
		})
		_ = leg.Close()
		_ = client.Close()
		waitFor(t, time.Second, func() bool {
			closed, sockets := datagramTargetState(target)
			return closed && sockets == 0 && instance.SessionCount() == 0
		})
		session.waitWorkers()
	}
	if count := instance.SessionCount(); count != 0 {
		t.Fatalf("churn left sessions: %d", count)
	}
	fdAfter, socketFDsAfter, fdSupported := processFDCounts()
	if fdSupported {
		t.Logf("FD_AFTER=%d SOCKET_FD_AFTER=%d", fdAfter, socketFDsAfter)
		if fdAfter > fdBefore+16 || socketFDsAfter > socketFDsBefore+8 {
			t.Fatalf("churn left residual process resources: fd delta=%d socket fd delta=%d", fdAfter-fdBefore, socketFDsAfter-socketFDsBefore)
		}
	}
}

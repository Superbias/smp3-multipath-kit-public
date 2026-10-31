package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func phase32ServerClientConfig(legs int) smp3core.StreamConfig {
	return smp3core.StreamConfig{
		SchedulerMode:     smp3core.StreamSchedulerAggregation,
		CapacityMode:      smp3core.StreamCapacityDynamic,
		MaxActiveLegs:     legs,
		ChunkSize:         16 * 1024,
		QueueFrames:       64,
		MaxReorderFrames:  512,
		MaxInflightFrames: 256,
		AckInterval:       2 * time.Millisecond,
		RetransmitTimeout: time.Second,
		RecoveryTimeout:   5 * time.Second,
		BandwidthMbps:     make([]uint32, legs),
	}
}

func phase32WaitClientACK(t *testing.T, engine *smp3core.StreamEngine, want uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if engine.Snapshot().TxAckedUsefulTotal >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("client ACK=%d, want %d", engine.Snapshot().TxAckedUsefulTotal, want)
}

func TestPhase32LandingNLegsJoinOneListenerAndContributeUniqueData(t *testing.T) {
	for _, legCount := range []int{3, 4, 8, 16} {
		t.Run(fmt.Sprintf("N-%d", legCount), func(t *testing.T) {
			target := startTCPEcho(t)
			cfg := testConfig()
			cfg.UDP.Enabled = false
			cfg.Telemetry.Enabled = true
			cfg.Telemetry.Listen = "127.0.0.1:0"
			cfg.Stream = StreamOptions{
				SchedulerMode: "aggregation", CapacityMode: "dynamic", MaxActiveLegs: legCount,
				ChunkSize: 16 * 1024, QueueFrames: 64, MaxReorderFrames: 512, MaxInflightFrames: 256,
				AckInterval: Duration(2 * time.Millisecond), RetransmitTimeout: Duration(time.Second),
				BandwidthMbps: make([]uint32, legCount),
			}
			for id := range cfg.Stream.BandwidthMbps {
				cfg.Stream.BandwidthMbps[id] = 100
			}
			landing := startTestServer(t, cfg)
			coreConfig := phase32ServerClientConfig(legCount)
			for id := range coreConfig.BandwidthMbps {
				coreConfig.BandwidthMbps[id] = 100
			}
			client, app := smp3core.NewStreamEngine(coreConfig)
			t.Cleanup(func() { _ = client.Close(); _ = app.Close() })
			var sessionID smp3core.SessionID
			sessionID[0] = byte(0x20 + legCount)
			endpoint := landing.String()
			connections := make([]net.Conn, legCount)
			for id := 0; id < legCount; id++ {
				connections[id] = connectLeg(t, landing, localTestCredential, sessionID, uint8(id), smp3core.ModeStream, target, byte(80+id))
				if err := client.AttachLeg(smp3core.LegID(id), connections[id], nil); err != nil {
					t.Fatalf("client attach leg %d: %v", id, err)
				}
				// The v4 fixture has no READY round trip. Wait until Landing commits
				// this leg before starting the next handshake so pending reservations
				// do not make the N=16 cap depend on goroutine scheduling.
				waitFor(t, 5*time.Second, func() bool {
					landing.access.Lock()
					defer landing.access.Unlock()
					session := landing.sessions[sessionID]
					return session != nil && session.stream != nil && session.stream.LegCount() == id+1
				})
			}
			t.Cleanup(func() {
				for _, conn := range connections {
					if conn != nil {
						_ = conn.Close()
					}
				}
			})
			waitFor(t, 5*time.Second, func() bool { return landing.SessionCount() == 1 })
			var session *serverSession
			waitFor(t, 5*time.Second, func() bool {
				landing.access.Lock()
				session = landing.sessions[sessionID]
				landing.access.Unlock()
				return session != nil && session.stream != nil && session.stream.LegCount() == legCount
			})
			payload := make([]byte, 4<<20)
			for i := range payload {
				payload[i] = byte((i*31 + i/113 + legCount) % 251)
			}
			wantSHA := sha256.Sum256(payload)
			writeDone := make(chan error, 1)
			go func() {
				n, err := app.Write(payload)
				if err == nil && n != len(payload) {
					err = io.ErrShortWrite
				}
				writeDone <- err
			}()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(app, got); err != nil {
				t.Fatalf("echo read: %v", err)
			}
			if !bytes.Equal(got, payload) || sha256.Sum256(got) != wantSHA {
				t.Fatal("same-session N-leg payload mismatch")
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("application write: %v", err)
			}
			phase32WaitClientACK(t, client, uint64(len(payload)))
			waitFor(t, 5*time.Second, func() bool { return session.stream.Snapshot().RxDeliveredBytes >= uint64(len(payload)) })
			clientStats := client.Snapshot()
			serverStats := session.stream.Snapshot()
			perLeg := make([]uint64, legCount)
			for id := 0; id < legCount; id++ {
				if !clientStats.Legs[id].Up || !serverStats.Legs[id].Up {
					t.Fatalf("leg %d not up client/server=%t/%t", id, clientStats.Legs[id].Up, serverStats.Legs[id].Up)
				}
				if clientStats.Legs[id].TxAckedUsefulBytes == 0 || serverStats.Legs[id].RxUniqueBytes == 0 {
					t.Fatalf("leg %d has no unique contribution txAck=%d serverRx=%d", id, clientStats.Legs[id].TxAckedUsefulBytes, serverStats.Legs[id].RxUniqueBytes)
				}
				perLeg[id] = serverStats.Legs[id].RxUniqueBytes
			}
			if clientStats.RxPendingFrames != 0 || clientStats.RxPendingBytes != 0 || clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 {
				t.Fatalf("client ledgers not empty: tx=%d/%d rx=%d/%d", clientStats.OutstandingFrames, clientStats.OutstandingBytes, clientStats.RxPendingFrames, clientStats.RxPendingBytes)
			}
			if serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
				t.Fatal("Landing RX pending ledger not empty")
			}
			telemetry := landing.telemetry.Snapshot()
			if len(telemetry.NLegs) < legCount || len(telemetry.Sessions) != 1 || len(telemetry.Sessions[0].NLegs) < legCount || len(telemetry.Sessions[0].Transport.NLegs) < legCount {
				t.Fatalf("Landing telemetry did not expose all N legs: aggregates=%d sessions=%d", len(telemetry.NLegs), len(telemetry.Sessions))
			}
			if landing.SessionCount() != 1 || landing.String() != endpoint {
				t.Fatalf("session/listener changed: sessions=%d endpoint=%s/%s", landing.SessionCount(), landing.String(), endpoint)
			}
			fmt.Printf("PHASE32_LANDING_NLEG_RESULT session_id=%x legs=%d listener=%s payload_bytes=%d sha256=%x per_leg_unique=%v telemetry_legs=%d strict_echo=true tx_ledger_empty=true rx_pending_empty=true\n", sessionID, legCount, endpoint, len(payload), wantSHA, perLeg, len(telemetry.Sessions[0].NLegs))
		})
	}
}

func TestPhase32LandingNLegsJoinOneTLSListenerAndContributeUniqueData(t *testing.T) {
	const legCount = 3
	target := startTCPEcho(t)
	certPath, keyPath, certPEM := makeServerCarrierCertificate(t)
	cfg := testConfig()
	cfg.UDP.Enabled = false
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.Listen = "127.0.0.1:0"
	cfg.CarrierTLS = CarrierTLSOptions{CertFile: certPath, KeyFile: keyPath}
	cfg.Stream = StreamOptions{
		SchedulerMode: "aggregation", CapacityMode: "dynamic", MaxActiveLegs: legCount,
		ChunkSize: 16 * 1024, QueueFrames: 64, MaxReorderFrames: 512, MaxInflightFrames: 256,
		AckInterval: Duration(2 * time.Millisecond), RetransmitTimeout: Duration(time.Second),
		BandwidthMbps: []uint32{100, 100, 100},
	}
	landing := startTestServer(t, cfg)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to trust test Landing certificate")
	}
	coreConfig := phase32ServerClientConfig(legCount)
	coreConfig.BandwidthMbps = []uint32{100, 100, 100}
	client, app := smp3core.NewStreamEngine(coreConfig)
	t.Cleanup(func() { _ = client.Close(); _ = app.Close() })
	var sessionID smp3core.SessionID
	sessionID[0] = 0x33
	connections := make([]net.Conn, legCount)
	for id := 0; id < legCount; id++ {
		raw, err := net.DialTimeout("tcp", landing.String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial TLS Landing Leg%d: %v", id, err)
		}
		secured := tls.Client(raw, &tls.Config{
			MinVersion: tls.VersionTLS12, ServerName: "localhost", RootCAs: roots,
			NextProtos: []string{standaloneCarrierALPN},
		})
		if err := secured.Handshake(); err != nil {
			_ = raw.Close()
			t.Fatalf("TLS handshake Leg%d: %v", id, err)
		}
		if got := secured.ConnectionState().NegotiatedProtocol; got != standaloneCarrierALPN {
			_ = secured.Close()
			t.Fatalf("Leg%d negotiated ALPN=%q, want %q", id, got, standaloneCarrierALPN)
		}
		writeHello(t, secured, localTestCredential, sessionID, uint8(id), smp3core.ModeStream, target, byte(160+id))
		if err := client.AttachLeg(smp3core.LegID(id), secured, nil); err != nil {
			_ = secured.Close()
			t.Fatalf("attach TLS client Leg%d: %v", id, err)
		}
		connections[id] = secured
	}
	t.Cleanup(func() {
		for _, conn := range connections {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})
	waitFor(t, 5*time.Second, func() bool {
		landing.access.Lock()
		defer landing.access.Unlock()
		session := landing.sessions[sessionID]
		return session != nil && session.stream != nil && session.stream.LegCount() == legCount
	})
	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte((i*29 + i/109 + 3) % 251)
	}
	wantSHA := sha256.Sum256(payload)
	writeDone := make(chan error, 1)
	go func() {
		n, err := app.Write(payload)
		if err == nil && n != len(payload) {
			err = io.ErrShortWrite
		}
		writeDone <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatalf("TLS N-leg echo read: %v", err)
	}
	if !bytes.Equal(got, payload) || sha256.Sum256(got) != wantSHA {
		t.Fatal("TLS N-leg payload content/SHA mismatch")
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("TLS N-leg app write: %v", err)
	}
	phase32WaitClientACK(t, client, uint64(len(payload)))
	landing.access.Lock()
	session := landing.sessions[sessionID]
	landing.access.Unlock()
	if session == nil || session.stream == nil {
		t.Fatal("TLS N-leg session disappeared before validation")
	}
	serverStats := session.stream.Snapshot()
	clientStats := client.Snapshot()
	for id := 0; id < legCount; id++ {
		if serverStats.Legs[id].RxUniqueBytes == 0 || clientStats.Legs[id].TxAckedUsefulBytes == 0 {
			t.Fatalf("TLS Leg%d has no unique DATA contribution, rx=%d ack=%d", id, serverStats.Legs[id].RxUniqueBytes, clientStats.Legs[id].TxAckedUsefulBytes)
		}
	}
	if clientStats.TxAckedUsefulTotal != uint64(len(payload)) || clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 {
		t.Fatalf("TLS client ACK/ledger mismatch: ack=%d outstanding=%d/%d", clientStats.TxAckedUsefulTotal, clientStats.OutstandingFrames, clientStats.OutstandingBytes)
	}
	if serverStats.RxDeliveredBytes != uint64(len(payload)) || serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
		t.Fatalf("TLS Landing receive/ledger mismatch: delivered=%d pending=%d/%d", serverStats.RxDeliveredBytes, serverStats.RxPendingFrames, serverStats.RxPendingBytes)
	}
	if landing.SessionCount() != 1 {
		t.Fatalf("TLS carriers were not bound to one Landing session: %d", landing.SessionCount())
	}
	telemetry := landing.telemetry.Snapshot()
	if len(telemetry.Sessions) != 1 || telemetry.Sessions[0].Transport == nil || len(telemetry.Sessions[0].Transport.NLegs) != legCount {
		t.Fatalf("TLS N-leg transport detail was not exported: sessions=%d", len(telemetry.Sessions))
	}
	for id, leg := range telemetry.Sessions[0].Transport.NLegs {
		if int(leg.LegID) != id || leg.Generation == 0 || !leg.Up || leg.UniqueRXBytes == 0 || leg.DataRXUniqueFrames == 0 || leg.CarrierWireBytes == 0 {
			t.Fatalf("TLS N-leg telemetry is incomplete or misbound at id=%d: %+v", id, leg)
		}
	}
	fmt.Printf("PHASE32_LANDING_TLS_NLEG_RESULT session_id=%x legs=%d listener=%s payload_bytes=%d sha256=%x per_leg_unique_bytes=[%d %d %d] strict_echo=true ack_full=true tx_ledger_empty=true rx_pending_empty=true sessions=1 alpn=%s\n", sessionID, legCount, landing.String(), len(payload), wantSHA, serverStats.Legs[0].RxUniqueBytes, serverStats.Legs[1].RxUniqueBytes, serverStats.Legs[2].RxUniqueBytes, standaloneCarrierALPN)
}

func TestPhase32LandingHotAddLegsToRunningSession(t *testing.T) {
	const maxLegs = 4
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.UDP.Enabled = false
	cfg.Stream.SchedulerMode = "aggregation"
	cfg.Stream.CapacityMode = "dynamic"
	cfg.Stream.MaxActiveLegs = maxLegs
	cfg.Stream.BandwidthMbps = []uint32{100, 100, 100, 100}
	landing := startTestServer(t, cfg)
	coreConfig := phase32ServerClientConfig(maxLegs)
	coreConfig.BandwidthMbps = []uint32{100, 100, 100, 100}
	client, app := smp3core.NewStreamEngine(coreConfig)
	t.Cleanup(func() { _ = client.Close(); _ = app.Close() })
	var sessionID smp3core.SessionID
	sessionID[0] = 0x34
	connections := make([]net.Conn, 0, maxLegs)
	attach := func(id int) {
		t.Helper()
		conn := connectLeg(t, landing, localTestCredential, sessionID, uint8(id), smp3core.ModeStream, target, byte(180+id))
		if err := client.AttachLeg(smp3core.LegID(id), conn, nil); err != nil {
			_ = conn.Close()
			t.Fatalf("attach runtime Leg%d: %v", id, err)
		}
		connections = append(connections, conn)
	}
	t.Cleanup(func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	})
	attach(0)
	attach(1)
	var originalSession *serverSession
	waitFor(t, 5*time.Second, func() bool {
		landing.access.Lock()
		defer landing.access.Unlock()
		originalSession = landing.sessions[sessionID]
		return originalSession != nil && originalSession.stream != nil && originalSession.stream.LegCount() == 2
	})
	phase31Payload := bytes.Repeat([]byte{0x31}, 1<<20)
	streamRoundTrip(t, app, phase31Payload)
	phase32WaitClientACK(t, client, uint64(len(phase31Payload)))
	for id := 2; id < maxLegs; id++ {
		attach(id)
	}
	waitFor(t, 5*time.Second, func() bool {
		landing.access.Lock()
		defer landing.access.Unlock()
		current := landing.sessions[sessionID]
		return current == originalSession && current.stream != nil && current.stream.LegCount() == maxLegs
	})
	newPayload := make([]byte, 4<<20)
	for i := range newPayload {
		newPayload[i] = byte((i*13 + i/97 + 4) % 251)
	}
	wantSHA := sha256.Sum256(newPayload)
	streamRoundTrip(t, app, newPayload)
	phase32WaitClientACK(t, client, uint64(len(phase31Payload)+len(newPayload)))
	clientStats := client.Snapshot()
	landing.access.Lock()
	current := landing.sessions[sessionID]
	landing.access.Unlock()
	if current != originalSession || landing.SessionCount() != 1 {
		t.Fatal("runtime Leg Hot Add replaced or split the existing Session")
	}
	serverStats := current.stream.Snapshot()
	for id := 0; id < maxLegs; id++ {
		if clientStats.Legs[id].TxAckedUsefulBytes == 0 || serverStats.Legs[id].RxUniqueBytes == 0 {
			t.Fatalf("hot-added N=%d Leg%d lacks unique DATA contribution: ack=%d rx=%d", maxLegs, id, clientStats.Legs[id].TxAckedUsefulBytes, serverStats.Legs[id].RxUniqueBytes)
		}
	}
	if clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 || serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
		t.Fatalf("ledger not empty after hot add: tx=%d/%d rx=%d/%d", clientStats.OutstandingFrames, clientStats.OutstandingBytes, serverStats.RxPendingFrames, serverStats.RxPendingBytes)
	}
	fmt.Printf("PHASE32_LANDING_HOT_ADD_RESULT session_id=%x initial_legs=2 final_legs=%d session_identity_preserved=true listener=%s new_payload_bytes=%d new_payload_sha256=%x per_leg_unique_total=[%d %d %d %d] ack_total=%d tx_ledger_empty=true rx_pending_empty=true\n", sessionID, current.stream.LegCount(), landing.String(), len(newPayload), wantSHA, serverStats.Legs[0].RxUniqueBytes, serverStats.Legs[1].RxUniqueBytes, serverStats.Legs[2].RxUniqueBytes, serverStats.Legs[3].RxUniqueBytes, clientStats.TxAckedUsefulTotal)
}

func TestPhase32LandingSessionIsolationForNLegSessions(t *testing.T) {
	const legCount = 3
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.UDP.Enabled = false
	cfg.Stream.MaxActiveLegs = legCount
	cfg.Stream.SchedulerMode = "aggregation"
	cfg.Stream.CapacityMode = "dynamic"
	cfg.Stream.BandwidthMbps = []uint32{100, 100, 100}
	landing := startTestServer(t, cfg)
	clients := make([]*smp3core.StreamEngine, 2)
	apps := make([]net.Conn, 2)
	sessions := make([]*serverSession, 2)
	connections := make([]net.Conn, 0, 2*legCount)
	t.Cleanup(func() {
		for i := range clients {
			if clients[i] != nil {
				_ = clients[i].Close()
				_ = apps[i].Close()
			}
		}
		for _, c := range connections {
			_ = c.Close()
		}
	})
	for s := 0; s < 2; s++ {
		engine, app := smp3core.NewStreamEngine(phase32ServerClientConfig(legCount))
		clients[s], apps[s] = engine, app
		var sid smp3core.SessionID
		sid[0] = byte(0x41 + s)
		for id := 0; id < legCount; id++ {
			conn := connectLeg(t, landing, localTestCredential, sid, uint8(id), smp3core.ModeStream, target, byte(100+s*legCount+id))
			connections = append(connections, conn)
			if err := engine.AttachLeg(smp3core.LegID(id), conn, nil); err != nil {
				t.Fatalf("session %d leg %d attach: %v", s, id, err)
			}
		}
		var session *serverSession
		waitFor(t, 3*time.Second, func() bool {
			landing.access.Lock()
			session = landing.sessions[sid]
			landing.access.Unlock()
			return session != nil && session.stream != nil && session.stream.LegCount() == legCount
		})
		sessions[s] = session
		if sessions[s] == nil || sessions[s].stream.LegCount() != legCount {
			t.Fatalf("session %d did not bind all N legs", s)
		}
	}
	if landing.SessionCount() != 2 || sessions[0] == sessions[1] || sessions[0].stream == sessions[1].stream {
		t.Fatal("distinct session IDs were merged")
	}
	for s := 0; s < 2; s++ {
		payload := bytes.Repeat([]byte{byte(0x50 + s)}, 1<<20)
		streamRoundTrip(t, apps[s], payload)
	}
	if landing.SessionCount() != 2 {
		t.Fatalf("session count after independent payloads=%d", landing.SessionCount())
	}
}

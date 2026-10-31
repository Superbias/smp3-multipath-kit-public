package server

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestStandaloneServerV7CoordinatesDrainAcrossTCPCarriers(t *testing.T) {
	type eventCollector struct {
		sync.Mutex
		events []smp3core.StreamDrainEvent
	}
	var clientEvents, landingEvents eventCollector
	clientObserver := func(event smp3core.StreamDrainEvent) {
		clientEvents.Lock()
		clientEvents.events = append(clientEvents.events, event)
		clientEvents.Unlock()
	}
	landingObserver := func(event smp3core.StreamDrainEvent) {
		landingEvents.Lock()
		landingEvents.events = append(landingEvents.events, event)
		landingEvents.Unlock()
	}
	target := startDrainEchoTarget(t)
	cfg := testConfig()
	cfg.Stream.ExperimentalPathAware = false
	cfg.Stream.MaxActiveLegs = 2
	cfg.drainEventObserver = landingObserver
	instance := startTestServer(t, cfg)

	clientCore, app := smp3core.NewStreamEngine(smp3core.StreamConfig{
		SchedulerMode:      smp3core.StreamSchedulerAggregation,
		CapacityMode:       smp3core.StreamCapacityDynamic,
		PathRole:           smp3core.StreamPathRoleClient,
		ChunkSize:          1024,
		QueueFrames:        32,
		BandwidthMbps:      []uint32{1, 1},
		MaxActiveLegs:      2,
		MaxReorderFrames:   128,
		MaxInflightFrames:  64,
		AckInterval:        20 * time.Millisecond,
		RetransmitTimeout:  time.Second,
		RecoveryTimeout:    2 * time.Second,
		DrainEventObserver: clientObserver,
	})
	t.Cleanup(func() { _ = clientCore.Close(); _ = app.Close() })

	var sessionID smp3core.SessionID
	if _, err := cryptorand.Read(sessionID[:]); err != nil {
		t.Fatal(err)
	}
	for id := uint8(0); id < 2; id++ {
		conn, err := net.DialTimeout("tcp", instance.Addr().String(), time.Second)
		if err != nil {
			t.Fatalf("dial carrier leg%d: %v", id, err)
		}
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		var nonce [16]byte
		if _, err := cryptorand.Read(nonce[:]); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		hello := smp3core.Hello{
			Version: smp3core.Version7, Capabilities: smp3core.StreamCapabilityCoordinatedDrain,
			SessionID: sessionID, LegID: smp3core.LegID(id), Mode: smp3core.ModeStream,
			Timestamp: time.Now().Unix(), Nonce: nonce, Destination: target.Addr().String(),
		}
		header, destination, mac, err := smp3core.EncodeHelloParts(hello, []byte(cfg.Password))
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		for _, part := range [][]byte{header, destination, mac} {
			if err := writeDrainTestBytes(conn, part); err != nil {
				_ = conn.Close()
				t.Fatalf("write HELLO leg%d: %v", id, err)
			}
		}
		_, auth, err := smp3core.ReadStreamReadyV2(conn, hello, []byte(cfg.Password))
		if err != nil {
			_ = conn.Close()
			t.Fatalf("read V7 READY leg%d: %v", id, err)
		}
		if !auth.CoordinatedDrainEnabled() || auth.PathRTTEnabled() {
			_ = conn.Close()
			t.Fatalf("server selected unexpected V7 capabilities on leg%d: drain=%t pathRTT=%t", id, auth.CoordinatedDrainEnabled(), auth.PathRTTEnabled())
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		if err := clientCore.AttachLegWithProtocolAuth(smp3core.LegID(id), conn, nil, auth); err != nil {
			_ = conn.Close()
			t.Fatalf("attach authenticated V7 carrier leg%d: %v", id, err)
		}
	}

	var landing *serverSession
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		instance.access.Lock()
		landing = instance.sessions[sessionID]
		instance.access.Unlock()
		if landing != nil && landing.stream != nil && landing.stream.LegCount() == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if landing == nil || landing.stream == nil || landing.stream.LegCount() != 2 {
		t.Fatal("Standalone server did not attach both authenticated V7 carriers to one Core session")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := clientCore.DrainLeg(ctx, 1); err != nil {
		clientEvents.Lock()
		clientJSON, _ := json.Marshal(struct {
			RunID    string                      `json:"run_id"`
			Endpoint string                      `json:"endpoint"`
			Events   []smp3core.StreamDrainEvent `json:"events"`
		}{fmt.Sprintf("%x", sessionID[:]), "client->" + instance.Addr().String(), append([]smp3core.StreamDrainEvent(nil), clientEvents.events...)})
		clientEvents.Unlock()
		landingEvents.Lock()
		landingJSON, _ := json.Marshal(struct {
			RunID    string                      `json:"run_id"`
			Endpoint string                      `json:"endpoint"`
			Events   []smp3core.StreamDrainEvent `json:"events"`
		}{fmt.Sprintf("%x", sessionID[:]), "landing@" + instance.Addr().String(), append([]smp3core.StreamDrainEvent(nil), landingEvents.events...)})
		landingEvents.Unlock()
		t.Fatalf("V7 coordinated drain over real TCP carriers: %v; client_drain_events=%s landing_drain_events=%s",
			err, clientJSON, landingJSON)
	}
	retireDeadline := time.Now().Add(time.Second)
	for time.Now().Before(retireDeadline) && (clientCore.HasLeg(1) || landing.stream.HasLeg(1)) {
		time.Sleep(time.Millisecond)
	}
	if err := clientCore.WaitLegRetired(ctx, 1); err != nil {
		t.Fatalf("client worker retirement: %v", err)
	}
	if err := landing.stream.WaitLegRetired(ctx, 1); err != nil {
		t.Fatalf("Landing worker retirement: %v", err)
	}
	if clientCore.LegCount() != 1 || landing.stream.LegCount() != 1 || clientCore.HasLeg(1) || landing.stream.HasLeg(1) {
		t.Fatalf("both Cores did not retire the same V7 carrier: client=%d/%t Landing=%d/%t",
			clientCore.LegCount(), clientCore.HasLeg(1), landing.stream.LegCount(), landing.stream.HasLeg(1))
	}
}

func startDrainEchoTarget(t *testing.T) net.Listener {
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
			go func(conn net.Conn) {
				defer conn.Close()
				buffer := make([]byte, 32*1024)
				for {
					n, readErr := conn.Read(buffer)
					if n > 0 && writeAllNoTest(conn, buffer[:n]) != nil {
						return
					}
					if readErr != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return listener
}

func writeDrainTestBytes(conn net.Conn, p []byte) error {
	for len(p) > 0 {
		n, err := conn.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

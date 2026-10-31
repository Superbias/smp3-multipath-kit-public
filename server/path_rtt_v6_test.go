package server

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestStandaloneServerV6PathRTTNegotiationAndData(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-off", true: "opt-in"}[enabled], func(t *testing.T) {
			target := startPathAwareEchoTarget(t)
			cfg := testConfig()
			cfg.Stream.ExperimentalPathAware = enabled
			instance := startTestServer(t, cfg)

			conn, err := net.DialTimeout("tcp", instance.String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var sessionID smp3core.SessionID
			sessionID[0] = 0x73
			hello := smp3core.Hello{
				Version:      smp3core.Version6,
				Capabilities: smp3core.StreamCapabilityPathRTT,
				SessionID:    sessionID,
				LegID:        0,
				Mode:         smp3core.ModeStream,
				Timestamp:    time.Now().Unix(),
				Destination:  target.Addr().String(),
			}
			hello.Nonce[0] = byte(time.Now().UnixNano())
			header, destination, mac, err := smp3core.EncodeHelloParts(hello, []byte(cfg.Password))
			if err != nil {
				t.Fatal(err)
			}
			writeAll(t, conn, header)
			writeAll(t, conn, destination)
			writeAll(t, conn, mac)
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, auth, err := smp3core.ReadStreamReadyV2(conn, hello, []byte(cfg.Password))
			if err != nil {
				t.Fatalf("read authenticated V6 READY: %v", err)
			}
			if got := auth.PathRTTEnabled(); got != enabled {
				t.Fatalf("path RTT selected=%t, server opt-in=%t", got, enabled)
			}

			engineCfg := smp3core.StreamConfig{
				ExperimentalPathAware: enabled,
				PathRole:              smp3core.StreamPathRoleClient,
				SchedulerMode:         smp3core.StreamSchedulerStatic,
				BandwidthMbps:         []uint32{1, 1},
				ChunkSize:             1024,
				QueueFrames:           32,
				MaxInflightFrames:     64,
				MaxReorderFrames:      128,
				AckInterval:           20 * time.Millisecond,
				RetransmitTimeout:     time.Second,
				RecoveryTimeout:       4 * time.Second,
			}
			engine, app := smp3core.NewStreamEngine(engineCfg)
			t.Cleanup(func() { _ = engine.Close(); _ = app.Close() })
			if enabled {
				if err := engine.AttachLegWithPathAuth(0, conn, nil, auth); err != nil {
					t.Fatal(err)
				}
			} else if err := engine.AttachLeg(0, conn, nil); err != nil {
				t.Fatal(err)
			}
			if enabled {
				deadline := time.Now().Add(400 * time.Millisecond)
				for time.Now().Before(deadline) {
					if !engine.HasLeg(0) {
						t.Fatalf("same-Carrier probe exchange retired the client leg: %v", engine.CloseError())
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			payload := bytes.Repeat([]byte("standalone-v6-shared-core/"), 128)
			if err := app.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := writeAllNoTest(app, payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(app, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("V6 stream data mismatch")
			}
		})
	}
}

func startPathAwareEchoTarget(t *testing.T) net.Listener {
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
					n, err := conn.Read(buffer)
					if n > 0 {
						if writeAllNoTest(conn, buffer[:n]) != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return listener
}

func writeAllNoTest(conn net.Conn, value []byte) error {
	for len(value) > 0 {
		n, err := conn.Write(value)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		value = value[n:]
	}
	return nil
}

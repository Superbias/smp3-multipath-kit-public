//go:build sidecar_stress

package server

import (
	"net"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestSidecarRuntimeStress(t *testing.T) {
	target := startTCPEcho(t)
	cfg := testConfig()
	cfg.SidecarListeners = []string{"127.0.0.2:0", "127.0.0.3:0"}
	instance := startTestServer(t, cfg)
	addresses := instance.ListenerAddrs()

	for i := 0; i < 500; i++ {
		var id smp3core.SessionID
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		var nonce [16]byte
		nonce[0] = byte(i)
		nonce[1] = byte(i >> 8)
		conn := connectSidecarAndReadReady(t, addresses[1], cfg.Password, sidecarTestHello(id, 0, smp3core.ModeStream, target, nonce))
		_ = conn.Close()
	}

	for i := 0; i < 100; i++ {
		var id smp3core.SessionID
		id[0] = 0x22
		id[1] = byte(i)
		id[2] = 1
		var nonce0, nonce1 [16]byte
		nonce0[0], nonce1[0] = byte(i), byte(i)
		nonce0[1], nonce1[1] = 1, 2
		nonce0[2], nonce1[2] = 1, 1
		leg0 := connectSidecarAndReadReady(t, addresses[1], cfg.Password, sidecarTestHello(id, 0, smp3core.ModeStream, target, nonce0))
		leg1 := connectSidecarAndReadReady(t, addresses[2], cfg.Password, sidecarTestHello(id, 1, smp3core.ModeStream, target, nonce1))
		_ = leg0.Close()
		_ = leg1.Close()
	}

	for i := 0; i < 500; i++ {
		var id smp3core.SessionID
		id[0] = byte(0x40 + i)
		id[1] = byte(i >> 8)
		conn, err := net.Dial("tcp", addresses[1])
		if err != nil {
			t.Fatal(err)
		}
		writeHello(t, conn, "wrong-password", id, 0, smp3core.ModeStream, target, byte(i))
		readClosedSidecar(t, conn)
	}

	closeCfg := testConfig()
	closeCfg.SidecarListeners = []string{"127.0.0.2:0"}
	for i := 0; i < 500; i++ {
		instance, err := New(closeCfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := instance.Start(); err != nil {
			t.Fatal(err)
		}
		conn, err := net.DialTimeout("tcp", instance.ListenerAddrs()[1], time.Second)
		if err != nil {
			_ = instance.Close()
			t.Fatal(err)
		}
		_ = conn.Close()
		if err := instance.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

package client

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	serverpkg "github.com/Superbias/smp3-multipath-kit-public/server"
	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestParseSocksUDPDatagramRejectsFragment(t *testing.T) {
	packet := []byte{0, 0, 1, 1, 127, 0, 0, 1, 0, 53, 1, 2, 3}
	if _, _, err := parseSocksUDPDatagram(packet); err == nil {
		t.Fatal("fragmented UDP packet was accepted")
	}
}

func TestParseAndEncodeSocksUDPDatagramSupportsDomain(t *testing.T) {
	packet := []byte{0, 0, 0, 3, 11}
	packet = append(packet, []byte("example.com")...)
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], 53)
	packet = append(packet, port[:]...)
	packet = append(packet, 1, 2, 3)

	address, payload, err := parseSocksUDPDatagram(packet)
	if err != nil {
		t.Fatal(err)
	}
	if address != "example.com:53" || string(payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("parsed address=%q payload=%v", address, payload)
	}
	encoded, err := encodeSocksUDPDatagram(address, payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(packet) {
		t.Fatalf("encoded packet differs: %v", encoded)
	}
}

func TestSocksUDPResponseAddressUsesUDPAddr(t *testing.T) {
	encoded, err := encodeSocksUDPDatagram("[2001:db8::7]:5353", []byte("ok"))
	if err != nil {
		t.Fatal(err)
	}
	address, payload, err := parseSocksUDPDatagram(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if address != "[2001:db8::7]:5353" || string(payload) != "ok" {
		t.Fatalf("round trip address=%q payload=%q", address, payload)
	}
	if _, err := net.ResolveUDPAddr("udp", address); err != nil {
		t.Fatal("response address is not a valid UDP endpoint")
	}
}

func TestSocksUDPAssociateRoundTripsThroughStandaloneServer(t *testing.T) {
	echo, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		buffer := make([]byte, 2048)
		for {
			n, peer, err := echo.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			_, _ = echo.WriteToUDP(buffer[:n], peer)
		}
	}()

	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0", "127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = true
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	serverAddresses := standalone.ListenerAddrs()
	if len(serverAddresses) != 3 {
		t.Fatalf("server listeners = %v", serverAddresses)
	}

	forwarder := newSOCKSForwarder(t)
	defer forwarder.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = forwarder.Addr().String()
	cfg.SMP3.Routes.Leg0 = serverAddresses[1]
	cfg.SMP3.Routes.Leg1 = serverAddresses[2]
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	defer instance.Close()

	control, err := net.DialTimeout("tcp", instance.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := writeAll(control, []byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(control, method); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(control, []byte{5, socksCommandUDPAssociate, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(control, reply); err != nil {
		t.Fatal(err)
	}
	proxyUDP := &net.UDPAddr{IP: net.IPv4(reply[4], reply[5], reply[6], reply[7]), Port: int(binary.BigEndian.Uint16(reply[8:]))}
	localUDP, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer localUDP.Close()
	if err := localUDP.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := []byte("sidecar UDP round trip")
	packet, err := encodeSocksUDPDatagram(echo.LocalAddr().String(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localUDP.WriteToUDP(packet, proxyUDP); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 2048)
	n, _, err := localUDP.ReadFromUDP(response)
	if err != nil {
		t.Fatal(err)
	}
	address, got, err := parseSocksUDPDatagram(response[:n])
	if err != nil {
		t.Fatal(err)
	}
	if address != echo.LocalAddr().String() || string(got) != string(payload) {
		t.Fatalf("UDP response address=%q payload=%q", address, got)
	}
	targets := forwarder.waitForTargets(t, 2)
	seen := map[string]bool{}
	for _, target := range targets {
		seen[target] = true
	}
	if !seen[serverAddresses[1]] || !seen[serverAddresses[2]] {
		t.Fatalf("targets = %v, want both %v", targets, serverAddresses)
	}
}

func TestDatagramAssociationRecreatesTerminalEngineSerially(t *testing.T) {
	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = true
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	serverAddresses := standalone.ListenerAddrs()
	if len(serverAddresses) != 2 {
		t.Fatalf("server listeners = %v", serverAddresses)
	}

	forwarder := newSOCKSForwarder(t)
	defer forwarder.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = forwarder.Addr().String()
	cfg.SMP3.Routes.Leg0 = serverAddresses[1]
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.UDP.IdleTimeout = Duration(1 * time.Second)
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()

	control, peer := net.Pipe()
	defer peer.Close()
	association, err := newDatagramAssociation(instance, control, control)
	if err != nil {
		t.Fatal(err)
	}
	first, err := association.ensureEngine()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !engineDone(first) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !engineDone(first) {
		t.Fatal("first datagram engine did not reach idle terminal state")
	}

	const callers = 8
	engines := make(chan *smp3core.DatagramEngine, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			engine, err := association.ensureEngine()
			engines <- engine
			errs <- err
		}()
	}
	var replacement *smp3core.DatagramEngine
	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		engine := <-engines
		if replacement == nil {
			replacement = engine
		}
		if engine != replacement {
			t.Fatal("concurrent sends created more than one replacement engine")
		}
	}
	if replacement == first {
		t.Fatal("terminal engine was reused")
	}
	_ = association.Close()
}

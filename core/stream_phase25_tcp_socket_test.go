package smp3core

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net"
	"testing"
	"time"
)

type phase25TCPReadResult struct {
	data []byte
	err  error
}

func phase25ReadToEOF(conn net.Conn) phase25TCPReadResult {
	var data []byte
	buffer := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			data = append(data, buffer[:n]...)
		}
		if err != nil {
			return phase25TCPReadResult{data: data, err: err}
		}
	}
}

func phase25LoopbackTCPPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial loopback: %v", err)
	}
	var server net.Conn
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		_ = client.Close()
		t.Fatalf("accept loopback: %v", err)
	case <-time.After(5 * time.Second):
		_ = client.Close()
		t.Fatal("timed out accepting loopback TCP socket")
	}
	for name, conn := range map[string]net.Conn{"client": client, "server": server} {
		if tcp, ok := conn.(*net.TCPConn); ok {
			if err := tcp.SetNoDelay(true); err != nil {
				t.Fatalf("%s TCP_NODELAY: %v", name, err)
			}
		}
		if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatalf("%s set deadline: %v", name, err)
		}
	}
	return client, server
}

func phase25TCPConfig(candidate StreamPolicyCandidate) (StreamConfig, error) {
	cfg := phase25IdentityConfig()
	cfg.BandwidthMbps = []uint32{100, 100}
	cfg.ChunkSize = 16 * 1024
	cfg.MaxInflightFrames = 256
	if candidate == StreamPolicyB1 {
		cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300}
	}
	return cfg, nil
}

func TestPhase25CanonicalCandidatesUseTwoRealLoopbackTCPLegs(t *testing.T) {
	const payloadSize = 2 << 20
	payload := bytes.Repeat([]byte("phase25-canonical-b0-b1-tcp-socket\x00"), payloadSize/len("phase25-canonical-b0-b1-tcp-socket\x00"))
	if len(payload) != payloadSize {
		payload = append(payload, bytes.Repeat([]byte{0xA5}, payloadSize-len(payload))...)
	}
	wantSHA := sha256.Sum256(payload)

	for _, candidate := range []StreamPolicyCandidate{StreamPolicyB0, StreamPolicyB1} {
		t.Run(string(candidate), func(t *testing.T) {
			cfg, err := phase25TCPConfig(candidate)
			if err != nil {
				t.Fatal(err)
			}
			client, clientApp, err := NewStreamEngineForCandidate(cfg, candidate)
			if err != nil {
				t.Fatalf("construct %s client: %v", candidate, err)
			}
			serverCfg := cfg
			if candidate == StreamPolicyB1 {
				copyOfPolicy := *cfg.ExperimentalBoundedRescue
				serverCfg.ExperimentalBoundedRescue = &copyOfPolicy
			}
			server, serverApp, err := NewStreamEngineForCandidate(serverCfg, candidate)
			if err != nil {
				_ = client.Close()
				_ = clientApp.Close()
				t.Fatalf("construct %s server: %v", candidate, err)
			}
			defer client.Close()
			defer clientApp.Close()
			defer server.Close()
			defer serverApp.Close()

			for legID := LegID(0); legID < 2; legID++ {
				clientLeg, serverLeg := phase25LoopbackTCPPair(t)
				if err := client.AttachLeg(legID, clientLeg, nil); err != nil {
					_ = clientLeg.Close()
					_ = serverLeg.Close()
					t.Fatalf("attach client leg %d: %v", legID, err)
				}
				if err := server.AttachLeg(legID, serverLeg, nil); err != nil {
					_ = serverLeg.Close()
					t.Fatalf("attach server leg %d: %v", legID, err)
				}
			}
			client.SetActiveForTest(true)
			server.SetActiveForTest(true)

			serverRead := make(chan phase25TCPReadResult, 1)
			clientRead := make(chan phase25TCPReadResult, 1)
			go func() { serverRead <- phase25ReadToEOF(serverApp) }()
			go func() { clientRead <- phase25ReadToEOF(clientApp) }()
			if err := writeAll(clientApp, payload); err != nil {
				t.Fatalf("write application payload: %v", err)
			}
			client.StartGracefulClose(io.EOF)

			var received phase25TCPReadResult
			select {
			case received = <-serverRead:
			case <-time.After(25 * time.Second):
				t.Fatal("server application did not reach EOF within 25 seconds")
			}
			if received.err != io.EOF {
				t.Fatalf("server application termination = %T %v, want normal EOF", received.err, received.err)
			}
			if len(received.data) != len(payload) {
				t.Fatalf("server received %d bytes, want %d", len(received.data), len(payload))
			}
			gotSHA := sha256.Sum256(received.data)
			if gotSHA != wantSHA {
				t.Fatalf("server application SHA-256 mismatch: got=%x want=%x", gotSHA, wantSHA)
			}
			select {
			case got := <-clientRead:
				if got.err != io.EOF || len(got.data) != 0 {
					t.Fatalf("client application termination/data = %+v, want EOF with no response payload", got)
				}
			case <-time.After(25 * time.Second):
				t.Fatal("client application did not receive peer EOF within 25 seconds")
			}

			tx := client.Snapshot()
			rx := server.Snapshot()
			if tx.OutstandingFrames != 0 || tx.OutstandingBytes != 0 || rx.RxPendingFrames != 0 || rx.RxPendingBytes != 0 {
				t.Fatalf("ledger not drained after TCP EOF: tx_out=%d/%d rx_pending=%d/%d", tx.OutstandingFrames, tx.OutstandingBytes, rx.RxPendingFrames, rx.RxPendingBytes)
			}
			if tx.TxAckedUsefulTotal != payloadSize || rx.RxDeliveredBytes != payloadSize {
				t.Fatalf("ACK/application totals mismatch: ack=%d delivered=%d payload=%d", tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, payloadSize)
			}
			if tx.TxWrittenBytesByLeg[0] == 0 || tx.TxWrittenBytesByLeg[1] == 0 || rx.RxUniqueBytesByLeg[0] == 0 || rx.RxUniqueBytesByLeg[1] == 0 {
				t.Fatalf("TCP transfer did not prove two-leg participation: tx=%v rx=%v", tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg)
			}
			if got := client.RuntimePolicyIdentity(); got.Candidate != candidate {
				t.Fatalf("client policy identity changed during transfer: %+v", got)
			}
			t.Logf("candidate=%s tcp_legs=2 payload=%d ack=%d delivered=%d sha256=%x tx_by_leg=%v rx_unique_by_leg=%v eof=io.EOF tx_ledger=0 rx_pending=0", candidate, payloadSize, tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, gotSHA, tx.TxWrittenBytesByLeg, rx.RxUniqueBytesByLeg)
		})
	}
}

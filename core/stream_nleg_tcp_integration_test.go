package smp3core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type phase32NLegDataEvent struct {
	LegID    uint8
	Sequence uint64
	At       time.Time
}

type phase32NLegTraceConn struct {
	net.Conn
	legID     uint8
	mu        sync.Mutex
	header    [StreamFrameHeaderSize]byte
	headerN   int
	remaining uint32
	events    []phase32NLegDataEvent
}

func (c *phase32NLegTraceConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.mu.Lock()
		c.consumeWritten(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func (c *phase32NLegTraceConn) consumeWritten(data []byte) {
	for len(data) > 0 {
		if c.remaining > 0 {
			n := len(data)
			if uint32(n) > c.remaining {
				n = int(c.remaining)
			}
			c.remaining -= uint32(n)
			data = data[n:]
			if c.remaining == 0 {
				c.headerN = 0
			}
			continue
		}

		n := copy(c.header[c.headerN:], data)
		c.headerN += n
		data = data[n:]
		if c.headerN != len(c.header) {
			continue
		}

		frameType := StreamFrameType(c.header[0])
		sequence := binary.BigEndian.Uint64(c.header[1:9])
		length := binary.BigEndian.Uint32(c.header[9:StreamFrameHeaderSize])
		if frameType == StreamFrameData {
			c.events = append(c.events, phase32NLegDataEvent{LegID: c.legID, Sequence: sequence, At: time.Now()})
		}
		if length == 0 {
			c.headerN = 0
		} else {
			c.remaining = length
		}
	}
}

func (c *phase32NLegTraceConn) snapshotEvents() []phase32NLegDataEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]phase32NLegDataEvent(nil), c.events...)
}

func phase32NewTCPConnPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen loopback TCP carrier: %v", err)
	}
	defer listener.Close()

	clientResult := make(chan struct {
		conn net.Conn
		err  error
	}, 1)
	go func() {
		conn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		clientResult <- struct {
			conn net.Conn
			err  error
		}{conn, dialErr}
	}()
	server, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept loopback TCP carrier: %v", err)
	}
	client := <-clientResult
	if client.err != nil {
		_ = server.Close()
		t.Fatalf("dial loopback TCP carrier: %v", client.err)
	}
	return client.conn, server
}

func phase32HasLegContributionWithin(events [][]phase32NLegDataEvent, window time.Duration) bool {
	if len(events) == 0 {
		return false
	}
	var starts []time.Time
	for _, legEvents := range events {
		if len(legEvents) == 0 {
			return false
		}
		for _, event := range legEvents {
			starts = append(starts, event.At)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, start := range starts {
		end := start.Add(window)
		allLegsContributed := true
		for _, legEvents := range events {
			index := sort.Search(len(legEvents), func(i int) bool { return !legEvents[i].At.Before(start) })
			if index == len(legEvents) || legEvents[index].At.After(end) {
				allLegsContributed = false
				break
			}
		}
		if allLegsContributed {
			return true
		}
	}
	return false
}

func phase32WaitForLegState(t *testing.T, engines []*StreamEngine, id uint8, wantPresent bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, engine := range engines {
			if engine.hasLeg(id) != wantPresent {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Leg%d presence did not become %t on every Core session", id, wantPresent)
}

func phase32WaitForAck(t *testing.T, engine *StreamEngine, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if engine.txAckedTotal.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ACK frontier did not reach %d bytes; reached %d", want, engine.txAckedTotal.Load())
}

func TestPhase32NLegFailureAndGenerationRejoin(t *testing.T) {
	const legCount = 3
	const phaseBytes = 2 << 20
	cfg := testStreamConfig()
	cfg.MaxActiveLegs = legCount
	cfg.ChunkSize = 16 * 1024
	cfg.QueueFrames = 32
	cfg.MaxInflightFrames = 96
	cfg.AckInterval = 2 * time.Millisecond
	cfg.RetransmitTimeout = time.Second
	cfg.RecoveryTimeout = 5 * time.Second
	cfg.SchedulerMode = StreamSchedulerAggregation
	cfg.CapacityMode = StreamCapacityDynamic
	cfg.BandwidthMbps = []uint32{100, 100, 100}
	cfg.Telemetry = &StreamTelemetry{}
	client, clientApp := NewStreamEngine(cfg)
	serverCfg := cfg
	serverCfg.Telemetry = &StreamTelemetry{}
	server, serverApp := NewStreamEngine(serverCfg)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		_ = clientApp.Close()
		_ = serverApp.Close()
	})

	clientLegs := make([]*phase32NLegTraceConn, legCount)
	serverLegs := make([]net.Conn, legCount)
	for id := 0; id < legCount; id++ {
		clientConn, serverConn := phase32NewTCPConnPair(t)
		clientLeg := &phase32NLegTraceConn{Conn: clientConn, legID: uint8(id)}
		clientLegs[id], serverLegs[id] = clientLeg, serverConn
		if err := client.AttachLeg(LegID(id), clientLeg, nil); err != nil {
			t.Fatalf("attach client Leg%d: %v", id, err)
		}
		if err := server.AttachLeg(LegID(id), serverConn, nil); err != nil {
			t.Fatalf("attach Landing Leg%d: %v", id, err)
		}
	}

	phases := make([][]byte, 3)
	for phase := range phases {
		phases[phase] = make([]byte, phaseBytes)
		for i := range phases[phase] {
			phases[phase][i] = byte((i*17 + phase*83 + i/127) % 251)
		}
	}
	allPayload := bytes.Join(phases, nil)
	wantSHA := sha256.Sum256(allPayload)
	serverRead := make(chan error, 1)
	go func() {
		got := make([]byte, len(allPayload))
		if _, err := io.ReadFull(serverApp, got); err != nil {
			serverRead <- fmt.Errorf("read continuous session payload: %w", err)
			return
		}
		if !bytes.Equal(got, allPayload) || sha256.Sum256(got) != wantSHA {
			serverRead <- errors.New("continuous session payload SHA/content mismatch")
			return
		}
		var extra [1]byte
		n, err := serverApp.Read(extra[:])
		if n != 0 || !errors.Is(err, io.EOF) {
			serverRead <- fmt.Errorf("Landing EOF: n=%d err=%v", n, err)
			return
		}
		serverRead <- nil
	}()

	var acked uint64
	var frontierAfterPhase [3]uint64
	for phase := 0; phase < 3; phase++ {
		written, err := clientApp.Write(phases[phase])
		if err != nil || written != len(phases[phase]) {
			t.Fatalf("phase %d application write: n=%d err=%v", phase, written, err)
		}
		acked += uint64(len(phases[phase]))
		phase32WaitForAck(t, client, acked)
		frontierAfterPhase[phase] = client.txLedger.NextSequence()
		if phase == 0 {
			stats := server.Snapshot()
			for id := 0; id < legCount; id++ {
				if stats.Legs[id].RxUniqueBytes == 0 {
					t.Fatalf("initial 3-leg phase had no unique DATA contribution on Leg%d", id)
				}
			}
			if err := clientLegs[2].Close(); err != nil {
				t.Fatalf("close client Leg2 TCP carrier: %v", err)
			}
			if err := serverLegs[2].Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatalf("close Landing Leg2 TCP carrier: %v", err)
			}
			phase32WaitForLegState(t, []*StreamEngine{client, server}, 2, false)
		}
		if phase == 1 {
			clientConn, serverConn := phase32NewTCPConnPair(t)
			clientLeg := &phase32NLegTraceConn{Conn: clientConn, legID: 2}
			if err := client.AttachLeg(2, clientLeg, nil); err != nil {
				t.Fatalf("rejoin client Leg2: %v", err)
			}
			if err := server.AttachLeg(2, serverConn, nil); err != nil {
				_ = clientLeg.Close()
				_ = serverConn.Close()
				t.Fatalf("rejoin Landing Leg2: %v", err)
			}
			clientLegs[2], serverLegs[2] = clientLeg, serverConn
			phase32WaitForLegState(t, []*StreamEngine{client, server}, 2, true)
		}
	}
	if !(frontierAfterPhase[0] < frontierAfterPhase[1] && frontierAfterPhase[1] < frontierAfterPhase[2]) {
		t.Fatalf("global sequence frontier did not continue across failure/rejoin: %v", frontierAfterPhase)
	}
	client.StartGracefulClose(io.EOF)
	select {
	case err := <-serverRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server read/EOF timed out after N-1 failure and same-session rejoin")
	}
	clientEOF := make(chan error, 1)
	go func() {
		var extra [1]byte
		n, err := clientApp.Read(extra[:])
		if n != 0 || !errors.Is(err, io.EOF) {
			clientEOF <- fmt.Errorf("client EOF after N-leg rejoin: n=%d err=%v", n, err)
			return
		}
		clientEOF <- nil
	}()
	select {
	case err := <-clientEOF:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client EOF timed out after N-leg rejoin")
	}

	clientStats, serverStats := client.Snapshot(), server.Snapshot()
	if clientStats.TxAckedUsefulTotal != uint64(len(allPayload)) || clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 {
		t.Fatalf("sender ledger/ACK mismatch: ack=%d outstanding=%d/%d", clientStats.TxAckedUsefulTotal, clientStats.OutstandingFrames, clientStats.OutstandingBytes)
	}
	if serverStats.RxDeliveredBytes != uint64(len(allPayload)) || serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
		t.Fatalf("receiver delivery/pending mismatch: delivered=%d pending=%d/%d", serverStats.RxDeliveredBytes, serverStats.RxPendingFrames, serverStats.RxPendingBytes)
	}
	if clientStats.Legs[2].Generation != 2 || serverStats.Legs[2].Generation != 2 {
		t.Fatalf("Leg2 generation after rejoin client=%d server=%d, want 2", clientStats.Legs[2].Generation, serverStats.Legs[2].Generation)
	}
	if serverStats.Legs[2].RxUniqueBytes <= 0 {
		t.Fatal("rejoined Leg2 has no unique receive contribution")
	}
	for id := 0; id < legCount; id++ {
		if serverStats.Legs[id].RxUniqueBytes == 0 {
			t.Fatalf("final same-session receive has no unique contribution from Leg%d", id)
		}
	}
	t.Logf("PHASE32_NLEG_REJOIN_RESULT session_preserved=true legs_initial=%d failed_leg=2 surviving_legs=2 rejoined_leg=2 client_generation=%d server_generation=%d sequence_frontiers=%v payload_bytes=%d sha256=%x strict_eof=true acked_bytes=%d tx_ledger_empty=true rx_pending_empty=true", legCount, clientStats.Legs[2].Generation, serverStats.Legs[2].Generation, frontierAfterPhase, len(allPayload), wantSHA, clientStats.TxAckedUsefulTotal)
}

func TestPhase32NLegArbitraryFailuresKeepRemainingSessionAlive(t *testing.T) {
	type failureCase struct {
		legs      int
		failedIDs []int
	}
	cases := []failureCase{
		{legs: 3, failedIDs: []int{1}},
		{legs: 4, failedIDs: []int{0}},
		{legs: 4, failedIDs: []int{1, 3}},
		{legs: 4, failedIDs: []int{0, 2, 3}},
		{legs: 8, failedIDs: []int{4}},
		{legs: 8, failedIDs: []int{1, 6}},
		{legs: 8, failedIDs: []int{0, 3, 7}},
		{legs: 8, failedIDs: []int{1, 2, 5, 7}},
		{legs: 8, failedIDs: []int{0, 2, 4, 6, 7}},
		{legs: 8, failedIDs: []int{0, 1, 3, 5, 6, 7}},
		{legs: 8, failedIDs: []int{1, 2, 3, 4, 5, 6, 7}},
		{legs: 16, failedIDs: []int{1, 3, 5, 7, 9, 11, 13, 15}},
	}

	for _, tc := range cases {
		failed := make(map[int]bool, len(tc.failedIDs))
		for _, id := range tc.failedIDs {
			failed[id] = true
		}
		t.Run(fmt.Sprintf("N=%d-failed=%v", tc.legs, tc.failedIDs), func(t *testing.T) {
			const payloadSize = 1 << 20
			cfg := testStreamConfig()
			cfg.MaxActiveLegs = tc.legs
			cfg.ChunkSize = 16 * 1024
			cfg.QueueFrames = 64
			cfg.MaxInflightFrames = 128
			cfg.MaxReorderFrames = 512
			cfg.AckInterval = 2 * time.Millisecond
			cfg.RetransmitTimeout = time.Second
			cfg.RecoveryTimeout = 5 * time.Second
			cfg.SchedulerMode = StreamSchedulerAggregation
			cfg.CapacityMode = StreamCapacityDynamic
			cfg.BandwidthMbps = make([]uint32, tc.legs)
			for i := range cfg.BandwidthMbps {
				cfg.BandwidthMbps[i] = 100
			}
			client, clientApp := NewStreamEngine(cfg)
			serverCfg := cfg
			server := func() (*StreamEngine, net.Conn) {
				return NewStreamEngine(serverCfg)
			}
			receiver, receiverApp := server()
			t.Cleanup(func() {
				_ = client.Close()
				_ = receiver.Close()
				_ = clientApp.Close()
				_ = receiverApp.Close()
			})

			clientLegs := make([]net.Conn, tc.legs)
			for id := 0; id < tc.legs; id++ {
				clientConn, serverConn := phase32NewTCPConnPair(t)
				clientLegs[id] = clientConn
				if err := client.AttachLeg(LegID(id), clientConn, nil); err != nil {
					_ = serverConn.Close()
					t.Fatalf("attach client Leg%d: %v", id, err)
				}
				if err := receiver.AttachLeg(LegID(id), serverConn, nil); err != nil {
					_ = clientConn.Close()
					_ = serverConn.Close()
					t.Fatalf("attach receiver Leg%d: %v", id, err)
				}
			}

			for id := 0; id < tc.legs; id++ {
				if !failed[id] {
					continue
				}
				if err := clientLegs[id].Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Fatalf("close client Leg%d: %v", id, err)
				}
			}
			for id := 0; id < tc.legs; id++ {
				if !failed[id] {
					continue
				}
				phase32WaitForLegState(t, []*StreamEngine{client, receiver}, uint8(id), false)
			}
			survivors := tc.legs - len(tc.failedIDs)
			if client.LegCount() != survivors || receiver.LegCount() != survivors {
				t.Fatalf("remaining active legs client/receiver=%d/%d, want %d", client.LegCount(), receiver.LegCount(), survivors)
			}

			payload := make([]byte, payloadSize)
			for i := range payload {
				payload[i] = byte((i*29 + i/193) % 251)
			}
			wantSHA := sha256.Sum256(payload)
			serverRead := make(chan error, 1)
			go func() {
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(receiverApp, got); err != nil {
					serverRead <- fmt.Errorf("read payload: %w", err)
					return
				}
				if sha256.Sum256(got) != wantSHA || !bytes.Equal(got, payload) {
					serverRead <- errors.New("payload checksum/content mismatch")
					return
				}
				var extra [1]byte
				n, err := receiverApp.Read(extra[:])
				if n != 0 || !errors.Is(err, io.EOF) {
					serverRead <- fmt.Errorf("terminal read n=%d err=%v, want strict EOF", n, err)
					return
				}
				serverRead <- nil
			}()

			started := time.Now()
			written, err := clientApp.Write(payload)
			if err != nil || written != len(payload) {
				t.Fatalf("client write n=%d err=%v, want %d", written, err, len(payload))
			}
			client.StartGracefulClose(io.EOF)
			select {
			case err := <-serverRead:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("remaining healthy legs did not deliver payload and EOF")
			}
			clientEOF := make(chan error, 1)
			go func() {
				var extra [1]byte
				n, err := clientApp.Read(extra[:])
				if n != 0 || !errors.Is(err, io.EOF) {
					clientEOF <- fmt.Errorf("client terminal read n=%d err=%v, want strict EOF", n, err)
					return
				}
				clientEOF <- nil
			}()
			select {
			case err := <-clientEOF:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("client did not observe strict EOF")
			}

			clientStats, receiverStats := client.Snapshot(), receiver.Snapshot()
			if clientStats.TxAckedUsefulTotal != payloadSize || clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 {
				t.Fatalf("TX ACK/ledger ack=%d outstanding=%d/%d", clientStats.TxAckedUsefulTotal, clientStats.OutstandingFrames, clientStats.OutstandingBytes)
			}
			if receiverStats.RxDeliveredBytes != payloadSize || receiverStats.RxPendingFrames != 0 || receiverStats.RxPendingBytes != 0 {
				t.Fatalf("RX delivery/pending delivered=%d pending=%d/%d", receiverStats.RxDeliveredBytes, receiverStats.RxPendingFrames, receiverStats.RxPendingBytes)
			}
			var unique uint64
			for id := 0; id < tc.legs; id++ {
				if failed[id] {
					continue
				}
				if receiverStats.Legs[id].RxUniqueBytes == 0 {
					t.Fatalf("surviving Leg%d made no unique contribution", id)
				}
				unique += receiverStats.Legs[id].RxUniqueBytes
			}
			if unique != payloadSize {
				t.Fatalf("surviving-leg unique total=%d, want %d", unique, payloadSize)
			}
			t.Logf("PHASE32_NLEG_FAILURE_RESULT legs=%d failed_ids=%v failed_legs=%d surviving_legs=%d payload_bytes=%d sha256=%x strict_eof=true acked_bytes=%d unique_bytes=%d tx_ledger_empty=true rx_pending_empty=true runtime_ms=%d", tc.legs, tc.failedIDs, len(tc.failedIDs), survivors, payloadSize, wantSHA, clientStats.TxAckedUsefulTotal, unique, time.Since(started).Milliseconds())
		})
	}
}

func TestPhase32NLegAllCarrierFailureTerminatesWithRecoveryError(t *testing.T) {
	cfg := testStreamConfig()
	cfg.MaxActiveLegs = 3
	cfg.ChunkSize = 16 * 1024
	cfg.QueueFrames = 16
	cfg.MaxInflightFrames = 32
	cfg.MaxReorderFrames = 128
	cfg.RecoveryTimeout = 100 * time.Millisecond
	cfg.SchedulerMode = StreamSchedulerAggregation
	cfg.CapacityMode = StreamCapacityDynamic
	cfg.BandwidthMbps = []uint32{100, 100, 100}
	client, app := NewStreamEngine(cfg)
	server, serverApp := NewStreamEngine(cfg)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		_ = app.Close()
		_ = serverApp.Close()
	})

	carriers := make([]net.Conn, 3)
	for id := range carriers {
		clientConn, serverConn := phase32NewTCPConnPair(t)
		carriers[id] = clientConn
		if err := client.AttachLeg(LegID(id), clientConn, nil); err != nil {
			_ = serverConn.Close()
			t.Fatalf("attach client Leg%d: %v", id, err)
		}
		if err := server.AttachLeg(LegID(id), serverConn, nil); err != nil {
			_ = clientConn.Close()
			t.Fatalf("attach server Leg%d: %v", id, err)
		}
	}
	for _, carrier := range carriers {
		if err := carrier.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("close carrier: %v", err)
		}
	}
	for id := range carriers {
		phase32WaitForLegState(t, []*StreamEngine{client, server}, uint8(id), false)
	}
	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("Core did not terminate after all configured carriers failed")
	}
	if err := client.CloseError(); err == nil || !strings.Contains(err.Error(), "no transport leg available") {
		t.Fatalf("all-leg failure close error=%v, want bounded no-transport failure", err)
	}
	var extra [1]byte
	if n, err := app.Read(extra[:]); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("all-leg failure app read n=%d err=%v, want forced non-EOF close", n, err)
	}
	t.Logf("PHASE32_NLEG_ALL_FAILURE_RESULT configured_legs=3 failed_legs=3 active_legs=%d bounded_recovery_error=%q app_terminal=ErrClosedPipe false_success=false", client.LegCount(), client.CloseError())
}

func TestPhase32NLegLoopbackTCPAggregation(t *testing.T) {
	for _, legCount := range []int{1, 2, 3, 4, 8, 16} {
		t.Run(fmt.Sprintf("N=%d", legCount), func(t *testing.T) {
			const payloadSize = 32 << 20
			payload := make([]byte, payloadSize)
			for i := range payload {
				payload[i] = byte((i*31 + i/251) % 251)
			}
			wantSHA := sha256.Sum256(payload)

			cfg := testStreamConfig()
			cfg.MaxActiveLegs = legCount
			cfg.ChunkSize = 64 * 1024
			cfg.QueueFrames = 64
			cfg.MaxReorderFrames = 4096
			cfg.MaxInflightFrames = 128
			cfg.AckInterval = 2 * time.Millisecond
			cfg.RetransmitTimeout = 1500 * time.Millisecond
			cfg.RecoveryTimeout = 10 * time.Second
			cfg.SchedulerMode = StreamSchedulerAggregation
			cfg.CapacityMode = StreamCapacityDynamic
			cfg.BandwidthMbps = make([]uint32, legCount)
			for i := range cfg.BandwidthMbps {
				cfg.BandwidthMbps[i] = 100
			}
			clientTelemetry, serverTelemetry := &StreamTelemetry{}, &StreamTelemetry{}
			cfg.Telemetry = clientTelemetry
			client, clientApp := NewStreamEngine(cfg)
			serverCfg := cfg
			serverCfg.Telemetry = serverTelemetry
			server, serverApp := NewStreamEngine(serverCfg)
			t.Cleanup(func() {
				_ = client.Close()
				_ = server.Close()
				_ = clientApp.Close()
				_ = serverApp.Close()
			})

			clientTraces := make([]*phase32NLegTraceConn, legCount)
			for id := 0; id < legCount; id++ {
				clientConn, serverConn := phase32NewTCPConnPair(t)
				traceConn := &phase32NLegTraceConn{Conn: clientConn, legID: uint8(id)}
				clientTraces[id] = traceConn
				if err := client.AttachLeg(LegID(id), traceConn, nil); err != nil {
					_ = serverConn.Close()
					t.Fatalf("attach client leg %d: %v", id, err)
				}
				if err := server.AttachLeg(LegID(id), serverConn, nil); err != nil {
					_ = traceConn.Close()
					t.Fatalf("attach server leg %d: %v", id, err)
				}
			}
			if got := client.legCount(); got != legCount {
				t.Fatalf("one client Core session has %d legs, want %d", got, legCount)
			}
			if got := server.legCount(); got != legCount {
				t.Fatalf("one Landing Core session has %d legs, want %d", got, legCount)
			}

			serverResult := make(chan error, 1)
			go func() {
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(serverApp, got); err != nil {
					serverResult <- fmt.Errorf("read payload: %w", err)
					return
				}
				if sha256.Sum256(got) != wantSHA || !bytes.Equal(got, payload) {
					serverResult <- errors.New("application payload SHA/content mismatch")
					return
				}
				var extra [1]byte
				n, err := serverApp.Read(extra[:])
				if n != 0 || !errors.Is(err, io.EOF) {
					serverResult <- fmt.Errorf("server application termination: n=%d err=%v, want strict io.EOF", n, err)
					return
				}
				serverResult <- nil
			}()

			started := time.Now()
			written, writeErr := clientApp.Write(payload)
			if writeErr != nil || written != len(payload) {
				t.Fatalf("client application write: n=%d err=%v, want %d bytes", written, writeErr, len(payload))
			}
			client.StartGracefulClose(io.EOF)

			clientEOF := make(chan error, 1)
			go func() {
				var extra [1]byte
				n, err := clientApp.Read(extra[:])
				if n != 0 || !errors.Is(err, io.EOF) {
					clientEOF <- fmt.Errorf("client application termination: n=%d err=%v, want strict io.EOF", n, err)
					return
				}
				clientEOF <- nil
			}()

			for name, result := range map[string]<-chan error{"server payload/EOF": serverResult, "client EOF": clientEOF} {
				select {
				case err := <-result:
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
				case <-time.After(45 * time.Second):
					t.Fatalf("%s timed out for N=%d", name, legCount)
				}
			}
			runtimeDuration := time.Since(started)

			clientStats := client.Snapshot()
			serverStats := server.Snapshot()
			if clientStats.TxAckedUsefulTotal != payloadSize {
				t.Fatalf("ACKed unique payload=%d, want %d", clientStats.TxAckedUsefulTotal, payloadSize)
			}
			if clientStats.OutstandingFrames != 0 || clientStats.OutstandingBytes != 0 {
				t.Fatalf("TX ledger not empty: frames=%d bytes=%d", clientStats.OutstandingFrames, clientStats.OutstandingBytes)
			}
			if serverStats.RxDeliveredBytes != payloadSize || serverStats.RxAckNext == 0 {
				t.Fatalf("receiver delivery/frontier incomplete: delivered=%d ack_next=%d", serverStats.RxDeliveredBytes, serverStats.RxAckNext)
			}
			if serverStats.RxPendingFrames != 0 || serverStats.RxPendingBytes != 0 {
				t.Fatalf("RX pending not empty: frames=%d bytes=%d", serverStats.RxPendingFrames, serverStats.RxPendingBytes)
			}
			if len(clientStats.Legs) != legCount || len(serverStats.Legs) != legCount {
				t.Fatalf("dynamic stats length client=%d server=%d, want %d", len(clientStats.Legs), len(serverStats.Legs), legCount)
			}

			clientEventsByLeg := make([][]phase32NLegDataEvent, legCount)
			var originalAssigned uint64
			var receiverUnique uint64
			for id := 0; id < legCount; id++ {
				clientRow, serverRow := clientStats.Legs[id], serverStats.Legs[id]
				if int(clientRow.LegID) != id || int(serverRow.LegID) != id {
					t.Fatalf("leg row order mismatch at %d: client=%d server=%d", id, clientRow.LegID, serverRow.LegID)
				}
				if clientRow.OrdinaryAssignedBytes == 0 {
					t.Fatalf("Leg%d received no original DATA assignment", id)
				}
				if serverRow.RxUniqueBytes == 0 {
					t.Fatalf("Leg%d delivered no unique application DATA", id)
				}
				originalAssigned += clientRow.OrdinaryAssignedBytes
				receiverUnique += serverRow.RxUniqueBytes
				for _, event := range clientTraces[id].snapshotEvents() {
					clientEventsByLeg[id] = append(clientEventsByLeg[id], event)
				}
				sort.Slice(clientEventsByLeg[id], func(i, j int) bool { return clientEventsByLeg[id][i].At.Before(clientEventsByLeg[id][j].At) })
			}
			if originalAssigned != payloadSize || receiverUnique != payloadSize {
				t.Fatalf("unique allocation/delivery totals assigned=%d rx_unique=%d want=%d", originalAssigned, receiverUnique, payloadSize)
			}
			if !phase32HasLegContributionWithin(clientEventsByLeg, 100*time.Millisecond) {
				t.Fatalf("no 100ms window contains DATA writes on all %d legs", legCount)
			}

			t.Logf("PHASE32_NLEG_RESULT legs=%d session_engines=2 carrier_protocol=TCP listener=loopback payload_bytes=%d payload_sha256=%x strict_server_eof=true strict_client_eof=true acked_bytes=%d rx_unique_bytes=%d tx_outstanding=%d rx_pending_frames=%d rx_pending_bytes=%d unique_contributing_legs=%d overlap_window_ms=100 runtime_ms=%d", legCount, payloadSize, wantSHA, clientStats.TxAckedUsefulTotal, receiverUnique, clientStats.OutstandingFrames, serverStats.RxPendingFrames, serverStats.RxPendingBytes, legCount, runtimeDuration.Milliseconds())
		})
	}
}

func TestPhase32LegIDLimitsAndMaxByteID(t *testing.T) {
	for _, maxActive := range []int{1, 16, 256} {
		t.Run(fmt.Sprintf("max=%d", maxActive), func(t *testing.T) {
			cfg := testStreamConfig()
			cfg.MaxActiveLegs = maxActive
			engine, app := NewStreamEngine(cfg)
			defer engine.Close()
			defer app.Close()
			id := LegID(maxActive - 1)
			local, peer := net.Pipe()
			defer peer.Close()
			if err := engine.AttachLeg(id, local, nil); err != nil {
				t.Fatalf("attach highest configured leg ID %d: %v", id, err)
			}
			if maxActive < MaxStreamLegIDCount {
				tooHighLocal, tooHighPeer := net.Pipe()
				defer tooHighPeer.Close()
				if err := engine.AttachLeg(LegID(maxActive), tooHighLocal, nil); err == nil {
					_ = tooHighLocal.Close()
					t.Fatalf("accepted leg ID %d beyond max_active_legs=%d", maxActive, maxActive)
				}
				_ = tooHighLocal.Close()
			}
		})
	}

	cfg := testStreamConfig()
	cfg.MaxActiveLegs = 257
	engine, app := NewStreamEngine(cfg)
	defer engine.Close()
	defer app.Close()
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	if err := engine.AttachLeg(0, local, nil); err == nil {
		t.Fatal("accepted max_active_legs above byte-addressable protocol range")
	}
}

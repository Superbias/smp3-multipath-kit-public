package smp3core

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type phase25RelayPacket struct {
	data   []byte
	readAt time.Time
	due    time.Time
}

type phase25RelayDirectionStats struct {
	ReadBytes                  atomic.Uint64
	DownstreamWriteBytes       atomic.Uint64
	ReadBlockedNS              atomic.Uint64
	DownstreamWriteBlockedNS   atomic.Uint64
	ScheduledServiceDelayNS    atomic.Uint64
	ScheduledServiceSamples    atomic.Uint64
	MaxScheduledServiceDelayNS atomic.Uint64
	QueuedBytes                atomic.Int64
	MaxQueuedBytes             atomic.Uint64
	QueuedPackets              atomic.Int64
	MaxQueuedPackets           atomic.Uint64
}

type phase25RelayDirectionSnapshot struct {
	ReadBytes                  uint64 `json:"read_bytes"`
	DownstreamWriteBytes       uint64 `json:"downstream_write_completed_bytes"`
	ReadBlockedNS              uint64 `json:"read_blocked_ns"`
	DownstreamWriteBlockedNS   uint64 `json:"downstream_write_blocked_ns"`
	ScheduledServiceDelayNS    uint64 `json:"scheduled_service_delay_ns"`
	ScheduledServiceSamples    uint64 `json:"scheduled_service_samples"`
	MaxScheduledServiceDelayNS uint64 `json:"max_scheduled_service_delay_ns"`
	CurrentQueuedBytes         int64  `json:"current_queued_bytes"`
	MaxQueuedBytes             uint64 `json:"max_queued_bytes"`
	CurrentQueuedPackets       int64  `json:"current_queued_packets"`
	MaxQueuedPackets           uint64 `json:"max_queued_packets"`
}

type phase25TCPRelay struct {
	clientSide net.Conn
	serverSide net.Conn
	relaySide  net.Conn
	upstream   net.Conn
	stop       chan struct{}
	stopOnce   sync.Once
	resume     chan struct{}
	resumeOnce sync.Once
	stats      [2]phase25RelayDirectionStats // 0: client->server, 1: server->client
}

func phase25AtomicMax(value *atomic.Uint64, candidate uint64) {
	for old := value.Load(); candidate > old; old = value.Load() {
		if value.CompareAndSwap(old, candidate) {
			return
		}
	}
}

func (r *phase25TCPRelay) snapshotStats() [2]phase25RelayDirectionSnapshot {
	var out [2]phase25RelayDirectionSnapshot
	for i := range r.stats {
		s := &r.stats[i]
		out[i] = phase25RelayDirectionSnapshot{
			ReadBytes: s.ReadBytes.Load(), DownstreamWriteBytes: s.DownstreamWriteBytes.Load(),
			ReadBlockedNS: s.ReadBlockedNS.Load(), DownstreamWriteBlockedNS: s.DownstreamWriteBlockedNS.Load(),
			ScheduledServiceDelayNS: s.ScheduledServiceDelayNS.Load(), ScheduledServiceSamples: s.ScheduledServiceSamples.Load(),
			MaxScheduledServiceDelayNS: s.MaxScheduledServiceDelayNS.Load(), CurrentQueuedBytes: s.QueuedBytes.Load(),
			MaxQueuedBytes: s.MaxQueuedBytes.Load(), CurrentQueuedPackets: s.QueuedPackets.Load(), MaxQueuedPackets: s.MaxQueuedPackets.Load(),
		}
	}
	return out
}

func phase25NewRelayPair(t *testing.T, oneWayDelay time.Duration, bitsPerSecond uint64, pauseClientToServer bool) (*phase25TCPRelay, net.Conn, net.Conn) {
	t.Helper()
	landing, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("landing listen: %v", err)
	}
	relayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = landing.Close()
		t.Fatalf("relay listen: %v", err)
	}
	acceptedRelay := make(chan net.Conn, 1)
	acceptRelayErr := make(chan error, 1)
	go func() {
		conn, err := relayListener.Accept()
		if err != nil {
			acceptRelayErr <- err
			return
		}
		acceptedRelay <- conn
	}()
	acceptedServer := make(chan net.Conn, 1)
	acceptServerErr := make(chan error, 1)
	go func() {
		conn, err := landing.Accept()
		if err != nil {
			acceptServerErr <- err
			return
		}
		acceptedServer <- conn
	}()
	clientSide, err := net.DialTimeout("tcp", relayListener.Addr().String(), 5*time.Second)
	if err != nil {
		_ = landing.Close()
		_ = relayListener.Close()
		t.Fatalf("dial relay: %v", err)
	}
	var relaySide net.Conn
	select {
	case relaySide = <-acceptedRelay:
	case err := <-acceptRelayErr:
		_ = clientSide.Close()
		t.Fatalf("accept relay: %v", err)
	case <-time.After(5 * time.Second):
		_ = clientSide.Close()
		t.Fatal("timed out accepting relay socket")
	}
	upstream, err := net.DialTimeout("tcp", landing.Addr().String(), 5*time.Second)
	if err != nil {
		_ = clientSide.Close()
		_ = relaySide.Close()
		_ = landing.Close()
		_ = relayListener.Close()
		t.Fatalf("relay dial landing: %v", err)
	}
	var serverSide net.Conn
	select {
	case serverSide = <-acceptedServer:
	case err := <-acceptServerErr:
		_ = upstream.Close()
		t.Fatalf("landing accept: %v", err)
	case <-time.After(5 * time.Second):
		_ = upstream.Close()
		t.Fatal("timed out accepting landing socket")
	}
	for name, conn := range map[string]net.Conn{
		"client": clientSide, "relay-client": relaySide, "relay-upstream": upstream, "server": serverSide,
	} {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
		}
		if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
			t.Fatalf("%s set deadline: %v", name, err)
		}
	}
	resume := make(chan struct{})
	if !pauseClientToServer {
		close(resume)
	}
	r := &phase25TCPRelay{
		clientSide: clientSide, serverSide: serverSide,
		relaySide: relaySide, upstream: upstream,
		stop: make(chan struct{}), resume: resume,
	}
	go r.forward(relaySide, upstream, oneWayDelay, bitsPerSecond, true)
	go r.forward(upstream, relaySide, oneWayDelay, 0, false)
	t.Cleanup(func() {
		r.Close()
		_ = landing.Close()
		_ = relayListener.Close()
	})
	return r, clientSide, serverSide
}

func (r *phase25TCPRelay) forward(src, dst net.Conn, delay time.Duration, bitsPerSecond uint64, gated bool) {
	packets := make(chan phase25RelayPacket, 64)
	direction := 1
	if gated {
		direction = 0
	}
	stats := &r.stats[direction]
	go func() {
		defer close(packets)
		buffer := make([]byte, 16*1024)
		var serviceReady time.Time
		for {
			if gated {
				select {
				case <-r.resume:
				case <-r.stop:
					return
				}
			}
			readStarted := time.Now()
			n, err := src.Read(buffer)
			readAt := time.Now()
			stats.ReadBlockedNS.Add(uint64(readAt.Sub(readStarted)))
			if n > 0 {
				stats.ReadBytes.Add(uint64(n))
				dueBase := readAt
				if bitsPerSecond > 0 {
					if serviceReady.Before(readAt) {
						serviceReady = readAt
					}
					serviceNS := int64((uint64(n)*8*uint64(time.Second) + bitsPerSecond - 1) / bitsPerSecond)
					serviceReady = serviceReady.Add(time.Duration(serviceNS))
					dueBase = serviceReady
				}
				payload := append([]byte(nil), buffer[:n]...)
				stats.QueuedBytes.Add(int64(len(payload)))
				stats.QueuedPackets.Add(1)
				phase25AtomicMax(&stats.MaxQueuedBytes, uint64(max(int64(0), stats.QueuedBytes.Load())))
				phase25AtomicMax(&stats.MaxQueuedPackets, uint64(max(int64(0), stats.QueuedPackets.Load())))
				select {
				case packets <- phase25RelayPacket{data: payload, readAt: readAt, due: dueBase.Add(delay)}:
				case <-r.stop:
					stats.QueuedBytes.Add(-int64(len(payload)))
					stats.QueuedPackets.Add(-1)
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case packet, ok := <-packets:
			if !ok {
				return
			}
			stats.QueuedBytes.Add(-int64(len(packet.data)))
			stats.QueuedPackets.Add(-1)
			wait := time.Until(packet.due)
			if wait > 0 {
				waitNS := uint64(wait)
				stats.ScheduledServiceDelayNS.Add(waitNS)
				stats.ScheduledServiceSamples.Add(1)
				phase25AtomicMax(&stats.MaxScheduledServiceDelayNS, waitNS)
			}
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-r.stop:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
			}
			writeStarted := time.Now()
			if err := writeAll(dst, packet.data); err != nil {
				stats.DownstreamWriteBlockedNS.Add(uint64(time.Since(writeStarted)))
				return
			}
			stats.DownstreamWriteBlockedNS.Add(uint64(time.Since(writeStarted)))
			stats.DownstreamWriteBytes.Add(uint64(len(packet.data)))
		case <-r.stop:
			return
		}
	}
}

func (r *phase25TCPRelay) ResumeClientToServer() {
	r.resumeOnce.Do(func() { close(r.resume) })
}

func (r *phase25TCPRelay) Close() {
	r.stopOnce.Do(func() { close(r.stop) })
	_ = r.clientSide.Close()
	_ = r.serverSide.Close()
	_ = r.relaySide.Close()
	_ = r.upstream.Close()
}

func phase25Percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	copySamples := append([]time.Duration(nil), samples...)
	sort.Slice(copySamples, func(i, j int) bool { return copySamples[i] < copySamples[j] })
	index := int(float64(len(copySamples))*p+0.999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(copySamples) {
		index = len(copySamples) - 1
	}
	return copySamples[index]
}

func TestPhase25UserSpaceTCPRelayRTTFidelity(t *testing.T) {
	for _, targetRTTMillis := range []int{20, 50, 100, 200, 400} {
		targetRTT := time.Duration(targetRTTMillis) * time.Millisecond
		t.Run(fmt.Sprintf("configured_%dms", targetRTTMillis), func(t *testing.T) {
			cfg := phase25IdentityConfig()
			cfg.BandwidthMbps = []uint32{100, 100}
			cfg.ExperimentalBoundedRescue = nil
			client, clientApp, err := NewStreamEngineForCandidate(cfg, StreamPolicyB0)
			if err != nil {
				t.Fatal(err)
			}
			server, serverApp, err := NewStreamEngineForCandidate(cfg, StreamPolicyB0)
			if err != nil {
				_ = client.Close()
				_ = clientApp.Close()
				t.Fatal(err)
			}
			defer client.Close()
			defer clientApp.Close()
			defer server.Close()
			defer serverApp.Close()
			for legID := LegID(0); legID < 2; legID++ {
				relay, clientConn, serverConn := phase25NewRelayPair(t, targetRTT/2, 0, false)
				if err := client.AttachLeg(legID, clientConn, nil); err != nil {
					relay.Close()
					t.Fatalf("attach client relay leg %d: %v", legID, err)
				}
				if err := server.AttachLeg(legID, serverConn, nil); err != nil {
					relay.Close()
					t.Fatalf("attach server relay leg %d: %v", legID, err)
				}
			}
			client.SetActiveForTest(true)
			server.SetActiveForTest(true)
			echoDone := make(chan error, 1)
			go func() {
				for {
					var request [1]byte
					if _, err := io.ReadFull(serverApp, request[:]); err != nil {
						echoDone <- err
						return
					}
					if err := writeAll(serverApp, request[:]); err != nil {
						echoDone <- err
						return
					}
				}
			}()
			const sampleCount = 10
			samples := make([]time.Duration, 0, sampleCount)
			for sample := 0; sample < sampleCount; sample++ {
				value := []byte{byte(sample + 1)}
				started := time.Now()
				if err := writeAll(clientApp, value); err != nil {
					t.Fatalf("request %d write: %v", sample, err)
				}
				var response [1]byte
				if _, err := io.ReadFull(clientApp, response[:]); err != nil {
					select {
					case echoErr := <-echoDone:
						t.Fatalf("request %d response: %v (echo worker: %v)", sample, err, echoErr)
					default:
						t.Fatalf("request %d response: %v", sample, err)
					}
				}
				if response[0] != value[0] {
					t.Fatalf("request %d response=%d want=%d", sample, response[0], value[0])
				}
				samples = append(samples, time.Since(started))
			}
			t.Logf("relay_model=user-space TCP byte-stream relay configured_rtt=%s measured_app_rtt_ms=%v p50=%s p95=%s p99=%s samples=%d", targetRTT, samples, phase25Percentile(samples, .50), phase25Percentile(samples, .95), phase25Percentile(samples, .99), len(samples))
		})
	}
}

func TestPhase25UserSpaceTCPRelaySerializationCalibration(t *testing.T) {
	const payloadSize = 4 << 20
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i*17 + i/127)
	}
	for _, rateMbps := range []uint64{100, 10} {
		t.Run(fmt.Sprintf("%dMbps", rateMbps), func(t *testing.T) {
			relay, writerConn, readerConn := phase25NewRelayPair(t, 0, rateMbps*1_000_000, false)
			defer relay.Close()
			writerDone := make(chan error, 1)
			started := time.Now()
			go func() { writerDone <- writeAll(writerConn, payload) }()
			received := make([]byte, payloadSize)
			if _, err := io.ReadFull(readerConn, received); err != nil {
				t.Fatalf("read raw relay payload: %v", err)
			}
			select {
			case err := <-writerDone:
				if err != nil {
					t.Fatalf("write raw relay payload: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("raw relay writer did not finish")
			}
			elapsed := time.Since(started)
			if !bytes.Equal(received, payload) {
				t.Fatal("raw relay payload mismatch")
			}
			actualMbps := float64(payloadSize*8) / elapsed.Seconds() / 1_000_000
			t.Logf("relay_model=user-space TCP byte-stream service configured_mbps=%d actual_mbps=%.3f elapsed=%s bytes=%d sha256=%x; not a kernel bandwidth shaper", rateMbps, actualMbps, elapsed, payloadSize, sha256.Sum256(received))
		})
	}
}

func TestPhase25UserSpaceRelayTCPFailureRecoveryB0B1(t *testing.T) {
	const payloadSize = 4 << 20
	testInjectedBudget := os.Getenv("SMP3_PHASE28_TEST_INJECT_BUDGET") == "1"
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i*31 + i/251)
	}
	wantSHA := sha256Sum(payload)
	for _, candidate := range []StreamPolicyCandidate{StreamPolicyB0, StreamPolicyB1} {
		t.Run(string(candidate), func(t *testing.T) {
			cfg := phase25IdentityConfig()
			cfg.BandwidthMbps = []uint32{100, 100}
			cfg.ChunkSize = 16 * 1024
			cfg.MaxInflightFrames = 256
			cfg.RetransmitTimeout = 1500 * time.Millisecond
			if candidate == StreamPolicyB1 {
				cfg.ExperimentalBoundedRescue = &StreamBoundedRescueConfig{Enabled: true, BudgetBasisPoints: 300}
			}
			var eventsMu sync.Mutex
			var events []StreamRescueEvent
			if candidate == StreamPolicyB1 {
				cfg.rescueEventHook = func(event StreamRescueEvent) {
					eventsMu.Lock()
					events = append(events, event)
					eventsMu.Unlock()
				}
			}
			client, clientApp, err := NewStreamEngineForCandidate(cfg, candidate)
			if err != nil {
				t.Fatal(err)
			}
			serverCfg := cfg
			serverCfg.rescueEventHook = nil
			server, serverApp, err := NewStreamEngineForCandidate(serverCfg, candidate)
			if err != nil {
				_ = client.Close()
				_ = clientApp.Close()
				t.Fatal(err)
			}
			defer client.Close()
			defer clientApp.Close()
			defer server.Close()
			defer serverApp.Close()

			relay0, client0, server0 := phase25NewRelayPair(t, 0, 100_000_000, false)
			defer relay0.Close()
			relay1, client1, server1 := phase25NewRelayPair(t, 0, 10_000_000, true)
			defer relay1.Close()
			if err := client.AttachLeg(0, client0, nil); err != nil {
				t.Fatal(err)
			}
			if err := server.AttachLeg(0, server0, nil); err != nil {
				t.Fatal(err)
			}
			if err := client.AttachLeg(1, client1, nil); err != nil {
				t.Fatal(err)
			}
			if err := server.AttachLeg(1, server1, nil); err != nil {
				t.Fatal(err)
			}
			client.SetActiveForTest(true)
			server.SetActiveForTest(true)

			serverRead := make(chan phase25TCPReadResult, 1)
			clientRead := make(chan phase25TCPReadResult, 1)
			go func() { serverRead <- phase25ReadToEOF(serverApp) }()
			go func() { clientRead <- phase25ReadToEOF(clientApp) }()
			writeDone := make(chan error, 1)
			go func() { writeDone <- writeAll(clientApp, payload) }()

			deadline := time.Now().Add(5 * time.Second)
			for client.TelemetrySnapshot().TxWrittenBytesByLeg[1] == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			writtenBeforeFault := client.TelemetrySnapshot().TxWrittenBytesByLeg[1]
			if writtenBeforeFault == 0 {
				relay1.ResumeClientToServer()
				relay1.Close()
				t.Fatal("fault gate did not hold any DATA written on slow Leg 1")
			}
			var injectedBudgetBytes, injectedBudgetLimit uint64
			injectBudget := testInjectedBudget && candidate == StreamPolicyB1
			if injectBudget {
				manager := client.boundedRescue
				if manager == nil {
					t.Fatal("TEST-INJECTED budget requested but B1 budget manager is absent")
				}
				injectedAt := time.Now()
				manager.mu.Lock()
				manager.pruneLocked(injectedAt)
				// Seed the budget to the maximum 300 bp quota for this complete
				// 4 MiB application input. This is test-only accepted-history state,
				// not Carrier traffic and not a natural W rejection.
				injectedBudgetLimit = boundedRescueRatio(uint64(payloadSize), manager.config.BudgetBasisPoints)
				used := saturatingAdd(manager.windowAccepted, manager.baseReserved)
				if used < injectedBudgetLimit {
					injectedBudgetBytes = injectedBudgetLimit - used
					heap.Push(&manager.acceptedEvents, rescueByteEvent{at: injectedAt, bytes: injectedBudgetBytes, target: 0xff})
					manager.windowAccepted = saturatingAdd(manager.windowAccepted, injectedBudgetBytes)
				}
				seeded := saturatingAdd(manager.windowAccepted, manager.baseReserved)
				manager.mu.Unlock()
				if seeded < injectedBudgetLimit {
					t.Fatalf("TEST-INJECTED budget seed failed: used=%d limit=%d", seeded, injectedBudgetLimit)
				}
			}
			faultAt := time.Now()
			relay1.Close() // User-space relay closes this TCP leg after withholding C->S bytes.
			var testInjectedExemptionSequence uint64
			if injectBudget {
				// The relay close is a real Carrier failure. Wait for the normal
				// read-loop invalidation to mark an outstanding Leg 1 owner, then
				// pass that ledger record through the existing bounded-rescue
				// admission path. The test does not forge owner/frontier state.
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) && testInjectedExemptionSequence == 0 {
					for _, retry := range client.txLedger.retrySnapshot() {
						record := retry.Record
						evidence := client.txLedger.rescueEvidenceSnapshot(record)
						if !evidence.Exists || !evidence.Invalidated || evidence.OrdinaryOwner != 1 || evidence.InTransit || retry.RescueInTransit {
							continue
						}
						target := client.getLeg(0)
						if target == nil || target.closed.Load() {
							continue
						}
						client.admission.mu.Lock()
						epoch := client.admission.legs[1].recoveryEpoch
						client.admission.mu.Unlock()
						if client.tryQueueBoundedFrontierRescue(target, record, 1, epoch, false) {
							testInjectedExemptionSequence = record.Sequence()
							break
						}
					}
					if testInjectedExemptionSequence == 0 {
						time.Sleep(time.Millisecond)
					}
				}
				if testInjectedExemptionSequence == 0 {
					t.Fatal("TEST-INJECTED budget + real TCP fault found no outstanding record with positive failed-owner invalidation")
				}
			}
			var writeErr error
			select {
			case writeErr = <-writeDone:
			case <-time.After(20 * time.Second):
				t.Fatal("application writer remained blocked after relay TCP leg closed")
			}
			if writeErr != nil {
				t.Fatalf("application write after relay fault: %v", writeErr)
			}
			client.StartGracefulClose(io.EOF)
			var received phase25TCPReadResult
			select {
			case received = <-serverRead:
			case <-time.After(30 * time.Second):
				t.Fatal("server application did not finish after user-space relay TCP failure")
			}
			if received.err != io.EOF || len(received.data) != len(payload) || sha256Sum(received.data) != wantSHA {
				t.Fatalf("server result after relay failure: bytes=%d err=%v sha=%x want=%x", len(received.data), received.err, sha256Sum(received.data), wantSHA)
			}
			select {
			case got := <-clientRead:
				if got.err != io.EOF || len(got.data) != 0 {
					t.Fatalf("client EOF after relay failure: bytes=%d err=%v", len(got.data), got.err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("client application did not observe peer EOF after recovery")
			}
			tx := client.Snapshot()
			rx := server.Snapshot()
			if tx.TxAckedUsefulTotal != payloadSize || rx.RxDeliveredBytes != payloadSize || tx.OutstandingFrames != 0 || tx.OutstandingBytes != 0 || rx.RxPendingFrames != 0 || rx.RxPendingBytes != 0 {
				t.Fatalf("recovery integrity/ledger failure: ack=%d delivered=%d tx=%d/%d rx=%d/%d", tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, tx.OutstandingFrames, tx.OutstandingBytes, rx.RxPendingFrames, rx.RxPendingBytes)
			}
			retryBytes := tx.TxRetryWrittenBytesByLeg[0] + tx.TxRetryWrittenBytesByLeg[1]
			if retryBytes == 0 {
				t.Fatalf("TCP relay leg failure recovered without observable Core retransmission; writtenBeforeFault=%d", writtenBeforeFault)
			}
			identity := client.RuntimePolicyIdentity()
			if identity.Candidate != candidate {
				t.Fatalf("candidate identity changed during relay recovery: %+v", identity)
			}
			eventsMu.Lock()
			b1Events := append([]StreamRescueEvent(nil), events...)
			eventsMu.Unlock()
			b1Stats := client.TelemetrySnapshot().BoundedRescue
			if injectBudget {
				foundExemption := false
				for _, event := range b1Events {
					if event.Evidence.Sequence == testInjectedExemptionSequence && event.Kind == "failure_recovery_exempt" && event.RecoveryExempted && event.FinalDecisionReason == "positive_owner_invalidation" {
						foundExemption = true
						break
					}
				}
				if b1Stats.FailureRecoveryExempted == 0 || !foundExemption {
					t.Fatalf("TEST-INJECTED budget + TCP fault did not traverse B1 failure exemption: stats=%+v event=%t", b1Stats, foundExemption)
				}
				t.Logf("retry_invocation=TEST_ONLY_existing_B1_bounded_admission_after_real_positive_owner_invalidation sequence=%d", testInjectedExemptionSequence)
				t.Logf("budget_state_origin=TEST-INJECTED accepted_history_bytes=%d max_300bp_quota_for_payload=%d natural_w_rejection=false b1_failure_exemption=observed", injectedBudgetBytes, injectedBudgetLimit)
			}
			t.Logf("candidate=%s injected=user-space-relay-pause-then-TCP-close fault_to_eof=%s written_on_paused_leg_before_fault=%d retry_bytes=%d ack=%d delivered=%d sha256=%x tx_ledger=0 rx_pending=0 B1_failure_exemptions=%d B1_budget_rejected=%d observed_policy_events=%d", candidate, time.Since(faultAt), writtenBeforeFault, retryBytes, tx.TxAckedUsefulTotal, rx.RxDeliveredBytes, wantSHA, b1Stats.FailureRecoveryExempted, b1Stats.BudgetRejected, len(b1Events))
		})
	}
}

func sha256Sum(data []byte) [32]byte {
	return sha256.Sum256(data)
}

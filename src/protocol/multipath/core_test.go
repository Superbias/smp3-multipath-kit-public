package multipath

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type failAfterConn struct {
	net.Conn
	mu        sync.Mutex
	remaining int
	failed    bool
}

func (c *failAfterConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed {
		return 0, io.ErrClosedPipe
	}
	if c.remaining <= 0 {
		c.failed = true
		_ = c.Conn.Close()
		return 0, io.ErrClosedPipe
	}
	if len(p) <= c.remaining {
		n, err := c.Conn.Write(p)
		c.remaining -= n
		return n, err
	}
	limit := c.remaining
	n, _ := c.Conn.Write(p[:limit])
	c.remaining -= n
	c.failed = true
	_ = c.Conn.Close()
	return n, io.ErrClosedPipe
}

func waitForCounterAtLeast(t *testing.T, name string, load func() uint64, want uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		got := load()
		if got >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s=%d, want at least %d", name, got, want)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func testCoreConfig() coreConfig {
	return coreConfig{
		ChunkSize:         4096,
		QueueFrames:       32,
		ThresholdBytesPS:  0,
		ActivationWindow:  50 * time.Millisecond,
		BandwidthMbps:     []uint32{100, 500},
		MaxReorderFrames:  256,
		MaxInflightFrames: 128,
		AckInterval:       5 * time.Millisecond,
		RetransmitTimeout: 50 * time.Millisecond,
		RecoveryTimeout:   2 * time.Second,
	}
}

func TestCoreSingleLegRoundTrip(t *testing.T) {
	leftCore, leftApp := newCore(testCoreConfig())
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a, b := net.Pipe()
	if err := leftCore.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("abc123"), 20000)
	readDone := make(chan error, 1)
	got := make([]byte, len(payload))
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestCoreRxReordersAndDropsDuplicateSequence(t *testing.T) {
	core, appConn := newCore(testCoreConfig())
	defer core.Close()
	defer appConn.Close()

	got := make([]byte, 2)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(appConn, got)
		readDone <- err
	}()

	// Sequence 1 must wait for sequence 0. The second sequence-0 frame is an
	// old/duplicate DATA frame and must be discarded without a second delivery.
	core.injectFrame(1, []byte("b"), 0)
	core.injectFrame(0, []byte("a"), 0)
	core.injectFrame(0, []byte("x"), 0)

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reordered DATA was not delivered")
	}
	if string(got) != "ab" {
		t.Fatalf("delivered=%q want=%q", got, "ab")
	}
}

func TestCoreRXReorderLimitDrainsPendingState(t *testing.T) {
	cfg := testCoreConfig()
	cfg.MaxReorderFrames = 1
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	core.injectFrame(1, make([]byte, cfg.ChunkSize), 0)
	deadline := time.Now().Add(time.Second)
	for core.snapshotStats().RxPendingFrames != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("pending frames=%d want=1", core.snapshotStats().RxPendingFrames)
		}
		time.Sleep(100 * time.Microsecond)
	}
	core.injectFrame(2, make([]byte, cfg.ChunkSize), 0)
	select {
	case <-core.Done():
	case <-time.After(time.Second):
		t.Fatal("reorder-limit failure did not close the core")
	}
	deadline = time.Now().Add(time.Second)
	for {
		stats := core.snapshotStats()
		if stats.RxPendingFrames == 0 && stats.RxPendingBytes == 0 && stats.RxGapAge == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending state not drained: frames=%d bytes=%d gap=%s", stats.RxPendingFrames, stats.RxPendingBytes, stats.RxGapAge)
		}
		time.Sleep(100 * time.Microsecond)
	}
	err := core.engine.CloseError()
	if err == nil || err.Error() != "multipath reorder buffer exceeded" {
		t.Fatalf("close error=%v", err)
	}
}

func TestCoreLegFailureRetransmitsWithoutLogicalBreak(t *testing.T) {
	cfg := testCoreConfig()
	leg0Down := make(chan struct{}, 1)
	cfg.OnLegDown = func(id uint8, _ error) {
		if id == 0 {
			select {
			case leg0Down <- struct{}{}:
			default:
			}
		}
	}
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	faulty := &failAfterConn{Conn: a0, remaining: 48 * 1024}
	if err := leftCore.addLeg(0, faulty, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("multipath-failover-"), 30000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()
	writeDone := make(chan error, 1)
	go func() {
		_, err := leftApp.Write(payload)
		writeDone <- err
	}()

	// Wait for the injected mid-frame failure instead of relying on a fixed
	// scheduler sleep; this keeps the failover test reliable under -race.
	select {
	case <-leg0Down:
	case <-time.After(5 * time.Second):
		t.Fatal("faulty leg did not fail")
	}
	// Add a completely new path with the same leg id semantics as a secondary
	// transport.
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-writeDone:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("logical write broke: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("logical write timeout")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("logical read broke: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("logical read timeout")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch after failover")
	}
}

type blackholeWriteConn struct{ net.Conn }

func (c *blackholeWriteConn) Write(p []byte) (int, error) { return len(p), nil }

func TestCoreAckTimeoutReplaysOnOtherLiveLeg(t *testing.T) {
	cfg := testCoreConfig()
	cfg.BandwidthMbps = []uint32{10000, 1} // initial scheduler strongly prefers leg 0
	cfg.RetransmitTimeout = 40 * time.Millisecond
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	if err := leftCore.addLeg(0, &blackholeWriteConn{Conn: a0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("timeout-replay-"), 4000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(rightApp, got); readDone <- err }()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for replay on second leg")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch after ACK-timeout replay")
	}
}

func TestCoreFrontierRescueBypassesBlockedInTransitPrimary(t *testing.T) {
	cfg := testCoreConfig()
	cfg.BandwidthMbps = []uint32{10000, 1}
	cfg.RetransmitTimeout = 40 * time.Millisecond
	cfg.MaxInflightFrames = 16
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	release := make(chan struct{})
	slow := &gatedWriteConn{Conn: a0, started: make(chan struct{}), release: release}
	if err := leftCore.addLeg(0, slow, nil); err != nil {
		close(release)
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		close(release)
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		close(release)
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		close(release)
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("blocked-in-transit-frontier-"), 500)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(rightApp, got); readDone <- err }()
	writeDone := make(chan error, 1)
	go func() { _, err := leftApp.Write(payload); writeDone <- err }()

	select {
	case <-slow.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("primary DATA write never became blocked inTransit")
	}

	// Keep the original leg0 write blocked. R9 must duplicate the ACK frontier
	// onto leg1 and complete delivery without waiting for release.
	select {
	case err := <-readDone:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("frontier rescue did not bypass blocked inTransit leg0")
	}
	if !bytes.Equal(got, payload) {
		close(release)
		t.Fatal("payload mismatch after concurrent frontier rescue")
	}
	if leftCore.snapshotStats().FrontierRescueAttempts == 0 {
		close(release)
		t.Fatal("expected at least one frontier rescue attempt")
	}

	close(release)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("logical write did not finish after rescue")
	}
}

func TestCoreAckProgressWakesNextOverdueFrontierRescueBeforeTicker(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestCoreFrontierRescueNeverBurstsPastAckedNext(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestCoreFailedRescueEnqueueDoesNotConsumeCooldown(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestCoreDeadLegReplayIsFrontierFirst(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestCoreHealthyFrontierFastPathAvoidsLegSnapshotAllocation(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestSnapshotStatsMarksConcurrentFrontierAttemptsMultipath(t *testing.T) {
	t.Skip("coverage migrated to smp3core StreamEngine tests")
}

func TestCoreAckBroadcastSurvivesOneWayControlBlackhole(t *testing.T) {
	cfg := testCoreConfig()
	cfg.BandwidthMbps = []uint32{10000, 1}
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	// Client->server data on leg0 works, but server->client writes on leg0 are
	// silently discarded. ACK broadcasting must still release the sender window
	// through leg1.
	if err := leftCore.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, &blackholeWriteConn{Conn: b0}, nil); err != nil {
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("ack-broadcast-"), 10000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(rightApp, got); readDone <- err }()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		remaining := leftCore.txLedger.ProgressSnapshot().Outstanding
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ACK window not released, %d outstanding", remaining)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCoreWriteThenCloseDrainsOutstanding(t *testing.T) {
	cfg := testCoreConfig()
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()
	a, b := net.Pipe()
	if err := leftCore.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("final-frame-"), 10000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(rightApp, got); readDone <- err }()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = leftApp.Close()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("reader lost final data: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for final drained data")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("final payload mismatch")
	}
}

func TestCoreSilentPrimaryStallActivatesBooster(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60 // bandwidth trigger must not be the reason
	cfg.RetransmitTimeout = 40 * time.Millisecond
	cfg.RecoveryTimeout = 500 * time.Millisecond

	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	if err := leftCore.addLeg(0, &blackholeWriteConn{Conn: a0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		t.Fatal(err)
	}

	activated := make(chan struct{}, 1)
	leftCore.cfg.OnActivate = func() {
		a1, b1 := net.Pipe()
		if err := leftCore.addLeg(1, a1, nil); err != nil {
			return
		}
		if err := rightCore.addLeg(1, b1, nil); err != nil {
			return
		}
		activated <- struct{}{}
	}
	leftCore.engine.SetOnActivateForTest(leftCore.cfg.OnActivate)

	payload := bytes.Repeat([]byte("silent-stall-"), 6000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() { _, err := io.ReadFull(rightApp, got); readDone <- err }()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}

	select {
	case <-activated:
	case <-time.After(2 * time.Second):
		t.Fatal("silent primary stall did not activate booster")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for replay after silent stall")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch after silent-stall failover")
	}
}

func TestRecoveryTimeoutResetsAfterTemporaryRecovery(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RecoveryTimeout = 180 * time.Millisecond
	core, _ := newCore(cfg)
	defer core.Close()

	// First outage starts a recovery timer.
	a0, b0 := net.Pipe()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	_ = b0.Close()
	time.Sleep(40 * time.Millisecond)
	if core.legCount() != 0 {
		t.Fatal("expected first leg to be removed")
	}

	// Recover before the first deadline, then fail again shortly before that old
	// deadline. The second outage must receive a full RecoveryTimeout.
	a1, b1 := net.Pipe()
	if err := core.addLeg(0, a1, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	_ = b1.Close()
	time.Sleep(55 * time.Millisecond) // crosses the first outage's old deadline
	select {
	case <-core.Done():
		t.Fatal("stale recovery timer killed a later outage too early")
	default:
	}

	select {
	case <-core.Done():
		// Expected only after the second outage's own timeout.
	case <-time.After(300 * time.Millisecond):
		t.Fatal("second outage did not expire after its own recovery timeout")
	}
}

func TestCoreFutureAckIsIgnoredWithoutRetiringData(t *testing.T) {
	cfg := testCoreConfig()
	var anomalyCount uint64
	cfg.OnFutureAck = func(next, max, count uint64) {
		if next != 99 || max != 1 {
			t.Errorf("unexpected future ACK callback: next=%d max=%d", next, max)
		}
		anomalyCount = count
	}
	core, _ := newCore(cfg)
	defer core.Close()

	record := testLedgerAdd(core, []byte("pending"), time.Now(), 0)
	core.inflight <- struct{}{}

	if err := core.handleAck(99); err != nil {
		t.Fatalf("future ACK should be ignored, got %v", err)
	}
	stillOutstanding := core.txLedger.IsOutstanding(record)
	ackedNext := core.txLedger.ProgressSnapshot().AckedNext
	if !stillOutstanding || ackedNext != 0 {
		t.Fatalf("future ACK mutated TX state: outstanding=%v ackedNext=%d", stillOutstanding, ackedNext)
	}
	if anomalyCount != 1 {
		t.Fatalf("future ACK anomaly callback count=%d, want 1", anomalyCount)
	}

	if err := core.handleAck(1); err != nil {
		t.Fatalf("valid ACK failed: %v", err)
	}
	stillOutstanding = core.txLedger.IsOutstanding(record)
	ackedNext = core.txLedger.ProgressSnapshot().AckedNext
	if stillOutstanding || ackedNext != 1 {
		t.Fatalf("valid ACK did not retire TX state: outstanding=%v ackedNext=%d", stillOutstanding, ackedNext)
	}
}

func TestCoreFutureAckDoesNotKillTransportLeg(t *testing.T) {
	cfg := testCoreConfig()
	core, app := newCore(cfg)
	defer core.Close()

	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}

	var ack [frameHeaderSize]byte
	ack[0] = frameTypeAck
	binary.BigEndian.PutUint64(ack[1:9], 12345)
	if _, err := b.Write(ack[:]); err != nil {
		t.Fatal(err)
	}

	payload := []byte("still-alive-after-future-ack")
	if err := writeDataFrame(b, dataFrame{seq: 0, data: payload}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if err := app.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatalf("leg stopped carrying data after future ACK: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: %q", got)
	}
	if !core.hasLeg(0) {
		t.Fatal("future ACK incorrectly removed the transport leg")
	}
}

func TestGracefulDrainUsesProgressTimeoutNotAbsoluteDeadline(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RecoveryTimeout = 120 * time.Millisecond
	core, _ := newCore(cfg)
	defer core.Close()

	const frames = 8
	for seq := uint64(0); seq < frames; seq++ {
		testLedgerAdd(core, []byte{byte(seq)}, time.Now(), 0)
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- core.drainOutstandingOnClose() }()

	// Keep making ACK progress for ~640ms. alpha2/alpha2.1 used a fixed close
	// deadline (500ms minimum / 5s maximum), so an absolute timer can return
	// while payload is still outstanding. alpha2.2 must extend the stall window
	// on every cumulative-ACK advance.
	for next := uint64(1); next <= frames; next++ {
		time.Sleep(80 * time.Millisecond)
		select {
		case err := <-drainDone:
			t.Fatalf("graceful drain returned before ACK progress completed at next=%d: %v", next, err)
		default:
		}
		if err := core.handleAck(next); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("graceful drain failed despite continuing ACK progress: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful drain did not finish after final ACK")
	}
}

func TestGracefulCloseFrameEndsPeerWithoutRecoveryDelay(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RecoveryTimeout = 2 * time.Second
	leftCore, leftApp := newCore(cfg)
	rightCore, rightApp := newCore(cfg)
	defer leftCore.Close()
	defer rightCore.Close()

	a, b := net.Pipe()
	if err := leftCore.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("graceful-close-frame-"), 12000)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()

	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := leftApp.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("peer lost payload before logical CLOSE: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for payload before logical CLOSE")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch before logical CLOSE")
	}

	// The peer should learn logical EOF from frameTypeClose instead of waiting
	// RecoveryTimeout for both carrier sockets to disappear.
	select {
	case <-rightCore.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("peer did not close promptly after logical CLOSE frame")
	}
}

func TestCoreSnapshotUsesLogicalBytesAndLegAttribution(t *testing.T) {
	leftCore, leftApp := newCore(testCoreConfig())
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a, b := net.Pipe()
	if err := leftCore.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("logical-goodput-"), 512)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, rightApp, int64(len(payload)))
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		left := leftCore.snapshotStats()
		right := rightCore.snapshotStats()
		if left.TxAckedUsefulByLeg[0] == uint64(len(payload)) &&
			right.RxUniqueBytesByLeg[0] == uint64(len(payload)) &&
			right.RxDeliveredBytes == uint64(len(payload)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("logical stats did not converge: left=%+v right=%+v", left, right)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoreReplaceLegPreservesLogicalConnectionAndOtherLeg(t *testing.T) {
	leftCore, _ := newCore(testCoreConfig())
	rightCore, _ := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a0, b0 := net.Pipe()
	if err := leftCore.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, b0, nil); err != nil {
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	if err := leftCore.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1, nil); err != nil {
		t.Fatal(err)
	}

	if !leftCore.replaceLeg(1, errors.New("intentional carrier replacement")) {
		t.Fatal("replaceLeg did not remove leg 1")
	}
	select {
	case <-leftCore.Done():
		t.Fatal("carrier replacement closed logical core")
	default:
	}
	if !leftCore.hasLeg(0) || leftCore.hasLeg(1) {
		t.Fatal("carrier replacement changed the wrong legs")
	}
	_ = b1.Close()
	deadline := time.Now().Add(time.Second)
	for rightCore.hasLeg(1) {
		if time.Now().After(deadline) {
			t.Fatal("peer did not release replaced leg")
		}
		time.Sleep(time.Millisecond)
	}

	// A replacement may rejoin with the same leg ID while leg 0 remains alive.
	a1b, b1b := net.Pipe()
	if err := leftCore.addLeg(1, a1b, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b1b, nil); err != nil {
		t.Fatal(err)
	}
	if !leftCore.hasLeg(0) || !leftCore.hasLeg(1) {
		t.Fatal("rejoined carrier did not restore both legs")
	}
}

func TestCoreSecondaryOnlyRoundTripBeforePrimaryJoin(t *testing.T) {
	leftCore, leftApp := newCore(testCoreConfig())
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()

	a, b := net.Pipe()
	if err := leftCore.addLeg(1, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(1, b, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("secondary-first"), 4096)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("secondary-only round trip timed out")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("secondary-only payload mismatch")
	}
}

func TestCorePrimaryPreferenceRestoredAfterLateLeg0Join(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60 // keep scheduler inactive/preferred for the assertion
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	leg1, peer1 := net.Pipe()
	defer peer1.Close()
	if err := core.addLeg(1, leg1, nil); err != nil {
		t.Fatal(err)
	}
	if chosen := core.chooseLeg(false, -1); chosen == nil || chosen.id != 1 {
		t.Fatalf("secondary-only core did not use surviving leg1: %#v", chosen)
	}

	leg0, peer0 := net.Pipe()
	defer peer0.Close()
	if err := core.addLeg(0, leg0, nil); err != nil {
		t.Fatal(err)
	}
	if chosen := core.chooseLeg(false, -1); chosen == nil || chosen.id != 0 {
		t.Fatalf("late leg0 did not regain preferred scheduling role: %#v", chosen)
	}
}

type delayedReadCloseConn struct {
	net.Conn
	started sync.Once
	ready   chan struct{}
	release chan struct{}
}

func (c *delayedReadCloseConn) Read(p []byte) (int, error) {
	c.started.Do(func() { close(c.ready) })
	n, err := c.Conn.Read(p)
	if err != nil {
		<-c.release
	}
	return n, err
}

func TestCoreGracefulClosingStillAllowsTransportRepair(t *testing.T) {
	core, appConn := newCore(testCoreConfig())
	defer core.Close()
	defer appConn.Close()

	// CLOSING means no new application payload, not "no transport repair".
	// Tail ACK/drain may still require a rejoined carrier.
	core.engine.SetClosingForTest(true)
	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(1, a, nil); err != nil {
		t.Fatalf("graceful closing rejected transport repair: %v", err)
	}
}

func TestCoreFinalizingRejectsLateLeg(t *testing.T) {
	core, appConn := newCore(testCoreConfig())
	defer core.Close()
	defer appConn.Close()

	if !core.beginFinalizing() {
		t.Fatal("failed to enter finalizing state")
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if err := core.addLeg(0, a, nil); !errors.Is(err, errCoreClosed) {
		t.Fatalf("late leg attached after finalizing: %v", err)
	}
	if core.hasLeg(0) {
		t.Fatal("finalizing core published a late leg")
	}
}

func TestCoreSameIDRejoinWaitsForOldWorkersToRetire(t *testing.T) {
	core, appConn := newCore(testCoreConfig())
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	release := make(chan struct{})
	wrapped := &delayedReadCloseConn{Conn: a0, ready: make(chan struct{}), release: release}
	if err := core.addLeg(0, wrapped, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wrapped.ready:
	case <-time.After(time.Second):
		t.Fatal("old leg read worker did not start")
	}
	old := core.getLeg(0)
	if old == nil {
		t.Fatal("old leg missing")
	}
	record := testLedgerAdd(core, []byte("unacked-old-generation"), time.Now().Add(-time.Second), 0)

	// Drive failure externally so the old read worker can be held after Close.
	core.handleLegFailure(old, io.ErrClosedPipe)
	ownerAfterFailure := int16(-2)
	for _, candidate := range core.txLedger.PlanRetries(smp3core.StreamLegAvailability{false, true}) {
		if candidate.Record == record {
			ownerAfterFailure = candidate.Avoid
		}
	}
	if ownerAfterFailure != -1 {
		t.Fatalf("dead leg generation retained ownership=%d, want -1", ownerAfterFailure)
	}
	deadline := time.Now().Add(time.Second)
	for !core.engine.IsRetiringForTest(0) {
		if time.Now().After(deadline) {
			t.Fatal("old leg was not marked retiring")
		}
		time.Sleep(time.Millisecond)
	}

	a1, b1 := net.Pipe()
	defer b1.Close()
	joined := make(chan error, 1)
	go func() { joined <- core.addLeg(0, a1, nil) }()

	select {
	case err := <-joined:
		t.Fatalf("replacement attached before old workers retired: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("replacement failed after retirement: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement did not attach after old workers retired")
	}
	if current := core.getLeg(0); current == nil || current == old {
		t.Fatal("same-ID replacement did not become current leg")
	}
	_ = b0.Close()
}

type signalWriteConn struct {
	net.Conn
	once    sync.Once
	started chan struct{}
}

func (c *signalWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(p)
}

type gatedWriteConn struct {
	net.Conn
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (c *gatedWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return c.Conn.Write(p)
}

func TestCoreAckBroadcastDoesNotHOLAcrossLegs(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	release := make(chan struct{})
	slow := &gatedWriteConn{Conn: a0, started: make(chan struct{}), release: release}
	if err := core.addLeg(0, slow, nil); err != nil {
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	defer b1.Close()
	if err := core.addLeg(1, a1, nil); err != nil {
		close(release)
		t.Fatal(err)
	}

	if !core.sendAckFrame(7, false) {
		close(release)
		t.Fatal("ACK was not scheduled")
	}
	select {
	case <-slow.started:
		// leg0 is now intentionally blocked inside its own writer.
	case <-time.After(time.Second):
		close(release)
		t.Fatal("slow leg did not begin ACK write")
	}

	_ = b1.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	frame, err := readWireFrame(b1, core)
	if err != nil {
		close(release)
		t.Fatalf("healthy leg ACK was HOL-blocked by slow leg: %v", err)
	}
	if frame.typ != frameTypeAck || frame.seq != 7 {
		close(release)
		t.Fatalf("unexpected healthy-leg control frame: type=%d value=%d", frame.typ, frame.seq)
	}

	// Release the synthetic blocked writer before cleanup. Closing the peer makes
	// its pending net.Pipe write return immediately without needing a reader.
	close(release)
	_ = b0.Close()
}

func TestCoreAckPriorityAheadOfQueuedData(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a, b := net.Pipe()
	defer b.Close()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWrite()
	wrapped := &gatedWriteConn{Conn: a, started: make(chan struct{}), release: release}
	if err := core.addLeg(0, wrapped, nil); err != nil {
		t.Fatal(err)
	}
	leg := core.getLeg(0)
	if leg == nil {
		t.Fatal("leg0 missing")
	}

	record0 := testLedgerAddTransit(core, []byte("first"), time.Now(), leg.id)
	if !core.queueDataAttempt(record0, leg.id) {
		t.Fatal("first DATA attempt was not queued")
	}

	select {
	case <-wrapped.started:
		// The first DATA frame is already in progress and blocked on net.Pipe.
	case <-time.After(time.Second):
		t.Fatal("first DATA write did not start")
	}
	record1 := testLedgerAddTransit(core, []byte("second"), time.Now(), leg.id)
	if !core.queueDataAttempt(record1, leg.id) {
		t.Fatal("second DATA attempt was not queued")
	}
	if !core.sendAckFrame(9, false) {
		t.Fatal("ACK was not scheduled")
	}
	releaseWrite()

	_ = b.SetReadDeadline(time.Now().Add(time.Second))
	first, err := readWireFrame(b, core)
	if err != nil {
		t.Fatal(err)
	}
	if first.typ != frameTypeData || first.seq != 0 {
		t.Fatalf("expected first DATA frame, got type=%d seq=%d", first.typ, first.seq)
	}
	core.putBuffer(first.data)

	second, err := readWireFrame(b, core)
	if err != nil {
		t.Fatal(err)
	}
	if second.typ != frameTypeAck || second.seq != 9 {
		if second.typ == frameTypeData {
			core.putBuffer(second.data)
		}
		t.Fatalf("ACK did not get priority over queued DATA: type=%d value=%d", second.typ, second.seq)
	}

	third, err := readWireFrame(b, core)
	if err != nil {
		t.Fatal(err)
	}
	if third.typ != frameTypeData || third.seq != 1 {
		if third.typ == frameTypeData {
			core.putBuffer(third.data)
		}
		t.Fatalf("queued DATA did not follow priority ACK: type=%d value=%d", third.typ, third.seq)
	}
	core.putBuffer(third.data)
}

func TestCoreClosingWithoutOutstandingSuppressesEmergencyActivation(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	legDown := make(chan struct{}, 1)
	cfg.OnLegDown = func(uint8, error) { legDown <- struct{}{} }
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	old := core.getLeg(0)
	core.engine.SetClosingForTest(true)
	core.handleLegFailure(old, io.EOF)

	if core.isActive() {
		t.Fatal("graceful closing with no outstanding DATA activated booster")
	}
	select {
	case <-legDown:
		t.Fatal("graceful closing with no outstanding DATA emitted repair callback")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestCoreClosingWithOutstandingStillAllowsEmergencyActivation(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}
	testLedgerAdd(core, []byte("tail"), time.Now(), 0)

	old := core.getLeg(0)
	core.engine.SetClosingForTest(true)
	core.handleLegFailure(old, io.ErrClosedPipe)
	if !core.isActive() {
		t.Fatal("graceful closing with outstanding DATA suppressed required transport repair")
	}
}

func TestCoreCloseBroadcastDoesNotHOLAcrossLegs(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	release := make(chan struct{})
	slow := &gatedWriteConn{Conn: a0, started: make(chan struct{}), release: release}
	if err := core.addLeg(0, slow, nil); err != nil {
		t.Fatal(err)
	}
	a1, b1 := net.Pipe()
	defer b1.Close()
	if err := core.addLeg(1, a1, nil); err != nil {
		close(release)
		t.Fatal(err)
	}

	result := make(chan bool, 1)
	go func() { result <- core.sendCloseFrame(11) }()
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("slow leg did not begin CLOSE write")
	}

	_ = b1.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	frame, err := readWireFrame(b1, core)
	if err != nil {
		close(release)
		t.Fatalf("healthy leg CLOSE was HOL-blocked by slow leg: %v", err)
	}
	if frame.typ != frameTypeClose || frame.seq != 11 {
		close(release)
		t.Fatalf("unexpected healthy-leg CLOSE frame: type=%d value=%d", frame.typ, frame.seq)
	}
	select {
	case ok := <-result:
		if !ok {
			close(release)
			t.Fatal("sendCloseFrame reported failure despite healthy carrier")
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("sendCloseFrame waited for blocked carrier after healthy CLOSE delivery")
	}

	close(release)
	_ = b0.Close()
}

func TestCoreForcedAckResendsSameCumulativeValue(t *testing.T) {
	cfg := testCoreConfig()
	cfg.ThresholdBytesPS = 1 << 60
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a, b := net.Pipe()
	defer b.Close()
	if err := core.addLeg(0, a, nil); err != nil {
		t.Fatal(err)
	}

	if !core.sendAckFrame(5, false) {
		t.Fatal("initial ACK was not scheduled")
	}
	_ = b.SetReadDeadline(time.Now().Add(time.Second))
	first, err := readWireFrame(b, core)
	if err != nil {
		t.Fatal(err)
	}
	if first.typ != frameTypeAck || first.seq != 5 {
		t.Fatalf("unexpected initial ACK: type=%d value=%d", first.typ, first.seq)
	}

	if !core.sendAckFrame(5, true) {
		t.Fatal("forced duplicate ACK was not scheduled")
	}
	_ = b.SetReadDeadline(time.Now().Add(time.Second))
	second, err := readWireFrame(b, core)
	if err != nil {
		t.Fatalf("forced cumulative ACK was not retransmitted: %v", err)
	}
	if second.typ != frameTypeAck || second.seq != 5 {
		t.Fatalf("unexpected forced ACK: type=%d value=%d", second.typ, second.seq)
	}
}

func TestSnapshotStatsTracksCumulativeAckFrontierLeg(t *testing.T) {
	now := time.Now()
	core, app := newCore(testCoreConfig())
	defer core.Close()
	defer app.Close()
	testLedgerAdvance(core, 10, now.Add(-4*time.Second))
	testLedgerAdd(core, []byte("slow-primary"), now.Add(-4*time.Second), 0)
	testLedgerAdd(core, []byte("fast-booster"), now.Add(-3*time.Second), 1)
	stats := core.snapshotStats()
	if !stats.AckFrontierValid {
		t.Fatal("ACK frontier not reported")
	}
	if stats.AckFrontierLeg != 0 {
		t.Fatalf("ACK frontier leg=%d, want leg0", stats.AckFrontierLeg)
	}
	if stats.AckFrontierAge < 3*time.Second {
		t.Fatalf("ACK frontier age=%s, want old leg0 blocker", stats.AckFrontierAge)
	}
	if stats.OutstandingFramesByLeg != [2]int{1, 1} {
		t.Fatalf("outstanding by leg=%v, want [1 1]", stats.OutstandingFramesByLeg)
	}
}

func TestCoreAdaptiveSchedulerPrefersUsefulLowLatencyLeg(t *testing.T) {
	cfg := testCoreConfig()
	cfg.SchedulerMode = schedulerAdaptive
	cfg.BandwidthMbps = []uint32{100, 100}
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	defer b0.Close()
	defer b1.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if !core.engine.SetLegPerformanceForTest(0, 20*1e6, 20*1e6, 180*time.Millisecond) ||
		!core.engine.SetLegPerformanceForTest(1, 20*1e6, 20*1e6, 2*time.Millisecond) {
		t.Fatal("failed to seed scheduler performance")
	}

	if chosen := core.chooseLeg(true, -1); chosen == nil || chosen.id != 1 {
		t.Fatalf("adaptive scheduler chose high-latency leg: %#v", chosen)
	}
}

func TestCoreStaticSchedulerIgnoresDynamicLatency(t *testing.T) {
	cfg := testCoreConfig()
	cfg.SchedulerMode = schedulerStatic
	cfg.BandwidthMbps = []uint32{400, 100}
	core, appConn := newCore(cfg)
	defer core.Close()
	defer appConn.Close()

	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	defer b0.Close()
	defer b1.Close()
	if err := core.addLeg(0, a0, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.addLeg(1, a1, nil); err != nil {
		t.Fatal(err)
	}
	if !core.engine.SetLegPerformanceForTest(0, 0, 0, time.Second) {
		t.Fatal("failed to seed scheduler performance")
	}
	if chosen := core.chooseLeg(true, -1); chosen == nil || chosen.id != 0 {
		t.Fatalf("static scheduler no longer honors configured weight: %#v", chosen)
	}
}

func TestCorePrimaryQueueSaturationImmediatelyActivatesBooster(t *testing.T) {
	cfg := testCoreConfig()
	cfg.QueueFrames = 1
	cfg.ChunkSize = 1024
	cfg.MaxInflightFrames = 16
	cfg.ActivationWindow = 5 * time.Second
	cfg.ThresholdBytesPS = 1 << 60
	activated := make(chan struct{}, 1)
	cfg.OnActivate = func() {
		select {
		case activated <- struct{}{}:
		default:
		}
	}
	core, app := newCore(cfg)
	defer core.Close()
	a, b := net.Pipe()
	defer b.Close()
	go func() { _, _ = io.Copy(io.Discard, b) }()
	release := make(chan struct{})
	slow := &gatedWriteConn{Conn: a, started: make(chan struct{}), release: release}
	if err := core.addLeg(0, slow, nil); err != nil {
		close(release)
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := app.Write(bytes.Repeat([]byte("q"), 8*cfg.ChunkSize))
		writeDone <- err
	}()
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("primary writer never blocked")
	}
	select {
	case <-activated:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("full preferred queue did not immediately activate booster")
	}
	close(release)
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("logical writer did not unblock after releasing primary")
	}
}

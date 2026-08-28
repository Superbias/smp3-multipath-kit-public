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
	if leftCore.frontierRescueAttempts.Load() == 0 {
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
	cfg := testCoreConfig()
	// retransmitLoop caps its periodic safety-net ticker at 250ms. Give the
	// frontier a much older send time and require the ACK-driven wake to queue
	// its rescue well before that ticker could fire.
	cfg.RetransmitTimeout = 4 * time.Second
	now := time.Now().Add(-10 * time.Second)
	leg0 := &mpLeg{
		id:     0,
		send:   make(chan txSendAttempt, 4),
		rescue: make(chan txSendAttempt, 4),
		done:   make(chan struct{}),
	}
	leg1 := &mpLeg{
		id:     1,
		send:   make(chan txSendAttempt, 4),
		rescue: make(chan txSendAttempt, 4),
		done:   make(chan struct{}),
	}
	core := &mpCore{
		cfg:              cfg,
		legs:             map[uint8]*mpLeg{0: leg0, 1: leg1},
		retiring:         make(map[uint8]*mpLeg),
		outstanding:      make(map[uint64]*txRecord),
		done:             make(chan struct{}),
		retryCh:          make(chan struct{}, 1),
		frontierRescueCh: make(chan struct{}, 1),
	}
	core.active.Store(true)
	core.txSeq.Store(2)
	core.outstanding[0] = &txRecord{
		seq:         0,
		data:        []byte("already-acked-head"),
		createdAt:   now,
		lastSentAt:  now,
		lastSentLeg: 0,
	}
	core.outstanding[1] = &txRecord{
		seq:         1,
		data:        []byte("new-overdue-frontier"),
		createdAt:   now,
		lastSentAt:  now,
		lastSentLeg: 0,
	}

	go core.retransmitLoop()
	defer close(core.done)

	if err := core.handleAck(1); err != nil {
		t.Fatal(err)
	}
	select {
	case attempt := <-leg1.rescue:
		if attempt.record != core.outstanding[1] || !attempt.rescue {
			t.Fatalf("unexpected ACK-paced rescue: %+v", attempt)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("new overdue ACK frontier waited for periodic retransmit ticker")
	}
	if got := waitForCounterAtLeast(t, "frontier rescue attempts", core.frontierRescueAttempts.Load, 1); got != 1 {
		t.Fatalf("frontier rescue attempts=%d, want 1", got)
	}
}

func TestCoreFrontierRescueNeverBurstsPastAckedNext(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RetransmitTimeout = 20 * time.Millisecond
	now := time.Now().Add(-time.Second)
	leg0 := &mpLeg{
		id:     0,
		send:   make(chan txSendAttempt, 4),
		rescue: make(chan txSendAttempt, 4),
		done:   make(chan struct{}),
	}
	leg1 := &mpLeg{
		id:     1,
		send:   make(chan txSendAttempt, 4),
		rescue: make(chan txSendAttempt, 4),
		done:   make(chan struct{}),
	}
	core := &mpCore{
		cfg:              cfg,
		legs:             map[uint8]*mpLeg{0: leg0, 1: leg1},
		retiring:         make(map[uint8]*mpLeg),
		outstanding:      make(map[uint64]*txRecord),
		ackedNext:        10,
		done:             make(chan struct{}),
		frontierRescueCh: make(chan struct{}, 1),
	}
	core.active.Store(true)
	core.txSeq.Store(12)
	head := &txRecord{seq: 10, data: []byte("head"), createdAt: now, lastSentAt: now, lastSentLeg: 0}
	next := &txRecord{seq: 11, data: []byte("later"), createdAt: now, lastSentAt: now, lastSentLeg: 0}
	core.outstanding[10] = head
	core.outstanding[11] = next

	core.scheduleFrontierRescue()
	select {
	case attempt := <-leg1.rescue:
		if attempt.record != head || !attempt.rescue {
			t.Fatalf("rescued non-frontier record: %+v", attempt)
		}
	default:
		t.Fatal("overdue ACK frontier was not rescued")
	}
	select {
	case attempt := <-leg1.rescue:
		t.Fatalf("frontier rescue burst past ackedNext to seq=%d", attempt.record.seq)
	default:
	}
	if next.rescueInTransit {
		t.Fatal("later unknown sequence was speculatively marked for rescue")
	}
}

func TestCoreFailedRescueEnqueueDoesNotConsumeCooldown(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RetransmitTimeout = time.Second
	now := time.Now().Add(-2 * time.Second)
	leg0 := &mpLeg{
		id:     0,
		send:   make(chan txSendAttempt, 2),
		rescue: make(chan txSendAttempt, 1),
		done:   make(chan struct{}),
	}
	leg1 := &mpLeg{
		id:     1,
		send:   make(chan txSendAttempt, 2),
		rescue: make(chan txSendAttempt, 1),
		done:   make(chan struct{}),
	}
	// Saturate the alternate leg's priority queue. This rescue attempt must fail
	// before enqueue and therefore must not consume RetransmitTimeout.
	leg1.rescue <- txSendAttempt{record: &txRecord{seq: 999}, rescue: true}
	core := &mpCore{
		cfg:         cfg,
		legs:        map[uint8]*mpLeg{0: leg0, 1: leg1},
		retiring:    make(map[uint8]*mpLeg),
		outstanding: make(map[uint64]*txRecord),
		ackedNext:   5,
		done:        make(chan struct{}),
	}
	core.active.Store(true)
	head := &txRecord{seq: 5, data: []byte("frontier"), createdAt: now, lastSentAt: now, lastSentLeg: 0}
	core.outstanding[5] = head

	core.scheduleFrontierRescue()
	if head.rescueInTransit {
		t.Fatal("failed rescue enqueue left rescueInTransit set")
	}
	if !head.lastRescueAt.IsZero() {
		t.Fatalf("failed rescue enqueue consumed cooldown at %v", head.lastRescueAt)
	}
	if got := core.frontierRescueAttempts.Load(); got != 0 {
		t.Fatalf("failed rescue enqueue counted as attempt: %d", got)
	}

	<-leg1.rescue // free the queue and retry immediately, without waiting 1s
	core.scheduleFrontierRescue()
	select {
	case attempt := <-leg1.rescue:
		if attempt.record != head || !attempt.rescue {
			t.Fatalf("unexpected immediate rescue after queue recovery: %+v", attempt)
		}
	default:
		t.Fatal("frontier remained throttled after failed rescue enqueue")
	}
	if got := waitForCounterAtLeast(t, "successful rescue attempts", core.frontierRescueAttempts.Load, 1); got != 1 {
		t.Fatalf("successful rescue attempts=%d, want 1", got)
	}
	if head.lastRescueAt.IsZero() {
		t.Fatal("successful rescue enqueue did not commit cooldown")
	}
}

func TestCoreDeadLegReplayIsFrontierFirst(t *testing.T) {
	cfg := testCoreConfig()
	leg1 := &mpLeg{
		id:     1,
		send:   make(chan txSendAttempt, 16),
		rescue: make(chan txSendAttempt, 2),
		done:   make(chan struct{}),
	}
	core := &mpCore{
		cfg:         cfg,
		legs:        map[uint8]*mpLeg{1: leg1},
		retiring:    make(map[uint8]*mpLeg),
		outstanding: make(map[uint64]*txRecord),
		ackedNext:   40,
		done:        make(chan struct{}),
	}
	core.active.Store(true)
	// These were last written by dead leg0. Map iteration order must never decide
	// the replay order because cumulative ACK can only advance from seq 40.
	for _, seq := range []uint64{47, 42, 45, 40, 46, 41, 44, 43} {
		core.outstanding[seq] = &txRecord{
			seq:         seq,
			data:        []byte{byte(seq)},
			createdAt:   time.Now().Add(-time.Second),
			lastSentAt:  time.Now().Add(-time.Second),
			lastSentLeg: 0,
		}
	}

	core.scheduleRetries()
	for want := uint64(40); want <= 47; want++ {
		select {
		case attempt := <-leg1.send:
			if attempt.record == nil || attempt.record.seq != want {
				t.Fatalf("replay order seq=%v, want %d", func() any {
					if attempt.record == nil {
						return nil
					}
					return attempt.record.seq
				}(), want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing replay seq=%d", want)
		}
	}
}

func TestCoreHealthyFrontierFastPathAvoidsLegSnapshotAllocation(t *testing.T) {
	cfg := testCoreConfig()
	cfg.RetransmitTimeout = time.Hour
	core := &mpCore{
		cfg: cfg,
		legs: map[uint8]*mpLeg{
			0: {id: 0, done: make(chan struct{})},
			1: {id: 1, done: make(chan struct{})},
		},
		retiring:    make(map[uint8]*mpLeg),
		outstanding: make(map[uint64]*txRecord),
		ackedNext:   1,
		done:        make(chan struct{}),
	}
	now := time.Now()
	core.outstanding[1] = &txRecord{seq: 1, createdAt: now, lastSentAt: now, lastSentLeg: 0}

	if allocs := testing.AllocsPerRun(1000, func() { core.scheduleFrontierRescue() }); allocs != 0 {
		t.Fatalf("healthy frontier fast path allocations=%v, want 0", allocs)
	}
}

func TestSnapshotStatsMarksConcurrentFrontierAttemptsMultipath(t *testing.T) {
	now := time.Now()
	core := &mpCore{
		legs:        make(map[uint8]*mpLeg),
		retiring:    make(map[uint8]*mpLeg),
		outstanding: make(map[uint64]*txRecord),
		ackedNext:   7,
	}
	core.outstanding[7] = &txRecord{
		seq:             7,
		data:            []byte("frontier"),
		createdAt:       now.Add(-3 * time.Second),
		inTransit:       true,
		transitLeg:      0,
		transitSince:    now.Add(-2 * time.Second),
		rescueInTransit: true,
		rescueLeg:       1,
		rescueSince:     now.Add(-time.Second),
		lastSentLeg:     -1,
	}
	stats := core.snapshotStats()
	if !stats.AckFrontierValid || !stats.AckFrontierMultiPath {
		t.Fatalf("concurrent frontier attempts not reported as multipath: %+v", stats)
	}
	if stats.OutstandingFramesByLeg != [2]int{1, 1} {
		t.Fatalf("outstanding ownership=%v, want [1 1]", stats.OutstandingFramesByLeg)
	}
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
		leftCore.txMu.Lock()
		remaining := len(leftCore.outstanding)
		leftCore.txMu.Unlock()
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

	record := &txRecord{seq: 0, data: []byte("pending"), lastSentLeg: 0}
	core.txSeq.Store(1)
	core.txMu.Lock()
	core.outstanding[0] = record
	core.txMu.Unlock()
	core.inflight <- struct{}{}

	if err := core.handleAck(99); err != nil {
		t.Fatalf("future ACK should be ignored, got %v", err)
	}
	core.txMu.Lock()
	_, stillOutstanding := core.outstanding[0]
	ackedNext := core.ackedNext
	core.txMu.Unlock()
	if !stillOutstanding || ackedNext != 0 {
		t.Fatalf("future ACK mutated TX state: outstanding=%v ackedNext=%d", stillOutstanding, ackedNext)
	}
	if anomalyCount != 1 {
		t.Fatalf("future ACK anomaly callback count=%d, want 1", anomalyCount)
	}

	if err := core.handleAck(1); err != nil {
		t.Fatalf("valid ACK failed: %v", err)
	}
	core.txMu.Lock()
	_, stillOutstanding = core.outstanding[0]
	ackedNext = core.ackedNext
	core.txMu.Unlock()
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
	core.txSeq.Store(frames)
	core.txMu.Lock()
	for seq := uint64(0); seq < frames; seq++ {
		core.outstanding[seq] = &txRecord{seq: seq, data: []byte{byte(seq)}, lastSentLeg: 0}
	}
	core.txMu.Unlock()

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
	core.closing.Store(true)
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
	record := &txRecord{
		seq:         0,
		data:        []byte("unacked-old-generation"),
		createdAt:   time.Now().Add(-time.Second),
		lastSentAt:  time.Now().Add(-time.Second),
		lastSentLeg: 0,
	}
	core.txMu.Lock()
	core.outstanding[0] = record
	core.txMu.Unlock()
	core.txSeq.Store(1)

	// Drive failure externally so the old read worker can be held after Close.
	core.handleLegFailure(old, io.ErrClosedPipe)
	core.txMu.Lock()
	ownerAfterFailure := record.lastSentLeg
	core.txMu.Unlock()
	if ownerAfterFailure != -1 {
		t.Fatalf("dead leg generation retained ownership=%d, want -1", ownerAfterFailure)
	}
	deadline := time.Now().Add(time.Second)
	for {
		core.legsMu.RLock()
		retiring := core.retiring[0]
		core.legsMu.RUnlock()
		if retiring == old {
			break
		}
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
	wrapped := &signalWriteConn{Conn: a, started: make(chan struct{})}
	if err := core.addLeg(0, wrapped, nil); err != nil {
		t.Fatal(err)
	}
	leg := core.getLeg(0)
	if leg == nil {
		t.Fatal("leg0 missing")
	}

	record0 := &txRecord{seq: 0, data: []byte("first"), lastSentLeg: -1}
	record1 := &txRecord{seq: 1, data: []byte("second"), lastSentLeg: -1}
	core.txMu.Lock()
	core.outstanding[0] = record0
	core.outstanding[1] = record1
	record0.inTransit = true
	record0.transitLeg = leg.id
	record0.transitSince = time.Now()
	record1.inTransit = true
	record1.transitLeg = leg.id
	record1.transitSince = time.Now()
	core.txMu.Unlock()
	leg.send <- txSendAttempt{record: record0}

	select {
	case <-wrapped.started:
		// The first DATA frame is already in progress and blocked on net.Pipe.
	case <-time.After(time.Second):
		t.Fatal("first DATA write did not start")
	}
	leg.send <- txSendAttempt{record: record1}
	if !core.sendAckFrame(9, false) {
		t.Fatal("ACK was not scheduled")
	}

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
	core.closing.Store(true)
	core.handleLegFailure(old, io.EOF)

	if core.active.Load() {
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
	core.txMu.Lock()
	core.outstanding[0] = &txRecord{seq: 0, data: []byte("tail"), lastSentLeg: 0}
	core.txMu.Unlock()

	old := core.getLeg(0)
	core.closing.Store(true)
	core.handleLegFailure(old, io.ErrClosedPipe)
	if !core.active.Load() {
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
	core := &mpCore{
		legs:        make(map[uint8]*mpLeg),
		retiring:    make(map[uint8]*mpLeg),
		outstanding: make(map[uint64]*txRecord),
		ackedNext:   10,
	}
	core.outstanding[10] = &txRecord{seq: 10, data: []byte("slow-primary"), createdAt: now.Add(-4 * time.Second), lastSentLeg: 0}
	core.outstanding[11] = &txRecord{seq: 11, data: []byte("fast-booster"), createdAt: now.Add(-3 * time.Second), lastSentLeg: 1}
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
	leg0 := core.getLeg(0)
	leg1 := core.getLeg(1)
	leg0.perf.mu.Lock()
	leg0.perf.ackedBPS = 20 * 1e6
	leg0.perf.writeBPS = 20 * 1e6
	leg0.perf.writeLatency = 180 * time.Millisecond
	leg0.perf.mu.Unlock()
	leg1.perf.mu.Lock()
	leg1.perf.ackedBPS = 20 * 1e6
	leg1.perf.writeBPS = 20 * 1e6
	leg1.perf.writeLatency = 2 * time.Millisecond
	leg1.perf.mu.Unlock()

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
	leg0 := core.getLeg(0)
	leg0.perf.mu.Lock()
	leg0.perf.writeLatency = time.Second
	leg0.perf.mu.Unlock()
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

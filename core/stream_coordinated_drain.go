package smp3core

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	streamDrainControlRetryInterval = 200 * time.Millisecond
	streamDrainPeerTimeout          = 30 * time.Second
	streamDrainReadyPollInterval    = 5 * time.Millisecond
)

var (
	ErrStreamDrainUnsupported     = errors.New("planned stream leg removal requires negotiated coordinated drain")
	ErrStreamDrainConflict        = errors.New("another coordinated drain is active for this leg")
	ErrStreamDrainCarrierChanged  = errors.New("carrier generation changed during coordinated drain")
	ErrStreamDrainCommitUncertain = errors.New("coordinated drain commit outcome uncertain; forced recovery started")
)

type streamDrainReplayKey struct {
	leg  uint8
	role StreamPathRole
}

type streamDrainOperation struct {
	target                *streamLeg
	operationID           [16]byte
	targetBinding         [16]byte
	counter               uint64
	originRole            StreamPathRole
	peerGeneration        uint64
	peerAckedNext         uint64
	peerOutstandingFrames uint32
	peerOutstandingBytes  uint64
	initiator             bool
	peerQuiesced          bool
	localReady            bool
	readySent             bool
	peerReady             bool
	commitSent            bool
	commitRequested       bool
	commitAck             bool
	terminalErr           error
	finished              bool
	changed               chan struct{}
	lastControlSent       time.Time
}

// StreamDrainEvent is payload-free operational evidence for one coordinated
// drain transition. Host runners should add run_id and endpoint metadata when
// persisting these events; the Core never records application payloads or
// authentication material.
type StreamDrainEvent struct {
	AtUnixNS                 int64          `json:"at_unix_ns"`
	Stage                    string         `json:"stage"`
	SessionID                string         `json:"session_id"`
	Direction                string         `json:"direction"`
	Role                     StreamPathRole `json:"role"`
	LegID                    uint8          `json:"leg_id"`
	Generation               uint64         `json:"generation"`
	DrainOperationID         string         `json:"drain_operation_id"`
	Counter                  uint64         `json:"counter"`
	LocalAckFrontier         uint64         `json:"local_ack_frontier"`
	PeerAckFrontier          uint64         `json:"peer_ack_frontier"`
	LocalTXOutstandingFrames uint32         `json:"local_tx_outstanding_frames"`
	LocalTXOutstandingBytes  uint64         `json:"local_tx_outstanding_bytes"`
	PeerTXOutstandingFrames  uint32         `json:"peer_tx_outstanding_frames"`
	PeerTXOutstandingBytes   uint64         `json:"peer_tx_outstanding_bytes"`
	RXPendingFrames          int            `json:"rx_pending_frames"`
	RXPendingBytes           uint64         `json:"rx_pending_bytes"`
	LocalQuiesceStarted      bool           `json:"local_quiesce_started"`
	LocalQuiesceCompleted    bool           `json:"local_quiesce_completed"`
	PeerQuiesceConfirmed     bool           `json:"peer_quiesce_confirmed"`
	LocalReady               bool           `json:"local_ready"`
	PeerReady                bool           `json:"peer_ready"`
	CommitSent               bool           `json:"commit_sent"`
	CommitRequested          bool           `json:"commit_requested"`
	CommitAck                bool           `json:"commit_ack"`
	TargetClosed             bool           `json:"target_closed"`
	TargetActiveBinding      bool           `json:"target_active_binding"`
	RetryWrittenBytes        uint64         `json:"retry_written_bytes"`
	Terminal                 bool           `json:"terminal"`
}

func (c *StreamEngine) emitDrainEvent(op *streamDrainOperation, stage string) {
	observer := c.cfg.DrainEventObserver
	if observer == nil || op == nil || op.target == nil {
		return
	}
	target := op.target
	c.drainMu.Lock()
	event := StreamDrainEvent{
		AtUnixNS: time.Now().UnixNano(), Stage: stage, SessionID: fmt.Sprintf("%x", c.drainSessionID[:]),
		Direction: "bidirectional", Role: c.cfg.PathRole, LegID: target.id, Generation: target.generation,
		DrainOperationID: fmt.Sprintf("%x", op.operationID[:]), Counter: op.counter,
		PeerAckFrontier: op.peerAckedNext, PeerTXOutstandingFrames: op.peerOutstandingFrames,
		PeerTXOutstandingBytes: op.peerOutstandingBytes, LocalQuiesceStarted: target.draining.Load(),
		LocalQuiesceCompleted: op.localReady, PeerQuiesceConfirmed: op.peerQuiesced,
		LocalReady: op.localReady, PeerReady: op.peerReady, CommitSent: op.commitSent,
		CommitRequested: op.commitRequested, CommitAck: op.commitAck, TargetClosed: target.closed.Load(),
		Terminal: op.finished,
	}
	c.drainMu.Unlock()
	event.LocalAckFrontier = c.txLedger.ProgressSnapshot().AckedNext
	localFrames := c.txLedger.outstandingForLeg(target.id)
	if localFrames > 0 {
		event.LocalTXOutstandingFrames = uint32(localFrames)
	}
	event.LocalTXOutstandingBytes = c.txLedger.outstandingBytesForLeg(target.id)
	event.RXPendingFrames = int(c.rxPendingFrames.Load())
	event.RXPendingBytes = c.rxPendingBytes.Load()
	event.TargetActiveBinding = c.getLeg(target.id) == target
	if int(target.id) < len(c.txRetryWrittenBytes) {
		event.RetryWrittenBytes = c.txRetryWrittenBytes[target.id].Load()
	}
	observer(event)
}

func (op *streamDrainOperation) signal() {
	select {
	case op.changed <- struct{}{}:
	default:
	}
}

// DrainLeg performs a negotiated bilateral quiesce and ACK settlement. A peer
// without Version7 coordinated-drain capability is never subjected to the old
// local-only graceful close; callers may explicitly choose Forced Remove.
func (c *StreamEngine) DrainLeg(ctx context.Context, id LegID) error {
	return c.coordinatedDrainLeg(ctx, id)
}

func (c *StreamEngine) coordinatedDrainLeg(ctx context.Context, id LegID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || int(id) >= c.maxActiveLegs {
		return ErrStreamLegNotFound
	}
	target := c.getLeg(uint8(id))
	if target == nil || target.closed.Load() {
		return nil // A physical failure already belongs to normal Recovery.
	}
	if c.legCount() <= 1 {
		return ErrStreamLastLeg
	}
	if !target.drainEnabled || !c.hasDrainControlLeg(target.id) {
		return ErrStreamDrainUnsupported
	}

	var opID [16]byte
	if _, err := rand.Read(opID[:]); err != nil {
		return fmt.Errorf("create coordinated drain operation ID: %w", err)
	}
	c.drainMu.Lock()
	if c.legCount()-len(c.drainOps) <= 1 {
		c.drainMu.Unlock()
		return ErrStreamLastLeg
	}
	if c.drainOps[target.id] != nil {
		c.drainMu.Unlock()
		return ErrStreamDrainConflict
	}
	target.dispatchMu.Lock()
	if target.closed.Load() || c.getLeg(target.id) != target {
		target.dispatchMu.Unlock()
		c.drainMu.Unlock()
		return ErrStreamDrainCarrierChanged
	}
	target.draining.Store(true)
	target.dispatchMu.Unlock()
	counter := c.drainOperationCounter.Add(1)
	op := &streamDrainOperation{
		target: target, operationID: opID, targetBinding: target.drainBinding,
		counter: counter, originRole: c.cfg.PathRole, initiator: true,
		changed: make(chan struct{}, 1), lastControlSent: time.Now(),
	}
	c.drainOps[target.id] = op
	c.drainMu.Unlock()
	c.emitDrainEvent(op, "drain_requested")
	c.emitDrainEvent(op, "local_quiesce_started")

	if err := c.sendDrainMessage(ctx, op, StreamDrainIntent); err != nil {
		c.emitDrainEvent(op, "drain_aborted")
		c.abortDrain(op, err, true)
		return err
	}
	ctxDone := ctx.Done()
	peerTimer := time.NewTimer(streamDrainPeerTimeout)
	defer peerTimer.Stop()
	ticker := time.NewTicker(streamDrainReadyPollInterval)
	defer ticker.Stop()
	for {
		if err := c.drainOperationError(op); err != nil {
			if errors.Is(err, ErrStreamDrainCarrierChanged) {
				err = c.drainCarrierChangedError(op, "operation_terminal")
			}
			c.abortDrain(op, err, true)
			return err
		}
		c.drainMu.Lock()
		commitAck := op.commitAck
		commitSent := op.commitSent
		c.drainMu.Unlock()
		if commitAck {
			if done, err := c.completeAcknowledgedDrain(op); done {
				return err
			}
		}
		if !commitAck && !commitSent && (target.closed.Load() || c.getLeg(target.id) != target) {
			err := c.drainCarrierChangedError(op, "precommit_binding_check")
			c.abortDrain(op, err, false)
			return err
		}
		if c.drainReady(target) {
			c.drainMu.Lock()
			becameLocalReady := false
			if c.drainOps[target.id] == op && !op.localReady {
				op.localReady = true
				op.signal()
				becameLocalReady = true
			}
			ready := op.localReady && op.peerQuiesced
			readySent := op.readySent
			peerReady := op.peerReady
			commitSent := op.commitSent
			lastControlSent := op.lastControlSent
			c.drainMu.Unlock()
			if becameLocalReady {
				c.emitDrainEvent(op, "local_quiesce_completed")
			}
			if ready && (!readySent || (!peerReady && time.Since(lastControlSent) >= streamDrainControlRetryInterval)) {
				if err := c.sendDrainMessage(ctx, op, StreamDrainReady); err != nil {
					c.abortDrain(op, err, true)
					return err
				}
				c.drainMu.Lock()
				if c.drainOps[target.id] == op {
					op.readySent = true
					op.lastControlSent = time.Now()
				}
				c.drainMu.Unlock()
			}
			if ready && readySent && peerReady && !commitSent {
				c.drainMu.Lock()
				canCommit := c.drainOps[target.id] == op && op.terminalErr == nil && op.readySent &&
					op.localReady && op.peerQuiesced && op.peerReady && !op.commitSent
				if canCommit {
					op.commitSent = true
					op.lastControlSent = time.Now()
				}
				c.drainMu.Unlock()
				if !canCommit {
					continue
				}
				if err := c.sendDrainMessage(ctx, op, StreamDrainCommit); err != nil {
					// Once COMMIT may have been delivered, do not reopen the local
					// scheduling barrier. Retire through existing Recovery instead.
					c.forceDrainRecovery(op, err)
					return ErrStreamDrainCommitUncertain
				}
			}
		}
		c.drainMu.Lock()
		commitAck = op.commitAck
		commitSent = op.commitSent
		c.drainMu.Unlock()
		if commitAck {
			if done, err := c.completeAcknowledgedDrain(op); done {
				return err
			}
		}
		var targetDone <-chan struct{}
		if !commitAck && !commitSent {
			targetDone = target.done
		}
		select {
		case <-op.changed:
		case <-ticker.C:
		case <-ctxDone:
			err := ctx.Err()
			if commitSent {
				uncertain := c.drainCommitUncertainError(op, "context_deadline")
				c.forceDrainRecovery(op, err)
				return uncertain
			}
			c.abortDrain(op, err, true)
			return err
		case <-peerTimer.C:
			err := context.DeadlineExceeded
			if commitSent {
				uncertain := c.drainCommitUncertainError(op, "peer_timeout")
				c.forceDrainRecovery(op, err)
				return uncertain
			}
			c.abortDrain(op, err, true)
			return err
		case <-targetDone:
			c.drainMu.Lock()
			commitInFlight := op.commitSent || op.commitAck
			c.drainMu.Unlock()
			if commitInFlight {
				continue
			}
			err := c.drainCarrierChangedError(op, "precommit_target_done")
			c.abortDrain(op, err, false)
			return err
		case <-c.done:
			err := ErrStreamClosed
			c.abortDrain(op, err, false)
			return err
		}
	}
}

func (c *StreamEngine) drainCarrierChangedError(op *streamDrainOperation, stage string) error {
	if op == nil || op.target == nil {
		return ErrStreamDrainCarrierChanged
	}
	target := op.target
	c.drainMu.Lock()
	localReady := op.localReady
	peerQuiesced := op.peerQuiesced
	readySent := op.readySent
	peerReady := op.peerReady
	commitSent := op.commitSent
	commitAck := op.commitAck
	finished := op.finished
	c.drainMu.Unlock()
	return fmt.Errorf("%w: stage=%s leg=%d operation=%x target_closed=%t current_binding=%t local_ready=%t peer_quiesced=%t ready_sent=%t peer_ready=%t commit_sent=%t commit_ack=%t finished=%t",
		ErrStreamDrainCarrierChanged, stage, target.id, op.operationID, target.closed.Load(), c.getLeg(target.id) == target,
		localReady, peerQuiesced, readySent, peerReady, commitSent, commitAck, finished)
}

func (c *StreamEngine) drainCommitUncertainError(op *streamDrainOperation, stage string) error {
	if op == nil || op.target == nil {
		return ErrStreamDrainCommitUncertain
	}
	target := op.target
	c.drainMu.Lock()
	localReady, peerQuiesced, readySent, peerReady := op.localReady, op.peerQuiesced, op.readySent, op.peerReady
	commitSent, commitRequested, commitAck, finished := op.commitSent, op.commitRequested, op.commitAck, op.finished
	peerFrames, peerBytes, peerAckedNext := op.peerOutstandingFrames, op.peerOutstandingBytes, op.peerAckedNext
	c.drainMu.Unlock()
	return fmt.Errorf("%w: stage=%s leg=%d operation=%x local_ready=%t peer_quiesced=%t ready_sent=%t peer_ready=%t commit_sent=%t commit_requested=%t commit_ack=%t peer_acked_next=%d peer_outstanding_frames=%d peer_outstanding_bytes=%d target_closed=%t current_binding=%t local_outstanding_frames=%d local_outstanding_bytes=%d finished=%t",
		ErrStreamDrainCommitUncertain, stage, target.id, op.operationID, localReady, peerQuiesced, readySent, peerReady,
		commitSent, commitRequested, commitAck, peerAckedNext, peerFrames, peerBytes, target.closed.Load(),
		c.getLeg(target.id) == target, c.txLedger.outstandingForLeg(target.id),
		c.txLedger.outstandingBytesForLeg(target.id), finished)
}

func (c *StreamEngine) completeAcknowledgedDrain(op *streamDrainOperation) (bool, error) {
	if op == nil || op.target == nil {
		return false, ErrStreamDrainCarrierChanged
	}
	target := op.target
	current := c.getLeg(target.id)
	if current == target && !target.closed.Load() {
		if hook := c.cfg.drainBeforeRetireHook; hook != nil {
			hook(target)
		}
		if !c.retireDrainedLeg(target) {
			// The peer may close the target immediately after sending the
			// authenticated CommitAck. The read loop marks the leg closed before
			// its failure handler removes this generation from the active registry.
			// Do not turn that expected cleanup window into a forced recovery.
			current = c.getLeg(target.id)
			if target.closed.Load() {
				if current == target {
					return false, nil
				}
				if c.txLedger.outstandingForLeg(target.id) == 0 {
					c.finishDrain(op, nil, false)
					return true, nil
				}
				return false, nil
			}
			err := c.drainRetireError(target)
			c.forceDrainRecovery(op, err)
			return true, err
		}
		c.finishDrain(op, nil, false)
		return true, nil
	}
	// A CommitAck is authenticated on a surviving carrier and proves the peer
	// committed this exact target binding. The peer may close the target socket
	// immediately after writing that ACK, racing the local read loop's normal
	// carrier-retirement path. Treat that expected close as graceful only after
	// the old leg has left the active registry and its TX ownership is settled.
	if target.closed.Load() {
		if current == target {
			return false, nil
		}
		if c.txLedger.outstandingForLeg(target.id) == 0 {
			c.finishDrain(op, nil, false)
			return true, nil
		}
	}
	return false, nil
}

func (c *StreamEngine) drainRetireError(target *streamLeg) error {
	if target == nil {
		return ErrStreamDrainCarrierChanged
	}
	return fmt.Errorf("%w: stage=commit_ack_retire leg=%d current_binding=%t closed=%t draining=%t ready=%t send_queued=%d rescue_queued=%d writer_state=%d outstanding_frames=%d outstanding_bytes=%d",
		ErrStreamDrainCarrierChanged, target.id, c.getLeg(target.id) == target, target.closed.Load(), target.draining.Load(),
		c.drainReady(target), len(target.send), len(target.rescue), target.debugWriterState.Load(),
		c.txLedger.outstandingForLeg(target.id), c.txLedger.outstandingBytesForLeg(target.id))
}

func (c *StreamEngine) hasDrainControlLeg(excluding uint8) bool {
	c.legsMu.RLock()
	defer c.legsMu.RUnlock()
	for id, leg := range c.legs {
		if id != excluding && leg.drainEnabled && !leg.draining.Load() && !leg.closed.Load() {
			return true
		}
	}
	return false
}

func (c *StreamEngine) drainReady(target *streamLeg) bool {
	if target == nil || target.closed.Load() || c.getLeg(target.id) != target || !target.draining.Load() {
		return false
	}
	if len(target.send) != 0 || len(target.rescue) != 0 || target.debugWriterState.Load() == 2 ||
		c.txLedger.outstandingForLeg(target.id) != 0 {
		return false
	}
	if int(target.id) < len(c.pendingDepth) && c.pendingDepth[target.id].Load() != 0 {
		return false
	}
	if int(target.id) < len(c.decoupledStats.feederHeld) && c.decoupledStats.feederHeld[target.id].Load() != 0 {
		return false
	}
	return true
}

func (c *StreamEngine) makeDrainMessage(op *streamDrainOperation, kind StreamDrainMessageKind) StreamDrainMessage {
	snapshot := c.txLedger.Snapshot(time.Now())
	outstanding := c.txLedger.outstandingForLeg(op.target.id)
	if outstanding < 0 {
		outstanding = 0
	}
	return StreamDrainMessage{
		Kind: kind, SenderRole: c.cfg.PathRole, SessionID: c.drainSessionID,
		TargetLeg: LegID(op.target.id), Generation: op.target.generation,
		TargetBinding: op.targetBinding, OperationID: op.operationID, Counter: op.counter,
		AckedNext: snapshot.AckedNext, OutstandingFrames: uint32(outstanding),
		OutstandingBytes: c.txLedger.outstandingBytesForLeg(op.target.id),
	}
}

func (c *StreamEngine) sendDrainMessage(ctx context.Context, op *streamDrainOperation, kind StreamDrainMessageKind) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, streamDrainPeerTimeout)
		defer cancel()
	}
	message := c.makeDrainMessage(op, kind)
	legs := make([]*streamLeg, 0, c.legCount())
	c.legsMu.RLock()
	for id, leg := range c.legs {
		if id != op.target.id && leg.drainEnabled && !leg.draining.Load() && !leg.closed.Load() {
			legs = append(legs, leg)
		}
	}
	c.legsMu.RUnlock()
	sort.Slice(legs, func(i, j int) bool { return legs[i].id < legs[j].id })
	var causes []error
	for _, carrier := range legs {
		wire, err := carrier.drainAuth.EncodeDrainMessage(message)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		done := make(chan error, 1)
		control := legControl{typ: frameTypeDrain, data: wire, done: done}
		select {
		case carrier.control <- control:
		case <-carrier.done:
			causes = append(causes, ErrStreamDrainCarrierChanged)
			continue
		default:
			causes = append(causes, errors.New("surviving carrier control queue is full"))
			continue
		}
		select {
		case err := <-done:
			if err == nil {
				switch kind {
				case StreamDrainIntent:
					c.emitDrainEvent(op, "drain_intent_sent")
				case StreamDrainQuiesced:
					c.emitDrainEvent(op, "peer_quiesce_response_sent")
				case StreamDrainReady:
					c.emitDrainEvent(op, "drain_ready_sent")
				case StreamDrainCommit:
					c.emitDrainEvent(op, "drain_commit_sent")
				case StreamDrainCommitAck:
					c.emitDrainEvent(op, "drain_commit_ack_sent")
				case StreamDrainAbort:
					c.emitDrainEvent(op, "drain_abort_sent")
				}
				return nil
			}
			causes = append(causes, err)
		case <-carrier.done:
			causes = append(causes, ErrStreamDrainCarrierChanged)
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return ErrStreamClosed
		}
	}
	if len(causes) == 0 {
		return ErrStreamDrainUnsupported
	}
	return fmt.Errorf("send coordinated drain control kind %d: %w", kind, errors.Join(causes...))
}

func (c *StreamEngine) handleDrainControlFrame(carrier *streamLeg, wire []byte) error {
	if carrier == nil || !carrier.drainEnabled {
		return ErrStreamDrainNegotiation
	}
	peerRole := StreamPathRoleClient
	if c.cfg.PathRole == StreamPathRoleClient {
		peerRole = StreamPathRoleServer
	} else if c.cfg.PathRole != StreamPathRoleServer {
		return ErrStreamPathRole
	}
	message, err := carrier.drainAuth.DecodeDrainMessage(wire, peerRole)
	if err != nil {
		return err
	}
	targetID := uint8(message.TargetLeg)
	if targetID == carrier.id {
		return nil
	}
	if message.Kind == StreamDrainIntent {
		target := c.getLeg(targetID)
		if target == nil || !target.drainEnabled || target.drainBinding != message.TargetBinding {
			// The control is correctly authenticated for this session but refers to
			// an absent or replaced carrier. Stale generations are inert and do not
			// invalidate a healthy control carrier.
			return nil
		}
		return c.acceptDrainIntent(target, message)
	}
	c.drainMu.Lock()
	op := c.drainOps[targetID]
	if op == nil || op.target == nil || op.target.id != targetID || !op.target.drainEnabled ||
		op.operationID != message.OperationID || op.counter != message.Counter ||
		op.targetBinding != message.TargetBinding || op.target.drainBinding != message.TargetBinding ||
		(op.peerGeneration != 0 && op.peerGeneration != message.Generation) {
		c.drainMu.Unlock()
		return nil // delayed/replayed operation or a control from an old peer generation
	}
	target := op.target
	current := c.getLeg(targetID)
	if current != nil && current != target {
		c.drainMu.Unlock()
		return nil // a replacement generation must not inherit the old drain operation
	}
	if current != target && (message.Kind != StreamDrainCommitAck || !target.closed.Load()) {
		c.drainMu.Unlock()
		return nil // only CommitAck can finish after the exact target generation retired
	}
	if op.peerGeneration == 0 {
		if current != target || target.closed.Load() || message.Kind == StreamDrainCommitAck {
			c.drainMu.Unlock()
			return nil // a retired target cannot establish the peer generation for a response
		}
		op.peerGeneration = message.Generation
	}
	op.peerAckedNext = message.AckedNext
	op.peerOutstandingFrames = message.OutstandingFrames
	op.peerOutstandingBytes = message.OutstandingBytes
	stage := ""
	switch message.Kind {
	case StreamDrainQuiesced:
		op.peerQuiesced = true
		stage = "peer_quiesce_confirmed"
	case StreamDrainReady:
		if message.OutstandingFrames != 0 || message.OutstandingBytes != 0 {
			op.terminalErr = ErrStreamDrainMessage
			op.signal()
			c.drainMu.Unlock()
			return nil
		}
		op.peerQuiesced = true
		op.peerReady = true
		stage = "peer_ready_received"
	case StreamDrainCommit:
		if message.OutstandingFrames == 0 && message.OutstandingBytes == 0 && !op.initiator && op.localReady && op.peerReady {
			op.commitRequested = true
			stage = "drain_commit_received"
		} else {
			op.terminalErr = ErrStreamDrainMessage
			stage = "drain_commit_rejected"
		}
	case StreamDrainCommitAck:
		if op.initiator && op.commitSent && message.OutstandingFrames == 0 && message.OutstandingBytes == 0 {
			op.commitAck = true
			stage = "drain_commit_ack_received"
		} else {
			op.terminalErr = ErrStreamDrainMessage
			stage = "drain_commit_ack_rejected"
		}
	case StreamDrainAbort:
		op.terminalErr = errors.New("peer aborted coordinated drain")
		stage = "drain_abort_received"
	default:
		c.drainMu.Unlock()
		return ErrStreamDrainMessage
	}
	op.signal()
	c.drainMu.Unlock()
	if stage != "" {
		c.emitDrainEvent(op, stage)
	}
	return nil
}

func (c *StreamEngine) acceptDrainIntent(target *streamLeg, message StreamDrainMessage) error {
	key := streamDrainReplayKey{leg: target.id, role: message.SenderRole}
	c.drainMu.Lock()
	if active := c.drainOps[target.id]; active != nil {
		if active.operationID == message.OperationID && active.counter == message.Counter && active.targetBinding == message.TargetBinding {
			active.signal() // duplicate intent is idempotent
			c.drainMu.Unlock()
			return nil
		}
		active.terminalErr = ErrStreamDrainConflict
		active.signal()
		c.drainRemoteHighWater[key] = message.Counter
		c.drainMu.Unlock()
		conflict := &streamDrainOperation{target: target, targetBinding: message.TargetBinding, operationID: message.OperationID,
			counter: message.Counter, originRole: message.SenderRole, peerGeneration: message.Generation, changed: make(chan struct{}, 1)}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		return c.sendDrainMessage(ctx, conflict, StreamDrainAbort)
	}
	if message.Counter <= c.drainRemoteHighWater[key] {
		c.drainMu.Unlock()
		return nil
	}
	if target.closed.Load() || c.getLeg(target.id) != target || c.legCount()-len(c.drainOps) <= 1 {
		c.drainRemoteHighWater[key] = message.Counter
		c.drainMu.Unlock()
		conflict := &streamDrainOperation{target: target, targetBinding: message.TargetBinding, operationID: message.OperationID,
			counter: message.Counter, originRole: message.SenderRole, peerGeneration: message.Generation, changed: make(chan struct{}, 1)}
		return c.sendDrainMessage(context.Background(), conflict, StreamDrainAbort)
	}
	target.dispatchMu.Lock()
	if target.closed.Load() || c.getLeg(target.id) != target {
		target.dispatchMu.Unlock()
		c.drainMu.Unlock()
		return nil
	}
	target.draining.Store(true)
	target.dispatchMu.Unlock()
	op := &streamDrainOperation{
		target: target, targetBinding: message.TargetBinding, operationID: message.OperationID,
		counter: message.Counter, originRole: message.SenderRole, peerGeneration: message.Generation,
		peerQuiesced: true, changed: make(chan struct{}, 1), lastControlSent: time.Time{},
	}
	c.drainRemoteHighWater[key] = message.Counter
	c.drainOps[target.id] = op
	c.drainMu.Unlock()
	c.emitDrainEvent(op, "drain_intent_received")
	c.emitDrainEvent(op, "local_quiesce_started")
	go c.runRemoteDrain(op)
	return nil
}

func (c *StreamEngine) runRemoteDrain(op *streamDrainOperation) {
	timer := time.NewTimer(streamDrainPeerTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(streamDrainReadyPollInterval)
	defer ticker.Stop()
	lastKind := StreamDrainMessageKind(0)
	lastSent := time.Time{}
	for {
		if err := c.drainOperationError(op); err != nil {
			c.abortDrain(op, err, true)
			return
		}
		if op.target.closed.Load() || c.getLeg(op.target.id) != op.target {
			c.finishDrain(op, ErrStreamDrainCarrierChanged, false)
			return
		}
		c.drainMu.Lock()
		ready := op.localReady
		commitRequested := op.commitRequested
		peerReady := op.peerReady
		becameLocalReady := false
		if c.drainReady(op.target) {
			becameLocalReady = !op.localReady
			op.localReady = true
			ready = true
		}
		c.drainMu.Unlock()
		if becameLocalReady {
			c.emitDrainEvent(op, "local_quiesce_completed")
		}
		kind := StreamDrainQuiesced
		if ready {
			kind = StreamDrainReady
		}
		if commitRequested && ready {
			kind = StreamDrainCommitAck
		}
		if kind != lastKind || time.Since(lastSent) >= streamDrainControlRetryInterval {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := c.sendDrainMessage(ctx, op, kind)
			cancel()
			if err != nil {
				// A response control path may fail while the target carrier remains
				// usable. Keep the quiesce barrier and retry on another negotiated
				// survivor until the operation's bounded timeout.
			} else {
				lastKind, lastSent = kind, time.Now()
			}
		}
		if commitRequested && ready && peerReady {
			// The CommitAck is the responder's final control-plane proof. Only
			// after the writer reports a complete authenticated frame do we retire
			// the target carrier locally.
			if c.retireDrainedLeg(op.target) {
				c.finishDrain(op, nil, false)
				return
			}
			c.forceDrainRecovery(op, ErrStreamDrainCarrierChanged)
			return
		}
		select {
		case <-op.changed:
		case <-ticker.C:
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			_ = c.sendDrainMessage(ctx, op, StreamDrainAbort)
			cancel()
			c.abortDrain(op, context.DeadlineExceeded, true)
			return
		case <-op.target.done:
			c.finishDrain(op, ErrStreamDrainCarrierChanged, false)
			return
		case <-c.done:
			c.finishDrain(op, ErrStreamClosed, false)
			return
		}
	}
}

func (c *StreamEngine) drainOperationError(op *streamDrainOperation) error {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	if c.drainOps[op.target.id] != op {
		if op.terminalErr != nil {
			return op.terminalErr
		}
		if op.finished {
			return ErrStreamDrainCarrierChanged
		}
	}
	return op.terminalErr
}

func (c *StreamEngine) abortDrain(op *streamDrainOperation, err error, restore bool) {
	if op == nil {
		return
	}
	c.drainMu.Lock()
	if op.commitSent || op.commitRequested {
		c.drainMu.Unlock()
		c.forceDrainRecovery(op, err)
		return
	}
	shouldNotifyPeer := op.initiator && !op.finished
	c.drainMu.Unlock()
	if shouldNotifyPeer {
		// Keep the local allocation barrier raised until the peer has had a
		// bounded opportunity to observe Abort. Otherwise the initiator could
		// resume DATA on the target while the responder remains quiesced.
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		_ = c.sendDrainMessage(ctx, op, StreamDrainAbort)
		cancel()
	}
	c.drainMu.Lock()
	if op.commitSent || op.commitRequested {
		c.drainMu.Unlock()
		c.forceDrainRecovery(op, err)
		return
	}
	if restore && c.getLeg(op.target.id) == op.target && !op.target.closed.Load() {
		op.target.dispatchMu.Lock()
		op.target.draining.Store(false)
		op.target.dispatchMu.Unlock()
	}
	op.terminalErr = err
	op.finished = true
	if c.drainOps[op.target.id] == op {
		delete(c.drainOps, op.target.id)
	}
	op.signal()
	c.drainMu.Unlock()
}

func (c *StreamEngine) finishDrain(op *streamDrainOperation, err error, restore bool) {
	if op == nil {
		return
	}
	c.drainMu.Lock()
	wasFinished := op.finished
	if restore && c.getLeg(op.target.id) == op.target && !op.target.closed.Load() {
		op.target.dispatchMu.Lock()
		op.target.draining.Store(false)
		op.target.dispatchMu.Unlock()
	}
	op.terminalErr = err
	op.finished = true
	if c.drainOps[op.target.id] == op {
		delete(c.drainOps, op.target.id)
	}
	op.signal()
	c.drainMu.Unlock()
	if !wasFinished {
		if err == nil {
			c.emitDrainEvent(op, "drain_completed")
		} else {
			c.emitDrainEvent(op, "drain_aborted")
		}
	}
}

func (c *StreamEngine) forceDrainRecovery(op *streamDrainOperation, cause error) {
	if op == nil {
		return
	}
	if c.getLeg(op.target.id) == op.target && !op.target.closed.Load() {
		c.emitDrainEvent(op, "recovery_started")
		c.ReplaceLeg(LegID(op.target.id), fmt.Errorf("coordinated drain commit uncertain: %w", cause))
	}
	c.finishDrain(op, cause, false)
}

func (c *StreamEngine) retireDrainedLeg(target *streamLeg) bool {
	if target == nil || !c.drainReady(target) {
		return false
	}
	target.dispatchMu.Lock()
	if target.closed.Load() || c.getLeg(target.id) != target || !target.draining.Load() ||
		len(target.send) != 0 || len(target.rescue) != 0 || target.debugWriterState.Load() == 2 ||
		c.txLedger.outstandingForLeg(target.id) != 0 {
		target.dispatchMu.Unlock()
		return false
	}
	c.legsMu.Lock()
	if c.legs[target.id] != target {
		c.legsMu.Unlock()
		target.dispatchMu.Unlock()
		return false
	}
	delete(c.legs, target.id)
	c.retiring[target.id] = target
	c.legsMu.Unlock()
	target.dispatchMu.Unlock()
	if target.pathRTTEnabled && c.pathRTTCache != nil {
		c.pathRTTCache.invalidateLeg(target.pathBinding)
	}
	if c.capacityProvider != nil {
		c.capacityProvider.Reset(target.id, time.Now())
	}
	target.close(nil)
	c.drainMu.Lock()
	op := c.drainOps[target.id]
	c.drainMu.Unlock()
	if op != nil && op.target == target {
		c.emitDrainEvent(op, "carrier_closed")
	}
	go func() {
		target.workers.Wait()
		c.legsMu.Lock()
		if c.retiring[target.id] == target {
			delete(c.retiring, target.id)
		}
		c.legsMu.Unlock()
		close(target.retired)
		c.kickRetry()
		c.kickSpaceSource("COORDINATED_DRAIN_RETIRED", int(target.id))
	}()
	return true
}

package multipath

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.MultipathOutboundOptions](registry, C.TypeMultipath, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx             context.Context
	manager         adapter.OutboundManager
	logger          log.ContextLogger
	tags            []string
	children        []adapter.Outbound
	leg1Fallback    adapter.Outbound
	leg1FallbackTag string
	udpTag          string
	udpOutbound     adapter.Outbound
	udpMultipath    bool
	udpCfg          datagramConfig
	aggregations    []M.Socksaddr
	cfg             coreConfig
	password        string
	redialInterval  time.Duration
	bootstrapDelay  time.Duration
	adaptive        adaptiveSettings
	adaptiveEnabled bool
	primaryHealth   primaryCarrierHealth
}

func adaptiveRoleTag(role carrierRole, primaryTag, fallbackTag string) string {
	if role == carrierFallback {
		return fallbackTag
	}
	return primaryTag
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) != 2 {
		return nil, E.New("multipath requires exactly 2 outbounds")
	}
	var aggregations []M.Socksaddr
	if len(options.Endpoints) > 0 {
		if len(options.Endpoints) != 2 {
			return nil, E.New("multipath endpoints must contain exactly 2 entries")
		}
		aggregations = make([]M.Socksaddr, 2)
		for idx, endpoint := range options.Endpoints {
			if endpoint.Server == "" || endpoint.ServerPort == 0 {
				return nil, E.New("invalid multipath endpoint at index ", idx)
			}
			aggregations[idx] = M.ParseSocksaddrHostPort(endpoint.Server, endpoint.ServerPort)
		}
	} else {
		if options.Server == "" || options.ServerPort == 0 {
			return nil, E.New("missing multipath endpoints or server/server_port")
		}
		shared := M.ParseSocksaddrHostPort(options.Server, options.ServerPort)
		aggregations = []M.Socksaddr{shared, shared}
	}
	if options.Password == "" {
		return nil, E.New("missing multipath password")
	}
	adaptiveEnabled := options.Leg1Adaptive != nil && options.Leg1Adaptive.Enabled
	adaptiveSettings := defaultAdaptiveSettings()
	if adaptiveEnabled {
		if options.Leg1Fallback == "" {
			return nil, E.New("leg1_fallback is required when leg1_adaptive.enabled")
		}
		if slices.Contains(options.Outbounds, options.Leg1Fallback) {
			return nil, E.New("leg1_fallback must be a separate outbound from outbounds")
		}
		var err error
		adaptiveSettings, err = makeAdaptiveSettings(*options.Leg1Adaptive)
		if err != nil {
			return nil, err
		}
	}
	preferred := options.Preferred
	if preferred == "" {
		preferred = options.Outbounds[0]
	}
	preferredIndex := slices.Index(options.Outbounds, preferred)
	if preferredIndex < 0 {
		return nil, E.New("preferred outbound is not in outbounds: ", preferred)
	}
	tags := append([]string(nil), options.Outbounds...)
	weights := append([]uint32(nil), options.BandwidthMbps...)
	if len(weights) != 0 && len(weights) != len(tags) {
		return nil, E.New("bandwidth_mbps must be empty or match outbounds length")
	}
	if preferredIndex != 0 {
		tags[0], tags[preferredIndex] = tags[preferredIndex], tags[0]
		aggregations[0], aggregations[preferredIndex] = aggregations[preferredIndex], aggregations[0]
		if len(weights) > 0 {
			weights[0], weights[preferredIndex] = weights[preferredIndex], weights[0]
		}
	}
	udpTag := options.UDPOutbound
	if udpTag == "" {
		udpTag = preferred
	}

	cfg, err := makeCoreConfig(
		options.ActivationThresholdMbps,
		time.Duration(options.ActivationWindow),
		int(options.ChunkSize),
		int(options.QueueFrames),
		weights,
		int(options.MaxReorderFrames),
		int(options.MaxInflightFrames),
		time.Duration(options.AckInterval),
		time.Duration(options.RetransmitTimeout),
		time.Duration(options.RecoveryTimeout),
	)
	if err != nil {
		return nil, err
	}
	scheduler, err := parseSchedulerMode(options.SchedulerMode)
	if err != nil {
		return nil, err
	}
	cfg.SchedulerMode = scheduler
	bootstrapDelay := time.Duration(options.BootstrapFallbackDelay)
	if bootstrapDelay <= 0 {
		bootstrapDelay = 250 * time.Millisecond
	}
	if bootstrapDelay < 25*time.Millisecond || bootstrapDelay > 10*time.Second {
		return nil, E.New("bootstrap_fallback_delay must be between 25ms and 10s")
	}
	udpEnabled := options.UDPMultipath != nil && options.UDPMultipath.Enabled
	udpCfg, err := makeDatagramConfig(options.UDPMultipath, weights, cfg.RecoveryTimeout)
	if err != nil {
		return nil, err
	}
	redialInterval := time.Duration(options.RedialInterval)
	if redialInterval <= 0 {
		redialInterval = time.Second
	}
	if redialInterval < 100*time.Millisecond || redialInterval > 30*time.Second {
		return nil, E.New("redial_interval must be between 100ms and 30s")
	}

	dependencies := append([]string(nil), options.Outbounds...)
	if !udpEnabled && !slices.Contains(dependencies, udpTag) {
		dependencies = append(dependencies, udpTag)
	}
	if adaptiveEnabled && !slices.Contains(dependencies, options.Leg1Fallback) {
		dependencies = append(dependencies, options.Leg1Fallback)
	}
	return &Outbound{
		Adapter:         outbound.NewAdapter(C.TypeMultipath, tag, []string{N.NetworkTCP, N.NetworkUDP}, dependencies),
		ctx:             ctx,
		manager:         service.FromContext[adapter.OutboundManager](ctx),
		logger:          logger,
		tags:            tags,
		udpTag:          udpTag,
		udpMultipath:    udpEnabled,
		udpCfg:          udpCfg,
		aggregations:    aggregations,
		password:        options.Password,
		cfg:             cfg,
		redialInterval:  redialInterval,
		bootstrapDelay:  bootstrapDelay,
		adaptive:        adaptiveSettings,
		adaptiveEnabled: adaptiveEnabled,
		leg1FallbackTag: options.Leg1Fallback,
	}, nil
}

func (o *Outbound) Start() error {
	o.children = make([]adapter.Outbound, len(o.tags))
	for index, tag := range o.tags {
		child, loaded := o.manager.Outbound(tag)
		if !loaded {
			return E.New("multipath child outbound not found: ", tag)
		}
		if !slices.Contains(child.Network(), N.NetworkTCP) {
			return E.New("multipath child does not support TCP: ", tag)
		}
		o.children[index] = child
	}
	if o.adaptiveEnabled {
		fallback, loaded := o.manager.Outbound(o.leg1FallbackTag)
		if !loaded {
			return E.New("multipath leg1 fallback outbound not found: ", o.leg1FallbackTag)
		}
		if !slices.Contains(fallback.Network(), N.NetworkTCP) {
			return E.New("multipath leg1 fallback outbound does not support TCP: ", o.leg1FallbackTag)
		}
		o.leg1Fallback = fallback
	}
	if !o.udpMultipath {
		udpOutbound, loaded := o.manager.Outbound(o.udpTag)
		if !loaded {
			return E.New("multipath UDP outbound not found: ", o.udpTag)
		}
		if !slices.Contains(udpOutbound.Network(), N.NetworkUDP) {
			return E.New("multipath UDP outbound does not support UDP: ", o.udpTag)
		}
		o.udpOutbound = udpOutbound
	}
	return nil
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkUDP:
		if o.udpMultipath {
			packetConn, err := o.ListenPacket(ctx, destination)
			if err != nil {
				return nil, err
			}
			return bufio.NewBindPacketConn(packetConn, destination), nil
		}
		return o.udpOutbound.DialContext(ctx, network, destination)
	case N.NetworkTCP:
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if len(o.children) != 2 {
		return nil, E.New("multipath outbound is not started")
	}

	sessionID, err := newSessionID()
	if err != nil {
		return nil, E.Cause(err, "generate multipath session id")
	}
	destinationString := destination.String()

	cfg := o.cfg
	sessionCtx, cancelSession := context.WithCancel(context.WithoutCancel(ctx))
	var core *mpCore
	var appConn net.Conn
	adaptive := newAdaptiveConn(o.adaptiveEnabled, o.adaptive, &o.primaryHealth, nil)
	type bootstrapResult struct {
		id         uint8
		conn       net.Conn
		carrierTag string
		err        error
	}
	dialBootstrapLeg := func(id uint8) bootstrapResult {
		dial := func(child adapter.Outbound, carrierTag string) bootstrapResult {
			conn, dialErr := child.DialContext(sessionCtx, N.NetworkTCP, o.aggregations[id])
			if dialErr == nil {
				dialErr = writeHello(conn, helloMessage{Session: sessionID, LegID: id, Mode: helloModeStream, Destination: destinationString}, o.password)
			}
			if dialErr != nil && conn != nil {
				_ = conn.Close()
				conn = nil
			}
			return bootstrapResult{id: id, conn: conn, carrierTag: carrierTag, err: dialErr}
		}

		child := o.children[id]
		carrierTag := o.tags[id]
		carrier := carrierPrimary
		if id == 1 && o.adaptiveEnabled {
			carrier = adaptive.carrierForLeg1(time.Now())
			if carrier == carrierFallback {
				child = o.leg1Fallback
				carrierTag = o.leg1FallbackTag
			}
		}
		result := dial(child, carrierTag)
		if result.err == nil || id != 1 || !o.adaptiveEnabled || carrier != carrierPrimary || sessionCtx.Err() != nil {
			return result
		}

		// A failed bootstrap primary carrier attempt is already actionable for this logical
		// connection. Switch locally to fallback carrier and retry it immediately, so a healthy
		// fallback can still win bootstrap when leg0 is unavailable.
		if adaptive.recordPrimaryFailure(false) && adaptive.currentCarrier() == carrierFallback {
			return dial(o.leg1Fallback, o.leg1FallbackTag)
		}
		return result
	}

	var dialMu sync.Mutex
	joining := [2]bool{}
	scheduled := [2]bool{}
	deferredRepair := [2]bool{}
	deferredDelay := [2]time.Duration{}
	var ensureLeg func(uint8)
	var scheduleLeg func(uint8, time.Duration)
	adaptive.onSelected = func(carrier carrierRole, probation bool) {
		if !o.adaptiveEnabled {
			return
		}
		o.logger.InfoContext(ctx, "multipath leg1 carrier selected: role=", carrier.String(), " tag=", adaptiveRoleTag(carrier, o.tags[1], o.leg1FallbackTag), " probation=", probation)
	}
	adaptive.onHealth = func(decision adaptiveDecision, stats coreStats) {
		// A zero-useful EOF on a just-added primary carrier leg can be the normal short-session
		// JOIN race rather than a carrier outage. In that case OnLegDown deliberately
		// avoids an immediate reconnect. If the logical stream remains active and
		// useful demand continues, the health loop requests a demand-driven repair.
		if core != nil && o.adaptiveEnabled && adaptive.currentCarrier() == carrierPrimary &&
			!core.isClosing() && !core.isFinalizing() && core.isActive() &&
			!stats.LegUp[1] && decision.Demand {
			scheduleLeg(1, o.redialInterval)
		}
		if decision.StateChanged && decision.State == adaptiveSuspect {
			if decision.Reason == "tx_ack_stall" {
				o.logger.InfoContext(ctx, "multipath leg1 carrier suspect: reason=", decision.Reason,
					" tx_outstanding=", stats.OutstandingFrames,
					" leg1_outstanding=", stats.OutstandingFramesByLeg[1],
					" ack_frontier_valid=", stats.AckFrontierValid,
					" ack_frontier_leg=", stats.AckFrontierLeg,
					" ack_frontier_multi=", stats.AckFrontierMultiPath,
					" ack_frontier_age=", stats.AckFrontierAge,
					" ack_progress_age=", ackProgressAge(stats))
			} else {
				o.logger.InfoContext(ctx, "multipath leg1 carrier suspect: reason=", decision.Reason,
					" rx_pending=", stats.RxPendingFrames, " gap_age=", stats.RxGapAge)
			}
		}
		if decision.Recovered {
			o.logger.InfoContext(ctx, "multipath leg1 carrier recovered: role=primary tag=", o.tags[1])
		}
		o.logger.DebugContext(ctx, "mp health: carrier=", adaptive.currentCarrier(),
			" state=", decision.State, " rx_goodput=", decision.RxLogicalGoodputBPS/1e6,
			" rx_baseline=", decision.RxBaselineGoodputBPS/1e6,
			" tx_goodput=", decision.TxLogicalGoodputBPS/1e6,
			" tx_baseline=", decision.TxBaselineGoodputBPS/1e6,
			" leg1_rx_goodput=", decision.Leg1RxGoodputBPS/1e6,
			" leg1_tx_goodput=", decision.Leg1TxGoodputBPS/1e6,
			" rx_pending=", stats.RxPendingFrames, " rx_gap_age=", stats.RxGapAge,
			" tx_outstanding=", stats.OutstandingFrames,
			" frontier_rescues=", stats.FrontierRescueAttempts,
			" ack_frontier_valid=", stats.AckFrontierValid,
			" ack_frontier_leg=", stats.AckFrontierLeg,
			" ack_frontier_multi=", stats.AckFrontierMultiPath,
			" ack_frontier_age=", stats.AckFrontierAge,
			" ack_progress_age=", ackProgressAge(stats))
	}
	adaptive.onRecovery = func() {
		o.primaryHealth.noteRecovery()
		o.logger.InfoContext(ctx, "multipath primary carrier cooldown cleared after healthy active canary tag=", o.tags[1])
	}
	adaptive.onActiveSuccess = func() {
		o.primaryHealth.noteActiveSuccess()
	}
	adaptive.onFallback = func(reason string, cooldown bool, probation bool) {
		// Global carrier health must be updated even during bootstrap, before a
		// logical core exists. In particular, a failed probation dial must re-enter
		// cooldown instead of leaving the single global canary slot stuck.
		o.logger.InfoContext(ctx, "multipath leg1 carrier fallback: primary=", o.tags[1], " fallback=", o.leg1FallbackTag, " reason=", reason)
		if cooldown || probation {
			duration := o.primaryHealth.noteFallback(time.Now(), o.adaptive, probation)
			o.logger.InfoContext(ctx, "multipath primary carrier cooldown: tag=", o.tags[1], " duration=", duration)
		}
		if core == nil || core.isClosing() || core.isFinalizing() {
			return
		}
		if !core.replaceLeg(1, &adaptiveCarrierReplacementError{from: carrierPrimary, reason: reason}) {
			scheduleLeg(1, o.redialInterval)
		}
	}

	// scheduleLeg coalesces delayed reconnect requests for a leg. This prevents
	// a carrier that accepts and then immediately drops a connection from causing
	// a tight connect/join/EOF loop that bypasses redial_interval.
	scheduleLeg = func(id uint8, delay time.Duration) {
		if id > 1 || core == nil {
			return
		}
		select {
		case <-core.Done():
			return
		default:
		}
		if core.isFinalizing() {
			return
		}
		if id == 1 && !core.isActive() {
			return
		}
		if core.hasLeg(id) {
			return
		}
		dialMu.Lock()
		if joining[id] {
			// Remember a repair request that races with the just-completed addLeg.
			// This replaces the old unconditional defer-repair, allowing benign
			// short-session EOFs to intentionally suppress immediate reconnect.
			if !deferredRepair[id] || deferredDelay[id] <= 0 || delay < deferredDelay[id] {
				deferredDelay[id] = delay
			}
			deferredRepair[id] = true
			dialMu.Unlock()
			return
		}
		if scheduled[id] {
			dialMu.Unlock()
			return
		}
		scheduled[id] = true
		dialMu.Unlock()
		time.AfterFunc(delay, func() {
			dialMu.Lock()
			scheduled[id] = false
			dialMu.Unlock()
			ensureLeg(id)
		})
	}

	ensureLeg = func(id uint8) {
		if id > 1 || core == nil {
			return
		}
		select {
		case <-core.Done():
			return
		default:
		}
		if core.isFinalizing() {
			return
		}
		if id == 1 && !core.isActive() {
			return
		}
		if core.hasLeg(id) {
			return
		}
		dialMu.Lock()
		if joining[id] {
			dialMu.Unlock()
			return
		}
		joining[id] = true
		dialMu.Unlock()
		go func() {
			defer func() {
				dialMu.Lock()
				joining[id] = false
				repair := deferredRepair[id]
				delay := deferredDelay[id]
				deferredRepair[id] = false
				deferredDelay[id] = 0
				dialMu.Unlock()
				select {
				case <-core.Done():
					return
				default:
				}
				// If OnLegDown requested repair while joining was still true, honor
				// it now. A benign EOF deliberately makes no such request.
				if repair && !core.hasLeg(id) && (id == 0 || core.isActive()) {
					if delay <= 0 {
						delay = o.redialInterval
					}
					scheduleLeg(id, delay)
				}
			}()
			for {
				select {
				case <-core.Done():
					return
				default:
				}
				if core.isFinalizing() {
					return
				}
				if id == 1 && !core.isActive() {
					return
				}
				if core.hasLeg(id) {
					return
				}
				child := o.children[id]
				carrierTag := o.tags[id]
				if id == 1 && o.adaptiveEnabled {
					if adaptive.carrierForLeg1(time.Now()) == carrierFallback {
						child = o.leg1Fallback
						carrierTag = o.leg1FallbackTag
					}
				}
				conn, dialErr := child.DialContext(sessionCtx, N.NetworkTCP, o.aggregations[id])
				if dialErr == nil {
					dialErr = writeHello(conn, helloMessage{Session: sessionID, LegID: id, Destination: destinationString}, o.password)
				}
				if dialErr == nil && core.isFinalizing() {
					_ = conn.Close()
					return
				}
				if dialErr == nil {
					dialErr = core.addLeg(id, conn, nil)
				}
				if dialErr == nil {
					if id == 1 {
						adaptive.markLegReady()
					}
					o.logger.InfoContext(ctx, "multipath leg ", id, " ready via ", carrierTag, " for ", destination)
					return
				}
				if conn != nil {
					conn.Close()
				}
				select {
				case <-core.Done():
					return
				default:
				}
				if errors.Is(dialErr, errCoreClosed) {
					return
				}
				if id == 1 && o.adaptiveEnabled && !core.isClosing() && !core.isFinalizing() && adaptive.currentCarrier() == carrierPrimary {
					stats := core.snapshotStats()
					if adaptive.shouldRecordCarrierFailure(dialErr, stats, false) {
						// This attempt never became an installed SMP3 leg. Treat it as an
						// initial carrier failure even if an older attempt had been ready.
						if adaptive.recordPrimaryFailure(false) {
							continue
						}
					} else {
						o.logger.DebugContext(ctx, "multipath ignored ambiguous short-session leg1 EOF before join for ", destination, ": ", dialErr)
					}
				}
				o.logger.WarnContext(ctx, "multipath leg ", id, " redial failed via ", carrierTag, ": ", dialErr)
				timer := time.NewTimer(o.redialInterval)
				select {
				case <-core.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}

	cfg.OnFutureAck = func(next, max, count uint64) {
		o.logger.WarnContext(ctx, "multipath ignored future ACK for ", destination, ": next=", next, " tx_seq=", max, " count=", count)
	}
	cfg.OnActivate = func() {
		o.logger.InfoContext(ctx, "multipath booster activated for ", destination)
		ensureLeg(1)
	}
	cfg.OnLegDown = func(id uint8, legErr error) {
		select {
		case <-core.Done():
			return
		default:
		}
		if core.isFinalizing() {
			return
		}
		// Preserve the carrier that actually owned leg1. Ordinary failures capture the
		// current carrier before health accounting can switch it; intentional adaptive
		// replacement carries the old carrier explicitly because the state switch
		// necessarily happens before core.replaceLeg().
		carrierAtFailure := carrierPrimary
		intentionalReplacement := false
		if id == 1 && o.adaptiveEnabled {
			carrierAtFailure = adaptive.currentCarrier()
			var replacementErr *adaptiveCarrierReplacementError
			if errors.As(legErr, &replacementErr) {
				carrierAtFailure = replacementErr.from
				intentionalReplacement = true
			}
		}
		benignSecondaryEOF := false
		if id == 1 && o.adaptiveEnabled && !intentionalReplacement && !core.isClosing() && !core.isFinalizing() && carrierAtFailure == carrierPrimary {
			stats := core.snapshotStats()
			if adaptive.shouldRecordCarrierFailure(legErr, stats, true) {
				adaptive.recordPrimaryFailure(true)
			} else {
				benignSecondaryEOF = true
				if !adaptive.noteAmbiguousCarrierEOF() {
					o.logger.DebugContext(ctx, "multipath ignored first ambiguous zero-useful leg1 EOF for ", destination, ": ", legErr)
				}
			}
		}
		carrierTag := o.tags[id]
		if id == 1 && o.adaptiveEnabled && carrierAtFailure == carrierFallback {
			carrierTag = o.leg1FallbackTag
		}
		o.logger.WarnContext(ctx, "multipath leg ", id, " down via ", carrierTag, ": ", legErr)
		// Repair the failed leg no faster than redial_interval. For an ambiguous
		// zero-useful primary carrier EOF we intentionally do not reconnect immediately; the
		// adaptive health loop will request repair if useful demand persists. This
		// prevents short HTTP/TLS sessions from creating a connect/JOIN/EOF storm.
		if !benignSecondaryEOF {
			scheduleLeg(id, o.redialInterval)
		}
		if id == 0 {
			// Primary loss is special: bring the already-authorized booster up now
			// so the logical connection is not forced to wait for the repair delay.
			ensureLeg(1)
		}
	}
	// Delayed parallel bootstrap. Leg0 gets a head start, but a slow initial
	// handshake is not allowed to serialize logical-session establishment forever.
	// A hard leg0 failure starts leg1 immediately; otherwise leg1 starts after the
	// configured fallback delay. The first authenticated HELLO wins bootstrap.
	bootstrapResults := make(chan bootstrapResult, 2)
	bootstrapStarted := [2]bool{}
	bootstrapCompleted := [2]bool{}
	bootstrapErrors := [2]error{}
	startBootstrap := func(id uint8) {
		if bootstrapStarted[id] {
			return
		}
		bootstrapStarted[id] = true
		go func() { bootstrapResults <- dialBootstrapLeg(id) }()
	}
	startBootstrap(0)
	bootstrapTimer := time.NewTimer(o.bootstrapDelay)
	defer bootstrapTimer.Stop()
	var first bootstrapResult
bootstrapLoop:
	for {
		select {
		case <-ctx.Done():
			cancelSession()
			return nil, ctx.Err()
		case <-bootstrapTimer.C:
			startBootstrap(1)
		case result := <-bootstrapResults:
			bootstrapCompleted[result.id] = true
			bootstrapErrors[result.id] = result.err
			if result.err == nil {
				first = result
				break bootstrapLoop
			}
			if result.id == 0 {
				startBootstrap(1)
			}
			if bootstrapStarted[0] && bootstrapStarted[1] && bootstrapCompleted[0] && bootstrapCompleted[1] {
				cancelSession()
				return nil, E.New("multipath bootstrap failed: leg0=", bootstrapErrors[0], " leg1=", bootstrapErrors[1])
			}
		}
	}
	if !bootstrapTimer.Stop() {
		select {
		case <-bootstrapTimer.C:
		default:
		}
	}

	core, appConn = newCore(cfg)
	adaptive.core = core
	go func() {
		<-core.Done()
		cancelSession()
	}()
	if err = core.addLeg(first.id, first.conn, nil); err != nil {
		_ = first.conn.Close()
		_ = appConn.Close()
		_ = core.Close()
		return nil, err
	}
	if first.id == 1 {
		adaptive.markLegReady()
		// If the fallback path created the logical stream, it must be schedulable
		// immediately and independently repairable even before throughput activation.
		core.activate()
	}
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " bootstrapped via leg ", first.id, " / ", first.carrierTag)

	// If the delayed race already launched the other leg, do not discard that
	// in-flight transport. Join a successful result into this same session; failed
	// speculative leg1 attempts remain lazy, while a missing preferred leg0 is
	// repaired because a leg1 bootstrap necessarily activated the core.
	otherID := first.id ^ 1
	if bootstrapStarted[otherID] {
		if bootstrapCompleted[otherID] {
			if first.id == 1 && bootstrapErrors[otherID] != nil {
				scheduleLeg(otherID, o.redialInterval)
			}
		} else {
			dialMu.Lock()
			joining[otherID] = true
			dialMu.Unlock()
			go func(expected uint8) {
				result := <-bootstrapResults
				dialMu.Lock()
				joining[expected] = false
				dialMu.Unlock()
				select {
				case <-core.Done():
					if result.conn != nil {
						_ = result.conn.Close()
					}
					return
				default:
				}
				if result.id != expected {
					if result.conn != nil {
						_ = result.conn.Close()
					}
					return
				}
				if result.err == nil {
					result.err = core.addLeg(result.id, result.conn, nil)
				}
				if result.err == nil {
					if result.id == 1 {
						adaptive.markLegReady()
						// A speculative leg1 that was slow enough to be launched is useful
						// immediately; activate rather than keep an idle paid-for transport.
						core.activate()
					}
					o.logger.InfoContext(ctx, "multipath bootstrap companion leg ", result.id, " ready via ", result.carrierTag, " for ", destination)
					return
				}
				if result.conn != nil {
					_ = result.conn.Close()
				}
				if expected == 0 || core.isActive() {
					scheduleLeg(expected, o.redialInterval)
				}
			}(otherID)
		}
	}
	return appConn, nil
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if !o.udpMultipath {
		if o.udpOutbound == nil {
			return nil, E.New("multipath outbound is not started")
		}
		return o.udpOutbound.ListenPacket(ctx, destination)
	}
	return o.newDatagramPacketConn(ctx, destination)
}

func (o *Outbound) newDatagramPacketConn(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if len(o.children) != 2 {
		return nil, E.New("multipath outbound is not started")
	}
	sessionID, err := newSessionID()
	if err != nil {
		return nil, E.Cause(err, "generate multipath UDP session id")
	}
	destinationString := destination.String()
	sessionCtx, cancelSession := context.WithCancel(context.WithoutCancel(ctx))
	udpAdaptive := newAdaptiveConn(o.adaptiveEnabled, o.adaptive, &o.primaryHealth, nil)
	udpAdaptive.onSelected = func(carrier carrierRole, probation bool) {
		if o.adaptiveEnabled {
			o.logger.InfoContext(ctx, "multipath UDP leg1 carrier selected: role=", carrier.String(), " tag=", adaptiveRoleTag(carrier, o.tags[1], o.leg1FallbackTag), " probation=", probation)
		}
	}
	udpAdaptive.onFallback = func(reason string, cooldown bool, probation bool) {
		o.logger.InfoContext(ctx, "multipath UDP leg1 carrier fallback: primary=", o.tags[1], " fallback=", o.leg1FallbackTag, " reason=", reason)
		if cooldown || probation {
			duration := o.primaryHealth.noteFallback(time.Now(), o.adaptive, probation)
			o.logger.InfoContext(ctx, "multipath primary carrier cooldown: tag=", o.tags[1], " duration=", duration, " source=udp")
		}
	}
	udpAdaptive.onRecovery = func() {
		o.primaryHealth.noteRecovery()
		o.logger.InfoContext(ctx, "multipath primary carrier cooldown cleared after useful UDP probation traffic tag=", o.tags[1])
	}
	udpAdaptive.onProbationRelease = func() { o.primaryHealth.releaseProbation() }

	var udpPrimaryUsefulReported atomic.Bool
	dialLeg := func(id uint8) (net.Conn, string, error) {
		dial := func(child adapter.Outbound, carrierTag string) (net.Conn, string, error) {
			conn, dialErr := child.DialContext(sessionCtx, N.NetworkTCP, o.aggregations[id])
			if dialErr == nil {
				dialErr = writeHello(conn, helloMessage{Session: sessionID, LegID: id, Mode: helloModeDatagram, Destination: destinationString}, o.password)
			}
			if dialErr != nil && conn != nil {
				_ = conn.Close()
				conn = nil
			}
			return conn, carrierTag, dialErr
		}

		child := o.children[id]
		carrierTag := o.tags[id]
		carrier := carrierPrimary
		if id == 1 && o.adaptiveEnabled {
			carrier = udpAdaptive.carrierForLeg1(time.Now())
			if carrier == carrierFallback {
				child = o.leg1Fallback
				carrierTag = o.leg1FallbackTag
			}
		}
		conn, usedTag, dialErr := dial(child, carrierTag)
		if dialErr == nil || id != 1 || !o.adaptiveEnabled || carrier != carrierPrimary || sessionCtx.Err() != nil {
			return conn, usedTag, dialErr
		}
		if udpAdaptive.recordPrimaryFailure(false) && udpAdaptive.currentCarrier() == carrierFallback {
			return dial(o.leg1Fallback, o.leg1FallbackTag)
		}
		return conn, usedTag, dialErr
	}

	// Datagram bootstrap uses the same delayed race as the stream data plane.
	// This avoids making every UDP flow wait behind a slow preferred carrier while
	// still giving leg0 a configurable head start.
	type udpBootstrapResult struct {
		id         uint8
		conn       net.Conn
		carrierTag string
		err        error
	}
	bootstrapResults := make(chan udpBootstrapResult, 2)
	bootstrapStarted := [2]bool{}
	bootstrapCompleted := [2]bool{}
	bootstrapErrors := [2]error{}
	startBootstrap := func(id uint8) {
		if bootstrapStarted[id] {
			return
		}
		bootstrapStarted[id] = true
		go func() {
			conn, carrierTag, dialErr := dialLeg(id)
			bootstrapResults <- udpBootstrapResult{id: id, conn: conn, carrierTag: carrierTag, err: dialErr}
		}()
	}
	startBootstrap(0)
	timer := time.NewTimer(o.bootstrapDelay)
	defer timer.Stop()
	var first udpBootstrapResult
udpBootstrapLoop:
	for {
		select {
		case <-ctx.Done():
			cancelSession()
			udpAdaptive.releaseUnusedProbation()
			return nil, ctx.Err()
		case <-timer.C:
			startBootstrap(1)
		case result := <-bootstrapResults:
			bootstrapCompleted[result.id] = true
			bootstrapErrors[result.id] = result.err
			if result.err == nil {
				first = result
				break udpBootstrapLoop
			}
			if result.id == 0 {
				startBootstrap(1)
			}
			if bootstrapStarted[0] && bootstrapStarted[1] && bootstrapCompleted[0] && bootstrapCompleted[1] {
				cancelSession()
				udpAdaptive.releaseUnusedProbation()
				return nil, E.New("multipath UDP bootstrap failed: leg0=", bootstrapErrors[0], " leg1=", bootstrapErrors[1])
			}
		}
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	cfg := o.udpCfg
	var core *mpDatagramCore
	var joiningMu sync.Mutex
	joining := [2]bool{}
	var ensureLeg func(uint8)
	cfg.OnLegUseful = func(id uint8, _ int) {
		if id != 1 || !o.adaptiveEnabled || udpAdaptive.currentCarrier() != carrierPrimary {
			return
		}
		if !udpPrimaryUsefulReported.CompareAndSwap(false, true) {
			return
		}
		// Unlike TCP, UDP has no cumulative ACK signal. A probation carrier is
		// therefore promoted only after a real application datagram has actually
		// traversed leg1 in either direction, not merely after HELLO/dial success.
		if !udpAdaptive.completeProbationRecovery() {
			o.primaryHealth.noteActiveSuccess()
		}
	}
	cfg.OnLegDown = func(id uint8, legErr error) {
		select {
		case <-core.Done():
			return
		default:
		}
		carrierAtFailure := carrierPrimary
		if id == 1 && o.adaptiveEnabled {
			carrierAtFailure = udpAdaptive.currentCarrier()
			if carrierAtFailure == carrierPrimary {
				udpAdaptive.recordPrimaryFailure(true)
			}
		}
		carrierTag := o.tags[id]
		if id == 1 && o.adaptiveEnabled && carrierAtFailure == carrierFallback {
			carrierTag = o.leg1FallbackTag
		}
		o.logger.WarnContext(ctx, "multipath UDP leg ", id, " down via ", carrierTag, ": ", legErr)
		time.AfterFunc(o.redialInterval, func() { ensureLeg(id) })
	}
	core, packetConn := newDatagramCore(cfg)

	// Install the repair closure before addLeg starts read/write workers. An
	// immediately-closing bootstrap transport can therefore never race OnLegDown
	// against an uninitialized ensureLeg function.
	ensureLeg = func(id uint8) {
		if id > 1 || core == nil || core.hasLeg(id) {
			return
		}
		select {
		case <-core.Done():
			return
		default:
		}
		joiningMu.Lock()
		if joining[id] {
			joiningMu.Unlock()
			return
		}
		joining[id] = true
		joiningMu.Unlock()
		go func() {
			defer func() { joiningMu.Lock(); joining[id] = false; joiningMu.Unlock() }()
			for {
				select {
				case <-core.Done():
					return
				default:
				}
				if core.hasLeg(id) {
					return
				}
				conn, carrierTag, dialErr := dialLeg(id)
				if dialErr == nil {
					if id == 1 && o.adaptiveEnabled && udpAdaptive.currentCarrier() == carrierPrimary {
						udpPrimaryUsefulReported.Store(false)
					}
					dialErr = core.addLeg(id, conn, nil)
				}
				if dialErr == nil {
					o.logger.InfoContext(ctx, "multipath UDP leg ", id, " ready via ", carrierTag, " for ", destination)
					return
				}
				if conn != nil {
					_ = conn.Close()
				}
				o.logger.WarnContext(ctx, "multipath UDP leg ", id, " redial failed: ", dialErr)
				timer := time.NewTimer(o.redialInterval)
				select {
				case <-core.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}

	if first.id == 1 && o.adaptiveEnabled && udpAdaptive.currentCarrier() == carrierPrimary {
		udpPrimaryUsefulReported.Store(false)
	}
	if err := core.addLeg(first.id, first.conn, nil); err != nil {
		cancelSession()
		udpAdaptive.releaseUnusedProbation()
		_ = first.conn.Close()
		_ = core.Close()
		return nil, err
	}
	o.logger.InfoContext(ctx, "multipath UDP session to ", destination, " bootstrapped via leg ", first.id, " / ", first.carrierTag, " mode=", cfg.Mode.String())

	go func() {
		<-core.Done()
		cancelSession()
		udpAdaptive.releaseUnusedProbation()
	}()

	otherID := first.id ^ 1
	if bootstrapStarted[otherID] && !bootstrapCompleted[otherID] {
		joiningMu.Lock()
		joining[otherID] = true
		joiningMu.Unlock()
		go func(expected uint8) {
			result := <-bootstrapResults
			joiningMu.Lock()
			joining[expected] = false
			joiningMu.Unlock()
			select {
			case <-core.Done():
				if result.conn != nil {
					_ = result.conn.Close()
				}
				return
			default:
			}
			if result.id == expected && result.err == nil {
				if result.id == 1 && o.adaptiveEnabled && udpAdaptive.currentCarrier() == carrierPrimary {
					udpPrimaryUsefulReported.Store(false)
				}
				result.err = core.addLeg(result.id, result.conn, nil)
			}
			if result.err == nil {
				o.logger.InfoContext(ctx, "multipath UDP bootstrap companion leg ", result.id, " ready via ", result.carrierTag, " for ", destination)
				return
			}
			if result.conn != nil {
				_ = result.conn.Close()
			}
			ensureLeg(expected)
		}(otherID)
	} else {
		// Datagram mode intentionally wants both paths available for stripe,
		// duplicate and adaptive decisions, so repair/join the companion even if
		// its speculative bootstrap already failed or was never started.
		ensureLeg(otherID)
	}
	return newSingDatagramPacketConn(packetConn), nil
}

func parseSchedulerMode(value string) (schedulerMode, error) {
	switch value {
	case "", "adaptive":
		return schedulerAdaptive, nil
	case "static":
		return schedulerStatic, nil
	default:
		return schedulerStatic, E.New("scheduler_mode must be static or adaptive")
	}
}

func makeDatagramConfig(options *option.MultipathUDPOptions, weights []uint32, recoveryTimeout time.Duration) (datagramConfig, error) {
	cfg := datagramConfig{Mode: datagramModeAdaptive, BandwidthMbps: append([]uint32(nil), weights...), RecoveryTimeout: recoveryTimeout}
	if options == nil || !options.Enabled {
		return cfg, nil
	}
	switch options.Mode {
	case "", "adaptive":
		cfg.Mode = datagramModeAdaptive
	case "stripe":
		cfg.Mode = datagramModeStripe
	case "duplicate":
		cfg.Mode = datagramModeDuplicate
	default:
		return datagramConfig{}, E.New("udp_multipath.mode must be stripe, duplicate or adaptive")
	}
	cfg.QueueFrames = int(options.QueueFrames)
	if cfg.QueueFrames == 0 {
		cfg.QueueFrames = 256
	}
	if cfg.QueueFrames < 8 || cfg.QueueFrames > 4096 {
		return datagramConfig{}, E.New("udp_multipath.queue_frames must be between 8 and 4096")
	}
	cfg.MaxDatagramSize = int(options.MaxDatagramSize)
	if cfg.MaxDatagramSize == 0 {
		cfg.MaxDatagramSize = maxRoutedDatagramSize
	}
	if cfg.MaxDatagramSize < 512 || cfg.MaxDatagramSize > maxRoutedDatagramSize {
		return datagramConfig{}, E.New("udp_multipath.max_datagram_size must be between 512 and 16384")
	}
	cfg.DedupWindow = uint64(options.DedupWindow)
	if cfg.DedupWindow == 0 {
		cfg.DedupWindow = 4096
	}
	if cfg.DedupWindow < 64 || cfg.DedupWindow > 1<<20 {
		return datagramConfig{}, E.New("udp_multipath.dedup_window must be between 64 and 1048576")
	}
	cfg.IdleTimeout = time.Duration(options.IdleTimeout)
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 2 * time.Minute
	}
	if cfg.IdleTimeout < 5*time.Second || cfg.IdleTimeout > time.Hour {
		return datagramConfig{}, E.New("udp_multipath.idle_timeout must be between 5s and 1h")
	}
	cfg.AdaptiveQueueDelay = time.Duration(options.AdaptiveQueueDelay)
	if cfg.AdaptiveQueueDelay <= 0 {
		cfg.AdaptiveQueueDelay = 120 * time.Millisecond
	}
	if cfg.AdaptiveQueueDelay < time.Millisecond || cfg.AdaptiveQueueDelay > 5*time.Second {
		return datagramConfig{}, E.New("udp_multipath.adaptive_queue_delay must be between 1ms and 5s")
	}
	cfg.AdaptiveDuplicateThreshold = int(options.AdaptiveDuplicateThreshold)
	if cfg.AdaptiveDuplicateThreshold < 0 || cfg.AdaptiveDuplicateThreshold > cfg.MaxDatagramSize {
		return datagramConfig{}, E.New("invalid udp_multipath.adaptive_duplicate_threshold")
	}
	return cfg, nil
}

func makeCoreConfig(threshold uint32, window time.Duration, chunkSize, queueFrames int, weights []uint32, maxReorder, maxInflight int, ackInterval, retransmitTimeout, recoveryTimeout time.Duration) (coreConfig, error) {
	if threshold == 0 {
		threshold = 150
	}
	if window <= 0 {
		window = time.Second
	}
	if chunkSize == 0 {
		chunkSize = 64 * 1024
	}
	if chunkSize < 1024 || chunkSize > maxFramePayload {
		return coreConfig{}, E.New("invalid chunk_size")
	}
	if queueFrames == 0 {
		queueFrames = 256
	}
	if queueFrames < 8 || queueFrames > 4096 {
		return coreConfig{}, E.New("invalid queue_frames")
	}
	if maxReorder == 0 {
		maxReorder = 4096
	}
	if maxReorder < 64 || maxReorder > 65536 {
		return coreConfig{}, E.New("invalid max_reorder_frames")
	}
	if maxInflight == 0 {
		maxInflight = 1024
	}
	if maxInflight < 32 || maxInflight > 65536 {
		return coreConfig{}, E.New("invalid max_inflight_frames")
	}
	if ackInterval <= 0 {
		ackInterval = 20 * time.Millisecond
	}
	if ackInterval < time.Millisecond || ackInterval > time.Second {
		return coreConfig{}, E.New("ack_interval must be between 1ms and 1s")
	}
	if retransmitTimeout <= 0 {
		retransmitTimeout = 1500 * time.Millisecond
	}
	if retransmitTimeout < 100*time.Millisecond || retransmitTimeout > 30*time.Second {
		return coreConfig{}, E.New("retransmit_timeout must be between 100ms and 30s")
	}
	if recoveryTimeout <= 0 {
		recoveryTimeout = 15 * time.Second
	}
	if recoveryTimeout < time.Second || recoveryTimeout > 5*time.Minute {
		return coreConfig{}, E.New("recovery_timeout must be between 1s and 5m")
	}
	return coreConfig{
		ChunkSize:         chunkSize,
		QueueFrames:       queueFrames,
		ThresholdBytesPS:  uint64(threshold) * 1000 * 1000 / 8,
		ActivationWindow:  window,
		BandwidthMbps:     append([]uint32(nil), weights...),
		MaxReorderFrames:  maxReorder,
		MaxInflightFrames: maxInflight,
		AckInterval:       ackInterval,
		RetransmitTimeout: retransmitTimeout,
		RecoveryTimeout:   recoveryTimeout,
	}, nil
}

func makeAdaptiveSettings(options option.MultipathAdaptiveOptions) (adaptiveSettings, error) {
	settings := defaultAdaptiveSettings()
	if value := time.Duration(options.EvaluationInterval); value > 0 {
		settings.EvaluationInterval = value
	}
	if value := time.Duration(options.Warmup); value > 0 {
		settings.Warmup = value
	}
	if value := time.Duration(options.SuspectWindow); value > 0 {
		settings.SuspectWindow = value
	}
	if value := time.Duration(options.HardFailureWindow); value > 0 {
		settings.HardFailureWindow = value
	}
	if value := time.Duration(options.Cooldown); value > 0 {
		settings.Cooldown = value
	}
	if value := time.Duration(options.MaxCooldown); value > 0 {
		settings.MaxCooldown = value
	}
	if value := time.Duration(options.RecoveryStableWindow); value > 0 {
		settings.RecoveryStableWindow = value
	}
	if options.HardFailureThreshold > 0 {
		settings.HardFailureThreshold = options.HardFailureThreshold
	}
	if options.MinTxOutstandingFrames > 0 {
		settings.MinTxOutstanding = int(options.MinTxOutstandingFrames)
	}
	if value := time.Duration(options.TxAckStall); value > 0 {
		settings.TxAckStall = value
	}
	if options.MinRxReorderFrames > 0 {
		settings.MinRxReorder = int(options.MinRxReorderFrames)
	}
	if value := time.Duration(options.RxGapStall); value > 0 {
		settings.RxGapStall = value
	}
	if options.GoodputDegradeRatio > 0 {
		settings.GoodputDegradeRatio = options.GoodputDegradeRatio
	}
	if options.MinCanaryUsefulBytes > 0 {
		settings.MinCanaryUsefulBytes = options.MinCanaryUsefulBytes
	}
	if options.MinCanaryActiveWindows > 0 {
		settings.MinCanaryActiveWindows = int(options.MinCanaryActiveWindows)
	}
	if options.InitialFailureThreshold > 0 {
		settings.InitialFailureThreshold = options.InitialFailureThreshold
	}
	if value := time.Duration(options.InitialFailureWindow); value > 0 {
		settings.InitialFailureWindow = value
	}
	if settings.EvaluationInterval < 100*time.Millisecond || settings.EvaluationInterval > time.Minute {
		return adaptiveSettings{}, E.New("leg1_adaptive evaluation_interval must be between 100ms and 1m")
	}
	if settings.Warmup > 10*time.Minute || settings.SuspectWindow < time.Second || settings.SuspectWindow > 10*time.Minute {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive warmup or suspect_window")
	}
	if settings.HardFailureThreshold < 1 || settings.HardFailureWindow < time.Second || settings.HardFailureWindow > 10*time.Minute {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive hard failure settings")
	}
	if settings.Cooldown < time.Second || settings.MaxCooldown < settings.Cooldown || settings.MaxCooldown > time.Hour {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive cooldown settings")
	}
	if settings.RecoveryStableWindow < time.Second || settings.RecoveryStableWindow > time.Hour {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive recovery_stable_window")
	}
	if settings.MinTxOutstanding < 1 || settings.TxAckStall < time.Second || settings.TxAckStall > time.Minute {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive TX pressure settings")
	}
	if settings.MinRxReorder < 1 || settings.RxGapStall < 100*time.Millisecond || settings.RxGapStall > time.Minute {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive RX pressure settings")
	}
	if settings.GoodputDegradeRatio <= 0 || settings.GoodputDegradeRatio >= 1 {
		return adaptiveSettings{}, E.New("leg1_adaptive goodput_degrade_ratio must be between 0 and 1")
	}
	if settings.MinCanaryUsefulBytes == 0 || settings.MinCanaryActiveWindows < 1 {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive canary useful-data settings")
	}
	if settings.InitialFailureThreshold < 1 || settings.InitialFailureWindow < time.Second || settings.InitialFailureWindow > time.Hour {
		return adaptiveSettings{}, E.New("invalid leg1_adaptive initial failure settings")
	}
	return settings, nil
}

package multipath

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
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
	aggregations    []M.Socksaddr
	cfg             coreConfig
	password        string
	redialInterval  time.Duration
	adaptive        adaptiveSettings
	adaptiveEnabled bool
	hy2Health       hy2GlobalHealth
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
	redialInterval := time.Duration(options.RedialInterval)
	if redialInterval <= 0 {
		redialInterval = time.Second
	}
	if redialInterval < 100*time.Millisecond || redialInterval > 30*time.Second {
		return nil, E.New("redial_interval must be between 100ms and 30s")
	}

	dependencies := append([]string(nil), options.Outbounds...)
	if !slices.Contains(dependencies, udpTag) {
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
		aggregations:    aggregations,
		password:        options.Password,
		cfg:             cfg,
		redialInterval:  redialInterval,
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
	udpOutbound, loaded := o.manager.Outbound(o.udpTag)
	if !loaded {
		return E.New("multipath UDP outbound not found: ", o.udpTag)
	}
	if !slices.Contains(udpOutbound.Network(), N.NetworkUDP) {
		return E.New("multipath UDP outbound does not support UDP: ", o.udpTag)
	}
	o.udpOutbound = udpOutbound
	return nil
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkUDP:
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
	primaryConn, err := o.children[0].DialContext(ctx, N.NetworkTCP, o.aggregations[0])
	if err != nil {
		return nil, E.Cause(err, "dial multipath preferred leg ", o.tags[0])
	}
	if err = writeHello(primaryConn, helloMessage{Session: sessionID, LegID: 0, Destination: destinationString}, o.password); err != nil {
		primaryConn.Close()
		return nil, E.Cause(err, "write multipath preferred hello")
	}

	cfg := o.cfg
	sessionCtx, cancelSession := context.WithCancel(context.WithoutCancel(ctx))
	var core *mpCore
	var appConn net.Conn
	adaptive := newAdaptiveConn(o.adaptiveEnabled, o.adaptive, &o.hy2Health, nil)
	var dialMu sync.Mutex
	joining := [2]bool{}
	scheduled := [2]bool{}
	deferredRepair := [2]bool{}
	deferredDelay := [2]time.Duration{}
	var ensureLeg func(uint8)
	var scheduleLeg func(uint8, time.Duration)
	adaptive.onSelected = func(carrier leg1Carrier, probation bool) {
		if !o.adaptiveEnabled {
			return
		}
		o.logger.InfoContext(ctx, "multipath leg1 carrier selected: ", carrier.String(), " probation=", probation)
	}
	adaptive.onHealth = func(decision adaptiveDecision, stats coreStats) {
		// A zero-useful EOF on a just-added Hy2 leg can be the normal short-session
		// JOIN race rather than a carrier outage. In that case OnLegDown deliberately
		// avoids an immediate reconnect. If the logical stream remains active and
		// useful demand continues, the health loop requests a demand-driven repair.
		if core != nil && o.adaptiveEnabled && adaptive.currentCarrier() == carrierHy2 &&
			!core.closing.Load() && !core.finalizing.Load() && core.active.Load() &&
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
			o.logger.InfoContext(ctx, "multipath leg1 carrier recovered: hysteria2 remains selected")
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
		o.hy2Health.noteRecovery()
		o.logger.InfoContext(ctx, "multipath hysteria2 cooldown cleared after healthy active canary")
	}
	adaptive.onActiveSuccess = func() {
		o.hy2Health.noteActiveSuccess()
	}
	adaptive.onFallback = func(reason string, cooldown bool, probation bool) {
		if core == nil || core.closing.Load() || core.finalizing.Load() {
			return
		}
		o.logger.InfoContext(ctx, "multipath leg1 carrier fallback: hysteria2 -> snell reason=", reason)
		if cooldown || probation {
			duration := o.hy2Health.noteFallback(time.Now(), o.adaptive, probation)
			o.logger.InfoContext(ctx, "multipath hysteria2 cooldown: duration=", duration)
		}
		if !core.replaceLeg(1, &adaptiveCarrierReplacementError{from: carrierHy2, reason: reason}) {
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
		if core.finalizing.Load() {
			return
		}
		if id == 1 && !core.active.Load() {
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
		if core.finalizing.Load() {
			return
		}
		if id == 1 && !core.active.Load() {
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
				if repair && !core.hasLeg(id) && (id == 0 || core.active.Load()) {
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
				if core.finalizing.Load() {
					return
				}
				if id == 1 && !core.active.Load() {
					return
				}
				if core.hasLeg(id) {
					return
				}
				child := o.children[id]
				carrierTag := o.tags[id]
				if id == 1 && o.adaptiveEnabled {
					if adaptive.carrierForLeg1(time.Now()) == carrierSnell {
						child = o.leg1Fallback
						carrierTag = o.leg1FallbackTag
					}
				}
				conn, dialErr := child.DialContext(sessionCtx, N.NetworkTCP, o.aggregations[id])
				if dialErr == nil {
					dialErr = writeHello(conn, helloMessage{Session: sessionID, LegID: id, Destination: destinationString}, o.password)
				}
				if dialErr == nil && core.finalizing.Load() {
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
				if id == 1 && o.adaptiveEnabled && !core.closing.Load() && !core.finalizing.Load() && adaptive.currentCarrier() == carrierHy2 {
					stats := core.snapshotStats()
					if adaptive.shouldRecordCarrierFailure(dialErr, stats, false) {
						// This attempt never became an installed SMP3 leg. Treat it as an
						// initial carrier failure even if an older attempt had been ready.
						if adaptive.recordCarrierFailure(false) {
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
		if core.finalizing.Load() {
			return
		}
		// Preserve the carrier that actually owned leg1. Ordinary failures capture the
		// current carrier before health accounting can switch it; intentional adaptive
		// replacement carries the old carrier explicitly because the state switch
		// necessarily happens before core.replaceLeg().
		carrierAtFailure := carrierHy2
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
		if id == 1 && o.adaptiveEnabled && !intentionalReplacement && !core.closing.Load() && !core.finalizing.Load() && carrierAtFailure == carrierHy2 {
			stats := core.snapshotStats()
			if adaptive.shouldRecordCarrierFailure(legErr, stats, true) {
				adaptive.recordCarrierFailure(true)
			} else {
				benignSecondaryEOF = true
				if !adaptive.noteAmbiguousCarrierEOF() {
					o.logger.DebugContext(ctx, "multipath ignored first ambiguous zero-useful leg1 EOF for ", destination, ": ", legErr)
				}
			}
		}
		carrierTag := o.tags[id]
		if id == 1 && o.adaptiveEnabled && carrierAtFailure == carrierSnell {
			carrierTag = o.leg1FallbackTag
		}
		o.logger.WarnContext(ctx, "multipath leg ", id, " down via ", carrierTag, ": ", legErr)
		// Repair the failed leg no faster than redial_interval. For an ambiguous
		// zero-useful Hy2 EOF we intentionally do not reconnect immediately; the
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
	core, appConn = newCore(cfg)
	adaptive.core = core
	go func() {
		<-core.Done()
		cancelSession()
	}()
	if err = core.addLeg(0, primaryConn, nil); err != nil {
		appConn.Close()
		return nil, err
	}
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " via preferred ", o.tags[0])
	return appConn, nil
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if o.udpOutbound == nil {
		return nil, E.New("multipath outbound is not started")
	}
	return o.udpOutbound.ListenPacket(ctx, destination)
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

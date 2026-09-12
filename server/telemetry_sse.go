package server

import "time"

const telemetryMaxSSEClients = 32

func (r *TelemetryRegistry) notifySubscribersLocked() {
	for _, subscriber := range r.subscribers {
		select {
		case subscriber.ch <- telemetrySSEMessage{Kind: "notify"}:
		default:
			// The ring and subscriber cursor are authoritative. A full
			// notification channel only means the consumer is already awake.
		}
	}
}

func (r *TelemetryRegistry) publishLifecycle(kind, displayID, mode string, leg *uint8, at time.Time) {
	if r == nil || !r.enabled {
		return
	}
	r.noteEvent()
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if r.sseClosed {
		return
	}
	event := telemetryLifecycleEvent{
		ID:               r.nextEventID.Add(1),
		Kind:             kind,
		At:               at,
		DisplaySessionID: displayID,
		Mode:             mode,
	}
	if leg != nil {
		value := *leg
		event.LegID = &value
	}
	r.lifecycleRing = append(r.lifecycleRing, event)
	for len(r.lifecycleRing) > r.cfg.MaxEvents {
		r.lifecycleRing = r.lifecycleRing[1:]
	}
	r.notifySubscribersLocked()
}

func (r *TelemetryRegistry) publishSnapshot(_ TelemetrySnapshot) {
	if r == nil || !r.enabled {
		return
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if r.sseClosed {
		return
	}
	r.snapshotSeq.Add(1)
	r.notifySubscribersLocked()
}

func (r *TelemetryRegistry) SubscribeSSE(afterID uint64, hasAfter bool) (*telemetrySSESubscriber, []telemetryLifecycleEvent, bool, bool) {
	if r == nil || !r.enabled {
		return nil, nil, false, false
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if r.sseClosed || len(r.subscribers) >= telemetryMaxSSEClients {
		return nil, nil, false, true
	}
	barrier := r.nextEventID.Load()
	stale := false
	if hasAfter {
		if afterID > barrier {
			stale = true
		} else if len(r.lifecycleRing) == 0 {
			stale = afterID < barrier
		} else {
			oldest := r.lifecycleRing[0].ID
			if oldest > 0 && afterID < oldest-1 {
				stale = true
			}
		}
	}
	replay := make([]telemetryLifecycleEvent, 0)
	if hasAfter && !stale {
		for _, event := range r.lifecycleRing {
			if event.ID > afterID {
				replay = append(replay, event)
			}
		}
	}
	r.nextSubID++
	subscriber := &telemetrySSESubscriber{
		id:              r.nextSubID,
		ch:              make(chan telemetrySSEMessage, 1),
		state:           telemetrySSESubscriberStarting,
		cursor:          barrier,
		snapshotVersion: r.snapshotSeq.Load(),
	}
	r.subscribers[subscriber.id] = subscriber
	return subscriber, replay, stale, false
}

func (r *TelemetryRegistry) claimLifecycleLocked(subscriber *telemetrySSESubscriber) ([]telemetryLifecycleEvent, bool) {
	last := r.nextEventID.Load()
	if subscriber.cursor >= last {
		return nil, false
	}
	if len(r.lifecycleRing) == 0 {
		subscriber.cursor = last
		return nil, true
	}
	oldest := r.lifecycleRing[0].ID
	if oldest > 0 && subscriber.cursor < oldest-1 {
		subscriber.cursor = last
		return nil, true
	}
	events := make([]telemetryLifecycleEvent, 0, len(r.lifecycleRing))
	for _, event := range r.lifecycleRing {
		if event.ID > subscriber.cursor {
			events = append(events, event)
		}
	}
	if len(events) > 0 {
		subscriber.cursor = events[len(events)-1].ID
	}
	return events, false
}

func (r *TelemetryRegistry) snapshotChangedLocked(subscriber *telemetrySSESubscriber) bool {
	version := r.snapshotSeq.Load()
	if version <= subscriber.snapshotVersion {
		return false
	}
	subscriber.snapshotVersion = version
	return true
}

func (r *TelemetryRegistry) activateSSE(subscriber *telemetrySSESubscriber) ([]telemetryLifecycleEvent, bool, bool, bool) {
	if r == nil || subscriber == nil {
		return nil, false, false, true
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	current := r.subscribers[subscriber.id]
	if r.sseClosed || current != subscriber || subscriber.state >= telemetrySSESubscriberClosing {
		return nil, false, false, true
	}
	subscriber.state = telemetrySSESubscriberActivating
	events, stale := r.claimLifecycleLocked(subscriber)
	snapshot := r.snapshotChangedLocked(subscriber)
	subscriber.state = telemetrySSESubscriberActive
	return events, snapshot, stale, false
}

func (r *TelemetryRegistry) drainSSE(subscriber *telemetrySSESubscriber) ([]telemetryLifecycleEvent, bool, bool, bool) {
	if r == nil || subscriber == nil {
		return nil, false, false, true
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	current := r.subscribers[subscriber.id]
	if r.sseClosed || current != subscriber || subscriber.state != telemetrySSESubscriberActive {
		return nil, false, false, true
	}
	events, stale := r.claimLifecycleLocked(subscriber)
	if stale {
		r.disconnectSlowLocked(subscriber.id)
		return nil, false, true, true
	}
	return events, r.snapshotChangedLocked(subscriber), false, false
}

func (r *TelemetryRegistry) UnsubscribeSSE(subscriber *telemetrySSESubscriber) {
	if r == nil || subscriber == nil {
		return
	}
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if current := r.subscribers[subscriber.id]; current == subscriber {
		delete(r.subscribers, subscriber.id)
		subscriber.state = telemetrySSESubscriberClosing
		close(subscriber.ch)
		subscriber.state = telemetrySSESubscriberClosed
	}
}

func (r *TelemetryRegistry) closeSSESubscribers() {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	r.sseClosed = true
	for id, subscriber := range r.subscribers {
		delete(r.subscribers, id)
		subscriber.state = telemetrySSESubscriberClosing
		close(subscriber.ch)
		subscriber.state = telemetrySSESubscriberClosed
	}
}

func (r *TelemetryRegistry) disconnectSlowLocked(id uint64) {
	if subscriber := r.subscribers[id]; subscriber != nil {
		delete(r.subscribers, id)
		subscriber.state = telemetrySSESubscriberClosing
		close(subscriber.ch)
		subscriber.state = telemetrySSESubscriberClosed
		r.sseSlowDrops.Add(1)
	}
}

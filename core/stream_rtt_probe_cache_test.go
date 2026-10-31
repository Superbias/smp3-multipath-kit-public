package smp3core

import (
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStreamRTTCacheReclaimsPendingAndNeverReusesTokens(t *testing.T) {
	cache := newStreamRTTProbeCache()
	binding := testStreamPathBinding(1)
	key := sha256.Sum256([]byte("generation-one-key"))
	if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
		t.Fatal(err)
	}
	identity := testStreamPathIdentity(1, StreamRTTClientToServer)
	base := time.Now()
	tokens := make([]uint64, 0, StreamRTTProbePendingLimit)
	for i := 0; i < StreamRTTProbePendingLimit; i++ {
		token, _, err := cache.startProbeAt(identity, base)
		if err != nil {
			t.Fatalf("start probe %d: %v", i, err)
		}
		tokens = append(tokens, token)
	}
	if _, _, err := cache.startProbeAt(identity, base.Add(time.Second)); !errors.Is(err, ErrStreamRTTPendingFull) {
		t.Fatalf("pending limit error=%v", err)
	}
	token, _, err := cache.startProbeAt(identity, base.Add(StreamRTTProbeTimeout))
	if err != nil {
		t.Fatalf("probe after expiry: %v", err)
	}
	if token <= tokens[len(tokens)-1] {
		t.Fatalf("token reused or regressed: old=%d new=%d", tokens[len(tokens)-1], token)
	}
	oldEcho, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTEcho, Identity: identity, Token: tokens[0]}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.acceptEchoAt(identity, oldEcho, base.Add(StreamRTTProbeTimeout+time.Millisecond)); !errors.Is(err, ErrStreamRTTNoPending) {
		t.Fatalf("expired token echo error=%v", err)
	}
	newEcho, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTEcho, Identity: identity, Token: token}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.acceptEchoAt(identity, newEcho, base.Add(StreamRTTProbeTimeout+10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRTTCacheGenerationHighWaterSurvivesInvalidation(t *testing.T) {
	cache := newStreamRTTProbeCache()
	key8 := sha256.Sum256([]byte("key-generation-8"))
	binding8 := testStreamPathBinding(8)
	if err := cache.bindLeg(binding8, testStreamPathAuth(binding8, key8)); err != nil {
		t.Fatal(err)
	}
	identity8 := testStreamPathIdentity(8, StreamRTTClientToServer)
	base := time.Now()
	if err := recordStreamRTTSample(cache, identity8, key8, base, 12*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.pathFeedbackAt(identity8, base.Add(13*time.Millisecond)); !ok {
		t.Fatal("generation 8 feedback not published")
	}
	if !cache.invalidateLeg(binding8) {
		t.Fatal("active generation invalidation failed")
	}
	if _, ok := cache.pathFeedbackAt(identity8, base.Add(14*time.Millisecond)); ok {
		t.Fatal("feedback survived invalidation")
	}
	if err := cache.bindLeg(testStreamPathBinding(7), testStreamPathAuth(testStreamPathBinding(7), key8)); !errors.Is(err, ErrStreamRTTStaleGeneration) {
		t.Fatalf("generation rollback error=%v", err)
	}
	if err := cache.bindLeg(binding8, testStreamPathAuth(binding8, key8)); !errors.Is(err, ErrStreamRTTGenerationReuse) {
		t.Fatalf("invalidated generation reuse error=%v", err)
	}
	key9 := sha256.Sum256([]byte("key-generation-9"))
	binding9 := testStreamPathBinding(9)
	if err := cache.bindLeg(binding9, testStreamPathAuth(binding9, key9)); err != nil {
		t.Fatal(err)
	}
	if err := cache.acceptEchoAt(identity8, []byte("old echo"), base.Add(time.Second)); !errors.Is(err, ErrStreamRTTStaleGeneration) {
		t.Fatalf("old generation echo error=%v", err)
	}
}

func TestStreamRTTCacheAuthenticatesPeerProbeAndRejectsReplay(t *testing.T) {
	cache := newStreamRTTProbeCache()
	binding := testStreamPathBinding(3)
	key := sha256.Sum256([]byte("shared-key"))
	if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
		t.Fatal(err)
	}
	identity := testStreamPathIdentity(3, StreamRTTClientToServer)
	probe, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTProbe, Identity: identity, Token: 4}, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.acceptProbe(binding, StreamRTTServerToClient, probe); !errors.Is(err, ErrStreamPathMessage) {
		t.Fatalf("wrong direction accepted: %v", err)
	}
	echo, err := cache.acceptProbe(binding, StreamRTTClientToServer, probe)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStreamRTTMessage(echo, key, binding)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != StreamRTTEcho || decoded.Token != 4 || decoded.Identity.Direction != StreamRTTClientToServer {
		t.Fatalf("unexpected echo=%+v", decoded)
	}
	if _, err := cache.acceptProbe(binding, StreamRTTClientToServer, probe); !errors.Is(err, ErrStreamRTTReplay) {
		t.Fatalf("replayed peer probe error=%v", err)
	}
	wrongKey := sha256.Sum256([]byte("wrong-key"))
	wrongMAC, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTProbe, Identity: identity, Token: 5}, wrongKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.acceptProbe(binding, StreamRTTClientToServer, wrongMAC); !errors.Is(err, errInvalidAuthentication) {
		t.Fatalf("untrusted probe error=%v", err)
	}
}

func TestStreamRTTFeedbackUsesPerSampleFreshnessWindow(t *testing.T) {
	cache := newStreamRTTProbeCache()
	binding := testStreamPathBinding(2)
	key := sha256.Sum256([]byte("freshness-key"))
	if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
		t.Fatal(err)
	}
	identity := testStreamPathIdentity(2, StreamRTTServerToClient)
	base := time.Now()
	if err := recordStreamRTTSample(cache, identity, key, base, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	secondStart := base.Add(29 * time.Second)
	if err := recordStreamRTTSample(cache, identity, key, secondStart, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	feedback, ok := cache.pathFeedbackAt(identity, base.Add(31*time.Second))
	if !ok {
		t.Fatal("fresh sample was not available")
	}
	if feedback.Samples != 1 || feedback.RTT != 20*time.Millisecond || !feedback.ObservedAt.Equal(secondStart.Add(20*time.Millisecond)) {
		t.Fatalf("stale sample entered fresh aggregate: %+v", feedback)
	}
	if _, ok := cache.pathFeedbackAt(identity, base.Add(61*time.Second)); ok {
		t.Fatal("expired feedback remained available")
	}
}

func TestStreamRTTCacheLateSampleDoesNotReplaceNewerWindow(t *testing.T) {
	cache := newStreamRTTProbeCache()
	binding := testStreamPathBinding(4)
	key := sha256.Sum256([]byte("out-of-order-sample-key"))
	if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
		t.Fatal(err)
	}
	identity := testStreamPathIdentity(4, StreamRTTClientToServer)
	base := time.Now()
	cache.mu.Lock()
	for i := 0; i < streamRTTMaxSamples; i++ {
		cache.recordSampleLocked(identity, time.Duration(10+i)*time.Millisecond, base.Add(time.Duration(i)*time.Millisecond))
	}
	// A delayed handshake observation arrives after fresher probe samples have
	// already been published. It must not erase those newer observations.
	cache.recordSampleLocked(identity, 500*time.Millisecond, base.Add(-time.Millisecond))
	cache.mu.Unlock()

	feedback, ok := cache.pathFeedbackAt(identity, base.Add(time.Second))
	if !ok {
		t.Fatal("newer feedback disappeared after a late sample")
	}
	if feedback.RTT != 12*time.Millisecond || feedback.Samples != streamRTTMaxSamples || !feedback.ObservedAt.Equal(base.Add(4*time.Millisecond)) {
		t.Fatalf("late sample replaced newer feedback: %+v", feedback)
	}
}

func TestStreamRTTCacheDiscardsClockQuantizedZeroSample(t *testing.T) {
	cache := newStreamRTTProbeCache()
	binding := testStreamPathBinding(1)
	key := sha256.Sum256([]byte("zero-rtt-key"))
	if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
		t.Fatal(err)
	}
	identity := testStreamPathIdentity(1, StreamRTTClientToServer)
	started := time.Now()
	token, _, err := cache.startProbeAt(identity, started)
	if err != nil {
		t.Fatal(err)
	}
	echo, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTEcho, Identity: identity, Token: token}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.acceptEchoAt(identity, echo, started); err != nil {
		t.Fatalf("clock-quantized echo closed the path: %v", err)
	}
	if _, ok := cache.pathFeedbackAt(identity, started.Add(time.Nanosecond)); ok {
		t.Fatal("unmeasurable zero-duration RTT was published")
	}
}

func TestStreamRTTConcurrentGenerationReplaceAndRead(t *testing.T) {
	cache := newStreamRTTProbeCache()
	base := time.Now()
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for generation := uint64(1); generation <= 1000; generation++ {
				identity := testStreamPathIdentity(generation, StreamRTTClientToServer)
				_, _ = cache.PathFeedback(identity)
			}
		}()
	}
	for generation := uint64(1); generation <= 1000; generation++ {
		binding := testStreamPathBinding(generation)
		key := sha256.Sum256([]byte{byte(generation), byte(generation >> 8)})
		if err := cache.bindLeg(binding, testStreamPathAuth(binding, key)); err != nil {
			t.Fatalf("bind generation %d: %v", generation, err)
		}
		identity := testStreamPathIdentity(generation, StreamRTTClientToServer)
		start := base.Add(time.Duration(generation) * time.Millisecond)
		if err := recordStreamRTTSample(cache, identity, key, start, time.Millisecond); err != nil {
			t.Fatalf("sample generation %d: %v", generation, err)
		}
		if _, ok := cache.pathFeedbackAt(identity, start.Add(2*time.Millisecond)); !ok {
			t.Fatalf("fresh feedback missing at generation %d", generation)
		}
		if !cache.invalidateLeg(binding) {
			t.Fatalf("invalidate generation %d failed", generation)
		}
	}
	readers.Wait()
	if err := cache.bindLeg(testStreamPathBinding(999), testStreamPathAuth(testStreamPathBinding(999), sha256.Sum256([]byte("stale")))); !errors.Is(err, ErrStreamRTTStaleGeneration) {
		t.Fatalf("late generation 999 bind error=%v", err)
	}
	if err := cache.bindLeg(testStreamPathBinding(1001), testStreamPathAuth(testStreamPathBinding(1001), sha256.Sum256([]byte("new")))); err != nil {
		t.Fatalf("generation 1001 bind: %v", err)
	}
}

func recordStreamRTTSample(cache *streamRTTProbeCache, identity StreamPathIdentity, key [sha256.Size]byte, started time.Time, rtt time.Duration) error {
	token, _, err := cache.startProbeAt(identity, started)
	if err != nil {
		return err
	}
	echo, err := encodeStreamRTTMessage(StreamRTTMessage{Kind: StreamRTTEcho, Identity: identity, Token: token}, key)
	if err != nil {
		return err
	}
	return cache.acceptEchoAt(identity, echo, started.Add(rtt))
}

func testStreamPathAuth(binding StreamPathBinding, key [sha256.Size]byte) StreamPathAuth {
	return StreamPathAuth{
		sessionID: binding.SessionID, legID: binding.LegID,
		capabilities: StreamCapabilityPathRTT, probeKey: key, authenticated: true,
	}
}

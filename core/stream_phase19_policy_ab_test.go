package smp3core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestStreamPhase19PolicyABSample is an opt-in measurement entry point. Policy
// selection comes from the separately compiled source variant; environment
// labels only describe the immutable binary and never steer production code.
func TestStreamPhase19PolicyABSample(t *testing.T) {
	if os.Getenv("SMP3_PHASE19_AB_SAMPLE") != "1" {
		t.Skip("set SMP3_PHASE19_AB_SAMPLE=1 to run one Phase 19 policy A/B sample")
	}
	sourceSHA := os.Getenv("SMP3_PHASE19_SOURCE_SHA256")
	if len(sourceSHA) != sha256.Size*2 {
		t.Fatal("SMP3_PHASE19_SOURCE_SHA256 must contain the frozen Core source digest")
	}
	if _, err := hex.DecodeString(sourceSHA); err != nil {
		t.Fatalf("invalid source digest: %v", err)
	}
	binarySHA, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	intEnv := func(name string, fallback int) int {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback
		}
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 0 {
			t.Fatalf("invalid %s=%q", name, raw)
		}
		return value
	}
	boolEnv := func(name string) bool { return os.Getenv(name) == "1" }
	policy := os.Getenv("SMP3_PHASE19_POLICY_LABEL")
	if policy != "policy_a_experimental" && policy != "policy_b_phase18" && policy != "fast_only_control" {
		t.Fatalf("unexpected policy label %q", policy)
	}
	pathMode := os.Getenv("SMP3_PHASE19_PATH_MODE")
	if pathMode == "" {
		pathMode = "off"
	}
	mode := streamPathQOEMode{name: policy + "_path_aware_" + pathMode}
	mode.singleLeg = boolEnv("SMP3_PHASE19_SINGLE_LEG")
	if policy == "fast_only_control" && !mode.singleLeg {
		t.Fatal("fast_only_control requires SMP3_PHASE19_SINGLE_LEG=1")
	}
	if policy != "fast_only_control" && mode.singleLeg {
		t.Fatal("SMP3_PHASE19_SINGLE_LEG is reserved for fast_only_control")
	}
	switch pathMode {
	case "off":
	case "ready":
		mode.pathAware, mode.waitForRTT = true, true
	default:
		t.Fatalf("unsupported path mode %q", pathMode)
	}
	rtt := [2]time.Duration{
		time.Duration(intEnv("SMP3_PHASE19_RTT0_MS", 20)) * time.Millisecond,
		time.Duration(intEnv("SMP3_PHASE19_RTT1_MS", 20)) * time.Millisecond,
	}
	rates := [2]int64{int64(intEnv("SMP3_PHASE19_RATE0_MBPS", 100)), int64(intEnv("SMP3_PHASE19_RATE1_MBPS", 1))}
	size := intEnv("SMP3_PHASE19_BYTES", 128<<20)
	if size <= 0 {
		t.Fatal("SMP3_PHASE19_BYTES must be positive")
	}
	run := intEnv("SMP3_PHASE19_RUN", 1)
	orderIndex := intEnv("SMP3_PHASE19_ORDER_INDEX", 1)
	seed := int64(intEnv("SMP3_PHASE19_ORDER_SEED", 20261025))
	requestResponse := boolEnv("SMP3_PHASE19_REQUEST_RESPONSE")
	traceFrames := boolEnv("SMP3_PHASE19_TRACE_FRAMES")
	coreTrace := boolEnv("SMP3_PHASE19_CORE_TRACE")
	timeout := time.Duration(intEnv("SMP3_PHASE19_TIMEOUT_SECONDS", 180)) * time.Second
	blockID := os.Getenv("SMP3_PHASE19_BLOCK_ID")
	if blockID == "" {
		blockID = policy + "/" + strconv.FormatInt(rates[0], 10) + "/" + strconv.FormatInt(rates[1], 10) + "/" + strconv.Itoa(int(rtt[0].Milliseconds())) + "/" + strconv.Itoa(int(rtt[1].Milliseconds())) + "/" + strconv.Itoa(size) + "/" + strconv.Itoa(run)
	}
	job := streamPathQOEJob{
		rtt: rtt, carrierRatesMbps: rates, size: size, run: run, mode: mode,
		orderSeed: seed, orderIndex: orderIndex, blockID: blockID,
		requestResponse: requestResponse, traceFrames: traceFrames,
		captureCoreRecoveryTrace: coreTrace, collectCoreTelemetry: true,
		tcpCarrier: true, dynamicCapacity: true, timeout: timeout,
	}
	sample := runStreamPathQoESample(t, job)
	retryBytes := sample.CoreTxRetryWrittenBytesByLeg[0] + sample.CoreTxRetryWrittenBytesByLeg[1]
	rescueBytes := sample.CoreRescueAttemptBytesByLeg[0] + sample.CoreRescueAttemptBytesByLeg[1]
	retransmitFrames := sample.CoreDataTxRetransmitFramesByLeg[0] + sample.CoreDataTxRetransmitFramesByLeg[1]
	duplicateBytes := sample.ReceiverDuplicateBytesByLeg[0] + sample.ReceiverDuplicateBytesByLeg[1]
	strictZeroRetry := retryBytes == 0 && rescueBytes == 0 && retransmitFrames == 0 && duplicateBytes == 0
	metadata := map[string]any{
		"phase":              "SMP3 v2.7 Phase 19 policy A/B",
		"policy_label":       policy,
		"source_core_sha256": sourceSHA,
		"test_binary_sha256": binarySHA,
		"go_version":         runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"gomaxprocs":             runtime.GOMAXPROCS(0),
		"carrier":                "loopback TCP with virtual per-leg serialization/delay; not physical Carrier/WAN",
		"strict_zero_retry_gate": strictZeroRetry,
		"core_retry_bytes":       retryBytes, "core_rescue_bytes": rescueBytes,
		"core_retransmit_frames": retransmitFrames, "receiver_duplicate_bytes": duplicateBytes,
		"sample": sample,
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("encode sample: %v", err)
	}
	t.Logf("SMP3_PHASE19_SAMPLE %s", encoded)
	if !sample.Pass {
		t.Fatalf("data correctness failed: termination=%s error=%s sender=%d receiver=%d ack=%d", sample.Termination, sample.Error, sample.SenderBytes, sample.ReceiverBytes, sample.UniqueAckBytes)
	}
}

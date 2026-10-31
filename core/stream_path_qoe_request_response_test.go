package smp3core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestStreamPathQoERequestResponseMeasurement(t *testing.T) {
	job := streamPathQOEJob{
		rtt:  [2]time.Duration{20 * time.Millisecond, 100 * time.Millisecond},
		size: 100 << 10, run: 1,
		mode:            streamPathQOEMode{name: "request_response_baseline"},
		requestResponse: true,
	}
	sample := runStreamPathQoESample(t, job)
	if !sample.Pass {
		t.Fatalf("request/response correctness failed: %+v", sample)
	}
	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatalf("encode request/response measurement: %v", err)
	}
	t.Logf("SMP3_PATH_QOE_RR_SMOKE %s", encoded)
	if sample.ClientFirstResponseTTFBMS <= 0 || sample.ServerRequestCompleteMS <= 0 || sample.ServerFirstResponseWriteMS <= 0 {
		t.Fatalf("request/response timestamps were not captured: %+v", sample)
	}
	if sample.ClientFirstResponseTTFBMS < sample.ServerRequestCompleteMS || sample.ClientFirstResponseTTFBMS < sample.ServerFirstResponseWriteMS {
		t.Fatalf("client observed the first response before the server completed the request or started its response: %+v", sample)
	}
	if sample.ClientResponseEOFMS < sample.ClientFirstResponseTTFBMS || sample.RequestResponseCompletionMS < sample.ClientResponseEOFMS {
		t.Fatalf("response completion timestamps are out of order: %+v", sample)
	}
	if sample.UniqueAckBytes != uint64(job.size) || sample.ResponseUniqueAckBytes != uint64(job.size) {
		t.Fatalf("unique ACK did not cover both directions: request=%d response=%d want=%d", sample.UniqueAckBytes, sample.ResponseUniqueAckBytes, job.size)
	}
}

// TestStreamPathQoERequestResponseDiagnostic is opt-in. It measures client-
// observed first-response TTFB after an application request, separately from
// the legacy upload-only diagnostic. It is still a virtual Carrier test.
func TestStreamPathQoERequestResponseDiagnostic(t *testing.T) {
	if os.Getenv("SMP3_PATH_QOE_RR_DIAGNOSTIC") != "1" {
		t.Skip("set SMP3_PATH_QOE_RR_DIAGNOSTIC=1 to run request/response QoE diagnostics")
	}
	formal := os.Getenv("SMP3_PATH_QOE_RR_FORMAL") == "1"
	repetitions := 5
	seed := int64(20261022)
	if formal {
		repetitions = 30
		seedText := os.Getenv("SMP3_PATH_QOE_ORDER_SEED")
		if seedText == "" {
			t.Fatal("formal request/response mode requires SMP3_PATH_QOE_ORDER_SEED")
		}
		parsed, err := strconv.ParseInt(seedText, 10, 64)
		if err != nil {
			t.Fatalf("parse SMP3_PATH_QOE_ORDER_SEED: %v", err)
		}
		seed = parsed
		if sourceDigest := os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256"); len(sourceDigest) != sha256.Size*2 {
			t.Fatal("formal request/response mode requires SMP3_PATH_QOE_SOURCE_SHA256")
		} else if _, err := hex.DecodeString(sourceDigest); err != nil {
			t.Fatalf("parse SMP3_PATH_QOE_SOURCE_SHA256: %v", err)
		}
	}
	rtts := [][2]time.Duration{
		{20 * time.Millisecond, 20 * time.Millisecond},
		{20 * time.Millisecond, 100 * time.Millisecond},
		{20 * time.Millisecond, 400 * time.Millisecond},
	}
	modes := []streamPathQOEMode{
		{name: "baseline_path_aware_off"},
		{name: "healthy_single_leg_reference", singleLeg: true},
		{name: "path_aware_cold_attachment_no_feedback", pathAware: true},
		{name: "path_aware_feedback_missing", pathAware: true, holdRTTEcho: true},
		{name: "path_aware_feedback_ready", pathAware: true, waitForRTT: true},
	}
	jobs := streamPathQOEBuildJobs(rtts, []int{100 << 10}, modes, repetitions, seed, true)
	if formal && os.Getenv("SMP3_PATH_QOE_RR_ONLY") != "" {
		t.Fatal("SMP3_PATH_QOE_RR_ONLY is not allowed in formal mode")
	}
	filter := os.Getenv("SMP3_PATH_QOE_RR_ONLY")
	if filter != "" {
		filtered := jobs[:0]
		for _, job := range jobs {
			if filter == strconv.Itoa(int(job.rtt[0].Milliseconds()))+"/"+strconv.Itoa(int(job.rtt[1].Milliseconds()))+":"+strconv.Itoa(job.size)+":"+job.mode.name {
				filtered = append(filtered, job)
			}
		}
		jobs = filtered
		if len(jobs) == 0 {
			t.Fatalf("SMP3_PATH_QOE_RR_ONLY %q matched no request/response job", filter)
		}
	}
	binaryDigest, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	var results []streamPathQoESample
	for _, job := range jobs {
		job.requestResponse = true
		sample := runStreamPathQoESample(t, job)
		sample.OrderSeed, sample.OrderIndex, sample.BlockID = job.orderSeed, job.orderIndex, job.blockID
		encoded, err := json.Marshal(sample)
		if err != nil {
			t.Fatalf("encode request/response sample order %d: %v", job.orderIndex, err)
		}
		t.Logf("SMP3_PATH_QOE_RR_SAMPLE %s", encoded)
		results = append(results, sample)
		if !sample.Pass {
			t.Fatalf("request/response correctness failed: %+v", sample)
		}
	}
	identity := map[string]any{
		"test_binary_sha256": binaryDigest,
		"go_version":         runtime.Version(),
		"goos":               runtime.GOOS,
		"goarch":             runtime.GOARCH,
		"gomaxprocs":         runtime.GOMAXPROCS(0),
		"source_sha256":      os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256"),
		"data_frame_bytes":   64 << 10,
		"carrier_mbps":       []int{100, 100},
	}
	encoded, err := json.Marshal(map[string]any{
		"scope":                      "shared Core request/response with calibrated virtual Carrier; not Native/Standalone or WAN",
		"formal_randomized_paired":   formal,
		"order_seed":                 seed,
		"repetitions_per_cell":       repetitions,
		"request_and_response_bytes": 100 << 10,
		"rtt_ms":                     "20/20,20/100,20/400",
		"modes":                      streamPathQOEModeNames(modes),
		"identity_and_configuration": identity,
		"results":                    results,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SMP3_PATH_QOE_RR_DIAGNOSTIC %s", encoded)
	for _, rtt := range rtts {
		for _, mode := range modes {
			var ttfb, completion []float64
			for _, sample := range results {
				if sample.RTTLeg0MS == int(rtt[0].Milliseconds()) && sample.RTTLeg1MS == int(rtt[1].Milliseconds()) && sample.Mode == mode.name {
					ttfb = append(ttfb, sample.ClientFirstResponseTTFBMS)
					completion = append(completion, sample.RequestResponseCompletionMS)
				}
			}
			t.Logf("SMP3_PATH_QOE_RR_SUMMARY mode=%s rtt_ms=%d/%d request_response_ttfb_ms=%v request_start_to_eof_and_ack_drain_ms=%v",
				mode.name, rtt[0].Milliseconds(), rtt[1].Milliseconds(), streamPathQoEPercentiles(ttfb), streamPathQoEPercentiles(completion))
		}
	}
}

// TestStreamPathQoETailTraceDiagnostic is opt-in and runs the exact previously
// questioned 20/400ms, 100KiB, feedback-ready upload-only case with frame-level
// Carrier write/read events. It diagnoses controls; it is not a performance A/B.
func TestStreamPathQoETailTraceDiagnostic(t *testing.T) {
	if os.Getenv("SMP3_PATH_QOE_TAIL_TRACE") != "1" {
		t.Skip("set SMP3_PATH_QOE_TAIL_TRACE=1 to trace the 20/400ms feedback-ready tail")
	}
	sourceDigest := os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256")
	if len(sourceDigest) != sha256.Size*2 {
		t.Fatal("tail trace requires SMP3_PATH_QOE_SOURCE_SHA256")
	}
	if _, err := hex.DecodeString(sourceDigest); err != nil {
		t.Fatalf("parse SMP3_PATH_QOE_SOURCE_SHA256: %v", err)
	}
	binaryDigest, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	t.Logf("SMP3_PATH_QOE_TAIL_TRACE_IDENTITY source_sha256=%s test_binary_sha256=%s go=%s goos=%s goarch=%s config=100KiB,20/400ms,100+100Mbps,64KiB-frame,queue=256,inflight=1024,reorder=4096,ack=20ms,retry=1500ms",
		sourceDigest, binaryDigest, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	for run := 1; run <= 30; run++ {
		job := streamPathQOEJob{
			rtt:  [2]time.Duration{20 * time.Millisecond, 400 * time.Millisecond},
			size: 100 << 10, run: run,
			mode:      streamPathQOEMode{name: "path_aware_feedback_ready", pathAware: true, waitForRTT: true},
			orderSeed: 20261022, orderIndex: run, blockID: "20/400:102400:" + strconv.Itoa(run), traceFrames: true,
		}
		sample := runStreamPathQoESample(t, job)
		encoded, err := json.Marshal(sample)
		if err != nil {
			t.Fatalf("encode tail trace run %d: %v", run, err)
		}
		t.Logf("SMP3_PATH_QOE_TAIL_TRACE_SAMPLE %s", encoded)
		if !sample.Pass {
			t.Fatalf("tail trace correctness failed at run %d: %+v", run, sample)
		}
	}
}

// TestStreamPathQoERequestResponseTailTraceDiagnostic is opt-in. It correlates
// application response EOF with per-leg Carrier write attempts, write returns,
// and peer-side frame reads for the WSL 20/400ms long-tail case. Carrier reads
// are boundary observations, not proof that Core decoded a frame.
func TestStreamPathQoERequestResponseTailTraceDiagnostic(t *testing.T) {
	if os.Getenv("SMP3_PATH_QOE_RR_TAIL_TRACE") != "1" {
		t.Skip("set SMP3_PATH_QOE_RR_TAIL_TRACE=1 to trace the 20/400ms request/response EOF tail")
	}
	sourceDigest := os.Getenv("SMP3_PATH_QOE_SOURCE_SHA256")
	if len(sourceDigest) != sha256.Size*2 {
		t.Fatal("request/response tail trace requires SMP3_PATH_QOE_SOURCE_SHA256")
	}
	if _, err := hex.DecodeString(sourceDigest); err != nil {
		t.Fatalf("parse SMP3_PATH_QOE_SOURCE_SHA256: %v", err)
	}
	binaryDigest, err := streamPathQOETestBinarySHA256()
	if err != nil {
		t.Fatalf("hash executing test binary: %v", err)
	}
	t.Logf("SMP3_PATH_QOE_RR_TAIL_TRACE_IDENTITY source_sha256=%s test_binary_sha256=%s go=%s goos=%s goarch=%s config=100KiB-request+response,20/400ms,100+100Mbps,64KiB-frame,queue=256,inflight=1024,reorder=4096,ack=20ms,retry=1500ms",
		sourceDigest, binaryDigest, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	for run := 1; run <= 30; run++ {
		job := streamPathQOEJob{
			rtt:  [2]time.Duration{20 * time.Millisecond, 400 * time.Millisecond},
			size: 100 << 10, run: run,
			mode:            streamPathQOEMode{name: "path_aware_feedback_ready", pathAware: true, waitForRTT: true},
			orderSeed:       20261022,
			orderIndex:      run,
			blockID:         "20/400:102400:rr-tail:" + strconv.Itoa(run),
			requestResponse: true,
			traceFrames:     true,
		}
		sample := runStreamPathQoESample(t, job)
		encoded, err := json.Marshal(sample)
		if err != nil {
			t.Fatalf("encode request/response tail trace run %d: %v", run, err)
		}
		t.Logf("SMP3_PATH_QOE_RR_TAIL_TRACE_SAMPLE %s", encoded)
		if !sample.Pass {
			t.Fatalf("request/response tail trace correctness failed at run %d: %+v", run, sample)
		}
		if len(sample.CarrierFrameEvents) == 0 {
			t.Fatalf("request/response tail trace run %d captured no Carrier frames", run)
		}
	}
}

func runStreamPathQoERequestResponseSample(
	t *testing.T,
	job streamPathQOEJob,
	result streamPathQoESample,
	client, server *StreamEngine,
	clientApp, serverApp net.Conn,
	frameRecorder *streamPathQoEFrameRecorder,
) streamPathQoESample {
	t.Helper()
	request := make([]byte, job.size)
	response := make([]byte, job.size)
	for i := range request {
		request[i] = byte((i*31 + 7) % 251)
		response[i] = byte((i*17 + 13) % 251)
	}
	requestHash := sha256.Sum256(request)
	responseHash := sha256.Sum256(response)
	flowTimeout := job.timeout
	if flowTimeout <= 0 {
		flowTimeout = 8 * time.Second
	}
	deadlineAt := time.Now().Add(flowTimeout)
	_ = clientApp.SetDeadline(deadlineAt)
	_ = serverApp.SetDeadline(deadlineAt)
	type requestResult struct {
		started time.Time
		bytes   int
		hash    string
		err     error
	}
	type serverResult struct {
		requestBytes         int
		requestHash          string
		firstRequestByteAt   time.Time
		requestCompleteAt    time.Time
		requestReadWaits     []streamPathQoEAppReadWait
		responseBytes        int
		responseHash         string
		firstResponseWriteAt time.Time
		err                  error
	}
	type responseResult struct {
		bytes       int
		hash        string
		firstByteAt time.Time
		eofAt       time.Time
		termination string
		readWaits   []streamPathQoEAppReadWait
		err         error
	}
	requestDone := make(chan requestResult, 1)
	serverDone := make(chan serverResult, 1)
	responseDone := make(chan responseResult, 1)
	requestStartedAt := time.Now()
	go func() {
		started := requestStartedAt
		if frameRecorder != nil {
			frameRecorder.setStart(started)
		}
		written, err := streamPathQoEWritePayload(clientApp, request)
		requestDone <- requestResult{started: started, bytes: written, hash: hex.EncodeToString(requestHash[:]), err: err}
	}()
	go func() {
		hash := sha256.New()
		buf := make([]byte, 32<<10)
		var received int
		var firstByteAt, previousReadAt time.Time
		var requestReadWaits []streamPathQoEAppReadWait
		for received < len(request) {
			readStarted := time.Now()
			n, err := serverApp.Read(buf)
			readReturned := time.Now()
			termination := ""
			if errors.Is(err, io.EOF) {
				termination = "io.EOF"
			} else if err != nil {
				termination = err.Error()
			}
			interarrival := 0.0
			if n > 0 && !previousReadAt.IsZero() {
				interarrival = float64(readReturned.Sub(previousReadAt).Microseconds()) / 1000
			}
			requestElapsed := 0.0
			if !requestStartedAt.IsZero() {
				requestElapsed = float64(readReturned.Sub(requestStartedAt).Microseconds()) / 1000
			}
			requestReadWaits = append(requestReadWaits, streamPathQoEAppReadWait{Endpoint: "server_request_reader", ElapsedMS: requestElapsed, WaitMS: float64(readReturned.Sub(readStarted).Microseconds()) / 1000, InterarrivalMS: interarrival, Bytes: n, Termination: termination})
			if n > 0 {
				previousReadAt = readReturned
				if firstByteAt.IsZero() {
					firstByteAt = time.Now()
				}
				_, _ = hash.Write(buf[:n])
				received += n
			}
			if err != nil {
				serverDone <- serverResult{requestBytes: received, requestHash: hex.EncodeToString(hash.Sum(nil)), firstRequestByteAt: firstByteAt, requestReadWaits: requestReadWaits, err: err}
				return
			}
		}
		requestCompleteAt := time.Now()
		responseHashText := hex.EncodeToString(responseHash[:])
		var responseWritten int
		var firstResponseWriteAt time.Time
		for responseWritten < len(response) {
			end := responseWritten + 64<<10
			if end > len(response) {
				end = len(response)
			}
			if firstResponseWriteAt.IsZero() {
				firstResponseWriteAt = time.Now()
			}
			n, err := serverApp.Write(response[responseWritten:end])
			responseWritten += n
			if err != nil {
				serverDone <- serverResult{requestBytes: received, requestHash: hex.EncodeToString(hash.Sum(nil)), firstRequestByteAt: firstByteAt, requestCompleteAt: requestCompleteAt, requestReadWaits: requestReadWaits, responseBytes: responseWritten, responseHash: responseHashText, firstResponseWriteAt: firstResponseWriteAt, err: err}
				return
			}
			if n == 0 {
				serverDone <- serverResult{requestBytes: received, requestHash: hex.EncodeToString(hash.Sum(nil)), firstRequestByteAt: firstByteAt, requestCompleteAt: requestCompleteAt, responseBytes: responseWritten, responseHash: responseHashText, firstResponseWriteAt: firstResponseWriteAt, err: io.ErrNoProgress}
				return
			}
		}
		serverDone <- serverResult{
			requestBytes: received, requestHash: hex.EncodeToString(hash.Sum(nil)),
			firstRequestByteAt: firstByteAt, requestCompleteAt: requestCompleteAt, requestReadWaits: requestReadWaits,
			responseBytes: responseWritten, responseHash: responseHashText,
			firstResponseWriteAt: firstResponseWriteAt, err: serverApp.Close(),
		}
	}()
	go func() {
		hash := sha256.New()
		buf := make([]byte, 32<<10)
		var received int
		var firstByteAt, previousReadAt time.Time
		var readWaits []streamPathQoEAppReadWait
		for {
			readStarted := time.Now()
			n, err := clientApp.Read(buf)
			readReturned := time.Now()
			termination := ""
			if errors.Is(err, io.EOF) {
				termination = "io.EOF"
			} else if err != nil {
				termination = err.Error()
			}
			interarrival := 0.0
			if n > 0 && !previousReadAt.IsZero() {
				interarrival = float64(readReturned.Sub(previousReadAt).Microseconds()) / 1000
			}
			elapsed := 0.0
			if !requestStartedAt.IsZero() {
				elapsed = float64(readReturned.Sub(requestStartedAt).Microseconds()) / 1000
			}
			readWaits = append(readWaits, streamPathQoEAppReadWait{Endpoint: "client_response_reader", ElapsedMS: elapsed, WaitMS: float64(readReturned.Sub(readStarted).Microseconds()) / 1000, InterarrivalMS: interarrival, Bytes: n, Termination: termination})
			if n > 0 {
				previousReadAt = readReturned
				if firstByteAt.IsZero() {
					firstByteAt = time.Now()
				}
				_, _ = hash.Write(buf[:n])
				received += n
			}
			if err != nil {
				termination := err.Error()
				if errors.Is(err, io.EOF) {
					termination = "io.EOF"
					err = nil
				}
				responseDone <- responseResult{
					bytes: received, hash: hex.EncodeToString(hash.Sum(nil)),
					firstByteAt: firstByteAt, eofAt: time.Now(), termination: termination, readWaits: readWaits, err: err,
				}
				return
			}
		}
	}()
	var requestData requestResult
	var serverData serverResult
	var responseData responseResult
	requestReceived, serverReceived, responseReceived := false, false, false
	timeout := time.NewTimer(flowTimeout)
	defer timeout.Stop()
	for !requestReceived || !serverReceived || !responseReceived {
		select {
		case requestData = <-requestDone:
			requestReceived = true
		case serverData = <-serverDone:
			serverReceived = true
		case responseData = <-responseDone:
			responseReceived = true
		case <-timeout.C:
			result.Error = "virtual Carrier request/response exceeded diagnostic timeout " + flowTimeout.String()
			result.Termination = "timeout"
			if frameRecorder != nil {
				result.CarrierFrameEvents = frameRecorder.snapshot()
			}
			return result
		}
	}
	_ = clientApp.Close()
	requestStart := requestData.started
	if requestStart.IsZero() {
		result.Error = "request write start timestamp missing"
		result.Termination = "invalid_measurement"
		return result
	}
	elapsedMS := func(at time.Time) float64 {
		if at.IsZero() {
			return 0
		}
		return float64(at.Sub(requestStart).Microseconds()) / 1000
	}
	result.SenderBytes = requestData.bytes
	result.ReceiverBytes = serverData.requestBytes
	result.ApplicationReadWaitEvents = append(append([]streamPathQoEAppReadWait(nil), serverData.requestReadWaits...), responseData.readWaits...)
	result.SenderSHA256 = requestData.hash
	result.ReceiverSHA256 = serverData.requestHash
	result.ResponseBytes = serverData.responseBytes
	result.ResponseReceivedBytes = responseData.bytes
	result.ResponseSHA256 = serverData.responseHash
	result.ResponseReceiverSHA256 = responseData.hash
	result.ServerFirstRequestByteMS = elapsedMS(serverData.firstRequestByteAt)
	result.ServerRequestCompleteMS = elapsedMS(serverData.requestCompleteAt)
	result.ServerFirstResponseWriteMS = elapsedMS(serverData.firstResponseWriteAt)
	result.ClientFirstResponseTTFBMS = elapsedMS(responseData.firstByteAt)
	result.ClientResponseEOFMS = elapsedMS(responseData.eofAt)
	result.Termination = responseData.termination
	if requestData.err != nil {
		result.Error = requestData.err.Error()
	} else if serverData.err != nil {
		result.Error = serverData.err.Error()
	} else if responseData.err != nil {
		result.Error = responseData.err.Error()
	}
	if frameRecorder != nil {
		result.CarrierFrameEvents = frameRecorder.snapshot()
	}
	drainDeadline := time.Now().Add(2 * time.Second)
	var clientStats, serverStats StreamStats
	var ledgerDrainedAt time.Time
	for time.Now().Before(drainDeadline) {
		clientStats, serverStats = client.Snapshot(), server.Snapshot()
		if clientStats.OutstandingFrames == 0 && clientStats.OutstandingBytes == 0 &&
			serverStats.OutstandingFrames == 0 && serverStats.OutstandingBytes == 0 &&
			clientStats.RxPendingFrames == 0 && clientStats.RxPendingBytes == 0 &&
			serverStats.RxPendingFrames == 0 && serverStats.RxPendingBytes == 0 &&
			streamPathQoEWriteTelemetrySettled(clientStats) && streamPathQoEWriteTelemetrySettled(serverStats) &&
			clientStats.BoundedRescue.ReservedBytes == 0 && clientStats.BoundedRescue.OutstandingReservations == 0 &&
			serverStats.BoundedRescue.ReservedBytes == 0 && serverStats.BoundedRescue.OutstandingReservations == 0 {
			ledgerDrainedAt = time.Now()
			break
		}
		time.Sleep(time.Millisecond)
	}
	if ledgerDrainedAt.IsZero() {
		clientStats, serverStats = client.Snapshot(), server.Snapshot()
	}
	result.UniqueAckBytes = clientStats.TxAckedUsefulTotal
	result.ResponseUniqueAckBytes = serverStats.TxAckedUsefulTotal
	result.OutstandingFrames = clientStats.OutstandingFrames
	result.OutstandingBytes = clientStats.OutstandingBytes
	result.RxPendingFrames = serverStats.RxPendingFrames
	result.RxPendingBytes = serverStats.RxPendingBytes
	result.ResponseOutstandingFrames = serverStats.OutstandingFrames
	result.ResponseOutstandingBytes = serverStats.OutstandingBytes
	result.ResponseRxPendingFrames = clientStats.RxPendingFrames
	result.ResponseRxPendingBytes = clientStats.RxPendingBytes
	result.SentBytesByLeg = clientStats.TxWrittenBytesByLeg
	result.ResponseSentBytesByLeg = serverStats.TxWrittenBytesByLeg
	result.CoreTxRetryWrittenBytesByLeg = clientStats.TxRetryWrittenBytesByLeg
	result.CoreDataTxAttemptFramesByLeg = clientStats.Telemetry.DataTxAttemptFrames
	result.CoreDataTxRetransmitFramesByLeg = clientStats.Telemetry.DataTxRetransmitFrames
	result.CoreRescueAttemptFramesByLeg = clientStats.Telemetry.RescueAttemptFrames
	result.CoreRescueAttemptBytesByLeg = clientStats.Telemetry.RescueAttemptBytes
	result.CoreCarrierWriteAttemptFramesByLeg = clientStats.Telemetry.DataTxCarrierWriteAttemptFrames
	result.CoreCarrierWriteAttemptBytesByLeg = clientStats.Telemetry.DataTxAttemptBytes
	result.CoreDataTxRetryWriteAttemptFramesByLeg = clientStats.Telemetry.DataTxRetryWriteAttemptFrames
	result.CoreDataTxRescueWriteAttemptFramesByLeg = clientStats.Telemetry.DataTxRescueWriteAttemptFrames
	result.CoreDataTxRescueWriteAttemptBytesByLeg = clientStats.Telemetry.DataTxRescueWriteAttemptBytes
	result.CoreDataTxCarrierWriteErrorFramesByLeg = clientStats.Telemetry.DataTxCarrierWriteErrorFrames
	result.CoreDataTxCarrierWireBytesByLeg = clientStats.Telemetry.DataTxCarrierWireBytes
	result.ResponseTxRetryWrittenBytesByLeg = serverStats.TxRetryWrittenBytesByLeg
	result.ResponseDataTxAttemptFramesByLeg = serverStats.Telemetry.DataTxAttemptFrames
	result.ResponseDataTxRetransmitFramesByLeg = serverStats.Telemetry.DataTxRetransmitFrames
	result.ResponseRescueAttemptFramesByLeg = serverStats.Telemetry.RescueAttemptFrames
	result.ResponseRescueAttemptBytesByLeg = serverStats.Telemetry.RescueAttemptBytes
	result.ResponseCarrierWriteAttemptFramesByLeg = serverStats.Telemetry.DataTxCarrierWriteAttemptFrames
	result.ResponseCarrierWriteAttemptBytesByLeg = serverStats.Telemetry.DataTxAttemptBytes
	result.ResponseRetryWriteAttemptFramesByLeg = serverStats.Telemetry.DataTxRetryWriteAttemptFrames
	result.ResponseDataTxRescueWriteAttemptFramesByLeg = serverStats.Telemetry.DataTxRescueWriteAttemptFrames
	result.ResponseDataTxRescueWriteAttemptBytesByLeg = serverStats.Telemetry.DataTxRescueWriteAttemptBytes
	result.ResponseDataTxCarrierWriteErrorFramesByLeg = serverStats.Telemetry.DataTxCarrierWriteErrorFrames
	result.ResponseDataTxCarrierWireBytesByLeg = serverStats.Telemetry.DataTxCarrierWireBytes
	result.ReceiverDuplicateFramesByLeg = serverStats.Telemetry.DataRxDuplicateFrames
	result.ReceiverDuplicateBytesByLeg = serverStats.Telemetry.DataRxDuplicateBytes
	result.ResponseReceiverDuplicateFramesByLeg = clientStats.Telemetry.DataRxDuplicateFrames
	result.ResponseReceiverDuplicateBytesByLeg = clientStats.Telemetry.DataRxDuplicateBytes
	result.CoreTxAckedUsefulByLeg = clientStats.TxAckedUsefulByLeg
	result.Phase19PolicyARescueSuppressions = clientStats.Phase19PolicyARescueSuppressions
	result.CoreRxUniqueBytesByLeg = serverStats.RxUniqueBytesByLeg
	result.CoreResponseTxAckedUsefulByLeg = serverStats.TxAckedUsefulByLeg
	result.CoreResponseRxUniqueBytesByLeg = clientStats.RxUniqueBytesByLeg
	result.BoundedRescueTX = clientStats.BoundedRescue
	result.BoundedRescueRX = serverStats.BoundedRescue
	if !ledgerDrainedAt.IsZero() && !responseData.eofAt.IsZero() {
		result.ResponseEOFToBothDirectionAckDrainMS = float64(ledgerDrainedAt.Sub(responseData.eofAt).Microseconds()) / 1000
		result.RequestResponseCompletionMS = elapsedMS(ledgerDrainedAt)
		result.ApplicationCompletionMS = result.RequestResponseCompletionMS
		if result.ApplicationCompletionMS > 0 {
			result.ApplicationGoodputMbps = float64((requestData.bytes+responseData.bytes)*8) / (result.ApplicationCompletionMS * 1000)
		}
	}
	result.ApplicationFirstByteTTFBMS = result.ClientFirstResponseTTFBMS
	result.Pass = requestData.err == nil && serverData.err == nil && responseData.err == nil &&
		responseData.termination == "io.EOF" && requestData.bytes == job.size && serverData.requestBytes == job.size &&
		serverData.responseBytes == job.size && responseData.bytes == job.size &&
		requestData.hash == serverData.requestHash && serverData.responseHash == responseData.hash &&
		result.UniqueAckBytes == uint64(job.size) && result.ResponseUniqueAckBytes == uint64(job.size) &&
		result.OutstandingFrames == 0 && result.OutstandingBytes == 0 &&
		result.ResponseOutstandingFrames == 0 && result.ResponseOutstandingBytes == 0 &&
		result.RxPendingFrames == 0 && result.RxPendingBytes == 0 &&
		result.ResponseRxPendingFrames == 0 && result.ResponseRxPendingBytes == 0 &&
		!ledgerDrainedAt.IsZero() && result.ClientFirstResponseTTFBMS > 0
	return result
}

func streamPathQoEWriteTelemetrySettled(stats StreamStats) bool {
	for leg := range 2 {
		started := stats.Telemetry.DataTxCarrierWriteAttemptFrames[leg]
		finished := stats.Telemetry.DataTxAttemptFrames[leg] + stats.Telemetry.DataTxCarrierWriteErrorFrames[leg]
		if started != finished {
			return false
		}
	}
	return true
}

func streamPathQoERescueTelemetryCheck(sample streamPathQoESample, job streamPathQOEJob) (bool, string) {
	var startedFrames, startedBytes, settledFrames, settledBytes uint64
	var successfulFrames, successfulBytes uint64
	for _, wrapped := range sample.CoreRescueEvents {
		if wrapped.Endpoint != "client" {
			continue
		}
		event := wrapped.Event
		if job.mode.boundedRescue == nil {
			switch event.Kind {
			case "b0_rescue_carrier_write_started":
				startedFrames++
				startedBytes += event.PayloadBytes
			case "b0_rescue_carrier_write_settled":
				settledFrames++
				settledBytes += event.AcceptedBytes
				if event.Reason == "carrier_write_complete" && event.AcceptedBytes == event.PayloadBytes {
					successfulFrames++
					successfulBytes += event.AcceptedBytes
				}
			}
		} else {
			switch event.Kind {
			case "carrier_write_started":
				startedFrames++
				startedBytes += event.PayloadBytes
			case "carrier_write_settled":
				settledFrames++
				settledBytes += event.AcceptedBytes
			}
		}
	}
	actualStartedFrames := sumQoEArray(sample.CoreDataTxRescueWriteAttemptFramesByLeg)
	actualStartedBytes := sumQoEArray(sample.CoreDataTxRescueWriteAttemptBytesByLeg)
	if startedFrames != actualStartedFrames || startedBytes != actualStartedBytes || settledFrames != startedFrames {
		return false, fmt.Sprintf("carrier-boundary events started=%d/%d bytes=%d/%d settled=%d", startedFrames, actualStartedFrames, startedBytes, actualStartedBytes, settledFrames)
	}
	if job.mode.boundedRescue == nil {
		if successfulFrames != sumQoEArray(sample.CoreRescueAttemptFramesByLeg) || successfulBytes != sumQoEArray(sample.CoreRescueAttemptBytesByLeg) {
			return false, fmt.Sprintf("B0 successful events=%d/%d bytes=%d/%d", successfulFrames, sumQoEArray(sample.CoreRescueAttemptFramesByLeg), successfulBytes, sumQoEArray(sample.CoreRescueAttemptBytesByLeg))
		}
	} else if settledBytes != sample.BoundedRescueTX.CarrierAcceptedBytes {
		return false, fmt.Sprintf("B1 settled event accepted bytes=%d budget ledger accepted bytes=%d", settledBytes, sample.BoundedRescueTX.CarrierAcceptedBytes)
	}
	return true, fmt.Sprintf("matched started=%d settled=%d accepted_bytes=%d", startedFrames, settledFrames, settledBytes)
}

func sumQoEArray(values [2]uint64) uint64 { return values[0] + values[1] }

func streamPathQoEWritePayload(writer io.Writer, payload []byte) (int, error) {
	written := 0
	for written < len(payload) {
		end := written + 64<<10
		if end > len(payload) {
			end = len(payload)
		}
		n, err := writer.Write(payload[written:end])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}

package smp3core

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

type closureShortSource struct {
	source    slowTestSource
	size      int
	remaining int64
}

func (s *closureShortSource) Read(p []byte) (int, error) {
	if s.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > s.size {
		n = s.size
	}
	if int64(n) > s.remaining {
		n = int(s.remaining)
	}
	n, err := s.source.Read(p[:n])
	s.remaining -= int64(n)
	return n, err
}

// Same total bytes and legal configured chunk, different application Read sizes.
// This is actual Core/virtual Carrier, not an actual Native process result.
func TestClosureFiniteShortReads(t *testing.T) {
	if os.Getenv("SMP3_CLOSURE_SHORT_READS") != "1" {
		t.Skip("explicit calibrated finite short-read test")
	}
	for _, size := range []int{1024, 4096, 16384, 32768, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{100, 100}, ChunkSize: 64 << 10, QueueFrames: 8, MaxInflightFrames: 1024, MaxReorderFrames: 4096, AckInterval: 20 * time.Millisecond, RetransmitTimeout: time.Second, RecoveryTimeout: 15 * time.Second}
			tx, app := NewStreamEngine(cfg)
			rx, out := NewStreamEngine(cfg)
			defer tx.Close()
			defer rx.Close()
			clocks := releaseClocks(100e6, 100e6)
			for id := 0; id < 2; id++ {
				a, b := releasePair(clocks[id], 20*time.Millisecond)
				if err := tx.AttachLeg(LegID(id), a, nil); err != nil {
					t.Fatal(err)
				}
				if err := rx.AttachLeg(LegID(id), b, nil); err != nil {
					t.Fatal(err)
				}
			}
			const total = 512 << 20
			sink := &slowTestSink{}
			readDone := make(chan error, 1)
			writeDone := make(chan error, 1)
			go func() { _, err := io.Copy(sink, out); readDone <- err }()
			go func() {
				_, err := io.Copy(app, &closureShortSource{size: size, remaining: total})
				app.Close()
				writeDone <- err
			}()
			faultsDone := make(chan struct{})
			go func() {
				defer close(faultsDone)
				time.Sleep(time.Second)
				clocks[1].rate.Store(1e6)
				time.Sleep(2 * time.Second)
				clocks[1].rate.Store(0)
				time.Sleep(2 * time.Second)
				clocks[1].rate.Store(100e6)
			}()
			select {
			case err := <-readDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("finite body/EOF timed out")
			}
			<-faultsDone
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
			if sink.bytes.Load() != total || tx.txLedger.ProgressSnapshot().Outstanding != 0 {
				t.Fatal("content length or normal EOF ledger mismatch")
			}
			if tx.Snapshot().OrdinaryAdmissionState[1] != "normal" {
				t.Fatal("short reads did not complete recovery")
			}
			releaseLog(t, map[string]any{"read_bytes": size, "total_bytes": total, "prefix": "PASS", "normal_EOF": "PASS", "ledger": tx.txLedger.ProgressSnapshot(), "state": releaseStats(tx)})
		})
	}
}

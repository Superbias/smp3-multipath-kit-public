package smp3core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Diagnostic-only explicit GC belongs in this opt-in test, never production.
func memorySample(stage string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fds, _ := os.ReadDir("/proc/self/fd")
	b, _ := json.Marshal(struct {
		Stage                                                                                         string
		RSS, HeapAlloc, HeapInuse, HeapIdle, HeapReleased, HeapObjects, Sys, TotalAlloc, PauseTotalNs uint64
		NumGC                                                                                         uint32
		GCCPUFraction                                                                                 float64
		Goroutines, FDs                                                                               int
	}{stage, closureRSS(), m.HeapAlloc, m.HeapInuse, m.HeapIdle, m.HeapReleased, m.HeapObjects, m.Sys, m.TotalAlloc, m.PauseTotalNs, m.NumGC, m.GCCPUFraction, runtime.NumGoroutine(), len(fds)})
	fmt.Printf("MEMORY %s\n", b)
}

func memoryProfile(t *testing.T, stage string) {
	t.Helper()
	dir := os.Getenv("SMP3_MEMORY_PROFILES")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, stage+".heap"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStabilityClosure(t *testing.T) {
	if os.Getenv("SMP3_MEMORY_CLOSURE") == "" {
		t.Skip("set SMP3_MEMORY_CLOSURE=1")
	}
	t.Setenv("SMP3_DYNAMIC_CLOSURE", "1")
	runtime.GC()
	memorySample("start")
	memoryProfile(t, "start")
	for round := 1; round <= 5; round++ {
		memorySample(fmt.Sprintf("round%d_before", round))
		closureRun(t, 4, 1<<30, true)
		time.Sleep(100 * time.Millisecond)
		memorySample(fmt.Sprintf("round%d_before_gc", round))
		if round == 1 || round == 3 || round == 5 {
			memoryProfile(t, fmt.Sprintf("round%d", round))
		}
		runtime.GC()
		memorySample(fmt.Sprintf("round%d_after_gc", round))
		time.Sleep(time.Second)
		memorySample(fmt.Sprintf("round%d_idle", round))
	}
	memoryProfile(t, "final_gc")
	for _, n := range []int{1, 8} {
		memorySample(fmt.Sprintf("streams%d_before", n))
		closureRun(t, n, 128<<20, true)
		runtime.GC()
		memorySample(fmt.Sprintf("streams%d_after", n))
	}
}

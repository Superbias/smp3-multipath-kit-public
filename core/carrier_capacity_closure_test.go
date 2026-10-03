package smp3core

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// closureCarrier is a single physical service clock shared by all logical
// stream sockets on one leg. Unlike N independent paced sockets, its total
// service cannot grow when streams join.
type closureCarrier struct {
	mu   sync.Mutex
	next time.Time
	bps  int64
}

func (c *closureCarrier) reserve(n int) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.next.Before(now) {
		c.next = now
	}
	c.next = c.next.Add(time.Duration(float64(n*8) / float64(c.bps) * float64(time.Second)))
	return c.next
}
func closureLink(c *closureCarrier) (StreamLeg, StreamLeg) {
	a, b := net.Pipe()
	l := &benchmarkLink{Conn: a, tx: make(chan []byte, 8), done: make(chan struct{})}
	go func() {
		timer := time.NewTimer(time.Hour)
		defer timer.Stop()
		for {
			select {
			case <-l.done:
				return
			case p := <-l.tx:
				if p[0] == byte(StreamFrameData) {
					timer.Reset(time.Until(c.reserve(len(p))))
					select {
					case <-timer.C:
					case <-l.done:
						return
					}
				}
				if _, err := l.Conn.Write(p); err != nil {
					l.Close()
					return
				}
			}
		}
	}()
	return l, b
}
func closureRSS() uint64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			f := strings.Fields(line)
			n, _ := strconv.ParseUint(f[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
func closureCPU() float64 {
	b, _ := os.ReadFile("/proc/self/stat")
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return 0
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 13 {
		return 0
	}
	u, _ := strconv.ParseFloat(f[11], 64)
	v, _ := strconv.ParseFloat(f[12], 64)
	return (u + v) / 100 // Linux USER_HZ=100
}
func closureRun(t *testing.T, n int, total int64, shared bool) {
	t.Helper()
	r := NewCarrierCapacityRegistry()
	carriers := [2]*closureCarrier{{bps: 50e6}, {bps: 200e6}}
	engines := make([]*StreamEngine, n)
	peers := make([]*StreamEngine, n)
	apps := make([]net.Conn, n)
	dest := make([]net.Conn, n)
	cfg := StreamConfig{SchedulerMode: StreamSchedulerAggregation, CapacityMode: StreamCapacityDynamic, BandwidthMbps: []uint32{50, 200}, ChunkSize: 64 << 10, QueueFrames: 32, MaxInflightFrames: 512, MaxReorderFrames: 4096, AckInterval: 5 * time.Millisecond, RetransmitTimeout: 1500 * time.Millisecond}
	if os.Getenv("SMP3_HOL_COMPLETION") != "" {
		cfg.HOLMode = StreamHOLCompletion
	}
	if os.Getenv("SMP3_DYNAMIC_CLOSURE") != "" {
		cfg.ActivationMode = StreamActivationDynamic
		cfg.ActivationWindow = time.Second
	}
	for i := range engines {
		c := cfg
		if shared {
			c.CapacityProvider = r.Provider([2]string{"closure|out|50", "closure|out|200"}, c.BandwidthMbps, c.ChunkSize)
		}
		engines[i], apps[i] = NewStreamEngine(c)
		recv := cfg
		recv.CapacityMode = StreamCapacityFixed
		peers[i], dest[i] = NewStreamEngine(recv)
		for id := 0; id < 2; id++ {
			a, b := closureLink(carriers[id])
			if err := engines[i].AttachLeg(LegID(id), a, nil); err != nil {
				t.Fatal(err)
			}
			if err := peers[i].AttachLeg(LegID(id), b, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	defer func() {
		for i := range engines {
			engines[i].Close()
			peers[i].Close()
		}
	}()
	if os.Getenv("SMP3_MEMORY_CLOSURE") == "" {
		runtime.GC()
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rssStart := closureRSS()
	peak := rssStart
	cpu := closureCPU()
	start := time.Now()
	stop, monDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(monDone)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if x := closureRSS(); x > peak {
					peak = x
				}
			}
		}
	}()
	errs := make(chan error, 2*n)
	dur := make([]time.Duration, n)
	for i := range engines {
		i := i
		size := total / int64(n)
		_ = apps[i].SetDeadline(time.Now().Add(3 * time.Minute))
		_ = dest[i].SetDeadline(time.Now().Add(3 * time.Minute))
		go func() { _, err := io.CopyN(benchmarkPayloadChecker{}, dest[i], size); errs <- err }()
		go func() {
			s := time.Now()
			_, err := io.CopyN(apps[i], benchmarkPayloadReader{}, size)
			dur[i] = time.Since(s)
			errs <- err
		}()
	}
	for i := 0; i < 2*n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range engines {
		promotionWait(t, "ACK ledger drain", func() bool { return !e.hasOutstanding() })
	}
	wall := time.Since(start)
	cpuDelta := closureCPU() - cpu
	close(stop)
	<-monDone
	runtime.ReadMemStats(&after)
	var ack, physical, retry, rescue uint64
	var assigned [2]uint64
	var est [2]float64
	for _, e := range engines {
		s := e.Snapshot()
		if s.OutstandingFrames != 0 {
			t.Fatal("ledger leak")
		}
		ack += s.TxAckedUsefulByLeg[0] + s.TxAckedUsefulByLeg[1]
		physical += s.TxSentBytesByLeg[0] + s.TxSentBytesByLeg[1]
		retry += s.TxRetransmitBytesByLeg[0] + s.TxRetransmitBytesByLeg[1]
		rescue += s.FrontierRescueAttempts
		as, _ := e.assignedSnapshot()
		assigned[0] += as[0]
		assigned[1] += as[1]
		est = s.CapacitySmoothedBPS
	}
	if ack != uint64(total) {
		t.Fatalf("payload/ACK mismatch %d/%d", ack, total)
	}
	if retry != 0 || rescue != 0 {
		t.Fatalf("healthy amplification retry=%d rescue=%d", retry, rescue)
	}
	for _, e := range engines {
		e.Close()
	}
	for _, e := range peers {
		e.Close()
	}
	if os.Getenv("SMP3_MEMORY_CLOSURE") == "" {
		runtime.GC()
	}
	rssEnd := closureRSS()
	r.mu.Lock()
	entries := len(r.entries)
	refs := 0
	for _, e := range r.entries {
		refs += e.refs
	}
	r.mu.Unlock()
	if shared && (entries != 2 || refs != 0) {
		t.Fatalf("registry lifecycle entries=%d refs=%d", entries, refs)
	}
	fmt.Printf("CLOSURE shared=%t streams=%d bytes=%d wall_s=%.4f cpu_s=%.4f useful_mbps=%.3f per_stream_wall=%v estimate_mbps=[%.3f %.3f] share=[%.4f %.4f] amp=%.6f retry=%d rescue=%d ledger=0 entries=%d refs=%d rss_bytes=[%d %d %d] alloc_bytes=%d mallocs=%d payload=PASS\n", shared, n, total, wall.Seconds(), cpuDelta, float64(total)*8/wall.Seconds()/1e6, dur, est[0]*8/1e6, est[1]*8/1e6, float64(assigned[0])/float64(assigned[0]+assigned[1]), float64(assigned[1])/float64(assigned[0]+assigned[1]), float64(physical)/float64(total), retry, rescue, entries, refs, rssStart, peak, rssEnd, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
}
func TestGlobalCapacityTransferClosure(t *testing.T) {
	if os.Getenv("SMP3_GLOBAL_CLOSURE") == "" {
		t.Skip("set SMP3_GLOBAL_CLOSURE=1")
	}
	for _, n := range []int{1, 4, 8} {
		for _, shared := range []bool{false, true} {
			t.Run(fmt.Sprintf("streams%d/shared%t", n, shared), func(t *testing.T) { closureRun(t, n, 128<<20, shared) })
		}
	}
	t.Run("GiB", func(t *testing.T) { closureRun(t, 4, 1<<30, true) })
}

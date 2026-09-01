package multipath

import (
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type datagramPacket struct {
	id       uint64
	address  string
	data     []byte
	queuedAt time.Time
}

type datagramLegPerf struct {
	mu              sync.Mutex
	ewmaBytesPerSec float64
	ewmaDelay       time.Duration
	lastSuccess     time.Time
}

type datagramLeg struct {
	id          uint8
	send        chan datagramPacket
	perf        datagramLegPerf
	queuedBytes atomic.Int64
}

func (c *mpDatagramCore) availableLegs() []*datagramLeg {
	stats := c.snapshotStats()
	legs := make([]*datagramLeg, 0, 2)
	for id := uint8(0); id < 2; id++ {
		if !stats.LegUp[id] {
			continue
		}
		leg := &datagramLeg{id: id, send: make(chan datagramPacket, c.cfg.QueueFrames)}
		leg.queuedBytes.Store(stats.QueueBytes[id])
		legs = append(legs, leg)
	}
	return legs
}

func (c *mpDatagramCore) chooseLeg(legs []*datagramLeg, packetSize int) *datagramLeg {
	var best *datagramLeg
	bestScore := math.MaxFloat64
	for _, leg := range legs {
		base := 1.0
		if int(leg.id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[leg.id] > 0 {
			base = float64(c.cfg.BandwidthMbps[leg.id])
		}
		if c.cfg.Mode == datagramModeAdaptive {
			leg.perf.mu.Lock()
			bps, delay := leg.perf.ewmaBytesPerSec, leg.perf.ewmaDelay
			leg.perf.mu.Unlock()
			if bps > 0 {
				dynamicMbps := bps * 8 / 1e6
				if dynamicMbps < 1 {
					dynamicMbps = 1
				}
				if base > 1 {
					base = math.Sqrt(base * dynamicMbps)
				} else {
					base = dynamicMbps
				}
			}
			if delay >= c.cfg.AdaptiveQueueDelay*2 {
				base *= 0.15
			} else if delay >= c.cfg.AdaptiveQueueDelay {
				base *= 0.4
			}
		}
		if base < 0.1 {
			base = 0.1
		}
		queued := leg.queuedBytes.Load()
		if queued < 0 {
			queued = 0
		}
		score := float64(queued+int64(packetSize)+1) / base
		if c.cfg.Mode == datagramModeAdaptive {
			leg.perf.mu.Lock()
			delay := leg.perf.ewmaDelay
			leg.perf.mu.Unlock()
			if delay > 0 {
				score *= 1 + float64(delay)/float64(c.cfg.AdaptiveQueueDelay)
			}
		}
		if best == nil || score < bestScore {
			best, bestScore = leg, score
		}
	}
	return best
}

func (c *mpDatagramCore) shouldDuplicate(size int, legs []*datagramLeg) bool {
	if len(legs) < 2 {
		return false
	}
	if c.cfg.Mode == datagramModeDuplicate {
		return true
	}
	return c.cfg.Mode == datagramModeAdaptive && c.cfg.AdaptiveDuplicateThreshold > 0 && size <= c.cfg.AdaptiveDuplicateThreshold
}

func (c *mpDatagramCore) acceptDatagramID(id uint64, size int) bool {
	return c.engine.AcceptDatagramIDForTest(id, size)
}

func (c *mpDatagramCore) injectDatagram(address string, payload []byte) {
	c.engine.InjectDatagramForTest(address, payload)
}

func (c *mpDatagramCore) handleLegFailure(id uint8, err error) bool {
	return c.engine.HandleLegFailureForTest(smp3core.LegID(id), err)
}

func (c *mpDatagramCore) hasLeg(id uint8) bool { return c.engine.HasLeg(smp3core.LegID(id)) }

func (c *mpDatagramCore) txSequence() uint64          { return c.engine.TxSequenceForTest() }
func (c *mpDatagramCore) maxSeen() uint64             { return c.engine.MaxSeenForTest() }
func (c *mpDatagramCore) seenContains(id uint64) bool { return c.engine.SeenContainsForTest(id) }

func writeDatagramFrame(writer io.Writer, packet datagramPacket) error {
	return smp3core.WriteDatagramFrame(writer, packet.id, packet.address, packet.data)
}

func readDatagramFrame(reader io.Reader, maxDatagram int) (datagramPacket, error) {
	id, address, data, err := smp3core.ReadDatagramFrame(reader, maxDatagram)
	return datagramPacket{id: id, address: address, data: data}, err
}

package multipath

import (
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func testLedgerAdvance(c *mpCore, target uint64, now time.Time) {
	for c.txLedger.NextSequence() < target {
		c.txLedger.Add(nil, now)
	}
	if progress := c.txLedger.ProgressSnapshot(); progress.AckedNext < target {
		c.txLedger.ApplyACK(target, now)
	}
}

func testLedgerAdd(c *mpCore, payload []byte, now time.Time, sentLeg int16) *smp3core.StreamTXRecord {
	record := c.txLedger.Add(payload, now)
	if sentLeg >= 0 {
		c.txLedger.MarkTransit(record, smp3core.LegID(sentLeg), now)
		c.txLedger.MarkAttemptSent(record, smp3core.LegID(sentLeg), false, now)
	}
	return record
}

func testLedgerAddTransit(c *mpCore, payload []byte, now time.Time, leg uint8) *smp3core.StreamTXRecord {
	record := c.txLedger.Add(payload, now)
	c.txLedger.MarkTransit(record, smp3core.LegID(leg), now)
	return record
}

package multipath

import (
	"net"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

// The generic stream engine is implemented in smp3core. These aliases keep
// the existing sing-box adapter vocabulary at the host boundary.
type schedulerMode = smp3core.StreamSchedulerMode

const (
	schedulerStatic   = smp3core.StreamSchedulerStatic
	schedulerAdaptive = smp3core.StreamSchedulerAdaptive
)

type coreConfig = smp3core.StreamConfig
type coreStats = smp3core.StreamStats

const frameHeaderSize = smp3core.StreamFrameHeaderSize
const maxFramePayload = smp3core.MaxStreamFramePayload

var errCoreClosed = smp3core.ErrStreamClosed

type mpCore struct {
	cfg      coreConfig
	engine   *smp3core.StreamEngine
	txLedger *smp3core.StreamTXLedger
	inflight chan struct{}
}

func newCore(cfg coreConfig) (*mpCore, net.Conn) {
	engine, appConn := smp3core.NewStreamEngine(cfg)
	return &mpCore{cfg: cfg, engine: engine, txLedger: engine.TXLedger(), inflight: engine.InflightForTest()}, appConn
}

func (c *mpCore) addLeg(id uint8, conn smp3core.StreamLeg, onClose func(error)) error {
	return c.engine.AttachLeg(smp3core.LegID(id), conn, onClose)
}

func (c *mpCore) replaceLeg(id uint8, err error) bool {
	return c.engine.ReplaceLeg(smp3core.LegID(id), err)
}

func (c *mpCore) AppConn() net.Conn     { return c.engine.AppConn() }
func (c *mpCore) Done() <-chan struct{} { return c.engine.Done() }
func (c *mpCore) Close() error          { return c.engine.Close() }

func (c *mpCore) snapshotStats() coreStats { return c.engine.Snapshot() }

func (c *mpCore) startGracefulClose(err error) { c.engine.StartGracefulClose(err) }
func (c *mpCore) activate() {
	c.engine.SetOnActivateForTest(c.cfg.OnActivate)
	c.engine.Activate()
}
func (c *mpCore) hasLeg(id uint8) bool { return c.engine.HasLeg(smp3core.LegID(id)) }
func (c *mpCore) legCount() int        { return c.engine.LegCount() }
func (c *mpCore) isActive() bool       { return c.engine.Active() }
func (c *mpCore) isClosing() bool      { return c.engine.Closing() }
func (c *mpCore) isFinalizing() bool   { return c.engine.Finalizing() }

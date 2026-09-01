package multipath

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	frameTypeDatagram      byte = smp3core.DatagramFrameType
	maxRoutedDatagramSize       = smp3core.MaxDatagramPayload
	maxDatagramAddressSize      = smp3core.MaxDatagramAddressSize
)

var (
	errDatagramClosed   = smp3core.ErrDatagramClosed
	errDatagramTooLarge = smp3core.ErrDatagramTooLarge
)

type datagramMode = smp3core.DatagramMode

const (
	datagramModeStripe    = smp3core.DatagramStripe
	datagramModeDuplicate = smp3core.DatagramDuplicate
	datagramModeAdaptive  = smp3core.DatagramAdaptive
)

type datagramConfig struct {
	Mode                       datagramMode
	QueueFrames                int
	MaxDatagramSize            int
	DedupWindow                uint64
	IdleTimeout                time.Duration
	RecoveryTimeout            time.Duration
	AdaptiveQueueDelay         time.Duration
	AdaptiveDuplicateThreshold int
	BandwidthMbps              []uint32
	OnLegDown                  func(uint8, error)
	OnLegUseful                func(uint8, int)
}

type datagramStats = smp3core.DatagramStats

type mpDatagramCore struct {
	cfg    datagramConfig
	engine *smp3core.DatagramEngine
}

func newDatagramCore(cfg datagramConfig) (*mpDatagramCore, *datagramPacketConn) {
	core := &mpDatagramCore{cfg: cfg}
	core.engine = smp3core.NewDatagramEngine(toDatagramConfig(cfg))
	return core, &datagramPacketConn{core: core}
}

func toDatagramConfig(cfg datagramConfig) smp3core.DatagramConfig {
	converted := smp3core.DatagramConfig{
		Mode:                       smp3core.DatagramMode(cfg.Mode),
		QueueFrames:                cfg.QueueFrames,
		MaxDatagramSize:            cfg.MaxDatagramSize,
		DedupWindow:                cfg.DedupWindow,
		IdleTimeout:                cfg.IdleTimeout,
		RecoveryTimeout:            cfg.RecoveryTimeout,
		AdaptiveQueueDelay:         cfg.AdaptiveQueueDelay,
		AdaptiveDuplicateThreshold: cfg.AdaptiveDuplicateThreshold,
		BandwidthMbps:              append([]uint32(nil), cfg.BandwidthMbps...),
	}
	if cfg.OnLegDown != nil {
		converted.OnLegDown = func(id smp3core.LegID, err error) { cfg.OnLegDown(uint8(id), err) }
	}
	if cfg.OnLegUseful != nil {
		converted.OnLegUseful = func(id smp3core.LegID, bytes int) { cfg.OnLegUseful(uint8(id), bytes) }
	}
	return converted
}

func (c *mpDatagramCore) Done() <-chan struct{} { return c.engine.Done() }
func (c *mpDatagramCore) Close() error          { return c.engine.Close() }

func (c *mpDatagramCore) addLeg(id uint8, leg smp3core.DatagramLeg, onClose func(error)) error {
	return c.engine.AttachLeg(smp3core.LegID(id), leg, onClose)
}

func (c *mpDatagramCore) sendDatagram(payload []byte, address string, deadline time.Time) error {
	return c.engine.Send(payload, address, deadline)
}

func (c *mpDatagramCore) snapshotStats() datagramStats { return c.engine.Snapshot() }

type smp3PacketAddr string

func (a smp3PacketAddr) Network() string { return "udp" }
func (a smp3PacketAddr) String() string  { return string(a) }

type datagramPacketConn struct {
	core          *mpDatagramCore
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
}

func (c *datagramPacketConn) readDatagram() (smp3core.Datagram, error) {
	deadline := deadlineFromAtomic(&c.readDeadline)
	datagram, err := c.core.engine.Receive(deadline)
	if errors.Is(err, smp3core.ErrDatagramTimeout) {
		return smp3core.Datagram{}, timeoutError{}
	}
	if errors.Is(err, smp3core.ErrDatagramClosed) {
		return smp3core.Datagram{}, net.ErrClosed
	}
	return datagram, err
}

func (c *datagramPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	datagram, err := c.readDatagram()
	if err != nil {
		return 0, nil, err
	}
	return copy(p, datagram.Payload), smp3PacketAddr(datagram.Address), nil
}

func (c *datagramPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, errors.New("nil datagram destination")
	}
	deadline := deadlineFromAtomic(&c.writeDeadline)
	if err := c.core.sendDatagram(p, addr.String(), deadline); err != nil {
		if errors.Is(err, errDatagramTooLarge) {
			return len(p), nil
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, timeoutError{}
		}
		return 0, err
	}
	return len(p), nil
}

func (c *datagramPacketConn) Close() error        { return c.core.Close() }
func (c *datagramPacketConn) LocalAddr() net.Addr { return smp3PacketAddr("0.0.0.0:0") }
func (c *datagramPacketConn) SetDeadline(t time.Time) error {
	c.storeDeadline(&c.readDeadline, t)
	c.storeDeadline(&c.writeDeadline, t)
	return nil
}
func (c *datagramPacketConn) SetReadDeadline(t time.Time) error {
	c.storeDeadline(&c.readDeadline, t)
	return nil
}
func (c *datagramPacketConn) SetWriteDeadline(t time.Time) error {
	c.storeDeadline(&c.writeDeadline, t)
	return nil
}
func (c *datagramPacketConn) storeDeadline(v *atomic.Int64, t time.Time) {
	if t.IsZero() {
		v.Store(0)
	} else {
		v.Store(t.UnixNano())
	}
}

func deadlineFromAtomic(v *atomic.Int64) time.Time {
	ns := v.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.PacketConn = (*datagramPacketConn)(nil)

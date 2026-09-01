package multipath

import (
	"io"
	"net"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	frameTypeData     byte = byte(smp3core.StreamFrameData)
	frameTypeActivate byte = byte(smp3core.StreamFrameActivate)
	frameTypeAck      byte = byte(smp3core.StreamFrameACK)
	frameTypeClose    byte = byte(smp3core.StreamFrameClose)
)

type wireFrame struct {
	typ  byte
	seq  uint64
	data []byte
}

type dataFrame struct {
	seq  uint64
	data []byte
	leg  uint8
}

type txSendAttempt struct {
	record *smp3core.StreamTXRecord
	rescue bool
}

type mpLeg struct {
	id     uint8
	send   chan txSendAttempt
	rescue chan txSendAttempt
	done   chan struct{}
	perf   streamLegPerf
}

type streamLegPerf struct {
	mu           sync.Mutex
	writeBPS     float64
	writeLatency time.Duration
	ackedBPS     float64
	lastAckAt    time.Time
}

func (c *mpCore) injectFrame(sequence uint64, payload []byte, leg uint8) {
	c.engine.InjectFrameForTest(sequence, payload, smp3core.LegID(leg))
}

func (c *mpCore) handleAck(next uint64) error    { return c.engine.HandleACKForTest(next) }
func (c *mpCore) drainOutstandingOnClose() error { return c.engine.DrainOutstandingForTest() }
func (c *mpCore) beginFinalizing() bool          { return c.engine.BeginFinalizingForTest() }
func (c *mpCore) putBuffer([]byte)               {}

func (c *mpCore) getLeg(id uint8) *mpLeg {
	if !c.engine.HasLeg(smp3core.LegID(id)) {
		return nil
	}
	return &mpLeg{id: id}
}

func (c *mpCore) handleLegFailure(leg *mpLeg, err error) bool {
	if leg == nil {
		return false
	}
	return c.engine.HandleLegFailureForTest(smp3core.LegID(leg.id), err)
}

func (c *mpCore) chooseLeg(active bool, avoid int16) *mpLeg {
	id, ok := c.engine.ChooseLegForTest(active, avoid)
	if !ok {
		return nil
	}
	return c.getLeg(uint8(id))
}

func (c *mpCore) sendAckFrame(value uint64, force bool) bool {
	return c.engine.SendACKFrameForTest(value, force)
}

func (c *mpCore) sendCloseFrame(value uint64) bool {
	return c.engine.SendCloseFrameForTest(value)
}

func (c *mpCore) queueDataAttempt(record *smp3core.StreamTXRecord, leg uint8) bool {
	return c.engine.QueueDataAttemptForTest(record, smp3core.LegID(leg))
}

func encodeControlFrame(typ byte, value uint64) [frameHeaderSize]byte {
	var header [frameHeaderSize]byte
	_ = smp3core.EncodeStreamFrameHeader(header[:], smp3core.StreamFrameHeader{
		Type:  smp3core.StreamFrameType(typ),
		Value: value,
	})
	return header
}

func writeDataFrame(conn smp3core.StreamLeg, frame dataFrame) error {
	var header [frameHeaderSize]byte
	if err := smp3core.EncodeStreamFrameHeader(header[:], smp3core.StreamFrameHeader{
		Type:   smp3core.StreamFrameData,
		Value:  frame.seq,
		Length: uint32(len(frame.data)),
	}); err != nil {
		return err
	}
	buffers := net.Buffers{header[:], frame.data}
	_, err := buffers.WriteTo(conn)
	return err
}

func readWireFrame(conn smp3core.StreamLeg, core *mpCore) (wireFrame, error) {
	header, err := smp3core.ReadStreamFrameHeader(conn)
	if err != nil {
		return wireFrame{}, err
	}
	frame := wireFrame{typ: byte(header.Type), seq: header.Value}
	if header.Type != smp3core.StreamFrameData {
		return frame, nil
	}
	length := int(header.Length)
	frame.data = make([]byte, length)
	if _, err := io.ReadFull(conn, frame.data); err != nil {
		return wireFrame{}, err
	}
	return frame, nil
}

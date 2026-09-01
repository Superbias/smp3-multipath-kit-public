package multipath

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// singDatagramPacketConn exposes the standalone SMP3 datagram core through
// sing's native PacketConn interface.  Keeping this adapter separate lets the
// datagram core itself remain dependency-free/testable while avoiding lossy or
// implementation-specific net.Addr -> Socksaddr conversion in the router.
type singDatagramPacketConn struct {
	*datagramPacketConn
}

func newSingDatagramPacketConn(conn *datagramPacketConn) *singDatagramPacketConn {
	return &singDatagramPacketConn{datagramPacketConn: conn}
}

func parsePacketDestination(address string) (M.Socksaddr, error) {
	destination := M.ParseSocksaddr(address)
	if !destination.IsValid() || destination.Port == 0 {
		return M.Socksaddr{}, errors.New("invalid multipath datagram destination")
	}
	return destination, nil
}

func (c *singDatagramPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	packet, err := c.readDatagram()
	if err != nil {
		return 0, nil, err
	}
	destination, err := parsePacketDestination(packet.Address)
	if err != nil {
		return 0, nil, err
	}
	n := copy(p, packet.Payload)
	return n, destination, nil
}

func (c *singDatagramPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, errors.New("nil datagram destination")
	}
	destination := M.SocksaddrFromNet(addr)
	if !destination.IsValid() || destination.Port == 0 {
		// Some callers use a lightweight net.Addr implementation.  String form is
		// the stable interoperability boundary for the SMP3 wire address.
		destination = M.ParseSocksaddr(addr.String())
	}
	if !destination.IsValid() || destination.Port == 0 {
		return 0, errors.New("invalid datagram destination")
	}
	deadline := deadlineFromAtomic(&c.writeDeadline)
	if err := c.core.sendDatagram(p, destination.String(), deadline); err != nil {
		if errors.Is(err, errDatagramTooLarge) {
			return len(p), nil
		}
		if deadlineExpired(deadline) {
			return 0, timeoutError{}
		}
		return 0, err
	}
	return len(p), nil
}

// ReaderMTU is the application payload capacity consumed by ReadPacket.  The
// pinned sing read-wait path adds the source's ReaderOverhead and the route's
// headroom before allocating its buffer, so exposing the configured payload
// bound prevents the default 16 KiB UDP buffer from losing headroom.
func (c *singDatagramPacketConn) ReaderMTU() int {
	if c == nil || c.core == nil {
		return 0
	}
	return c.core.cfg.MaxDatagramSize
}

func deadlineExpired(deadline time.Time) bool {
	return !deadline.IsZero() && !time.Now().Before(deadline)
}

func (c *singDatagramPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	packet, err := c.readDatagram()
	if err != nil {
		return M.Socksaddr{}, err
	}
	destination, err := parsePacketDestination(packet.Address)
	if err != nil {
		return M.Socksaddr{}, err
	}
	if buffer.FreeLen() < len(packet.Payload) {
		return M.Socksaddr{}, io.ErrShortBuffer
	}
	copy(buffer.Extend(len(packet.Payload)), packet.Payload)
	return destination, nil
}

func (c *singDatagramPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	defer buffer.Release()
	if !destination.IsValid() || destination.Port == 0 {
		return errors.New("invalid datagram destination")
	}
	// Oversize is rejected at the datagram boundary, not as a carrier error.
	// Returning nil keeps sing's packet-copy loop alive so a later legal packet
	// on the same SOCKS association is still delivered.
	if buffer.Len() > c.core.cfg.MaxDatagramSize {
		return nil
	}
	deadline := deadlineFromAtomic(&c.writeDeadline)
	if err := c.core.sendDatagram(buffer.Bytes(), destination.String(), deadline); err != nil {
		if deadlineExpired(deadline) {
			return timeoutError{}
		}
		return err
	}
	return nil
}

var _ net.PacketConn = (*singDatagramPacketConn)(nil)
var _ N.PacketConn = (*singDatagramPacketConn)(nil)
var _ N.ReaderWithMTU = (*singDatagramPacketConn)(nil)

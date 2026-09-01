package socks

import (
	"net"

	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/mihomo/transport/socks5"
)

// socksUDPReceiveBufferSize preserves the ordinary UDP payload budget while
// reserving enough space for the largest SOCKS5 UDP request header.
const socksUDPReceiveBufferSize = pool.UDPBufferSize + 3 + socks5.MaxAddrLen

func waitReadSocksUDP(conn net.PacketConn) (data []byte, put func(), addr net.Addr, err error) {
	buffer := pool.Get(socksUDPReceiveBufferSize)
	put = func() { _ = pool.Put(buffer) }
	n, addr, err := conn.ReadFrom(buffer)
	if n > 0 {
		data = buffer[:n]
		return
	}
	put()
	put = nil
	return nil, nil, addr, err
}

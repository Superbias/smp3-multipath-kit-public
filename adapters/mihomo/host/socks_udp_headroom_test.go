package socks

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/pool"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/socks5"
)

type socksUDPHeadroomResult struct {
	payload     []byte
	destination string
}
type socksUDPHeadroomTunnel struct{ packets chan socksUDPHeadroomResult }

func (t *socksUDPHeadroomTunnel) HandleTCPConn(net.Conn, *C.Metadata) {}
func (t *socksUDPHeadroomTunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {
	payload := append([]byte(nil), packet.Data()...)
	destination := metadata.RemoteAddress()
	packet.Drop()
	t.packets <- socksUDPHeadroomResult{payload: payload, destination: destination}
}
func (t *socksUDPHeadroomTunnel) NatTable() C.NatTable { return nil }

func TestSOCKSUDPIngressHeadroom(t *testing.T) {
	tunnel := &socksUDPHeadroomTunnel{packets: make(chan socksUDPHeadroomResult, 16)}
	listener, err := NewUDP("127.0.0.1:0", tunnel)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := net.Dial("udp", listener.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cases := []struct {
		name        string
		destination string
		size        int
	}{
		{"ipv4-1200", "192.0.2.1:53", 1200},
		{"ipv4-16383", "192.0.2.1:53", 16383},
		{"ipv4-16384", "192.0.2.1:53", 16384},
		{"ipv4-16385", "192.0.2.1:53", 16385},
		{"ipv6-16384", "[2001:db8::1]:5353", 16384},
		{"domain-16384", "example.com:443", 16384},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{byte(index + 1)}, test.size)
			packet, err := socks5.EncodeUDPPacket(socks5.ParseAddr(test.destination), payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(packet); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-tunnel.packets:
				if got.destination != test.destination {
					t.Fatalf("destination=%q want=%q", got.destination, test.destination)
				}
				if !bytes.Equal(got.payload, payload) {
					t.Fatalf("payload len=%d want=%d", len(got.payload), len(payload))
				}
			case <-time.After(time.Second):
				t.Fatal("SOCKS UDP packet did not reach downstream tunnel")
			}
		})
	}
	fragment, err := socks5.EncodeUDPPacket(socks5.ParseAddr("192.0.2.1:53"), []byte("fragment"))
	if err != nil {
		t.Fatal(err)
	}
	fragment[2] = 1
	if _, err := conn.Write(fragment); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-tunnel.packets:
		t.Fatalf("fragment unexpectedly delivered: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSOCKSUDPReceiveCapacity(t *testing.T) {
	required := pool.UDPBufferSize + 3 + socks5.MaxAddrLen
	if socksUDPReceiveBufferSize != required {
		t.Fatalf("capacity=%d want=%d", socksUDPReceiveBufferSize, required)
	}
	if socksUDPReceiveBufferSize != 16646 {
		t.Fatalf("unexpected required capacity=%d", socksUDPReceiveBufferSize)
	}
	buffer := pool.Get(socksUDPReceiveBufferSize)
	if len(buffer) != socksUDPReceiveBufferSize || cap(buffer) != 32768 {
		t.Fatalf("len=%d cap=%d", len(buffer), cap(buffer))
	}
	if err := pool.Put(buffer); err != nil {
		t.Fatal(err)
	}
}

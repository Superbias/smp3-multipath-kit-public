package server

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func datagramMode(value string) smp3core.DatagramMode {
	switch value {
	case "stripe":
		return smp3core.DatagramStripe
	case "duplicate":
		return smp3core.DatagramDuplicate
	default:
		return smp3core.DatagramAdaptive
	}
}

func (s *Server) startDatagramHost(session *serverSession) error {
	target := &udpTarget{
		server:  s,
		session: session,
		maxSize: s.cfg.UDP.MaxDatagramSize,
		sockets: make(map[string]net.PacketConn),
	}
	if !session.setTarget(target) {
		_ = target.Close()
		return errors.New("datagram session closed before target setup")
	}
	session.goWorker(func() { s.datagramRequestLoop(session, target) })
	return nil
}

func (s *Server) datagramRequestLoop(session *serverSession, target *udpTarget) {
	for {
		datagram, err := session.dgram.Receive(time.Time{})
		if err != nil {
			if !errors.Is(err, smp3core.ErrDatagramClosed) {
				s.logger.Debug("datagram application reader stopped", "session", sessionLogID(session.id), "error", err)
			}
			return
		}
		address, err := resolveUDP(datagram.Address)
		if err != nil {
			s.logger.Warn("datagram destination rejected", "session", sessionLogID(session.id), "error", err)
			session.close()
			return
		}
		if err := target.write(datagram.Payload, address, time.Time{}); err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, smp3core.ErrDatagramClosed) {
				s.logger.Warn("datagram target write failed", "session", sessionLogID(session.id), "destination", datagram.Address, "error", err)
			}
			session.close()
			return
		}
	}
}

type udpTarget struct {
	server  *Server
	session *serverSession
	maxSize int

	access   sync.Mutex
	closed   bool
	sockets  map[string]net.PacketConn
	closeOne sync.Once
}

func (u *udpTarget) socket(family string) (net.PacketConn, error) {
	u.access.Lock()
	defer u.access.Unlock()
	if u.closed {
		return nil, net.ErrClosed
	}
	if socket := u.sockets[family]; socket != nil {
		return socket, nil
	}
	socket, err := net.ListenPacket(family, ":0")
	if err != nil {
		return nil, fmt.Errorf("listen %s target socket: %w", family, err)
	}
	u.sockets[family] = socket
	u.session.goWorker(func() { u.readLoop(family, socket) })
	return socket, nil
}

func (u *udpTarget) write(payload []byte, address *net.UDPAddr, deadline time.Time) error {
	socket, err := u.socket(udpFamily(address))
	if err != nil {
		return err
	}
	if !deadline.IsZero() {
		if err := socket.SetWriteDeadline(deadline); err != nil {
			return err
		}
		defer socket.SetWriteDeadline(time.Time{})
	}
	_, err = socket.WriteTo(payload, address)
	return err
}

func (u *udpTarget) readLoop(family string, socket net.PacketConn) {
	buffer := make([]byte, u.maxSize+1)
	for {
		n, address, err := socket.ReadFrom(buffer)
		if err != nil {
			u.access.Lock()
			closed := u.closed
			u.access.Unlock()
			if !closed {
				u.server.logger.Warn("datagram target read failed", "session", sessionLogID(u.session.id), "family", family, "error", err)
				u.session.close()
			}
			return
		}
		if n > u.maxSize {
			u.server.logger.Debug("oversize UDP response dropped", "session", sessionLogID(u.session.id), "family", family, "size", n)
			continue
		}
		payload := append([]byte(nil), buffer[:n]...)
		if err := u.session.dgram.Send(payload, udpAddressString(address), time.Time{}); err != nil {
			if !errors.Is(err, smp3core.ErrDatagramClosed) {
				u.server.logger.Warn("datagram response enqueue failed", "session", sessionLogID(u.session.id), "error", err)
			}
			return
		}
	}
}

func (u *udpTarget) Close() error {
	u.closeOne.Do(func() {
		u.access.Lock()
		u.closed = true
		sockets := make([]net.PacketConn, 0, len(u.sockets))
		for _, socket := range u.sockets {
			sockets = append(sockets, socket)
		}
		u.sockets = make(map[string]net.PacketConn)
		u.access.Unlock()
		for _, socket := range sockets {
			_ = socket.Close()
		}
	})
	return nil
}

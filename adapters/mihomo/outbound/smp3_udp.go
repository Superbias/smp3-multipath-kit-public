package outbound

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
	C "github.com/metacubex/mihomo/constant"
	M "github.com/metacubex/sing/common/metadata"
)

var errSMP3UDPNotImplemented = errors.New("smp3 udp packet connection is not implemented")
var errSMP3UDPDisabled = errors.New("smp3 udp is disabled")

type smp3DatagramIO interface {
	Send([]byte, string, time.Time) error
	Receive(time.Time) (smp3core.Datagram, error)
	Done() <-chan struct{}
	Close() error
}

type smp3DatagramEngine interface {
	smp3DatagramIO
	AttachLeg(smp3core.LegID, smp3core.DatagramLeg, func(error)) error
}

type smp3UDPSession struct {
	id     smp3core.SessionID
	engine smp3DatagramEngine
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
}

func (s *smp3UDPSession) Send(payload []byte, address string, deadline time.Time) error {
	return s.engine.Send(payload, address, deadline)
}

func (s *smp3UDPSession) Receive(deadline time.Time) (smp3core.Datagram, error) {
	return s.engine.Receive(deadline)
}

func (s *smp3UDPSession) Done() <-chan struct{} { return s.engine.Done() }

func (s *smp3UDPSession) Close() error {
	s.closeOnce.Do(func() {
		_ = s.engine.Close()
		if s.cancel != nil {
			s.cancel()
		}
	})
	return nil
}

// listenPacketContext owns the production UDP lifecycle and delegates the
// dual-leg bootstrap to the B2 session implementation.
func (s *SMP3) listenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	_, raw, err := s.newDualLegUDPPacketConn(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return NewPacketConn(raw, s), nil
}

func newSMP3UDPSessionID() (smp3core.SessionID, error) {
	var id smp3core.SessionID
	_, err := cryptorand.Read(id[:])
	return id, err
}

func writeSMP3DatagramHello(conn io.Writer, sessionID smp3core.SessionID, destination string, password []byte) error {
	var nonce [16]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return err
	}
	return writeSMP3DatagramHelloAt(conn, sessionID, destination, password, time.Now().Unix(), nonce)
}

func writeSMP3DatagramHelloAt(conn io.Writer, sessionID smp3core.SessionID, destination string, password []byte, timestamp int64, nonce [16]byte) error {
	header, dest, mac, err := smp3core.EncodeHelloParts(smp3core.Hello{
		Version:     smp3core.Version5,
		SessionID:   sessionID,
		LegID:       0,
		Mode:        smp3core.ModeDatagram,
		Timestamp:   timestamp,
		Nonce:       nonce,
		Destination: destination,
	}, password)
	if err != nil {
		return err
	}
	buffers := net.Buffers{header, dest, mac}
	_, err = buffers.WriteTo(conn)
	return err
}

type smp3UDPBootstrapDeps struct {
	newEngine  func(smp3core.DatagramConfig) smp3DatagramEngine
	writeHello func(io.Writer, smp3core.SessionID, string, []byte) error
}

func (s *SMP3) newSingleLegUDPPacketConn(ctx context.Context, metadata *C.Metadata) (*smp3UDPSession, *smp3PacketConn, error) {
	return s.newSingleLegUDPPacketConnWith(ctx, metadata, smp3UDPBootstrapDeps{
		newEngine:  func(cfg smp3core.DatagramConfig) smp3DatagramEngine { return smp3core.NewDatagramEngine(cfg) },
		writeHello: writeSMP3DatagramHello,
	})
}

func (s *SMP3) newSingleLegUDPPacketConnWith(ctx context.Context, metadata *C.Metadata, deps smp3UDPBootstrapDeps) (*smp3UDPSession, *smp3PacketConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if metadata == nil || metadata.NetWork != C.UDP || !metadata.Valid() || metadata.DstPort == 0 {
		return nil, nil, fmt.Errorf("smp3: invalid UDP application destination")
	}
	if deps.newEngine == nil || deps.writeHello == nil {
		return nil, nil, errors.New("smp3: incomplete UDP bootstrap dependencies")
	}
	cfg, err := s.datagramConfig()
	if err != nil {
		return nil, nil, err
	}
	sessionID, err := newSMP3UDPSessionID()
	if err != nil {
		return nil, nil, fmt.Errorf("smp3: generate UDP session id: %w", err)
	}
	engine := deps.newEngine(cfg)
	if engine == nil {
		return nil, nil, errors.New("smp3: UDP engine factory returned nil")
	}
	cleanup := func(conn net.Conn) {
		if conn != nil {
			_ = conn.Close()
		}
		_ = engine.Close()
	}
	child, err := s.child(s.option.Legs[0].Proxy)
	if err != nil {
		cleanup(nil)
		return nil, nil, err
	}
	endpoint, err := s.endpointMetadata()
	if err != nil {
		cleanup(nil)
		return nil, nil, err
	}
	conn, err := child.DialContext(ctx, endpoint)
	if err != nil {
		cleanup(conn)
		return nil, nil, err
	}
	if conn == nil {
		cleanup(nil)
		return nil, nil, errors.New("smp3: UDP child returned nil carrier")
	}
	if err := ctx.Err(); err != nil {
		cleanup(conn)
		return nil, nil, err
	}
	if err := deps.writeHello(conn, sessionID, metadata.RemoteAddress(), []byte(s.option.Password)); err != nil {
		cleanup(conn)
		return nil, nil, fmt.Errorf("smp3: UDP HELLO: %w", err)
	}
	if err := ctx.Err(); err != nil {
		cleanup(conn)
		return nil, nil, err
	}
	if err := engine.AttachLeg(0, conn, nil); err != nil {
		cleanup(conn)
		return nil, nil, fmt.Errorf("smp3: attach UDP leg0: %w", err)
	}
	sessionCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	session := &smp3UDPSession{id: sessionID, engine: engine, ctx: sessionCtx, cancel: cancel}
	go func() {
		select {
		case <-engine.Done():
			cancel()
		case <-sessionCtx.Done():
		}
	}()
	return session, newSMP3PacketConn(session), nil
}

type smp3PacketConn struct {
	engine        smp3DatagramIO
	closeOnce     sync.Once
	closed        atomic.Bool
	mu            sync.RWMutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func newSMP3PacketConn(engine smp3DatagramIO) *smp3PacketConn {
	return &smp3PacketConn{engine: engine}
}

func (c *smp3PacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if c.closed.Load() {
		return 0, nil, net.ErrClosed
	}
	if c.engine == nil {
		return 0, nil, errSMP3UDPNotImplemented
	}
	datagram, err := c.engine.Receive(c.readDeadlineValue())
	if err != nil {
		return 0, nil, mapSMP3DatagramError(err)
	}
	if len(p) < len(datagram.Payload) {
		return 0, nil, io.ErrShortBuffer
	}
	addr, err := smp3NetAddr(datagram.Address)
	if err != nil {
		return 0, nil, err
	}
	return copy(p, datagram.Payload), addr, nil
}

func (c *smp3PacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	if c.engine == nil {
		return 0, errSMP3UDPNotImplemented
	}
	destination, err := smp3DatagramAddress(addr)
	if err != nil {
		return 0, err
	}
	deadline := c.writeDeadlineValue()
	err = c.engine.Send(p, destination, deadline)
	if errors.Is(err, smp3core.ErrDatagramTooLarge) {
		return len(p), nil
	}
	if err != nil {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, smp3PacketTimeoutError{}
		}
		return 0, mapSMP3DatagramError(err)
	}
	return len(p), nil
}

func (c *smp3PacketConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.engine != nil {
			_ = c.engine.Close()
		}
	})
	return nil
}

func (*smp3PacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4zero, Port: 0}
}

func (c *smp3PacketConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *smp3PacketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *smp3PacketConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *smp3PacketConn) readDeadlineValue() time.Time {
	c.mu.RLock()
	deadline := c.readDeadline
	c.mu.RUnlock()
	return deadline
}

func (c *smp3PacketConn) writeDeadlineValue() time.Time {
	c.mu.RLock()
	deadline := c.writeDeadline
	c.mu.RUnlock()
	return deadline
}

func smp3DatagramAddress(addr net.Addr) (string, error) {
	if addr == nil {
		return "", errors.New("nil datagram destination")
	}
	destination := M.SocksaddrFromNet(addr)
	destination = destination.Unwrap()
	if !destination.IsValid() || destination.Port == 0 {
		destination = M.ParseSocksaddr(addr.String())
		destination = destination.Unwrap()
	}
	if !destination.IsValid() || destination.Port == 0 {
		return "", fmt.Errorf("invalid datagram destination %q", addr.String())
	}
	return destination.String(), nil
}

func smp3NetAddr(address string) (net.Addr, error) {
	destination := M.ParseSocksaddr(address)
	if !destination.IsValid() || destination.Port == 0 {
		return nil, fmt.Errorf("invalid datagram source address %q", address)
	}
	return destination, nil
}

func mapSMP3DatagramError(err error) error {
	switch {
	case errors.Is(err, smp3core.ErrDatagramClosed):
		return net.ErrClosed
	case errors.Is(err, smp3core.ErrDatagramTimeout):
		return smp3PacketTimeoutError{}
	default:
		return err
	}
}

type smp3PacketTimeoutError struct{}

func (smp3PacketTimeoutError) Error() string   { return "i/o timeout" }
func (smp3PacketTimeoutError) Timeout() bool   { return true }
func (smp3PacketTimeoutError) Temporary() bool { return true }

func (s *SMP3) datagramConfig() (smp3core.DatagramConfig, error) {
	if s == nil || !s.option.UDP.Enabled {
		return smp3core.DatagramConfig{}, errSMP3UDPDisabled
	}
	cfg := smp3core.DatagramConfig{
		Mode:                       smp3core.DatagramAdaptive,
		QueueFrames:                256,
		MaxDatagramSize:            smp3core.MaxDatagramPayload,
		DedupWindow:                4096,
		IdleTimeout:                2 * time.Minute,
		RecoveryTimeout:            15 * time.Second,
		AdaptiveQueueDelay:         120 * time.Millisecond,
		AdaptiveDuplicateThreshold: s.option.UDP.AdaptiveDuplicateThreshold,
		BandwidthMbps:              append([]uint32(nil), s.streamConfig.BandwidthMbps...),
	}
	if s.streamConfig.RecoveryTimeout > 0 {
		cfg.RecoveryTimeout = s.streamConfig.RecoveryTimeout
	}
	switch s.option.UDP.Mode {
	case "", "adaptive":
		cfg.Mode = smp3core.DatagramAdaptive
	case "stripe":
		cfg.Mode = smp3core.DatagramStripe
	case "duplicate":
		cfg.Mode = smp3core.DatagramDuplicate
	default:
		return smp3core.DatagramConfig{}, fmt.Errorf("smp3: udp mode must be stripe, duplicate or adaptive")
	}
	if s.option.UDP.MaxDatagramSize > 0 {
		cfg.MaxDatagramSize = s.option.UDP.MaxDatagramSize
	}
	if cfg.MaxDatagramSize < 512 || cfg.MaxDatagramSize > smp3core.MaxDatagramPayload {
		return smp3core.DatagramConfig{}, fmt.Errorf("smp3: udp max-datagram-size must be between 512 and %d", smp3core.MaxDatagramPayload)
	}
	if s.option.UDP.IdleTimeout != "" {
		idleTimeout, err := parseSMP3Duration(s.option.UDP.IdleTimeout, cfg.IdleTimeout)
		if err != nil {
			return smp3core.DatagramConfig{}, fmt.Errorf("smp3: udp idle-timeout: %w", err)
		}
		cfg.IdleTimeout = idleTimeout
	}
	if cfg.IdleTimeout < 5*time.Second || cfg.IdleTimeout > time.Hour {
		return smp3core.DatagramConfig{}, fmt.Errorf("smp3: udp idle-timeout must be between 5s and 1h")
	}
	if cfg.AdaptiveDuplicateThreshold < 0 || cfg.AdaptiveDuplicateThreshold > cfg.MaxDatagramSize {
		return smp3core.DatagramConfig{}, fmt.Errorf("smp3: invalid udp adaptive-duplicate-threshold")
	}
	return cfg, nil
}

var _ net.PacketConn = (*smp3PacketConn)(nil)

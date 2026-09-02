package client

import (
	"bufio"
	"bytes"
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
)

const socksUDPHeaderSize = 3

var errSocksUDPFragmented = errors.New("fragmented SOCKS5 UDP datagram is not supported")

type socksUDPDatagram struct {
	address string
	payload []byte
}

func parseSocksUDPDatagram(packet []byte) (string, []byte, error) {
	if len(packet) < socksUDPHeaderSize {
		return "", nil, errors.New("short SOCKS5 UDP datagram")
	}
	if packet[0] != 0 || packet[1] != 0 {
		return "", nil, errors.New("invalid SOCKS5 UDP reserved field")
	}
	if packet[2] != 0 {
		return "", nil, errSocksUDPFragmented
	}
	reader := bufio.NewReader(bytes.NewReader(packet[socksUDPHeaderSize:]))
	address, err := decodeSocksAddress(reader, false)
	if err != nil {
		return "", nil, err
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, err
	}
	if len(payload) == 0 {
		return "", nil, errors.New("empty SOCKS5 UDP datagram")
	}
	return address, payload, nil
}

func encodeSocksUDPDatagram(address string, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty SOCKS5 UDP datagram")
	}
	encodedAddress, err := encodeSocksAddress(address, false)
	if err != nil {
		return nil, err
	}
	packet := make([]byte, 0, socksUDPHeaderSize+len(encodedAddress)+len(payload))
	packet = append(packet, 0, 0, 0)
	packet = append(packet, encodedAddress...)
	packet = append(packet, payload...)
	return packet, nil
}

type datagramAssociation struct {
	client   *Client
	control  net.Conn
	controlR io.Reader
	udp      *net.UDPConn
	ctx      context.Context
	cancel   context.CancelFunc

	closed   atomic.Bool
	closeOne sync.Once
	runWG    sync.WaitGroup

	engineMu    sync.Mutex
	engine      *smp3core.DatagramEngine
	sessionID   smp3core.SessionID
	engineEvent chan struct{}

	peerMu sync.Mutex
	peer   *net.UDPAddr

	repairMu  sync.Mutex
	repairing [2]bool
}

func newDatagramAssociation(client *Client, control net.Conn, controlReader io.Reader) (*datagramAssociation, error) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, fmt.Errorf("listen local SOCKS UDP: %w", err)
	}
	ctx, cancel := context.WithCancel(client.ctx)
	return &datagramAssociation{
		client: client, control: control, controlR: controlReader, udp: udp,
		ctx: ctx, cancel: cancel, engineEvent: make(chan struct{}),
	}, nil
}

func (c *Client) handleUDPAssociate(control net.Conn, reader *bufio.Reader) {
	association, err := newDatagramAssociation(c, control, reader)
	if err != nil {
		_ = writeSocksReply(control, socksReplyGeneralFailure, nil)
		_ = control.Close()
		return
	}
	if err := writeSocksReply(control, 0, association.udp.LocalAddr()); err != nil {
		_ = association.Close()
		return
	}
	association.run()
}

func (a *datagramAssociation) run() {
	a.runWG.Add(3)
	go func() { defer a.runWG.Done(); a.readControl() }()
	go func() { defer a.runWG.Done(); a.pumpLocal() }()
	go func() { defer a.runWG.Done(); a.pumpCore() }()
	a.runWG.Wait()
	_ = a.Close()
}

func (a *datagramAssociation) readControl() {
	buffer := make([]byte, 1)
	_, _ = a.controlR.Read(buffer)
	_ = a.Close()
}

func (a *datagramAssociation) pumpLocal() {
	buffer := make([]byte, 64*1024)
	for {
		n, peer, err := a.udp.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		address, payload, err := parseSocksUDPDatagram(buffer[:n])
		if err != nil {
			// Unsupported fragments and malformed datagrams are isolated to the
			// individual UDP packet; the control association remains usable.
			continue
		}
		a.peerMu.Lock()
		a.peer = &net.UDPAddr{IP: append(net.IP(nil), peer.IP...), Port: peer.Port, Zone: peer.Zone}
		a.peerMu.Unlock()
		if err := a.send(payload, address); err != nil && !a.isClosed() {
			continue
		}
	}
}

func (a *datagramAssociation) pumpCore() {
	for {
		engine := a.currentEngine()
		if engine == nil {
			event := a.currentEngineEvent()
			select {
			case <-a.ctx.Done():
				return
			case <-event:
			}
			continue
		}
		datagram, err := engine.Receive(time.Time{})
		if err != nil {
			if errors.Is(err, smp3core.ErrDatagramClosed) && !a.isClosed() {
				event := a.currentEngineEvent()
				select {
				case <-a.ctx.Done():
					return
				case <-event:
				}
				continue
			}
			return
		}
		packet, err := encodeSocksUDPDatagram(datagram.Address, datagram.Payload)
		if err != nil {
			continue
		}
		a.peerMu.Lock()
		peer := a.peer
		if peer != nil {
			peer = &net.UDPAddr{IP: append(net.IP(nil), peer.IP...), Port: peer.Port, Zone: peer.Zone}
		}
		a.peerMu.Unlock()
		if peer == nil {
			continue
		}
		_, _ = a.udp.WriteToUDP(packet, peer)
	}
}

func (a *datagramAssociation) send(payload []byte, address string) error {
	for attempt := 0; attempt < 2; attempt++ {
		engine, err := a.ensureEngine()
		if err != nil {
			return err
		}
		err = engine.Send(payload, address, time.Time{})
		if !errors.Is(err, smp3core.ErrDatagramClosed) {
			return err
		}
	}
	return smp3core.ErrDatagramClosed
}

func (a *datagramAssociation) ensureEngine() (*smp3core.DatagramEngine, error) {
	a.engineMu.Lock()
	defer a.engineMu.Unlock()
	if a.isClosed() {
		return nil, smp3core.ErrDatagramClosed
	}
	if a.engine != nil {
		select {
		case <-a.engine.Done():
		default:
			return a.engine, nil
		}
	}
	sid, err := newSessionID()
	if err != nil {
		return nil, err
	}
	var engine *smp3core.DatagramEngine
	config := a.datagramConfig(func(id smp3core.LegID, _ error) {
		if engine != nil {
			a.scheduleRepair(engine, uint8(id))
		}
	})
	engine = smp3core.NewDatagramEngine(config)
	conn, err := a.dialLeg(smp3core.LegID(0), sid)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	if a.isClosed() {
		_ = conn.Close()
		_ = engine.Close()
		return nil, smp3core.ErrDatagramClosed
	}
	if err := engine.AttachLeg(0, conn, nil); err != nil {
		_ = conn.Close()
		_ = engine.Close()
		return nil, err
	}
	if a.isClosed() {
		_ = engine.Close()
		return nil, smp3core.ErrDatagramClosed
	}
	old := a.engine
	a.engine = engine
	a.sessionID = sid
	oldEvent := a.engineEvent
	a.engineEvent = make(chan struct{})
	close(oldEvent)
	if old != nil {
		_ = old.Close()
	}
	go a.bootstrapLeg(engine, sid, 1)
	return engine, nil
}

func (a *datagramAssociation) datagramConfig(onLegDown func(smp3core.LegID, error)) smp3core.DatagramConfig {
	options := a.client.cfg.SMP3.UDP
	mode := smp3core.DatagramAdaptive
	switch options.Mode {
	case "stripe":
		mode = smp3core.DatagramStripe
	case "duplicate":
		mode = smp3core.DatagramDuplicate
	}
	return smp3core.DatagramConfig{
		Mode:                       mode,
		QueueFrames:                options.QueueFrames,
		MaxDatagramSize:            options.MaxDatagramSize,
		DedupWindow:                options.DedupWindow,
		IdleTimeout:                options.IdleTimeout.Time(),
		RecoveryTimeout:            15 * time.Second,
		AdaptiveQueueDelay:         options.AdaptiveQueueDelay.Time(),
		AdaptiveDuplicateThreshold: options.AdaptiveDuplicateThreshold,
		BandwidthMbps:              append([]uint32(nil), a.client.cfg.SMP3.Stream.BandwidthMbps...),
		OnLegDown:                  onLegDown,
	}
}

func (a *datagramAssociation) dialLeg(id smp3core.LegID, sid smp3core.SessionID) (net.Conn, error) {
	if id > 1 {
		return nil, errors.New("invalid datagram leg")
	}
	route := a.client.cfg.SMP3.Routes.Leg0
	if id == 1 {
		route = a.client.cfg.SMP3.Routes.Leg1
	}
	routes := []string{route}
	if id == 1 && a.client.cfg.SMP3.Routes.Leg1Fallback != "" {
		routes = append(routes, a.client.cfg.SMP3.Routes.Leg1Fallback)
	}
	var causes []error
	for _, endpoint := range routes {
		conn, err := dialUpstream(a.ctx, a.client.cfg.UpstreamSocks, endpoint)
		if err != nil {
			if ctxErr := a.ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			causes = append(causes, fmt.Errorf("%s: %w", endpoint, err))
			continue
		}
		hello, err := writeDatagramHello(conn, sid, id, a.client.cfg.SMP3.Password)
		if err != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s HELLO: %w", endpoint, err))
			continue
		}
		if err := readSidecarReadyV1(conn, hello, []byte(a.client.cfg.SMP3.Password), a.client.cfg.SMP3.CarrierReadyTimeout.Time()); err != nil {
			_ = conn.Close()
			causes = append(causes, fmt.Errorf("%s READY: %w", endpoint, err))
			continue
		}
		if ctxErr := a.ctx.Err(); ctxErr != nil {
			_ = conn.Close()
			return nil, ctxErr
		}
		return conn, nil
	}
	return nil, errors.Join(causes...)
}

func writeDatagramHello(conn io.Writer, sid smp3core.SessionID, leg smp3core.LegID, password string) (smp3core.Hello, error) {
	var nonce [16]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return smp3core.Hello{}, err
	}
	hello := smp3core.Hello{
		Version:     smp3core.Version5,
		SessionID:   sid,
		LegID:       leg,
		Mode:        smp3core.ModeDatagram,
		Timestamp:   time.Now().Unix(),
		Nonce:       nonce,
		Destination: "0.0.0.0:1",
	}
	header, destination, mac, err := smp3core.EncodeHelloParts(hello, []byte(password))
	if err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, header); err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, destination); err != nil {
		return smp3core.Hello{}, err
	}
	if err := writeAll(conn, mac); err != nil {
		return smp3core.Hello{}, err
	}
	return hello, nil
}

func (a *datagramAssociation) bootstrapLeg(engine *smp3core.DatagramEngine, sid smp3core.SessionID, id uint8) {
	if a.isClosed() {
		return
	}
	conn, err := a.dialLeg(smp3core.LegID(id), sid)
	if err != nil {
		a.scheduleRepair(engine, id)
		return
	}
	if a.isClosed() || a.currentEngine() != engine {
		_ = conn.Close()
		return
	}
	if err := engine.AttachLeg(smp3core.LegID(id), conn, nil); err != nil {
		_ = conn.Close()
		a.scheduleRepair(engine, id)
	}
}

func (a *datagramAssociation) scheduleRepair(engine *smp3core.DatagramEngine, id uint8) {
	if id > 1 || a.isClosed() || engine == nil || a.currentEngine() != engine {
		return
	}
	a.repairMu.Lock()
	if a.repairing[id] {
		a.repairMu.Unlock()
		return
	}
	a.repairing[id] = true
	a.repairMu.Unlock()
	go func() {
		defer func() {
			a.repairMu.Lock()
			a.repairing[id] = false
			a.repairMu.Unlock()
		}()
		for {
			if a.isClosed() || a.currentEngine() != engine {
				return
			}
			select {
			case <-a.ctx.Done():
				return
			default:
			}
			if engineDone(engine) {
				return
			}
			if engine.HasLeg(smp3core.LegID(id)) {
				return
			}
			conn, err := a.dialLeg(smp3core.LegID(id), a.currentSessionID())
			if err == nil {
				if a.isClosed() || a.currentEngine() != engine || engineDone(engine) {
					_ = conn.Close()
					return
				}
				if err := engine.AttachLeg(smp3core.LegID(id), conn, nil); err == nil {
					return
				}
				_ = conn.Close()
			}
			timer := time.NewTimer(a.client.cfg.SMP3.Stream.RedialInterval.Time())
			select {
			case <-a.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func engineDone(engine *smp3core.DatagramEngine) bool {
	select {
	case <-engine.Done():
		return true
	default:
		return false
	}
}

func (a *datagramAssociation) currentEngine() *smp3core.DatagramEngine {
	a.engineMu.Lock()
	engine := a.engine
	a.engineMu.Unlock()
	return engine
}

func (a *datagramAssociation) currentEngineEvent() <-chan struct{} {
	a.engineMu.Lock()
	event := a.engineEvent
	a.engineMu.Unlock()
	return event
}

func (a *datagramAssociation) currentSessionID() smp3core.SessionID {
	a.engineMu.Lock()
	sid := a.sessionID
	a.engineMu.Unlock()
	return sid
}

func (a *datagramAssociation) isClosed() bool { return a.closed.Load() }

func (a *datagramAssociation) Close() error {
	a.closeOne.Do(func() {
		a.closed.Store(true)
		a.cancel()
		a.engineMu.Lock()
		engine := a.engine
		event := a.engineEvent
		a.engineEvent = make(chan struct{})
		close(event)
		a.engineMu.Unlock()
		if engine != nil {
			_ = engine.Close()
		}
		_ = a.udp.Close()
		_ = a.control.Close()
	})
	return nil
}

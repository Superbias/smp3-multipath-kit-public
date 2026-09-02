package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var ErrClientClosed = errors.New("smp3 sidecar client closed")

type Client struct {
	cfg Config

	mu       sync.Mutex
	listener net.Listener
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func New(cfg Config) (*Client, error) {
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{cfg: cfg, ctx: ctx, cancel: cancel, conns: make(map[net.Conn]struct{})}, nil
}

func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClientClosed
	}
	if c.listener != nil {
		return errors.New("sidecar client already started")
	}
	listener, err := net.Listen("tcp", c.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen sidecar SOCKS5: %w", err)
	}
	c.listener = listener
	c.wg.Add(1)
	go c.acceptLoop(listener)
	return nil
}

func (c *Client) Addr() net.Addr {
	c.mu.Lock()
	listener := c.listener
	c.mu.Unlock()
	if listener == nil {
		return nil
	}
	return listener.Addr()
}

func (c *Client) Wait() error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClientClosed
	}
	<-c.ctx.Done()
	return ErrClientClosed
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	listener := c.listener
	conns := make([]net.Conn, 0, len(c.conns))
	for conn := range c.conns {
		conns = append(conns, conn)
	}
	c.mu.Unlock()
	c.cancel()
	if listener != nil {
		_ = listener.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	c.wg.Wait()
	return nil
}

func (c *Client) acceptLoop(listener net.Listener) {
	defer c.wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if c.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		c.trackConn(conn)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer c.untrackConn(conn)
			c.handleSOCKS(conn)
		}()
	}
}

func (c *Client) handleSOCKS(conn net.Conn) {
	reader, command, destination, err := serverHandshake(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	switch command {
	case socksCommandConnect:
		c.handleConnect(conn, reader, destination)
	case socksCommandUDPAssociate:
		c.handleUDPAssociate(conn, reader)
	case socksCommandBind:
		_ = writeSocksReply(conn, socksReplyCommandNotSupported, nil)
		_ = conn.Close()
	default:
		_ = writeSocksReply(conn, socksReplyCommandNotSupported, nil)
		_ = conn.Close()
	}
}

func serverHandshake(conn net.Conn) (*bufio.Reader, byte, string, error) {
	reader := bufio.NewReader(conn)
	version, err := reader.ReadByte()
	if err != nil {
		return nil, 0, "", err
	}
	if version != socksVersion5 {
		return nil, 0, "", errors.New("unsupported SOCKS version")
	}
	methodCount, err := reader.ReadByte()
	if err != nil {
		return nil, 0, "", err
	}
	methods := make([]byte, methodCount)
	if _, err := io.ReadFull(reader, methods); err != nil {
		return nil, 0, "", err
	}
	foundNoAuth := false
	for _, method := range methods {
		if method == socksNoAuth {
			foundNoAuth = true
			break
		}
	}
	if !foundNoAuth {
		_ = writeAll(conn, []byte{socksVersion5, 0xff})
		return nil, 0, "", errors.New("local SOCKS5 requires no authentication")
	}
	if err := writeAll(conn, []byte{socksVersion5, socksNoAuth}); err != nil {
		return nil, 0, "", err
	}
	var request [3]byte
	if _, err := io.ReadFull(reader, request[:]); err != nil {
		return nil, 0, "", err
	}
	if request[0] != socksVersion5 || request[2] != 0 {
		return nil, 0, "", errors.New("invalid SOCKS5 request")
	}
	destination, err := decodeSocksAddress(reader, true)
	if err != nil {
		return nil, 0, "", err
	}
	return reader, request[1], destination, nil
}

func writeSocksReply(w io.Writer, reply byte, address net.Addr) error {
	encoded := []byte{1, 0, 0, 0, 0, 0, 0}
	if address != nil {
		if value, err := encodeSocksAddress(address.String(), true); err == nil {
			encoded = value
		}
	}
	return writeAll(w, append([]byte{socksVersion5, reply, 0}, encoded...))
}

func (c *Client) handleConnect(conn net.Conn, reader *bufio.Reader, destination string) {
	if err := validateEndpoint(destination, "SOCKS CONNECT destination", false); err != nil {
		_ = writeSocksReply(conn, socksReplyGeneralFailure, nil)
		_ = conn.Close()
		return
	}
	session, err := newStreamSession(c, destination)
	if err != nil {
		_ = writeSocksReply(conn, socksReplyGeneralFailure, nil)
		_ = conn.Close()
		return
	}
	if err := writeSocksReply(conn, 0, nil); err != nil {
		_ = session.Close()
		_ = conn.Close()
		return
	}
	session.run(conn, reader)
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	return closed
}

func (c *Client) trackConn(conn net.Conn) {
	c.mu.Lock()
	c.conns[conn] = struct{}{}
	c.mu.Unlock()
}

func (c *Client) untrackConn(conn net.Conn) {
	c.mu.Lock()
	delete(c.conns, conn)
	c.mu.Unlock()
}

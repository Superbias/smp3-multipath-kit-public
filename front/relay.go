package front

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("SMP3 TCP front relay closed")

type Relay struct {
	cfg            Config
	logger         *slog.Logger
	allowedSources []*net.IPNet
	ctx            context.Context
	cancel         context.CancelFunc

	mu       sync.Mutex
	listener net.Listener
	active   map[net.Conn]struct{}
	clients  map[net.Conn]struct{}
	closed   bool
	accepted int
	wg       sync.WaitGroup
}

func New(cfg Config, logger *slog.Logger) (*Relay, error) {
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	relay := &Relay{cfg: cfg, logger: logger, ctx: ctx, cancel: cancel, active: make(map[net.Conn]struct{}), clients: make(map[net.Conn]struct{})}
	for _, source := range cfg.AllowedSources {
		_, cidr, _ := net.ParseCIDR(source)
		relay.allowedSources = append(relay.allowedSources, cidr)
	}
	return relay, nil
}

func (r *Relay) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if r.listener != nil {
		return errors.New("front relay already started")
	}
	listener, err := net.Listen("tcp", r.cfg.Listen)
	if err != nil {
		return err
	}
	r.listener = listener
	r.wg.Add(1)
	go r.acceptLoop(listener)
	r.logger.Info("SMP3 TCP front relay listening", "address", listener.Addr().String(), "landing_target", r.cfg.LandingTarget)
	return nil
}

func (r *Relay) Addr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener == nil {
		return nil
	}
	return r.listener.Addr()
}

func (r *Relay) ActiveConnections() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accepted
}

func (r *Relay) acceptLoop(listener net.Listener) {
	defer r.wg.Done()
	for {
		client, err := listener.Accept()
		if err != nil {
			if r.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			r.logger.Error("front TCP accept failed", "error", err)
			continue
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = client.Close()
			return
		}
		if !r.sourceAllowed(client.RemoteAddr()) {
			r.mu.Unlock()
			r.logger.Debug("front rejected client outside allowed_sources", "remote", client.RemoteAddr().String())
			_ = client.Close()
			continue
		}
		if r.accepted >= r.cfg.MaxConnections {
			r.mu.Unlock()
			r.logger.Debug("front connection limit reached; rejecting connection")
			_ = client.Close()
			continue
		}
		r.accepted++
		r.active[client] = struct{}{}
		r.clients[client] = struct{}{}
		r.wg.Add(1)
		r.mu.Unlock()
		go r.handle(client)
	}
}

func (r *Relay) sourceAllowed(address net.Addr) bool {
	if len(r.allowedSources) == 0 {
		return true
	}
	tcp, ok := address.(*net.TCPAddr)
	if !ok || tcp.IP == nil {
		return false
	}
	for _, network := range r.allowedSources {
		if network.Contains(tcp.IP) {
			return true
		}
	}
	return false
}

func (r *Relay) handle(client net.Conn) {
	defer r.wg.Done()
	defer r.removeAndClose(client)
	dialCtx, cancel := context.WithTimeout(r.ctx, r.cfg.ConnectTimeout.Time())
	defer cancel()
	landing, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", r.cfg.LandingTarget)
	if err != nil {
		r.logger.Warn("front failed to connect to Landing", "client", client.RemoteAddr().String(), "error", err)
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = landing.Close()
		return
	}
	r.active[landing] = struct{}{}
	r.mu.Unlock()
	defer r.removeAndClose(landing)

	state := &activityState{}
	clientSide := activityConn{Conn: client, state: state}
	landingSide := activityConn{Conn: landing, state: state}
	copyDone := make(chan error, 2)
	go func() { copyDone <- copyHalfClose(landingSide, clientSide, r.cfg.BufferBytes) }()
	go func() { copyDone <- copyHalfClose(clientSide, landingSide, r.cfg.BufferBytes) }()
	idleStop := make(chan struct{})
	if r.cfg.IdleTimeout.Time() > 0 {
		go r.watchIdle(client, landing, state, idleStop)
	}
	first := <-copyDone
	if first != nil {
		r.logger.Debug("front relay direction ended", "error", first)
		_ = client.Close()
		_ = landing.Close()
	}
	second := <-copyDone
	close(idleStop)
	if second != nil {
		r.logger.Debug("front relay reverse direction ended", "error", second)
	}
}

type activityState struct{ last atomic.Int64 }

func (s *activityState) touch() { s.last.Store(time.Now().UnixNano()) }

type activityConn struct {
	net.Conn
	state *activityState
}

func (c activityConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.state.touch()
	}
	return n, err
}

func (c activityConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.state.touch()
	}
	return n, err
}

// Preserve TCP half-close through the activity wrapper. net.Conn itself does
// not declare CloseWrite, so embedding net.Conn alone hides that capability.
func (c activityConn) CloseWrite() error {
	if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	return c.Conn.Close()
}

func copyHalfClose(dst, src net.Conn, bufferBytes int) error {
	buffer := make([]byte, bufferBytes)
	_, err := io.CopyBuffer(dst, src, buffer)
	if err == nil {
		if closer, ok := dst.(interface{ CloseWrite() error }); ok {
			return closer.CloseWrite()
		}
		return dst.Close()
	}
	return err
}

func (r *Relay) watchIdle(client, landing net.Conn, state *activityState, stop <-chan struct{}) {
	state.touch()
	period := r.cfg.IdleTimeout.Time() / 4
	if period < 100*time.Millisecond {
		period = 100 * time.Millisecond
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-stop:
			return
		case now := <-ticker.C:
			last := time.Unix(0, state.last.Load())
			if now.Sub(last) >= r.cfg.IdleTimeout.Time() {
				r.logger.Debug("front idle timeout reached", "client", client.RemoteAddr().String())
				_ = client.Close()
				_ = landing.Close()
				return
			}
		}
	}
}

func (r *Relay) removeAndClose(conn net.Conn) {
	_ = conn.Close()
	r.mu.Lock()
	if _, exists := r.active[conn]; exists {
		delete(r.active, conn)
		if _, isClient := r.clients[conn]; isClient {
			delete(r.clients, conn)
			if r.accepted > 0 {
				r.accepted--
			}
		}
	}
	r.mu.Unlock()
}

func (r *Relay) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *Relay) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.cancel()
		if r.listener != nil {
			_ = r.listener.Close()
		}
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		connections := make([]net.Conn, 0, len(r.active))
		for conn := range r.active {
			connections = append(connections, conn)
		}
		r.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		<-done
		return ctx.Err()
	}
}

func (r *Relay) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GracefulShutdownTimeout.Time())
	defer cancel()
	return r.Shutdown(ctx)
}

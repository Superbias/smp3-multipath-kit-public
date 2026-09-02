package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	serverpkg "github.com/Superbias/smp3-multipath-kit-public/server"
	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func TestDecodeSocksAddressSupportsIPv4IPv6AndDomain(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
		want string
	}{
		{name: "ipv4", wire: append([]byte{1, 192, 0, 2, 7, 0, 53}, nil...), want: "192.0.2.7:53"},
		{name: "ipv6", wire: append([]byte{4}, append(net.ParseIP("2001:db8::7").To16(), 0, 53)...), want: "[2001:db8::7]:53"},
		{name: "domain", wire: append([]byte{3, 11}, append([]byte("example.com"), 0, 53)...), want: "example.com:53"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeSocksAddress(bufio.NewReader(bytesReader(test.wire)), false)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("address = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDialUpstreamSocks5Connect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		var greeting [2]byte
		if _, err := io.ReadFull(reader, greeting[:]); err != nil {
			serverDone <- err
			return
		}
		methods := make([]byte, int(greeting[1]))
		if _, err := io.ReadFull(reader, methods); err != nil {
			serverDone <- err
			return
		}
		if _, err := conn.Write([]byte{5, 0}); err != nil {
			serverDone <- err
			return
		}
		request := make([]byte, 4)
		if _, err := io.ReadFull(reader, request); err != nil {
			serverDone <- err
			return
		}
		if request[0] != 5 || request[1] != 1 || request[3] != 3 {
			serverDone <- errors.New("unexpected SOCKS request")
			return
		}
		length, err := reader.ReadByte()
		if err != nil {
			serverDone <- err
			return
		}
		domain := make([]byte, length)
		if _, err := io.ReadFull(reader, domain); err != nil {
			serverDone <- err
			return
		}
		var port [2]byte
		if _, err := io.ReadFull(reader, port[:]); err != nil {
			serverDone <- err
			return
		}
		if string(domain) != "example.com" || binary.BigEndian.Uint16(port[:]) != 443 {
			serverDone <- errors.New("unexpected SOCKS destination")
			return
		}
		_, err = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1})
		serverDone <- err
	}()

	conn, err := dialUpstream(context.Background(), UpstreamSocksOptions{Address: listener.Addr().String()}, "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestDialUpstreamConnectTimeoutClosesHangingRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestReceived := make(chan struct{})
	connectionClosed := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		reader := bufio.NewReader(conn)
		var greeting [2]byte
		if _, err := io.ReadFull(reader, greeting[:]); err != nil {
			conn.Close()
			return
		}
		methods := make([]byte, greeting[1])
		if _, err := io.ReadFull(reader, methods); err != nil {
			conn.Close()
			return
		}
		if err := writeAll(conn, []byte{5, 0}); err != nil {
			conn.Close()
			return
		}
		var request [4]byte
		if _, err := io.ReadFull(reader, request[:]); err != nil {
			conn.Close()
			return
		}
		if _, err := decodeSocksAddressWithType(reader, request[3]); err != nil {
			conn.Close()
			return
		}
		close(requestReceived)
		_, _ = io.ReadAll(reader)
		close(connectionClosed)
	}()

	started := time.Now()
	result := make(chan error, 1)
	go func() {
		_, err := dialUpstream(context.Background(), UpstreamSocksOptions{
			Address:        listener.Addr().String(),
			ConnectTimeout: Duration(200 * time.Millisecond),
		}, "example.com:443")
		result <- err
	}()
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("upstream CONNECT request was not received")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("hanging upstream CONNECT unexpectedly succeeded")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("upstream CONNECT exceeded bounded timeout: %s", time.Since(started))
	}
	select {
	case <-connectionClosed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("upstream SOCKS connection remained open after timeout")
	}
}

func TestSocksLocalBindIsRejected(t *testing.T) {
	client, err := New(validTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	conn, err := net.Dial("tcp", client.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != string([]byte{5, 0}) {
		t.Fatalf("method response = %v", response)
	}
	if _, err := conn.Write([]byte{5, 2, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	result := make([]byte, 10)
	if _, err := io.ReadFull(conn, result); err != nil {
		t.Fatal(err)
	}
	if result[1] != socksCommandNotSupported {
		t.Fatalf("BIND reply = %d", result[1])
	}
}

func TestSocksLocalUDPAssociateReturnsLoopbackEndpoint(t *testing.T) {
	client, err := New(validTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	conn, err := net.DialTimeout("tcp", client.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(conn, method); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if reply[1] != 0 || reply[3] != 1 || reply[4] != 127 || reply[5] != 0 || reply[6] != 0 || reply[7] != 1 {
		t.Fatalf("UDP associate reply = %v", reply)
	}
	if binary.BigEndian.Uint16(reply[8:]) == 0 {
		t.Fatal("UDP associate returned port zero")
	}
}

func TestSocksLocalConnectRoundTripsThroughStandaloneServer(t *testing.T) {
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = false
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	serverAddresses := standalone.ListenerAddrs()
	if len(serverAddresses) != 2 {
		t.Fatalf("server listeners = %v", serverAddresses)
	}

	forwarder := newSOCKSForwarder(t)
	defer forwarder.Close()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = forwarder.Addr().String()
	cfg.SMP3.Routes.Leg0 = serverAddresses[1]
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.Stream.SchedulerMode = "static"
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	defer instance.Close()

	conn, err := net.Dial("tcp", instance.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := socksConnect(conn, echoListener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	payload := []byte("sidecar stream round trip")
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(conn, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("echo = %q, want %q", got, payload)
	}
}

func TestSocksLocalConnectActivatesSecondStreamLeg(t *testing.T) {
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	serverConfig := serverpkg.DefaultConfig()
	serverConfig.Listen = "127.0.0.1:0"
	serverConfig.SidecarListeners = []string{"127.0.0.1:0", "127.0.0.1:0"}
	serverConfig.Password = "test-password"
	serverConfig.UDP.Enabled = false
	standalone, err := serverpkg.New(serverConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Start(); err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	serverAddresses := standalone.ListenerAddrs()
	if len(serverAddresses) != 3 {
		t.Fatalf("server listeners = %v", serverAddresses)
	}

	forwarder := newSOCKSForwarder(t)
	defer forwarder.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = forwarder.Addr().String()
	cfg.SMP3.Routes.Leg0 = serverAddresses[1]
	cfg.SMP3.Routes.Leg1 = serverAddresses[2]
	cfg.SMP3.Stream.ActivationThresholdMbps = 1
	cfg.SMP3.Stream.ActivationWindow = Duration(20 * time.Millisecond)
	cfg.SMP3.Stream.ChunkSize = 1024
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	defer instance.Close()

	conn, err := net.Dial("tcp", instance.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := socksConnect(conn, echoListener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(conn, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("stream echo payload mismatch")
	}
	targets := forwarder.waitForTargets(t, 2)
	seen := map[string]bool{}
	for _, target := range targets {
		seen[target] = true
	}
	if !seen[serverAddresses[1]] || !seen[serverAddresses[2]] {
		t.Fatalf("targets = %v, want both %v", targets, serverAddresses)
	}
}

func TestStreamLegUsesConfiguredFallbackAfterPrimaryFailure(t *testing.T) {
	fallback, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	helloReceived := make(chan error, 1)
	go func() {
		conn, err := fallback.Accept()
		if err != nil {
			helloReceived <- err
			return
		}
		defer conn.Close()
		hello, err := smp3core.ReadHelloAt(conn, []byte("test-password"), time.Now())
		if err == nil {
			var ready []byte
			ready, err = encodeSidecarReadyV1(hello, []byte("test-password"))
			if err == nil {
				err = writeAll(conn, ready)
			}
		}
		helloReceived <- err
	}()

	forwarder := newSOCKSForwarder(t)
	defer forwarder.Close()
	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = forwarder.Addr().String()
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.Routes.Leg1Fallback = fallback.Addr().String()
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	session := &streamSession{client: instance, id: id, destination: "example.com:443", ctx: instance.ctx}
	conn, err := session.dialLeg(1)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if err := <-helloReceived; err != nil {
		t.Fatal(err)
	}
	targets := forwarder.waitForTargets(t, 2)
	if targets[0] != "127.0.0.1:1" || targets[1] != fallback.Addr().String() {
		t.Fatalf("fallback target order = %v", targets)
	}
}

func TestStreamLegTimeoutFallsBackWithSameSessionID(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	primaryReceived := make(chan struct{})
	primaryClosed := make(chan struct{})
	fallbackReceived := make(chan struct{})
	helloResult := make(chan struct {
		hello smp3core.Hello
		err   error
	}, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			var greeting [2]byte
			if _, err := io.ReadFull(reader, greeting[:]); err != nil {
				conn.Close()
				return
			}
			methods := make([]byte, greeting[1])
			if _, err := io.ReadFull(reader, methods); err != nil {
				conn.Close()
				return
			}
			if err := writeAll(conn, []byte{5, 0}); err != nil {
				conn.Close()
				return
			}
			var request [4]byte
			if _, err := io.ReadFull(reader, request[:]); err != nil {
				conn.Close()
				return
			}
			target, err := decodeSocksAddressWithType(reader, request[3])
			if err != nil {
				conn.Close()
				return
			}
			if target == "127.0.0.1:1" {
				close(primaryReceived)
				_, _ = io.ReadAll(reader)
				close(primaryClosed)
				continue
			}
			if target != "127.0.0.1:2" {
				conn.Close()
				return
			}
			close(fallbackReceived)
			if err := writeAll(conn, []byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
				helloResult <- struct {
					hello smp3core.Hello
					err   error
				}{err: err}
				conn.Close()
				return
			}
			hello, err := smp3core.ReadHelloAt(reader, []byte("test-password"), time.Now())
			if err == nil {
				var ready []byte
				ready, err = encodeSidecarReadyV1(hello, []byte("test-password"))
				if err == nil {
					err = writeAll(conn, ready)
				}
			}
			helloResult <- struct {
				hello smp3core.Hello
				err   error
			}{hello: hello, err: err}
			conn.Close()
			return
		}
	}()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = listener.Addr().String()
	cfg.UpstreamSocks.ConnectTimeout = Duration(200 * time.Millisecond)
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.Routes.Leg1Fallback = "127.0.0.1:2"
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	session := &streamSession{client: instance, id: id, destination: "example.com:443", ctx: instance.ctx}
	started := time.Now()
	conn, err := session.dialLeg(1)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("fallback took too long: %s", elapsed)
	}
	select {
	case <-primaryReceived:
	case <-time.After(time.Second):
		t.Fatal("primary SOCKS request was not observed")
	}
	select {
	case <-primaryClosed:
	case <-time.After(time.Second):
		t.Fatal("primary SOCKS connection was not closed after timeout")
	}
	select {
	case <-fallbackReceived:
	case <-time.After(time.Second):
		t.Fatal("fallback SOCKS request was not observed")
	}
	result := <-helloResult
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.hello.SessionID != id || result.hello.LegID != 1 || result.hello.Mode != smp3core.ModeStream {
		t.Fatalf("fallback HELLO = %+v, want same session leg1 stream", result.hello)
	}
}

func TestStreamLegFalseSuccessReadyTimeoutFallsBack(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	primaryReceived := make(chan struct{})
	primaryClosed := make(chan struct{})
	fallbackReceived := make(chan struct{})
	helloResult := make(chan struct {
		hello smp3core.Hello
		err   error
	}, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			var greeting [2]byte
			if _, err := io.ReadFull(reader, greeting[:]); err != nil {
				conn.Close()
				return
			}
			methods := make([]byte, greeting[1])
			if _, err := io.ReadFull(reader, methods); err != nil {
				conn.Close()
				return
			}
			if err := writeAll(conn, []byte{5, 0}); err != nil {
				conn.Close()
				return
			}
			var request [4]byte
			if _, err := io.ReadFull(reader, request[:]); err != nil {
				conn.Close()
				return
			}
			target, err := decodeSocksAddressWithType(reader, request[3])
			if err != nil {
				conn.Close()
				return
			}
			if target == "127.0.0.1:1" {
				if err := writeAll(conn, []byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
					conn.Close()
					return
				}
				close(primaryReceived)
				_, _ = io.ReadAll(reader)
				close(primaryClosed)
				continue
			}
			if target != "127.0.0.1:2" {
				conn.Close()
				return
			}
			close(fallbackReceived)
			if err := writeAll(conn, []byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
				helloResult <- struct {
					hello smp3core.Hello
					err   error
				}{err: err}
				conn.Close()
				return
			}
			hello, err := smp3core.ReadHelloAt(reader, []byte("test-password"), time.Now())
			if err == nil {
				var ready []byte
				ready, err = encodeSidecarReadyV1(hello, []byte("test-password"))
				if err == nil {
					err = writeAll(conn, ready)
				}
			}
			helloResult <- struct {
				hello smp3core.Hello
				err   error
			}{hello: hello, err: err}
			conn.Close()
			return
		}
	}()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = listener.Addr().String()
	cfg.UpstreamSocks.ConnectTimeout = Duration(200 * time.Millisecond)
	cfg.SMP3.CarrierReadyTimeout = Duration(200 * time.Millisecond)
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.Routes.Leg1Fallback = "127.0.0.1:2"
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	session := &streamSession{client: instance, id: id, destination: "example.com:443", ctx: instance.ctx}
	started := time.Now()
	conn, err := session.dialLeg(1)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("false-success fallback took too long: %s", elapsed)
	}
	select {
	case <-primaryReceived:
	case <-time.After(time.Second):
		t.Fatal("false-success primary was not observed")
	}
	select {
	case <-primaryClosed:
	case <-time.After(time.Second):
		t.Fatal("false-success primary was not closed after READY timeout")
	}
	select {
	case <-fallbackReceived:
	case <-time.After(time.Second):
		t.Fatal("false-success fallback was not observed")
	}
	result := <-helloResult
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.hello.SessionID != id || result.hello.LegID != 1 || result.hello.Mode != smp3core.ModeStream {
		t.Fatalf("fallback HELLO = %+v, want same session leg1 stream", result.hello)
	}
}

func TestStreamLegParentCancellationDoesNotTryFallback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	primaryReceived := make(chan struct{})
	primaryClosed := make(chan struct{})
	fallbackReceived := make(chan struct{})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			var greeting [2]byte
			if _, err := io.ReadFull(reader, greeting[:]); err != nil {
				conn.Close()
				return
			}
			methods := make([]byte, greeting[1])
			if _, err := io.ReadFull(reader, methods); err != nil {
				conn.Close()
				return
			}
			if err := writeAll(conn, []byte{5, 0}); err != nil {
				conn.Close()
				return
			}
			var request [4]byte
			if _, err := io.ReadFull(reader, request[:]); err != nil {
				conn.Close()
				return
			}
			target, err := decodeSocksAddressWithType(reader, request[3])
			if err != nil {
				conn.Close()
				return
			}
			if target != "127.0.0.1:1" {
				close(fallbackReceived)
				conn.Close()
				return
			}
			close(primaryReceived)
			_, _ = io.ReadAll(reader)
			close(primaryClosed)
			conn.Close()
		}
	}()

	cfg := validTestConfig()
	cfg.UpstreamSocks.Address = listener.Addr().String()
	cfg.UpstreamSocks.ConnectTimeout = Duration(5 * time.Second)
	cfg.SMP3.Routes.Leg1 = "127.0.0.1:1"
	cfg.SMP3.Routes.Leg1Fallback = "127.0.0.1:2"
	instance, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	ctx, cancel := context.WithCancel(instance.ctx)
	id, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	session := &streamSession{client: instance, id: id, destination: "example.com:443", ctx: ctx, cancel: cancel}
	result := make(chan error, 1)
	go func() {
		conn, err := session.dialLeg(1)
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	select {
	case <-primaryReceived:
	case <-time.After(time.Second):
		t.Fatal("primary SOCKS request was not observed")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled carrier dial unexpectedly succeeded")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("parent cancellation did not abort carrier dial")
	}
	select {
	case <-primaryClosed:
	case <-time.After(time.Second):
		t.Fatal("primary SOCKS connection was not closed after cancellation")
	}
	select {
	case <-fallbackReceived:
		t.Fatal("fallback was attempted after parent cancellation")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHundredHangingUpstreamConnectsCloseAllSockets(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	const attempts = 100
	var accepted atomic.Int32
	var closed atomic.Int32
	go func() {
		for accepted.Load() < attempts {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				defer closed.Add(1)
				defer conn.Close()
				reader := bufio.NewReader(conn)
				var greeting [2]byte
				if _, err := io.ReadFull(reader, greeting[:]); err != nil {
					return
				}
				methods := make([]byte, greeting[1])
				if _, err := io.ReadFull(reader, methods); err != nil {
					return
				}
				if err := writeAll(conn, []byte{5, 0}); err != nil {
					return
				}
				var request [4]byte
				if _, err := io.ReadFull(reader, request[:]); err != nil {
					return
				}
				if _, err := decodeSocksAddressWithType(reader, request[3]); err != nil {
					return
				}
				_, _ = io.ReadAll(reader)
			}()
		}
	}()

	for i := 0; i < attempts; i++ {
		_, err := dialUpstream(context.Background(), UpstreamSocksOptions{
			Address:        listener.Addr().String(),
			ConnectTimeout: Duration(20 * time.Millisecond),
		}, "example.com:443")
		if err == nil {
			t.Fatal("hanging upstream CONNECT unexpectedly succeeded")
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for closed.Load() < attempts && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if accepted.Load() != attempts || closed.Load() != attempts {
		t.Fatalf("accepted=%d closed=%d, want %d/%d", accepted.Load(), closed.Load(), attempts, attempts)
	}
}

func socksConnect(conn net.Conn, target string) error {
	if err := writeAll(conn, []byte{5, 1, 0}); err != nil {
		return err
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(conn, method); err != nil {
		return err
	}
	request, err := encodeConnectRequest(target)
	if err != nil {
		return err
	}
	if err := writeAll(conn, request); err != nil {
		return err
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	if err := discardSocksAddress(reader, reply[3]); err != nil {
		return err
	}
	if reply[1] != 0 {
		return errors.New("local SOCKS CONNECT failed")
	}
	return nil
}

type socksForwarder struct {
	listener net.Listener
	closeOne sync.Once
	mu       sync.Mutex
	targets  []string
}

func newSOCKSForwarder(t *testing.T) *socksForwarder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	forwarder := &socksForwarder{listener: listener}
	go forwarder.acceptLoop()
	return forwarder
}

func (s *socksForwarder) Addr() net.Addr { return s.listener.Addr() }

func (s *socksForwarder) waitForTargets(t *testing.T, count int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		targets := append([]string(nil), s.targets...)
		s.mu.Unlock()
		if len(targets) >= count {
			return targets
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("forwarder targets = %v, want at least %d", s.targets, count)
	return nil
}

func (s *socksForwarder) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *socksForwarder) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var greeting [2]byte
	if _, err := io.ReadFull(reader, greeting[:]); err != nil {
		return
	}
	methods := make([]byte, greeting[1])
	if _, err := io.ReadFull(reader, methods); err != nil {
		return
	}
	if err := writeAll(conn, []byte{5, 0}); err != nil {
		return
	}
	var request [4]byte
	if _, err := io.ReadFull(reader, request[:]); err != nil {
		return
	}
	target, err := decodeSocksAddressWithType(reader, request[3])
	if err != nil {
		return
	}
	s.mu.Lock()
	s.targets = append(s.targets, target)
	s.mu.Unlock()
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		_ = writeAll(conn, []byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if err := writeAll(conn, []byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(upstream, reader) }()
	go func() { defer wg.Done(); _, _ = io.Copy(conn, upstream) }()
	wg.Wait()
}

func (s *socksForwarder) Close() {
	s.closeOne.Do(func() { _ = s.listener.Close() })
}

type byteReader struct{ data []byte }

func validTestConfig() Config {
	cfg := DefaultConfig()
	cfg.SMP3.Password = "test-password"
	cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24441", Leg1: "127.0.0.1:24442"}
	cfg.Listen = "127.0.0.1:0"
	return cfg
}

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func bytesReader(data []byte) io.Reader { return &byteReader{data: append([]byte(nil), data...)} }

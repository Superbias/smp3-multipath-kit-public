package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	socksVersion5                 = 5
	socksNoAuth                   = 0
	socksUserPass                 = 2
	socksCommandConnect           = 1
	socksCommandBind              = 2
	socksCommandUDPAssociate      = 3
	socksCommandNotSupported      = 7
	socksReplyGeneralFailure      = 1
	socksReplyCommandNotSupported = 7
)

var errSOCKS5NoAcceptableMethod = errors.New("SOCKS5 upstream offered no supported authentication method")

func dialUpstream(ctx context.Context, options UpstreamSocksOptions, target string) (net.Conn, error) {
	if options.Address == "" {
		return nil, errors.New("upstream SOCKS address is empty")
	}
	timeout := options.ConnectTimeout.Time()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(attemptCtx, "tcp", options.Address)
	if err != nil {
		return nil, fmt.Errorf("dial upstream SOCKS: %w", err)
	}
	var watchMu sync.Mutex
	finished := false
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-attemptCtx.Done():
			watchMu.Lock()
			if !finished {
				_ = conn.Close()
			}
			watchMu.Unlock()
		case <-watchDone:
		}
	}()
	finish := func() {
		watchMu.Lock()
		finished = true
		close(watchDone)
		watchMu.Unlock()
	}
	defer finish()
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set upstream SOCKS deadline: %w", err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()

	methods := []byte{socksNoAuth}
	if options.Username != "" || options.Password != "" {
		if options.Username == "" || options.Password == "" {
			return nil, errors.New("upstream SOCKS username and password must be provided together")
		}
		methods = append(methods, socksUserPass)
	}
	if err := writeAll(conn, append([]byte{socksVersion5, byte(len(methods))}, methods...)); err != nil {
		return nil, fmt.Errorf("write upstream SOCKS greeting: %w", err)
	}
	var selection [2]byte
	if _, err := io.ReadFull(conn, selection[:]); err != nil {
		return nil, fmt.Errorf("read upstream SOCKS method: %w", err)
	}
	if selection[0] != socksVersion5 {
		return nil, errors.New("upstream SOCKS returned an invalid version")
	}
	switch selection[1] {
	case socksNoAuth:
	case socksUserPass:
		if options.Username == "" || options.Password == "" {
			return nil, errSOCKS5NoAcceptableMethod
		}
		if err := writeUserPassRequest(conn, options.Username, options.Password); err != nil {
			return nil, err
		}
		var authReply [2]byte
		if _, err := io.ReadFull(conn, authReply[:]); err != nil {
			return nil, fmt.Errorf("read upstream SOCKS authentication: %w", err)
		}
		if authReply[0] != 1 || authReply[1] != 0 {
			return nil, errors.New("upstream SOCKS authentication failed")
		}
	default:
		return nil, errSOCKS5NoAcceptableMethod
	}
	request, err := encodeConnectRequest(target)
	if err != nil {
		return nil, err
	}
	if err := writeAll(conn, request); err != nil {
		return nil, fmt.Errorf("write upstream SOCKS CONNECT: %w", err)
	}
	var reply [4]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return nil, fmt.Errorf("read upstream SOCKS CONNECT: %w", err)
	}
	if reply[0] != socksVersion5 {
		return nil, errors.New("upstream SOCKS CONNECT returned an invalid version")
	}
	if err := discardSocksAddress(bufio.NewReader(conn), reply[3]); err != nil {
		return nil, fmt.Errorf("read upstream SOCKS bound address: %w", err)
	}
	if reply[1] != 0 {
		return nil, fmt.Errorf("upstream SOCKS CONNECT failed with reply %d", reply[1])
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear upstream SOCKS deadline: %w", err)
	}
	closeOnError = false
	return conn, nil
}

func writeUserPassRequest(w io.Writer, username, password string) error {
	if len(username) > 255 || len(password) > 255 {
		return errors.New("upstream SOCKS credentials exceed 255 bytes")
	}
	request := []byte{1, byte(len(username))}
	request = append(request, username...)
	request = append(request, byte(len(password)))
	request = append(request, password...)
	if err := writeAll(w, request); err != nil {
		return fmt.Errorf("write upstream SOCKS authentication: %w", err)
	}
	return nil
}

func encodeConnectRequest(target string) ([]byte, error) {
	address, err := encodeSocksAddress(target, false)
	if err != nil {
		return nil, err
	}
	return append([]byte{socksVersion5, socksCommandConnect, 0}, address...), nil
}

func encodeSocksAddress(target string, allowZeroPort bool) ([]byte, error) {
	host, portText, err := net.SplitHostPort(target)
	if err != nil || host == "" {
		return nil, fmt.Errorf("invalid SOCKS address %q", target)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || (!allowZeroPort && port == 0) {
		return nil, fmt.Errorf("invalid SOCKS port %q", portText)
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			address := []byte{1, v4[0], v4[1], v4[2], v4[3]}
			return append(address, byte(port>>8), byte(port)), nil
		}
		ip = ip.To16()
		if ip == nil {
			return nil, fmt.Errorf("invalid SOCKS IP address %q", host)
		}
		address := append([]byte{4}, ip...)
		return append(address, byte(port>>8), byte(port)), nil
	}
	if len(host) > 255 {
		return nil, errors.New("SOCKS domain exceeds 255 bytes")
	}
	address := []byte{3, byte(len(host))}
	address = append(address, host...)
	return append(address, byte(port>>8), byte(port)), nil
}

func decodeSocksAddress(reader *bufio.Reader, allowZeroPort bool) (string, error) {
	addressType, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	var host string
	switch addressType {
	case 1:
		var value [4]byte
		if _, err := io.ReadFull(reader, value[:]); err != nil {
			return "", err
		}
		host = net.IP(value[:]).String()
	case 3:
		length, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if length == 0 {
			return "", errors.New("empty SOCKS domain")
		}
		value := make([]byte, length)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		host = string(value)
	case 4:
		var value [16]byte
		if _, err := io.ReadFull(reader, value[:]); err != nil {
			return "", err
		}
		host = net.IP(value[:]).String()
	default:
		return "", fmt.Errorf("unsupported SOCKS address type %d", addressType)
	}
	var portBytes [2]byte
	if _, err := io.ReadFull(reader, portBytes[:]); err != nil {
		return "", err
	}
	port := int(binary.BigEndian.Uint16(portBytes[:]))
	if port == 0 && !allowZeroPort {
		return "", errors.New("SOCKS port is zero")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func discardSocksAddress(reader *bufio.Reader, addressType byte) error {
	_, err := decodeSocksAddressWithType(reader, addressType)
	return err
}

func decodeSocksAddressWithType(reader *bufio.Reader, addressType byte) (string, error) {
	var host string
	switch addressType {
	case 1:
		var value [4]byte
		if _, err := io.ReadFull(reader, value[:]); err != nil {
			return "", err
		}
		host = net.IP(value[:]).String()
	case 3:
		length, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		value := make([]byte, length)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		host = string(value)
	case 4:
		var value [16]byte
		if _, err := io.ReadFull(reader, value[:]); err != nil {
			return "", err
		}
		host = net.IP(value[:]).String()
	default:
		return "", fmt.Errorf("unsupported SOCKS bound address type %d", addressType)
	}
	var port [2]byte
	if _, err := io.ReadFull(reader, port[:]); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:])))), nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		data = data[n:]
	}
	return nil
}

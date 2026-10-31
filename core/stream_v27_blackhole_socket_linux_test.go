//go:build linux

package smp3core

import (
	"fmt"
	"net"
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

func v27BlackholeSetTCPWriteBuffer(conn net.Conn, size int) (int, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return 0, fmt.Errorf("retry profile requires *net.TCPConn, got %T", conn)
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return 0, err
	}
	var actual int
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		socketErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUFFORCE, size)
		if socketErr == nil {
			actual, socketErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
		}
	}); err != nil {
		return 0, err
	}
	if socketErr != nil {
		return 0, socketErr
	}
	return actual, nil
}

func v27BlackholeTCPInfoSnapshot(conn net.Conn) map[string]any {
	result := map[string]any{"available": false}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		result["error"] = fmt.Sprintf("requires *net.TCPConn, got %T", conn)
		return result
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	var info syscall.TCPInfo
	length := uint32(unsafe.Sizeof(info))
	var socketErr syscall.Errno
	if err := raw.Control(func(fd uintptr) {
		_, _, socketErr = syscall.Syscall6(
			syscall.SYS_GETSOCKOPT, fd, uintptr(syscall.IPPROTO_TCP), uintptr(syscall.TCP_INFO),
			uintptr(unsafe.Pointer(&info)), uintptr(unsafe.Pointer(&length)), 0,
		)
	}); err != nil {
		result["error"] = err.Error()
		return result
	}
	runtime.KeepAlive(&info)
	if socketErr != 0 {
		result["error"] = socketErr.Error()
		return result
	}
	result["available"] = true
	result["length"] = length
	result["state"] = info.State
	result["retransmits"] = info.Retransmits
	result["probes"] = info.Probes
	result["backoff"] = info.Backoff
	result["rto_us"] = info.Rto
	result["unacked"] = info.Unacked
	result["sacked"] = info.Sacked
	result["lost"] = info.Lost
	result["retrans"] = info.Retrans
	result["rtt_us"] = info.Rtt
	result["rttvar_us"] = info.Rttvar
	result["total_retrans"] = info.Total_retrans
	result["local_addr"] = tcp.LocalAddr().String()
	result["remote_addr"] = tcp.RemoteAddr().String()
	return result
}

func TestV27BlackholeTCPInfoSnapshot(t *testing.T) {
	client, server, _ := v27BlackholeTCPPair(t)
	defer client.Close()
	defer server.Close()
	for _, conn := range []net.Conn{client, server} {
		snapshot := v27BlackholeTCPInfoSnapshot(conn)
		if snapshot["available"] != true {
			t.Fatalf("TCP_INFO unavailable for %T: %+v", conn, snapshot)
		}
		if _, ok := snapshot["total_retrans"].(uint32); !ok {
			t.Fatalf("TCP_INFO total_retrans missing or has wrong type: %+v", snapshot)
		}
		if snapshot["local_addr"] == "" || snapshot["remote_addr"] == "" {
			t.Fatalf("TCP_INFO socket identity missing: %+v", snapshot)
		}
	}
}

//go:build !linux

package smp3core

import (
	"errors"
	"net"
)

func v27BlackholeSetTCPWriteBuffer(net.Conn, int) (int, error) {
	return 0, errors.New("Phase15 retry-profile socket buffer is Linux-only")
}

func v27BlackholeTCPInfoSnapshot(net.Conn) map[string]any {
	return map[string]any{"available": false, "error": "TCP_INFO helper is Linux-only"}
}

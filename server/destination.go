package server

import (
	"fmt"
	"net"
	"strconv"
)

func normalizeDestination(value string) (string, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil || host == "" {
		return "", fmt.Errorf("invalid destination %q: expected host:port", value)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid destination port in %q", value)
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func resolveUDP(value string) (*net.UDPAddr, error) {
	normalized, err := normalizeDestination(value)
	if err != nil {
		return nil, err
	}
	return net.ResolveUDPAddr("udp", normalized)
}

func udpFamily(address *net.UDPAddr) string {
	if address != nil && address.IP.To4() == nil {
		return "udp6"
	}
	return "udp4"
}

func udpAddressString(address net.Addr) string {
	if udp, ok := address.(*net.UDPAddr); ok {
		ip := udp.IP
		if v4 := ip.To4(); v4 != nil {
			return net.JoinHostPort(v4.String(), strconv.Itoa(udp.Port))
		}
		host := ip.String()
		if udp.Zone != "" {
			host += "%" + udp.Zone
		}
		return net.JoinHostPort(host, strconv.Itoa(udp.Port))
	}
	return address.String()
}

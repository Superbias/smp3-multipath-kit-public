package smp3core

import (
	"net"
	"testing"
)

func phase36AttachDrainLegPair(t *testing.T, client, landing *StreamEngine, id LegID) (StreamPathAuth, StreamPathAuth) {
	t.Helper()
	clientConn, landingConn := net.Pipe()
	hello := phase36V7Hello(id, false)
	clientAuth, landingAuth := phase36V7AuthPair(t, hello, StreamCapabilityCoordinatedDrain)
	if err := client.AttachLegWithProtocolAuth(id, clientConn, nil, clientAuth); err != nil {
		_ = clientConn.Close()
		_ = landingConn.Close()
		t.Fatalf("attach client V7 Leg %d: %v", id, err)
	}
	if err := landing.AttachLegWithProtocolAuth(id, landingConn, nil, landingAuth); err != nil {
		_ = landingConn.Close()
		t.Fatalf("attach Landing V7 Leg %d: %v", id, err)
	}
	return clientAuth, landingAuth
}

func phase36AttachDrainLegToUnresponsivePeer(t *testing.T, engine *StreamEngine, id LegID) net.Conn {
	t.Helper()
	if engine.cfg.PathRole != StreamPathRoleClient && engine.cfg.PathRole != StreamPathRoleServer {
		engine.cfg.PathRole = StreamPathRoleClient
	}
	local, peer := net.Pipe()
	hello := phase36V7Hello(id, false)
	clientAuth, _ := phase36V7AuthPair(t, hello, StreamCapabilityCoordinatedDrain)
	if err := engine.AttachLegWithProtocolAuth(id, local, nil, clientAuth); err != nil {
		_ = local.Close()
		_ = peer.Close()
		t.Fatalf("attach unresponsive V7 Leg %d: %v", id, err)
	}
	return peer
}

package smp3core

import (
	"bytes"
	"net"
	"testing"
)

type p0cdCountingConn struct {
	net.Conn
	writes [][]byte
}

func (c *p0cdCountingConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func TestP0CDataFrameCoalescerWritesHeaderAndPayloadOnce(t *testing.T) {
	payload := []byte("fixture payload")
	var header [StreamFrameHeaderSize]byte
	if err := EncodeStreamFrameHeader(header[:], StreamFrameHeader{
		Type: StreamFrameData, Value: 17, Length: uint32(len(payload)),
	}); err != nil {
		t.Fatal(err)
	}
	carrier := &p0cdCountingConn{}
	coalescer := &p0cdDataFrameCoalescer{Conn: carrier}

	if n, err := coalescer.Write(header[:]); err != nil || n != len(header) {
		t.Fatalf("header Write = (%d, %v), want (%d, nil)", n, err, len(header))
	}
	if len(carrier.writes) != 0 {
		t.Fatalf("header reached carrier before payload: writes=%d", len(carrier.writes))
	}
	if n, err := coalescer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("payload Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if len(carrier.writes) != 1 {
		t.Fatalf("DATA frame produced %d carrier writes, want exactly one", len(carrier.writes))
	}
	want := append(append([]byte(nil), header[:]...), payload...)
	if !bytes.Equal(carrier.writes[0], want) {
		t.Fatalf("coalesced bytes differ: got=%d want=%d", len(carrier.writes[0]), len(want))
	}
}

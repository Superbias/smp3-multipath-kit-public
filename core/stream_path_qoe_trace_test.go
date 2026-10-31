package smp3core

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type streamPathQoEFrameEvent struct {
	Endpoint     string  `json:"endpoint"`
	Direction    string  `json:"direction"`
	Leg          int     `json:"leg"`
	Action       string  `json:"action"`
	Type         string  `json:"type"`
	Value        uint64  `json:"value"`
	PayloadBytes int     `json:"payload_bytes"`
	ElapsedMS    float64 `json:"elapsed_ms"`
	WriteCallMS  float64 `json:"write_call_ms,omitempty"`
	Error        string  `json:"error,omitempty"`
}

type streamPathQoEFrameRecorder struct {
	mu     sync.Mutex
	start  time.Time
	events []streamPathQoEFrameEvent
}

func (r *streamPathQoEFrameRecorder) setStart(start time.Time) {
	r.mu.Lock()
	r.start = start
	r.mu.Unlock()
}

func (r *streamPathQoEFrameRecorder) record(event streamPathQoEFrameEvent, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.start.IsZero() || at.Before(r.start) {
		return
	}
	event.ElapsedMS = float64(at.Sub(r.start).Microseconds()) / 1000
	r.events = append(r.events, event)
}

func (r *streamPathQoEFrameRecorder) snapshot() []streamPathQoEFrameEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]streamPathQoEFrameEvent(nil), r.events...)
}

type streamPathQoEFrameParser struct {
	pending []byte
}

func (p *streamPathQoEFrameParser) feed(data []byte) []StreamFrameHeader {
	p.pending = append(p.pending, data...)
	var frames []StreamFrameHeader
	for len(p.pending) >= StreamFrameHeaderSize {
		header := StreamFrameHeader{
			Type:   StreamFrameType(p.pending[0]),
			Value:  binary.BigEndian.Uint64(p.pending[1:9]),
			Length: binary.BigEndian.Uint32(p.pending[9:StreamFrameHeaderSize]),
		}
		if err := validateStreamFrameHeader(header); err != nil {
			// This is passive test instrumentation. Resynchronize on the next byte
			// instead of changing the wrapped Carrier's read/write behavior.
			p.pending = p.pending[1:]
			continue
		}
		frameBytes := StreamFrameHeaderSize + int(header.Length)
		if len(p.pending) < frameBytes {
			break
		}
		frames = append(frames, header)
		p.pending = p.pending[frameBytes:]
	}
	return frames
}

// streamPathQoETraceConn observes complete SMP3 frames at the test Carrier
// boundary. It records no payload bytes and never delays or rewrites traffic.
type streamPathQoETraceConn struct {
	net.Conn
	endpoint       string
	leg            int
	writeDirection string
	readDirection  string
	recorder       *streamPathQoEFrameRecorder
	writeMu        sync.Mutex
	writeParser    streamPathQoEFrameParser
	readMu         sync.Mutex
	readParser     streamPathQoEFrameParser
}

func newStreamPathQoETraceConn(conn net.Conn, leg int, writeDirection string, recorder *streamPathQoEFrameRecorder) *streamPathQoETraceConn {
	readDirection := "client_to_server"
	endpoint := "client"
	if writeDirection == "client_to_server" {
		readDirection = "server_to_client"
	} else {
		endpoint = "server"
	}
	return &streamPathQoETraceConn{
		Conn: conn, endpoint: endpoint, leg: leg, writeDirection: writeDirection,
		readDirection: readDirection, recorder: recorder,
	}
}

func (c *streamPathQoETraceConn) Write(data []byte) (int, error) {
	started := time.Now()
	c.writeMu.Lock()
	frames := c.writeParser.feed(data)
	c.writeMu.Unlock()
	for _, frame := range frames {
		c.recorder.record(streamPathQoEFrameEvent{
			Endpoint: c.endpoint, Direction: c.writeDirection, Leg: c.leg, Action: "carrier_write_attempt",
			Type: frame.Type.String(), Value: frame.Value, PayloadBytes: int(frame.Length),
		}, started)
	}
	n, err := c.Conn.Write(data)
	finished := time.Now()
	for _, frame := range frames {
		event := streamPathQoEFrameEvent{
			Endpoint: c.endpoint, Direction: c.writeDirection, Leg: c.leg, Action: "carrier_write_return",
			Type: frame.Type.String(), Value: frame.Value, PayloadBytes: int(frame.Length),
			WriteCallMS: float64(finished.Sub(started).Microseconds()) / 1000,
		}
		if err != nil {
			event.Error = err.Error()
		} else if n != len(data) {
			event.Error = fmt.Sprintf("short Carrier write: %d/%d", n, len(data))
		}
		c.recorder.record(event, finished)
	}
	return n, err
}

func (c *streamPathQoETraceConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if n <= 0 {
		return n, err
	}
	finished := time.Now()
	c.readMu.Lock()
	frames := c.readParser.feed(data[:n])
	c.readMu.Unlock()
	for _, frame := range frames {
		c.recorder.record(streamPathQoEFrameEvent{
			Endpoint: c.endpoint, Direction: c.readDirection, Leg: c.leg, Action: "carrier_read_frame",
			Type: frame.Type.String(), Value: frame.Value, PayloadBytes: int(frame.Length),
		}, finished)
	}
	return n, err
}

func (typ StreamFrameType) String() string {
	switch typ {
	case StreamFrameData:
		return "DATA"
	case StreamFrameActivate:
		return "ACTIVATE"
	case StreamFrameACK:
		return "ACK"
	case StreamFrameClose:
		return "CLOSE"
	case StreamFramePathRTT:
		return "PATH_RTT"
	default:
		return fmt.Sprintf("UNKNOWN_%d", typ)
	}
}

func TestStreamPathQoETraceRecordsSplitFrameWriteAndRead(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	recorder := &streamPathQoEFrameRecorder{}
	recorder.setStart(time.Now())
	leftTrace := newStreamPathQoETraceConn(left, 1, "client_to_server", recorder)
	rightTrace := newStreamPathQoETraceConn(right, 1, "server_to_client", recorder)
	header := make([]byte, StreamFrameHeaderSize)
	if err := EncodeStreamFrameHeader(header, StreamFrameHeader{Type: StreamFrameData, Value: 42, Length: 3}); err != nil {
		t.Fatal(err)
	}
	payload := []byte{1, 2, 3}
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, len(header)+len(payload))
		_, err := io.ReadFull(rightTrace, got)
		readDone <- err
	}()
	if n, err := leftTrace.Write(header); err != nil || n != len(header) {
		t.Fatalf("write DATA header: %d/%d, %v", n, len(header), err)
	}
	if n, err := leftTrace.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write DATA payload: %d/%d, %v", n, len(payload), err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("read split DATA frame: %v", err)
	}
	events := recorder.snapshot()
	if len(events) != 3 {
		t.Fatalf("got %d trace events, want write attempt, write return, and read: %+v", len(events), events)
	}
	actions := make(map[string]int)
	for _, event := range events {
		if event.Type != "DATA" || event.Value != 42 || event.PayloadBytes != len(payload) {
			t.Fatalf("trace did not preserve frame header: %+v", event)
		}
		actions[event.Action]++
		switch event.Action {
		case "carrier_write_attempt":
			if event.Endpoint != "client" || event.Direction != "client_to_server" {
				t.Fatalf("wrong write-attempt endpoint or direction: %+v", event)
			}
		case "carrier_write_return":
			if event.Endpoint != "client" || event.Direction != "client_to_server" {
				t.Fatalf("wrong write-return endpoint or direction: %+v", event)
			}
		case "carrier_read_frame":
			if event.Endpoint != "server" || event.Direction != "client_to_server" {
				t.Fatalf("wrong read endpoint or direction: %+v", event)
			}
		default:
			t.Fatalf("unexpected trace action: %+v", event)
		}
	}
	if actions["carrier_write_attempt"] != 1 || actions["carrier_write_return"] != 1 || actions["carrier_read_frame"] != 1 {
		t.Fatalf("unexpected action counts: %+v", actions)
	}
}

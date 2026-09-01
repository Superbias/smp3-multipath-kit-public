package multipath

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

const (
	goldenPassword = "golden-password"
	goldenUnixTime = int64(1700000000)
)

var goldenSession = [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
var goldenNonce = [16]byte{0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98, 0xa9, 0xba, 0xcb, 0xdc, 0xed, 0xfe, 0x0f}

type helloGoldenVector struct {
	name        string
	fixture     string
	version     byte
	leg         uint8
	mode        helloMode
	destination string
	headerSize  int
}

func wireFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "wire", filepath.FromSlash(name))
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wire fixture %s: %v", path, err)
	}
	decoded, err := hex.DecodeString(strings.Join(strings.Fields(string(encoded)), ""))
	if err != nil {
		t.Fatalf("decode wire fixture %s: %v", path, err)
	}
	return decoded
}

func captureWireBytes(t *testing.T, length int, write func(net.Conn) error) []byte {
	t.Helper()
	conn, peer := net.Pipe()
	defer peer.Close()
	readDone := make(chan struct{})
	var got []byte
	var readErr error
	go func() {
		got = make([]byte, length)
		_, readErr = io.ReadFull(peer, got)
		close(readDone)
	}()
	if err := write(conn); err != nil {
		_ = conn.Close()
		<-readDone
		t.Fatalf("write wire bytes: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close wire writer: %v", err)
	}
	<-readDone
	if readErr != nil {
		t.Fatalf("read captured wire bytes: %v", readErr)
	}
	return got
}

func feedWireBytes(t *testing.T, input []byte, read func(net.Conn) error) error {
	t.Helper()
	conn, peer := net.Pipe()
	writeDone := make(chan struct{})
	go func() {
		_, _ = peer.Write(input)
		_ = peer.Close()
		close(writeDone)
	}()
	err := read(conn)
	_ = conn.Close()
	<-writeDone
	return err
}

func TestHelloWireGoldenVectors(t *testing.T) {
	vectors := []helloGoldenVector{
		{name: "v4_leg0_ipv4", fixture: "hello_v4/v4_leg0_ipv4.hex", version: helloVersion, leg: 0, mode: helloModeStream, destination: "192.0.2.1:443", headerSize: 48},
		{name: "v4_leg1_ipv6", fixture: "hello_v4/v4_leg1_ipv6.hex", version: helloVersion, leg: 1, mode: helloModeStream, destination: "[2001:db8::1]:8443", headerSize: 48},
		{name: "v4_leg0_domain", fixture: "hello_v4/v4_leg0_domain.hex", version: helloVersion, leg: 0, mode: helloModeStream, destination: "example.com:443", headerSize: 48},
		{name: "v5_leg0_ipv4", fixture: "hello_v5/v5_leg0_ipv4.hex", version: helloVersionDatagram, leg: 0, mode: helloModeDatagram, destination: "192.0.2.1:443", headerSize: 49},
		{name: "v5_leg1_ipv6", fixture: "hello_v5/v5_leg1_ipv6.hex", version: helloVersionDatagram, leg: 1, mode: helloModeDatagram, destination: "[2001:db8::1]:8443", headerSize: 49},
		{name: "v5_leg0_domain", fixture: "hello_v5/v5_leg0_domain.hex", version: helloVersionDatagram, leg: 0, mode: helloModeDatagram, destination: "example.com:443", headerSize: 49},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			message := helloMessage{Session: goldenSession, LegID: vector.leg, Mode: vector.mode, Destination: vector.destination}
			header, destination, mac, err := encodeHelloParts(message, goldenPassword, goldenUnixTime, goldenNonce)
			if err != nil {
				t.Fatal(err)
			}
			wire := append(append(append([]byte(nil), header...), destination...), mac...)
			want := wireFixture(t, vector.fixture)
			if !bytes.Equal(wire, want) {
				t.Fatalf("encoded wire mismatch:\n got %x\nwant %x", wire, want)
			}
			if len(header) != vector.headerSize || header[4] != vector.version {
				t.Fatalf("unexpected HELLO header: version=%d size=%d", header[4], len(header))
			}

			var decoded helloMessage
			var nonce [16]byte
			err = feedWireBytes(t, want, func(conn net.Conn) error {
				decoded, nonce, err = readHelloAt(conn, goldenPassword, time.Unix(goldenUnixTime, 0))
				return err
			})
			if err != nil {
				t.Fatalf("decode golden HELLO: %v", err)
			}
			if decoded != message || nonce != goldenNonce {
				t.Fatalf("decoded HELLO = %#v nonce=%x, want %#v nonce=%x", decoded, nonce, message, goldenNonce)
			}

			tampered := append([]byte(nil), want...)
			tampered[vector.headerSize] ^= 0x01
			err = feedWireBytes(t, tampered, func(conn net.Conn) error {
				_, _, err := readHelloAt(conn, goldenPassword, time.Unix(goldenUnixTime, 0))
				return err
			})
			if err == nil || !strings.Contains(err.Error(), "authentication") {
				t.Fatalf("tampered HELLO error = %v, want authentication failure", err)
			}
		})
	}
}

func TestHelloV5DoesNotEncodeDatagramPolicy(t *testing.T) {
	header, destination, mac, err := encodeHelloParts(helloMessage{
		Session: goldenSession, LegID: 0, Mode: helloModeDatagram, Destination: "192.0.2.1:443",
	}, goldenPassword, goldenUnixTime, goldenNonce)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) != 49 || header[4] != helloVersionDatagram || header[6] != byte(helloModeDatagram) {
		t.Fatalf("unexpected v5 header: %x", header)
	}
	if len(destination) == 0 || len(mac) != 32 {
		t.Fatalf("unexpected v5 parts: destination=%d mac=%d", len(destination), len(mac))
	}
	// No mode policy, MTU, dedup, timeout, or adaptive threshold fields exist
	// in the v5 header; those remain local datagramConfig decisions.
}

func TestHelloFreshnessBoundaries(t *testing.T) {
	message := helloMessage{Session: goldenSession, LegID: 0, Mode: helloModeStream, Destination: "example.com:443"}
	header, destination, mac, err := encodeHelloParts(message, goldenPassword, goldenUnixTime, goldenNonce)
	if err != nil {
		t.Fatal(err)
	}
	wire := append(append(append([]byte(nil), header...), destination...), mac...)
	for _, test := range []struct {
		name   string
		offset int64
		valid  bool
	}{
		{name: "minus_90s", offset: -90, valid: true},
		{name: "plus_90s", offset: 90, valid: true},
		{name: "minus_91s", offset: -91, valid: false},
		{name: "plus_91s", offset: 91, valid: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := feedWireBytes(t, wire, func(conn net.Conn) error {
				_, _, err := readHelloAt(conn, goldenPassword, time.Unix(goldenUnixTime+test.offset, 0))
				return err
			})
			if test.valid && err != nil {
				t.Fatalf("fresh HELLO rejected: %v", err)
			}
			if !test.valid && (err == nil || !strings.Contains(err.Error(), "stale")) {
				t.Fatalf("stale HELLO error = %v", err)
			}
		})
	}
}

func TestStreamWireGoldenVectors(t *testing.T) {
	data := captureWireBytes(t, frameHeaderSize+5, func(conn net.Conn) error {
		return writeDataFrame(conn, dataFrame{seq: 0x0102030405060708, data: []byte("hello")})
	})
	if want := wireFixture(t, "stream_frames/data.hex"); !bytes.Equal(data, want) {
		t.Fatalf("DATA frame mismatch: got %x want %x", data, want)
	}
	for _, vector := range []struct {
		name    string
		fixture string
		typ     byte
		value   uint64
	}{
		{name: "activate", fixture: "stream_frames/activate.hex", typ: frameTypeActivate, value: 0x1122334455667788},
		{name: "ack", fixture: "stream_frames/ack.hex", typ: frameTypeAck, value: 42},
		{name: "close", fixture: "stream_frames/close.hex", typ: frameTypeClose, value: 0xdeadbeef},
	} {
		t.Run(vector.name, func(t *testing.T) {
			got := encodeControlFrame(vector.typ, vector.value)
			want := wireFixture(t, vector.fixture)
			if !bytes.Equal(got[:], want) {
				t.Fatalf("control frame mismatch: got %x want %x", got, want)
			}
		})
	}
}

func TestStreamFrameCoreDifferentialGoldenVectors(t *testing.T) {
	vectors := []struct {
		name    string
		fixture string
		header  smp3core.StreamFrameHeader
		payload []byte
	}{
		{name: "data", fixture: "stream_frames/data.hex", header: smp3core.StreamFrameHeader{Type: smp3core.StreamFrameData, Value: 0x0102030405060708, Length: 5}, payload: []byte("hello")},
		{name: "activate", fixture: "stream_frames/activate.hex", header: smp3core.StreamFrameHeader{Type: smp3core.StreamFrameActivate, Value: 0x1122334455667788}},
		{name: "ack", fixture: "stream_frames/ack.hex", header: smp3core.StreamFrameHeader{Type: smp3core.StreamFrameACK, Value: 42}},
		{name: "close", fixture: "stream_frames/close.hex", header: smp3core.StreamFrameHeader{Type: smp3core.StreamFrameClose, Value: 0xdeadbeef}},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			want := wireFixture(t, vector.fixture)
			legacy := captureWireBytes(t, len(want), func(conn net.Conn) error {
				if vector.header.Type == smp3core.StreamFrameData {
					return writeDataFrame(conn, dataFrame{seq: vector.header.Value, data: vector.payload})
				}
				frame := encodeControlFrame(byte(vector.header.Type), vector.header.Value)
				_, err := conn.Write(frame[:])
				return err
			})
			if !bytes.Equal(legacy, want) {
				t.Fatalf("legacy wire mismatch: got %x want %x", legacy, want)
			}

			coreHeader := make([]byte, smp3core.StreamFrameHeaderSize)
			if err := smp3core.EncodeStreamFrameHeader(coreHeader, vector.header); err != nil {
				t.Fatal(err)
			}
			coreWire := append(append([]byte(nil), coreHeader...), vector.payload...)
			if !bytes.Equal(coreWire, want) {
				t.Fatalf("Core wire mismatch: got %x want %x", coreWire, want)
			}
			decoded, err := smp3core.ReadStreamFrameHeader(bytes.NewReader(want))
			if err != nil {
				t.Fatalf("Core decode: %v", err)
			}
			if decoded != vector.header {
				t.Fatalf("Core header=%+v want=%+v", decoded, vector.header)
			}
			if vector.header.Type == smp3core.StreamFrameData {
				payload := make([]byte, vector.header.Length)
				if _, err := io.ReadFull(bytes.NewReader(want[smp3core.StreamFrameHeaderSize:]), payload); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(payload, vector.payload) {
					t.Fatalf("Core payload=%x want=%x", payload, vector.payload)
				}
			}

			core, _ := newCore(testCoreConfig())
			defer core.Close()
			var legacyFrame wireFrame
			if err := feedWireBytes(t, want, func(conn net.Conn) error {
				var err error
				legacyFrame, err = readWireFrame(conn, core)
				return err
			}); err != nil {
				t.Fatalf("legacy decode: %v", err)
			}
			if legacyFrame.typ != byte(vector.header.Type) || legacyFrame.seq != vector.header.Value || !bytes.Equal(legacyFrame.data, vector.payload) {
				t.Fatalf("legacy frame=%+v want type=%d value=%d payload=%x", legacyFrame, vector.header.Type, vector.header.Value, vector.payload)
			}
		})
	}
}

func readCoreStreamFrameForTest(wire []byte) error {
	reader := bytes.NewReader(wire)
	header, err := smp3core.ReadStreamFrameHeader(reader)
	if err != nil {
		return err
	}
	if header.Type == smp3core.StreamFrameData {
		payload := make([]byte, header.Length)
		_, err = io.ReadFull(reader, payload)
	}
	return err
}

func TestStreamFrameCoreInvalidParity(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture string
	}{
		{name: "unknown_type", fixture: "invalid/stream_unknown_type.hex"},
		{name: "control_length", fixture: "invalid/stream_invalid_control_length.hex"},
		{name: "oversized_length", fixture: "invalid/stream_oversized_length.hex"},
		{name: "truncated_header", fixture: "invalid/stream_truncated_header.hex"},
		{name: "truncated_payload", fixture: "invalid/stream_truncated_payload.hex"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := wireFixture(t, test.fixture)
			coreErr := readCoreStreamFrameForTest(wire)
			core, _ := newCore(testCoreConfig())
			defer core.Close()
			legacyErr := feedWireBytes(t, wire, func(conn net.Conn) error {
				_, err := readWireFrame(conn, core)
				return err
			})
			if (coreErr == nil) != (legacyErr == nil) {
				t.Fatalf("invalid acceptance mismatch: Core=%v legacy=%v", coreErr, legacyErr)
			}
			if coreErr == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
}

func TestStreamInvalidWireFixtures(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture string
		want    string
	}{
		{name: "unknown_type", fixture: "invalid/stream_unknown_type.hex", want: "unknown multipath frame type"},
		{name: "control_length", fixture: "invalid/stream_invalid_control_length.hex", want: "invalid multipath control frame length"},
		{name: "oversized_length", fixture: "invalid/stream_oversized_length.hex", want: "invalid multipath frame length"},
		{name: "truncated_header", fixture: "invalid/stream_truncated_header.hex"},
		{name: "truncated_payload", fixture: "invalid/stream_truncated_payload.hex"},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, _ := newCore(testCoreConfig())
			defer core.Close()
			err := feedWireBytes(t, wireFixture(t, test.fixture), func(conn net.Conn) error {
				_, err := readWireFrame(conn, core)
				return err
			})
			if err == nil {
				t.Fatal("invalid stream frame was accepted")
			}
			if test.want != "" && !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func datagramWireBytes(packet datagramPacket) []byte {
	address := []byte(packet.address)
	payload := make([]byte, 2+len(address)+len(packet.data))
	binary.BigEndian.PutUint16(payload[:2], uint16(len(address)))
	copy(payload[2:], address)
	copy(payload[2+len(address):], packet.data)
	wire := make([]byte, frameHeaderSize+len(payload))
	wire[0] = frameTypeDatagram
	binary.BigEndian.PutUint64(wire[1:9], packet.id)
	binary.BigEndian.PutUint32(wire[9:13], uint32(len(payload)))
	copy(wire[13:], payload)
	return wire
}

func TestDatagramWireGoldenVectors(t *testing.T) {
	for _, vector := range []struct {
		name    string
		fixture string
		packet  datagramPacket
	}{
		{name: "ipv4_empty", fixture: "datagram_frames/ipv4_empty.hex", packet: datagramPacket{id: 0x1020304050607080, address: "192.0.2.1:53"}},
		{name: "ipv6_hello", fixture: "datagram_frames/ipv6_hello.hex", packet: datagramPacket{id: 0x2030405060708090, address: "[2001:db8::1]:443", data: []byte("hello")}},
		{name: "domain_hello", fixture: "datagram_frames/domain_hello.hex", packet: datagramPacket{id: 0x30405060708090a0, address: "example.com:443", data: []byte("hello")}},
	} {
		t.Run(vector.name, func(t *testing.T) {
			want := wireFixture(t, vector.fixture)
			if len(vector.packet.data) == 0 {
				if !bytes.Equal(want, datagramWireBytes(vector.packet)) {
					t.Fatalf("empty datagram frame mismatch: got %x want %x", want, datagramWireBytes(vector.packet))
				}
				if err := feedWireBytes(t, want, func(conn net.Conn) error {
					_, err := readDatagramFrame(conn, maxRoutedDatagramSize)
					return err
				}); err != nil {
					t.Fatalf("decode empty datagram frame: %v", err)
				}
				return
			}
			got := captureWireBytes(t, frameHeaderSize+2+len(vector.packet.address)+len(vector.packet.data), func(conn net.Conn) error {
				return writeDatagramFrame(conn, vector.packet)
			})
			if !bytes.Equal(got, want) || !bytes.Equal(got, datagramWireBytes(vector.packet)) {
				t.Fatalf("datagram frame mismatch: got %x want %x", got, want)
			}
		})
	}
}

type datagramBoundaryVector struct {
	Name        string
	ID          string
	Address     string
	PayloadSize int
	Pattern     string
	WireSha256  string
}

func TestDatagramPayloadBoundaryWireVectors(t *testing.T) {
	encoded, err := os.ReadFile(filepath.Join("testdata", "wire", "datagram_frames", "payload_boundaries.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors []datagramBoundaryVector
	if err := json.Unmarshal(encoded, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			if vector.Pattern != "byte(i % 251)" {
				t.Fatalf("unsupported payload pattern %q", vector.Pattern)
			}
			id, err := strconv.ParseUint(strings.TrimPrefix(vector.ID, "0x"), 16, 64)
			if err != nil {
				t.Fatal(err)
			}
			data := make([]byte, vector.PayloadSize)
			for i := range data {
				data[i] = byte(i % 251)
			}
			packet := datagramPacket{id: id, address: vector.Address, data: data}
			var got []byte
			if vector.PayloadSize == 0 {
				got = datagramWireBytes(packet)
			} else {
				got = captureWireBytes(t, frameHeaderSize+2+len(vector.Address)+len(data), func(conn net.Conn) error {
					return writeDatagramFrame(conn, packet)
				})
			}
			if !bytes.Equal(got, datagramWireBytes(packet)) {
				t.Fatalf("full datagram wire bytes differ for %d-byte payload", vector.PayloadSize)
			}
			digest := sha256.Sum256(got)
			if hex.EncodeToString(digest[:]) != vector.WireSha256 {
				t.Fatalf("wire sha256 = %x, want %s (id=%s size=%d prefix=%x)", digest, vector.WireSha256, vector.ID, vector.PayloadSize, got[:min(len(got), 40)])
			}
		})
	}
}

func TestDatagramInvalidWireFixtures(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture string
		want    string
	}{
		{name: "payload_16385", fixture: "invalid/datagram_payload_16385_header.hex", want: "multipath datagram exceeds negotiated maximum"},
		{name: "payload_20480", fixture: "invalid/datagram_payload_20480_header.hex", want: "invalid multipath datagram frame length"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := wireFixture(t, test.fixture)
			if test.name == "payload_16385" {
				data := make([]byte, maxRoutedDatagramSize+1)
				for i := range data {
					data[i] = byte(i % 251)
				}
				full := datagramWireBytes(datagramPacket{id: 0x5000000000004001, address: "192.0.2.1:53", data: data})
				if !bytes.Equal(full[:frameHeaderSize], input) {
					t.Fatalf("16385-byte invalid fixture header mismatch")
				}
				input = full
			}
			conn := &wireReaderConn{Reader: bytes.NewReader(input)}
			_, err := readDatagramFrame(conn, maxRoutedDatagramSize)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	validAddress := strings.Repeat("a", maxDatagramAddressSize)
	valid := datagramPacket{id: 1, address: validAddress, data: []byte{0x01}}
	got := captureWireBytes(t, frameHeaderSize+2+len(validAddress)+len(valid.data), func(conn net.Conn) error {
		return writeDatagramFrame(conn, valid)
	})
	if err := feedWireBytes(t, got, func(conn net.Conn) error {
		_, err := readDatagramFrame(conn, maxRoutedDatagramSize)
		return err
	}); err != nil {
		t.Fatalf("512-byte address was rejected: %v", err)
	}
	if err := writeDatagramFrame(netDiscardConn{}, datagramPacket{address: strings.Repeat("a", maxDatagramAddressSize+1)}); err == nil {
		t.Fatal("513-byte address was accepted")
	}
}

type wireReaderConn struct {
	*bytes.Reader
}

func (wireReaderConn) Write(p []byte) (int, error)      { return len(p), nil }
func (wireReaderConn) Close() error                     { return nil }
func (wireReaderConn) LocalAddr() net.Addr              { return discardAddr{} }
func (wireReaderConn) RemoteAddr() net.Addr             { return discardAddr{} }
func (wireReaderConn) SetDeadline(time.Time) error      { return nil }
func (wireReaderConn) SetReadDeadline(time.Time) error  { return nil }
func (wireReaderConn) SetWriteDeadline(time.Time) error { return nil }

type netDiscardConn struct{}

func (netDiscardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (netDiscardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (netDiscardConn) Close() error                     { return nil }
func (netDiscardConn) LocalAddr() net.Addr              { return discardAddr{} }
func (netDiscardConn) RemoteAddr() net.Addr             { return discardAddr{} }
func (netDiscardConn) SetDeadline(time.Time) error      { return nil }
func (netDiscardConn) SetReadDeadline(time.Time) error  { return nil }
func (netDiscardConn) SetWriteDeadline(time.Time) error { return nil }

type discardAddr struct{}

func (discardAddr) Network() string { return "discard" }
func (discardAddr) String() string  { return "discard" }

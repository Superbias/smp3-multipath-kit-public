package smp3core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestStreamFrameEncodeGoldenHeaders(t *testing.T) {
	for _, test := range []struct {
		name   string
		header StreamFrameHeader
		want   string
	}{
		{name: "data", header: StreamFrameHeader{Type: StreamFrameData, Value: 0x0102030405060708, Length: 5}, want: "01010203040506070800000005"},
		{name: "activate", header: StreamFrameHeader{Type: StreamFrameActivate, Value: 0x1122334455667788}, want: "02112233445566778800000000"},
		{name: "ack", header: StreamFrameHeader{Type: StreamFrameACK, Value: 42}, want: "03000000000000002a00000000"},
		{name: "close", header: StreamFrameHeader{Type: StreamFrameClose, Value: 0xdeadbeef}, want: "0400000000deadbeef00000000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			want, err := hex.DecodeString(test.want)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, StreamFrameHeaderSize)
			if err := EncodeStreamFrameHeader(got, test.header); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("encoded=%x want=%x", got, want)
			}
		})
	}
}

func TestStreamFrameDecodeGoldenHeaders(t *testing.T) {
	for _, test := range []struct {
		name string
		wire string
		want StreamFrameHeader
	}{
		{name: "data", wire: "01010203040506070800000005", want: StreamFrameHeader{Type: StreamFrameData, Value: 0x0102030405060708, Length: 5}},
		{name: "activate", wire: "02112233445566778800000000", want: StreamFrameHeader{Type: StreamFrameActivate, Value: 0x1122334455667788}},
		{name: "ack", wire: "03000000000000002a00000000", want: StreamFrameHeader{Type: StreamFrameACK, Value: 42}},
		{name: "close", wire: "0400000000deadbeef00000000", want: StreamFrameHeader{Type: StreamFrameClose, Value: 0xdeadbeef}},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire, err := hex.DecodeString(test.wire)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadStreamFrameHeader(bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("decoded=%+v want=%+v", got, test.want)
			}
		})
	}
}

func TestStreamFrameRejectsInvalidHeaders(t *testing.T) {
	valid := func(header StreamFrameHeader) []byte {
		wire := make([]byte, StreamFrameHeaderSize)
		if err := EncodeStreamFrameHeader(wire, header); err != nil {
			t.Fatal(err)
		}
		return wire
	}
	for _, test := range []struct {
		name    string
		wire    []byte
		wantErr string
	}{
		{name: "unknown_type", wire: append([]byte{9}, make([]byte, StreamFrameHeaderSize-1)...), wantErr: "unknown multipath frame type"},
		{name: "control_length", wire: func() []byte {
			wire := valid(StreamFrameHeader{Type: StreamFrameACK, Value: 1})
			wire[12] = 1
			return wire
		}(), wantErr: "invalid multipath control frame length"},
		{name: "zero_data_length", wire: func() []byte {
			wire := make([]byte, StreamFrameHeaderSize)
			wire[0] = byte(StreamFrameData)
			return wire
		}(), wantErr: "invalid multipath frame length"},
		{name: "truncated_header", wire: make([]byte, StreamFrameHeaderSize-1), wantErr: "unexpected EOF"},
		{name: "oversized_data_length", wire: func() []byte {
			wire := valid(StreamFrameHeader{Type: StreamFrameData, Value: 1, Length: MaxStreamFramePayload})
			wire[9] = 0x10
			wire[10] = 0x00
			return wire
		}(), wantErr: "invalid multipath frame length"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadStreamFrameHeader(bytes.NewReader(test.wire))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error=%v want substring=%q", err, test.wantErr)
			}
		})
	}
	if err := EncodeStreamFrameHeader(make([]byte, StreamFrameHeaderSize-1), StreamFrameHeader{Type: StreamFrameACK}); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("short destination error=%v", err)
	}
}

package multipath

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
)

func TestSingDatagramPacketConnReaderMTUAndHeadroom(t *testing.T) {
	core, packetConn := newDatagramCore(datagramConfig{MaxDatagramSize: maxRoutedDatagramSize})
	defer core.Close()
	adapter := newSingDatagramPacketConn(packetConn)
	if got := adapter.ReaderMTU(); got != maxRoutedDatagramSize {
		t.Fatalf("ReaderMTU = %d, want %d", got, maxRoutedDatagramSize)
	}

	const maxSocks5UDPHeader = 3 + 1 + 255 + 2
	for _, test := range []struct {
		name   string
		header int
	}{
		{name: "ipv4", header: 3 + 1 + 4 + 2},
		{name: "ipv6", header: 3 + 1 + 16 + 2},
		{name: "max-domain", header: maxSocks5UDPHeader},
	} {
		t.Run(test.name, func(t *testing.T) {
			if maxRoutedDatagramSize+test.header > (1<<16)-1 {
				t.Fatalf("payload plus %s header exceeds scoped UDP buffer", test.name)
			}
			core.injectDatagram("198.51.100.8:53", make([]byte, maxRoutedDatagramSize))
			buffer := buf.NewSize(maxRoutedDatagramSize + test.header)
			buffer.Resize(test.header, 0)
			destination, err := adapter.ReadPacket(buffer)
			if err != nil {
				buffer.Release()
				t.Fatal(err)
			}
			if destination.String() != "198.51.100.8:53" || buffer.Len() != maxRoutedDatagramSize {
				buffer.Release()
				t.Fatalf("destination=%v payload length=%d", destination, buffer.Len())
			}
			buffer.Release()
		})
	}
}

func TestSingDatagramPacketConnShortBufferDoesNotCloseCore(t *testing.T) {
	core, packetConn := newDatagramCore(datagramConfig{MaxDatagramSize: maxRoutedDatagramSize})
	defer core.Close()
	adapter := newSingDatagramPacketConn(packetConn)
	core.injectDatagram("198.51.100.8:53", make([]byte, maxRoutedDatagramSize))
	buffer := buf.NewSize(1200)
	_, err := adapter.ReadPacket(buffer)
	buffer.Release()
	if !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("short buffer error = %v, want io.ErrShortBuffer", err)
	}
	select {
	case <-core.Done():
		t.Fatal("short buffer closed the logical datagram core")
	default:
	}
}

func TestSingDatagramPacketConnOversizeIsolatedOnSameAssociation(t *testing.T) {
	core, packetConn := newDatagramCore(datagramConfig{
		Mode: datagramModeAdaptive, MaxDatagramSize: maxRoutedDatagramSize, QueueFrames: 8,
	})
	defer core.Close()
	local, peer := net.Pipe()
	defer peer.Close()
	if err := core.addLeg(0, local, nil); err != nil {
		t.Fatal(err)
	}
	adapter := newSingDatagramPacketConn(packetConn)
	destination := M.ParseSocksaddr("192.0.2.1:53")
	readFrames := make(chan []datagramPacket, 1)
	readErrors := make(chan error, 1)
	go func() {
		frames := make([]datagramPacket, 0, 2)
		for len(frames) < 2 {
			frame, err := readDatagramFrame(peer, maxRoutedDatagramSize)
			if err != nil {
				readErrors <- err
				return
			}
			frames = append(frames, frame)
		}
		readFrames <- frames
	}()

	writePacket := func(size int) error {
		buffer := buf.NewSize(size)
		if _, err := buffer.Write(make([]byte, size)); err != nil {
			buffer.Release()
			return err
		}
		return adapter.WritePacket(buffer, destination)
	}
	if err := writePacket(1200); err != nil {
		t.Fatal(err)
	}
	if err := writePacket(maxRoutedDatagramSize + 1); err != nil {
		t.Fatal(err)
	}
	if err := writePacket(maxRoutedDatagramSize); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-readErrors:
		t.Fatal(err)
	case frames := <-readFrames:
		if len(frames) != 2 || len(frames[0].data) != 1200 || len(frames[1].data) != maxRoutedDatagramSize {
			t.Fatalf("wire frames after oversize = %d (%d,%d)", len(frames), len(frames[0].data), len(frames[1].data))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for valid datagrams after oversize")
	}
	if got := core.txSequence(); got != 2 {
		t.Fatalf("tx sequence after valid/oversize/valid = %d, want 2", got)
	}
}

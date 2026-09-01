package multipath

import (
	"crypto/rand"
	"errors"
	"net"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

var helloMagic = [4]byte{'S', 'M', 'P', '3'}

const (
	// helloVersion remains the ordered stream HELLO used by alpha2.2/alpha2.3-r10.
	// R11 deliberately keeps writing v4 for TCP so an r11 client can still use an
	// r10 server for the stream data plane during a staged upgrade.
	helloVersion byte = 4
	// Datagram mode extends HELLO with one explicit mode byte. Only the new UDP
	// data plane writes v5; an r11 server accepts both v4 stream and v5 datagram.
	helloVersionDatagram byte = 5
)

const helloSkew = 90 * time.Second

type helloMode byte

const (
	helloModeStream helloMode = iota
	helloModeDatagram
)

func (m helloMode) String() string {
	if m == helloModeDatagram {
		return "datagram"
	}
	return "stream"
}

type helloMessage struct {
	Session     [16]byte
	LegID       uint8
	Mode        helloMode
	Destination string
}

func newSessionID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func writeHello(conn net.Conn, message helloMessage, password string) error {
	if err := validateHelloMessage(message, password); err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	header, dest, sum, err := encodeHelloParts(message, password, time.Now().Unix(), nonce)
	if err != nil {
		return err
	}
	buffers := net.Buffers{header, dest, sum}
	_, err = buffers.WriteTo(conn)
	return err
}

// encodeHelloParts keeps the protocol bytes deterministic for wire tests while
// leaving writeHello's random nonce, current timestamp, and write segmentation
// unchanged in production.
func encodeHelloParts(message helloMessage, password string, timestamp int64, nonce [16]byte) ([]byte, []byte, []byte, error) {
	if err := validateHelloMessage(message, password); err != nil {
		return nil, nil, nil, err
	}
	version := smp3core.Version4
	if message.Mode == helloModeDatagram {
		version = smp3core.Version5
	}
	return smp3core.EncodeHelloParts(smp3core.Hello{
		Version:     version,
		SessionID:   smp3core.SessionID(message.Session),
		LegID:       smp3core.LegID(message.LegID),
		Mode:        smp3core.HelloMode(message.Mode),
		Timestamp:   timestamp,
		Nonce:       nonce,
		Destination: message.Destination,
	}, []byte(password))
}

func validateHelloMessage(message helloMessage, password string) error {
	if password == "" {
		return errors.New("empty multipath password")
	}
	if len(message.Destination) == 0 || len(message.Destination) > 65535 {
		return errors.New("invalid multipath destination")
	}
	if message.LegID > 1 {
		return errors.New("invalid multipath leg id")
	}
	if message.Mode != helloModeStream && message.Mode != helloModeDatagram {
		return errors.New("invalid multipath hello mode")
	}
	return nil
}

func readHello(conn net.Conn, password string) (helloMessage, [16]byte, error) {
	return readHelloAt(conn, password, time.Now())
}

func readHelloAt(conn net.Conn, password string, now time.Time) (helloMessage, [16]byte, error) {
	coreHello, err := smp3core.ReadHelloAt(conn, []byte(password), now)
	if err != nil {
		return helloMessage{}, [16]byte{}, err
	}
	return helloMessage{
		Session:     [16]byte(coreHello.SessionID),
		LegID:       uint8(coreHello.LegID),
		Mode:        helloMode(coreHello.Mode),
		Destination: coreHello.Destination,
	}, coreHello.Nonce, nil
}

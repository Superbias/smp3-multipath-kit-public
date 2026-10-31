package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testCarrierALPN = "smp3/1"

func makeClientCarrierCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, certPEM
}

func writeClientCarrierTrustFile(t *testing.T, certPEM []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "root-ca.pem")
	if err := os.WriteFile(path, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func startClientCarrierTLSEcho(t *testing.T, certificate tls.Certificate) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			raw, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer raw.Close()
				conn := tls.Server(raw, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, NextProtos: []string{testCarrierALPN}})
				if conn.Handshake() != nil {
					return
				}
				var request [5]byte
				if _, readErr := io.ReadFull(conn, request[:]); readErr == nil {
					_, _ = conn.Write(request[:])
				}
			}()
		}
	}()
	return listener.Addr().String()
}

func TestStandaloneCarrierTLSVerifiedAndALPNBound(t *testing.T) {
	certificate, certPEM := makeClientCarrierCertificate(t)
	address := startClientCarrierTLSEcho(t, certificate)
	clientTLS, err := newCarrierTLSConfig(CarrierTLSOptions{Enabled: true, ServerName: "localhost", RootCAFile: writeClientCarrierTrustFile(t, certPEM)})
	if err != nil {
		t.Fatal(err)
	}
	instance := &Client{cfg: Config{SMP3: SMP3Options{CarrierReadyTimeout: Duration(time.Second)}}, carrierTLS: clientTLS}
	raw, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	secured, err := instance.secureCarrier(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	defer secured.Close()
	if got := secured.(*tls.Conn).ConnectionState().NegotiatedProtocol; got != testCarrierALPN {
		t.Fatalf("negotiated ALPN=%q, want %q", got, testCarrierALPN)
	}
	if _, err := secured.Write([]byte("HELLO")); err != nil {
		t.Fatal(err)
	}
	var reply [5]byte
	if _, err := io.ReadFull(secured, reply[:]); err != nil {
		t.Fatal(err)
	}
	if string(reply[:]) != "HELLO" {
		t.Fatalf("TLS payload=%q", reply)
	}
}

func TestStandaloneCarrierTLSRejectsWrongServerIdentity(t *testing.T) {
	serverCertificate, _ := makeClientCarrierCertificate(t)
	_, trustedPEM := makeClientCarrierCertificate(t)
	address := startClientCarrierTLSEcho(t, serverCertificate)
	clientTLS, err := newCarrierTLSConfig(CarrierTLSOptions{Enabled: true, ServerName: "localhost", RootCAFile: writeClientCarrierTrustFile(t, trustedPEM)})
	if err != nil {
		t.Fatal(err)
	}
	instance := &Client{cfg: Config{SMP3: SMP3Options{CarrierReadyTimeout: Duration(time.Second)}}, carrierTLS: clientTLS}
	raw, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.secureCarrier(context.Background(), raw); err == nil {
		t.Fatal("TLS accepted a server certificate outside the configured trust root")
	}
}

func TestStandaloneCarrierTLSRejectsALPNMismatch(t *testing.T) {
	certificate, certPEM := makeClientCarrierCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer raw.Close()
		conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{"http/1.1"}})
		_ = conn.Handshake()
	}()
	clientTLS, err := newCarrierTLSConfig(CarrierTLSOptions{Enabled: true, ServerName: "localhost", RootCAFile: writeClientCarrierTrustFile(t, certPEM)})
	if err != nil {
		t.Fatal(err)
	}
	instance := &Client{cfg: Config{SMP3: SMP3Options{CarrierReadyTimeout: Duration(time.Second)}}, carrierTLS: clientTLS}
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.secureCarrier(context.Background(), raw); err == nil {
		t.Fatal("TLS accepted a peer without SMP3 carrier ALPN")
	}
}

func TestStandaloneCarrierTLSRemainsDisabledByDefault(t *testing.T) {
	if got, err := newCarrierTLSConfig(CarrierTLSOptions{}); err != nil || got != nil {
		t.Fatalf("default TLS config = %v, err=%v; want disabled", got, err)
	}
	cfg := DefaultConfig()
	cfg.SMP3.Password = "phase30-test-secret"
	cfg.SMP3.Routes = RouteOptions{Leg0: "127.0.0.1:24444", Leg1: "127.0.0.1:24444"}
	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatalf("legacy config should remain valid with TLS disabled: %v", err)
	}
}

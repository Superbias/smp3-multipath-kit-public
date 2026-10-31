package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeServerCarrierCertificate(t *testing.T) (string, string, []byte) {
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
	dir := t.TempDir()
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certPath, keyPath := filepath.Join(dir, "server.pem"), filepath.Join(dir, "server-key.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, certPEM
}

func TestStandaloneCarrierTLSListenerAndPlaintextNoFallback(t *testing.T) {
	certPath, keyPath, certPEM := makeServerCarrierCertificate(t)
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:0"
	cfg.Password = "phase30-test-secret"
	cfg.CarrierTLS = CarrierTLSOptions{CertFile: certPath, KeyFile: keyPath}
	instance, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("test certificate could not be added to trust roots")
	}
	secured, err := tls.Dial("tcp", instance.Addr().String(), &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost", RootCAs: roots, NextProtos: []string{standaloneCarrierALPN}})
	if err != nil {
		t.Fatal(err)
	}
	state := secured.ConnectionState()
	if state.NegotiatedProtocol != standaloneCarrierALPN {
		t.Fatalf("negotiated ALPN=%q, want %q", state.NegotiatedProtocol, standaloneCarrierALPN)
	}
	_ = secured.Close()

	plain, err := net.DialTimeout("tcp", instance.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_ = plain.SetDeadline(time.Now().Add(time.Second))
	if _, err := plain.Write([]byte("SMP3 plaintext must not be accepted")); err != nil {
		t.Fatal(err)
	}
	var response [1]byte
	if _, err := plain.Read(response[:]); err == nil {
		t.Fatal("TLS listener returned application data to a plaintext carrier")
	}
}

func TestStandaloneCarrierTLSCertificateAndKeyMustBePaired(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CarrierTLS.CertFile = "only-cert.pem"
	if err := cfg.NormalizeAndValidate(); err == nil {
		t.Fatal("configuration accepted a certificate without a key")
	}
}

func TestStandaloneCarrierTLSRejectsInvalidCertificateAtConstruction(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Password = "phase30-test-secret"
	cfg.CarrierTLS = CarrierTLSOptions{CertFile: "missing-cert.pem", KeyFile: "missing-key.pem"}
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("server construction accepted a missing TLS certificate pair")
	}
}

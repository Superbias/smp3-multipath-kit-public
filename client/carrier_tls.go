package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

const standaloneCarrierALPN = "smp3/1"

func newCarrierTLSConfig(options CarrierTLSOptions) (*tls.Config, error) {
	if !options.Enabled {
		return nil, nil
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: options.ServerName,
		NextProtos: []string{standaloneCarrierALPN},
	}
	if options.RootCAFile != "" {
		pem, err := os.ReadFile(options.RootCAFile)
		if err != nil {
			return nil, fmt.Errorf("read SMP3 carrier root CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("SMP3 carrier root CA file contains no valid PEM certificates")
		}
		config.RootCAs = roots
	}
	return config, nil
}

func (c *Client) secureCarrier(ctx context.Context, conn net.Conn) (net.Conn, error) {
	return c.secureCarrierWithRuntime(ctx, conn, c.currentRuntime())
}

func (c *Client) secureCarrierWithRuntime(ctx context.Context, conn net.Conn, runtime *clientRuntimeConfig) (net.Conn, error) {
	if runtime == nil || runtime.carrierTLS == nil {
		return conn, nil
	}
	timeout := runtime.cfg.SMP3.CarrierReadyTimeout.Time()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set SMP3 carrier TLS deadline: %w", err)
	}
	secured := tls.Client(conn, runtime.carrierTLS.Clone())
	if err := secured.HandshakeContext(handshakeCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("SMP3 carrier TLS handshake: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clear SMP3 carrier TLS deadline: %w", err)
	}
	if secured.ConnectionState().NegotiatedProtocol != standaloneCarrierALPN {
		_ = conn.Close()
		return nil, fmt.Errorf("SMP3 carrier TLS ALPN mismatch: got %q", secured.ConnectionState().NegotiatedProtocol)
	}
	return secured, nil
}

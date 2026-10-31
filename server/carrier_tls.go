package server

import (
	"crypto/tls"
	"fmt"
)

const standaloneCarrierALPN = "smp3/1"

func newCarrierTLSConfig(options CarrierTLSOptions) (*tls.Config, error) {
	if options.CertFile == "" && options.KeyFile == "" {
		return nil, nil
	}
	certificate, err := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load SMP3 carrier TLS certificate/key: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{standaloneCarrierALPN},
	}, nil
}

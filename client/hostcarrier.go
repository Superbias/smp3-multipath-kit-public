package client

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

type hostCarrierOpener interface {
	Open(context.Context, smp3core.SessionID, uint8) (net.Conn, error)
}

type hostCarrierOpenerImpl struct {
	cfg HostCarrierOptions
}

type hostOpenRequest struct {
	Op            string `json:"op"`
	RequestID     string `json:"request_id"`
	LegID         uint8  `json:"leg_id"`
	CarrierHandle string `json:"carrier_handle"`
	TargetHandle  string `json:"target_handle"`
}

type hostOpenResponse struct {
	Op        string `json:"op"`
	RequestID string `json:"request_id"`
	Endpoint  string `json:"endpoint"`
	Token     string `json:"token"`
	ErrorCode string `json:"error_code"`
}

func newHostCarrierOpener(cfg HostCarrierOptions) hostCarrierOpener {
	return &hostCarrierOpenerImpl{cfg: cfg}
}

func (o *hostCarrierOpenerImpl) Open(ctx context.Context, sessionID smp3core.SessionID, legID uint8) (net.Conn, error) {
	dialer := net.Dialer{}
	control, err := dialer.DialContext(ctx, "tcp", o.cfg.ControlAddress)
	if err != nil {
		return nil, fmt.Errorf("host carrier control: %w", err)
	}
	defer control.Close()
	requestID := hex.EncodeToString(sessionID[:])
	request := hostOpenRequest{
		Op:        "OPEN",
		RequestID: requestID,
		LegID:     legID,
		CarrierHandle: func() string {
			if legID == 0 {
				return o.cfg.Leg0Handle
			}
			return o.cfg.Leg1Handle
		}(),
		TargetHandle: o.cfg.TargetHandle,
	}
	if err := json.NewEncoder(control).Encode(request); err != nil {
		return nil, fmt.Errorf("host carrier OPEN: %w", err)
	}
	_ = control.SetReadDeadline(time.Now().Add(o.cfg.ConnectTimeout.Time()))
	var response hostOpenResponse
	if err := json.NewDecoder(bufio.NewReader(control)).Decode(&response); err != nil {
		return nil, fmt.Errorf("host carrier OPEN response: %w", err)
	}
	if response.Op != "OPEN_OK" {
		if response.ErrorCode == "" {
			response.ErrorCode = "unknown"
		}
		return nil, fmt.Errorf("host carrier OPEN_ERR: %s", response.ErrorCode)
	}
	data, err := dialer.DialContext(ctx, "tcp", response.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("host carrier data stream: %w", err)
	}
	if _, err := fmt.Fprintf(data, "%s\n", response.Token); err != nil {
		_ = data.Close()
		return nil, fmt.Errorf("host carrier data authentication: %w", err)
	}
	return data, nil
}

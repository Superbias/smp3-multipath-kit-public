package adapter

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestSMP3ProxyConfigBootstrap(t *testing.T) {
	proxy, err := ParseProxy(map[string]any{
		"type":     "smp3",
		"name":     "mp-jp",
		"server":   "10.66.66.1",
		"port":     24444,
		"password": "placeholder",
		"legs": []any{
			map[string]any{"proxy": "line-path"},
			map[string]any{"proxy": "public-hy2"},
		},
		"leg1-fallback":  "public-snell",
		"scheduler-mode": "adaptive",
		"udp": map[string]any{
			"enabled":           true,
			"mode":              "adaptive",
			"max-datagram-size": 16384,
			"idle-timeout":      "2m",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if proxy.Name() != "mp-jp" {
		t.Fatalf("unexpected name %q", proxy.Name())
	}
	if proxy.Type() != C.Relay {
		t.Fatalf("unexpected type %v", proxy.Type())
	}
	if !proxy.SupportUDP() {
		t.Fatal("B2-complete SMP3 adapter must advertise UDP capability")
	}
}

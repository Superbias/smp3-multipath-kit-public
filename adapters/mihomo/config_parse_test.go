package config

import "testing"

func TestSMP3ConfigParserBuildsRealProxyGraph(t *testing.T) {
	_, _, err := parseProxies(&RawConfig{Proxy: []map[string]any{
		{"name": "line-path", "type": "direct"},
		{"name": "public-hy2", "type": "direct"},
		{"name": "public-snell", "type": "direct"},
		{"name": "mp-jp", "type": "smp3", "server": "127.0.0.1", "port": 24444, "password": "test-password", "legs": []map[string]any{{"proxy": "line-path"}, {"proxy": "public-hy2"}}, "leg1-fallback": "public-snell", "scheduler-mode": "adaptive"},
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSMP3ConfigParserRejectsMissingChild(t *testing.T) {
	_, _, err := parseProxies(&RawConfig{Proxy: []map[string]any{
		{"name": "mp-jp", "type": "smp3", "server": "127.0.0.1", "port": 24444, "password": "test-password", "legs": []map[string]any{{"proxy": "missing-leg"}, {"proxy": "another-missing-leg"}}},
	}})
	if err == nil {
		t.Fatal("missing SMP3 child proxy was accepted")
	}
}

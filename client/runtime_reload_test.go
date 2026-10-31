package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func configWithLegCount(count int) Config {
	cfg := validTestConfig()
	addresses := []string{"24441", "24442", "24443", "24444", "24445", "24446", "24447", "24448", "24449", "24450", "24451", "24452", "24453", "24454", "24455", "24456"}
	cfg.SMP3.Routes = RouteOptions{Legs: make([]RouteLegOptions, count)}
	for id := range cfg.SMP3.Routes.Legs {
		cfg.SMP3.Routes.Legs[id] = RouteLegOptions{ID: uint8(id), Name: fmt.Sprintf("leg%d", id), Address: "127.0.0.1:" + addresses[id]}
	}
	cfg.SMP3.Stream.MaxActiveLegs = 16
	cfg.SMP3.Stream.BandwidthMbps = make([]uint32, count)
	for id := range cfg.SMP3.Stream.BandwidthMbps {
		cfg.SMP3.Stream.BandwidthMbps[id] = 100
	}
	return cfg
}

func TestRuntimeReloadValidatesAndCommitsTailRouteChange(t *testing.T) {
	client, err := New(configWithLegCount(1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	result, err := client.Reload(context.Background(), configWithLegCount(4), RuntimeReloadOptions{})
	if err != nil {
		t.Fatalf("Reload add 1→4 without live Sessions: %v", err)
	}
	if !result.Committed || result.Partial || len(result.AddedLegIDs) != 3 || result.ConfigVersion != 1 {
		t.Fatalf("unexpected reload result: %+v", result)
	}
	if got := len(client.currentRuntime().cfg.SMP3.Routes.entries()); got != 4 {
		t.Fatalf("committed route count=%d, want 4", got)
	}

	result, err = client.Reload(context.Background(), configWithLegCount(2), RuntimeReloadOptions{})
	if err != nil {
		t.Fatalf("Reload remove 4→2 without live Sessions: %v", err)
	}
	if !result.Committed || len(result.RemovedLegIDs) != 2 || result.ConfigVersion != 2 {
		t.Fatalf("unexpected route removal result: %+v", result)
	}
}

func TestRuntimeReloadRejectsInvalidAndRestartOnlyChanges(t *testing.T) {
	client, err := New(configWithLegCount(2))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	invalid := configWithLegCount(3)
	invalid.SMP3.Routes.Legs[2].Address = invalid.SMP3.Routes.Legs[0].Address
	if _, err := client.Reload(context.Background(), invalid, RuntimeReloadOptions{}); err == nil {
		t.Fatal("duplicate route endpoint unexpectedly accepted")
	}
	if got := len(client.currentRuntime().cfg.SMP3.Routes.entries()); got != 2 {
		t.Fatalf("invalid candidate changed active config: routes=%d", got)
	}

	restartOnly := configWithLegCount(2)
	restartOnly.SMP3.Password = "new-identity"
	if _, err := client.Reload(context.Background(), restartOnly, RuntimeReloadOptions{}); !errors.Is(err, ErrReloadRequiresRestart) {
		t.Fatalf("identity change error=%v, want ErrReloadRequiresRestart", err)
	}
	if got := client.currentRuntime().cfg.SMP3.Password; got != "test-password" {
		t.Fatalf("rejected identity change committed: %q", got)
	}

	tooMany := configWithLegCount(3)
	tooMany.SMP3.Stream.MaxActiveLegs = 2
	if _, err := client.Reload(context.Background(), tooMany, RuntimeReloadOptions{}); err == nil {
		t.Fatal("route count above MaxActiveLegs unexpectedly accepted")
	}
}

func TestRuntimeReloadRejectsInvalidTLSWithoutChangingCurrent(t *testing.T) {
	client, err := New(configWithLegCount(2))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	invalidTLS := configWithLegCount(3)
	invalidTLS.SMP3.CarrierTLS.Enabled = true
	invalidTLS.SMP3.CarrierTLS.ServerName = ""
	if _, err := client.Reload(context.Background(), invalidTLS, RuntimeReloadOptions{}); err == nil {
		t.Fatal("invalid TLS identity was accepted")
	}
	if got := len(client.currentRuntime().cfg.SMP3.Routes.entries()); got != 2 {
		t.Fatalf("invalid TLS candidate changed current route set: got %d, want 2", got)
	}
	if client.currentRuntime().cfg.SMP3.CarrierTLS.Enabled {
		t.Fatal("invalid TLS candidate changed active TLS configuration")
	}
}

func TestRuntimeReloadConcurrentWithClientClose(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		client, err := New(configWithLegCount(1))
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = client.Reload(context.Background(), configWithLegCount(2), RuntimeReloadOptions{})
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = client.Close()
		}()
		close(start)
		wg.Wait()
		if !client.isClosed() {
			t.Fatalf("iteration %d: Client did not close", iteration)
		}
	}
}

func TestRuntimeReloadMarkerTriggersOnlyExplicitReload(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "client.json")
	cfg := configWithLegCount(1)
	encoded, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.WatchConfigReloadMarker(watchCtx, configPath, 10*time.Millisecond)

	request, err := RequestConfigReload(configPath, false)
	if err != nil {
		t.Fatal(err)
	}
	resultPath := client.ReloadResultPath(configPath)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(resultPath)
		if readErr == nil {
			var result RuntimeReloadResult
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			if result.OperationID != request.RequestID || !result.Committed || result.Error != "" {
				t.Fatalf("unexpected reload marker result: %+v", result)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("local reload marker was not consumed and reported")
}

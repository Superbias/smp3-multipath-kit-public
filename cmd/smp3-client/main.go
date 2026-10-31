package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	smp3client "github.com/Superbias/smp3-multipath-kit-public/client"
)

func main() {
	configPath := flag.String("c", "", "path to standalone SMP3 sidecar JSON config")
	check := flag.Bool("check", false, "validate config without binding or starting")
	version := flag.Bool("version", false, "print standalone sidecar version")
	reload := flag.Bool("reload", false, "request the running client to reload its config through a local marker")
	forceReload := flag.Bool("force-reload", false, "request a reload that forcibly removes deleted Legs; outstanding DATA uses Core recovery")
	flag.Parse()

	if *version {
		fmt.Println(smp3client.Version)
		return
	}
	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := smp3client.LoadConfigFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	if *reload || *forceReload {
		request, err := smp3client.RequestConfigReload(*configPath, *forceReload)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reload request error:", err)
			os.Exit(1)
		}
		fmt.Printf("reload request queued: id=%s marker=%s\n", request.RequestID, *configPath+".reload.request")
		return
	}
	if *check {
		instance, err := smp3client.New(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "client config error:", err)
			os.Exit(1)
		}
		_ = instance.Close()
		upstreams := cfg.LegUpstreamAddresses()
		fmt.Printf("config OK: mode=Standalone listen=%s leg0_upstream_socks=%s leg1_upstream_socks=%s connect_timeout=%s\n", cfg.Listen, upstreams[0], upstreams[1], cfg.UpstreamSocks.ConnectTimeout.Time())
		return
	}
	instance, err := smp3client.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client error:", err)
		os.Exit(1)
	}
	if err := instance.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "client start error:", err)
		os.Exit(1)
	}
	watchCtx, stopWatch := context.WithCancel(context.Background())
	instance.WatchConfigReloadMarker(watchCtx, *configPath, 500*time.Millisecond)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)}))
	slog.SetDefault(logger)
	logger.Info("SMP3 standalone sidecar listening", "listen", instance.Addr().String())

	signals := make(chan os.Signal, 1)
	registerTerminationSignals(signals)
	reloadSignals := make(chan os.Signal, 1)
	registerReloadSignals(reloadSignals)
	for {
		select {
		case <-signals:
			stopWatch()
			_ = instance.Close()
			return
		case <-reloadSignals:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			result, reloadErr := instance.ReloadFromFile(ctx, *configPath, smp3client.RuntimeReloadOptions{})
			cancel()
			if reloadErr != nil {
				logger.Error("SMP3 runtime reload failed", "operation_id", result.OperationID, "error", reloadErr, "partial", result.Partial, "compensations", result.Compensations)
			} else {
				logger.Info("SMP3 runtime reload committed", "operation_id", result.OperationID, "config_version", result.ConfigVersion, "sessions", result.SessionCount, "added_legs", result.AddedLegIDs, "removed_legs", result.RemovedLegIDs)
			}
		}
	}
}

func parseLogLevel(value string) slog.Level {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

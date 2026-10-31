package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Superbias/smp3-multipath-kit-public/front"
)

func main() {
	configPath := flag.String("c", "", "path to TCP-only SMP3 Front JSON config")
	check := flag.Bool("check", false, "validate config without listening")
	version := flag.Bool("version", false, "print SMP3 Front version")
	flag.Parse()
	if *version {
		fmt.Println(front.Version)
		return
	}
	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := front.LoadConfigFile(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	if *check {
		fmt.Printf("config OK: tcp_listen=%s landing_target=%s max_connections=%d\n", cfg.Listen, cfg.LandingTarget, cfg.MaxConnections)
		return
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))
	relay, err := front.New(cfg, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "front error:", err)
		os.Exit(1)
	}
	if err := relay.Start(); err != nil {
		logger.Error("front start failed", "error", err)
		os.Exit(1)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout.Time())
	defer cancel()
	if err := relay.Shutdown(ctx); err != nil {
		logger.Error("front shutdown forced after grace period", "error", err)
		os.Exit(1)
	}
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
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

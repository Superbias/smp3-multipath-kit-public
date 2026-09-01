package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	server "github.com/Superbias/smp3-multipath-kit-public/server"
)

func main() {
	configPath := flag.String("c", "", "path to standalone SMP3 JSON config")
	check := flag.Bool("check", false, "validate config without binding or starting")
	version := flag.Bool("version", false, "print standalone server version")
	flag.Parse()

	if *version {
		fmt.Println(server.Version)
		return
	}
	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	if *check {
		fmt.Printf("config OK: listen=%s udp_enabled=%t\n", cfg.Listen, cfg.UDP.Enabled)
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	instance, err := server.New(cfg, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server error:", err)
		os.Exit(1)
	}
	if err := instance.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "server start error:", err)
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	registerTerminationSignals(signals)
	<-signals
	_ = instance.Close()
}

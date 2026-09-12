package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	smp3client "github.com/Superbias/smp3-multipath-kit-public/client"
)

func main() {
	configPath := flag.String("c", "", "path to standalone SMP3 sidecar JSON config")
	check := flag.Bool("check", false, "validate config without binding or starting")
	version := flag.Bool("version", false, "print standalone sidecar version")
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
	if *check {
		fmt.Printf("config OK: listen=%s upstream_socks=%s connect_timeout=%s\n", cfg.Listen, cfg.UpstreamSocks.Address, cfg.UpstreamSocks.ConnectTimeout.Time())
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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("SMP3 standalone sidecar listening", "listen", instance.Addr().String())

	signals := make(chan os.Signal, 1)
	registerTerminationSignals(signals)
	<-signals
	_ = instance.Close()
}

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Superbias/smp3-multipath-kit-public/panel"
	"github.com/Superbias/smp3-multipath-kit-public/panel/monitor"
)

func main() {
	defaults := monitor.DefaultConfig()
	listen := flag.String("listen", defaults.Listen, "literal loopback address for the Panel")
	telemetry := flag.String("telemetry", defaults.TelemetryURL, "literal loopback telemetry URL")
	history := flag.String("history", "monitor-history.jsonl", "local read-only monitor history file")
	flag.Parse()
	config := defaults
	config.Listen, config.TelemetryURL, config.PersistentPath = *listen, *telemetry, *history
	server, err := panel.New(config)
	if err != nil {
		log.Fatal(err)
	}
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("SMP3 read-only Panel listening on %s", server.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	if err := server.Close(); err != nil {
		log.Printf("Panel shutdown: %v", err)
	}
}

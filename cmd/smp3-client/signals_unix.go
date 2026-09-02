//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func registerTerminationSignals(channel chan<- os.Signal) {
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM)
}

//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func registerReloadSignals(channel chan<- os.Signal) {
	signal.Notify(channel, syscall.SIGHUP)
}

//go:build windows

package main

import (
	"os"
	"os/signal"
)

func registerTerminationSignals(channel chan<- os.Signal) {
	signal.Notify(channel, os.Interrupt)
}

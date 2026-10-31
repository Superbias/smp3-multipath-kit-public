//go:build windows

package main

import "os"

func registerReloadSignals(channel chan<- os.Signal) {}

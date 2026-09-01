package server

import (
	"encoding/hex"
	"log/slog"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

func sessionLogID(id smp3core.SessionID) string { return hex.EncodeToString(id[:4]) }

func loggerOrDefault(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

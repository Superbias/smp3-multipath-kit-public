# Upgrade alpha2.1 -> alpha2.2

alpha2.2 keeps the same JSON configuration schema, secrets, ports, child outbounds and SMP3 aggregation addresses. Only the binaries need to be rebuilt/replaced.

**Upgrade both Windows and landing-server binaries together.** alpha2.2 bumps the SMP3 hello version byte to 4 because it adds the logical CLOSE frame. Mixing alpha2.1 and alpha2.2 is intentionally rejected at handshake time.

## Build

```bash
cd smp3-multipath-kit-alpha2.2
mkdir -p ~/go-tmp ~/go-build-cache
GOTMPDIR="$HOME/go-tmp" GOCACHE="$HOME/go-build-cache" ./validate-kit.sh
GOTMPDIR="$HOME/go-tmp" GOCACHE="$HOME/go-build-cache" ./build.sh
```

Outputs remain:

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
```

## Live regression test

Keep the existing configs. Replace both binaries, start both processes, then repeat the same 256 MiB local landing test without restarting any process during the transfer. Expected behavior:

```text
preferred leg connects
booster activates
leg 1 joins and remains stable
256 MiB reaches 100%
normal logical CLOSE ends the session without a reconnect loop
```

The server must no longer truncate the tail because a fixed 5-second drain deadline expired.

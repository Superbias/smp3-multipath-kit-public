# SMP3 Standalone Android APK v2.6.1

The v2.6.1 Android artifact is a launcher, configuration shell,
foreground-service supervisor, and log viewer for the existing standalone SMP3
runtime. It is not a proxy core.

## Architecture

```text
Android APK
  └─ foreground service
       └─ independent smp3-client process
            ├─ Carrier A -> standard SOCKS5 endpoint
            └─ Carrier B -> standard SOCKS5 endpoint

External proxy core(s) own the nodes behind those SOCKS5 endpoints.
```

The APK does not implement VLESS, Reality, Hysteria2, Snell, Shadowsocks,
subscriptions, routing rules, VPN/TUN mode, Mihomo APIs, or sing-box APIs.
`PROXY_CONFIGURATION_OWNERSHIP` remains `EXTERNAL_PROXY_CORE`.

## Configuration

The first screen stores only SMP3 settings in the app-private configuration:

- local SOCKS host and port; default `127.0.0.1:18080`;
- one SMP3 server endpoint used by both legs;
- Carrier A SOCKS host/port; default `127.0.0.1:20001`;
- Carrier B SOCKS host/port; default `127.0.0.1:20002`;
- the SMP3 protocol password.

The APK does not store node UUIDs, Reality keys, Snell PSKs, subscriptions, or
proxy-provider credentials. The external proxy core must provide the two
carrier SOCKS listeners before starting SMP3.

## Start, stop, and status

Start validates the loopback listener and all endpoint ports, writes a minimal
SMP3 JSON config to the app-private files directory, starts the Android
foreground service, and launches the packaged `smp3-client` as a separate
process. The service tracks that process object and never uses `killall` or
`pkill`.

Stop sends a graceful termination request to the managed child, waits briefly,
and force-terminates only that process if it does not exit. A crashed child is
shown as stopped with its exit code; the first version does not perform an
unbounded automatic restart.

The UI distinguishes:

- `PROCESS_RUNNING` / `PROCESS_STOPPED`;
- `SOCKS_READY` / `SOCKS_NOT_READY` based on a loopback connection check.

Carrier rows represent configured standard SOCKS endpoints. They are not
shown as connected unless a future authoritative carrier status is added.

## Logs and limitations

The app retains the last 200 stdout/stderr lines, applies basic secret-word
redaction, and provides clear/copy actions. It does not expose the generated
password-bearing config in the UI log.

This release provides an `arm64-v8a` debug-signed APK named
`smp3-android-standalone-2.6.1-debug.apk`; the debug signing is intentional
because no release signing credential is stored in the repository. The native
payload is the existing
`smp3-client-android-arm64` executable packaged under the APK native library
directory and launched as an independent process. A real Android ARM64
device in SELinux Enforcing mode was used for the R19-F runtime qualification.

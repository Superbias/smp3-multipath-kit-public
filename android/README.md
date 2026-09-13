# SMP3 Android Standalone v2.4.1

R20/v2.4.1 turns the Android app into a multi-instance manager. One app process
supervises one foreground service, and the service starts one independent
`libsmp3-client.so` process per instance.

## Install and migrate

Build or install the debug APK for `arm64-v8a`. On first launch, an existing
single-instance configuration is migrated to a `Default` instance without
changing its local SOCKS port, server endpoint, password, or two carrier
endpoints. A fresh install starts with a default instance that can be edited.

## Configure an instance

Open **Instances → Add** (or **Details → Edit**) and enter:

- Local SOCKS: loopback host and an unused local port, such as `127.0.0.1:18080`
- Server: the SMP3 server endpoint and SMP3 protocol password
- Carrier 1 and Carrier 2: local SOCKS5 endpoints exposed by an external proxy
  core, such as `127.0.0.1:20001` and `127.0.0.1:20002`

The app does not contain VLESS, Reality, Snell, Hysteria2, Mihomo, or sing-box.
Configure those nodes in the external proxy core and expose standard SOCKS5
listeners to the app. The app never manages node subscriptions or routing.

Save validates the instance and checks all local/carrier endpoint collisions
against other instances. New instances receive a recommended port; the
current UI supports up to eight configured instances and the SMP3 runtime uses
two carrier endpoints per instance.

## Run instances

Use **Start** or **Save & start** on one card, or **Start enabled** for all
enabled instances. Each instance receives its own private config file and
local SOCKS listener. **Stop**, **Restart**, and **Delete** affect only the
selected instance. The foreground notification summarizes all running
instances and disappears when none remain.

The **Logs** page can filter by instance and level. Logs are bounded to 1,000
entries per instance and redact password, token, key, UUID, and secret values.

If handshakes repeatedly fail, enable Android **Automatic date & time**. This
only provides troubleshooting guidance; the app does not change SMP3 wire
freshness rules.

## Local verification

From this directory:

```text
gradle clean testDebugUnitTest lintDebug assembleDebug
```

The APK is emitted at
`app/build/outputs/apk/debug/app-debug.apk`. The release APK is debug-signed
because no release signing credential is stored in the repository; verify its
SHA256 against the published `SHA256SUMS`.

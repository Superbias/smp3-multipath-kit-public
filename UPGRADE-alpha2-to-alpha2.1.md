# Upgrade alpha2 -> alpha2.1

No configuration schema or SMP3 password change is required. Both client and server binaries should be rebuilt/replaced together.

Recommended flow:

```bash
# WSL/Linux build machine
cd smp3-multipath-kit-alpha2.1
./validate-kit.sh
GOTMPDIR="$HOME/go-tmp" GOCACHE="$HOME/go-build-cache" ./build.sh
```

Then replace the landing binary and Windows binary with the newly built alpha2.1 outputs. Keep the existing `config.json` files.

Before replacement, stop the foreground test instances. After replacement:

```bash
# landing
/opt/smp3/smp3-proxy check -c /opt/smp3/config.json
/opt/smp3/smp3-proxy run -c /opt/smp3/config.json
```

```powershell
# Windows
.\smp3-proxy.exe check -c .\config.json
.\smp3-proxy.exe run -c .\config.json
```

For the first retest, keep `activation_threshold_mbps` low (for example 5 Mbps), repeat the landing-local HTTP download, and verify that leg 1 remains up instead of cycling through `Remote EOF`.

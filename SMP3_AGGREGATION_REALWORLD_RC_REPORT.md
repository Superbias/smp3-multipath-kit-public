# SMP3 Aggregation Final RC Real-World Validation Report

## Decision

`A. PHYSICAL_CARRIER_1_STALL`

The exact frozen RC **client** binary completed real standalone traffic through
SOCKS5, a separate Mihomo process, HTTPS/browser traffic, a 1 GiB download,
per-leg relay failure, reconnect, and a temporary two-second interruption.
The reachable remote server was identified and upgraded in place to the exact
frozen Linux RC artifact using its existing systemd lifecycle. The stall follows
the original local carrier listener `127.0.0.1:17899`, while the same logical
leg1 using carrier `127.0.0.1:20002` completes 100 MiB. Direct carrier tests
show 20002 completes 1 GiB, whereas direct 17899 stalls after roughly 20 MiB.
The blocker is therefore outside SMP3 scheduler and stream logic.

No scheduler code, defaults, tag, release, or broad deployment was changed;
the single reachable server binary was replaced under the controlled gate.

## Frozen identity

- Final RC source SHA: `4a1149ff1c1136027a81ce447de7b923519e3090`
- Historical broken SHA: `2b0d02e262a5e33838cc1318df24d2c8217813c4`
- Tested client binary: `D:\SMP3\aggregation-final-rc\bin\smp3-client-windows-amd64.exe`
- Tested client SHA256: `DD7E816B4E820004ED51E87EE1B904BA2474FD87837B7F3766EE0CD9F316ADB7`
- Frozen server Windows SHA256: `9571892377018A53DFFD1C2D9B30440EE18C0BCE47CED5EF32A511E849EB794B`
- Frozen server Linux SHA256: `E6BCA5A7F28E4CEA6FB782035677A0E93F6CDD4BCEED540CB4CDE8C3BAAC82F4`

The four final artifact hashes were rechecked after testing and still match the
frozen manifest. No replacement or rebuild was used.

## Real topology

```text
curl / Chrome
  -> SMP3 SOCKS5 127.0.0.1:18080
  -> exact frozen smp3-client Windows/amd64
  -> local Mihomo/FlClash carrier listeners 127.0.0.1:20001 and :20002
  -> existing remote SMP3 standalone server sidecar :24445
  -> Internet
```

The client config explicitly used `scheduler_mode: aggregation`; `-check`
accepted the exact config and runtime logs showed the frozen sidecar listening
on `127.0.0.1:18080`. The remote server was identified as `root@64.204.48.130`,
unit `smp3-standalone.service`, PID `288055` after replacement, with command
`/opt/smp3-standalone/smp3-server -c /etc/smp3-standalone/config.json`.
The old binary, config, and unit were backed up under
`/opt/smp3-standalone/backups/pre-final-rc-20261002T065707Z/`; the config hash
remained `67434e9eae26de471403fdb60662efbd730bf8578e1a006423510b5145befd70`.

## Controls

- Default control: scheduler field omitted; exact client `-check` and startup
  both passed, preserving the adaptive default in config loading.
- Static control: `scheduler_mode: static`; exact client `-check` and startup
  passed with the same local SOCKS listener behavior.
- Server binary control: exact frozen Windows server `-version` and `-check`
  passed using an isolated loopback smoke config (`127.0.0.1:24455`, sidecar
  `127.0.0.1:24456`).
- Remote server provenance: exact frozen Linux server hash matched on the
  running `/proc/288055/exe`; service active/running, restart count 0 after
  replacement, listeners `10.66.66.1:24444`, `10.66.66.1:24445`, and
  `127.0.0.1:24500` present.
- Exact frozen client plus exact frozen remote server quick smoke: PASS
  (`https://example.com`, HTTP 200, 713 bytes).

## Real traffic results

- HTTPS through exact client: PASS (`https://example.com`, 713 bytes).
- HTTP through exact client: PASS (`example.com`, 713 bytes).
- 1 GiB download: PASS from `http://ipv4.download.thinkbroadband.com/1GB.zip`;
  1,073,725,334 bytes, curl reported about 22 seconds and 44.56 MB/s average.
  Local SHA256: `E5C9B51BDFAF6337202810C3BC8FA789CA7E059A62FE747AB7FD86CB547B2C00`.
  The source did not provide a published checksum in this run.
- Additional 10 MiB and 100 MiB downloads completed successfully.
- Leg-0-only control: PASS for 100 MiB.
- Leg-1-only control: reproducible stall at approximately 28 MiB with both
  the original `127.0.0.1:17899` path; process required termination. The same
  logical leg1 mapped temporarily to `127.0.0.1:20002` completed 100 MiB.
- Direct carrier 20002: 100 MiB and 1 GiB completed successfully.
- Direct carrier 17899: stalled at 20,461,233 bytes during a 100 MiB transfer.
- Logical-leg swap: logical leg0 mapped to carrier 20002 completed 100 MiB;
  the stall did not remain attached to logical leg1.
- Source B control (`https://proof.ovh.net/files/10Mb.dat`) completed 10 MiB
  through carrier 20002. Its 100 MiB endpoint did not provide a completed
  run within the bounded window, so no source-B 100 MiB claim is made.
- Dual useful throughput is approximately 356 Mbps from the curl transfer
  average. A meaningful aggregation efficiency ratio is **not claimed**:
  the remote server did not expose per-leg byte counters through the reachable
  path, and the two carrier capacities were not independently measured with
  stable exact-server controls.

## Per-leg evidence

During a throttled 100 MiB transfer, the exact client had simultaneous
established connections to both local carrier listeners:

```text
127.0.0.1:18080 -> client PID 23084
127.0.0.1:20001 -> carrier process PID 37708
127.0.0.1:20002 -> carrier process PID 37708
```

This proves both carrier sessions were active. It does not prove per-leg DATA
bytes; service-side counters were inaccessible, so no numeric leg share is
claimed.

## Mihomo and browser gates

- Mihomo integration: PASS. A separate temporary Mihomo process listened on
  `127.0.0.1:7891` and routed through the SMP3 SOCKS5 endpoint. HTTP traffic
  through `7891` returned the expected 713-byte Example Domain response.
- Browser smoke: PASS. Chrome headless used the Mihomo SOCKS path and returned
  an Example Domain document with the expected title. This covered persistent
  HTTPS and a normal page load; it is not a latency-optimality claim.
- Sustained video playback: NOT VALIDATED. A stable media source was not
  available in the current environment, so no playback claim is made.

## Failure and recovery

Fault injection used temporary local TCP relays in front of the two real carrier
SOCKS listeners; the underlying carrier and remote server path remained real.

- Leg 1 relay failure: 100 MiB transfer completed after relay termination;
  healthy leg continued and no deadlock occurred.
- Leg 0 relay failure: 100 MiB transfer completed after relay termination;
  healthy leg continued and no deadlock occurred.
- Reconnect: both relays were restarted; a subsequent throttled 100 MiB transfer
  showed simultaneous connections to both relays and completed successfully.
- Temporary interruption: one relay was stopped for approximately two seconds,
  restarted, and the 100 MiB transfer completed successfully.
- No reconnect loop or permanent pending backlog was observed in client-side
  process behavior.

These relay tests validate client recovery behavior, but not remote server
per-leg accounting.

## Leg-1 attribution controls

- Three historical leg1-only observations remained in the same order of
  magnitude (about 20–30 MiB) on the original 17899 carrier path; one run
  stopped at 29,609,984 bytes and a direct carrier run stopped at 20,461,233
  bytes.
- The server remained the exact frozen binary during these controls. No remote
  listener swap was needed after the local carrier split was demonstrated.
- Remote `TCP_INFO` and packet capture were not required to close causality:
  the direct carrier control and logical-leg/carrier swap separated the physical
  carrier from SMP3. No SMP3 state snapshot was added.
- Scheduler-mode comparison was not expanded after the physical carrier cause
  was isolated; a single active leg has no scheduling choice, and changing
  scheduler code or defaults was explicitly out of scope.

## Long-run and lifecycle

- Sustained transfer sample: three sequential 100 MiB downloads at a 2 MiB/s
  cap completed in about 152 seconds (300 MiB total). Client working set after
  the run was approximately 23 MiB; no progressive growth was observed in this
  short sample.
- Required 30–60 minute gate: NOT COMPLETED.
- Client stop/restart: PASS. Port `127.0.0.1:18080` was released after stop
  and rebound successfully on restart.
- Temporary validation processes and relays were stopped after testing; the
  pre-existing carrier services were not modified.

## Final rehash

After all real-world runs, the client and all four frozen artifact hashes were
recomputed and matched the frozen provenance manifest exactly. The running
remote server was rehashed after replacement and matched the frozen Linux
server hash exactly.

## Final stable-carrier long-run closure

- Stable final carrier set: logical leg0 via 127.0.0.1:20001, logical leg1
  via 127.0.0.1:20002. The excluded 127.0.0.1:17899 carrier was not used.
- Exact client and server hashes were rechecked before and after the run.
- A 30-minute sustained run completed with ten 1 GiB chunks and
  10,706,152,518 total bytes. The measured wall interval was approximately
  30 minutes; the transfer was rate-capped at 8 MiB/s to keep the session
  continuous. The run client PID was 10872 and the remote server PID was
  288055; overall throughput was approximately 49 Mbps.
- At periodic samples both carrier sockets were established (1/1) during
  active transfer. A single sample observed transient extra established
  connections (3/2) during reconnect/rotation; the next sample returned to
  1/1 and transfer continued. No deadlock or permanent disconnect occurred.
- Client RSS samples ranged from approximately 27–46 MiB (completion-boundary
  zero samples are excluded from the non-zero range); final process RSS was
  approximately 32 MiB. Current server RSS was approximately 39 MiB.
- After the long-run process cleanup, the same exact client passed HTTPS
  (713 bytes), 10 MiB, and 100 MiB downloads. Stopping the client released
  port 18080; restart restored it and HTTPS passed again.
- Streaming remains NOT_RUN_ENVIRONMENT_LIMITATION.

## Final decision

A. AGGREGATION_RC_REALWORLD_VALIDATED

The prior 17899 result remains an explicit external finding:
127.0.0.1:17899 REPRODUCIBLE PHYSICAL CARRIER STALL EXCLUDED FROM FINAL RC
QUALIFICATION. No SMP3 fix or claim is made for that carrier.

No release boundary action is authorized by this report.

Final rehash: client SHA256
DD7E816B4E820004ED51E87EE1B904BA2474FD87837B7F3766EE0CD9F316ADB7;
running server SHA256
E6BCA5A7F28E4CEA6FB782035677A0E93F6CDD4BCEED540CB4CDE8C3BAAC82F4.
Tag created: NO. Release created: NO. Broad deployment: NO.

- Tag created: NO
- Release created: NO
- Deployed: NO

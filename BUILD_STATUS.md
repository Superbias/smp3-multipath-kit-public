# Alpha2.3-r10 verification status

## Source verification

R10 retains R9's single-sequence cumulative-frontier rescue boundary while removing the additional periodic-ticker wait after real cumulative ACK progress.

Completed source checks:

- standalone SMP3 tests discovered: 65
- `go test`: PASS
- `go test -race -count=5`: PASS
- standalone `go vet`: PASS
- gofmt: PASS
- example JSON syntax: PASS
- Python/shell syntax: PASS

R10-specific regressions cover:

- ACK progress waking a newly exposed overdue frontier before the periodic safety-net tick;
- never rescuing a speculative later sequence beyond `ackedNext`;
- failed/full rescue enqueue not consuming the retransmit cooldown;
- same-ID transport failure invalidating stale generation ownership;
- dead-leg replay ordered frontier-first;
- non-overdue frontier fast path avoiding transport-snapshot allocation.

## Live acceptance

Live 500 MiB (`524288000` bytes) acceptance is complete.

Accepted scenarios include:

- normal multipath upload;
- controlled +300 ms preferred-path delay/HOL pressure;
- forced preferred-leg TCP destruction while outstanding DATA remained;
- replacement preferred leg reconnect with the same numeric leg ID;
- server rejoin into the same logical session using a new TCP transport;
- complete HTTP 200 payload after repair;
- no observed false Hy2 fallback / actual `tx_ack_stall` in the accepted r10 runs.

## Build status of this archive

This environment has no outbound DNS/HTTPS access to fetch the pinned sing-box repository/toolchain dependency graph, so this archive is intentionally shipped as a **clean source release** without prebuilt binaries.

Run `./build.sh` in WSL/Linux with network access to build both targets.

Expected binary version:

`1.14.0-beta.14-smp3-alpha2.3-r10`

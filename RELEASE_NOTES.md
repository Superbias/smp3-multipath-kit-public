# SMP3 Multipath Kit alpha2.3-r10 release notes

## Release status

`alpha2.3-r10` is the clean source release for the ACK-paced cumulative-frontier rescue and same-ID rejoin hardening work.

## Main changes

- ACK progress immediately wakes a single-frontier rescue check for the newly exposed `ackedNext` blocker.
- Failed/abandoned rescue enqueue no longer consumes a full retransmit cooldown.
- Failed concrete leg generation invalidates stale `lastSentLeg` ownership for outstanding DATA.
- Dead-leg replay is ordered from the cumulative ACK frontier forward.
- Healthy/non-overdue ACK fast path avoids unnecessary transport snapshot allocation.
- Wire version, JSON schema and adaptive thresholds remain unchanged.

## Live acceptance completed

A 500 MiB upload (`524288000` bytes) was used for live acceptance.

Verified:

- exact payload completion with HTTP 200;
- controlled preferred-path +300 ms delay without false fallback;
- real preferred-leg TCP destruction while DATA was outstanding;
- same numeric leg ID reconnect using a new TCP connection;
- server `joined/rejoined` the replacement into the existing logical session;
- public leg kept the logical session alive during the preferred-leg outage;
- recovered preferred leg resumed data-plane participation;
- no observed false `tx_ack_stall` / Hy2 fallback in the accepted r10 runs.

Benchmark numbers in `README.md` are environment-specific and are not performance guarantees.

## Compatibility

- SMP3 HELLO version: `4`
- Compatible wire generation: alpha2.2 / alpha2.3
- Expected binary version: `1.14.0-beta.14-smp3-alpha2.3-r10`

## Distribution hygiene

This source release contains placeholders only for deployment secrets. It intentionally excludes live deployment configs, keys, logs, build caches and VCS metadata.

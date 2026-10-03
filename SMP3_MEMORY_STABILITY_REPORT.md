# SMP3 Memory Stability Report

## Classification

**A. MEMORY_BEHAVIOR_BOUNDED_AND_EXPLAINED**

The observed high RSS is runtime and allocator retention around bounded stream receive buffers. The measurements do not show progressive live-object retention, registry growth, goroutine growth, socket leakage, or an unresolved production leak.

## Baseline

The prior dynamic 1 GiB / 4-stream closure reported RSS 8.88 MB at start, 326.79 MB peak, and 326.92 MB at end. That single endpoint was not treated as a leak.

The diagnostic closure uses `io.CopyN` with a generated reader and a streaming checker. It does not use `bytes.Buffer`, `io.ReadAll`, or whole-payload collection.

## Five same-process 1 GiB rounds

The following run used one Go test process, five consecutive 1 GiB rounds, dynamic capacity and dynamic activation, explicit diagnostic GC after every round, and a one-second idle sample.

| round | RSS before GC | HeapAlloc before GC | RSS after GC | HeapAlloc after GC | RSS idle | HeapObjects after GC | NumGC |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 310.4 MB | 232.8 MB | 310.5 MB | 61.1 MB | 309.0 MB | 3,700 | 93 |
| 2 | 321.4 MB | 224.8 MB | 321.4 MB | 64.4 MB | 115.9 MB | 3,964 | 185 |
| 3 | 300.7 MB | 268.4 MB | 300.7 MB | 58.9 MB | 298.6 MB | 4,206 | 266 |
| 4 | 285.4 MB | 214.8 MB | 284.9 MB | 55.3 MB | 284.8 MB | 4,273 | 358 |
| 5 | 321.7 MB | 219.9 MB | 322.3 MB | 63.6 MB | 321.7 MB | 4,642 | 450 |

All five rounds passed payload validation. Each reported retry=0, rescue=0, ledger=0, amplification=1.000000. Goroutines stayed at 2 and FDs stayed at 6. The memory trend that matters is HeapAlloc and HeapObjects after GC: they remain in the same bounded range rather than rising with the 5 GiB transferred.

The RSS spread is explained by Go heap pages remaining mapped or being released back to the OS at different idle points. In the same process, `HeapAlloc` fell from roughly 215–269 MB to 55–64 MB after GC.

## Forced-GC and heap profile attribution

Heap profiles were captured at start, after rounds 1, 3, and 5, and after final GC.

Top in-use owners after transfer were:

- `StreamEngine.txLoop` and `StreamEngine.getBuffer`
- `StreamEngine.NewStreamEngine.func1`, the RX `sync.Pool` allocation site
- small runtime scheduler allocations
- benchmark link buffers in the diagnostic harness

The final post-GC profile contained about 67.9 MB, with 65.4 MB attributed to the RX buffer pool allocation site and about 2 MB to runtime worker allocation. In-use objects were 1,046 pool buffers and 4,681 `net.Pipe` objects in the diagnostic harness. The pool is bounded by configured chunk size and in-flight/queue limits; registry, ACK state, and demand accounting were not top owners.

The profile does not show a growing pending-record or capacity-registry owner. Allocation-space profiling is dominated by expected wire-frame transfer and benchmark link writes, not retained live objects.

## RSS versus heap

A default-GC 20 GiB standalone run reached a client RSS of about 399,544 KB while the server remained about 141,488 KB. A second 30 GiB run with `GODEBUG=gctrace=1` stayed around 9–14 MB client RSS and 12–20 MB server RSS. The gctrace live heap after collections stayed about 2–4 MB.

This difference demonstrates allocator/GC timing and pool/page retention. RSS alone overstates live application ownership. The low live heap in the repeated gctrace run rules out progressive live-object retention as the explanation for the earlier high RSS.

## Production standalone process traffic

The actual Linux amd64 standalone client and server used real SOCKS traffic and were not restarted between rounds.

Five sequential 1 GiB transfers (5 GiB total) all completed and activated leg 1:

- client RSS: 3,992 KB before the first transfer; 161,848 KB after round 5 and idle;
- server RSS: 5,520 KB before the first transfer; 142,064 KB after round 5 and idle;
- client threads: 8 initially, 14 after load;
- server threads: 8 initially, 16 after load.

The extended same-process run transferred 20 GiB without restart. Client RSS reached 399,544 KB and server RSS remained 141–144 MB. The gctrace-controlled 30 GiB run stayed at the low RSS range above while live heap stayed 2–4 MB. No transfer failed.

## Stream churn

A real standalone SOCKS churn run created, transferred 4 KiB, and closed 1,000 streams:

| stage | client RSS | server RSS | client threads | server threads | client FDs | server FDs |
|---|---:|---:|---:|---:|---:|---:|
| before | 3,888 KB | 5,768 KB | 8 | 8 | 7 | 9 |
| after | 9,684 KB | 12,384 KB | 13 | 20 | 7 | 9 |
| idle | 9,684 KB | 12,384 KB | 13 | 20 | 7 | 9 |

The server log recorded 1,000 session creations. Core registry lifecycle churn of 1,200 provider lifecycles remained at the configured 1,024-entry cap. Shared global closure checks returned two carrier entries and zero active references after teardown.

## Goroutines, connections, and registry

- Diagnostic core goroutines: stable at 2 after each round.
- Production client/server threads: stable after startup and load; no monotonic growth.
- Production churn FDs: unchanged from after-load to idle.
- Registry: two shared carrier entries and zero refs after the global closure; the 1,200-lifecycle bound remains enforced at 1,024 entries.
- No socket or connection accumulation was observed in the churn run.

## GC pressure and performance

The five-round diagnostic run moved from NumGC 1 at startup to 450 after round 5. PauseTotalNs remained about 63 ms and GCCPUFraction stayed around 0.04%. Dynamic 1 GiB closure throughput remained in the previously validated range of about 202–236 Mbps, including the earlier 212.903 Mbps result, with payload PASS and zero retry/rescue/ledger amplification.

The memory diagnostic adds no production GC calls or tuning.

## Native Mihomo and tracked-only validation

The native Mihomo adapter race and full native test/build evidence from the dynamic activation closure remain passing. The memory work adds only diagnostic tests and documentation; production runtime architecture was not redesigned.

The tracked-only archive gate must include the new memory diagnostic test, source tests, and four amd64 builds before the local commit is considered closed.

## Root-cause explanation and limitation

The retained bytes are bounded RX buffers held by `sync.Pool` and Go heap pages. A large transfer can temporarily raise RSS until normal GC and allocator release occur. The stream pool is limited by chunk size and the configured in-flight/queue bounds. The evidence does not justify changing production buffer policy: live heap and object counts plateau, and repeated gctrace traffic remains low and stable.

Known limitation: RSS release timing is runtime- and platform-dependent, so RSS may remain higher than HeapAlloc after a transfer. Operational monitoring should use RSS together with heap/GC and stream lifecycle telemetry rather than treating a single RSS endpoint as a leak.
 

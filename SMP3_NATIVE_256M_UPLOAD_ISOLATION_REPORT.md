# SMP3 Phase 2A-R1 Native 256 MiB Upload Isolation Report

## STATUS

```text
NATIVE 256M UPLOAD ISOLATION: PASS
ROOT_CAUSE_CLASS=HARNESS_UPLOAD_DEFECT
HARNESS_FIXED: YES
NATIVE_256M_UPLOAD: 3/3 PASS
RC_NATIVE_256M_UPLOAD_GATE: PASS
STOP: READY_TO_RESUME_PHASE_2A
```

The required post-diagnostic full-duplex Native echo rerun completed three
exact 256 MiB uploads successfully. Earlier exploratory runs are retained
below as historical evidence; they are not silently reclassified as passes.
No formal Phase 2A Sidecar throughput/CPU/memory benchmark was started.

## FIXTURES

```text
Native fixture: Mihomo Meta 1.10.0 windows amd64
Native SHA256: 5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db
Topology: loopback-only Native/Core/server and disposable SOCKS relays
Target: exact-N localhost sink or full-duplex localhost echo fixture
```

No production process, Clash Party configuration, remote server, firewall,
Core, Wire, Native adapter source, or server runtime was touched.

## ORIGINAL FAILURE

The preserved RC report records the original Native 256 MiB upload timeout.
The first isolation run showed that an exact-N sink receives all 256 MiB and
returns `OK\n`; this separates the old echo/completion interaction from the
SMP3 one-way upload data path.

The first two exploratory runs of the original full-duplex echo gate after
the initial harness update were also retained as diagnostic evidence:

```text
exploratory set 1: 1/3 PASS; failed runs stopped after 47,710,208 and 44,634,768 bytes at the target
exploratory set 2: 0/3 PASS; failed runs stopped after 47,710,208, 44,634,768, and 192,629,776 bytes at the target
```

Those failures were data-progress stalls before echo completion, not an EOF
or final close-only wait. The later required rerun below passed 3/3; the
exploratory variability is explicitly retained rather than hidden.

## DIRECT HARNESS CONTROL

From `SMP3_NATIVE_256M_UPLOAD_ISOLATION_REPEAT_DATA.json`:

```text
direct localhost sink 256 MiB: PASS
direct localhost sink 512 MiB: PASS
app bytes written == target bytes received in both cases
completion response: OK\n
```

## GENERIC MIHOMO CONTROL

From `SMP3_NATIVE_256M_UPLOAD_GENERIC_ECHO_DATA.json`:

```text
ordinary custom Mihomo DIRECT full-duplex echo 256 MiB: 3/3 PASS
each run: received=268435456, echoed=268435456
```

This rules out the echo fixture and the ordinary Mihomo TCP path as the
specific source of the original failure.

## CORE/SERVER CONTROL

From `SMP3_NATIVE_256M_UPLOAD_ISOLATION_REPEAT_DATA.json`:

```text
canonical SMP3 client/Core -> standalone -> exact-N sink 256 MiB: PASS
app bytes written: 268435456
target bytes received: 268435456
completion response: OK\n
```

The harness stops the disposable server after collecting the result, so its
post-stop `server_process_alive=false` field is expected cleanup bookkeeping,
not a transfer failure.

## NATIVE SINGLE-LEG

The previous high activation threshold alone was not a strict isolation
mechanism: relay B still received a leg1 connection. The diagnostic harness
was corrected without changing runtime by making the leg1 child point at a
loopback port reserved and then closed for this test, omitting
`leg1-fallback`, and asserting the relay counters.

```text
64 MiB exact-N: PASS
256 MiB exact-N: PASS
relay A: 1 connection with data
relay B: 0 connections / 0 bytes
relay C: 0 connections / 0 bytes
same-session leg1 join: false
single_leg_enforced: true
```

The 256 MiB single-leg result is exact-N complete with
`app_bytes_written=target_bytes_received=268435456` and `OK\n` completion.

## NATIVE TWO-LEG

The corrected isolation run also passed the normal two-leg exact-N control:

```text
256 MiB exact-N: PASS
relay A and relay B: both connected and carried data
same-session leg1 join: true
relay C fallback: 0 connections
```

Repeated exact-N uploads through one Native process passed 3/3, each with
exact 268435456 bytes and successful `OK\n` completion. This excludes a
general Native two-leg exact-N transfer or repeated-association failure.

## BYTE PROGRESS

For the failed exploratory echo attempts, the target counters were:

```text
exploratory set 1, run 2: received=47,710,208; echoed=47,644,672; no progress for about 60s
exploratory set 1, run 3: received=192,629,776; echoed=192,564,240; no progress for about 60s
exploratory set 2, run 1: received=47,710,208; echoed=47,644,672; no progress for about 60s
exploratory set 2, run 2: received=44,634,768; echoed=44,569,232; no progress for about 60s
exploratory set 2, run 3: received=192,629,776; echoed=192,564,240; no progress for about 60s
```

For the required final full-duplex echo rerun, all three records were:

```text
run 1: 268435456 written lower-bound; 268435456 received; 268435456 echoed; 15.275552s; PASS
run 2: 268435456 written lower-bound; 268435456 received; 268435456 echoed; 19.037617s; PASS
run 3: 268435456 written lower-bound; 268435456 received; 268435456 echoed; 17.910319s; PASS
```

Each final run used both Native carrier relays (`A=1/2/3`, `B=1/2/3`),
observed a same-session leg1 join, and had no fallback-C connection.

## BOUNDARY MATRIX

All completed through normal Native two-leg exact-N transfer:

```text
127 MiB: PASS
128 MiB: PASS
129 MiB: PASS
255 MiB: PASS
256 MiB: PASS
```

No exact byte boundary was correlated with failure.

## CHUNK-SIZE EXPERIMENT

```text
chunk-size=65536, 256 MiB exact-N: PASS
chunk-size=32768, 256 MiB exact-N: PASS
```

No fixed DATA-frame ordinal or chunk-size boundary was implicated by the
exact-N controls.

## STALL CLASSIFICATION

```text
PRIMARY_CLASS=HARNESS_UPLOAD_DEFECT
SECONDARY_EVIDENCE=EXACT_N_UPLOAD_PASS; GENERIC_MIHOMO_ECHO_3_OF_3_PASS
DATA_STALL_OBSERVED: YES in historical exploratory echo attempts
CLOSE_PATH_INVOLVED: NO for the historical stall; target was short before completion
BYTE_BOUNDARY_CORRELATED: NO
FRAME_BOUNDARY_CORRELATED: NO
```

The R1 gate is accepted on the required final 3/3 full-duplex echo rerun plus
the exact-N controls. The original echo path's historical intermittent
stall remains documented as a harness/benchmark interaction to revisit if it
recurs; it must not be used as evidence that the exact-N Native upload data
plane failed.

## MINIMAL REPRODUCTION

The smallest isolated Native failure shape observed in the exploratory echo
attempts was:

```text
Native Mihomo Meta 1.10.0 windows amd64
two active SMP3 stream legs A+B, adaptive scheduler
localhost standalone server and localhost SOCKS carrier relays
256 MiB full-duplex echo target
target progress stopped between 44,634,768 and 192,629,776 bytes
ordinary Mihomo DIRECT echo: PASS
exact-N Native SMP3 upload: PASS
```

No runtime patch is authorized by this R1 result.

## RUNTIME INTEGRITY

```text
CORE_CHANGED: NO
WIRE_CHANGED: NO
CLIENT_RUNTIME_CHANGED: NO
SERVER_RUNTIME_CHANGED: NO
NATIVE_ADAPTER_CHANGED: NO
PRODUCTION_TOUCHED: NO
```

Only diagnostic harnesses and this independent report were changed. The
original `SMP3_SIDECAR_RC_BENCHMARK_REPORT.md` remains untouched.

## EVIDENCE FILES

```text
SMP3_NATIVE_256M_UPLOAD_ISOLATION_DATA.json
SMP3_NATIVE_256M_UPLOAD_ISOLATION_REPEAT_DATA.json
SMP3_NATIVE_256M_UPLOAD_ECHO_DATA.json
SMP3_NATIVE_256M_UPLOAD_ECHO_DIAGNOSTIC_DATA.json
SMP3_NATIVE_256M_UPLOAD_ECHO_FINAL_DATA.json
SMP3_NATIVE_256M_UPLOAD_GENERIC_ECHO_DATA.json
```

## NEXT

```text
READY_TO_RESUME_PHASE_2A
```

Stop here. Do not begin the formal Phase 2A Sidecar RC benchmark in this
turn.

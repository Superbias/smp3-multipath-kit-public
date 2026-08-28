# Changelog

## alpha2.3-r10 — ACK-paced cumulative-frontier rescue

- Kept R9's single-sequence concurrent frontier rescue semantics, but removed the extra periodic-ticker delay after cumulative ACK progress. When an ACK advances `ackedNext`, R10 immediately wakes an O(1) check of the newly exposed `outstanding[ackedNext]` frontier.
- Split frontier repair from the full retry scan. ACK-driven wakeups no longer scan the complete outstanding map; dead-leg replay and ordinary recovery still run on the existing retry/timer path.
- Intentionally did **not** add fixed multi-frame rescue bursts. SMP3 v4 only has cumulative ACK, not SACK, so later sequences may already be buffered by the receiver. R10 continues to rescue exactly one proven blocker at a time and lets cumulative ACK retirement release any already-received tail.
- ACK-paced wakeups do not bypass `retransmit_timeout`. They only eliminate the additional 0–250ms scheduling wait when the next frontier is already overdue.
- Pre-deployment review hardened rescue enqueue accounting: a full/retiring rescue queue no longer commits `lastRescueAt`, so an attempt that never entered the priority queue cannot consume a full retransmit cooldown. Failed queued writes also roll their cooldown state back.
- Invalidated `lastSentLeg` ownership when the concrete transport generation carrying that leg ID fails. A same-ID replacement can therefore replay old unacked DATA instead of being mistaken for the retired writer.
- Made ordinary dead-leg replay deterministic and cumulative-ACK friendly by sorting retry candidates by sequence, so the oldest outstanding/frontier is queued before later DATA instead of inheriting Go map iteration order.
- Moved the healthy frontier age check ahead of transport snapshots; non-overdue ACK wakeups return without the previous leg-slice/map allocation work.
- Added regressions proving a newly exposed overdue frontier is queued before the 250ms safety-net ticker, rescue never speculatively bursts past `ackedNext`, failed rescue enqueue does not consume cooldown, same-ID failure invalidates stale ownership, and dead-leg replay is frontier-first.
- Wire/hello version remains `4`; configuration schema, rescue queue size, and adaptive fallback thresholds are unchanged.
- Completed live r10 acceptance with exact 500 MiB uploads, including a controlled +300 ms preferred-path delay and a forced preferred-leg TCP destruction while DATA remained outstanding. The replacement leg0 established a new TCP transport, rejoined the same logical session with the same numeric leg ID, and the full payload completed with HTTP 200 without observed false `tx_ack_stall` or Hy2 fallback.

## alpha2.3-r9 — concurrent ACK-frontier rescue

- Fixed the live R8 data-plane stall where a cumulative-ACK frontier could remain `inTransit` on a heavily delayed/backpressured leg0, preventing the existing retry scheduler from duplicating that missing seq onto healthy leg1. This could fill the entire 1024-frame inflight window and stop application forward progress even while Hy2 remained healthy.
- Added a separate priority rescue lane per transport leg. An overdue ACK-frontier seq may now have a concurrent duplicate DATA attempt on the alternate carrier even while the older attempt is still queued/writing. The receiver's existing seq deduplication makes late duplicate copies harmless.
- Added attempt-start ordering so a stale slow-path write that completes after a newer rescue cannot steal frontier attribution back from the newer attempt.
- Added rescue throttling at `retransmit_timeout` cadence and a `frontier_rescues` health counter for live verification.
- Added `ack_frontier_multi` diagnostics. Adaptive health does not blame a single carrier while the same frontier has attempts on both paths.
- Stabilized TX carrier attribution: leg1 must own the ACK frontier exclusively and continuously for at least `tx_ack_stall` before Hy2 can enter TX-pressure SUSPECT. A one-sample ownership flip is no longer sufficient.
- Added regressions proving a blocked in-transit primary can be bypassed by a concurrent secondary rescue, concurrent frontier ownership is reported as multipath, and one-sample/multipath frontier attribution does not falsely penalize Hy2.
- Wire/hello version remains `4`; configuration schema is unchanged.


## alpha2.3-r7 — per-leg control-plane isolation / graceful-close boundary

- Reworked ACK emission so cumulative ACKs are published independently to each leg's writer instead of synchronously writing every carrier from one ACK loop. A blocked/black-holed leg can no longer head-of-line block ACK progress on a healthy carrier.
- Added per-leg cumulative ACK coalescing (`ackPending`) and wakeups; only the newest ACK value needs to be written. Forced ACK retransmission remains explicit so duplicate-DATA / ACK-loss recovery can resend the same cumulative value.
- Prioritized ACK and explicit control frames ahead of queued DATA at wire-frame boundaries. Already-started stream writes are intentionally not preempted.
- Routed ACTIVATE/CLOSE through the per-leg writer so all transport writes remain serialized without a cross-leg write lock.
- Hardened logical CLOSE broadcast: CLOSE is queued to every live carrier first, and successful delivery on one healthy carrier is sufficient to proceed even if another carrier is blocked.
- Tightened graceful CLOSING semantics: transport repair remains allowed while outstanding DATA exists, but once the TX tail is fully ACKed a racing leg EOF no longer wakes the booster, schedules reconnect, or starts recovery.
- Fixed adaptive leg-down logging so the carrier that actually failed is captured before health accounting can switch the state from Hy2 to Snell.
- Wire/hello version remains `4`; configuration is unchanged.

## alpha2.3-r6 — session lifecycle / rejoin race hardening

- Added completed-session tombstones for `2 * helloSkew` so a delayed authenticated old leg cannot resurrect a finished wire-v4 session ID into a fresh destination TCP connection.
- Made FINALIZING/DONE transition atomic with `addLeg()`. ACTIVE and graceful CLOSING still accept transport repair required to drain outstanding DATA; FINALIZING/DONE reject all new legs.
- Added per-leg retiring barriers. A confirmed-dead same-ID transport is removed from scheduling, but its replacement waits until both old read/write workers have exited, preventing stale generation workers from modifying new-generation TX transit state.
- Added a per-leg done signal so idle write workers terminate on carrier close, and hardened enqueue-vs-close so DATA cannot remain pinned to a dead send queue.
- Removed the session-wide attach mutex; core-level locking now serializes attachment without allowing a waiting same-ID retirement to head-of-line block the other leg.
- Added inbound shutdown gating so `Close()` cannot race with creation of a new logical session.
- Wire/hello version remains `4`; configuration is unchanged.

## alpha2.3-r5 — first authenticated leg establishes the server core

- Removed the R4 fixed 10-second secondary-before-primary ordering grace. Ordering is no longer a correctness prerequisite.
- After full HELLO authentication/validation, either leg0 or leg1 may atomically create a missing logical session; a concurrent arrival joins that same fully initialized core.
- Session creator identity does not change scheduling roles: leg0 remains preferred when present and leg1 remains the client-controlled lazy booster / adaptive public carrier.
- Retained conservative rejection of truly simultaneous live duplicate same-ID transports because wire v4 has no monotonic leg generation.
- Wire/hello version remains `4`; configuration is unchanged.

## alpha2.3-r4 — server secondary-HELLO ordering fix

- Live client/server logs proved that public Hy2/Snell leg1 can reach the landing SMP3 listener before the preferred-line leg0 has registered the logical session. The old server rejected such an authenticated secondary immediately as an orphan, producing a false zero-useful EOF and eventually adaptive fallback.
- Server now gives an authenticated secondary leg a bounded 10-second ordering grace to wait for leg0 to publish the same session ID. Buffered DATA behind the HELLO remains on the carrier until the join is installed.
- Added explicit server-side `join rejected` and `secondary leg has no active session after wait` diagnostics so duplicate-leg versus expired-session failures are visible in `journalctl`.
- Concurrent attaches for one server session are serialized. Duplicate leg IDs are still conservatively rejected in r4 because wire v4 has no monotonic leg-generation field; r4 does not guess which of two simultaneously valid same-ID transports is newer.
- Client adaptive/cooldown behavior from r3 is unchanged. SMP3 wire/hello version remains `4`; no configuration schema change is required.

## alpha2.3-r4 — live EOF attribution / cooldown burst fix

- Do not treat every immediate secondary-leg EOF as a Hy2 carrier failure. Zero-useful EOFs on small, unpressured logical sessions with healthy leg0 are classified as an ambiguous short-session JOIN/teardown race.
- Still count EOF as a real carrier signal after leg1 useful contribution, under RX/TX pressure, or when leg0 is also unavailable. Repeated zero-useful EOFs on the same still-live, demand-driven logical session also become actionable.
- Suppress immediate reconnect for ambiguous Hy2 EOF; sustained logical demand triggers demand-driven repair from the health loop.
- Coalesce concurrent established-failure bursts at global health level so one physical outage opens one cooldown penalty. Only a later failed probation canary advances exponential backoff.
- Keep SMP3 wire/hello version 4 unchanged.


## alpha2.3 — adaptive public carrier

### r2 stabilization

- Added conservative no-baseline severe-stall detection so a Hy2 carrier that is effectively black-holed from its first active samples can still enter SUSPECT and eventually fall back without requiring a previously learned goodput baseline.
- Recovery canary and ordinary active-success windows now require continuous real leg1 useful traffic; long idle gaps reset their stable window instead of being counted as healthy Hy2 time.
- Coalesced global initial-dial failure bursts so concurrently failing Hy2 dials trigger one cooldown transition rather than multiplying 90s -> 180s -> 300s inside the same burst.
- Global initial-failure history is only cleared after sustained, real Hy2 useful contribution rather than logical demand carried solely by leg0.
- Added regression tests for no-baseline RX/TX black holes, idle canary continuity, concurrent initial-failure bursts, and real-data active-success gating.
- Release tooling now emits relative binary checksums and provides a clean whitelist-based staging script that excludes `.git`, `.work`, real deployment configs, caches, and stale archives.

- Added Hy2-preferred `leg1` with Snell fallback inside the SMP3 multipath outbound.
- Added user-impact-based health detection with HEALTHY/SUSPECT/FALLBACK hysteresis.
- Added logical useful-goodput, reorder-pressure, outstanding-window, ACK-progress, and per-leg attribution snapshots.
- Added same-session carrier replacement without closing the logical SOCKS TCP connection or changing wire protocol.
- Added global Hy2 cooldown and real-traffic canary recovery for new logical connections.
- Fixed stale `RxGapAge` attribution by tracking the expected sequence of the current unresolved gap.
- Changed health attribution to a per-leg useful-goodput/ACK heuristic so slow leg0 traffic does not blame healthy Hy2.
- Changed the goodput baseline to fast-rise/slow-decay and freeze it while SUSPECT is active.
- Added single-owner Hy2 probation canary concurrency, normal-close owner release, and failed-probation cooldown backoff.
- Split RX/TX logical goodput and baselines so healthy upload cannot mask broken download, or vice versa.
- Added a 1 MiB / 3 active-window useful-data gate for probation recovery and a cross-connection 3/30s initial-failure learner.
- Changed continuous-pending gap age to start from the current unresolved expected sequence instead of an old pending-frame timestamp.
- Rebuilt Windows from the pinned alpha2.3 source and removed stale/secret deployment artifacts from the release tree.
- Expanded deterministic long-duration, leg-attribution, canary, gap-age, carrier-switch, race, and regression coverage.
- Preserved alpha2.2 server compatibility, hello version `4`, logical CLOSE, replay, and graceful tail behavior.

## alpha2.2 — graceful tail + logical CLOSE

- Replaced the fixed graceful-drain deadline (maximum 5 seconds) with ACK-progress-based draining. `recovery_timeout` now acts as a stall/no-progress timeout during graceful close.
- Added SMP3 logical `CLOSE` control frames so peers can distinguish normal logical EOF from carrier failure and avoid pointless reconnects after a completed stream.
- Routed/server-side close now starts graceful drain instead of immediately failing the core; the server session stays registered until the core is actually done.
- Carrier failures during the drain remain recoverable, while carrier EOFs during final CLOSE teardown do not start new reconnect/recovery work.
- Added regression tests for long-but-progressing close drains and prompt peer termination through logical CLOSE.
- Bumped the SMP3 hello version byte from 3 to 4. alpha2.2 is intentionally not wire-compatible with alpha2/alpha2.1; upgrade both endpoints together.
- Configuration schema is unchanged.

## alpha2.1 — SMP3 hotfix

- Changed impossible/future cumulative ACK handling from fatal leg teardown to non-destructive ignore; TX acknowledgement state is never advanced by such an ACK, and rate-limited anomaly logging remains enabled.
- Added regression coverage proving a future ACK neither retires outstanding payload nor kills the transport leg.
- Added per-leg delayed/coalesced reconnect scheduling so a carrier that accepts then immediately EOFs cannot bypass `redial_interval`.
- Suppressed recovery callbacks during normal logical-core teardown.
- Cancel reconnect dial contexts when the logical connection closes.
- Included the `net.Buffers` addressability build fix directly in `protocol.go`.
- Wire protocol remains SMP3; no config schema change is required from alpha2.

## alpha2 — SMP3

- Wire protocol magic/version bumped from SMP2 to SMP3.
- Added cumulative ACK control frames.
- Added bounded outstanding TX window and ACK-driven release.
- Added cross-leg retransmission of unacknowledged frames.
- Added graceful single-leg removal instead of immediate core failure.
- Added emergency secondary activation on preferred-leg loss.
- Added client automatic re-dial/rejoin of either failed leg.
- Added `recovery_timeout`, `retransmit_timeout`, `ack_interval`, `max_inflight_frames`, `redial_interval`.
- Added per-child aggregation `endpoints` while preserving shared `server/server_port` compatibility.
- Reject orphan secondary legs from creating new server sessions.
- Added standalone reliability tests for hard failure and black-hole replay.
- Added silent-stall activation when the preferred TCP stays ESTABLISHED but logical ACKs stop.
- Fixed stale recovery timers so a later outage receives a fresh full recovery window.
- Fixed the example Snell carrier pairing: sing-box outbound v4 to Snell server v5.
- Build script now pins Go 1.25.5 through GOTOOLCHAIN and includes sing-box release/LDFLAGS.
- Install scripts are safer for in-place upgrades.
- Downstream executable/service renamed to `smp3-proxy` to avoid implying an official upstream build.

## alpha1 — SMP2

- HMAC-authenticated hello with timestamp and replay nonce.
- Lazy secondary activation.
- Bidirectional activation signalling.
- Weighted two-leg striping and ordered reassembly.

# Standalone Client Source Provenance

The Standalone client in this release is the canonical R14.1.2 source line
promoted from the qualified disposable snapshot:

```text
D:\smp3-singbox\.work\r14.1.2-20260910-231500\r14\client
D:\smp3-singbox\.work\r14.1.2-20260910-231500\r14\cmd\smp3-client
```

The source includes the already-qualified preferred-startup contract:

```text
startup_policy: preferred
startup_preferred_leg: 0
startup_grace: 500ms
```

The release source keeps the existing SMP3 wire, server, carrier, and
production configuration semantics. This document records source lineage;
the immutable release commit and every built artifact hash are recorded in
the unified release report and `SHA256SUMS`.

The copied server-backed SOCKS/UDP integration tests that require historical
server APIs remain behind the explicit `standalone_server_integration` build
tag and are not part of the default release gate.

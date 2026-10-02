# Race validation status

## Result

Race validation is now complete after installing MSYS2 and its UCRT64 GCC toolchain.

```text
core:   go test -race ./... PASS
client: go test -race ./... PASS
server: go test -race ./... PASS
```

The commands used `CC=C:\\msys64\\ucrt64\\bin\\gcc.exe`, `CGO_ENABLED=1`, and the UCRT64 bin directory on `PATH`.

## Historical host limitation

Before MSYS2 installation, `go test -race ./...` was not executable because no GCC/MinGW C toolchain was installed.

Observed attempts:

```text
CGO_ENABLED=1 go test -race ./...
  gcc not found

CC=clang go test -race ./...
  linker error: cannot open mingwex.lib

CC=clang-cl CXX=clang-cl go test -race ./...
  clang-cl rejects Go's cgo flags (-dM, -fno-stack-protector)
```

The host has LLVM `clang.exe`, `clang-cl.exe`, and `ld.lld.exe`, but not the MinGW runtime libraries required by Go's Windows race build. No production code was changed to bypass this gate.

## Completed race verification

MSYS2 UCRT64 GCC is now installed at `C:\msys64\ucrt64\bin\gcc.exe`. The following command completed successfully from `core`:

```powershell
$env:CGO_ENABLED='1'
$env:Path='C:\msys64\ucrt64\bin;'+$env:Path
go test -race ./...
```

Result: `ok github.com/Superbias/smp3-multipath-kit-public/smp3core`.
All non-race core, client, and server tests also pass on this host. Long-flow and continuous-stream evidence is recorded in `artifacts/` and `SMP3_BACKPRESSURE_RUNTIME_EVIDENCE.md`.

## Native Linux recovery-candidate gate

The native qualification host at `192.168.112.104` has `/usr/local/go/bin/go` 1.22.12, Linux/amd64, GCC 4.8.5 and `CGO_ENABLED=1`. After syncing the decoupled assignment recovery candidate, this command passed:

```sh
CGO_ENABLED=1 /usr/local/go/bin/go test -race ./...
```

The same native command was rerun after the frontier-exposure telemetry and
benchmark-only grace candidate were added; it passed in 4.590 s.

It was rerun after the explicit production aggregation mode and internal
decoupled admission extraction were added; it passed in 4.555 s.

## Final RC equivalence
The final RC runtime was race-qualified on native Linux and the v2.5.0 preparation preserves runtime behavior; only release metadata and documentation are changed in this preparation commit.


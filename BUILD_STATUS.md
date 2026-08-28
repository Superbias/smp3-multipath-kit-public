# alpha2.3-r11 build status

Status: **final closeout artifact (not published)**.

Completed in the artifact environment:

- 101 standalone multipath tests / 101 injected multipath tests / 441 generated source-tree `Test*` cases
- standalone normal Go test PASS
- standalone normal stress (`-count=20`) PASS
- standalone race gate (`-race -count=5`) PASS
- TCP core/protocol race stress (`-race -count=20`) PASS
- adaptive-controller race stress (`-race -count=20`) PASS
- Datagram race stress (`-race -count=100`) PASS
- standalone go vet PASS
- gofmt PASS
- JSON examples / source injector / shell syntax PASS via `validate-kit.sh`

The pinned source was injected and rebuilt from scratch after the r11 PacketConn integration changes. Both Linux/amd64 and Windows/amd64 binaries were produced; no older binary was reused or mislabeled as r11. The resulting binaries report `1.14.0-beta.14-smp3-alpha2.3-r11`.

The required integration build is:

- sing-box `v1.14.0-beta.14`
- commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- Go `1.25.5`

The reproducible build commands are:

```bash
./validate-kit.sh
./build.sh
```

Both binaries report:

```text
1.14.0-beta.14-smp3-alpha2.3-r11
```

Final live TCP/UDP acceptance is recorded in `TEST_RESULTS.txt`. The H3 100 MiB diagnostic is explicitly inconclusive because it reproduces on direct aioquic without SMP3. A full generated-tree `go test ./...` has non-SMP3 failures caused by namespace permissions, a current-Go runtime linkname incompatibility, and internet-dependent TLS tests; the SMP3 package gates pass.

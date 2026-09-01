#!/usr/bin/env python3
"""Run the standalone multipath package in a disposable temporary module.

The adapter sources are tested in a disposable sing-box-shaped module while
the canonical Core is copied into a local replacement module.
"""

from pathlib import Path
import os
import shutil
import subprocess
import sys
import tempfile


SOURCE_FILES = (
    "adaptive.go",
    "core.go",
    "datagram.go",
    "protocol.go",
    "adaptive_test.go",
    "core_test.go",
    "core_tx_test_helpers.go",
    "core_wire_test_compat.go",
    "datagram_test_compat.go",
    "datagram_test.go",
    "protocol_test.go",
    "protocol_differential_test.go",
    "protocol_legacy_test.go",
    "stream_leg_test.go",
    "wire_golden_test.go",
)


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[2] not in {"test", "vet"}:
        print("usage: run-standalone-go.py ROOT (test|vet) [go args...]", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    go_args = sys.argv[3:]
    enable_cgo = "--enable-cgo" in go_args
    go_args = [arg for arg in go_args if arg != "--enable-cgo"]
    source = root / "src" / "protocol" / "multipath"

    with tempfile.TemporaryDirectory(prefix="smp3-standalone-") as temporary:
        target = Path(temporary) / "protocol" / "multipath"
        target.mkdir(parents=True)
        for name in SOURCE_FILES:
            shutil.copy2(source / name, target / name)
        shutil.copytree(root / "core", Path(temporary) / "smp3core")
        shutil.copytree(source / "testdata", target / "testdata")
        module = Path(temporary) / "go.mod"
        module.write_text(
            "module github.com/sagernet/sing-box\n\n"
            "go 1.22\n\n"
            "require github.com/Superbias/smp3-multipath-kit-public/smp3core v0.0.0\n\n"
            "replace github.com/Superbias/smp3-multipath-kit-public/smp3core => ./smp3core\n",
            encoding="utf-8",
        )
        command = ["go", operation, "./..."] + go_args
        environment = os.environ.copy()
        environment.setdefault("GOTOOLCHAIN", "local")
        if enable_cgo:
            environment["CGO_ENABLED"] = "1"
        completed = subprocess.run(command, cwd=temporary, env=environment)
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())

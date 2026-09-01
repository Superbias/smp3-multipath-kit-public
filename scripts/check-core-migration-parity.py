#!/usr/bin/env python3
"""Fingerprint the frozen Core before/after its module relocation."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import sys


def files(core: Path) -> dict[str, str]:
    paths = sorted(core.glob("*.go"))
    if not paths:
        raise SystemExit(f"no Core Go files found: {core}")
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def write_manifest(path: Path, label: str, prefix: str, hashes: dict[str, str]) -> None:
    lines = [label]
    lines.extend(f"{digest}  {prefix}/{name}" for name, digest in sorted(hashes.items()))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_manifest(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        digest, _, name = line.partition("  ")
        result[Path(name).name] = digest
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    current = files(root / "core")
    before_path = root / "CORE_SOURCE_MANIFEST_BEFORE"
    after_path = root / "CORE_SOURCE_MANIFEST_AFTER"
    if args.write or not before_path.exists():
        write_manifest(before_path, "CORE_SOURCE_MANIFEST_BEFORE", "src/protocol/multipath/smp3core", current)
    write_manifest(after_path, "CORE_SOURCE_MANIFEST_AFTER", "core", current)
    before = read_manifest(before_path)
    if before != current:
        print("Core migration parity: FAIL", file=sys.stderr)
        for name in sorted(set(before) | set(current)):
            if before.get(name) != current.get(name):
                print(f"{name}: before={before.get(name)} after={current.get(name)}", file=sys.stderr)
        return 1
    legacy = root / "src/protocol/multipath/smp3core"
    if legacy.exists():
        print(f"legacy Core implementation still exists: {legacy}", file=sys.stderr)
        return 1
    print(f"Core migration parity: PASS files={len(current)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Create and verify per-test fixture identity manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path


def fingerprint(path: Path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "bytes": stat.st_size,
        "sha256": digest.hexdigest(),
        "mtime_ns": stat.st_mtime_ns,
        "captured_at_ns": time.time_ns(),
    }


def fail(message: str) -> int:
    print(f"FIXTURE_INVALID: {message}", file=sys.stderr)
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-name", required=True)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--phase", choices=("pre", "post"), required=True)
    parser.add_argument("--destination", type=Path)
    options = parser.parse_args()

    if not options.source.is_file():
        return fail(f"source does not exist: {options.source}")
    if options.phase == "post" and options.destination is not None and not options.destination.is_file():
        return fail(f"destination does not exist: {options.destination}")

    manifest = {"tests": {}}
    if options.manifest.exists():
        try:
            manifest = json.loads(options.manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return fail(f"cannot read manifest: {exc}")
    entry = manifest.setdefault("tests", {}).setdefault(options.test_name, {})
    current = fingerprint(options.source)
    if options.phase == "pre":
        if entry.get("source_pre") is not None:
            return fail(f"pre-transfer identity already exists for {options.test_name}")
        entry["source_pre"] = current
        entry["source"] = current["path"]
        entry["test_name"] = options.test_name
    else:
        if entry.get("source_pre") is None:
            return fail(f"missing pre-transfer identity for {options.test_name}")
        entry["source_post"] = current
        if current["path"] != entry["source_pre"]["path"]:
            return fail("source path changed during transfer")
        if current["bytes"] != entry["source_pre"]["bytes"] or current["sha256"] != entry["source_pre"]["sha256"]:
            return fail("source bytes or SHA256 changed during transfer")
        if options.destination is not None:
            destination = fingerprint(options.destination)
            entry["destination"] = destination
            if destination["bytes"] != entry["source_pre"]["bytes"]:
                return fail("destination byte count differs from source")
            if destination["sha256"] != entry["source_pre"]["sha256"]:
                return fail("destination SHA256 differs from source")

    options.manifest.parent.mkdir(parents=True, exist_ok=True)
    options.manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(entry, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

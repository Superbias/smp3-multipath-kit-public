#!/usr/bin/env python3
"""Capture full migration-checker failure sets for frozen B0, C0, and C1."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


MANIFEST = "CORE_SOURCE_MANIFEST_AFTER"
CHECKER = Path("scripts/check-core-migration-parity.py")
FAILURE = re.compile(r"([^\s:]+\.go): before=(\S+) after=(\S+)")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalized(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def run_checker(label: str, root: Path, output: Path) -> dict:
    manifest = root / MANIFEST
    original = manifest.read_bytes()
    before_sha = sha(original)
    command = [sys.executable, str(root / CHECKER), str(root)]
    (output / f"parity-{label}-command.json").write_text(
        json.dumps({"cwd": str(root), "command": command}, indent=2) + "\n",
        encoding="utf-8",
    )
    try:
        completed = subprocess.run(command, cwd=root, capture_output=True, text=True)
    finally:
        manifest.write_bytes(original)
    after_sha = sha(manifest.read_bytes())
    if before_sha != after_sha:
        raise RuntimeError(f"checker mutated {label} manifest: {before_sha} != {after_sha}")
    raw = completed.stdout + completed.stderr
    (output / f"parity-{label}-raw.log").write_text(raw, encoding="utf-8")
    failures = {
        path: {"before": before, "after": after}
        for path, before, after in FAILURE.findall(raw)
    }
    return {
        "exit_code": completed.returncode,
        "manifest_sha256_before": before_sha,
        "manifest_sha256_after": after_sha,
        "failures": failures,
        "failure_count": len(failures),
        "raw_log_sha256": sha(raw.encode()),
    }


def file_change(label_a: str, root_a: Path, label_b: str, root_b: Path, name: str) -> dict:
    path_a, path_b = root_a / "core" / name, root_b / "core" / name
    data_a = path_a.read_bytes() if path_a.exists() else None
    data_b = path_b.read_bytes() if path_b.exists() else None
    if data_a is None or data_b is None:
        classification = "added_or_removed_source"
    elif data_a == data_b:
        classification = "identical_source"
    elif normalized(data_a) == normalized(data_b):
        classification = "CRLF_to_LF_only"
    else:
        classification = "source_content_changed"
    return {
        "file": name,
        "source_comparison": f"{label_a}->{label_b}",
        "classification": classification,
        f"{label_a}_raw_sha256": sha(data_a) if data_a is not None else None,
        f"{label_b}_raw_sha256": sha(data_b) if data_b is not None else None,
        "normalized_equal": data_a is not None and data_b is not None and normalized(data_a) == normalized(data_b),
    }


def compare(label_a: str, label_b: str, a: dict, b: dict, root_a: Path, root_b: Path) -> dict:
    set_a, set_b = set(a["failures"]), set(b["failures"])
    all_paths = sorted(set_a | set_b)
    rows = []
    for name in all_paths:
        left, right = a["failures"].get(name), b["failures"].get(name)
        if left == right:
            failure_delta = "unchanged_failure_record"
        elif left is None:
            failure_delta = "new_failure_record"
        elif right is None:
            failure_delta = "removed_failure_record"
        else:
            failure_delta = "changed_failure_hashes"
        rows.append({
            **file_change(label_a, root_a, label_b, root_b, name),
            f"{label_a}_failure": left,
            f"{label_b}_failure": right,
            "failure_delta": failure_delta,
        })
    return {
        "from": label_a,
        "to": label_b,
        "from_failure_count": len(set_a),
        "to_failure_count": len(set_b),
        "new_paths": sorted(set_b - set_a),
        "removed_paths": sorted(set_a - set_b),
        "changed_records": [row for row in rows if row["failure_delta"] == "changed_failure_hashes"],
        "all_paths": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--b0", required=True, type=Path)
    parser.add_argument("--c0", required=True, type=Path)
    parser.add_argument("--c1", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    roots = {"B0": args.b0.resolve(), "C0": args.c0.resolve(), "C1": args.c1.resolve()}
    results = {label: run_checker(label, root, output) for label, root in roots.items()}
    comparisons = [
        compare("B0", "C0", results["B0"], results["C0"], roots["B0"], roots["C0"]),
        compare("C0", "C1", results["C0"], results["C1"], roots["C0"], roots["C1"]),
        compare("B0", "C1", results["B0"], results["C1"], roots["B0"], roots["C1"]),
    ]
    overall = "FAIL" if any(result["exit_code"] != 0 for result in results.values()) else "PASS"
    summary = {"overall": overall, "runs": results, "comparisons": comparisons}
    (output / "parity-matrix.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"overall": overall, "failure_counts": {key: value["failure_count"] for key, value in results.items()}, "output": str(output)}, indent=2))
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

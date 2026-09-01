#!/usr/bin/env python3
"""Fail-closed exact TCP socket destruction helper for live acceptance.

This is test tooling only.  It deliberately requires an already-calibrated
tuple and inode; it never discovers a leg and never widens a kill filter.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path


ENDPOINT_RE = re.compile(r"(?P<host>\[[^]]+\]|[^\s:]+):(?P<port>\d+)")
INODE_RE = re.compile(r"\bino:(?P<inode>\d+)")


def parse_rows(text: str, local_ip: str, local_port: int, peer_ip: str, peer_port: int):
    local = f"{local_ip}:{local_port}"
    peer = f"{peer_ip}:{peer_port}"
    rows = []
    for line in text.splitlines():
        if local not in line or peer not in line:
            continue
        inode_match = INODE_RE.search(line)
        rows.append(
            {
                "line": line,
                "local": local,
                "peer": peer,
                "inode": inode_match.group("inode") if inode_match else None,
            }
        )
    return rows


def contains_inode(text: str, inode: str) -> bool:
    return any(match.group("inode") == str(inode) for match in INODE_RE.finditer(text))


def run_ss(args: list[str], kill: bool = False):
    command = ["ss", "-Ktn" if kill else "-Htnpe", *args]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def write_text(path: Path, value: str):
    path.write_text(value, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--local-port", required=True, type=int)
    parser.add_argument("--peer-ip", required=True)
    parser.add_argument("--peer-port", required=True, type=int)
    parser.add_argument("--expected-inode", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--poll-ms", type=int, default=20)
    parser.add_argument("--post-timeout-ms", type=int, default=1000)
    options = parser.parse_args()

    out_dir = options.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    filter_args = [
        "state",
        "established",
        "src",
        options.local_ip,
        "sport",
        "=",
        f":{options.local_port}",
        "dst",
        options.peer_ip,
        "dport",
        "=",
        f":{options.peer_port}",
    ]
    evidence = {
        "local_ip": options.local_ip,
        "local_port": options.local_port,
        "peer_ip": options.peer_ip,
        "peer_port": options.peer_port,
        "expected_inode": str(options.expected_inode),
        "filter_args": filter_args,
        "started_at_ns": time.time_ns(),
        "result": "INVALID",
        "reason": None,
    }

    if hasattr(os, "geteuid") and os.geteuid() != 0:
        evidence["reason"] = "injector is not uid 0"
        write_text(out_dir / "evidence.json", json.dumps(evidence, indent=2))
        print("KILL_RESULT=INVALID")
        print(f"REASON={evidence['reason']}")
        return 2

    broad_pre = run_ss(["state", "established"])
    exact_pre = run_ss(filter_args)
    evidence["broad_pre"] = broad_pre
    evidence["exact_pre"] = exact_pre
    pre_rows = parse_rows(
        exact_pre["stdout"],
        options.local_ip,
        options.local_port,
        options.peer_ip,
        options.peer_port,
    )
    evidence["pre_rows"] = pre_rows
    write_text(out_dir / "PRE.txt", exact_pre["stdout"] + exact_pre["stderr"])

    if len(pre_rows) != 1:
        evidence["reason"] = f"exact PRE tuple count is {len(pre_rows)}, expected 1"
    elif pre_rows[0]["inode"] != str(options.expected_inode):
        evidence["reason"] = (
            f"inode mismatch: observed {pre_rows[0]['inode']!r}, "
            f"expected {options.expected_inode!r}"
        )
    else:
        evidence["pre_inode"] = pre_rows[0]["inode"]
        evidence["kill_at_ns"] = time.time_ns()
        kill_result = run_ss(filter_args, kill=True)
        evidence["kill"] = kill_result
        write_text(out_dir / "KILL.txt", kill_result["stdout"] + kill_result["stderr"])

        deadline = time.monotonic() + options.post_timeout_ms / 1000
        post_samples = []
        while True:
            sample = run_ss(filter_args)
            rows = parse_rows(
                sample["stdout"],
                options.local_ip,
                options.local_port,
                options.peer_ip,
                options.peer_port,
            )
            post_samples.append(
                {"at_ns": time.time_ns(), "result": sample, "rows": rows}
            )
            if not rows or time.monotonic() >= deadline:
                break
            time.sleep(max(options.poll_ms, 1) / 1000)

        evidence["post_samples"] = post_samples
        final_sample = post_samples[-1]
        evidence["post"] = final_sample["result"]
        evidence["post_rows"] = final_sample["rows"]
        broad_post = run_ss(["state", "established"])
        evidence["broad_post"] = broad_post
        write_text(
            out_dir / "POST.txt",
            final_sample["result"]["stdout"] + final_sample["result"]["stderr"],
        )
        if final_sample["rows"]:
            evidence["reason"] = "exact POST tuple still exists"
        elif contains_inode(broad_post["stdout"], str(options.expected_inode)):
            evidence["reason"] = "expected inode still exists after POST"
        elif kill_result["returncode"] != 0:
            evidence["reason"] = f"ss -K exit code was {kill_result['returncode']}"
        else:
            evidence["result"] = "PASS"
            evidence["reason"] = "exact tuple and expected inode disappeared"

    evidence["finished_at_ns"] = time.time_ns()
    write_text(out_dir / "evidence.json", json.dumps(evidence, indent=2))
    print(f"KILL_RESULT={evidence['result']}")
    if evidence["reason"]:
        print(f"REASON={evidence['reason']}")
    print(f"EVIDENCE={out_dir / 'evidence.json'}")
    return 0 if evidence["result"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

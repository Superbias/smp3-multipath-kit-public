#!/usr/bin/env python3
"""Map one SMP3 leg by peer IP and invoke the fail-closed exact killer."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path


ENDPOINT_RE = re.compile(r"^(?P<host>\[[^]]+\]|[^\s:]+):(?P<port>\d+)$")
INODE_RE = re.compile(r"\bino:(?P<inode>\d+)")
KILLER = Path(__file__).with_name("live-exact-socket-kill.py")


def endpoint(token: str):
    match = ENDPOINT_RE.match(token)
    if not match:
        return None
    return match.group("host").strip("[]"), int(match.group("port"))


def parse_rows(text: str, local_ip: str, local_port: int):
    rows = []
    expected_local = (local_ip, local_port)
    for line in text.splitlines():
        endpoints = [parsed for token in line.split() if (parsed := endpoint(token))]
        if len(endpoints) < 2 or endpoints[0] != expected_local:
            continue
        inode_match = INODE_RE.search(line)
        rows.append(
            {
                "line": line,
                "local_ip": endpoints[0][0],
                "local_port": endpoints[0][1],
                "peer_ip": endpoints[1][0],
                "peer_port": endpoints[1][1],
                "inode": inode_match.group("inode") if inode_match else None,
            }
        )
    return rows


def snapshot(local_ip: str, local_port: int):
    command = ["ss", "-Htnpe", "state", "established"]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    return {
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "rows": parse_rows(result.stdout, local_ip, local_port),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leg", required=True, choices=("leg0", "leg1"))
    parser.add_argument("--local-ip", required=True)
    parser.add_argument("--local-port", required=True, type=int)
    parser.add_argument("--peer-ip", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--wait-ms", type=int, default=120000)
    parser.add_argument("--rejoin-wait-ms", type=int, default=5000)
    options = parser.parse_args()
    options.out_dir.mkdir(parents=True, exist_ok=True)
    evidence = {
        "leg": options.leg,
        "local_ip": options.local_ip,
        "local_port": options.local_port,
        "peer_ip": options.peer_ip,
        "started_at_ns": time.time_ns(),
        "result": "INVALID",
    }

    deadline = time.monotonic() + options.wait_ms / 1000
    selected = None
    last = None
    while time.monotonic() < deadline:
        last = snapshot(options.local_ip, options.local_port)
        rows = last["rows"]
        if len(rows) == 2:
            candidates = [row for row in rows if row["peer_ip"] == options.peer_ip]
            if len(candidates) == 1 and candidates[0]["inode"]:
                selected = candidates[0]
                break
        time.sleep(0.02)

    evidence["last_snapshot"] = last
    if selected is None:
        evidence["reason"] = "did not observe exactly two sockets and one target peer with inode"
        (options.out_dir / "mapping.json").write_text(
            json.dumps(evidence, indent=2), encoding="utf-8"
        )
        print("KILL_RESULT=INVALID")
        print(f"REASON={evidence['reason']}")
        return 2

    evidence["old_socket"] = selected
    killer_out = options.out_dir / "kill"
    command = [
        sys.executable,
        str(KILLER),
        "--local-ip",
        options.local_ip,
        "--local-port",
        str(options.local_port),
        "--peer-ip",
        selected["peer_ip"],
        "--peer-port",
        str(selected["peer_port"]),
        "--expected-inode",
        selected["inode"],
        "--out-dir",
        str(killer_out),
    ]
    evidence["killer_command"] = command
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    evidence["killer_result"] = {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    rejoin = None
    if result.returncode == 0:
        rejoin_deadline = time.monotonic() + options.rejoin_wait_ms / 1000
        while time.monotonic() < rejoin_deadline:
            current = snapshot(options.local_ip, options.local_port)
            for row in current["rows"]:
                if (
                    row["peer_ip"] == options.peer_ip
                    and row["peer_port"] != selected["peer_port"]
                    and row["inode"] != selected["inode"]
                ):
                    rejoin = row
                    break
            if rejoin:
                break
            time.sleep(0.02)
    evidence["rejoin_socket"] = rejoin
    evidence["finished_at_ns"] = time.time_ns()
    evidence["result"] = "PASS" if result.returncode == 0 and rejoin else "INVALID"
    if evidence["result"] != "PASS":
        evidence["reason"] = (
            "exact killer did not pass"
            if result.returncode != 0
            else "same-leg rejoin socket with a new tuple/inode was not observed"
        )
    (options.out_dir / "mapping.json").write_text(
        json.dumps(evidence, indent=2), encoding="utf-8"
    )
    print(f"KILL_RESULT={evidence['result']}")
    if evidence.get("reason"):
        print(f"REASON={evidence['reason']}")
    print(f"EVIDENCE={options.out_dir / 'mapping.json'}")
    return 0 if evidence["result"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

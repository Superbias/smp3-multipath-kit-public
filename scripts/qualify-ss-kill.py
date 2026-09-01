#!/usr/bin/env python3
"""Independently qualify the exact socket killer on a disposable TCP flow."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
KILLER = ROOT / "live-exact-socket-kill.py"


def command_result(command: list[str]):
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=19090)
    parser.add_argument("--out-dir", required=True, type=Path)
    options = parser.parse_args()
    out_dir = options.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    evidence = {
        "started_at_ns": time.time_ns(),
        "uid": os.geteuid() if hasattr(os, "geteuid") else None,
        "environment": {
            "uname": command_result(["uname", "-a"]),
            "ss_version": command_result(["ss", "-V"]),
            "ip_version": command_result(["ip", "-V"]),
            "id": command_result(["id"]),
        },
        "port": options.port,
        "result": "FAIL",
    }
    (out_dir / "environment.json").write_text(
        json.dumps(evidence["environment"], indent=2), encoding="utf-8"
    )

    if evidence["uid"] != 0:
        evidence["result"] = "BLOCKED_NON_ROOT"
        evidence["reason"] = "qualification must run as uid 0"
        (out_dir / "qualification.json").write_text(
            json.dumps(evidence, indent=2), encoding="utf-8"
        )
        print("EXACT SOCKET DESTROY HARNESS: FAIL")
        print("REASON=injector is not uid 0")
        return 2

    ready = threading.Event()
    stop = threading.Event()
    server_error: list[str] = []
    accepted: dict[str, object] = {}

    def server():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", options.port))
                listener.listen(1)
                ready.set()
                listener.settimeout(1)
                while not stop.is_set():
                    try:
                        conn, address = listener.accept()
                        break
                    except socket.timeout:
                        continue
                else:
                    return
                with conn:
                    accepted["peer_port"] = address[1]
                    conn.settimeout(1)
                    while not stop.is_set():
                        try:
                            conn.sendall(b"x" * 1024)
                            conn.recv(1024)
                        except socket.timeout:
                            continue
                        except OSError as exc:
                            accepted["connection_error"] = repr(exc)
                            break
                        time.sleep(0.05)
        except Exception as exc:  # pragma: no cover - diagnostic path
            server_error.append(repr(exc))
            ready.set()

    server_thread = threading.Thread(target=server, daemon=True)
    server_thread.start()
    if not ready.wait(3):
        evidence["reason"] = "disposable server did not become ready"
        stop.set()
        return finish(evidence, out_dir, 2)

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(1)
    try:
        client.connect(("127.0.0.1", options.port))
        deadline = time.monotonic() + 3
        peer_port = None
        while time.monotonic() < deadline:
            peer_port = accepted.get("peer_port")
            if peer_port:
                break
            time.sleep(0.02)
        if not peer_port:
            evidence["reason"] = "disposable server accepted no connection"
            return finish(evidence, out_dir, 2)

        command = [
            sys.executable,
            str(KILLER),
            "--local-ip",
            "127.0.0.1",
            "--local-port",
            str(options.port),
            "--peer-ip",
            "127.0.0.1",
            "--peer-port",
            str(peer_port),
            "--expected-inode",
            "AUTO",
            "--out-dir",
            str(out_dir / "kill"),
        ]

        # The reusable killer requires the inode captured by the caller.  Read
        # it from the exact pre-filter before invoking the kill phase.
        pre = command_result(
            [
                "ss",
                "-Htnpe",
                "state",
                "established",
                "src",
                "127.0.0.1",
                "sport",
                "=",
                f":{options.port}",
                "dst",
                "127.0.0.1",
                "dport",
                "=",
                f":{peer_port}",
            ]
        )
        inode = None
        for token in pre["stdout"].split():
            if token.startswith("ino:"):
                inode = token[4:]
                break
        evidence["pre_probe"] = pre
        if inode is None:
            evidence["reason"] = "qualification pre-filter did not expose an inode"
            return finish(evidence, out_dir, 2)
        command[command.index("AUTO")] = inode
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        evidence["killer"] = {
            "command": command,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        evidence["server_error"] = server_error
        evidence["connection_error"] = accepted.get("connection_error")
        if result.returncode == 0 and result.stdout.find("KILL_RESULT=PASS") >= 0:
            if accepted.get("connection_error") or not client.recv(1):
                evidence["result"] = "PASS"
                evidence["reason"] = "tuple and inode disappeared; application socket broke"
            else:
                evidence["reason"] = "killer passed but client socket remained readable"
        else:
            evidence["reason"] = "reusable exact-socket killer did not pass"
    finally:
        stop.set()
        try:
            client.close()
        except OSError:
            pass
        server_thread.join(timeout=2)
    return finish(evidence, out_dir, 0 if evidence["result"] == "PASS" else 2)


def finish(evidence: dict, out_dir: Path, code: int) -> int:
    evidence["finished_at_ns"] = time.time_ns()
    (out_dir / "qualification.json").write_text(
        json.dumps(evidence, indent=2), encoding="utf-8"
    )
    if evidence["result"] == "PASS":
        print("EXACT SOCKET DESTROY HARNESS: PASS")
    else:
        print("EXACT SOCKET DESTROY HARNESS: FAIL")
        if evidence.get("reason"):
            print(f"REASON={evidence['reason']}")
    print(f"EVIDENCE={out_dir / 'qualification.json'}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())

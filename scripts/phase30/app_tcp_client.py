#!/usr/bin/env python3
"""Single-connection loopback SOCKS client used by the Phase 30 process harness."""

import argparse
import hashlib
import json
import socket
import threading
import time
import traceback
import uuid
from pathlib import Path


def endpoint(value):
    host, sep, port_text = value.rpartition(":")
    if not sep:
        raise argparse.ArgumentTypeError("endpoint must be IPv4:port")
    try:
        socket.inet_aton(host)
        port = int(port_text)
    except (OSError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if host != "127.0.0.1" or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("test endpoints must be 127.0.0.1:port")
    return host, port


def exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError(f"early EOF after {len(data)} of {size} bytes")
        data.extend(part)
    return bytes(data)


def transfer(args):
    app_id = uuid.uuid4().hex
    result = {"application_connection_id": app_id, "application_connections": 1, "strict_eof": False, "sha256_match": False}
    app = socket.create_connection(args.listen, timeout=10)
    app.settimeout(args.timeout_seconds)
    try:
        app.sendall(b"\x05\x01\x00")
        if exact(app, 2) != b"\x05\x00":
            raise RuntimeError("client SOCKS5 ingress rejected no-auth")
        host, port = args.target
        app.sendall(b"\x05\x01\x00\x01" + socket.inet_aton(host) + port.to_bytes(2, "big"))
        reply = exact(app, 4)
        if reply[3] == 1:
            exact(app, 4)
        elif reply[3] == 3:
            exact(app, exact(app, 1)[0])
        elif reply[3] == 4:
            exact(app, 16)
        else:
            raise RuntimeError(f"invalid SOCKS5 reply address type: {reply!r}")
        exact(app, 2)
        if reply[0] != 5 or reply[1] != 0:
            raise RuntimeError(f"client SOCKS5 CONNECT failed: {reply!r}")

        payload_size = args.payload_mib * 1024 * 1024
        block = bytes((index * 31 + 7) & 255 for index in range(65536))
        expected = hashlib.sha256()
        remaining = payload_size
        while remaining:
            part = block[:min(len(block), remaining)]
            expected.update(part)
            remaining -= len(part)
        actual = hashlib.sha256()
        received = 0
        send_state = {"sent": 0, "error": None}
        started = time.monotonic()

        def send_request():
            try:
                sent = 0
                while sent < payload_size:
                    part = block[:min(len(block), payload_size - sent)]
                    app.sendall(part)
                    sent += len(part)
                send_state["sent"] = sent
            except Exception as exc:  # reported to the main thread after the read side unblocks
                send_state["sent"] = sent if "sent" in locals() else 0
                send_state["error"] = repr(exc)

        sender = threading.Thread(target=send_request, name="phase30-app-sender", daemon=True)
        sender.start()
        while received < payload_size:
            part = app.recv(min(65536, payload_size - received))
            if not part:
                raise EOFError(f"early application EOF at {received}/{payload_size}")
            actual.update(part)
            received += len(part)
        tail = app.recv(1)
        if tail:
            raise RuntimeError("extra application payload after expected length")
        sender.join(timeout=5)
        if sender.is_alive():
            raise TimeoutError("application sender did not finish after full response EOF")
        if send_state["error"]:
            raise RuntimeError(f"application sender failed after {send_state['sent']} bytes: {send_state['error']}")
        elapsed = time.monotonic() - started
        expected_sha, actual_sha = expected.hexdigest(), actual.hexdigest()
        return {**result, "payload_bytes_sent": send_state["sent"], "payload_bytes_received": received,
                "expected_sha256": expected_sha, "received_sha256": actual_sha,
                "sha256_match": expected_sha == actual_sha, "strict_eof": True, "extra_bytes": 0,
                "elapsed_seconds": elapsed, "application_goodput_mbps": received * 8 / elapsed / 1_000_000,
                "status": "PASS" if expected_sha == actual_sha else "FAIL"}
    finally:
        app.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True, type=endpoint)
    parser.add_argument("--target", required=True, type=endpoint)
    parser.add_argument("--payload-mib", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.payload_mib <= 0 or args.timeout_seconds < 10:
        parser.error("payload must be positive and timeout at least 10 seconds")
    try:
        result = transfer(args)
    except Exception as exc:
        result = {"application_connection_id": None, "application_connections": 0,
                  "status": "FAIL", "error": repr(exc), "strict_eof": False, "sha256_match": False}
        args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        traceback.print_exc()
        raise
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if result["status"] != "PASS":
        raise RuntimeError("application payload hash mismatch")


if __name__ == "__main__":
    main()

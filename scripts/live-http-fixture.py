#!/usr/bin/env python3
"""Serve a frozen download fixture and capture one upload for live tests."""

from __future__ import annotations

import argparse
import hashlib
import http.server
from pathlib import Path


class FixtureHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_GET(self):
        if self.path != "/test.bin":
            self.send_error(404)
            return
        path = self.server.root / "test.bin"
        try:
            size = path.stat().st_size
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            with path.open("rb") as source:
                while block := source.read(1024 * 1024):
                    self.wfile.write(block)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_PUT(self):
        self.receive_upload()

    def do_POST(self):
        self.receive_upload()

    def receive_upload(self):
        try:
            length = int(self.headers.get("Content-Length", "-1"))
        except ValueError:
            length = -1
        if length < 0:
            self.send_error(411)
            return
        digest = hashlib.sha256()
        received = 0
        with self.server.upload_output.open("wb") as destination:
            while received < length:
                block = self.rfile.read(min(1024 * 1024, length - received))
                if not block:
                    break
                destination.write(block)
                digest.update(block)
                received += len(block)
        body = f"BYTES={received} SHA256={digest.hexdigest()}\n".encode()
        self.send_response(200 if received == length else 400)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--upload-output", required=True, type=Path)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    options = parser.parse_args()
    options.upload_output.parent.mkdir(parents=True, exist_ok=True)
    server = http.server.ThreadingHTTPServer((options.bind, options.port), FixtureHandler)
    server.root = options.root
    server.upload_output = options.upload_output
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

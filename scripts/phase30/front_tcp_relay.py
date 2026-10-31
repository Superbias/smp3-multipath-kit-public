#!/usr/bin/env python3
"""Fixed-target, bounded-buffer, TLS-opaque TCP relay for Phase 30 local tests."""

import argparse
import ipaddress
import json
import signal
import socket
import threading
import time
import uuid


STOP = threading.Event()
LOG_LOCK = threading.Lock()
RATE_BURST_BYTES = 1024 * 1024


def parse_endpoint(value):
    host, sep, port_text = value.rpartition(":")
    if not sep:
        raise argparse.ArgumentTypeError("endpoint must be IPv4:port")
    try:
        ip = ipaddress.ip_address(host)
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not ip.is_loopback or ip.version != 4 or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("endpoint must use a loopback IPv4 address and port 1..65535")
    return str(ip), port


def emit(log, event):
    event["monotonic_ns"] = time.monotonic_ns()
    event["utc"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    with LOG_LOCK:
        log.write(json.dumps(event, separators=(",", ":")) + "\n")
        log.flush()


def rate_limited_send(sock, data, bytes_per_second, bucket):
    if bytes_per_second <= 0:
        sock.sendall(data)
        return True
    # Use a bounded token bucket instead of one timer wait per recv(). TCP may
    # return small fragments; per-fragment sleeps are badly quantized on
    # Windows and can turn a nominal 50 Mbps fixture into sub-Mbps service.
    now = time.monotonic()
    rate = bytes_per_second
    capacity = max(RATE_BURST_BYTES, len(data))
    tokens, updated = bucket
    tokens = min(capacity, tokens + max(0.0, now - updated) * rate)
    while tokens < len(data) and not STOP.is_set():
        delay = min(0.1, (len(data) - tokens) / rate)
        if STOP.wait(delay):
            bucket[:] = [tokens, time.monotonic()]
            return False
        now = time.monotonic()
        tokens = min(capacity, tokens + max(0.0, now - updated) * rate)
        updated = now
    if STOP.is_set():
        bucket[:] = [tokens, time.monotonic()]
        return False
    tokens -= len(data)
    sock.sendall(data)
    bucket[:] = [tokens, time.monotonic()]
    return True


def bridge_direction(source, target, label, bytes_per_second, counters, log, connection_id):
    bucket = [float(RATE_BURST_BYTES), time.monotonic()]
    try:
        while not STOP.is_set():
            data = source.recv(65536)
            if not data:
                try:
                    target.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                emit(log, {"event": "half_close", "connection_id": connection_id, "direction": label})
                return
            if not rate_limited_send(target, data, bytes_per_second, bucket):
                return
            counters[label] += len(data)
    except (OSError, ConnectionError) as exc:
        emit(log, {"event": "direction_error", "connection_id": connection_id, "direction": label, "error": str(exc)})
        try:
            target.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def handle(client, target, args, log):
    connection_id = uuid.uuid4().hex
    accepted_ns = time.monotonic_ns()
    upstream = None
    counters = {"front_to_landing": 0, "landing_to_front": 0}
    try:
        upstream = socket.create_connection(target, timeout=5)
        client.settimeout(None)
        upstream.settimeout(None)
        emit(log, {"event": "connected", "connection_id": connection_id, "front_local": client.getsockname(), "front_peer": client.getpeername(), "landing_local": upstream.getsockname(), "landing_peer": upstream.getpeername(), "tls_termination": False})
        limit = args.rate_mbps * 1_000_000 / 8 if args.rate_mbps > 0 else 0
        a = threading.Thread(target=bridge_direction, args=(client, upstream, "front_to_landing", limit, counters, log, connection_id), daemon=True)
        b = threading.Thread(target=bridge_direction, args=(upstream, client, "landing_to_front", limit, counters, log, connection_id), daemon=True)
        a.start()
        b.start()
        while (a.is_alive() or b.is_alive()) and not STOP.wait(0.1):
            pass
        a.join(timeout=1)
        b.join(timeout=1)
    except OSError as exc:
        emit(log, {"event": "connect_error", "connection_id": connection_id, "error": str(exc)})
    finally:
        if upstream is not None:
            upstream.close()
        client.close()
        emit(log, {"event": "closed", "connection_id": connection_id, "duration_ns": time.monotonic_ns() - accepted_ns, **counters})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True, type=parse_endpoint)
    parser.add_argument("--target", required=True, type=parse_endpoint)
    parser.add_argument("--rate-mbps", type=float, default=0, help="per-direction payload cap; 0 means unshaped")
    parser.add_argument("--jsonl", required=True)
    args = parser.parse_args()
    if args.rate_mbps < 0:
        parser.error("--rate-mbps cannot be negative")
    log = open(args.jsonl, "a", encoding="utf-8", buffering=1)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(args.listen)
    listener.listen(32)
    listener.settimeout(0.5)

    def stop_handler(_signum, _frame):
        STOP.set()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    emit(log, {"event": "listening", "listen": args.listen, "fixed_target": args.target, "rate_mbps_per_direction": args.rate_mbps, "open_proxy": False, "tls_termination": False, "buffer_bytes_per_read": 65536})
    workers = []
    try:
        while not STOP.is_set():
            try:
                client, _ = listener.accept()
            except socket.timeout:
                continue
            worker = threading.Thread(target=handle, args=(client, args.target, args, log), daemon=True)
            worker.start()
            workers.append(worker)
    finally:
        STOP.set()
        listener.close()
        for worker in workers:
            worker.join(timeout=2)
        emit(log, {"event": "stopped", "worker_count": len(workers)})
        log.close()


if __name__ == "__main__":
    main()

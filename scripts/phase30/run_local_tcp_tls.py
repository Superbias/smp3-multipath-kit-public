#!/usr/bin/env python3
"""Run independent Standalone Client, fixed TCP Front, and Landing processes.

All endpoints are loopback. Each SOCKS fixture is fixed to one configured
target, the Front is fixed-target, TLS stays end-to-end, and no payload bytes
are logged. One external SOCKS CONNECT carries one finite application stream.
This is Level B local-process evidence, not independent physical paths/WAN.
"""

import argparse
import datetime
import hashlib
import ipaddress
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path


STOP = threading.Event()
LOG_LOCK = threading.Lock()
TEST_PASSWORD = "phase30-local-test-only-not-a-production-secret"
ALPN = "smp3/1"
RATE_BURST_BYTES = 1024 * 1024


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for part in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        part = sock.recv(count - len(result))
        if not part:
            raise EOFError("unexpected EOF while reading SOCKS5 request")
        result.extend(part)
    return bytes(result)


def read_socks_target(sock):
    version, command, _reserved, atyp = exact(sock, 4)
    if version != 5 or command != 1:
        raise ValueError("only SOCKS5 CONNECT is accepted")
    if atyp == 1:
        host = socket.inet_ntoa(exact(sock, 4))
    elif atyp == 3:
        size = exact(sock, 1)[0]
        host = exact(sock, size).decode("ascii")
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, exact(sock, 16))
    else:
        raise ValueError("unsupported SOCKS address type")
    port = int.from_bytes(exact(sock, 2), "big")
    return host, port


def socks_reply(sock, code):
    sock.sendall(bytes([5, code, 0, 1, 0, 0, 0, 0, 0, 0]))


def emit_jsonl(path, lock, value):
    row = dict(value)
    row["monotonic_ns"] = time.monotonic_ns()
    row["utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with lock:
        with path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(row, separators=(",", ":")) + "\n")


def bridge(source, target, direction, rate_mbps, counters, errors, log_path, lock, conn_id):
    rate = rate_mbps * 1_000_000 / 8 if rate_mbps > 0 else 0
    bucket = [float(RATE_BURST_BYTES), time.monotonic()]
    try:
        while not STOP.is_set():
            data = source.recv(65536)
            if not data:
                try:
                    target.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                emit_jsonl(log_path, lock, {"event": "half_close", "connection_id": conn_id, "direction": direction})
                return
            if rate:
                now = time.monotonic()
                tokens = min(float(RATE_BURST_BYTES), bucket[0] + max(0.0, now - bucket[1]) * rate)
                while tokens < len(data) and not STOP.is_set():
                    if STOP.wait(min(0.1, (len(data) - tokens) / rate)):
                        return
                    now = time.monotonic()
                    tokens = min(float(RATE_BURST_BYTES), tokens + max(0.0, now - bucket[1]) * rate)
                    bucket[1] = now
                if STOP.is_set():
                    return
                tokens -= len(data)
                bucket[:] = [tokens, time.monotonic()]
            target.sendall(data)
            counters[direction] += len(data)
    except (OSError, EOFError) as error:
        errors.append(repr(error))
        emit_jsonl(log_path, lock, {"event": "direction_error", "connection_id": conn_id, "direction": direction, "error": repr(error)})
        try:
            target.shutdown(socket.SHUT_WR)
        except OSError:
            pass


class FixedSOCKS:
    def __init__(self, name, target, log_path, reject=False, rate_mbps=0):
        self.name = name
        self.target = target
        self.log_path = log_path
        self.reject = reject
        self.rate_mbps = rate_mbps
        self.lock = threading.Lock()
        self.workers = []
        self.active_sockets = set()
        self.active_sockets_lock = threading.Lock()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(32)
        self.listener.settimeout(0.25)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept_loop, daemon=True)
        self.thread.start()

    def accept_loop(self):
        while not STOP.is_set():
            try:
                client, peer = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            worker = threading.Thread(target=self.handle, args=(client, peer), daemon=True)
            worker.start()
            self.workers.append(worker)

    def handle(self, client, peer):
        started_at = time.monotonic_ns()
        conn_id = uuid.uuid4().hex
        counters = {"client_to_target": 0, "target_to_client": 0}
        errors = []
        upstream = None
        with self.active_sockets_lock:
            self.active_sockets.add(client)
        try:
            version, count = exact(client, 2)
            methods = exact(client, count)
            if version != 5 or 0 not in methods:
                client.sendall(b"\x05\xff")
                raise ValueError("SOCKS5 no-auth method not offered")
            client.sendall(b"\x05\x00")
            requested = read_socks_target(client)
            normalized = (str(ipaddress.ip_address(requested[0])), requested[1]) if requested[0] not in ("localhost",) else ("127.0.0.1", requested[1])
            if normalized != self.target:
                socks_reply(client, 2)
                raise ValueError(f"fixed-target SOCKS rejected {requested}; expected {self.target}")
            if self.reject:
                socks_reply(client, 5)
                emit_jsonl(self.log_path, self.lock, {"event": "rejected_by_scenario", "proxy": self.name, "connection_id": conn_id, "peer": peer, "target": self.target})
                return
            upstream = socket.create_connection(self.target, timeout=5)
            with self.active_sockets_lock:
                self.active_sockets.add(upstream)
            socks_reply(client, 0)
            client.settimeout(None)
            upstream.settimeout(None)
            emit_jsonl(self.log_path, self.lock, {"event": "connected", "proxy": self.name, "connection_id": conn_id, "peer": peer, "fixed_target": self.target, "tcp_only": True})
            a = threading.Thread(target=bridge, args=(client, upstream, "client_to_target", self.rate_mbps, counters, errors, self.log_path, self.lock, conn_id), daemon=True)
            b = threading.Thread(target=bridge, args=(upstream, client, "target_to_client", self.rate_mbps, counters, errors, self.log_path, self.lock, conn_id), daemon=True)
            a.start()
            b.start()
            while (a.is_alive() or b.is_alive()) and not STOP.wait(0.1):
                pass
            a.join(timeout=1)
            b.join(timeout=1)
        except (OSError, EOFError, ValueError) as error:
            emit_jsonl(self.log_path, self.lock, {"event": "error", "proxy": self.name, "connection_id": conn_id, "error": repr(error)})
        finally:
            if upstream is not None:
                upstream.close()
                with self.active_sockets_lock:
                    self.active_sockets.discard(upstream)
            client.close()
            with self.active_sockets_lock:
                self.active_sockets.discard(client)
            emit_jsonl(self.log_path, self.lock, {"event": "closed", "proxy": self.name, "connection_id": conn_id, **counters, "duration_ns": time.monotonic_ns() - started_at, "errors": errors})

    def close(self):
        self.listener.close()
        with self.active_sockets_lock:
            active = list(self.active_sockets)
        for conn in active:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        for worker in self.workers:
            worker.join(timeout=2)


class EchoTarget:
    def __init__(self, log_path, expected_bytes):
        self.log_path = log_path
        self.expected_bytes = expected_bytes
        self.lock = threading.Lock()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.25)
        self.port = self.listener.getsockname()[1]
        self.connections = 0
        self.bytes_received = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.accept_loop, daemon=True)
        self.thread.start()

    def accept_loop(self):
        while not STOP.is_set():
            try:
                conn, peer = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connections += 1
            emit_jsonl(self.log_path, self.lock, {"event": "accepted", "peer": peer, "connection_number": self.connections})
            threading.Thread(target=self.echo, args=(conn,), daemon=True).start()

    def echo(self, conn):
        count = 0
        result = "incomplete"
        try:
            with conn:
                while not STOP.is_set() and count < self.expected_bytes:
                    data = conn.recv(min(65536, self.expected_bytes - count))
                    if not data:
                        result = "early_request_eof"
                        break
                    count += len(data)
                    self.bytes_received += len(data)
                    conn.sendall(data)
                if count == self.expected_bytes:
                    # End only the response direction. Keep reading until the
                    # SMP3 server closes its request side after logical CLOSE;
                    # this avoids making the application synthesize SHUT_WR.
                    conn.shutdown(socket.SHUT_WR)
                    result = "expected_request_echoed"
                    while not STOP.is_set():
                        extra = conn.recv(65536)
                        if not extra:
                            break
                        count += len(extra)
                        self.bytes_received += len(extra)
                        emit_jsonl(self.log_path, self.lock, {"event": "unexpected_extra_request_bytes", "bytes": len(extra), "cumulative_bytes": count})
        except OSError:
            result = "socket_error"
        emit_jsonl(self.log_path, self.lock, {"event": "closed", "bytes_received": count, "expected_bytes": self.expected_bytes, "result": result})

    def close(self):
        self.listener.close()
        self.thread.join(timeout=1)


def wait_port(port, children, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for child in children:
            if child.poll() is not None:
                raise RuntimeError(f"child exited while waiting for port {port}: pid={child.pid}, code={child.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"port 127.0.0.1:{port} did not become ready")


def wait_log_content(path, marker, children, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for child in children:
            if child.poll() is not None:
                raise RuntimeError(f"child exited while waiting for log marker {marker!r}: pid={child.pid}, code={child.returncode}")
        if path.exists() and marker in path.read_text(encoding="utf-8", errors="replace"):
            return
        time.sleep(0.05)
    raise TimeoutError(f"log marker {marker!r} did not appear in {path}")


def get_json(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def compact_telemetry_session(session):
    session_fields = (
        "display_session_id", "mode", "created_at", "duration_ms", "first_leg_ready_at",
        "first_data_seen_at", "leg0_generation", "leg1_generation", "leg0_carrier_id",
        "leg1_carrier_id", "leg0_state", "leg1_state", "leg0_data_sent_bytes",
        "leg1_data_sent_bytes", "leg0_logical_tx_bytes", "leg1_logical_tx_bytes",
    )
    transport_fields = (
        "rx_delivered_bytes", "rx_ack_next", "rx_pending_frames", "rx_pending_bytes",
        "tx_outstanding_frames_total", "ordinary_outstanding_frames", "ordinary_bytes",
        "retry_bytes", "rescue_attempts", "admission_state", "admission_reason",
        "effective_weight_mbps", "weight_mbps", "sample_valid", "sample_age_ns",
        "dropped_data_rx_sequence_events",
    )
    compact = {key: session.get(key) for key in session_fields if key in session}
    transport = session.get("stream_transport") or {}
    compact["stream_transport"] = {key: transport.get(key) for key in transport_fields if key in transport}
    compact["data_rx_sequence_event_count"] = len(transport.get("data_rx_sequence_events") or [])
    return compact


def start_telemetry_collector(url, path, samples, errors, stop):
    lock = threading.Lock()

    def run():
        while not stop.is_set():
            try:
                catalog = get_json(url + "/api/v1/sessions")
                sessions = []
                for item in catalog.get("items", []):
                    display_id = item.get("display_session_id")
                    if display_id:
                        try:
                            sessions.append(get_json(url + "/api/v1/sessions/" + display_id).get("session", {}))
                        except OSError as error:
                            errors.append(repr(error))
                row = {"sampled_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "sessions": [compact_telemetry_session(session) for session in sessions]}
                with lock:
                    samples.append(row)
                emit_jsonl(path, LOG_LOCK, row)
            except (OSError, ValueError) as error:
                errors.append(repr(error))
            # Full sequence events are captured once at the end of the run.
            # Polling the full growing event ring at high frequency caused
            # quadratic JSON work and inflated the diagnostic's own overhead.
            stop.wait(1.0)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--front-script", type=Path, required=True)
    parser.add_argument("--application-script", type=Path, required=True)
    parser.add_argument("--cert-generator", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mode", choices=("dual-b0", "fast-only", "slow-only"), default="dual-b0")
    parser.add_argument("--payload-mib", type=int, default=32)
    parser.add_argument("--slow-weight-mbps", type=float, default=50)
    parser.add_argument("--fast-weight-mbps", type=float, default=100)
    parser.add_argument("--slow-cap-mbps", type=float, default=0, help="user-space Front cap; 0 disables shaping")
    parser.add_argument("--fast-cap-mbps", type=float, default=0, help="user-space direct SOCKS cap; 0 disables shaping")
    parser.add_argument("--front-stop-after-leg1-unique", action="store_true", help="diagnostic TCP process failure: stop the Front only after Landing observed unique DATA on leg1")
    parser.add_argument("--front-restart-delay-seconds", type=float, default=0, help="restart the same fixed-target Front after a requested Front failure; 0 leaves it stopped")
    parser.add_argument("--direct-stop-after-leg0-unique", action="store_true", help="diagnostic TCP connection failure: close the direct fixed-SOCKS listener/connections only after Landing observed unique DATA on leg0")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()

    for path in (args.client, args.server, args.front_script, args.application_script, args.cert_generator):
        if not path.is_file():
            parser.error(f"required file does not exist: {path}")
    if args.payload_mib <= 0 or args.timeout_seconds < 10:
        parser.error("payload must be positive and timeout at least 10 seconds")
    if min(args.slow_weight_mbps, args.fast_weight_mbps) <= 0 or min(args.slow_cap_mbps, args.fast_cap_mbps) < 0 or args.front_restart_delay_seconds < 0:
        parser.error("Core weights must be positive and user-space caps nonnegative")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "tls").mkdir()

    children = []
    child_logs = []
    sock_proxies = []
    target = EchoTarget(out / "echo.jsonl", args.payload_mib * 1024 * 1024)
    telemetry_samples, telemetry_errors = [], []
    collector_stop = threading.Event()
    collector = None
    application = None
    front_fault_thread = None
    front_fault_record = {"requested": bool(args.front_stop_after_leg1_unique), "triggered": False}
    direct_fault_thread = None
    direct_fault_record = {"requested": bool(args.direct_stop_after_leg0_unique), "triggered": False}
    front_port, landing_port, client_port, telemetry_port = (free_port() for _ in range(4))
    direct_target = ("127.0.0.1", landing_port)
    relay_target = ("127.0.0.1", front_port)
    mode = args.mode
    fast_proxy = FixedSOCKS("leg0-direct", direct_target, out / "proxy_leg0.jsonl", reject=(mode == "slow-only"), rate_mbps=args.fast_cap_mbps)
    slow_proxy = FixedSOCKS("leg1-front", relay_target, out / "proxy_leg1.jsonl", reject=(mode == "fast-only"), rate_mbps=0)
    sock_proxies.extend((fast_proxy, slow_proxy))
    relay_log = out / "front.jsonl"
    server_log = out / "landing.stdout_stderr.log"
    client_log = out / "client.stdout_stderr.log"
    front_stdout = out / "front.stdout_stderr.log"
    cert_log = out / "certificate_generation.log"
    result = {"run_id": out.name, "mode": mode, "evidence_level": "B_LOCAL_INDEPENDENT_PROCESSES", "production_actions": "NONE", "real_wan": False, "independent_physical_paths": False}

    try:
        cert_command = [str(args.cert_generator.resolve()), "-out", str((out / "tls").resolve())]
        cert_result = subprocess.run(cert_command, capture_output=True, text=True, timeout=30)
        cert_log.write_text("COMMAND: " + subprocess.list2cmdline(cert_command) + "\nEXIT: " + str(cert_result.returncode) + "\nSTDOUT:\n" + cert_result.stdout + "\nSTDERR:\n" + cert_result.stderr, encoding="utf-8")
        if cert_result.returncode:
            raise RuntimeError("test certificate generator failed")
        ca_cert, server_cert, server_key = (out / "tls" / name for name in ("test-ca.pem", "server-cert.pem", "server-key.pem"))

        server_config = {
            "listen": f"127.0.0.1:{landing_port}",
            "password": TEST_PASSWORD,
            "hello_read_timeout": "10s",
            "recovery_timeout": "15s",
            "carrier_tls": {"cert_file": str(server_cert), "key_file": str(server_key)},
            # The single main TCP/TLS listener sends authenticated READY only
            # for v6. Keep B1 rescue policy disabled; Path-aware is enabled
            # here solely to negotiate v6 and bind the measured handshake RTT
            # to each active carrier for this integration scenario.
            "stream": {"scheduler_mode": "aggregation", "capacity_mode": "dynamic", "activation_mode": "dynamic", "experimental_path_aware": True, "activation_threshold_mbps": 80, "activation_window": "1s", "chunk_size": 65536, "queue_frames": 256, "bandwidth_mbps": [int(args.fast_weight_mbps), int(args.slow_weight_mbps)], "max_reorder_frames": 4096, "max_inflight_frames": 1024, "ack_interval": "20ms", "retransmit_timeout": "1500ms"},
            "udp": {"enabled": False},
            "telemetry": {"enabled": True, "data_sequence_events": True, "listen": f"127.0.0.1:{telemetry_port}", "snapshot_interval": "100ms", "max_active_sessions": 16, "max_events": 256, "close_grace": "3s"},
        }
        client_config = {
            "listen": f"127.0.0.1:{client_port}",
            "upstream_socks": {"connect_timeout": "5s", "leg0": {"address": f"127.0.0.1:{fast_proxy.port}"}, "leg1": {"address": f"127.0.0.1:{slow_proxy.port}"}},
            "smp3": {
                "password": TEST_PASSWORD,
                "carrier_ready_timeout": "5s",
                "carrier_tls": {"enabled": True, "server_name": "localhost", "root_ca_file": str(ca_cert)},
                "routes": {"leg0": f"127.0.0.1:{landing_port}", "leg1": f"127.0.0.1:{front_port}"},
                "stream": {"scheduler_mode": "aggregation", "capacity_mode": "dynamic", "activation_mode": "dynamic", "experimental_path_aware": True, "startup_policy": "preferred", "startup_preferred_leg": 1 if mode == "slow-only" else 0, "startup_grace": "500ms", "activation_threshold_mbps": 80, "activation_window": "1s", "chunk_size": 65536, "queue_frames": 256, "bandwidth_mbps": [int(args.fast_weight_mbps), int(args.slow_weight_mbps)], "max_reorder_frames": 4096, "max_inflight_frames": 1024, "ack_interval": "20ms", "retransmit_timeout": "1500ms", "recovery_timeout": "15s", "redial_interval": "1s"},
                "udp": {"enabled": False},
            },
        }
        write_json(out / "server.json", server_config)
        write_json(out / "client.json", client_config)

        process_spec = [
            (args.server, ["-c", str((out / "server.json").resolve())], "landing"),
        ]
        server_output = server_log.open("wb")
        child_logs.append(server_output)
        server_process = subprocess.Popen([str(args.server.resolve()), *process_spec[0][1]], stdout=server_output, stderr=subprocess.STDOUT)
        children.append(server_process)
        wait_log_content(server_log, "standalone SMP3 server started", children)

        front_command = [sys.executable, str(args.front_script.resolve()), "--listen", f"127.0.0.1:{front_port}", "--target", f"127.0.0.1:{landing_port}", "--rate-mbps", str(args.slow_cap_mbps), "--jsonl", str(relay_log.resolve())]
        front_output = front_stdout.open("wb")
        child_logs.append(front_output)
        front_process = subprocess.Popen(front_command, stdout=front_output, stderr=subprocess.STDOUT)
        children.append(front_process)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if front_process.poll() is not None:
                raise RuntimeError(f"front relay exited: {front_process.returncode}")
            if relay_log.exists() and '"event":"listening"' in relay_log.read_text(encoding="utf-8", errors="replace"):
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("front relay did not record listening")

        client_output = client_log.open("wb")
        child_logs.append(client_output)
        client_process = subprocess.Popen([str(args.client.resolve()), "-c", str((out / "client.json").resolve())], stdout=client_output, stderr=subprocess.STDOUT)
        children.append(client_process)
        wait_log_content(client_log, "SMP3 standalone sidecar listening", children)
        telemetry_url = f"http://127.0.0.1:{telemetry_port}"
        collector = start_telemetry_collector(telemetry_url, out / "telemetry_samples.jsonl", telemetry_samples, telemetry_errors, collector_stop)

        result["processes"] = {
            "landing": {"pid": server_process.pid, "binary": str(args.server.resolve()), "binary_sha256": sha256_file(args.server), "command": [str(args.server.resolve()), "-c", str((out / "server.json").resolve())]},
            "front": {"pid": front_process.pid, "command": front_command, "script_sha256": sha256_file(args.front_script)},
            "client": {"pid": client_process.pid, "binary": str(args.client.resolve()), "binary_sha256": sha256_file(args.client), "command": [str(args.client.resolve()), "-c", str((out / "client.json").resolve())]},
            "test_orchestrator_pid": os.getpid(),
        }
        result["topology"] = {"direct_leg": f"client -> fixed SOCKS leg0 -> TLS Landing 127.0.0.1:{landing_port}", "relay_leg": f"client -> fixed SOCKS leg1 -> TLS-opaque Front 127.0.0.1:{front_port} -> TCP Landing 127.0.0.1:{landing_port}", "landing_listener_count": 1, "landing_listener": f"127.0.0.1:{landing_port}", "main_listener_v6_ready": True, "experimental_path_aware_enabled_for_v6_handshake_and_active_carrier_rtt": True, "b1_bounded_rescue_policy": "not configured; default off", "front_target_fixed": True, "front_tls_termination": False, "carrier_tls_server_name": "localhost", "carrier_tls_alpn": ALPN, "udp_enabled_client": False, "udp_enabled_server": False, "service_scope": "all harness endpoints bind loopback; no WAN or physical-path independence"}
        result["config_identity"] = {"client_config_sha256": sha256_file(out / "client.json"), "server_config_sha256": sha256_file(out / "server.json"), "tls_ca_sha256": sha256_file(ca_cert), "tls_server_cert_sha256": sha256_file(server_cert), "private_key_in_evidence": False}

        app_result_path = out / "application_result.json"
        app_log = out / "application.stdout_stderr.log"
        app_command = [sys.executable, str(args.application_script.resolve()), "--listen", f"127.0.0.1:{client_port}", "--target", f"127.0.0.1:{target.port}", "--payload-mib", str(args.payload_mib), "--timeout-seconds", str(args.timeout_seconds), "--out", str(app_result_path.resolve())]
        with app_log.open("wb") as app_output:
            application = subprocess.Popen(app_command, stdout=app_output, stderr=subprocess.STDOUT)
            children.append(application)
            result.setdefault("processes", {})["application"] = {"pid": application.pid, "command": app_command, "script_sha256": sha256_file(args.application_script)}
            if args.front_stop_after_leg1_unique:
                def stop_front_after_leg1_unique():
                    deadline = time.monotonic() + min(float(args.timeout_seconds), 30.0)
                    while time.monotonic() < deadline and application.poll() is None:
                        try:
                            catalog = get_json(telemetry_url + "/api/v1/sessions")
                            observed = None
                            for item in catalog.get("items", []):
                                display_id = item.get("display_session_id")
                                if not display_id:
                                    continue
                                detail = get_json(telemetry_url + "/api/v1/sessions/" + display_id).get("session", {})
                                transport = detail.get("stream_transport") or {}
                                events = transport.get("data_rx_sequence_events") or []
                                leg1_event = next((event for event in events if int(event.get("leg_id", -1)) == 1 and event.get("unique")), None)
                                if leg1_event is not None:
                                    observed = {"display_session_id": display_id, "sequence": leg1_event.get("sequence"), "payload_bytes": leg1_event.get("payload_bytes"), "generation": leg1_event.get("generation"), "carrier_read_at": leg1_event.get("carrier_read_at"), "core_accepted_at": leg1_event.get("core_accepted_at")}
                                    break
                        except (OSError, ValueError):
                            observed = None
                        if observed is not None:
                            front_fault_record.update({"triggered": True, "reason": "first landing-observed unique DATA on leg1", "observed_leg1_data": observed, "terminate_requested_monotonic_ns": time.monotonic_ns(), "front_pid": front_process.pid})
                            write_json(out / "front_fault_injection.json", front_fault_record)
                            front_process.terminate()
                            try:
                                front_fault_record["front_process_exit_code"] = front_process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                front_process.kill()
                                front_fault_record["front_process_exit_code"] = front_process.wait(timeout=2)
                            front_fault_record["front_exit_observed_monotonic_ns"] = time.monotonic_ns()
                            if args.front_restart_delay_seconds > 0:
                                front_fault_record["restart_delay_seconds"] = args.front_restart_delay_seconds
                                time.sleep(args.front_restart_delay_seconds)
                                restart_log = out / "front_restart_01.stdout_stderr.log"
                                restart_output = restart_log.open("ab")
                                child_logs.append(restart_output)
                                restart_process = subprocess.Popen(front_command, stdout=restart_output, stderr=subprocess.STDOUT)
                                children.append(restart_process)
                                result.setdefault("processes", {})["front_restart_01"] = {"pid": restart_process.pid, "command": front_command, "script_sha256": sha256_file(args.front_script)}
                                listen_deadline = time.monotonic() + 8
                                while time.monotonic() < listen_deadline and restart_process.poll() is None:
                                    if relay_log.exists() and relay_log.read_text(encoding="utf-8", errors="replace").count('"event":"listening"') >= 2:
                                        front_fault_record["restart_listening"] = True
                                        break
                                    time.sleep(0.05)
                                front_fault_record["restart_pid"] = restart_process.pid
                                front_fault_record["restart_exit_code_at_start"] = restart_process.poll()
                            write_json(out / "front_fault_injection.json", front_fault_record)
                            return
                        time.sleep(0.05)
                    front_fault_record["triggered"] = False
                    front_fault_record["reason"] = "no unique leg1 DATA observed before application completed or fault wait deadline"
                    write_json(out / "front_fault_injection.json", front_fault_record)

                front_fault_thread = threading.Thread(target=stop_front_after_leg1_unique, name="phase30-front-fault", daemon=True)
                front_fault_thread.start()
            if args.direct_stop_after_leg0_unique:
                def stop_direct_after_leg0_unique():
                    deadline = time.monotonic() + min(float(args.timeout_seconds), 30.0)
                    while time.monotonic() < deadline and application.poll() is None:
                        try:
                            catalog = get_json(telemetry_url + "/api/v1/sessions")
                            observed = None
                            for item in catalog.get("items", []):
                                display_id = item.get("display_session_id")
                                if not display_id:
                                    continue
                                detail = get_json(telemetry_url + "/api/v1/sessions/" + display_id).get("session", {})
                                events = ((detail.get("stream_transport") or {}).get("data_rx_sequence_events") or [])
                                leg0_event = next((event for event in events if int(event.get("leg_id", -1)) == 0 and event.get("unique")), None)
                                if leg0_event is not None:
                                    observed = {"display_session_id": display_id, "sequence": leg0_event.get("sequence"), "payload_bytes": leg0_event.get("payload_bytes"), "generation": leg0_event.get("generation"), "carrier_read_at": leg0_event.get("carrier_read_at"), "core_accepted_at": leg0_event.get("core_accepted_at")}
                                    break
                        except (OSError, ValueError):
                            observed = None
                        if observed is not None:
                            direct_fault_record.update({"triggered": True, "reason": "first landing-observed unique DATA on leg0", "observed_leg0_data": observed, "close_requested_monotonic_ns": time.monotonic_ns()})
                            fast_proxy.close()
                            direct_fault_record["close_completed_monotonic_ns"] = time.monotonic_ns()
                            write_json(out / "direct_fault_injection.json", direct_fault_record)
                            return
                        time.sleep(0.05)
                    direct_fault_record["triggered"] = False
                    direct_fault_record["reason"] = "no unique leg0 DATA observed before application completed or fault wait deadline"
                    write_json(out / "direct_fault_injection.json", direct_fault_record)

                direct_fault_thread = threading.Thread(target=stop_direct_after_leg0_unique, name="phase30-direct-fault", daemon=True)
                direct_fault_thread.start()
            try:
                app_exit = application.wait(timeout=args.timeout_seconds + 15)
            except subprocess.TimeoutExpired:
                application.kill()
                app_exit = application.wait(timeout=5)
                result["application_runner_timeout"] = True
        if front_fault_thread is not None:
            front_fault_thread.join(timeout=6)
            result["front_fault_injection"] = front_fault_record
        if direct_fault_thread is not None:
            direct_fault_thread.join(timeout=6)
            result["direct_fault_injection"] = direct_fault_record
        result["application_process_exit_code"] = app_exit
        try:
            app_result = json.loads(app_result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            app_result = {"status": "FAIL", "error": repr(error), "application_connections": 0, "strict_eof": False, "sha256_match": False}
        result["application"] = app_result
        # The registry publishes immutable snapshots on its configured 100 ms
        # cadence. A fast local transfer can complete between ticks, so poll
        # the retained session record until delivery and both ledgers settle or
        # the configured close-grace window is nearly exhausted.
        expected_payload_bytes = int(app_result.get("payload_bytes_sent") or args.payload_mib * 1024 * 1024)
        final_poll_path = out / "final_telemetry_poll.jsonl"
        final_sessions = []
        final_poll_started = time.monotonic()
        final_poll_deadline = final_poll_started + 2.5
        final_telemetry_settled = False
        final_poll_count = 0
        try:
            while True:
                final_catalog = get_json(telemetry_url + "/api/v1/sessions")
                current_sessions = []
                for item in final_catalog.get("items", []):
                    display_id = item.get("display_session_id")
                    if display_id:
                        current_sessions.append(get_json(telemetry_url + "/api/v1/sessions/" + display_id).get("session", {}))
                final_sessions = current_sessions
                final_poll_count += 1
                poll_row = {"poll_index": final_poll_count, "elapsed_ms": round((time.monotonic() - final_poll_started) * 1000, 3), "sessions": final_sessions}
                with final_poll_path.open("a", encoding="utf-8") as poll_file:
                    poll_file.write(json.dumps(poll_row, separators=(",", ":")) + "\n")
                if len(final_sessions) == 1:
                    transport = final_sessions[0].get("stream_transport") or {}
                    settled = (
                        int(transport.get("rx_delivered_bytes") or 0) >= expected_payload_bytes
                        and int(transport.get("rx_pending_frames") or 0) == 0
                        and int(transport.get("rx_pending_bytes") or 0) == 0
                        and int(transport.get("tx_outstanding_frames_total") or 0) == 0
                    )
                    if settled:
                        final_telemetry_settled = True
                        break
                if time.monotonic() >= final_poll_deadline:
                    break
                time.sleep(0.05)
            write_json(out / "final_telemetry.json", {"sessions": final_sessions})
        except (OSError, ValueError) as error:
            final_sessions = []
            write_json(out / "final_telemetry.json", {"error": repr(error), "sessions": []})
        result["final_telemetry_settlement"] = {"settled": final_telemetry_settled, "poll_count": final_poll_count, "wait_ms": round((time.monotonic() - final_poll_started) * 1000, 3), "expected_payload_bytes": expected_payload_bytes, "close_grace_ms": 3000}
        collector_stop.set()
        collector.join(timeout=3)

        server_text = server_log.read_text(encoding="utf-8", errors="replace")
        log_rows = re.findall(r'msg="multipath (session created|leg joined/rejoined)" session=([0-9a-f]+) leg=(\d+)', server_text)
        log_sessions = sorted({row[1] for row in log_rows})
        log_legs = sorted({int(row[2]) for row in log_rows})
        single_session = len(final_sessions) == 1
        session = final_sessions[0] if single_session else {}
        events = (((session.get("stream_transport") or {}).get("data_rx_sequence_events")) or [])
        unique_by_leg = {0: 0, 1: 0}
        duplicate_by_leg = {0: 0, 1: 0}
        sequences_by_leg = {0: [], 1: []}
        for event in events:
            leg = int(event.get("leg_id", -1))
            if leg not in unique_by_leg:
                continue
            if event.get("unique"):
                unique_by_leg[leg] += int(event.get("payload_bytes", 0))
                sequences_by_leg[leg].append(int(event.get("sequence", 0)))
            elif "duplicate" in str(event.get("disposition", "")):
                duplicate_by_leg[leg] += int(event.get("payload_bytes", 0))
        event_times = {leg: [] for leg in (0, 1)}
        unique_sequences = set()
        unique_bytes_by_window = {}
        for event in events:
            leg = int(event.get("leg_id", -1))
            if leg in event_times and event.get("unique") and event.get("core_accepted_at"):
                accepted_at = datetime.datetime.fromisoformat(event["core_accepted_at"])
                event_times[leg].append(accepted_at)
                unique_sequences.add(int(event.get("sequence", 0)))
                window = int(accepted_at.timestamp() * 10)  # 100 ms bins from common Landing wall clock.
                unique_bytes_by_window.setdefault(window, {0: 0, 1: 0})[leg] += int(event.get("payload_bytes", 0))
        overlap_windows = [window for window, by_leg in unique_bytes_by_window.items() if by_leg[0] > 0 and by_leg[1] > 0]
        overlap = bool(overlap_windows)
        if single_session:
            transport = session.get("stream_transport") or {}
            leg_generations = {0: session.get("leg0_generation"), 1: session.get("leg1_generation")}
            carriers = {0: session.get("leg0_carrier_id"), 1: session.get("leg1_carrier_id")}
            rx_delivered = int(transport.get("rx_delivered_bytes", 0))
            rx_ack_next = int(transport.get("rx_ack_next", 0))
            rx_pending = int(transport.get("rx_pending_frames", 0))
            rx_pending_bytes = int(transport.get("rx_pending_bytes", 0))
            tx_outstanding = int(transport.get("tx_outstanding_frames_total", -1))
            rx_dropped = int(transport.get("dropped_data_rx_sequence_events", 0))
            rx_unique_total = sum(unique_by_leg.values())
            contiguous_sequence_space = unique_sequences == set(range(rx_ack_next))
            result["multipath_proof"] = {"external_application_connections": 1, "telemetry_session_count": len(final_sessions), "server_hmac_session_log_prefixes": log_sessions, "server_session_leg_log_ids": log_rows, "legs_seen_in_server_session_logs": log_legs, "session_id_same_for_both_legs": len(log_sessions) == 1 and set(log_legs) == {0, 1}, "landing_listener_count": 1, "leg_generations": leg_generations, "telemetry_carrier_ids": carriers, "telemetry_carrier_id_semantics": "server-local privacy-preserving key derived from one telemetry session display ID + leg ID + observed generation; not a wire field", "data_rx_event_count": len(events), "dropped_data_rx_sequence_events": rx_dropped, "unique_landing_rx_bytes_by_leg": unique_by_leg, "rx_duplicate_bytes_by_leg": duplicate_by_leg, "rx_unique_total": rx_unique_total, "rx_unique_sequence_count": len(unique_sequences), "rx_sequence_space_contiguous_to_ack_next": contiguous_sequence_space, "rx_delivered_bytes": rx_delivered, "rx_ack_next": rx_ack_next, "rx_pending_frames": rx_pending, "rx_pending_bytes": rx_pending_bytes, "tx_outstanding_frames": tx_outstanding, "ordinary_outstanding_frames": transport.get("ordinary_outstanding_frames"), "server_tx_useful_ack_bytes_by_leg": transport.get("useful_ack_bytes"), "sequence_ranges_by_leg": {str(leg): ([min(items), max(items)] if items else None) for leg, items in sequences_by_leg.items()}, "unique_data_bytes_by_100ms_window": {str(window): value for window, value in sorted(unique_bytes_by_window.items())}, "overlap_100ms_windows": overlap_windows, "unique_data_intervals_overlap": overlap, "attribution_scope": "first valid Core StreamRXWindow acceptance by wire-ingress leg/generation; does not alone prove client Carrier Write completion or application delivery"}

        proxy_events = []
        for path in (out / "proxy_leg0.jsonl", out / "proxy_leg1.jsonl", relay_log):
            if path.exists():
                for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                    try:
                        proxy_events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        closed_events = [event for event in proxy_events if event.get("event") == "closed"]
        measured_service = []
        for event in closed_events:
            duration = int(event.get("duration_ns") or 0)
            rate_fields = [field for field in ("client_to_target", "target_to_client", "front_to_landing", "landing_to_front") if field in event]
            measured_service.append({"connection_id": event.get("connection_id"), "duration_ns": duration,
                                    "bytes_by_direction": {field: event.get(field, 0) for field in rate_fields},
                                    "average_forwarded_bytes_per_second": {field: (int(event.get(field, 0)) / (duration / 1_000_000_000) if duration > 0 else None) for field in rate_fields},
                                    "metric_scope": "encrypted Carrier TCP bytes at user-space proxy boundary; excludes neither TLS nor SMP3 overhead; not physical path capacity"})
        result["carrier_connection_evidence"] = {"fixed_socks_leg0_target": direct_target, "fixed_socks_leg1_target": relay_target, "front_json_events": [event for event in proxy_events if event.get("event") in ("connected", "closed")], "measured_service": measured_service, "configured_core_scheduler_weights_mbps": {"leg0": args.fast_weight_mbps, "leg1": args.slow_weight_mbps}, "configured_user_space_caps_mbps_per_direction": {"direct_leg0": args.fast_cap_mbps, "relayed_leg1_and_front": args.slow_cap_mbps}, "rate_limiter": {"kind": "token bucket", "burst_bytes": RATE_BURST_BYTES, "timer_note": "Windows timer granularity can affect short interval rates; only long-run forwarded bytes divided by elapsed time are reported"}, "echo_target_connections": target.connections, "echo_target_received_bytes": target.bytes_received, "tcp_only_harness_sockets": True}
        expected_leg = 0 if mode == "fast-only" else 1 if mode == "slow-only" else None
        expected_legs = {expected_leg} if expected_leg is not None else {0, 1}
        session_legs = set(log_legs)
        per_mode_unique_ok = all(unique_by_leg[leg] > 0 for leg in expected_legs) and all(unique_by_leg[leg] == 0 for leg in ({0, 1} - expected_legs))
        expected_session_legs_ok = bool(single_session and session_legs == expected_legs and len(log_sessions) == 1)
        dual_mode = mode == "dual-b0"
        result["gates"] = {
            "tls_validated_end_to_end": bool(app_exit == 0 and app_result.get("sha256_match") and single_session),
            "strict_eof_and_sha": bool(app_exit == 0 and app_result.get("strict_eof") and app_result.get("sha256_match") and app_result.get("extra_bytes") == 0),
            "one_external_connection_one_server_session": single_session and app_exit == 0 and app_result.get("application_connections") == 1,
            "same_landing_session_has_expected_legs": expected_session_legs_ok,
            "per_mode_expected_leg_unique_contribution": per_mode_unique_ok,
            "same_landing_session_has_both_legs": bool(single_session and session_legs == {0, 1} and len(log_sessions) == 1) if dual_mode else None,
            "both_legs_unique_data_contribution": unique_by_leg[0] > 0 and unique_by_leg[1] > 0 if dual_mode else None,
            "final_server_telemetry_settled": final_telemetry_settled,
            "data_reassembly_and_ack": bool(single_session and app_exit == 0 and rx_delivered == app_result.get("payload_bytes_sent") and rx_unique_total == app_result.get("payload_bytes_sent") and rx_ack_next == len(unique_sequences) and contiguous_sequence_space and rx_pending == 0 and rx_pending_bytes == 0 and tx_outstanding == 0 and rx_dropped == 0) if single_session else False,
            "single_listener_direct_and_front_routes": bool(single_session and target.connections == 1 and relay_log.exists() and server_config["listen"] == f"127.0.0.1:{landing_port}" and not server_config.get("sidecar_listeners") and direct_target == ("127.0.0.1", landing_port) and relay_target != direct_target),
            "dual_leg_data_time_overlap": overlap if dual_mode else None,
        }
        if args.front_stop_after_leg1_unique:
            result["gates"]["front_process_failure_injected_after_leg1_unique_data"] = bool(front_fault_record.get("triggered"))
            result["gates"]["application_survived_front_process_failure"] = bool(front_fault_record.get("triggered") and app_exit == 0 and app_result.get("sha256_match") and app_result.get("strict_eof"))
            if args.front_restart_delay_seconds > 0:
                leg1_join_count = sum(1 for row in log_rows if int(row[2]) == 1)
                result["front_fault_injection"]["same_session_leg1_join_events"] = leg1_join_count
                result["front_fault_injection"]["final_leg1_generation"] = session.get("leg1_generation") if session else None
                result["gates"]["front_rejoined_same_session_new_generation"] = bool(front_fault_record.get("restart_listening") and single_session and len(log_sessions) == 1 and leg1_join_count >= 2 and int(session.get("leg1_generation") or 0) >= 2)
        if args.direct_stop_after_leg0_unique:
            result["gates"]["direct_route_failure_injected_after_leg0_unique_data"] = bool(direct_fault_record.get("triggered"))
            result["gates"]["application_survived_direct_route_failure"] = bool(direct_fault_record.get("triggered") and app_exit == 0 and app_result.get("sha256_match") and app_result.get("strict_eof"))
        result["telemetry_collection_errors"] = telemetry_errors
        result["telemetry_sample_count"] = len(telemetry_samples)
        result["status"] = "PASS" if all(value is True for value in result["gates"].values() if value is not None) else "FAIL"
        write_json(out / "result.json", result)
        if result["status"] != "PASS":
            result["error"] = result.get("error") or "one or more integration evidence gates failed; inspect result.json"
            write_json(out / "result.json", result)
            raise RuntimeError("one or more integration evidence gates failed; inspect result.json")
    except Exception as error:
        result["status"] = "FAIL"
        result["error"] = result.get("error") or repr(error)
        result["telemetry_collection_errors"] = telemetry_errors
        result["telemetry_sample_count"] = len(telemetry_samples)
        write_json(out / "result.json", result)
        raise
    finally:
        collector_stop.set()
        if collector is not None:
            collector.join(timeout=2)
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=2)
        for handle in child_logs:
            handle.close()
        for proxy in sock_proxies:
            proxy.close()
        target.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"phase30 integration failed: {exc}", file=sys.stderr)
        raise

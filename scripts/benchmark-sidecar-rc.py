#!/usr/bin/env python3
"""Localhost Native Mihomo vs standalone sidecar RC qualification.

The harness deliberately uses only disposable loopback processes and observable
SOCKS relays. It never reads or changes a production configuration.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable


SCRIPT_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("stock_fixture", SCRIPT_DIR / "qualify-stock-mihomo-sidecar.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load localhost fixture helpers")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


LOCAL_SECRET = "RCPHASE2A_LOCAL_ONLY"
LEGACY_PORT = 21440
SIDECAR_PORTS = (21441, 21442, 21443)
STOCK_PORT = 21080
SIDECAR_PORT = 21081
NATIVE_PORT = 21082
GENERIC_PORT = 21083
THROUGHPUT_SIZES = (64, 256, 512)
THROUGHPUT_RUNS = 3
CHURN_COUNT = 1000


def wait_tcp(address: tuple[str, int], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = socket.create_connection(address, timeout=0.2)
            conn.close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"timeout waiting for {address}")


def wait_until(predicate: Callable[[], bool], timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise RuntimeError(f"timeout waiting for {description}")


def recvn(conn: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        data = conn.recv(size - len(chunks))
        if not data:
            raise EOFError("unexpected EOF")
        chunks.extend(data)
    return bytes(chunks)


def process_stats(pid: int) -> dict[str, float] | None:
    command = f"Get-Process -Id {pid} | Select-Object WorkingSet64,CPU | ConvertTo-Json -Compress"
    try:
        completed = subprocess.run(["powershell.exe", "-NoProfile", "-Command", command], capture_output=True, text=True, timeout=3)
        if completed.returncode != 0 or not completed.stdout.strip():
            return None
        value = json.loads(completed.stdout)
        return {"working_set": float(value.get("WorkingSet64", 0)), "cpu_seconds": float(value.get("CPU", 0) or 0)}
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        return None


class StreamingFixture:
    def __init__(self, mode: str, size: int = 0, hold: float = 1.0) -> None:
        self.mode = mode
        self.size = size
        self.hold = hold
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(32)
        self.listener.settimeout(0.5)
        self.stop = threading.Event()
        self.active: set[socket.socket] = set()
        self.lock = threading.Lock()
        self.received = 0
        self.echoed = 0
        self.last_progress = time.monotonic()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    @property
    def address(self) -> tuple[str, int]:
        return self.listener.getsockname()

    def _accept(self) -> None:
        while not self.stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                self.active.add(conn)
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            if self.mode == "download":
                chunk = b"D" * (1024 * 1024)
                remaining = self.size
                while remaining:
                    part = chunk if remaining >= len(chunk) else chunk[:remaining]
                    conn.sendall(part)
                    remaining -= len(part)
                time.sleep(self.hold)
            elif self.mode == "echo":
                while True:
                    data = conn.recv(64 * 1024)
                    if not data:
                        return
                    with self.lock:
                        self.received += len(data)
                        self.last_progress = time.monotonic()
                    conn.sendall(data)
                    with self.lock:
                        self.echoed += len(data)
                        self.last_progress = time.monotonic()
        except OSError:
            return
        finally:
            with self.lock:
                self.active.discard(conn)
            conn.close()

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {
                "received": self.received,
                "echoed": self.echoed,
                "last_progress_age": time.monotonic() - self.last_progress,
                "active": len(self.active),
            }

    def close(self) -> None:
        self.stop.set()
        self.listener.close()
        with self.lock:
            active = list(self.active)
        for conn in active:
            conn.close()
        self.thread.join(timeout=1)


def recv_repeated(conn: socket.socket, size: int, expected: int) -> None:
    remaining = size
    while remaining:
        data = conn.recv(min(1024 * 1024, remaining))
        if not data:
            raise EOFError("short transfer")
        if any(byte != expected for byte in data):
            raise RuntimeError("transfer payload mismatch")
        remaining -= len(data)


def send_repeated(conn: socket.socket, size: int, value: int, progress: dict[str, int] | None = None) -> None:
    chunk = bytes((value,)) * (1024 * 1024)
    remaining = size
    while remaining:
        part = chunk if remaining >= len(chunk) else chunk[:remaining]
        conn.sendall(part)
        remaining -= len(part)
        if progress is not None:
            progress["written"] = size - remaining


def tcp_transfer(proxy: tuple[str, int], fixture: StreamingFixture, upload: bool, progress: dict[str, int] | None = None) -> float:
    conn = FIXTURE.socks_connect(proxy, fixture.address)
    try:
        conn.settimeout(60)
        started = time.perf_counter()
        if upload:
            errors: list[Exception] = []

            def read_upload_echo() -> None:
                try:
                    recv_repeated(conn, fixture.size, ord("U"))
                except Exception as exc:  # pragma: no cover - diagnostic path
                    errors.append(exc)

            reader = threading.Thread(target=read_upload_echo)
            reader.start()
            send_repeated(conn, fixture.size, ord("U"), progress)
            reader.join(timeout=60)
            if reader.is_alive() or errors:
                raise RuntimeError(f"upload echo failed: {errors}")
        else:
            recv_repeated(conn, fixture.size, ord("D"))
        return time.perf_counter() - started
    finally:
        conn.close()


def rtt_case(proxy: tuple[str, int], payload_size: int) -> dict[str, float]:
    fixture = StreamingFixture("echo")
    try:
        conn = FIXTURE.socks_connect(proxy, fixture.address)
        conn.settimeout(10)
        samples: list[float] = []
        payload = b"R" * payload_size
        for _ in range(1000):
            started = time.perf_counter_ns()
            conn.sendall(payload)
            if recvn(conn, payload_size) != payload:
                raise RuntimeError("RTT payload mismatch")
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
        conn.close()
        samples.sort()
        return {"median_ms": statistics.median(samples), "p95_ms": samples[949], "p99_ms": samples[989]}
    finally:
        fixture.close()


def bidirectional_case(proxy: tuple[str, int]) -> float:
    size = 16 * 1024 * 1024
    fixture = StreamingFixture("echo")
    try:
        conn = FIXTURE.socks_connect(proxy, fixture.address)
        conn.settimeout(60)
        result: list[Exception] = []

        def reader() -> None:
            try:
                recv_repeated(conn, size, ord("B"))
            except Exception as exc:  # pragma: no cover - diagnostic path
                result.append(exc)

        thread = threading.Thread(target=reader)
        thread.start()
        started = time.perf_counter()
        send_repeated(conn, size, ord("B"))
        thread.join(timeout=60)
        conn.close()
        if thread.is_alive() or result:
            raise RuntimeError(f"bidirectional transfer failed: {result}")
        return time.perf_counter() - started
    finally:
        fixture.close()


def churn_case(proxy: tuple[str, int], count: int = CHURN_COUNT) -> None:
    fixture = StreamingFixture("echo")
    try:
        for _ in range(count):
            conn = FIXTURE.socks_connect(proxy, fixture.address)
            conn.settimeout(5)
            conn.sendall(b"c")
            if recvn(conn, 1) != b"c":
                raise RuntimeError("churn payload mismatch")
            conn.close()
    finally:
        fixture.close()


def udp_case(proxy: tuple[str, int], count: int) -> dict[str, int]:
    fixture = FIXTURE.UDPFixture()
    control = data = None
    received = 0
    try:
        print(f"RC_STAGE_UDP_START count={count}", flush=True)
        control, data, relay = FIXTURE.udp_associate(proxy)
        for index in range(count):
            payload = f"rc-udp-{index:06d}".encode()
            data.sendto(FIXTURE.udp_packet(fixture.address, payload), relay)
            data.settimeout(10)
            response, _ = data.recvfrom(2048)
            _, got = FIXTURE.parse_udp_packet(response)
            if got != payload:
                raise RuntimeError("UDP payload mismatch")
            received += 1
    finally:
        if data is not None:
            data.close()
        if control is not None:
            control.close()
        fixture.close()
    return {"sent": count, "received": received, "lost": count - received, "bad": 0}


def establishment_case(proxy: tuple[str, int]) -> dict[str, float]:
    fixture = StreamingFixture("echo")
    try:
        samples = []
        for _ in range(20):
            started = time.perf_counter()
            conn = FIXTURE.socks_connect(proxy, fixture.address)
            samples.append((time.perf_counter() - started) * 1000)
            conn.close()
        return {"median_ms": statistics.median(samples), "p95_ms": sorted(samples)[18]}
    finally:
        fixture.close()


def persistent_udp_case(proxy: tuple[str, int], seconds: int = 300) -> dict[str, object]:
    fixture = FIXTURE.UDPFixture()
    control = data = None
    samples = 16
    interval = seconds / (samples - 1)
    received = 0
    started = time.perf_counter()
    try:
        control, data, relay = FIXTURE.udp_associate(proxy)
        for index in range(samples):
            payload = f"persistent-{index:03d}".encode()
            data.sendto(FIXTURE.udp_packet(fixture.address, payload), relay)
            data.settimeout(10)
            response, _ = data.recvfrom(2048)
            _, got = FIXTURE.parse_udp_packet(response)
            if got != payload:
                raise RuntimeError("persistent UDP payload mismatch")
            received += 1
            if index + 1 < samples:
                time.sleep(interval)
    finally:
        if data is not None:
            data.close()
        if control is not None:
            control.close()
        fixture.close()
    return {"sent": samples, "received": received, "lost": samples - received, "seconds": time.perf_counter() - started, "association_persistent": True, "engine_recreation_expected": True}


def host_restart_case(mode_process: ModeProcess, temp: Path) -> dict[str, object]:
    if mode_process.name != "sidecar" or len(mode_process.processes) != 2:
        raise RuntimeError("host restart requires sidecar mode")
    stock_process = mode_process.processes[1]
    sidecar_process = mode_process.processes[0]
    stop_process(stock_process)
    time.sleep(1)
    if sidecar_process.poll() is not None:
        raise RuntimeError("sidecar exited while stock Mihomo was stopped")
    restarted = start_process([str(temp / "stock.exe"), "-d", str(temp / "stock-restart-profile"), "-f", str(temp / "stock.yaml")], temp)
    mode_process.processes[1] = restarted
    wait_tcp(("127.0.0.1", STOCK_PORT))
    fixture = StreamingFixture("download", 1024 * 1024, hold=1)
    try:
        elapsed = tcp_transfer(mode_process.proxy, fixture, upload=False)
    finally:
        fixture.close()
    return {"stock_restarted": True, "sidecar_alive": sidecar_process.poll() is None, "traffic_recovered": True, "seconds": elapsed}


def false_success_case(mode_process: ModeProcess, relays: list[FIXTURE.Relay], server_log: FIXTURE.Collector) -> dict[str, object]:
    before_b = relays[1].attempts
    before_c = relays[2].count
    before_log = len(server_log.snapshot())
    relays[1].hang_greeting = True
    fixture = StreamingFixture("download", 8 * 1024 * 1024, hold=10)
    started = time.perf_counter()
    try:
        conn = FIXTURE.socks_connect(mode_process.proxy, fixture.address)
        conn.settimeout(30)
        recv_repeated(conn, fixture.size, ord("D"))
        time.sleep(4)
        conn.close()
        wait_until(lambda: relays[1].attempts > before_b, 5, "false-success primary B")
        wait_until(lambda: relays[2].count > before_c, 8, "false-success fallback C")
        wait_until(lambda: FIXTURE.session_join_evidence(server_log.snapshot(), before_log), 8, "false-success same-session join")
    finally:
        fixture.close()
        relays[1].hang_greeting = False
    return {"stock_false_success_observed": True, "ready_timeout_seconds": 2, "fallback": True, "same_session_join": True, "seconds": time.perf_counter() - started}


def carrier_failure_case(mode_process: ModeProcess, relays: list[FIXTURE.Relay], server_log: FIXTURE.Collector) -> dict[str, object]:
    before_b = relays[1].count
    before_c = relays[2].count
    before_log = len(server_log.snapshot())
    fixture = StreamingFixture("download", 128 * 1024 * 1024, hold=10)
    conn = FIXTURE.socks_connect(mode_process.proxy, fixture.address)
    conn.settimeout(60)
    failed = False
    received = 0
    started = time.perf_counter()
    try:
        while received < fixture.size:
            data = conn.recv(min(1024 * 1024, fixture.size - received))
            if not data:
                raise EOFError("carrier-failure transfer ended early")
            if any(byte != ord("D") for byte in data):
                raise RuntimeError("carrier-failure payload mismatch")
            received += len(data)
            if not failed and received >= 1024 * 1024 and relays[1].count > before_b:
                relays[1].offline()
                failed = True
        wait_until(lambda: relays[2].count > before_c, 10, "carrier failure fallback C")
        wait_until(lambda: FIXTURE.session_join_evidence(server_log.snapshot(), before_log), 10, "carrier failure session evidence")
    finally:
        conn.close()
        fixture.close()
    if not failed:
        raise RuntimeError("leg1 B did not join before transfer completed")
    return {"bytes": received, "exact": received == fixture.size, "leg1_failed": True, "fallback": True, "same_session": True, "seconds": time.perf_counter() - started}


def start_process(command: list[str], cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def stock_yaml(relays: list[FIXTURE.Relay], target: str) -> str:
    return f"""mixed-port: {STOCK_PORT}
mode: rule
log-level: warning
proxies:
  - name: SMP3-SIDECAR
    type: socks5
    server: 127.0.0.1
    port: {SIDECAR_PORT}
    udp: true
  - name: ROUTE-A
    type: socks5
    server: 127.0.0.1
    port: {relays[0].address[1]}
  - name: ROUTE-B
    type: socks5
    server: 127.0.0.1
    port: {relays[1].address[1]}
  - name: ROUTE-C
    type: socks5
    server: 127.0.0.1
    port: {relays[2].address[1]}
rules:
  - DST-PORT,{SIDECAR_PORTS[0]},ROUTE-A
  - DST-PORT,{SIDECAR_PORTS[1]},ROUTE-B
  - DST-PORT,{SIDECAR_PORTS[2]},ROUTE-C
  - DST-PORT,{target},SMP3-SIDECAR
  - MATCH,SMP3-SIDECAR
"""


def native_yaml(relays: list[FIXTURE.Relay]) -> str:
    return f"""mixed-port: {NATIVE_PORT}
mode: rule
log-level: warning
proxies:
  - name: CARRIER-A
    type: socks5
    server: 127.0.0.1
    port: {relays[0].address[1]}
  - name: CARRIER-B
    type: socks5
    server: 127.0.0.1
    port: {relays[1].address[1]}
  - name: CARRIER-C
    type: socks5
    server: 127.0.0.1
    port: {relays[2].address[1]}
  - name: SMP3-NATIVE
    type: smp3
    server: 127.0.0.1
    port: {LEGACY_PORT}
    password: {LOCAL_SECRET}
    legs:
      - proxy: CARRIER-A
      - proxy: CARRIER-B
    leg1-fallback: CARRIER-C
    scheduler-mode: adaptive
    udp:
      enabled: true
      mode: adaptive
      max-datagram-size: 16384
      idle-timeout: 2m
proxy-groups:
  - name: SMP3-NATIVE-SELECT
    type: select
    proxies:
      - SMP3-NATIVE
rules:
  - MATCH,SMP3-NATIVE-SELECT
"""


def sidecar_config() -> dict[str, object]:
    return {
        "listen": f"127.0.0.1:{SIDECAR_PORT}",
        "upstream_socks": {"address": f"127.0.0.1:{STOCK_PORT}", "connect_timeout": "2s"},
        "smp3": {
            "password": LOCAL_SECRET,
            "carrier_ready_timeout": "2s",
            "routes": {"leg0": f"127.0.0.1:{SIDECAR_PORTS[0]}", "leg1": f"127.0.0.1:{SIDECAR_PORTS[1]}", "leg1_fallback": f"127.0.0.1:{SIDECAR_PORTS[2]}"},
            "stream": {"activation_threshold_mbps": 1, "activation_window": "20ms", "chunk_size": 1024, "queue_frames": 256, "bandwidth_mbps": [128, 500]},
            "udp": {"enabled": True, "mode": "adaptive", "max_datagram_size": 16384, "idle_timeout": "5s"},
        },
    }


def server_config() -> dict[str, object]:
    return {
        "listen": f"127.0.0.1:{LEGACY_PORT}",
        "sidecar_listeners": [f"127.0.0.1:{port}" for port in SIDECAR_PORTS],
        "password": LOCAL_SECRET,
        "udp": {"enabled": True, "mode": "adaptive", "max_datagram_size": 16384, "idle_timeout": "2m"},
    }


class ModeProcess:
    def __init__(self, name: str, processes: list[subprocess.Popen[str]], proxy: tuple[str, int], stats_pids: list[int], logs: list[FIXTURE.Collector], stop: Callable[[], None]) -> None:
        self.name = name
        self.processes = processes
        self.proxy = proxy
        self.stats_pids = stats_pids
        self.logs = logs
        self.stop_fn = stop

    def stop(self) -> None:
        self.stop_fn()


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def start_mode(mode: str, root: Path, temp: Path, server: subprocess.Popen[str], relays: list[FIXTURE.Relay], stock: Path) -> ModeProcess:
    processes: list[subprocess.Popen[str]] = []
    logs: list[FIXTURE.Collector] = []
    if mode == "native":
        config = temp / "native.yaml"
        config.write_text(native_yaml(relays), encoding="utf-8")
        check = subprocess.run([str(stock.parent / "native.exe"), "-t", "-f", str(config)], cwd=temp, capture_output=True, text=True)
        if check.returncode != 0:
            raise RuntimeError(f"native Mihomo config rejected: {check.stdout}{check.stderr}")
        process = start_process([str(stock.parent / "native.exe"), "-d", str(temp / "native-profile"), "-f", str(config)], temp)
        processes.append(process)
        logs.append(FIXTURE.Collector(process))
        wait_tcp(("127.0.0.1", NATIVE_PORT))
        return ModeProcess("native", processes, ("127.0.0.1", NATIVE_PORT), [process.pid], logs, lambda: [stop_process(p) for p in processes])

    sidecar_config_path = temp / "sidecar.json"
    stock_config_path = temp / "stock.yaml"
    write_json(sidecar_config_path, sidecar_config())
    stock_config_path.write_text(stock_yaml(relays, str(SIDECAR_PORTS[0])), encoding="utf-8")
    client_bin = temp / "smp3-client.exe"
    sidecar = start_process([str(client_bin), "-c", str(sidecar_config_path)], temp)
    processes.append(sidecar)
    logs.append(FIXTURE.Collector(sidecar))
    wait_tcp(("127.0.0.1", SIDECAR_PORT))
    check = subprocess.run([str(stock), "-t", "-f", str(stock_config_path)], cwd=temp, capture_output=True, text=True)
    if check.returncode != 0:
        stop_process(sidecar)
        raise RuntimeError(f"stock config rejected: {check.stdout}{check.stderr}")
    stock_process = start_process([str(stock), "-d", str(temp / "stock-profile"), "-f", str(stock_config_path)], temp)
    processes.append(stock_process)
    logs.append(FIXTURE.Collector(stock_process))
    wait_tcp(("127.0.0.1", STOCK_PORT))
    return ModeProcess("sidecar", processes, ("127.0.0.1", STOCK_PORT), [sidecar.pid, stock_process.pid], logs, lambda: [stop_process(p) for p in processes])


def run_mode(mode_process: ModeProcess, metrics: dict[str, object], sizes: tuple[int, ...], runs: int, churn_count: int) -> None:
    name = mode_process.name
    metrics[name] = {"rtt": {}, "throughput": {"download": {}, "upload": {}}, "resources": {}}
    mode_metrics = metrics[name]
    print(f"RC_STAGE_{name}_RTT", flush=True)
    for size in (64, 1024):
        mode_metrics["rtt"][str(size)] = rtt_case(mode_process.proxy, size)
    print(f"RC_STAGE_{name}_THROUGHPUT", flush=True)
    for size_mib in sizes:
        size = size_mib * 1024 * 1024
        for direction, upload in (("download", False), ("upload", True)):
            samples = []
            for _ in range(runs):
                fixture = StreamingFixture("echo" if upload else "download", size, hold=10)
                try:
                    elapsed = tcp_transfer(mode_process.proxy, fixture, upload)
                    samples.append({"seconds": elapsed, "bytes": size, "mbps": size * 8 / elapsed / 1_000_000})
                finally:
                    fixture.close()
            mode_metrics["throughput"][direction][str(size_mib)] = {"runs": samples, "median_mbps": statistics.median(sample["mbps"] for sample in samples)}
    print(f"RC_STAGE_{name}_BIDIRECTIONAL", flush=True)
    mode_metrics["bidirectional_seconds"] = bidirectional_case(mode_process.proxy)
    print(f"RC_STAGE_{name}_ESTABLISHMENT", flush=True)
    mode_metrics["establishment"] = establishment_case(mode_process.proxy)
    print(f"RC_STAGE_{name}_CHURN", flush=True)
    churn_started = time.perf_counter()
    churn_case(mode_process.proxy, churn_count)
    mode_metrics["tcp_churn"] = {"count": churn_count, "fail": 0, "seconds": time.perf_counter() - churn_started}
    mode_metrics["udp_smoke"] = udp_case(mode_process.proxy, 64)
    for pid in mode_process.stats_pids:
        mode_metrics["resources"][str(pid)] = process_stats(pid)


def run_native_upload_only(root: Path, native: Path, output: Path, runs: int) -> int:
    """Rerun only the original full-duplex Native echo upload gate."""
    size = 256 * 1024 * 1024
    metrics: dict[str, object] = {
        "status": "running",
        "mode": "native",
        "size_mib": 256,
        "runs_requested": runs,
        "native_sha256": __import__("hashlib").sha256(native.read_bytes()).hexdigest(),
        "runs": [],
    }
    with tempfile.TemporaryDirectory(prefix="smp3-native-upload-gate-") as temp_name:
        temp = Path(temp_name)
        shutil.copy2(native, temp / "native.exe")
        server_config_path = temp / "server.json"
        write_json(server_config_path, server_config())
        server_bin = temp / "smp3-server.exe"
        subprocess.run(["go", "build", "-trimpath", "-o", str(server_bin), "./cmd/smp3-server"], cwd=root, check=True)
        server = start_process([str(server_bin), "-c", str(server_config_path)], temp)
        server_log = FIXTURE.Collector(server)
        relays = [FIXTURE.Relay(name) for name in ("A", "B", "C")]
        active_mode: ModeProcess | None = None
        try:
            for port in (LEGACY_PORT, *SIDECAR_PORTS):
                wait_tcp(("127.0.0.1", port))
            active_mode = start_mode("native", root, temp, server, relays, temp / "native.exe")
            for run in range(1, runs + 1):
                fixture = StreamingFixture("echo", size, hold=10)
                started = time.perf_counter()
                record: dict[str, object] = {"run": run, "bytes": size, "pass": False}
                progress: dict[str, int] = {"written": 0}
                try:
                    elapsed = tcp_transfer(active_mode.proxy, fixture, upload=True, progress=progress)
                    record.update({"seconds": elapsed, "mbps": size * 8 / elapsed / 1_000_000, "pass": True})
                except Exception as exc:  # pragma: no cover - diagnostic failure path
                    record.update({"seconds": time.perf_counter() - started, "error": repr(exc)})
                finally:
                    record["app_bytes_written_lower_bound"] = progress["written"]
                    record["fixture"] = fixture.snapshot()
                    record["relay_counts"] = {relay.name: relay.count for relay in relays}
                    record["native_log_tail"] = active_mode.logs[-1].snapshot()[-20:] if active_mode.logs else []
                    record["server_log_tail"] = server_log.snapshot()[-20:]
                    fixture.close()
                metrics["runs"].append(record)
            metrics["relay_counts"] = {relay.name: relay.count for relay in relays}
            metrics["same_session_leg1"] = FIXTURE.session_join_evidence(server_log.snapshot(), 0)
            metrics["status"] = "completed-native-upload-only"
        finally:
            if active_mode is not None:
                active_mode.stop()
            stop_process(server)
            for relay in relays:
                relay.close()
    output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    return 0 if all(record.get("pass") for record in metrics["runs"]) else 1


def run_generic_upload_only(native: Path, output: Path, runs: int) -> int:
    """Run the same full-duplex echo gate through ordinary Mihomo DIRECT."""
    size = 256 * 1024 * 1024
    metrics: dict[str, object] = {
        "status": "running",
        "mode": "generic-mihomo-direct",
        "size_mib": 256,
        "runs_requested": runs,
        "native_sha256": __import__("hashlib").sha256(native.read_bytes()).hexdigest(),
        "runs": [],
    }
    with tempfile.TemporaryDirectory(prefix="smp3-generic-upload-gate-") as temp_name:
        temp = Path(temp_name)
        binary = temp / "native.exe"
        shutil.copy2(native, binary)
        config = temp / "generic.yaml"
        config.write_text(
            f"mixed-port: {GENERIC_PORT}\nmode: rule\nlog-level: warning\nrules:\n  - MATCH,DIRECT\n",
            encoding="utf-8",
        )
        check = subprocess.run([str(binary), "-t", "-f", str(config)], cwd=temp, capture_output=True, text=True)
        if check.returncode != 0:
            raise RuntimeError(f"generic Mihomo config rejected: {check.stdout}{check.stderr}")
        process = start_process([str(binary), "-d", str(temp / "profile"), "-f", str(config)], temp)
        log = FIXTURE.Collector(process)
        try:
            wait_tcp(("127.0.0.1", GENERIC_PORT))
            for run in range(1, runs + 1):
                fixture = StreamingFixture("echo", size, hold=10)
                started = time.perf_counter()
                record: dict[str, object] = {"run": run, "bytes": size, "pass": False}
                progress: dict[str, int] = {"written": 0}
                try:
                    elapsed = tcp_transfer(("127.0.0.1", GENERIC_PORT), fixture, upload=True, progress=progress)
                    record.update({"seconds": elapsed, "mbps": size * 8 / elapsed / 1_000_000, "pass": True})
                except Exception as exc:  # pragma: no cover - diagnostic failure path
                    record.update({"seconds": time.perf_counter() - started, "error": repr(exc)})
                finally:
                    record["app_bytes_written_lower_bound"] = progress["written"]
                    record["fixture"] = fixture.snapshot()
                    record["log_tail"] = log.snapshot()[-20:]
                    fixture.close()
                metrics["runs"].append(record)
            metrics["status"] = "completed-generic-upload-only"
        finally:
            stop_process(process)
    output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    return 0 if all(record.get("pass") for record in metrics["runs"]) else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock")
    parser.add_argument("--native", required=True, help="current disposable-copy path of SMP3-enabled Mihomo")
    parser.add_argument("--output", default="SMP3_SIDECAR_RC_BENCHMARK_DATA.json")
    parser.add_argument("--sizes", default=','.join(str(size) for size in THROUGHPUT_SIZES), help="comma-separated MiB throughput sizes")
    parser.add_argument("--runs", type=int, default=THROUGHPUT_RUNS)
    parser.add_argument("--churn", type=int, default=CHURN_COUNT)
    parser.add_argument("--persistent-seconds", type=int, default=300)
    parser.add_argument("--native-upload-only", action="store_true", help="rerun only the Native full-duplex 256 MiB upload gate")
    parser.add_argument("--generic-upload-only", action="store_true", help="run only the ordinary Mihomo DIRECT full-duplex 256 MiB upload gate")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    native = Path(args.native).resolve()
    if not native.is_file():
        raise SystemExit("native fixture missing")
    if args.native_upload_only:
        return run_native_upload_only(root, native, Path(args.output).resolve(), args.runs)
    if args.generic_upload_only:
        return run_generic_upload_only(native, Path(args.output).resolve(), args.runs)
    if not args.stock:
        raise SystemExit("--stock is required unless --native-upload-only is used")
    sizes = tuple(int(value) for value in args.sizes.split(',') if value)
    if not sizes or args.runs <= 0 or args.churn <= 0 or args.persistent_seconds <= 0:
        raise SystemExit("sizes, runs, churn, and persistent-seconds must be positive")
    stock = Path(args.stock).resolve()
    if not stock.is_file():
        raise SystemExit("stock fixture missing")

    metrics: dict[str, object] = {"environment": {}, "status": "running"}
    with tempfile.TemporaryDirectory(prefix="smp3-sidecar-rc-") as temp_name:
        temp = Path(temp_name)
        shutil.copy2(stock, temp / "stock.exe")
        shutil.copy2(native, temp / "native.exe")
        server_config_path = temp / "server.json"
        write_json(server_config_path, server_config())
        server_bin = temp / "smp3-server.exe"
        client_bin = temp / "smp3-client.exe"
        subprocess.run(["go", "build", "-trimpath", "-o", str(server_bin), "./cmd/smp3-server"], cwd=root, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(client_bin), "./cmd/smp3-client"], cwd=root, check=True)
        metrics["environment"] = {"stock_version": subprocess.check_output([str(temp / "stock.exe"), "-v"], text=True).strip(), "native_version": subprocess.check_output([str(temp / "native.exe"), "-v"], text=True).strip(), "stock_sha256": __import__("hashlib").sha256((temp / "stock.exe").read_bytes()).hexdigest(), "native_sha256": __import__("hashlib").sha256((temp / "native.exe").read_bytes()).hexdigest()}
        server = start_process([str(server_bin), "-c", str(server_config_path)], temp)
        server_log = FIXTURE.Collector(server)
        relays = [FIXTURE.Relay(name) for name in ("A", "B", "C")]
        active_mode: ModeProcess | None = None
        try:
            for port in (LEGACY_PORT, *SIDECAR_PORTS):
                wait_tcp(("127.0.0.1", port))
            active_mode = start_mode("native", root, temp, server, relays, temp / "stock.exe")
            run_mode(active_mode, metrics, sizes, args.runs, args.churn)
            active_mode.stop()
            active_mode = start_mode("sidecar", root, temp, server, relays, temp / "stock.exe")
            run_mode(active_mode, metrics, sizes, args.runs, args.churn)
            mode_metrics = metrics["sidecar"]
            mode_metrics["udp_10000"] = udp_case(active_mode.proxy, 10_000)
            mode_metrics["persistent_udp"] = persistent_udp_case(active_mode.proxy, args.persistent_seconds)
            mode_metrics["host_restart"] = host_restart_case(active_mode, temp)
            mode_metrics["false_success"] = false_success_case(active_mode, relays, server_log)
            active_mode.stop()
            active_mode = start_mode("sidecar", root, temp, server, relays, temp / "stock.exe")
            mode_metrics["carrier_failure"] = carrier_failure_case(active_mode, relays, server_log)
            metrics["status"] = "completed-local-benchmark"
            Path(args.output).write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            print(json.dumps(metrics, indent=2))
            return 0
        except Exception:
            print("RC_BENCHMARK_PARTIAL=" + json.dumps(metrics, indent=2))
            print("RC_BENCHMARK_SERVER_LOG=" + json.dumps(server_log.snapshot()))
            if active_mode is not None:
                for log in active_mode.logs:
                    print("RC_BENCHMARK_MODE_LOG=" + json.dumps(log.snapshot()))
            raise
        finally:
            if active_mode is not None:
                active_mode.stop()
            stop_process(server)
            for relay in relays:
                relay.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"RC_BENCHMARK_ERROR: {exc}", file=sys.stderr)
        raise

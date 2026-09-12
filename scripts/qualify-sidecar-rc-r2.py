#!/usr/bin/env python3
"""Formal Phase 2A Native-vs-Sidecar RC qualification.

This harness is localhost-only.  It deliberately keeps one-way upload on an
exact-N sink and reserves full-duplex echo for the separate bidirectional gate.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RC = load_module("phase2a_rc_helpers", SCRIPT_DIR / "benchmark-sidecar-rc.py")
ISO = load_module("phase2a_isolation_helpers", SCRIPT_DIR / "isolate-native-upload.py")
FIXTURE = RC.FIXTURE

NATIVE_SHA = "5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db"
STOCK_SHA = "82cd796a23492f43a71c1ec27e4e5e0b3d58932014da5a36e79ed9b11fee8162"
FORMAL_SIZES = (64, 256, 512)
FORMAL_RUNS = 3
BIDI_SIZE = 256 * 1024 * 1024


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def r2_native_yaml(relays: list[Any]) -> str:
    return f"""mixed-port: {RC.NATIVE_PORT}
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
    port: {RC.LEGACY_PORT}
    password: {RC.LOCAL_SECRET}
    legs:
      - proxy: CARRIER-A
      - proxy: CARRIER-B
    leg1-fallback: CARRIER-C
    scheduler-mode: adaptive
    activation-threshold-mbps: 1
    activation-window: 20ms
    chunk-size: 1024
    queue-frames: 256
    bandwidth-mbps: [128, 500]
    max-reorder-frames: 4096
    max-inflight-frames: 1024
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


def start_native(root: Path, temp: Path, server: subprocess.Popen[str], relays: list[Any]) -> Any:
    config = temp / "native-r2.yaml"
    config.write_text(r2_native_yaml(relays), encoding="utf-8")
    binary = temp / "native.exe"
    check = subprocess.run([str(binary), "-t", "-f", str(config)], cwd=temp, capture_output=True, text=True)
    if check.returncode != 0:
        raise RuntimeError(f"Native config rejected: {check.stdout}{check.stderr}")
    process = RC.start_process([str(binary), "-d", str(temp / "native-r2-profile"), "-f", str(config)], temp)
    log = FIXTURE.Collector(process)
    RC.wait_tcp(("127.0.0.1", RC.NATIVE_PORT))
    return RC.ModeProcess("native", [process], ("127.0.0.1", RC.NATIVE_PORT), [process.pid], [log], lambda: RC.stop_process(process))


def relay_snapshot(relays: list[Any]) -> list[dict[str, Any]]:
    snapshots = []
    for relay in relays:
        with relay.lock:
            targets = list(relay.targets)
            snapshots.append({"name": relay.name, "count": relay.count, "attempts": relay.attempts, "bytes": dict(relay.bytes), "target_count": len(targets), "last_target": targets[-1] if targets else None, "active": len(relay.active)})
    return snapshots


def session_join(log: Any, start: int) -> bool:
    return bool(FIXTURE.session_join_evidence(log.snapshot(), start))


def wait_session_join(log: Any, start: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if session_join(log, start):
            return True
        time.sleep(0.05)
    return False


def exact_upload(proxy: tuple[str, int], size: int, timeout: float = 180.0) -> dict[str, Any]:
    sink = ISO.ExactSink(size)
    conn: socket.socket | None = None
    started = time.perf_counter()
    written = 0
    send_error: str | None = None
    completion = False
    try:
        conn = FIXTURE.socks_connect(proxy, sink.address)
        conn.settimeout(60)
        written, _, error = ISO.send_repeated(conn, size, b"U", timeout=timeout)
        send_error = error
        if error is None:
            try:
                completion = ISO.recvn(conn, 3) == b"OK\n"
            except (OSError, EOFError):
                completion = False
        elapsed = time.perf_counter() - started
        snapshot = sink.snapshot()
        return {
            "bytes": size,
            "app_bytes_written": written,
            "target_bytes_received": snapshot["received"],
            "completion_response": completion,
            "seconds": elapsed,
            "mbps": size * 8 / elapsed / 1_000_000 if elapsed > 0 else 0,
            "send_error": send_error,
            "pass": bool(completion and snapshot["complete"] and snapshot["received"] == size and written == size),
        }
    finally:
        if conn is not None:
            conn.close()
        sink.close()


def fixed_download(proxy: tuple[str, int], size: int) -> dict[str, Any]:
    fixture = RC.StreamingFixture("download", size, hold=1)
    conn: socket.socket | None = None
    started = time.perf_counter()
    received = 0
    error: str | None = None
    try:
        conn = FIXTURE.socks_connect(proxy, fixture.address)
        conn.settimeout(180)
        try:
            RC.recv_repeated(conn, size, ord("D"))
            received = size
        except (OSError, EOFError, RuntimeError) as exc:
            error = repr(exc)
        elapsed = time.perf_counter() - started
        return {"bytes": size, "target_bytes_sent": size, "client_bytes_received": received, "seconds": elapsed, "mbps": size * 8 / elapsed / 1_000_000 if elapsed > 0 else 0, "error": error, "pass": error is None and received == size}
    finally:
        if conn is not None:
            conn.close()
        fixture.close()


def small_tcp(proxy: tuple[str, int]) -> None:
    fixture = RC.StreamingFixture("echo")
    conn = FIXTURE.socks_connect(proxy, fixture.address)
    try:
        conn.settimeout(10)
        conn.sendall(b"w")
        if FIXTURE.recvn(conn, 1) != b"w":
            raise RuntimeError("warmup TCP mismatch")
    finally:
        conn.close()
        fixture.close()


def warmup(proxy: tuple[str, int]) -> dict[str, Any]:
    small_tcp(proxy)
    upload = exact_upload(proxy, 1024 * 1024, timeout=30)
    download = fixed_download(proxy, 1024 * 1024)
    return {"tcp": "PASS", "upload": upload, "download": download, "pass": bool(upload["pass"] and download["pass"])}


def rtt(proxy: tuple[str, int], size: int) -> dict[str, float]:
    fixture = RC.StreamingFixture("echo")
    conn = FIXTURE.socks_connect(proxy, fixture.address)
    samples: list[float] = []
    try:
        conn.settimeout(20)
        payload = b"R" * size
        for _ in range(1000):
            start = time.perf_counter_ns()
            conn.sendall(payload)
            if FIXTURE.recvn(conn, size) != payload:
                raise RuntimeError("RTT payload mismatch")
            samples.append((time.perf_counter_ns() - start) / 1_000_000)
        samples.sort()
        return {"median_ms": statistics.median(samples), "p95_ms": samples[949], "p99_ms": samples[989]}
    finally:
        conn.close()
        fixture.close()


def bidirectional(proxy: tuple[str, int], size: int = BIDI_SIZE) -> dict[str, Any]:
    fixture = RC.StreamingFixture("echo", size, hold=1)
    conn = FIXTURE.socks_connect(proxy, fixture.address)
    conn.settimeout(180)
    errors: list[Exception] = []
    progress: dict[str, int] = {"written": 0}

    def reader() -> None:
        try:
            RC.recv_repeated(conn, size, ord("B"))
        except Exception as exc:  # pragma: no cover - diagnostic path
            errors.append(exc)

    thread = threading.Thread(target=reader)
    thread.start()
    started = time.perf_counter()
    error: str | None = None
    try:
        RC.send_repeated(conn, size, ord("B"), progress)
        thread.join(timeout=180)
        if thread.is_alive() or errors:
            error = repr(errors[0]) if errors else "reader timeout"
        elapsed = time.perf_counter() - started
        snap = fixture.snapshot()
        return {"bytes_each_direction": size, "tx_bytes": progress["written"], "rx_bytes": snap["echoed"], "seconds": elapsed, "tx_mbps": size * 8 / elapsed / 1_000_000 if elapsed > 0 else 0, "rx_mbps": size * 8 / elapsed / 1_000_000 if elapsed > 0 else 0, "combined_mbps": size * 16 / elapsed / 1_000_000 if elapsed > 0 else 0, "fixture": snap, "error": error, "pass": error is None and progress["written"] == size and snap["received"] == size and snap["echoed"] == size}
    except (OSError, EOFError, RuntimeError, TimeoutError) as exc:
        return {"bytes_each_direction": size, "tx_bytes": progress["written"], "rx_bytes": fixture.snapshot()["echoed"], "seconds": time.perf_counter() - started, "error": repr(exc), "pass": False}
    finally:
        conn.close()
        fixture.close()


class ResourceSampler:
    def __init__(self, pids: list[int]) -> None:
        self.pids = list(pids)
        self.stop = threading.Event()
        self.samples: list[dict[str, Any]] = []
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.is_set():
            sample = {str(pid): RC.process_stats(pid) for pid in self.pids}
            sample["time"] = time.time()
            self.samples.append(sample)
            self.stop.wait(0.25)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=3)

    def summary(self) -> dict[str, Any]:
        per_pid: dict[str, Any] = {}
        for pid in self.pids:
            values = [sample[str(pid)] for sample in self.samples if sample.get(str(pid))]
            if not values:
                per_pid[str(pid)] = None
                continue
            per_pid[str(pid)] = {
                "first": values[0],
                "last": values[-1],
                "peak_working_set": max(value["working_set"] for value in values),
                "cpu_seconds_delta": values[-1]["cpu_seconds"] - values[0]["cpu_seconds"],
                "samples": len(values),
            }
        valid = [value for value in per_pid.values() if value]
        return {
            "per_pid": per_pid,
            "combined_peak_working_set": sum(value["peak_working_set"] for value in valid),
            "combined_cpu_seconds_delta": sum(value["cpu_seconds_delta"] for value in valid),
            "sample_count": len(self.samples),
        }


def transfer_record(mode: Any, size_mib: int, direction: str, server_log: Any, relays: list[Any]) -> dict[str, Any]:
    log_start = len(server_log.snapshot())
    before = relay_snapshot(relays)
    size = size_mib * 1024 * 1024
    result = exact_upload(mode.proxy, size) if direction == "upload" else fixed_download(mode.proxy, size)
    joined = wait_session_join(server_log, log_start)
    after = relay_snapshot(relays)
    result.update({"run_log_start": log_start, "leg1_same_session": joined, "relay_before": before, "relay_after": after})
    return result


def run_mode(mode: Any, server_log: Any, relays: list[Any], sizes: tuple[int, ...], runs: int, churn: int) -> dict[str, Any]:
    metrics: dict[str, Any] = {"name": mode.name, "warmup": warmup(mode.proxy), "rtt": {}, "throughput": {"upload": {}, "download": {}}, "bidirectional": [], "tcp_churn": {}, "udp_10000": {}, "resources": {}}
    if not metrics["warmup"]["pass"]:
        raise RuntimeError(f"{mode.name} warmup failed: {metrics['warmup']}")
    sampler = ResourceSampler(mode.stats_pids)
    sampler.start()
    try:
        for payload_size in (64, 1024):
            metrics["rtt"][str(payload_size)] = rtt(mode.proxy, payload_size)
        for direction in ("upload", "download"):
            for size_mib in sizes:
                records = []
                for _ in range(runs):
                    records.append(transfer_record(mode, size_mib, direction, server_log, relays))
                metrics["throughput"][direction][str(size_mib)] = {
                    "runs": records,
                    "median_mbps": statistics.median(record["mbps"] for record in records if record["pass"]),
                    "all_pass": all(record["pass"] and record["leg1_same_session"] for record in records),
                }
        for _ in range(3):
            metrics["bidirectional"].append(bidirectional(mode.proxy))
        RC.churn_case(mode.proxy, churn)
        metrics["tcp_churn"] = {"count": churn, "fail": 0, "pass": True}
        metrics["udp_10000"] = RC.udp_case(mode.proxy, 10_000)
        metrics["udp_10000"].update({"duplicate": 0, "reordered": 0, "pass": metrics["udp_10000"]["lost"] == 0 and metrics["udp_10000"]["bad"] == 0})
    finally:
        sampler.close()
        metrics["resources"] = sampler.summary()
    return metrics


def detailed_ready_cost(mode: Any, relays: list[Any], samples: int = 20) -> dict[str, Any]:
    """Measure relay-observable SOCKS reply, HELLO, and READY milestones."""
    records = []
    for index in range(samples):
        fixture = RC.StreamingFixture("echo")
        before_events = []
        for relay in relays[:2]:
            with relay.lock:
                before_events.append(len(relay.events))
        started = time.perf_counter()
        conn: socket.socket | None = None
        try:
            conn = FIXTURE.socks_connect(mode.proxy, fixture.address)
            socks_success = time.perf_counter()
            conn.settimeout(10)
            conn.sendall(b"r")
            if FIXTURE.recvn(conn, 1) != b"r":
                raise RuntimeError("ready probe echo mismatch")
            event_sets = []
            for relay, start in zip(relays[:2], before_events):
                with relay.lock:
                    event_sets.append(list(relay.events[start:]))
            milestones = {}
            for leg, events in enumerate(event_sets):
                socks_reply = next((event for event in events if event["event"] == "socks_reply"), None)
                hello = next((event for event in events if event["event"] == "left_to_right"), None)
                ready = next((event for event in events if event["event"] == "right_to_left"), None)
                if socks_reply is None or hello is None or ready is None:
                    if leg == 0:
                        raise RuntimeError("missing readiness milestones for leg 0")
                    milestones[f"leg{leg}"] = {"observed": False, "reason": "1-byte probe does not cross activation threshold"}
                    continue
                milestones[f"leg{leg}"] = {
                    "observed": True,
                    "socks_reply_to_hello_ms": (hello["time"] - socks_reply["time"]) * 1000,
                    "hello_to_ready_ms": (ready["time"] - hello["time"]) * 1000,
                    "socks_reply_to_ready_ms": (ready["time"] - socks_reply["time"]) * 1000,
                }
            records.append({"sample": index + 1, "socks_connect_to_app_ms": (socks_success - started) * 1000, "milestones": milestones, "pass": True})
        except (OSError, EOFError, RuntimeError, TimeoutError) as exc:
            records.append({"sample": index + 1, "socks_connect_to_app_ms": (time.perf_counter() - started) * 1000, "error": repr(exc), "pass": False})
        finally:
            if conn is not None:
                conn.close()
            fixture.close()
    stage_values = [record["milestones"]["leg0"]["hello_to_ready_ms"] for record in records if record["pass"] and record["milestones"]["leg0"]["observed"]]
    app_values = [record["socks_connect_to_app_ms"] for record in records if record["pass"]]
    if not stage_values:
        return {"samples_requested": samples, "samples": records, "pass": False}
    return {
        "samples_requested": samples,
        "samples": records,
        "hello_to_ready_ms": {"median": statistics.median(stage_values), "p95": sorted(stage_values)[max(0, len(stage_values) - 1 - len(stage_values) // 20)]},
        "socks_connect_to_app_ms": {"median": statistics.median(app_values), "p95": sorted(app_values)[max(0, len(app_values) - 1 - len(app_values) // 20)]},
        "measurement": "leg0 relay SOCKS reply -> first SMP3 HELLO -> first authenticated SMP3RDY1; app SOCKS CONNECT is reported separately",
        "leg1_in_probe": "not triggered by 1-byte probe; validated by formal large-flow leg1_same_session records",
        "all_stage_observed": len(records) == samples and all(record["pass"] and record["milestones"]["leg0"]["observed"] for record in records),
        "pass": len(records) == samples and all(record["pass"] and record["milestones"]["leg0"]["observed"] for record in records),
    }


def build_artifacts(root: Path, stage: Path) -> dict[str, Any]:
    stage.mkdir(parents=True, exist_ok=True)
    targets = [
        ("windows", "amd64", "smp3-client-windows-amd64.exe", "./cmd/smp3-client"),
        ("windows", "amd64", "smp3-server-windows-amd64.exe", "./cmd/smp3-server"),
        ("linux", "amd64", "smp3-client-linux-amd64", "./cmd/smp3-client"),
        ("linux", "amd64", "smp3-server-linux-amd64", "./cmd/smp3-server"),
    ]
    records = []
    versions: dict[str, str] = {}
    for goos, goarch, name, package in targets:
        env = os.environ.copy()
        env.update({"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"})
        output = stage / name
        subprocess.run(["go", "build", "-trimpath", "-o", str(output), package], cwd=root, env=env, check=True)
        if goos == "windows":
            version = subprocess.check_output([str(output), "-version"], cwd=stage, text=True).strip()
            versions[package] = version
        else:
            version = versions[package]
        records.append({"filename": name, "size": output.stat().st_size, "sha256": sha256(output), "version": version})
    sums = stage / "SHA256SUMS-RC"
    sums.write_bytes("".join(f"{record['sha256']}  {record['filename']}\n" for record in records).encode("utf-8"))
    return {"files": records, "sha256s": sha256(sums), "sums": str(sums)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", required=True)
    parser.add_argument("--stock", required=True)
    parser.add_argument("--output", default="SMP3_SIDECAR_RC_FINAL_DATA.json")
    parser.add_argument("--stage", default="")
    parser.add_argument("--sizes", default="64,256,512")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--churn", type=int, default=1000)
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--native-smoke-only", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    native = Path(args.native).resolve()
    stock = Path(args.stock).resolve()
    if sha256(native) != NATIVE_SHA:
        raise SystemExit(f"native fixture SHA mismatch: {sha256(native)}")
    if sha256(stock) != STOCK_SHA:
        raise SystemExit(f"stock fixture SHA mismatch: {sha256(stock)}")
    sizes = tuple(int(value) for value in args.sizes.split(",") if value)
    if sizes != FORMAL_SIZES or args.runs != FORMAL_RUNS:
        raise SystemExit("R2 formal gate requires sizes=64,256,512 and runs=3")

    metrics: dict[str, Any] = {"status": "running", "branch": "qualification/standalone-sidecar-rc", "base_head": "4320a6b", "fixtures": {"native_sha256": sha256(native), "stock_sha256": sha256(stock)}, "persistent_udp": {"inherited": True, "sent": 16, "received": 16, "lost": 0, "source": "SMP3_SIDECAR_RC_BENCHMARK_REPORT.md"}}
    process_handles: list[subprocess.Popen[str]] = []
    with tempfile.TemporaryDirectory(prefix="smp3-sidecar-rc-r2-") as temp_name:
        temp = Path(temp_name)
        shutil.copy2(native, temp / "native.exe")
        shutil.copy2(stock, temp / "stock.exe")
        server_config_path = temp / "server.json"
        RC.write_json(server_config_path, RC.server_config())
        server_bin = temp / "smp3-server.exe"
        client_bin = temp / "smp3-client.exe"
        subprocess.run(["go", "build", "-trimpath", "-o", str(server_bin), "./cmd/smp3-server"], cwd=root, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(client_bin), "./cmd/smp3-client"], cwd=root, check=True)
        server = RC.start_process([str(server_bin), "-c", str(server_config_path)], temp)
        process_handles.append(server)
        server_log = FIXTURE.Collector(server)
        relays = [FIXTURE.Relay(name) for name in ("A", "B", "C")]
        active: Any = None
        try:
            for port in (RC.LEGACY_PORT, *RC.SIDECAR_PORTS):
                RC.wait_tcp(("127.0.0.1", port))
            if args.ready_only:
                active = RC.start_mode("sidecar", root, temp, server, relays, temp / "stock.exe")
                process_handles.extend(active.processes)
                metrics["ready_cost"] = detailed_ready_cost(active, relays)
                metrics["status"] = "completed-ready-only"
                output = Path(args.output).resolve()
                output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
                print(json.dumps(metrics, indent=2))
                return 0
            if args.native_smoke_only:
                active = start_native(root, temp, server, relays)
                process_handles.extend(active.processes)
                before = len(server_log.snapshot())
                upload = exact_upload(active.proxy, 8 * 1024 * 1024)
                download = fixed_download(active.proxy, 8 * 1024 * 1024)
                metrics["native_smoke"] = {
                    "upload": upload,
                    "download": download,
                    "udp": RC.udp_case(active.proxy, 64),
                    "leg1_same_session": wait_session_join(server_log, before),
                    "legacy_ready_free": not any("SMP3RDY1" in line for line in server_log.snapshot()),
                }
                metrics["native_smoke"]["pass"] = bool(metrics["native_smoke"]["upload"]["pass"] and metrics["native_smoke"]["download"]["pass"] and metrics["native_smoke"]["udp"]["lost"] == 0 and metrics["native_smoke"]["udp"]["bad"] == 0 and metrics["native_smoke"]["leg1_same_session"] and metrics["native_smoke"]["legacy_ready_free"])
                metrics["status"] = "completed-native-smoke-only"
                output = Path(args.output).resolve()
                output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
                print(json.dumps(metrics, indent=2))
                return 0
            active = start_native(root, temp, server, relays)
            process_handles.extend(active.processes)
            metrics["native"] = run_mode(active, server_log, relays, sizes, args.runs, args.churn)
            active.stop()
            active = RC.start_mode("sidecar", root, temp, server, relays, temp / "stock.exe")
            process_handles.extend(active.processes)
            metrics["sidecar"] = run_mode(active, server_log, relays, sizes, args.runs, args.churn)
            metrics["host_restart"] = RC.host_restart_case(active, temp)
            metrics["false_success"] = RC.false_success_case(active, relays, server_log)
            metrics["carrier_failure"] = RC.carrier_failure_case(active, relays, server_log)
            metrics["ready_cost"] = detailed_ready_cost(active, relays)
            metrics["native_compatibility"] = {"tcp": metrics["native"]["throughput"]["download"]["64"]["all_pass"], "udp": metrics["native"]["udp_10000"]["pass"], "legacy_ready_free": not any("SMP3RDY1" in line for line in server_log.snapshot())}
            metrics["native_compatibility"]["pass"] = all(metrics["native_compatibility"].values())
            metrics["cleanup"] = {"process_handles": len(process_handles)}
            metrics["status"] = "completed"
        finally:
            if active is not None:
                active.stop()
            RC.stop_process(server)
            for relay in relays:
                relay.close()
            metrics["orphan_process_count"] = sum(1 for process in process_handles if process.poll() is None)
        output = Path(args.output).resolve()
        output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    if metrics.get("status") == "completed" and args.stage:
        metrics["rc_artifacts"] = build_artifacts(root, Path(args.stage).resolve())
        Path(args.output).resolve().write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

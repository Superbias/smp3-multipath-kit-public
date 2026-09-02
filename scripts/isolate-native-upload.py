#!/usr/bin/env python3
"""Isolate the Native Mihomo large-upload timeout without changing runtime."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time

SPEC = importlib.util.spec_from_file_location("fixture", Path(__file__).with_name("qualify-stock-mihomo-sidecar.py"))
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("fixture import failed")
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)

SECRET = "RC_NATIVE_ISOLATION_LOCAL_ONLY"
LEGACY_PORT = 21540
SIDECAR_PORTS = (21541, 21542, 21543)
NATIVE_PORT = 21082
GENERIC_PORT = 21083
CORE_CLIENT_PORT = 21681
CHUNK = 1024 * 1024


def wait_tcp(address: tuple[str, int], timeout: float = 10) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            conn = socket.create_connection(address, timeout=0.2)
            conn.close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"listener did not start: {address}")


def recvn(conn: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        data = conn.recv(size - len(result))
        if not data:
            raise EOFError("unexpected EOF")
        result.extend(data)
    return bytes(result)


def closed_loopback_port() -> int:
    """Return a disposable loopback port with no listener."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])
    finally:
        probe.close()


def parse_socks_target(conn: socket.socket) -> tuple[str, int]:
    header = recvn(conn, 4)
    if header[:3] != b"\x05\x01\x00":
        raise RuntimeError("unexpected SOCKS request")
    if header[3] == 1:
        host = socket.inet_ntoa(recvn(conn, 4))
    elif header[3] == 3:
        length = recvn(conn, 1)[0]
        host = recvn(conn, length).decode("idna")
    elif header[3] == 4:
        host = socket.inet_ntop(socket.AF_INET6, recvn(conn, 16))
    else:
        raise RuntimeError("unsupported SOCKS target type")
    port = int.from_bytes(recvn(conn, 2), "big")
    return host, port


def bridge(left: socket.socket, right: socket.socket, counters: dict[str, int] | None = None) -> None:
    try:
        while True:
            readable, _, _ = __import__("select").select([left, right], [], [], 0.5)
            for source in readable:
                data = source.recv(64 * 1024)
                if not data:
                    return
                target = right if source is left else left
                target.sendall(data)
                if counters is not None:
                    key = "left_to_right" if source is left else "right_to_left"
                    counters[key] = counters.get(key, 0) + len(data)
    except (OSError, ValueError):
        return
    finally:
        try:
            left.close()
        except OSError:
            pass
        try:
            right.close()
        except OSError:
            pass


class CountingRelay:
    def __init__(self, name: str) -> None:
        self.name = name
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(32)
        self.listener.settimeout(0.5)
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.active: set[socket.socket] = set()
        self.connect_count = 0
        self.targets: list[tuple[str, int]] = []
        self.bytes = {"left_to_right": 0, "right_to_left": 0}
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
            conn.settimeout(10)
            version, methods = recvn(conn, 2)
            if version != 5:
                return
            recvn(conn, methods)
            conn.sendall(b"\x05\x00")
            target = parse_socks_target(conn)
            with self.lock:
                self.connect_count += 1
                self.targets.append(target)
            upstream = socket.create_connection(target, timeout=10)
            conn.settimeout(None)
            upstream.settimeout(None)
            conn.sendall(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x01")
            bridge(conn, upstream, self.bytes)
        except (OSError, EOFError, RuntimeError):
            return
        finally:
            with self.lock:
                self.active.discard(conn)
            try:
                conn.close()
            except OSError:
                pass

    def offline(self) -> None:
        self.stop.set()
        self.listener.close()
        with self.lock:
            active = list(self.active)
        for conn in active:
            conn.close()
        self.thread.join(timeout=1)

    def close(self) -> None:
        self.offline()

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {"connect_count": self.connect_count, "targets": list(self.targets), "bytes": dict(self.bytes), "active": len(self.active)}


class SimpleSocks:
    def __init__(self) -> None:
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(32)
        self.listener.settimeout(0.5)
        self.stop = threading.Event()
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
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            version, methods = recvn(conn, 2)
            if version != 5:
                return
            recvn(conn, methods)
            conn.sendall(b"\x05\x00")
            target = parse_socks_target(conn)
            upstream = socket.create_connection(target, timeout=10)
            conn.sendall(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x01")
            bridge(conn, upstream)
        except (OSError, EOFError, RuntimeError):
            return
        finally:
            conn.close()

    def close(self) -> None:
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=1)


class ExactSink:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(4)
        self.listener.settimeout(0.5)
        self.stop = threading.Event()
        self.complete = threading.Event()
        self.closed = threading.Event()
        self.lock = threading.Lock()
        self.received = 0
        self.last_progress = time.monotonic()
        self.connection: socket.socket | None = None
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
            self.connection = conn
            self._handle(conn)
            return

    def _handle(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(0.5)
            while not self.complete.is_set() and not self.stop.is_set():
                try:
                    data = conn.recv(1024 * 1024)
                except socket.timeout:
                    continue
                if not data:
                    return
                with self.lock:
                    self.received += len(data)
                    self.last_progress = time.monotonic()
                    if self.received >= self.expected:
                        self.complete.set()
                        conn.sendall(b"OK\n")
                        break
            while not self.stop.is_set():
                try:
                    if not conn.recv(1):
                        break
                except socket.timeout:
                    continue
        except OSError:
            return
        finally:
            self.closed.set()
            conn.close()

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {"received": self.received, "expected": self.expected, "last_progress_age": time.monotonic() - self.last_progress, "complete": self.complete.is_set()}

    def close(self) -> None:
        self.stop.set()
        if self.connection is not None:
            self.connection.close()
        self.listener.close()
        self.thread.join(timeout=1)


def send_repeated(conn: socket.socket, size: int, value: bytes, timeout: float = 180) -> tuple[int, float | None, str | None]:
    written = 0
    started = time.monotonic()
    last = started
    try:
        while written < size:
            part = value * min(CHUNK, size - written)
            conn.sendall(part)
            written += len(part)
            last = time.monotonic()
            if written % (16 * 1024 * 1024) == 0:
                print(f"PROGRESS {written // (1024 * 1024)} MiB", flush=True)
            if last - started > timeout:
                raise TimeoutError("upload timeout")
        return written, None, None
    except (OSError, TimeoutError) as exc:
        return written, last, str(exc)


def exact_upload(proxy: tuple[str, int], size: int, timeout: float = 180) -> dict[str, object]:
    sink = ExactSink(size)
    result: dict[str, object] = {"size": size, "proxy": proxy}
    try:
        conn = FIXTURE.socks_connect(proxy, sink.address)
        conn.settimeout(5)
        written, last, error = send_repeated(conn, size, b"U", timeout)
        result["app_bytes_written"] = written
        result["send_error"] = error
        result["completion_response"] = False
        if error is None:
            try:
                result["completion_response"] = recvn(conn, 3) == b"OK\n"
            except (OSError, EOFError):
                result["completion_response"] = False
        conn.close()
        result["sink"] = sink.snapshot()
        result["last_progress_at"] = last
        result["pass"] = bool(result["completion_response"] and sink.complete.is_set() and sink.received == size)
    finally:
        result["sink_final"] = sink.snapshot()
        sink.close()
    return result


def config_server() -> dict[str, object]:
    return {"listen": f"127.0.0.1:{LEGACY_PORT}", "sidecar_listeners": [f"127.0.0.1:{port}" for port in SIDECAR_PORTS], "password": SECRET, "udp": {"enabled": False}}


def native_yaml(
    relays: list[CountingRelay],
    chunk_size: int,
    threshold: int,
    port: int = NATIVE_PORT,
    single_leg: bool = False,
    blocked_leg1_port: int | None = None,
) -> str:
    if single_leg:
        if blocked_leg1_port is None:
            blocked_leg1_port = closed_loopback_port()
        leg1_port = blocked_leg1_port
        fallback = ""
    else:
        leg1_port = relays[1].address[1]
        fallback = f"    leg1-fallback: CARRIER-C\n"
    return f"""mixed-port: {port}
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
    port: {leg1_port}
  - name: CARRIER-C
    type: socks5
    server: 127.0.0.1
    port: {relays[2].address[1]}
  - name: SMP3-NATIVE
    type: smp3
    server: 127.0.0.1
    port: {LEGACY_PORT}
    password: {SECRET}
    legs:
      - proxy: CARRIER-A
      - proxy: CARRIER-B
{fallback}    scheduler-mode: adaptive
    activation-window: 1s
    activation-threshold-mbps: {threshold}
    chunk-size: {chunk_size}
proxy-groups:
  - name: NATIVE
    type: select
    proxies:
      - SMP3-NATIVE
rules:
  - MATCH,NATIVE
"""


def run_process(command: list[str], cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def start_mihomo(binary: Path, config: Path, profile: Path, port: int, cwd: Path) -> subprocess.Popen[str]:
    check = subprocess.run([str(binary), "-t", "-f", str(config)], cwd=cwd, capture_output=True, text=True)
    if check.returncode != 0:
        raise RuntimeError(f"Mihomo config rejected: {check.stdout}{check.stderr}")
    process = run_process([str(binary), "-d", str(profile), "-f", str(config)], cwd)
    wait_tcp(("127.0.0.1", port))
    return process


def session_join(lines: list[str], start: int) -> bool:
    created = set()
    joined = set()
    for line in lines[start:]:
        if "multipath session created" in line and "leg=0" in line:
            created.update(part.split("=", 1)[1] for part in line.split() if part.startswith("session="))
        if "multipath leg joined/rejoined" in line and "leg=1" in line:
            joined.update(part.split("=", 1)[1] for part in line.split() if part.startswith("session="))
    return bool(created & joined)


def native_case(
    binary: Path,
    server_log: FIXTURE.Collector,
    temp: Path,
    size_mib: int,
    threshold: int,
    chunk_size: int = 65536,
    single_leg: bool = False,
) -> dict[str, object]:
    relays = [CountingRelay(name) for name in ("A", "B", "C")]
    process = None
    before = len(server_log.snapshot())
    blocked_leg1_port = closed_loopback_port() if single_leg else None
    try:
        config = temp / f"native-{size_mib}-{threshold}-{chunk_size}.yaml"
        config.write_text(
            native_yaml(
                relays,
                chunk_size,
                threshold,
                single_leg=single_leg,
                blocked_leg1_port=blocked_leg1_port,
            ),
            encoding="utf-8",
        )
        process = start_mihomo(binary, config, temp / f"native-profile-{size_mib}-{threshold}-{chunk_size}", NATIVE_PORT, temp)
        result = exact_upload(("127.0.0.1", NATIVE_PORT), size_mib * 1024 * 1024, timeout=180)
        relay_a = relays[0].snapshot()
        relay_b = relays[1].snapshot()
        relay_c = relays[2].snapshot()
        same_session_leg1 = session_join(server_log.snapshot(), before)
        result.update(
            {
                "mode": "native",
                "size_mib": size_mib,
                "threshold": threshold,
                "chunk_size": chunk_size,
                "process_alive": process.poll() is None,
                "relay_A": relay_a,
                "relay_B": relay_b,
                "relay_C": relay_c,
                "same_session_leg1": same_session_leg1,
                "single_leg_requested": single_leg,
                "single_leg_method": "leg1 child endpoint closed; no leg1-fallback" if single_leg else "normal leg1 and fallback relays",
                "leg1_endpoint_port": blocked_leg1_port,
                "leg1_fallback_configured": not single_leg,
                "single_leg_enforced": bool(single_leg and relay_b["connect_count"] == 0 and relay_c["connect_count"] == 0 and not same_session_leg1),
            }
        )
        return result
    finally:
        if process is not None:
            stop_process(process)
        for relay in relays:
            relay.close()


def native_exact_repeat(
    binary: Path,
    server_log: FIXTURE.Collector,
    temp: Path,
    size_mib: int,
    runs: int = 3,
) -> dict[str, object]:
    """Exercise repeated exact-N uploads through one Native process."""
    relays = [CountingRelay(name) for name in ("A", "B", "C")]
    process = None
    records: list[dict[str, object]] = []
    before = len(server_log.snapshot())
    try:
        config = temp / f"native-exact-repeat-{size_mib}.yaml"
        config.write_text(native_yaml(relays, 65536, 1), encoding="utf-8")
        process = start_mihomo(binary, config, temp / f"native-profile-exact-repeat-{size_mib}", NATIVE_PORT, temp)
        for run in range(1, runs + 1):
            result = exact_upload(("127.0.0.1", NATIVE_PORT), size_mib * 1024 * 1024, timeout=180)
            result["run"] = run
            records.append(result)
        return {
            "size_mib": size_mib,
            "runs_requested": runs,
            "runs": records,
            "process_alive": process.poll() is None,
            "relay_A": relays[0].snapshot(),
            "relay_B": relays[1].snapshot(),
            "relay_C": relays[2].snapshot(),
            "same_session_leg1": session_join(server_log.snapshot(), before),
        }
    finally:
        if process is not None:
            stop_process(process)
        for relay in relays:
            relay.close()


def generic_case(binary: Path, temp: Path) -> dict[str, object]:
    config = temp / "generic.yaml"
    config.write_text(f"mixed-port: {GENERIC_PORT}\nmode: rule\nlog-level: warning\nrules:\n  - MATCH,DIRECT\n", encoding="utf-8")
    process = start_mihomo(binary, config, temp / "generic-profile", GENERIC_PORT, temp)
    try:
        result = exact_upload(("127.0.0.1", GENERIC_PORT), 256 * 1024 * 1024)
        result.update({"mode": "generic-mihomo-direct", "process_alive": process.poll() is None})
        return result
    finally:
        stop_process(process)


def core_server_case(server_binary: Path, client_binary: Path, temp: Path) -> dict[str, object]:
    server_config_path = temp / "control-server.json"
    server_config_path.write_text(json.dumps(config_server()), encoding="utf-8")
    server = run_process([str(server_binary), "-c", str(server_config_path)], temp)
    server_log = FIXTURE.Collector(server)
    upstream = SimpleSocks()
    client_config = {"listen": f"127.0.0.1:{CORE_CLIENT_PORT}", "upstream_socks": {"address": f"127.0.0.1:{upstream.address[1]}"}, "smp3": {"password": SECRET, "carrier_ready_timeout": "2s", "routes": {"leg0": f"127.0.0.1:{SIDECAR_PORTS[0]}", "leg1": f"127.0.0.1:{SIDECAR_PORTS[1]}", "leg1_fallback": f"127.0.0.1:{SIDECAR_PORTS[2]}"}, "stream": {"activation_threshold_mbps": 1000000}, "udp": {"enabled": False}}}
    client_config_path = temp / "control-client.json"
    client_config_path.write_text(json.dumps(client_config), encoding="utf-8")
    client = run_process([str(client_binary), "-c", str(client_config_path)], temp)
    try:
        wait_tcp(("127.0.0.1", LEGACY_PORT))
        wait_tcp(("127.0.0.1", *SIDECAR_PORTS[:1]))
        result = exact_upload(("127.0.0.1", CORE_CLIENT_PORT), 256 * 1024 * 1024)
        result.update({"mode": "canonical-core-server", "process_alive": client.poll() is None, "server_process_alive": server.poll() is None})
        return result
    finally:
        stop_process(client)
        stop_process(server)
        upstream.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--native", required=True)
    parser.add_argument("--output", default="SMP3_NATIVE_256M_UPLOAD_ISOLATION_DATA.json")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    native = Path(args.native).resolve()
    metrics: dict[str, object] = {"status": "running", "branch": "qualification/standalone-sidecar-rc", "base_head": "82fe101", "results": {}}
    with tempfile.TemporaryDirectory(prefix="smp3-native-isolation-") as temp_name:
        temp = Path(temp_name)
        shutil.copy2(native, temp / "native.exe")
        server_binary = temp / "smp3-server.exe"
        client_binary = temp / "smp3-client.exe"
        subprocess.run(["go", "build", "-trimpath", "-o", str(server_binary), "./cmd/smp3-server"], cwd=root, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(client_binary), "./cmd/smp3-client"], cwd=root, check=True)
        server_config_path = temp / "server.json"
        server_config_path.write_text(json.dumps(config_server()), encoding="utf-8")
        server = run_process([str(server_binary), "-c", str(server_config_path)], temp)
        server_log = FIXTURE.Collector(server)
        try:
            for port in (LEGACY_PORT, *SIDECAR_PORTS):
                wait_tcp(("127.0.0.1", port))
            print("DIRECT_CONTROL_START", flush=True)
            direct_results = {}
            for size in (256, 512):
                sink = ExactSink(size * 1024 * 1024)
                try:
                    conn = socket.create_connection(sink.address, timeout=5)
                    written, last, error = send_repeated(conn, size * 1024 * 1024, b"D")
                    completion = False
                    if error is None:
                        try:
                            completion = recvn(conn, 3) == b"OK\n"
                        except (OSError, EOFError):
                            pass
                    conn.close()
                    direct_results[str(size)] = {"app_bytes_written": written, "target": sink.snapshot(), "completion_response": completion, "pass": completion and sink.received == size * 1024 * 1024}
                finally:
                    sink.close()
            metrics["results"]["direct_sink"] = direct_results
            metrics["results"]["generic_mihomo"] = generic_case(temp / "native.exe", temp)
            metrics["results"]["core_server"] = core_server_case(server_binary, client_binary, temp)
            metrics["results"]["native_single_leg_64"] = native_case(temp / "native.exe", server_log, temp, 64, 1000000, single_leg=True)
            metrics["results"]["native_single_leg_256"] = native_case(temp / "native.exe", server_log, temp, 256, 1000000, single_leg=True)
            metrics["results"]["native_exact_repeat_256"] = native_exact_repeat(temp / "native.exe", server_log, temp, 256)
            metrics["results"]["native_two_leg_256"] = native_case(temp / "native.exe", server_log, temp, 256, 1)
            boundary = {}
            for size in (127, 128, 129, 255, 256):
                boundary[str(size)] = native_case(temp / "native.exe", server_log, temp, size, 1)
                if not boundary[str(size)].get("pass"):
                    break
            metrics["results"]["native_boundary"] = boundary
            metrics["results"]["native_chunk_32k_256"] = native_case(temp / "native.exe", server_log, temp, 256, 1, 32768)
            metrics["status"] = "completed-isolation"
        except Exception as exc:
            metrics["status"] = "failed-isolation"
            metrics["error"] = repr(exc)
            raise
        finally:
            Path(args.output).write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            stop_process(server)
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

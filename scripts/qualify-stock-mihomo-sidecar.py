#!/usr/bin/env python3
"""Disposable localhost qualification for stock Mihomo -> SMP3 sidecar.

This is an R1 diagnostic harness. It builds the branch's standalone server and
sidecar into a temporary directory, starts the verified stock Mihomo fixture,
and uses observable SOCKS relays to prove route selection.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable


PASSWORD = "r1-localhost-test-password"
STOCK_PORT = 19080
SIDECAR_PORT = 19081
SERVER_PORTS = (19441, 19442, 19443)
LEGACY_SERVER_PORT = 19440
TCP_SIZE = 8 * 1024 * 1024


def recvn(conn: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise EOFError("unexpected EOF")
        data.extend(chunk)
    return bytes(data)


def socks_addr(host: str, port: int) -> bytes:
    try:
        ip = socket.inet_aton(host)
    except OSError:
        encoded = host.encode("idna")
        if len(encoded) > 255:
            raise ValueError("SOCKS domain is too long")
        return bytes((3, len(encoded))) + encoded + port.to_bytes(2, "big")
    return bytes((1,)) + ip + port.to_bytes(2, "big")


def socks_connect(proxy: tuple[str, int], target: tuple[str, int]) -> socket.socket:
    conn = socket.create_connection(proxy, timeout=5)
    conn.settimeout(5)
    conn.sendall(b"\x05\x01\x00")
    if recvn(conn, 2) != b"\x05\x00":
        conn.close()
        raise RuntimeError("SOCKS method negotiation failed")
    conn.sendall(b"\x05\x01\x00" + socks_addr(target[0], target[1]))
    header = recvn(conn, 4)
    if header[0] != 5 or header[1] != 0:
        conn.close()
        raise RuntimeError(f"SOCKS CONNECT failed: reply={header[1]}")
    atyp = header[3]
    if atyp == 1:
        recvn(conn, 4)
    elif atyp == 3:
        recvn(conn, recvn(conn, 1)[0])
    elif atyp == 4:
        recvn(conn, 16)
    else:
        conn.close()
        raise RuntimeError(f"invalid SOCKS reply address type {atyp}")
    recvn(conn, 2)
    return conn


def udp_packet(target: tuple[str, int], payload: bytes) -> bytes:
    return b"\x00\x00\x00" + socks_addr(target[0], target[1]) + payload


def parse_udp_packet(packet: bytes) -> tuple[tuple[str, int], bytes]:
    if len(packet) < 4 or packet[:3] != b"\x00\x00\x00":
        raise RuntimeError("invalid SOCKS UDP response header")
    atyp = packet[3]
    offset = 4
    if atyp == 1:
        host = socket.inet_ntoa(packet[offset : offset + 4])
        offset += 4
    elif atyp == 3:
        length = packet[offset]
        offset += 1
        host = packet[offset : offset + length].decode("idna")
        offset += length
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, packet[offset : offset + 16])
        offset += 16
    else:
        raise RuntimeError("invalid SOCKS UDP address type")
    port = int.from_bytes(packet[offset : offset + 2], "big")
    return (host, port), packet[offset + 2 :]


def udp_associate(proxy: tuple[str, int]) -> tuple[socket.socket, socket.socket, tuple[str, int]]:
    control = socket.create_connection(proxy, timeout=5)
    control.settimeout(5)
    control.sendall(b"\x05\x01\x00")
    if recvn(control, 2) != b"\x05\x00":
        control.close()
        raise RuntimeError("SOCKS UDP method negotiation failed")
    control.sendall(b"\x05\x03\x00" + socks_addr("0.0.0.0", 0))
    header = recvn(control, 4)
    if header[1] != 0 or header[3] != 1:
        control.close()
        raise RuntimeError(f"SOCKS UDP ASSOCIATE failed: reply={header[1]}")
    host = socket.inet_ntoa(recvn(control, 4))
    port = int.from_bytes(recvn(control, 2), "big")
    data = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data.settimeout(5)
    return control, data, (host, port)


class Collector:
    def __init__(self, process: subprocess.Popen[str]) -> None:
        self.lines: list[str] = []
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._read, args=(process,), daemon=True)
        self.thread.start()

    def _read(self, process: subprocess.Popen[str]) -> None:
        if process.stdout is None:
            return
        for line in process.stdout:
            with self.lock:
                self.lines.append(line.rstrip())

    def snapshot(self) -> list[str]:
        with self.lock:
            return list(self.lines)


class Relay:
    def __init__(self, name: str) -> None:
        self.name = name
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(64)
        self.listener.settimeout(0.5)
        self.allow = True
        self.drop = False
        self.hang = False
        self.hang_greeting = False
        self.active: set[socket.socket] = set()
        self.count = 0
        self.attempts = 0
        self.targets: list[tuple[str, int]] = []
        self.lock = threading.Lock()
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
            with self.lock:
                self.attempts += 1
                drop = self.drop
                hang = self.hang
                hang_greeting = self.hang_greeting
            if drop:
                conn.close()
                continue
            if hang_greeting:
                with self.lock:
                    self.active.add(conn)
                threading.Thread(target=self._hold, args=(conn,), daemon=True).start()
                continue
            with self.lock:
                self.active.add(conn)
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    @staticmethod
    def _hold(conn: socket.socket) -> None:
        try:
            while conn.recv(4096):
                pass
        except OSError:
            pass
        finally:
            conn.close()

    def _handle(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(5)
            version, methods = recvn(conn, 2)
            if version != 5:
                return
            recvn(conn, methods)
            conn.sendall(b"\x05\x00")
            header = recvn(conn, 4)
            if header[:3] != b"\x05\x01\x00":
                return
            atyp = header[3]
            if atyp == 1:
                host = socket.inet_ntoa(recvn(conn, 4))
            elif atyp == 3:
                host = recvn(conn, recvn(conn, 1)[0]).decode("idna")
            elif atyp == 4:
                host = socket.inet_ntop(socket.AF_INET6, recvn(conn, 16))
            else:
                return
            port = int.from_bytes(recvn(conn, 2), "big")
            target = (host, port)
            with self.lock:
                self.count += 1
                self.targets.append(target)
                allow = self.allow
                hang = self.hang
            if hang:
                while conn.recv(4096):
                    pass
                return
            if not allow:
                # A carrier blackhole is modeled as an immediate TCP failure.
                # This avoids exercising any implementation-specific retry
                # policy inside the stock SOCKS outbound itself.
                conn.close()
                return
            upstream = socket.create_connection(target, timeout=5)
            upstream.settimeout(None)
            conn.settimeout(None)
            conn.sendall(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x01")
            bridge(conn, upstream)
        except (OSError, EOFError):
            return
        finally:
            # The handler removes the socket from the active set so an
            # in-flight carrier can be failed deterministically by the RC
            # harness.
            with self.lock:
                self.active.discard(conn)
            try:
                conn.close()
            except OSError:
                pass

    def close(self) -> None:
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=1)

    def offline(self) -> None:
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=1)
        with self.lock:
            active = list(self.active)
        for conn in active:
            conn.close()


def bridge(left: socket.socket, right: socket.socket) -> None:
    sockets = [left, right]
    try:
        while True:
            readable, _, _ = select.select(sockets, [], [], 0.5)
            if not readable:
                continue
            for source in readable:
                target = right if source is left else left
                data = source.recv(64 * 1024)
                if not data:
                    return
                target.sendall(data)
    except (OSError, ValueError):
        return
    finally:
        try:
            right.close()
        except OSError:
            pass


class TCPFixture:
    def __init__(self, mode: str, size: int) -> None:
        self.mode = mode
        self.size = size
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
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
            if self.mode == "download":
                conn.sendall(b"D" * self.size)
                time.sleep(10)
            else:
                while True:
                    data = conn.recv(64 * 1024)
                    if not data:
                        return
                    conn.sendall(data)
        except OSError:
            return
        finally:
            conn.close()

    def close(self) -> None:
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=1)


class UDPFixture:
    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(0.5)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    @property
    def address(self) -> tuple[str, int]:
        return self.socket.getsockname()

    def _loop(self) -> None:
        while not self.stop.is_set():
            try:
                data, peer = self.socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            self.socket.sendto(data, peer)

    def close(self) -> None:
        self.stop.set()
        self.socket.close()
        self.thread.join(timeout=1)


def wait_until(predicate: Callable[[], bool], timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise RuntimeError(f"timeout waiting for {description}")


def wait_tcp(address: tuple[str, int], timeout: float = 10) -> None:
    def ready() -> bool:
        try:
            conn = socket.create_connection(address, timeout=0.2)
            conn.close()
            return True
        except OSError:
            return False

    wait_until(ready, timeout, f"TCP listener {address}")


def write_config(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def start_process(command: list[str], cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def session_join_evidence(lines: list[str], start: int) -> bool:
    created: set[str] = set()
    joined: set[str] = set()
    for line in lines[start:]:
        if "multipath session created" in line and "leg=0" in line:
            for part in line.split():
                if part.startswith("session="):
                    created.add(part.split("=", 1)[1])
        if "multipath leg joined/rejoined" in line and "leg=1" in line:
            for part in line.split():
                if part.startswith("session="):
                    joined.add(part.split("=", 1)[1])
    return bool(created & joined)


def tcp_case(proxy: tuple[str, int], fixture: TCPFixture, upload: bool) -> None:
    conn = socks_connect(proxy, fixture.address)
    try:
        conn.settimeout(20)
        if upload:
            payload = b"U" * TCP_SIZE
            conn.sendall(payload)
            got = recvn(conn, len(payload))
            if got != payload:
                raise RuntimeError("upload echo mismatch")
            # Keep the logical stream open beyond the Core's minimum activation
            # sampling interval so this small TX qualification is deterministic.
            time.sleep(2.0)
        else:
            got = recvn(conn, fixture.size)
            if got != b"D" * fixture.size:
                raise RuntimeError("download payload mismatch")
            # Keep the SOCKS session alive long enough for the bounded carrier
            # timeout to expire in the fallback qualification case.
            time.sleep(8.0)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", required=True, help="verified stock Mihomo executable")
    args = parser.parse_args()
    stock = Path(args.stock).resolve()
    if not stock.is_file():
        raise SystemExit("stock executable not found")

    with tempfile.TemporaryDirectory(prefix="smp3-stock-sidecar-r1-") as temp_name:
        temp = Path(temp_name)
        build = temp / "build"
        build.mkdir()
        root = Path(__file__).resolve().parents[1]
        server_bin = build / ("smp3-server.exe" if os.name == "nt" else "smp3-server")
        client_bin = build / ("smp3-client.exe" if os.name == "nt" else "smp3-client")
        subprocess.run(["go", "build", "-trimpath", "-o", str(server_bin), "./cmd/smp3-server"], cwd=root, check=True)
        subprocess.run(["go", "build", "-trimpath", "-o", str(client_bin), "./cmd/smp3-client"], cwd=root, check=True)

        relays = [Relay("A"), Relay("B"), Relay("C")]
        server_config = {
            "listen": f"127.0.0.1:{LEGACY_SERVER_PORT}",
            "sidecar_listeners": [f"127.0.0.1:{port}" for port in SERVER_PORTS],
            "password": PASSWORD,
            "udp": {"enabled": True, "mode": "adaptive", "max_datagram_size": 16384},
        }
        sidecar_config = {
            "listen": f"127.0.0.1:{SIDECAR_PORT}",
            "upstream_socks": {"address": f"127.0.0.1:{STOCK_PORT}", "connect_timeout": "2s"},
            "smp3": {
                "password": PASSWORD,
                "carrier_ready_timeout": "2s",
                "routes": {
                    "leg0": f"127.0.0.1:{SERVER_PORTS[0]}",
                    "leg1": f"127.0.0.1:{SERVER_PORTS[1]}",
                    "leg1_fallback": f"127.0.0.1:{SERVER_PORTS[2]}",
                },
                "stream": {
                    "activation_threshold_mbps": 1,
                    "activation_window": "20ms",
                    "chunk_size": 1024,
                    "queue_frames": 256,
                    "bandwidth_mbps": [128, 500],
                },
                "udp": {
                    "enabled": True,
                    "mode": "adaptive",
                    "max_datagram_size": 16384,
                    "idle_timeout": "2m",
                },
            },
        }
        stock_config = f"""mixed-port: {STOCK_PORT}
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
  - DST-PORT,{SERVER_PORTS[0]},ROUTE-A
  - DST-PORT,{SERVER_PORTS[1]},ROUTE-B
  - DST-PORT,{SERVER_PORTS[2]},ROUTE-C
  - MATCH,SMP3-SIDECAR
"""
        server_config_path = temp / "server.json"
        sidecar_config_path = temp / "sidecar.json"
        stock_config_path = temp / "stock.yaml"
        write_config(server_config_path, server_config)
        write_config(sidecar_config_path, sidecar_config)
        stock_config_path.write_text(stock_config, encoding="utf-8")

        server = start_process([str(server_bin), "-c", str(server_config_path)], temp)
        server_log = Collector(server)
        sidecar = None
        sidecar_log = None
        stock_process = None
        stock_log = None
        fixtures: list[object] = []
        try:
            for port in SERVER_PORTS:
                wait_tcp(("127.0.0.1", port))
            sidecar = start_process([str(client_bin), "-c", str(sidecar_config_path)], temp)
            sidecar_log = Collector(sidecar)
            wait_tcp(("127.0.0.1", SIDECAR_PORT))
            sidecar_check = subprocess.run([str(client_bin), "-check", "-c", str(sidecar_config_path)], cwd=temp, capture_output=True, text=True)
            if sidecar_check.returncode != 0:
                raise RuntimeError(f"sidecar config rejected: {sidecar_check.stdout}{sidecar_check.stderr}")
            print(f"SIDECAR_CONFIG_CHECK: {sidecar_check.stdout.strip()}")
            check = subprocess.run([str(stock), "-t", "-f", str(stock_config_path)], cwd=temp, capture_output=True, text=True)
            if check.returncode != 0:
                raise RuntimeError(f"stock Mihomo test config rejected: {check.stdout}{check.stderr}")
            stock_profile = temp / "stock-profile"
            stock_process = start_process([str(stock), "-d", str(stock_profile), "-f", str(stock_config_path)], temp)
            stock_log = Collector(stock_process)
            wait_tcp(("127.0.0.1", STOCK_PORT))

            route_before = [relay.count for relay in relays]
            for index, port in enumerate(SERVER_PORTS):
                conn = socks_connect(("127.0.0.1", STOCK_PORT), ("127.0.0.1", port))
                conn.close()
                wait_until(lambda index=index: relays[index].count > route_before[index], 5, f"stock route {chr(65 + index)}")
                target = relays[index].targets[-1]
                if target != ("127.0.0.1", port):
                    raise RuntimeError(f"route {chr(65 + index)} requested {target}, expected port {port}")
            print("STOCK_MIHOMO_ROUTE_A: PASS")
            print("STOCK_MIHOMO_ROUTE_B: PASS")
            print("STOCK_MIHOMO_ROUTE_C: PASS")
            print("STOCK_MIHOMO_ROUTE_DISCRIMINATION: PASS")

            before = len(server_log.snapshot())
            download = TCPFixture("download", TCP_SIZE)
            fixtures.append(download)
            started = time.monotonic()
            tcp_case(("127.0.0.1", STOCK_PORT), download, upload=False)
            elapsed = time.monotonic() - started
            print(f"STOCK_MIHOMO_SIDECAR_TCP: PASS bytes={TCP_SIZE} time={elapsed:.3f}s")
            wait_until(lambda: relays[1].count > route_before[1] + 0, 5, "stock route B for RX activation")
            wait_until(lambda: session_join_evidence(server_log.snapshot(), before), 5, "same-session leg1 join")
            print("STOCK_MIHOMO_SIDECAR_RX_ACTIVATION: PASS")
            print("LEG1_SAME_SESSION_JOIN: PASS")

            before = len(server_log.snapshot())
            upload = TCPFixture("echo", 0)
            fixtures.append(upload)
            tcp_case(("127.0.0.1", STOCK_PORT), upload, upload=True)
            wait_until(lambda: session_join_evidence(server_log.snapshot(), before), 5, "TX same-session leg1 join")
            print("STOCK_MIHOMO_SIDECAR_TX_ACTIVATION: PASS")

            # Close prior disposable logical sessions before the failure case so
            # their repair loops cannot pollute primary/fallback observations.
            sidecar.terminate()
            sidecar.wait(timeout=3)
            sidecar = start_process([str(client_bin), "-c", str(sidecar_config_path)], temp)
            sidecar_log = Collector(sidecar)
            wait_tcp(("127.0.0.1", SIDECAR_PORT))
            relays[1].hang_greeting = True
            direct_c = socks_connect(("127.0.0.1", STOCK_PORT), ("127.0.0.1", SERVER_PORTS[2]))
            direct_c.close()
            wait_until(lambda: relays[2].count >= 2, 5, "direct stock route C after B offline")
            before = len(server_log.snapshot())
            fallback_b_before = relays[1].attempts
            fallback_c_before = relays[2].count
            fallback = TCPFixture("download", TCP_SIZE)
            fixtures.append(fallback)
            tcp_case(("127.0.0.1", STOCK_PORT), fallback, upload=False)
            try:
                wait_until(lambda: any("dial ROUTE-B" in line for line in stock_log.snapshot()), 10, "failed stock route B fallback attempt")
                wait_until(lambda: relays[2].count > fallback_c_before, 20, "stock route C fallback")
            except RuntimeError:
                print(f"FALLBACK_DIAGNOSTIC_RELAY_COUNTS={[relay.count for relay in relays]}")
                print(f"FALLBACK_DIAGNOSTIC_RELAY_TARGETS={[relay.targets for relay in relays]}")
                print(f"FALLBACK_DIAGNOSTIC_SERVER_LOG={server_log.snapshot()}")
                if stock_process is not None and stock_log is not None:
                    print(f"FALLBACK_DIAGNOSTIC_STOCK_LOG={stock_process.poll()} {stock_log.snapshot()}")
                if sidecar is not None and sidecar_log is not None:
                    print(f"FALLBACK_DIAGNOSTIC_SIDECAR_LOG={sidecar.poll()} {sidecar_log.snapshot()}")
                raise
            wait_until(lambda: session_join_evidence(server_log.snapshot(), before), 5, "fallback same-session leg1 join")
            print("STOCK_MIHOMO_SIDECAR_FALLBACK: PASS")

            udp_fixture = UDPFixture()
            fixtures.append(udp_fixture)
            control, data, relay = udp_associate(("127.0.0.1", STOCK_PORT))
            try:
                received = 0
                for index in range(64):
                    payload = f"r1-udp-{index:03d}".encode()
                    data.sendto(udp_packet(udp_fixture.address, payload), relay)
                    response, _ = data.recvfrom(2048)
                    _, got = parse_udp_packet(response)
                    if got != payload:
                        raise RuntimeError("stock Mihomo UDP payload mismatch")
                    received += 1
                if received != 64:
                    raise RuntimeError(f"stock UDP received {received}/64")
            finally:
                data.close()
                control.close()
            print("STOCK_MIHOMO_SIDECAR_UDP: PASS sent=64 received=64 lost=0 bad=0")

            total_relays = sum(relay.count for relay in relays)
            if total_relays > 32:
                raise RuntimeError(f"unexpected recursive relay storm: total connections={total_relays}")
            print(f"SIDECAR_LOOP_PREVENTION: PASS relay_connections={total_relays}")
            print(f"A_CONNECT_COUNT={relays[0].count}")
            print(f"B_CONNECT_COUNT={relays[1].count}")
            print(f"C_CONNECT_COUNT={relays[2].count}")
            return 0
        finally:
            for fixture in fixtures:
                fixture.close()  # type: ignore[union-attr]
            for process in (stock_process, sidecar, server):
                if process is not None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
            for relay in relays:
                relay.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STOCK_MIHOMO_QUALIFICATION_ERROR: {exc}", file=sys.stderr)
        raise

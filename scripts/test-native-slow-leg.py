#!/usr/bin/env python3
"""Loopback-only Native Mihomo + real server downlink fault experiment.

No route/firewall/tc changes. Two local SOCKS carriers limit only their own
server->client connections. Upload/ACK direction remains unthrottled. All
children are terminated by PID in finally; no installation or deployment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import select
import subprocess
import threading
import time
import urllib.request


def listener():
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    return sock


def port():
    sock = listener()
    value = sock.getsockname()[1]
    sock.close()
    return value


def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        data = sock.recv(count - len(result))
        if not data:
            raise EOFError("truncated socket")
        result.extend(data)
    return bytes(result)


def socks_connect(sock, target):
    sock.sendall(b"\x05\x01\x00")
    if exact(sock, 2) != b"\x05\x00":
        raise RuntimeError("SOCKS auth failed")
    sock.sendall(b"\x05\x01\x00\x01" + socket.inet_aton("127.0.0.1") + target.to_bytes(2, "big"))
    header = exact(sock, 4)
    if header[1] != 0:
        raise RuntimeError(f"SOCKS connect failed: {header!r}")
    length = {1: 4, 4: 16}.get(header[3])
    if length is None:
        length = exact(sock, 1)[0]
    exact(sock, length + 2)


class Carrier:
    def __init__(self, target):
        self.listener = listener()
        self.target = target
        self.rate = 100_000_000
        self.stop = threading.Event()
        self.connections = 0

        self.disabled = False
        self.sockets = []
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(conn,), daemon=True).start()

    def serve(self, client):
        try:
            header = exact(client, 2)
            methods = exact(client, header[1])
            if header[0] != 5 or 0 not in methods:
                raise RuntimeError("SOCKS methods")
            client.sendall(b"\x05\x00")
            header = exact(client, 4)
            if header != b"\x05\x01\x00\x01":
                raise RuntimeError("fixture requires loopback IPv4")
            host = socket.inet_ntoa(exact(client, 4))
            target = int.from_bytes(exact(client, 2), "big")
            if (host, target) != ("127.0.0.1", self.target):
                raise RuntimeError("fixture refuses non-test destinations")
            if self.disabled:
                client.sendall(b"\x05\x05\x00\x01\x7f\x00\x00\x01\x00\x00")
                client.close()
                return
            upstream = socket.create_connection((host, target))
            self.connections += 1
            self.sockets.extend([client, upstream])
            # Windows timer jitter needs one 64KiB source-unit credit; Linux
            # retains its independently calibrated 16KiB relay quantum.
            for sock in (client, upstream):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536 if os.name=="nt" else 32768)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536 if os.name=="nt" else 32768)
            client.sendall(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x00")
            threading.Thread(target=self.copy, args=(client, upstream, False), daemon=True).start()
            self.copy(upstream, client, True)
        except (OSError, EOFError):
            client.close()

    def wait_paced(self, seconds):
        if os.name != "nt":
            return self.stop.wait(seconds)
        # Event.wait uses coarse Windows timeout ticks. Python 3.13 sleep uses
        # a high-resolution waitable timer; bound cancellation polling to 20ms.
        until=time.monotonic()+seconds
        while not self.stop.is_set():
            remaining=until-time.monotonic()
            if remaining<=0:return False
            time.sleep(min(remaining,.02))
        return True

    def copy(self, source, target, paced):
        try:
            deadline = time.monotonic()
            relay_unit = 65536 if os.name=="nt" else 16384
            while not self.stop.is_set():
                data = source.recv(relay_unit)
                if not data:
                    break
                if paced and os.name == "nt":
                    # Only drain immediately readable bytes up to the same relay
                    # quantum. No timeout, extra pacing credit or payload loss.
                    while len(data) < relay_unit and select.select([source], [], [], 0)[0]:
                        part = source.recv(relay_unit - len(data))
                        if not part:
                            break
                        data += part
                if paced:
                    while self.rate == 0 and not self.stop.wait(.05):
                        pass
                    if self.stop.is_set():
                        break
                    # Retain cumulative timer debt; late credit is bounded to one
                    # fixed relay unit, including tiny framing-header reads.
                    deadline = max(deadline, time.monotonic() - relay_unit * 8 / self.rate) + len(data) * 8 / self.rate
                    if self.wait_paced(max(0, deadline - time.monotonic())):
                        break
                target.sendall(data)
        except OSError:
            pass
        finally:
            source.close()
            target.close()

    def close(self):
        self.stop.set()
        self.listener.close()
        for sock in self.sockets:
            sock.close()


def wait_port(value, children):
    end = time.monotonic() + 15
    while time.monotonic() < end:
        if any(child.poll() is not None for child in children):
            raise RuntimeError("child exited; inspect process logs")
        try:
            with socket.create_connection(("127.0.0.1", value), .2):
                return
        except OSError:
            time.sleep(.05)
    raise TimeoutError("listener startup")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mihomo", type=Path)
    parser.add_argument("--standalone-client", type=Path, help="run the real cmd/smp3-client process instead of Native Mihomo")
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fault", choices=["slow", "blackhole", "none"], default="slow")
    parser.add_argument("--carrier-rates", type=int, nargs=2, default=[100_000_000,100_000_000], help="local forward Carrier rates in bits/s")
    # Calibrated 200 Mbps recovery can finish 1 GiB before the fixed 55-65s
    # observation window. Extend payload, never move the acceptance window.
    parser.add_argument("--bytes", type=int, default=2 << 30)
    parser.add_argument("--startup-preferred", action="store_true", help="explicitly bootstrap both healthy legs; retain first-ready failures separately")
    parser.add_argument("--single", action="store_true", help="leave the second local SOCKS carrier unavailable for an actual single-leg baseline")
    parser.add_argument("--single-leg", choices=["0", "1"], help="leave the other local SOCKS carrier unavailable")
    parser.add_argument("--fault-end", type=float, default=35, help="seconds after first response before restoring leg1; fault starts at 5s")
    parser.add_argument("--production-profile", action="store_true", help="sanitized supplied queue/prior/startup settings with loopback SOCKS carriers")
    parser.add_argument("--legacy-server", action="store_true", help="allow unavailable new transport telemetry; EOF/content still mandatory")
    args = parser.parse_args()
    if bool(args.mihomo) == bool(args.standalone_client):
        parser.error("select exactly one of --mihomo or --standalone-client")
    if args.single and args.single_leg is not None:
        parser.error("use only one of --single or --single-leg")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    server_port, native_port, telemetry_port = port(), port(), port()
    carriers = [Carrier(server_port), Carrier(server_port)]
    for carrier, rate in zip(carriers,args.carrier_rates):
        if rate <= 0: raise ValueError("carrier rates must be positive")
        carrier.rate = rate
    target = listener()
    content = bytes(range(256)) * 256
    expected = hashlib.sha256()
    for offset in range(0, args.bytes, len(content)):
        expected.update(content[:min(len(content), args.bytes - offset)])

    def serve_target():
        try:
            conn, _ = target.accept()
            with conn:
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    part=conn.recv(1024)
                    if not part:return
                    request.extend(part)
                conn.sendall(f"HTTP/1.1 200 OK\r\nContent-Length: {args.bytes}\r\nConnection: close\r\n\r\n".encode())
                for offset in range(0, args.bytes, len(content)):
                    conn.sendall(content[:min(len(content), args.bytes - offset)])
        except OSError:
            # Closing the loopback listener after a child startup error must
            # not print from a daemon thread during Python finalization.
            return

    threading.Thread(target=serve_target, daemon=True).start()
    secret = "SMP3_LOOPBACK_TEST_ONLY"
    config = {"listen": f"127.0.0.1:{server_port}", "password": secret,
              "stream": {"scheduler_mode": "aggregation", "capacity_mode": "dynamic",
                         "activation_threshold_mbps": 0, "bandwidth_mbps": [100, 100],
                         "chunk_size": 65536, "queue_frames": 8, "max_inflight_frames": 128,
                         "max_reorder_frames": 256, "ack_interval": "5ms", "retransmit_timeout": "1s"},
              "telemetry": {"enabled": True, "listen": f"127.0.0.1:{telemetry_port}"}}
    if args.standalone_client:
        # The real Standalone client uses the authenticated sidecar READY
        # handshake, so target the server's explicit sidecar listener. Keep
        # the server's Native listener on a separate loopback port.
        config["listen"] = f"127.0.0.1:{port()}"
        config["sidecar_listeners"] = [f"127.0.0.1:{server_port}"]
    server_config = args.output / "server.json"
    if args.production_profile:
        config["recovery_timeout"] = "15s"
        config["stream"].update(activation_threshold_mbps=80, activation_window="1s", bandwidth_mbps=[128,500], queue_frames=256,
                                max_inflight_frames=1024, max_reorder_frames=4096, ack_interval="20ms", retransmit_timeout="1500ms")
    if not args.production_profile:
        config["stream"]["bandwidth_mbps"] = [rate//1_000_000 for rate in args.carrier_rates]
    server_config.write_text(json.dumps(config, indent=2))
    yaml = f"mixed-port: {native_port}\nallow-lan: false\nmode: rule\nlog-level: warning\nproxies:\n"
    for index, carrier in enumerate(carriers):
        yaml += f"  - name: carrier{index}\n    type: socks5\n    server: 127.0.0.1\n    port: {carrier.listener.getsockname()[1]}\n"
    yaml += f"""  - name: MP
    type: smp3
    server: 127.0.0.1
    port: {server_port}
    password: {secret}
    legs:
      - proxy: carrier0
      - proxy: carrier1
    scheduler-mode: aggregation
    capacity-mode: dynamic
    bandwidth-mbps: [100, 100]
    chunk-size: 65536
    queue-frames: 8
    max-inflight-frames: 128
    max-reorder-frames: 256
    activation-threshold-mbps: 0
    ack-interval: 5ms
    retransmit-timeout: 1s
rules:
  - MATCH,MP
"""
    native_config = args.output / "native.yaml"
    if args.production_profile:
        for old,new in [("activation-threshold-mbps: 0","activation-threshold-mbps: 80"),("bandwidth-mbps: [100, 100]","bandwidth-mbps: [128, 500]"),
                        ("queue-frames: 8","queue-frames: 256"),("max-inflight-frames: 128","max-inflight-frames: 1024"),
                        ("max-reorder-frames: 256","max-reorder-frames: 4096"),("ack-interval: 5ms","ack-interval: 20ms"),("retransmit-timeout: 1s","retransmit-timeout: 1500ms")]:
            yaml=yaml.replace(old,new)
        yaml=yaml.replace("rules:\n","    activation-window: 10s\n    recovery-timeout: 15s\n    startup-policy: preferred\n    startup-preferred-leg: 0\n    startup-grace: 500ms\nrules:\n")
    if not args.production_profile:
        yaml = yaml.replace("bandwidth-mbps: [100, 100]",f"bandwidth-mbps: {[rate//1_000_000 for rate in args.carrier_rates]}")
    if args.startup_preferred and not args.production_profile:
        yaml=yaml.replace("rules:\n","    startup-policy: preferred\n    startup-preferred-leg: 0\n    startup-grace: 500ms\nrules:\n")
    if not args.standalone_client:
        native_config.write_text(yaml)
    standalone_config = None
    if args.standalone_client:
        standalone_config = args.output / "standalone-client.json"
        client_config = {
            "listen": f"127.0.0.1:{native_port}",
            "upstream_socks": {
                "connect_timeout": "2s",
                "leg0": {"address": f"127.0.0.1:{carriers[0].listener.getsockname()[1]}"},
                "leg1": {"address": f"127.0.0.1:{carriers[1].listener.getsockname()[1]}"},
            },
            "smp3": {
                "password": secret,
                "carrier_ready_timeout": "5s",
                "routes": {"leg0": f"127.0.0.1:{server_port}", "leg1": f"127.0.0.1:{server_port}"},
                "stream": {
                    "scheduler_mode": "aggregation",
                    "capacity_mode": "dynamic",
                    "activation_mode": "legacy",
                    "startup_policy": "preferred",
                    "startup_preferred_leg": 0,
                    "startup_grace": "500ms",
                    "activation_threshold_mbps": 80,
                    "activation_window": "10s",
                    "chunk_size": 65536,
                    "queue_frames": 256,
                    "bandwidth_mbps": [128, 500],
                    "max_reorder_frames": 4096,
                    "max_inflight_frames": 1024,
                    "ack_interval": "20ms",
                    "retransmit_timeout": "1500ms",
                    "recovery_timeout": "15s",
                    "redial_interval": "1s",
                },
                "udp": {"enabled": False},
            },
        }
        standalone_config.write_text(json.dumps(client_config, indent=2))
    single_leg = 0 if args.single else (int(args.single_leg) if args.single_leg is not None else None)
    if single_leg is not None:
        carriers[1 - single_leg].disabled = True
    children, logs = [], []
    stop_monitor = threading.Event()
    received, samples, telemetry = [0], [], []
    start = [0.0]
    ready = args.output / "native.ready"
    ready.unlink(missing_ok=True)
    engine_name = "standalone-client" if args.standalone_client else "native"
    engine_binary = args.standalone_client or args.mihomo
    engine_argv = (["-c", str(standalone_config)] if args.standalone_client else
                   ["-post-up", ('echo READY>native.ready' if os.name=="nt" else f"touch {shlex.quote(str(ready.resolve()))}"), "-d", str(args.output / "mihomo-home"), "-f", str(native_config)])
    try:
        for binary, argv, name in [(args.server, ["-c", str(server_config)], "server"),
                                   (engine_binary, engine_argv, engine_name)]:
            log = (args.output / f"{name}.log").open("w")
            logs.append(log)
            children.append(subprocess.Popen([str(binary.resolve()), *argv], cwd=args.output, stdout=log, stderr=log, creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)))
        wait_port(server_port, children)
        wait_port(native_port, children)
        if not args.standalone_client:
            # The pinned main.go runs post-up after ApplyConfig/OnRunning.
            # Listening can precede this callback.
            ready_deadline = time.monotonic() + 10
            while not ready.exists():
                if any(child.poll() is not None for child in children) or time.monotonic() >= ready_deadline:
                    raise RuntimeError("Native configuration did not become ready")
                time.sleep(.02)
        sock = socket.create_connection(("127.0.0.1", native_port))
        sock.settimeout(120)
        socks_connect(sock, target.getsockname()[1])
        sock.sendall(b"GET /deterministic HTTP/1.1\r\nHost: localhost\r\n\r\n")
        header = bytearray()
        while not header.endswith(b"\r\n\r\n"):
            header.extend(exact(sock, 1))
        if not header.startswith(b"HTTP/1.1 200"):
            raise RuntimeError("target response")
        start[0] = time.monotonic()

        def monitor():
            last = 0
            while not stop_monitor.wait(1):
                elapsed = time.monotonic() - start[0]
                carriers[1].rate = args.carrier_rates[1] if args.fault == "none" or elapsed < 5 or elapsed >= args.fault_end else (0 if args.fault == "blackhole" else 1_000_000)
                now = received[0]
                sample = {"seconds": elapsed, "bytes": now, "goodput_mbps": (now - last) * 8 / 1e6, "leg1_bps": carriers[1].rate}
                sample["process_resources"] = {}
                for name, child in zip(["server", engine_name], children):
                    process = Path(f"/proc/{child.pid}")
                    try:
                        fields = dict(line.split(":", 1) for line in (process / "status").read_text().splitlines() if ":" in line)
                        sample["process_resources"][name] = {"rss_kib": int(fields["VmRSS"].split()[0]),
                            "os_threads": int(fields["Threads"]), "fds": len(list((process / "fd").iterdir()))}
                    except (OSError, KeyError):
                        pass
                samples.append(sample)
                last = now
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{telemetry_port}/api/v1/sessions", timeout=.5) as response:
                        sessions = json.load(response)
                    details = []
                    for row in sessions.get("items", []):
                        with urllib.request.urlopen(f"http://127.0.0.1:{telemetry_port}/api/v1/sessions/{row['display_session_id']}", timeout=.5) as response:
                            details.append(json.load(response))
                    telemetry.append({"seconds": elapsed, "sessions": details})
                except (OSError, ValueError):
                    pass
                print(json.dumps(sample), flush=True)

        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        actual = hashlib.sha256()
        while received[0] < args.bytes:
            data = sock.recv(min(65536, args.bytes - received[0]))
            if not data:
                raise EOFError("application connection ended before complete payload")
            actual.update(data)
            received[0] += len(data)
        elapsed = time.monotonic() - start[0]
        if sock.recv(1) != b"":
            raise RuntimeError("unexpected bytes after complete HTTP body")
        stop_monitor.set()
        monitor_thread.join(2)
        sock.close()
        final_transport = None
        final_telemetry_error = None
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if telemetry and telemetry[-1]["sessions"]:
                display_id = telemetry[-1]["sessions"][0]["session"]["display_session_id"]
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{telemetry_port}/api/v1/sessions/{display_id}", timeout=.5) as response:
                        final_response = json.load(response)
                        final_transport = final_response["session"].get("stream_transport")
                        if args.legacy_server and isinstance(final_transport, dict) and not {"ordinary_outstanding_frames", "pending_frames"}.issubset(final_transport):
                            # Preserve the complete old response; missing gauges
                            # cannot establish terminal ledger drain.
                            (args.output / "legacy-final-response.json").write_text(json.dumps(final_response, indent=2))
                            final_telemetry_error = "legacy transport lacks terminal ledger/queue gauges"
                            final_transport = None
                            break
                except (OSError, ValueError) as error:
                    # Legacy may remove a completed session before the final read.
                    # Record missing telemetry; content + EOF remain mandatory.
                    final_telemetry_error = repr(error)
                    break
                if final_transport and final_transport["ordinary_outstanding_frames"] == [0, 0] and final_transport["pending_frames"] == [0, 0]:
                    break
            time.sleep(.05)
        drain_pass = final_transport is not None and final_transport["ordinary_outstanding_frames"] == [0, 0] and final_transport["pending_frames"] == [0, 0]
        expected_connections = [1, 1]
        if single_leg is not None:
            expected_connections[1 - single_leg] = 0
        result = {"native": args.standalone_client is None, "host": ("standalone" if args.standalone_client else "native"), "fault": args.fault, "fault_start": (None if args.fault=="none" else 5), "fault_end": (None if args.fault=="none" else args.fault_end), "carrier_rates_bps":args.carrier_rates, "single_leg": single_leg, "received": received[0], "elapsed": elapsed,
                  "sha256": actual.hexdigest(), "expected_sha256": expected.hexdigest(),
                  "payload_pass": actual.digest() == expected.digest(), "carrier_connections": [c.connections for c in carriers],
                  "binaries": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [args.server, engine_binary]},
                  "samples": samples, "telemetry": telemetry, "drain_pass": drain_pass, "final_transport": final_transport, "final_telemetry_error": final_telemetry_error}
        (args.output / "result.json").write_text(json.dumps(result, indent=2))
        if args.legacy_server and final_transport is None:
            result["drain_pass"] = None
            result["drain_status"] = "NOT AVAILABLE: legacy transport telemetry; complete body and EOF verified"
            (args.output / "result.json").write_text(json.dumps(result, indent=2))
        if not result["payload_pass"] or (not drain_pass and not (args.legacy_server and final_transport is None)) or result["carrier_connections"] != expected_connections or (elapsed < 30 if args.fault=="none" else elapsed <= args.fault_end):
            raise RuntimeError("content/session/fault-and-recovery coverage failed")
        print(json.dumps({k: v for k, v in result.items() if k not in ["samples", "telemetry"]}), flush=True)
    finally:
        stop_monitor.set()
        for carrier in carriers:
            carrier.close()
        target.close()
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()

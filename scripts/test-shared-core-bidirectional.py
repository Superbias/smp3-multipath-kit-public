#!/usr/bin/env python3
"""Exercise real Native and Standalone clients with upload, download, and echo.

All endpoints are local loopback processes. Supplied binaries are used as-is;
this runner neither builds nor installs binaries.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "scripts" / "benchmark-sidecar-rc.py"
SPEC = importlib.util.spec_from_file_location("shared_core_local_bench", BENCH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot import local benchmark helpers")
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def exercise_mode(mode, relays, size: int) -> dict[str, object]:
    results: dict[str, object] = {"status": "running", "cases": {}}
    for label, fixture_mode, upload in (("upload_echo", "echo", True), ("download", "download", False)):
        fixture = BENCHMARK.StreamingFixture(fixture_mode, size, hold=0.2)
        try:
            started = time.perf_counter()
            BENCHMARK.tcp_transfer(mode.proxy, fixture, upload=upload)
            elapsed = time.perf_counter() - started
            results["cases"][label] = {"pass": True, "bytes": size, "elapsed_seconds": elapsed, "fixture": fixture.snapshot()}
        except Exception as error:
            results["cases"][label] = {"pass": False, "error": repr(error), "fixture": fixture.snapshot()}
        finally:
            fixture.close()

    started = time.perf_counter()
    try:
        elapsed = BENCHMARK.bidirectional_case(mode.proxy)
        results["cases"]["simultaneous_upload_and_echo"] = {"pass": True, "bytes_each_direction": 16 * 1024 * 1024, "elapsed_seconds": elapsed}
    except Exception as error:
        results["cases"]["simultaneous_upload_and_echo"] = {"pass": False, "error": repr(error), "elapsed_seconds": time.perf_counter() - started}

    results["proxy"] = list(mode.proxy)
    results["relay_connections"] = {relay.name: relay.count for relay in relays}
    results["logs"] = [collector.snapshot() for collector in mode.logs]
    results["status"] = "PASS" if all(case.get("pass") for case in results["cases"].values()) else "FAIL"
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", required=True, type=Path)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--bytes", type=int, default=16 * 1024 * 1024)
    args = parser.parse_args()
    native, client, server_bin = (path.resolve() for path in (args.native, args.client, args.server))
    for path in (native, client, server_bin):
        if not path.is_file():
            parser.error(f"binary not found: {path}")
    if args.bytes <= 0:
        parser.error("--bytes must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)

    binaries = {"native_sha256": sha256(native), "standalone_client_sha256": sha256(client), "standalone_server_sha256": sha256(server_bin)}
    results: dict[str, object] = {
        "status": "running",
        "scope": "actual local Native or Standalone client process and Standalone server process; loopback only",
        "bytes": args.bytes,
        "binaries": binaries,
        "harness_sha256": sha256(Path(__file__).resolve()),
        "benchmark_helper_sha256": sha256(BENCH),
        "fixture_helper_sha256": sha256(ROOT / "scripts" / "qualify-stock-mihomo-sidecar.py"),
        "modes": {},
    }
    with tempfile.TemporaryDirectory(prefix="smp3-shared-core-bidi-") as temp_name:
        temp = Path(temp_name)
        shutil.copy2(native, temp / "native.exe")
        shutil.copy2(native, temp / "stock.exe")
        shutil.copy2(client, temp / "smp3-client.exe")
        shutil.copy2(server_bin, temp / "smp3-server.exe")
        server_config = temp / "server.json"
        BENCHMARK.write_json(server_config, BENCHMARK.server_config())
        shutil.copy2(server_config, output / "server-config.json")
        server = BENCHMARK.start_process([str(temp / "smp3-server.exe"), "-c", str(server_config)], temp)
        server_log = BENCHMARK.FIXTURE.Collector(server)
        relays = [BENCHMARK.FIXTURE.Relay(label) for label in ("A", "B", "C")]
        native_mode = None
        standalone_mode = None
        try:
            for port in (BENCHMARK.LEGACY_PORT, *BENCHMARK.SIDECAR_PORTS):
                BENCHMARK.wait_tcp(("127.0.0.1", port))
            native_mode = BENCHMARK.start_mode("native", ROOT, temp, server, relays, temp / "stock.exe")
            shutil.copy2(temp / "native.yaml", output / "native-config.yaml")
            results["modes"]["native"] = exercise_mode(native_mode, relays, args.bytes)
            native_mode.stop()
            native_mode = None
            standalone_mode = BENCHMARK.start_mode("sidecar", ROOT, temp, server, relays, temp / "stock.exe")
            shutil.copy2(temp / "sidecar.json", output / "standalone-client-config.json")
            shutil.copy2(temp / "stock.yaml", output / "stock-proxy-config.yaml")
            results["modes"]["standalone"] = exercise_mode(standalone_mode, relays, args.bytes)
        finally:
            if standalone_mode is not None:
                standalone_mode.stop()
            if native_mode is not None:
                native_mode.stop()
            BENCHMARK.stop_process(server)
            for relay in relays:
                relay.close()
        results["server_log"] = server_log.snapshot()
        results["relay_connections_final"] = {relay.name: relay.count for relay in relays}

    results["status"] = "PASS" if all(mode["status"] == "PASS" for mode in results["modes"].values()) else "FAIL"
    (output / "result.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return 0 if results["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

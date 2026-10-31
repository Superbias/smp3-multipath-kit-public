#!/usr/bin/env python3
"""Serial final verification; Linux/WSL, isolated binaries and loopback only.

Retains every command/exit code. No deployment, installer, git reset/clean or
golden refresh. Runtime and performance jobs run serially, outside race.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


def main():
    root = Path(__file__).resolve().parents[1]
    output = root / "artifacts/slow-leg"
    output.mkdir(parents=True, exist_ok=True)
    ledger = []

    def run(name, argv, cwd=root, extra=None):
        started = time.monotonic()
        with (output / f"{name}.log").open("w") as log:
            code = subprocess.run(argv, cwd=cwd, env=dict(os.environ, **(extra or {})), stdout=log, stderr=subprocess.STDOUT).returncode
        entry = {"name": name, "command": argv, "cwd": str(cwd), "env": extra or {}, "exit_code": code,
                 "seconds": time.monotonic() - started, "result": "PASS" if code == 0 else "FAIL"}
        ledger.append(entry)
        (output / "verification-ledger.json").write_text(json.dumps(ledger, indent=2))
        print(json.dumps(entry), flush=True)
        return code

    modules = ["./core/...", "./client/...", "./server/...", "./cmd/smp3-client/...", "./cmd/smp3-server/..."]
    run("final-tests", ["go", "test", *modules])
    run("final-vet", ["go", "vet", *modules])
    run("final-race", ["go", "test", "-race", *modules])
    run("legacy-race", ["python3", "scripts/run-standalone-go.py", str(root), "test", "--enable-cgo", "-race", "-count=5"])
    run("final-unit-repeat", ["go", "test", "./core/...", "./server/...", "-run", "TestSlowLeg(NoACK|Admission|Healthy|SharedProvider|LowRate|Idle|Budget|RescueACK|Bounded|Retired|ProviderClose|ShortReads)|TestStreamTXLedgerRandomized|TestStreamTransportTelemetry", "-count=10", "-v"])
    run("final-core-imports", ["python3", "scripts/check-core-imports.py", str(root)])
    run("final-test-count", ["python3", "scripts/check-test-count.py", str(root)])
    native = root / ".work/native-build/mihomo"
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "core").glob("*.go"))}
    built = run("final-native-build", ["bash", "scripts/test-slow-leg-native-build.sh"])
    if built == 0:
        run("final-native-race", ["go", "test", "-race", "-mod=mod", "./adapter/outbound"], native, {"GOWORK": "off", "CGO_ENABLED": "1"})
        metadata = json.loads(subprocess.check_output(["go", "list", "-json", "github.com/Superbias/smp3-multipath-kit-public/smp3core"], cwd=native, env=dict(os.environ, GOWORK="off")))
        if Path(metadata["Dir"]).resolve() != (root / "core").resolve():
            raise RuntimeError("Native resolved a noncanonical Core")
        if hashes != {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "core").glob("*.go"))}:
            raise RuntimeError("Core changed during final Native build")
        proof = {"canonical_core": str(Path(metadata["Dir"]).resolve()), "runtime_files": metadata["GoFiles"], "core_sha256": hashes,
                 "mihomo_pin": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=native, text=True).strip()}
        (output / "final-native-source-proof.json").write_text(json.dumps(proof, indent=2))
        run("final-native-binary-info", ["go", "version", "-m", "artifacts/slow-leg/native/mihomo-smp3-linux-amd64"])
    server_built = run("final-server-build", ["go", "build", "-o", "artifacts/slow-leg/smp3-server", "./cmd/smp3-server"])
    if built == 0 and server_built == 0:
        for name, args in [("single", ["--single"]), ("slow", ["--fault", "slow"]), ("blackhole", ["--fault", "blackhole"])]:
            run(f"final-native-process-{name}", ["python3", "scripts/test-native-slow-leg.py", "--mihomo", "artifacts/slow-leg/native/mihomo-smp3-linux-amd64", "--server", "artifacts/slow-leg/smp3-server", "--output", f"artifacts/slow-leg/final-native-{name}", *args])
    run("final-performance", ["go", "test", "./core/...", "-run", "TestSlowLegReliableStream", "-count=3", "-v"], extra={"SMP3_SLOW_LEG_DURATION": "40s"})
    run("final-supplemental", ["go", "test", "./core/...", "-run", "TestSlowLegSupplementalMatrix", "-v"], extra={"SMP3_SLOW_LEG_MATRIX": "1"})
    run("final-shared-physical", ["go", "test", "./core/...", "-run", "TestSlowLegSharedPhysicalBudget", "-v"])
    run("final-drain", ["go", "test", "./core/...", "-run", "TestSlowLegGracefulDrainReliable", "-count=3", "-v"])

    spec = importlib.util.spec_from_file_location("compare", root / "scripts/test-slow-leg-compare.py")
    compare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(compare)
    rows = compare.results(output / "final-performance.log")
    accepted = True
    for case in ("cold_100_1", "swapped_1_100"):
        if len(rows.get(case, [])) != 3 or len(rows.get("single", [])) != 3:
            accepted = False
            continue
        for index, windows in enumerate(rows[case]):
            b = statistics.median(rows["single"][index][1:4])
            if len(windows) != 4 or any(value < .90 * b for value in windows[1:4]):
                accepted = False
    for index, windows in enumerate(rows.get("blackhole_recovery", [])):
        if windows[-1] < statistics.median(rows["single"][index][1:4]) * 1.5:
            accepted = False
    (output / "final-performance.json").write_text(json.dumps({"runs": rows, "result": "PASS" if accepted else "FAIL"}, indent=2))
    print("fixed_window_acceptance", "PASS" if accepted else "FAIL", flush=True)
    return 0 if accepted and all(entry["exit_code"] == 0 for entry in ledger) else 1


if __name__ == "__main__":
    raise SystemExit(main())

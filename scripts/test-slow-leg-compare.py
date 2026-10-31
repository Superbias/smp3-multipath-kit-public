#!/usr/bin/env python3
"""Compare frozen HEAD with the working tree using the same reliable fixture.

Run in Linux without race or concurrent build/stress jobs. All three repetitions
and all fixed 10s windows are retained. Nonzero exit means an acceptance FAIL,
not a reason to move windows or loosen thresholds.
"""
import argparse
import io
import json
from pathlib import Path
import re
import statistics
import subprocess
import tarfile
import os


def results(path):
    case = None
    rows = {}
    for line in path.read_text().splitlines():
        match = re.search(r"=== RUN\s+TestSlowLegReliableStream/(\w+)", line)
        if match:
            case = match[1]
        match = re.search(r"RESULT .*windows=\[([^]]+)\].*payload=PASS", line)
        if match and case:
            rows.setdefault(case, []).append([float(n) for n in match[1].split()])
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", default="40s")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--cases", default="single|cold_100_1|swapped_1_100|blackhole_recovery")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = root / "artifacts/slow-leg"
    output.mkdir(parents=True, exist_ok=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    baseline = root / ".work" / f"slow-leg-baseline-{commit[:12]}"
    baseline.mkdir(parents=True, exist_ok=True)
    archive = subprocess.check_output(["git", "archive", commit], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(baseline, filter="data")
    fixture = (root / "core/stream_slow_leg_integration_test.go").read_text()
    # Only additional diagnostic fields are unavailable in the baseline. Its
    # transport, rate limits, clocks, windows, payload and assertions are exact.
    for before, after in [("stats.OrdinaryAdmissionState", "([2]string{})"),
                          ("stats.OrdinaryOutstandingFrames", "stats.OutstandingFramesByLeg"),
                          ("stats.OrdinaryAssignedBytes", "stats.TxSentBytesByLeg"),
                          ("stats.LegGeneration", "([2]uint64{})"),
                          ("stats.TxWrittenBytesByLeg", "stats.TxSentBytesByLeg"),
                          ("stats.TxRetryWrittenBytesByLeg", "stats.TxRetransmitBytesByLeg"),
                          ("stats.TxAckedUsefulTotal", "(stats.TxAckedUsefulByLeg[0]+stats.TxAckedUsefulByLeg[1])")]:
        fixture = fixture.replace(before, after)
    (baseline / "core/stream_slow_leg_integration_test.go").write_text(fixture)
    env = dict(os.environ, SMP3_SLOW_LEG_DURATION=args.duration)
    command = ["go", "test", "./core/...", "-run", f"TestSlowLegReliableStream/({args.cases})$", "-v", f"-count={args.count}"]
    exits = {}
    for label, cwd in [("baseline", baseline), ("final", root)]:
        with (output / f"compare-{label}.log").open("w") as log:
            exits[label] = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    rows = {label: results(output / f"compare-{label}.log") for label in exits}
    summary = {"baseline_commit": commit, "duration": args.duration, "count": args.count,
               "command": command, "exit_codes": exits, "runs": rows, "acceptance": {}}
    for label, cases in rows.items():
        for case, runs in cases.items():
            summary["acceptance"][f"{label}/{case}"] = {"median_last_window": statistics.median(run[-1] for run in runs),
                                                       "worst_stable_window": min(n for run in runs for n in run[1:])}
    passed = exits["final"] == 0
    final = rows["final"]
    if "single" not in final or len(final["single"]) != args.count:
        passed = False
    for case in ("cold_100_1", "swapped_1_100"):
        if case not in final or len(final[case]) != args.count:
            passed = False
            continue
        for index, run in enumerate(final[case]):
            baseline_b = statistics.median(final["single"][index][1:])
            if len(run) < 4 or any(n < baseline_b * .90 for n in run[1:4]):
                passed = False
    # Recovery must provide real aggregate gain by the last fixed window.
    if "blackhole_recovery" in final:
        for index, run in enumerate(final["blackhole_recovery"]):
            if run[-1] < statistics.median(final["single"][index][1:]) * 1.5:
                passed = False
    summary["result"] = "PASS" if passed else "FAIL"
    (output / "comparison.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Loopback RTT/fault run using the real standalone client and server binaries."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtt-ms", type=float, required=True)
    parser.add_argument("--client", type=Path, required=True, help="real cmd/smp3-client binary")
    args, rest = parser.parse_known_args()
    if not 0 <= args.rtt_ms <= 400:
        parser.error("controlled local RTT range is 0..400 ms")

    script = Path(__file__).with_name("test-native-rtt.py")
    delayed = load(script, "smp3_native_rtt_runner")
    sys.argv = [str(script), "--rtt-ms", str(args.rtt_ms), *rest, "--standalone-client", str(args.client)]
    delayed.main()
    output = Path(rest[rest.index("--output") + 1])
    provenance_path = output / "rtt-tool-provenance.json"
    provenance = json.loads(provenance_path.read_text())
    provenance["standalone_wrapper_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    provenance["client_binary_sha256"] = hashlib.sha256(args.client.read_bytes()).hexdigest()
    provenance_path.write_text(json.dumps(provenance, indent=2))


if __name__ == "__main__":
    main()

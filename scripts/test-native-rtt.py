#!/usr/bin/env python3
"""Bounded delayed Carrier around the unchanged real Native process harness.

RTT delays both relay directions; pacing and content/EOF assertions remain those
of test-native-slow-leg.py. A queue pipelines the delay rather than sleeping per
chunk (which would incorrectly throttle bandwidth). All endpoints are loopback.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time

_path = Path(__file__).with_name('test-native-slow-leg.py')
_spec = importlib.util.spec_from_file_location('native_base', _path)
_base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base)


class DelayedWriter:
    def __init__(self, target, stop, delay):
        self.target, self.stop, self.delay = target, stop, delay
        self.items = queue.Queue(1024)  # <=16MiB at the Linux relay quantum
        self.error = None
        self.worker = threading.Thread(target=self.deliver, daemon=True)
        self.worker.start()

    def deliver(self):
        try:
            while not self.stop.is_set():
                try:
                    item = self.items.get(timeout=.02)
                except queue.Empty:
                    continue
                if item is None:
                    return
                data, deadline = item
                while not self.stop.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(.02, remaining))
                if self.stop.is_set():
                    return
                self.target.sendall(data)
        except OSError as error:
            self.error = error

    def sendall(self, data):
        while not self.stop.is_set():
            if self.error:
                raise self.error
            try:
                self.items.put((data, time.monotonic() + self.delay), timeout=.02)
                return
            except queue.Full:
                continue
        raise OSError('Carrier cancelled')

    def close(self):
        while self.worker.is_alive() and not self.stop.is_set():
            try:
                self.items.put(None, timeout=.02)
                break
            except queue.Full:
                continue
        self.worker.join(self.delay + 2)
        self.target.close()


class Carrier(_base.Carrier):
    rtt_ms = float(os.environ.get('SMP3_NATIVE_TEST_RTT_MS', '0'))

    def copy(self, source, target, paced):
        if self.rtt_ms:
            target = DelayedWriter(target, self.stop, self.rtt_ms / 2000)
        super().copy(source, target, paced)


# The raw calibrator imports these same relay operations without using Core.
listener = _base.listener
socks_connect = _base.socks_connect


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--rtt-ms', type=float, required=True)
    args, rest = parser.parse_known_args()
    if not 0 <= args.rtt_ms <= 400:
        raise SystemExit('Controlled local RTT range is 0..400 ms')
    Carrier.rtt_ms = args.rtt_ms
    _base.Carrier = Carrier
    sys.argv = [str(_path), *rest]
    _base.main()
    out = Path(rest[rest.index('--output') + 1])
    (out / 'rtt-tool-provenance.json').write_text(json.dumps({
        'controlled_relay_rtt_ms': args.rtt_ms,
        'scope': 'actual Native/server processes; delayed loopback relay, not production RTT',
        'base_harness_sha256': hashlib.sha256(_path.read_bytes()).hexdigest(),
        'delay_wrapper_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'queue_max_units_per_direction': 1024,
    }, indent=2))


if __name__ == '__main__':
    main()

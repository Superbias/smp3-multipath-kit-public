#!/usr/bin/env python3
"""Generate Phase 32 client and Landing JSON examples from the checked config schema."""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[2] / "packaging" / "phase32" / "configs"
root.mkdir(parents=True, exist_ok=True)
rates = {2: [100, 50], 3: [100, 50, 20], 4: [100, 50, 20, 10], 8: [100, 80, 60, 40, 30, 20, 15, 10], 16: [100, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40, 35, 30, 25]}

def client(n):
    routes = []
    upstreams = []
    for i in range(n):
        name = "direct" if i == 0 else f"realm-{i}"
        endpoint = "landing.example.net:8443" if i == 0 else f"realm-{i}.example.net:8443"
        routes.append({"id": i, "name": name, "address": endpoint})
        upstreams.append({"address": f"127.0.0.1:{7890+i}", "connect_timeout": "10s"})
    return {
        "listen": "127.0.0.1:10880",
        "log_level": "info",
        "upstream_socks": {"address": "127.0.0.1:7890", "connect_timeout": "10s", "legs": upstreams},
        "smp3": {
            "password": "REPLACE_WITH_LANDING_PASSWORD",
            "carrier_mode": "legacy_socks",
            "carrier_tls": {"enabled": True, "server_name": "landing.example.net"},
            "routes": {"legs": routes},
            "carrier_ready_timeout": "5s",
            "stream": {
                "scheduler_mode": "aggregation", "capacity_mode": "dynamic", "activation_mode": "dynamic",
                "experimental_path_aware": False, "startup_policy": "first-ready", "startup_preferred_leg": 0,
                "startup_grace": "500ms", "activation_threshold_mbps": 80, "activation_window": "1s",
                "chunk_size": 65536, "queue_frames": 256, "bandwidth_mbps": rates[n],
                "max_active_legs": n, "max_reorder_frames": 4096, "max_inflight_frames": 1024,
                "ack_interval": "20ms", "retransmit_timeout": "1500ms", "recovery_timeout": "15s", "redial_interval": "1s"
            },
            "udp": {"enabled": False, "mode": "adaptive", "queue_frames": 256, "max_datagram_size": 16384,
                    "dedup_window": 4096, "idle_timeout": "2m", "adaptive_queue_delay": "120ms", "adaptive_duplicate_threshold": 0}
        }
    }

def landing(n):
    return {
        "listen": "0.0.0.0:8443",
        "carrier_tls": {"cert_file": "/etc/smp3/tls/fullchain.pem", "key_file": "/etc/smp3/tls/privkey.pem"},
        "password": "REPLACE_WITH_LANDING_PASSWORD",
        "application_target": "127.0.0.1:9080",
        "max_sessions": 256, "max_connections": 512, "idle_timeout": "10m", "graceful_shutdown_timeout": "30s", "log_level": "info",
        "hello_read_timeout": "10s", "recovery_timeout": "15s",
        "stream": {"scheduler_mode": "aggregation", "capacity_mode": "dynamic", "activation_mode": "dynamic",
                   "experimental_path_aware": False, "activation_threshold_mbps": 80, "activation_window": "1s",
                   "chunk_size": 65536, "queue_frames": 256, "bandwidth_mbps": rates[n], "max_active_legs": n,
                   "max_reorder_frames": 4096, "max_inflight_frames": 1024, "ack_interval": "20ms", "retransmit_timeout": "1500ms"},
        "udp": {"enabled": False, "mode": "adaptive", "queue_frames": 256, "max_datagram_size": 16384, "dedup_window": 4096, "idle_timeout": "2m", "adaptive_queue_delay": "120ms", "adaptive_duplicate_threshold": 0},
        "telemetry": {"enabled": True, "data_sequence_events": False, "listen": "127.0.0.1:24500", "snapshot_interval": "1s", "max_active_sessions": 1024, "max_events": 2048, "close_grace": "5s", "accounting_path": "/var/lib/smp3/traffic-accounting.json", "accounting_timezone": "UTC", "accounting_interval": "10s"}
    }

for n in (2, 3, 4, 8):
    (root / f"client-{n}leg.example.json").write_text(json.dumps(client(n), indent=2) + "\n", encoding="utf-8")
# Compatibility aliases used by the existing install scripts.
(root / "client.example.json").write_text(json.dumps(client(2), indent=2) + "\n", encoding="utf-8")
(root / "landing.example.json").write_text(json.dumps(landing(16), indent=2) + "\n", encoding="utf-8")
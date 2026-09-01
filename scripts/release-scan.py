#!/usr/bin/env python3
"""Fail-closed scan for credentials and machine-specific paths in release content."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".work", ".release-stage", "graphify-out", ".audit-graphify"}
TEXT_SUFFIXES = {".json", ".yaml", ".yml", ".md", ".txt", ".sh", ".ps1", ".py", ".go"}
SECRET_KEYS = {"password", "psk", "obfs_password", "private_key", "token", "api_key"}
PLACEHOLDER_PREFIXES = ("YOUR_", "CHANGE_")
PRIVATE_KEY = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |PRIVATE )?PRIVATE KEY-----")
USER_PATH = re.compile(r"C:\\\\Users\\[^\\\s]+\\|/home/[^/\s]+/")


def walk_json(value: object, path: Path, findings: list[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in SECRET_KEYS and isinstance(child, str) and child:
                if not child.startswith(PLACEHOLDER_PREFIXES):
                    findings.append(f"{path}: non-placeholder {key}")
            walk_json(child, path, findings)
    elif isinstance(value, list):
        for child in value:
            walk_json(child, path, findings)


def main() -> int:
    files = []
    findings: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.name in {"package-release.sh", "release-scan.py"}:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        files.append(path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        if PRIVATE_KEY.search(text):
            findings.append(f"{path}: private-key marker")
        if USER_PATH.search(text):
            findings.append(f"{path}: user-home path")
        if path.suffix.lower() == ".json":
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                findings.append(f"{path}: invalid JSON ({exc})")
            else:
                walk_json(value, path, findings)
    print(f"SCANNED_FILES={len(files)}")
    print(f"SENSITIVE_FINDINGS={len(findings)}")
    for finding in findings:
        print(finding)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())

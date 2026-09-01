#!/usr/bin/env python3
"""Report concrete public IPv4 literals in a release tree."""

from __future__ import annotations

import ipaddress
import re
import sys
from pathlib import Path


PATTERN = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
TEXT_SUFFIXES = {".json", ".yaml", ".yml", ".md", ".txt", ".sh", ".ps1", ".py", ".go"}


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    hits: list[str] = []
    files = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        files += 1
        text = path.read_text(encoding="utf-8", errors="ignore")
        for literal in sorted(set(PATTERN.findall(text))):
            try:
                address = ipaddress.ip_address(literal)
            except ValueError:
                continue
            if address.version == 4 and address.is_global:
                hits.append(f"{path}: {literal}")
    print(f"SCANNED_FILES={files}")
    print(f"PUBLIC_IPV4_FINDINGS={len(hits)}")
    for hit in hits:
        print(hit)
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())

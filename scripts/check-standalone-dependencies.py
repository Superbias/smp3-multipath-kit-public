#!/usr/bin/env python3
"""Fail closed if Standalone production Go code imports proxy-core packages."""

from pathlib import Path
import re
import sys


PRODUCTION_ROOTS = (
    "client",
    "cmd/smp3-client",
    "core",
    "server",
    "cmd/smp3-server",
)
FORBIDDEN_PREFIXES = (
    "github.com/sagernet/",
    "github.com/metacubex/",
    "github.com/Dreamacro/",
)
IMPORT_LINE = re.compile(r'^\s*(?:[A-Za-z_]\w*\s+)?"([^"]+)"\s*$')


def production_imports(root: Path):
    for relative_root in PRODUCTION_ROOTS:
        directory = root / relative_root
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*.go")):
            if path.name.endswith("_test.go"):
                continue
            in_block = False
            for line in path.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if stripped == "import (":
                    in_block = True
                    continue
                if in_block and stripped == ")":
                    in_block = False
                    continue
                match = IMPORT_LINE.match(line)
                if match:
                    yield path, match.group(1)


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]).resolve()
    imports = list(production_imports(root))
    forbidden = [(path, package) for path, package in imports
                 if package.startswith(FORBIDDEN_PREFIXES)]
    if forbidden:
        for path, package in forbidden:
            print(f"forbidden Standalone production import {package} in {path}", file=sys.stderr)
        return 1

    packages = sorted({package for _, package in imports})
    print("STANDALONE_MIHOMO_PRODUCTION_IMPORT_COUNT: 0")
    print("STANDALONE_SING_PRODUCTION_IMPORT_COUNT: 0")
    print("STANDALONE_DEPENDENCY_GATE: PASS")
    print("STANDALONE_PRODUCTION_IMPORTS: " + ",".join(packages))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

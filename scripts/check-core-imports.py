#!/usr/bin/env python3
"""Enforce that the extracted SMP3 Core package uses only the Go stdlib."""

from pathlib import Path
import re
import sys


SINGLE_IMPORT = re.compile(r'^\s*import\s+(?:[A-Za-z_]\w*\s+)?"([^"]+)"\s*$')
BLOCK_IMPORT = re.compile(r'^\s*(?:[A-Za-z_]\w*\s+)?"([^"]+)"\s*$')
FORBIDDEN = (
    "github.com/sagernet/",
    "github.com/metacubex/",
    "github.com/Dreamacro/",
)


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
    core = root / "core"
    files = sorted(core.rglob("*.go"))
    if not files:
        print(f"core package has no Go files: {core}", file=sys.stderr)
        return 1

    imports: list[tuple[Path, str]] = []
    in_block = False
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped == "import (":
                in_block = True
                continue
            if in_block and stripped == ")":
                in_block = False
                continue
            match = BLOCK_IMPORT.match(line) if in_block else SINGLE_IMPORT.match(line)
            if match:
                imports.append((path, match.group(1)))

    errors: list[str] = []
    for path, package in imports:
        if any(marker in package for marker in FORBIDDEN):
            errors.append(f"forbidden import {package} in {path}")
        if "." in package.split("/", 1)[0]:
            errors.append(f"non-stdlib import {package} in {path}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    unique = sorted({package for _, package in imports})
    print(f"core_imports=stdlib_only files={len(files)} imports={','.join(unique)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Report mutually-exclusive Phase 3A source-tree Test* categories."""

from pathlib import Path
import re
import sys


TEST_RE = re.compile(r"^\s*func\s+(Test[A-Za-z0-9_]*)\s*\(")
ADAPTER_FILES = {
    "inbound_test.go",
    "outbound_test.go",
    "packetconn_sing_test.go",
}


def category_for(path: Path, test_dir: Path, core_dir: Path) -> str:
    try:
        path.relative_to(core_dir)
    except ValueError:
        pass
    else:
        return "smp3core"
    relative = path.relative_to(test_dir)
    if relative.as_posix() in ADAPTER_FILES:
        return "sing_adapter"
    return "legacy_semantic"


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
    test_dir = root / "src" / "protocol" / "multipath"
    core_dir = root / "core"
    test_files = sorted(test_dir.rglob("*_test.go")) + sorted(core_dir.rglob("*_test.go"))
    category_counts = {"smp3core": 0, "legacy_semantic": 0, "sing_adapter": 0}
    total = 0
    for path in test_files:
        category = category_for(path, test_dir, core_dir)
        relative = path.relative_to(core_dir if category == "smp3core" else test_dir).as_posix()
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            match = TEST_RE.match(line)
            if not match:
                continue
            name = match.group(1)
            print(f"TEST category={category} file={relative}:{line_number} name={name}")
            category_counts[category] += 1
            total += 1
    print(f"smp3core={category_counts['smp3core']}")
    print(f"legacy_semantic={category_counts['legacy_semantic']}")
    print(f"sing_adapter={category_counts['sing_adapter']}")
    print(f"total={total}")
    if sum(category_counts.values()) != total:
        print("test category sum does not equal total", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

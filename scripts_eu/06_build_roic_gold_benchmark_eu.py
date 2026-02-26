#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from eu_config import ROIC_SOURCE_NAME_DEFAULT
from eu_paths import (
    ROIC_DUMPS_DIR_DEFAULT,
    ROIC_GOLD_BENCHMARK_CSV,
    ROIC_GOLD_BENCHMARK_REPORT_JSON,
)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
US_ROIC_BUILDER = PROJECT_ROOT / "scripts" / "09_build_roic_gold_benchmark.py"


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description=(
            "Build EU ROIC gold benchmark. "
            "This wraps scripts/09_build_roic_gold_benchmark.py with data_eu defaults."
        )
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=ROIC_DUMPS_DIR_DEFAULT,
        help="Folder containing ROIC JSON dumps.",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=ROIC_GOLD_BENCHMARK_CSV,
        help="Output CSV path under data_eu/processed/benchmarks.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=ROIC_GOLD_BENCHMARK_REPORT_JSON,
        help="Output report JSON path under data_eu/processed/benchmarks.",
    )
    parser.add_argument(
        "--source-name",
        type=str,
        default=ROIC_SOURCE_NAME_DEFAULT,
        help="Source label (default roic.ai).",
    )
    return parser.parse_known_args()


def main() -> int:
    if not US_ROIC_BUILDER.exists():
        print(f"Missing script: {US_ROIC_BUILDER}")
        return 2

    args, passthrough = parse_args()
    cmd = [
        sys.executable,
        str(US_ROIC_BUILDER),
        "--in-dir",
        str(args.in_dir),
        "--out-csv",
        str(args.out_csv),
        "--report-json",
        str(args.report_json),
        "--source-name",
        str(args.source_name),
        *passthrough,
    ]
    print(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, check=False, cwd=str(PROJECT_ROOT))
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())

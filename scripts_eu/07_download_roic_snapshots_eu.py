#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from eu_config import EU_TICKERS
from eu_paths import ROIC_DUMPS_DIR_DEFAULT


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
US_ROIC_DOWNLOADER = PROJECT_ROOT / "scripts" / "10_download_roic_snapshots.py"


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description=(
            "Download ROIC statistics snapshots for EU tickers. "
            "This wraps scripts/10_download_roic_snapshots.py with EU defaults."
        )
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROIC_DUMPS_DIR_DEFAULT,
        help="Output folder for ROIC JSON dumps under data_eu.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(EU_TICKERS),
        help="Comma-separated ticker list.",
    )
    parser.add_argument(
        "--mode",
        choices=["monthly_last_year", "single"],
        default="single",
        help="single downloads today's snapshot; monthly_last_year downloads monthly snapshots.",
    )
    return parser.parse_known_args()


def main() -> int:
    if not US_ROIC_DOWNLOADER.exists():
        print(f"Missing script: {US_ROIC_DOWNLOADER}")
        return 2

    args, passthrough = parse_args()
    cmd = [
        sys.executable,
        str(US_ROIC_DOWNLOADER),
        "--out-dir",
        str(args.out_dir),
        "--tickers",
        str(args.tickers),
        "--mode",
        str(args.mode),
        *passthrough,
    ]
    print(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, check=False, cwd=str(PROJECT_ROOT))
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())

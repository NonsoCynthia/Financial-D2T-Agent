from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    ROIC_GOLD_BENCHMARK_CSV,
    ROIC_GOLD_BENCHMARK_REPORT_JSON,
    ROIC_DUMPS_DIR_DEFAULT,
    ROIC_SOURCE_NAME_DEFAULT,
    SCRIPT_PIPELINE_STEP_ORDER,
)

# Keep these in execution order for --all.
STEP_ORDER = list(SCRIPT_PIPELINE_STEP_ORDER)

STEP_SCRIPTS = {
    "download_prices": SCRIPT_DIR / "01_download_prices.py",
    "sec_map": SCRIPT_DIR / "02_sec_ticker_cik.py",
    "sec_companyfacts": SCRIPT_DIR / "03a_sec_companyfacts.py",
    "sec_filings": SCRIPT_DIR / "03b_sec_download_filings.py",
    "compute_returns": SCRIPT_DIR / "04a_compute_returns.py",
    "align_fundamentals": SCRIPT_DIR / "04b_align_fundamentals.py",
    "make_splits": SCRIPT_DIR / "05_make_splits.py",
    "monthly_panel": SCRIPT_DIR / "06_make_monthly_panel.py",
    "yahoo_spotcheck": SCRIPT_DIR / "07_yahoo_gold_spotcheck.py",
    "fundamental_db": SCRIPT_DIR / "08_build_mcp_db.py",
    "roic_gold_benchmark": SCRIPT_DIR / "09_build_roic_gold_benchmark.py",
    "roic_dump_download": SCRIPT_DIR / "10_download_roic_snapshots.py",
}


def _resolve_roic_input_dir(requested: Path) -> Path | None:
    """
    Resolve a usable ROIC dumps directory.

    Search order:
    1) requested path
    2) siblings like roic_*_json_dumps in requested parent
    3) data/ and data/processed/benchmarks fallback roots
    """
    if requested.exists() and requested.is_dir():
        return requested

    roots = [requested.parent, PROJECT_ROOT / "data", PROJECT_ROOT / "data" / "processed" / "benchmarks"]
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        if not root.exists() or not root.is_dir():
            continue
        candidates = sorted(p for p in root.glob("roic_*_json_dumps") if p.is_dir())
        if candidates:
            return candidates[-1]
    return None


def _run_roic_dump_download(args: argparse.Namespace) -> int:
    downloader = STEP_SCRIPTS["roic_dump_download"]
    if not downloader.exists():
        print(f"Missing ROIC downloader script: {downloader}")
        return 2
    cmd = [
        sys.executable,
        str(downloader),
        "--out-dir",
        str(args.roic_in_dir),
        "--mode",
        "single",
    ]
    if bool(args.preserve_roic_dumps):
        cmd.append("--no-prune-existing")
    else:
        cmd.append("--prune-existing")
    print(f"Running [roic_dump_download]: {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, check=False, cwd=str(PROJECT_ROOT)).returncode


def run_step(step: str, args: argparse.Namespace, from_all: bool = False) -> int:
    path = STEP_SCRIPTS.get(step)
    if path is None:
        print(f"Unknown step: {step}")
        print(f"Available: {', '.join(STEP_ORDER)}")
        return 2

    if not path.exists():
        print(f"Missing script file: {path}")
        return 2

    cmd = [sys.executable, str(path)]
    if step == "roic_gold_benchmark":
        resolved_in_dir = _resolve_roic_input_dir(requested=Path(args.roic_in_dir))
        if resolved_in_dir is None:
            if bool(args.auto_download_roic):
                code = _run_roic_dump_download(args=args)
                if code == 0:
                    resolved_in_dir = _resolve_roic_input_dir(requested=Path(args.roic_in_dir))
            msg = (
                f"ROIC input directory not found: {args.roic_in_dir}. "
                "Skipping roic_gold_benchmark in --all."
            )
            if resolved_in_dir is None and from_all:
                print(msg)
                print(
                    "To run this step, pass --roic-in-dir <dir> after creating/download "
                    "ROIC JSON dumps."
                )
                return 0
            if resolved_in_dir is None:
                print(msg.replace("Skipping roic_gold_benchmark in --all.", ""))
                print("Provide --roic-in-dir <dir> with ROIC JSON dumps.")
                return 2
        if resolved_in_dir != Path(args.roic_in_dir):
            print(f"ROIC input fallback: using {resolved_in_dir} (requested {args.roic_in_dir})")
        cmd.extend(
            [
                "--in-dir",
                str(resolved_in_dir),
                "--out-csv",
                str(args.roic_out_csv),
                "--report-json",
                str(args.roic_report_json),
                "--source-name",
                str(args.roic_source_name),
            ]
        )
        if bool(args.preserve_existing_output):
            cmd.append("--preserve-existing-output")
        else:
            cmd.append("--overwrite-existing-output")

    print(f"Running [{step}]: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, check=False, cwd=str(PROJECT_ROOT))
    return proc.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=sorted(STEP_SCRIPTS.keys()), help="Run one step")
    parser.add_argument("--all", action="store_true", help="Run all steps in order")
    parser.add_argument(
        "--roic-in-dir",
        type=Path,
        default=ROIC_DUMPS_DIR_DEFAULT,
        help="ROIC JSON dump directory for roic_gold_benchmark step.",
    )
    parser.add_argument(
        "--roic-out-csv",
        type=Path,
        default=ROIC_GOLD_BENCHMARK_CSV,
        help="Output CSV path for roic_gold_benchmark step.",
    )
    parser.add_argument(
        "--roic-report-json",
        type=Path,
        default=ROIC_GOLD_BENCHMARK_REPORT_JSON,
        help="Output report path for roic_gold_benchmark step.",
    )
    parser.add_argument(
        "--roic-source-name",
        type=str,
        default=ROIC_SOURCE_NAME_DEFAULT,
        help="Source name label for roic_gold_benchmark step.",
    )
    parser.add_argument(
        "--auto-download-roic",
        dest="auto_download_roic",
        action="store_true",
        help="Auto-download ROIC dumps when missing for roic_gold_benchmark.",
    )
    parser.add_argument(
        "--no-auto-download-roic",
        dest="auto_download_roic",
        action="store_false",
        help="Disable auto-download when ROIC dumps are missing.",
    )
    parser.add_argument(
        "--preserve-roic-dumps",
        dest="preserve_roic_dumps",
        action="store_true",
        help="Keep historical ROIC dump files when downloading snapshots (default).",
    )
    parser.add_argument(
        "--prune-roic-dumps",
        dest="preserve_roic_dumps",
        action="store_false",
        help="Delete historical ROIC dump files outside the requested snapshot date.",
    )
    parser.add_argument(
        "--preserve-existing-output",
        dest="preserve_existing_output",
        action="store_true",
        help="Write versioned benchmark output files instead of overwriting existing ones (default).",
    )
    parser.add_argument(
        "--overwrite-existing-output",
        dest="preserve_existing_output",
        action="store_false",
        help="Allow benchmark output files to overwrite existing files.",
    )
    parser.set_defaults(auto_download_roic=True)
    parser.set_defaults(preserve_roic_dumps=True)
    parser.set_defaults(preserve_existing_output=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.all:
        for step in STEP_ORDER:
            code = run_step(step=step, args=args, from_all=True)
            if code != 0:
                sys.exit(code)
        return

    if args.step:
        sys.exit(run_step(step=args.step, args=args, from_all=False))

    print(f"Available steps: {', '.join(STEP_ORDER)}")
    print("Tip: python scripts/run_scripts.py --step roic_gold_benchmark")


if __name__ == "__main__":
    main()

# python scripts/run_scripts.py --all

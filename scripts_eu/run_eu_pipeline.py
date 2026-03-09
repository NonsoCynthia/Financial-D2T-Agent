from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

from eu_config import ROIC_SOURCE_NAME_DEFAULT
from eu_paths import (
    LEGACY_FUNDAMENTALS_DIR,
    LEGACY_MONTHLY_RETURNS_DIR,
    LEGACY_PRICES_DIR,
    LEGACY_REPORTS_DIR,
    PROCESSED_BENCHMARKS_DIRS,
    PROCESSED_MCP_DIRS,
    PROCESSED_MONTHLY_RETURNS_DIRS,
    PROCESSED_PANEL_DIRS,
    PROCESSED_PRICES_DIRS,
    RAW_FUNDAMENTALS_DIRS,
    RAW_PRICES_DIRS,
    RAW_REPORTS_DIRS,
    RAW_SEC_COMPANYFACTS_DIRS,
    RAW_SEC_DIRS,
    ROIC_DUMPS_DIR_DEFAULT,
    ROIC_GOLD_BENCHMARK_CSV,
    ROIC_GOLD_BENCHMARK_REPORT_JSON,
    ensure_dirs,
)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

STEP_ORDER = [
    "prices",
    "returns",
    "fundamentals",
    "reports",
    "panel",
    "databases",
    "roic_gold_benchmark",
]

STEP_SCRIPTS = {
    "prices": SCRIPT_DIR / "01_download_prices_eu.py",
    "returns": SCRIPT_DIR / "02_compute_returns_eu.py",
    "fundamentals": SCRIPT_DIR / "03_download_fundamentals_eu.py",
    "reports": SCRIPT_DIR / "03b_download_reports_eu.py",
    "panel": SCRIPT_DIR / "04_make_monthly_panel_eu.py",
    "databases": SCRIPT_DIR / "05_build_eu_databases.py",
    "roic_gold_benchmark": SCRIPT_DIR / "06_build_roic_gold_benchmark_eu.py",
    "roic_dump_download": SCRIPT_DIR / "07_download_roic_snapshots_eu.py",
}


ROIC_DUMP_FILE_RE = re.compile(r"^roic_[A-Za-z0-9.\-]+_asof_\d{4}-\d{2}-\d{2}\.json$")


def _has_roic_json_dumps(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    for p in path.glob("*.json"):
        if ROIC_DUMP_FILE_RE.match(p.name):
            return True
    return False


def _resolve_roic_input_dir(requested: Path) -> Path | None:
    if _has_roic_json_dumps(path=requested):
        return requested

    roots = [requested.parent, PROJECT_ROOT / "data_eu", PROJECT_ROOT / "data_eu" / "processed" / "benchmarks"]
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        if not root.exists() or not root.is_dir():
            continue
        # Prefer known ROIC dump dir naming patterns first.
        candidates = sorted(
            p
            for p in root.glob("*")
            if p.is_dir() and (p.name.startswith("roic_") or p.name == "roic_json_dumps_monthly_last_year")
        )
        for c in candidates:
            if _has_roic_json_dumps(path=c):
                return c
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
                f"ROIC input directory missing or empty: {args.roic_in_dir}. "
                "Skipping roic_gold_benchmark in --all."
            )
            if resolved_in_dir is None and from_all:
                print(msg)
                print(
                    "To run this step, pass --roic-in-dir <dir> after creating/downloading "
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


def main() -> int:
    args = parse_args()

    ensure_dirs(
        paths=[
            *RAW_PRICES_DIRS,
            *RAW_FUNDAMENTALS_DIRS,
            *RAW_REPORTS_DIRS,
            *RAW_SEC_DIRS,
            *RAW_SEC_COMPANYFACTS_DIRS,
            *PROCESSED_PRICES_DIRS,
            *PROCESSED_MONTHLY_RETURNS_DIRS,
            *PROCESSED_PANEL_DIRS,
            *PROCESSED_MCP_DIRS,
            *PROCESSED_BENCHMARKS_DIRS,
            ROIC_DUMPS_DIR_DEFAULT,
            LEGACY_PRICES_DIR,
            LEGACY_FUNDAMENTALS_DIR,
            LEGACY_REPORTS_DIR,
            LEGACY_MONTHLY_RETURNS_DIR,
        ]
    )

    if args.all:
        for step in STEP_ORDER:
            code = run_step(step=step, args=args, from_all=True)
            if code != 0:
                return code
        return 0

    if args.step:
        return run_step(step=args.step, args=args, from_all=False)

    print(f"Available steps: {', '.join(STEP_ORDER)}")
    print("Tip: python scripts_eu/run_eu_pipeline.py --step roic_gold_benchmark")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# python scripts_eu/run_eu_pipeline.py --all

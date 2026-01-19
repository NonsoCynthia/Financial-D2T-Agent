import argparse
import subprocess
import sys
from pathlib import Path


# STEP_ORDER = ["prices", "sec_map", "sec_facts", "returns", "panel"]

STEP_SCRIPTS = {
    "prices": "scripts/01_download_prices.py",
    "sec_map": "scripts/02_sec_ticker_cik.py",
    "sec_facts": "scripts/03a_sec_companyfacts.py",
    # "sec_filings": "scripts/03b_sec_download_filings.py",
    "returns": "scripts/04a_compute_returns.py",
    "panel": "scripts/04b_align_fundamentals.py",
    "splits": "scripts/05_make_splits.py",
}


def run_step(step: str) -> int:
    script = STEP_SCRIPTS.get(step)
    if not script:
        print(f"Unknown step: {step}")
        print(f"Available: {', '.join(STEP_SCRIPTS.keys())}")
        return 2

    path = Path(script)
    if not path.exists():
        print(f"Missing script file: {script}")
        return 2

    print(f"Running: {script}")
    proc = subprocess.run([sys.executable, str(path)], check=False)
    return proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=list(STEP_SCRIPTS.keys()), help="Run one step")
    parser.add_argument("--all", action="store_true", help="Run all steps in order")
    args = parser.parse_args()

    if args.all:
        for s in STEP_SCRIPTS:
            code = run_step(s)
            if code != 0:
                sys.exit(code)
        return

    if args.step:
        sys.exit(run_step(args.step))

    parser.print_help()


if __name__ == "__main__":
    main()


# python run_scripts.py --step prices
# python run_scripts.py --all
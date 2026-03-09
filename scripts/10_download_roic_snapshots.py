#!/usr/bin/env python3
"""Download ROIC statistics snapshots into JSON dumps for benchmark building."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import TICKERS, ROIC_DUMPS_DIR_DEFAULT


ASOF_FILE_RE = re.compile(r"^roic_[A-Za-z0-9.\-]+_asof_(\d{4}-\d{2}-\d{2})\.json$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def first_business_days_for_year(year: int) -> list[str]:
    """
    Return the first business day (Mon-Fri) of each month for a year.

    This avoids external market-calendar dependencies.
    """
    first_days: list[str] = []
    for month in range(1, 13):
        ts = pd.Timestamp(year=year, month=month, day=1)
        if ts.weekday() >= 5:
            ts = ts + pd.offsets.BDay(1)
        first_days.append(ts.date().isoformat())
    return first_days


def extract_tables_from_html(html: str) -> list[dict]:
    tables_json: list[dict] = []
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return tables_json

    for idx, df in enumerate(tables, start=1):
        tables_json.append(
            {
                "table_index": idx,
                "shape": [int(df.shape[0]), int(df.shape[1])],
                "columns": [str(c) for c in df.columns],
                "rows": df.to_dict(orient="records"),
            }
        )

    return tables_json


def fetch_roic_snapshot(
    session: requests.Session,
    ticker: str,
    asof_date: str,
    timeout_s: int,
    retries: int,
    user_agent: str,
) -> dict:
    url = f"https://www.roic.ai/quote/{ticker}/statistics"
    headers = {"User-Agent": user_agent}

    last_err: Exception | None = None
    for i in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=timeout_s)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.5 + i * 0.5)
                continue
            resp.raise_for_status()

            html = resp.text
            return {
                "ticker": ticker,
                "source": "roic.ai",
                "url": url,
                "asof_date": asof_date,
                "downloaded_at_utc": utc_now_iso(),
                "html_len": len(html),
                "tables": extract_tables_from_html(html=html),
                "raw_html": html,
            }
        except Exception as e:
            last_err = e
            time.sleep(0.5 + i * 0.5)

    raise RuntimeError(f"Failed to fetch {url}. Last error: {last_err}")


def save_payload(payload: dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ticker = str(payload.get("ticker", "UNKNOWN"))
    asof_date = str(payload.get("asof_date", "unknown"))
    out_path = out_dir / f"roic_{ticker}_asof_{asof_date}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def prune_existing_dumps(out_dir: Path, keep_dates: set[str]) -> int:
    deleted = 0
    for path in out_dir.glob("roic_*_asof_*.json"):
        m = ASOF_FILE_RE.match(path.name)
        if m is None:
            continue
        asof_date = m.group(1)
        if asof_date not in keep_dates:
            path.unlink(missing_ok=True)
            deleted += 1
    return deleted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download ROIC statistics JSON dumps.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROIC_DUMPS_DIR_DEFAULT,
        help="Output folder for JSON dumps.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(TICKERS),
        help="Comma-separated tickers.",
    )
    parser.add_argument(
        "--mode",
        choices=["monthly_last_year", "single"],
        default="single",
        help="Download one snapshot per month in last year or a single current snapshot.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Override year for monthly mode (defaults to previous calendar year).",
    )
    parser.add_argument(
        "--exchange-calendar",
        type=str,
        default="NYSE",
        help="Kept for compatibility; first business day uses Mon-Fri calendar.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="HTTP retries per request.",
    )
    parser.add_argument(
        "--request-sleep",
        type=float,
        default=0.2,
        help="Sleep seconds between requests.",
    )
    parser.add_argument(
        "--user-agent",
        type=str,
        default="Mozilla/5.0 (compatible; roic-downloader/1.0)",
        help="User-Agent header.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing JSON files.",
    )
    parser.add_argument(
        "--prune-existing",
        dest="prune_existing",
        action="store_true",
        help="Delete existing ROIC dump files outside requested asof date(s).",
    )
    parser.add_argument(
        "--no-prune-existing",
        dest="prune_existing",
        action="store_false",
        help="Keep existing historical ROIC dump files (default).",
    )
    parser.set_defaults(prune_existing=False)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    tickers = [t.strip().upper() for t in str(args.tickers).split(",") if t.strip()]
    if not tickers:
        raise ValueError("No tickers provided.")

    if args.mode == "single":
        asof_dates = [datetime.now(timezone.utc).date().isoformat()]
    else:
        year = int(args.year) if args.year is not None else (date.today().year - 1)
        asof_dates = first_business_days_for_year(year=year)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    pruned = 0
    if bool(args.prune_existing):
        pruned = prune_existing_dumps(out_dir=args.out_dir, keep_dates=set(asof_dates))
        if pruned > 0:
            print(f"pruned old dumps: {pruned}")

    successes = 0
    failures: list[str] = []
    skipped = 0

    with requests.Session() as session:
        for ticker in tickers:
            for asof_date in asof_dates:
                out_path = args.out_dir / f"roic_{ticker}_asof_{asof_date}.json"
                if out_path.exists() and not args.overwrite:
                    skipped += 1
                    continue

                try:
                    payload = fetch_roic_snapshot(
                        session=session,
                        ticker=ticker,
                        asof_date=asof_date,
                        timeout_s=int(args.timeout),
                        retries=int(args.retries),
                        user_agent=str(args.user_agent),
                    )
                    saved = save_payload(payload=payload, out_dir=args.out_dir)
                    successes += 1
                    print(f"saved: {saved}")
                except Exception as e:
                    failures.append(f"{ticker} {asof_date} -> {e}")
                    print(f"failed: {ticker} {asof_date} -> {e}")

                time.sleep(float(args.request_sleep))

    print(
        "ROIC dump download complete: "
        f"successes={successes} skipped={skipped} failures={len(failures)} pruned={pruned} out_dir={args.out_dir}"
    )
    if failures:
        print("Failures:")
        for item in failures:
            print(f"  {item}")

    return 0 if successes > 0 or skipped > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

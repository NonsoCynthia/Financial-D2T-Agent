from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


# ROIC statistics-page labels mapped to:
#   (project indicator name, default units, scale factor after numeric parsing)
ROIC_LABEL_TO_INDICATOR: dict[str, tuple[str, str, float]] = {
    "Revenue": ("NetRevenue_TTM", "USD", 1.0),
    "EBIT": ("EBIT_TTM", "USD", 1.0),
    "Net Income": ("NetProfit_TTM", "USD", 1.0),
    "Cash & Cash Equivalents": ("CashAndEquivalents", "USD", 1.0),
    "Total Debt": ("GrossDebt", "USD", 1.0),
    "Net Debt": ("NetDebt", "USD", 1.0),
    "Equity (Book Value)": ("ShareholdersEquity", "USD", 1.0),
    "Earnings Per Share (EPS)": ("EPS", "USD_per_share", 1.0),
    "Book Value Per Share": ("BVPS", "USD_per_share", 1.0),
    "PE Ratio": ("P_E", "multiple", 1.0),
    "PB Ratio": ("P_B", "multiple", 1.0),
    "PS Ratio": ("PriceToSales", "multiple", 1.0),
    "EV / EBIT": ("EV_EBIT", "multiple", 1.0),
    "EV / EBITDA": ("EV_EBITDA", "multiple", 1.0),
    "Current Ratio": ("CurrentRatio", "multiple", 1.0),
    # ROIC statistics page displays this as percent-like values (for example 133.8).
    # Convert to the ratio convention used by the agent (for example 1.338).
    "Debt / Equity": ("GrossDebt_Equity", "multiple", 0.01),
    "Return on Equity (ROE)": ("ROE", "percent", 1.0),
    "Return on Invested Capital (ROIC)": ("ROIC", "percent", 1.0),
    "Gross Margin": ("GrossMargin", "percent", 1.0),
    "Operating Margin": ("EBITMargin", "percent", 1.0),
    "Profit Margin": ("NetMargin", "percent", 1.0),
}

MISSING_TOKENS = {"", "-", "--", "na", "n/a", "none", "null", "nan"}
SUFFIX_SCALE = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
DATE_LIKE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
DATE_SUFFIX_RE = re.compile(r"_\d{4}-\d{2}-\d{2}$")

BASE_OUTPUT_COLS = [
    "ticker",
    "date",
    "capture_date",
    "indicator",
    "gold_value",
    "units",
    "metric_name",
    "raw_value",
    "table_index",
]


def _parse_scaled_number(value: Any) -> tuple[float | None, str | None]:
    """
    Parse values like '3.89T', '46.91', '12.4%', '(1.2B)' into float.
    Returns (value, parsed_unit_hint) where parsed_unit_hint may be 'percent'.
    """
    if value is None:
        return None, None

    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None, None
        return float(value), None

    s = str(value).strip()
    if not s:
        return None, None
    if s.lower() in MISSING_TOKENS:
        return None, None
    if DATE_LIKE_RE.match(s):
        return None, None

    s = s.replace(",", "")
    s = s.replace("$", "").replace("€", "").replace("£", "")
    s = s.replace(" ", "")

    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1]

    unit_hint: str | None = None
    if s.endswith("%"):
        unit_hint = "percent"
        s = s[:-1]

    if s.endswith(("x", "X")):
        s = s[:-1]

    scale = 1.0
    if s and s[-1].upper() in SUFFIX_SCALE:
        scale = SUFFIX_SCALE[s[-1].upper()]
        s = s[:-1]

    try:
        base = float(s)
    except Exception:
        return None, None

    out = base * scale
    if is_negative:
        out = -out
    return out, unit_hint


def _iter_rows_from_dump(path: Path, source_name: str) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    ticker = str(payload.get("ticker") or "").strip().upper()
    downloaded_at_utc = str(payload.get("downloaded_at_utc") or "").strip()
    capture_date = downloaded_at_utc[:10] if downloaded_at_utc else ""

    # Requested behavior: use capture date only (snapshot date).
    date_for_eval = capture_date

    if not ticker or not capture_date:
        return []

    rows_out: list[dict[str, Any]] = []
    for table in payload.get("tables", []):
        table_index = table.get("table_index")
        for row in table.get("rows", []):
            raw_metric = str(row.get("0") or "").strip()
            if raw_metric not in ROIC_LABEL_TO_INDICATOR:
                continue

            indicator, default_unit, scale_factor = ROIC_LABEL_TO_INDICATOR[raw_metric]
            raw_value = row.get("1")
            parsed_value, parsed_unit = _parse_scaled_number(value=raw_value)
            if parsed_value is None:
                continue
            parsed_value = float(parsed_value) * float(scale_factor)

            rows_out.append(
                {
                    "ticker": ticker,
                    "date": date_for_eval,
                    "capture_date": capture_date,
                    "indicator": indicator,
                    "gold_value": float(parsed_value),
                    "units": parsed_unit or default_unit,
                    "metric_name": raw_metric,
                    "raw_value": raw_value,
                    "table_index": table_index,
                    "dump_file": path.name,
                }
            )

    return rows_out


def _resolve_input_dir(in_dir: Path) -> tuple[Path, str | None]:
    """Resolve input dir and fallback to the latest roic_*_json_dumps sibling if needed."""
    if in_dir.exists() and in_dir.is_dir():
        return in_dir, None

    parent = in_dir.parent
    if not parent.exists() or not parent.is_dir():
        return in_dir, None

    candidates = sorted(p for p in parent.glob("roic_*_json_dumps") if p.is_dir())
    if not candidates:
        return in_dir, None

    chosen = candidates[-1]
    note = f"Input directory not found: {in_dir}. Using fallback: {chosen}"
    return chosen, note


def _latest_iso_date_from_values(values: list[Any]) -> str | None:
    if not values:
        return None
    s = pd.to_datetime(pd.Series(values), errors="coerce").dropna()
    if s.empty:
        return None
    return s.max().strftime("%Y-%m-%d")


def _latest_capture_from_df(df: pd.DataFrame) -> str | None:
    if "capture_date" in df.columns:
        return _latest_iso_date_from_values(values=df["capture_date"].dropna().tolist())
    if "date" in df.columns:
        return _latest_iso_date_from_values(values=df["date"].dropna().tolist())
    return None


def _latest_capture_from_csv(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        return None
    if "capture_date" in df.columns:
        return _latest_iso_date_from_values(values=df["capture_date"].dropna().tolist())
    if "date" in df.columns:
        return _latest_iso_date_from_values(values=df["date"].dropna().tolist())
    return None


def _versioned_path(path: Path, suffix_date: str) -> Path:
    stem = path.stem
    if stem.endswith("_report"):
        base = stem[: -len("_report")]
        base = DATE_SUFFIX_RE.sub("", base)
        new_stem = f"{base}_{suffix_date}_report"
    else:
        base = DATE_SUFFIX_RE.sub("", stem)
        new_stem = f"{base}_{suffix_date}"
    return path.with_name(f"{new_stem}{path.suffix}")


def _resolve_output_paths_by_capture(
    out_csv: Path,
    report_json: Path | None,
    latest_capture_date: str | None,
) -> tuple[Path, Path | None, str | None]:
    """
    Always return dated output paths. If capture date changed vs existing base CSV, signal it.
    """
    if not latest_capture_date:
        return out_csv, report_json, None

    note = None
    existing_capture = _latest_capture_from_csv(path=out_csv)
    if existing_capture and existing_capture != latest_capture_date:
        note = f"capture_changed:{existing_capture}->{latest_capture_date}; writing new file"

    dated_csv = _versioned_path(path=out_csv, suffix_date=latest_capture_date)
    dated_report = _versioned_path(path=report_json, suffix_date=latest_capture_date) if report_json else None
    return dated_csv, dated_report, note


def build_gold_csv_from_roic_dumps(
    in_dir: Path,
    out_csv: Path,
    report_json: Path | None,
    source_name: str = "roic.ai",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    resolved_in_dir, fallback_note = _resolve_input_dir(in_dir=in_dir)
    if not resolved_in_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {in_dir}")

    json_files = sorted(resolved_in_dir.glob("*.json"))
    all_rows: list[dict[str, Any]] = []
    for path in json_files:
        all_rows.extend(_iter_rows_from_dump(path=path, source_name=source_name))

    df = pd.DataFrame(all_rows)
    if df.empty:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        if not out_csv.exists():
            pd.DataFrame(columns=BASE_OUTPUT_COLS).to_csv(out_csv, index=False)
        report = {
            "input_dir": str(resolved_in_dir),
            "json_files_scanned": len(json_files),
            "rows_written": int(len(pd.read_csv(out_csv, low_memory=False))) if out_csv.exists() else 0,
            "rows_new_batch": 0,
            "note": "No mapped ROIC metrics found in new dumps; existing CSV preserved.",
            "out_csv_written": str(out_csv),
            "source_name": source_name,
        }
        if fallback_note:
            report["input_dir_fallback_used"] = True
            report["requested_input_dir"] = str(in_dir)
            report["input_dir_fallback_note"] = fallback_note
        if report_json is not None:
            report_json.parent.mkdir(parents=True, exist_ok=True)
            report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
            report["report_json_written"] = str(report_json)
        return df, report

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["capture_date"] = pd.to_datetime(df["capture_date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["ticker", "date", "capture_date", "indicator", "gold_value"]).copy()

    # Save snapshot once per ticker/indicator/date (no fake monthly repeats).
    df = (
        df.sort_values(
            [
                "ticker",
                "date",
                "indicator",
                "table_index",
                "dump_file",
            ]
        )
        .drop_duplicates(subset=["ticker", "date", "indicator"], keep="last")
        .reset_index(drop=True)
    )

    out_df = df.copy()
    out_df["date"] = out_df["date"].dt.strftime("%Y-%m-%d")
    out_df["capture_date"] = out_df["capture_date"].dt.strftime("%Y-%m-%d")
    out_df = out_df[BASE_OUTPUT_COLS]
    out_df = out_df.sort_values(["ticker", "date", "indicator"]).reset_index(drop=True)

    latest_capture_date = _latest_capture_from_df(df=out_df)
    out_csv_resolved, report_json_resolved, output_version_note = _resolve_output_paths_by_capture(
        out_csv=out_csv,
        report_json=report_json,
        latest_capture_date=latest_capture_date,
    )

    out_csv_resolved.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv_resolved, index=False)

    dates_per_ticker = (
        out_df[["ticker", "date"]].drop_duplicates().groupby("ticker")["date"].nunique().sort_values().to_dict()
    )
    capture_dates = sorted({d for d in out_df["capture_date"].dropna().unique().tolist()})

    series_values: dict[tuple[str, str], set[float]] = defaultdict(set)
    for _, row in out_df.iterrows():
        series_values[(str(row["ticker"]), str(row["indicator"]))].add(float(row["gold_value"]))
    n_series = len(series_values)
    n_constant = sum(1 for vals in series_values.values() if len(vals) <= 1)

    report: dict[str, Any] = {
        "input_dir": str(resolved_in_dir),
        "json_files_scanned": len(json_files),
        "rows_written": int(len(out_df)),
        "rows_new_batch": int(len(out_df)),
        "source_name": source_name,
        "unique_tickers": int(out_df["ticker"].nunique()),
        "unique_dates": int(out_df["date"].nunique()),
        "date_min": str(out_df["date"].min()),
        "date_max": str(out_df["date"].max()),
        "capture_dates_unique_count": int(len(capture_dates)),
        "capture_dates": capture_dates,
        "date_definition": "date is capture snapshot date (downloaded_at_utc[:10])",
        "dates_per_ticker": dates_per_ticker,
        "latest_capture_date": latest_capture_date,
        "series_count": int(n_series),
        "series_constant_count": int(n_constant),
        "series_constant_ratio": float(n_constant / n_series) if n_series else None,
        "out_csv_written": str(out_csv_resolved),
    }
    if output_version_note:
        report["output_version_note"] = output_version_note
    if fallback_note:
        report["input_dir_fallback_used"] = True
        report["requested_input_dir"] = str(in_dir)
        report["input_dir_fallback_note"] = fallback_note

    report["warning_labelled_not_historical"] = bool(
        report["unique_dates"] >= 12
        and report["capture_dates_unique_count"] <= 2
        and (report["series_constant_ratio"] or 0.0) >= 0.8
    )

    if report_json_resolved is not None:
        report_json_resolved.parent.mkdir(parents=True, exist_ok=True)
        report_json_resolved.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        report["report_json_written"] = str(report_json_resolved)

    return out_df, report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build table2-compatible gold benchmark CSV from ROIC statistics JSON dumps. "
            "The output 'date' column uses capture_date (snapshot date)."
        )
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=Path("data/roic_json_dumps_monthly_last_year"),
        help="Folder containing roic_<ticker>_asof_<date>.json dumps.",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=Path(f"data/processed/benchmarks/roic_gold_benchmark_{date.today().isoformat()}.csv"),
        help="Output long-format CSV for evaluate.py --mode table2.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=Path(f"data/processed/benchmarks/roic_gold_benchmark_{date.today().isoformat()}_report.json"),
        help="Optional quality/coverage report path.",
    )
    parser.add_argument(
        "--source-name",
        type=str,
        default="roic.ai",
        help="Source label written into CSV.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_df, report = build_gold_csv_from_roic_dumps(
        in_dir=args.in_dir.expanduser().resolve(),
        out_csv=args.out_csv.expanduser().resolve(),
        report_json=args.report_json.expanduser().resolve() if args.report_json else None,
        source_name=args.source_name.strip() or "roic.ai",
    )

    out_csv_written = report.get("out_csv_written", str(args.out_csv))
    print(f"Wrote gold benchmark CSV: {out_csv_written}")
    print(
        "Rows={rows} Tickers={tickers} Dates={dates} CaptureDates={caps}".format(
            rows=int(report.get("rows_written", 0)),
            tickers=int(report.get("unique_tickers", 0)),
            dates=int(report.get("unique_dates", 0)),
            caps=int(report.get("capture_dates_unique_count", 0)),
        )
    )
    if bool(report.get("warning_labelled_not_historical", False)):
        print("WARNING: data appears labelled monthly but captured in a narrow real-time window.")
    if bool(report.get("input_dir_fallback_used", False)):
        print(f"WARNING: {report.get('input_dir_fallback_note')}")
    if args.report_json and report.get("report_json_written"):
        print(f"Wrote report: {report['report_json_written']}")
    if report.get("output_version_note"):
        print(f"Note: {report['output_version_note']}")
    if out_df.empty:
        print("No mapped rows produced. Check dump format and label mapping.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Yahoo gold-standard spot check for model-generated indicators.

Compares predicted indicators to Yahoo Finance-derived snapshots for one month
(default: 2025-04), then writes a comparison table for manual validation.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

def canonical_ticker(ticker: Any) -> str:
    return str(ticker or "").strip().upper()


def canonical_month(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m")


def indicators_to_canonical_map(indicators: Any) -> Dict[str, Any]:
    """
    Convert common prediction formats into {indicator_name: value}.
    Supports:
    - dict: {"P_E": 10.0, ...}
    - list[dict]: [{"indicator": "P_E", "value": 10.0}, ...]
    """
    if isinstance(indicators, dict):
        return {str(k): v for k, v in indicators.items()}

    if isinstance(indicators, list):
        out: Dict[str, Any] = {}
        for row in indicators:
            if not isinstance(row, dict):
                continue
            key = row.get("indicator")
            if key is None:
                continue
            out[str(key)] = row.get("value")
        return out

    return {}


TARGET_KEYS = [
    "Assets",
    "CashAndEquivalents",
    "NetRevenue_TTM",
    "EBIT_TTM",
    "NetProfit_TTM",
    "EPS",
    "P_E",
    "P_B",
    "last_price",
]


def _to_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return float(v)


def _pick_numeric(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in d:
            v = _to_float(d.get(k))
            if v is not None:
                return v
    return None


def _pick_series_row(df: pd.DataFrame, aliases: List[str]) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    idx_map = {str(i).strip().lower(): i for i in df.index}
    for alias in aliases:
        key = alias.strip().lower()
        if key in idx_map:
            row = df.loc[idx_map[key]]
            if isinstance(row, pd.Series):
                return row
    return None


def _pick_asof_value(row: Optional[pd.Series], as_of_date: pd.Timestamp) -> Optional[float]:
    if row is None or len(row) == 0:
        return None
    s = pd.to_numeric(row, errors="coerce").dropna()
    if s.empty:
        return None
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s[~s.index.isna()]
    if getattr(s.index, "tz", None) is not None:
        s.index = s.index.tz_convert(None)
    if s.empty:
        return None
    s = s.sort_index()
    if getattr(as_of_date, "tzinfo", None) is not None:
        as_of_date = as_of_date.tz_convert(None)
    s = s[s.index <= as_of_date]
    if s.empty:
        return None
    return _to_float(s.iloc[-1])


def _fetch_price_asof(ticker: yf.Ticker, as_of_date: pd.Timestamp) -> Optional[float]:
    start = (as_of_date - pd.Timedelta(days=40)).strftime("%Y-%m-%d")
    end = (as_of_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    hist = ticker.history(start=start, end=end, auto_adjust=False)
    if hist is None or hist.empty:
        return None
    hist = hist.copy()
    hist.index = pd.to_datetime(hist.index, errors="coerce")
    hist = hist[~hist.index.isna()]
    if getattr(hist.index, "tz", None) is not None:
        hist.index = hist.index.tz_convert(None)
    if getattr(as_of_date, "tzinfo", None) is not None:
        as_of_date = as_of_date.tz_convert(None)
    hist = hist[hist.index <= as_of_date]
    if hist.empty:
        return None
    return _to_float(hist.iloc[-1].get("Close"))


def fetch_yahoo_snapshot(symbol: str, as_of_date: str) -> Dict[str, Optional[float]]:
    t = yf.Ticker(symbol)
    dt = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(dt):
        return {k: None for k in TARGET_KEYS}

    qbs = t.quarterly_balance_sheet
    qis = t.quarterly_income_stmt
    info = t.info if isinstance(getattr(t, "info", None), dict) else {}

    assets = _pick_asof_value(_pick_series_row(qbs, ["Total Assets"]), dt)
    cash = _pick_asof_value(
        _pick_series_row(
            qbs,
            [
                "Cash And Cash Equivalents",
                "Cash And Short Term Investments",
                "Cash Cash Equivalents And Short Term Investments",
            ],
        ),
        dt,
    )
    revenue = _pick_asof_value(_pick_series_row(qis, ["Total Revenue", "Revenue"]), dt)
    ebit = _pick_asof_value(_pick_series_row(qis, ["EBIT", "Operating Income"]), dt)
    net_profit = _pick_asof_value(_pick_series_row(qis, ["Net Income", "Net Income Common Stockholders"]), dt)

    price = _fetch_price_asof(t, dt)
    eps = _to_float(info.get("trailingEps"))
    pe = _to_float(info.get("trailingPE"))
    pb = _to_float(info.get("priceToBook"))

    if pe is None and price is not None and eps not in (None, 0.0):
        pe = _to_float(price / eps)

    return {
        "Assets": assets,
        "CashAndEquivalents": cash,
        "NetRevenue_TTM": revenue,
        "EBIT_TTM": ebit,
        "NetProfit_TTM": net_profit,
        "EPS": eps,
        "P_E": pe,
        "P_B": pb,
        "last_price": price,
    }


def _extract_pred_map(indicators: Dict[str, Any]) -> Dict[str, Optional[float]]:
    m = indicators_to_canonical_map(indicators)
    out: Dict[str, Optional[float]] = {}
    for k in TARGET_KEYS:
        out[k] = _to_float(m.get(k))
    return out


def detect_mode(folder: Path) -> str:
    if any(folder.glob("*_workflow_output_*.json")):
        return "workflow"
    if any(folder.glob("*_output_*.json")):
        return "agent"
    raise RuntimeError(f"No workflow or agent output files found in: {folder}")


def load_predictions(folder: Path, mode: str, month: str, tickers: Optional[List[str]]) -> List[Dict[str, Any]]:
    month = month.strip()
    out: List[Dict[str, Any]] = []
    keep = set([canonical_ticker(t) for t in (tickers or []) if str(t).strip()])

    if mode == "workflow":
        files = sorted(folder.glob("*_workflow_output_*.json"))
        for p in files:
            payload = json.loads(p.read_text(encoding="utf-8"))
            ticker = canonical_ticker(payload.get("ticker", ""))
            if keep and ticker not in keep:
                continue
            for item in payload.get("outputs", []):
                d = str(item.get("date", "")).strip()
                if not d:
                    continue
                dt = pd.to_datetime(d, errors="coerce")
                if pd.isna(dt) or canonical_month(dt) != month:
                    continue
                indicators_raw = ((item.get("indicator_analysis") or {}).get("values") or {})
                if not isinstance(indicators_raw, (dict, list)):
                    continue
                out.append({"ticker": ticker, "date": dt.strftime("%Y-%m-%d"), "month": month, "pred": _extract_pred_map(indicators_raw)})
    else:
        files = sorted(folder.glob("*_output_*.json"))
        for p in files:
            payload = json.loads(p.read_text(encoding="utf-8"))
            ticker = canonical_ticker(payload.get("ticker", ""))
            if keep and ticker not in keep:
                continue
            for item in payload.get("outputs", []):
                d = str(item.get("date", "")).strip()
                if not d:
                    continue
                dt = pd.to_datetime(d, errors="coerce")
                if pd.isna(dt) or canonical_month(dt) != month:
                    continue
                indicators_raw = ((item.get("analyst") or {}).get("indicators") or {})
                if not isinstance(indicators_raw, (dict, list)):
                    continue
                out.append({"ticker": ticker, "date": dt.strftime("%Y-%m-%d"), "month": month, "pred": _extract_pred_map(indicators_raw)})
    return out


def compare_one(
    ticker: str,
    date: str,
    pred: Dict[str, Optional[float]],
    gold: Dict[str, Optional[float]],
    tolerance: float,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for k in TARGET_KEYS:
        pv = pred.get(k)
        gv = gold.get(k)
        rel = None
        ok = None
        if pv is not None and gv is not None:
            rel = abs(float(pv) - float(gv)) / max(abs(float(gv)), 1e-9)
            ok = bool(rel <= tolerance)
        rows.append(
            {
                "ticker": ticker,
                "date": date,
                "indicator": k,
                "predicted": pv,
                "gold_yahoo": gv,
                "relative_error": rel,
                "within_tolerance": ok,
            }
        )
    return rows


@dataclass
class Args:
    pred_dir: Path
    mode: str
    month: str
    tickers: Optional[List[str]]
    tolerance: float
    out_csv: Path
    out_json: Path


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--pred_dir",
        type=str,
        default="results/final_report2025_us",
        help="Prediction directory (agent or workflow outputs).",
    )
    p.add_argument("--mode", type=str, choices=["auto", "workflow", "agent"], default="auto")
    p.add_argument("--month", type=str, default="2025-04", help="Spot-check month in YYYY-MM format.")
    p.add_argument("--tickers", type=str, default=None, help="Optional comma-separated ticker filter.")
    p.add_argument("--tolerance", type=float, default=0.30, help="Relative error tolerance.")
    p.add_argument(
        "--out_csv",
        type=str,
        default="results/validation/yahoo_spotcheck_2025-04.csv",
        help="Output CSV path.",
    )
    p.add_argument(
        "--out_json",
        type=str,
        default="results/validation/yahoo_spotcheck_2025-04.json",
        help="Output JSON path.",
    )
    a = p.parse_args()
    tickers = [x.strip().upper() for x in (a.tickers or "").split(",") if x.strip()] or None
    return Args(
        pred_dir=Path(a.pred_dir),
        mode=a.mode,
        month=str(a.month).strip(),
        tickers=tickers,
        tolerance=float(a.tolerance),
        out_csv=Path(a.out_csv),
        out_json=Path(a.out_json),
    )


def main() -> None:
    args = parse_args()
    if not args.pred_dir.exists():
        print(f"pred_dir not found: {args.pred_dir}")
        return

    try:
        mode = args.mode if args.mode != "auto" else detect_mode(args.pred_dir)
    except RuntimeError as e:
        print(f"{e}")
        return
    preds = load_predictions(args.pred_dir, mode, args.month, args.tickers)
    if not preds:
        print(f"No prediction rows found for month {args.month} in {args.pred_dir} (mode={mode}).")
        return

    all_rows: List[Dict[str, Any]] = []
    cache: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}
    for r in preds:
        key = (r["ticker"], r["date"])
        if key not in cache:
            cache[key] = fetch_yahoo_snapshot(r["ticker"], r["date"])
        all_rows.extend(compare_one(r["ticker"], r["date"], r["pred"], cache[key], args.tolerance))

    df = pd.DataFrame(all_rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    args.out_json.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")

    comp = df[df["relative_error"].notna()].copy()
    fail = comp[comp["within_tolerance"] == False]  # noqa: E712
    print(f"Mode: {mode}")
    print(f"Month: {args.month}")
    print(f"Rows compared: {len(comp)}")
    print(f"Out of tolerance: {len(fail)}")
    print(f"Saved CSV: {args.out_csv}")
    print(f"Saved JSON: {args.out_json}")


if __name__ == "__main__":
    main()

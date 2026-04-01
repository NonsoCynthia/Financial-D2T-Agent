"""
Scoring module: compare agent predictions against ROIC.ai gold-standard values.

Gold data is a long-format CSV from ROIC.ai with columns:
    ticker, date, indicator, gold_value  (+ optional capture_date, units, ...)

Main metric (Equation 1 in the paper):
    Score = NMAE + alpha * PenaltyRate

where:
    NMAE         = normalised mean absolute error across scored indicators
    PenaltyRate  = fraction of indicators predicted as 0 when gold is non-zero
    alpha        = 10 (default)

Important: only indicators present in gold are scored.  If ROIC.ai provides
21 of 32 indicators, the other 11 are skipped (not penalised).  This avoids
the previous bug where missing gold columns defaulted to 0.0 and corrupted
both NMAE and PenaltyRate.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from financial_agents.us_indicator_schema import Indicator


# ── Scoring hyper-parameter ──────────────────────────────────────────────────
ALPHA = 10.0


# ── Indicator helpers ────────────────────────────────────────────────────────

def indicator_keys() -> list[str]:
    """Return the canonical list of 32 indicator names from the schema."""
    return [str(i) for i in Indicator]


# ── Core metrics ─────────────────────────────────────────────────────────────

def nmae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Normalised Mean Absolute Error (Equation 2).

        NMAE = (1/N) * sum(|y_true - y_pred|) / (max(y_true) - min(y_true))

    Returns inf when all gold values are identical (zero range).
    Returns nan when arrays are empty.
    """
    y_true = np.asarray(y_true, dtype=float).reshape(-1)
    y_pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if y_true.shape != y_pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if y_true.size == 0:
        return float("nan")

    denom = float(np.max(y_true) - np.min(y_true))
    if denom == 0.0:
        return float("inf")

    return float(np.mean(np.abs(y_true - y_pred)) / denom)


def penalty_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Fraction of indicators where prediction is 0 but gold is non-zero
    (Equation 3).

        PenaltyRate = (1/N) * sum( I(pred == 0 AND gold != 0) )
    """
    y_true = np.asarray(y_true, dtype=float).reshape(-1)
    y_pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if y_true.shape != y_pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if y_true.size == 0:
        return float("nan")

    misses = float(np.sum((y_pred == 0.0) & (y_true != 0.0)))
    return float(misses / float(y_true.size))


def score_components(
    y_true: Dict[str, float],
    y_pred: Dict[str, float],
    alpha: float = ALPHA,
) -> Tuple[float, float, float]:
    """
    Compute the full scoring triple: (NMAE, PenaltyRate, Score).

    Only indicators present in y_true (gold) are evaluated.  This avoids
    penalising the agent for indicators that ROIC.ai doesn't provide.

    Args:
        y_true: Gold values (indicator name -> value).  Only keys present
                in this dict are scored.
        y_pred: Predicted values (indicator name -> value).
        alpha:  Weight for the penalty term (default 10).

    Returns:
        Tuple of (nmae_value, penalty_rate_value, score_value).
        Returns (nan, nan, nan) if no scorable indicators exist.
    """
    # Score only indicators that exist in gold
    keys_with_gold = [k for k in indicator_keys() if k in y_true]
    if not keys_with_gold:
        return float("nan"), float("nan"), float("nan")

    yt = np.array([float(y_true[k]) for k in keys_with_gold], dtype=float)
    yp = np.array([float(y_pred.get(k, 0.0)) for k in keys_with_gold], dtype=float)

    nmae_value = nmae(y_true=yt, y_pred=yp)
    penalty_value = penalty_rate(y_true=yt, y_pred=yp)
    score_value = float(nmae_value + (float(alpha) * penalty_value))
    return nmae_value, penalty_value, score_value


# ── Gold data loading (ROIC.ai long-format CSV) ─────────────────────────────

def default_gold_csv() -> Path:
    """
    Default path to the ROIC.ai gold benchmark CSV.

    The CSV is in long format with columns:
        ticker, date, indicator, gold_value  (+ optional capture_date, units)
    """
    # .../openai-agent/experiments/final_report2025/scoring.py
    # -> parents[3] is Financial-D2T-Agent project root
    return (
        Path(__file__).resolve().parents[3]
        / "data"
        / "processed"
        / "benchmarks"
        / "roic_gold_benchmark_2026-02-25.csv"
    )


@lru_cache(maxsize=4)
def _load_gold_frame_cached(gold_csv_resolved: str) -> pd.DataFrame:
    """
    Load and normalise the ROIC.ai gold benchmark CSV (long format).

    Expected columns: ticker, date, indicator, gold_value
    Optional columns: capture_date, units, source, metric_name, raw_value

    Returns a DataFrame with at least: ticker, date, indicator, gold_value
    sorted by (ticker, date, indicator).
    """
    gold_csv = Path(gold_csv_resolved)
    df = pd.read_csv(gold_csv, low_memory=False)

    # Build a case-insensitive column lookup for flexible aliasing
    norm_cols = {c.lower().strip(): c for c in df.columns}

    def _find_col(candidates: list[str]) -> str | None:
        for cand in candidates:
            found = norm_cols.get(cand.lower().strip())
            if found:
                return found
        return None

    # Rename aliased columns to canonical names
    rename_map = {}
    for target, aliases in [
        ("ticker", ["ticker", "symbol"]),
        ("date", ["date", "analysis_date", "as_of_date"]),
        ("indicator", ["indicator", "metric"]),
        ("gold_value", ["gold_value", "value", "gold"]),
    ]:
        found = _find_col(aliases)
        if found and found != target:
            rename_map[found] = target

    if rename_map:
        df = df.rename(columns=rename_map)

    # Validate required columns
    for required in ["ticker", "date", "indicator", "gold_value"]:
        if required not in df.columns:
            raise ValueError(
                f"Gold CSV must contain '{required}' column (or a known alias). "
                f"Found columns: {list(df.columns)}"
            )

    # Normalise types
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["gold_value"] = pd.to_numeric(df["gold_value"], errors="coerce")
    df = df.dropna(subset=["ticker", "date", "indicator", "gold_value"]).copy()

    return df.sort_values(["ticker", "date", "indicator"]).reset_index(drop=True)


def _load_gold_frame(gold_csv: Path) -> pd.DataFrame:
    """Load gold frame with caching."""
    return _load_gold_frame_cached(str(gold_csv.expanduser().resolve()))


def gold_values_for_ticker_date(
    ticker: str,
    analysis_date: str | None,
    gold_csv: Path | None = None,
) -> Dict[str, float] | None:
    """
    Retrieve gold indicator values for a specific ticker and date.

    Uses the closest date on or before analysis_date.  Returns None if
    no gold data exists for the ticker.

    Args:
        ticker:        Stock ticker symbol (e.g. "AAPL").
        analysis_date: Target date string (YYYY-MM-DD).  Uses latest if None.
        gold_csv:      Path to gold CSV.  Uses default ROIC benchmark if None.

    Returns:
        Dict mapping indicator name -> gold value, or None if no data found.
    """
    csv_path = default_gold_csv() if gold_csv is None else gold_csv
    if not csv_path.exists():
        return None

    gold = _load_gold_frame(gold_csv=csv_path)
    ticker_u = str(ticker).upper().strip()
    ticker_gold = gold[gold["ticker"] == ticker_u]
    if ticker_gold.empty:
        return None

    # Find the best matching date (on or before analysis_date)
    if analysis_date:
        cutoff = pd.to_datetime(analysis_date, errors="coerce")
        if pd.isna(cutoff):
            eligible = ticker_gold
        else:
            eligible = ticker_gold[ticker_gold["date"] <= cutoff]
            if eligible.empty:
                # No gold data on or before this date
                return None
    else:
        eligible = ticker_gold

    # Use the most recent available date
    latest_date = eligible["date"].max()
    latest_rows = eligible[eligible["date"] == latest_date]

    # Build indicator -> value dict from the long-format rows
    result: Dict[str, float] = {}
    for _, row in latest_rows.iterrows():
        indicator_name = str(row["indicator"]).strip()
        result[indicator_name] = float(row["gold_value"])

    return result if result else None


# ── Public scoring entry point ───────────────────────────────────────────────

def output_metrics_for_prediction(
    ticker: str,
    analysis_date: str | None,
    y_pred: Dict[str, float],
    gold_csv: Path | None = None,
) -> Dict[str, float] | None:
    """
    Compute scoring metrics for a single prediction against gold data.

    Returns None if no gold data is available for the given ticker/date
    (e.g. gold only covers 2026-02-25 but analysis_date is 2025-03-31).

    Returns:
        Dict with keys: nmae, penalty_rate, score_one, alpha, n_scored_indicators
    """
    y_true = gold_values_for_ticker_date(
        ticker=ticker,
        analysis_date=analysis_date,
        gold_csv=gold_csv,
    )
    if y_true is None:
        return None

    nmae_value, penalty_value, score_value = score_components(
        y_true=y_true, y_pred=y_pred, alpha=ALPHA,
    )
    return {
        "nmae": nmae_value,
        "penalty_rate": penalty_value,
        "score_one": score_value,
        "alpha": ALPHA,
        "n_scored_indicators": len(y_true),
    }

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from financial_agents.us_indicator_schema import Indicator


ALPHA = 10.0


def indicator_keys() -> list[str]:
    return [str(i) for i in Indicator]


def nmae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float).reshape(-1)
    y_pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if y_true.shape != y_pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if y_true.size == 0:
        return float("nan")

    denom = float(np.max(y_true) - np.min(y_true))
    if denom == 0.0:
        return float("inf")

    n = float(y_true.size)
    abs_err_sum = float(np.sum(np.abs(y_true - y_pred)))
    return float((abs_err_sum / n) / denom)


def penalty_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float).reshape(-1)
    y_pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if y_true.shape != y_pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if y_true.size == 0:
        return float("nan")

    n = float(y_true.size)
    misses = float(np.sum((y_pred == 0.0) & (y_true != 0.0)))
    return float(misses / n)


def score_components(
    y_true: Dict[str, float],
    y_pred: Dict[str, float],
    alpha: float = ALPHA,
) -> Tuple[float, float, float]:
    keys = indicator_keys()
    yt = np.array([float(y_true.get(k, 0.0)) for k in keys], dtype=float)
    yp = np.array([float(y_pred.get(k, 0.0)) for k in keys], dtype=float)
    nmae_value = nmae(y_true=yt, y_pred=yp)
    penalty_value = penalty_rate(y_true=yt, y_pred=yp)
    score_value = float(nmae_value + (float(alpha) * penalty_value))
    return nmae_value, penalty_value, score_value


def default_gold_csv() -> Path:
    # .../openai-agent/experiments/final_report2025/scoring.py -> parents[3] is Financial-D2T-Agent
    return (
        Path(__file__).resolve().parents[3]
        / "data"
        / "processed"
        / "panel"
        / "daily_panel_prices_returns_fundamentals.csv"
    )


@lru_cache(maxsize=4)
def _load_gold_frame_cached(gold_csv_resolved: str) -> pd.DataFrame:
    gold_csv = Path(gold_csv_resolved)
    gold = pd.read_csv(gold_csv, low_memory=False)
    if "ticker" not in gold.columns or "date" not in gold.columns:
        raise ValueError("gold_csv must contain columns: ticker, date")

    gold["date"] = pd.to_datetime(gold["date"], errors="coerce")
    gold = gold.dropna(subset=["date"]).copy()
    gold["ticker"] = gold["ticker"].astype(str).str.upper().str.strip()

    for key in indicator_keys():
        if key not in gold.columns:
            gold[key] = 0.0
        gold[key] = pd.to_numeric(gold[key], errors="coerce").fillna(0.0)

    return gold.sort_values(["ticker", "date"]).reset_index(drop=True)


def _load_gold_frame(gold_csv: Path) -> pd.DataFrame:
    return _load_gold_frame_cached(str(gold_csv.expanduser().resolve()))


def gold_values_for_ticker_date(
    ticker: str,
    analysis_date: str | None,
    gold_csv: Path | None = None,
) -> Dict[str, float] | None:
    csv_path = default_gold_csv() if gold_csv is None else gold_csv
    if not csv_path.exists():
        return None

    gold = _load_gold_frame(gold_csv=csv_path)
    ticker_u = str(ticker).upper().strip()
    ticker_gold = gold[gold["ticker"] == ticker_u]
    if ticker_gold.empty:
        return None

    if analysis_date:
        cutoff = pd.to_datetime(analysis_date, errors="coerce")
        if pd.isna(cutoff):
            chosen = ticker_gold.iloc[-1]
        else:
            eligible = ticker_gold[ticker_gold["date"] <= cutoff]
            chosen = eligible.iloc[-1] if not eligible.empty else ticker_gold.iloc[-1]
    else:
        chosen = ticker_gold.iloc[-1]

    return {k: float(chosen[k]) for k in indicator_keys()}


def output_metrics_for_prediction(
    ticker: str,
    analysis_date: str | None,
    y_pred: Dict[str, float],
    gold_csv: Path | None = None,
) -> Dict[str, float] | None:
    y_true = gold_values_for_ticker_date(
        ticker=ticker,
        analysis_date=analysis_date,
        gold_csv=gold_csv,
    )
    if y_true is None:
        return None

    nmae_value, penalty_value, score_value = score_components(y_true=y_true, y_pred=y_pred, alpha=ALPHA)
    return {
        "nmae": nmae_value,
        "penalty_rate": penalty_value,
        "score_one": score_value,
        "alpha": ALPHA,
    }

"""Evaluation aligned with the paper:
- Score(y, yhat) = NMAE(y, yhat) + alpha * PenaltyRate(y, yhat), with alpha=10
- Token efficiency = average consumed tokens per analysis
- Reported approach values are averages over repeated runs

Additional mode:
- Table-2 style gold-benchmark diagnostics with per-indicator normalized error.
  For each indicator, normalization follows gold-fitted MinMax scaling:
    scaled_gold = (gold - min_gold) / (max_gold - min_gold)
    scaled_pred = (pred - min_gold) / (max_gold - min_gold)
    nMAE_indicator = mean(abs(scaled_pred - scaled_gold))
  If max_gold == min_gold, fallback to absolute percentage error.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Ensure imports work when this file is executed directly.
OPENAI_AGENT_DIR = Path(__file__).resolve().parents[2]
if str(OPENAI_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(OPENAI_AGENT_DIR))

try:
    from financial_agents.us_indicator_schema import Indicator
except ModuleNotFoundError:
    # Fallback for evaluation-only environments without the Agents SDK dependency chain.
    schema_path = OPENAI_AGENT_DIR / "financial_agents" / "us_indicator_schema.py"
    spec = importlib.util.spec_from_file_location("us_indicator_schema_fallback", schema_path)
    if spec is None or spec.loader is None:
        raise
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    Indicator = module.Indicator
from experiments import Model


ALPHA = 10.0
EPS = 1e-12

# Indicators where optional robust clipping can reduce outlier domination.
RATIO_INDICATORS = {
    "P_E",
    "P_B",
    "P_EBIT",
    "PriceToSales",
    "PriceToAssets",
    "PriceToWorkingCapital",
    "PriceToNetCurrentAssets",
    "EV_EBIT",
    "EV_EBITDA",
    "GrossMargin",
    "EBITMargin",
    "NetMargin",
    "EBIT_Assets",
    "ROE",
    "ROIC",
    "CurrentRatio",
    "GrossDebt_Equity",
    "AssetTurnover",
    "BVPS",
    "EPS",
}


def _indicator_keys() -> List[str]:
    return [str(i) for i in Indicator]


def _parse_output_filename(name: str) -> Tuple[str, str, int, str | None]:
    m = re.match(r"^(?P<left>.+)_output_(?P<run_id>\d+)\.json$", name)
    if m is None:
        raise ValueError(f"Unexpected output filename format: {name}")

    left = m.group("left")
    run_id = int(m.group("run_id"))

    m_left = re.match(r"^(?P<ticker>[A-Za-z0-9.\-]+?)(?:_(?P<analysis_date>\d{4}-\d{2}-\d{2}))?$", left)
    if m_left is None:
        return left.upper(), left, run_id, None

    ticker = m_left.group("ticker").upper()
    analysis_date = m_left.group("analysis_date")
    return ticker, left, run_id, analysis_date


def _load_pred_outputs(folder: Path) -> List[Tuple[str, int, str | None, Dict[str, float], Dict[str, int]]]:
    """
    Load Thiago-style per-stock outputs:
      - <ticker>_output_<k>.json contains {"indicators":[{"indicator":..., "value":...}, ...]}
      - <ticker>_<k>.json contains usage counters
    Returns list of (ticker, run_id, analysis_date, pred_map, usage_map)
    """
    out: List[Tuple[str, int, str | None, Dict[str, float], Dict[str, int]]] = []

    for p in sorted(folder.glob("*_output_*.json")):
        name = p.name
        try:
            ticker, stem_left, run_id, parsed_analysis_date = _parse_output_filename(name=name)
        except ValueError:
            continue

        pred_payload = json.loads(p.read_text(encoding="utf-8"))
        preds = {}
        for row in pred_payload.get("indicators", []):
            k = str(row.get("indicator", "")).strip()
            v = float(row.get("value", 0.0) or 0.0)
            preds[k] = v
        if not preds:
            continue

        payload_analysis_date = str(pred_payload.get("analysis_date", "")).strip() or None
        analysis_date = parsed_analysis_date or payload_analysis_date

        usage_file = p.parent / f"{stem_left}_{run_id}.json"
        usage_payload = json.loads(usage_file.read_text(encoding="utf-8")) if usage_file.exists() else {}
        usage = (usage_payload.get("usage") or {})
        usage_map = {
            "requests": int(usage.get("requests", 0) or 0),
            "input_tokens": int(usage.get("input_tokens", 0) or 0),
            "output_tokens": int(usage.get("output_tokens", 0) or 0),
            "total_tokens": int(usage.get("total_tokens", 0) or 0),
        }

        out.append((ticker, run_id, analysis_date, preds, usage_map))

    return out


def _normalise_recommendation(value: str) -> str:
    token = re.sub(r"\s+", " ", str(value).strip().lower())
    if not token:
        return "other"
    if "sell" in token:
        return "sell"
    if "buy" in token:
        return "buy"
    if "hold" in token or "neutral" in token:
        return "hold"
    return "other"


def _manager_recommendation_counts(folder: Path) -> Dict[str, int]:
    counts = {"buy": 0, "hold": 0, "sell": 0, "other": 0}
    for path in sorted(folder.glob("*_manager_decision_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        recommendation = str(payload.get("recommendation", "") or "")
        category = _normalise_recommendation(value=recommendation)
        counts[category] += 1
    return counts


def _nmae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    # Equation (2): NMAE(y, yhat) = (1/N) * sum_i |y_i - yhat_i| / (max_i(y_i) - min_i(y_i))
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


def _penalty_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    # Equation (3): PenaltyRate(y, yhat) = (1/N) * sum_i I(yhat_i = 0 and y_i != 0)
    y_true = np.asarray(y_true, dtype=float).reshape(-1)
    y_pred = np.asarray(y_pred, dtype=float).reshape(-1)
    if y_true.shape != y_pred.shape:
        raise ValueError("y_true and y_pred must have the same shape")
    if y_true.size == 0:
        return float("nan")

    n = float(y_true.size)
    misses = float(np.sum((y_pred == 0.0) & (y_true != 0.0)))
    return float(misses / n)


def _score(nmae_value: float, penalty_rate_value: float, alpha: float = ALPHA) -> float:
    # Equation (1): Score(y, yhat) = NMAE(y, yhat) + alpha * PenaltyRate(y, yhat)
    return float(nmae_value + (float(alpha) * penalty_rate_value))


def _mean_tokens_per_analysis(tokens: pd.Series | np.ndarray) -> float:
    # Token-efficiency metric in the paper: average consumed tokens per analysis.
    arr = pd.to_numeric(tokens, errors="coerce")
    arr = arr.dropna() if isinstance(arr, pd.Series) else arr[~np.isnan(arr)]
    if len(arr) == 0:
        return float("nan")
    return float(np.mean(arr))


def score_components_one(y_true: Dict[str, float], y_pred: Dict[str, float]) -> Tuple[float, float, float]:
    keys = [str(i) for i in Indicator]
    yt = np.array([float(y_true.get(k, 0.0)) for k in keys], dtype=float)
    yp = np.array([float(y_pred.get(k, 0.0)) for k in keys], dtype=float)
    nmae = _nmae(y_true=yt, y_pred=yp)
    penalty_rate = _penalty_rate(y_true=yt, y_pred=yp)
    score = _score(nmae_value=nmae, penalty_rate_value=penalty_rate, alpha=ALPHA)
    return nmae, penalty_rate, score


def score_one(y_true: Dict[str, float], y_pred: Dict[str, float]) -> float:
    _, _, score = score_components_one(y_true=y_true, y_pred=y_pred)
    return score


def _abs_pct_error_array(y_true: np.ndarray, y_pred: np.ndarray, eps: float = EPS) -> np.ndarray:
    denom = np.maximum(np.abs(y_true), float(eps))
    return np.abs(y_pred - y_true) / denom


def _clip_for_ratio_indicator(
    indicator: str,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    ratio_clip_quantile: float,
) -> tuple[np.ndarray, np.ndarray, bool]:
    if indicator not in RATIO_INDICATORS:
        return y_true, y_pred, False
    q = float(ratio_clip_quantile)
    if q <= 0.0:
        return y_true, y_pred, False
    q = min(max(q, 0.0), 0.49)
    if y_true.size < 3:
        return y_true, y_pred, False

    low = float(np.quantile(y_true, q))
    high = float(np.quantile(y_true, 1.0 - q))
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        return y_true, y_pred, False

    return np.clip(y_true, low, high), np.clip(y_pred, low, high), True


def _apply_table2_normalisation(
    table2_df: pd.DataFrame,
    ratio_clip_quantile: float = 0.0,
    eps: float = EPS,
) -> pd.DataFrame:
    """
    Compute pointwise table2 normalized errors so that per-indicator mean equals:
      mean(abs((pred-min_gold)/(max_gold-min_gold) - (gold-min_gold)/(max_gold-min_gold)))
    using min/max from gold values of the same indicator.
    If max_gold == min_gold, fallback to absolute percentage error.
    """
    out = table2_df.copy()
    out["normalised_error"] = np.nan
    out["abs_pct_error"] = np.nan
    out["normalisation_method"] = ""
    out["ratio_clip_applied"] = 0
    out["gold_min_for_scaling"] = np.nan
    out["gold_max_for_scaling"] = np.nan

    for indicator, g in out.groupby("indicator", as_index=False):
        idx = g.index
        y_true = g["gold"].astype(float).to_numpy()
        y_pred = g["predicted"].astype(float).to_numpy()

        y_true_work, y_pred_work, clipped = _clip_for_ratio_indicator(
            indicator=indicator,
            y_true=y_true,
            y_pred=y_pred,
            ratio_clip_quantile=ratio_clip_quantile,
        )

        abs_pct = _abs_pct_error_array(y_true=y_true, y_pred=y_pred, eps=eps)
        gold_min = float(np.min(y_true_work))
        gold_max = float(np.max(y_true_work))
        denom = float(gold_max - gold_min)

        if denom > float(eps):
            scaled_true = (y_true_work - gold_min) / denom
            scaled_pred = (y_pred_work - gold_min) / denom
            norm_err = np.abs(scaled_pred - scaled_true)
            method = "minmax_gold"
        else:
            norm_err = abs_pct
            method = "ape_fallback_zero_range"

        out.loc[idx, "normalised_error"] = norm_err
        out.loc[idx, "abs_pct_error"] = abs_pct
        out.loc[idx, "normalisation_method"] = method
        out.loc[idx, "ratio_clip_applied"] = int(clipped)
        out.loc[idx, "gold_min_for_scaling"] = gold_min
        out.loc[idx, "gold_max_for_scaling"] = gold_max

    out["ratio_clip_applied"] = out["ratio_clip_applied"].astype(int)
    return out


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _normalise_source_name(s: str) -> str:
    token = _norm_token(s)
    if token in {"roic", "roicai"}:
        return "roic"
    if token in {"google", "googlefinance"}:
        return "google"
    if token in {"gurufocus", "guru"}:
        return "gurufocus"
    if token in {"yahoo", "yahoofinance"}:
        return "yahoo"
    if token in {"gold", "benchmark"}:
        return "gold"
    return token or "gold"


def _indicator_name_aliases() -> Dict[str, str]:
    """
    Map normalized indicator aliases to canonical Indicator enum names.
    """
    canonical = _indicator_keys()
    alias_map: Dict[str, str] = {}

    for key in canonical:
        alias_map[_norm_token(key)] = key

    # Common source-side aliases.
    extras = {
        "stockholdersequity": "ShareholdersEquity",
        "shareholderequity": "ShareholdersEquity",
        "commonstockequity": "ShareholdersEquity",
        "bookvaluepershare": "BVPS",
        "bookvalpershare": "BVPS",
        "sharesoutstanding": "CommonStockSharesOutstanding",
        "basiceps": "EPS",
        "earningspersharebasic": "EPS",
        "pe": "P_E",
        "peratio": "P_E",
        "pricetoearnings": "P_E",
        "pb": "P_B",
        "pbratio": "P_B",
        "pricetobook": "P_B",
        "pricetosalesratio": "PriceToSales",
        "psratio": "PriceToSales",
        "revenues": "NetRevenue_TTM",
        "totalrevenue": "NetRevenue_TTM",
        "revenue": "NetRevenue_TTM",
        "netincome": "NetProfit_TTM",
        "operatingincome": "EBIT_TTM",
        "cash": "CashAndEquivalents",
        "totalliabilities": "GrossDebt",
    }
    alias_map.update(extras)
    return alias_map


def _canonical_indicator_name(name: str) -> str | None:
    alias_map = _indicator_name_aliases()
    return alias_map.get(_norm_token(name))


def _parse_source_priority(raw: str) -> list[str]:
    if not raw:
        return []
    return [_normalise_source_name(s) for s in raw.split(",") if str(s).strip()]


def _load_gold_benchmark_long(gold_benchmark_csv: Path) -> pd.DataFrame:
    """
    Load benchmark values from CSV in either shape:

    1) Long:
       ticker,date,indicator,gold_value,source
       (aliases accepted for gold_value: value,gold)
    2) Wide:
       ticker,date,[indicator columns...],source(optional)
    """
    df = pd.read_csv(gold_benchmark_csv, low_memory=False).copy()
    norm_cols = {_norm_token(c): c for c in df.columns}

    def _find_col(candidates: list[str]) -> str | None:
        for cand in candidates:
            found = norm_cols.get(_norm_token(cand))
            if found:
                return found
        return None

    ticker_col = _find_col(["ticker", "symbol"])
    date_col = _find_col(["date", "analysis_date", "analysisdate", "as_of_date", "asofdate", "target_date"])
    if ticker_col is None or date_col is None:
        raise ValueError("gold benchmark csv must contain ticker and date (or aliases such as symbol/as_of_date).")

    rename_map = {}
    if ticker_col != "ticker":
        rename_map[ticker_col] = "ticker"
    if date_col != "date":
        rename_map[date_col] = "date"

    source_col = _find_col(["source"])
    if source_col and source_col != "source":
        rename_map[source_col] = "source"

    capture_date_col = _find_col(["capture_date", "capturedate", "captured_at", "snapshot_date"])
    if capture_date_col and capture_date_col != "capture_date":
        rename_map[capture_date_col] = "capture_date"

    units_col = _find_col(["units", "unit"])
    if units_col and units_col != "units":
        rename_map[units_col] = "units"

    if rename_map:
        df = df.rename(columns=rename_map)

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "capture_date" in df.columns:
        df["capture_date"] = pd.to_datetime(df["capture_date"], errors="coerce")
    df = df.dropna(subset=["ticker", "date"]).copy()
    if "source" not in df.columns:
        df["source"] = "gold"
    df["source"] = df["source"].astype(str).apply(_normalise_source_name)

    indicator_set = set(_indicator_keys())

    indicator_col = _find_col(["indicator", "metric", "metric_name", "metricname"])
    value_col = _find_col(["gold_value", "value", "gold", "metric_value", "metricvalue"])
    if indicator_col and value_col:
        keep_cols = ["ticker", "date", "source", indicator_col, value_col]
        if "capture_date" in df.columns:
            keep_cols.append("capture_date")
        if "units" in df.columns:
            keep_cols.append("units")
        out = df[keep_cols].copy()
        out = out.rename(columns={indicator_col: "indicator", value_col: "gold_value"})
        out["indicator"] = out["indicator"].astype(str).map(_canonical_indicator_name)
        out = out[out["indicator"].isin(indicator_set)].copy()
        out["gold_value"] = pd.to_numeric(out["gold_value"], errors="coerce")
        out = out.dropna(subset=["gold_value"]).copy()
        sort_cols = ["ticker", "date", "source", "indicator"]
        if "capture_date" in out.columns:
            sort_cols.append("capture_date")
        out = out.sort_values(sort_cols).drop_duplicates(
            subset=["ticker", "date", "source", "indicator"], keep="last"
        )
        return out.reset_index(drop=True)

    id_cols = {"ticker", "date", "source", "capture_date", "units"}
    mapped_cols: list[tuple[str, str, int]] = []
    for idx, col in enumerate(df.columns):
        if col in id_cols:
            continue
        canonical = _canonical_indicator_name(col)
        if canonical is None:
            continue
        if canonical not in indicator_set:
            continue
        mapped_cols.append((col, canonical, idx))

    if not mapped_cols:
        raise ValueError(
            "gold benchmark csv did not match long format and has no indicator columns for wide format."
        )

    source_cols = [m[0] for m in mapped_cols]
    id_vars = ["ticker", "date", "source"]
    if "capture_date" in df.columns:
        id_vars.append("capture_date")
    if "units" in df.columns:
        id_vars.append("units")
    out = df[[*id_vars, *source_cols]].melt(
        id_vars=id_vars,
        var_name="indicator_col",
        value_name="gold_value",
    )
    col_to_indicator = {col: canonical for col, canonical, _ in mapped_cols}
    col_to_rank = {col: rank for col, _, rank in mapped_cols}

    out["indicator"] = out["indicator_col"].map(col_to_indicator)
    out["col_rank"] = out["indicator_col"].map(col_to_rank).astype(int)
    out["gold_value"] = pd.to_numeric(out["gold_value"], errors="coerce")
    out = out.dropna(subset=["indicator", "gold_value"]).copy()
    out = out.sort_values(["ticker", "date", "source", "indicator", "col_rank"])
    out = out.drop_duplicates(subset=["ticker", "date", "source", "indicator"], keep="first")
    out_cols = ["ticker", "date", "source", "indicator", "gold_value"]
    if "capture_date" in out.columns:
        out_cols.append("capture_date")
    if "units" in out.columns:
        out_cols.append("units")
    out = out[out_cols].copy()
    return out.reset_index(drop=True)


def _pick_gold_row(
    gold_long: pd.DataFrame,
    ticker: str,
    indicator: str,
    analysis_date: str | None,
    source_priority: list[str],
    date_match: str,
    fixed_date: str | None = None,
) -> tuple[float | None, str | None, str | None, str | None, str | None]:
    ticker_u = str(ticker).upper().strip()
    target_date_raw = fixed_date if fixed_date else analysis_date
    target_dt = pd.to_datetime(target_date_raw, errors="coerce")
    if pd.isna(target_dt):
        return None, None, None, None, None

    candidates = gold_long[
        (gold_long["ticker"] == ticker_u) & (gold_long["indicator"] == indicator)
    ].copy()
    if candidates.empty:
        return None, None, None, None, None

    if date_match == "exact":
        candidates = candidates[candidates["date"] == target_dt].copy()
    else:
        candidates = candidates[candidates["date"] <= target_dt].copy()
        if not candidates.empty:
            latest = candidates["date"].max()
            candidates = candidates[candidates["date"] == latest].copy()

    if candidates.empty:
        return None, None, None, None, None

    if source_priority:
        rank = {s: i for i, s in enumerate(source_priority)}
        candidates["source_rank"] = candidates["source"].map(rank).fillna(len(rank)).astype(int)
        candidates = candidates.sort_values(["source_rank"]).reset_index(drop=True)
    else:
        candidates = candidates.reset_index(drop=True)

    row = candidates.iloc[0]
    value = float(row["gold_value"])
    source = str(row["source"])
    gold_date = pd.to_datetime(row["date"], errors="coerce")
    gold_date_str = gold_date.strftime("%Y-%m-%d") if not pd.isna(gold_date) else None
    capture_date_str = None
    if "capture_date" in row:
        capture_dt = pd.to_datetime(row["capture_date"], errors="coerce")
        capture_date_str = capture_dt.strftime("%Y-%m-%d") if not pd.isna(capture_dt) else None
    units = None
    if "units" in row:
        units = str(row["units"]) if pd.notna(row["units"]) else None
    return value, source, gold_date_str, capture_date_str, units


def evaluate_table2(
    pred_folder: Path,
    gold_benchmark_csv: Path,
    source_priority: list[str],
    date_match: str = "exact",
    fixed_date: str | None = None,
    ratio_clip_quantile: float = 0.0,
) -> pd.DataFrame:
    """
    Build Table-2 style rows:
      ticker, analysis_date, run_id, indicator, predicted, gold, source, gold_date,
      capture_date, units, normalised_error, abs_pct_error, zero_penalty
    """
    preds = _load_pred_outputs(folder=pred_folder)
    if not preds:
        return pd.DataFrame()

    gold_long = _load_gold_benchmark_long(gold_benchmark_csv=gold_benchmark_csv)
    if gold_long.empty:
        return pd.DataFrame()

    keys = _indicator_keys()
    rows = []
    for ticker, run_id, analysis_date, pred_map, usage in preds:
        if fixed_date:
            analysis_dt = pd.to_datetime(analysis_date, errors="coerce")
            fixed_dt = pd.to_datetime(fixed_date, errors="coerce")
            if pd.isna(analysis_dt) or pd.isna(fixed_dt):
                continue
            if analysis_dt.normalize() != fixed_dt.normalize():
                continue

        target_date = fixed_date if fixed_date else analysis_date
        for indicator in keys:
            pred_value = float(pred_map.get(indicator, 0.0))
            gold_value, source, gold_date, capture_date, units = _pick_gold_row(
                gold_long=gold_long,
                ticker=ticker,
                indicator=indicator,
                analysis_date=analysis_date,
                source_priority=source_priority,
                date_match=date_match,
                fixed_date=fixed_date,
            )
            if gold_value is None:
                continue

            zero_penalty = int((pred_value == 0.0) and (gold_value != 0.0))
            rows.append(
                {
                    "ticker": ticker,
                    "analysis_date": target_date,
                    "run_id": run_id,
                    "indicator": indicator,
                    "predicted": pred_value,
                    "gold": gold_value,
                    "source": source,
                    "gold_date": gold_date,
                    "capture_date": capture_date,
                    "units": units,
                    "zero_penalty": zero_penalty,
                    "input_tokens": usage["input_tokens"],
                    "output_tokens": usage["output_tokens"],
                    "total_tokens": usage["total_tokens"],
                    "requests": usage["requests"],
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = _apply_table2_normalisation(
        table2_df=out,
        ratio_clip_quantile=ratio_clip_quantile,
        eps=EPS,
    )

    out = out.sort_values(["ticker", "analysis_date", "run_id", "indicator"]).reset_index(drop=True)
    return out


def summarise_table2_by_indicator(table2_df: pd.DataFrame) -> pd.DataFrame:
    """
    Indicator-level evaluation required for Table-2 style analysis.
    Uses per-indicator normalized error from gold-fitted MinMax scaling:
      nmae_indicator = mean(pointwise_normalised_error)
      penalty_rate_indicator = mean(zero_penalty)
      score_one = nmae_indicator + alpha * penalty_rate_indicator
    """
    if table2_df.empty:
        return pd.DataFrame()

    rows = []
    for indicator, g in table2_df.groupby("indicator", as_index=False):
        nmae_val = float(pd.to_numeric(g["normalised_error"], errors="coerce").mean())
        median_ape = float(pd.to_numeric(g["abs_pct_error"], errors="coerce").median())
        penalty_val = float(pd.to_numeric(g["zero_penalty"], errors="coerce").mean())
        score_val = _score(nmae_value=nmae_val, penalty_rate_value=penalty_val, alpha=ALPHA)

        notes: list[str] = []
        methods = set(g["normalisation_method"].astype(str))
        if "ape_fallback_zero_range" in methods:
            notes.append("constant_gold_range_fallback")
        if int(pd.to_numeric(g["ratio_clip_applied"], errors="coerce").fillna(0).max()) == 1:
            notes.append("ratio_clip_applied")

        analysis_dt = pd.to_datetime(g["analysis_date"], errors="coerce")
        gold_dt = pd.to_datetime(g["gold_date"], errors="coerce")
        capture_dt = pd.to_datetime(g["capture_date"], errors="coerce")
        valid_dates = (~analysis_dt.isna()) & (~gold_dt.isna())
        if bool(valid_dates.any()):
            mismatch = (analysis_dt[valid_dates].dt.normalize() != gold_dt[valid_dates].dt.normalize()).any()
            if bool(mismatch):
                notes.append("date_alignment_on_or_before")

        capture_non_na = capture_dt.dropna()
        capture_unique_count = int(capture_non_na.nunique()) if not capture_non_na.empty else 0
        capture_date_min = capture_non_na.min().strftime("%Y-%m-%d") if not capture_non_na.empty else None
        capture_date_max = capture_non_na.max().strftime("%Y-%m-%d") if not capture_non_na.empty else None

        note_text = ";".join(notes)
        rows.append(
            {
                "indicator": indicator,
                "n_points": int(len(g)),
                "n_rows": int(len(g)),
                "n_tickers": int(g["ticker"].nunique()),
                "n_analysis_dates": int(g["analysis_date"].nunique()),
                "mean_normalised_error": nmae_val,
                "median_normalised_error": float(pd.to_numeric(g["normalised_error"], errors="coerce").median()),
                "nmae": nmae_val,
                "median_ape": median_ape,
                "penalty_rate": penalty_val,
                "score_one": score_val,
                "alpha": ALPHA,
                "capture_dates_unique_count": capture_unique_count,
                "capture_date_min": capture_date_min,
                "capture_date_max": capture_date_max,
                "notes": note_text,
            }
        )

    out = pd.DataFrame(rows).sort_values(["nmae", "indicator"], ascending=[True, True]).reset_index(drop=True)
    return out


def summarise_table2_overall(table2_df: pd.DataFrame) -> Dict[str, float]:
    if table2_df.empty:
        return {
            "n_rows": 0.0,
            "n_tickers": 0.0,
            "n_analysis_dates": 0.0,
            "n_runs": 0.0,
            "mean_normalised_error": float("nan"),
            "nmae": float("nan"),
            "median_ape": float("nan"),
            "penalty_rate": float("nan"),
            "score_one": float("nan"),
            "capture_dates_unique_count": 0.0,
            "capture_date_min": None,
            "capture_date_max": None,
        }

    nmae_val = float(pd.to_numeric(table2_df["normalised_error"], errors="coerce").mean())
    median_ape = float(pd.to_numeric(table2_df["abs_pct_error"], errors="coerce").median())
    penalty_val = float(pd.to_numeric(table2_df["zero_penalty"], errors="coerce").mean())
    score_val = _score(nmae_value=nmae_val, penalty_rate_value=penalty_val, alpha=ALPHA)
    capture_dt = pd.to_datetime(table2_df["capture_date"], errors="coerce").dropna()
    return {
        "n_rows": float(len(table2_df)),
        "n_tickers": float(table2_df["ticker"].nunique()),
        "n_analysis_dates": float(table2_df["analysis_date"].nunique()),
        "n_runs": float(table2_df["run_id"].nunique()),
        "mean_normalised_error": nmae_val,
        "nmae": nmae_val,
        "median_ape": median_ape,
        "penalty_rate": penalty_val,
        "score_one": score_val,
        "capture_dates_unique_count": float(capture_dt.nunique()) if not capture_dt.empty else 0.0,
        "capture_date_min": capture_dt.min().strftime("%Y-%m-%d") if not capture_dt.empty else None,
        "capture_date_max": capture_dt.max().strftime("%Y-%m-%d") if not capture_dt.empty else None,
    }


def _paper_metric_summary(df: pd.DataFrame) -> Dict[str, float]:
    """
    Paper-style aggregation:
      - First average per run_id (robustness over repeated runs).
      - Then average across runs.
    """
    if df.empty:
        return {
            "n_analyses": 0.0,
            "n_runs": 0.0,
            "mean_nmae": float("nan"),
            "mean_penalty_rate": float("nan"),
            "mean_score_one": float("nan"),
            "mean_tokens_per_analysis": float("nan"),
            "mean_total_tokens": float("nan"),
        }

    run_df = (
        df.groupby("run_id", as_index=False)
        .agg(
            run_nmae=("nmae", "mean"),
            run_penalty_rate=("penalty_rate", "mean"),
            run_score_one=("score_one", "mean"),
            run_total_tokens=("total_tokens", "mean"),
        )
        .reset_index(drop=True)
    )

    mean_tokens = _mean_tokens_per_analysis(tokens=run_df["run_total_tokens"])
    return {
        "n_analyses": float(len(df)),
        "n_runs": float(len(run_df)),
        "mean_nmae": float(run_df["run_nmae"].mean()),
        "mean_penalty_rate": float(run_df["run_penalty_rate"].mean()),
        "mean_score_one": float(run_df["run_score_one"].mean()),
        "mean_tokens_per_analysis": mean_tokens,
        # Backward-compatible alias.
        "mean_total_tokens": mean_tokens,
    }


def evaluate_folder(pred_folder: Path, gold_csv: Path) -> pd.DataFrame:
    """
    Evaluate a Thiago-style experiment folder against a gold CSV containing indicator columns.
    """
    gold = pd.read_csv(gold_csv, low_memory=False)
    if "ticker" not in gold.columns or "date" not in gold.columns:
        raise ValueError("gold_csv must contain columns: ticker, date")

    gold["date"] = pd.to_datetime(gold["date"], errors="coerce")
    gold = gold.dropna(subset=["date"]).copy()
    gold["ticker"] = gold["ticker"].astype(str).str.upper().str.strip()

    expected_cols = [str(i) for i in Indicator]
    for c in expected_cols:
        if c not in gold.columns:
            gold[c] = 0.0
        gold[c] = pd.to_numeric(gold[c], errors="coerce").fillna(0.0)
    gold = gold.sort_values(["ticker", "date"]).reset_index(drop=True)
    gold_by_ticker = {t: df for t, df in gold.groupby("ticker")}

    preds = _load_pred_outputs(folder=pred_folder)

    rows = []
    for ticker, run_id, analysis_date, pred_map, usage in preds:
        ticker_gold = gold_by_ticker.get(ticker)
        if ticker_gold is None or ticker_gold.empty:
            continue

        if analysis_date:
            cutoff = pd.to_datetime(analysis_date, errors="coerce")
            if pd.isna(cutoff):
                chosen = ticker_gold.iloc[-1]
            else:
                eligible = ticker_gold[ticker_gold["date"] <= cutoff]
                chosen = eligible.iloc[-1] if not eligible.empty else ticker_gold.iloc[-1]
        else:
            chosen = ticker_gold.iloc[-1]

        gold_values = {c: float(chosen[c]) for c in expected_cols}
        nmae, penalty_rate, s = score_components_one(y_true=gold_values, y_pred=pred_map)
        rows.append(
            {
                "ticker": ticker,
                "run_id": run_id,
                "analysis_date": analysis_date,
                "nmae": nmae,
                "penalty_rate": penalty_rate,
                "score_one": s,
                "score": s,
                "tokens_per_analysis": usage["total_tokens"],
                "total_tokens": usage["total_tokens"],
                "input_tokens": usage["input_tokens"],
                "output_tokens": usage["output_tokens"],
                "requests": usage["requests"],
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    return df


def summarise_experiment(root_results: Path, gold_csv: Path) -> pd.DataFrame:
    """
    Walk the results folder and summarise across models / architectures / reflection flags.
    Returns a table like the paper’s Table 1.
    """
    summaries = []

    for model in [m.value for m in Model]:
        for arch in ["agent", "workflow"]:
            for reflection in [False, True]:
                folder = root_results / model / f"{arch}_{reflection}"
                if not folder.exists():
                    continue
                df = evaluate_folder(pred_folder=folder, gold_csv=gold_csv)
                if df.empty:
                    continue
                paper = _paper_metric_summary(df=df)
                rec_counts = _manager_recommendation_counts(folder=folder)
                summaries.append(
                    {
                        "model": model,
                        "architecture": arch,
                        "reflection": reflection,
                        "n_analyses": int(paper["n_analyses"]),
                        "n_runs": int(paper["n_runs"]),
                        "n_buy": int(rec_counts["buy"]),
                        "n_hold": int(rec_counts["hold"]),
                        "n_sell": int(rec_counts["sell"]),
                        "mean_nmae": paper["mean_nmae"],
                        "mean_penalty_rate": paper["mean_penalty_rate"],
                        "mean_score_one": paper["mean_score_one"],
                        "mean_score": paper["mean_score_one"],
                        "mean_tokens_per_analysis": paper["mean_tokens_per_analysis"],
                        "mean_total_tokens": paper["mean_total_tokens"],
                    }
                )

    out = pd.DataFrame(summaries)
    if not out.empty:
        out = out.sort_values(["model", "architecture", "reflection"]).reset_index(drop=True)
    return out


def _default_gold_csv() -> Path:
    # .../openai-agent/experiments/final_report2025/evaluate.py -> parents[3] is Financial-D2T-Agent
    return Path(__file__).resolve().parents[3] / "data" / "processed" / "panel" / "daily_panel_prices_returns_fundamentals.csv"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate final_report2025 outputs.")
    parser.add_argument(
        "--mode",
        choices=["folder", "summary", "table2"],
        default="summary",
        help="folder: one-folder score summary; summary: scan model/arch/reflection folders; table2: per-indicator diagnostics with MinMax-normalized error.",
    )
    parser.add_argument(
        "--pred-folder",
        default="",
        help="Path to one predictions folder, e.g. results/.../gpt-5-mini/workflow_False",
    )
    parser.add_argument(
        "--results-root",
        default="",
        help="Path to results root, e.g. results/final_report2025_us_test_one_ticker",
    )
    parser.add_argument(
        "--gold-csv",
        default=str(_default_gold_csv()),
        help="Gold CSV with ticker/date + indicator columns.",
    )
    parser.add_argument(
        "--gold-benchmark-csv",
        default="",
        help="Gold benchmark CSV for table2 mode. Supports long (ticker,date,indicator,gold_value,source) or wide (ticker,date,<indicator cols>,source).",
    )
    parser.add_argument(
        "--source-priority",
        default="roic.ai,gurufocus,yahoo,gold",
        help="Comma-separated source priority for table2 mode when multiple gold values exist for same ticker/date/indicator.",
    )
    parser.add_argument(
        "--date-match",
        choices=["exact", "on_or_before"],
        default="exact",
        help="Gold date alignment in table2 mode.",
    )
    parser.add_argument(
        "--fixed-date",
        default="",
        help="Optional fixed analysis date (YYYY-MM-DD) for fair fixed-date comparisons in table2 mode.",
    )
    parser.add_argument(
        "--ratio-clip-quantile",
        type=float,
        default=0.0,
        help="Optional robust clipping for ratio indicators in table2 mode. Example 0.01 clips at [1%%,99%%]. Default 0.0 (disabled).",
    )
    parser.add_argument(
        "--out-csv",
        default="",
        help="Optional output CSV. For folder mode: per-analysis rows. For summary mode: summary table. For table2 mode: Table-2 rows.",
    )
    parser.add_argument(
        "--summary-out-csv",
        default="",
        help="Optional path to save per-indicator summary table in table2 mode.",
    )
    args = parser.parse_args()
    if args.ratio_clip_quantile < 0.0 or args.ratio_clip_quantile >= 0.5:
        raise SystemExit("--ratio-clip-quantile must be in [0.0, 0.5).")

    if args.mode != "table2":
        gold_csv = Path(args.gold_csv).expanduser().resolve()
        if not gold_csv.exists():
            raise SystemExit(f"Gold CSV not found: {gold_csv}")

    if args.mode == "folder":
        if not args.pred_folder:
            raise SystemExit("--pred-folder is required when --mode folder")
        pred_folder = Path(args.pred_folder).expanduser().resolve()
        if not pred_folder.exists():
            raise SystemExit(f"Prediction folder not found: {pred_folder}")

        df = evaluate_folder(pred_folder=pred_folder, gold_csv=gold_csv)
        if df.empty:
            print("No indicator predictions found for scoring.")
        else:
            paper = _paper_metric_summary(df=df)
            print(df.to_string(index=False))
            print(
                "\nIndicator summary: "
                f"n_analyses={int(paper['n_analyses'])}, n_runs={int(paper['n_runs'])}, "
                f"mean_nmae={paper['mean_nmae']:.6f}, "
                f"mean_penalty_rate={paper['mean_penalty_rate']:.6f}, "
                f"mean_score_one={paper['mean_score_one']:.6f}, "
                f"mean_score={paper['mean_score_one']:.6f}, "
                f"mean_tokens_per_analysis={paper['mean_tokens_per_analysis']:.1f}, "
                f"mean_total_tokens={paper['mean_total_tokens']:.1f}"
            )
            if args.out_csv:
                out_csv = Path(args.out_csv).expanduser().resolve()
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_csv, index=False)
                print(f"Saved indicator evaluation to {out_csv}")
        return 0

    if args.mode == "table2":
        if not args.pred_folder:
            raise SystemExit("--pred-folder is required when --mode table2")
        if not args.gold_benchmark_csv:
            raise SystemExit("--gold-benchmark-csv is required when --mode table2")

        pred_folder = Path(args.pred_folder).expanduser().resolve()
        if not pred_folder.exists():
            raise SystemExit(f"Prediction folder not found: {pred_folder}")

        gold_benchmark_csv = Path(args.gold_benchmark_csv).expanduser().resolve()
        if not gold_benchmark_csv.exists():
            raise SystemExit(f"Gold benchmark CSV not found: {gold_benchmark_csv}")

        source_priority = _parse_source_priority(raw=args.source_priority)
        fixed_date = args.fixed_date.strip() or None

        table2_df = evaluate_table2(
            pred_folder=pred_folder,
            gold_benchmark_csv=gold_benchmark_csv,
            source_priority=source_priority,
            date_match=args.date_match,
            fixed_date=fixed_date,
            ratio_clip_quantile=float(args.ratio_clip_quantile),
        )
        if table2_df.empty:
            print("No Table-2 rows produced. Check predictions, benchmark CSV, and date/source filters.")
            return 0

        indicator_summary = summarise_table2_by_indicator(table2_df=table2_df)
        overall = summarise_table2_overall(table2_df=table2_df)

        print("Table-2 rows (sample):")
        print(table2_df.head(40).to_string(index=False))
        print(f"\nTable-2 normalisation: minmax-on-gold; ratio_clip_quantile={float(args.ratio_clip_quantile):.4f}")
        print(
            "\nTable-2 overall: "
            f"n_rows={int(overall['n_rows'])}, n_tickers={int(overall['n_tickers'])}, "
            f"n_analysis_dates={int(overall['n_analysis_dates'])}, n_runs={int(overall['n_runs'])}, "
            f"mean_normalised_error={overall['mean_normalised_error']:.6f}, "
            f"nmae={overall['nmae']:.6f}, median_ape={overall['median_ape']:.6f}, "
            f"penalty_rate={overall['penalty_rate']:.6f}, "
            f"score_one={overall['score_one']:.6f}, "
            f"capture_dates_unique_count={int(overall['capture_dates_unique_count'])}, "
            f"capture_date_min={overall['capture_date_min']}, "
            f"capture_date_max={overall['capture_date_max']}"
        )
        print("\nPer-indicator summary (Table-2 style):")
        print(indicator_summary.to_string(index=False))

        if args.out_csv:
            out_csv = Path(args.out_csv).expanduser().resolve()
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            table2_df.to_csv(out_csv, index=False)
            print(f"Saved Table-2 rows to {out_csv}")

        if args.summary_out_csv:
            summary_out_csv = Path(args.summary_out_csv).expanduser().resolve()
            summary_out_csv.parent.mkdir(parents=True, exist_ok=True)
            indicator_summary.to_csv(summary_out_csv, index=False)
            print(f"Saved Table-2 indicator summary to {summary_out_csv}")
        return 0

    if not args.results_root:
        raise SystemExit("--results-root is required when --mode summary")
    results_root = Path(args.results_root).expanduser().resolve()
    if not results_root.exists():
        raise SystemExit(f"Results root not found: {results_root}")

    summary_df = summarise_experiment(root_results=results_root, gold_csv=gold_csv)
    if summary_df.empty:
        print("No indicator experiment folders found for summary.")
    else:
        print(summary_df.to_string(index=False))
        if args.out_csv:
            out_csv = Path(args.out_csv).expanduser().resolve()
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            summary_df.to_csv(out_csv, index=False)
            print(f"Saved indicator summary to {out_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

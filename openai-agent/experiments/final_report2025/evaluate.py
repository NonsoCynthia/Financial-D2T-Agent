"""This provides Experiment 1 evaluation output in the paper’s style: NMAE + alpha·PenaltyRate and token efficiency. 
The paper defines this composite scoring approach and uses alpha=10."""

from __future__ import annotations

import argparse
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

from financial_agents.us_indicator_schema import Indicator
from experiments import Model


ALPHA = 10.0


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
            ticker, stem_left, run_id, parsed_analysis_date = _parse_output_filename(name)
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


def _nmae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    # NMAE = mean(|y - yhat|) / (max(y) - min(y))
    denom = float(np.max(y_true) - np.min(y_true))
    if denom == 0.0:
        return float("inf")
    return float(np.mean(np.abs(y_true - y_pred)) / denom)


def _penalty_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    # mean(I(yhat == 0 and y != 0))
    return float(np.mean(((y_pred == 0.0) & (y_true != 0.0)).astype(float)))


def score_one(y_true: Dict[str, float], y_pred: Dict[str, float]) -> float:
    keys = [str(i) for i in Indicator]
    yt = np.array([float(y_true.get(k, 0.0)) for k in keys], dtype=float)
    yp = np.array([float(y_pred.get(k, 0.0)) for k in keys], dtype=float)
    return _nmae(yt, yp) + (ALPHA * _penalty_rate(yt, yp))


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

    preds = _load_pred_outputs(pred_folder)

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
        s = score_one(gold_values, pred_map)
        rows.append(
            {
                "ticker": ticker,
                "run_id": run_id,
                "analysis_date": analysis_date,
                "score": s,
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
                df = evaluate_folder(folder, gold_csv)
                if df.empty:
                    continue
                summaries.append(
                    {
                        "model": model,
                        "architecture": arch,
                        "reflection": reflection,
                        "mean_score": float(df["score"].mean()),
                        "mean_total_tokens": float(df["total_tokens"].mean()),
                    }
                )

    out = pd.DataFrame(summaries)
    if not out.empty:
        out = out.sort_values(["model", "architecture", "reflection"]).reset_index(drop=True)
    return out


def load_manager_decisions(folder: Path) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for p in sorted(folder.glob("*_output_*.json")):
        name = p.name
        try:
            ticker, _, run_id, parsed_analysis_date = _parse_output_filename(name)
        except ValueError:
            continue

        payload = json.loads(p.read_text(encoding="utf-8"))
        manager = payload.get("manager", {})
        if not isinstance(manager, dict):
            continue
        recommendation = str(manager.get("recommendation", "")).strip()
        if not recommendation:
            continue

        rows.append(
            {
                "ticker": ticker,
                "run_id": run_id,
                "analysis_date": (
                    str(manager.get("analysis_date", "")).strip()
                    or str(payload.get("analysis_date", "")).strip()
                    or parsed_analysis_date
                ),
                "recommendation": recommendation,
                "target_price": float(manager.get("target_price", 0.0) or 0.0),
                "has_justification": bool(str(manager.get("justification", "")).strip()),
            }
        )

    return pd.DataFrame(rows)


def _load_price_panel(gold_csv: Path) -> dict[str, pd.DataFrame]:
    cols = pd.read_csv(gold_csv, nrows=0).columns.tolist()
    if "ticker" not in cols or "date" not in cols:
        raise ValueError("gold_csv must contain columns: ticker, date")

    price_col = None
    for candidate in ["adj_close", "close", "Adj Close", "CLOSE"]:
        if candidate in cols:
            price_col = candidate
            break
    if price_col is None:
        raise ValueError("gold_csv must contain a price column such as adj_close or close")

    prices = pd.read_csv(gold_csv, usecols=["ticker", "date", price_col], low_memory=False)
    prices = prices.rename(columns={price_col: "price"})
    prices["ticker"] = prices["ticker"].astype(str).str.upper().str.strip()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["price"] = pd.to_numeric(prices["price"], errors="coerce")
    prices = prices.dropna(subset=["date", "price"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    return {t: df.reset_index(drop=True) for t, df in prices.groupby("ticker")}


def _position_from_recommendation(rec: str) -> int:
    rec_u = str(rec).strip().upper()
    if rec_u == "BUY":
        return 1
    if rec_u == "SELL":
        return -1
    return 0


def _price_on_or_before(price_df: pd.DataFrame, dt: pd.Timestamp) -> float | None:
    eligible = price_df[price_df["date"] <= dt]
    if eligible.empty:
        return None
    try:
        return float(eligible.iloc[-1]["price"])
    except Exception:
        return None


def _manager_strategy_returns(
    decisions: pd.DataFrame,
    gold_csv: Path,
) -> pd.DataFrame:
    if decisions.empty:
        return pd.DataFrame()

    work = decisions.copy()
    work["analysis_date"] = pd.to_datetime(work["analysis_date"], errors="coerce")
    work = work.dropna(subset=["analysis_date"]).copy()
    if work.empty:
        return pd.DataFrame()

    work["position"] = work["recommendation"].map(_position_from_recommendation)
    work = work.sort_values(["ticker", "run_id", "analysis_date"]).reset_index(drop=True)

    prices_by_ticker = _load_price_panel(gold_csv)
    rows: List[Dict[str, object]] = []

    for (_, _), grp in work.groupby(["ticker", "run_id"], as_index=False):
        grp = grp.sort_values("analysis_date").reset_index(drop=True)
        if len(grp) < 2:
            continue

        ticker = str(grp.loc[0, "ticker"]).upper()
        price_df = prices_by_ticker.get(ticker)
        if price_df is None or price_df.empty:
            continue

        for i in range(len(grp) - 1):
            cur = grp.iloc[i]
            nxt = grp.iloc[i + 1]
            start_dt = pd.Timestamp(cur["analysis_date"])
            end_dt = pd.Timestamp(nxt["analysis_date"])

            start_px = _price_on_or_before(price_df, start_dt)
            end_px = _price_on_or_before(price_df, end_dt)
            if start_px is None or end_px is None or start_px <= 0:
                continue

            forward_ret = (end_px / start_px) - 1.0
            position = int(cur["position"])
            strategy_ret = position * forward_ret

            rows.append(
                {
                    "ticker": ticker,
                    "run_id": int(cur["run_id"]),
                    "analysis_date": start_dt.strftime("%Y-%m-%d"),
                    "next_analysis_date": end_dt.strftime("%Y-%m-%d"),
                    "recommendation": str(cur["recommendation"]),
                    "position": position,
                    "start_price": float(start_px),
                    "end_price": float(end_px),
                    "forward_return": float(forward_ret),
                    "strategy_return": float(strategy_ret),
                }
            )

    return pd.DataFrame(rows)


def _annualized_sharpe(
    returns: pd.Series,
    risk_free_annual: float,
    periods_per_year: int,
) -> float | None:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if len(clean) < 2:
        return None

    rf_period = (1.0 + float(risk_free_annual)) ** (1.0 / float(periods_per_year)) - 1.0
    excess = clean - rf_period
    std = float(excess.std(ddof=1))
    if std == 0.0:
        return None
    mean = float(excess.mean())
    return float((mean / std) * np.sqrt(float(periods_per_year)))


def summarise_manager_folder(
    folder: Path,
    gold_csv: Path | None = None,
    risk_free_annual: float = 0.0,
    periods_per_year: int = 12,
) -> pd.DataFrame:
    df = load_manager_decisions(folder)
    if df.empty:
        return df

    rec = df["recommendation"].astype(str).str.upper().str.strip()
    row: Dict[str, object] = {
        "n_decisions": int(len(df)),
        "buy_count": int((rec == "BUY").sum()),
        "sell_count": int((rec == "SELL").sum()),
        "hold_count": int((rec == "HOLD").sum()),
        "mean_target_price": float(df["target_price"].mean()),
        "justification_coverage": float(df["has_justification"].mean()),
    }

    if gold_csv is not None:
        try:
            strat = _manager_strategy_returns(df, gold_csv=gold_csv)
        except Exception:
            strat = pd.DataFrame()
        if strat.empty:
            row["n_return_obs"] = 0
            row["mean_strategy_return"] = np.nan
            row["vol_strategy_return"] = np.nan
            row["sharpe_ratio"] = np.nan
            row["hit_rate"] = np.nan
        else:
            row["n_return_obs"] = int(len(strat))
            row["mean_strategy_return"] = float(strat["strategy_return"].mean())
            row["vol_strategy_return"] = float(strat["strategy_return"].std(ddof=1)) if len(strat) > 1 else np.nan
            row["sharpe_ratio"] = _annualized_sharpe(
                strat["strategy_return"],
                risk_free_annual=risk_free_annual,
                periods_per_year=periods_per_year,
            )
            row["hit_rate"] = float((strat["strategy_return"] > 0).mean())

    out = pd.DataFrame([row])
    return out


def summarise_manager_experiment(
    root_results: Path,
    gold_csv: Path | None = None,
    risk_free_annual: float = 0.0,
    periods_per_year: int = 12,
) -> pd.DataFrame:
    summaries = []
    for model in [m.value for m in Model]:
        for arch in ["agent", "workflow"]:
            for reflection in [False, True]:
                folder = root_results / model / f"{arch}_{reflection}"
                if not folder.exists():
                    continue
                msum = summarise_manager_folder(
                    folder,
                    gold_csv=gold_csv,
                    risk_free_annual=risk_free_annual,
                    periods_per_year=periods_per_year,
                )
                if msum.empty:
                    continue
                row = msum.iloc[0].to_dict()
                row["model"] = model
                row["architecture"] = arch
                row["reflection"] = reflection
                summaries.append(row)

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
        choices=["folder", "summary"],
        default="summary",
        help="folder: evaluate one result folder; summary: scan all model/arch/reflection folders under --results-root.",
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
        "--out-csv",
        default="",
        help="Optional path to save indicator evaluation table.",
    )
    parser.add_argument(
        "--manager-out-csv",
        default="",
        help="Optional path to save manager decision summary.",
    )
    parser.add_argument(
        "--risk-free-annual",
        type=float,
        default=0.0,
        help="Annual risk-free rate for Sharpe ratio (e.g., 0.05 for 5%%).",
    )
    parser.add_argument(
        "--periods-per-year",
        type=int,
        default=12,
        help="Return periods per year for Sharpe annualization (12 for monthly decisions).",
    )
    args = parser.parse_args()

    gold_csv = Path(args.gold_csv).expanduser().resolve()
    if not gold_csv.exists():
        raise SystemExit(f"Gold CSV not found: {gold_csv}")

    if args.mode == "folder":
        if not args.pred_folder:
            raise SystemExit("--pred-folder is required when --mode folder")
        pred_folder = Path(args.pred_folder).expanduser().resolve()
        if not pred_folder.exists():
            raise SystemExit(f"Prediction folder not found: {pred_folder}")

        df = evaluate_folder(pred_folder, gold_csv)
        if df.empty:
            print("No indicator predictions found for scoring.")
        else:
            print(df.to_string(index=False))
            print(
                "\nIndicator summary: "
                f"n={len(df)}, mean_score={df['score'].mean():.6f}, "
                f"mean_total_tokens={df['total_tokens'].mean():.1f}"
            )
            if args.out_csv:
                out_csv = Path(args.out_csv).expanduser().resolve()
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_csv, index=False)
                print(f"Saved indicator evaluation to {out_csv}")

        manager_df = summarise_manager_folder(
            pred_folder,
            gold_csv=gold_csv,
            risk_free_annual=args.risk_free_annual,
            periods_per_year=args.periods_per_year,
        )
        if manager_df.empty:
            print("No manager decisions found in this folder.")
        else:
            print("\nManager summary:")
            print(manager_df.to_string(index=False))
            if args.manager_out_csv:
                manager_out = Path(args.manager_out_csv).expanduser().resolve()
                manager_out.parent.mkdir(parents=True, exist_ok=True)
                manager_df.to_csv(manager_out, index=False)
                print(f"Saved manager summary to {manager_out}")
        return 0

    if not args.results_root:
        raise SystemExit("--results-root is required when --mode summary")
    results_root = Path(args.results_root).expanduser().resolve()
    if not results_root.exists():
        raise SystemExit(f"Results root not found: {results_root}")

    summary_df = summarise_experiment(results_root, gold_csv)
    if summary_df.empty:
        print("No indicator experiment folders found for summary.")
    else:
        print(summary_df.to_string(index=False))
        if args.out_csv:
            out_csv = Path(args.out_csv).expanduser().resolve()
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            summary_df.to_csv(out_csv, index=False)
            print(f"Saved indicator summary to {out_csv}")

    manager_summary_df = summarise_manager_experiment(
        results_root,
        gold_csv=gold_csv,
        risk_free_annual=args.risk_free_annual,
        periods_per_year=args.periods_per_year,
    )
    if manager_summary_df.empty:
        print("No manager decisions found across experiment folders.")
    else:
        print("\nManager summary:")
        print(manager_summary_df.to_string(index=False))
        if args.manager_out_csv:
            manager_out = Path(args.manager_out_csv).expanduser().resolve()
            manager_out.parent.mkdir(parents=True, exist_ok=True)
            manager_summary_df.to_csv(manager_out, index=False)
            print(f"Saved manager summary to {manager_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

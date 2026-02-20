"""
Monthly evaluation for agent/workflow outputs.

Stage 1 (agent): indicator accuracy metrics (NMAE, penalty, composite).
Stage 2 (agent/workflow): simple trading simulation from manager actions.
Pass --pred_dir pointing at *_output_*.json (agent) or *_workflow_output_*.json (workflow).
"""

# run_eval_monthly.py

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from finAgents.financial_agents.indicator_mapping import (
    EVAL_INDICATORS,
    canonical_month,
    canonical_ticker,
    indicators_to_canonical_map,
    pick_gold_column,
)


def normalize_action(x: Any) -> str:
    """Canonicalise manager action to BUY/SELL/HOLD."""
    if isinstance(x, str):
        a = x.strip().upper()
    else:
        a = "HOLD"
    if a == "KEEP":
        a = "HOLD"
    if a in {"BUY", "SELL", "HOLD"}:
        return a
    return "HOLD"


def minmax_scale(x: pd.Series) -> pd.Series:
    """Min-max scale a numeric series to [0, 1]."""
    x = pd.to_numeric(x, errors="coerce")
    mn = x.min()
    mx = x.max()
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return x * 0.0
    return (x - mn) / (mx - mn)


def nmae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Normalised MAE as MAE divided by the gold range (max(gold) - min(gold))."""
    t = pd.to_numeric(y_true, errors="coerce")
    p = pd.to_numeric(y_pred, errors="coerce")

    mask = t.notna() & p.notna()
    if int(mask.sum()) == 0:
        return float("nan")

    t2 = t[mask]
    p2 = p[mask]

    denom = float(t2.max() - t2.min())
    if denom == 0.0 or np.isnan(denom):
        return float("nan")

    return float(np.mean(np.abs(t2 - p2)) / denom)


def penalty_rate(y_true: pd.Series, y_pred: pd.Series, zero_eps: float = 0.0) -> float:
    """PenaltyRate as mean(I(pred ~ 0 AND gold !~ 0)) over comparable points."""
    t = pd.to_numeric(y_true, errors="coerce")
    p = pd.to_numeric(y_pred, errors="coerce")

    mask = t.notna() & p.notna()
    if int(mask.sum()) == 0:
        return float("nan")

    t2 = t[mask]
    p2 = p[mask]

    eps = float(abs(zero_eps))
    penalised = (p2.abs() <= eps) & (t2.abs() > eps)
    return float(np.mean(penalised.astype(float)))



def composite_score(nmae_val: float, penalty_val: float, alpha: float = 10.0) -> float:
    """Composite score: NMAE + alpha * PenaltyRate."""
    if np.isnan(nmae_val) and np.isnan(penalty_val):
        return float("nan")
    n = 0.0 if np.isnan(nmae_val) else float(nmae_val)
    pr = 0.0 if np.isnan(penalty_val) else float(penalty_val)
    return float(n + (float(alpha) * pr))


def thiago_mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """Thiago-style MAE after min-max scaling with scaler fit on gold values."""
    t = pd.to_numeric(y_true, errors="coerce")
    p = pd.to_numeric(y_pred, errors="coerce")
    mask = t.notna() & p.notna()
    if int(mask.sum()) == 0:
        return float("nan")
    t2 = t[mask]
    p2 = p[mask]
    t_scaled = minmax_scale(t2)
    mn = t2.min()
    mx = t2.max()
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        p_scaled = p2 * 0.0
    else:
        p_scaled = (p2 - mn) / (mx - mn)
    return float(np.mean(np.abs(p_scaled - t_scaled)))


def _first_trading_day_per_month(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Keep exactly one row per ticker per calendar month, picking the first trading day."""
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])

    out["month"] = out[date_col].dt.to_period("M").astype(str)
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()

    out = out.sort_values(["ticker", date_col]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["ticker", "month"], keep="first").reset_index(drop=True)
    out[date_col] = out[date_col].dt.strftime("%Y-%m-%d")
    return out


def load_gold_monthly_panel(path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load gold monthly panel and enforce one record per ticker-month.

    If the file contains daily rows, we collapse to the first trading day per month.
    """
    if path is None:
        path = Path("data") / "processed" / "panel" / "monthly_panel_prices_returns_fundamentals.csv"

    df = pd.read_csv(path, low_memory=False)
    if "ticker" not in df.columns or "date" not in df.columns:
        raise ValueError("Gold panel must include 'ticker' and 'date' columns.")
    return _first_trading_day_per_month(df, date_col="date")


def build_gold_indicator_long(
    gold: pd.DataFrame, indicators: List[str]
) -> tuple[pd.DataFrame, Dict[str, Optional[str]], List[str]]:
    """
    Build long-form gold indicator table with explicit mapping diagnostics.

    Returns:
    - long dataframe columns: ticker, month, indicator, gold
    - mapping used: indicator -> chosen gold column or None
    - indicators with no usable gold column
    """
    rows: List[Dict[str, Any]] = []
    mapping_used: Dict[str, Optional[str]] = {}
    missing_gold: List[str] = []

    for ind in indicators:
        col = pick_gold_column(ind, list(gold.columns))
        mapping_used[ind] = col
        if col is None:
            missing_gold.append(ind)
            continue
        tmp = gold[["ticker", "month", col]].rename(columns={col: "gold"}).copy()
        tmp["indicator"] = ind
        tmp["gold"] = pd.to_numeric(tmp["gold"], errors="coerce")
        rows.extend(tmp.to_dict(orient="records"))

    long_df = pd.DataFrame(rows)
    if not long_df.empty:
        long_df["ticker"] = long_df["ticker"].map(canonical_ticker)
        long_df["month"] = long_df["month"].astype(str).str.strip()
        long_df["indicator"] = long_df["indicator"].astype(str).str.strip()
        long_df = long_df.dropna(subset=["gold"]).reset_index(drop=True)
    return long_df, mapping_used, missing_gold


def _detect_mode_from_folder(folder: Path) -> str:
    """Detect prediction mode by filename pattern."""
    any_workflow = any(folder.glob("*_workflow_output_*.json"))
    any_agent = any(folder.glob("*_output_*.json"))
    if any_workflow and not any_agent:
        return "workflow"
    if any_agent and not any_workflow:
        return "agent"
    if any_workflow:
        return "workflow"
    return "agent"


def load_predictions_agent(folder: Path) -> pd.DataFrame:
    """
    Load agent JSON outputs into a flat indicator table.

    Expected file pattern:
    - *_output_*.json

    Expected structure:
    - payload["outputs"][i]["analyst"]["indicators"] as list[{indicator,value}]
    """
    rows: List[Dict[str, Any]] = []

    for p in sorted(folder.glob("*_output_*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        ticker = canonical_ticker(payload.get("ticker", ""))
        outputs = payload.get("outputs", [])

        if not isinstance(outputs, list):
            continue

        for item in outputs:
            date = item.get("date")
            analyst = item.get("analyst", {}) or {}
            indicators_raw = analyst.get("indicators", []) or []
            indicators = indicators_to_canonical_map(indicators_raw)

            if not date or not indicators:
                continue

            dt = pd.to_datetime(str(date), errors="coerce")
            if pd.isna(dt):
                continue

            month = canonical_month(dt)

            for k, v in indicators.items():
                rows.append(
                    {
                        "ticker": canonical_ticker(ticker),
                        "date": dt.strftime("%Y-%m-%d"),
                        "month": month,
                        "indicator": str(k).strip(),
                        "pred_value": v,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ticker"] = df["ticker"].map(canonical_ticker)
    df["pred_value"] = pd.to_numeric(df["pred_value"], errors="coerce")
    df["indicator"] = df["indicator"].astype(str).str.strip()

    df = df.sort_values(["ticker", "indicator", "date"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["ticker", "month", "indicator"], keep="first").reset_index(drop=True)
    return df


def load_manager_actions_agent(folder: Path) -> pd.DataFrame:
    """
    Load manager actions and prices from saved agent outputs.

    Expected file pattern:
    - *_output_*.json

    Expected structure:
    - item["price"]
    - item["manager"]["decision"] (or legacy recommendation/action)
    """
    rows: List[Dict[str, Any]] = []

    for p in sorted(folder.glob("*_output_*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        ticker = canonical_ticker(payload.get("ticker", ""))
        outputs = payload.get("outputs", [])

        if not isinstance(outputs, list):
            continue

        for item in outputs:
            date = item.get("date")
            price = item.get("price")
            manager = item.get("manager", {}) or {}

            if not date:
                continue

            dt = pd.to_datetime(str(date), errors="coerce")
            if pd.isna(dt):
                continue

            action = normalize_action(
                manager.get("decision") or manager.get("action") or manager.get("recommendation")
            )

            rows.append(
                {
                    "ticker": canonical_ticker(ticker),
                    "date": dt.strftime("%Y-%m-%d"),
                    "month": canonical_month(dt),
                    "price": pd.to_numeric(price, errors="coerce"),
                    "action": action if action in {"BUY", "SELL", "HOLD"} else "HOLD",
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ticker"] = df["ticker"].map(canonical_ticker)
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values(["ticker", "date_dt"]).reset_index(drop=True)
    return df.drop(columns=["date_dt"])


def load_manager_actions_workflow(folder: Path) -> pd.DataFrame:
    """
    Load manager actions for workflow outputs.

    Expected file pattern:
    - *_workflow_output_*.json

    Expected structure:
    - payload["outputs"][i]["manager"]["decision"] (BUY|HOLD|SELL)
    - payload["outputs"][i]["date"]
    There is no "price" stored in workflow output, so we will join gold prices later.
    """
    rows: List[Dict[str, Any]] = []

    for p in sorted(folder.glob("*_workflow_output_*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        ticker = canonical_ticker(payload.get("ticker", ""))
        outputs = payload.get("outputs", [])

        if not isinstance(outputs, list):
            continue

        for item in outputs:
            date = item.get("date")
            manager = item.get("manager", {}) or {}

            if not date:
                continue

            dt = pd.to_datetime(str(date), errors="coerce")
            if pd.isna(dt):
                continue

            action = normalize_action(
                manager.get("decision") or manager.get("action") or manager.get("recommendation")
            )

            rows.append(
                {
                    "ticker": canonical_ticker(ticker),
                    "date": dt.strftime("%Y-%m-%d"),
                    "month": canonical_month(dt),
                    "action": action,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ticker"] = df["ticker"].map(canonical_ticker)
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values(["ticker", "date_dt"]).reset_index(drop=True)
    return df.drop(columns=["date_dt"])


def simulate_simple_trades(actions: pd.DataFrame, risk_free_rate_annual: float = 0.0) -> pd.DataFrame:
    """
    Simulate simple monthly trading from manager actions.

    Logic:
    - BUY: append current price to open list
    - SELL: if open list non-empty, close everything at current price using average buy price
    - HOLD: do nothing
    - Final liquidation: at last month, if positions remain open, sell at last price
    """
    if actions.empty:
        return pd.DataFrame()

    rf_monthly = float(risk_free_rate_annual) / 12.0
    results: List[Dict[str, Any]] = []

    for ticker, df in actions.groupby("ticker", sort=True):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

        open_buys: List[float] = []
        equity = 1.0
        monthly_rets: List[float] = []

        n_buys = 0
        n_sells = 0
        n_trades_closed = 0

        for _, row in df.iterrows():
            price = row.get("price")
            action = row.get("action", "HOLD")

            if pd.isna(price) or float(price) <= 0:
                monthly_rets.append(0.0)
                continue

            if action == "BUY":
                open_buys.append(float(price))
                n_buys += 1
                monthly_rets.append(0.0)
                continue

            if action == "SELL":
                n_sells += 1
                if open_buys:
                    avg_buy = float(np.mean(open_buys))  # average all open entries
                    r = (float(price) - avg_buy) / avg_buy
                    equity *= (1.0 + r)
                    open_buys = []
                    n_trades_closed += 1
                    monthly_rets.append(float(r))
                else:
                    monthly_rets.append(0.0)
                continue

            # HOLD or anything else: no trade, zero monthly return
            monthly_rets.append(0.0)

        if len(df) > 0:
            last_price = df.iloc[-1]["price"]
            if open_buys and not pd.isna(last_price) and float(last_price) > 0:
                avg_buy = float(np.mean(open_buys))  # close remaining buys at final price
                r = (float(last_price) - avg_buy) / avg_buy
                equity *= (1.0 + r)
                n_trades_closed += 1
                monthly_rets[-1] = float(monthly_rets[-1] + r)
                open_buys = []

            cum_return = float(equity - 1.0)

            rets = np.array(monthly_rets, dtype=float)
            excess = rets - rf_monthly

            std = float(np.std(excess, ddof=1)) if len(excess) > 1 else 0.0
            mean = float(np.mean(excess)) if len(excess) > 0 else 0.0
            sharpe = float((mean / std) * math.sqrt(12.0)) if std > 0 else float("nan")

            results.append(
                {
                    "ticker": ticker,
                    "cumulative_return": cum_return,
                    "sharpe_ratio": sharpe,
                    "n_months": int(len(df)),
                    "n_buys": int(n_buys),
                    "n_sells": int(n_sells),
                    "n_trades_closed": int(n_trades_closed),
                }
            )

    return pd.DataFrame(results).sort_values("ticker").reset_index(drop=True)


def load_experiment_config() -> Dict[str, Any]:
    """Load experiment config to get risk_free_rate if present."""
    p = Path("config") / "experiment.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {"risk_free_rate": 0.0}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for evaluation."""
    p = argparse.ArgumentParser()
    p.add_argument(
        "--mode",
        type=str,
        default="auto",
        choices=["auto", "agent", "workflow"],
        help="Which prediction format to evaluate",
    )
    p.add_argument(
        "--stage1",
        type=str,
        default="nmae",
        choices=["thiago", "nmae"],
        help="Stage 1 scoring: thiago (minmax + MAE) or nmae (current default).",
    )
    p.add_argument(
        "--pred_dir",
        type=str,
        default=None,
        help="Folder containing prediction JSON files",
    )
    p.add_argument(
        "--gold_csv",
        type=str,
        default="data/processed/panel/monthly_panel_prices_returns_fundamentals.csv",
        help="Gold panel CSV path",
    )
    p.add_argument(
        "--alpha",
        type=float,
        default=10.0,
        help="Penalty weight for Stage 1 composite score",
    )
    return p.parse_args()


def main() -> None:
    """
    Full monthly evaluation.

    Stage 1 (agent only):
    - NMAE
    - PenaltyRate
    - CompositeScore = NMAE + alpha * PenaltyRate

    Stage 2 (agent and workflow):
    - Cumulative return
    - Sharpe ratio
    """
    args = parse_args()

    cfg = load_experiment_config()
    rf = float(cfg.get("risk_free_rate", 0.0))
    alpha = float(args.alpha)
    stage1_indicators = list(EVAL_INDICATORS)

    gold = load_gold_monthly_panel(Path(args.gold_csv))
    gold["month"] = gold["date"].map(canonical_month)
    gold["ticker"] = gold["ticker"].map(canonical_ticker)

    if "adj_close" in gold.columns:
        gold["last_price"] = pd.to_numeric(gold["adj_close"], errors="coerce")
    elif "price" in gold.columns:
        gold["last_price"] = pd.to_numeric(gold["price"], errors="coerce")

    if args.pred_dir:
        pred_dir = Path(args.pred_dir)
    else:
        pred_dir = Path("results") / "experiments" / "monthly_workflow"

    mode = args.mode
    if mode == "auto":
        mode = _detect_mode_from_folder(pred_dir)

    if mode not in {"agent", "workflow"}:
        mode = "agent"

    if mode == "agent":
        pred = load_predictions_agent(pred_dir)
        if pred.empty:
            print(f"No agent predictions found in {pred_dir}")
            return

        gold_long, mapping_used, missing_gold = build_gold_indicator_long(gold, stage1_indicators)
        print("Stage 1 indicator mapping (gold column per indicator)")
        for ind in stage1_indicators:
            col = mapping_used.get(ind)
            print(f"- {ind}: {col if col is not None else 'MISSING'}")
        if missing_gold:
            print()
            print(
                "Stage 1 warning: missing gold columns/mappings for "
                f"{len(missing_gold)} indicator(s): {missing_gold}"
            )
        if gold_long.empty:
            raise ValueError(
                "Stage 1 aborted: no mapped gold indicators available. "
                "Add gold columns or extend GOLD_COLUMN_CANDIDATES mapping."
            )

        overall_rows: List[Dict[str, Any]] = []
        per_ticker_rows: List[Dict[str, Any]] = []

        scorer = args.stage1

        for ind in stage1_indicators:
            g = gold_long[gold_long["indicator"] == ind][["ticker", "month", "gold"]].copy()
            if g.empty:
                continue

            p = (
                pred[pred["indicator"] == ind][["ticker", "month", "pred_value"]]
                .rename(columns={"pred_value": "pred"})
                .copy()
            )

            merged = g.merge(p, on=["ticker", "month"], how="inner")
            if merged.empty:
                continue

            if scorer == "thiago":
                mae_val = thiago_mae(merged["gold"], merged["pred"])
                overall_rows.append({"indicator": ind, "mae": mae_val, "n": int(len(merged))})
                for tkr, mm in merged.groupby("ticker", sort=True):
                    per_ticker_rows.append(
                        {"ticker": tkr, "indicator": ind, "mae": thiago_mae(mm["gold"], mm["pred"]), "n": int(len(mm))}
                    )
            else:
                nm = nmae(merged["gold"], merged["pred"])
                pr = penalty_rate(merged["gold"], merged["pred"], zero_eps=0.0)
                cs = composite_score(nm, pr, alpha=alpha)

                overall_rows.append(
                    {"indicator": ind, "nmae": nm, "penalty_rate": pr, "composite": cs, "n": int(len(merged))}
                )

                for tkr, mm in merged.groupby("ticker", sort=True):
                    nm_t = nmae(mm["gold"], mm["pred"])
                    pr_t = penalty_rate(mm["gold"], mm["pred"], zero_eps=0.0)
                    cs_t = composite_score(nm_t, pr_t, alpha=alpha)
                    per_ticker_rows.append(
                        {
                            "ticker": tkr,
                            "indicator": ind,
                            "nmae": nm_t,
                            "penalty_rate": pr_t,
                            "composite": cs_t,
                            "n": int(len(mm)),
                        }
                    )

        overall_df = pd.DataFrame(overall_rows)
        per_ticker_df = pd.DataFrame(per_ticker_rows)

        if not overall_df.empty:
            overall_df = overall_df.reset_index(drop=True)
        if not per_ticker_df.empty:
            per_ticker_df = per_ticker_df.reset_index(drop=True)

        print("Stage 1 (Indicator accuracy). Overall (pooled across tickers)")
        if overall_df.empty:
            raise ValueError(
                "Stage 1 aborted: no prediction/gold overlaps after mapping and key alignment. "
                "Check ticker/month/indicator canonicalization and mappings."
            )
        print(overall_df.to_string(index=False))
        if scorer == "thiago" and not overall_df.empty:
            print(f"Stage 1 overall MAE (Thiago): {float(overall_df['mae'].mean()):.6f}")

        print()
        print("Stage 1 (Indicator accuracy). Per ticker")
        print(per_ticker_df.to_string(index=False) if not per_ticker_df.empty else "No per-ticker overlaps found.")

        actions = load_manager_actions_agent(pred_dir)

    else:
        actions = load_manager_actions_workflow(pred_dir)
        if actions.empty:
            print(f"No workflow actions found in {pred_dir}")
            return

        if "last_price" not in gold.columns:
            raise ValueError("Gold panel is missing price column required for workflow Stage 2 (need adj_close or price).")
        gold_price = gold[["ticker", "month", "last_price"]].rename(columns={"last_price": "price"}).copy()
        actions = actions.merge(gold_price, on=["ticker", "month"], how="left")

        print("Stage 1 (Indicator accuracy) skipped (workflow outputs do not include analyst indicators).")

    trade_df = simulate_simple_trades(actions, risk_free_rate_annual=rf)

    print()
    print(f"Stage 2 (Trading simulation). Results ({mode})")
    print(trade_df.to_string(index=False) if not trade_df.empty else "No manager actions found for trading simulation.")

    gold_points = len(gold)
    if mode == "agent":
        pred_points = len(load_manager_actions_agent(pred_dir).drop_duplicates(subset=["ticker", "month"]))
    else:
        pred_points = len(load_manager_actions_workflow(pred_dir).drop_duplicates(subset=["ticker", "month"]))

    print()
    print(f"Monthly points (gold): {gold_points}")
    print(f"Monthly points (pred): {pred_points}")
    print(f"Tickers (gold): {gold['ticker'].nunique()}")

    if mode == "agent":
        pred_actions = load_manager_actions_agent(pred_dir)
    else:
        pred_actions = load_manager_actions_workflow(pred_dir)

    print(f"Tickers (pred): {pred_actions['ticker'].nunique() if not pred_actions.empty else 0}")
    print(f"Tickers (overlap): {len(set(pred_actions['ticker']).intersection(set(gold['ticker'])))}")


if __name__ == "__main__":
    main()

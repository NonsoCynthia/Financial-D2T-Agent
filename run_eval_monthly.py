# run_eval_monthly.py

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Scaling and error metrics
# ----------------------------

def minmax_scale(x: pd.Series) -> pd.Series:
    """Min-max scale a numeric series to [0, 1]."""
    x = pd.to_numeric(x, errors="coerce")
    mn = x.min()
    mx = x.max()
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return x * 0.0
    return (x - mn) / (mx - mn)


def nmae(y_true: pd.Series, y_pred: pd.Series) -> float:
    """
    Normalised MAE (Thiago-style), using min-max scaling of gold values.

    This is equivalent to MAE on min-max normalised true values, which makes
    errors comparable across indicators with different magnitudes.
    """
    t = minmax_scale(y_true)
    p = minmax_scale(y_pred)
    mask = t.notna() & p.notna()
    if int(mask.sum()) == 0:
        return float("nan")
    return float(np.mean(np.abs(t[mask] - p[mask])))


def penalty_rate(y_true: pd.Series, y_pred: pd.Series, zero_eps: float = 0.0) -> float:
    """
    Penalty rate (Thiago-style idea).

    Penalises cases where:
    - gold is non-zero (|gold| > zero_eps)
    - prediction is zero or missing-like (pred is 0, or |pred| <= zero_eps, or NaN)

    Returns the fraction of penalised points among comparable points.
    """
    t = pd.to_numeric(y_true, errors="coerce")
    p = pd.to_numeric(y_pred, errors="coerce")

    comparable = t.notna()
    if int(comparable.sum()) == 0:
        return float("nan")

    gold_non_zero = comparable & (t.abs() > float(zero_eps))
    pred_zero_or_missing = p.isna() | (p.abs() <= float(zero_eps))

    penalised = gold_non_zero & pred_zero_or_missing
    denom = int(comparable.sum())
    if denom == 0:
        return float("nan")
    return float(int(penalised.sum()) / denom)


def composite_score(nmae_val: float, penalty_val: float, alpha: float = 10.0) -> float:
    """Composite score: Score = NMAE + alpha * PenaltyRate."""
    if np.isnan(nmae_val) and np.isnan(penalty_val):
        return float("nan")
    n = 0.0 if np.isnan(nmae_val) else float(nmae_val)
    pr = 0.0 if np.isnan(penalty_val) else float(penalty_val)
    return float(n + (float(alpha) * pr))


# ----------------------------
# Loading gold and predictions
# ----------------------------

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


def load_gold_monthly_panel() -> pd.DataFrame:
    """
    Load your gold monthly panel and enforce one record per ticker-month.

    Important: your file can contain daily rows. We collapse to the first trading day per month,
    which is aligned with your monthly experiment setup.
    """
    p = Path("data") / "processed" / "panel" / "monthly_panel_prices_returns_fundamentals.csv"
    df = pd.read_csv(p, low_memory=False)
    if "ticker" not in df.columns or "date" not in df.columns:
        raise ValueError("Gold panel must include 'ticker' and 'date' columns.")
    df = _first_trading_day_per_month(df, date_col="date")
    return df


def load_predictions(folder: Path) -> pd.DataFrame:
    """
    Load monthly workflow JSON outputs into a flat table.

    Returns a DataFrame with columns:
    - ticker, date, month, indicator, pred_value
    """
    rows: List[Dict[str, Any]] = []

    for p in sorted(folder.glob("*_output_*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        ticker = str(payload.get("ticker", "")).upper().strip()
        outputs = payload.get("outputs", [])

        if not isinstance(outputs, list):
            continue

        for item in outputs:
            date = item.get("date")
            analyst = item.get("analyst", {}) or {}
            indicators = analyst.get("indicators", {}) or {}

            if not date or not isinstance(indicators, dict):
                continue

            dt = pd.to_datetime(str(date), errors="coerce")
            if pd.isna(dt):
                continue

            month = str(dt.to_period("M"))

            for k, v in indicators.items():
                rows.append(
                    {
                        "ticker": ticker,
                        "date": dt.strftime("%Y-%m-%d"),
                        "month": month,
                        "indicator": str(k).strip(),
                        "pred_value": v,
                    }
                )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["pred_value"] = pd.to_numeric(df["pred_value"], errors="coerce")
    df["indicator"] = df["indicator"].astype(str).str.strip()

    df = df.sort_values(["ticker", "indicator", "date"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["ticker", "month", "indicator"], keep="first").reset_index(drop=True)

    return df


def load_manager_actions(folder: Path) -> pd.DataFrame:
    """
    Load manager actions and prices from the saved monthly workflow outputs.

    Returns:
    - ticker, date, month, price, action
    """
    rows: List[Dict[str, Any]] = []

    for p in sorted(folder.glob("*_output_*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        ticker = str(payload.get("ticker", "")).upper().strip()
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

            action = manager.get("action")
            if isinstance(action, str):
                action = action.strip().upper()
            else:
                action = "HOLD"

            rows.append(
                {
                    "ticker": ticker,
                    "date": dt.strftime("%Y-%m-%d"),
                    "month": str(dt.to_period("M")),
                    "price": pd.to_numeric(price, errors="coerce"),
                    "action": action if action in {"BUY", "SELL", "HOLD"} else "HOLD",
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values(["ticker", "date_dt"]).reset_index(drop=True)
    df = df.drop(columns=["date_dt"])
    return df


# ----------------------------
# Trading evaluation (Thiago-style)
# ----------------------------

def simulate_thiago_trades(
    actions: pd.DataFrame,
    risk_free_rate_annual: float = 0.0,
) -> pd.DataFrame:
    """
    Simulate Thiago-style monthly trading from manager actions.

    Logic:
    - BUY: append the current price to an open list (can accumulate multiple buys)
    - SELL: if open list non-empty, close everything at current price using average buy price
    - HOLD: do nothing
    - Final liquidation: at the last available month, if positions remain open, sell at last price

    Outputs per ticker:
    - cumulative_return (multiplicative equity - 1)
    - sharpe_ratio (monthly returns, annualised with sqrt(12))
    - n_months, n_buys, n_sells, n_trades_closed
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

            if pd.isna(price) or price <= 0:
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
                    avg_buy = float(np.mean(open_buys))
                    r = (float(price) - avg_buy) / avg_buy
                    equity *= (1.0 + r)
                    open_buys = []
                    n_trades_closed += 1
                    monthly_rets.append(float(r))
                else:
                    monthly_rets.append(0.0)
                continue

            monthly_rets.append(0.0)

        # Final liquidation at last price
        if len(df) > 0:
            last_price = df.iloc[-1]["price"]
            if open_buys and not pd.isna(last_price) and float(last_price) > 0:
                avg_buy = float(np.mean(open_buys))
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
        if std > 0:
            sharpe = float((mean / std) * math.sqrt(12.0))
        else:
            sharpe = float("nan")

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


# ----------------------------
# Main evaluation
# ----------------------------

def load_experiment_config() -> Dict[str, Any]:
    """Load experiment config to get risk_free_rate if present."""
    p = Path("config") / "experiment.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {"risk_free_rate": 0.0}


def main() -> None:
    """
    Full monthly evaluation aligned with Thiago:

    Stage 1:
    - NMAE (normalised MAE)
    - PenaltyRate
    - CompositeScore = NMAE + alpha * PenaltyRate
    Reported overall and per ticker.

    Stage 2:
    - Cumulative return (Thiago-style averaging buy positions, monthly)
    - Sharpe ratio (monthly, annualised)
    """
    cfg = load_experiment_config()
    rf = float(cfg.get("risk_free_rate", 0.0))
    alpha = 10.0

    gold = load_gold_monthly_panel()

    pred_dir = Path("results") / "experiments" / "monthly_workflow"
    pred = load_predictions(pred_dir)

    if pred.empty:
        print(f"No predictions found in {pred_dir}")
        return

    # Gold month key
    gold["month"] = pd.to_datetime(gold["date"], errors="coerce").dt.to_period("M").astype(str)
    gold["ticker"] = gold["ticker"].astype(str).str.upper().str.strip()

    # Ensure last_price exists in gold (from adj_close if present)
    if "adj_close" in gold.columns:
        gold["last_price"] = pd.to_numeric(gold["adj_close"], errors="coerce")
    elif "price" in gold.columns:
        gold["last_price"] = pd.to_numeric(gold["price"], errors="coerce")

    # Indicators to evaluate (Stage 1). Add more if your gold panel has them.
    stage1_indicators = [
        "Revenues",
        "NetIncomeLoss",
        "Assets",
        "Liabilities",
        "last_price",
    ]

    for col in stage1_indicators:
        if col in gold.columns:
            gold[col] = pd.to_numeric(gold[col], errors="coerce")

    # Stage 1 overall
    overall_rows: List[Dict[str, Any]] = []
    per_ticker_rows: List[Dict[str, Any]] = []

    for ind in stage1_indicators:
        if ind not in gold.columns:
            continue

        g = gold[["ticker", "month", ind]].rename(columns={ind: "gold"}).copy()
        p = pred[pred["indicator"] == ind][["ticker", "month", "pred_value"]].rename(columns={"pred_value": "pred"}).copy()

        merged = g.merge(p, on=["ticker", "month"], how="inner")
        if merged.empty:
            continue

        nm = nmae(merged["gold"], merged["pred"])
        pr = penalty_rate(merged["gold"], merged["pred"], zero_eps=0.0)
        cs = composite_score(nm, pr, alpha=alpha)

        overall_rows.append(
            {
                "indicator": ind,
                "nmae": nm,
                "penalty_rate": pr,
                "composite": cs,
                "n": int(len(merged)),
            }
        )

        # Per ticker breakdown
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
        overall_df = overall_df.sort_values(["composite", "indicator"], na_position="last").reset_index(drop=True)

    if not per_ticker_df.empty:
        per_ticker_df = per_ticker_df.sort_values(["ticker", "composite", "indicator"], na_position="last").reset_index(drop=True)

    print("Stage 1 (Indicator accuracy). Overall (pooled across tickers)")
    if overall_df.empty:
        print("No indicator overlaps found.")
    else:
        print(overall_df.to_string(index=False))

    print()
    print("Stage 1 (Indicator accuracy). Per ticker")
    if per_ticker_df.empty:
        print("No per-ticker overlaps found.")
    else:
        print(per_ticker_df.to_string(index=False))

    # Stage 2 trading evaluation
    actions = load_manager_actions(pred_dir)
    trade_df = simulate_thiago_trades(actions, risk_free_rate_annual=rf)

    print()
    print("Stage 2 (Trading simulation). Thiago-style monthly results")
    if trade_df.empty:
        print("No manager actions found for trading simulation.")
    else:
        print(trade_df.to_string(index=False))

    # Diagnostics
    gold_points = len(gold)
    pred_points = len(pred.drop_duplicates(subset=["ticker", "month"]))
    print()
    print(f"Monthly points (gold): {gold_points}")
    print(f"Monthly points (pred): {pred_points}")
    print(f"Tickers (pred): {pred['ticker'].nunique()}")
    print(f"Tickers (gold): {gold['ticker'].nunique()}")
    print(f"Tickers (overlap): {len(set(pred['ticker']).intersection(set(gold['ticker'])))}")


if __name__ == "__main__":
    main()

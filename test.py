import json
from pathlib import Path
from textwrap import dedent


def add_md(nb: dict, s: str) -> None:
    """Append a markdown cell to a notebook dict."""
    nb["cells"].append(
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": s,
        }
    )


def add_code(nb: dict, s: str) -> None:
    """Append a code cell to a notebook dict."""
    nb["cells"].append(
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "outputs": [],
            "source": s,
        }
    )


def build_notebook() -> dict:
    """Build and return the notebook structure as a Python dict."""
    nb = {
        "cells": [],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    add_md(
        nb,
        "# Monthly evaluation notebook\n\n"
        "This notebook evaluates monthly outputs for two modes:\n"
        "- **agent**: outputs include analyst indicators and manager actions\n"
        "- **workflow**: outputs include manager recommendations only, prices are joined from the gold panel\n\n"
        "Stage 1 follows Thiago style scaling, MinMaxScaler fitted on gold values.\n"
        "Stage 2 is an optional trading simulation based on BUY, SELL, HOLD actions.\n",
    )

    add_code(
        nb,
        dedent(
            """\
            # Imports
            import json
            import math
            from pathlib import Path
            from typing import Any, Dict, List, Optional, Tuple

            import numpy as np
            import pandas as pd
            from sklearn.preprocessing import MinMaxScaler
            """
        ),
    )

    add_md(nb, "## 1. Helper functions")

    add_code(
        nb,
        dedent(
            """\
            def first_trading_day_per_month(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
                \"\"\"Keep exactly one row per ticker per calendar month, picking the first trading day.\"\"\"
                out = df.copy()
                out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
                out = out.dropna(subset=[date_col])

                out["month"] = out[date_col].dt.to_period("M").astype(str)
                out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()

                out = out.sort_values(["ticker", date_col]).reset_index(drop=True)
                out = out.drop_duplicates(subset=["ticker", "month"], keep="first").reset_index(drop=True)
                out[date_col] = out[date_col].dt.strftime("%Y-%m-%d")
                return out


            def load_gold_monthly_panel(path: Path) -> pd.DataFrame:
                \"\"\"Load the gold panel and collapse to one record per ticker-month.\"\"\"
                df = pd.read_csv(path, low_memory=False)
                if "ticker" not in df.columns or "date" not in df.columns:
                    raise ValueError("Gold panel must include 'ticker' and 'date' columns.")

                df = first_trading_day_per_month(df, date_col="date")

                df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").astype(str)
                df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

                if "adj_close" in df.columns:
                    df["last_price"] = pd.to_numeric(df["adj_close"], errors="coerce")
                elif "price" in df.columns:
                    df["last_price"] = pd.to_numeric(df["price"], errors="coerce")
                elif "price_avg" in df.columns:
                    df["last_price"] = pd.to_numeric(df["price_avg"], errors="coerce")

                return df


            def detect_mode_from_folder(folder: Path) -> str:
                \"\"\"Detect prediction mode using filename patterns.\"\"\"
                any_workflow = any(folder.glob("*_workflow_output_*.json"))
                any_agent = any(folder.glob("*_output_*.json"))
                if any_workflow and not any_agent:
                    return "workflow"
                if any_agent and not any_workflow:
                    return "agent"
                if any_workflow:
                    return "workflow"
                return "agent"
            """
        ),
    )

    add_md(nb, "## 2. Load predictions")

    add_code(
        nb,
        dedent(
            """\
            def load_predictions_agent(folder: Path) -> pd.DataFrame:
                \"\"\"Load agent outputs into a flat indicator table.\"\"\"
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
                df["indicator"] = df["indicator"].astype(str).str.strip()
                df["pred_value"] = pd.to_numeric(df["pred_value"], errors="coerce")

                df = df.sort_values(["ticker", "indicator", "date"]).reset_index(drop=True)
                df = df.drop_duplicates(subset=["ticker", "month", "indicator"], keep="first").reset_index(drop=True)
                return df


            def load_actions_workflow(folder: Path) -> pd.DataFrame:
                \"\"\"Load workflow outputs into a flat action table.\"\"\"
                rows: List[Dict[str, Any]] = []

                for p in sorted(folder.glob("*_workflow_output_*.json")):
                    payload = json.loads(p.read_text(encoding="utf-8"))
                    ticker = str(payload.get("ticker", "")).upper().strip()
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

                        rec = manager.get("action")
                        if rec is None:
                            rec = manager.get("recommendation")

                        if isinstance(rec, str):
                            rec_u = rec.strip().upper()
                        else:
                            rec_u = "HOLD"

                        if rec_u == "KEEP":
                            rec_u = "HOLD"
                        if rec_u not in {"BUY", "SELL", "HOLD"}:
                            rec_u = "HOLD"

                        rows.append(
                            {
                                "ticker": ticker,
                                "date": dt.strftime("%Y-%m-%d"),
                                "month": str(dt.to_period("M")),
                                "action": rec_u,
                            }
                        )

                df = pd.DataFrame(rows)
                if df.empty:
                    return df

                df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
                df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.dropna(subset=["date_dt"]).sort_values(["ticker", "date_dt"]).reset_index(drop=True)
                return df.drop(columns=["date_dt"])
            """
        ),
    )

    add_md(nb, "## 3. Stage 1. Thiago style MAE")

    add_code(
        nb,
        dedent(
            """\
            def mae_list_minmax_fit_gold(y_true: pd.Series, y_pred: pd.Series) -> List[float]:
                \"\"\"Per point absolute errors after MinMax scaling fitted on gold values.\"\"\"
                t = pd.to_numeric(y_true, errors="coerce").to_numpy()
                p = pd.to_numeric(y_pred, errors="coerce").to_numpy()

                mask = ~np.isnan(t) & ~np.isnan(p)
                t = t[mask]
                p = p[mask]

                if t.size == 0:
                    return []

                scaler = MinMaxScaler()
                scaler.fit(t.reshape(-1, 1))

                t_s = scaler.transform(t.reshape(-1, 1)).reshape(-1)
                p_s = scaler.transform(p.reshape(-1, 1)).reshape(-1)

                return [float(abs(t_s[i] - p_s[i])) for i in range(len(t_s))]


            def stage1_agent_thiago_style(gold: pd.DataFrame, pred_indicators: pd.DataFrame, terms: List[str]) -> pd.DataFrame:
                \"\"\"Per term mean MAE for agent mode, Thiago style.\"\"\"
                rows: List[Dict[str, Any]] = []

                for term in terms:
                    if term not in gold.columns:
                        continue

                    g = gold[["ticker", "month", term]].rename(columns={term: "gold"}).copy()
                    p = (
                        pred_indicators[pred_indicators["indicator"] == term][["ticker", "month", "pred_value"]]
                        .rename(columns={"pred_value": "pred"})
                        .copy()
                    )

                    merged = g.merge(p, on=["ticker", "month"], how="inner")
                    if merged.empty:
                        continue

                    mae_list = mae_list_minmax_fit_gold(merged["gold"], merged["pred"])
                    rows.append(
                        {
                            "term": term,
                            "n_points": int(len(mae_list)),
                            "mean_mae": float(np.mean(mae_list)) if len(mae_list) > 0 else float("nan"),
                        }
                    )

                return pd.DataFrame(rows).sort_values(["mean_mae", "term"], na_position="last").reset_index(drop=True)
            """
        ),
    )

    add_md(nb, "## 4. Stage 2. Trading simulation")

    add_code(
        nb,
        dedent(
            """\
            def simulate_simple_trades(actions: pd.DataFrame, risk_free_rate_annual: float = 0.0) -> pd.DataFrame:
                \"\"\"Simulate simple monthly trading from BUY, SELL, HOLD actions.\"\"\"
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
            """
        ),
    )
    
    add_md(
        nb,
        "## 5. Configuration\n\n"
        "Edit these paths and settings as needed, then run the notebook top to bottom.",
    )

    add_code(
        nb,
        dedent(
            """\
            GOLD_CSV = Path("data/processed/panel/monthly_panel_prices_returns_fundamentals.csv")

            # For agent: results/experiments/monthly_workflow
            # For workflow: results/experiments/monthly_workflow_workflow
            PRED_DIR = Path("results/experiments/monthly_workflow_workflow")

            # Mode: "auto", "agent", "workflow"
            MODE = "auto"

            # Stage 1 terms for US panel
            STAGE1_TERMS = ["Revenues", "NetIncomeLoss", "Assets", "Liabilities", "last_price"]

            # Stage 2
            RISK_FREE_RATE_ANNUAL = 0.0
            """
        ),
    )

    add_md(nb, "## 6. Run evaluation")

    add_code(
        nb,
        dedent(
            """\
            gold = load_gold_monthly_panel(GOLD_CSV)
            print("Gold rows:", len(gold))
            print("Gold tickers:", gold["ticker"].nunique())
            gold.head()
            """
        ),
    )

    add_code(
        nb,
        dedent(
            """\
            mode = MODE
            if mode == "auto":
                mode = detect_mode_from_folder(PRED_DIR)

            print("Mode:", mode)
            print("Prediction dir:", str(PRED_DIR))
            """
        ),
    )

    add_code(
        nb,
        dedent(
            """\
            if mode == "agent":
                pred_indicators = load_predictions_agent(PRED_DIR)
                if pred_indicators.empty:
                    raise RuntimeError("No agent predictions found in {0}".format(str(PRED_DIR)))

                per_term = stage1_agent_thiago_style(gold, pred_indicators, STAGE1_TERMS)
                display(per_term)
                print("Overall mean of per term means:", float(per_term["mean_mae"].mean()) if not per_term.empty else float("nan"))

            elif mode == "workflow":
                print("Stage 1 skipped for workflow outputs.")
            else:
                raise ValueError("Unknown mode: {0}".format(mode))
            """
        ),
    )

    add_code(
        nb,
        dedent(
            """\
            if mode == "workflow":
                actions = load_actions_workflow(PRED_DIR)
                if actions.empty:
                    raise RuntimeError("No workflow actions found in {0}".format(str(PRED_DIR)))

                gold_price = gold[["ticker", "month", "last_price"]].rename(columns={"last_price": "price"}).copy()
                actions = actions.merge(gold_price, on=["ticker", "month"], how="left")

                trade_df = simulate_simple_trades(actions, risk_free_rate_annual=RISK_FREE_RATE_ANNUAL)
                display(trade_df)

                print("Monthly points (gold):", len(gold))
                print("Monthly points (pred):", len(actions.drop_duplicates(subset=["ticker", "month"])))
            """
        ),
    )

    return nb


def main() -> None:
    """Create the notebook file in the current directory."""
    nb = build_notebook()
    out_path = Path("run_eval_monthly_notebook.ipynb")
    out_path.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print("Wrote:", out_path.resolve())


if __name__ == "__main__":
    main()

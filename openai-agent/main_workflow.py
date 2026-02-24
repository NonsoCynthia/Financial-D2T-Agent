import os
import json
import time
import sqlite3
import pandas as pd
import numpy as np

from agents import Agent, ModelSettings, Runner
from openai.types.shared import Reasoning

from experiments import ExperimentMetadata, Model, Intensity
from experiments.final_report2025.config import STOCKS, DB_PATH
from experiments.utils import get_result
from db import get_latest_assets_end_date, get_previous_assets_end_date

from financial_agents import get_agent
from financial_agents.financial_analyst import FINANCIAL_ANALYST_INSTRUCTION
from financial_agents.us_indicator_schema import IndicatorOutput
from financial_agents.financial_manager import MANAGER_INSTRUCTION, ManagerDecision

from tools import code_interpreter
from db.base_query import run_sql_query

from experiments.validation.yahoo_spotcheck import spotcheck_pe_pb


WRITE_FOLDER = "results/manager_us"
os.makedirs(WRITE_FOLDER, exist_ok=True)

SPOTCHECK_MONTH = "2025-04"
DO_YAHOO_SPOTCHECK = os.getenv("DO_YAHOO_SPOTCHECK", "1") == "1"


def _monthly_panel(ticker: str) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM US_MONTHLY_PANEL
            WHERE ticker = ?
            ORDER BY date ASC
            """,
            con,
            params=(ticker,),
        )
        return df
    finally:
        con.close()


def _prefetch_report(ticker: str, end_date: str) -> str:
    q = f"""
    SELECT CONCEPT, UNIT, VALUE_REAL, FORM, FY, FP, START_DATE, END_DATE, FILED_DATE
    FROM SEC_COMPANYFACTS
    WHERE TICKER = '{ticker}' AND END_DATE = '{end_date}'
    ORDER BY CONCEPT, FILED_DATE;
    """
    return run_sql_query({"sql_query": q}, db_path=DB_PATH).get("report", "")


def _prefetch_composition(ticker: str, end_date: str) -> str:
    q = f"""
    SELECT CONCEPT, UNIT, VALUE_REAL, FORM, FY, FP, END_DATE, FILED_DATE
    FROM SEC_COMPANYFACTS
    WHERE TICKER = '{ticker}'
      AND END_DATE = '{end_date}'
      AND CONCEPT IN ('CommonStockSharesOutstanding', 'EarningsPerShareBasic')
    ORDER BY CONCEPT, FILED_DATE DESC;
    """
    return run_sql_query({"sql_query": q}, db_path=DB_PATH).get("report", "")


def _template_workflow_prompt(name: str, ticker: str, cik: str, date_str: str, price: float, end_date: str, prev_end: str,
                              report: str, composition: str, prev_report: str) -> str:
    return f"""
Run fundamental analysis for {name} (Ticker {ticker}, CIK {cik}) as of {date_str} with last price {price:.2f} USD.

# SEC snapshot (end_date {end_date})
{report}

# Share snapshot (end_date {end_date})
{composition}

# Previous snapshot (end_date {prev_end})
{prev_report}

Compute the full set of 32 indicators and return them in the required schema.
"""


def _save_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _flatten_indicators(output: dict) -> dict:
    return {str(x["indicator"]): float(x["value"]) for x in output.get("indicators", [])}


def _sharpe(monthly_returns: np.ndarray) -> float:
    if len(monthly_returns) < 2:
        return 0.0
    if np.std(monthly_returns) == 0.0:
        return 0.0
    return float(np.mean(monthly_returns) / np.std(monthly_returns) * np.sqrt(12.0))


def main():
    # Analyst uses medium reasoning
    analyst_model = Model.GPT_5_MINI
    analyst_settings = ModelSettings(
        tool_choice="required",
        reasoning=Reasoning(effort=Intensity.MEDIUM),
        verbosity=Intensity.MEDIUM,
    )

    # Manager uses high reasoning
    manager_model = Model.GPT_5_MINI
    manager_settings = ModelSettings(
        tool_choice="required",
        reasoning=Reasoning(effort=Intensity.HIGH),
        verbosity=Intensity.MEDIUM,
    )

    analyst_agent = get_agent(
        name="financial_analyst_workflow",
        instructions=FINANCIAL_ANALYST_INSTRUCTION,
        tools=[code_interpreter],
        servers=[],
        model=analyst_model,
        model_settings=analyst_settings,
        output_type=IndicatorOutput,
    )

    manager_agent = get_agent(
        name="financial_manager",
        instructions=MANAGER_INSTRUCTION,
        tools=[code_interpreter],
        servers=[],
        model=manager_model,
        model_settings=manager_settings,
        output_type=ManagerDecision,
    )

    all_decisions = []
    all_rows = []

    for stock in STOCKS:
        ticker = stock.stock_id
        monthly = _monthly_panel(ticker=ticker)
        if monthly.empty:
            continue

        for i in range(len(monthly) - 1):
            row = monthly.iloc[i]
            next_row = monthly.iloc[i + 1]

            date_str = str(row["date"])
            next_date = str(next_row["date"])

            # Require a price_close field from US_MONTHLY_PANEL
            try:
                price = float(row["price_close"])
                next_price = float(next_row["price_close"])
            except Exception:
                continue

            out_dir = f"{WRITE_FOLDER}/{ticker}"
            analyst_path = f"{out_dir}/{date_str}_analyst.json"
            manager_path = f"{out_dir}/{date_str}_manager.json"

            if os.path.exists(manager_path):
                continue

            # Prefetch SEC snapshots for workflow prompt
            end_date = get_latest_assets_end_date(ticker, date_str, db_path=DB_PATH) or date_str
            prev_end = get_previous_assets_end_date(ticker, end_date, db_path=DB_PATH) or end_date

            report = _prefetch_report(ticker=ticker, end_date=end_date)
            composition = _prefetch_composition(ticker=ticker, end_date=end_date)
            prev_report = _prefetch_report(ticker=ticker, end_date=prev_end)

            analyst_prompt = _template_workflow_prompt(name=stock.name, ticker=ticker, cik=stock.cnpj, date_str=date_str, price=price, end_date=end_date, prev_end=prev_end, report=report, composition=composition, prev_report=prev_report)

            t0 = time.time()
            analyst_result = Runner.run_sync(analyst_agent, input=analyst_prompt, max_turns=15)
            t1 = time.time()

            analyst_payload = get_result(analyst_result, t1 - t0)
            _save_json(path=analyst_path, payload=analyst_payload)

            indicators = _flatten_indicators(output=analyst_payload.get("output", {}))

            enriched = row.to_dict()
            enriched.update(indicators)
            enriched["ticker"] = ticker
            enriched["analysis_date"] = date_str
            enriched["next_date"] = next_date
            enriched["price_close"] = price
            enriched["next_price_close"] = next_price

            all_rows.append(enriched)

            # Build last 12 months context
            hist = pd.DataFrame([r for r in all_rows if r["ticker"] == ticker]).sort_values("analysis_date", ascending=True).tail(12)

            manager_prompt = (
                f"Make an investment decision for {stock.name} ({ticker}) on {date_str}.\n"
                f"Use the last 12 months of history below.\n"
                f"{hist.to_string(index=False)}"
            )

            m0 = time.time()
            manager_result = Runner.run_sync(manager_agent, input=manager_prompt, max_turns=15)
            m1 = time.time()

            manager_payload = get_result(manager_result, m1 - m0)
            _save_json(path=manager_path, payload=manager_payload)

            decision = manager_payload.get("output", {})
            decision["stock_id"] = ticker
            decision["analysis_date"] = date_str
            decision["price"] = price
            decision["next_price"] = next_price
            all_decisions.append(decision)

            # Compute strategy return: BUY means take next month return, else 0
            rec = str(decision.get("recommendation", "HOLD")).upper()
            monthly_ret = (next_price / price - 1.0) if price > 0 else 0.0
            strat_ret = monthly_ret if rec == "BUY" else 0.0
            decision["monthly_return"] = monthly_ret
            decision["strategy_return"] = strat_ret

            # Compute cumulative return and sharpe for this ticker so far
            ticker_decisions = [d for d in all_decisions if d["stock_id"] == ticker]
            strat = np.array([float(d.get("strategy_return", 0.0)) for d in ticker_decisions], dtype=float)
            decision["cum_return"] = float(np.prod(1.0 + strat) - 1.0)
            decision["sharpe_12m"] = _sharpe(monthly_returns=strat[-12:])

            _save_json(path=f"{WRITE_FOLDER}/decisions_sample.json", payload=all_decisions)

            # Optional Yahoo spotcheck for a single month across tickers
            if DO_YAHOO_SPOTCHECK and date_str.startswith(SPOTCHECK_MONTH):
                # Collect model P/E and P/B for all tickers at that month
                latest = {}
                for s in STOCKS:
                    td = [r for r in all_rows if r["ticker"] == s.stock_id and r["analysis_date"].startswith(SPOTCHECK_MONTH)]
                    if td:
                        latest[s.stock_id] = {k: float(td[-1].get(k, 0.0)) for k in ["P_E", "P_B"]}
                df_check = spotcheck_pe_pb(latest, [s.stock_id for s in STOCKS])
                df_check.to_csv(f"{WRITE_FOLDER}/yahoo_spotcheck_{SPOTCHECK_MONTH}.csv", index=False)

            time.sleep(5)


if __name__ == "__main__":
    main()


# export DO_YAHOO_SPOTCHECK=1
# python main_workflow_us.py

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from agents import Agent, Runner
from agents.mcp import MCPServerStdio

from finAgents.financial_agents.agent_prompts import (
    FINANCIAL_ANALYST_INSTRUCTIONS,
    FINANCIAL_MANAGER_INSTRUCTIONS,
    ANALYST_TASK_PROMPT,
    MANAGER_TASK_PROMPT,
    JSON_REPAIR_PROMPT,
)

RESULTS_DIR = Path("results") / "experiments" / "monthly_workflow"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]


def parse_json_strict(text: str) -> Dict[str, Any]:
    """Parse a JSON string into a Python dict. Raises json.JSONDecodeError if invalid."""
    return json.loads(text)


async def repair_to_json(agent: Agent, bad_output: str, expected_schema: str) -> Dict[str, Any]:
    """Use the model to convert invalid JSON-like output into valid JSON only."""
    prompt = JSON_REPAIR_PROMPT.format(expected_schema=expected_schema, bad_output=bad_output)
    result = await Runner.run(agent, prompt)
    return parse_json_strict(result.final_output)


def safe_float(x: Any) -> Optional[float]:
    """Convert a value to float, return None if conversion fails."""
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def pick_first_trading_day_per_month(rows: List[Dict[str, Any]], max_months: int = 12) -> List[Dict[str, Any]]:
    """Collapse daily rows into one row per calendar month using the first trading day."""
    if not rows:
        return []

    df = pd.DataFrame(rows).copy()
    if "date" not in df.columns:
        return []

    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values("date_dt").reset_index(drop=True)
    if df.empty:
        return []

    df["month"] = df["date_dt"].dt.to_period("M").astype(str)

    monthly = (
        df.drop_duplicates(subset=["month"], keep="first")
        .sort_values("date_dt")
        .reset_index(drop=True)
    )

    if max_months is not None and len(monthly) > max_months:
        monthly = monthly.iloc[-max_months:].reset_index(drop=True)

    monthly = monthly.drop(columns=["date_dt", "month"], errors="ignore")
    return monthly.to_dict(orient="records")


async def fetch_monthly_rows(analyst: Agent, ticker: str, test_start: str, test_end: str, max_months: int = 12) -> List[Dict[str, Any]]:
    """Fetch monthly price rows for one ticker, prefer monthly tool, fall back to daily then collapse."""
    prompt_monthly = (
        f'Use the tool get_monthly_price_series for ticker "{ticker}" from "{test_start}" to "{test_end}" '
        'with price_field "Adj Close", then return JSON with the tool output only.'
    )

    monthly_payload = await Runner.run(analyst, prompt_monthly)

    try:
        monthly_json = parse_json_strict(monthly_payload.final_output)
    except json.JSONDecodeError:
        expected_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA","price":123.45}], "n": 12 }'
        monthly_json = await repair_to_json(analyst, monthly_payload.final_output, expected_schema)

    rows = monthly_json.get("rows", [])
    if isinstance(rows, list) and rows:
        if max_months is not None and len(rows) > max_months:
            rows = rows[-max_months:]
        return rows

    prompt_daily = (
        f'Use the tool get_price_series for ticker "{ticker}" from "{test_start}" to "{test_end}" '
        'with price_field "Adj Close", then return JSON with the tool output only.'
    )
    daily_payload = await Runner.run(analyst, prompt_daily)

    try:
        daily_json = parse_json_strict(daily_payload.final_output)
    except json.JSONDecodeError:
        expected_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA","price":123.45}], "n": 252 }'
        daily_json = await repair_to_json(analyst, daily_payload.final_output, expected_schema)

    daily_rows = daily_json.get("rows", [])
    if not isinstance(daily_rows, list) or not daily_rows:
        return []

    return pick_first_trading_day_per_month(daily_rows, max_months=max_months)


def load_experiment_config() -> Dict[str, Any]:
    """Load config/experiment.json if present, otherwise return an empty dict."""
    p = Path("config") / "experiment.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_tickers(cli_tickers: Optional[str]) -> List[str]:
    """Resolve tickers from CLI, otherwise from config, otherwise from defaults."""
    if cli_tickers:
        out = [t.strip().upper() for t in cli_tickers.split(",") if t.strip()]
        return out

    cfg = load_experiment_config()
    cfg_tickers = cfg.get("tickers")
    if isinstance(cfg_tickers, list) and cfg_tickers:
        return [str(t).strip().upper() for t in cfg_tickers if str(t).strip()]

    return DEFAULT_TICKERS


def get_date_range() -> tuple[str, str]:
    """Resolve test_start and test_end from config/experiment.json, otherwise use defaults."""
    cfg = load_experiment_config()
    test_start = str(cfg.get("test_start", "2025-01-02"))
    test_end = str(cfg.get("test_end", "2025-12-31"))
    return test_start, test_end


async def run_one_ticker(
    analyst: Agent,
    manager: Agent,
    ticker: str,
    test_start: str,
    test_end: str,
    max_months: int = 12,
) -> None:
    """Run the monthly workflow for a single ticker and save outputs to RESULTS_DIR."""
    rows = await fetch_monthly_rows(analyst, ticker, test_start, test_end, max_months=max_months)

    out_path = RESULTS_DIR / f"{ticker}_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    if not rows:
        out_path.write_text(
            json.dumps({"ticker": ticker, "outputs": [], "error": "No monthly rows returned"}, indent=2),
            encoding="utf-8",
        )
        print(f"No monthly rows returned for {ticker}. Saved: {out_path}")
        return

    outputs: List[Dict[str, Any]] = []

    for r in rows:
        date = str(r.get("date", "")).strip()
        price_today = safe_float(r.get("price"))
        if not date or price_today is None:
            continue

        analyst_prompt = ANALYST_TASK_PROMPT.format(ticker=ticker, as_of_date=date)
        analyst_result = await Runner.run(analyst, analyst_prompt)

        analyst_schema = (
            '{ "ticker": "TSLA", "as_of_date": "2025-01-02", '
            '"indicators": {"last_price": 123.45, "total_return": 0.12}, '
            '"methodology": {"notes": "...", "sources": ["get_panel"]} }'
        )

        try:
            analyst_json = parse_json_strict(analyst_result.final_output)
        except json.JSONDecodeError:
            analyst_json = await repair_to_json(analyst, analyst_result.final_output, analyst_schema)

        manager_prompt = MANAGER_TASK_PROMPT.format(
            ticker=ticker,
            date=date,
            allow_short="False",
            position_sizing="one_share",
            trade_price_field="Adj Close",
            transaction_cost_bps="0",
            cash="1000000",
            shares_held="0",
            price_today=str(price_today),
            portfolio_value="1000000",
            analyst_report_json=json.dumps(analyst_json),
        )

        manager_result = await Runner.run(manager, manager_prompt)

        manager_schema = (
            '{ "ticker": "TSLA", "date": "2025-01-02", '
            '"action": "HOLD", "target_position": 0, "justification": "..." }'
        )

        try:
            manager_json = parse_json_strict(manager_result.final_output)
        except json.JSONDecodeError:
            manager_json = await repair_to_json(manager, manager_result.final_output, manager_schema)

        outputs.append(
            {
                "date": date,
                "price": price_today,
                "analyst": analyst_json,
                "manager": manager_json,
            }
        )

    out_path.write_text(json.dumps({"ticker": ticker, "outputs": outputs}, indent=2), encoding="utf-8")
    print(f"Saved: {out_path}")
    print(f"{ticker} monthly decisions saved: {len(outputs)}")


async def main() -> None:
    """Run the monthly workflow for one or more tickers and write one output file per ticker."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers, e.g. TSLA,MSFT,AAPL")
    parser.add_argument("--max_months", type=int, default=12, help="Number of monthly decision points to keep")
    args = parser.parse_args()

    tickers = get_tickers(args.tickers)
    test_start, test_end = get_date_range()

    server_path = Path("finAgents") / "server_us_finance.py"

    async with MCPServerStdio(
        name="US Finance MCP Server",
        params={"command": sys.executable, "args": [str(server_path)]},
        cache_tools_list=True,
    ) as server:
        analyst = Agent(
            name="financial_analyst",
            instructions=FINANCIAL_ANALYST_INSTRUCTIONS,
            mcp_servers=[server],
            model="gpt-5-mini",
            # model_settings=ModelSettings(tool_choice="required", reasoning_effort="high"),
        )
        manager = Agent(
            name="financial_manager",
            instructions=FINANCIAL_MANAGER_INSTRUCTIONS,
            mcp_servers=[server],
            model="gpt-5-mini",
            # model_settings=ModelSettings(tool_choice="required", reasoning_effort="high"),
        )

        for t in tickers:
            await run_one_ticker(
                analyst=analyst,
                manager=manager,
                ticker=t,
                test_start=test_start,
                test_end=test_end,
                max_months=int(args.max_months),
            )


if __name__ == "__main__":
    asyncio.run(main())


# TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]

# python run_monthly_experiment.py
# python run_eval_monthly.py
# How to use it:
# Use tickers from config/experiment.json:
# python run_monthly_experiment.py

# Override tickers from the command line:
# python run_monthly_experiment.py --tickers TSLA,AMZN,NIO,MSFT,AAPL,GOOG,NFLX,COIN

# Change number of months:
# python run_monthly_experiment.py --max_months 12
# Default model = gpt 4.1
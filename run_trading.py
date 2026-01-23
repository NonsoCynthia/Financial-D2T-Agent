import os

os.environ["OPENAI_AGENTS_DISABLE_TRACING"] = "1"
os.environ["OPENAI_AGENTS_DONT_LOG_TOOL_DATA"] = "1"
os.environ["OPENAI_AGENTS_DONT_LOG_MODEL_DATA"] = "1"

import asyncio
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from agents import Agent, Runner
from agents.mcp import MCPServerStdio

from finAgents.financial_agents.agent_prompts import (
    FINANCIAL_ANALYST_INSTRUCTIONS,
    FINANCIAL_MANAGER_INSTRUCTIONS,
    ANALYST_TASK_PROMPT,
    REFLECTION_TASK_PROMPT,
    MANAGER_TASK_PROMPT,
    JSON_REPAIR_PROMPT,
)

from finAgents.financial_agents.financial_analyst import (
    Indicator,
    find_missing_indicators_from_json,
)


def load_experiment_config() -> Dict[str, Any]:
    """
    Load experiment settings from config/experiment.json.

    If it does not exist, return a safe default config.
    """
    p = Path("config") / "experiment.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))

    return {
        "tickers": ["TSLA"],
        "test_start": "2025-01-02",
        "test_end": "2025-12-31",
        "initial_cash": 1_000_000,
        "position_sizing": {"type": "one_share", "allow_short": False},
        "execution": {"trade_price": "adj_close", "transaction_cost_bps": 0},
        "lookbacks": {"volatility_days": 252, "return_days": 252},
        "risk_free_rate": 0.0,
    }


def parse_json_strict(text: str) -> Dict[str, Any]:
    """
    Parse JSON text strictly.

    Raises json.JSONDecodeError if the text is not valid JSON.
    """
    return json.loads(text)


async def repair_to_json(agent: Agent, bad_output: str, expected_schema: str) -> Dict[str, Any]:
    """
    Ask the model to convert invalid JSON-like output into valid JSON.

    This is only used when the model returns extra text or malformed JSON.
    """
    prompt = JSON_REPAIR_PROMPT.format(expected_schema=expected_schema, bad_output=bad_output)
    result = await Runner.run(agent, prompt)
    return parse_json_strict(result.final_output)


def normalise_action(x: Any) -> str:
    """
    Normalise the manager action to BUY, SELL, or HOLD.

    Any unknown action becomes HOLD to keep the simulator stable.
    """
    if not isinstance(x, str):
        return "HOLD"
    x = x.strip().upper()
    if x in {"BUY", "SELL", "HOLD"}:
        return x
    return "HOLD"


def clamp_target_position(x: Any) -> int:
    """
    Clamp target_position to 0 or 1.

    Any invalid value becomes 0.
    """
    try:
        v = int(x)
    except Exception:
        return 0
    return 1 if v == 1 else 0


def compute_portfolio_value(cash: float, shares_held: int, price: float) -> float:
    """
    Compute mark-to-market portfolio value.
    """
    return float(cash + (shares_held * price))


def transaction_cost(price: float, shares_traded: int, bps: float) -> float:
    """
    Compute transaction cost in basis points on notional traded.
    """
    notional = abs(shares_traded) * price
    return float(notional * (bps / 10_000.0))


def apply_trade(
    cash: float,
    shares_held: int,
    price: float,
    target_position: int,
    sizing_type: str,
    transaction_cost_bps: float,
) -> Tuple[float, int, float, int]:
    """
    Apply a trade to move towards the target position.

    sizing_type:
    - one_share: position is either 0 or 1 share
    - all_in: position is either 0 shares, or as many shares as possible when entering

    Returns:
    - new_cash
    - new_shares_held
    - cost_paid
    - shares_traded (positive buy, negative sell)
    """
    sizing_type = (sizing_type or "one_share").strip().lower()

    if price <= 0:
        return cash, shares_held, 0.0, 0

    if sizing_type == "all_in":
        if target_position == 1:
            if shares_held > 0:
                desired_shares = shares_held
            else:
                desired_shares = int(math.floor(cash / price))
        else:
            desired_shares = 0
    else:
        desired_shares = 1 if target_position == 1 else 0

    shares_traded = int(desired_shares - shares_held)
    if shares_traded == 0:
        return cash, shares_held, 0.0, 0

    cost = transaction_cost(price, shares_traded, transaction_cost_bps)

    if shares_traded > 0:
        total_spend = (shares_traded * price) + cost
        if total_spend > cash:
            return cash, shares_held, 0.0, 0
        cash = cash - total_spend
        shares_held = shares_held + shares_traded
        return cash, shares_held, cost, shares_traded

    proceeds = (abs(shares_traded) * price) - cost
    cash = cash + proceeds
    shares_held = shares_held + shares_traded
    return cash, shares_held, cost, shares_traded


def map_trade_price_setting(execution_cfg: Dict[str, Any]) -> Tuple[str, str]:
    """
    Map your config value into:
    - the MCP tool argument for get_price_series: 'Adj Close' or 'Close'
    - the human readable name stored in outputs
    """
    trade_price_setting = execution_cfg.get("trade_price_field") or execution_cfg.get("trade_price") or "adj_close"
    tp = str(trade_price_setting).strip().lower()

    if tp in {"adj_close", "adj close", "adjclose"}:
        return "Adj Close", "Adj Close"
    if tp in {"close"}:
        return "Close", "Close"

    return "Adj Close", "Adj Close"


async def main() -> None:
    """
    Run a daily trading loop for each ticker.

    For each trading day:
    - fetch a minimal price series (date, price) once
    - analyst outputs indicators JSON for that date
    - manager outputs action JSON for that date
    - simulator updates cash and holdings
    - save full trajectory to results/runs/<ticker>/
    """
    cfg = load_experiment_config()

    tickers: List[str] = cfg["tickers"]
    test_start: str = cfg["test_start"]
    test_end: str = cfg["test_end"]
    initial_cash: float = float(cfg["initial_cash"])

    sizing_type: str = cfg["position_sizing"]["type"]
    allow_short: bool = bool(cfg["position_sizing"].get("allow_short", False))

    execution_cfg = cfg.get("execution", {})
    price_field_for_tool, trade_price_field_label = map_trade_price_setting(execution_cfg)
    transaction_cost_bps: float = float(execution_cfg.get("transaction_cost_bps", 0))

    server_path = Path("finAgents") / "server_us_finance.py"

    async with MCPServerStdio(
        name="US Finance MCP Server",
        params={"command": sys.executable, "args": [str(server_path)]},
        client_session_timeout_seconds=30.0,
    ) as server:
        analyst = Agent(
            name="financial_analyst",
            instructions=FINANCIAL_ANALYST_INSTRUCTIONS,
            mcp_servers=[server],
        )

        manager = Agent(
            name="financial_manager",
            instructions=FINANCIAL_MANAGER_INSTRUCTIONS,
            mcp_servers=[server],
        )

        for ticker in tickers:
            ticker = ticker.upper().strip()

            series_prompt = (
                "Call the tool get_price_series with these arguments and return JSON only.\n"
                f'ticker="{ticker}"\n'
                f'start_date="{test_start}"\n'
                f'end_date_inclusive="{test_end}"\n'
                f'price_field="{price_field_for_tool}"\n'
            )
            series_result = await Runner.run(analyst, series_prompt)

            series_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA","price":123.45}], "n": 1 }'
            try:
                series_json = parse_json_strict(series_result.final_output)
            except json.JSONDecodeError:
                series_json = await repair_to_json(analyst, series_result.final_output, series_schema)

            rows = series_json.get("rows", [])
            if not isinstance(rows, list) or not rows:
                print(f"No price series for {ticker}. Skipping.")
                continue

            cash = initial_cash
            shares_held = 0
            prev_value: float | None = None

            trajectory: List[Dict[str, Any]] = []

            expected = [i.value for i in Indicator]

            for row in rows:
                date = str(row.get("date"))
                price_today = row.get("price")

                if price_today is None:
                    continue

                try:
                    price_today = float(price_today)
                except Exception:
                    continue

                portfolio_value = compute_portfolio_value(cash, shares_held, price_today)

                analyst_prompt = ANALYST_TASK_PROMPT.format(ticker=ticker, as_of_date=date)
                analyst_result = await Runner.run(analyst, analyst_prompt)

                analyst_schema = (
                    '{ "ticker":"TSLA","as_of_date":"2025-12-31",'
                    '"indicators":{"last_price":123.45,"total_return":0.1,"volatility":0.2,'
                    '"Revenues":null,"NetIncomeLoss":null,"Assets":null,"Liabilities":null,"roe":null,"profit_margin":null},'
                    '"methodology":{"notes":"...","sources":["get_panel"]}}'
                )

                try:
                    analyst_json = parse_json_strict(analyst_result.final_output)
                except json.JSONDecodeError:
                    analyst_json = await repair_to_json(analyst, analyst_result.final_output, analyst_schema)

                missing = find_missing_indicators_from_json(analyst_json, expected)

                if missing:
                    missing_block = "\n".join(missing)
                    reflection_prompt = REFLECTION_TASK_PROMPT.format(
                        ticker=ticker,
                        as_of_date=date,
                        missing_indicators=missing_block,
                    )
                    reflection_result = await Runner.run(analyst, reflection_prompt)

                    reflection_schema = '{ "ticker":"TSLA","as_of_date":"2025-12-31","indicators":{"roe":0.1} }'

                    try:
                        reflection_json = parse_json_strict(reflection_result.final_output)
                    except json.JSONDecodeError:
                        reflection_json = await repair_to_json(analyst, reflection_result.final_output, reflection_schema)

                    base_ind = analyst_json.get("indicators", {})
                    ref_ind = reflection_json.get("indicators", {})

                    if isinstance(base_ind, dict) and isinstance(ref_ind, dict):
                        for k, v in ref_ind.items():
                            base_ind[k] = v
                        analyst_json["indicators"] = base_ind

                analyst_report_compact = json.dumps(analyst_json, separators=(",", ":"), ensure_ascii=True)

                manager_prompt = MANAGER_TASK_PROMPT.format(
                    ticker=ticker,
                    date=date,
                    allow_short=str(allow_short),
                    position_sizing=sizing_type,
                    trade_price_field=trade_price_field_label,
                    transaction_cost_bps=str(transaction_cost_bps),
                    cash=str(round(cash, 2)),
                    shares_held=str(shares_held),
                    price_today=str(round(price_today, 6)),
                    portfolio_value=str(round(portfolio_value, 2)),
                    analyst_report_json=analyst_report_compact,
                )

                manager_result = await Runner.run(manager, manager_prompt)

                manager_schema = (
                    '{ "ticker":"TSLA","date":"2025-01-02","action":"HOLD","target_position":0,"justification":"..." }'
                )

                try:
                    manager_json = parse_json_strict(manager_result.final_output)
                except json.JSONDecodeError:
                    manager_json = await repair_to_json(manager, manager_result.final_output, manager_schema)

                action = normalise_action(manager_json.get("action"))
                target_position = clamp_target_position(manager_json.get("target_position"))

                if action == "BUY":
                    target_position = 1
                elif action == "SELL":
                    target_position = 0
                else:
                    target_position = 1 if shares_held > 0 else 0

                cash, shares_held, cost_paid, shares_traded = apply_trade(
                    cash=cash,
                    shares_held=shares_held,
                    price=price_today,
                    target_position=target_position,
                    sizing_type=sizing_type,
                    transaction_cost_bps=transaction_cost_bps,
                )

                new_value = compute_portfolio_value(cash, shares_held, price_today)

                if prev_value is None:
                    daily_port_ret = 0.0
                else:
                    daily_port_ret = (new_value / prev_value) - 1.0

                prev_value = new_value

                trajectory.append(
                    {
                        "date": date,
                        "price": price_today,
                        "action": action,
                        "shares_traded": shares_traded,
                        "shares_held": shares_held,
                        "transaction_cost": cost_paid,
                        "cash": cash,
                        "portfolio_value": new_value,
                        "portfolio_return_1d": daily_port_ret,
                        "analyst": analyst_json,
                        "manager": manager_json,
                    }
                )

            out_dir = Path("results") / "runs" / ticker
            out_dir.mkdir(parents=True, exist_ok=True)

            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = out_dir / f"{ticker}_{test_start}_{test_end}_{run_id}.json"

            payload = {
                "config": cfg,
                "ticker": ticker,
                "test_start": test_start,
                "test_end": test_end,
                "trajectory": trajectory,
            }

            out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
            print(f"Saved run: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())

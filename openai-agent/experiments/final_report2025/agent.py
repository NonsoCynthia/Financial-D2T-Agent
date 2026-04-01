import asyncio
import contextlib
import json
import os
import sys
import time
from pathlib import Path
import re

from agents import Agent, ModelSettings, Runner, RunResult
from agents.exceptions import MaxTurnsExceeded
from agents.mcp import MCPServerStdio
from experiments import ExperimentMetadata, Intensity
from experiments.final_report2025.config import STOCKS, ANALYSIS_DATES, DB_PATH
from experiments.final_report2025.scoring import output_metrics_for_prediction
from experiments.utils import get_result, save_results
from openai.types.shared import Reasoning

from tools import code_interpreter, us_reports_query_tool, us_share_composition_query_tool
from financial_agents import get_agent
from financial_agents.financial_analyst import FINANCIAL_ANALYST_INSTRUCTION
from financial_agents.financial_manager import MANAGER_INSTRUCTION, ManagerDecision
from financial_agents.us_indicator_schema import IndicatorOutput, Indicator
from db import get_price_on_or_before
from db.base_query import run_sql_query

from experiments.validation.indicator_sanity import find_sanity_issues, indicators_to_recompute
import numpy as np


TEMPLATE_INPUT = """Run fundamental analysis for {name} (CIK {cnpj}) as of {analysis_date} with last price {price_str} USD.
Feedback: {feedback}"""

MANAGER_TEMPLATE_INPUT = """Make an investment decision for {name} ({ticker}, CIK {cnpj}) as of {analysis_date}.
Current price: {price_str} USD.

Previous manager decision context:
{previous_decision_context}

Historical context (last 12 months up to analysis date; prices + fundamentals):
{history_lines}

You are given the analyst indicators below:
{indicator_lines}

Return your response in the required schema with:
- recommendation (Buy/Sell/Hold)
- monthly_report
- justification
- target_price
"""


def _inter_run_sleep_seconds() -> float:
    raw = os.getenv("INTER_RUN_SLEEP_SECONDS", "10")
    try:
        value = float(raw)
    except ValueError:
        return 10.0
    return max(0.0, value)


INTER_RUN_SLEEP_SECONDS = _inter_run_sleep_seconds()


def _use_mcp_agent() -> bool:
    return os.getenv("USE_MCP_AGENT", "1") == "1"


def _default_mcp_server_script() -> Path:
    # .../openai-agent/experiments/final_report2025/agent.py -> parents[3] is Financial-D2T-Agent
    env = os.getenv("MCP_SERVER_SCRIPT")
    if env:
        return Path(env).expanduser().resolve()
    return (Path(__file__).resolve().parents[3] / "mcp" / "mcp_us_stock.py").resolve()


def _default_db_path() -> str:
    # Keep this aligned with the project's consolidated DB output.
    return str((Path(__file__).resolve().parents[3] / "data" / "processed" / "mcp" / "fundamental_analysis.db").resolve())


def _build_mcp_servers() -> list[MCPServerStdio]:
    if not _use_mcp_agent():
        return []

    server_script = _default_mcp_server_script()
    if not server_script.exists():
        raise FileNotFoundError(f"MCP server script not found: {server_script}")

    env = dict(os.environ)
    env.setdefault("US_DB_PATH", _default_db_path())
    env.setdefault("PYTHONUNBUFFERED", "1")

    params = {
        "command": sys.executable,
        "args": [str(server_script)],
        "cwd": str(server_script.parent),
        "env": env,
    }
    return [
        MCPServerStdio(
            params=params,
            cache_tools_list=True,
            name="us-finance-mcp-stdio",
            max_retry_attempts=2,
        )
    ]


def init_agent(experiment_metadata: ExperimentMetadata) -> Agent:
    # Enforce analyst reasoning policy: always medium.
    analyst_reasoning = Reasoning(effort=Intensity.MEDIUM)
    model_settings = ModelSettings(reasoning=analyst_reasoning, verbosity=experiment_metadata.verbosity)

    mcp_servers = _build_mcp_servers()
    if mcp_servers:
        tools = [code_interpreter]
    else:
        # Fallback path: local SQL function tools.
        tools = [code_interpreter, us_reports_query_tool, us_share_composition_query_tool]

    return get_agent(
        name="financial_analyst",
        instructions=FINANCIAL_ANALYST_INSTRUCTION,
        tools=tools,
        servers=mcp_servers,
        model=experiment_metadata.model,
        model_settings=model_settings,
        output_type=IndicatorOutput,
    )


def init_manager_agent(experiment_metadata: ExperimentMetadata) -> Agent:
    manager_settings = ModelSettings(tool_choice="required")
    manager_reasoning = Reasoning(effort=Intensity.HIGH)
    manager_settings = ModelSettings(reasoning=manager_reasoning, verbosity=experiment_metadata.verbosity)

    return get_agent(
        name="financial_manager_agent_mode",
        instructions=MANAGER_INSTRUCTION,
        tools=[code_interpreter],
        servers=[],
        model=experiment_metadata.model,
        model_settings=manager_settings,
        output_type=ManagerDecision,
    )


async def _run_with_optional_mcp(agent: Agent, inp_data: str, max_turns: int) -> RunResult:
    async with contextlib.AsyncExitStack() as stack:
        for server in agent.mcp_servers:
            await stack.enter_async_context(server)
        return await Runner.run(starting_agent=agent, input=inp_data, max_turns=max_turns)


def _run_with_turn_retry(agent: Agent, inp_data: str, max_turns: int, label: str) -> RunResult:
    try:
        return asyncio.run(
            _run_with_optional_mcp(
                agent=agent,
                inp_data=inp_data,
                max_turns=max_turns,
            )
        )
    except MaxTurnsExceeded:
        retry_turns = max(30, max_turns * 2)
        print(f"{label}: max turns exceeded at {max_turns}. Retrying with {retry_turns}.")
        return asyncio.run(
            _run_with_optional_mcp(
                agent=agent,
                inp_data=inp_data,
                max_turns=retry_turns,
            )
        )


def analyse(
    agent: Agent,
    name: str,
    cnpj: str,
    price: str,
    analysis_date: str,
    experiment_metadata: ExperimentMetadata,
    feedback: str,
) -> RunResult:
    inp_data = TEMPLATE_INPUT.format(
        name=name,
        cnpj=cnpj,
        analysis_date=analysis_date,
        price_str=price,
        feedback=feedback,
    )
    return _run_with_turn_retry(
        agent=agent,
        inp_data=inp_data,
        max_turns=experiment_metadata.max_turns,
        label="analyst",
    )


def _to_map(result: RunResult) -> dict[str, float]:
    out = {}
    for row in result.final_output.indicators:
        out[str(row.indicator)] = float(row.value)
    return out


def _to_map_from_payload(payload: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in payload.get("indicators", []):
        k = str(row.get("indicator", "")).strip()
        if not k:
            continue
        v = row.get("value", 0.0)
        out[k] = float(v or 0.0)
    return out


def _apply_benchmark_indicator_conventions(indicators: dict[str, float]) -> dict[str, float]:
    """
    Align key leverage definitions with benchmark conventions:
    - NetDebt = GrossDebt - CashAndEquivalents
    - GrossDebt_Equity = GrossDebt / ShareholdersEquity (ratio, not percent)
    """
    out = dict(indicators)
    gross_debt = float(out.get("GrossDebt", 0.0) or 0.0)
    cash = float(out.get("CashAndEquivalents", 0.0) or 0.0)
    equity = float(out.get("ShareholdersEquity", 0.0) or 0.0)

    out["NetDebt"] = float(gross_debt - cash)
    out["GrossDebt_Equity"] = float(gross_debt / equity) if equity != 0.0 else 0.0
    return out


def _sync_indicator_payload(payload: dict, indicators: dict[str, float]) -> dict:
    out = dict(payload)
    rows = out.get("indicators")
    if not isinstance(rows, list):
        return out

    synced: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        k = str(row.get("indicator", "")).strip()
        if not k:
            continue
        seen.add(k)
        synced.append(
            {
                "indicator": k,
                "value": float(indicators.get(k, float(row.get("value", 0.0) or 0.0))),
            }
        )

    for k, v in indicators.items():
        if k not in seen:
            synced.append({"indicator": k, "value": float(v)})

    out["indicators"] = synced
    return out


def _manager_prompt(
    name: str,
    ticker: str,
    cnpj: str,
    analysis_date: str,
    price_str: str,
    indicators: dict[str, float],
    previous_decision_context: str,
) -> str:
    safe_ticker = ticker.replace("'", "''")
    history_query = f"""
    SELECT
        date(date) AS date,
        price_close,
        ret_1d,
        Assets,
        Liabilities,
        StockholdersEquity,
        Revenues,
        NetIncomeLoss,
        OperatingIncomeLoss,
        EarningsPerShareBasic,
        CommonStockSharesOutstanding
    FROM US_MONTHLY_PANEL
    WHERE ticker = '{safe_ticker}'
      AND date(date) <= date('{analysis_date}')
    ORDER BY date(date) DESC
    LIMIT 12;
    """
    history_lines = run_sql_query(inp={"sql_query": history_query}, db_path=DB_PATH).get("report", "")
    if not history_lines:
        history_lines = "No historical monthly context found."
    indicator_lines = "\n".join([f"- {k}: {v}" for k, v in sorted(indicators.items())])
    return MANAGER_TEMPLATE_INPUT.format(
        name=name,
        ticker=ticker,
        cnpj=cnpj,
        analysis_date=analysis_date,
        price_str=price_str,
        previous_decision_context=previous_decision_context,
        history_lines=history_lines,
        indicator_lines=indicator_lines,
    )


def _sanitize_prompt_text(text: str, max_chars: int = 800) -> str:
    s = str(text or "")
    # Keep prompt text printable and compact to reduce policy false-positives.
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_chars:
        return s[:max_chars].rstrip() + " ...[truncated]"
    return s


def _format_previous_decision_context(
    previous_decision: dict | None,
    include_justification: bool = True,
    include_monthly_report: bool = True,
) -> str:
    if not isinstance(previous_decision, dict):
        return (
            "- previous_analysis_date: N/A\n"
            "- previous_recommendation: N/A\n"
            "- previous_target_price: N/A\n"
            "- previous_justification: N/A\n"
            "- previous_monthly_report: N/A"
        )

    prev_date = _sanitize_prompt_text(str(previous_decision.get("analysis_date", "N/A")), max_chars=32)
    prev_rec = _sanitize_prompt_text(str(previous_decision.get("recommendation", "N/A")), max_chars=32)
    prev_target = previous_decision.get("target_price", "N/A")
    prev_just = "N/A"
    if include_justification:
        prev_just = _sanitize_prompt_text(str(previous_decision.get("justification", "N/A")), max_chars=700)
    prev_report = "N/A"
    if include_monthly_report:
        prev_report = _sanitize_prompt_text(str(previous_decision.get("monthly_report", "N/A")), max_chars=900)
    return (
        f"- previous_analysis_date: {prev_date}\n"
        f"- previous_recommendation: {prev_rec}\n"
        f"- previous_target_price: {prev_target}\n"
        f"- previous_justification: {prev_just}\n"
        f"- previous_monthly_report: {prev_report}"
    )


def _load_json(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _save_json(path: str, payload: dict | list) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)


def _usage_metrics_from_payload(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    return {
        "requests": int(usage.get("requests", 0) or 0),
        "input_tokens": int(usage.get("input_tokens", 0) or 0),
        "output_tokens": int(usage.get("output_tokens", 0) or 0),
        "total_tokens": int(usage.get("total_tokens", 0) or 0),
    }


def _is_invalid_prompt_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    markers = (
        "invalid_prompt",
        "invalid prompt",
        "flagged as potentially violating our usage policy",
    )
    return any(m in msg for m in markers)


def _payload_is_invalid_prompt_fallback(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if isinstance(error, dict):
        msg = str(error.get("error_message", "")).lower()
        if any(
            marker in msg
            for marker in (
                "invalid_prompt",
                "invalid prompt",
                "flagged as potentially violating our usage policy",
            )
        ):
            return True
    output = payload.get("output")
    if isinstance(output, dict):
        just = str(output.get("justification", "")).lower()
        if "due api invalid_prompt" in just:
            return True
    return False


def _paper_metrics_for_output(
    ticker: str,
    analysis_date: str,
    indicators: dict[str, float],
    usage_metrics: dict | None = None,
) -> dict | None:
    try:
        metrics = output_metrics_for_prediction(
            ticker=ticker,
            analysis_date=analysis_date,
            y_pred=indicators,
        )
        if not isinstance(metrics, dict):
            return None
        if isinstance(usage_metrics, dict):
            metrics.update(usage_metrics)
        return metrics
    except Exception as exc:
        print(f"Skipping paper metrics for {ticker} on {analysis_date}: {exc}")
        return None


def _sharpe(monthly_returns: np.ndarray) -> float:
    if len(monthly_returns) < 2:
        return 0.0
    if np.std(monthly_returns) == 0.0:
        return 0.0
    return float(np.mean(monthly_returns) / np.std(monthly_returns) * np.sqrt(12.0))


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


def _apply_trade_signal(
    manager_output: dict,
    stock_id: str,
    experiment_id: int,
    analysis_date: str,
    price: float,
    open_positions: list[float],
    strategy_returns: list[float],
) -> dict:
    """
    Thiago-style simulation state update:
    - BUY always opens/adds a position (including first signal).
    - SELL closes all open positions using average entry.
    - HOLD/other keeps current open positions.
    """
    out = dict(manager_output)
    out.setdefault("monthly_report", str(out.get("justification", "") or "N/A"))
    rec_norm = _normalise_recommendation(value=out.get("recommendation", ""))
    avg_entry_before = float(np.mean(open_positions)) if open_positions else None
    n_open_before = int(len(open_positions))
    action = "hold_or_other"
    trade_return = 0.0

    if rec_norm == "buy":
        open_positions.append(float(price))
        action = "open_or_add_position"
    elif rec_norm == "sell":
        if open_positions:
            avg_entry = float(np.mean(open_positions))
            trade_return = (float(price) / avg_entry - 1.0) if avg_entry > 0 else 0.0
            open_positions.clear()
            action = "close_all_positions"
        else:
            action = "sell_without_open_position"

    strategy_returns.append(trade_return)
    strat = np.array(strategy_returns, dtype=float)

    out["stock_id"] = stock_id
    out["experiment_id"] = int(experiment_id)
    out["analysis_date"] = analysis_date
    out["price"] = float(price)
    out["signal_action"] = action
    out["trade_return_on_signal"] = float(trade_return)
    out["strategy_return"] = float(trade_return)
    out["n_open_positions_before_signal"] = n_open_before
    out["avg_entry_price_before_signal"] = avg_entry_before
    out["n_open_positions_after_signal"] = int(len(open_positions))
    out["avg_entry_price_after_signal"] = float(np.mean(open_positions)) if open_positions else None
    out["cum_return"] = float(np.prod(1.0 + strat) - 1.0)
    out["sharpe_12m"] = _sharpe(monthly_returns=strat[-12:])
    return out


def _build_forced_final_liquidation(
    stock_id: str,
    experiment_id: int,
    final_date: str,
    final_price: float,
    open_positions: list[float],
    strategy_returns: list[float],
) -> dict | None:
    if not open_positions:
        return None

    avg_entry = float(np.mean(open_positions))
    liquidation_return = (float(final_price) / avg_entry - 1.0) if avg_entry > 0 else 0.0
    strategy_returns.append(liquidation_return)
    strat = np.array(strategy_returns, dtype=float)

    event = {
        "stock_id": stock_id,
        "experiment_id": int(experiment_id),
        "analysis_date": final_date,
        "recommendation": "FORCED_SELL_FINAL_DAY",
        "price": float(final_price),
        "signal_action": "forced_final_liquidation",
        "trade_return_on_signal": float(liquidation_return),
        "strategy_return": float(liquidation_return),
        "n_open_positions_before_signal": int(len(open_positions)),
        "avg_entry_price_before_signal": avg_entry,
        "n_open_positions_after_signal": 0,
        "avg_entry_price_after_signal": None,
        "cum_return": float(np.prod(1.0 + strat) - 1.0),
        "sharpe_12m": _sharpe(monthly_returns=strat[-12:]),
    }
    open_positions.clear()
    return event


def guardrail_reflection(
    agent: Agent,
    name: str,
    cnpj: str,
    price: str,
    analysis_date: str,
    result: RunResult,
    experiment_metadata: ExperimentMetadata,
) -> RunResult:
    """
    Improved reflection:
    - Recompute missing or zero indicators.
    - Recompute indicators flagged by sanity checks (e.g., absurd P/E or margins).
    """
    expected = {str(i) for i in Indicator}
    current = _to_map(result=result)

    missing = [k for k in expected if (k not in current) or (current[k] == 0.0)]
    issues = find_sanity_issues(indicators=current)
    suspect = indicators_to_recompute(issues=issues)

    to_fix = sorted(set(missing + suspect))
    if not to_fix:
        return result

    issue_text = "\n".join([f"- {i.indicator}: {i.message}" for i in issues]) if issues else "None"
    feedback = (
        f"Recompute ONLY these indicators: {to_fix}\n"
        f"Sanity issues detected:\n{issue_text}\n"
        "Use correct US decimal formatting and verify concept selection."
    )

    reflected = analyse(
        agent=agent,
        name=name,
        cnpj=cnpj,
        price=price,
        analysis_date=analysis_date,
        experiment_metadata=experiment_metadata,
        feedback=feedback,
    )

    ref_map = {str(r.indicator): r for r in reflected.final_output.indicators}

    merged = []
    for row in result.final_output.indicators:
        k = str(row.indicator)
        if k in to_fix and k in ref_map:
            merged.append(ref_map[k])
        else:
            merged.append(row)

    merged_keys = {str(r.indicator) for r in merged}
    for k in to_fix:
        if k in ref_map and k not in merged_keys:
            merged.append(ref_map[k])

    result.final_output.indicators = merged

    result.context_wrapper.usage.requests += reflected.context_wrapper.usage.requests
    result.context_wrapper.usage.input_tokens += reflected.context_wrapper.usage.input_tokens
    result.context_wrapper.usage.output_tokens += reflected.context_wrapper.usage.output_tokens
    result.context_wrapper.usage.total_tokens += reflected.context_wrapper.usage.total_tokens

    return result


def run(experiment_metadata: ExperimentMetadata, n_times: int = 3):
    write_folder = f"{experiment_metadata.write_folder}/{experiment_metadata.model}/agent_{experiment_metadata.reflection}"
    os.makedirs(write_folder, exist_ok=True)

    with open(f"{write_folder}/experiment_metadata.json", "w", encoding="utf-8") as f:
        json.dump(experiment_metadata.model_dump(), f, indent=4)

    agent = init_agent(experiment_metadata=experiment_metadata)
    manager_agent = init_manager_agent(experiment_metadata=experiment_metadata)
    print(
        f"Agent mode MCP: {'enabled' if _use_mcp_agent() else 'disabled'} "
        f"(servers={len(agent.mcp_servers)})"
    )
    all_decisions: list[dict] = []

    for stock in STOCKS:
        name, cnpj, stock_id = stock.name, stock.cnpj, stock.stock_id
        stock_write_folder = f"{write_folder}/{stock_id}"
        os.makedirs(stock_write_folder, exist_ok=True)
        print(
            f"[agent] ticker={stock_id} start "
            f"(dates={len(ANALYSIS_DATES)} runs={n_times})"
        )
        open_positions_by_run = {experiment_id: [] for experiment_id in range(n_times)}
        strategy_returns_by_run = {experiment_id: [] for experiment_id in range(n_times)}
        last_seen_date_by_run = {experiment_id: None for experiment_id in range(n_times)}
        last_seen_price_by_run = {experiment_id: None for experiment_id in range(n_times)}
        stock_decisions_by_run = {experiment_id: [] for experiment_id in range(n_times)}
        previous_decision_by_run: dict[int, dict | None] = {
            experiment_id: None for experiment_id in range(n_times)
        }

        for analysis_date in ANALYSIS_DATES:
            price = get_price_on_or_before(ticker=stock_id, as_of_date=analysis_date)
            if price is None:
                print(f"Skipping {stock_id} on {analysis_date}: no price on/before {analysis_date}")
                continue
            price_str = f"{price:.2f}"  # US decimal format
            print(f"[agent] ticker={stock_id} date={analysis_date} start")

            for experiment_id in range(n_times):
                previous_decision = previous_decision_by_run.get(experiment_id)
                analyst_file = f"{stock_write_folder}/{stock_id}_{analysis_date}_{experiment_id}.json"
                analyst_output_file = f"{stock_write_folder}/{stock_id}_{analysis_date}_output_{experiment_id}.json"
                manager_file = f"{stock_write_folder}/{stock_id}_{analysis_date}_manager_{experiment_id}.json"
                manager_decision_file = (
                    f"{stock_write_folder}/{stock_id}_{analysis_date}_manager_decision_{experiment_id}.json"
                )
                error_file = f"{stock_write_folder}/{stock_id}_{analysis_date}_error_{experiment_id}.json"

                existing_analyst_output = _load_json(path=analyst_output_file)
                existing_manager_payload = _load_json(path=manager_file)
                if _payload_is_invalid_prompt_fallback(existing_manager_payload):
                    existing_manager_payload = None
                existing_metrics = (
                    existing_analyst_output.get("paper_metrics")
                    if isinstance(existing_analyst_output, dict)
                    else None
                )
                if (
                    os.path.exists(analyst_file)
                    and existing_analyst_output is not None
                    and isinstance(existing_analyst_output.get("manager"), dict)
                    and existing_manager_payload is not None
                    and not _payload_is_invalid_prompt_fallback(existing_manager_payload)
                    and os.path.exists(manager_decision_file)
                    and isinstance(existing_metrics, dict)
                ):
                    existing_decision = _load_json(path=manager_decision_file)
                    if isinstance(existing_decision, dict):
                        existing_decision = _apply_trade_signal(
                            manager_output=existing_decision,
                            stock_id=stock_id,
                            experiment_id=experiment_id,
                            analysis_date=analysis_date,
                            price=float(price),
                            open_positions=open_positions_by_run[experiment_id],
                            strategy_returns=strategy_returns_by_run[experiment_id],
                        )
                        _save_json(path=manager_decision_file, payload=existing_decision)
                        last_seen_date_by_run[experiment_id] = analysis_date
                        last_seen_price_by_run[experiment_id] = float(price)
                        stock_decisions_by_run[experiment_id].append(dict(existing_decision))
                        all_decisions.append(dict(existing_decision))
                        _save_json(path=f"{write_folder}/decisions_sample.json", payload=all_decisions)
                        previous_decision_by_run[experiment_id] = dict(existing_decision)
                        print(
                            f"[agent] ticker={stock_id} date={analysis_date} run={experiment_id} "
                            f"done cached rec={existing_decision.get('recommendation', 'N/A')} "
                            f"action={existing_decision.get('signal_action', 'N/A')}"
                        )
                    continue

                indicators_payload = existing_analyst_output
                indicators_map: dict[str, float] = {}
                analyst_payload_for_usage: dict | None = None

                if indicators_payload is None:
                    try:
                        start = time.time()
                        base_feedback = "Compute all 32 indicators."
                        result = analyse(
                            agent=agent,
                            name=name,
                            cnpj=cnpj,
                            price=price_str,
                            analysis_date=analysis_date,
                            experiment_metadata=experiment_metadata,
                            feedback=base_feedback,
                        )

                        if experiment_metadata.reflection:
                            result = guardrail_reflection(
                                agent=agent,
                                name=name,
                                cnpj=cnpj,
                                price=price_str,
                                analysis_date=analysis_date,
                                result=result,
                                experiment_metadata=experiment_metadata,
                            )
                    except Exception as exc:
                        if _is_invalid_prompt_error(exc):
                            print(
                                f"Skipping {stock_id} on {analysis_date} run {experiment_id}: "
                                "analyst prompt flagged by API policy."
                            )
                            _save_json(
                                path=error_file,
                                payload={
                                    "analysis_date": analysis_date,
                                    "stage": "analyst",
                                    "error_type": type(exc).__name__,
                                    "error_message": str(exc),
                                },
                            )
                            continue
                        raise

                    end = time.time()
                    save_results(
                        write_folder=stock_write_folder,
                        stock_id=stock_id,
                        result=result,
                        elapsed_time=(end - start),
                        experiment_id=experiment_id,
                        analysis_date=analysis_date,
                    )
                    indicators_payload = result.final_output.model_dump()
                    indicators_payload["analysis_date"] = analysis_date
                    indicators_map = _to_map(result=result)
                    indicators_map = _apply_benchmark_indicator_conventions(indicators=indicators_map)
                    indicators_payload = _sync_indicator_payload(
                        payload=indicators_payload,
                        indicators=indicators_map,
                    )
                    analyst_payload_for_usage = _load_json(path=analyst_file)
                else:
                    indicators_map = _to_map_from_payload(payload=indicators_payload)
                    indicators_map = _apply_benchmark_indicator_conventions(indicators=indicators_map)
                    if isinstance(indicators_payload, dict):
                        indicators_payload = _sync_indicator_payload(
                            payload=indicators_payload,
                            indicators=indicators_map,
                        )
                    analyst_payload_for_usage = _load_json(path=analyst_file)

                if not indicators_map:
                    print(
                        f"Skipping manager for {stock_id} on {analysis_date} run {experiment_id}: "
                        "no analyst indicators found."
                    )
                    continue

                usage_metrics = _usage_metrics_from_payload(payload=analyst_payload_for_usage)
                paper_metrics = _paper_metrics_for_output(
                    ticker=stock_id,
                    analysis_date=analysis_date,
                    indicators=indicators_map,
                    usage_metrics=usage_metrics,
                )
                if isinstance(indicators_payload, dict) and isinstance(paper_metrics, dict):
                    indicators_payload["paper_metrics"] = paper_metrics

                previous_decision_context = _format_previous_decision_context(
                    previous_decision=previous_decision
                )
                manager_context_used = previous_decision_context
                manager_payload = existing_manager_payload
                if manager_payload is None:
                    manager_prompt = _manager_prompt(
                        name=name,
                        ticker=stock_id,
                        cnpj=cnpj,
                        analysis_date=analysis_date,
                        price_str=price_str,
                        indicators=indicators_map,
                        previous_decision_context=previous_decision_context,
                    )
                    try:
                        manager_start = time.time()
                        manager_result = _run_with_turn_retry(
                            agent=manager_agent,
                            inp_data=manager_prompt,
                            max_turns=experiment_metadata.max_turns,
                            label="manager",
                        )
                        manager_end = time.time()
                        manager_payload = get_result(
                            result=manager_result,
                            elapsed_time=(manager_end - manager_start),
                        )
                    except Exception as exc:
                        if _is_invalid_prompt_error(exc):
                            print(
                                f"Manager prompt flagged by API policy for {stock_id} on {analysis_date} "
                                f"run {experiment_id}; retrying with redacted previous justification."
                            )
                            redacted_context = _format_previous_decision_context(
                                previous_decision=previous_decision,
                                include_justification=False,
                                include_monthly_report=False,
                            )
                            redacted_prompt = _manager_prompt(
                                name=name,
                                ticker=stock_id,
                                cnpj=cnpj,
                                analysis_date=analysis_date,
                                price_str=price_str,
                                indicators=indicators_map,
                                previous_decision_context=redacted_context,
                            )
                            try:
                                manager_start = time.time()
                                manager_result = _run_with_turn_retry(
                                    agent=manager_agent,
                                    inp_data=redacted_prompt,
                                    max_turns=experiment_metadata.max_turns,
                                    label="manager",
                                )
                                manager_end = time.time()
                                manager_payload = get_result(
                                    result=manager_result,
                                    elapsed_time=(manager_end - manager_start),
                                )
                                if isinstance(manager_payload, dict):
                                    manager_payload["error_recovery"] = {
                                        "recovered_from_invalid_prompt": True,
                                        "strategy": "redacted_previous_justification",
                                        "initial_error_message": str(exc),
                                    }
                                manager_context_used = redacted_context
                            except Exception as retry_exc:
                                if _is_invalid_prompt_error(retry_exc):
                                    print(
                                        f"Manager retry still flagged for {stock_id} on {analysis_date} "
                                        f"run {experiment_id}; saving fallback Hold decision."
                                    )
                                    manager_payload = {
                                        "usage": {
                                            "requests": 0,
                                            "input_tokens": 0,
                                            "output_tokens": 0,
                                            "total_tokens": 0,
                                        },
                                        "steps": [],
                                        "time": 0.0,
                                        "output": {
                                            "recommendation": "Hold",
                                            "monthly_report": (
                                                "Monthly report unavailable because manager prompt was rejected "
                                                "as invalid_prompt by the API."
                                            ),
                                            "justification": "Manager decision unavailable due API invalid_prompt.",
                                            "target_price": float(price),
                                        },
                                        "analysis_date": analysis_date,
                                        "error": {
                                            "stage": "manager",
                                            "error_type": type(retry_exc).__name__,
                                            "error_message": str(retry_exc),
                                            "initial_error_message": str(exc),
                                            "recovery_strategy": "redacted_previous_justification",
                                        },
                                    }
                                    manager_context_used = redacted_context
                                else:
                                    raise
                        else:
                            raise

                if isinstance(manager_payload, dict):
                    manager_payload["analysis_date"] = analysis_date
                    manager_input = manager_payload.get("manager_input")
                    if not isinstance(manager_input, dict):
                        manager_input = {}
                    manager_input = dict(manager_input)
                    manager_input["previous_decision_context"] = manager_context_used
                    manager_payload["manager_input"] = manager_input

                manager_output = manager_payload.get("output", {}) if isinstance(manager_payload, dict) else {}
                if not isinstance(manager_output, dict):
                    manager_output = {}
                manager_output = dict(manager_output)
                manager_output = _apply_trade_signal(
                    manager_output=manager_output,
                    stock_id=stock_id,
                    experiment_id=experiment_id,
                    analysis_date=analysis_date,
                    price=float(price),
                    open_positions=open_positions_by_run[experiment_id],
                    strategy_returns=strategy_returns_by_run[experiment_id],
                )
                last_seen_date_by_run[experiment_id] = analysis_date
                last_seen_price_by_run[experiment_id] = float(price)
                stock_decisions_by_run[experiment_id].append(dict(manager_output))
                all_decisions.append(dict(manager_output))
                _save_json(path=f"{write_folder}/decisions_sample.json", payload=all_decisions)
                previous_decision_by_run[experiment_id] = dict(manager_output)

                if isinstance(manager_payload, dict):
                    manager_payload["output"] = manager_output
                    _save_json(path=manager_file, payload=manager_payload)

                _save_json(path=manager_decision_file, payload=manager_output)

                merged_output = dict(indicators_payload)
                merged_output["analysis_date"] = analysis_date
                merged_output["manager"] = manager_output
                if isinstance(paper_metrics, dict):
                    merged_output["paper_metrics"] = paper_metrics
                _save_json(path=analyst_output_file, payload=merged_output)

                analyst_payload = _load_json(path=analyst_file)
                if isinstance(analyst_payload, dict):
                    analyst_payload["analysis_date"] = analysis_date
                    analyst_payload["manager"] = manager_payload
                    if isinstance(paper_metrics, dict):
                        analyst_payload["paper_metrics"] = paper_metrics
                        output_payload = analyst_payload.get("output")
                        if isinstance(output_payload, dict):
                            output_payload = dict(output_payload)
                            output_payload["analysis_date"] = analysis_date
                            output_payload["paper_metrics"] = paper_metrics
                            analyst_payload["output"] = output_payload
                    _save_json(path=analyst_file, payload=analyst_payload)
                print(
                    f"[agent] ticker={stock_id} date={analysis_date} run={experiment_id} "
                    f"done rec={manager_output.get('recommendation', 'N/A')} "
                    f"action={manager_output.get('signal_action', 'N/A')}"
                )
                if INTER_RUN_SLEEP_SECONDS > 0:
                    time.sleep(INTER_RUN_SLEEP_SECONDS)

        for experiment_id in range(n_times):
            final_date = last_seen_date_by_run.get(experiment_id)
            final_price = last_seen_price_by_run.get(experiment_id)
            if final_date is None or final_price is None:
                continue

            liquidation_event = _build_forced_final_liquidation(
                stock_id=stock_id,
                experiment_id=experiment_id,
                final_date=str(final_date),
                final_price=float(final_price),
                open_positions=open_positions_by_run[experiment_id],
                strategy_returns=strategy_returns_by_run[experiment_id],
            )
            if liquidation_event is not None:
                forced_file = (
                    f"{stock_write_folder}/{stock_id}_{final_date}_forced_liquidation_{experiment_id}.json"
                )
                _save_json(path=forced_file, payload=liquidation_event)
                stock_decisions_by_run[experiment_id].append(dict(liquidation_event))
                all_decisions.append(dict(liquidation_event))
                _save_json(path=f"{write_folder}/decisions_sample.json", payload=all_decisions)
                print(
                    f"[agent] ticker={stock_id} date={final_date} run={experiment_id} "
                    "forced_final_liquidation"
                )

            decisions_file = f"{stock_write_folder}/{stock_id}_decisions_{experiment_id}.json"
            if stock_decisions_by_run[experiment_id]:
                _save_json(path=decisions_file, payload=stock_decisions_by_run[experiment_id])

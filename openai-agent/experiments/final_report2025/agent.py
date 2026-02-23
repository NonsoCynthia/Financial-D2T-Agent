import asyncio
import contextlib
import json
import os
import sys
import time
from pathlib import Path

from agents import Agent, ModelSettings, Runner, RunResult
from agents.exceptions import MaxTurnsExceeded
from agents.mcp import MCPServerStdio
from experiments import ExperimentMetadata, Intensity
from experiments.final_report2025.config import STOCKS, ANALYSIS_DATES
from experiments.utils import get_result, save_results
from openai.types.shared import Reasoning

from tools import code_interpreter, us_reports_query_tool, us_share_composition_query_tool
from financial_agents import get_agent
from financial_agents.financial_analyst import FINANCIAL_ANALYST_INSTRUCTION
from financial_agents.financial_manager import MANAGER_INSTRUCTION, ManagerDecision
from financial_agents.us_indicator_schema import IndicatorOutput, Indicator
from db import get_price_on_or_before

from experiments.validation.indicator_sanity import find_sanity_issues, indicators_to_recompute


TEMPLATE_INPUT = """Run fundamental analysis for {name} (CIK {cnpj}) as of {analysis_date} with last price {price_str} USD.
Feedback: {feedback}"""

MANAGER_TEMPLATE_INPUT = """Make an investment decision for {name} ({ticker}, CIK {cnpj}) as of {analysis_date}.
Current price: {price_str} USD.

You are given the analyst indicators below:
{indicator_lines}

Return your response in the required schema with:
- recommendation (Buy/Sell/Hold)
- justification
- target_price
"""


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
    model_settings = ModelSettings(tool_choice="required")
    if experiment_metadata.reasoning is not None:
        reasoning = Reasoning(effort=experiment_metadata.reasoning)
        model_settings = ModelSettings(reasoning=reasoning, verbosity=experiment_metadata.verbosity)

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
        return await Runner.run(agent, input=inp_data, max_turns=max_turns)


def _run_with_turn_retry(agent: Agent, inp_data: str, max_turns: int, label: str) -> RunResult:
    try:
        return asyncio.run(_run_with_optional_mcp(agent, inp_data, max_turns))
    except MaxTurnsExceeded:
        retry_turns = max(30, max_turns * 2)
        print(f"{label}: max turns exceeded at {max_turns}. Retrying with {retry_turns}.")
        return asyncio.run(_run_with_optional_mcp(agent, inp_data, retry_turns))


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
    return _run_with_turn_retry(agent, inp_data, experiment_metadata.max_turns, label="analyst")


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


def _manager_prompt(name: str, ticker: str, cnpj: str, analysis_date: str, price_str: str, indicators: dict[str, float]) -> str:
    indicator_lines = "\n".join([f"- {k}: {v}" for k, v in sorted(indicators.items())])
    return MANAGER_TEMPLATE_INPUT.format(
        name=name,
        ticker=ticker,
        cnpj=cnpj,
        analysis_date=analysis_date,
        price_str=price_str,
        indicator_lines=indicator_lines,
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


def _save_json(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)


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
    current = _to_map(result)

    missing = [k for k in expected if (k not in current) or (current[k] == 0.0)]
    issues = find_sanity_issues(current)
    suspect = indicators_to_recompute(issues)

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

    for stock in STOCKS:
        name, cnpj, stock_id = stock.name, stock.cnpj, stock.stock_id
        for analysis_date in ANALYSIS_DATES:
            price = get_price_on_or_before(ticker=stock_id, as_of_date=analysis_date)
            if price is None:
                print(f"Skipping {stock_id} on {analysis_date}: no price on/before {analysis_date}")
                continue
            price_str = f"{price:.2f}"  # US decimal format

            for experiment_id in range(n_times):
                analyst_file = f"{write_folder}/{stock_id}_{analysis_date}_{experiment_id}.json"
                analyst_output_file = f"{write_folder}/{stock_id}_{analysis_date}_output_{experiment_id}.json"
                manager_file = f"{write_folder}/{stock_id}_{analysis_date}_manager_{experiment_id}.json"
                manager_decision_file = (
                    f"{write_folder}/{stock_id}_{analysis_date}_manager_decision_{experiment_id}.json"
                )

                existing_analyst_output = _load_json(analyst_output_file)
                existing_manager_payload = _load_json(manager_file)
                if (
                    os.path.exists(analyst_file)
                    and existing_analyst_output is not None
                    and isinstance(existing_analyst_output.get("manager"), dict)
                    and existing_manager_payload is not None
                    and os.path.exists(manager_decision_file)
                ):
                    continue

                indicators_payload = existing_analyst_output
                indicators_map: dict[str, float] = {}

                if indicators_payload is None:
                    start = time.time()
                    base_feedback = "Compute all 32 indicators."
                    result = analyse(
                        agent,
                        name,
                        cnpj,
                        price_str,
                        analysis_date,
                        experiment_metadata,
                        base_feedback,
                    )

                    if experiment_metadata.reflection:
                        result = guardrail_reflection(
                            agent,
                            name,
                            cnpj,
                            price_str,
                            analysis_date,
                            result,
                            experiment_metadata,
                        )

                    end = time.time()
                    save_results(
                        write_folder,
                        stock_id,
                        result,
                        end - start,
                        experiment_id,
                        analysis_date=analysis_date,
                    )
                    indicators_payload = result.final_output.model_dump()
                    indicators_payload["analysis_date"] = analysis_date
                    indicators_map = _to_map(result)
                else:
                    indicators_map = _to_map_from_payload(indicators_payload)

                if not indicators_map:
                    print(
                        f"Skipping manager for {stock_id} on {analysis_date} run {experiment_id}: "
                        "no analyst indicators found."
                    )
                    continue

                manager_payload = existing_manager_payload
                if manager_payload is None:
                    manager_prompt = _manager_prompt(
                        name=name,
                        ticker=stock_id,
                        cnpj=cnpj,
                        analysis_date=analysis_date,
                        price_str=price_str,
                        indicators=indicators_map,
                    )
                    manager_start = time.time()
                    manager_result = _run_with_turn_retry(
                        manager_agent,
                        manager_prompt,
                        experiment_metadata.max_turns,
                        label="manager",
                    )
                    manager_end = time.time()
                    manager_payload = get_result(manager_result, manager_end - manager_start)

                if isinstance(manager_payload, dict):
                    manager_payload["analysis_date"] = analysis_date

                manager_output = manager_payload.get("output", {}) if isinstance(manager_payload, dict) else {}
                if not isinstance(manager_output, dict):
                    manager_output = {}
                manager_output = dict(manager_output)
                manager_output["analysis_date"] = analysis_date

                if isinstance(manager_payload, dict):
                    manager_payload["output"] = manager_output
                    _save_json(manager_file, manager_payload)

                _save_json(manager_decision_file, manager_output)

                merged_output = dict(indicators_payload)
                merged_output["analysis_date"] = analysis_date
                merged_output["manager"] = manager_output
                _save_json(analyst_output_file, merged_output)

                analyst_payload = _load_json(analyst_file)
                if isinstance(analyst_payload, dict):
                    analyst_payload["analysis_date"] = analysis_date
                    analyst_payload["manager"] = manager_payload
                    _save_json(analyst_file, analyst_payload)
                time.sleep(10)

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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
from finAgents.financial_agents.financial_analyst import Indicator, find_missing_indicators_from_json


def parse_json_strict(text: str) -> Dict[str, Any]:
    """
    Parse a JSON string into a Python dict.

    Raises json.JSONDecodeError if invalid.
    """
    return json.loads(text)


async def repair_to_json(agent: Agent, bad_output: str, expected_schema: str) -> Dict[str, Any]:
    """
    Ask the model to convert invalid JSON-like output into valid JSON.

    This is a recovery mechanism when the model returns extra text.
    """
    prompt = JSON_REPAIR_PROMPT.format(expected_schema=expected_schema, bad_output=bad_output)
    result = await Runner.run(agent, prompt)
    return parse_json_strict(result.final_output)


def save_run_json(ticker: str, as_of_date: str, payload: Dict[str, Any]) -> Path:
    """
    Save the combined run output as a JSON file under results/agents/.

    returns: path to the saved JSON file
    """
    out_dir = Path("results") / "agents"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{ticker}_{as_of_date}_{run_id}.json"

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


async def main() -> None:
    """
    Run analyst then manager using MCP tools, enforce JSON outputs, and save results to disk.
    """
    server_path = Path(__file__).resolve().parent / "finAgents" / "server_us_finance.py"

    async with MCPServerStdio(
        name="US Finance MCP Server",
        params={"command": sys.executable, "args": [str(server_path)]},
        cache_tools_list=True,
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

        ticker = "TSLA"
        as_of_date = "2025-12-31"

        analyst_prompt = ANALYST_TASK_PROMPT.format(ticker=ticker, as_of_date=as_of_date)
        analyst_result = await Runner.run(analyst, analyst_prompt)

        analyst_schema = (
            '{ "ticker": "TSLA", "as_of_date": "2025-12-31", '
            '"indicators": {"last_price": 123.45, "roe": 0.10}, '
            '"methodology": {"notes": "...", "sources": ["get_panel"]} }'
        )

        try:
            analyst_json = parse_json_strict(analyst_result.final_output)
        except json.JSONDecodeError:
            analyst_json = await repair_to_json(analyst, analyst_result.final_output, analyst_schema)

        expected = [i.value for i in Indicator]
        missing = find_missing_indicators_from_json(analyst_json, expected)

        if missing:
            missing_block = "\n".join(missing)
            reflection_prompt = REFLECTION_TASK_PROMPT.format(
                ticker=ticker,
                as_of_date=as_of_date,
                missing_indicators=missing_block,
            )
            reflection_result = await Runner.run(analyst, reflection_prompt)

            reflection_schema = (
                '{ "ticker": "TSLA", "as_of_date": "2025-12-31", '
                '"indicators": {"roe": 0.10} }'
            )

            try:
                reflection_json = parse_json_strict(reflection_result.final_output)
            except json.JSONDecodeError:
                reflection_json = await repair_to_json(analyst, reflection_result.final_output, reflection_schema)

            base_ind = analyst_json.get("indicators", {})
            ref_ind = reflection_json.get("indicators", {})

            if not isinstance(base_ind, dict):
                base_ind = {}
            if not isinstance(ref_ind, dict):
                ref_ind = {}

            for k, v in ref_ind.items():
                base_ind[k] = v

            analyst_json["indicators"] = base_ind

        manager_prompt = MANAGER_TASK_PROMPT.format(
            ticker=ticker,
            as_of_date=as_of_date,
            analyst_report_json=json.dumps(analyst_json, indent=2),
        )
        manager_result = await Runner.run(manager, manager_prompt)

        manager_schema = (
            '{ "ticker": "TSLA", "as_of_date": "2025-12-31", '
            '"recommendation": "BUY", "interpretation": "...", "justification": "..." }'
        )

        try:
            manager_json = parse_json_strict(manager_result.final_output)
        except json.JSONDecodeError:
            manager_json = await repair_to_json(manager, manager_result.final_output, manager_schema)

        final_payload = {
            "ticker": ticker,
            "as_of_date": as_of_date,
            "finacial_analyst": analyst_json,
            "finacial_manager": manager_json,
        }

        saved_path = save_run_json(ticker, as_of_date, final_payload)

        print("\nSaved result JSON:", saved_path)
        print("\nManager JSON:\n", json.dumps(manager_json, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

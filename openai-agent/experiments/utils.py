import json
from agents import RunResult
from typing import TypedDict


class LLMUsage(TypedDict):
    requests: int
    input_tokens: int
    output_tokens: int
    total_tokens: int


class AgentResult(TypedDict):
    usage: LLMUsage
    steps: list
    time: float
    output: dict


def get_result(result: RunResult, elapsed_time: float) -> AgentResult:
    usage = result.context_wrapper.usage
    steps = [item.to_input_item() for item in result.new_items]

    return AgentResult(
        usage=LLMUsage(
            requests=usage.requests,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
        ),
        steps=steps,
        time=elapsed_time,
        output=result.final_output.model_dump(), ###
    )


def save_results(
    write_folder: str,
    stock_id: str,
    result: RunResult,
    elapsed_time: float,
    experiment_id: int,
    analysis_date: str | None = None,
) -> None:
    agent_result = get_result(result=result, elapsed_time=elapsed_time)
    stock_key = f"{stock_id}_{analysis_date}" if analysis_date else stock_id

    if analysis_date:
        agent_result["analysis_date"] = analysis_date

    with open(f"{write_folder}/{stock_key}_{experiment_id}.json", "w", encoding="utf-8") as f:
        json.dump(agent_result, f, indent=4)

    output_payload = agent_result.get("output")
    if analysis_date and isinstance(output_payload, dict):
        output_payload = dict(output_payload)
        output_payload["analysis_date"] = analysis_date

    with open(f"{write_folder}/{stock_key}_output_{experiment_id}.json", "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=4)

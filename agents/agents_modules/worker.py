__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Worker agent that executes tasks based on the orchestrator's instructions.
"""

from typing import Dict, List, Text, Any, Union, Optional
import json
import re

from langchain_classic.agents import AgentExecutor, create_json_chat_agent
from langchain_core.prompts import MessagesPlaceholder, ChatPromptTemplate
from langgraph.errors import GraphRecursionError

from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import UnifiedModel, resolve_model_config
from agents.agent_prompts import WORKER_SYSTEM_PROMPT, WORKER_HUMAN_PROMPT
from agents.utilities.agent_utils import apply_variable_substitution, _handle_parsing_errors
from agents.utilities.token_tracker import token_tracking_callback


class TaskWorker:
    @classmethod
    def init(
        cls,
        description: Text,
        tools: List[Any],
        context: Union[Text, Dict[str, Any]],
        provider: str = "ollama",
        model_name_override: Optional[str] = None,
        reasoning_effort: Optional[str] = None,
    ) -> Any:
        params = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            reasoning_effort=reasoning_effort,
        )
        model = UnifiedModel(provider=provider, **params).raw_model()

        agent_description = (
            apply_variable_substitution(description, context) if description else ""
        )

        if tools:
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        (
                            "AGENT DESCRIPTION:\n"
                            f"{agent_description}\n\n"
                            "EXECUTION INSTRUCTION:\n"
                            f"{WORKER_SYSTEM_PROMPT}"
                        ),
                    ),
                    MessagesPlaceholder(variable_name="chat_history", optional=True),
                    ("human", WORKER_HUMAN_PROMPT),
                ]
            ).partial(output_format="text")

            return AgentExecutor(
                agent=create_json_chat_agent(model, tools, prompt),
                tools=tools,
                verbose=True,
                max_iterations=max(4, 4 * len(tools)),
                handle_parsing_errors=_handle_parsing_errors,
                return_result_steps=True,
            )

        # Text-only workers do not need the JSON tool-calling shell. Using a
        # plain prompt chain avoids parser failures that waste retries/tokens.
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    agent_description,
                ),
                ("human", "{input}"),
            ]
        )
        return prompt | model

    @classmethod
    def execute(cls, agent: AgentExecutor, role: str):
        role = role.strip().lower()

        def latest_output_for(history: List[AgentStepOutput], agent_name: str) -> Text:
            target = agent_name.strip().lower()
            for step in reversed(history):
                if step.agent_name.strip().lower() == target:
                    return str(step.agent_output)
            return ""

        def normalise_feedback(raw: Any) -> Text:
            if not raw:
                return ""
            if isinstance(raw, (dict, list)):
                try:
                    return json.dumps(raw, ensure_ascii=False, indent=2)
                except Exception:
                    return str(raw)
            return str(raw)

        def build_metadata_block(state: ExecutionState) -> Text:
            sample_meta = state.get("sample_metadata", {}) or {}
            lines: List[str] = ["REPORT METADATA:"]

            analysis_date = str(state.get("analysis_date", "") or sample_meta.get("analysis_date", "")).strip()
            end_date = str(state.get("end_date", "")).strip()
            horizon_months = str(state.get("horizon_months", "")).strip()
            tickers = sample_meta.get("tickers") or []
            previous_report = str(sample_meta.get("previous_report", "") or "").strip()

            if analysis_date:
                lines.append(f"- analysis_date={analysis_date}")
                lines.append(f"- price_reference_date={analysis_date}")
            if end_date:
                lines.append(f"- coverage_window_end_date={end_date}")
            if horizon_months:
                lines.append(f"- investment_horizon_months={horizon_months}")
            if isinstance(tickers, list) and tickers:
                lines.append(f"- coverage_universe={', '.join(str(t) for t in tickers)}")

            stocks = sample_meta.get("stocks") or []
            if stocks:
                lines.append("AUTHORITATIVE PER-TICKER DECISIONS:")
                lines.append(
                    "- Treat Recommendation and TargetPrice below as canonical ground truth. "
                    "Do not recalculate target prices from valuation anchors mentioned in the justification."
                )
                for row in stocks:
                    bits = [
                        f"recommendation={row.get('recommendation', 'N/A')}",
                        f"target_price={row.get('target_price', 'N/A')}",
                    ]
                    if row.get("current_price") and row.get("current_price") != "N/A":
                        bits.append(f"current_price={row['current_price']}")
                    if row.get("implied_move_pct") and row.get("implied_move_pct") != "N/A":
                        bits.append(f"implied_move_pct={row['implied_move_pct']}")
                    lines.append(f"- {row.get('ticker', 'UNKNOWN')}: {', '.join(bits)}")

            if previous_report and previous_report != "N/A":
                lines.append("PREVIOUS REPORT CONTEXT:")
                lines.append(previous_report)

            return "\n".join(lines)

        def build_worker_input(state: ExecutionState) -> Text:
            orch_instruction = state.get("next_agent_payload", "").strip()
            history = state.get("history_of_steps", []) or []
            metadata_block = build_metadata_block(state)
            sample_meta = state.get("sample_metadata", {}) or {}
            bundle_context = str(sample_meta.get("prompt_context", "") or state.get("user_prompt", "")).strip()

            if role == "content ordering":
                source_bundle = bundle_context or str(state.get("data_input", ""))
                return (
                    f"Worker: content ordering\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"SOURCE BUNDLE:\n{source_bundle}"
                ).strip()

            if role == "text structuring":
                ordering_output = latest_output_for(history, "content ordering")
                return (
                    f"Worker: text structuring\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"ORDERING OUTPUT: {ordering_output}"
                ).strip()

            if role == "surface realization":
                structuring_output = latest_output_for(history, "text structuring")
                previous_sr_output = latest_output_for(history, "surface realization")

                raw_feedback = (
                    state.get("review")
                    or state.get("guardrail_feedback")
                    or state.get("guardrail_review")
                    or ""
                )
                guardrail_feedback_str = normalise_feedback(raw_feedback)

                previous_block = (
                    f"\nPREV OUTPUT: {previous_sr_output}" if previous_sr_output else ""
                )
                
                feedback_block = (
                    f"\nGUARDRAIL FEEDBACK: {guardrail_feedback_str}"
                    if guardrail_feedback_str else ""
                )

                return (
                    f"Worker: surface realization\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"TEXT INPUT: {structuring_output}"
                    f"{previous_block}"
                    f"{feedback_block}"
                ).strip()

            return orch_instruction

        def run(state: ExecutionState):
            # Global iteration counter
            idx = state.get("iteration_count", 0)
            history = state.get("history_of_steps", []) or []

            # Per worker attempt bookkeeping
            worker_attempts: Dict[str, int] = state.get("worker_attempts", {}) or {}
            current_attempts = worker_attempts.get(role, 0)

            # Resolve max attempts for this worker
            max_cfg = state.get("max_worker_attempts", None)
            max_for_role: Union[int, None] = None
            if isinstance(max_cfg, int):
                max_for_role = max_cfg
            elif isinstance(max_cfg, dict):
                # tolerate non int values
                raw_val = max_cfg.get(role)
                try:
                    max_for_role = int(raw_val) if raw_val is not None else None
                except (TypeError, ValueError):
                    max_for_role = None

            # If this worker has already used up its budget. do not call the LLM again
            if max_for_role is not None and current_attempts >= max_for_role:
                # No new worker output exists to evaluate, so send control back to
                # the orchestrator rather than paying for another guardrail pass on
                # the same stale worker output.
                print("Moving forward to orchestrator")
                return {
                    "next_agent": "orchestrator",
                    "history_of_steps": history,
                    "iteration_count": idx,
                    "worker_attempts": worker_attempts,
                    "last_worker": role,
                    "max_worker_attempts": max_cfg,
                    "review": state.get("review", ""),
                    "passed_stages": state.get("passed_stages", []),
                    "closed_stages": state.get("closed_stages", []),
                }

            # Build input and run the worker LLM
            inputs = build_worker_input(state)

            try:
                out = agent.invoke({"input": inputs}, config={"callbacks": [token_tracking_callback()]})
                if isinstance(out, dict):
                    text = (
                        out.get("output")
                        or out.get("action_input")
                        or getattr(out, "content", str(out))
                    )
                    tools = out.get("result_steps", [])
                else:
                    text = getattr(out, "content", str(out))
                    tools = []
            except GraphRecursionError:
                text, tools = "Too many iterations. Try splitting task.", []

            # Update attempts only when the worker actually ran
            worker_attempts[role] = current_attempts + 1

            print(f"[Worker: {role}] Attempt {worker_attempts[role]} / {max_for_role or 'unlimited'}")
            print("Moving forward to guardrail")

            history.append(
                AgentStepOutput(
                    agent_name=role,
                    agent_input=inputs,
                    agent_output=text,
                    rationale="",
                    tool_steps=tools,
                )
            )

            return {
                "next_agent": "guardrail",
                "history_of_steps": history,
                "iteration_count": idx + 1,
                "worker_attempts": worker_attempts,
                "last_worker": role,
                "max_worker_attempts": max_cfg,
                "passed_stages": state.get("passed_stages", []),
                "closed_stages": state.get("closed_stages", []),
            }

        return run

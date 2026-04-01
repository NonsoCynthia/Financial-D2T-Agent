# agents/agents_modules/task.py

__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Unified worker that can perform content ordering, text structuring,
    and surface realization using a single prompt.
"""

from typing import Any, Dict, List, Text, Union
import json

from langchain_classic.agents import AgentExecutor, create_json_chat_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.agent_prompts import WORKER_SYSTEM_PROMPT, WORKER_HUMAN_PROMPT
from agents.llm_model import UnifiedModel, resolve_model_config
from agents.agent_prompts import (
    UNIFIED_WORKER_PROMPT_EN,
    UNIFIED_WORKER_PROMPT_GA,
)
from agents.utilities.agent_utils import apply_variable_substitution, _handle_parsing_errors
from agents.utilities.token_tracker import token_tracking_callback


class UnifiedTaskWorker:
    @classmethod
    def init(
        cls,
        provider: str = "ollama",
        language: str = "en",
        model_name_override: str | None = None,
        reasoning_effort: str | None = None,
    ) -> Any:
        """
        Initialise a unified worker as a JSON agent:
        - system message = unified worker spec + generic worker instructions
        - human message = WORKER_HUMAN_PROMPT (which will receive {input})
        """
        params = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            temperature=0.0,
            reasoning_effort=reasoning_effort,
        )
        model = UnifiedModel(provider=provider, **params).raw_model()

        # Choose unified prompt for language
        base_prompt = (
            UNIFIED_WORKER_PROMPT_GA
            if language.lower() == "ga"
            else UNIFIED_WORKER_PROMPT_EN
        )

        tools: List[Any] = []
        if tools:
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        (
                            "AGENT DESCRIPTION:\n"
                            f"{base_prompt}\n\n"
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

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", base_prompt),
                ("human", "{input}"),
            ]
        )
        return prompt | model

    @classmethod
    def execute(cls, agent: AgentExecutor, language: str = "en"):
        """
        Return a LangGraph node function that:
        - Builds a stage specific input for the unified worker
        - Calls the JSON agent
        - Logs the step into history_of_steps
        - Hands control back to the guardrail
        """

        def latest_output_for(
            history: List[AgentStepOutput],
            agent_name: str,
        ) -> Text:
            target = agent_name.strip().lower()
            for step in reversed(history):
                if getattr(step, "agent_name", "").strip().lower() == target:
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

        def build_task_input(state: ExecutionState):
            """
            Build the text that will be given to the unified worker.

            Returns (task_name, payload_for_model).
            task_name is one of 'content ordering', 'text structuring', 'surface realization'.
            """
            task = state.get("next_agent", "").strip().lower()
            orch_instruction = state.get("next_agent_payload", "").strip()
            history = state.get("history_of_steps", []) or []
            metadata_block = build_metadata_block(state)
            sample_meta = state.get("sample_metadata", {}) or {}
            bundle_context = str(sample_meta.get("prompt_context", "") or state.get("user_prompt", "")).strip()

            if task == "content ordering":
                source_bundle = bundle_context or str(state.get("data_input", ""))
                payload = (
                    "Task: content ordering\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"SOURCE BUNDLE:\n{source_bundle}"
                ).strip()
                return task, payload

            if task == "text structuring":
                ordering_output = latest_output_for(history, "content ordering")
                payload = (
                    "Task: text structuring\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"ORDERING OUTPUT: {ordering_output}"
                ).strip()
                return task, payload

            if task == "surface realization":
                structuring_output = latest_output_for(history, "text structuring")
                prev_sr_output = latest_output_for(history, "surface realization")

                raw_feedback = (
                    state.get("review")
                    or state.get("guardrail_feedback")
                    or state.get("guardrail_review")
                    or ""
                )
                feedback_str = normalise_feedback(raw_feedback)

                prev_block = f"\nPREV OUTPUT:\n{prev_sr_output}" if prev_sr_output else ""
                feedback_block = (
                    f"\nGUARDRAIL FEEDBACK:\n{feedback_str}" if feedback_str else ""
                )

                payload = (
                    "Task: surface realization\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"TEXT INPUT:\n{structuring_output}"
                    f"{prev_block}"
                    f"{feedback_block}"
                ).strip()
                return task, payload

            # Fallback
            payload = f"Task: unknown\nINSTRUCTION: {orch_instruction}"
            return task or "unknown", payload

        def run(state: ExecutionState):
            idx = state.get("iteration_count", 0)
            history = state.get("history_of_steps", []) or []

            task_name, worker_input = build_task_input(state)

            out = agent.invoke({"input": worker_input}, config={"callbacks": [token_tracking_callback()]})

            if isinstance(out, dict):
                raw_output = out.get("output", out)
                if isinstance(raw_output, dict) and "action_input" in raw_output:
                    text = raw_output["action_input"]
                elif isinstance(raw_output, str):
                    text = raw_output
                elif "action_input" in out:
                    text = out["action_input"]
                else:
                    text = getattr(raw_output, "content", str(raw_output))
                tool_steps = out.get("result_steps", [])
            else:
                text = getattr(out, "content", str(out))
                tool_steps = []

            history.append(
                AgentStepOutput(
                    agent_name=task_name,
                    agent_input=worker_input,
                    agent_output=text,
                    rationale=text,
                    tool_steps=tool_steps,
                )
            )

            worker_attempts: Dict[str, int] = state.get("worker_attempts", {}) or {}
            if task_name:
                worker_attempts[task_name] = worker_attempts.get(task_name, 0) + 1

            return {
                "next_agent": "guardrail",
                "history_of_steps": history,
                "iteration_count": idx + 1,
                "worker_attempts": worker_attempts,
                "last_worker": task_name,
                "passed_stages": state.get("passed_stages", []),
                "closed_stages": state.get("closed_stages", []),
            }

        return run

__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Worker agent that executes tasks based on the orchestrator's instructions.

Changes:
    - Numeric anchor and boundary instructions moved to agent prompts
      (TEXT_STRUCTURING_PROMPT and SURFACE_REALIZATION_PROMPT_EN) so the
      system message carries the standing rules and the runtime input carries
      only labelled data sections.
    - source_bundle resolved once at the top of build_worker_input using
      sample_meta["prompt_context"] as the preferred source, with data_input
      as a fallback. This prevents user_prompt being overwritten by routing
      logic between graph nodes.
    - A one-line REMINDER is appended immediately before TEXT INPUT in the SR
      input to reinforce the numeric constraint at the point of generation,
      where models weight instructions most heavily.
    - build_worker_input now returns clean, minimal input strings: labelled
      data sections only, with no inline rule text.
"""

from typing import Dict, List, Text, Any, Union, Optional
import json
import re

from langchain_classic.agents import AgentExecutor, create_json_chat_agent
from langchain_core.prompts import MessagesPlaceholder, ChatPromptTemplate
from langgraph.errors import GraphRecursionError

from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import (
    UnifiedModel,
    extract_text_output,
    resolve_model_config,
    model_label_from_config,
)
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
        """
        Initialise and return a worker agent or plain chain.
        Workers with tools use a JSON chat agent. Text-only workers use a
        plain prompt chain to avoid parser failures that waste retries.
        """
        params = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            reasoning_effort=reasoning_effort,
        )
        model_label = model_label_from_config(params)
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

            executor = AgentExecutor(
                agent=create_json_chat_agent(model, tools, prompt),
                tools=tools,
                verbose=True,
                max_iterations=max(4, 4 * len(tools)),
                handle_parsing_errors=_handle_parsing_errors,
                return_result_steps=True,
            )
            try:
                setattr(executor, "_token_model_name", model_label)
            except Exception:
                pass
            return executor

        # Text-only workers use a plain chain to avoid JSON parser overhead.
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", agent_description),
                ("human", "{input}"),
            ]
        )
        chain = prompt | model
        try:
            setattr(chain, "_token_model_name", model_label)
        except Exception:
            pass
        return chain

    @classmethod
    def execute(
        cls,
        agent: AgentExecutor,
        role: str,
        success_next_agent: str = "guardrail",
    ):
        """
        Return a LangGraph node function that runs the worker for the given role.
        The returned function reads state, builds a minimal input string,
        invokes the agent, and returns updated state.
        """
        role = role.strip().lower()
        success_next_agent = str(success_next_agent or "guardrail").strip().lower()
        model_name = str(getattr(agent, "_token_model_name", "") or "").strip()

        # ------------------------------------------------------------------
        # Internal helpers
        # ------------------------------------------------------------------

        def latest_output_for(history: List[AgentStepOutput], agent_name: str) -> Text:
            """Return the most recent output for a named agent from history."""
            target = agent_name.strip().lower()
            for step in reversed(history):
                if step.agent_name.strip().lower() == target:
                    return str(step.agent_output)
            return ""

        def normalise_feedback(raw: Any) -> Text:
            """Serialise guardrail feedback to a plain string regardless of type."""
            if not raw:
                return ""
            if isinstance(raw, (dict, list)):
                try:
                    return json.dumps(raw, ensure_ascii=False, indent=2)
                except Exception:
                    return str(raw)
            return str(raw)

        def resolve_source_bundle(state: ExecutionState) -> Text:
            """
            Resolve the raw source bundle from state.
            prompt_context from sample_metadata is preferred because it is set
            at ingestion time and is never overwritten by graph routing logic.
            data_input is the fallback for backwards compatibility.
            """
            sample_meta = state.get("sample_metadata", {}) or {}
            return (
                str(sample_meta.get("prompt_context", "") or "").strip()
                or str(state.get("data_input", "") or "").strip()
            )

        def build_metadata_block(state: ExecutionState) -> Text:
            """
            Build the REPORT METADATA block that every worker receives.
            Contains dates, horizon, coverage universe, and canonical per-ticker
            decisions (recommendation, target price, current price, implied move).
            Also includes the previous report context when available.
            """
            sample_meta = state.get("sample_metadata", {}) or {}
            lines: List[str] = ["REPORT METADATA:"]

            analysis_date = str(
                state.get("analysis_date", "") or sample_meta.get("analysis_date", "")
            ).strip()
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
                    "- Treat Recommendation and TargetPrice below as canonical "
                    "ground truth. Do not recalculate target prices from valuation "
                    "anchors mentioned in the justification."
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
                
            if previous_report and previous_report != "N/A":
                lines.append("PREVIOUS REPORT CONTEXT:")
                lines.append(previous_report)
            else:
                lines.append(
                    "PREVIOUS REPORT CONTEXT: None. This is the inaugural report "
                    "for this coverage set. Do not reference any prior month, prior "
                    "recommendation, prior target price, or prior metric value anywhere "
                    "in your output."
                )

            return "\n".join(lines)

        def build_worker_input(state: ExecutionState) -> Text:
            """
            Build the runtime input string for the current worker invocation.

            The input carries only labelled data sections. All standing rules,
            numeric discipline instructions, and role definitions live in the
            agent prompt (system message) so they are never diluted by data.

            Sections by worker:
              content ordering  — REPORT METADATA, SOURCE BUNDLE
              text structuring  — REPORT METADATA, NUMERIC ANCHOR, ORDERING OUTPUT
              surface realization — REPORT METADATA, NUMERIC BOUNDARY, TEXT INPUT,
                                    and optionally PREV OUTPUT and GUARDRAIL FEEDBACK
            """
            orch_instruction = state.get("next_agent_payload", "").strip()
            history = state.get("history_of_steps", []) or []
            metadata_block = build_metadata_block(state)

            # Resolve once here. All three workers may need it.
            source_bundle = resolve_source_bundle(state)

            # ------------------------------------------------------------------
            # Content ordering
            # ------------------------------------------------------------------
            if role == "content ordering":
                return (
                    f"Worker: content ordering\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"SOURCE BUNDLE:\n{source_bundle}"
                ).strip()

            # ------------------------------------------------------------------
            # Text structuring
            # The NUMERIC ANCHOR section passes the raw source bundle so TS can
            # verify that every number from the ORDERING OUTPUT lands in a <snt>
            # block. The agent prompt explains how to use it.
            # ------------------------------------------------------------------
            if role == "text structuring":
                ordering_output = latest_output_for(history, "content ordering")
                return (
                    f"Worker: text structuring\n"
                    f"{metadata_block}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"NUMERIC ANCHOR:\n{source_bundle}\n"
                    f"ORDERING OUTPUT:\n{ordering_output}"
                ).strip()

            # ------------------------------------------------------------------
            # Surface realization
            # The NUMERIC BOUNDARY section passes the raw source bundle as the
            # authoritative list of permitted figures. The agent prompt explains
            # the constraint in full. A one-line REMINDER is placed immediately
            # before TEXT INPUT because models weight instructions closest to the
            # generation point most heavily.
            # ------------------------------------------------------------------
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
                    f"\nPREV OUTPUT:\n{previous_sr_output}"
                    if previous_sr_output else ""
                )
                feedback_block = (
                    f"\nGUARDRAIL FEEDBACK:\n{guardrail_feedback_str}"
                    if guardrail_feedback_str else ""
                )

                return (
                    f"Worker: surface realization\n"
                    f"{metadata_block}\n"
                    f"NUMERIC BOUNDARY:\n{source_bundle}\n"
                    f"INSTRUCTION: {orch_instruction}\n"
                    f"REMINDER: every number you write must appear in the "
                    f"NUMERIC BOUNDARY above or derive from it by arithmetic.\n"
                    f"TEXT INPUT:\n{structuring_output}"
                    f"{previous_block}"
                    f"{feedback_block}"
                ).strip()

            # Fallback: return the orchestrator instruction as-is.
            return orch_instruction

        # ------------------------------------------------------------------
        # LangGraph node function
        # ------------------------------------------------------------------

        def run(state: ExecutionState):
            """
            Execute the worker for one iteration and return updated state.
            If the worker has exhausted its attempt budget, return control to
            the orchestrator without calling the LLM.
            """
            idx = state.get("iteration_count", 0)
            history = state.get("history_of_steps", []) or []

            worker_attempts: Dict[str, int] = state.get("worker_attempts", {}) or {}
            current_attempts = worker_attempts.get(role, 0)

            # Resolve the attempt limit for this worker.
            max_cfg = state.get("max_worker_attempts", None)
            max_for_role: Union[int, None] = None
            if isinstance(max_cfg, int):
                max_for_role = max_cfg
            elif isinstance(max_cfg, dict):
                raw_val = max_cfg.get(role)
                try:
                    max_for_role = int(raw_val) if raw_val is not None else None
                except (TypeError, ValueError):
                    max_for_role = None

            # Budget exhausted: return to orchestrator without an LLM call.
            if max_for_role is not None and current_attempts >= max_for_role:
                print(f"[Worker: {role}] Budget exhausted. Moving forward to orchestrator.")
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

            inputs = build_worker_input(state)

            try:
                out = agent.invoke(
                    {"input": inputs},
                    config={
                        "callbacks": [
                            token_tracking_callback(agent_name=role, model_name=model_name)
                        ]
                    },
                )
                if isinstance(out, dict):
                    raw_text = out.get("output")
                    if raw_text is None:
                        raw_text = out.get("action_input")
                    text = (
                        raw_text
                        if isinstance(raw_text, str)
                        else extract_text_output(raw_text if raw_text is not None else out)
                    )
                    tools = out.get("result_steps", [])
                else:
                    text = extract_text_output(out)
                    tools = []
            except GraphRecursionError:
                text, tools = "Too many iterations. Try splitting task.", []

            worker_attempts[role] = current_attempts + 1

            print(
                f"[Worker: {role}] Attempt {worker_attempts[role]} / "
                f"{max_for_role or 'unlimited'}"
            )
            print(f"Moving forward to {success_next_agent}")

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
                "next_agent": success_next_agent,
                "history_of_steps": history,
                "iteration_count": idx + 1,
                "worker_attempts": worker_attempts,
                "last_worker": role,
                "max_worker_attempts": max_cfg,
                "passed_stages": state.get("passed_stages", []),
                "closed_stages": state.get("closed_stages", []),
            }

        return run
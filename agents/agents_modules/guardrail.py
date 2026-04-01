__author__='chinonsocynthiaosuji'

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Provide guardrail evaluation for worker outputs
"""

import re
import json
from langchain_classic.agents import AgentExecutor
from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import UnifiedModel, resolve_model_config
from agents.agent_prompts import (
    GUARDRAIL_PROMPT,
    GUARDRAIL_INPUT,
    GUARDRAIL_PROMPT_CONTENT_ORDERING,
    GUARDRAIL_PROMPT_TEXT_STRUCTURING,
    GUARDRAIL_PROMPT_SURFACE_REALIZATION, # Imported the new unified prompt
)
from agents.utilities.token_tracker import track_response


def _is_correct_verdict(raw_text: str) -> bool:
    text = str(raw_text or "").strip()
    if not text:
        return False

    overall_match = re.search(r"OVERALL:\s*(.+)$", text, re.MULTILINE | re.IGNORECASE)
    candidate = overall_match.group(1).strip() if overall_match else text
    candidate = candidate.splitlines()[0].strip()

    return bool(re.match(r"^CORRECT\b", candidate, re.IGNORECASE))

class TaskGuardrail:
    provider = "openai"
    model_name_override = None

    @classmethod
    def init(
        cls,
        provider: str = "ollama",
        model_name_override: str | None = None,
        reasoning_effort: str | None = None,
    ) -> AgentExecutor:
        cls.provider = provider
        cls.model_name_override = model_name_override
        conf = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            temperature=0.0,
            reasoning_effort=reasoning_effort,
        )
        return UnifiedModel(provider=provider, **conf).model_(GUARDRAIL_PROMPT)

    @classmethod
    def evaluate(cls, agent: AgentExecutor):
        def run(state: ExecutionState):
            history = state.get("history_of_steps", [])
            idx = state.get("iteration_count", 0)
            max_iter = state.get("max_iteration", 50)
            user_input = state.get("user_prompt", "")
            data_input = state.get("data_input", "")
            expected_order = ["content ordering", "text structuring", "surface realization"]
            passed_stages = [
                str(name).strip().lower()
                for name in (state.get("passed_stages", []) or [])
                if str(name).strip()
            ]
            closed_stages = [
                str(name).strip().lower()
                for name in (state.get("closed_stages", []) or [])
                if str(name).strip()
            ]
            worker_attempts = state.get("worker_attempts", {}) or {}
            raw_limits = state.get("max_worker_attempts", 3)

            def limit_for(worker_name: str) -> int:
                if isinstance(raw_limits, dict):
                    try:
                        return int(raw_limits.get(worker_name, 3))
                    except Exception:
                        return 3
                try:
                    return int(raw_limits)
                except Exception:
                    return 3

            def next_route(done: bool, passed: list[str]) -> str:
                passed_set = {
                    str(name).strip().lower()
                    for name in passed
                    if str(name).strip()
                }
                if done and set(expected_order).issubset(passed_set):
                    return "finalizer"
                return "orchestrator"

            # Identify the latest orchestrator, worker, and guardrail steps. We use
            # indices so we can cheaply detect whether the guardrail is being asked
            # to re-evaluate the exact same worker output again.
            orch = next((s for s in reversed(history) if s.agent_name == "orchestrator"), None)
            worker = None
            worker_idx = -1
            guardrail_idx = -1
            last_guardrail_output = ""

            for idx_step in range(len(history) - 1, -1, -1):
                step = history[idx_step]
                step_name = str(getattr(step, "agent_name", "") or "").strip().lower()
                if guardrail_idx < 0 and step_name == "guardrail":
                    guardrail_idx = idx_step
                    last_guardrail_output = str(getattr(step, "agent_output", "") or "").strip()
                elif worker is None and step_name not in ["orchestrator", "guardrail"]:
                    worker = step
                    worker_idx = idx_step
                if orch is not None and worker is not None and guardrail_idx >= 0:
                    break
            
            task, task_input, output, rationale = "", "", "", ""
            if orch:
                rationale = orch.rationale
                match = re.search(
                    r"^(.*?)\(input=['\"](.*?)['\"](?:,\s*instruction=['\"].*?['\"])?\)$",
                    orch.agent_output.strip(),
                    re.DOTALL,
                )
                if not match:
                    match = re.search(
                        r"^(.*?)\(input=['\"](.*?)['\"]",
                        orch.agent_output.strip(),
                        re.DOTALL,
                    )
                if match:
                    task, task_input = match.groups()
                    task = task.strip().lower()
            if worker:
                worker_name = str(getattr(worker, "agent_name", "") or "").strip().lower()
                if worker_name in expected_order:
                    task = worker_name
                output = worker.agent_output

            # Prepare base context
            base_context = f"""Orchestrator Thought: {rationale}\nWorker Input: {task_input}\nWorker Output: {output}"""
            prompt = GUARDRAIL_INPUT.format(input=base_context)
            final_verdict = ""

            # If no new worker output has appeared since the last guardrail step,
            # do not pay for another guardrail model call. Reuse the last verdict.
            if worker_idx >= 0 and guardrail_idx > worker_idx and last_guardrail_output:
                final_verdict = last_guardrail_output
                done = _is_correct_verdict(final_verdict)
                if done and task in expected_order and task not in passed_stages:
                    passed_stages.append(task)
                if done and task in expected_order and task not in closed_stages:
                    closed_stages.append(task)
                if (
                    not done
                    and task in expected_order
                    and task not in closed_stages
                    and int(worker_attempts.get(task, 0)) >= limit_for(task)
                ):
                    closed_stages.append(task)

                route = next_route(done, passed_stages)
                print(f"Moving forward to {route}")
                return {
                    "next_agent": route,
                    "response": "done" if route == "finalizer" else None,
                    "history_of_steps": history,
                    "iteration_count": idx,
                    "max_iteration": max_iter,
                    "next_agent_payload": user_input,
                    "review": final_verdict,
                    "last_worker": state.get("last_worker", ""),
                    "worker_attempts": worker_attempts,
                    "passed_stages": passed_stages,
                    "closed_stages": closed_stages,
                }
            
            # --- SURFACE REALIZATION (OPTIMIZED: 1 CALL) ---
            if task == "surface realization":
                conf = resolve_model_config(
                    provider=cls.provider,
                    model_override=cls.model_name_override,
                )
                unified_guard = UnifiedModel(cls.provider, **conf).model_(GUARDRAIL_PROMPT_SURFACE_REALIZATION)

                # Prepare source facts + generated text context
                triples_text = data_input
                if isinstance(data_input, list):
                    triples_text = "\n".join(str(t) for t in data_input)

                unified_context = f"""INPUT FACTS:\n{triples_text}\n\nGENERATED TEXT:\n{output}"""
                unified_prompt = GUARDRAIL_INPUT.format(input=unified_context)

                # Single LLM Call
                _raw_msg = unified_guard.invoke({"input": unified_prompt})
                track_response(_raw_msg)
                raw_response = _raw_msg.content.strip()
                
                # Cleanup potential markdown wrapper from LLM (e.g., ```json ... ```)
                clean_json = raw_response.replace("FEEDBACK:", "").strip()
                if "```" in clean_json:
                    clean_json = clean_json.split("```")[1].replace("json", "").strip()

                try:
                    eval_data = json.loads(clean_json)
                    
                    overall_status = eval_data.get("overall_verdict", "FAIL").upper()
                    ling_score = eval_data.get("linguistic_score", "FAIL")
                    ling_feed = eval_data.get("linguistic_feedback", "")
                    omissions = eval_data.get("omissions", [])
                    additions = eval_data.get("additions", [])

                    # Construct readable feedback for the Orchestrator
                    review_parts = [
                        "=== GUARDRAIL REVIEW (Unified) ===",
                        f"[Linguistic Quality]: {ling_score} - {ling_feed}",
                        f"[Factuality]: {eval_data.get('factuality_verdict', 'FAIL')}",
                    ]
                    if omissions:
                        review_parts.append(f"  - Omissions: {omissions}")
                    if additions:
                        review_parts.append(f"  - Additions/Hallucinations: {additions}")
                    
                    review_parts.append(f"OVERALL: {overall_status}")
                    
                    final_verdict = "\n".join(review_parts)

                except json.JSONDecodeError:
                    # Fallback if JSON fails
                    print(f"JSON Parse Error. Raw: {raw_response}")
                    final_verdict = f"GUARDRAIL ERROR: Could not parse evaluation.\nRaw output: {raw_response}\nOVERALL: FAIL"

            # --- CONTENT ORDERING ---
            elif task == "content ordering":
                conf = resolve_model_config(
                    provider=cls.provider,
                    model_override=cls.model_name_override,
                )
                ordering_guard = UnifiedModel(cls.provider, **conf).model_(GUARDRAIL_PROMPT_CONTENT_ORDERING)
                _raw_msg = ordering_guard.invoke({"input": prompt})
                track_response(_raw_msg)
                result = _raw_msg.content.strip()
                final_verdict = result.split("FEEDBACK:")[-1].strip()

            # --- TEXT STRUCTURING ---
            elif task == "text structuring":
                conf = resolve_model_config(
                    provider=cls.provider,
                    model_override=cls.model_name_override,
                )
                structuring_guard = UnifiedModel(cls.provider, **conf).model_(GUARDRAIL_PROMPT_TEXT_STRUCTURING)
                _raw_msg = structuring_guard.invoke({"input": prompt})
                track_response(_raw_msg)
                result = _raw_msg.content.strip()
                final_verdict = result.split("FEEDBACK:")[-1].strip()

            # --- DEFAULT ---
            else:
                _raw_msg = agent.invoke({"input": prompt})
                track_response(_raw_msg)
                response = _raw_msg.content.strip()
                final_verdict = response.split("FEEDBACK:")[-1].strip()
            
            print(f"\n\nGUARDRAIL OUTPUT: {final_verdict}")

            history.append(
                AgentStepOutput(
                    agent_name="guardrail",
                    agent_input=prompt,
                    agent_output=final_verdict,
                    rationale="Evaluation of worker output."
                )
            )

            done = _is_correct_verdict(final_verdict)
            if done and task in expected_order and task not in passed_stages:
                passed_stages.append(task)
            if done and task in expected_order and task not in closed_stages:
                closed_stages.append(task)
            if (
                not done
                and task in expected_order
                and task not in closed_stages
                and int(worker_attempts.get(task, 0)) >= limit_for(task)
            ):
                closed_stages.append(task)

            route = next_route(done, passed_stages)
            print(f"Moving forward to {route}")
            return {
                "next_agent": route,
                "response": "done" if route == "finalizer" else None,
                "history_of_steps": history,
                "iteration_count": idx + 1,
                "max_iteration": max_iter,
                "next_agent_payload": user_input,
                "review": final_verdict,
                # Preserve worker tracking fields through the guardrail node
                "last_worker": state.get("last_worker", ""),
                "worker_attempts": worker_attempts,
                "passed_stages": passed_stages,
                "closed_stages": closed_stages,
            }

        return run

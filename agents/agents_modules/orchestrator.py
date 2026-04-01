__author__='chinonsocynthiaosuji'

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Orchestrate the task execution by directing agents to perform specific tasks
"""

import re
from typing import Dict, List, Text, Any, Union, Optional, Set
from langchain_classic.agents import AgentExecutor
from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import UnifiedModel, resolve_model_config
from agents.utilities.agent_utils import summarize_agent_steps
from agents.utilities.token_tracker import track_response
from agents.agent_prompts import (
    ORCHESTRATOR_PROMPT,
    ORCHESTRATOR_INPUT,
)

class TaskOrchestrator:
    @classmethod
    def init(
        cls,
        provider: str = "ollama",
        model_name_override: Optional[str] = None,
        reasoning_effort: Optional[str] = None,
    ) -> AgentExecutor:
        conf = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            temperature=0.0,
            reasoning_effort=reasoning_effort,
        )
        return UnifiedModel(provider=provider, **conf).model_(ORCHESTRATOR_PROMPT)

    @classmethod
    def execute(cls, executor: AgentExecutor):
        def run(state: ExecutionState):
            idx = state.get("iteration_count", 0)
            limit = state.get("max_iteration", 50)
            history = state.get("history_of_steps", []) 
            passed_stages = {
                str(name).strip().lower()
                for name in (state.get("passed_stages", []) or [])
                if str(name).strip()
            }
            closed_stages = {
                str(name).strip().lower()
                for name in (state.get("closed_stages", []) or [])
                if str(name).strip()
            }

            # New: worker attempts info
            worker_attempts: Dict[str, int] = state.get("worker_attempts", {}) or {}
            raw_limit = state.get("max_worker_attempts", 3)

            # Allow max_worker_attempts to be either an int or a dict of per worker limits
            if isinstance(raw_limit, dict):
                per_worker_limits: Dict[str, int] = raw_limit
                try:
                    default_limit = max(per_worker_limits.values())  # just for display
                except ValueError:
                    default_limit = 3
            else:
                per_worker_limits = {}
                try:
                    default_limit = int(raw_limit)
                except Exception:
                    default_limit = 3

            if worker_attempts:
                attempts_lines = []
                for name, count in worker_attempts.items():
                    limit_for_this = per_worker_limits.get(name, default_limit)
                    attempts_lines.append(
                        f"- {name}: {count} attempt(s) out of {limit_for_this} allowed"
                    )
                attempts_block = "WORKER ATTEMPTS:\n" + "\n".join(attempts_lines)
            else:
                attempts_block = "WORKER ATTEMPTS:\n- no worker has run yet"

            expected_order = ["content ordering", "text structuring", "surface realization"]
            expected_set: Set[str] = set(expected_order)
            passed_stages = {name for name in passed_stages if name in expected_set}
            closed_stages = {name for name in closed_stages if name in expected_set}.union(passed_stages)

            def limit_for(worker_name: str) -> int:
                if per_worker_limits:
                    try:
                        return int(per_worker_limits.get(worker_name, default_limit))
                    except Exception:
                        return default_limit
                return default_limit

            def first_open_stage() -> str:
                for stage_name in expected_order:
                    if stage_name not in closed_stages:
                        return stage_name
                return "finish"

            def required_role() -> str:
                return first_open_stage()

            prompt = state.get("user_prompt", "")
            summary = "\n\n".join(summarize_agent_steps(history)[-2:])  # last 2 steps
            feedback = str(state.get("review", "") or "")
            forced_role = required_role()

            payload = ORCHESTRATOR_INPUT.format(
                input=prompt,
                result_steps=f"\nRESULT STEPS:\n{summary}" if summary else "",
                feedback=f"\nFEEDBACK:\n{feedback}" if feedback else "",
                attempts=f"\n{attempts_block}",
                required_worker=forced_role,
            ).replace("\n\n\n", "\n")

            raw_response = executor.invoke({"input": payload})
            track_response(raw_response)
            output = raw_response.content.strip()
            
            try:
                output_lower = output.lower()
                if any(keyword in output_lower for keyword in ["instructions:", "instruction:"]):
                    rationale, role, role_input, instruction = re.findall(
                        r"Thought:\s*(.*?)\s*Worker:\s*(.*?)\s*Worker Input:\s*(.*?)\s*Instructions?:\s*(.*)",
                        output,
                        re.DOTALL,
                    )[0]
                else:
                    rationale, role, role_input = re.findall(
                        r"Thought:\s*(.*?)\s*Worker:\s*(.*?)\s*Worker Input:\s*(.*)",
                        output,
                        re.DOTALL,
                    )[0]
                    instruction = None
            except Exception:
                rationale, role, role_input, instruction = "parse error", "finish", output, None

            role = role.lower().strip("'\"").replace("_", " ")
            if forced_role == "finish":
                role = "finish"
            elif role != forced_role:
                rationale = (
                    f"{rationale}\nRouting override: forced next worker is "
                    f"'{forced_role}' to preserve strict stage order."
                )
                role = forced_role

            history.append(
                AgentStepOutput(
                    agent_name="orchestrator",
                    agent_input=payload,
                    agent_output=f"{role}(input='{role_input}', instruction='{instruction}')",
                    rationale=f"{rationale}\nInstruction:\n{instruction}",
                )
            )

            if idx >= limit:
                print("Moving forward to finish")
                return {
                    "next_agent": "finish",
                    "final_response": "Stopped due to limit reached.",
                    "next_agent_payload": "Limit reached.",
                    "history_of_steps": history,
                    "iteration_count": idx + 1,
                    "max_iteration": limit,
                    "response": "incomplete",
                    # Preserve worker tracking fields through the orchestrator node
                    "last_worker": state.get("last_worker", ""),
                    "worker_attempts": worker_attempts,
                    "passed_stages": sorted(passed_stages, key=lambda name: expected_order.index(name)),
                    "closed_stages": sorted(closed_stages, key=lambda name: expected_order.index(name)),
                }

            print(f"Moving forward to {role}")
            return {
                "next_agent": role,
                "final_response": role_input,
                "next_agent_payload": (
                    f"{role_input}\nAdditional Instruction: {instruction}"
                    if instruction
                    else role_input
                ),
                "history_of_steps": history,
                "iteration_count": idx + 1,
                "max_iteration": limit,
                # Preserve worker tracking fields through the orchestrator node
                "last_worker": state.get("last_worker", ""),
                "worker_attempts": worker_attempts,
                "passed_stages": sorted(passed_stages, key=lambda name: expected_order.index(name)),
                "closed_stages": sorted(closed_stages, key=lambda name: expected_order.index(name)),
            }

        return run

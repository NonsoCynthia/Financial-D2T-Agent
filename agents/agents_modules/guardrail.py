__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Provide guardrail evaluation for worker outputs.

Changes:
    - TS guardrail now receives the CO output explicitly so it can run
      the numeric figure coverage check (verify every number from CO
      appears in at least one <snt> block).
    - SR guardrail now receives both the raw source bundle and the TS
      scaffold, giving it two reference points: the authoritative numeric
      ground truth and what SR actually received as input. This allows
      the guardrail to distinguish scaffold-faithful elaboration from
      genuine hallucination.
    - Helper functions extracted to reduce duplication across the three
      guardrail branches.
"""

import re
import json
from langchain_classic.agents import AgentExecutor
from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import (
    UnifiedModel,
    extract_text_output,
    resolve_model_config,
    model_label_from_config,
)
from agents.agent_prompts import (
    GUARDRAIL_PROMPT,
    GUARDRAIL_INPUT,
    GUARDRAIL_PROMPT_CONTENT_ORDERING,
    GUARDRAIL_PROMPT_TEXT_STRUCTURING,
    GUARDRAIL_PROMPT_SURFACE_REALIZATION,
)
from agents.utilities.token_tracker import track_response


# ---------------------------------------------------------------------------
# Verdict parsing
# ---------------------------------------------------------------------------

def _is_correct_verdict(raw_text: str) -> bool:
    """Return True only when the guardrail verdict is unambiguously CORRECT."""
    text = str(raw_text or "").strip()
    if not text:
        return False
    overall_match = re.search(r"OVERALL:\s*(.+)$", text, re.MULTILINE | re.IGNORECASE)
    candidate = overall_match.group(1).strip() if overall_match else text
    candidate = candidate.splitlines()[0].strip()
    return bool(re.match(r"^CORRECT\b", candidate, re.IGNORECASE))


# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

def _latest_output_for(history: list, agent_name: str) -> str:
    """Return the most recent agent_output for a named agent in history."""
    target = agent_name.strip().lower()
    for step in reversed(history):
        if str(getattr(step, "agent_name", "") or "").strip().lower() == target:
            return str(getattr(step, "agent_output", "") or "").strip()
    return ""


def _make_model(provider: str, model_override: str | None, system_prompt: str):
    """Instantiate a guardrail chain with a given system prompt."""
    conf = resolve_model_config(
        provider=provider,
        model_override=model_override,
        temperature=0.0,
    )
    return UnifiedModel(provider, **conf).model_(system_prompt)


# ---------------------------------------------------------------------------
# SR verdict formatting
# ---------------------------------------------------------------------------

def _format_sr_verdict(eval_data: dict) -> str:
    """Build a human-readable guardrail review string from the SR JSON verdict."""
    overall_status = eval_data.get("overall_verdict", "FAIL").upper()
    ling_score = eval_data.get("linguistic_score", "FAIL")
    ling_feed = eval_data.get("linguistic_feedback", "")
    omissions = eval_data.get("omissions", [])
    additions = eval_data.get("additions", [])

    parts = [
        "=== GUARDRAIL REVIEW ===",
        f"[Linguistic Quality]: {ling_score} - {ling_feed}",
        f"[Factuality]: {eval_data.get('factuality_verdict', 'FAIL')}",
    ]
    if omissions:
        parts.append(f"  - Omissions: {omissions}")
    if additions:
        parts.append(f"  - Additions/Hallucinations: {additions}")
    parts.append(f"OVERALL: {overall_status}")
    return "\n".join(parts)


def _parse_sr_json(raw_response: str) -> str:
    """
    Extract and parse the JSON block from the SR guardrail response.
    Returns a formatted verdict string. Falls back gracefully on parse errors.
    """
    clean = raw_response.replace("FEEDBACK:", "").strip()
    if "```" in clean:
        clean = clean.split("```")[1].replace("json", "").strip()

    try:
        eval_data = json.loads(clean)
        return _format_sr_verdict(eval_data)
    except json.JSONDecodeError:
        print(f"[Guardrail] JSON parse error. Raw SR response:\n{raw_response}")
        return (
            f"GUARDRAIL ERROR: Could not parse SR evaluation.\n"
            f"Raw output: {raw_response}\nOVERALL: FAIL"
        )


# ---------------------------------------------------------------------------
# Main guardrail class
# ---------------------------------------------------------------------------

class TaskGuardrail:
    provider = "openai"
    model_name_override = None
    model_label = ""

    @classmethod
    def init(
        cls,
        provider: str = "ollama",
        model_name_override: str | None = None,
        reasoning_effort: str | None = None,
    ) -> AgentExecutor:
        """Initialise the default guardrail chain (used for the fallback branch)."""
        cls.provider = provider
        cls.model_name_override = model_name_override
        conf = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            temperature=0.0,
            reasoning_effort=reasoning_effort,
        )
        cls.model_label = model_label_from_config(conf)
        chain = UnifiedModel(provider=provider, **conf).model_(GUARDRAIL_PROMPT)
        try:
            setattr(chain, "_token_model_name", cls.model_label)
        except Exception:
            pass
        return chain

    @classmethod
    def evaluate(cls, agent: AgentExecutor):
        """Return a LangGraph node function that evaluates the latest worker output."""

        def run(state: ExecutionState):
            model_name = str(
                getattr(agent, "_token_model_name", cls.model_label) or ""
            ).strip()
            history = state.get("history_of_steps", [])
            idx = state.get("iteration_count", 0)
            max_iter = state.get("max_iteration", 50)
            user_input = state.get("user_prompt", "")
            data_input = state.get("data_input", "")

            expected_order = [
                "content ordering",
                "text structuring",
                "surface realization",
            ]
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

            # ------------------------------------------------------------------
            # Helper: resolve per-worker attempt limit
            # ------------------------------------------------------------------
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

            # ------------------------------------------------------------------
            # Helper: decide the next graph route
            # ------------------------------------------------------------------
            def next_route(done: bool, passed: list[str]) -> str:
                passed_set = {
                    str(name).strip().lower()
                    for name in passed
                    if str(name).strip()
                }
                if done and set(expected_order).issubset(passed_set):
                    return "finalizer"
                return "orchestrator"

            # ------------------------------------------------------------------
            # Helper: build a consistent return dict
            # ------------------------------------------------------------------
            def make_return(route: str, verdict: str, new_idx: int) -> dict:
                return {
                    "next_agent": route,
                    "response": "done" if route == "finalizer" else None,
                    "history_of_steps": history,
                    "iteration_count": new_idx,
                    "max_iteration": max_iter,
                    "next_agent_payload": user_input,
                    "review": verdict,
                    "last_worker": state.get("last_worker", ""),
                    "worker_attempts": worker_attempts,
                    "passed_stages": passed_stages,
                    "closed_stages": closed_stages,
                }

            # ------------------------------------------------------------------
            # Helper: update passed/closed stage bookkeeping
            # ------------------------------------------------------------------
            def update_stage_tracking(done: bool, task: str) -> None:
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

            # ------------------------------------------------------------------
            # Identify the latest orchestrator, worker, and guardrail steps.
            # We track indices so we can detect whether the guardrail is being
            # asked to re-evaluate the exact same worker output (stale check).
            # ------------------------------------------------------------------
            orch = next(
                (s for s in reversed(history) if s.agent_name == "orchestrator"),
                None,
            )
            worker = None
            worker_idx = -1
            guardrail_idx = -1
            last_guardrail_output = ""

            for idx_step in range(len(history) - 1, -1, -1):
                step = history[idx_step]
                step_name = str(getattr(step, "agent_name", "") or "").strip().lower()
                if guardrail_idx < 0 and step_name == "guardrail":
                    guardrail_idx = idx_step
                    last_guardrail_output = str(
                        getattr(step, "agent_output", "") or ""
                    ).strip()
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

            base_context = (
                f"Orchestrator Thought: {rationale}\n"
                f"Worker Input: {task_input}\n"
                f"Worker Output: {output}"
            )
            prompt = GUARDRAIL_INPUT.format(input=base_context)
            final_verdict = ""

            # ------------------------------------------------------------------
            # Stale-output short-circuit: if no new worker output has appeared
            # since the last guardrail step, reuse the cached verdict to avoid
            # paying for a redundant LLM call.
            # ------------------------------------------------------------------
            if worker_idx >= 0 and guardrail_idx > worker_idx and last_guardrail_output:
                final_verdict = last_guardrail_output
                done = _is_correct_verdict(final_verdict)
                update_stage_tracking(done, task)
                route = next_route(done, passed_stages)
                print(f"[Guardrail] Stale output detected — reusing cached verdict.")
                print(f"Moving forward to {route}")
                return make_return(route, final_verdict, idx)

            # ------------------------------------------------------------------
            # SURFACE REALIZATION guardrail
            # The guardrail receives three inputs:
            #   1. The raw source bundle — authoritative numeric ground truth.
            #   2. The TS scaffold — what SR actually received as its input.
            #   3. The generated report — what SR produced.
            # Having both (1) and (2) lets the guardrail distinguish between
            # figures SR invented from memory and figures that were legitimately
            # present in the scaffold.
            # ------------------------------------------------------------------
            if task == "surface realization":
                source_bundle = data_input
                if isinstance(source_bundle, list):
                    source_bundle = "\n".join(str(t) for t in source_bundle)

                # Pull the most recent TS output so the guardrail sees the
                # exact scaffold SR was given, not just the raw bundle.
                ts_scaffold = _latest_output_for(history, "text structuring")

                unified_context = (
                    f"AUTHORITATIVE SOURCE BUNDLE (numeric ground truth — "
                    f"every figure in the report must trace back to this):\n"
                    f"{source_bundle}\n\n"
                    f"TEXT STRUCTURING SCAFFOLD (what surface realization received "
                    f"as input — use this to verify scaffold fidelity):\n"
                    f"{ts_scaffold}\n\n"
                    f"GENERATED REPORT:\n"
                    f"{output}"
                )
                unified_prompt = GUARDRAIL_INPUT.format(input=unified_context)

                sr_guard = _make_model(
                    cls.provider,
                    cls.model_name_override,
                    GUARDRAIL_PROMPT_SURFACE_REALIZATION,
                )
                _raw_msg = sr_guard.invoke({"input": unified_prompt})
                track_response(_raw_msg, agent_name="guardrail", model_name=model_name)
                raw_response = extract_text_output(_raw_msg)
                final_verdict = _parse_sr_json(raw_response)

            # ------------------------------------------------------------------
            # CONTENT ORDERING guardrail
            # The guardrail receives the base context (orchestrator thought +
            # worker input + worker output) built from history above.
            # ------------------------------------------------------------------
            elif task == "content ordering":
                co_guard = _make_model(
                    cls.provider,
                    cls.model_name_override,
                    GUARDRAIL_PROMPT_CONTENT_ORDERING,
                )
                _raw_msg = co_guard.invoke({"input": prompt})
                track_response(_raw_msg, agent_name="guardrail", model_name=model_name)
                result = extract_text_output(_raw_msg)
                final_verdict = result.split("FEEDBACK:")[-1].strip()

            # ------------------------------------------------------------------
            # TEXT STRUCTURING guardrail
            # The guardrail receives two explicit inputs:
            #   1. The CO output — numeric ground truth for the coverage check
            #      (every number in CO must appear in at least one <snt> block).
            #   2. The TS output — the scaffold to evaluate.
            # Without the CO output the guardrail can only check for brackets,
            # not whether figures were silently dropped during structuring.
            # ------------------------------------------------------------------
            elif task == "text structuring":
                co_output = _latest_output_for(history, "content ordering")

                ts_context = (
                    f"CONTENT ORDERING OUTPUT (numeric ground truth — use this to "
                    f"verify that every figure appears inside at least one <snt> "
                    f"block in the structuring output below):\n"
                    f"{co_output}\n\n"
                    f"TEXT STRUCTURING OUTPUT (scaffold to evaluate):\n"
                    f"{output}"
                )
                ts_prompt = GUARDRAIL_INPUT.format(input=ts_context)

                ts_guard = _make_model(
                    cls.provider,
                    cls.model_name_override,
                    GUARDRAIL_PROMPT_TEXT_STRUCTURING,
                )
                _raw_msg = ts_guard.invoke({"input": ts_prompt})
                track_response(_raw_msg, agent_name="guardrail", model_name=model_name)
                result = extract_text_output(_raw_msg)
                final_verdict = result.split("FEEDBACK:")[-1].strip()

            # ------------------------------------------------------------------
            # DEFAULT fallback — used when task cannot be identified
            # ------------------------------------------------------------------
            else:
                _raw_msg = agent.invoke({"input": prompt})
                track_response(_raw_msg, agent_name="guardrail", model_name=model_name)
                response = extract_text_output(_raw_msg)
                final_verdict = response.split("FEEDBACK:")[-1].strip()

            print(f"\n[Guardrail] Task: {task}")
            print(f"[Guardrail] Verdict:\n{final_verdict}")

            history.append(
                AgentStepOutput(
                    agent_name="guardrail",
                    agent_input=prompt,
                    agent_output=final_verdict,
                    rationale="Evaluation of worker output.",
                )
            )

            done = _is_correct_verdict(final_verdict)
            update_stage_tracking(done, task)
            route = next_route(done, passed_stages)
            print(f"Moving forward to {route}")
            return make_return(route, final_verdict, idx + 1)

        return run
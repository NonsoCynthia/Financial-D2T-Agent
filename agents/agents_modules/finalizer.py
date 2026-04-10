__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Refine, polish and return the final output
"""

from langchain_classic.agents import AgentExecutor
from agents.utilities.utils import ExecutionState, AgentStepOutput
from agents.llm_model import UnifiedModel, extract_text_output, resolve_model_config, model_label_from_config
from agents.agent_prompts import FINALIZER_PROMPT, FINALIZER_INPUT
from agents.utilities.token_tracker import track_response


class TaskFinalizer:
    @classmethod
    def init(
        cls,
        provider: str = "ollama",
        model_name_override: str | None = None,
        reasoning_effort: str | None = None,
    ) -> AgentExecutor:
        """
        Initialise the finalizer with a conservative configuration
        so it only performs light post processing.
        """
        cfg = resolve_model_config(
            provider=provider,
            model_override=model_name_override,
            temperature=0.0,
            reasoning_effort=reasoning_effort,
        )
        chain = UnifiedModel(provider=provider, **cfg).model_(FINALIZER_PROMPT)
        try:
            setattr(chain, "_token_model_name", model_label_from_config(cfg))
        except Exception:
            pass
        return chain

    @classmethod
    def compile(cls, executor: AgentExecutor):
        model_name = str(getattr(executor, "_token_model_name", "") or "").strip()

        def run(state: ExecutionState):
            history = state.get("history_of_steps", []) or []
            sample_meta = state.get("sample_metadata", {}) or {}

            def latest_output_for(agent_name: str):
                target = agent_name.strip().lower()
                for step in reversed(history):
                    if getattr(step, "agent_name", "").strip().lower() == target:
                        return getattr(step, "agent_output", "")
                return ""

            def build_authoritative_bundle_summary() -> str:
                lines = []
                tickers = sample_meta.get("tickers") or []
                if isinstance(tickers, list) and tickers:
                    lines.append(f"Coverage universe: {', '.join(str(t) for t in tickers)}")

                stocks = sample_meta.get("stocks") or []
                if stocks:
                    lines.append("Per-ticker ground truth:")
                    lines.append(
                        "Recommendation and TargetPrice below are canonical. "
                        "Do not replace them with alternative values mentioned inside justifications."
                    )
                    for row in stocks:
                        bits = [
                            f"recommendation={row.get('recommendation', 'N/A')}",
                            f"target_price={row.get('target_price', 'N/A')}",
                        ]
                        if row.get("current_price") and row["current_price"] != "N/A":
                            bits.append(f"current_price={row['current_price']}")
                        if row.get("implied_move_pct") and row["implied_move_pct"] != "N/A":
                            bits.append(f"implied_move_pct={row['implied_move_pct']}")
                        lines.append(f"- {row.get('ticker', 'UNKNOWN')}: {', '.join(bits)}")

                return "\n".join(lines) if lines else "N/A"

            # Last surface realization text
            sr_output = latest_output_for("surface realization")
            # Last guardrail review, if any
            gd_output = latest_output_for("guardrail")

            if not sr_output:
                sr_output = "NO SURFACE REALIZATION OUTPUT AVAILABLE."

            final_input = FINALIZER_INPUT.format(
                analysis_date=str(state.get("analysis_date", "") or sample_meta.get("analysis_date", "") or ""),
                surface_realization_output=sr_output,
                guardrail_feedback=gd_output or "None",
                horizon_months=str(state.get("horizon_months", "") or ""),
                end_date=str(state.get("end_date", "") or ""),
                authoritative_bundle_summary=build_authoritative_bundle_summary(),
            )

            if state.get("response") == "incomplete":
                reply = (
                    "Final Answer: The system reached the maximum number of iterations "
                    "and no stable final output could be produced."
                )
            else:
                raw = executor.invoke({"input": final_input})
                track_response(raw, agent_name="finalizer", model_name=model_name)
                reply = extract_text_output(raw)

                # Normalise in case the model includes a prefix
                prefix = "final answer:"
                if reply.lower().startswith(prefix):
                    reply = reply[len(prefix):].lstrip()

            history.append(
                AgentStepOutput(
                    agent_name="finalizer",
                    agent_input=final_input,
                    agent_output=reply,
                )
            )

            return {
                "final_response": reply,
                "history_of_steps": history,
            }

        return run

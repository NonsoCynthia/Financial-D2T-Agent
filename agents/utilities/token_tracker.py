"""
Lightweight token-usage tracker for monitoring API costs.

Usage
-----
    from agents.utilities.token_tracker import token_tracker, track_response

    # After a chain invoke (returns AIMessage):
    response = chain.invoke({"input": payload})
    track_response(response)

    # For AgentExecutor, pass the callback so internal LLM calls are tracked:
    from agents.utilities.token_tracker import token_tracking_callback
    out = agent.invoke({"input": inputs}, config={"callbacks": [token_tracking_callback()]})

    # At the end of a run:
    print(token_tracker.summary_str())
    token_tracker.reset()
"""

import threading
from typing import Any, Dict, Optional

from langchain_core.callbacks import BaseCallbackHandler


class TokenTracker:
    """Thread-safe singleton that accumulates token counts."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                obj.input_tokens = 0
                obj.output_tokens = 0
                obj.by_agent = {}
                cls._instance = obj
            return cls._instance

    def add(
        self,
        input_tokens: int = 0,
        output_tokens: int = 0,
        agent_name: Optional[str] = None,
        model_name: Optional[str] = None,
    ):
        with self._lock:
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens
            normalized_agent = str(agent_name or "").strip().lower()
            if normalized_agent:
                entry = self.by_agent.setdefault(
                    normalized_agent,
                    {
                        "agent_name": normalized_agent,
                        "model_name": str(model_name or "").strip(),
                        "input_tokens": 0,
                        "output_tokens": 0,
                    },
                )
                if model_name and not entry.get("model_name"):
                    entry["model_name"] = str(model_name).strip()
                entry["input_tokens"] += input_tokens
                entry["output_tokens"] += output_tokens

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def reset(self):
        with self._lock:
            self.input_tokens = 0
            self.output_tokens = 0
            self.by_agent = {}

    def as_dict(self, include_breakdown: bool = False) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }
        if include_breakdown:
            payload["by_agent"] = {
                name: {
                    **entry,
                    "total_tokens": int(entry.get("input_tokens", 0)) + int(entry.get("output_tokens", 0)),
                }
                for name, entry in self.by_agent.items()
            }
        return payload

    def summary_str(self) -> str:
        lines = [(
            f"[Token Usage] "
            f"Input: {self.input_tokens:,} | "
            f"Output: {self.output_tokens:,} | "
            f"Total: {self.total_tokens:,}"
        )]
        for entry in self.by_agent.values():
            agent_name = str(entry.get("agent_name", "") or "").strip() or "unknown"
            model_name = str(entry.get("model_name", "") or "").strip()
            label = f"{agent_name} [{model_name}]" if model_name else agent_name
            input_tokens = int(entry.get("input_tokens", 0) or 0)
            output_tokens = int(entry.get("output_tokens", 0) or 0)
            total_tokens = input_tokens + output_tokens
            lines.append(
                f"  - {label}: "
                f"Input: {input_tokens:,} | "
                f"Output: {output_tokens:,} | "
                f"Total: {total_tokens:,}"
            )
        return "\n".join(lines)


# Module-level singleton
token_tracker = TokenTracker()


def track_response(
    response: Any,
    agent_name: Optional[str] = None,
    model_name: Optional[str] = None,
) -> None:
    """Extract token usage from an AIMessage (chain invoke result) and accumulate."""
    usage = getattr(response, "usage_metadata", None)
    if usage and isinstance(usage, dict):
        token_tracker.add(
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            agent_name=agent_name,
            model_name=model_name,
        )


class _TokenTrackingCallback(BaseCallbackHandler):
    """LangChain callback that captures token usage from every LLM call."""

    def __init__(
        self,
        agent_name: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.agent_name = agent_name
        self.model_name = model_name

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        # LLMResult.llm_output typically has {"token_usage": {...}} for OpenAI
        llm_output = getattr(response, "llm_output", None) or {}
        usage = llm_output.get("token_usage") or {}
        if usage:
            token_tracker.add(
                input_tokens=usage.get("prompt_tokens", 0),
                output_tokens=usage.get("completion_tokens", 0),
                agent_name=self.agent_name,
                model_name=self.model_name,
            )
            return

        # Fallback: check individual generations for usage_metadata
        for gen_list in getattr(response, "generations", []):
            for gen in gen_list:
                msg = getattr(gen, "message", None)
                if msg:
                    track_response(msg, agent_name=self.agent_name, model_name=self.model_name)


def token_tracking_callback(
    agent_name: Optional[str] = None,
    model_name: Optional[str] = None,
) -> _TokenTrackingCallback:
    """Return a fresh callback instance for use in invoke configs."""
    return _TokenTrackingCallback(agent_name=agent_name, model_name=model_name)

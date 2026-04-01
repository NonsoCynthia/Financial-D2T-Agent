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
from typing import Any, Dict

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
                cls._instance = obj
            return cls._instance

    def add(self, input_tokens: int = 0, output_tokens: int = 0):
        with self._lock:
            self.input_tokens += input_tokens
            self.output_tokens += output_tokens

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def reset(self):
        with self._lock:
            self.input_tokens = 0
            self.output_tokens = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
        }

    def summary_str(self) -> str:
        return (
            f"[Token Usage] "
            f"Input: {self.input_tokens:,} | "
            f"Output: {self.output_tokens:,} | "
            f"Total: {self.total_tokens:,}"
        )


# Module-level singleton
token_tracker = TokenTracker()


def track_response(response: Any) -> None:
    """Extract token usage from an AIMessage (chain invoke result) and accumulate."""
    usage = getattr(response, "usage_metadata", None)
    if usage and isinstance(usage, dict):
        token_tracker.add(
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
        )


class _TokenTrackingCallback(BaseCallbackHandler):
    """LangChain callback that captures token usage from every LLM call."""

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        # LLMResult.llm_output typically has {"token_usage": {...}} for OpenAI
        llm_output = getattr(response, "llm_output", None) or {}
        usage = llm_output.get("token_usage") or {}
        if usage:
            token_tracker.add(
                input_tokens=usage.get("prompt_tokens", 0),
                output_tokens=usage.get("completion_tokens", 0),
            )
            return

        # Fallback: check individual generations for usage_metadata
        for gen_list in getattr(response, "generations", []):
            for gen in gen_list:
                msg = getattr(gen, "message", None)
                if msg:
                    track_response(msg)


def token_tracking_callback() -> _TokenTrackingCallback:
    """Return a fresh callback instance for use in invoke configs."""
    return _TokenTrackingCallback()

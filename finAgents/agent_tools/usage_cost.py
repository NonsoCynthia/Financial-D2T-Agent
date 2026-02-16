from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Usage:
    """Container for token usage."""
    input_tokens: int
    output_tokens: int


def extract_usage_from_runner_result(result) -> Optional[Usage]:
    """Extract token usage from an Agents SDK Runner result if present."""
    usage = getattr(result, "usage", None)
    if usage is None:
        return None

    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
    return Usage(input_tokens=input_tokens, output_tokens=output_tokens)


def estimate_cost_usd(model: str, usage: Usage) -> float:
    """Estimate USD cost from tokens using a configurable price map.

    Prices are per 1K tokens. Update these as OpenAI pricing changes.
    If a model is missing, returns 0.0.
    """
    # Pricing per 1K tokens (input, output) in USD
    # Based on OpenAI pricing as of 2025
    price_map = {
        "gpt-5-mini": (0.0004, 0.0016),      # $0.40 / $1.60 per 1M tokens
        "gpt-5-nano": (0.0001, 0.0004),      # $0.10 / $0.40 per 1M tokens
        "gpt-4.1-mini": (0.0004, 0.0016),    # $0.40 / $1.60 per 1M tokens
        "gpt-4.1-nano": (0.0001, 0.0004),    # $0.10 / $0.40 per 1M tokens
        "gpt-4.1": (2.00, 8.00),             # $2.00 / $8.00 per 1M tokens
        "gpt-4o": (2.50, 10.00),             # $2.50 / $10.00 per 1M tokens
        "gpt-4o-mini": (0.15, 0.60),         # $0.15 / $0.60 per 1M tokens
        "o3-mini": (1.10, 4.40),             # $1.10 / $4.40 per 1M tokens
        "o1-mini": (1.10, 4.40),             # $1.10 / $4.40 per 1M tokens
    }

    # Normalize model name
    model_key = model.strip().lower() if model else ""
    
    # Get pricing, default to 0.0 if not found
    pricing = price_map.get(model_key, (0.0, 0.0))
    pin, pout = pricing

    input_cost = (usage.input_tokens / 1000.0) * pin
    output_cost = (usage.output_tokens / 1000.0) * pout

    return input_cost + output_cost


def format_cost_report(model: str, usage: Usage) -> str:
    """Format a human-readable cost report."""
    cost = estimate_cost_usd(model, usage)
    return (
        f"Model: {model}\n"
        f"Input tokens: {usage.input_tokens:,}\n"
        f"Output tokens: {usage.output_tokens:,}\n"
        f"Estimated cost: ${cost:.6f} USD"
    )

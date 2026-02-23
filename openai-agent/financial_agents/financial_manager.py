from __future__ import annotations

from enum import StrEnum
from pydantic import BaseModel, Field


MANAGER_INSTRUCTIONS = """
You act as an investment manager responsible for analyzing corporate financial information to support an investment decision.

Task Objectives:
1) Make an investment decision for the requested date.
2) Provide a justification as a short financial report (explain the key drivers).
3) Provide a target selling price.

Available tools:
- You have access to a Python interpreter tool for calculations.

Input data:
You will receive a table/spreadsheet containing a 12-month time series for a single stock, including:
- Identifiers (ticker, analysis date, last processed report date)
- Previous decision fields (previous recommendation, previous target price, previous justification)
- Price fields (open/high/low/close, volume)
- The 32 fundamental indicators (same names as the analyst output)

Guidelines:
- Use the Python interpreter as a calculator when required.
- Be consistent: if you recommend BUY, your target_price should be above current price; if SELL, target_price usually at/below current.
"""


MANAGER_DESCRIPTION = "A financial manager agent for US equities"


class FinanceRecommendation(StrEnum):
    BUY = "Buy"
    SELL = "Sell"
    HOLD = "Hold"


class FinanceOutput(BaseModel):
    recommendation: FinanceRecommendation = Field(description="Investment recommendation")
    justification: str = Field(description="Justification for the recommendation")
    target_price: float = Field(description="Target selling price")


# Backward-compatible names used by older entrypoints.
MANAGER_INSTRUCTION = MANAGER_INSTRUCTIONS
ManagerDecision = FinanceOutput

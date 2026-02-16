from enum import Enum

from pydantic import BaseModel, Field


class Recommendation(str, Enum):
    """Thiago style manager recommendations."""

    buy = "BUY"
    keep = "KEEP"
    sell = "SELL"


class ManagerDecision(BaseModel):
    """Structured output for the Financial Manager."""

    ticker: str = Field(...)
    date: str = Field(...)
    recommendation: Recommendation = Field(...)
    target_price: float | None = Field(default=None)
    justification: str = Field(...)

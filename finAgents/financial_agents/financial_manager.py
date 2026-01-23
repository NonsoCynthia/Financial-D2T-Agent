from enum import Enum
from pydantic import BaseModel, Field

class Recommendation(str, Enum):
    """
    Final decision.
    """
    buy = "BUY"
    sell = "SELL"


class FinanceOutput(BaseModel):
    """
    The manager output. Interpretation, decision, justification.
    """
    interpretation: str = Field(...)
    recommendation: Recommendation = Field(...)
    justification: str = Field(...)


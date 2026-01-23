from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class Indicator(str, Enum):
    last_price = "last_price"
    total_return = "total_return"
    volatility = "volatility"
    revenues = "Revenues"
    net_income = "NetIncomeLoss"
    assets = "Assets"
    liabilities = "Liabilities"
    roe = "roe"
    profit_margin = "profit_margin"


class IndicatorsReport(BaseModel):
    """
    Optional structured type if you want it later.
    """
    ticker: str = Field(...)
    as_of_date: str = Field(...)
    indicators: Dict[str, float | None] = Field(default_factory=dict)


def find_missing_indicators_from_json(payload: Dict[str, Any], expected: List[str]) -> List[str]:
    """
    Identify which expected indicators are missing or null in the analyst JSON.

    payload: parsed JSON dict returned by the analyst
    expected: list of indicator names expected to exist in payload["indicators"]
    returns: list of indicator names that are missing or have null value
    """
    indicators = payload.get("indicators", {})

    missing: List[str] = []
    for name in expected:
        if name not in indicators:
            missing.append(name)
            continue
        if indicators[name] is None:
            missing.append(name)

    return missing

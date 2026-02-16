from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class FundamentalIndicator(str, Enum):
    """Canonical names for the 32 indicators in Thiago's fundamental analysis setup."""

    # Balance sheet
    assets = "Assets"
    current_assets = "CurrentAssets"
    cash_and_equivalents = "CashAndEquivalents"
    gross_debt = "GrossDebt"
    net_debt = "NetDebt"
    shareholders_equity = "ShareholdersEquity"

    # Income statement (quarter and trailing twelve months)
    net_revenue_ttm = "NetRevenue_TTM"
    net_revenue_q = "NetRevenue_Q"
    ebit_ttm = "EBIT_TTM"
    ebit_q = "EBIT_Q"
    net_profit_ttm = "NetProfit_TTM"
    net_profit_q = "NetProfit_Q"

    # Valuation
    pe = "P_E"
    pb = "P_B"
    pebit = "P_EBIT"
    price_to_sales = "PriceToSales"
    price_to_assets = "PriceToAssets"
    price_to_working_capital = "PriceToWorkingCapital"
    price_to_net_current_assets = "PriceToNetCurrentAssets"
    ev_ebit = "EV_EBIT"
    ev_ebitda = "EV_EBITDA"

    # Per share
    eps = "EPS"
    bvps = "BVPS"

    # Profitability and margins
    gross_margin = "GrossMargin"
    ebit_margin = "EBITMargin"
    net_margin = "NetMargin"
    ebit_to_assets = "EBIT_Assets"
    roe = "ROE"
    roic = "ROIC"

    # Leverage and efficiency
    current_ratio = "CurrentRatio"
    gross_debt_to_equity = "GrossDebt_Equity"
    asset_turnover = "AssetTurnover"


class FundamentalReport(BaseModel):
    """Structured output for the Fundamental Analyst."""

    ticker: str = Field(...)
    as_of_date: str = Field(...)
    window_months: int = Field(12)
    indicators: Dict[str, Optional[float]] = Field(default_factory=dict)
    notes: str = Field(default="")
    sources: List[str] = Field(default_factory=list)


def expected_indicator_keys() -> List[str]:
    """Return the expected indicator keys in the output JSON."""

    return [i.value for i in FundamentalIndicator]


def find_missing_indicators_from_json(payload: Dict[str, Any], expected: List[str]) -> List[str]:
    """Identify which expected indicators are missing or null in the analyst JSON."""

    indicators = payload.get("indicators", {})
    if not isinstance(indicators, dict):
        indicators = {}

    missing: List[str] = []
    for name in expected:
        if name not in indicators:
            missing.append(name)
            continue
        if indicators[name] is None:
            missing.append(name)

    return missing

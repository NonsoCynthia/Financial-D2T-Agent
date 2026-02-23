from __future__ import annotations

from enum import StrEnum
from pydantic import BaseModel, Field
from typing import Optional


class USIndicator(StrEnum):
    """Canonical names for the 32 indicators fundamental analysis setup."""

    # Balance sheet
    ASSETS = "Assets"
    CURRENT_ASSETS = "CurrentAssets"
    CASH_AND_EQUIVALENTS = "CashAndEquivalents"
    GROSS_DEBT = "GrossDebt"
    NET_DEBT = "NetDebt"
    SHAREHOLDERS_EQUITY = "ShareholdersEquity"

    # Income statement (TTM and quarter)
    NET_REVENUE_TTM = "NetRevenue_TTM"
    NET_REVENUE_Q = "NetRevenue_Q"
    EBIT_TTM = "EBIT_TTM"
    EBIT_Q = "EBIT_Q"
    NET_PROFIT_TTM = "NetProfit_TTM"
    NET_PROFIT_Q = "NetProfit_Q"

    # Valuation ratios
    P_E = "P_E"
    P_B = "P_B"
    P_EBIT = "P_EBIT"
    PRICE_TO_SALES = "PriceToSales"
    PRICE_TO_ASSETS = "PriceToAssets"
    PRICE_TO_WORKING_CAPITAL = "PriceToWorkingCapital"
    PRICE_TO_NET_CURRENT_ASSETS = "PriceToNetCurrentAssets"
    EV_EBIT = "EV_EBIT"
    EV_EBITDA = "EV_EBITDA"

    # Per share
    EPS = "EPS"
    BVPS = "BVPS"

    # Profitability and margins
    GROSS_MARGIN = "GrossMargin"
    EBIT_MARGIN = "EBITMargin"
    NET_MARGIN = "NetMargin"
    EBIT_TO_ASSETS = "EBIT_Assets"
    ROE = "ROE"
    ROIC = "ROIC"

    # Liquidity and leverage
    CURRENT_RATIO = "CurrentRatio"
    GROSS_DEBT_TO_EQUITY = "GrossDebt_Equity"

    # Efficiency
    ASSET_TURNOVER = "AssetTurnover"

    def __str__(self) -> str:
        """Return the string value for clean printing."""
        return self.value


class USIndicatorItem(BaseModel):
    """One indicator value in the agent output."""

    indicator: USIndicator = Field(description="Indicator name")
    value: Optional[float] = Field(description="Numeric value, or null if unavailable")


class USIndicatorOutput(BaseModel):
    """Full structured output from the US fundamental analyst agent."""

    indicators: list[USIndicatorItem] = Field(description="List of 32 indicators")


# Backward-compatible aliases expected by existing experiment/workflow imports.
Indicator = USIndicator
IndicatorItem = USIndicatorItem
IndicatorOutput = USIndicatorOutput

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class FundamentalIndicator(str, Enum):
    """Canonical names for the 32 indicators fundamental analysis setup."""

    # Balance sheet style items
    assets = "Assets" # Total assets
    current_assets = "CurrentAssets" # assets expected to turn into cash within a year
    cash_and_equivalents = "CashAndEquivalents" # cash in hand, bank balances, very liquid short-term investments
    gross_debt = "GrossDebt" #total interest-bearing debt, short-term plus long-term, sometimes including debentures
    net_debt = "NetDebt" #gross debt minus cash and cash equivalents
    shareholders_equity = "ShareholdersEquity" #net assets attributable to owners

    # Income statement items (quarter and trailing twelve months(TTM))
    net_revenue_ttm = "NetRevenue_TTM" #Net revenue over the last 12 months
    net_revenue_q = "NetRevenue_Q" #Net revenue for the last quarter (3 months)
    ebit_ttm = "EBIT_TTM" #Earnings before interest and taxes over the last 12 months
    ebit_q = "EBIT_Q" #Earnings before interest and taxes over the last 12 months
    net_profit_ttm = "NetProfit_TTM" #Net profit over the last 12 months.
    net_profit_q = "NetProfit_Q" #Net profit over the last quarter

    # Valuation Ratios
    pe = "P_E" #Price to earnings ratio (share price divided by earnings per share)
    pb = "P_B" #Price to book ratio (share price divided by book value per share).
    pebit = "P_EBIT" #Price to EBIT (price per share divided by EBIT per share, or equivalently market cap divided by EBIT, depending on convention).
    price_to_sales = "PriceToSales" #Price to sales ratio (price per share divided by sales per share, or market cap divided by revenue).
    price_to_assets = "PriceToAssets" #Price to assets (price per share divided by assets per share).
    price_to_working_capital = "PriceToWorkingCapital" #Price to working capital per share. Working capital is current assets minus current liabilities.
    price_to_net_current_assets = "PriceToNetCurrentAssets" #Price to net current assets per share. Often refers to current assets minus total liabilities, but exact definition can vary by dataset.
    ev_ebit = "EV_EBIT" #Enterprise value divided by EBIT.
    ev_ebitda = "EV_EBITDA" #Enterprise value divided by EBITDA.

    # Per share metrics
    eps = "EPS" #Earnings per share (EPS).
    bvps = "BVPS" #Book value per share (equity divided by number of shares).

    # Profitability and margins
    gross_margin = "GrossMargin" #Gross margin (gross profit divided by net revenue).
    ebit_margin = "EBITMargin" #EBIT margin (EBIT divided by net revenue).
    net_margin = "NetMargin" #Net margin (net profit divided by net revenue).
    ebit_to_assets = "EBIT_Assets" #EBIT divided by total assets (a profitability over assets measure).
    roe = "ROE" #Return on invested capital (a profitability measure on invested capital, definition can vary).
    roic = "ROIC" #Return on equity (net profit divided by shareholders’ equity).

    # Liquidity and Leverage
    current_ratio = "CurrentRatio" #Current ratio (current assets divided by current liabilities).
    gross_debt_to_equity = "GrossDebt_Equity" #Gross debt divided by equity (a leverage ratio).
    
    # Efficiency
    asset_turnover = "AssetTurnover" #Asset turnover (net revenue divided by total assets).


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


# """Canonical names for the 32 indicators fundamental analysis setup."""

# # Balance sheet style items
# assets = "Assets" # Total assets
# current_assets = "CurrentAssets" # assets expected to turn into cash within a year
# cash_and_equivalents = "CashAndEquivalents" # cash in hand, bank balances, very liquid short-term investments
# gross_debt = "GrossDebt" #total interest-bearing debt, short-term plus long-term, sometimes including debentures
# net_debt = "NetDebt" #gross debt minus cash and cash equivalents
# shareholders_equity = "ShareholdersEquity" #net assets attributable to owners

# # Income statement items (quarter and trailing twelve months(TTM))
# net_revenue_ttm = "NetRevenue_TTM" #Net revenue over the last 12 months
# net_revenue_q = "NetRevenue_Q" #Net revenue for the last quarter (3 months)
# ebit_ttm = "EBIT_TTM" #Earnings before interest and taxes over the last 12 months
# ebit_q = "EBIT_Q" #Earnings before interest and taxes over the last 12 months
# net_profit_ttm = "NetProfit_TTM" #Net profit over the last 12 months.
# net_profit_q = "NetProfit_Q" #Net profit over the last quarter

# # Valuation Ratios
# pe = "P_E" #Price to earnings ratio (share price divided by earnings per share)
# pb = "P_B" #Price to book ratio (share price divided by book value per share).
# pebit = "P_EBIT" #Price to EBIT (price per share divided by EBIT per share, or equivalently market cap divided by EBIT, depending on convention).
# price_to_sales = "PriceToSales" #Price to sales ratio (price per share divided by sales per share, or market cap divided by revenue).
# price_to_assets = "PriceToAssets" #Price to assets (price per share divided by assets per share).
# price_to_working_capital = "PriceToWorkingCapital" #Price to working capital per share. Working capital is current assets minus current liabilities.
# price_to_net_current_assets = "PriceToNetCurrentAssets" #Price to net current assets per share. Often refers to current assets minus total liabilities, but exact definition can vary by dataset.
# ev_ebit = "EV_EBIT" #Enterprise value divided by EBIT.
# ev_ebitda = "EV_EBITDA" #Enterprise value divided by EBITDA.

# # Per share metrics
# eps = "EPS" #Earnings per share (EPS).
# bvps = "BVPS" #Book value per share (equity divided by number of shares).

# # Profitability and margins
# gross_margin = "GrossMargin" #Gross margin (gross profit divided by net revenue).
# ebit_margin = "EBITMargin" #EBIT margin (EBIT divided by net revenue).
# net_margin = "NetMargin" #Net margin (net profit divided by net revenue).
# ebit_to_assets = "EBIT_Assets" #EBIT divided by total assets (a profitability over assets measure).
# roe = "ROE" #Return on invested capital (a profitability measure on invested capital, definition can vary).
# roic = "ROIC" #Return on equity (net profit divided by shareholders’ equity).

# # Liquidity and Leverage
# current_ratio = "CurrentRatio" #Current ratio (current assets divided by current liabilities).
# gross_debt_to_equity = "GrossDebt_Equity" #Gross debt divided by equity (a leverage ratio).

# # Efficiency
# asset_turnover = "AssetTurnover" #Asset turnover (net revenue divided by total assets).

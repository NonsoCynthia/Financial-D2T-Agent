from __future__ import annotations

from enum import StrEnum
from pydantic import BaseModel, Field
from typing import Optional


class USIndicator(StrEnum):
    """Canonical names for the 32 indicators fundamental analysis setup."""

    # Balance sheet
    ASSETS = "Assets" #the total assets, rights, and value that the company owns
    CURRENT_ASSETS = "CurrentAssets" #total assets and rights that the company owns and can be converted into cash in the short term, usually in one year
    CASH_AND_EQUIVALENTS = "CashAndEquivalents" #the amounts that the company has in cash, banks, and cash equivalents
    GROSS_DEBT = "GrossDebt" #Crosscheck with: GrossDebt = ShortTermDebt + LongTermDebt
    NET_DEBT = "NetDebt" #Crosscheck with:NET_DEBT = GROSS_DEBT - CASH_AND_EQUIVALENTS
    SHAREHOLDERS_EQUITY = "ShareholdersEquity" #Crosscheck with: ShareholdersEquity = Assets - TotalLiabilities

    # Income statement (TTM and quarter)
    NET_REVENUE_TTM = "NetRevenue_TTM" 
    NET_REVENUE_Q = "NetRevenue_Q"
    EBIT_TTM = "EBIT_TTM" #EBIT = GrossProfit - SellingExpenses - AdministrativeExpenses. Then: EBIT_TTM = sum(EBIT_Q over last 4 quarters)
    EBIT_Q = "EBIT_Q"
    NET_PROFIT_TTM = "NetProfit_TTM" #NetProfit_TTM = sum(NetProfit_Q over last 4 quarters)
    NET_PROFIT_Q = "NetProfit_Q"
    
    # Per share
    EPS = "EPS" #EPS = NetIncome / Shares
    BVPS = "BVPS" #BVPS = ShareholdersEquity / Shares

    # Valuation ratios
    P_E = "P_E" #P_E = Price / EPS
    P_B = "P_B" #P_B = Price / BVPS
    P_EBIT = "P_EBIT" #EBIT_per_share = EBIT / Shares. Then, P_EBIT = Price / EBIT_per_share
    PRICE_TO_SALES = "PriceToSales" #Revenue_per_share = NetRevenue_TTM / Shares (or annual, depending on your choice). Then: PriceToSales = Price / Revenue_per_share
    PRICE_TO_ASSETS = "PriceToAssets" #Assets_per_share = Assets / Shares. Then: PriceToAssets = Price / Assets_per_share
    PRICE_TO_WORKING_CAPITAL = "PriceToWorkingCapital" #WorkingCapital = CurrentAssets - CurrentLiabilities. 
    # WorkingCapital_per_share = WorkingCapital / Shares. PriceToWorkingCapital = Price / WorkingCapital_per_share
    PRICE_TO_NET_CURRENT_ASSETS = "PriceToNetCurrentAssets" #NetCurrentAssets = CurrentAssets - CurrentLiabilities. 
    # NetCurrentAssets_per_share = NetCurrentAssets / Shares. PriceToNetCurrentAssets = Price / NetCurrentAssets_per_share
    # Let MarketCap = Price * Shares. Let EV = MarketCap + GrossDebt - CashAndEquivalents.
    EV_EBIT = "EV_EBIT" #EV_EBIT = EV / EBIT
    EV_EBITDA = "EV_EBITDA" #EBITDA = OperatingIncome + Depreciation + Amortization. Then: EV_EBITDA = EV / EBITDA

    # Profitability and margins
    GROSS_MARGIN = "GrossMargin" #GrossMargin = GrossProfit / NetRevenue
    EBIT_MARGIN = "EBITMargin" #EBITMargin = EBIT / NetRevenue
    NET_MARGIN = "NetMargin" #NetMargin = NetIncome / NetRevenue
    EBIT_TO_ASSETS = "EBIT_Assets" #EBIT_Assets = EBIT / Assets
    ROE = "ROE" #ROE = NetIncome / ShareholdersEquity
    ROIC = "ROIC" #ROIC = EBIT / (Assets - TradePayables - CashAndEquivalents)

    # Liquidity and leverage
    CURRENT_RATIO = "CurrentRatio" #CurrentRatio = (CurrentAssets - CurrentLiabilities) / CurrentLiabilities
    # OR CurrentAssets / CurrentLiabilities
    GROSS_DEBT_TO_EQUITY = "GrossDebt_Equity" #GrossDebt_Equity = GrossDebt / ShareholdersEquity

    # Efficiency
    ASSET_TURNOVER = "AssetTurnover" #AssetTurnover = NetRevenue / Assets OR NetRevenue_TTM / average(Assets)

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

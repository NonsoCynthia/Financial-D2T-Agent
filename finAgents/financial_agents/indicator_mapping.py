"""Canonical indicator and key-normalization helpers shared across scripts."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd

from finAgents.financial_agents.financial_analyst import expected_indicator_keys


EXPECTED_INDICATORS: List[str] = expected_indicator_keys()
EVAL_INDICATORS: List[str] = EXPECTED_INDICATORS + ["last_price"]


INDICATOR_ALIASES: Dict[str, str] = {
    # Identity aliases (all canonical names)
    **{k: k for k in EXPECTED_INDICATORS},
    # Common legacy/alternative names
    "PE": "P_E",
    "PB": "P_B",
    "NETREVENUE": "NetRevenue_TTM",
    "NETREVENUE_TTM": "NetRevenue_TTM",
    "NETREVENUE_Q": "NetRevenue_Q",
    "REVENUES": "NetRevenue_TTM",
    "EBIT": "EBIT_TTM",
    "EBIT_TTM": "EBIT_TTM",
    "EBIT_Q": "EBIT_Q",
    "OPERATINGINCOMELOSS": "EBIT_TTM",
    "NETPROFIT": "NetProfit_TTM",
    "NETPROFIT_TTM": "NetProfit_TTM",
    "NETPROFIT_Q": "NetProfit_Q",
    "NETINCOMELOSS": "NetProfit_TTM",
    "STOCKHOLDERSEQUITY": "ShareholdersEquity",
    "SHAREHOLDERSEQUITY": "ShareholdersEquity",
    "EARNINGSPERSHAREBASIC": "EPS",
    "CASHANDCASHEQUIVALENTSATCARRYINGVALUE": "CashAndEquivalents",
    "PRICE": "last_price",
    "ADJ_CLOSE": "last_price",
    "CLOSE": "last_price",
    "PRICE_CLOSE": "last_price",
    "LAST_PRICE": "last_price",
    "P/B": "P_B",
    "P/E": "P_E",
}


GOLD_COLUMN_CANDIDATES: Dict[str, List[str]] = {
    "Assets": ["Assets"],
    "CurrentAssets": ["CurrentAssets", "AssetsCurrent"],
    "CashAndEquivalents": ["CashAndEquivalents", "CashAndCashEquivalentsAtCarryingValue"],
    "GrossDebt": ["GrossDebt", "LongTermDebt"],
    "NetDebt": ["NetDebt"],
    "ShareholdersEquity": ["ShareholdersEquity", "StockholdersEquity"],
    "NetRevenue_TTM": ["NetRevenue_TTM", "Revenues"],
    "NetRevenue_Q": ["NetRevenue_Q", "Revenues"],
    "EBIT_TTM": ["EBIT_TTM", "OperatingIncomeLoss"],
    "EBIT_Q": ["EBIT_Q", "OperatingIncomeLoss"],
    "NetProfit_TTM": ["NetProfit_TTM", "NetIncomeLoss"],
    "NetProfit_Q": ["NetProfit_Q", "NetIncomeLoss"],
    "P_E": ["P_E"],
    "P_B": ["P_B"],
    "P_EBIT": ["P_EBIT"],
    "PriceToSales": ["PriceToSales"],
    "PriceToAssets": ["PriceToAssets"],
    "PriceToWorkingCapital": ["PriceToWorkingCapital"],
    "PriceToNetCurrentAssets": ["PriceToNetCurrentAssets"],
    "EV_EBIT": ["EV_EBIT"],
    "EV_EBITDA": ["EV_EBITDA"],
    "EPS": ["EPS", "EarningsPerShareBasic"],
    "BVPS": ["BVPS"],
    "GrossMargin": ["GrossMargin"],
    "EBITMargin": ["EBITMargin"],
    "NetMargin": ["NetMargin"],
    "EBIT_Assets": ["EBIT_Assets"],
    "ROE": ["ROE"],
    "ROIC": ["ROIC"],
    "CurrentRatio": ["CurrentRatio"],
    "GrossDebt_Equity": ["GrossDebt_Equity"],
    "AssetTurnover": ["AssetTurnover"],
    "last_price": ["last_price", "adj_close", "price", "price_close", "Close", "close"],
}


def canonical_ticker(x: Any) -> str:
    return str(x or "").strip().upper()


def canonical_month(x: Any) -> str:
    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return ""
    return str(dt.to_period("M"))


def canonical_indicator_name(name: Any) -> Optional[str]:
    s = str(name or "").strip()
    if not s:
        return None
    if s in EXPECTED_INDICATORS or s == "last_price":
        return s
    key = re.sub(r"[^A-Z0-9_]+", "", s.upper().replace("-", "_").replace(" ", "_"))
    return INDICATOR_ALIASES.get(key)


def indicators_to_canonical_map(indicators: Any) -> Dict[str, Optional[float]]:
    """Normalise analyst indicators from dict or list[{indicator,value}] to canonical map."""
    out: Dict[str, Optional[float]] = {}

    def _to_float(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            f = float(v)
        except Exception:
            return None
        if pd.isna(f):
            return None
        return float(f)

    if isinstance(indicators, dict):
        for k, v in indicators.items():
            c = canonical_indicator_name(k)
            if c is None:
                continue
            out[c] = _to_float(v)
        return out

    if isinstance(indicators, list):
        for row in indicators:
            if not isinstance(row, dict):
                continue
            c = canonical_indicator_name(row.get("indicator") or row.get("name"))
            if c is None:
                continue
            out[c] = _to_float(row.get("value"))
    return out


def pick_gold_column(indicator: str, gold_columns: List[str]) -> Optional[str]:
    candidates = GOLD_COLUMN_CANDIDATES.get(indicator, [])
    cols = set(gold_columns)
    for c in candidates:
        if c in cols:
            return c
    return None

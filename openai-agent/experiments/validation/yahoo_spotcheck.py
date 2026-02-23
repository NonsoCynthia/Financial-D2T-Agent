from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd

import yfinance as yf


@dataclass(frozen=True)
class YahooSpotcheckRow:
    ticker: str
    pe_model: float
    pe_yahoo: float
    pb_model: float
    pb_yahoo: float


def yahoo_pe_pb(ticker: str) -> tuple[Optional[float], Optional[float]]:
    """
    Return (trailingPE, priceToBook) from Yahoo Finance.
    """
    try:
        info = yf.Ticker(ticker).info
        pe = info.get("trailingPE", None)
        pb = info.get("priceToBook", None)
        return (float(pe) if pe is not None else None, float(pb) if pb is not None else None)
    except Exception:
        return (None, None)


def spotcheck_pe_pb(model_outputs: Dict[str, Dict[str, float]], tickers: List[str]) -> pd.DataFrame:
    """
    model_outputs: {ticker: {indicator: value}}
    """
    rows = []
    for t in tickers:
        y_pe, y_pb = yahoo_pe_pb(t)
        pe_model = float(model_outputs.get(t, {}).get("P_E", 0.0))
        pb_model = float(model_outputs.get(t, {}).get("P_B", 0.0))
        rows.append(
            {
                "ticker": t,
                "P_E_model": pe_model,
                "P_E_yahoo": y_pe,
                "P_B_model": pb_model,
                "P_B_yahoo": y_pb,
                "abs_diff_pe": None if y_pe is None else abs(pe_model - y_pe),
                "abs_diff_pb": None if y_pb is None else abs(pb_model - y_pb),
            }
        )
    return pd.DataFrame(rows)

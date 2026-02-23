from __future__ import annotations

import calendar
from datetime import date, datetime
from pathlib import Path
import pandas as pd

from experiments import StockInput


def project_root() -> Path:
    # Financial-D2T-Agent/openai-agent/experiments/final_report2025/config.py -> parents[3] is Financial-D2T-Agent
    return Path(__file__).resolve().parents[3]


# The consolidated DB from scripts/08_build_mcp_db.py
DB_PATH = str(project_root() / "data" / "processed" / "mcp" / "fundamental_analysis.db")

ANALYSIS_START_DATE = "2025-01-01"
ANALYSIS_END_DATE = "2025-12-31"
ANALYSIS_MONTH_LABEL = "January-December 2025"


def _monthly_end_dates(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        return []

    year = start.year
    month = start.month
    out: list[str] = []

    while (year, month) <= (end.year, end.month):
        day = calendar.monthrange(year, month)[1]
        month_end = date(year, month, day)
        if start <= month_end <= end:
            out.append(month_end.isoformat())

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return out


ANALYSIS_DATES = _monthly_end_dates(ANALYSIS_START_DATE, ANALYSIS_END_DATE)
if not ANALYSIS_DATES:
    ANALYSIS_DATES = [ANALYSIS_END_DATE]

# Backward-compatible single-date aliases for legacy callers.
TARGET_END_DATE = ANALYSIS_DATES[-1]
TARGET_PREV_END_DATE = "2025-09-30"
PRICE_ASOF_DATE = ANALYSIS_DATES[-1]

# Optional: map ticker->CIK10 if you have the SEC map file.
SEC_MAP_PATH = project_root() / "data" / "raw" / "sec" / "sec_ticker_cik_selected.csv"


def _cik10_for_ticker(ticker: str) -> str:
    if not SEC_MAP_PATH.exists():
        return "N/A"
    try:
        df = pd.read_csv(SEC_MAP_PATH, dtype=str)
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["cik10"] = df["cik10"].astype(str).str.zfill(10)
        row = df[df["ticker"] == ticker.upper().strip()]
        if row.empty:
            return "N/A"
        return str(row.iloc[0]["cik10"])
    except Exception:
        return "N/A"


STOCKS = [
    StockInput(name="Tesla", cnpj=_cik10_for_ticker("TSLA"), stock_id="TSLA"),
    StockInput(name="Amazon", cnpj=_cik10_for_ticker("AMZN"), stock_id="AMZN"),
    StockInput(name="Microsoft", cnpj=_cik10_for_ticker("MSFT"), stock_id="MSFT"),
    StockInput(name="Apple", cnpj=_cik10_for_ticker("AAPL"), stock_id="AAPL"),
    StockInput(name="Alphabet", cnpj=_cik10_for_ticker("GOOG"), stock_id="GOOG"),
    StockInput(name="Netflix", cnpj=_cik10_for_ticker("NFLX"), stock_id="NFLX"),
    StockInput(name="Coinbase", cnpj=_cik10_for_ticker("COIN"), stock_id="COIN"),
    StockInput(name="NIO", cnpj=_cik10_for_ticker("NIO"), stock_id="NIO"),
]

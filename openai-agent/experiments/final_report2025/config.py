from __future__ import annotations

import calendar
import os
from datetime import date, datetime
from pathlib import Path
import pandas as pd

from experiments import StockInput


def project_root() -> Path:
    # Financial-D2T-Agent/openai-agent/experiments/final_report2025/config.py -> parents[3] is Financial-D2T-Agent
    return Path(__file__).resolve().parents[3]


# The consolidated DB from scripts/08_build_mcp_db.py
DB_PATH = os.getenv("US_DB_PATH", str(project_root() / "data" / "processed" / "mcp" / "fundamental_analysis.db"))

ANALYSIS_START_DATE = os.getenv("ANALYSIS_START_DATE", "2025-01-01")
# Default analysis horizon for agent/workflow runs.
# Override at runtime with ANALYSIS_END_DATE (or set ANALYSIS_END_DATE_DEFAULT).
ANALYSIS_END_DATE_DEFAULT = os.getenv("ANALYSIS_END_DATE_DEFAULT", "2026-02-25")
ANALYSIS_END_DATE = os.getenv("ANALYSIS_END_DATE", ANALYSIS_END_DATE_DEFAULT)


def _monthly_end_dates(start_date: str, end_date: str) -> list[str]:
    """
    Build monthly analysis checkpoints between start and end.

    Includes month-end dates within the range and always includes the exact
    end_date as a final checkpoint when it is not itself a month-end date.
    """
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

    end_iso = end.isoformat()
    if not out or out[-1] != end_iso:
        out.append(end_iso)
    return out


ANALYSIS_DATES = _monthly_end_dates(start_date=ANALYSIS_START_DATE, end_date=ANALYSIS_END_DATE)
if not ANALYSIS_DATES:
    ANALYSIS_DATES = [ANALYSIS_END_DATE]


def _analysis_month_label(start_date: str, end_date: str) -> str:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    start_label = start.strftime("%B %Y")
    end_label = end.strftime("%B %Y")
    if start_label == end_label:
        base = start_label
    else:
        base = f"{start_label}-{end_label}"

    end_is_month_end = end.day == calendar.monthrange(end.year, end.month)[1]
    if not end_is_month_end:
        return f"{base} (through {end.isoformat()})"
    return base


ANALYSIS_MONTH_LABEL = _analysis_month_label(
    start_date=ANALYSIS_START_DATE,
    end_date=ANALYSIS_END_DATE,
)

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
    StockInput(name="Tesla", cnpj=_cik10_for_ticker(ticker="TSLA"), stock_id="TSLA"),
    StockInput(name="Amazon", cnpj=_cik10_for_ticker(ticker="AMZN"), stock_id="AMZN"),
    StockInput(name="Microsoft", cnpj=_cik10_for_ticker(ticker="MSFT"), stock_id="MSFT"),
    StockInput(name="Apple", cnpj=_cik10_for_ticker(ticker="AAPL"), stock_id="AAPL"),
    StockInput(name="Alphabet", cnpj=_cik10_for_ticker(ticker="GOOG"), stock_id="GOOG"),
    StockInput(name="Netflix", cnpj=_cik10_for_ticker(ticker="NFLX"), stock_id="NFLX"),
    StockInput(name="Coinbase", cnpj=_cik10_for_ticker(ticker="COIN"), stock_id="COIN"),
    StockInput(name="NIO", cnpj=_cik10_for_ticker(ticker="NIO"), stock_id="NIO"),
]

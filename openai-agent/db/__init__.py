""" US helper functions for date selection and price lookup (used by experiments)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from db.base_query import run_sql_query, ResponseFormat


def project_root() -> Path:
    # Financial_D2T/openai-agent/db/__init__.py -> parents[2] is Financial_D2T
    return Path(__file__).resolve().parents[2]


def default_db_path() -> str:
    return str(project_root() / "data" / "processed" / "mcp" / "fundamental_analysis.db")


def get_price_on_or_before(ticker: str, as_of_date: str, db_path: Optional[str] = None) -> Optional[float]:
    """
    Return CLOSE price from US_PRICES on the last trading day <= as_of_date.
    """
    db = db_path or default_db_path()
    q = f"""
    SELECT TRADE_DATE, CLOSE
    FROM US_PRICES
    WHERE TICKER = '{ticker}' AND TRADE_DATE <= '{as_of_date}'
    ORDER BY TRADE_DATE DESC
    LIMIT 1;
    """
    r = run_sql_query({"sql_query": q}, db_path=db, response_format=ResponseFormat.DICT)
    rows = r.get("report", [])
    if not rows or not isinstance(rows, list):
        return None
    try:
        return float(rows[0]["CLOSE"])
    except Exception:
        return None


def get_nearest_report_end_date(ticker: str, target_end_date: str, db_path: Optional[str] = None) -> Optional[str]:
    """
    Find the nearest END_DATE <= target_end_date using the Assets concept as anchor.
    """
    db = db_path or default_db_path()
    q = f"""
    SELECT END_DATE
    FROM SEC_COMPANYFACTS
    WHERE TICKER = '{ticker}' AND CONCEPT = 'Assets' AND END_DATE <= '{target_end_date}'
    ORDER BY END_DATE DESC
    LIMIT 1;
    """
    r = run_sql_query({"sql_query": q}, db_path=db, response_format=ResponseFormat.DICT)
    rows = r.get("report", [])
    if not rows or not isinstance(rows, list):
        return None
    return str(rows[0]["END_DATE"])


def get_previous_report_end_date(ticker: str, end_date: str, db_path: Optional[str] = None) -> Optional[str]:
    db = db_path or default_db_path()
    q = f"""
    SELECT END_DATE
    FROM SEC_COMPANYFACTS
    WHERE TICKER = '{ticker}' AND CONCEPT = 'Assets' AND END_DATE < '{end_date}'
    ORDER BY END_DATE DESC
    LIMIT 1;
    """
    r = run_sql_query({"sql_query": q}, db_path=db, response_format=ResponseFormat.DICT)
    rows = r.get("report", [])
    if not rows or not isinstance(rows, list):
        return None
    return str(rows[0]["END_DATE"])


# Backward-compatible aliases used by older scripts.
def get_latest_assets_end_date(ticker: str, target_end_date: str, db_path: Optional[str] = None) -> Optional[str]:
    return get_nearest_report_end_date(ticker=ticker, target_end_date=target_end_date, db_path=db_path)


def get_previous_assets_end_date(ticker: str, end_date: str, db_path: Optional[str] = None) -> Optional[str]:
    return get_previous_report_end_date(ticker=ticker, end_date=end_date, db_path=db_path)

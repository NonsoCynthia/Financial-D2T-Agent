"""This is where your read-only SQL guard lives (your explicit requirement), and it exposes 
two tools mirroring Thiago’s “reports DB tool” and “share composition DB tool” concept. """

from __future__ import annotations

import os
import re
from pathlib import Path

from agents import function_tool
from db.base_query import QueryInput, run_sql_query


def _default_db_path() -> str:
    # Allow override via env for convenience (no Docker).
    env = os.getenv("US_DB_PATH")
    if env:
        return env

    # Financial_D2T/openai-agent/tools/us_sql_tool.py -> parents[2] is Financial_D2T
    root = Path(__file__).resolve().parents[2]
    return str(root / "data" / "processed" / "mcp" / "fundamental_analysis.db")


DB_PATH = _default_db_path()


def _is_read_only_select(sql: str) -> bool:
    """
    Minimal guardrail to allow only a single read-only SELECT or WITH...SELECT statement.

    Rejects: INSERT, UPDATE, DELETE, DROP, ATTACH, PRAGMA, etc.
    """
    if not isinstance(sql, str):
        return False

    s = sql.strip()
    if not s:
        return False

    # Reject multiple statements; allow a single trailing semicolon.
    if ";" in s.rstrip(";"):
        return False

    # Reject SQL comments to reduce bypass surface.
    if re.search(r"(--|/\*)", s):
        return False

    banned = [
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bALTER\b",
        r"\bCREATE\b",
        r"\bREPLACE\b",
        r"\bTRUNCATE\b",
        r"\bATTACH\b",
        r"\bDETACH\b",
        r"\bPRAGMA\b",
        r"\bVACUUM\b",
        r"\bREINDEX\b",
        r"\bANALYZE\b",
        r"\bBEGIN\b",
        r"\bCOMMIT\b",
        r"\bROLLBACK\b",
    ]
    if re.search("|".join(banned), s, flags=re.IGNORECASE):
        return False

    # Must start with SELECT or WITH
    if not re.match(r"^(SELECT|WITH)\b", s, flags=re.IGNORECASE):
        return False

    return True


@function_tool
def us_reports_query_tool(inp: QueryInput) -> dict:
    """
    Query a US SQL database containing SEC CompanyFacts and aligned market data.

    Consolidated schema (key tables you can query):

    1) SEC_COMPANYFACTS (long table, from SEC CompanyFacts JSON)
       Columns (common): TICKER, CONCEPT, UNIT, VALUE_REAL, FORM, FY, FP, START_DATE, END_DATE, FILED_DATE, ACCN, FRAME

    2) US_PRICES (daily OHLCV)
       Columns: TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, ...

    3) US_DAILY_PANEL / US_MONTHLY_PANEL / US_FUNDAMENTALS_WIDE_BY_FILED (from your panel scripts)

    READ-ONLY policy:
    - Only a single SELECT query is allowed.
    """
    sql = inp.get("sql_query", "")
    if not _is_read_only_select(sql=sql):
        return {"status": "error", "report": "Rejected. Only a single read-only SELECT query is allowed."}
    return run_sql_query(inp, db_path=DB_PATH)


@function_tool
def us_share_composition_query_tool(inp: QueryInput) -> dict:
    """
    Query the same US SQL database, focusing on share composition / shares outstanding.

    Typical share concept in SEC_COMPANYFACTS:
    - CommonStockSharesOutstanding

    READ-ONLY policy:
    - Only a single SELECT query is allowed.
    """
    sql = inp.get("sql_query", "")
    if not _is_read_only_select(sql=sql):
        return {"status": "error", "report": "Rejected. Only a single read-only SELECT query is allowed."}
    return run_sql_query(inp, db_path=DB_PATH)

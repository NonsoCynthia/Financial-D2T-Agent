#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from fastmcp import FastMCP


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "processed" / "mcp" / "fundamental_analysis.db"
DB_PATH = Path(os.getenv("US_DB_PATH", str(DEFAULT_DB_PATH))).resolve()

mcp = FastMCP("US Finance SQL MCP")


def _create_markdown_table(rows: list[tuple[Any, ...]], columns: list[str]) -> str:
    out = "| " + " | ".join(columns) + " |\n"
    out += "| " + " | ".join(["---"] * len(columns)) + " |\n"
    for row in rows:
        out += "| " + " | ".join([str(cell) if cell is not None else "" for cell in row]) + " |\n"
    return out


def _is_read_only_select(sql: str) -> bool:
    if not isinstance(sql, str):
        return False
    s = sql.strip()
    if not s:
        return False

    # Reject multiple statements; allow one optional trailing semicolon.
    if ";" in s.rstrip(";"):
        return False

    # Reduce bypass surface.
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

    return bool(re.match(r"^(SELECT|WITH)\b", s, flags=re.IGNORECASE))


def _run_sql_query(sql_query: str) -> dict[str, Any]:
    if not DB_PATH.exists():
        return {"status": "error", "report": f"Database not found: {DB_PATH}"}

    if not _is_read_only_select(sql_query):
        return {"status": "error", "report": "Rejected. Only a single read-only SELECT query is allowed."}

    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            if not rows:
                return {"status": "success", "report": "No data found with the given query"}
            return {"status": "success", "report": _create_markdown_table(rows, columns)}
    except Exception as exc:
        return {"status": "error", "report": f"Failed to execute query: {exc}"}


@mcp.tool()
def us_reports_query_tool(sql_query: str) -> dict[str, Any]:
    """
    Run a read-only SQL query for SEC reports/fundamental data.
    """
    return _run_sql_query(sql_query)


@mcp.tool()
def us_share_composition_query_tool(sql_query: str) -> dict[str, Any]:
    """
    Run a read-only SQL query for shares/EPS composition data.
    """
    return _run_sql_query(sql_query)


@mcp.tool()
def list_tickers(limit: int = 200) -> dict[str, Any]:
    """
    List distinct tickers from SEC_COMPANYFACTS.
    """
    sql = f"""
    SELECT DISTINCT TICKER
    FROM SEC_COMPANYFACTS
    ORDER BY TICKER
    LIMIT {int(max(1, min(limit, 2000)))}
    """
    return _run_sql_query(sql)


if __name__ == "__main__":
    # Use stdio transport for local agent integration.
    mcp.run(transport="stdio", show_banner=False)

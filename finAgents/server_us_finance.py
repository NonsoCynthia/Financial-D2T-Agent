import sys
import os
import logging
from pathlib import Path
import sqlite3
import pandas as pd
from typing import Any, Dict, List, Optional, Literal
from fastmcp import FastMCP

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from finAgents.agent_tools.us_queries import (
    get_companyfacts_tool,
    get_panel_tool,
    get_prices_tool,
    get_returns_tool,
    list_tickers_tool,
)

MAX_TOOL_ROWS = 800

DEFAULT_CONCEPTS = [
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
]

def setup_logging() -> Path:
    """
    Configure file logging for the MCP server.

    Logs are written to logs/us_finance_mcp.log by default.
    You can override the log file path by setting MCP_LOG_PATH.
    """
    logs_dir = ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_path = Path(os.environ.get("MCP_LOG_PATH", str(logs_dir / "mcp_server.log")))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8")],
    )
    return log_path


LOG_PATH = setup_logging()
logger = logging.getLogger(__name__)
logger.info("Starting US Finance MCP Server. Logging to %s", str(LOG_PATH))

mcp = FastMCP("US Finance MCP Server")

def prices_db_path() -> Path:
    """
    Return the SQLite path that stores US price data.
    """
    return ROOT / "data" / "raw" / "prices_us.db"


def read_sqlite(db_path: Path, query: str, params: tuple[Any, ...]) -> pd.DataFrame:
    """
    Execute a parameterised SQL query against a SQLite database and return a DataFrame.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    con = sqlite3.connect(str(db_path))
    try:
        return pd.read_sql_query(query, con, params=params)
    finally:
        con.close()


def df_to_records(df: pd.DataFrame, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Convert a DataFrame into JSON-serialisable rows.

    We enforce a hard cap (MAX_TOOL_ROWS) so tool outputs never explode.
    """
    if df is None or df.empty:
        return []
    hard_limit = min(int(limit), int(MAX_TOOL_ROWS))
    if len(df) > hard_limit:
        df = df.head(hard_limit)
    return df.to_dict(orient="records")


@mcp.tool()
def list_tickers() -> Dict[str, Any]:
    """
    List distinct tickers available in the US_PRICES table.
    """
    logger.info("Tool call: list_tickers")
    return list_tickers_tool()


@mcp.tool()
def get_prices(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch OHLCV prices for one ticker between two dates (inclusive).
    """
    logger.info("Tool call: get_prices ticker=%s start=%s end=%s limit=%s", ticker, start_date, end_date_inclusive, limit)
    return get_prices_tool(ticker=ticker, start_date=start_date, end_date_inclusive=end_date_inclusive, limit=limit)


@mcp.tool()
def get_returns(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch daily returns for one ticker between two dates (inclusive).
    """
    logger.info("Tool call: get_returns ticker=%s start=%s end=%s limit=%s", ticker, start_date, end_date_inclusive, limit)
    return get_returns_tool(ticker=ticker, start_date=start_date, end_date_inclusive=end_date_inclusive, limit=limit)


@mcp.tool()
def get_companyfacts(
    ticker: str,
    concepts: Optional[List[str]] = None,
    start_end_date: Optional[str] = None,
    end_end_date: Optional[str] = None,
    limit: int = 5000,
) -> Dict[str, Any]:
    """
    Fetch SEC companyfacts for one ticker.
    """
    logger.info(
        "Tool call: get_companyfacts ticker=%s concepts=%s start_end=%s end_end=%s limit=%s",
        ticker,
        concepts,
        start_end_date,
        end_end_date,
        limit,
    )
    if not concepts:
        concepts = DEFAULT_CONCEPTS
        
    return get_companyfacts_tool(
        ticker=ticker,
        concepts=concepts,
        start_end_date=start_end_date,
        end_end_date=end_end_date,
        limit=limit,
    )


@mcp.tool()
def get_panel(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch merged daily panel rows for one ticker between two dates (inclusive).
    """
    logger.info("Tool call: get_panel ticker=%s start=%s end=%s limit=%s", ticker, start_date, end_date_inclusive, limit)
    return get_panel_tool(ticker=ticker, start_date=start_date, end_date_inclusive=end_date_inclusive, limit=limit)


@mcp.tool()
def get_price_series(
    ticker: str,
    start_date: str,
    end_date_inclusive: str,
    price_field: Literal["Adj Close", "Close"] = "Adj Close",
    limit: int = 10000,
) -> Dict[str, Any]:
    """
    Return a minimal price series for one ticker.

    Output rows contain:
    - date
    - ticker
    - price

    This keeps the payload small for the daily trading loop.
    """
    logger.info(
        "Tool call: get_price_series ticker=%s start=%s end=%s price_field=%s limit=%s",
        ticker,
        start_date,
        end_date_inclusive,
        price_field,
        limit,
    )

    col = "ADJ_CLOSE" if price_field == "Adj Close" else "CLOSE"

    q = f"""
    SELECT
      TRADE_DATE AS date,
      TICKER AS ticker,
      {col} AS price
    FROM US_PRICES
    WHERE TICKER = ? AND TRADE_DATE >= ? AND TRADE_DATE <= ?
    ORDER BY TRADE_DATE
    """

    df = read_sqlite(
        prices_db_path(),
        q,
        (ticker.upper().strip(), start_date, end_date_inclusive),
    )

    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


if __name__ == "__main__":
    mcp.run()

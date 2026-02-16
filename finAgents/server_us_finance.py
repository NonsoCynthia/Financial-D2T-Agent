import os
import sys
from pathlib import Path

# def redirect_stderr_to_file() -> None:
#     """Redirect stderr to a file so Rich banners and logs do not break STDIO MCP."""
#     root = Path(__file__).resolve().parents[1]
#     logs_dir = root / "logs"
#     logs_dir.mkdir(parents=True, exist_ok=True)
#     err_path = logs_dir / "mcp_server_stderr.log"

#     f = open(err_path, "a", encoding="utf-8")
#     sys.stderr = f

# redirect_stderr_to_file()

import logging
import sqlite3
from typing import Any, Dict, List, Optional, Literal, Tuple

import pandas as pd
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

from finAgents.agent_tools.monthly_queries import get_monthly_window_tool


MAX_TOOL_ROWS = 800

DEFAULT_CONCEPTS = [
    "Assets",
    "AssetsCurrent",
    "Liabilities",
    "LiabilitiesCurrent",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
    "CashAndCashEquivalentsAtCarryingValue",
    "LongTermDebt",
    "LongTermDebtCurrent",
    "DebtCurrent",
]


def setup_logging() -> Path:
    """
    Configure file logging for the MCP server.

    Logs are written to logs/mcp_server.log by default.
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


def panel_db_path() -> Path:
    """
    Return the SQLite path that stores the merged daily panel.
    """
    return ROOT / "data" / "processed" / "panel" / "panel.db"


def read_sqlite(db_path: Path, query: str, params: Tuple[Any, ...]) -> pd.DataFrame:
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


def _coerce_json_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a DataFrame safe for JSON serialisation.

    This:
    - converts bytes to UTF-8 strings (with replacement for invalid bytes)
    - converts datetimes to ISO strings
    - leaves numeric columns as-is
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    for col in out.columns:
        s = out[col]

        if s.dtype == "object":
            def _fix(v: Any) -> Any:
                if isinstance(v, (bytes, bytearray)):
                    return v.decode("utf-8", errors="replace")
                return v
            out[col] = s.map(_fix)

        if pd.api.types.is_datetime64_any_dtype(s):
            out[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S")

    return out


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
    df = _coerce_json_safe_df(df)
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
    logger.info(
        "Tool call: get_prices ticker=%s start=%s end=%s limit=%s",
        ticker,
        start_date,
        end_date_inclusive,
        limit,
    )
    return get_prices_tool(ticker=ticker, start_date=start_date, end_date_inclusive=end_date_inclusive, limit=limit)


@mcp.tool()
def get_returns(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch daily returns for one ticker between two dates (inclusive).
    """
    logger.info(
        "Tool call: get_returns ticker=%s start=%s end=%s limit=%s",
        ticker,
        start_date,
        end_date_inclusive,
        limit,
    )
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
    logger.info(
        "Tool call: get_panel ticker=%s start=%s end=%s limit=%s",
        ticker,
        start_date,
        end_date_inclusive,
        limit,
    )
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
    Return a minimal daily price series for one ticker.

    Output rows contain: date, ticker, price
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

    df = read_sqlite(prices_db_path(), q, (ticker.upper().strip(), start_date, end_date_inclusive))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


@mcp.tool()
def get_monthly_price_series(
    ticker: str,
    start_date: str,
    end_date_inclusive: str,
    price_field: Literal["Adj Close", "Close"] = "Adj Close",
    limit: int = 240,
) -> Dict[str, Any]:
    """
    Return one row per calendar month for a ticker.

    The row selected is the first trading day in each month within the requested range.
    Output rows contain: date, ticker, price
    """
    logger.info(
        "Tool call: get_monthly_price_series ticker=%s start=%s end=%s price_field=%s limit=%s",
        ticker,
        start_date,
        end_date_inclusive,
        price_field,
        limit,
    )

    col = "ADJ_CLOSE" if price_field == "Adj Close" else "CLOSE"

    q = f"""
    WITH first_days AS (
      SELECT
        TICKER,
        strftime('%Y-%m', TRADE_DATE) AS ym,
        MIN(TRADE_DATE) AS first_trade_date
      FROM US_PRICES
      WHERE TICKER = ? AND TRADE_DATE >= ? AND TRADE_DATE <= ?
      GROUP BY TICKER, ym
    )
    SELECT
      u.TRADE_DATE AS date,
      u.TICKER AS ticker,
      u.{col} AS price
    FROM US_PRICES u
    INNER JOIN first_days f
      ON u.TICKER = f.TICKER AND u.TRADE_DATE = f.first_trade_date
    ORDER BY u.TRADE_DATE
    """

    df = read_sqlite(prices_db_path(), q, (ticker.upper().strip(), start_date, end_date_inclusive))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


@mcp.tool()
def get_monthly_window(ticker: str, as_of_date: str, months: int = 12, limit: int = 200) -> Dict[str, Any]:
    """
    Fetch a rolling window of monthly snapshots (default 12 months) up to as_of_date.

    This is the core Thiago-style input table.
    """
    logger.info(
        "Tool call: get_monthly_window ticker=%s as_of_date=%s months=%s limit=%s",
        ticker,
        as_of_date,
        months,
        limit,
    )

    return get_monthly_window_tool(
        panel_db_path=panel_db_path(),
        ticker=ticker,
        as_of_date=as_of_date,
        months=months,
        limit=limit,
    )


if __name__ == "__main__":
    mcp.run()

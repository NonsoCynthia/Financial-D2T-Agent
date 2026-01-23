from typing import Any, Dict, List, Optional

from finAgents.agent_tools.paths import data_paths
from finAgents.agent_tools.sqlite_utils import df_to_records, read_sql


def list_tickers_tool() -> Dict[str, Any]:
    """
    List distinct tickers available in the US_PRICES table.
    """
    p = data_paths()
    q = "SELECT DISTINCT TICKER AS ticker FROM US_PRICES ORDER BY TICKER"
    df = read_sql(p["prices_db"], q)
    return {"tickers": df["ticker"].tolist(), "count": int(df.shape[0])}


def get_prices_tool(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch OHLCV prices for one ticker between two dates (inclusive).
    """
    p = data_paths()
    q = """
    SELECT
      TRADE_DATE AS date,
      TICKER AS ticker,
      OPEN AS "Open",
      HIGH AS "High",
      LOW AS "Low",
      CLOSE AS "Close",
      ADJ_CLOSE AS "Adj Close",
      VOLUME AS "Volume"
    FROM US_PRICES
    WHERE TICKER = ? AND TRADE_DATE >= ? AND TRADE_DATE <= ?
    ORDER BY TRADE_DATE
    """
    df = read_sql(p["prices_db"], q, (ticker.upper().strip(), start_date, end_date_inclusive))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


def get_returns_tool(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch daily returns for one ticker between two dates (inclusive).
    """
    p = data_paths()
    q = """
    SELECT
      TRADE_DATE AS date,
      TICKER AS ticker,
      ADJ_CLOSE AS adj_close,
      RET_1D AS ret_1d,
      LOG_RET_1D AS log_ret_1d,
      VOLUME AS Volume
    FROM US_RETURNS
    WHERE TICKER = ? AND TRADE_DATE >= ? AND TRADE_DATE <= ?
    ORDER BY TRADE_DATE
    """
    df = read_sql(p["prices_db"], q, (ticker.upper().strip(), start_date, end_date_inclusive))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


def get_companyfacts_tool(
    ticker: str,
    concepts: Optional[List[str]] = None,
    start_end_date: Optional[str] = None,
    end_end_date: Optional[str] = None,
    limit: int = 5000,
) -> Dict[str, Any]:
    """
    Fetch SEC companyfacts for one ticker.

    Filters:
    - concepts: list of XBRL concepts to include
    - start_end_date, end_end_date: filter by report end date
    """
    p = data_paths()

    where = ["TICKER = ?"]
    params: List[Any] = [ticker.upper().strip()]

    if concepts:
        placeholders = ",".join(["?"] * len(concepts))
        where.append(f"CONCEPT IN ({placeholders})")
        params.extend([c.strip() for c in concepts])

    if start_end_date:
        where.append("END_DATE >= ?")
        params.append(start_end_date)

    if end_end_date:
        where.append("END_DATE <= ?")
        params.append(end_end_date)

    q = f"""
    SELECT
      TICKER AS ticker,
      CIK10 AS cik10,
      COMPANY_TITLE AS company_title,
      CONCEPT AS concept,
      UNIT AS unit,
      VALUE_REAL AS value,
      FORM AS form,
      FY AS fy,
      FP AS fp,
      END_DATE AS end,
      FILED_DATE AS filed,
      ACCN AS accn,
      FRAME AS frame
    FROM SEC_COMPANYFACTS
    WHERE {" AND ".join(where)}
    ORDER BY FILED_DATE, END_DATE
    """
    df = read_sql(p["facts_db"], q, tuple(params))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}


def get_panel_tool(ticker: str, start_date: str, end_date_inclusive: str, limit: int = 5000) -> Dict[str, Any]:
    """
    Fetch merged daily panel rows for one ticker between two dates (inclusive).
    """
    p = data_paths()
    q = """
    SELECT
      TICKER AS ticker,
      TRADE_DATE AS date,
      ADJ_CLOSE AS adj_close,
      RET_1D AS ret_1d,
      LOG_RET_1D AS log_ret_1d,
      VOLUME AS Volume,
      FILED_DATE AS filed,
      Assets, Liabilities, StockholdersEquity, Revenues, NetIncomeLoss,
      OperatingIncomeLoss, EarningsPerShareBasic, CommonStockSharesOutstanding
    FROM US_DAILY_PANEL
    WHERE TICKER = ? AND TRADE_DATE >= ? AND TRADE_DATE <= ?
    ORDER BY TRADE_DATE
    """
    df = read_sql(p["panel_db"], q, (ticker.upper().strip(), start_date, end_date_inclusive))
    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}

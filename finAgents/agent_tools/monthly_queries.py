import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def read_sql(db_path: Path, query: str, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
    """
    Execute a parameterised SQL query against a SQLite database and return a DataFrame.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    con = sqlite3.connect(str(db_path))
    try:
        return pd.read_sql_query(query, con, params=params or ())
    finally:
        con.close()


def df_to_records(df: pd.DataFrame, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Convert a DataFrame to JSON serialisable records with a hard row limit.
    """
    if df is None or df.empty:
        return []
    if len(df) > limit:
        df = df.head(limit)
    return df.to_dict(orient="records")


def get_monthly_window_tool(panel_db_path: Path, ticker: str, as_of_date: str, months: int = 12, limit: int = 200) -> Dict[str, Any]:
    """
    Fetch the last N monthly snapshot rows up to (and including) as_of_date.

    The monthly table is assumed to be US_MONTHLY_PANEL in data/processed/panel/panel.db.
    """
    t = ticker.upper().strip()
    m = int(months)

    q = """
    SELECT *
    FROM US_MONTHLY_PANEL
    WHERE ticker = ? AND date <= ?
    ORDER BY date DESC
    LIMIT ?
    """

    df = read_sql(panel_db_path, q, (t, as_of_date, m))
    df = df.sort_values(["date"]).reset_index(drop=True)

    return {"rows": df_to_records(df, limit=limit), "n": int(df.shape[0])}

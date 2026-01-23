import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def read_sql(db_path: Path, query: str, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
    """
    Execute a parameterised SQL query against a SQLite database and return a DataFrame.

    Important: set text_factory so any non-UTF8 bytes in TEXT fields are decoded safely,
    rather than raising errors later during JSON serialisation.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    con = sqlite3.connect(str(db_path))

    con.text_factory = lambda b: b.decode("utf-8", "replace") if isinstance(b, (bytes, bytearray)) else b

    try:
        return pd.read_sql_query(query, con, params=params or ())
    finally:
        con.close()


def _to_json_safe_value(x: Any) -> Any:
    """
    Convert values to JSON safe Python primitives.

    Handles:
    - bytes or bytearray: decode using UTF-8 with replacement
    - numpy scalars: convert to Python scalars
    - pandas timestamps: convert to ISO format
    - NaN: convert to None
    """
    if x is None:
        return None

    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")

    if isinstance(x, (np.generic,)):
        return x.item()

    if isinstance(x, (pd.Timestamp,)):
        if pd.isna(x):
            return None
        return x.isoformat()

    if isinstance(x, float) and np.isnan(x):
        return None

    return x


def df_to_records(df: pd.DataFrame, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Convert a DataFrame to JSON serialisable records with a hard row limit.

    This ensures FastMCP can always serialise the output even if the database contains
    odd encodings or binary values.
    """
    if df is None or df.empty:
        return []

    if len(df) > limit:
        df = df.head(limit)

    records = df.to_dict(orient="records")

    safe_records: List[Dict[str, Any]] = []
    for r in records:
        safe_r = {k: _to_json_safe_value(v) for k, v in r.items()}
        safe_records.append(safe_r)

    return safe_records

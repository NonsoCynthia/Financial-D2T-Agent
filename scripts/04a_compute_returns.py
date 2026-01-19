import sys
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import PRICES_RAW_DIR, PROCESSED_DIR, RAW_DIR

OUT_DIR = PROCESSED_DIR / "prices"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRICES_DB_PATH = RAW_DIR / "prices_us.db"
PRICES_TABLE = "US_PRICES"

RETURNS_TABLE = "US_RETURNS"


def load_prices_long(csv_path: Path, db_path: Path) -> pd.DataFrame:
    """
    Load the long format price table.

    Priority:
    1) CSV produced by the prices step (all_prices_long.csv)
    2) SQLite fallback (prices_us.db, table US_PRICES)

    Returns a DataFrame with columns compatible with the CSV schema:
    - date, ticker, Adj Close, Volume
    """
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        df = load_prices_long_from_sqlite(db_path)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def load_prices_long_from_sqlite(db_path: Path) -> pd.DataFrame:
    """
    Load prices from SQLite and return a DataFrame matching the CSV schema.

    Output columns:
    - date, ticker, Adj Close, Volume
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite DB: {db_path}")

    q = f"""
    SELECT
        TRADE_DATE as date,
        TICKER as ticker,
        ADJ_CLOSE as "Adj Close",
        VOLUME as "Volume"
    FROM {PRICES_TABLE}
    ORDER BY TICKER, TRADE_DATE
    """

    con = sqlite3.connect(str(db_path))
    df = pd.read_sql_query(q, con)
    con.close()
    return df


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily simple returns and log returns per ticker using adjusted close.

    Adds:
    - adj_close: numeric adjusted close
    - ret_1d: simple percentage return
    - log_ret_1d: log return
    """
    df = df.copy()

    df["adj_close"] = pd.to_numeric(df["Adj Close"], errors="coerce")

    df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()

    ratio = df.groupby("ticker")["adj_close"].shift(0) / df.groupby("ticker")["adj_close"].shift(1)
    df["log_ret_1d"] = np.log(ratio)

    return df


def save_outputs(df: pd.DataFrame, out_dir: Path) -> Path:
    """
    Save returns to CSV (and Parquet if available).

    Returns the path to the saved CSV file.
    """
    out = df[["date", "ticker", "adj_close", "ret_1d", "log_ret_1d", "Volume"]].copy()

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)

    csv_path = out_dir / "daily_returns.csv"
    out.to_csv(csv_path, index=False)

    try:
        pq_path = out_dir / "daily_returns.parquet"
        out.to_parquet(pq_path, index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {csv_path}")
    return csv_path


def create_returns_table(db_path: Path) -> None:
    """
    Create a SQLite table for returns plus indexes.

    The UNIQUE constraint prevents duplicates on re runs.
    """
    schema = f"""
    CREATE TABLE IF NOT EXISTS {RETURNS_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        ADJ_CLOSE REAL,
        RET_1D REAL,
        LOG_RET_1D REAL,
        VOLUME INTEGER,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_us_returns_ticker
    ON {RETURNS_TABLE} (TICKER);

    CREATE INDEX IF NOT EXISTS idx_us_returns_trade_date
    ON {RETURNS_TABLE} (TRADE_DATE);
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executescript(schema)
    con.commit()
    cur.close()
    con.close()


def insert_returns_sqlite(db_path: Path, df: pd.DataFrame) -> int:
    """
    Bulk insert returns into SQLite.

    Returns the number of rows attempted.
    """
    df2 = df[["date", "ticker", "adj_close", "ret_1d", "log_ret_1d", "Volume"]].copy()
    df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df2 = df2.rename(
        columns={
            "date": "TRADE_DATE",
            "ticker": "TICKER",
            "adj_close": "ADJ_CLOSE",
            "ret_1d": "RET_1D",
            "log_ret_1d": "LOG_RET_1D",
            "Volume": "VOLUME",
        }
    )

    df2["ADJ_CLOSE"] = pd.to_numeric(df2["ADJ_CLOSE"], errors="coerce")
    df2["RET_1D"] = pd.to_numeric(df2["RET_1D"], errors="coerce")
    df2["LOG_RET_1D"] = pd.to_numeric(df2["LOG_RET_1D"], errors="coerce")
    df2["VOLUME"] = pd.to_numeric(df2["VOLUME"], errors="coerce").astype("Int64")

    df2 = df2.where(pd.notnull(df2), None)

    rows = list(df2[["TICKER", "TRADE_DATE", "ADJ_CLOSE", "RET_1D", "LOG_RET_1D", "VOLUME"]].itertuples(index=False, name=None))

    insert_sql = f"""
    INSERT OR REPLACE INTO {RETURNS_TABLE}
    (TICKER, TRADE_DATE, ADJ_CLOSE, RET_1D, LOG_RET_1D, VOLUME)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executemany(insert_sql, rows)
    con.commit()
    cur.close()
    con.close()

    print(f"Inserted into SQLite: {db_path} table {RETURNS_TABLE}. Rows: {len(rows)}")
    return len(rows)


def main() -> None:
    """
    Pipeline entrypoint.

    - Loads prices (CSV first, SQLite fallback)
    - Computes returns
    - Saves CSV and Parquet outputs
    - Writes returns into SQLite
    """
    prices_csv = PRICES_RAW_DIR / "all_prices_long.csv"
    df = load_prices_long(prices_csv, PRICES_DB_PATH)

    df = add_returns(df)

    save_outputs(df, OUT_DIR)

    create_returns_table(PRICES_DB_PATH)
    insert_returns_sqlite(PRICES_DB_PATH, df)


if __name__ == "__main__":
    main()

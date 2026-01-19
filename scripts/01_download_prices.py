import sys
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import (
    TICKERS,
    START_DATE,
    END_DATE_EXCLUSIVE,
    END_DATE_INCLUSIVE,
    PRICES_RAW_DIR,
    RAW_DIR,
    SEC_MAP_DIR,
)

REQUIRED_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

PRICES_DB_PATH = RAW_DIR / "prices_us.db"
PRICES_TABLE = "US_PRICES"


def download_bundle(tickers: list[str], start: str, end_exclusive: str) -> pd.DataFrame:
    """
    Download daily OHLCV prices for all tickers in one call.

    Returns
    - A pandas DataFrame. For multiple tickers, yfinance returns MultiIndex columns:
      level 0 is ticker, level 1 is field name (Open, Close, etc).
    """
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end_exclusive,
        auto_adjust=False,
        group_by="ticker",
        progress=True,
        threads=True,
    )
    if df is None or df.empty:
        raise RuntimeError("No data returned by yfinance.")
    return df


def load_sec_map_if_available() -> pd.DataFrame:
    """
    Load the SEC ticker to CIK map if it exists.

    This is optional so the prices step can run before the SEC map step.
    If the file does not exist, return an empty DataFrame with expected columns.
    """
    p = SEC_MAP_DIR / "sec_ticker_cik_selected.csv"
    if not p.exists():
        return pd.DataFrame(columns=["ticker", "cik10", "title"])

    df = pd.read_csv(p, dtype=str)
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["cik10"] = df["cik10"].astype(str).str.zfill(10)
    df["title"] = df["title"].astype(str)
    return df[["ticker", "cik10", "title"]].drop_duplicates(subset=["ticker"]).reset_index(drop=True)


def normalise_one(df_raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Convert the downloaded yfinance output into a long table for one ticker.

    Output columns mirror the Brazilian pattern (one row per day per ticker),
    and keep the original yfinance column names for compatibility with later steps.
    """
    if isinstance(df_raw.columns, pd.MultiIndex):
        if ticker not in df_raw.columns.get_level_values(0):
            raise KeyError(f"{ticker} missing from downloaded data.")
        df = df_raw[ticker].copy()
    else:
        df = df_raw.copy()

    df = df.reset_index()
    df = df.rename(columns={"Date": "date"})
    df["ticker"] = ticker

    keep = ["date"] + [c for c in REQUIRED_COLS if c in df.columns] + ["ticker"]
    df = df[keep]

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    df = df.dropna(subset=[c for c in REQUIRED_COLS if c in df.columns], how="all")

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df


def build_long_table(df_raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """
    Build the combined long table for all tickers.

    Also writes one CSV per ticker, consistent with your current behaviour.
    """
    parts: list[pd.DataFrame] = []

    for t in tickers:
        try:
            df_t = normalise_one(df_raw, t)
        except Exception as e:
            print(f"Skipping {t}. {e}")
            continue

        df_t.to_csv(PRICES_RAW_DIR / f"{t}.csv", index=False)
        parts.append(df_t)
        print(f"Saved {t}. {len(df_t)} rows")

    if not parts:
        raise RuntimeError("No ticker files saved.")

    df_long = pd.concat(parts, ignore_index=True).sort_values(["date", "ticker"]).reset_index(drop=True)
    return df_long


def save_csv_and_parquet(df_long: pd.DataFrame) -> None:
    """
    Save combined long and wide price tables to disk.

    Keeps the same filenames you already use so downstream scripts do not break.
    """
    all_long_csv = PRICES_RAW_DIR / "all_prices_long.csv"
    df_long.to_csv(all_long_csv, index=False)

    wide_adj = df_long.pivot(index="date", columns="ticker", values="Adj Close")
    wide_adj.to_csv(PRICES_RAW_DIR / "all_prices_wide_adj_close.csv")

    try:
        df_long.to_parquet(PRICES_RAW_DIR / "all_prices_long.parquet", index=False)
        wide_adj.to_parquet(PRICES_RAW_DIR / "all_prices_wide_adj_close.parquet")
    except Exception as e:
        print(f"Parquet not written. {e}")


def create_prices_table(db_path: Path) -> None:
    """
    Create the SQLite table and indexes, similar to the Brazilian prices.db approach.

    We store:
    - ticker
    - date (YYYY-MM-DD text)
    - OHLC, Adj Close, Volume
    - cik10 and company_title if available
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    schema = f"""
    CREATE TABLE IF NOT EXISTS {PRICES_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        CIK10 TEXT,
        COMPANY_TITLE TEXT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        OPEN REAL,
        HIGH REAL,
        LOW REAL,
        CLOSE REAL,
        ADJ_CLOSE REAL,
        VOLUME INTEGER,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_us_prices_ticker
    ON {PRICES_TABLE} (TICKER);

    CREATE INDEX IF NOT EXISTS idx_us_prices_trade_date
    ON {PRICES_TABLE} (TRADE_DATE);

    CREATE INDEX IF NOT EXISTS idx_us_prices_cik10
    ON {PRICES_TABLE} (CIK10);
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executescript(schema)
    con.commit()
    cur.close()
    con.close()


def insert_prices_sqlite(
    db_path: Path,
    df_long: pd.DataFrame,
    sec_map: Optional[pd.DataFrame] = None,
) -> None:
    """
    Bulk insert the long price table into SQLite.

    If sec_map is provided and contains ticker, cik10, title,
    we merge it into the long table before insert.
    """
    df = df_long.copy()

    if sec_map is not None and not sec_map.empty:
        df = df.merge(sec_map, on="ticker", how="left")
        df = df.rename(columns={"title": "company_title"})
    else:
        df["cik10"] = None
        df["company_title"] = None

    df = df.rename(
        columns={
            "date": "TRADE_DATE",
            "ticker": "TICKER",
            "Open": "OPEN",
            "High": "HIGH",
            "Low": "LOW",
            "Close": "CLOSE",
            "Adj Close": "ADJ_CLOSE",
            "Volume": "VOLUME",
            "cik10": "CIK10",
            "company_title": "COMPANY_TITLE",
        }
    )

    cols = ["CIK10", "COMPANY_TITLE", "TICKER", "TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
    for c in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["VOLUME"] = pd.to_numeric(df["VOLUME"], errors="coerce").astype("Int64")
    df = df.where(pd.notnull(df), None)

    rows = list(df[cols].itertuples(index=False, name=None))

    insert_sql = f"""
    INSERT OR REPLACE INTO {PRICES_TABLE}
    (CIK10, COMPANY_TITLE, TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executemany(insert_sql, rows)
    con.commit()
    cur.close()
    con.close()

    print(f"Inserted into SQLite: {db_path} table {PRICES_TABLE}. Rows: {len(rows)}")


def main() -> None:
    """
    Pipeline entrypoint.

    - Downloads prices
    - Writes CSV and Parquet outputs (per ticker, long, wide)
    - Creates and populates SQLite table, similar to Brazilian prices.db pattern
    """
    PRICES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    tickers = [t.strip().upper() for t in TICKERS if t.strip()]
    df_raw = download_bundle(tickers, START_DATE, END_DATE_EXCLUSIVE)

    df_long = build_long_table(df_raw, tickers)
    save_csv_and_parquet(df_long)

    sec_map = load_sec_map_if_available()
    create_prices_table(PRICES_DB_PATH)
    insert_prices_sqlite(PRICES_DB_PATH, df_long, sec_map)

    print(f"Done. {START_DATE} to {END_DATE_INCLUSIVE} inclusive")


if __name__ == "__main__":
    main()

# What this script does:
# - Pulls and filters prices for selected tickers across the period.
# - Stores per ticker CSVs and combined CSVs.
# - Creates a SQLite table plus indexes.
# - Bulk inserts all rows into SQLite.
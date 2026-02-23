#!/usr/bin/env python3
"""
Create consolidated fundamental_analysis.db for Thiago-style openai-agent experiments (US data).

Inputs:
- data/raw/prices_us.db (US_PRICES, optionally US_RETURNS)
- data/raw/sec/sec_companyfacts.db (SEC_COMPANYFACTS optional)
- data/raw/sec/companyfacts/companyfacts_2022_2025.csv or *_companyfacts.csv (fallback for SEC)
- data/processed/panel/panel.db (US_DAILY_PANEL, US_MONTHLY_PANEL, US_FUNDAMENTALS_WIDE_BY_FILED)

Output:
- data/processed/mcp/fundamental_analysis.db
"""

from __future__ import annotations

import sqlite3
import shutil
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PRICES_DB = PROJECT_ROOT / "data" / "raw" / "prices_us.db"
SEC_DB = PROJECT_ROOT / "data" / "raw" / "sec" / "sec_companyfacts.db"
PANEL_DB = PROJECT_ROOT / "data" / "processed" / "panel" / "panel.db"

SEC_COMPANYFACTS_DIR = PROJECT_ROOT / "data" / "raw" / "sec" / "companyfacts"
SEC_COMPANYFACTS_CSV = SEC_COMPANYFACTS_DIR / "companyfacts_2022_2025.csv"

OUT_DIR = PROJECT_ROOT / "data" / "processed" / "mcp"
OUT_DB = OUT_DIR / "fundamental_analysis.db"

CHUNKSIZE = 200_000


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    """
    Open SQLite DB in read-only mode to avoid accidental creation of empty databases.
    """
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def list_tables(db_path: Path) -> list[str]:
    """
    List tables in a SQLite database.
    """
    try:
        with _connect_readonly(db_path) as con:
            rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"Failed to list tables in {db_path}. Error: {e}") from e


def normalise_sec_companyfacts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise SEC CompanyFacts columns to the canonical SEC_COMPANYFACTS schema.
    """
    rename_map = {
        "ticker": "TICKER",
        "cik": "CIK",
        "concept": "CONCEPT",
        "unit": "UNIT",
        "value": "VALUE_REAL",
        "val": "VALUE_REAL",
        "form": "FORM",
        "fy": "FY",
        "fp": "FP",
        "start": "START_DATE",
        "end": "END_DATE",
        "filed": "FILED_DATE",
        "accn": "ACCN",
        "frame": "FRAME",
    }
    required_cols = [
        "TICKER",
        "CIK",
        "CONCEPT",
        "UNIT",
        "VALUE_REAL",
        "FORM",
        "FY",
        "FP",
        "START_DATE",
        "END_DATE",
        "FILED_DATE",
        "ACCN",
        "FRAME",
    ]

    df = df.rename(columns=rename_map)
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df = df[required_cols].copy()
    df["VALUE_REAL"] = pd.to_numeric(df["VALUE_REAL"], errors="coerce")
    df["TICKER"] = df["TICKER"].astype(str).str.upper().str.strip()
    return df


def copy_table_from_db(src_path: Path, table: str, dst_con: sqlite3.Connection) -> None:
    """
    Copy a table from a source SQLite DB into destination DB using chunked reads.
    """
    print(f"Copying table {table} from {src_path}")

    with _connect_readonly(src_path) as src_con:
        chunks = pd.read_sql_query(f'SELECT * FROM "{table}"', src_con, chunksize=CHUNKSIZE)
        first = True
        total = 0
        for chunk in chunks:
            chunk.to_sql(table, dst_con, if_exists="replace" if first else "append", index=False)
            total += len(chunk)
            first = False

    print(f"  inserted {total} rows into {table}")


def copy_sec_companyfacts(dst_con: sqlite3.Connection) -> None:
    """
    Copy SEC_COMPANYFACTS into destination DB.
    Prefer the SEC SQLite table if present, otherwise fall back to CSV.
    """
    sec_tables = set(list_tables(SEC_DB)) if SEC_DB.exists() else set()

    if "SEC_COMPANYFACTS" in sec_tables:
        copy_table_from_db(SEC_DB, "SEC_COMPANYFACTS", dst_con)
        return

    print(
        "SEC_COMPANYFACTS not found in sec_companyfacts.db "
        f"(found: {sorted(sec_tables) if sec_tables else 'none'}). Falling back to CSV."
    )

    if SEC_COMPANYFACTS_CSV.exists():
        facts_df = pd.read_csv(SEC_COMPANYFACTS_CSV, low_memory=False)
    else:
        csv_files = sorted(SEC_COMPANYFACTS_DIR.glob("*_companyfacts.csv"))
        if not csv_files:
            raise RuntimeError(f"No fallback CSVs found in {SEC_COMPANYFACTS_DIR}")
        facts_df = pd.concat((pd.read_csv(p, low_memory=False) for p in csv_files), ignore_index=True)

    facts_df = normalise_sec_companyfacts(facts_df)
    facts_df.to_sql("SEC_COMPANYFACTS", dst_con, if_exists="replace", index=False)
    print(f"  inserted {len(facts_df)} rows into SEC_COMPANYFACTS")


def create_indexes(dst_con: sqlite3.Connection) -> None:
    """
    Create indexes needed for fast agent and workflow SQL queries.
    """
    # Avoid temp-file writes during index builds in restricted environments.
    dst_con.execute("PRAGMA temp_store=MEMORY")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_concept_end ON SEC_COMPANYFACTS (TICKER, CONCEPT, END_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_end ON SEC_COMPANYFACTS (TICKER, END_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON US_PRICES (TICKER, TRADE_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_returns_ticker_date ON US_RETURNS (TICKER, TRADE_DATE)")


def main() -> None:
    """
    Build consolidated DB by copying panel.db as base, then writing SEC and price tables into it.
    """
    for p in [PRICES_DB, PANEL_DB]:
        if not p.exists():
            raise FileNotFoundError(p)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUT_DB.exists():
        OUT_DB.unlink()

    shutil.copyfile(PANEL_DB, OUT_DB)

    dst_con = sqlite3.connect(str(OUT_DB))
    try:
        copy_sec_companyfacts(dst_con)
        copy_table_from_db(PRICES_DB, "US_PRICES", dst_con)

        try:
            copy_table_from_db(PRICES_DB, "US_RETURNS", dst_con)
        except Exception:
            print("US_RETURNS not found, skipping")

        # Commit copied rows first, then build indexes.
        dst_con.commit()

        try:
            create_indexes(dst_con)
        except sqlite3.OperationalError as e:
            # Some environments fail index creation on a long-lived write transaction.
            # Retry once on a fresh connection.
            print(f"Index creation retry after error: {e}")
            dst_con.close()
            dst_con = sqlite3.connect(str(OUT_DB))
            create_indexes(dst_con)

        dst_con.commit()

        final_tables = list_tables(OUT_DB)
        print("Final tables:", final_tables)
        print("Consolidated DB created at:", OUT_DB)

    finally:
        dst_con.close()


if __name__ == "__main__":
    main()

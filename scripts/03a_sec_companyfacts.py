#!/usr/bin/env python3
"""
Build SEC_COMPANYFACTS SQLite database from processed SEC CSV files.

Expected input:
data/raw/sec/companyfacts/*_companyfacts.csv

Output:
data/raw/sec/sec_companyfacts.db

The table schema matches openai-agent US workflow requirements.
"""

from __future__ import annotations

import sqlite3
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CSV_DIR = PROJECT_ROOT / "data" / "raw" / "sec" / "companyfacts"
OUT_DB = PROJECT_ROOT / "data" / "raw" / "sec" / "sec_companyfacts.db"


def create_schema(con: sqlite3.Connection) -> None:
    """
    Create SEC_COMPANYFACTS table.
    """

    con.execute("DROP TABLE IF EXISTS SEC_COMPANYFACTS")

    con.execute(
        """
        CREATE TABLE SEC_COMPANYFACTS (
            TICKER TEXT,
            CIK TEXT,
            CONCEPT TEXT,
            UNIT TEXT,
            VALUE_REAL REAL,
            FORM TEXT,
            FY TEXT,
            FP TEXT,
            START_DATE TEXT,
            END_DATE TEXT,
            FILED_DATE TEXT,
            ACCN TEXT,
            FRAME TEXT
        )
        """
    )

    con.commit()


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure expected column names exist.
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

    df = df.rename(columns=rename_map)

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

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    return df[required_cols]


def main() -> None:

    if not CSV_DIR.exists():
        raise FileNotFoundError(f"Missing directory: {CSV_DIR}")

    csv_files = sorted(CSV_DIR.glob("*_companyfacts.csv"))

    if not csv_files:
        raise RuntimeError("No *_companyfacts.csv files found.")

    print(f"Found {len(csv_files)} CSV files")

    if OUT_DB.exists():
        OUT_DB.unlink()

    con = sqlite3.connect(str(OUT_DB))

    try:
        create_schema(con=con)

        total_rows = 0

        for file_path in csv_files:
            print(f"Processing {file_path.name}")

            df = pd.read_csv(file_path, low_memory=False)

            df = normalise_columns(df=df)

            df["VALUE_REAL"] = pd.to_numeric(df["VALUE_REAL"], errors="coerce")

            df.to_sql(
                "SEC_COMPANYFACTS",
                con,
                if_exists="append",
                index=False,
            )

            total_rows += len(df)

        con.commit()

        cur = con.execute("SELECT COUNT(*) FROM SEC_COMPANYFACTS")
        count = cur.fetchone()[0]

        print(f"Inserted {count} rows")

        if count == 0:
            raise RuntimeError("SEC_COMPANYFACTS table is empty.")

        # Create useful indexes
        con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_concept_end ON SEC_COMPANYFACTS (TICKER, CONCEPT, END_DATE)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_filed ON SEC_COMPANYFACTS (TICKER, FILED_DATE)")
        con.commit()

        print(f"Database written to: {OUT_DB}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
import sys
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import PROCESSED_DIR

PANEL_DIR = PROCESSED_DIR / "panel"
SPLITS_DIR = PROCESSED_DIR / "splits"


def build_monthly_panel(daily_panel_csv: Path) -> pd.DataFrame:
    """
    Build a monthly panel by selecting the first trading day of each month for each ticker.

    Input: daily panel produced by scripts/04b_align_fundamentals.py
    Output: one row per (ticker, year-month), using the earliest available trading date in that month.
    """
    df = pd.read_csv(daily_panel_csv, low_memory=False)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    df = df.dropna(subset=["ticker", "date"]).sort_values(["ticker", "date"]).reset_index(drop=True)

    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    first_rows = (
        df.groupby(["ticker", "year_month"], as_index=False)
        .first()
        .drop(columns=["year_month"])
        .reset_index(drop=True)
    )

    return first_rows


def save_monthly_outputs(monthly: pd.DataFrame, out_csv: Path) -> None:
    """
    Save the monthly panel CSV (and parquet when possible).
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    monthly.to_csv(out_csv, index=False)

    try:
        monthly.to_parquet(out_csv.with_suffix(".parquet"), index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {out_csv}")


def save_monthly_to_sqlite(monthly: pd.DataFrame, db_path: Path, table_name: str) -> None:
    """
    Save the monthly panel into SQLite for MCP tool querying.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path))
    try:
        monthly.to_sql(table_name, con, if_exists="replace", index=False)
        cur = con.cursor()
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ticker_date ON {table_name} (ticker, date)")
        con.commit()
    finally:
        con.close()

    print(f"Inserted into SQLite: {db_path} table {table_name}. Rows: {len(monthly)}")


def main() -> None:
    daily_csv = PANEL_DIR / "daily_panel_prices_returns_fundamentals.csv"
    if not daily_csv.exists():
        raise FileNotFoundError(f"Missing daily panel CSV: {daily_csv}")

    monthly = build_monthly_panel(daily_csv)

    out_csv = PANEL_DIR / "monthly_panel_prices_returns_fundamentals.csv"
    save_monthly_outputs(monthly, out_csv)

    db_path = PANEL_DIR / "panel.db"
    save_monthly_to_sqlite(monthly, db_path, "US_MONTHLY_PANEL")

    print("Done")


if __name__ == "__main__":
    main()

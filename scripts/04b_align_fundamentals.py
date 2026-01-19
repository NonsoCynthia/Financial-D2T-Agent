import sys
import sqlite3
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import PROCESSED_DIR, RAW_DIR

FACTS_DIR = RAW_DIR / "sec" / "companyfacts"
RETURNS_DIR = PROCESSED_DIR / "prices"
OUT_DIR = PROCESSED_DIR / "panel"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRICES_DB_PATH = RAW_DIR / "prices_us.db"
RETURNS_TABLE = "US_RETURNS"

FACTS_DB_PATH = RAW_DIR / "sec" / "sec_companyfacts.db"
FACTS_TABLE = "SEC_COMPANYFACTS"

PANEL_DB_PATH = OUT_DIR / "panel.db"
PANEL_TABLE = "US_DAILY_PANEL"
WIDE_TABLE = "US_FUNDAMENTALS_WIDE_BY_FILED"

CONCEPTS = [
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
]


def load_daily_returns() -> pd.DataFrame:
    """
    Load daily returns created in Step 04a.

    Priority:
    1) CSV output from Step 04a
    2) SQLite fallback (prices_us.db, table US_RETURNS)

    Returns a DataFrame with columns:
    date, ticker, adj_close, ret_1d, log_ret_1d, Volume
    """
    p = RETURNS_DIR / "daily_returns.csv"

    if p.exists():
        df = pd.read_csv(p)
    else:
        df = load_daily_returns_from_sqlite(PRICES_DB_PATH)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def load_daily_returns_from_sqlite(db_path: Path) -> pd.DataFrame:
    """
    Load daily returns from SQLite.

    This allows the pipeline to run even if the CSV is missing.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite DB: {db_path}")

    q = f"""
    SELECT
        TRADE_DATE AS date,
        TICKER AS ticker,
        ADJ_CLOSE AS adj_close,
        RET_1D AS ret_1d,
        LOG_RET_1D AS log_ret_1d,
        VOLUME AS Volume
    FROM {RETURNS_TABLE}
    ORDER BY TICKER, TRADE_DATE
    """

    con = sqlite3.connect(str(db_path))
    df = pd.read_sql_query(q, con)
    con.close()

    return df


def load_companyfacts() -> pd.DataFrame:
    """
    Load CompanyFacts extracted in Step 03a.

    Priority:
    1) Combined CSV output from Step 03a
    2) SQLite fallback (sec_companyfacts.db, table SEC_COMPANYFACTS)

    Returns a long DataFrame with columns:
    ticker, concept, unit, value, form, fy, fp, end, filed, accn
    """
    p = FACTS_DIR / "companyfacts_2022_2025.csv"

    if p.exists():
        df = pd.read_csv(p, dtype={"ticker": str, "concept": str, "unit": str}, low_memory=False)
    else:
        df = load_companyfacts_from_sqlite(FACTS_DB_PATH)

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["concept"] = df["concept"].astype(str).str.strip()
    df["unit"] = df["unit"].astype(str).str.strip()

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce")

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["ticker", "concept", "end", "filed"])
    df = df.sort_values(["ticker", "concept", "filed", "end"]).reset_index(drop=True)
    return df


def load_companyfacts_from_sqlite(db_path: Path) -> pd.DataFrame:
    """
    Load CompanyFacts from SQLite and return a DataFrame matching the CSV schema used downstream.

    The SEC_COMPANYFACTS table stores numeric values in VALUE_REAL.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Missing SQLite DB: {db_path}")

    q = f"""
    SELECT
        TICKER AS ticker,
        CONCEPT AS concept,
        UNIT AS unit,
        VALUE_REAL AS value,
        FORM AS form,
        FY AS fy,
        FP AS fp,
        END_DATE AS end,
        FILED_DATE AS filed,
        ACCN AS accn
    FROM {FACTS_TABLE}
    ORDER BY TICKER, CONCEPT, FILED_DATE, END_DATE
    """

    con = sqlite3.connect(str(db_path))
    df = pd.read_sql_query(q, con)
    con.close()

    return df


def filter_concepts(df: pd.DataFrame, concepts: list[str]) -> pd.DataFrame:
    """
    Keep only a stable set of concepts for the feature table.

    This prevents the panel from exploding into too many columns and keeps it consistent across tickers.
    """
    return df[df["concept"].isin(concepts)].copy()


def choose_single_unit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Many concepts appear with multiple units per ticker, for example USD, USD/shares, shares.

    This chooses one unit per (ticker, concept) by taking the most frequent unit in the data.
    The output keeps only the selected unit rows per (ticker, concept).
    """
    counts = (
        df.groupby(["ticker", "concept", "unit"])
        .size()
        .reset_index(name="n")
        .sort_values(["ticker", "concept", "n"], ascending=[True, True, False])
    )

    chosen = counts.drop_duplicates(subset=["ticker", "concept"])[["ticker", "concept", "unit"]]
    out = df.merge(chosen, on=["ticker", "concept", "unit"], how="inner")
    return out


def keep_latest_per_filed_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given (ticker, concept, filed date), multiple rows can exist.

    We keep the last row by end date so each (ticker, concept, filed) has a single value,
    which makes the pivot stable.
    """
    df = df.sort_values(["ticker", "concept", "filed", "end"])
    df = df.drop_duplicates(subset=["ticker", "concept", "filed"], keep="last")
    return df


def fundamentals_wide_by_filed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert long facts into a wide table keyed by (ticker, filed).

    Each concept becomes a column. Values are the most recent values at that filing date.
    """
    wide = (
        df.pivot_table(
            index=["ticker", "filed"],
            columns="concept",
            values="value",
            aggfunc="last",
        )
        .reset_index()
    )

    wide = wide.sort_values(["ticker", "filed"]).reset_index(drop=True)
    return wide


def merge_fundamentals_daily(daily: pd.DataFrame, wide_facts: pd.DataFrame) -> pd.DataFrame:
    """
    Merge fundamentals into daily returns using a backward asof join on filed date.

    For each trading day, attach the latest fundamentals filed on or before that day.
    If a ticker has no fundamentals, output still contains the same columns with missing values.
    """
    base_cols = list(daily.columns)

    out_parts = []
    for t, df_t in daily.groupby("ticker", sort=False):
        df_t = df_t.sort_values("date").copy()

        facts_t = wide_facts[wide_facts["ticker"] == t].copy()
        facts_t = facts_t.sort_values("filed") if not facts_t.empty else facts_t

        if facts_t.empty:
            df_t["filed"] = pd.NaT
            for c in CONCEPTS:
                df_t[c] = np.nan
            df_t = df_t.reindex(columns=base_cols + ["filed"] + CONCEPTS)
            out_parts.append(df_t)
            continue

        merged = pd.merge_asof(
            df_t,
            facts_t.drop(columns=["ticker"]),
            left_on="date",
            right_on="filed",
            direction="backward",
            allow_exact_matches=True,
        )

        for c in CONCEPTS:
            if c not in merged.columns:
                merged[c] = np.nan

        merged = merged.reindex(columns=base_cols + ["filed"] + CONCEPTS)
        out_parts.append(merged)

    out = pd.concat(out_parts, ignore_index=True)
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def save_outputs(panel: pd.DataFrame, wide_facts: pd.DataFrame) -> None:
    """
    Save the daily panel dataset and the wide fundamentals table to CSV and Parquet.
    """
    panel_csv = OUT_DIR / "daily_panel_prices_returns_fundamentals.csv"
    panel.to_csv(panel_csv, index=False)

    facts_csv = OUT_DIR / "fundamentals_wide_by_filed.csv"
    wide_facts.to_csv(facts_csv, index=False)

    try:
        panel.to_parquet(OUT_DIR / "daily_panel_prices_returns_fundamentals.parquet", index=False)
        wide_facts.to_parquet(OUT_DIR / "fundamentals_wide_by_filed.parquet", index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {panel_csv}")
    print(f"Saved: {facts_csv}")


def create_panel_tables(db_path: Path) -> None:
    """
    Create SQLite tables for:
    - daily panel (prices, returns, fundamentals aligned daily)
    - fundamentals wide by filed date

    Indexes are added for fast queries by ticker and date.
    """
    schema = f"""
    CREATE TABLE IF NOT EXISTS {PANEL_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        ADJ_CLOSE REAL,
        RET_1D REAL,
        LOG_RET_1D REAL,
        VOLUME INTEGER,
        FILED_DATE TEXT,
        Assets REAL,
        Liabilities REAL,
        StockholdersEquity REAL,
        Revenues REAL,
        NetIncomeLoss REAL,
        OperatingIncomeLoss REAL,
        EarningsPerShareBasic REAL,
        CommonStockSharesOutstanding REAL,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_panel_ticker
    ON {PANEL_TABLE} (TICKER);

    CREATE INDEX IF NOT EXISTS idx_panel_trade_date
    ON {PANEL_TABLE} (TRADE_DATE);

    CREATE TABLE IF NOT EXISTS {WIDE_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        FILED_DATE TEXT NOT NULL,
        Assets REAL,
        Liabilities REAL,
        StockholdersEquity REAL,
        Revenues REAL,
        NetIncomeLoss REAL,
        OperatingIncomeLoss REAL,
        EarningsPerShareBasic REAL,
        CommonStockSharesOutstanding REAL,
        UNIQUE(TICKER, FILED_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_wide_ticker
    ON {WIDE_TABLE} (TICKER);

    CREATE INDEX IF NOT EXISTS idx_wide_filed_date
    ON {WIDE_TABLE} (FILED_DATE);
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executescript(schema)
    con.commit()
    cur.close()
    con.close()


def insert_panel_sqlite(db_path: Path, panel: pd.DataFrame, wide: pd.DataFrame) -> None:
    """
    Bulk insert the panel and the wide fundamentals tables into SQLite.

    Values are inserted as NULL when missing to keep SQLite consistent.
    """
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()

    panel2 = panel.copy()
    panel2["date"] = pd.to_datetime(panel2["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel2["filed"] = pd.to_datetime(panel2["filed"], errors="coerce").dt.strftime("%Y-%m-%d")

    panel2 = panel2.rename(
        columns={
            "ticker": "TICKER",
            "date": "TRADE_DATE",
            "adj_close": "ADJ_CLOSE",
            "ret_1d": "RET_1D",
            "log_ret_1d": "LOG_RET_1D",
            "Volume": "VOLUME",
            "filed": "FILED_DATE",
        }
    )

    panel_cols = ["TICKER", "TRADE_DATE", "ADJ_CLOSE", "RET_1D", "LOG_RET_1D", "VOLUME", "FILED_DATE"] + CONCEPTS
    panel2 = panel2[panel_cols].where(pd.notnull(panel2[panel_cols]), None)

    insert_panel = f"""
    INSERT OR REPLACE INTO {PANEL_TABLE}
    (TICKER, TRADE_DATE, ADJ_CLOSE, RET_1D, LOG_RET_1D, VOLUME, FILED_DATE,
     Assets, Liabilities, StockholdersEquity, Revenues, NetIncomeLoss, OperatingIncomeLoss,
     EarningsPerShareBasic, CommonStockSharesOutstanding)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cur.executemany(insert_panel, list(panel2.itertuples(index=False, name=None)))

    wide2 = wide.copy()
    wide2["filed"] = pd.to_datetime(wide2["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    wide2 = wide2.rename(columns={"ticker": "TICKER", "filed": "FILED_DATE"})

    wide_cols = ["TICKER", "FILED_DATE"] + CONCEPTS
    for c in CONCEPTS:
        if c not in wide2.columns:
            wide2[c] = np.nan
    wide2 = wide2[wide_cols].where(pd.notnull(wide2[wide_cols]), None)

    insert_wide = f"""
    INSERT OR REPLACE INTO {WIDE_TABLE}
    (TICKER, FILED_DATE,
     Assets, Liabilities, StockholdersEquity, Revenues, NetIncomeLoss, OperatingIncomeLoss,
     EarningsPerShareBasic, CommonStockSharesOutstanding)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cur.executemany(insert_wide, list(wide2.itertuples(index=False, name=None)))

    con.commit()
    cur.close()
    con.close()

    print(f"Inserted into SQLite: {db_path} tables {PANEL_TABLE}, {WIDE_TABLE}")


def main() -> None:
    """
    Build the daily panel dataset:
    - load daily returns
    - load CompanyFacts and filter concepts
    - pivot fundamentals to wide by filed date
    - merge fundamentals into daily data using asof join
    - save outputs to CSV and Parquet
    - save outputs into SQLite tables
    """
    daily = load_daily_returns()
    facts = load_companyfacts()

    facts = filter_concepts(facts, CONCEPTS)
    facts = choose_single_unit(facts)
    facts = keep_latest_per_filed_date(facts)

    wide = fundamentals_wide_by_filed(facts)
    panel = merge_fundamentals_daily(daily, wide)

    save_outputs(panel, wide)

    create_panel_tables(PANEL_DB_PATH)
    insert_panel_sqlite(PANEL_DB_PATH, panel, wide)

    print("Done")


if __name__ == "__main__":
    main()

#  You should get:
#  - data/processed/panel/daily_panel_prices_returns_fundamentals.csv
#  - data/processed/panel/fundamentals_wide_by_filed.csv
#  - data/processed/panel/panel.db containing tables: 
#     - US_DAILY_PANEL and 
#     - US_FUNDAMENTALS_WIDE_BY_FILED
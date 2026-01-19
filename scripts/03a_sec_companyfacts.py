import sys
import time
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import SEC_HEADERS_BASE, SEC_MAP_DIR, RAW_DIR

START_END = date(2022, 1, 1)
END_END = date(2025, 12, 31)

OUT_DIR = RAW_DIR / "sec" / "companyfacts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = RAW_DIR / "sec" / "sec_companyfacts.db"
TABLE = "SEC_COMPANYFACTS"


def parse_ymd(s: str) -> Optional[date]:
    """
    Parse a date string in YYYY-MM-DD format into a date object.

    Returns None if parsing fails.
    """
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def fetch_json(
    session: requests.Session,
    url: str,
    headers: dict,
    retries: int = 6,
) -> Optional[dict]:
    """
    Fetch a JSON payload from the SEC endpoint with basic retry logic.

    Behaviour:
    - Retries on common transient failures (429, 5xx).
    - Returns None on 404.
    - Raises on repeated failures after retries.
    """
    last = None
    for i in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=40)

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.5 + i * 0.7)
                continue

            if r.status_code == 404:
                return None

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last = e
            time.sleep(0.5 + i * 0.7)

    raise RuntimeError(f"Failed to fetch {url}. Last error: {last}")


def load_selected_map() -> pd.DataFrame:
    """
    Load the selected ticker to CIK mapping produced by 02_sec_ticker_cik.py.

    This file is required for the CompanyFacts step.
    """
    p = SEC_MAP_DIR / "sec_ticker_cik_selected.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}. Run step sec_map first.")

    df = pd.read_csv(p, dtype=str)

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["cik10"] = df["cik10"].astype(str).str.zfill(10)

    if "title" in df.columns:
        df["title"] = df["title"].astype(str)
    else:
        df["title"] = ""

    df = df[["ticker", "cik10", "title"]].drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    return df


def extract_companyfacts(ticker: str, cik10: str, company_title: str, data: dict) -> pd.DataFrame:
    """
    Extract quarterly or annual XBRL facts from the SEC CompanyFacts JSON.

    Steps:
    - Focus on US GAAP concepts when available, otherwise fall back to IFRS.
    - Keep only 10-K and 10-Q filings.
    - Keep only rows where the 'end' date is within START_END and END_END.
    - Return a tidy long table with one row per (concept, unit, period, value).
    """
    facts_block = data.get("facts", {})
    facts = facts_block.get("us-gaap", {})
    if not facts:
        facts = facts_block.get("ifrs-full", {})

    rows = []

    for concept, concept_data in facts.items():
        units = concept_data.get("units", {})

        for unit, items in units.items():
            for it in items:
                end = it.get("end")
                end_d = parse_ymd(end) if end else None

                if end_d is None:
                    continue

                if not (START_END <= end_d <= END_END):
                    continue
                
                form = it.get("form")
                #NIO is a foreign company so form not in {10-K, 10-Q} (foreign issuers often use 20-F, 6-K)
                allowed_forms = {"10-K", "10-Q", "20-F", "40-F", "6-K"}
                if form not in allowed_forms:
                    continue

                rows.append(
                    {
                        "ticker": ticker,
                        "cik10": cik10,
                        "company_title": company_title,
                        "concept": concept,
                        "unit": unit,
                        "value": it.get("val"),
                        "form": form,
                        "fy": it.get("fy"),
                        "fp": it.get("fp"),
                        "start": it.get("start"),
                        "end": end,
                        "filed": it.get("filed"),
                        "accn": it.get("accn"),
                        "frame": it.get("frame"),
                    }
                )

    cols = [
        "ticker",
        "cik10",
        "company_title",
        "concept",
        "unit",
        "value",
        "form",
        "fy",
        "fp",
        "start",
        "end",
        "filed",
        "accn",
        "frame",
    ]

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    df["end"] = pd.to_datetime(df["end"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["start"] = pd.to_datetime(df["start"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce").dt.strftime("%Y-%m-%d")

    df = df.sort_values(["ticker", "end", "concept", "unit"]).reset_index(drop=True)
    return df


def save_outputs(df_all: pd.DataFrame) -> None:
    """
    Save the aggregated CompanyFacts dataset to CSV and Parquet.

    This preserves your existing outputs so downstream scripts keep working.
    """
    out_csv = OUT_DIR / "companyfacts_2022_2025.csv"
    df_all.to_csv(out_csv, index=False)

    try:
        out_parquet = OUT_DIR / "companyfacts_2022_2025.parquet"
        df_all.to_parquet(out_parquet, index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {out_csv}")


def create_companyfacts_table(db_path: Path) -> None:
    """
    Create the SQLite table and indexes for CompanyFacts.

    Design notes:
    - We allow NULLs because SEC facts can be sparse.
    - We store value as both text and numeric when possible.
    - We add a uniqueness constraint to prevent duplication on re-runs.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    schema = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        CIK10 TEXT NOT NULL,
        COMPANY_TITLE TEXT,
        CONCEPT TEXT NOT NULL,
        UNIT TEXT NOT NULL,
        VALUE_TEXT TEXT,
        VALUE_REAL REAL,
        FORM TEXT,
        FY INTEGER,
        FP TEXT,
        START_DATE TEXT,
        END_DATE TEXT,
        FILED_DATE TEXT,
        ACCN TEXT,
        FRAME TEXT,
        UNIQUE(CIK10, CONCEPT, UNIT, END_DATE, ACCN, FORM, FP, FY)
    );

    CREATE INDEX IF NOT EXISTS idx_companyfacts_ticker
    ON {TABLE} (TICKER);

    CREATE INDEX IF NOT EXISTS idx_companyfacts_cik10
    ON {TABLE} (CIK10);

    CREATE INDEX IF NOT EXISTS idx_companyfacts_end_date
    ON {TABLE} (END_DATE);

    CREATE INDEX IF NOT EXISTS idx_companyfacts_concept
    ON {TABLE} (CONCEPT);
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executescript(schema)
    con.commit()
    cur.close()
    con.close()


def insert_companyfacts_sqlite(db_path: Path, df: pd.DataFrame) -> int:
    """
    Bulk insert CompanyFacts rows into SQLite.

    Returns the number of rows attempted for insertion.
    """
    if df is None or df.empty:
        return 0

    df2 = df.copy()

    df2["VALUE_TEXT"] = df2["value"].astype(str)
    df2["VALUE_REAL"] = pd.to_numeric(df2["value"], errors="coerce")

    df2 = df2.rename(
        columns={
            "ticker": "TICKER",
            "cik10": "CIK10",
            "company_title": "COMPANY_TITLE",
            "concept": "CONCEPT",
            "unit": "UNIT",
            "form": "FORM",
            "fy": "FY",
            "fp": "FP",
            "start": "START_DATE",
            "end": "END_DATE",
            "filed": "FILED_DATE",
            "accn": "ACCN",
            "frame": "FRAME",
        }
    )

    cols = [
        "TICKER",
        "CIK10",
        "COMPANY_TITLE",
        "CONCEPT",
        "UNIT",
        "VALUE_TEXT",
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

    df2 = df2[cols]

    df2["FY"] = pd.to_numeric(df2["FY"], errors="coerce").astype("Int64")
    df2 = df2.where(pd.notnull(df2), None)

    rows = list(df2.itertuples(index=False, name=None))

    insert_sql = f"""
    INSERT OR REPLACE INTO {TABLE}
    (TICKER, CIK10, COMPANY_TITLE, CONCEPT, UNIT, VALUE_TEXT, VALUE_REAL, FORM, FY, FP,
     START_DATE, END_DATE, FILED_DATE, ACCN, FRAME)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.executemany(insert_sql, rows)
    con.commit()
    cur.close()
    con.close()

    return len(rows)


def main() -> None:
    """
    Pipeline entry point.

    - Loads selected ticker to CIK map.
    - Downloads SEC CompanyFacts for each ticker.
    - Extracts facts into a tidy long table.
    - Saves per ticker CSVs and a combined CSV and Parquet.
    - Creates and populates a SQLite database table plus indexes.
    """
    mapping = load_selected_map()

    create_companyfacts_table(DB_PATH)

    all_parts = []
    inserted_total = 0

    with requests.Session() as session:
        for _, row in mapping.iterrows():
            ticker = row["ticker"]
            cik10 = row["cik10"]
            title = row.get("title", "")

            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
            data = fetch_json(session, url, SEC_HEADERS_BASE)

            if data is None:
                print(f"Missing CompanyFacts for {ticker} (CIK {cik10})")
                continue

            df = extract_companyfacts(ticker, cik10, title, data)

            per_ticker_csv = OUT_DIR / f"{ticker}_companyfacts.csv"
            df.to_csv(per_ticker_csv, index=False)

            if df.empty:
                print(f"Saved {ticker}: 0 facts")
            else:
                all_parts.append(df)
                inserted_total += insert_companyfacts_sqlite(DB_PATH, df)
                print(f"Saved {ticker}: {len(df)} facts")

            time.sleep(0.12)

    if not all_parts:
        raise RuntimeError("No CompanyFacts extracted.")

    df_all = pd.concat(all_parts, ignore_index=True)
    save_outputs(df_all)

    print(f"SQLite saved: {DB_PATH} table {TABLE}. Inserted rows: {inserted_total}")
    print("Done")


if __name__ == "__main__":
    main()

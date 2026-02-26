#!/usr/bin/env python3
"""
Build SEC_COMPANYFACTS SQLite database.

Behavior:
1) If `data/raw/sec/companyfacts/*_companyfacts.csv` exists, build DB from those files.
2) If missing, auto-download SEC CompanyFacts JSON for selected tickers and write CSVs first.

Output:
data/raw/sec/sec_companyfacts.db

The table schema matches openai-agent US workflow requirements.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    SEC_COMPANYFACTS_DIR as CSV_DIR,
    SEC_COMPANYFACTS_CSV,
    SEC_DB_PATH as OUT_DB,
    SEC_COMPANYFACTS_TABLE,
    SEC_TICKER_MAP_CSV_SELECTED,
    SEC_HEADERS_BASE,
    SEC_RETRY_INITIAL_SLEEP_SECONDS,
    SEC_FILINGS_RETRY_STEP_SECONDS,
    SEC_COMPANYFACTS_FETCH_RETRIES,
    SEC_COMPANYFACTS_TIMEOUT_SECONDS,
    SEC_COMPANYFACTS_RETRY_STATUSES,
    SEC_COMPANYFACTS_INTER_REQUEST_SLEEP_SECONDS,
    SEC_COMPANYFACTS_FORMS,
)

COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"


def create_schema(con: sqlite3.Connection) -> None:
    """
    Create SEC_COMPANYFACTS table.
    """
    con.execute(f"DROP TABLE IF EXISTS {SEC_COMPANYFACTS_TABLE}")

    con.execute(
        f"""
        CREATE TABLE {SEC_COMPANYFACTS_TABLE} (
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


def load_selected_map() -> pd.DataFrame:
    if not SEC_TICKER_MAP_CSV_SELECTED.exists():
        raise FileNotFoundError(
            f"Missing SEC ticker map: {SEC_TICKER_MAP_CSV_SELECTED}. "
            "Run step `sec_map` first."
        )

    df = pd.read_csv(SEC_TICKER_MAP_CSV_SELECTED, dtype=str)
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["cik10"] = df["cik10"].astype(str).str.zfill(10)
    return df[["ticker", "cik10"]].drop_duplicates(subset=["ticker"]).reset_index(drop=True)


def fetch_json(session: requests.Session, url: str, retries: int = SEC_COMPANYFACTS_FETCH_RETRIES) -> dict | None:
    last_err = None
    for i in range(retries):
        try:
            r = session.get(url, headers=SEC_HEADERS_BASE, timeout=SEC_COMPANYFACTS_TIMEOUT_SECONDS)
            if r.status_code == 404:
                return None
            if r.status_code in SEC_COMPANYFACTS_RETRY_STATUSES:
                time.sleep(SEC_RETRY_INITIAL_SLEEP_SECONDS + i * SEC_FILINGS_RETRY_STEP_SECONDS)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(SEC_RETRY_INITIAL_SLEEP_SECONDS + i * SEC_FILINGS_RETRY_STEP_SECONDS)
    raise RuntimeError(f"Failed to fetch {url}. Last error: {last_err}")


def rows_from_companyfacts(payload: dict[str, Any], ticker: str, cik10: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    allowed_forms = set(SEC_COMPANYFACTS_FORMS)

    facts = payload.get("facts", {})
    if not isinstance(facts, dict):
        return rows

    for _, concept_map in facts.items():
        if not isinstance(concept_map, dict):
            continue
        for concept, concept_payload in concept_map.items():
            if not isinstance(concept_payload, dict):
                continue
            units = concept_payload.get("units", {})
            if not isinstance(units, dict):
                continue
            for unit, observations in units.items():
                if not isinstance(observations, list):
                    continue
                for obs in observations:
                    if not isinstance(obs, dict):
                        continue
                    form = str(obs.get("form") or "").strip()
                    if form and allowed_forms and form not in allowed_forms:
                        continue
                    rows.append(
                        {
                            "ticker": ticker,
                            "cik": cik10,
                            "concept": concept,
                            "unit": unit,
                            "value": obs.get("val"),
                            "form": form or None,
                            "fy": obs.get("fy"),
                            "fp": obs.get("fp"),
                            "start": obs.get("start"),
                            "end": obs.get("end"),
                            "filed": obs.get("filed"),
                            "accn": obs.get("accn"),
                            "frame": obs.get("frame"),
                        }
                    )

    return rows


def auto_download_companyfacts_csvs() -> int:
    """
    Download and save per-ticker CompanyFacts CSV files for selected tickers.

    Returns number of per-ticker CSV files written.
    """
    mapping = load_selected_map()
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    written = 0
    combined_parts: list[pd.DataFrame] = []

    with requests.Session() as session:
        for _, row in mapping.iterrows():
            ticker = row["ticker"]
            cik10 = row["cik10"]
            url = COMPANYFACTS_URL.format(cik10=cik10)

            try:
                payload = fetch_json(session=session, url=url)
            except Exception as e:
                print(f"Failed CompanyFacts fetch for {ticker} ({cik10}): {e}")
                continue
            if payload is None:
                print(f"No CompanyFacts found for {ticker} ({cik10})")
                continue

            rows = rows_from_companyfacts(payload=payload, ticker=ticker, cik10=cik10)
            if not rows:
                print(f"No usable CompanyFacts rows for {ticker} ({cik10})")
                continue

            df_t = pd.DataFrame(rows)
            out_ticker = CSV_DIR / f"{ticker}_companyfacts.csv"
            df_t.to_csv(out_ticker, index=False)
            combined_parts.append(df_t)
            written += 1
            print(f"Saved {out_ticker.name} ({len(df_t)} rows)")

            time.sleep(SEC_COMPANYFACTS_INTER_REQUEST_SLEEP_SECONDS)

    if combined_parts:
        combined = pd.concat(combined_parts, ignore_index=True)
        SEC_COMPANYFACTS_CSV.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(SEC_COMPANYFACTS_CSV, index=False)
        print(f"Saved combined CSV: {SEC_COMPANYFACTS_CSV} ({len(combined)} rows)")

    return written


def collect_companyfacts_csv_files() -> list[Path]:
    return sorted(CSV_DIR.glob("*_companyfacts.csv"))


def main() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = collect_companyfacts_csv_files()
    if not csv_files:
        print(f"No *_companyfacts.csv files found under {CSV_DIR}. Auto-downloading from SEC...")
        written = auto_download_companyfacts_csvs()
        csv_files = collect_companyfacts_csv_files()
        if written:
            print(f"Downloaded CompanyFacts CSV files: {written}")

    if not csv_files:
        raise RuntimeError(
            f"No *_companyfacts.csv files found in {CSV_DIR} after auto-download attempt. "
            "Check SEC_USER_AGENT in config/env and network access."
        )

    print(f"Found {len(csv_files)} CSV files")

    if OUT_DB.exists():
        OUT_DB.unlink()

    con = sqlite3.connect(str(OUT_DB))

    try:
        create_schema(con=con)

        for file_path in csv_files:
            print(f"Processing {file_path.name}")
            df = pd.read_csv(file_path, low_memory=False)
            df = normalise_columns(df=df)
            df["VALUE_REAL"] = pd.to_numeric(df["VALUE_REAL"], errors="coerce")
            df.to_sql(SEC_COMPANYFACTS_TABLE, con, if_exists="append", index=False)

        con.commit()

        cur = con.execute(f"SELECT COUNT(*) FROM {SEC_COMPANYFACTS_TABLE}")
        count = cur.fetchone()[0]

        print(f"Inserted {count} rows")

        if count == 0:
            raise RuntimeError("SEC_COMPANYFACTS table is empty.")

        # Create useful indexes
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sec_ticker_concept_end ON {SEC_COMPANYFACTS_TABLE} (TICKER, CONCEPT, END_DATE)"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_sec_ticker_filed ON {SEC_COMPANYFACTS_TABLE} (TICKER, FILED_DATE)"
        )
        con.commit()

        print(f"Database written to: {OUT_DB}")

    finally:
        con.close()


if __name__ == "__main__":
    main()

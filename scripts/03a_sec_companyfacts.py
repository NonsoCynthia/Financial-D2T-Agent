import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import SEC_HEADERS_BASE, SEC_MAP_DIR, RAW_DIR

START_END = date(2022, 1, 1)
END_END = date(2025, 12, 31)

OUT_DIR = RAW_DIR / "sec" / "companyfacts"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_ymd(s: str) -> date | None:
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def fetch_json(session: requests.Session, url: str, headers: dict, retries: int = 6) -> dict | None:
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
    p = SEC_MAP_DIR / "sec_ticker_cik_selected.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}. Run step sec_map first.")
    df = pd.read_csv(p, dtype=str)
    df["ticker"] = df["ticker"].str.upper().str.strip()
    df["cik10"] = df["cik10"].str.zfill(10)
    return df


def extract_companyfacts(ticker: str, cik10: str, data: dict) -> pd.DataFrame:
    """
    Extract quarterly or annual XBRL facts from the SEC CompanyFacts JSON.

    We:
    - Focus on us-gaap concepts (US GAAP).
    - Keep only 10-K and 10-Q filings.
    - Keep only facts whose 'end' date falls within START_END to END_END.
    - Return a tidy table with one row per (concept, unit, period, value).
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
                if end_d is None or not (START_END <= end_d <= END_END):
                    continue

                form = it.get("form")
                if form not in {"10-K", "10-Q"}:
                    continue

                rows.append(
                    {
                        "ticker": ticker,
                        "cik10": cik10,
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

    if not rows:
        return pd.DataFrame(
            columns=[
                "ticker",
                "cik10",
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
        )

    df = pd.DataFrame(rows)
    df["end"] = pd.to_datetime(df["end"], errors="coerce").dt.date
    df = df.sort_values(["ticker", "end", "concept", "unit"]).reset_index(drop=True)
    return df


def save_outputs(df_all: pd.DataFrame) -> None:
    out_csv = OUT_DIR / "companyfacts_2022_2025.csv"
    df_all.to_csv(out_csv, index=False)

    try:
        out_parquet = OUT_DIR / "companyfacts_2022_2025.parquet"
        df_all.to_parquet(out_parquet, index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {out_csv}")


def main() -> None:
    mapping = load_selected_map()

    all_parts = []
    with requests.Session() as session:
        for _, row in mapping.iterrows():
            ticker = row["ticker"]
            cik10 = row["cik10"]
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
            data = fetch_json(session, url, SEC_HEADERS_BASE)
            if data is None:
                print(f"Missing CompanyFacts for {ticker} (CIK {cik10})")
                continue

            df = extract_companyfacts(ticker, cik10, data)
            
            per_ticker_csv = OUT_DIR / f"{ticker}_companyfacts.csv"
            df.to_csv(per_ticker_csv, index=False)

            if df.empty:
                print(f"Saved {ticker}: 0 facts (likely non us-gaap or no 10-K/10-Q in range)")
            else:
                all_parts.append(df)
                print(f"Saved {ticker}: {len(df)} facts")

            time.sleep(0.12)

    if not all_parts:
        raise RuntimeError("No CompanyFacts extracted.")

    df_all = pd.concat(all_parts, ignore_index=True)
    save_outputs(df_all)
    print("Done")


if __name__ == "__main__":
    main()

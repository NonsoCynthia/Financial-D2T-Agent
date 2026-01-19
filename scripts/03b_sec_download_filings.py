import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import SEC_HEADERS_BASE, SEC_MAP_DIR, RAW_DIR

START_FILED = date(2022, 1, 1)
END_FILED = date(2025, 12, 31)

OUT_DIR = RAW_DIR / "sec" / "filings_raw"
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


def filing_url(cik10: str, accession: str, primary_doc: str) -> str:
    cik_nolead = str(int(cik10))
    acc_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/{primary_doc}"


def main() -> None:
    mapping = load_selected_map()
    index_rows = []

    with requests.Session() as session:
        for _, row in mapping.iterrows():
            ticker = row["ticker"]
            cik10 = row["cik10"]

            sub_url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
            sub = fetch_json(session, sub_url, SEC_HEADERS_BASE)
            if sub is None:
                print(f"Missing submissions for {ticker} (CIK {cik10})")
                continue

            recent = sub.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            filing_dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])

            out_ticker_dir = OUT_DIR / ticker
            out_ticker_dir.mkdir(parents=True, exist_ok=True)

            kept = 0
            for form, fdate, acc, pdoc in zip(forms, filing_dates, accessions, primary_docs):
                if form not in {"10-K", "10-Q"}:
                    continue

                d = parse_ymd(fdate)
                if d is None or not (START_FILED <= d <= END_FILED):
                    continue

                url = filing_url(cik10, acc, pdoc)
                ext = pdoc.split(".")[-1] if "." in pdoc else "txt"
                out_path = out_ticker_dir / f"{fdate}_{form}_{acc}.{ext}"

                try:
                    r = session.get(url, headers={"User-Agent": SEC_HEADERS_BASE["User-Agent"]}, timeout=60)
                    if r.status_code in (429, 503):
                        time.sleep(1.0)
                        r = session.get(url, headers={"User-Agent": SEC_HEADERS_BASE["User-Agent"]}, timeout=60)
                    r.raise_for_status()
                    out_path.write_bytes(r.content)

                    index_rows.append(
                        {
                            "ticker": ticker,
                            "cik10": cik10,
                            "form": form,
                            "filing_date": fdate,
                            "accession": acc,
                            "primary_doc": pdoc,
                            "url": url,
                            "local_path": str(out_path),
                        }
                    )
                    kept += 1
                except Exception:
                    index_rows.append(
                        {
                            "ticker": ticker,
                            "cik10": cik10,
                            "form": form,
                            "filing_date": fdate,
                            "accession": acc,
                            "primary_doc": pdoc,
                            "url": url,
                            "local_path": "",
                        }
                    )

                time.sleep(0.12)

            print(f"Saved {ticker}: {kept} filings in range")

    if index_rows:
        df_idx = pd.DataFrame(index_rows).sort_values(["ticker", "filing_date", "form"])
        out_idx = OUT_DIR / "filings_index_2022_2025.csv"
        df_idx.to_csv(out_idx, index=False)
        print(f"Saved: {out_idx}")

    print("Done")


if __name__ == "__main__":
    main()

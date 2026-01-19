import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import TICKERS, SEC_TICKER_MAP_URL, SEC_HEADERS_BASE, SEC_MAP_DIR


def fetch_json(session: requests.Session, url: str, headers: dict, retries: int = 5) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=30)
            if r.status_code in (429, 503):
                time.sleep(0.5 + i * 0.5)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5 + i * 0.5)
    raise RuntimeError(f"Failed to fetch {url}. Last error: {last_err}")


def build_ticker_cik_df(raw: dict) -> pd.DataFrame:
    rows = []
    for _, v in raw.items():
        ticker = str(v.get("ticker", "")).upper().strip()
        cik = v.get("cik_str", None)
        title = v.get("title", "")
        if not ticker or cik is None:
            continue
        cik10 = str(int(cik)).zfill(10)
        rows.append({"ticker": ticker, "cik10": cik10, "title": title})
    df = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).sort_values("ticker").reset_index(drop=True)
    return df


def filter_to_tickers(df: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    want = {t.upper().strip() for t in tickers if t.strip()}
    if not want:
        return df
    return df[df["ticker"].isin(want)].sort_values("ticker").reset_index(drop=True)


def main() -> None:
    SEC_MAP_DIR.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        raw = fetch_json(session, SEC_TICKER_MAP_URL, SEC_HEADERS_BASE)

    df_all = build_ticker_cik_df(raw)
    df_sel = filter_to_tickers(df_all, TICKERS)

    out_all = SEC_MAP_DIR / "sec_ticker_cik_all.csv"
    out_sel = SEC_MAP_DIR / "sec_ticker_cik_selected.csv"

    df_all.to_csv(out_all, index=False)
    df_sel.to_csv(out_sel, index=False)

    missing = sorted(set(t.upper() for t in TICKERS) - set(df_sel["ticker"].tolist()))
    print(f"Saved: {out_all}")
    print(f"Saved: {out_sel}")
    print(f"Selected rows: {len(df_sel)}")
    if missing:
        print(f"Tickers not found in SEC map: {missing}")


if __name__ == "__main__":
    main()

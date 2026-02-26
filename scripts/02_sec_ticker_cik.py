import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import (
    TICKERS,
    SEC_TICKER_MAP_URL,
    SEC_HEADERS_BASE,
    SEC_TICKER_MAP_CSV_ALL,
    SEC_TICKER_MAP_CSV_SELECTED,
    SEC_MAP_FETCH_RETRIES,
    SEC_MAP_TIMEOUT_SECONDS,
    SEC_RETRY_INITIAL_SLEEP_SECONDS,
    SEC_MAP_RETRY_STEP_SECONDS,
    SEC_MAP_RETRY_STATUSES,
)


def fetch_json(session: requests.Session, url: str, headers: dict, retries: int = SEC_MAP_FETCH_RETRIES) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=SEC_MAP_TIMEOUT_SECONDS)
            if r.status_code in SEC_MAP_RETRY_STATUSES:
                time.sleep(SEC_RETRY_INITIAL_SLEEP_SECONDS + i * SEC_MAP_RETRY_STEP_SECONDS)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(SEC_RETRY_INITIAL_SLEEP_SECONDS + i * SEC_MAP_RETRY_STEP_SECONDS)
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
    SEC_TICKER_MAP_CSV_ALL.parent.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        raw = fetch_json(session=session, url=SEC_TICKER_MAP_URL, headers=SEC_HEADERS_BASE)

    df_all = build_ticker_cik_df(raw=raw)
    df_sel = filter_to_tickers(df=df_all, tickers=TICKERS)

    out_all = SEC_TICKER_MAP_CSV_ALL
    out_sel = SEC_TICKER_MAP_CSV_SELECTED

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

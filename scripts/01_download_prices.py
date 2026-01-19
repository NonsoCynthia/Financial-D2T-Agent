import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf

from config import (
    TICKERS,
    START_DATE,
    END_DATE_EXCLUSIVE,
    END_DATE_INCLUSIVE,
    PRICES_RAW_DIR,
)

REQUIRED_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def download_bundle(tickers: list[str], start: str, end_exclusive: str) -> pd.DataFrame:
    df = yf.download(
        tickers=tickers,
        start=start,
        end=end_exclusive,
        auto_adjust=False,
        group_by="ticker",
        progress=True,
        threads=True,
    )
    if df is None or df.empty:
        raise RuntimeError("No data returned by yfinance.")
    return df


def normalise_one(df_raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(df_raw.columns, pd.MultiIndex):
        if ticker not in df_raw.columns.get_level_values(0):
            raise KeyError(f"{ticker} missing from downloaded data.")
        df = df_raw[ticker].copy()
    else:
        df = df_raw.copy()

    df = df.reset_index()
    df = df.rename(columns={"Date": "date"})
    df["ticker"] = ticker

    keep = ["date"] + [c for c in REQUIRED_COLS if c in df.columns] + ["ticker"]
    df = df[keep]

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.dropna(subset=[c for c in REQUIRED_COLS if c in df.columns], how="all")
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    return df


def main() -> None:
    PRICES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    tickers = [t.strip().upper() for t in TICKERS if t.strip()]
    df_raw = download_bundle(tickers, START_DATE, END_DATE_EXCLUSIVE)

    parts: list[pd.DataFrame] = []
    for t in tickers:
        try:
            df_t = normalise_one(df_raw, t)
        except Exception as e:
            print(f"Skipping {t}. {e}")
            continue

        df_t.to_csv(PRICES_RAW_DIR / f"{t}.csv", index=False)
        parts.append(df_t)
        print(f"Saved {t}. {len(df_t)} rows")

    if not parts:
        raise RuntimeError("No ticker files saved.")

    df_long = pd.concat(parts, ignore_index=True).sort_values(["date", "ticker"])
    df_long.to_csv(PRICES_RAW_DIR / "all_prices_long.csv", index=False)

    wide_adj = df_long.pivot(index="date", columns="ticker", values="Adj Close")
    wide_adj.to_csv(PRICES_RAW_DIR / "all_prices_wide_adj_close.csv")

    df_long.to_parquet(PRICES_RAW_DIR / "all_prices_long.parquet", index=False)
    wide_adj.to_parquet(PRICES_RAW_DIR / "all_prices_wide_adj_close.parquet")

    print(f"Done. {START_DATE} to {END_DATE_INCLUSIVE} inclusive")


if __name__ == "__main__":
    main()

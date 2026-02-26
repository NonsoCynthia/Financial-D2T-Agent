from __future__ import annotations

from pathlib import Path

import pandas as pd
import yfinance as yf

from eu_config import END_DATE_EXCLUSIVE, END_DATE_INCLUSIVE, EU_TICKERS, START_DATE
from eu_paths import LEGACY_PRICES_DIR, RAW_PRICES_DIRS, ensure_dirs


def _normalise_downloaded_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    work = df.copy()
    if isinstance(work.columns, pd.MultiIndex):
        work.columns = [str(col[0]) for col in work.columns]

    work = work.reset_index()
    if "Adj Close" not in work.columns and "Close" in work.columns:
        work["Adj Close"] = work["Close"]
    work["ticker"] = ticker
    return work


def download_prices(tickers: list[str], start_date: str, end_date: str, output_dirs: list[Path]) -> None:
    """
    Download daily OHLCV prices and save:
    - per-ticker CSVs
    - combined long and wide adjusted-close CSVs
    under data_eu (raw + legacy paths).
    """
    ensure_dirs(paths=output_dirs)

    parts: list[pd.DataFrame] = []
    for ticker in tickers:
        print(f"Downloading prices for {ticker}")
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if data is None or data.empty:
            print(f"No data found for {ticker}")
            continue

        out = _normalise_downloaded_frame(df=data, ticker=ticker)
        parts.append(out.copy())

        for d in output_dirs:
            # US-style filename
            out.to_csv(d / f"{ticker}.csv", index=False)
            # Legacy EU filename
            out.to_csv(d / f"{ticker}_prices.csv", index=False)

    if not parts:
        raise RuntimeError("No price files were downloaded.")

    long_df = pd.concat(parts, ignore_index=True)
    long_df["Date"] = pd.to_datetime(long_df["Date"], errors="coerce")
    long_df = long_df.dropna(subset=["Date"]).sort_values(["ticker", "Date"]).reset_index(drop=True)

    wide_adj = long_df.pivot(index="Date", columns="ticker", values="Adj Close").sort_index()

    for d in output_dirs:
        long_df.to_csv(d / "all_prices_long.csv", index=False)
        wide_adj.to_csv(d / "all_prices_wide_adj_close.csv")
        try:
            long_df.to_parquet(d / "all_prices_long.parquet", index=False)
            wide_adj.to_parquet(d / "all_prices_wide_adj_close.parquet")
        except Exception as exc:
            print(f"Parquet not written in {d}: {exc}")


if __name__ == "__main__":
    download_prices(
        tickers=EU_TICKERS,
        start_date=START_DATE,
        end_date=END_DATE_EXCLUSIVE,
        output_dirs=[*RAW_PRICES_DIRS, LEGACY_PRICES_DIR],
    )
    print(f"Done. {START_DATE} to {END_DATE_INCLUSIVE} inclusive")

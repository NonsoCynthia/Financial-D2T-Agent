from __future__ import annotations

from pathlib import Path

import pandas as pd
import yfinance as yf

from eu_config import EU_TICKERS
from eu_paths import LEGACY_FUNDAMENTALS_DIR, RAW_FUNDAMENTALS_DIRS, ensure_dirs


def download_fundamentals(tickers: list[str], output_dirs: list[Path]) -> None:
    """
    Download annual fundamentals and write per-ticker + combined files
    under data_eu (raw + legacy paths).
    """
    ensure_dirs(paths=output_dirs)
    all_rows: list[pd.DataFrame] = []

    for ticker in tickers:
        print(f"Downloading fundamentals for {ticker}")
        stock = yf.Ticker(ticker)

        income = stock.financials.T
        balance = stock.balance_sheet.T
        if income is None or balance is None or income.empty or balance.empty:
            print(f"No fundamentals for {ticker}")
            continue

        df = pd.DataFrame(index=income.index)
        df["revenue"] = income.get("Total Revenue")
        df["net_income"] = income.get("Net Income")
        df["ebit"] = income.get("Ebit")
        df["total_assets"] = balance.get("Total Assets")
        df["cash"] = balance.get("Cash")
        df["ticker"] = ticker
        df = df.reset_index().rename(columns={"index": "report_date"})
        all_rows.append(df.copy())

        for d in output_dirs:
            df.to_csv(d / f"{ticker}_fundamentals.csv", index=False)

    if not all_rows:
        raise RuntimeError("No fundamentals were downloaded.")

    combined = pd.concat(all_rows, ignore_index=True).sort_values(["ticker", "report_date"]).reset_index(drop=True)
    for d in output_dirs:
        combined.to_csv(d / "all_fundamentals.csv", index=False)
        try:
            combined.to_parquet(d / "all_fundamentals.parquet", index=False)
        except Exception as exc:
            print(f"Parquet not written in {d}: {exc}")


if __name__ == "__main__":
    download_fundamentals(
        tickers=EU_TICKERS,
        output_dirs=[*RAW_FUNDAMENTALS_DIRS, LEGACY_FUNDAMENTALS_DIR],
    )

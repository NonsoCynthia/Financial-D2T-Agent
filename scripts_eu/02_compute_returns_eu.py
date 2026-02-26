from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

from eu_paths import (
    LEGACY_MONTHLY_RETURNS_DIR,
    LEGACY_PRICES_DIR,
    PROCESSED_MONTHLY_RETURNS_DIRS,
    PROCESSED_PRICES_DIRS,
    RAW_PRICES_DIRS,
    ensure_dirs,
)


def _pick_input_dir(candidates: list[Path]) -> Path:
    for c in candidates:
        if c.exists() and any(c.glob("*.csv")):
            return c
    raise FileNotFoundError(f"No input price CSVs found in: {candidates}")


def _iter_price_files(input_dir: Path) -> list[Path]:
    out: list[Path] = []
    for p in sorted(input_dir.glob("*.csv")):
        name = p.name
        if name in {"all_prices_long.csv", "all_prices_wide_adj_close.csv"}:
            continue
        if name.endswith("_prices.csv") or name.endswith(".csv"):
            out.append(p)
    return out


def _ticker_from_file(path: Path) -> str:
    n = path.name
    if n.endswith("_prices.csv"):
        return n[: -len("_prices.csv")]
    return n[: -len(".csv")]


def _resolve_date_col(df: pd.DataFrame) -> str:
    for c in ["Date", "date"]:
        if c in df.columns:
            return c
    raise ValueError(f"Could not find date column in: {list(df.columns)}")


def _resolve_price_col(df: pd.DataFrame) -> str:
    for candidate in ["Adj Close", "Close", "adj_close", "close"]:
        if candidate in df.columns:
            return candidate
    raise ValueError(f"Could not find close price column in: {list(df.columns)}")


def _standard_daily_frame(df: pd.DataFrame, date_col: str, price_col: str, ticker: str) -> pd.DataFrame:
    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)

    work[price_col] = pd.to_numeric(work[price_col], errors="coerce")
    work = work.dropna(subset=[price_col]).copy()

    work["daily_return"] = work[price_col].pct_change()
    ratio = work[price_col] / work[price_col].shift(1)
    work["log_ret_1d"] = np.log(ratio)

    vol_col = "Volume" if "Volume" in work.columns else ("volume" if "volume" in work.columns else None)
    volume = pd.to_numeric(work[vol_col], errors="coerce") if vol_col is not None else np.nan

    return pd.DataFrame(
        {
            "date": work[date_col].dt.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "adj_close": work[price_col],
            "ret_1d": work["daily_return"],
            "log_ret_1d": work["log_ret_1d"],
            "Volume": volume,
        }
    )


def compute_returns(input_dir: Path, monthly_output_dirs: list[Path], daily_output_dirs: list[Path]) -> None:
    """
    Build both:
    - monthly per-ticker returns files
    - daily consolidated returns table
    """
    ensure_dirs(paths=monthly_output_dirs)
    ensure_dirs(paths=daily_output_dirs)

    price_files = _iter_price_files(input_dir=input_dir)
    if not price_files:
        raise RuntimeError(f"No price files found in {input_dir}")

    daily_parts: list[pd.DataFrame] = []
    for file_path in price_files:
        df = pd.read_csv(file_path)
        date_col = _resolve_date_col(df=df)
        price_col = _resolve_price_col(df=df)

        ticker = (
            str(df["ticker"].dropna().astype(str).iloc[0]).strip()
            if "ticker" in df.columns and df["ticker"].notna().any()
            else _ticker_from_file(path=file_path)
        )

        # Clean header-like rows that sometimes leak from CSV exports.
        df = df[pd.to_datetime(df[date_col], errors="coerce").notna()].copy()
        if df.empty:
            continue

        daily_std = _standard_daily_frame(df=df, date_col=date_col, price_col=price_col, ticker=ticker)
        daily_parts.append(daily_std)

        monthly = df.copy()
        monthly[date_col] = pd.to_datetime(monthly[date_col], errors="coerce")
        monthly = monthly.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
        monthly[price_col] = pd.to_numeric(monthly[price_col], errors="coerce")
        monthly = monthly.dropna(subset=[price_col]).copy()
        monthly = monthly.resample("ME", on=date_col).last()
        monthly["monthly_return"] = monthly[price_col].pct_change()
        monthly["ticker"] = ticker
        monthly = monthly.reset_index()

        for d in monthly_output_dirs:
            monthly.to_csv(d / f"{ticker}_monthly_returns.csv", index=False)

    if not daily_parts:
        raise RuntimeError("No daily returns were computed.")

    daily = pd.concat(daily_parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)

    for d in daily_output_dirs:
        csv_path = d / "daily_returns.csv"
        daily.to_csv(csv_path, index=False)
        try:
            daily.to_parquet(d / "daily_returns.parquet", index=False)
        except Exception as exc:
            print(f"Parquet not written in {d}: {exc}")

if __name__ == "__main__":
    input_dir = _pick_input_dir(candidates=[*RAW_PRICES_DIRS, LEGACY_PRICES_DIR])

    compute_returns(
        input_dir=input_dir,
        monthly_output_dirs=[*PROCESSED_MONTHLY_RETURNS_DIRS, LEGACY_MONTHLY_RETURNS_DIR],
        daily_output_dirs=PROCESSED_PRICES_DIRS,
    )

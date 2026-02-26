from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from eu_paths import (
    LEGACY_FUNDAMENTALS_DIR,
    LEGACY_MONTHLY_PANEL_FILE,
    LEGACY_MONTHLY_RETURNS_DIR,
    PROCESSED_MONTHLY_RETURNS_DIRS,
    PROCESSED_PANEL_DIRS,
    RAW_FUNDAMENTALS_DIRS,
    ensure_parent_dirs,
)


def _pick_input_dir(candidates: list[Path]) -> Path:
    for c in candidates:
        if c.exists() and any(c.glob("*.csv")):
            return c
    raise FileNotFoundError(f"No CSV files found in candidate input dirs: {candidates}")


def _standardize_panel_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "date"})
    if "ticker_x" in out.columns and out["ticker_x"].notna().any():
        out["ticker"] = out["ticker_x"].astype(str)
    elif "ticker_y" in out.columns and out["ticker_y"].notna().any():
        out["ticker"] = out["ticker_y"].astype(str)
    elif "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str)

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()

    if "Adj Close" in out.columns:
        out["adj_close"] = pd.to_numeric(out["Adj Close"], errors="coerce")
    elif "Close" in out.columns:
        out["adj_close"] = pd.to_numeric(out["Close"], errors="coerce")
    else:
        out["adj_close"] = pd.NA

    if "daily_return" in out.columns:
        out["ret_1d"] = pd.to_numeric(out["daily_return"], errors="coerce")
    if "log_ret_1d" in out.columns:
        out["log_ret_1d"] = pd.to_numeric(out["log_ret_1d"], errors="coerce")
    if "report_date" in out.columns:
        out["filed"] = pd.to_datetime(out["report_date"], errors="coerce")

    # Add simple price columns used in downstream monthly workflows.
    close = pd.to_numeric(out["Close"], errors="coerce") if "Close" in out.columns else pd.Series(pd.NA, index=out.index)
    open_ = pd.to_numeric(out["Open"], errors="coerce") if "Open" in out.columns else pd.Series(pd.NA, index=out.index)
    high = pd.to_numeric(out["High"], errors="coerce") if "High" in out.columns else pd.Series(pd.NA, index=out.index)
    low = pd.to_numeric(out["Low"], errors="coerce") if "Low" in out.columns else pd.Series(pd.NA, index=out.index)
    volume = pd.to_numeric(out["Volume"], errors="coerce") if "Volume" in out.columns else pd.Series(pd.NA, index=out.index)

    out["price_open"] = open_
    out["price_close"] = close
    out["price_avg"] = pd.concat([open_, high, low, close], axis=1).mean(axis=1, skipna=True)
    out["price_min"] = low
    out["price_max"] = high
    out["price_volume"] = volume

    drop_cols = [c for c in ["ticker_x", "ticker_y"] if c in out.columns]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def make_monthly_panel(price_dir: Path, fundamentals_dir: Path) -> pd.DataFrame:
    """
    Merge monthly returns with fundamentals using backward asof alignment.
    """
    all_data: list[pd.DataFrame] = []

    for file in os.listdir(price_dir):
        if not file.endswith("_monthly_returns.csv"):
            continue

        ticker = file.replace("_monthly_returns.csv", "")
        price_df = pd.read_csv(price_dir / file)
        date_col = "Date" if "Date" in price_df.columns else ("date" if "date" in price_df.columns else None)
        if date_col is None:
            continue
        price_df[date_col] = pd.to_datetime(price_df[date_col], errors="coerce")
        price_df = price_df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)

        fund_path = fundamentals_dir / f"{ticker}_fundamentals.csv"
        if not fund_path.exists():
            continue

        fund_df = pd.read_csv(fund_path)
        if "report_date" not in fund_df.columns:
            continue
        fund_df["report_date"] = pd.to_datetime(fund_df["report_date"], errors="coerce")
        fund_df = fund_df.dropna(subset=["report_date"]).sort_values("report_date").reset_index(drop=True)

        merged = pd.merge_asof(
            price_df,
            fund_df,
            left_on=date_col,
            right_on="report_date",
            direction="backward",
        )
        all_data.append(merged)

    if not all_data:
        raise RuntimeError("No monthly panel rows produced. Check monthly returns and fundamentals files.")

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = _standardize_panel_columns(df=final_df)
    return final_df


def save_panel_outputs(final_df: pd.DataFrame, output_files: list[Path]) -> None:
    ensure_parent_dirs(paths=output_files)
    for p in output_files:
        final_df.to_csv(p, index=False)
        try:
            final_df.to_parquet(p.with_suffix(".parquet"), index=False)
        except Exception as exc:
            print(f"Parquet not written for {p}: {exc}")


if __name__ == "__main__":
    price_dir = _pick_input_dir(candidates=[*PROCESSED_MONTHLY_RETURNS_DIRS, LEGACY_MONTHLY_RETURNS_DIR])
    fundamentals_dir = _pick_input_dir(candidates=[*RAW_FUNDAMENTALS_DIRS, LEGACY_FUNDAMENTALS_DIR])
    final_df = make_monthly_panel(price_dir=price_dir, fundamentals_dir=fundamentals_dir)

    output_files = [d / "monthly_panel_prices_returns_fundamentals.csv" for d in PROCESSED_PANEL_DIRS]
    output_files.append(LEGACY_MONTHLY_PANEL_FILE)

    save_panel_outputs(
        final_df=final_df,
        output_files=output_files,
    )

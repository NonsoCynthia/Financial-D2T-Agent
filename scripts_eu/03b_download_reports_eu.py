from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from eu_config import EU_TICKERS
from eu_paths import LEGACY_REPORTS_DIR, RAW_REPORTS_DIRS, ensure_dirs


def _safe_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Ensures a returned object is a DataFrame and standardises empty returns.
    """
    if df is None:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    return df


def _to_long_format(df: pd.DataFrame, ticker: str, statement: str, period: str) -> pd.DataFrame:
    """
    Converts a wide statement matrix into a tidy long format:
    ticker, period_type, statement, report_date, item, value.
    """
    if df.empty:
        return pd.DataFrame(columns=["ticker", "period_type", "statement", "report_date", "item", "value"])

    wide = df.copy()

    if wide.columns.dtype != "datetime64[ns]":
        try:
            wide.columns = pd.to_datetime(wide.columns)
        except Exception:
            pass

    wide.index = wide.index.astype(str)

    long_df = (
        wide.reset_index()
        .melt(id_vars=["index"], var_name="report_date", value_name="value")
        .rename(columns={"index": "item"})
    )

    long_df.insert(0, "ticker", ticker)
    long_df.insert(1, "period_type", period)
    long_df.insert(2, "statement", statement)

    long_df["report_date"] = pd.to_datetime(long_df["report_date"], errors="coerce")

    long_df = long_df.dropna(subset=["report_date"])

    return long_df


def _save_statement(
    ticker: str,
    df: pd.DataFrame,
    output_dirs: list[Path],
    period: str,
    statement: str
) -> Tuple[bool, str]:
    """
    Saves one statement in both wide and long CSV formats.
    Returns (saved, message).
    """
    if df.empty:
        return False, f"{ticker}: {period} {statement} is empty"

    long_df = _to_long_format(df, ticker=ticker, statement=statement, period=period)
    for base_dir in output_dirs:
        out_dir = base_dir / period / ticker
        out_dir.mkdir(parents=True, exist_ok=True)

        wide_path = out_dir / f"{statement}_{period}_wide.csv"
        long_path = out_dir / f"{statement}_{period}_long.csv"
        df.to_csv(wide_path)
        long_df.to_csv(long_path, index=False)

    return True, f"{ticker}: saved {period} {statement}"


def download_annual_and_quarterly_reports(
    tickers: List[str],
    output_dirs: list[Path],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Downloads annual and quarterly financial statements via yfinance and writes CSV files.
    Returns a nested status dictionary for logging and debugging.
    """
    ensure_dirs(paths=output_dirs)

    status: Dict[str, Dict[str, Dict[str, str]]] = {}

    for ticker in tickers:
        print(f"\nFetching statements for {ticker}")
        tk = yf.Ticker(ticker)

        annual_income = _safe_df(getattr(tk, "financials", None))
        quarterly_income = _safe_df(getattr(tk, "quarterly_financials", None))

        annual_balance = _safe_df(getattr(tk, "balance_sheet", None))
        quarterly_balance = _safe_df(getattr(tk, "quarterly_balance_sheet", None))

        annual_cashflow = _safe_df(getattr(tk, "cashflow", None))
        quarterly_cashflow = _safe_df(getattr(tk, "quarterly_cashflow", None))

        status[ticker] = {"annual": {}, "quarterly": {}}

        ok, msg = _save_statement(ticker, annual_income, output_dirs, "annual", "income_statement")
        status[ticker]["annual"]["income_statement"] = msg
        print(msg)

        ok, msg = _save_statement(ticker, annual_balance, output_dirs, "annual", "balance_sheet")
        status[ticker]["annual"]["balance_sheet"] = msg
        print(msg)

        ok, msg = _save_statement(ticker, annual_cashflow, output_dirs, "annual", "cashflow")
        status[ticker]["annual"]["cashflow"] = msg
        print(msg)

        ok, msg = _save_statement(ticker, quarterly_income, output_dirs, "quarterly", "income_statement")
        status[ticker]["quarterly"]["income_statement"] = msg
        print(msg)

        ok, msg = _save_statement(ticker, quarterly_balance, output_dirs, "quarterly", "balance_sheet")
        status[ticker]["quarterly"]["balance_sheet"] = msg
        print(msg)

        ok, msg = _save_statement(ticker, quarterly_cashflow, output_dirs, "quarterly", "cashflow")
        status[ticker]["quarterly"]["cashflow"] = msg
        print(msg)

    for output_dir in output_dirs:
        summary_path = output_dir / "download_status.json"
        pd.Series(status).to_json(summary_path, indent=2)
        print(f"\nSaved status summary to {summary_path}")

    return status


if __name__ == "__main__":
    download_annual_and_quarterly_reports(EU_TICKERS, output_dirs=[*RAW_REPORTS_DIRS, LEGACY_REPORTS_DIR])

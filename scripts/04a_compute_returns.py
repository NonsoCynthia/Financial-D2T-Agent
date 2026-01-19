import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import PRICES_RAW_DIR, PROCESSED_DIR

OUT_DIR = PROCESSED_DIR / "prices"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_prices_long(csv_path: Path) -> pd.DataFrame:
    """
    Load the long format price table produced by the prices step.

    Expected columns include:
    - date, ticker, Adj Close, Volume

    Returns a DataFrame with parsed datetime and sorted rows.
    """
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple and log daily returns per ticker.

    - ret_1d is percentage return using adjusted close.
    - log_ret_1d is log return, which is often easier to sum across time.
    """
    df = df.copy()

    df["adj_close"] = pd.to_numeric(df["Adj Close"], errors="coerce")

    # Simple returns: (P_t / P_{t-1}) - 1
    df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()

    # Log returns: ln(P_t / P_{t-1})
    ratio = df.groupby("ticker")["adj_close"].shift(0) / df.groupby("ticker")["adj_close"].shift(1)
    df["log_ret_1d"] = np.log(ratio)

    return df


def save_outputs(df: pd.DataFrame, out_dir: Path) -> None:
    """
    Save processed returns to CSV and optionally Parquet.

    Parquet is written only if a Parquet engine is installed.
    """
    out = df[["date", "ticker", "adj_close", "ret_1d", "log_ret_1d", "Volume"]].copy()

    csv_path = out_dir / "daily_returns.csv"
    out.to_csv(csv_path, index=False)

    try:
        pq_path = out_dir / "daily_returns.parquet"
        out.to_parquet(pq_path, index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {csv_path}")


def main() -> None:
    """
    Pipeline entrypoint.

    Loads prices, computes returns, and writes processed outputs.
    """
    prices_path = PRICES_RAW_DIR / "all_prices_long.csv"
    df = load_prices_long(prices_path)
    df = add_returns(df)
    save_outputs(df, OUT_DIR)


if __name__ == "__main__":
    main()

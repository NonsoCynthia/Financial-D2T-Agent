import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import PROCESSED_DIR, TRAIN_END, TEST_START, TEST_END

PANEL_DIR = PROCESSED_DIR / "panel"
OUT_DIR = PROCESSED_DIR / "splits"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_panel() -> pd.DataFrame:
    """
    Load the daily panel dataset produced in Step 04b.
    """
    p = PANEL_DIR / "daily_panel_prices_returns_fundamentals.csv"
    df = pd.read_csv(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def add_simple_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a few basic time series features derived from returns.
    These are simple, stable, and usually useful in trading tasks.
    """
    df = df.copy()
    df["ret_1d"] = pd.to_numeric(df["ret_1d"], errors="coerce")

    df["ret_5d"] = df.groupby("ticker")["ret_1d"].rolling(5).sum().reset_index(level=0, drop=True)
    df["ret_20d"] = df.groupby("ticker")["ret_1d"].rolling(20).sum().reset_index(level=0, drop=True)

    df["vol_20d"] = df.groupby("ticker")["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    df["vol_60d"] = df.groupby("ticker")["ret_1d"].rolling(60).std().reset_index(level=0, drop=True)

    return df


def make_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a next day return target for supervised learning.
    target_ret_1d is the next trading day's return per ticker.
    """
    df = df.copy()
    df["target_ret_1d"] = df.groupby("ticker")["ret_1d"].shift(-1)
    return df


def split_by_date(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split into train and test based on fixed date boundaries.
    """
    train_end = pd.to_datetime(TRAIN_END)
    test_start = pd.to_datetime(TEST_START)
    test_end = pd.to_datetime(TEST_END)

    train = df[df["date"] <= train_end].copy()
    test = df[(df["date"] >= test_start) & (df["date"] <= test_end)].copy()

    return train, test


def standardise(train: pd.DataFrame, test: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Standardise features using train statistics only to avoid leakage.
    """
    train = train.copy()
    test = test.copy()

    mu = train[feature_cols].mean(numeric_only=True)
    sigma = train[feature_cols].std(numeric_only=True).replace(0, np.nan)

    train_std = (train[feature_cols] - mu) / sigma
    test_std = (test[feature_cols] - mu) / sigma

    for c in feature_cols:
        train[c] = train_std[c]
        test[c] = test_std[c]

    return train, test


def save_split(df: pd.DataFrame, name: str) -> None:
    """
    Save a split to disk.
    """
    out_csv = OUT_DIR / f"{name}.csv"
    df.to_csv(out_csv, index=False)

    try:
        df.to_parquet(OUT_DIR / f"{name}.parquet", index=False)
    except Exception as e:
        print(f"Parquet not written for {name}. {e}")

    print(f"Saved: {out_csv}")


def main() -> None:
    df = load_panel()
    df = add_simple_features(df)
    df = make_target(df)

    train, test = split_by_date(df)

    feature_cols = [
        "ret_1d",
        "ret_5d",
        "ret_20d",
        "vol_20d",
        "vol_60d",
        "Volume",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "Revenues",
        "NetIncomeLoss",
        "OperatingIncomeLoss",
        "EarningsPerShareBasic",
        "CommonStockSharesOutstanding",
    ]

    for c in feature_cols:
        if c not in train.columns:
            train[c] = np.nan
            test[c] = np.nan

    train, test = standardise(train, test, feature_cols)

    train = train.dropna(subset=["target_ret_1d"])
    test = test.dropna(subset=["target_ret_1d"])

    save_split(train, "train_2022_2024")
    save_split(test, "test_2025")

    meta = {
        "train_end": TRAIN_END,
        "test_start": TEST_START,
        "test_end": TEST_END,
        "n_train_rows": int(len(train)),
        "n_test_rows": int(len(test)),
        "tickers_train": sorted(train["ticker"].unique().tolist()),
        "tickers_test": sorted(test["ticker"].unique().tolist()),
        "feature_cols": feature_cols,
        "target_col": "target_ret_1d",
    }
    (OUT_DIR / "split_meta.json").write_text(pd.Series(meta).to_json(), encoding="utf-8")
    print("Done")


if __name__ == "__main__":
    main()

# Splits (extra step)
# - data/processed/splits/train_2022_2024.csv
# - data/processed/splits/test_2025.csv
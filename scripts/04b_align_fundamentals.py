import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
from config import PROCESSED_DIR, RAW_DIR

FACTS_DIR = RAW_DIR / "sec" / "companyfacts"
RETURNS_DIR = PROCESSED_DIR / "prices"
OUT_DIR = PROCESSED_DIR / "panel"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONCEPTS = [
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
]


def load_daily_returns() -> pd.DataFrame:
    """
    Load daily returns created in Step 04a.
    Expected columns include: date, ticker, adj_close, ret_1d, log_ret_1d, Volume.
    """
    p = RETURNS_DIR / "daily_returns.csv"
    df = pd.read_csv(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def load_companyfacts() -> pd.DataFrame:
    """
    Load CompanyFacts extracted in Step 03a.
    Expected columns include: ticker, concept, unit, value, form, fy, fp, end, filed, accn.
    """
    p = FACTS_DIR / "companyfacts_2022_2025.csv"
    df = pd.read_csv(p, dtype={"ticker": str, "concept": str, "unit": str}, low_memory=False)

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["concept"] = df["concept"].astype(str).str.strip()
    df["unit"] = df["unit"].astype(str).str.strip()

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce")

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["ticker", "concept", "end", "filed"])
    df = df.sort_values(["ticker", "concept", "filed", "end"]).reset_index(drop=True)
    return df


def filter_concepts(df: pd.DataFrame, concepts: list[str]) -> pd.DataFrame:
    """
    Keep only a small set of concepts to make a stable feature table.
    """
    return df[df["concept"].isin(concepts)].copy()


def choose_single_unit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Many concepts appear with multiple units for the same ticker (for example USD vs USD/shares).
    This function picks one unit per (ticker, concept), using the most frequent unit in the data.

    Output is filtered to only the chosen unit for each (ticker, concept).
    """
    counts = (
        df.groupby(["ticker", "concept", "unit"])
        .size()
        .reset_index(name="n")
        .sort_values(["ticker", "concept", "n"], ascending=[True, True, False])
    )

    chosen = counts.drop_duplicates(subset=["ticker", "concept"])[["ticker", "concept", "unit"]]
    out = df.merge(chosen, on=["ticker", "concept", "unit"], how="inner")
    return out


def keep_latest_per_filed_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given (ticker, concept, filed date), there can still be multiple rows.
    We keep the last row after sorting by end date.

    This reduces duplicates before we pivot to wide format.
    """
    df = df.sort_values(["ticker", "concept", "filed", "end"])
    df = df.drop_duplicates(subset=["ticker", "concept", "filed"], keep="last")
    return df


def fundamentals_wide_by_filed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert long facts into a wide table keyed by (ticker, filed).
    Each concept becomes a column, values are the latest numeric values at that filing date.
    """
    wide = df.pivot_table(
        index=["ticker", "filed"],
        columns="concept",
        values="value",
        aggfunc="last",
    ).reset_index()

    wide = wide.sort_values(["ticker", "filed"]).reset_index(drop=True)
    return wide


def merge_fundamentals_daily(daily: pd.DataFrame, wide_facts: pd.DataFrame) -> pd.DataFrame:
    """
    Merge fundamentals into daily data using a backward asof join on filed date.

    For each trading day, we attach the latest fundamentals that were filed on or before that day.
    If a ticker has no fundamentals, we still output the same columns, filled with missing values.
    """
    base_cols = list(daily.columns)

    out_parts = []
    for t, df_t in daily.groupby("ticker", sort=False):
        df_t = df_t.sort_values("date").copy()

        facts_t = wide_facts[wide_facts["ticker"] == t].copy()
        facts_t = facts_t.sort_values("filed") if not facts_t.empty else facts_t

        if facts_t.empty:
            df_t["filed"] = pd.NaT
            for c in CONCEPTS:
                df_t[c] = np.nan
            df_t = df_t.reindex(columns=base_cols + ["filed"] + CONCEPTS)
            out_parts.append(df_t)
            continue

        merged = pd.merge_asof(
            df_t,
            facts_t.drop(columns=["ticker"]),
            left_on="date",
            right_on="filed",
            direction="backward",
            allow_exact_matches=True,
        )

        for c in CONCEPTS:
            if c not in merged.columns:
                merged[c] = np.nan

        merged = merged.reindex(columns=base_cols + ["filed"] + CONCEPTS)
        out_parts.append(merged)

    out = pd.concat(out_parts, ignore_index=True)
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out



def save_outputs(panel: pd.DataFrame, wide_facts: pd.DataFrame) -> None:
    """
    Save the daily panel dataset and the wide fundamentals table.
    """
    panel_csv = OUT_DIR / "daily_panel_prices_returns_fundamentals.csv"
    panel.to_csv(panel_csv, index=False)

    facts_csv = OUT_DIR / "fundamentals_wide_by_filed.csv"
    wide_facts.to_csv(facts_csv, index=False)

    try:
        panel.to_parquet(OUT_DIR / "daily_panel_prices_returns_fundamentals.parquet", index=False)
        wide_facts.to_parquet(OUT_DIR / "fundamentals_wide_by_filed.parquet", index=False)
    except Exception as e:
        print(f"Parquet not written. {e}")

    print(f"Saved: {panel_csv}")
    print(f"Saved: {facts_csv}")


def main() -> None:
    daily = load_daily_returns()
    facts = load_companyfacts()
    facts = filter_concepts(facts, CONCEPTS)
    facts = choose_single_unit(facts)
    facts = keep_latest_per_filed_date(facts)

    wide = fundamentals_wide_by_filed(facts)
    panel = merge_fundamentals_daily(daily, wide)

    save_outputs(panel, wide)
    print("Done")


if __name__ == "__main__":
    main()

from __future__ import annotations

import sqlite3
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from eu_paths import (
    LEGACY_FUNDAMENTALS_DIR,
    LEGACY_REPORTS_DIR,
    LEGACY_PRICES_DIR,
    PROCESSED_MCP_DB_PATH,
    PROCESSED_MCP_DIRS,
    PROCESSED_PANEL_DB_PATH,
    PROCESSED_PANEL_DIRS,
    PROCESSED_PRICES_DIRS,
    RAW_FUNDAMENTALS_DIRS,
    RAW_PRICES_DB_PATH,
    RAW_PRICES_DIRS,
    RAW_REPORTS_DIRS,
    RAW_SEC_COMPANYFACTS_DB_PATH,
    RAW_SEC_COMPANYFACTS_DIRS,
    RAW_SEC_DIRS,
    ensure_dirs,
)


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

CONCEPT_ITEM_PRIORITY: dict[str, list[str]] = {
    "Assets": ["Total Assets", "Assets", "Current Assets"],
    "Liabilities": [
        "Total Liabilities Net Minority Interest",
        "Liabilities",
        "Current Liabilities",
        "Total Non Current Liabilities Net Minority Interest",
    ],
    "StockholdersEquity": [
        "Stockholders Equity",
        "Common Stock Equity",
        "Total Equity Gross Minority Interest",
    ],
    "Revenues": ["Total Revenue", "Operating Revenue", "Revenues"],
    "NetIncomeLoss": [
        "Net Income",
        "Net Income Common Stockholders",
        "Net Income From Continuing Operation Net Minority Interest",
        "Net Income From Continuing Operations",
        "Net Income Including Noncontrolling Interests",
    ],
    "OperatingIncomeLoss": ["Operating Income", "Total Operating Income As Reported"],
    "EarningsPerShareBasic": ["Basic EPS"],
    "CommonStockSharesOutstanding": ["Ordinary Shares Number", "Share Issued", "Basic Average Shares"],
}

CONCEPT_UNIT = {
    "Assets": "LOCAL_CURRENCY",
    "Liabilities": "LOCAL_CURRENCY",
    "StockholdersEquity": "LOCAL_CURRENCY",
    "Revenues": "LOCAL_CURRENCY",
    "NetIncomeLoss": "LOCAL_CURRENCY",
    "OperatingIncomeLoss": "LOCAL_CURRENCY",
    "EarningsPerShareBasic": "LOCAL_CURRENCY_PER_SHARE",
    "CommonStockSharesOutstanding": "shares",
}


def _pick_file(candidates: list[Path]) -> Path:
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Could not find any of: {candidates}")


def _pick_dir_with_pattern(candidates: list[Path], pattern: str) -> Path:
    for p in candidates:
        if p.exists() and any(p.glob(pattern)):
            return p
    raise FileNotFoundError(f"No files matching '{pattern}' in candidates: {candidates}")


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _first_present(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _normalise_prices_long(prices_long_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(prices_long_csv, low_memory=False)

    date_col = _first_present(df, ["Date", "date", "TRADE_DATE"])
    ticker_col = _first_present(df, ["ticker", "TICKER"])
    open_col = _first_present(df, ["Open", "open", "OPEN"])
    high_col = _first_present(df, ["High", "high", "HIGH"])
    low_col = _first_present(df, ["Low", "low", "LOW"])
    close_col = _first_present(df, ["Close", "close", "CLOSE"])
    adj_col = _first_present(df, ["Adj Close", "adj_close", "ADJ_CLOSE", "Adj_Close"])
    volume_col = _first_present(df, ["Volume", "volume", "VOLUME"])

    if date_col is None or ticker_col is None:
        raise ValueError(f"Unexpected prices schema in {prices_long_csv}. Columns: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "CIK10": None,
            "COMPANY_TITLE": None,
            "TICKER": df[ticker_col].astype(str).str.upper().str.strip(),
            "TRADE_DATE": pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d"),
            "OPEN": _to_numeric(df[open_col]) if open_col else np.nan,
            "HIGH": _to_numeric(df[high_col]) if high_col else np.nan,
            "LOW": _to_numeric(df[low_col]) if low_col else np.nan,
            "CLOSE": _to_numeric(df[close_col]) if close_col else np.nan,
            "ADJ_CLOSE": _to_numeric(df[adj_col]) if adj_col else np.nan,
            "VOLUME": pd.to_numeric(df[volume_col], errors="coerce").astype("Int64") if volume_col else pd.Series(pd.NA, index=df.index, dtype="Int64"),
        }
    )

    out = out.dropna(subset=["TICKER", "TRADE_DATE"]).copy()
    out = out.drop_duplicates(subset=["TICKER", "TRADE_DATE"], keep="last").sort_values(["TICKER", "TRADE_DATE"]).reset_index(drop=True)
    out = out.where(pd.notnull(out), None)
    return out


def _normalise_daily_returns(daily_returns_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(daily_returns_csv, low_memory=False)

    date_col = _first_present(df, ["date", "Date", "TRADE_DATE"])
    ticker_col = _first_present(df, ["ticker", "TICKER"])
    adj_col = _first_present(df, ["adj_close", "Adj Close", "ADJ_CLOSE"])
    ret_col = _first_present(df, ["ret_1d", "daily_return", "RET_1D"])
    log_col = _first_present(df, ["log_ret_1d", "LOG_RET_1D"])
    volume_col = _first_present(df, ["Volume", "volume", "VOLUME"])

    if date_col is None or ticker_col is None:
        raise ValueError(f"Unexpected daily returns schema in {daily_returns_csv}. Columns: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "TICKER": df[ticker_col].astype(str).str.upper().str.strip(),
            "TRADE_DATE": pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d"),
            "ADJ_CLOSE": _to_numeric(df[adj_col]) if adj_col else np.nan,
            "RET_1D": _to_numeric(df[ret_col]) if ret_col else np.nan,
            "LOG_RET_1D": _to_numeric(df[log_col]) if log_col else np.nan,
            "VOLUME": pd.to_numeric(df[volume_col], errors="coerce").astype("Int64") if volume_col else pd.Series(pd.NA, index=df.index, dtype="Int64"),
        }
    )

    out = out.dropna(subset=["TICKER", "TRADE_DATE"]).copy()
    out = out.drop_duplicates(subset=["TICKER", "TRADE_DATE"], keep="last").sort_values(["TICKER", "TRADE_DATE"]).reset_index(drop=True)
    out = out.where(pd.notnull(out), None)
    return out


def _create_prices_and_returns_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = """
    CREATE TABLE IF NOT EXISTS US_PRICES (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        CIK10 TEXT,
        COMPANY_TITLE TEXT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        OPEN REAL,
        HIGH REAL,
        LOW REAL,
        CLOSE REAL,
        ADJ_CLOSE REAL,
        VOLUME INTEGER,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_us_prices_ticker ON US_PRICES (TICKER);
    CREATE INDEX IF NOT EXISTS idx_us_prices_trade_date ON US_PRICES (TRADE_DATE);
    CREATE INDEX IF NOT EXISTS idx_us_prices_cik10 ON US_PRICES (CIK10);

    CREATE TABLE IF NOT EXISTS US_RETURNS (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        ADJ_CLOSE REAL,
        RET_1D REAL,
        LOG_RET_1D REAL,
        VOLUME INTEGER,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_us_returns_ticker ON US_RETURNS (TICKER);
    CREATE INDEX IF NOT EXISTS idx_us_returns_trade_date ON US_RETURNS (TRADE_DATE);
    """
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(schema)
        con.commit()


def _insert_prices(db_path: Path, prices: pd.DataFrame) -> None:
    sql = """
    INSERT OR REPLACE INTO US_PRICES
    (CIK10, COMPANY_TITLE, TICKER, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    rows = list(prices[["CIK10", "COMPANY_TITLE", "TICKER", "TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]].itertuples(index=False, name=None))
    with sqlite3.connect(str(db_path)) as con:
        con.executemany(sql, rows)
        con.commit()
    print(f"Inserted US_PRICES rows: {len(rows)}")


def _insert_returns(db_path: Path, returns_df: pd.DataFrame) -> None:
    sql = """
    INSERT OR REPLACE INTO US_RETURNS
    (TICKER, TRADE_DATE, ADJ_CLOSE, RET_1D, LOG_RET_1D, VOLUME)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    rows = list(returns_df[["TICKER", "TRADE_DATE", "ADJ_CLOSE", "RET_1D", "LOG_RET_1D", "VOLUME"]].itertuples(index=False, name=None))
    with sqlite3.connect(str(db_path)) as con:
        con.executemany(sql, rows)
        con.commit()
    print(f"Inserted US_RETURNS rows: {len(rows)}")


def _load_reports_long(report_root: Path) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    files = sorted(report_root.glob("*/*/*_long.csv"))
    if not files:
        return pd.DataFrame(columns=["ticker", "period_type", "item", "report_date", "value"])

    for p in files:
        df = pd.read_csv(p, low_memory=False)
        if df.empty:
            continue

        ticker_col = _first_present(df, ["ticker", "TICKER"])
        period_col = _first_present(df, ["period_type", "period"])
        item_col = _first_present(df, ["item", "concept"])
        date_col = _first_present(df, ["report_date", "date", "END_DATE"])
        value_col = _first_present(df, ["value", "VALUE_REAL"])

        if item_col is None or date_col is None or value_col is None:
            continue

        ticker_default = p.parent.name
        period_default = p.parent.parent.name

        part = pd.DataFrame(
            {
                "ticker": df[ticker_col].astype(str) if ticker_col else ticker_default,
                "period_type": df[period_col].astype(str) if period_col else period_default,
                "item": df[item_col].astype(str).str.strip(),
                "report_date": pd.to_datetime(df[date_col], errors="coerce"),
                "value": _to_numeric(df[value_col]),
            }
        )
        parts.append(part)

    if not parts:
        return pd.DataFrame(columns=["ticker", "period_type", "item", "report_date", "value"])

    out = pd.concat(parts, ignore_index=True)
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["period_type"] = out["period_type"].astype(str).str.lower().str.strip()
    out = out.dropna(subset=["ticker", "report_date", "item", "value"])
    return out.sort_values(["ticker", "period_type", "report_date"]).reset_index(drop=True)


def _facts_from_reports(reports: pd.DataFrame) -> pd.DataFrame:
    if reports.empty:
        return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value"])

    base = reports.copy()
    base["item_norm"] = base["item"].astype(str).str.strip().str.lower()

    out_parts: list[pd.DataFrame] = []
    for concept, candidates in CONCEPT_ITEM_PRIORITY.items():
        rank = {c.lower(): i for i, c in enumerate(candidates)}
        part = base[base["item_norm"].isin(rank)].copy()
        if part.empty:
            continue

        part["prio"] = part["item_norm"].map(rank).astype(int)
        part = part.sort_values(["ticker", "period_type", "report_date", "prio"])
        part = part.drop_duplicates(subset=["ticker", "period_type", "report_date"], keep="first")
        part = part[["ticker", "period_type", "report_date", "value"]].copy()
        part["concept"] = concept
        out_parts.append(part)

    if not out_parts:
        return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value"])

    out = pd.concat(out_parts, ignore_index=True)
    out["source_rank"] = 0
    return out


def _facts_from_fundamentals(fund_dir: Path) -> pd.DataFrame:
    combined_path = fund_dir / "all_fundamentals.csv"
    if combined_path.exists():
        df = pd.read_csv(combined_path, low_memory=False)
    else:
        files = sorted(fund_dir.glob("*_fundamentals.csv"))
        if not files:
            return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value"])
        df = pd.concat((pd.read_csv(p, low_memory=False) for p in files), ignore_index=True)

    if df.empty:
        return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value"])

    ticker_col = _first_present(df, ["ticker", "TICKER"])
    date_col = _first_present(df, ["report_date", "date"])
    if ticker_col is None or date_col is None:
        return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value"])

    concept_cols = [
        ("total_assets", "Assets"),
        ("revenue", "Revenues"),
        ("net_income", "NetIncomeLoss"),
        ("ebit", "OperatingIncomeLoss"),
    ]

    parts: list[pd.DataFrame] = []
    for source_col, concept in concept_cols:
        if source_col not in df.columns:
            continue
        part = pd.DataFrame(
            {
                "ticker": df[ticker_col].astype(str).str.upper().str.strip(),
                "period_type": "annual",
                "report_date": pd.to_datetime(df[date_col], errors="coerce"),
                "concept": concept,
                "value": _to_numeric(df[source_col]),
                "source_rank": 1,
            }
        )
        part = part.dropna(subset=["ticker", "report_date", "value"])
        parts.append(part)

    if not parts:
        return pd.DataFrame(columns=["ticker", "period_type", "report_date", "concept", "value", "source_rank"])

    return pd.concat(parts, ignore_index=True)


def _build_sec_companyfacts(report_root: Path, fundamentals_root: Path) -> pd.DataFrame:
    report_facts = _facts_from_reports(reports=_load_reports_long(report_root=report_root))
    fundamentals_facts = _facts_from_fundamentals(fund_dir=fundamentals_root)

    all_facts = pd.concat([report_facts, fundamentals_facts], ignore_index=True)
    if all_facts.empty:
        raise RuntimeError("No EU facts found from reports/fundamentals to build SEC_COMPANYFACTS.")

    all_facts["period_rank"] = all_facts["period_type"].map({"quarterly": 0, "annual": 1}).fillna(2).astype(int)
    all_facts = all_facts.sort_values(["ticker", "concept", "report_date", "source_rank", "period_rank"])
    all_facts = all_facts.drop_duplicates(subset=["ticker", "concept", "report_date"], keep="first").reset_index(drop=True)

    end_date = pd.to_datetime(all_facts["report_date"], errors="coerce")
    fy = end_date.dt.year
    fp = np.where(all_facts["period_type"].astype(str).str.lower().eq("quarterly"), "Q" + end_date.dt.quarter.astype("Int64").astype(str), "FY")
    filed_date = end_date.dt.strftime("%Y-%m-%d")

    sec = pd.DataFrame(
        {
            "TICKER": all_facts["ticker"].astype(str).str.upper().str.strip(),
            "CIK": None,
            "CONCEPT": all_facts["concept"].astype(str),
            "UNIT": all_facts["concept"].map(CONCEPT_UNIT).fillna("LOCAL_CURRENCY"),
            "VALUE_REAL": _to_numeric(all_facts["value"]),
            "FORM": np.where(all_facts["period_type"].astype(str).str.lower().eq("quarterly"), "QUARTERLY", "ANNUAL"),
            "FY": fy.astype("Int64"),
            "FP": fp,
            "START_DATE": None,
            "END_DATE": filed_date,
            "FILED_DATE": filed_date,
            "ACCN": (
                all_facts["ticker"].astype(str).str.upper().str.strip()
                + "-"
                + all_facts["period_type"].astype(str).str.upper().str.strip()
                + "-"
                + filed_date.fillna("NA")
            ),
            "FRAME": None,
        }
    )

    sec = sec.dropna(subset=["TICKER", "CONCEPT", "END_DATE", "VALUE_REAL"]).copy()
    sec = sec.drop_duplicates(subset=["TICKER", "CONCEPT", "END_DATE", "FORM"], keep="first")
    sec = sec.sort_values(["TICKER", "END_DATE", "CONCEPT"]).reset_index(drop=True)
    return sec


def _save_sec_companyfacts_csvs(sec: pd.DataFrame, companyfacts_dir: Path) -> None:
    companyfacts_dir.mkdir(parents=True, exist_ok=True)

    for ticker, df_t in sec.groupby("TICKER"):
        out_path = companyfacts_dir / f"{ticker}_companyfacts.csv"
        tmp = df_t.rename(
            columns={
                "TICKER": "ticker",
                "CIK": "cik",
                "CONCEPT": "concept",
                "UNIT": "unit",
                "VALUE_REAL": "value",
                "FORM": "form",
                "FY": "fy",
                "FP": "fp",
                "START_DATE": "start",
                "END_DATE": "end",
                "FILED_DATE": "filed",
                "ACCN": "accn",
                "FRAME": "frame",
            }
        )
        tmp.to_csv(out_path, index=False)

    years = pd.to_datetime(sec["END_DATE"], errors="coerce").dt.year.dropna()
    if not years.empty:
        start_y = int(years.min())
        end_y = int(years.max())
    else:
        start_y, end_y = 2022, 2025

    combined = sec.rename(
        columns={
            "TICKER": "ticker",
            "CIK": "cik",
            "CONCEPT": "concept",
            "UNIT": "unit",
            "VALUE_REAL": "value",
            "FORM": "form",
            "FY": "fy",
            "FP": "fp",
            "START_DATE": "start",
            "END_DATE": "end",
            "FILED_DATE": "filed",
            "ACCN": "accn",
            "FRAME": "frame",
        }
    )
    combined_path = companyfacts_dir / f"companyfacts_{start_y}_{end_y}.csv"
    combined.to_csv(combined_path, index=False)
    print(f"Saved SEC-style CSVs under: {companyfacts_dir}")


def _save_sec_companyfacts_db(sec: pd.DataFrame, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(str(db_path)) as con:
        sec.to_sql("SEC_COMPANYFACTS", con, if_exists="replace", index=False)
        con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_concept_end ON SEC_COMPANYFACTS (TICKER, CONCEPT, END_DATE)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_end ON SEC_COMPANYFACTS (TICKER, END_DATE)")
        con.commit()
    print(f"Saved SEC_COMPANYFACTS DB: {db_path}")


def _build_wide_facts(sec: pd.DataFrame) -> pd.DataFrame:
    df = sec[sec["CONCEPT"].isin(CONCEPTS)].copy()
    df["FILED_DATE"] = pd.to_datetime(df["FILED_DATE"], errors="coerce")
    df = df.dropna(subset=["TICKER", "FILED_DATE", "CONCEPT", "VALUE_REAL"]).sort_values(["TICKER", "CONCEPT", "FILED_DATE"])
    df = df.drop_duplicates(subset=["TICKER", "CONCEPT", "FILED_DATE"], keep="last")

    wide = (
        df.pivot_table(
            index=["TICKER", "FILED_DATE"],
            columns="CONCEPT",
            values="VALUE_REAL",
            aggfunc="last",
        )
        .reset_index()
        .sort_values(["TICKER", "FILED_DATE"])
        .reset_index(drop=True)
    )

    for c in CONCEPTS:
        if c not in wide.columns:
            wide[c] = np.nan

    return wide[["TICKER", "FILED_DATE", *CONCEPTS]]


def _build_daily_panel(returns_df: pd.DataFrame, wide_facts: pd.DataFrame) -> pd.DataFrame:
    daily = pd.DataFrame(
        {
            "ticker": returns_df["TICKER"].astype(str).str.upper().str.strip(),
            "date": pd.to_datetime(returns_df["TRADE_DATE"], errors="coerce"),
            "adj_close": _to_numeric(returns_df["ADJ_CLOSE"]),
            "ret_1d": _to_numeric(returns_df["RET_1D"]),
            "log_ret_1d": _to_numeric(returns_df["LOG_RET_1D"]),
            "Volume": pd.to_numeric(returns_df["VOLUME"], errors="coerce").astype("Int64"),
        }
    )
    daily = daily.dropna(subset=["ticker", "date"]).sort_values(["ticker", "date"]).reset_index(drop=True)

    out_parts: list[pd.DataFrame] = []
    for ticker, df_t in daily.groupby("ticker", sort=False):
        facts_t = wide_facts[wide_facts["TICKER"] == ticker].copy()
        if facts_t.empty:
            part = df_t.copy()
            part["filed"] = pd.NaT
            for c in CONCEPTS:
                part[c] = np.nan
            out_parts.append(part)
            continue

        facts_t = facts_t.sort_values("FILED_DATE").rename(columns={"FILED_DATE": "filed"})
        merged = pd.merge_asof(
            df_t.sort_values("date"),
            facts_t.drop(columns=["TICKER"]).sort_values("filed"),
            left_on="date",
            right_on="filed",
            direction="backward",
            allow_exact_matches=True,
        )
        for c in CONCEPTS:
            if c not in merged.columns:
                merged[c] = np.nan
        out_parts.append(merged)

    panel = pd.concat(out_parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    panel = panel[["ticker", "date", "adj_close", "ret_1d", "log_ret_1d", "Volume", "filed", *CONCEPTS]]
    return panel


def _build_monthly_panel_from_daily(daily_panel: pd.DataFrame) -> pd.DataFrame:
    monthly = daily_panel.copy()
    monthly["date"] = pd.to_datetime(monthly["date"], errors="coerce")
    monthly = monthly.dropna(subset=["ticker", "date"]).sort_values(["ticker", "date"]).reset_index(drop=True)

    monthly["year_month"] = monthly["date"].dt.to_period("M").astype(str)
    monthly = monthly.groupby(["ticker", "year_month"], as_index=False).first().drop(columns=["year_month"]).reset_index(drop=True)

    # Keep monthly schema aligned with US workflow table columns.
    monthly["price_open"] = monthly["adj_close"]
    monthly["price_close"] = monthly["adj_close"]
    monthly["price_avg"] = monthly["adj_close"]
    monthly["price_min"] = monthly["adj_close"]
    monthly["price_max"] = monthly["adj_close"]
    monthly["price_volume"] = monthly["Volume"]
    return monthly


def _save_panel_csvs(daily_panel: pd.DataFrame, wide_facts: pd.DataFrame, monthly_panel: pd.DataFrame, panel_dir: Path) -> None:
    panel_dir.mkdir(parents=True, exist_ok=True)

    daily_out = daily_panel.copy()
    daily_out["date"] = pd.to_datetime(daily_out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily_out["filed"] = pd.to_datetime(daily_out["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily_out.to_csv(panel_dir / "daily_panel_prices_returns_fundamentals.csv", index=False)

    wide_out = wide_facts.copy()
    wide_out = wide_out.rename(columns={"TICKER": "ticker", "FILED_DATE": "filed"})
    wide_out["filed"] = pd.to_datetime(wide_out["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    wide_out.to_csv(panel_dir / "fundamentals_wide_by_filed.csv", index=False)

    monthly_out = monthly_panel.copy()
    monthly_out["date"] = pd.to_datetime(monthly_out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    monthly_out["filed"] = pd.to_datetime(monthly_out["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    monthly_out.to_csv(panel_dir / "monthly_panel_prices_returns_fundamentals.csv", index=False)

    try:
        daily_out.to_parquet(panel_dir / "daily_panel_prices_returns_fundamentals.parquet", index=False)
        wide_out.to_parquet(panel_dir / "fundamentals_wide_by_filed.parquet", index=False)
        monthly_out.to_parquet(panel_dir / "monthly_panel_prices_returns_fundamentals.parquet", index=False)
    except Exception as exc:
        print(f"Parquet not written for one or more panel outputs: {exc}")

    print(f"Saved panel CSV/Parquet outputs under: {panel_dir}")


def _create_panel_schema(panel_db_path: Path) -> None:
    panel_db_path.parent.mkdir(parents=True, exist_ok=True)
    if panel_db_path.exists():
        panel_db_path.unlink()

    schema = """
    CREATE TABLE IF NOT EXISTS US_DAILY_PANEL (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        TRADE_DATE TEXT NOT NULL,
        ADJ_CLOSE REAL,
        RET_1D REAL,
        LOG_RET_1D REAL,
        VOLUME INTEGER,
        FILED_DATE TEXT,
        Assets REAL,
        Liabilities REAL,
        StockholdersEquity REAL,
        Revenues REAL,
        NetIncomeLoss REAL,
        OperatingIncomeLoss REAL,
        EarningsPerShareBasic REAL,
        CommonStockSharesOutstanding REAL,
        UNIQUE(TICKER, TRADE_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_panel_ticker ON US_DAILY_PANEL (TICKER);
    CREATE INDEX IF NOT EXISTS idx_panel_trade_date ON US_DAILY_PANEL (TRADE_DATE);

    CREATE TABLE IF NOT EXISTS US_FUNDAMENTALS_WIDE_BY_FILED (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TICKER TEXT NOT NULL,
        FILED_DATE TEXT NOT NULL,
        Assets REAL,
        Liabilities REAL,
        StockholdersEquity REAL,
        Revenues REAL,
        NetIncomeLoss REAL,
        OperatingIncomeLoss REAL,
        EarningsPerShareBasic REAL,
        CommonStockSharesOutstanding REAL,
        UNIQUE(TICKER, FILED_DATE)
    );

    CREATE INDEX IF NOT EXISTS idx_wide_ticker ON US_FUNDAMENTALS_WIDE_BY_FILED (TICKER);
    CREATE INDEX IF NOT EXISTS idx_wide_filed_date ON US_FUNDAMENTALS_WIDE_BY_FILED (FILED_DATE);
    """

    with sqlite3.connect(str(panel_db_path)) as con:
        con.executescript(schema)
        con.commit()


def _insert_panel_tables(panel_db_path: Path, daily_panel: pd.DataFrame, wide_facts: pd.DataFrame, monthly_panel: pd.DataFrame) -> None:
    daily = daily_panel.copy()
    daily["TRADE_DATE"] = pd.to_datetime(daily["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily["FILED_DATE"] = pd.to_datetime(daily["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily["TICKER"] = daily["ticker"].astype(str).str.upper().str.strip()
    daily["VOLUME"] = pd.to_numeric(daily["Volume"], errors="coerce").astype("Int64")
    daily = daily.rename(
        columns={
            "adj_close": "ADJ_CLOSE",
            "ret_1d": "RET_1D",
            "log_ret_1d": "LOG_RET_1D",
        }
    )
    daily_cols = ["TICKER", "TRADE_DATE", "ADJ_CLOSE", "RET_1D", "LOG_RET_1D", "VOLUME", "FILED_DATE", *CONCEPTS]
    for c in CONCEPTS:
        if c not in daily.columns:
            daily[c] = np.nan
    daily = daily[daily_cols].where(pd.notnull(daily[daily_cols]), None)

    wide = wide_facts.copy()
    wide["FILED_DATE"] = pd.to_datetime(wide["FILED_DATE"], errors="coerce").dt.strftime("%Y-%m-%d")
    wide_cols = ["TICKER", "FILED_DATE", *CONCEPTS]
    for c in CONCEPTS:
        if c not in wide.columns:
            wide[c] = np.nan
    wide = wide[wide_cols].where(pd.notnull(wide[wide_cols]), None)

    insert_daily = """
    INSERT OR REPLACE INTO US_DAILY_PANEL
    (TICKER, TRADE_DATE, ADJ_CLOSE, RET_1D, LOG_RET_1D, VOLUME, FILED_DATE,
     Assets, Liabilities, StockholdersEquity, Revenues, NetIncomeLoss, OperatingIncomeLoss,
     EarningsPerShareBasic, CommonStockSharesOutstanding)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    insert_wide = """
    INSERT OR REPLACE INTO US_FUNDAMENTALS_WIDE_BY_FILED
    (TICKER, FILED_DATE,
     Assets, Liabilities, StockholdersEquity, Revenues, NetIncomeLoss, OperatingIncomeLoss,
     EarningsPerShareBasic, CommonStockSharesOutstanding)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    monthly_out = monthly_panel.copy()
    monthly_out["ticker"] = monthly_out["ticker"].astype(str).str.upper().str.strip()
    monthly_out["date"] = pd.to_datetime(monthly_out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    monthly_out["filed"] = pd.to_datetime(monthly_out["filed"], errors="coerce").dt.strftime("%Y-%m-%d")
    monthly_out = monthly_out.where(pd.notnull(monthly_out), None)

    with sqlite3.connect(str(panel_db_path)) as con:
        con.executemany(insert_daily, list(daily.itertuples(index=False, name=None)))
        con.executemany(insert_wide, list(wide.itertuples(index=False, name=None)))
        monthly_out.to_sql("US_MONTHLY_PANEL", con, if_exists="replace", index=False)
        con.execute("CREATE INDEX IF NOT EXISTS idx_US_MONTHLY_PANEL_ticker_date ON US_MONTHLY_PANEL (ticker, date)")
        con.commit()

    print(f"Saved panel DB tables at: {panel_db_path}")


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _copy_table_from_db(src_path: Path, table: str, dst_con: sqlite3.Connection) -> None:
    with _connect_readonly(db_path=src_path) as src_con:
        chunks = pd.read_sql_query(f'SELECT * FROM "{table}"', src_con, chunksize=200_000)
        first = True
        copied = 0
        for chunk in chunks:
            chunk.to_sql(table, dst_con, if_exists="replace" if first else "append", index=False)
            copied += len(chunk)
            first = False
    print(f"Copied {copied} rows into {table}")


def _create_mcp_indexes(dst_con: sqlite3.Connection) -> None:
    dst_con.execute("PRAGMA temp_store=MEMORY")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_concept_end ON SEC_COMPANYFACTS (TICKER, CONCEPT, END_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_sec_ticker_end ON SEC_COMPANYFACTS (TICKER, END_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON US_PRICES (TICKER, TRADE_DATE)")
    dst_con.execute("CREATE INDEX IF NOT EXISTS idx_returns_ticker_date ON US_RETURNS (TICKER, TRADE_DATE)")


def _build_consolidated_mcp_db(panel_db_path: Path, prices_db_path: Path, sec_db_path: Path, out_db_path: Path) -> None:
    if not panel_db_path.exists():
        raise FileNotFoundError(panel_db_path)
    if not prices_db_path.exists():
        raise FileNotFoundError(prices_db_path)
    if not sec_db_path.exists():
        raise FileNotFoundError(sec_db_path)

    out_db_path.parent.mkdir(parents=True, exist_ok=True)
    if out_db_path.exists():
        out_db_path.unlink()

    shutil.copyfile(panel_db_path, out_db_path)
    with sqlite3.connect(str(out_db_path)) as dst_con:
        _copy_table_from_db(src_path=sec_db_path, table="SEC_COMPANYFACTS", dst_con=dst_con)
        _copy_table_from_db(src_path=prices_db_path, table="US_PRICES", dst_con=dst_con)
        _copy_table_from_db(src_path=prices_db_path, table="US_RETURNS", dst_con=dst_con)
        dst_con.commit()
        _create_mcp_indexes(dst_con=dst_con)
        dst_con.commit()

    print(f"Saved consolidated MCP DB: {out_db_path}")


def main() -> None:
    ensure_dirs(
        paths=[
            *RAW_SEC_DIRS,
            *RAW_SEC_COMPANYFACTS_DIRS,
            *PROCESSED_MCP_DIRS,
            *PROCESSED_PANEL_DIRS,
        ]
    )

    prices_csv = _pick_file(
        candidates=[
            RAW_PRICES_DIRS[0] / "all_prices_long.csv",
            LEGACY_PRICES_DIR / "all_prices_long.csv",
        ]
    )
    daily_returns_csv = _pick_file(
        candidates=[
            PROCESSED_PRICES_DIRS[0] / "daily_returns.csv",
        ]
    )
    reports_root = _pick_dir_with_pattern(candidates=[*RAW_REPORTS_DIRS, LEGACY_REPORTS_DIR], pattern="*/*/*_long.csv")
    fundamentals_root = _pick_dir_with_pattern(
        candidates=[*RAW_FUNDAMENTALS_DIRS, LEGACY_FUNDAMENTALS_DIR],
        pattern="*_fundamentals.csv",
    )

    print("Building EU prices + returns DB tables...")
    prices = _normalise_prices_long(prices_long_csv=prices_csv)
    returns_df = _normalise_daily_returns(daily_returns_csv=daily_returns_csv)
    _create_prices_and_returns_tables(db_path=RAW_PRICES_DB_PATH)
    _insert_prices(db_path=RAW_PRICES_DB_PATH, prices=prices)
    _insert_returns(db_path=RAW_PRICES_DB_PATH, returns_df=returns_df)

    print("Building EU SEC-style companyfacts DB...")
    sec = _build_sec_companyfacts(report_root=reports_root, fundamentals_root=fundamentals_root)
    _save_sec_companyfacts_csvs(sec=sec, companyfacts_dir=RAW_SEC_COMPANYFACTS_DIRS[0])
    _save_sec_companyfacts_db(sec=sec, db_path=RAW_SEC_COMPANYFACTS_DB_PATH)

    print("Building EU panel tables and panel DB...")
    wide_facts = _build_wide_facts(sec=sec)
    daily_panel = _build_daily_panel(returns_df=returns_df, wide_facts=wide_facts)
    monthly_panel = _build_monthly_panel_from_daily(daily_panel=daily_panel)
    _save_panel_csvs(
        daily_panel=daily_panel,
        wide_facts=wide_facts,
        monthly_panel=monthly_panel,
        panel_dir=PROCESSED_PANEL_DIRS[0],
    )
    _create_panel_schema(panel_db_path=PROCESSED_PANEL_DB_PATH)
    _insert_panel_tables(
        panel_db_path=PROCESSED_PANEL_DB_PATH,
        daily_panel=daily_panel,
        wide_facts=wide_facts,
        monthly_panel=monthly_panel,
    )

    print("Building EU consolidated MCP DB...")
    _build_consolidated_mcp_db(
        panel_db_path=PROCESSED_PANEL_DB_PATH,
        prices_db_path=RAW_PRICES_DB_PATH,
        sec_db_path=RAW_SEC_COMPANYFACTS_DB_PATH,
        out_db_path=PROCESSED_MCP_DB_PATH,
    )
    print("Done.")


if __name__ == "__main__":
    main()

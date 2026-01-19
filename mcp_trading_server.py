import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from mcp.server.fastmcp import FastMCP

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=str(LOG_DIR / "mcp_trading_server.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    encoding="utf-8",
)

logger = logging.getLogger("mcp_trading_server")

DATA_PANEL = Path("data/processed/panel/daily_panel_prices_returns_fundamentals.csv")

DEFAULT_FEATURE_COLS = [
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

mcp = FastMCP("TradingTools")

_PANEL_CACHE: Optional[pd.DataFrame] = None
_PANEL_CACHE_MTIME: Optional[float] = None


def _panel_mtime() -> Optional[float]:
    try:
        return DATA_PANEL.stat().st_mtime
    except Exception:
        return None


def _load_panel_cached() -> pd.DataFrame:
    global _PANEL_CACHE, _PANEL_CACHE_MTIME

    mtime = _panel_mtime()
    if _PANEL_CACHE is not None and _PANEL_CACHE_MTIME == mtime:
        return _PANEL_CACHE

    if not DATA_PANEL.exists():
        logger.error("Panel file not found: %s", str(DATA_PANEL))
        raise FileNotFoundError(f"Missing panel file: {DATA_PANEL}")

    df = pd.read_csv(DATA_PANEL)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    _PANEL_CACHE = df
    _PANEL_CACHE_MTIME = mtime

    logger.info("Loaded panel. rows=%d cols=%d path=%s", len(df), df.shape[1], str(DATA_PANEL))
    return df


def _ensure_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ret_1d"] = pd.to_numeric(df.get("ret_1d"), errors="coerce")

    if "ret_5d" not in df.columns:
        df["ret_5d"] = df.groupby("ticker")["ret_1d"].rolling(5).sum().reset_index(level=0, drop=True)
    if "ret_20d" not in df.columns:
        df["ret_20d"] = df.groupby("ticker")["ret_1d"].rolling(20).sum().reset_index(level=0, drop=True)
    if "vol_20d" not in df.columns:
        df["vol_20d"] = df.groupby("ticker")["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    if "vol_60d" not in df.columns:
        df["vol_60d"] = df.groupby("ticker")["ret_1d"].rolling(60).std().reset_index(level=0, drop=True)

    return df


def _get_row(df: pd.DataFrame, ticker: str, date_str: str) -> Optional[pd.Series]:
    t = ticker.upper().strip()
    d = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(d):
        return None
    d = d.normalize()

    sub = df[(df["ticker"] == t) & (df["date"] == d)]
    if sub.empty:
        return None
    return sub.iloc[-1]


def _heuristic_score(row: pd.Series) -> Tuple[float, Dict[str, Any]]:
    ret_20d = float(row.get("ret_20d", np.nan))
    vol_20d = float(row.get("vol_20d", np.nan))

    components: Dict[str, Any] = {}

    if np.isnan(ret_20d):
        components["ret_20d"] = None
        ret_20d = 0.0
    else:
        components["ret_20d"] = ret_20d

    if np.isnan(vol_20d) or vol_20d == 0:
        components["vol_20d"] = None
        vol_20d = 1.0
    else:
        components["vol_20d"] = vol_20d

    score = ret_20d / vol_20d
    components["score"] = score
    return score, components


def _score_to_action(score: float, buy_th: float, sell_th: float) -> str:
    if score >= buy_th:
        return "BUY"
    if score <= sell_th:
        return "SELL"
    return "HOLD"


def _compute_score_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ret = pd.to_numeric(df.get("ret_20d"), errors="coerce")
    vol = pd.to_numeric(df.get("vol_20d"), errors="coerce")
    safe_vol = vol.replace(0.0, np.nan)
    df["score_mom"] = (ret / safe_vol).replace([np.inf, -np.inf], np.nan)
    return df


def _percentile_thresholds(
    df: pd.DataFrame,
    ticker: str,
    q_buy: float,
    q_sell: float,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[float, float, int]:
    t = ticker.upper().strip()
    sub = df[df["ticker"] == t].copy()

    if start_date:
        s = pd.to_datetime(start_date, errors="coerce")
        if not pd.isna(s):
            sub = sub[sub["date"] >= s.normalize()]

    if end_date:
        e = pd.to_datetime(end_date, errors="coerce")
        if not pd.isna(e):
            sub = sub[sub["date"] <= e.normalize()]

    sub = sub.dropna(subset=["score_mom"])
    n = int(len(sub))
    if n == 0:
        return 0.25, -0.25, 0

    buy_th = float(sub["score_mom"].quantile(q_buy))
    sell_th = float(sub["score_mom"].quantile(q_sell))
    return buy_th, sell_th, n


@mcp.tool()
def list_tickers() -> List[str]:
    logger.info("list_tickers called")
    df = _load_panel_cached()
    tickers = sorted(df["ticker"].unique().tolist())
    logger.info("list_tickers result count=%d", len(tickers))
    return tickers


@mcp.tool()
def available_date_range(ticker: str) -> Dict[str, str]:
    logger.info("available_date_range called ticker=%s", ticker)
    df = _ensure_features(_load_panel_cached())
    t = ticker.upper().strip()
    sub = df[df["ticker"] == t]
    if sub.empty:
        logger.warning("available_date_range no data ticker=%s", t)
        return {"ticker": t, "min_date": "", "max_date": ""}

    out = {
        "ticker": t,
        "min_date": str(sub["date"].min().date()),
        "max_date": str(sub["date"].max().date()),
    }
    logger.info("available_date_range result ticker=%s min=%s max=%s", t, out["min_date"], out["max_date"])
    return out


@mcp.tool()
def get_features(ticker: str, date: str, feature_cols: Optional[List[str]] = None) -> Dict[str, Any]:
    logger.info("get_features called ticker=%s date=%s", ticker, date)
    df = _ensure_features(_load_panel_cached())
    row = _get_row(df, ticker, date)
    if row is None:
        logger.warning("get_features missing row ticker=%s date=%s", ticker, date)
        return {"ok": False, "error": "No row for ticker and date."}

    cols = feature_cols if feature_cols else DEFAULT_FEATURE_COLS
    out: Dict[str, Any] = {"ok": True, "ticker": ticker.upper().strip(), "date": str(pd.to_datetime(date).date())}

    for c in cols:
        v = row.get(c, None)
        if pd.isna(v):
            out[c] = None
        else:
            out[c] = float(v) if isinstance(v, (np.floating, float, int)) else v

    logger.info("get_features ok ticker=%s date=%s n_features=%d", out["ticker"], out["date"], len(cols))
    return out


@mcp.tool()
def get_percentile_thresholds(
    ticker: str,
    q_buy: float = 0.7,
    q_sell: float = 0.3,
    start_date: str = "2022-01-03",
    end_date: str = "2024-12-31",
) -> Dict[str, Any]:
    logger.info(
        "get_percentile_thresholds called ticker=%s q_buy=%.3f q_sell=%.3f start=%s end=%s",
        ticker,
        q_buy,
        q_sell,
        start_date,
        end_date,
    )

    df = _compute_score_column(_ensure_features(_load_panel_cached()))
    buy_th, sell_th, n_used = _percentile_thresholds(
        df,
        ticker=ticker,
        q_buy=q_buy,
        q_sell=q_sell,
        start_date=start_date,
        end_date=end_date,
    )

    out = {
        "ok": True,
        "ticker": ticker.upper().strip(),
        "method": "percentile",
        "q_buy": float(q_buy),
        "q_sell": float(q_sell),
        "start_date": start_date,
        "end_date": end_date,
        "buy_threshold": float(buy_th),
        "sell_threshold": float(sell_th),
        "n_used": int(n_used),
    }
    logger.info("get_percentile_thresholds result ticker=%s buy=%.6f sell=%.6f n=%d", out["ticker"], buy_th, sell_th, n_used)
    return out


@mcp.tool()
def predict_action(
    ticker: str,
    date: str,
    threshold_method: str = "fixed",
    buy_threshold: float = 0.25,
    sell_threshold: float = -0.25,
    q_buy: float = 0.7,
    q_sell: float = 0.3,
    start_date: str = "2022-01-03",
    end_date: str = "2024-12-31",
) -> Dict[str, Any]:
    logger.info(
        "predict_action called ticker=%s date=%s method=%s",
        ticker,
        date,
        threshold_method,
    )

    df = _compute_score_column(_ensure_features(_load_panel_cached()))
    method = str(threshold_method).lower().strip()

    n_used: Optional[int] = None
    if method == "percentile":
        buy_threshold, sell_threshold, n_used = _percentile_thresholds(
            df,
            ticker=ticker,
            q_buy=float(q_buy),
            q_sell=float(q_sell),
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(
            "predict_action percentile thresholds ticker=%s buy=%.6f sell=%.6f n=%s",
            ticker,
            buy_threshold,
            sell_threshold,
            str(n_used),
        )

    row = _get_row(df, ticker, date)
    if row is None:
        logger.warning("predict_action missing row ticker=%s date=%s", ticker, date)
        return {"ok": False, "error": "No row for ticker and date."}

    score, components = _heuristic_score(row)
    action = _score_to_action(score, float(buy_threshold), float(sell_threshold))
    confidence = float(min(1.0, abs(score)))

    out = {
        "ok": True,
        "ticker": ticker.upper().strip(),
        "date": str(pd.to_datetime(date).date()),
        "action": action,
        "score": float(score),
        "confidence": confidence,
        "details": components,
        "threshold_method": method,
        "thresholds": {
            "buy": float(buy_threshold),
            "sell": float(sell_threshold),
            "q_buy": float(q_buy) if method == "percentile" else None,
            "q_sell": float(q_sell) if method == "percentile" else None,
            "start_date": start_date if method == "percentile" else None,
            "end_date": end_date if method == "percentile" else None,
            "n_used": int(n_used) if method == "percentile" and n_used is not None else None,
        },
    }

    logger.info(
        "predict_action result ticker=%s date=%s action=%s score=%.6f",
        out["ticker"],
        out["date"],
        action,
        float(score),
    )
    return out


@mcp.tool()
def backtest_heuristic(
    ticker: str,
    start_date: str,
    end_date: str,
    threshold_method: str = "fixed",
    buy_threshold: float = 0.25,
    sell_threshold: float = -0.25,
    q_buy: float = 0.7,
    q_sell: float = 0.3,
    thresh_start_date: str = "2022-01-03",
    thresh_end_date: str = "2024-12-31",
) -> Dict[str, Any]:
    logger.info(
        "backtest_heuristic called ticker=%s start=%s end=%s method=%s",
        ticker,
        start_date,
        end_date,
        threshold_method,
    )

    df = _compute_score_column(_ensure_features(_load_panel_cached()))
    method = str(threshold_method).lower().strip()

    if method == "percentile":
        buy_threshold, sell_threshold, _ = _percentile_thresholds(
            df,
            ticker=ticker,
            q_buy=float(q_buy),
            q_sell=float(q_sell),
            start_date=thresh_start_date,
            end_date=thresh_end_date,
        )

    t = ticker.upper().strip()
    s = pd.to_datetime(start_date, errors="coerce")
    e = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(s) or pd.isna(e):
        logger.warning("backtest_heuristic invalid dates start=%s end=%s", start_date, end_date)
        return {"ok": False, "error": "Invalid dates."}

    s = s.normalize()
    e = e.normalize()

    sub = df[(df["ticker"] == t) & (df["date"] >= s) & (df["date"] <= e)].copy()
    if sub.empty:
        logger.warning("backtest_heuristic no data ticker=%s start=%s end=%s", t, str(s.date()), str(e.date()))
        return {"ok": False, "error": "No data in range."}

    scores = []
    actions = []
    for _, r in sub.iterrows():
        score, _ = _heuristic_score(r)
        scores.append(score)
        actions.append(_score_to_action(score, float(buy_threshold), float(sell_threshold)))

    sub["action"] = actions

    pos_map = {"BUY": 1.0, "HOLD": 0.0, "SELL": -1.0}
    sub["position"] = sub["action"].map(pos_map).astype(float)

    sub["ret_1d"] = pd.to_numeric(sub["ret_1d"], errors="coerce").fillna(0.0)
    sub["strategy_ret_1d"] = sub["position"] * sub["ret_1d"]
    sub["equity"] = (1.0 + sub["strategy_ret_1d"]).cumprod()

    total_return = float(sub["equity"].iloc[-1] - 1.0)
    vol = float(sub["strategy_ret_1d"].std(ddof=0))
    mean = float(sub["strategy_ret_1d"].mean())
    sharpe = float(mean / vol) if vol > 0 else 0.0

    out = {
        "ok": True,
        "ticker": t,
        "start_date": str(s.date()),
        "end_date": str(e.date()),
        "threshold_method": method,
        "thresholds": {
            "buy": float(buy_threshold),
            "sell": float(sell_threshold),
            "q_buy": float(q_buy) if method == "percentile" else None,
            "q_sell": float(q_sell) if method == "percentile" else None,
            "thresh_start_date": thresh_start_date if method == "percentile" else None,
            "thresh_end_date": thresh_end_date if method == "percentile" else None,
        },
        "total_return": total_return,
        "daily_mean": mean,
        "daily_vol": vol,
        "daily_sharpe_like": sharpe,
        "n_days": int(len(sub)),
    }

    logger.info(
        "backtest_heuristic result ticker=%s total_return=%.6f sharpe_like=%.6f n_days=%d",
        t,
        total_return,
        sharpe,
        int(len(sub)),
    )
    return out


@mcp.tool()
def self_test_one(ticker: str, date: str) -> Dict[str, Any]:
    df = _compute_score_column(_ensure_features(_load_panel_cached()))
    row = _get_row(df, ticker, date)
    if row is None:
        return {"ok": False, "error": "No row for ticker and date."}

    score, components = _heuristic_score(row)
    return {
        "ok": True,
        "ticker": ticker.upper().strip(),
        "date": str(pd.to_datetime(date).date()),
        "score": float(score),
        "details": components,
    }


if __name__ == "__main__":
    logger.info("Starting MCP TradingTools server")
    mcp.run()

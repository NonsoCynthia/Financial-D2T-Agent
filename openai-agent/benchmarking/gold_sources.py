from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass(frozen=True)
class GoldSnapshot:
    """Holds gold standard values plus the raw payload for reproducibility."""
    source: str
    ticker: str
    asof: str
    values: Dict[str, Optional[float]]
    raw: Dict[str, Any]


def _safe_float(x: Any) -> Optional[float]:
    """Convert a value to float where possible, otherwise return None."""
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:
            return None
        return v
    except Exception:
        return None


def fetch_roic_snapshot(
    ticker: str,
    apikey: str,
    asof: str,
    period: str = "annual",
    limit: int = 1,
    timeout_s: int = 30,
) -> GoldSnapshot:
    """Fetch ROIC.ai fundamentals and multiples for a ticker and map them to benchmark keys.

    Uses ROIC endpoints for:
    - balance sheet (assets, cash, equity)
    - per-share (book value per share, shares outstanding, EPS)
    - multiples (P/E, P/B if provided)

    ROIC.ai endpoints and example fields are documented in their API docs. :contentReference[oaicite:0]{index=0}
    """
    base = "https://api.roic.ai/v2"
    session = requests.Session()
    session.headers.update({"User-Agent": "Financial-D2T-Agent-Benchmark/1.0"})

    def _get(path: str) -> Any:
        url = f"{base}{path}"
        params = {"apikey": apikey, "format": "json", "period": period, "limit": str(limit), "order": "desc"}
        r = session.get(url, params=params, timeout=timeout_s)
        r.raise_for_status()
        return r.json()

    bs = _get(path=f"/fundamental/balance-sheet/{ticker}")
    ps = _get(path=f"/fundamental/per-share/{ticker}")
    mult = _get(path=f"/fundamental/multiples/{ticker}")

    bs0 = bs[0] if isinstance(bs, list) and bs else {}
    ps0 = ps[0] if isinstance(ps, list) and ps else {}
    m0 = mult[0] if isinstance(mult, list) and mult else {}

    values: Dict[str, Optional[float]] = {
        "Assets": _safe_float(x=bs0.get("bs_tot_asset")),
        "CashAndEquivalents": _safe_float(x=bs0.get("bs_c_and_ce_and_sti_detailed")),
        "ShareholderEquity": _safe_float(x=bs0.get("bs_total_equity")),
        "EPS": _safe_float(x=ps0.get("eps")),
        "BookValuePerShare": _safe_float(x=ps0.get("book_val_per_sh")),
        "SharesOutstanding": _safe_float(x=ps0.get("bs_sh_out")),
        "P_E": _safe_float(x=m0.get("pe_ratio")),
        "P_B": _safe_float(x=m0.get("pr_to_book_ratio")),
    }

    raw = {"balance_sheet": bs0, "per_share": ps0, "multiples": m0}
    return GoldSnapshot(source="roic_ai", ticker=ticker, asof=asof, values=values, raw=raw)


def fetch_google_finance_snapshot(
    ticker: str,
    exchange: str,
    asof: str,
    timeout_s: int = 30,
) -> GoldSnapshot:
    """Fetch Google Finance quote page and try to extract key ratios.

    Google Finance does not provide an official public API. This function scrapes the public quote page.
    Treat it as best effort and always snapshot the raw HTML derived tables so results are reproducible.
    """
    import pandas as pd

    symbol = f"{ticker}:{exchange}"
    url = f"https://www.google.com/finance/quote/{symbol}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Financial-D2T-Agent-Benchmark/1.0)"}

    r = requests.get(url, headers=headers, timeout=timeout_s)
    r.raise_for_status()
    html = r.text

    tables: Dict[str, Any] = {}
    values: Dict[str, Optional[float]] = {
        "P_E": None,
        "P_B": None,
        "last_price": None,
    }

    try:
        dfs = pd.read_html(html)
        tables["tables"] = [df.to_dict(orient="records") for df in dfs]
        for df in dfs:
            if df.shape[1] >= 2:
                for _, row in df.iterrows():
                    k = str(row.iloc[0]).strip().lower()
                    v = str(row.iloc[1]).strip()
                    if "p/e" in k and values["P_E"] is None:
                        values["P_E"] = _safe_float(x=v.replace(",", ""))
                    if ("p/b" in k or "price to book" in k) and values["P_B"] is None:
                        values["P_B"] = _safe_float(x=v.replace(",", ""))
    except Exception:
        tables["tables"] = []

    raw = {"url": url, "html_len": len(html), "extracted_tables": tables}
    return GoldSnapshot(source="google_finance", ticker=ticker, asof=asof, values=values, raw=raw)
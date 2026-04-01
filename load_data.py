import os
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence
import xml.etree.ElementTree as ET

import pandas as pd


BASE_FINANCIAL_FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("adj_close", "adjusted_close"),
    ("ret_1d", "one_day_return"),
    ("Volume", "trading_volume"),
    ("Assets", "assets"),
    ("Liabilities", "liabilities"),
    ("StockholdersEquity", "stockholders_equity"),
    ("Revenues", "revenues"),
    ("NetIncomeLoss", "net_income_loss"),
    ("OperatingIncomeLoss", "operating_income"),
    ("EarningsPerShareBasic", "earnings_per_share_basic"),
    ("CommonStockSharesOutstanding", "shares_outstanding"),
)

ROIC_LABEL_MAPPING: Dict[str, str] = {
    "Market Cap": "market_cap",
    "Enterprise Value": "enterprise_value",
    "Shares Outstanding": "roic_shares_outstanding",
    "PE Ratio": "pe_ratio",
    "PS Ratio": "ps_ratio",
    "PB Ratio": "pb_ratio",
    "EV / Sales": "ev_to_sales",
    "EV / EBITDA": "ev_to_ebitda",
    "EV / EBIT": "ev_to_ebit",
    "Current Ratio": "roic_current_ratio",
    "Debt / Equity": "debt_to_equity",
    "Return on Equity (ROE)": "return_on_equity",
    "Return on Invested Capital (ROIC)": "return_on_invested_capital",
    "52-Week Price Change": "fifty_two_week_price_change",
    "50-Day Moving Average": "fifty_day_moving_average",
    "200-Day Moving Average": "two_hundred_day_moving_average",
    "Revenue": "roic_revenue",
    "Operating Income": "roic_operating_income",
    "Net Income": "roic_net_income",
    "EBITDA": "ebitda",
    "EBIT": "ebit",
    "Earnings Per Share (EPS)": "roic_eps",
}

def extract_modified_triplesets_from_file(path):
    """
    Read XML from a file path and extract modified triplesets.
    """
    tree = ET.parse(path)
    root = tree.getroot()
    all_triplesets = []

    for entry in root.findall("./entries/entry"):
        tripleset = []
        for mtriple in entry.findall("./modifiedtripleset/mtriple"):
            if mtriple.text is None:
                continue
            parts = [p.strip() for p in mtriple.text.split("|")]
            if len(parts) != 3:
                continue
            subj, rel, obj = parts
            tripleset.append([subj, rel, obj])  # Changed to list for consistency

        if tripleset:
            all_triplesets.append(tripleset)

    return all_triplesets


def load_generation_samples(
    dataset_path: str,
    dataset_kind: str = "auto",
    filings_index_path: Optional[str] = None,
    roic_dumps_dir: Optional[str] = None,
    previous_reports_path: Optional[str] = None,
    tickers: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_months_per_ticker: Optional[int] = None,
    min_stocks_per_month: int = 1,
) -> list[dict[str, Any]]:
    """
    Load either legacy XML triplesets or monthly financial ticker-month samples.

    Returns a list of dictionaries with at least:
      - sample_id
      - sample_name
      - data_input
      - prompt_context
    """
    path = Path(dataset_path)
    resolved_kind = _resolve_dataset_kind(path=path, dataset_kind=dataset_kind)

    if resolved_kind == "xml":
        triplesets = extract_modified_triplesets_from_file(str(path))
        return [
            {
                "sample_id": idx,
                "sample_name": f"sample_{idx}",
                "data_input": tripleset,
                "prompt_context": _format_triples_for_prompt(tripleset),
            }
            for idx, tripleset in enumerate(triplesets, start=1)
        ]

    if resolved_kind == "financial_monthly":
        return load_monthly_financial_report_samples(
            panel_path=path,
            filings_index_path=filings_index_path,
            roic_dumps_dir=roic_dumps_dir,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            max_months_per_ticker=max_months_per_ticker,
        )

    if resolved_kind == "financial_multi_stock_monthly":
        return load_multi_stock_monthly_report_samples(
            dataset_root=path,
            previous_reports_path=previous_reports_path,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            min_stocks_per_month=min_stocks_per_month,
        )

    raise ValueError(
        "Unsupported dataset kind "
        f"'{resolved_kind}'. Expected 'xml', 'financial_monthly', or 'financial_multi_stock_monthly'."
    )


def load_monthly_financial_report_samples(
    panel_path: str | Path,
    filings_index_path: Optional[str | Path] = None,
    roic_dumps_dir: Optional[str | Path] = None,
    tickers: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_months_per_ticker: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Build one data-to-text sample per (ticker, analysis month) from the monthly panel.

    Each sample contains finance-specific structured facts ready to feed into the
    LangGraph pipeline, plus readable prompt context and metadata for lookup.
    """
    panel_file = Path(panel_path)
    panel_df = _read_tabular_dataset(panel_file)
    panel_df = _normalise_monthly_panel(panel_df)
    panel_df = _filter_monthly_panel(
        panel_df=panel_df,
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        max_months_per_ticker=max_months_per_ticker,
    )

    filings_df = _load_filings_index(
        explicit_path=Path(filings_index_path) if filings_index_path else None,
        panel_path=panel_file,
    )
    filings_by_ticker: Dict[str, pd.DataFrame] = {
        ticker: grp.sort_values("filing_date").reset_index(drop=True)
        for ticker, grp in filings_df.groupby("ticker")
    } if not filings_df.empty else {}

    roic_index = _load_roic_snapshot_index(
        explicit_dir=Path(roic_dumps_dir) if roic_dumps_dir else None,
        panel_path=panel_file,
    )

    samples: list[dict[str, Any]] = []
    for sample_id, row in enumerate(panel_df.to_dict(orient="records"), start=1):
        ticker = str(row["ticker"]).strip().upper()
        analysis_date = _normalise_date_str(row["date"])
        filing_record = _latest_filing_for_month(
            ticker=ticker,
            analysis_date=analysis_date,
            filings_by_ticker=filings_by_ticker,
        )
        roic_context = roic_index.get((ticker, analysis_date[:7]))
        sample = _build_monthly_financial_sample(
            sample_id=sample_id,
            row=row,
            filing_record=filing_record,
            roic_context=roic_context,
        )
        samples.append(sample)

    return samples


def load_multi_stock_monthly_report_samples(
    dataset_root: str | Path,
    previous_reports_path: Optional[str | Path] = None,
    tickers: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_stocks_per_month: int = 1,
) -> list[dict[str, Any]]:
    """
    Build one sample per analysis month for a multi-stock monthly report generator.

    Source directory contains per-ticker single-stock artifacts such as:
      - *_output_*.json for indicators, and sometimes an embedded manager decision
      - *_manager_decision_*.json for recommendation, target price, and metadata
    """
    root = Path(dataset_root)
    if not root.exists():
        raise FileNotFoundError(f"Dataset root not found: {root}")

    indicator_payloads = _load_indicator_payloads(root=root)
    decision_payloads = _load_manager_decisions(root=root)
    selected_tickers = {str(t).strip().upper() for t in tickers} if tickers else None

    rows_by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key, indicator_payload in indicator_payloads.items():
        embedded_decision_payload = _extract_manager_payload_from_output_payload(
            indicator_payload=indicator_payload,
        )
        decision_payload = _merge_manager_payloads(
            explicit_payload=decision_payloads.get(key),
            fallback_payload=embedded_decision_payload,
        )
        if decision_payload is None:
            continue

        ticker, analysis_date = key
        if selected_tickers and ticker not in selected_tickers:
            continue
        if not _date_in_range(
            analysis_date=analysis_date,
            start_date=start_date,
            end_date=end_date,
        ):
            continue

        stock_bundle = _build_multi_stock_bundle_row(
            ticker=ticker,
            analysis_date=analysis_date,
            indicator_payload=indicator_payload,
            decision_payload=decision_payload,
        )
        rows_by_month[analysis_date].append(stock_bundle)

    previous_reports_map = _load_previous_reports_map(previous_reports_path)

    min_count = max(1, int(min_stocks_per_month))
    eligible_months = [
        analysis_date
        for analysis_date in sorted(rows_by_month)
        if len(rows_by_month[analysis_date]) >= min_count
    ]

    samples: list[dict[str, Any]] = []
    for sample_id, analysis_date in enumerate(eligible_months, start=1):
        stock_rows = sorted(rows_by_month[analysis_date], key=lambda row: row["ticker"])

        previous_date = eligible_months[sample_id - 2] if sample_id > 1 else None
        previous_report = (
            previous_reports_map.get(previous_date, "N/A")
            if previous_date
            else "N/A"
        )

        facts = _build_multi_stock_month_facts(
            analysis_date=analysis_date,
            stock_rows=stock_rows,
        )
        prompt_context = build_multi_stock_prompt_context(
            analysis_date=analysis_date,
            stock_rows=stock_rows,
            previous_report=previous_report,
        )

        samples.append(
            {
                "sample_id": len(samples) + 1,
                "sample_name": f"multi_stock_{analysis_date}",
                "sample_type": "multi_stock_monthly",
                "analysis_date": analysis_date,
                "tickers": [row["ticker"] for row in stock_rows],
                "ticker_count": len(stock_rows),
                "previous_analysis_date": previous_date,
                "previous_report": previous_report,
                "stocks": stock_rows,
                "data_input": facts,
                "prompt_context": prompt_context,
            }
        )

    return samples


def _extract_manager_payload_from_output_payload(
    indicator_payload: dict[str, Any],
) -> Optional[dict[str, Any]]:
    manager_payload = indicator_payload.get("manager")
    if isinstance(manager_payload, dict):
        merged_payload = dict(manager_payload)
        merged_payload.setdefault("analysis_date", indicator_payload.get("analysis_date"))
        return merged_payload

    if any(key in indicator_payload for key in ("recommendation", "target_price")):
        return indicator_payload

    return None


def _merge_manager_payloads(
    explicit_payload: Optional[dict[str, Any]],
    fallback_payload: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if explicit_payload is None and fallback_payload is None:
        return None

    merged: dict[str, Any] = {}
    if fallback_payload:
        merged.update(fallback_payload)
    if explicit_payload:
        for key, value in explicit_payload.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            merged[key] = value

    return merged


def _resolve_dataset_kind(path: Path, dataset_kind: str) -> str:
    if dataset_kind != "auto":
        return dataset_kind

    if path.is_dir():
        if any(path.rglob("*_manager_decision_*.json")) and any(path.rglob("*_output_*.json")):
            return "financial_multi_stock_monthly"

    suffix = path.suffix.lower()
    if suffix == ".xml":
        return "xml"
    if suffix in {".csv", ".parquet"}:
        return "financial_monthly"
    raise ValueError(
        f"Could not infer dataset kind from '{path}'. Pass dataset_kind explicitly."
    )


def _read_tabular_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def _normalise_monthly_panel(panel_df: pd.DataFrame) -> pd.DataFrame:
    required = {"ticker", "date"}
    missing = required.difference(panel_df.columns)
    if missing:
        raise ValueError(
            f"Monthly panel is missing required columns: {sorted(missing)}"
        )

    out = panel_df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if "filed" in out.columns:
        out["filed"] = pd.to_datetime(out["filed"], errors="coerce")

    out = out.dropna(subset=["ticker", "date"]).sort_values(["ticker", "date"])
    return out.reset_index(drop=True)


def _filter_monthly_panel(
    panel_df: pd.DataFrame,
    tickers: Optional[Sequence[str]],
    start_date: Optional[str],
    end_date: Optional[str],
    max_months_per_ticker: Optional[int],
) -> pd.DataFrame:
    out = panel_df.copy()

    if tickers:
        wanted = {str(t).upper().strip() for t in tickers}
        out = out[out["ticker"].isin(wanted)]

    if start_date:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if pd.notna(start_ts):
            out = out[out["date"] >= start_ts]

    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts):
            out = out[out["date"] <= end_ts]

    if max_months_per_ticker and max_months_per_ticker > 0:
        out = (
            out.sort_values(["ticker", "date"])
            .groupby("ticker", group_keys=False)
            .tail(max_months_per_ticker)
        )

    return out.reset_index(drop=True)


def _load_filings_index(
    explicit_path: Optional[Path],
    panel_path: Path,
) -> pd.DataFrame:
    index_path = explicit_path or _infer_latest_filings_index(panel_path)
    if index_path is None or not index_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(index_path, low_memory=False)
    required = {"ticker", "filing_date", "form"}
    missing = required.difference(df.columns)
    if missing:
        return pd.DataFrame()

    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["filing_date"] = pd.to_datetime(out["filing_date"], errors="coerce")
    out = out.dropna(subset=["ticker", "filing_date"]).sort_values(
        ["ticker", "filing_date"]
    )
    return out.reset_index(drop=True)


def _infer_latest_filings_index(panel_path: Path) -> Optional[Path]:
    data_root = _infer_data_root(panel_path)
    if data_root is None:
        return None
    filings_dir = data_root / "raw" / "sec" / "filings_raw"
    matches = sorted(filings_dir.glob("filings_index_*.csv"))
    return matches[-1] if matches else None


def _infer_roic_dir(panel_path: Path) -> Optional[Path]:
    data_root = _infer_data_root(panel_path)
    if data_root is None:
        return None
    roic_dir = data_root / "roic_json_dumps_monthly_last_year"
    return roic_dir if roic_dir.exists() else None


def _infer_data_root(path: Path) -> Optional[Path]:
    resolved = path.resolve()
    for parent in [resolved.parent, *resolved.parents]:
        if parent.name in {"data", "data_eu"}:
            return parent
    return None


def _load_roic_snapshot_index(
    explicit_dir: Optional[Path],
    panel_path: Path,
) -> dict[tuple[str, str], dict[str, Any]]:
    roic_dir = explicit_dir or _infer_roic_dir(panel_path)
    if roic_dir is None or not roic_dir.exists():
        return {}

    index: dict[tuple[str, str], dict[str, Any]] = {}
    for file_path in sorted(roic_dir.glob("*.json")):
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        ticker = str(payload.get("ticker") or "").strip().upper()
        asof_date = _normalise_date_str(payload.get("asof_date"))
        if not ticker or not asof_date:
            continue

        facts = _extract_roic_facts(ticker=ticker, payload=payload)
        if not facts:
            continue

        key = (ticker, asof_date[:7])
        existing = index.get(key)
        if existing is None or asof_date > existing["asof_date"]:
            index[key] = {
                "asof_date": asof_date,
                "facts": facts,
            }

    return index


def _extract_roic_facts(ticker: str, payload: dict[str, Any]) -> list[list[str]]:
    facts: list[list[str]] = []
    seen_relations: set[str] = set()

    asof_date = _normalise_date_str(payload.get("asof_date"))
    if asof_date:
        facts.append([ticker, "roic_snapshot_as_of", asof_date])

    for table in payload.get("tables", []):
        columns = table.get("columns") or []
        if len(columns) != 2:
            continue

        for row in table.get("rows", []):
            label = str(row.get("0") or "").strip()
            relation = ROIC_LABEL_MAPPING.get(label)
            raw_value = row.get("1")
            if not relation or relation in seen_relations:
                continue

            value = _format_scalar(raw_value)
            if value is None:
                continue

            facts.append([ticker, relation, value])
            seen_relations.add(relation)

    return facts


def _latest_filing_for_month(
    ticker: str,
    analysis_date: str,
    filings_by_ticker: Dict[str, pd.DataFrame],
) -> Optional[dict[str, str]]:
    filings = filings_by_ticker.get(ticker)
    if filings is None or filings.empty:
        return None

    analysis_ts = pd.to_datetime(analysis_date, errors="coerce")
    if pd.isna(analysis_ts):
        return None

    eligible = filings[filings["filing_date"] <= analysis_ts]
    if eligible.empty:
        return None

    latest = eligible.iloc[-1]
    return {
        "form": str(latest.get("form") or "").strip(),
        "filing_date": _normalise_date_str(latest.get("filing_date")),
    }


def _build_monthly_financial_sample(
    sample_id: int,
    row: dict[str, Any],
    filing_record: Optional[dict[str, str]],
    roic_context: Optional[dict[str, Any]],
) -> dict[str, Any]:
    ticker = str(row["ticker"]).strip().upper()
    analysis_date = _normalise_date_str(row["date"])

    facts: list[list[str]] = [
        [ticker, "analysis_month", analysis_date],
    ]

    filed_value = _normalise_date_str(row.get("filed"))
    if filed_value:
        facts.append([ticker, "latest_fundamentals_filed_date", filed_value])

    for column, relation in BASE_FINANCIAL_FIELD_MAP:
        value = _format_scalar(row.get(column))
        if value is None:
            continue
        facts.append([ticker, relation, value])

    if filing_record:
        if filing_record.get("form"):
            facts.append([ticker, "latest_sec_form", filing_record["form"]])
        if filing_record.get("filing_date"):
            facts.append([ticker, "latest_sec_filing_date", filing_record["filing_date"]])

    if roic_context:
        facts.extend(roic_context.get("facts", []))

    prompt_context = build_financial_prompt_context(
        ticker=ticker,
        analysis_date=analysis_date,
        facts=facts,
    )

    raw_metrics = {
        key: _serialise_for_metadata(value)
        for key, value in row.items()
        if _serialise_for_metadata(value) is not None
    }

    return {
        "sample_id": sample_id,
        "sample_name": f"{ticker}_{analysis_date}",
        "sample_type": "single_stock_monthly",
        "ticker": ticker,
        "analysis_date": analysis_date,
        "filed_date": filed_value,
        "latest_sec_form": (filing_record or {}).get("form"),
        "data_input": facts,
        "prompt_context": prompt_context,
        "raw_metrics": raw_metrics,
    }


def build_financial_prompt_context(
    ticker: str,
    analysis_date: str,
    facts: Sequence[Sequence[Any]],
) -> str:
    lines = [
        f"Ticker: {ticker}",
        f"Analysis month: {analysis_date}",
        "Structured monthly financial facts:",
    ]
    lines.extend(f"- {subj} | {rel} | {obj}" for subj, rel, obj in facts)
    return "\n".join(lines)


def build_multi_stock_prompt_context(
    analysis_date: str,
    stock_rows: Sequence[dict[str, Any]],
    previous_report: str,
) -> str:
    tickers = [str(row["ticker"]) for row in stock_rows]
    lines = [
        f"Analysis month: {analysis_date}",
        f"Tickers in bundle ({len(tickers)}): {', '.join(tickers)}",
        "Previous month's multi-stock report context:",
        _sanitize_context_text(previous_report),
        "",
        "Canonical fields rule:",
        "- Recommendation and target_price are authoritative manager outputs.",
        "- If the justification mentions other valuation anchors or scenario prices, do not replace the canonical target_price with them.",
        "",
        "Current month structured bundle:",
    ]

    for row in stock_rows:
        ticker = row["ticker"]
        header_bits = [
            f"recommendation={row['recommendation']}",
            f"target_price={row['target_price']}",
        ]
        if row.get("current_price") and row["current_price"] != "N/A":
            header_bits.append(f"current_price={row['current_price']}")
        if row.get("implied_move_pct") and row["implied_move_pct"] != "N/A":
            header_bits.append(f"implied_move_pct={row['implied_move_pct']}")
        lines.append(f"- {ticker}: {', '.join(header_bits)}")
        lines.append(
            f"  - recommendation_justification: {row['recommendation_justification']}"
        )
        for indicator_name, indicator_value in sorted(row["indicators"].items()):
            lines.append(f"  - ({ticker}, {indicator_name}, {indicator_value})")

    return "\n".join(lines)


def _format_triples_for_prompt(tripleset: Iterable[Sequence[Any]]) -> str:
    return "\n".join(
        f"- {subj} | {rel} | {obj}"
        for subj, rel, obj in tripleset
    )


def _load_indicator_payloads(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payloads: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(root.rglob("*_output_*.json")):
        parsed = _parse_single_stock_artifact_name(path.name, artifact_kind="output")
        if parsed is None:
            continue
        ticker, analysis_date = parsed
        payload = _read_json_dict(path)
        if payload is None:
            continue
        payloads[(ticker, analysis_date)] = payload
    return payloads


def _load_manager_decisions(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payloads: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(root.rglob("*_manager_decision_*.json")):
        parsed = _parse_single_stock_artifact_name(path.name, artifact_kind="manager_decision")
        if parsed is None:
            continue
        ticker, analysis_date = parsed
        payload = _read_json_dict(path)
        if payload is None:
            continue
        payloads[(ticker, analysis_date)] = payload
    return payloads


def _parse_single_stock_artifact_name(
    filename: str,
    artifact_kind: str,
) -> Optional[tuple[str, str]]:
    if artifact_kind == "output":
        pattern = r"^(?P<ticker>.+?)_(?P<date>\d{4}-\d{2}-\d{2})_output_\d+\.json$"
    elif artifact_kind == "manager_decision":
        pattern = r"^(?P<ticker>.+?)_(?P<date>\d{4}-\d{2}-\d{2})_manager_decision_\d+\.json$"
    else:
        raise ValueError(f"Unknown artifact_kind: {artifact_kind}")

    match = re.match(pattern, filename)
    if not match:
        return None
    return (
        str(match.group("ticker")).strip().upper(),
        str(match.group("date")).strip(),
    )


def _read_json_dict(path: Path) -> Optional[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_multi_stock_bundle_row(
    ticker: str,
    analysis_date: str,
    indicator_payload: dict[str, Any],
    decision_payload: dict[str, Any],
) -> dict[str, Any]:
    indicators = _flatten_indicator_payload(indicator_payload)
    recommendation = str(decision_payload.get("recommendation", "N/A")).strip() or "N/A"
    target_price = _format_currency_scalar(decision_payload.get("target_price")) or "N/A"
    current_price = _format_currency_scalar(decision_payload.get("price")) or "N/A"
    implied_move_pct = _compute_target_implied_move_pct(
        decision_payload.get("target_price"),
        decision_payload.get("price"),
    ) or "N/A"
    raw_justification = (
        decision_payload.get("justification")
        or decision_payload.get("monthly_report")
        or "N/A"
    )
    recommendation_justification = _sanitize_context_text(raw_justification)

    return {
        "ticker": ticker,
        "analysis_date": analysis_date,
        "recommendation": recommendation,
        "target_price": target_price,
        "current_price": current_price,
        "implied_move_pct": implied_move_pct,
        "recommendation_justification": recommendation_justification,
        "indicators": indicators,
    }


def _flatten_indicator_payload(payload: dict[str, Any]) -> dict[str, str]:
    indicators = payload.get("indicators", [])
    flat: dict[str, str] = {}

    if isinstance(indicators, dict):
        for key, value in indicators.items():
            formatted = _format_scalar(value)
            if formatted is not None:
                flat[str(key).strip()] = formatted
        return flat

    if isinstance(indicators, list):
        for row in indicators:
            if not isinstance(row, dict):
                continue
            key = str(row.get("indicator", "")).strip()
            if not key:
                continue
            formatted = _format_scalar(row.get("value"))
            if formatted is not None:
                flat[key] = formatted

    return flat


def _build_multi_stock_month_facts(
    analysis_date: str,
    stock_rows: Sequence[dict[str, Any]],
) -> list[list[str]]:
    report_id = f"M_SMRG_{analysis_date}"
    facts: list[list[str]] = [
        [report_id, "analysis_month", analysis_date],
        [report_id, "stock_count", str(len(stock_rows))],
    ]

    for row in stock_rows:
        ticker = row["ticker"]
        facts.append([report_id, "covers_ticker", ticker])
        facts.append([ticker, "Recommendation", row["recommendation"]])
        facts.append([ticker, "TargetPrice", row["target_price"]])
        if row.get("current_price") and row["current_price"] != "N/A":
            facts.append([ticker, "CurrentPrice", row["current_price"]])
        if row.get("implied_move_pct") and row["implied_move_pct"] != "N/A":
            facts.append([ticker, "TargetImpliedMovePct", row["implied_move_pct"]])
        facts.append([ticker, "RecommendationJustification", row["recommendation_justification"]])
        for indicator_name, indicator_value in sorted(row["indicators"].items()):
            facts.append([ticker, indicator_name, indicator_value])

    return facts


def _load_previous_reports_map(
    previous_reports_path: Optional[str | Path],
) -> dict[str, str]:
    if previous_reports_path is None:
        return {}

    path = Path(previous_reports_path)
    if not path.exists():
        return {}

    if path.is_file():
        payload = _read_json_dict(path)
        if payload is None:
            return {}
        return {
            str(k).strip()[:10]: _sanitize_context_text(v)
            for k, v in payload.items()
            if isinstance(v, str)
        }

    if path.is_dir():
        reports: dict[str, str] = {}
        for report_file in sorted(path.rglob("*.json")):
            payload = _read_json_dict(report_file)
            if payload is None:
                continue
            analysis_date = _normalise_date_str(payload.get("analysis_date"))
            report_text = (
                payload.get("final_response")
                or payload.get("generated_text")
                or payload.get("report")
            )
            if analysis_date and isinstance(report_text, str) and report_text.strip():
                reports[analysis_date] = _sanitize_context_text(report_text)
        return reports

    return {}


def _sanitize_context_text(value: Any, max_chars: int = 4000) -> str:
    text = str(value or "").strip()
    if not text:
        return "N/A"
    text = re.sub(r"\s+", " ", text)
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + " ...[truncated]"
    return text


def _date_in_range(
    analysis_date: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> bool:
    analysis_ts = pd.to_datetime(analysis_date, errors="coerce")
    if pd.isna(analysis_ts):
        return False

    if start_date:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if pd.notna(start_ts) and analysis_ts < start_ts:
            return False

    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts) and analysis_ts > end_ts:
            return False

    return True


def _normalise_date_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date().isoformat()
    as_ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(as_ts):
        text = str(value).strip()
        return text or None
    return as_ts.date().isoformat()


def _format_scalar(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None

    if isinstance(value, bool):
        return str(value)

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if pd.isna(value):
            return None
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")

    return str(value)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _format_currency_scalar(value: Any) -> Optional[str]:
    number = _coerce_float(value)
    if number is None:
        return _format_scalar(value)
    return f"${number:.2f}"


def _compute_target_implied_move_pct(
    target_price: Any,
    current_price: Any,
) -> Optional[str]:
    target_val = _coerce_float(target_price)
    current_val = _coerce_float(current_price)
    if target_val is None or current_val in (None, 0):
        return None
    implied_move = ((target_val - current_val) / current_val) * 100.0
    return f"{implied_move:+.2f}%"


def _serialise_for_metadata(value: Any) -> Optional[Any]:
    text = _format_scalar(value)
    if text is None:
        return None
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            return text
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            return text
    return text


def save_result_to_json(state: dict, dataset_folder= "", filename: str = "result.json", directory: str = "results") -> None:
    """
    Saves the given agent workflow state to a JSON file in a specified directory.
    """
    # Ensure full directory path exists
    if dataset_folder != "":
        full_directory = os.path.join(directory, dataset_folder)
    else:
        full_directory = directory

    os.makedirs(full_directory, exist_ok=True)

    file_path = os.path.join(full_directory, filename)

    if os.path.isdir(file_path):
        raise IsADirectoryError(f"Cannot write to '{file_path}' because it is a directory.")

    def make_serializable(obj):
        if isinstance(obj, list):
            return [make_serializable(x) for x in obj]
        elif hasattr(obj, "model_dump"):
            return obj.model_dump()
        elif isinstance(obj, dict):
            return {k: make_serializable(v) for k, v in obj.items()}
        else:
            return obj

    serializable_state = make_serializable(dict(state))

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(serializable_state, f, indent=4)

    print(f"[SAVED] Agent result saved to: {file_path}")

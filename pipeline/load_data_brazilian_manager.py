from __future__ import annotations

import calendar
import json
import re
from collections import defaultdict
from datetime import date as _date
from pathlib import Path
from typing import Any, Optional, Sequence

from agents.agent_prompts_brazilian_manager import PT_BR_MANAGER_SAMPLE_TEMPLATE


MANAGER_FILE_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_manager_\d+\.json$")
ANALYST_FILE_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_analyst_\d+\.json$")
MATERIAL_FACTS_FILE_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_material_facts_\d+\.txt$")


def load_brazilian_manager_samples(
    dataset_root: str | Path,
    tickers: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_stocks_per_month: int = 1,
) -> list[dict[str, Any]]:
    """
    Load AIDA-BR workflow-vs-agent manager outputs as monthly multi-stock samples.

    Expected source layout:
      results_v2/completo_gpt5mini/
        TICKER/
          YYYY-MM-DD_analyst_0.json
          YYYY-MM-DD_manager_0.json
          YYYY-MM-DD_material_facts_0.txt

    The analyst file supplies indicators; the manager file supplies the
    canonical recommendation, justification, and target price. Material-facts
    files are optional but included in the NLG context when present.
    """
    root = Path(dataset_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Brazilian manager dataset root not found: {root}")

    selected_tickers = {str(t).strip().upper() for t in tickers} if tickers else None
    analyst_payloads = _load_artifacts(root=root, file_re=ANALYST_FILE_RE)
    manager_payloads = _load_artifacts(root=root, file_re=MANAGER_FILE_RE)
    material_facts = _load_text_artifacts(root=root, file_re=MATERIAL_FACTS_FILE_RE)

    rows_by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key, manager_payload in manager_payloads.items():
        ticker, analysis_date = key
        if selected_tickers and ticker not in selected_tickers:
            continue
        if not _date_in_range(analysis_date, start_date=start_date, end_date=end_date):
            continue

        analyst_payload = analyst_payloads.get(key, {})
        material_facts_text = material_facts.get(key, "N/A")
        rows_by_month[analysis_date].append(
            _build_stock_row(
                ticker=ticker,
                analysis_date=analysis_date,
                analyst_payload=analyst_payload,
                manager_payload=manager_payload,
                material_facts=material_facts_text,
            )
        )

    min_count = max(1, int(min_stocks_per_month))
    eligible_dates = [
        date_value
        for date_value in sorted(rows_by_month)
        if len(rows_by_month[date_value]) >= min_count
    ]

    coverage_end_date = _infer_coverage_end_date(eligible_dates)

    samples: list[dict[str, Any]] = []
    previous_report = "N/A"
    for sample_id, analysis_date in enumerate(eligible_dates, start=1):
        stock_rows = sorted(rows_by_month[analysis_date], key=lambda row: row["ticker"])
        prompt_context = build_brazilian_manager_prompt_context(
            analysis_date=analysis_date,
            stock_rows=stock_rows,
            previous_report=previous_report,
            coverage_end_date=coverage_end_date,
        )
        sample = {
            "sample_id": sample_id,
            "sample_name": f"br_manager_{analysis_date}",
            "sample_type": "brazilian_manager_monthly",
            "analysis_date": analysis_date,
            "tickers": [row["ticker"] for row in stock_rows],
            "ticker_count": len(stock_rows),
            "stocks": stock_rows,
            "previous_report": previous_report,
            "data_input": stock_rows,
            "prompt_context": prompt_context,
        }
        samples.append(sample)
        previous_report = "N/A"

    return samples


def _infer_coverage_end_date(eligible_dates: list[str]) -> str:
    if not eligible_dates:
        return ""
    latest = max(eligible_dates)
    try:
        year, month = int(latest[:4]), int(latest[5:7])
        last_day = calendar.monthrange(year, month)[1]
        return _date(year, month, last_day).isoformat()
    except Exception:
        return latest


def _compute_horizon_months(analysis_date: str, end_date: str) -> int:
    try:
        a_year, a_month = int(analysis_date[:4]), int(analysis_date[5:7])
        e_year, e_month = int(end_date[:4]), int(end_date[5:7])
        return max((e_year - a_year) * 12 + (e_month - a_month), 0)
    except Exception:
        return 0


def build_brazilian_manager_prompt_context(
    analysis_date: str,
    stock_rows: Sequence[dict[str, Any]],
    previous_report: str = "N/A",
    coverage_end_date: str = "",
) -> str:
    stock_blocks = "\n\n".join(_format_stock_block(row) for row in stock_rows)
    return PT_BR_MANAGER_SAMPLE_TEMPLATE.format(
        analysis_date=analysis_date,
        end_date=coverage_end_date or analysis_date,
        horizon_months=_compute_horizon_months(analysis_date, coverage_end_date),
        ticker_count=len(stock_rows),
        tickers=", ".join(row["ticker"] for row in stock_rows),
        previous_report=_clean_text(previous_report) or "N/A",
        stock_blocks=stock_blocks,
    )


def save_result_to_json(payload: dict[str, Any], filename: str, directory: str | Path) -> Path:
    out_dir = Path(directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return out_path


def _load_artifacts(root: Path, file_re: re.Pattern[str]) -> dict[tuple[str, str], dict[str, Any]]:
    payloads: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(root.glob("*/*.json")):
        match = file_re.match(path.name)
        if match is None:
            continue
        ticker = path.parent.name.strip().upper()
        analysis_date = match.group("date")
        payload = _read_json_dict(path)
        if payload is not None:
            payloads[(ticker, analysis_date)] = payload
    return payloads


def _load_text_artifacts(root: Path, file_re: re.Pattern[str]) -> dict[tuple[str, str], str]:
    payloads: dict[tuple[str, str], str] = {}
    for path in sorted(root.glob("*/*.txt")):
        match = file_re.match(path.name)
        if match is None:
            continue
        ticker = path.parent.name.strip().upper()
        analysis_date = match.group("date")
        try:
            text = path.read_text(encoding="utf-8").strip()
        except OSError:
            text = ""
        if text:
            payloads[(ticker, analysis_date)] = _clean_text(text)
    return payloads


def _read_json_dict(path: Path) -> Optional[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_stock_row(
    ticker: str,
    analysis_date: str,
    analyst_payload: dict[str, Any],
    manager_payload: dict[str, Any],
    material_facts: str = "N/A",
) -> dict[str, Any]:
    manager_output = _extract_output_dict(manager_payload)
    indicators = _extract_indicators(analyst_payload)
    recommendation = _format_scalar(manager_output.get("recommendation")) or "N/A"
    target_price = _format_scalar(manager_output.get("target_price")) or "N/A"
    justification = _clean_text(_format_scalar(manager_output.get("justification")) or "N/A")

    return {
        "ticker": ticker,
        "analysis_date": analysis_date,
        "recommendation": recommendation,
        "target_price": target_price,
        "justification": justification,
        "indicators": indicators,
        "material_facts": _clean_text(material_facts) or "N/A",
        "raw_manager_output": manager_output,
    }


def _extract_output_dict(payload: dict[str, Any]) -> dict[str, Any]:
    output = payload.get("output")
    if isinstance(output, dict):
        return output

    for step in payload.get("steps", []):
        if not isinstance(step, dict):
            continue
        for content in step.get("content", []):
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
    return {}


def _extract_indicators(payload: dict[str, Any]) -> dict[str, Any]:
    output = payload.get("output")
    if isinstance(output, dict):
        indicators = output.get("indicators")
        if isinstance(indicators, list):
            return _normalise_indicator_list(indicators)

    indicators = payload.get("indicators")
    if isinstance(indicators, list):
        return _normalise_indicator_list(indicators)

    return {}


def _normalise_indicator_list(indicators: Sequence[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in indicators:
        if not isinstance(item, dict):
            continue
        name = str(item.get("indicator") or "").strip()
        if not name:
            continue
        out[name] = item.get("value")
    return out


def _format_stock_block(row: dict[str, Any]) -> str:
    lines = [
        f"Ativo: {row['ticker']}",
        f"Data: {row['analysis_date']}",
        f"Recomendacao canonica: {row['recommendation']}",
        f"Preco-alvo canonico: {row['target_price']}",
        "Justificativa canonica do gerente:",
        row["justification"],
        "Fatos relevantes recentes:",
        row.get("material_facts") or "N/A",
        "Indicadores do analista:",
    ]
    indicators = row.get("indicators") or {}
    if not indicators:
        lines.append("- N/A")
    else:
        for name, value in sorted(indicators.items()):
            lines.append(f"- {name}: {_format_scalar(value) or 'N/A'}")
    return "\n".join(lines)


def _date_in_range(
    analysis_date: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> bool:
    if start_date and analysis_date < start_date:
        return False
    if end_date and analysis_date > end_date:
        return False
    return True


def _format_scalar(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:
            return None
        return str(value)
    if isinstance(value, (int, bool)):
        return str(value)
    text = str(value).strip()
    return text or None


def _clean_text(value: str) -> str:
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", str(value or "")).strip()

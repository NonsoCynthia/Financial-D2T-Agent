"""
Monthly agent runner.

Creates two MCP-backed agents (analyst + manager), slides a 12-month window per
ticker/date, and saves one JSON file per ticker with analyst indicators plus a
manager recommendation/target. Use with run_eval_monthly.py for scoring.
"""

import argparse
import asyncio
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from agents import Agent, Runner, function_tool
from agents.mcp import MCPServerStdio
from openai.types.shared import Reasoning
from typing_extensions import TypedDict

try:
    from agents import ModelSettings
except Exception:  # pragma: no cover - compatibility with older SDKs
    ModelSettings = None

from finAgents.financial_agents.agent_prompts import (
    FINANCIAL_ANALYST_INSTRUCTIONS,
    FINANCIAL_MANAGER_INSTRUCTIONS,
    MANAGER_MONTHLY_TASK_PROMPT,
    JSON_REPAIR_PROMPT,
    analyst_prompt,
)
from finAgents.financial_agents.financial_analyst import expected_indicator_keys

DEFAULT_TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]
REQUIRED_INDICATORS = ["Assets", "Liabilities", "Revenues", "NetIncomeLoss"]
EXPECTED_ANALYST_INDICATORS = expected_indicator_keys()
CANONICAL_ACTIONS = {"BUY", "SELL", "HOLD"}


class CodeInterpreterInput(TypedDict):
    code: str


@function_tool
def code_interpreter(inp: CodeInterpreterInput) -> dict:
    """Execute Python code and return stdout/stderr report."""
    try:
        result = subprocess.run(
            [sys.executable, "-c", inp.get("code")],
            capture_output=True,
        )
        report = f"StdOut:\n{result.stdout}\nStdErr:\n{result.stderr}"
        return {"status": "success", "report": report}
    except Exception as e:
        return {"status": "error", "report": f"Failed to run code: {e}"}


def normalize_action(x: Any) -> str:
    """Canonicalise manager action to BUY/SELL/HOLD."""
    if isinstance(x, str):
        action = x.strip().upper()
    else:
        action = "HOLD"
    if action == "KEEP":
        action = "HOLD"
    if action in CANONICAL_ACTIONS:
        return action
    return "HOLD"


def _supports_reasoning(model: str) -> bool:
    """Return True if model supports explicit reasoning-effort controls."""
    m = model.lower().strip()
    return m.startswith("o3") or m.startswith("gpt-5")


def parse_json_strict(text: str) -> Dict[str, Any]:
    """Parse a JSON string into a Python dict. Raises json.JSONDecodeError if invalid."""
    return json.loads(text)


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort extractor for a JSON object embedded in model output."""
    if not isinstance(text, str):
        return None

    s = text.strip()
    if not s:
        return None

    # Common case: fenced code block output
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s).strip()

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        return None

    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _normalize_json_like(text: str) -> str:
    """Normalize common JSON-like artifacts emitted by models."""
    s = text
    s = re.sub(r"\bNone\b", "null", s)
    s = re.sub(r"\bTrue\b", "true", s)
    s = re.sub(r"\bFalse\b", "false", s)
    s = re.sub(r"\bNaN\b", "null", s)
    s = re.sub(r"\bInfinity\b", "null", s)
    s = re.sub(r"\b-Infinity\b", "null", s)
    # Remove trailing commas before object/array closes.
    s = re.sub(r",\s*([}\]])", r"\1", s)
    return s


def parse_json_safe(text: Any, fallback: Optional[Dict[str, Any]] = None, label: str = "") -> Dict[str, Any]:
    """Parse JSON; on failure return fallback (default empty dict) and print a short diagnostic."""
    if fallback is None:
        fallback = {}

    if isinstance(text, dict):
        return text
    if hasattr(text, "model_dump"):
        try:
            dumped = text.model_dump()
            if isinstance(dumped, dict):
                return dumped
        except Exception:
            pass

    raw = text if isinstance(text, str) else repr(text)
    normalized = _normalize_json_like(raw)

    obj = _extract_json_object(normalized)
    if obj is not None:
        return obj

    try:
        return parse_json_strict(normalized)
    except Exception as e:
        msg = (
            f"[agent][warn] JSON parse failed{f' ({label})' if label else ''}: {e}. "
            f"Raw head: {raw[:200]!r}"
        )
        print(msg, file=sys.stderr, flush=True)
        return fallback


def make_agent(
    *,
    name: str,
    instructions: str,
    mcp_servers: List[Any],
    model: str,
    reasoning_effort: Optional[str] = None,
    tools: Optional[List[Any]] = None,
    output_type: Optional[Any] = None,
) -> Agent:
    """Create an Agent and prefer structured output when the SDK supports it."""
    kwargs: Dict[str, Any] = {
        "name": name,
        "instructions": instructions,
        "mcp_servers": mcp_servers,
        "model": model,
        "tools": tools or [],
        "model_settings": ModelSettings(tool_choice="required") if ModelSettings is not None else None,
    }
    if kwargs.get("model_settings") is None:
        kwargs.pop("model_settings", None)
    if reasoning_effort:
        if ModelSettings is None:
            raise RuntimeError(
                "Manager reasoning effort requires agents.ModelSettings support in the installed SDK."
            )
        kwargs["model_settings"] = ModelSettings(reasoning=Reasoning(effort=reasoning_effort), verbosity="medium")
    if output_type is not None:
        try:
            return Agent(output_type=output_type, **kwargs)
        except TypeError:
            print(
                f"[agent][warn] SDK does not support output_type for {name}; using text output fallback.",
                file=sys.stderr,
                flush=True,
            )
    return Agent(**kwargs)


async def repair_to_json(
    agent: Agent,
    bad_output: str,
    expected_schema: str,
    fallback: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Use the model to convert invalid JSON-like output into valid JSON only."""
    print("[agent][warn] attempting JSON repair", file=sys.stderr, flush=True)
    prompt = JSON_REPAIR_PROMPT.format(expected_schema=expected_schema, bad_output=bad_output)
    result = await Runner.run(agent, prompt)
    return parse_json_safe(result.final_output, fallback=fallback, label="repair_to_json")


def safe_float(x: Any) -> Optional[float]:
    """Convert a value to float, return None if conversion fails."""
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _numeric_or_none(x: Any) -> Optional[float]:
    """Return a finite float or None."""
    v = safe_float(x)
    if v is None:
        return None
    if pd.isna(v):
        return None
    return float(v)


def _pick_numeric(row: Dict[str, Any], keys: List[str]) -> Optional[float]:
    """Pick first numeric value for any matching key in row."""
    for k in keys:
        if k in row:
            v = _numeric_or_none(row.get(k))
            if v is not None:
                return v
    return None


def _indicators_to_map(indicators: Any) -> Dict[str, Optional[float]]:
    """Normalize indicator payload from dict or list[{indicator,value}] into a map."""
    out: Dict[str, Optional[float]] = {}
    if isinstance(indicators, dict):
        for k, v in indicators.items():
            name = str(k).strip()
            if not name:
                continue
            out[name] = _numeric_or_none(v)
        return out

    if isinstance(indicators, list):
        for item in indicators:
            if not isinstance(item, dict):
                continue
            name = str(item.get("indicator") or item.get("name") or "").strip()
            if not name:
                continue
            out[name] = _numeric_or_none(item.get("value"))
    return out


def _indicators_to_list(ind_map: Dict[str, Optional[float]]) -> List[Dict[str, Any]]:
    """Render indicator map as paper-style list[{indicator,value}]."""
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for k in EXPECTED_ANALYST_INDICATORS:
        rows.append({"indicator": k, "value": _numeric_or_none(ind_map.get(k))})
        seen.add(k)
    for k, v in ind_map.items():
        if k in seen:
            continue
        rows.append({"indicator": str(k), "value": _numeric_or_none(v)})
    return rows


def _normalize_date_token(x: Any) -> str:
    """Normalize a date-like value to YYYY-MM-DD when possible."""
    s = str(x).strip()
    if not s:
        return ""
    return s[:10]


def _extract_row_price(row: Dict[str, Any]) -> Optional[float]:
    """Extract a numeric price from common row schemas."""
    for k in [
        "price",
        "adj_close",
        "Adj Close",
        "price_close",
        "Close",
        "close",
        "price_avg",
        "price_open",
    ]:
        v = _numeric_or_none(row.get(k))
        if v is not None:
            return v
    return None


def _normalize_monthly_price_rows(
    rows: List[Dict[str, Any]],
    *,
    ticker: str,
    max_months: int,
) -> List[Dict[str, Any]]:
    """Normalize monthly rows into {date,ticker,price} and keep chronological order."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        date = _normalize_date_token(r.get("date"))
        price = _extract_row_price(r)
        if not date or price is None:
            continue
        row_ticker = str(r.get("ticker") or ticker).strip().upper() or str(ticker).upper()
        out.append(
            {
                "date": date,
                "ticker": row_ticker,
                "price": float(price),
            }
        )

    if not out:
        return []

    df = pd.DataFrame(out)
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values("date_dt")
    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    if max_months is not None and len(df) > max_months:
        df = df.tail(max_months)
    df = df.drop(columns=["date_dt"], errors="ignore").reset_index(drop=True)
    return df.to_dict(orient="records")


def _row_for_decision_date(monthly_window_rows: List[Dict[str, Any]], decision_date: str) -> Dict[str, Any]:
    """Pick the row matching decision_date, otherwise fall back to the latest window row."""
    target = _normalize_date_token(decision_date)
    for row in reversed(monthly_window_rows):
        if not isinstance(row, dict):
            continue
        if _normalize_date_token(row.get("date")) == target:
            return dict(row)

    for row in reversed(monthly_window_rows):
        if isinstance(row, dict):
            return dict(row)
    return {}


def _build_results_row(
    *,
    ticker: str,
    decision_date: str,
    month_price: float,
    monthly_window_rows: List[Dict[str, Any]],
    analyst_indicators: Dict[str, Any],
    previous_decision: Dict[str, Any],
) -> Dict[str, Any]:
    """Build Thiago-style results row with panel context plus analyst indicators."""
    row = _row_for_decision_date(monthly_window_rows, decision_date)
    out: Dict[str, Any] = dict(row) if isinstance(row, dict) else {}

    out["ticker"] = str(ticker)
    out["date"] = str(decision_date)
    out["month_price"] = float(month_price)

    indicator_map = _indicators_to_map(analyst_indicators)
    for k, v in indicator_map.items():
        key = str(k).strip()
        if not key:
            continue
        out[key] = _numeric_or_none(v)

    # Keep this explicit and consistent for manager decision context.
    out["last_price"] = float(month_price)
    out["PREVIOUS_JUSTIFICATION"] = previous_decision.get("Justification", "N/A")
    out["PREVIOUS_TARGET_PRICE"] = previous_decision.get("Target Price", "N/A")
    out["PREVIOUS_RECOMMENDATION"] = previous_decision.get("Recommendation", "N/A")
    return out


def _build_manager_panel_table(
    *,
    fundamental_analyses: List[Dict[str, Any]],
    ticker: str,
    max_months: int,
) -> str:
    """Render the manager's rolling 12-month panel table for a ticker."""
    rows = [r for r in fundamental_analyses if str(r.get("ticker", "")).upper() == str(ticker).upper()]
    if not rows:
        return "(empty)"

    df = pd.DataFrame(rows).copy()
    if "date" in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date_dt").drop(columns=["date_dt"], errors="ignore")
    if max_months is not None and len(df) > max_months:
        df = df.tail(max_months).reset_index(drop=True)
    return df.to_string(index=False)


def _validate_analyst_output(
    *,
    raw_output: Any,
    ticker: str,
    as_of_date: str,
    window_months_requested: int,
    window_rows_used: int,
    price_used: float,
    base_errors: List[str],
    sources_fallback: List[str],
) -> Dict[str, Any]:
    """Build a strict paper-style analyst object: {'indicators': [{indicator,value}, ...]}."""
    _ = (
        ticker,
        as_of_date,
        window_months_requested,
        window_rows_used,
        price_used,
        sources_fallback,
    )
    if base_errors:
        print(
            f"[agent][warn] analyst parse/repair issues ({ticker} {as_of_date}): {len(base_errors)}",
            file=sys.stderr,
            flush=True,
        )

    raw_indicators_any: Any = raw_output.get("indicators", []) if isinstance(raw_output, dict) else raw_output
    if not isinstance(raw_indicators_any, (dict, list)):
        raw_indicators_any = []
        print(
            f"[agent][warn] analyst indicators payload invalid ({ticker} {as_of_date}); using null-filled fallback",
            file=sys.stderr,
            flush=True,
        )
    raw_indicators = _indicators_to_map(raw_indicators_any)

    out_rows: List[Dict[str, Any]] = []
    missing_expected: List[str] = []
    for k in EXPECTED_ANALYST_INDICATORS:
        v = _numeric_or_none(raw_indicators.get(k))
        out_rows.append({"indicator": k, "value": v})
        if v is None:
            missing_expected.append(k)

    missing_core = [k for k in REQUIRED_INDICATORS if k in set(missing_expected)]
    if missing_core:
        print(
            f"[agent][warn] analyst core indicators missing_or_null ({ticker} {as_of_date}): {', '.join(missing_core)}",
            file=sys.stderr,
            flush=True,
        )

    if missing_expected:
        print(
            f"[agent][warn] analyst expected indicators still null ({ticker} {as_of_date}): {len(missing_expected)}",
            file=sys.stderr,
            flush=True,
        )
    return {"indicators": out_rows}


def _missing_expected_indicators(indicators: Any) -> List[str]:
    """Return expected indicators that are missing/null."""
    ind_map = _indicators_to_map(indicators)
    missing: List[str] = []
    for k in EXPECTED_ANALYST_INDICATORS:
        if _numeric_or_none(ind_map.get(k)) is None:
            missing.append(k)
    return missing


def _validate_manager_output(
    *,
    raw_output: Dict[str, Any],
    ticker: str,
    date: str,
    first_month: bool,
    window_rows_used: int,
    used_price: float,
) -> Dict[str, Any]:
    """Build a strict paper-style manager object: recommendation, target_price, justification."""
    _ = (ticker, date, first_month, window_rows_used, used_price)
    raw_rec = raw_output.get("recommendation") or raw_output.get("action")
    rec = normalize_action(raw_rec)
    target_price = _numeric_or_none(raw_output.get("target_price"))
    if target_price is None:
        target_price = float(used_price)
        print(
            f"[agent][warn] manager validation issue ({ticker} {date}): target_price missing; defaulted to used_price",
            file=sys.stderr,
            flush=True,
        )

    justification = raw_output.get("justification", "")
    if not isinstance(justification, str) or not justification.strip():
        justification = "Insufficient evidence for stronger action; defaulting to HOLD."
        print(
            f"[agent][warn] manager validation issue ({ticker} {date}): empty justification replaced",
            file=sys.stderr,
            flush=True,
        )
    if raw_rec is None or str(raw_rec).strip().upper() not in {"BUY", "SELL", "HOLD", "KEEP"}:
        print(
            f"[agent][warn] manager validation issue ({ticker} {date}): recommendation normalised to {rec}",
            file=sys.stderr,
            flush=True,
        )

    return {
        "recommendation": rec,
        "target_price": target_price,
        "justification": justification.strip(),
    }


def pick_first_trading_day_per_month(rows: List[Dict[str, Any]], max_months: int = 12) -> List[Dict[str, Any]]:
    """Collapse daily rows into one row per calendar month using the first trading day."""
    if not rows:
        return []

    df = pd.DataFrame(rows).copy()
    if "date" not in df.columns:
        return []

    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"]).sort_values("date_dt").reset_index(drop=True)
    if df.empty:
        return []

    df["month"] = df["date_dt"].dt.to_period("M").astype(str)

    monthly = (
        df.drop_duplicates(subset=["month"], keep="first")
        .sort_values("date_dt")
        .reset_index(drop=True)
    )

    if max_months is not None and len(monthly) > max_months:
        monthly = monthly.iloc[-max_months:].reset_index(drop=True)

    monthly = monthly.drop(columns=["date_dt", "month"], errors="ignore")
    return monthly.to_dict(orient="records")


async def fetch_monthly_window(agent: Agent, ticker: str, as_of_date: str, months: int = 12) -> Dict[str, Any]:
    """Fetch the Thiago style 12-row monthly table up to as_of_date."""

    prompt = (
        f'Use the tool get_monthly_window for ticker "{ticker}" as_of_date "{as_of_date}" '
        f'with months={int(months)}, then return JSON with the tool output only.'
    )
    result = await Runner.run(agent, prompt)

    expected_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA"}], "n": 12 }'
    try:
        return parse_json_strict(result.final_output)
    except json.JSONDecodeError:
        print("[agent][warn] get_monthly_window payload was not strict JSON", file=sys.stderr, flush=True)
        return await repair_to_json(
            agent,
            result.final_output,
            expected_schema,
            fallback={"rows": [], "n": 0},
        )


async def fetch_monthly_rows(analyst: Agent, ticker: str, test_start: str, test_end: str, max_months: int = 12) -> List[Dict[str, Any]]:
    """Fetch monthly price rows for one ticker, prefer monthly tool, fall back to daily then collapse."""
    prompt_monthly = (
        f'Use the tool get_monthly_price_series for ticker "{ticker}" from "{test_start}" to "{test_end}" '
        'with price_field "Adj Close", then return JSON with the tool output only.'
    )

    monthly_payload = await Runner.run(analyst, prompt_monthly)

    try:
        monthly_json = parse_json_strict(monthly_payload.final_output)
    except json.JSONDecodeError:
        print("[agent][warn] get_monthly_price_series payload was not strict JSON", file=sys.stderr, flush=True)
        expected_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA","price":123.45}], "n": 12 }'
        monthly_json = await repair_to_json(
            analyst,
            monthly_payload.final_output,
            expected_schema,
            fallback={"rows": [], "n": 0},
        )

    rows = monthly_json.get("rows", [])
    if isinstance(rows, list) and rows:
        normalized = _normalize_monthly_price_rows(
            rows,
            ticker=ticker,
            max_months=max_months,
        )
        if normalized:
            return normalized
        sample_keys = sorted(list(rows[0].keys())) if rows and isinstance(rows[0], dict) else []
        print(
            f"[agent][warn] get_monthly_price_series rows could not be normalized ({ticker}); sample_keys={sample_keys}",
            file=sys.stderr,
            flush=True,
        )

    prompt_daily = (
        f'Use the tool get_price_series for ticker "{ticker}" from "{test_start}" to "{test_end}" '
        'with price_field "Adj Close", then return JSON with the tool output only.'
    )
    daily_payload = await Runner.run(analyst, prompt_daily)

    try:
        daily_json = parse_json_strict(daily_payload.final_output)
    except json.JSONDecodeError:
        print("[agent][warn] get_price_series payload was not strict JSON", file=sys.stderr, flush=True)
        expected_schema = '{ "rows": [{"date":"2025-01-02","ticker":"TSLA","price":123.45}], "n": 252 }'
        daily_json = await repair_to_json(
            analyst,
            daily_payload.final_output,
            expected_schema,
            fallback={"rows": [], "n": 0},
        )

    daily_rows = daily_json.get("rows", [])
    if not isinstance(daily_rows, list) or not daily_rows:
        return []

    monthly_rows = pick_first_trading_day_per_month(daily_rows, max_months=max_months)
    return _normalize_monthly_price_rows(
        monthly_rows,
        ticker=ticker,
        max_months=max_months,
    )


def load_experiment_config() -> Dict[str, Any]:
    """Load config/experiment.json if present, otherwise return an empty dict."""
    p = Path("config") / "experiment.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_tickers(cli_tickers: Optional[str]) -> List[str]:
    """Resolve tickers from CLI, otherwise from config, otherwise from defaults."""
    if cli_tickers:
        out = [t.strip().upper() for t in cli_tickers.split(",") if t.strip()]
        return out

    cfg = load_experiment_config()
    cfg_tickers = cfg.get("tickers")
    if isinstance(cfg_tickers, list) and cfg_tickers:
        return [str(t).strip().upper() for t in cfg_tickers if str(t).strip()]

    return DEFAULT_TICKERS


def get_date_range(cli_test_start: Optional[str], cli_test_end: Optional[str]) -> tuple[str, str]:
    """Resolve test_start and test_end from CLI args, then config, then defaults."""
    cfg = load_experiment_config()
    
    test_start = cli_test_start or str(cfg.get("test_start", "2024-01-02"))
    test_end = cli_test_end or str(cfg.get("test_end", "2025-12-31"))
    
    return test_start, test_end


async def run_one_ticker(
    analyst: Agent,
    manager: Agent,
    ticker: str,
    test_start: str,
    test_end: str,
    max_months: int = 12,
    reflection: bool = True,
    reflection_max_rounds: int = 1,
    out_dir: Path = Path("results") / "experiments" / "monthly_agent_workflow",
) -> None:
    """
    Thiago-style monthly loop: per month run analyst -> manager, keep rolling memory, persist per-step files.
    """
    rows = await fetch_monthly_rows(analyst, ticker, test_start, test_end, max_months=max_months)
    out_dir.mkdir(parents=True, exist_ok=True)
    ticker_dir = out_dir / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    fundamental_analyses: List[Dict[str, Any]] = []
    manager_decisions: List[Dict[str, Any]] = []
    outputs: List[Dict[str, Any]] = []
    action_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}
    months_processed = 0
    months_with_null_indicators = 0

    for r in rows:
        date = _normalize_date_token(r.get("date"))
        price_today = _extract_row_price(r)
        if not date or price_today is None:
            print(f"[agent][warn] skipped malformed row for {ticker}: {r!r}", file=sys.stderr, flush=True)
            continue

        months_processed += 1

        monthly_window_payload = await fetch_monthly_window(analyst, ticker, date, months=max_months)
        monthly_window_rows = monthly_window_payload.get("rows", [])
        if not isinstance(monthly_window_rows, list):
            print(f"[agent][warn] invalid monthly window rows for {ticker} {date}", file=sys.stderr, flush=True)
            monthly_window_rows = []
        monthly_window_rows = [row for row in monthly_window_rows if isinstance(row, dict)]
        if monthly_window_rows:
            monthly_window_df = pd.DataFrame(monthly_window_rows).copy()
            if "date" in monthly_window_df.columns:
                monthly_window_df["date_dt"] = pd.to_datetime(monthly_window_df["date"], errors="coerce")
                monthly_window_df = monthly_window_df.sort_values("date_dt").drop(columns=["date_dt"], errors="ignore")
            if max_months is not None and len(monthly_window_df) > max_months:
                monthly_window_df = monthly_window_df.tail(max_months).reset_index(drop=True)
            monthly_window_rows = monthly_window_df.to_dict(orient="records")
        window_rows_used = int(len(monthly_window_rows))

        compute_errors: List[str] = []

        # Analyst step with strict parse/repair/validation
        analyst_result = await Runner.run(analyst, analyst_prompt(ticker=ticker, as_of_date=date))
        analyst_schema = '{ "indicators": [{"indicator":"Assets","value":null}] }'
        analyst_parse_errors: List[str] = []
        analyst_raw = parse_json_safe(analyst_result.final_output, fallback={}, label="analyst_output")
        if not analyst_raw:
            analyst_parse_errors.append("analyst output parse failed; attempting repair")
            analyst_raw = await repair_to_json(
                analyst,
                str(analyst_result.final_output),
                analyst_schema,
                fallback={},
            )
            if not analyst_raw:
                analyst_parse_errors.append("analyst output repair failed; fallback object used")
                print(f"[agent][warn] analyst output invalid after repair ({ticker} {date})", file=sys.stderr, flush=True)

        prev_for_ticker = [d for d in manager_decisions if d.get("stock_id") == ticker]
        previous_decision = (
            {
                "Justification": prev_for_ticker[-1]["justification"],
                "Target Price": prev_for_ticker[-1]["target_price"],
                "Recommendation": prev_for_ticker[-1]["recommendation"],
            }
            if prev_for_ticker
            else {
                "Justification": "N/A",
                "Target Price": "N/A",
                "Recommendation": "N/A",
            }
        )

        analyst_json = _validate_analyst_output(
            raw_output=analyst_raw,
            ticker=ticker,
            as_of_date=date,
            window_months_requested=int(max_months),
            window_rows_used=window_rows_used,
            price_used=price_today,
            base_errors=analyst_parse_errors + compute_errors,
            sources_fallback=["get_monthly_window", "get_companyfacts"],
        )

        # Paper-faithful reflection: if expected indicators are missing, re-call analyst with explicit feedback.
        reflection_used = 0
        if reflection and int(reflection_max_rounds) > 0:
            for rr in range(int(reflection_max_rounds)):
                missing = _missing_expected_indicators(analyst_json.get("indicators", {}))
                if not missing:
                    break

                reflection_used += 1
                reflection_prompt = (
                    analyst_prompt(ticker=ticker, as_of_date=date)
                    + "\n\nFeedback: Compute ONLY the following missing indicators and return full JSON: "
                    + json.dumps(missing)
                )
                reflected_result = await Runner.run(analyst, reflection_prompt)
                reflected_raw = parse_json_safe(
                    reflected_result.final_output,
                    fallback={},
                    label=f"analyst_reflection_round_{rr + 1}",
                )
                if not reflected_raw:
                    reflected_raw = await repair_to_json(
                        analyst,
                        str(reflected_result.final_output),
                        analyst_schema,
                        fallback={},
                    )

                base_map = _indicators_to_map(analyst_json.get("indicators", {}))
                reflected_map = _indicators_to_map(reflected_raw.get("indicators", {}))
                if not reflected_map:
                    print(
                        f"[agent][warn] reflection round {rr + 1} returned no indicators ({ticker} {date})",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue

                filled = 0
                for k in missing:
                    v = _numeric_or_none(reflected_map.get(k))
                    if v is None:
                        continue
                    if _numeric_or_none(base_map.get(k)) is None:
                        base_map[k] = v
                        filled += 1
                analyst_json["indicators"] = _indicators_to_list(base_map)
                print(
                    f"[agent][debug] reflection round {rr + 1} requested={len(missing)} filled={filled} ({ticker} {date})",
                    flush=True,
                )

            if reflection_used:
                print(
                    f"[agent][debug] reflection rounds used={reflection_used} ({ticker} {date})",
                    flush=True,
                )

        ind_map_for_count = _indicators_to_map(analyst_json.get("indicators", {}))
        if any(_numeric_or_none(ind_map_for_count.get(k)) is None for k in REQUIRED_INDICATORS):
            months_with_null_indicators += 1
        print(
            f"[agent][debug] {ticker} {date} window_rows_used={window_rows_used}",
            flush=True,
        )

        fundamental_analyses.append(
            _build_results_row(
                ticker=ticker,
                decision_date=date,
                month_price=price_today,
                monthly_window_rows=monthly_window_rows,
                analyst_indicators=analyst_json.get("indicators", {}),
                previous_decision=previous_decision,
            )
        )

        manager_panel_text = _build_manager_panel_table(
            fundamental_analyses=fundamental_analyses,
            ticker=ticker,
            max_months=max_months,
        )

        manager_prompt = MANAGER_MONTHLY_TASK_PROMPT.format(
            ticker=ticker,
            date=date,
            manager_panel_table=manager_panel_text,
        )

        manager_result = await Runner.run(manager, manager_prompt)
        manager_schema = '{ "recommendation": "HOLD", "target_price": 123.45, "justification": "..." }'
        manager_raw = parse_json_safe(manager_result.final_output, fallback={}, label="manager_output")
        if not manager_raw:
            manager_raw = await repair_to_json(
                manager,
                str(manager_result.final_output),
                manager_schema,
                fallback={"recommendation": "HOLD", "target_price": float(price_today), "justification": "repair_failed"},
            )
            if not manager_raw:
                print(f"[agent][warn] manager output invalid after repair ({ticker} {date})", file=sys.stderr, flush=True)

        manager_json = _validate_manager_output(
            raw_output=manager_raw,
            ticker=ticker,
            date=date,
            first_month=(len(prev_for_ticker) == 0),
            window_rows_used=window_rows_used,
            used_price=price_today,
        )
        action_counts[manager_json["recommendation"]] += 1

        manager_decisions.append(
            {
                "analysis_date": date,
                "stock_id": ticker,
                "recommendation": manager_json["recommendation"],
                "target_price": manager_json["target_price"],
                "justification": manager_json["justification"],
            }
        )

        outputs.append(
            {
                "date": date,
                "price": price_today,
                "analyst": analyst_json,
                "manager": manager_json,
            }
        )

        # Thiago-style per-step artifacts and rolling snapshots.
        (ticker_dir / f"{date}_analyst_0.json").write_text(
            json.dumps(analyst_json, indent=2),
            encoding="utf-8",
        )
        (ticker_dir / f"{date}_manager_0.json").write_text(
            json.dumps(manager_json, indent=2),
            encoding="utf-8",
        )
        (out_dir / f"{ticker}_results_sample.json").write_text(
            json.dumps(fundamental_analyses, indent=2),
            encoding="utf-8",
        )
        (out_dir / f"{ticker}_decisions_sample.json").write_text(
            json.dumps(manager_decisions, indent=2),
            encoding="utf-8",
        )
        (out_dir / f"{ticker}_all_in_one.json").write_text(
            json.dumps(
                {
                    "ticker": ticker,
                    "results_sample": fundamental_analyses,
                    "decisions_sample": manager_decisions,
                    "outputs": outputs,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    out_path = out_dir / f"{ticker}_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(
        json.dumps(
            {
                "ticker": ticker,
                "results_sample": fundamental_analyses,
                "decisions_sample": manager_decisions,
                "outputs": outputs,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Saved: {out_path}")
    print(f"Saved: {out_dir / f'{ticker}_all_in_one.json'}")
    print(f"{ticker} monthly decisions saved: {len(outputs)}")
    print(
        f"[agent][summary] {ticker}: months={months_processed}, "
        f"months_with_null_indicators={months_with_null_indicators}, "
        f"BUY={action_counts['BUY']} SELL={action_counts['SELL']} HOLD={action_counts['HOLD']}",
        flush=True,
    )


async def main() -> None:
    """Run the monthly workflow for one or more tickers and write one output file per ticker."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers, e.g. TSLA,MSFT,AAPL")
    parser.add_argument("--max_months", type=int, default=12, help="Number of monthly decision points to keep")
    parser.add_argument("--model", type=str, default="gpt-5-mini", help="Model name to use for agents")
    parser.add_argument(
        "--reasoning_effort",
        type=str,
        default="medium",
        help="Reasoning effort for analyst; manager remains fixed to high.",
    )
    parser.add_argument("--test_start", type=str, default=None, help="Test start date (YYYY-MM-DD)")
    parser.add_argument("--test_end", type=str, default=None, help="Test end date (YYYY-MM-DD)")
    parser.add_argument(
        "--reflection",
        dest="reflection",
        action="store_true",
        default=True,
        help="Enable paper-style reflection loop for missing analyst indicators (default: enabled).",
    )
    parser.add_argument(
        "--no_reflection",
        dest="reflection",
        action="store_false",
        help="Disable analyst reflection loop.",
    )
    parser.add_argument(
        "--reflection_max_rounds",
        type=int,
        default=1,
        help="Maximum analyst reflection rounds when --reflection is enabled.",
    )
    parser.add_argument("--server_path", type=str, default="finAgents/server_us_finance.py", help="Path to MCP server script")
    parser.add_argument(
        "--out_dir",
        type=str,
        default="results/experiments/monthly_agent_workflow_us2025",
        help="Output directory for JSON results",
    )
    args = parser.parse_args()

    tickers = get_tickers(args.tickers)
    test_start, test_end = get_date_range(args.test_start, args.test_end)

    server_path = Path(args.server_path).resolve()
    if not server_path.exists():
        raise FileNotFoundError(f"MCP server script not found: {server_path}")

    out_dir = Path(args.out_dir).resolve()
    if not _supports_reasoning(str(args.model).strip()):
        raise ValueError(
            "Financial manager is fixed to reasoning_effort='high'; choose a reasoning-capable model (o3 or gpt-5)."
        )

    async with MCPServerStdio(
        name="US Finance MCP Server",
        params={"command": sys.executable, "args": [str(server_path)]},
        cache_tools_list=True,
    ) as server:  # Reuse the same MCP server for both agents.
        analyst = make_agent(
            name="financial_analyst",
            instructions=FINANCIAL_ANALYST_INSTRUCTIONS,
            mcp_servers=[server],
            model=args.model,
            reasoning_effort=str(args.reasoning_effort).strip().lower() or None,
            tools=[code_interpreter],
        )
        manager = make_agent(
            name="financial_manager",
            instructions=FINANCIAL_MANAGER_INSTRUCTIONS,
            mcp_servers=[server],
            model=args.model,
            reasoning_effort="high",
            tools=[code_interpreter],
        )

        for t in tickers:
            await run_one_ticker(
                analyst=analyst,
                manager=manager,
                ticker=t,
                test_start=test_start,
                test_end=test_end,
                max_months=int(args.max_months),
                reflection=bool(args.reflection),
                reflection_max_rounds=int(args.reflection_max_rounds),
                out_dir=out_dir,
            )


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Workflow baseline monthly runner (no tools).

What this script does:
- Reads the prepared monthly panel CSV.
- For each ticker, it iterates month by month.
- At each month i, it builds a rolling window (default 12 months) ending at i.
- Sends that window directly to the LLM manager (no MCP tools, no database access).
- Saves one JSON file per ticker.

Outputs:
- results/experiments/monthly_workflow_workflow/<TICKER>_workflow_output_<timestamp>.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from agents import Agent, ModelSettings, Runner, function_tool
from openai.types.shared import Reasoning
from typing_extensions import TypedDict

def _supports_reasoning(model: str) -> bool:
    """Return True if the model name supports explicit reasoning effort controls."""
    m = model.lower().strip()
    return m.startswith("o3") or m.startswith("gpt-5")

DEBUG = (os.environ.get("FIN_AGENT_DEBUG") or "").lower() in {"1", "true", "yes"}


class CodeInterpreterInput(TypedDict):
    code: str


@function_tool
def code_interpreter(inp: CodeInterpreterInput) -> dict:
    """A simple Python tool mirroring the reference implementation."""
    try:
        result = subprocess.run(
            [sys.executable, "-c", inp.get("code")],
            capture_output=True,
        )
        report = f"StdOut:\n{result.stdout}\nStdErr:\n{result.stderr}"
        return {"status": "success", "report": report}
    except Exception as e:
        return {"status": "error", "report": f"Failed to run code: {e}"}


@dataclass(frozen=True)
class RunConfig:
    """Holds all run-time configuration for the workflow runner."""
    monthly_panel_csv: Path
    out_dir: Path
    model: str
    indicator_reasoning_effort: Optional[str]
    manager_reasoning_effort: Optional[str]
    lookback_months: int
    max_months: int
    tickers_csv: Optional[str]
    test_start: str
    test_end: str
    max_output_tokens: int
    indicator_max_output_tokens: int
    use_code_interpreter: bool
    gold_csv: Optional[Path]
    spot_check_month: Optional[str]
    spot_check_tolerance: float
    strict_paper: bool


def load_tickers(tickers_csv: Optional[str]) -> List[str]:
    """Resolve tickers from CLI input, else config/experiment.json, else defaults."""
    if tickers_csv:
        return [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]

    exp_path = Path("config") / "experiment.json"
    if exp_path.exists():
        try:
            data = json.loads(exp_path.read_text(encoding="utf-8"))
            tickers = data.get("tickers") or data.get("TICKERS") or []
            if isinstance(tickers, list) and tickers:
                return [str(t).strip().upper() for t in tickers if str(t).strip()]
        except Exception:
            pass

    return ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]


def load_monthly_panel(path: Path) -> pd.DataFrame:
    """Load the monthly panel CSV and normalise expected columns."""
    if not path.exists():
        raise FileNotFoundError("Monthly panel not found: {0}".format(path))

    df = pd.read_csv(path, low_memory=False)

    if "ticker" not in df.columns and "TICKER" in df.columns:
        df = df.rename(columns={"TICKER": "ticker"})

    if "date" not in df.columns:
        raise ValueError("Monthly panel must contain a 'date' column")
    if "ticker" not in df.columns:
        raise ValueError("Monthly panel must contain a 'ticker' column")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def filter_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Filter rows to the closed interval [start_date, end_date]."""
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise ValueError("Invalid --test_start or --test_end date format. Use YYYY-MM-DD.")
    return df[(df["date"] >= start) & (df["date"] <= end)].copy()


def make_window_table(df_t: pd.DataFrame, end_idx: int, lookback_months: int) -> pd.DataFrame:
    """Create a rolling window ending at position end_idx (inclusive)."""
    start_idx = max(0, end_idx - (lookback_months - 1))
    return df_t.iloc[start_idx : end_idx + 1].copy()


def _format_cell(v: Any) -> str:
    """Format a cell for a compact markdown table."""
    if v is None:
        return ""
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        return "{0:.6g}".format(v)
    if isinstance(v, int):
        return str(v)
    return str(v)


def df_to_markdown_table(df_window: pd.DataFrame) -> str:
    """Render a simple markdown table from a DataFrame."""
    if df_window is None or df_window.empty:
        return "(empty)"

    df2 = df_window.copy()

    all_cols = set(df2.columns)

    priority_cols = [
        "date",
        "price_open",
        "price_close",
        "price_avg",
        "price_min",
        "price_max",
        "price_volume",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "Revenues",
        "NetIncomeLoss",
        "OperatingIncomeLoss",
        "EarningsPerShareBasic",
        "ret_1d",
        "ret_5d",
        "ret_20d",
        "vol_20d",
        "vol_60d",
    ]

    available_cols = [c for c in priority_cols if c in all_cols]

    if len(available_cols) < 5:
        extra_candidates: List[str] = []
        for c in all_cols:
            if c in available_cols or c in ["ticker", "year_month"]:
                continue
            if any(
                x in c.lower()
                for x in [
                    "price",
                    "revenue",
                    "income",
                    "asset",
                    "liabilit",
                    "equity",
                    "debt",
                    "cash",
                    "ebit",
                    "eps",
                    "return",
                    "ret_",
                    "vol_",
                    "volume",
                ]
            ):
                extra_candidates.append(c)
        available_cols = available_cols + extra_candidates[:8]

    df2 = df2[available_cols].copy()

    if "ticker" in df2.columns:
        df2 = df2.drop(columns=["ticker"], errors="ignore")

    if "date" in df2.columns:
        df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    cols = list(df2.columns)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"

    lines = [header, sep]
    for _, row in df2.iterrows():
        cells = [_format_cell(row.get(c)) for c in cols]
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


def _pick_numeric(row: Dict[str, Any], keys: List[str]) -> Optional[float]:
    """Pick the first finite numeric value for any key in the row."""
    for k in keys:
        if k not in row:
            continue
        try:
            v = float(row.get(k))
        except Exception:
            continue
        if not pd.isna(v) and math.isfinite(v):
            return float(v)
    return None


def _safe_ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
    """Compute a finite ratio when denominator is not near zero."""
    if num is None or den is None:
        return None
    if abs(float(den)) < 1e-12:
        return None
    v = float(num) / float(den)
    if not math.isfinite(v):
        return None
    return float(v)


def compute_deterministic_indicators(window_rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """Compute fallback indicator estimates from the latest monthly row."""
    row = window_rows[-1] if window_rows else {}
    if not isinstance(row, dict):
        row = {}

    last_price = _pick_numeric(row, ["adj_close", "price_close", "price_avg", "price_open", "Close"])
    equity = _pick_numeric(row, ["StockholdersEquity", "ShareholdersEquity"])
    eps = _pick_numeric(row, ["EarningsPerShareBasic", "EPS"])
    shares = _pick_numeric(row, ["CommonStockSharesOutstanding", "WeightedAverageNumberOfSharesOutstandingBasic"])
    cash = _pick_numeric(
        row,
        ["CashAndCashEquivalentsAtCarryingValue", "CashAndEquivalents", "Cash", "cash_and_equivalents"],
    )
    revenue = _pick_numeric(row, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"])
    ebit = _pick_numeric(row, ["OperatingIncomeLoss", "EBIT"])
    net_profit = _pick_numeric(row, ["NetIncomeLoss", "NetProfit"])

    bvps = _safe_ratio(equity, shares) if equity is not None and shares is not None else None
    pe = _safe_ratio(last_price, eps) if last_price is not None and eps is not None else None
    pb = _safe_ratio(last_price, bvps) if last_price is not None and bvps is not None else None

    return {
        "CashAndEquivalents": cash,
        "NetRevenue": revenue,
        "EBIT": ebit,
        "NetProfit": net_profit,
        "P_E": pe,
        "P_B": pb,
        "EPS": eps,
        "BVPS": bvps,
        "last_price": last_price,
    }


def detect_indicator_sanity_issues(indicators: Dict[str, Optional[float]]) -> List[str]:
    """Flag suspicious valuation outputs for quick debugging."""
    issues: List[str] = []
    pe = indicators.get("P_E")
    pb = indicators.get("P_B")
    price = indicators.get("last_price")
    bvps = indicators.get("BVPS")

    if pe is not None and pe > 100:
        issues.append("P_E above 100; valuation may be extreme or earnings input may be wrong.")
    if pe is not None and pe > 170:
        issues.append("P_E above 170 (roughly >170-year payback at current earnings).")
    if pb is not None and pb > 20:
        issues.append("P_B above 20; verify book value and share count inputs.")
    if pb is not None and pb < 0:
        issues.append("P_B is negative; check equity sign and denominator handling.")
    if price is not None and bvps is not None and price > 100 and 0 < bvps < 15:
        issues.append("High price with very low BVPS; verify book value normalization.")
    return issues


def _indicator_json_schema() -> Dict[str, Any]:
    """Schema for the workflow indicator extraction step."""
    return {
        "type": "json_schema",
        "name": "workflow_indicators",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "CashAndEquivalents": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "NetRevenue": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "EBIT": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "NetProfit": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "P_E": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "P_B": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "EPS": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "BVPS": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "last_price": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "notes": {"type": "string"},
            },
            "required": [
                "CashAndEquivalents",
                "NetRevenue",
                "EBIT",
                "NetProfit",
                "P_E",
                "P_B",
                "EPS",
                "BVPS",
                "last_price",
                "notes",
            ],
        },
        "strict": True,
    }


def _validate_indicator_json(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize indicator output into a fixed numeric/null object."""
    keys = ["CashAndEquivalents", "NetRevenue", "EBIT", "NetProfit", "P_E", "P_B", "EPS", "BVPS", "last_price"]
    out: Dict[str, Any] = {}
    for k in keys:
        v = obj.get(k)
        if v is None:
            out[k] = None
            continue
        try:
            fv = float(v)
        except Exception:
            out[k] = None
            continue
        out[k] = float(fv) if math.isfinite(fv) else None
    notes = obj.get("notes", "")
    out["notes"] = notes.strip() if isinstance(notes, str) else str(notes)
    return out


def build_indicator_prompt(ticker: str, decision_date: str, window_table_md: str) -> str:
    """Prompt for indicator extraction from the rolling monthly table."""
    return """
You are computing key fundamental indicators for a monthly stock workflow.

Ticker: {ticker}
Decision date: {decision_date}

Monthly table (last row is current decision month):
{window_table_md}

Compute these fields from the table only:
- CashAndEquivalents
- NetRevenue
- EBIT
- NetProfit
- EPS
- BVPS
- P_E (price / EPS)
- P_B (price / BVPS)
- last_price

Rules:
- Use only values present in the table.
- If a field cannot be computed reliably, return null for that field.
- Do not invent data.
- Return strict JSON only that matches the schema.
- Include a short notes string on assumptions or missing fields.
""".format(
        ticker=ticker,
        decision_date=decision_date,
        window_table_md=window_table_md,
    ).strip()


def get_gold_indicator_snapshot(
    gold_df: Optional[pd.DataFrame],
    ticker: str,
    decision_date: str,
) -> Dict[str, Optional[float]]:
    """Fetch comparable gold indicator values for a ticker/date."""
    if gold_df is None or gold_df.empty:
        return {}

    g = gold_df.copy()
    g["ticker"] = g["ticker"].astype(str).str.upper().str.strip()
    g["date"] = pd.to_datetime(g["date"], errors="coerce")
    dt = pd.to_datetime(decision_date, errors="coerce")
    if pd.isna(dt):
        return {}

    sub = g[(g["ticker"] == str(ticker).upper()) & (g["date"] == dt)]
    if sub.empty:
        month = str(dt.to_period("M"))
        gm = g[g["ticker"] == str(ticker).upper()].copy()
        gm["month"] = gm["date"].dt.to_period("M").astype(str)
        sub = gm[gm["month"] == month].sort_values("date").head(1)
    if sub.empty:
        return {}

    row = sub.iloc[0].to_dict()
    return {
        "CashAndEquivalents": _pick_numeric(row, ["CashAndCashEquivalentsAtCarryingValue", "CashAndEquivalents", "Cash"]),
        "NetRevenue": _pick_numeric(row, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"]),
        "EBIT": _pick_numeric(row, ["OperatingIncomeLoss", "EBIT"]),
        "NetProfit": _pick_numeric(row, ["NetIncomeLoss", "NetProfit"]),
        "P_E": _pick_numeric(row, ["P_E", "PE"]),
        "P_B": _pick_numeric(row, ["P_B", "PB"]),
        "last_price": _pick_numeric(row, ["adj_close", "price_close", "price_avg", "price_open", "Close"]),
    }


def build_spot_check_report(
    *,
    predicted: Dict[str, Optional[float]],
    gold: Dict[str, Optional[float]],
    tolerance: float,
) -> Dict[str, Any]:
    """Create relative-error checks against gold values for selected indicators."""
    keys = ["CashAndEquivalents", "NetRevenue", "EBIT", "NetProfit", "P_E", "P_B", "last_price"]
    rows: List[Dict[str, Any]] = []
    fail_count = 0
    for k in keys:
        p = predicted.get(k)
        g = gold.get(k)
        if p is None or g is None:
            rows.append({"indicator": k, "predicted": p, "gold": g, "relative_error": None, "within_tolerance": None})
            continue
        denom = max(abs(float(g)), 1e-9)
        rel = abs(float(p) - float(g)) / denom
        ok = bool(rel <= tolerance)
        if not ok:
            fail_count += 1
        rows.append(
            {
                "indicator": k,
                "predicted": float(p),
                "gold": float(g),
                "relative_error": float(rel),
                "within_tolerance": ok,
            }
        )
    return {
        "tolerance": float(tolerance),
        "failed_indicators": int(fail_count),
        "comparisons": rows,
    }


def build_manager_prompt(
    ticker: str,
    decision_date: str,
    window_table_md: str,
    indicator_json: Dict[str, Any],
    sanity_issues: List[str],
    previous_decision: Optional[dict],
) -> str:
    """Build the workflow manager prompt with clear rules and strict output requirements."""
    keep_band = 0.05  # 5% band where HOLD is acceptable
    previous_block = ""
    if previous_decision is not None:
        previous_block = (
            "Previous decision (for continuity):\n{0}\n".format(json.dumps(previous_decision, indent=2))
        )
    else:
        # Explicitly surface N/A for the first month, mirroring Thiago's pattern.
        previous_block = (
            "Previous decision (for continuity):\n"
            '{\n  "Recommendation": "N/A",\n'
            '  "Target Price": "N/A",\n'
            '  "Justification": "N/A"\n}\n'
        )

    prompt = """
You are a financial manager performing monthly fundamental analysis for long term investing.

You have a rolling monthly table below. Each row is one month. The last row is the current decision month.

Task:
1) Estimate a fair value for the stock (target_price) using the table.
2) Compare the current month price (last row price) to your target_price.
3) Choose exactly one recommendation using a strict band:
   - BUY: current price < target_price * (1 - {keep_band:.0%})
   - SELL: current price > target_price * (1 + {keep_band:.0%})
   - HOLD: only if current price is within ±{keep_band:.0%} of target_price

Ticker: {ticker}
Decision date: {decision_date}

Computed key indicators:
{indicator_json}

Sanity checks:
{sanity_block}

Monthly table:
{window_table_md}

{previous_block}

Return STRICT JSON only:
{{ "recommendation": "BUY|HOLD|SELL", "target_price": number|null, "justification": "..." }}

Rules:
- Do not output N/A for recommendation, target_price, or justification.
- justification must mention at least 2 signals from the table and explicitly reference price vs target_price.
- If you cannot infer a target_price, set it to null and return HOLD, explaining the missing data.
- Output only JSON, no extra text.
""".format(
        ticker=ticker,
        decision_date=decision_date,
        indicator_json=json.dumps(indicator_json, indent=2),
        sanity_block=json.dumps(sanity_issues, indent=2),
        window_table_md=window_table_md,
        previous_block=previous_block.strip(),
        keep_band=keep_band,
    ).strip()

    return prompt


def extract_json_object(text: str) -> dict:
    """Extract a JSON object from text, raising ValueError if none is found."""
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty model output")

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        raise ValueError("No JSON object found in model output")

    obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise ValueError("Extracted JSON is not an object")
    return obj


def _validate_manager_json(obj: dict) -> dict:
    """Validate and normalise the manager JSON into the expected schema."""
    rec = obj.get("recommendation")
    if isinstance(rec, str):
        rec2 = rec.strip().upper()
    else:
        rec2 = "HOLD"

    if rec2 == "KEEP":
        rec2 = "HOLD"
    if rec2 not in {"BUY", "HOLD", "SELL"}:
        rec2 = "HOLD"

    tp = obj.get("target_price", None)
    if tp is None:
        tp2 = None
    else:
        try:
            tp2 = float(tp)
        except Exception:
            tp2 = None

    just = obj.get("justification", "")
    if not isinstance(just, str) or not just.strip():
        just = "No justification provided."

    return {"recommendation": rec2, "target_price": tp2, "justification": just.strip()}


def _manager_json_schema() -> Dict[str, Any]:
    """Return JSON schema used to force strict manager output."""
    return {
        "name": "manager_decision",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "recommendation": {"type": "string", "enum": ["BUY", "HOLD", "SELL"]},
                "target_price": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "justification": {"type": "string"},
            },
            "required": ["recommendation", "target_price", "justification"],
        },
        "strict": True,
    }

def _manager_text_format_json_schema() -> Dict[str, Any]:
    """Return the Responses API JSON schema format block for the manager output."""
    return {
        "type": "json_schema",
        "name": "manager_decision",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "recommendation": {"type": "string", "enum": ["BUY", "HOLD", "SELL"]},
                "target_price": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "justification": {"type": "string"},
            },
            "required": ["recommendation", "target_price", "justification"],
        },
        "strict": True,
    }

def _make_model_settings(
    model: str,
    reasoning_effort: Optional[str],
) -> ModelSettings:
    """Mirror reference setup: tool_choice required unless reasoning mode is set."""
    model_settings = ModelSettings(tool_choice="required")
    if reasoning_effort and _supports_reasoning(model):
        model_settings = ModelSettings(
            reasoning=Reasoning(effort=reasoning_effort),
            verbosity="medium",
        )
    return model_settings


def make_workflow_agents(cfg: RunConfig) -> Tuple[Agent, Agent]:
    """Create indicator and manager agents following the reference code style."""
    tools = [code_interpreter] if cfg.use_code_interpreter else []
    indicator_agent = Agent(
        name="workflow_indicator_analyst",
        model=cfg.model,
        instructions="Compute financial indicators and return strict JSON only.",
        tools=tools,
        mcp_servers=[],
        model_settings=_make_model_settings(cfg.model, cfg.indicator_reasoning_effort),
    )
    manager_agent = Agent(
        name="workflow_financial_manager",
        model=cfg.model,
        instructions="Make final BUY/HOLD/SELL decision and return strict JSON only.",
        tools=tools,
        mcp_servers=[],
        model_settings=_make_model_settings(cfg.model, cfg.manager_reasoning_effort),
    )
    return indicator_agent, manager_agent


def _result_to_json_obj(x: Any) -> Dict[str, Any]:
    """Convert agent output to dict when possible, else parse JSON from text."""
    if isinstance(x, dict):
        return x
    if hasattr(x, "model_dump"):
        dumped = x.model_dump()
        if isinstance(dumped, dict):
            return dumped
    s = x if isinstance(x, str) else str(x)
    return extract_json_object(s)


async def call_manager_llm(agent: Agent, cfg: RunConfig, prompt: str) -> dict:
    """Call manager agent with Runner."""
    result = await Runner.run(agent, input=prompt, max_turns=15)
    out = _result_to_json_obj(result.final_output)
    return _validate_manager_json(out)


async def call_indicator_llm(
    agent: Agent,
    cfg: RunConfig,
    prompt: str,
    fallback_indicators: Dict[str, Optional[float]],
) -> Dict[str, Any]:
    """Call indicator agent with Runner and merge deterministic fallback when enabled."""
    result = await Runner.run(agent, input=prompt, max_turns=15)
    parsed = _result_to_json_obj(result.final_output)
    out = _validate_indicator_json(parsed)
    if not cfg.strict_paper:
        for k, v in fallback_indicators.items():
            if k in out and out[k] is None:
                out[k] = v
    return out


async def call_indicator_llm_with_retries(
    agent: Agent,
    cfg: RunConfig,
    prompt: str,
    fallback_indicators: Dict[str, Optional[float]],
    attempts: int = 3,
) -> Dict[str, Any]:
    """Call indicator step with retries, then fall back to deterministic values."""
    last_err: Optional[Exception] = None
    for k in range(attempts):
        try:
            return await call_indicator_llm(agent, cfg, prompt, fallback_indicators)
        except Exception as e:
            last_err = e
            print(
                "[workflow] Indicator call failed (attempt {0}/{1}): {2}".format(k + 1, attempts, str(e)),
                flush=True,
            )
            await asyncio.sleep(0.5 * (k + 1))

    if cfg.strict_paper:
        raise RuntimeError("Indicator call failed after retries: {0}".format(last_err))
    out = dict(fallback_indicators)
    out["notes"] = "Indicator model call failed after retries: {0}".format(last_err)
    return _validate_indicator_json(out)




async def call_manager_llm_with_retries(agent: Agent, cfg: RunConfig, prompt: str, attempts: int = 3) -> dict:
    """Call the manager with retries and better diagnostics for empty outputs."""
    last_err: Optional[Exception] = None
    for k in range(attempts):
        try:
            return await call_manager_llm(agent, cfg, prompt)
        except Exception as e:
            last_err = e
            msg = str(e)
            print("[workflow] Manager call failed (attempt {0}/{1}): {2}".format(k + 1, attempts, msg), flush=True)
            await asyncio.sleep(0.5 * (k + 1))

    raise RuntimeError("Manager failed after retries: {0}".format(last_err))


async def run_one_ticker(
    indicator_agent: Agent,
    manager_agent: Agent,
    df_monthly: pd.DataFrame,
    gold_df: Optional[pd.DataFrame],
    ticker: str,
    cfg: RunConfig,
) -> dict:
    """Run month-by-month workflow decisions for a single ticker."""
    df_t = df_monthly[df_monthly["ticker"] == ticker].sort_values("date").reset_index(drop=True)
    df_t = filter_date_range(df_t, cfg.test_start, cfg.test_end).reset_index(drop=True)

    if df_t.empty:
        return {"ticker": ticker, "outputs": [], "error": "No monthly rows in the chosen date range."}

    outputs: List[dict] = []
    prev_decision: Optional[dict] = None  # carry last recommendation into the next prompt

    n_steps = min(len(df_t), int(cfg.max_months))
    for i in range(n_steps):
        window = make_window_table(df_t, i, int(cfg.lookback_months))
        window_md = df_to_markdown_table(window)
        window_rows = window.to_dict(orient="records")
        deterministic = compute_deterministic_indicators(window_rows)

        decision_dt = pd.to_datetime(df_t.iloc[i]["date"], errors="coerce").strftime("%Y-%m-%d")
        indicator_prompt = build_indicator_prompt(ticker, decision_dt, window_md)
        indicator_json = await call_indicator_llm_with_retries(
            indicator_agent,
            cfg,
            indicator_prompt,
            deterministic,
            attempts=3,
        )
        sanity_issues = [] if cfg.strict_paper else detect_indicator_sanity_issues(indicator_json)
        prompt = build_manager_prompt(ticker, decision_dt, window_md, indicator_json, sanity_issues, prev_decision)

        if i == 0:
            print(window_md, flush=True)

        manager_json = await call_manager_llm_with_retries(manager_agent, cfg, prompt, attempts=3)
        spot_check = None
        if (not cfg.strict_paper) and cfg.spot_check_month:
            dt = pd.to_datetime(decision_dt, errors="coerce")
            month = str(dt.to_period("M")) if not pd.isna(dt) else ""
            if month == str(cfg.spot_check_month).strip():
                gold_snapshot = get_gold_indicator_snapshot(gold_df, ticker, decision_dt)
                spot_check = build_spot_check_report(
                    predicted=indicator_json,
                    gold=gold_snapshot,
                    tolerance=float(cfg.spot_check_tolerance),
                )

        prev_ctx = {
            "previous_recommendation": (prev_decision or {}).get("recommendation", "N/A"),
            "previous_target_price": (prev_decision or {}).get("target_price", "N/A"),
            "previous_justification": (prev_decision or {}).get("justification", "N/A"),
        }

        outputs.append(
            {
                "date": decision_dt,
                "ticker": ticker,
                "indicator_analysis": {
                    "values": indicator_json,
                    "sanity_issues": sanity_issues,
                },
                "manager": manager_json,
                "previous_decision": prev_ctx,
                "window_rows": int(len(window)),
                "spot_check": spot_check,
                "meta": {
                    "model": cfg.model,
                    "indicator_reasoning_effort": cfg.indicator_reasoning_effort,
                    "manager_reasoning_effort": cfg.manager_reasoning_effort,
                    "use_code_interpreter": bool(cfg.use_code_interpreter),
                    "max_output_tokens": int(cfg.max_output_tokens),
                    "indicator_max_output_tokens": int(cfg.indicator_max_output_tokens),
                    "test_start": cfg.test_start,
                    "test_end": cfg.test_end,
                    "lookback_months": int(cfg.lookback_months),
                },
            }
        )

        prev_decision = manager_json

    return {"ticker": ticker, "outputs": outputs}


async def main_async(cfg: RunConfig) -> None:
    """Async entrypoint that runs the workflow runner and writes output files."""
    df = load_monthly_panel(cfg.monthly_panel_csv)
    gold_df: Optional[pd.DataFrame] = None
    if cfg.gold_csv is not None and cfg.gold_csv.exists():
        try:
            gold_df = pd.read_csv(cfg.gold_csv, low_memory=False)
        except Exception:
            gold_df = None
    tickers = load_tickers(cfg.tickers_csv)

    indicator_agent, manager_agent = make_workflow_agents(cfg)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    for t in tickers:
        result = await run_one_ticker(indicator_agent, manager_agent, df, gold_df, t, cfg)
        out_path = cfg.out_dir / "{0}_workflow_output_{1}.json".format(t, stamp)
        out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print("Saved: {0}".format(out_path), flush=True)


def _load_default_date_range() -> Tuple[str, str]:
    """Load default date range from config/experiment.json, else use safe defaults."""
    start = "2025-01-01"
    end = "2025-12-31"

    exp_path = Path("config") / "experiment.json"
    if exp_path.exists():
        try:
            data = json.loads(exp_path.read_text(encoding="utf-8"))
            start = str(data.get("test_start", start))
            end = str(data.get("test_end", end))
        except Exception:
            pass

    return start, end


def parse_args() -> RunConfig:
    """Parse CLI args and environment overrides into a RunConfig."""
    default_start, default_end = _load_default_date_range()

    p = argparse.ArgumentParser()
    p.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers, e.g. TSLA,MSFT,AAPL")
    p.add_argument("--max_months", type=int, default=12, help="Number of monthly decision points to keep")
    p.add_argument("--lookback_months", type=int, default=12, help="Rolling window length in months")
    p.add_argument(
        "--monthly_panel",
        type=str,
        default="data/processed/panel/monthly_panel_prices_returns_fundamentals.csv",
        help="Path to monthly panel CSV",
    )
    p.add_argument(
        "--out_dir",
        type=str,
        default="results/experiments/monthly_workflow_workflow",
        help="Output directory",
    )

    default_model = (os.environ.get("FIN_AGENT_MODEL") or "").strip() or "gpt-5-mini"
    p.add_argument("--model", type=str, default=default_model, help="Model name")
    p.add_argument(
        "--indicator_reasoning_effort",
        type=str,
        default=(os.environ.get("FIN_AGENT_INDICATOR_REASONING_EFFORT", "medium") or "medium"),
        help="Reasoning effort for indicator computation stage (default: medium).",
    )
    p.add_argument(
        "--manager_reasoning_effort",
        type=str,
        default=(os.environ.get("FIN_AGENT_MANAGER_REASONING_EFFORT", "high") or "high"),
        help="Reasoning effort for final manager decision stage (default: high).",
    )
    p.add_argument("--test_start", type=str, default=default_start, help="Test start date (YYYY-MM-DD)")
    p.add_argument("--test_end", type=str, default=default_end, help="Test end date (YYYY-MM-DD)")
    p.add_argument(
        "--max_output_tokens",
        type=int,
        default=int(os.environ.get("FIN_AGENT_MAX_OUTPUT_TOKENS", "700")),
        help="Max tokens for the manager response",
    )
    p.add_argument(
        "--indicator_max_output_tokens",
        type=int,
        default=int(os.environ.get("FIN_AGENT_INDICATOR_MAX_OUTPUT_TOKENS", "500")),
        help="Max tokens for the indicator response",
    )
    p.add_argument(
        "--use_code_interpreter",
        action="store_true",
        help="Enable Python code interpreter function tool for indicator and manager agents.",
    )
    p.add_argument(
        "--no_code_interpreter",
        dest="use_code_interpreter",
        action="store_false",
        help="Disable Python code interpreter tool.",
    )
    p.set_defaults(use_code_interpreter=True)
    p.add_argument(
        "--gold_csv",
        type=str,
        default="data/processed/panel/monthly_panel_prices_returns_fundamentals.csv",
        help="Gold panel CSV path used for spot-check comparisons.",
    )
    p.add_argument(
        "--spot_check_month",
        type=str,
        default="",
        help="YYYY-MM month for spot-check diagnostics (set empty string to disable).",
    )
    p.add_argument(
        "--spot_check_tolerance",
        type=float,
        default=0.25,
        help="Relative error tolerance for spot-check comparisons.",
    )
    p.add_argument(
        "--strict_paper",
        dest="strict_paper",
        action="store_true",
        help="Enable strict paper reproduction behavior.",
    )
    p.add_argument(
        "--no_strict_paper",
        dest="strict_paper",
        action="store_false",
        help="Disable strict paper reproduction behavior.",
    )
    p.set_defaults(strict_paper=True)

    a = p.parse_args()

    indicator_reff = (a.indicator_reasoning_effort or "").strip().lower()
    manager_reff = (a.manager_reasoning_effort or "").strip().lower()
    if indicator_reff not in {"low", "medium", "high"}:
        raise ValueError("--indicator_reasoning_effort must be one of: low, medium, high")
    if manager_reff not in {"low", "medium", "high"}:
        raise ValueError("--manager_reasoning_effort must be one of: low, medium, high")
    if not _supports_reasoning(str(a.model).strip()):
        raise ValueError(
            "Choose a reasoning-capable model (o3 or gpt-5*) for split reasoning configuration."
        )

    return RunConfig(
        monthly_panel_csv=Path(a.monthly_panel),
        out_dir=Path(a.out_dir),
        model=str(a.model).strip(),
        indicator_reasoning_effort=indicator_reff,
        manager_reasoning_effort=manager_reff,
        lookback_months=int(a.lookback_months),
        max_months=int(a.max_months),
        tickers_csv=a.tickers,
        test_start=str(a.test_start).strip(),
        test_end=str(a.test_end).strip(),
        max_output_tokens=int(a.max_output_tokens),
        indicator_max_output_tokens=int(a.indicator_max_output_tokens),
        use_code_interpreter=bool(a.use_code_interpreter),
        gold_csv=Path(str(a.gold_csv).strip()) if str(a.gold_csv).strip() else None,
        spot_check_month=(str(a.spot_check_month).strip() or None),
        spot_check_tolerance=float(a.spot_check_tolerance),
        strict_paper=bool(a.strict_paper),
    )


def main() -> None:
    """CLI entrypoint."""
    cfg = parse_args()
    asyncio.run(main_async(cfg))


if __name__ == "__main__":
    main()

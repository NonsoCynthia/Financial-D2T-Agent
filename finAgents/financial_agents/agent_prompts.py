"""Prompts aligned with Thiago's fundamental analysis agent paper."""

from finAgents.financial_agents.financial_analyst import expected_indicator_keys


FINANCIAL_ANALYST_INSTRUCTIONS = (
    "You are a Fundamental Analyst for US stocks. "
    "Your job is to compute fundamental indicators carefully from available data only. "
    "You must use the provided tools to fetch a 12-row monthly window and any SEC companyfacts needed. "
    "If an indicator cannot be computed from available data, set it to null. "
    "Do not invent values, do not guess missing data. "
    "Return valid JSON only."
)


FINANCIAL_MANAGER_INSTRUCTIONS = (
    "You are a Financial Manager for long-term investment decisions. "
    "You receive a 12-month monthly time series, the computed fundamental indicators, and the previous decision context. "
    "Decide whether to BUY, HOLD, or SELL a single stock. "
    "You may use the Python tool for calculations. "
    "Do not invent data. "
    "Return valid JSON only."
)


ANALYST_MONTHLY_TASK_PROMPT = """Ticker: {ticker}
As of date: {as_of_date}

1) Use get_monthly_window(ticker, as_of_date, months=12) to fetch the last 12 monthly rows (first trading day of each month).
2) If needed, use get_companyfacts(ticker, concepts=[...]) to fetch missing accounting fields.
3) Compute the 32 fundamental indicators listed below. Use only information available on or before as_of_date.

Indicators (must all exist as keys under indicators):
{indicator_keys}

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "ticker": "{ticker}",
  "as_of_date": "{as_of_date}",
  "window_months": 12,
  "indicators": {{
    "Assets": 0.0,
    "CurrentAssets": 0.0,
    "CashAndEquivalents": 0.0,
    "GrossDebt": 0.0,
    "NetDebt": 0.0,
    "ShareholdersEquity": 0.0,
    "NetRevenue_TTM": 0.0,
    "NetRevenue_Q": 0.0,
    "EBIT_TTM": 0.0,
    "EBIT_Q": 0.0,
    "NetProfit_TTM": 0.0,
    "NetProfit_Q": 0.0,
    "P_E": 0.0,
    "P_B": 0.0,
    "P_EBIT": 0.0,
    "PriceToSales": 0.0,
    "PriceToAssets": 0.0,
    "PriceToWorkingCapital": 0.0,
    "PriceToNetCurrentAssets": 0.0,
    "EV_EBIT": 0.0,
    "EV_EBITDA": 0.0,
    "EPS": 0.0,
    "BVPS": 0.0,
    "GrossMargin": 0.0,
    "EBITMargin": 0.0,
    "NetMargin": 0.0,
    "EBIT_Assets": 0.0,
    "ROE": 0.0,
    "ROIC": 0.0,
    "CurrentRatio": 0.0,
    "GrossDebt_Equity": 0.0,
    "AssetTurnover": 0.0
  }},
  "notes": "brief explanation of any mapping assumptions or missing fields",
  "sources": ["get_monthly_window", "get_companyfacts"]
}}

Rules:
- Every key must exist in indicators.
- Each value must be a number or null.
- Prefer computing from the monthly window rows. Only call get_companyfacts when a required accounting field is missing.
"""


MANAGER_MONTHLY_TASK_PROMPT = """Ticker: {ticker}
Decision date: {date}

Monthly window (last 12 months including decision month):
{monthly_window_json}

Fundamental analyst indicators (computed using only data available up to Decision date):
{analyst_report_json}

Previous decision context (if any):
{previous_decision_json}

Decide whether to BUY, HOLD, or SELL.

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "ticker": "{ticker}",
  "date": "{date}",
  "recommendation": "HOLD",
  "target_price": 0.0,
  "justification": "short justification grounded in the monthly window and indicators"
}}

Rules:
- Use a strict ±5% band around target_price:
  - BUY if current price < target_price * 0.95
  - SELL if current price > target_price * 1.05
  - HOLD only if current price is within 5% of target_price
- recommendation must be one of BUY, HOLD, SELL
- target_price must be a number or null; use null only if you truly cannot infer a fair value
- justification must mention price vs target_price and at least one indicator
- never output N/A for recommendation, target_price, or justification
- if insufficient information, output HOLD with target_price null and explain why
- do not invent data
"""


JSON_REPAIR_PROMPT = """The following output was supposed to be valid JSON, but it is not. Convert it into valid JSON only. No markdown. No extra text.

Expected JSON schema:
{expected_schema}

Bad output:
{bad_output}

Rules:
- Output exactly one JSON object.
- Use standard JSON only (double quotes, no trailing commas).
- Use null for missing values (never None, NaN, Infinity, -Infinity).
"""


def analyst_prompt(ticker: str, as_of_date: str) -> str:
    """Render the analyst prompt with the full indicator list."""

    keys = "\n".join(expected_indicator_keys())
    return ANALYST_MONTHLY_TASK_PROMPT.format(ticker=ticker, as_of_date=as_of_date, indicator_keys=keys)

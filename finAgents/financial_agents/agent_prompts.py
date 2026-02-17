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

Indicators (all must be present as entries in indicators):
{indicator_keys}

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "indicators": [
    {{"indicator": "Assets", "value": 0.0}},
    {{"indicator": "CurrentAssets", "value": 0.0}},
    {{"indicator": "CashAndEquivalents", "value": 0.0}},
    {{"indicator": "GrossDebt", "value": 0.0}},
    {{"indicator": "NetDebt", "value": 0.0}},
    {{"indicator": "ShareholdersEquity", "value": 0.0}},
    {{"indicator": "NetRevenue_TTM", "value": 0.0}},
    {{"indicator": "NetRevenue_Q", "value": 0.0}},
    {{"indicator": "EBIT_TTM", "value": 0.0}},
    {{"indicator": "EBIT_Q", "value": 0.0}},
    {{"indicator": "NetProfit_TTM", "value": 0.0}},
    {{"indicator": "NetProfit_Q", "value": 0.0}},
    {{"indicator": "P_E", "value": 0.0}},
    {{"indicator": "P_B", "value": 0.0}},
    {{"indicator": "P_EBIT", "value": 0.0}},
    {{"indicator": "PriceToSales", "value": 0.0}},
    {{"indicator": "PriceToAssets", "value": 0.0}},
    {{"indicator": "PriceToWorkingCapital", "value": 0.0}},
    {{"indicator": "PriceToNetCurrentAssets", "value": 0.0}},
    {{"indicator": "EV_EBIT", "value": 0.0}},
    {{"indicator": "EV_EBITDA", "value": 0.0}},
    {{"indicator": "EPS", "value": 0.0}},
    {{"indicator": "BVPS", "value": 0.0}},
    {{"indicator": "GrossMargin", "value": 0.0}},
    {{"indicator": "EBITMargin", "value": 0.0}},
    {{"indicator": "NetMargin", "value": 0.0}},
    {{"indicator": "EBIT_Assets", "value": 0.0}},
    {{"indicator": "ROE", "value": 0.0}},
    {{"indicator": "ROIC", "value": 0.0}},
    {{"indicator": "CurrentRatio", "value": 0.0}},
    {{"indicator": "GrossDebt_Equity", "value": 0.0}},
    {{"indicator": "AssetTurnover", "value": 0.0}}
  ]
}}

Rules:
- indicators must be a list of objects with keys: indicator, value.
- Every required indicator name must appear exactly once.
- Each value must be a number or null.
- Prefer computing from the monthly window rows. Only call get_companyfacts when a required accounting field is missing.
"""


MANAGER_MONTHLY_TASK_PROMPT = """Ticker: {ticker}
Decision date: {date}

Monthly manager panel (last 12 rows, one row per month, including current month):
{manager_panel_table}

Decide whether to BUY, HOLD, or SELL.

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "recommendation": "HOLD",
  "target_price": 0.0,
  "justification": "short justification grounded in the monthly panel and indicators"
}}

Rules:
- recommendation must be one of BUY, HOLD, SELL
- target_price must be a number
- justification must mention price vs target_price and at least one indicator
- never output N/A for recommendation, target_price, or justification
- if information is weak, output HOLD with a conservative numeric target_price and explain why
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

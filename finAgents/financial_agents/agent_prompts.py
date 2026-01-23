FINANCIAL_ANALYST_INSTRUCTIONS = """You are a financial analyst for US stocks. Use tools to fetch prices, returns, SEC companyfacts, and the merged panel. Compute indicators carefully and do not invent values. If a metric cannot be computed from available data, set its value to null. Prefer panel data when it contains the required fields, otherwise use prices and companyfacts."""

FINANCIAL_MANAGER_INSTRUCTIONS = """You are a financial manager. You will receive computed indicators for a stock and the current portfolio state. Produce a trading action BUY, SELL, or HOLD and a target_position of 0 or 1. Do not invent indicators. Base your decision only on the provided indicator values and the provided portfolio state."""

ANALYST_TASK_PROMPT = """Ticker: {ticker}
As of date: {as_of_date}

Compute the indicators listed below using tools as needed:
last_price
total_return
volatility
Revenues
NetIncomeLoss
Assets
Liabilities
roe
profit_margin

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "ticker": "{ticker}",
  "as_of_date": "{as_of_date}",
  "indicators": {{
    "last_price": 123.45,
    "total_return": 0.12,
    "volatility": 0.34,
    "Revenues": 1000000,
    "NetIncomeLoss": 1000,
    "Assets": 5000,
    "Liabilities": 2000,
    "roe": 0.10,
    "profit_margin": 0.05
  }},
  "methodology": {{
    "notes": "Brief description of what periods were used and how values were computed",
    "sources": ["get_panel", "get_returns", "get_companyfacts", "get_prices"]
  }}
}}

Rules:
- Every key must exist in "indicators"
- Each value must be a number or null
- Use only information available on or before as_of_date
"""

REFLECTION_TASK_PROMPT = """Ticker: {ticker}
As of date: {as_of_date}

You previously returned JSON but some indicator values were null. Recompute ONLY the missing indicators listed below. Use tools as needed.

Missing indicators:
{missing_indicators}

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "ticker": "{ticker}",
  "as_of_date": "{as_of_date}",
  "indicators": {{
    "some_missing_indicator": 123.45
  }}
}}

Rules:
- Only include keys listed in Missing indicators
- Each value must be a number or null
- Use only information available on or before as_of_date
"""

MANAGER_TASK_PROMPT = """Ticker: {ticker}
Decision date: {date}

Trading constraints:
- allow_short: {allow_short}
- position_sizing: {position_sizing}
- valid actions: BUY, SELL, HOLD
- target_position must be 0 or 1 (long-only)

Execution assumptions:
- trade_price_field: {trade_price_field}
- transaction_cost_bps: {transaction_cost_bps}

Current portfolio state BEFORE today's action:
- cash: {cash}
- shares_held: {shares_held}
- price_today: {price_today}
- portfolio_value: {portfolio_value}

Analyst JSON (signals computed using only data available up to Decision date):
{analyst_report_json}

Decide what to do today.

Output JSON only. No markdown. No extra text. The JSON must follow this schema:
{{
  "ticker": "{ticker}",
  "date": "{date}",
  "action": "HOLD",
  "target_position": 0,
  "justification": "short justification based only on the analyst indicators and portfolio state"
}}

Rules:
- action must be one of BUY, SELL, HOLD
- target_position must be 0 or 1
- do not invent indicators
- do not use any information not present in the prompt
"""

JSON_REPAIR_PROMPT = """The following output was supposed to be valid JSON, but it is not. Convert it into valid JSON only. No markdown. No extra text.

Expected JSON schema:
{expected_schema}

Bad output:
{bad_output}
"""

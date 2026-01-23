## Finacial Agents

This folder contains the US stock multi agent trading setup. It includes:

1) an MCP server that exposes your dataset as tools  
2) agent prompt templates and agent definitions  
3) a trading runner that simulates daily decisions and stores results  

The goal is to reproduce a FINCON style pipeline without news or multimodal inputs. The agents only see structured signals computed from your stored price and SEC fundamentals data.

---

## Folder structure

Typical layout:

- `finAgents/server_us_finance.py`  
  FastMCP server that exposes SQLite backed tools (prices, returns, fundamentals, panel). It is launched by the runner through `MCPServerStdio`, so the agents can call tools.

- `finAgents/agent_tools/`  
  Pure Python helper layer. This is where all SQL queries and database access live. The MCP server imports and wraps these functions.

- `finAgents/financial_agents/`  
  Agent level code, schemas, and prompts.  
  `agent_prompts.py` contains all instruction strings and task prompts.  
  `financial_analyst.py` contains indicator definitions and validation helpers.  
  `financial_manager.py` contains the decision schema.

- `results/`  
  Run outputs are saved here by the trading runner. Each run writes a JSON file containing configuration and the full daily trajectory.

- `logs/`  
  MCP server logs can be written here if logging is enabled in the server.

---

## What the MCP server provides

The MCP server is your tool layer. Instead of reading CSVs inside the agents, the agents request data through tools. Each tool returns JSON.

Core tools:

- `list_tickers`  
  Lists tickers available in the database.

- `get_prices`  
  Returns OHLCV price rows for a ticker and date range.

- `get_returns`  
  Returns daily returns rows for a ticker and date range.

- `get_companyfacts`  
  Returns SEC companyfacts rows for a ticker. Supports optional concept filtering and date filtering on the reporting end date.

- `get_panel`  
  Returns the merged daily panel for a ticker and date range. This panel combines prices, returns, and selected fundamentals aligned by filing date.

- `get_price_series` (recommended)  
  Returns only `date` and `price` for iterating the trading loop. This keeps the payload small compared to full OHLCV.

Data sources the tools read from:

- `data/raw/prices_us.db` (tables like `US_PRICES`, `US_RETURNS`)  
- `data/raw/sec/sec_companyfacts.db` (table `SEC_COMPANYFACTS`)  
- `data/processed/panel/panel.db` (table `US_DAILY_PANEL`, plus the fundamentals wide table)

---

## The agents and their roles

### Financial Analyst agent

Purpose:

- Computes a fixed set of indicators for a ticker as of a given decision date.
- Uses MCP tools to fetch only data available up to that date.

Inputs:

- `ticker`
- `as_of_date` (the trading day you are deciding on)

Outputs:

- JSON report containing:
  - `ticker`
  - `as_of_date`
  - `indicators` (all keys exist, values are numbers or null)
  - `methodology` (short notes and tool names used)

Typical indicators:

- price based: last_price, total_return, volatility
- fundamentals: Revenues, NetIncomeLoss, Assets, Liabilities
- derived: roe, profit_margin

Rules:

- Do not invent values.
- If an indicator cannot be computed from available data, set it to null.

### Financial Manager agent

Purpose:

- Converts the analyst report into a trading decision under constraints.

Inputs:

- portfolio state (cash, shares held, portfolio value)
- execution assumptions (price field, transaction cost)
- analyst JSON report

Outputs:

- JSON decision containing:
  - action: BUY, SELL, HOLD
  - target_position: 0 or 1 (long only)
  - short justification

Rules:

- Do not invent indicators.
- Justify only from the analyst report and portfolio state.

---

## The trading runner

The runner simulates a daily loop across the test period.

For each trading day:

1) Fetch the day’s price used for execution (usually Adj Close) from the MCP server  
2) Ask the analyst for indicators as of that date  
3) If some indicator values are null, optionally run a reflection prompt to fill only the missing ones  
4) Ask the manager for an action and target_position  
5) Execute the trade in the simulator (one_share or all_in)  
6) Record a trajectory row  

At the end:

- Save a JSON run file to `results/runs/<TICKER>/...json`

Each trajectory row includes:

- date, price
- action, shares traded, shares held
- cash, portfolio value, daily portfolio return
- full analyst JSON and manager JSON for auditability

---

## How to run a quick test

1) Ensure your dataset pipeline has already produced the SQLite artefacts.
2) From repo root, run the trading loop:

```bash
python run_trading.py
```

You should see a saved run file under `results/runs/<TICKER>/`.

---

## Why MCP is used here

MCP lets you treat your dataset as a tool API.

- agents do not need direct file access or pandas logic
- you can swap databases, add tools, or change schemas without rewriting agent code
- runs are easier to reproduce because every data retrieval is explicit via tool calls

---

## Next extensions

Common next steps:

- add a `run_eval.py` that computes portfolio metrics (cumulative return, Sharpe, max drawdown)
- store per day outputs also as compact CSV for faster analysis
- add more tools for smaller payloads or specific signals (for example rolling volatility tool)
- move from single ticker to multi ticker portfolio allocation logic




<!-- finAgents/
  __init__.py

  server_us_finance.py
  run_multi_agent.py

  financial_agents/
    __init__.py
    financial_analyst.py
    financial_manager.py

  agent_tools/

What each file does:
`server_us_finance`.py: MCP server. Exposes tools to query your SQLite artefacts: prices, returns, companyfacts, panel.
`financial_agents/financial_analyst.py`: Analyst agent builder. Instructions and output schema for indicators.
`financial_agents/financial_manager.py`: Manager agent builder. Instructions and output schema for buy or sell decision.
`run_multi_agent.py`: Starts the MCP server over stdio, runs analyst first, then manager, prints outputs. -->

<!-- mkdir -p agents/financial_agents
touch agents/__init__.py agents/financial_agents/__init__.py -->
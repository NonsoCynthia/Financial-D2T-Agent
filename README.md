# Financial-D2T-Agent

**Financial-D2T-Agent** is a research-oriented pipeline and agent system for predicting **BUY**, **HOLD**, or **SELL** actions on selected stocks using fundamentals, price dynamics, and risk-adjusted features. It combines a reproducible data pipeline with a multi-tool agent powered by the **MCP** (Multi-tool Command Protocol) framework.

---

## 🔧 Overview

This project builds a structured dataset of stock prices and financial fundamentals, computes engineered features (e.g. returns, volatility), and runs predictions through a lightweight agent system that uses tools to call predictive logic. The model currently uses a rule-based heuristic (momentum/volatility ratio) but is extensible to other strategies.

---

## 📁 Project Structure

```
.
├─ data/
│  ├─ raw/                  # Raw SEC filings and price data
│  │  ├─ prices/            # Downloaded stock prices (yfinance)
│  │  └─ sec/               # SEC mappings and company facts
│  └─ processed/
│     └─ panel/             # Final panel with prices, returns, fundamentals
├─ scripts/
│  ├─ 01_download_prices.py         # Fetch historical prices
│  ├─ 02_sec_ticker_cik.py          # Map tickers to SEC CIKs
│  ├─ 03a_sec_companyfacts.py       # Extract fundamentals
│  ├─ 04a_compute_returns.py        # Calculate returns, volatility
│  └─ 04b_align_fundamentals.py     # Join features into a panel
├─ config.py               # Tickers, date ranges, SEC headers
├─ run_scripts.py          # Orchestrates pipeline by step
├─ mcp_trading_server.py   # MCP tool server (feature extractor, predictor)
├─ agent_trade.py          # Agent that queries MCP server
└─ requirements.txt
```

---

## 🔄 Data Pipeline

The pipeline is fully scripted and modular, with `run_scripts.py` coordinating execution. It performs:

1. **Price download**: Yahoo Finance data from `2022-01-03` to `2025-12-31`
2. **SEC mapping**: Maps ticker to CIK using the official SEC JSON map
3. **Fundamentals**: Fetches and processes company facts (e.g. Assets, Revenues)
4. **Returns**: Computes rolling returns and volatility
5. **Panel join**: Aligns everything into a clean panel CSV with one row per (ticker, date)

All scripts write to `data/raw/` and `data/processed/`. You can rerun any step safely.

Configuration (tickers, date ranges) is set in `config.py`:

```python
TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]
START_DATE = "2022-01-03"
END_DATE_INCLUSIVE = "2025-12-31"
```

---

## 🧠 Agent Prediction

After building the dataset, predictions are made using an MCP agent:

- **`mcp_trading_server.py`** exposes tools like:

  - `predict_action`: returns BUY/SELL/HOLD for a given stock on a date
  - `backtest_heuristic`: runs a basic backtest using a signal strategy
  - `get_features`, `list_tickers`, `available_date_range` for utility
- **`agent_trade.py`** controls the agent:

  - `--mode sample`: predict one example
  - `--mode all`: run predictions across multiple stocks and dates
  - CLI arguments support percentile or fixed threshold modes

---

## ▶️ How to Run

1. **Install dependencies**

```bash
pip install -r requirements.txt
```

2. **Build the dataset**

```bash
python run_scripts.py --step all
```

3. **Run a single prediction**

```bash
python agent_trade.py --mode sample --ticker TSLA --date 2025-06-03
```

4. **Run batch predictions**

```bash
python agent_trade.py --mode all --limit 100
```

5. **Optional threshold override**

```bash
python agent_trade.py --mode sample --ticker TSLA --date 2025-06-03 --method percentile --q_buy 0.85 --q_sell 0.15
```

---

## 📌 Notes

- MCP agents interact with tools defined in the server, using reasoning and tool calls to arrive at actions.
- Thresholds can be defined as fixed numbers or inferred using quantiles from the training data.
- SEC access is rate-limited. You should set a valid `SEC_USER_AGENT` string in `config.py`.
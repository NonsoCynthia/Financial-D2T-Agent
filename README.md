# Financial-D2T-Agent

**Financial-D2T-Agent** is a research-oriented system for generating BUY, HOLD, or SELL trading signals on selected stocks. It builds a clean dataset of price and fundamental features from 2022 to 2025, and uses an MCP agent to call decision-making tools.

---

## 🔧 Overview

This project constructs a dataset from scratch and exposes a tool-based interface for agent-driven prediction. It includes:

- Automated pipeline scripts
- MCP tool server with prediction logic
- Agent interface for querying tools
- Configurable thresholds and splits

---

## 📁 Core Directories

```
.
├── data/
│   ├── raw/
│   │   ├── prices/              # Raw daily price CSVs from yfinance
│   │   └── sec/                 # SEC company facts, ticker maps
│   └── processed/
│       ├── prices/             # Daily returns
│       ├── panel/              # Final daily panel (prices + fundamentals)
│       └── splits/             # Train/test splits for modelling
├── scripts/                    # Step-by-step data processing scripts
├── logs/                       # Server logs (e.g. MCP activity)
```

---

## 🧱 Pipeline Scripts

Executed via `run_scripts.py --step {step_name}` or `--step all`:

- `01_download_prices.py`: Downloads historical price data for selected tickers
- `02_sec_ticker_cik.py`: Maps stock tickers to SEC CIKs
- `03a_sec_companyfacts.py`: Retrieves and stores SEC fundamental facts
- `03b_sec_download_filings.py`: (Optional) Downloads full filings (not always needed)
- `04a_compute_returns.py`: Computes daily and rolling returns, volatility
- `04b_align_fundamentals.py`: Joins returns and fundamentals into panel format
- `05_make_splits.py`: Splits data into train/test periods

---

## 📊 Data Summary

- Tickers: `["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]`
- Date range: `2022-01-03` to `2025-12-31` inclusive
- Train split ends: `2024-12-31`
- Output files:
  - `data/processed/panel/daily_panel_prices_returns_fundamentals.csv`
  - `data/processed/splits/train_2022_2024.csv`
  - `data/processed/splits/test_2025.csv`

---

## 🤖 MCP Agent + Tools

- `mcp_trading_server.py`: Exposes prediction and feature tools using MCP protocol
  - Tools: `predict_action`, `get_features`, `list_tickers`, `get_percentile_thresholds`, etc.
- `agent_trade.py`: Agent interface for calling tools
  - Supports:
    - `--mode sample --ticker TSLA --date 2025-06-03`
    - `--mode all --limit 50`
    - Threshold method: `--method fixed` or `--method percentile`

---

## ▶️ Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python run_scripts.py --step all

# Run a single prediction
python agent_trade.py --mode sample --ticker TSLA --date 2025-06-03

# Run multiple predictions
python agent_trade.py --mode all --limit 100
```

Optional override:

```bash
python agent_trade.py --mode sample --ticker TSLA --date 2025-06-03 \
  --method percentile --q_buy 0.85 --q_sell 0.15
```

---

## 📝 Notes

- Logs from the MCP server are written to `logs/mcp_trading_server.log`
- SEC requests require a valid user agent string in `config.py`
- All scripts are designed to be re-runnable without overwriting valid output
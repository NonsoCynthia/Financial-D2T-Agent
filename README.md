# Financial-D2T-Agent

Thiago style monthly fundamental-analysis workflow for US equities.

## Quick start

1) Build the daily panel

- scripts/01_download_prices.py
- scripts/04a_compute_returns.py
- scripts/02_sec_ticker_cik.py
- scripts/03a_sec_companyfacts.py
- scripts/04b_align_fundamentals.py

2) Build the monthly panel used by the agents

- scripts/06_make_monthly_panel.py

3) Run the monthly multi-agent workflow

- python run_monthly_experiment.py

4) Evaluate indicator accuracy and Thiago style trading simulation

- python run_eval_monthly.py

## Monthly agent run and evaluation (current pipeline)

Run monthly agent outputs (one final file per ticker plus Thiago-style step artifacts):

```bash
python run_agent_monthly.py \
  --tickers "TSLA,AMZN,NIO,MSFT,AAPL,GOOG,NFLX,COIN" \
  --max_months 12 \
  --model gpt-4.1-mini \
  --test_start 2025-01-02 \
  --test_end 2025-12-31 \
  --server_path finAgents/server_us_finance.py \
  --out_dir results/experiments/monthly_agent_workflow
```

Expected output structure:

- `results/experiments/monthly_agent_workflow/<TICKER>_output_<timestamp>.json`
- `results/experiments/monthly_agent_workflow/<TICKER>/<YYYY-MM-DD>_analyst_0.json`
- `results/experiments/monthly_agent_workflow/<TICKER>/<YYYY-MM-DD>_manager_0.json`
- `results/experiments/monthly_agent_workflow/results_sample.json`
- `results/experiments/monthly_agent_workflow/decisions_sample.json`

Evaluate in agent mode:

```bash
# Thiago-style Stage 1 (default): min-max on gold + MAE
python run_eval_monthly.py \
  --mode agent \
  --pred_dir results/experiments/monthly_agent_workflow \
  --gold_csv data/processed/panel/monthly_panel_prices_returns_fundamentals.csv \
  --stage1 thiago

# Alternative Stage 1: NMAE + penalty
python run_eval_monthly.py \
  --mode agent \
  --pred_dir results/experiments/monthly_agent_workflow \
  --gold_csv data/processed/panel/monthly_panel_prices_returns_fundamentals.csv \
  --stage1 nmae
```

## Data sources in this repository (US pipeline)

This document lists the data sources you currently use, what each one provides, and where you store it.

## 1) Market price data (OHLCV)
Source: Yahoo Finance via yfinance (scripts/01_download_prices.py)
Provides: Daily Open, High, Low, Close, Adj Close, Volume for each ticker (TSLA, AMZN, NIO, MSFT, AAPL, GOOG, NFLX, COIN) across your configured date range.
Stored as:
- CSV per ticker under data/raw/prices/ (if enabled in your scripts)
- SQLite database data/raw/prices_us.db, table US_PRICES

## 2) Return series derived from prices
Source: Computed locally from Yahoo Finance prices (scripts/04a_compute_returns.py)
Provides: Daily return fields such as RET_1D and LOG_RET_1D (plus aligned adjusted close where applicable).
Stored as:
- CSV data/processed/prices/daily_returns.csv
- SQLite database data/raw/prices_us.db, table US_RETURNS

## 3) SEC ticker to CIK mapping
Source: SEC published mapping (scripts/02_sec_ticker_cik.py)
Provides: Ticker to CIK mapping used to query SEC endpoints.
Stored as:
- CSV data/raw/sec/sec_ticker_cik_all.csv
- CSV data/raw/sec/sec_ticker_cik_selected.csv

## 4) SEC CompanyFacts fundamentals (XBRL facts)
Source: SEC EDGAR CompanyFacts JSON endpoint (scripts/03a_sec_companyfacts.py)
Provides: XBRL facts and metadata, including concepts such as:
- Assets, Liabilities, StockholdersEquity
- Revenues, NetIncomeLoss, OperatingIncomeLoss
- EarningsPerShareBasic, CommonStockSharesOutstanding
Plus fields like form (10-K, 10-Q), fiscal year, fiscal period, end date, filed date, accession number, unit, and frame where available.
Stored as:
- CSV data/raw/sec/companyfacts/companyfacts_2022_2025.csv
- SQLite database data/raw/sec/sec_companyfacts.db, table SEC_COMPANYFACTS

## 5) Merged daily panel (prices + returns + fundamentals aligned)
Source: Built locally by aligning the latest filed fundamentals to each trading day (scripts/04b_align_fundamentals.py)
Provides: Per day per ticker:
- Price and return fields
- Fundamentals carried forward using the latest filed data on or before each trading day
Stored as:
- CSV data/processed/panel/daily_panel_prices_returns_fundamentals.csv
- CSV data/processed/panel/fundamentals_wide_by_filed.csv
- SQLite database data/processed/panel/panel.db, tables US_DAILY_PANEL and US_FUNDAMENTALS_WIDE_BY_FILED

## 6) Train and test splits
Source: Built locally (scripts/05_make_splits.py)
Provides: Train and test split files by date ranges.
Stored as:
- CSV data/processed/splits/train_2022_2024.csv
- CSV data/processed/splits/test_2025.csv

## 7) Agent runtime inputs via MCP tools
These are not new data sources. They are tool wrappers over your stored artefacts.

MCP server: finAgents/server_us_finance.py
Reads from:
- data/raw/prices_us.db (prices and returns)
- data/raw/sec/sec_companyfacts.db (SEC fundamentals)
- data/processed/panel/panel.db (merged panel)

Tools exposed:
- list_tickers
- get_prices
- get_returns
- get_companyfacts
- get_panel
- get_price_series (minimal series, if enabled)

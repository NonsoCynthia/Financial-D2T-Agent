# EU Stock Data Pipeline

This folder contains the European stock data pipeline for the Financial-D2T-Agent project.

It mirrors the US pipeline but uses Yahoo Finance instead of SEC filings. The goal is to generate EU artefacts with the same `raw/processed` layout pattern while keeping storage separate by region.

Primary outputs are:

- `data_eu/processed/panel/monthly_panel_prices_returns_fundamentals.csv`
- `data_eu/processed/mcp/fundamental_analysis.db` (agent-query DB, US-compatible table names)
- `data_eu/processed/benchmarks/roic_gold_benchmark_<YYYY-MM-DD>.csv`

Backward-compatible EU legacy output is also written:

- `data_eu/eu_monthly_panel.csv`

Storage convention:

- EU pipeline writes to `data_eu/...`
- US pipeline writes to `data/...`

This dataset can be merged with your US dataset for cross-region modelling.

---

## Stocks Covered

Irish Listings:

* KRZ.IR – Kerry Group
* A5G.IR – AIB Group
* BIRG.IR – Bank of Ireland

European Large Caps:

* ASML.AS – ASML
* SAP.DE – SAP
* MC.PA – LVMH
* NOVO-B.CO – Novo Nordisk
* SIE.DE – Siemens
* OR.PA – L’Oréal
* NESN.SW – Nestlé

---

## Folder Structure

Financial-D2T-Agent/

scripts_eu/
│
├── 01_download_prices_eu.py
├── 02_compute_returns_eu.py
├── 03_download_fundamentals_eu.py
├── 03b_download_reports_eu.py
├── 04_make_monthly_panel_eu.py
├── 05_build_eu_databases.py
├── 06_build_roic_gold_benchmark_eu.py
├── 07_download_roic_snapshots_eu.py
├── run_eu_pipeline.py
└── README.md
│
data/
└── ...   # US pipeline data lives here

data_eu/
├── raw/
│   ├── prices/
│   ├── fundamentals/
│   └── reports/
├── processed/
│   ├── prices/
│   ├── panel/
│   ├── mcp/
│   └── benchmarks/
└── eu_monthly_panel.csv   # legacy compatibility file

---

## Dependencies

Install dependencies inside your virtual environment:

pip install yfinance pandas numpy

Optional but recommended:

pip install tqdm

---

## Environment Setup

From the root of your repository:

python -m venv venv

Mac / Linux:
source venv/bin/activate

Windows:
venv\Scripts\activate

Then install packages:

pip install -r requirements.txt

Or manually:

pip install yfinance pandas numpy

---

## How the Pipeline Works

Step 1
Download daily price data from Yahoo Finance.

Step 2
Compute daily and monthly returns.

Step 3
Download annual fundamentals.

Step 3b
Download annual and quarterly financial reports.

Step 4
Align fundamentals to monthly frequency and build a monthly panel.

Step 5
Build SQLite databases for agent querying with US-compatible table names:
`US_PRICES`, `US_RETURNS`, `SEC_COMPANYFACTS`, `US_DAILY_PANEL`,
`US_FUNDAMENTALS_WIDE_BY_FILED`, `US_MONTHLY_PANEL`.

Step 6
Download ROIC snapshots for EU tickers (current day by default).

Step 7
Build ROIC gold benchmark CSV/report from ROIC dumps.

Final Output
data_eu/processed/panel/monthly_panel_prices_returns_fundamentals.csv

---

## Running the Full Pipeline

From the project root:

python scripts_eu/run_eu_pipeline.py --all

That executes all configured steps in order.

---

## Running Individual Steps

If you want granular control:

python scripts_eu/01_download_prices_eu.py
python scripts_eu/02_compute_returns_eu.py
python scripts_eu/03_download_fundamentals_eu.py
python scripts_eu/03b_download_reports_eu.py
python scripts_eu/04_make_monthly_panel_eu.py
python scripts_eu/05_build_eu_databases.py
python scripts_eu/07_download_roic_snapshots_eu.py --mode single
python scripts_eu/06_build_roic_gold_benchmark_eu.py

Or with step control:

python scripts_eu/run_eu_pipeline.py --step prices
python scripts_eu/run_eu_pipeline.py --step panel
python scripts_eu/run_eu_pipeline.py --step roic_dump_download
python scripts_eu/run_eu_pipeline.py --step roic_gold_benchmark
python scripts_eu/run_eu_pipeline.py --all

---

## Notes on Fundamentals

Yahoo Finance fundamentals are not as standardised as SEC filings.

Some companies may have:

* Missing balance sheet fields
* Different naming conventions
* Null values

The pipeline handles missing values gracefully, but you should always inspect output before modelling.

---

## Combining US and EU Datasets

Once generated, you can merge:

data/us_monthly_panel.csv
data_eu/processed/panel/monthly_panel_prices_returns_fundamentals.csv

Using:

pd.concat([us_df, eu_df])

Ensure:

* Column names match
* Date format is identical
* Currency differences are considered

---

## Next Possible Extensions

* FX normalisation to USD or EUR
* Region indicator column
* European market index benchmark
* Sector normalisation

---

This EU pipeline allows you to train:

* US only models
* EU only models
* Global cross-market models

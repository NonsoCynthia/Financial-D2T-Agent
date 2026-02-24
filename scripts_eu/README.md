# EU Stock Data Pipeline

This folder contains the European stock data pipeline for the Financial-D2T-Agent project.

It mirrors the US pipeline but uses Yahoo Finance instead of SEC filings. The goal is to generate a monthly panel dataset for 10 major EU and Irish stocks with both price and fundamental features.

The final output is:

data_eu/eu_monthly_panel.csv

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
├── 04_make_monthly_panel_eu.py
├── run_eu_pipeline.py
└── README.md
│
data_eu/
├── prices/
├── monthly_returns/
├── fundamentals/
└── eu_monthly_panel.csv

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

Step 4
Align fundamentals to monthly frequency.

Final Output
data_eu/eu_monthly_panel.csv

---

## Running the Full Pipeline

From the project root:

cd scripts_eu
python run_eu_pipeline.py

That executes all four scripts in the correct order.

---

## Running Individual Steps

If you want granular control:

python 01_download_prices_eu.py
python 02_compute_returns_eu.py
python 03_download_fundamentals_eu.py
python 04_make_monthly_panel_eu.py

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
data_eu/eu_monthly_panel.csv

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
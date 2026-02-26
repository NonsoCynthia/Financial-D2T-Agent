from pathlib import Path
import os
from datetime import date, timedelta

TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]

START_DATE = os.getenv("START_DATE", "2022-01-03")
# Always use today's date for rolling experiments.
END_DATE_INCLUSIVE = date.today().isoformat()
_end_date_obj = date.fromisoformat(END_DATE_INCLUSIVE)
END_DATE_EXCLUSIVE = (_end_date_obj + timedelta(days=1)).isoformat()

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = Path("results")
RESULTS_US_DIR = RESULTS_DIR / "final_report2025_us"
VALIDATION_DIR = RESULTS_DIR / "validation"

PRICES_RAW_DIR = RAW_DIR / "prices"
SEC_RAW_DIR = RAW_DIR / "sec"

# Canonical DB paths used by scripts/.
PRICES_DB_PATH = RAW_DIR / "prices_us.db"
SEC_DB_PATH = SEC_RAW_DIR / "sec_companyfacts.db"
PANEL_DB_PATH = PROCESSED_DIR / "panel" / "panel.db"
MCP_DB_PATH = PROCESSED_DIR / "mcp" / "fundamental_analysis.db"

SEC_COMPANYFACTS_DIR = SEC_RAW_DIR / "companyfacts"
SEC_COMPANYFACTS_END_YEAR = END_DATE_INCLUSIVE[:4]
SEC_COMPANYFACTS_CSV = SEC_COMPANYFACTS_DIR / f"companyfacts_2022_{SEC_COMPANYFACTS_END_YEAR}.csv"
SEC_COMPANYFACTS_CSV_LEGACY = SEC_COMPANYFACTS_DIR / "companyfacts_2022_2025.csv"
SEC_FILINGS_OUT_DIR = SEC_RAW_DIR / "filings_raw"
SEC_TICKER_MAP_CSV_ALL = SEC_RAW_DIR / "sec_ticker_cik_all.csv"
SEC_TICKER_MAP_CSV_SELECTED = SEC_RAW_DIR / "sec_ticker_cik_selected.csv"

US_PRICES_TABLE = "US_PRICES"
US_RETURNS_TABLE = "US_RETURNS"
SEC_COMPANYFACTS_TABLE = "SEC_COMPANYFACTS"
US_DAILY_PANEL_TABLE = "US_DAILY_PANEL"
US_MONTHLY_PANEL_TABLE = "US_MONTHLY_PANEL"
US_FUNDAMENTALS_WIDE_TABLE = "US_FUNDAMENTALS_WIDE_BY_FILED"

REQUIRED_PRICE_COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

SEC_FILED_START = os.getenv("SEC_FILED_START", "2022-01-01")
SEC_FILED_END = os.getenv("SEC_FILED_END", END_DATE_INCLUSIVE)
SEC_FILINGS_FORMS = tuple(
    x.strip()
    for x in os.getenv("SEC_FILINGS_FORMS", "10-K,10-Q,10-K/A,10-Q/A,20-F,20-F/A,40-F,40-F/A").split(",")
    if x.strip()
)
SEC_MAP_FETCH_RETRIES = int(os.getenv("SEC_MAP_FETCH_RETRIES", "5"))
SEC_MAP_TIMEOUT_SECONDS = int(os.getenv("SEC_MAP_TIMEOUT_SECONDS", "30"))
SEC_FILINGS_FETCH_RETRIES = int(os.getenv("SEC_FILINGS_FETCH_RETRIES", "6"))
SEC_FILINGS_JSON_TIMEOUT_SECONDS = int(os.getenv("SEC_FILINGS_JSON_TIMEOUT_SECONDS", "40"))
SEC_FILINGS_DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("SEC_FILINGS_DOWNLOAD_TIMEOUT_SECONDS", "60"))
SEC_RETRY_INITIAL_SLEEP_SECONDS = float(os.getenv("SEC_RETRY_INITIAL_SLEEP_SECONDS", "0.5"))
SEC_MAP_RETRY_STEP_SECONDS = float(os.getenv("SEC_MAP_RETRY_STEP_SECONDS", "0.5"))
SEC_FILINGS_RETRY_STEP_SECONDS = float(os.getenv("SEC_FILINGS_RETRY_STEP_SECONDS", "0.7"))
SEC_FILINGS_INTER_REQUEST_SLEEP_SECONDS = float(os.getenv("SEC_FILINGS_INTER_REQUEST_SLEEP_SECONDS", "0.12"))
SEC_FILINGS_RETRY_STATUSES = tuple(
    int(x.strip()) for x in os.getenv("SEC_FILINGS_RETRY_STATUSES", "429,500,502,503,504").split(",") if x.strip()
)
SEC_MAP_RETRY_STATUSES = tuple(
    int(x.strip()) for x in os.getenv("SEC_MAP_RETRY_STATUSES", "429,503").split(",") if x.strip()
)
SEC_COMPANYFACTS_FETCH_RETRIES = int(os.getenv("SEC_COMPANYFACTS_FETCH_RETRIES", str(SEC_FILINGS_FETCH_RETRIES)))
SEC_COMPANYFACTS_TIMEOUT_SECONDS = int(
    os.getenv("SEC_COMPANYFACTS_TIMEOUT_SECONDS", str(SEC_FILINGS_JSON_TIMEOUT_SECONDS))
)
SEC_COMPANYFACTS_INTER_REQUEST_SLEEP_SECONDS = float(
    os.getenv("SEC_COMPANYFACTS_INTER_REQUEST_SLEEP_SECONDS", "0.12")
)
SEC_COMPANYFACTS_RETRY_STATUSES = tuple(
    int(x.strip())
    for x in os.getenv("SEC_COMPANYFACTS_RETRY_STATUSES", "429,500,502,503,504").split(",")
    if x.strip()
)
SEC_COMPANYFACTS_FORMS = tuple(
    x.strip()
    for x in os.getenv(
        "SEC_COMPANYFACTS_FORMS",
        "10-K,10-Q,10-K/A,10-Q/A,20-F,20-F/A,40-F,40-F/A,6-K",
    ).split(",")
    if x.strip()
)

#For SEC endpoints, you should replace that with a real contact string you control. Otherwise you will eventually hit rate limits or blocks.
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "YourName yourmail@domain.com") 

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_HEADERS_BASE = {
    "User-Agent": SEC_USER_AGENT,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Encoding": "gzip, deflate",
}

SEC_MAP_DIR = RAW_DIR / "sec"

TRAIN_END = os.getenv("TRAIN_END", "2024-12-31")
TEST_START = os.getenv("TEST_START", "2025-01-01")
TEST_END = os.getenv("TEST_END", END_DATE_INCLUSIVE)
TRAIN_SPLIT_ALIAS = os.getenv("TRAIN_SPLIT_ALIAS", "train_2022_2024")
TEST_SPLIT_ALIAS = os.getenv("TEST_SPLIT_ALIAS", "test_2025")

FUNDAMENTAL_CONCEPTS = [
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
]

SPLIT_FEATURE_COLS = [
    "ret_1d",
    "ret_5d",
    "ret_20d",
    "vol_20d",
    "vol_60d",
    "Volume",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "Revenues",
    "NetIncomeLoss",
    "OperatingIncomeLoss",
    "EarningsPerShareBasic",
    "CommonStockSharesOutstanding",
]

ROIC_GOLD_BENCHMARK_CSV = (
    PROCESSED_DIR / "benchmarks" / f"roic_gold_benchmark_{END_DATE_INCLUSIVE}.csv"
)
ROIC_GOLD_BENCHMARK_REPORT_JSON = (
    PROCESSED_DIR / "benchmarks" / f"roic_gold_benchmark_{END_DATE_INCLUSIVE}_report.json"
)
ROIC_DUMPS_DIR_DEFAULT = DATA_DIR / "roic_json_dumps_monthly_last_year"
ROIC_SOURCE_NAME_DEFAULT = os.getenv("ROIC_SOURCE_NAME_DEFAULT", "roic.ai")
SQLITE_COPY_CHUNKSIZE = int(os.getenv("SQLITE_COPY_CHUNKSIZE", "200000"))
SCRIPT_PIPELINE_STEP_ORDER = [
    "download_prices",
    "sec_map",
    "sec_companyfacts",
    "sec_filings",
    "compute_returns",
    "align_fundamentals",
    "make_splits",
    "monthly_panel",
    "yahoo_spotcheck",
    "fundamental_db",
    "roic_gold_benchmark",
]

ALL_PRICES_LONG_CSV = PRICES_RAW_DIR / "all_prices_long.csv"
DAILY_RETURNS_CSV = PROCESSED_DIR / "prices" / "daily_returns.csv"
DAILY_PANEL_CSV = PROCESSED_DIR / "panel" / "daily_panel_prices_returns_fundamentals.csv"
FUNDAMENTALS_WIDE_BY_FILED_CSV = PROCESSED_DIR / "panel" / "fundamentals_wide_by_filed.csv"
MONTHLY_PANEL_CSV = PROCESSED_DIR / "panel" / "monthly_panel_prices_returns_fundamentals.csv"

YAHOO_SPOTCHECK_TARGET_KEYS = [
    "Assets",
    "CashAndEquivalents",
    "NetRevenue_TTM",
    "EBIT_TTM",
    "NetProfit_TTM",
    "EPS",
    "P_E",
    "P_B",
    "last_price",
]
YAHOO_SPOTCHECK_TOLERANCE = float(os.getenv("YAHOO_SPOTCHECK_TOLERANCE", "0.30"))

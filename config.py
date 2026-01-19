from pathlib import Path

TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]

START_DATE = "2022-01-03"
END_DATE_INCLUSIVE = "2025-12-31"
END_DATE_EXCLUSIVE = "2026-01-01"

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

PRICES_RAW_DIR = RAW_DIR / "prices"
SEC_RAW_DIR = RAW_DIR / "sec"

SEC_USER_AGENT = "YourName yourmail@domain.com"
# SEC_USER_AGENT = "Chinonso Osuji your.email@domain.com"

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_HEADERS_BASE = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    # "Host": "www.sec.gov",
}

SEC_MAP_DIR = RAW_DIR / "sec"

TRAIN_END = "2024-12-31"
TEST_START = "2025-01-01"
TEST_END = "2025-12-31"
from __future__ import annotations

from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# EU storage root (kept separate from US `data/`).
DATA_EU_ROOT = PROJECT_ROOT / "data_eu"
DATA_EU_ROOTS = [DATA_EU_ROOT]

# Backward-compatible legacy flat data_eu paths.
DATA_EU_LEGACY_ROOT = DATA_EU_ROOT


def rel_to_all_roots(relative: Path) -> list[Path]:
    return [root / relative for root in DATA_EU_ROOTS]


def ensure_dirs(paths: list[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def ensure_parent_dirs(paths: list[Path]) -> None:
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)


RAW_PRICES_DIRS = rel_to_all_roots(Path("raw") / "prices")
RAW_FUNDAMENTALS_DIRS = rel_to_all_roots(Path("raw") / "fundamentals")
RAW_REPORTS_DIRS = rel_to_all_roots(Path("raw") / "reports")

PROCESSED_PRICES_DIRS = rel_to_all_roots(Path("processed") / "prices")
PROCESSED_MONTHLY_RETURNS_DIRS = rel_to_all_roots(Path("processed") / "prices" / "monthly_returns")
PROCESSED_PANEL_DIRS = rel_to_all_roots(Path("processed") / "panel")
PROCESSED_MCP_DIRS = rel_to_all_roots(Path("processed") / "mcp")
PROCESSED_BENCHMARKS_DIRS = rel_to_all_roots(Path("processed") / "benchmarks")

RAW_SEC_DIRS = rel_to_all_roots(Path("raw") / "sec")
RAW_SEC_COMPANYFACTS_DIRS = rel_to_all_roots(Path("raw") / "sec" / "companyfacts")

RAW_PRICES_DB_PATH = DATA_EU_ROOT / "raw" / "prices_us.db"
RAW_SEC_COMPANYFACTS_DB_PATH = DATA_EU_ROOT / "raw" / "sec" / "sec_companyfacts.db"
PROCESSED_PANEL_DB_PATH = DATA_EU_ROOT / "processed" / "panel" / "panel.db"
PROCESSED_MCP_DB_PATH = DATA_EU_ROOT / "processed" / "mcp" / "fundamental_analysis.db"
ROIC_DUMPS_DIR_DEFAULT = DATA_EU_ROOT / "roic_json_dumps_monthly_last_year"
ROIC_GOLD_BENCHMARK_CSV = (
    DATA_EU_ROOT / "processed" / "benchmarks" / f"roic_gold_benchmark_{date.today().isoformat()}.csv"
)
ROIC_GOLD_BENCHMARK_REPORT_JSON = (
    DATA_EU_ROOT / "processed" / "benchmarks" / f"roic_gold_benchmark_{date.today().isoformat()}_report.json"
)

# Legacy flat dirs/files kept for compatibility with existing outputs.
LEGACY_PRICES_DIR = DATA_EU_LEGACY_ROOT / "prices"
LEGACY_FUNDAMENTALS_DIR = DATA_EU_LEGACY_ROOT / "fundamentals"
LEGACY_REPORTS_DIR = DATA_EU_LEGACY_ROOT / "reports"
LEGACY_MONTHLY_RETURNS_DIR = DATA_EU_LEGACY_ROOT / "monthly_returns"
LEGACY_MONTHLY_PANEL_FILE = DATA_EU_LEGACY_ROOT / "eu_monthly_panel.csv"

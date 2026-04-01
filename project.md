# Financial-D2T-Agent

A multi-agent data-to-text (D2T) system for generating professional monthly equity research reports from structured financial data. Built as part of PhD research on multi-agent NLG architectures, supporting both English and Irish (Gaeilge) output.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Dataset Extraction Pipeline — US](#dataset-extraction-pipeline--us)
4. [Dataset Extraction Pipeline — EU](#dataset-extraction-pipeline--eu)
5. [Upstream Stock Analysis (run.sh)](#upstream-stock-analysis-runsh)
6. [Financial Analyst & Manager Agent Simulations](#financial-analyst--manager-agent-simulations)
7. [Data Flow — NLG Pipeline](#data-flow--nlg-pipeline)
8. [Input Data Format](#input-data-format)
9. [NLG Pipeline Architecture](#nlg-pipeline-architecture)
10. [NLG Workflow Variants](#nlg-workflow-variants)
11. [NLG Agent Roles](#nlg-agent-roles)
12. [Prompt Design](#prompt-design)
13. [LLM Providers](#llm-providers)
14. [Configuration](#configuration)
15. [CLI Usage — NLG (run_nlg.sh)](#cli-usage--nlg-run_nlgsh)
16. [CLI Usage — Upstream Analysis (run.sh)](#cli-usage--upstream-analysis-runsh)
17. [Output Format](#output-format)
18. [Evaluation](#evaluation)
19. [Key Design Decisions](#key-design-decisions)

---

## Overview

Financial-D2T-Agent converts structured financial indicators (valuation ratios, profitability metrics, balance sheet figures, recommendations, and target prices) into document-level equity research reports. The system uses a LangGraph-based multi-agent pipeline where specialized agents handle content ordering, text structuring, and surface realization, with guardrails validating each stage.

**Research context**: Ablation studies on multi-agent NLG architectures — comparing full pipeline vs. unified worker vs. end-to-end generation, with and without guardrails, finalizer, and orchestrator.

**Dataset**: US and EU equities, January 2025 to February 2026 (14 monthly reports per region).

---

## Project Structure

```
Financial-D2T-Agent/
├── config.py                  # Central configuration: paths, model names, dates, constants
├── main.py                    # D2TAgentExperimentRunner — main orchestration class
├── load_data.py               # Data loading, sample construction, previous report loading
├── run.sh                     # Upstream stock analysis pipeline runner
├── run_nlg.sh                 # NLG pipeline bash wrapper (primary entry point)
├── run_nlg.py                 # NLG CLI entry point with full argument parsing
├── run_pipeline.py            # Coordinates agent/workflow runs and evaluation
│
├── openai-agent/              # Upstream financial agent simulations
│   └── experiments/
│       └── final_report2025/
│           ├── run_one_ticker.py  # Per-ticker analysis runner
│           ├── agent.py           # Financial Analyst & Manager agent definitions
│           ├── workflow.py        # Agent/workflow mode orchestration
│           └── evaluate.py        # Evaluation against gold benchmarks
│
├── agents/                    # NLG multi-agent pipeline
│   ├── llm_model.py           # UnifiedModel: multi-provider LLM interface
│   ├── agent_prompts.py       # All prompts: orchestrator, workers, guardrails, finalizer, e2e
│   ├── agents_modules/
│   │   ├── orchestrator.py    # TaskOrchestrator — directs NLG pipeline stages
│   │   ├── worker.py          # TaskWorker — executes CO, TS, SR stages
│   │   ├── task.py            # UnifiedTaskWorker — single worker for all stages
│   │   ├── guardrail.py       # TaskGuardrail — validates worker outputs
│   │   ├── finalizer.py       # TaskFinalizer — light post-editing of final report
│   │   └── workflow.py        # LangGraph workflow builders + guardrail routing logic
│   └── utilities/
│       ├── utils.py           # ExecutionState TypedDict, AgentStepOutput dataclass
│       └── agent_utils.py     # Helpers: summarize steps, variable substitution
│
├── scripts/                   # US dataset extraction pipeline (10 steps)
│   ├── run_scripts.py         # Pipeline runner: --all or --step <name>
│   ├── 01_download_prices.py        # Download OHLCV from Yahoo Finance
│   ├── 02_sec_ticker_cik.py         # Fetch ticker↔CIK mapping from SEC EDGAR
│   ├── 03a_sec_companyfacts.py      # Download XBRL fundamentals from SEC
│   ├── 03b_sec_download_filings.py  # Download raw SEC filings
│   ├── 04a_compute_returns.py       # Compute daily returns
│   ├── 04b_align_fundamentals.py    # Align fundamentals to daily frequency
│   ├── 05_make_splits.py            # Create train/test splits
│   ├── 06_make_monthly_panel.py     # Resample to monthly frequency
│   ├── 07_yahoo_gold_spotcheck.py   # Validate data against Yahoo Finance
│   ├── 08_build_mcp_db.py           # Build consolidated MCP database
│   ├── 09_build_roic_gold_benchmark.py   # Build ROIC gold benchmark
│   └── 10_download_roic_snapshots.py     # Download ROIC snapshots
│
├── scripts_eu/                # EU dataset extraction pipeline (7 steps)
│   ├── run_eu_pipeline.py     # Pipeline runner: --all or --step <name>
│   ├── 01_download_prices_eu.py          # Download EU stock prices
│   ├── 02_compute_returns_eu.py          # Compute EU returns
│   ├── 03_download_fundamentals_eu.py    # Download fundamentals (Yahoo, no SEC)
│   ├── 03b_download_reports_eu.py        # Download annual/quarterly reports
│   ├── 04_make_monthly_panel_eu.py       # Build monthly panel
│   ├── 05_build_eu_databases.py          # Build SQLite databases
│   ├── 06_build_roic_gold_benchmark_eu.py  # Build ROIC gold benchmark
│   └── 07_download_roic_snapshots_eu.py    # Download ROIC snapshots
│
├── data/                      # Raw + processed US data
│   ├── raw/
│   │   ├── prices_us.db              # SQLite: US_PRICES, US_RETURNS tables
│   │   ├── prices/*.csv              # Per-ticker price CSVs
│   │   └── sec/
│   │       ├── sec_ticker_cik_selected.csv
│   │       ├── sec_companyfacts.db   # SQLite: SEC_COMPANYFACTS table
│   │       ├── companyfacts/         # XBRL CSVs
│   │       └── filings_raw/         # Raw SEC filings
│   └── processed/
│       ├── panel/                    # Daily & monthly panels
│       ├── splits/                   # Train/test splits
│       ├── benchmarks/               # ROIC gold benchmarks
│       └── mcp/
│           └── fundamental_analysis.db  # Consolidated agent runtime DB
│
├── data_eu/                   # Raw + processed EU data (mirrors US structure)
│
├── results/
│   ├── final_report2025_us/   # Upstream US analysis results
│   │   ├── gpt-5/
│   │   │   ├── agent_False/         # Agent mode, no reflection
│   │   │   ├── agent_True/          # Agent mode, with reflection
│   │   │   ├── workflow_False/      # Workflow mode, no reflection
│   │   │   └── workflow_True/       # Workflow mode, with reflection
│   │   └── gpt-5-mini/
│   │       └── workflow_False/
│   ├── final_report2025_eu/   # Upstream EU analysis results (same structure)
│   ├── nlg/                   # NLG output directory (auto-structured)
│   └── validation/            # Evaluation metrics
│       ├── us/                # table1, table2 CSVs
│       └── eu/
```

---

## Dataset Extraction Pipeline — US

The US data pipeline (`scripts/`) downloads, processes, and validates financial data in 10 sequential steps. Run with:

```bash
# Full pipeline
python scripts/run_scripts.py --all

# Single step
python scripts/run_scripts.py --step 01_download_prices
```

### Step-by-Step Breakdown

| Step | Script | What It Does | Data Source | Output |
|------|--------|-------------|-------------|--------|
| 1 | `01_download_prices.py` | Downloads daily OHLCV (Open, High, Low, Close, Volume) price data for all tickers | Yahoo Finance (`yfinance`) | `data/raw/prices_us.db` (US_PRICES table), `data/raw/prices/*.csv` |
| 2 | `02_sec_ticker_cik.py` | Fetches the ticker ↔ CIK (Central Index Key) mapping needed for SEC EDGAR queries | SEC EDGAR API | `data/raw/sec/sec_ticker_cik_selected.csv` |
| 3a | `03a_sec_companyfacts.py` | Downloads XBRL-tagged fundamental data (assets, revenue, earnings, etc.) | SEC CompanyFacts JSON endpoint | `data/raw/sec/companyfacts/companyfacts_2022_*.csv`, `data/raw/sec/sec_companyfacts.db` |
| 3b | `03b_sec_download_filings.py` | Downloads raw SEC filings (10-K, 10-Q) for each ticker | SEC EDGAR filings API | `data/raw/sec/filings_raw/` |
| 4a | `04a_compute_returns.py` | Computes daily percentage returns from closing prices | Local computation | `data/processed/prices/daily_returns.csv`, US_RETURNS table |
| 4b | `04b_align_fundamentals.py` | Aligns quarterly/annual fundamentals to daily frequency, building a combined panel | Local panel building | `data/processed/panel/daily_panel_prices_returns_fundamentals.csv`, US_DAILY_PANEL & US_FUNDAMENTALS_WIDE_BY_FILED tables |
| 5 | `05_make_splits.py` | Creates train (2022–2024) and test (2025+) splits | Local split generation | `data/processed/splits/train_2022_2024.csv`, `data/processed/splits/test_2025.csv` |
| 6 | `06_make_monthly_panel.py` | Resamples the daily panel to monthly frequency (month-end snapshots) | Local resampling | `data/processed/panel/monthly_panel_prices_returns_fundamentals.csv` |
| 7 | `07_yahoo_gold_spotcheck.py` | Validates computed indicators against Yahoo Finance live data | Yahoo Finance spot checks | Validation report (stdout) |
| 8 | `08_build_mcp_db.py` | Consolidates all processed data into a single SQLite database for agent runtime | SQLite consolidation | `data/processed/mcp/fundamental_analysis.db` |
| 9 | `09_build_roic_gold_benchmark.py` | Parses ROIC.ai JSON dumps to build a gold-standard benchmark for evaluation | ROIC JSON dump files | `data/processed/benchmarks/roic_gold_benchmark_<DATE>.csv` |
| 10 | `10_download_roic_snapshots.py` | Downloads raw ROIC.ai snapshots for all tickers | ROIC.ai website | ROIC JSON dump files |

### Fundamental Concepts Extracted (from SEC XBRL)

```python
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
```

### US Tickers

```python
TICKERS = ["TSLA", "AMZN", "NIO", "MSFT", "AAPL", "GOOG", "NFLX", "COIN"]
```

### Date Window

- **Start**: 2022-01-03
- **End**: Today (rolling)
- **Train split**: 2022-01-01 to 2024-12-31
- **Test split**: 2025-01-01 to present

---

## Dataset Extraction Pipeline — EU

The EU data pipeline (`scripts_eu/`) mirrors the US pipeline but uses Yahoo Finance instead of SEC EDGAR (no SEC filings for European stocks). Run with:

```bash
# Full pipeline
python scripts_eu/run_eu_pipeline.py --all

# Single step
python scripts_eu/run_eu_pipeline.py --step 01_download_prices_eu
```

### Step-by-Step Breakdown

| Step | Script | What It Does | Data Source |
|------|--------|-------------|-------------|
| 1 | `01_download_prices_eu.py` | Downloads daily OHLCV prices for EU tickers | Yahoo Finance |
| 2 | `02_compute_returns_eu.py` | Computes daily and monthly returns | Local computation |
| 3 | `03_download_fundamentals_eu.py` | Downloads fundamental data (limited vs SEC) | Yahoo Finance |
| 3b | `03b_download_reports_eu.py` | Downloads annual/quarterly financial reports | Financial websites |
| 4 | `04_make_monthly_panel_eu.py` | Builds monthly panel from daily data | Local resampling |
| 5 | `05_build_eu_databases.py` | Builds SQLite databases using US-compatible table schemas | SQLite consolidation |
| 6 | `06_build_roic_gold_benchmark_eu.py` | Builds ROIC gold benchmark for EU stocks | ROIC.ai dumps |
| 7 | `07_download_roic_snapshots_eu.py` | Downloads ROIC.ai snapshots | ROIC.ai |

### EU Tickers

```
Irish:      KRZ.IR, A5G.IR, BIRG.IR
Large Caps: ASML.AS, SAP.DE, MC.PA, NOVO-B.CO, SIE.DE, OR.PA, NESN.SW
```

### Output

- EU data: `data_eu/processed/` (mirrors US structure)
- MCP database: `data_eu/processed/mcp/fundamental_analysis.db` (US-compatible table names)

---

## Upstream Stock Analysis (run.sh)

`run.sh` is the entry point for the **upstream stock analysis pipeline** — the stage that runs **before** NLG. It simulates financial analyst and financial manager agents that analyze each stock and produce structured predictions (indicators, recommendations, target prices).

### How It Works

```
run.sh
  ↓
run_pipeline.py (orchestration engine)
  ↓
openai-agent/experiments/final_report2025/run_one_ticker.py (per-ticker analysis)
  ↓
openai-agent/experiments/final_report2025/evaluate.py (evaluation against gold benchmarks)
```

### Pipeline Phases

1. **Analysis phase**: For each ticker × each month, runs the financial analyst agent followed by the financial manager agent
2. **Evaluation phase**: Compares predicted indicators against ROIC gold-standard benchmarks

### Run Modes (`--mode`)

| Mode | What It Does |
|------|-------------|
| `agent` | Runs analyst + manager agents only (OpenAI Agents SDK) |
| `workflow` | Runs a structured workflow orchestration |
| `both` | Runs agent mode first, then workflow mode |

### Reflection Branches

Each run creates a branch directory based on the `--reflection` flag:

- `workflow_False/` or `agent_False/` — No reflection (single-pass)
- `workflow_True/` or `agent_True/` — Reflection enabled (agent can revise its answers)

### Output

Per-ticker, per-month JSON files saved in:

```
results/final_report2025_{us,eu}/{model}/{mode}_{reflection}/
```

Each JSON file contains:
- 32 computed financial indicators
- Recommendation (Buy / Hold / Sell)
- Target price
- Recommendation justification text
- Monthly report narrative

---

## Financial Analyst & Manager Agent Simulations

Located in `openai-agent/experiments/final_report2025/agent.py`, these are the two LLM-based agents that simulate a two-tier financial analysis workflow.

### Financial Analyst Agent

**Role**: Computes fundamental indicators from raw financial data for a single ticker on a single analysis date.

**Reasoning level**: `medium` (ExtendedThinking disabled to control cost)

**Input**: Ticker name, CIK, analysis date, last known price, any feedback from prior attempts.

**Output schema** (`IndicatorOutput`):
```python
class Indicator:
    indicator_name: str   # e.g. "P_E", "ROE", "Assets"
    value: float          # computed value

class IndicatorOutput:
    indicators: list[Indicator]
```

**What it computes** (32 indicators across 4 categories):
- **Valuation**: P/E, P/B, EV/EBIT, EV/EBITDA, P/EBIT, P/S, P/Assets, P/NCA, P/WC
- **Profitability**: ROE, ROIC, EBIT Margin, Net Margin, Gross Margin, EPS
- **Earnings/Revenue**: Net Income, Revenue TTM, Net Income TTM, EBIT Q, Revenue Q
- **Balance Sheet**: Assets, Current Assets, Current Ratio, Cash, Gross Debt, Net Debt, BVPS, Asset Turnover, Gross Debt/Equity

### Financial Manager Agent

**Role**: Makes investment decisions based on the analyst's indicator output, historical price/fundamental context, and any previous manager decisions.

**Reasoning level**: `high` (ExtendedThinking enabled for higher-quality decisions)

**Input**: Ticker, analysis date, current price, analyst indicators, historical context (last 12 months of prices + fundamentals), previous manager decision context.

**Output schema** (`ManagerDecision`):
```python
class ManagerDecision:
    recommendation: str     # "Buy", "Sell", or "Hold"
    monthly_report: str     # Narrative monthly report text
    justification: str      # Rationale for the recommendation
    target_price: float     # Predicted target price
```

### MCP Integration (Optional)

When enabled (`--mcp` flag), agents can use **Model Context Protocol** servers to query the `fundamental_analysis.db` database directly during analysis, providing access to historical data via SQL queries rather than pre-formatted context.

### Agent Execution Flow

```
For each ticker × each month:
    1. Financial Analyst Agent
       - Receives: ticker, CIK, date, price
       - Returns: IndicatorOutput (32 indicators)
            ↓
    2. Financial Manager Agent
       - Receives: ticker, date, price, analyst indicators,
                   12-month history, previous decision context
       - Returns: ManagerDecision (recommendation, target price,
                  justification, monthly report)
            ↓
    3. Save results as JSON → results/final_report2025_{region}/{model}/{branch}/
```

### Reasoning Policy

The reasoning levels are **enforced in code** (not configurable at runtime):

| Agent | Reasoning Level | Why |
|-------|----------------|-----|
| Financial Analyst | `medium` | Cost control — indicator computation is formulaic |
| Financial Manager | `high` | Quality — investment decisions require deeper reasoning |

---

## Data Flow — NLG Pipeline

### 1. Upstream Analysis Produces Structured Data

The analyst + manager agents (run.sh) produce per-ticker, per-month JSON files containing 32 indicators, recommendations, and target prices. These are stored in `results/final_report2025_{us,eu}/{model}/{branch}/`.

### 2. Data Loading (load_data.py)

`load_data.py` reads upstream results and builds generation samples:
- Groups all tickers for a given month into one **multi-stock bundle**
- Each sample = one month of all tickers as **SPO (Subject-Predicate-Object) triples**
- Also loads previous NLG reports for temporal continuity context

Key functions:
- `build_multi_stock_prompt_context()` — Formats triples into human-readable text for the LLM
- `_load_previous_reports_map()` — Loads previous month's reports from directory or file
- `_sanitize_context_text()` — Truncates context to max 4000 chars

### 3. Query Building (main.py)

`D2TAgentExperimentRunner.build_query()` fills the input prompt template with:
- `{analysis_date}` — The month being reported on
- `{tickers}` — Comma-separated list of stock tickers
- `{ticker_count}` — Number of tickers in the bundle
- `{horizon_months}` — Months remaining from analysis date to coverage end (Feb 2026)
- `{end_date}` — Coverage end date (2026-02-28)
- `{previous_report}` — Previous month's NLG output (or "N/A")
- `{data}` — The formatted multi-stock prompt context

Uses `_SafeFormatDict` so missing keys pass through as `{key}` rather than crashing.

### 4. Pipeline Execution (workflow.py)

For the **default** workflow:
```
orchestrator → content ordering → guardrail → orchestrator →
text structuring → guardrail → orchestrator →
surface realization → guardrail → finalizer → END
```

### 5. Output

Saved as JSON (full execution state) and TXT (report text) under the auto-structured output directory.

---

## Input Data Format

Each generation sample is a multi-stock monthly bundle of SPO triples:

```python
# Report-level facts
["M_SMRG_2025-01-31", "analysis_month", "2025-01-31"]
["M_SMRG_2025-01-31", "stock_count", "8"]
["M_SMRG_2025-01-31", "covers_ticker", "AAPL"]

# Per-ticker facts
["AAPL", "Recommendation", "Hold"]
["AAPL", "TargetPrice", "236"]
["AAPL", "RecommendationJustification", "Apple trades at elevated..."]
["AAPL", "P_E", "38.63"]
["AAPL", "ROIC", "25.8"]
["AAPL", "Assets", "344085000000"]
# ... all indicators, then next ticker
```

### Indicator Categories

| Category | Indicators |
|----------|-----------|
| **Valuation** | P_E, P_B, EV_EBIT, EV_EBITDA, P_EBIT, P_S, P_Assets, P_NCA, P_WC |
| **Profitability** | ROE, ROIC, EBITMargin, NetMargin, GrossMargin, EPS |
| **Earnings/Revenue** | NetIncome, Revenue_TTM, NetIncome_TTM, EBIT_Q, Revenue_Q |
| **Balance Sheet** | Assets, CurrentAssets, CurrentRatio, CashAndEquivalents, GrossDebt, NetDebt, BVPS, AssetTurnover, GrossDebtEquity |

### Prompt Context Format

The triples are rendered into human-readable text:

```
Analysis month: 2025-01-31
Tickers in bundle (8): AAPL, AMZN, COIN, GOOG, MSFT, NFLX, NIO, TSLA
Previous month's multi-stock report context:
N/A

Current month structured bundle:
- AAPL: recommendation=Hold, target_price=236
  - recommendation_justification: Apple trades at elevated multiples...
  - (AAPL, AssetTurnover, 0)
  - (AAPL, Assets, 344085000000)
  ...
- AMZN: recommendation=Hold, target_price=241.91
  ...
```

---

## NLG Pipeline Architecture

### ExecutionState (TypedDict)

The shared state flowing through the LangGraph pipeline:

| Field | Type | Description |
|-------|------|-------------|
| `data_input` | str | Raw SPO triples |
| `user_prompt` | str | Filled input_prompt template |
| `history_of_steps` | list[AgentStepOutput] | Trace of all agent actions |
| `worker_attempts` | dict | Per-worker attempt counts |
| `max_worker_attempts` | int | Global cap (default 5) |
| `last_worker` | str | Name of most recently run worker |
| `review` | str | Latest guardrail feedback text |
| `final_response` | str | The finished report text |
| `next_agent` | str | Routing target for the next node |

### AgentStepOutput (dataclass)

Each step records:
- `agent_name` — Which agent ran
- `input` — What it received
- `output` — What it produced
- `rationale` — Why it made its decisions

---

## NLG Workflow Variants

### Default (Multi-Agent)

Full pipeline with 3 specialized workers, guardrails after each, and a finalizer:

```
orchestrator → CO worker → CO guardrail → orchestrator →
TS worker → TS guardrail → orchestrator →
SR worker → SR guardrail → finalizer → END
```

### Unified Worker

Same flow but a single worker handles all three stages (CO, TS, SR) using one combined prompt.

### End-to-End (e2e)

Single LLM call — no multi-agent stages. Fastest but least controllable.

### Ablation Variants (for research)

| Variant | Description |
|---------|-------------|
| `single_module` | All workers share one prompt |
| `no_guardrail` | Guardrails replaced with passthrough |
| `no_finalizer` | Finalizer replaced with passthrough |
| `no_orchestrator` | Fixed CO→TS→SR pipeline, no orchestrator routing |

---

## NLG Agent Roles

### Orchestrator (orchestrator.py)

Directs the 3-stage pipeline. Decides which worker runs next based on completed stages and guardrail feedback. Ensures all three stages (CO → TS → SR) execute before finalizing.

### Workers (worker.py)

Three specialized workers, each with a dedicated prompt:

1. **Content Ordering (CO)**: Reorders facts into optimal narrative sequence — strongest signals first, thematic grouping within each ticker (valuation → profitability → balance sheet)
2. **Text Structuring (TS)**: Groups ordered facts into `<paragraph>` and `<snt>` XML blocks supporting a six-section report structure
3. **Surface Realization (SR)**: Converts structured blocks into fluent institutional-grade research note prose

### Unified Worker (task.py)

Single worker that handles all 3 stages in sequence within one prompt. Used by the `unified_worker` workflow.

### Guardrail (guardrail.py)

Stage-specific validation after each worker:
- **CO guardrail**: Checks fact preservation — no facts dropped or invented
- **TS guardrail**: Checks XML tag structure and completeness
- **SR guardrail**: Returns structured JSON with verdicts on: report identity, naming discipline, linguistic quality, factuality, analytical quality, numerical discipline

### Finalizer (finalizer.py)

Light post-editing of the surface realization output. Must not shorten the report — only fix minor issues.

### Guardrail Routing Logic (workflow.py)

After each guardrail evaluation:
- If all 3 stages done + review says CORRECT → **finalizer**
- If last worker hit attempt limit but later stages pending → **orchestrator** (advance to next stage)
- If last worker hit attempt limit and all stages done → **finalizer**
- Otherwise → **orchestrator** (continue/retry)

---

## Prompt Design

All prompts live in `agents/agent_prompts.py`.

### System-Level Prompts

Used as `ChatPromptTemplate` system messages. These must **NOT** contain `{variable}` placeholders for data fields because LangChain treats them as required template variables. Data-specific values are passed through the user-level `input_prompt`.

### User-Level Prompt (input_prompt)

Contains template variables filled at runtime by `build_query()`:
- `{analysis_date}`, `{tickers}`, `{ticker_count}`
- `{horizon_months}`, `{end_date}`
- `{previous_report}`, `{data}`

Uses `_SafeFormatDict` so missing keys pass through as `{key}` rather than crashing.

### Report Structure (Six Required Sections)

1. **Report Identity** — Date, tickers, investment horizon
2. **Executive Summary** — Key findings across all stocks
3. **Methodology** — Data sources and analytical approach
4. **Per-Ticker Analysis** — Detailed per-stock analysis covering all indicators
5. **Cross-Stock Comparative** — Explicit comparisons naming tickers
6. **Risk & Conclusion** — Risk factors and closing summary

### Key Prompt Rules

- Zero values must be flagged as unavailable, not reported as real figures
- Target prices rounded to 2 decimal places
- Cross-stock comparisons must always name tickers explicitly (no anonymous references)
- Every indicator from the input data must be verbalized in the output
- Reports must be document-level (institutional grade), not brief summaries

---

## LLM Providers

`UnifiedModel` in `agents/llm_model.py` supports multiple providers:

| Provider | Flag Value | Notes |
|----------|-----------|-------|
| OpenAI | `openai` | Default. Models: gpt-5, gpt-5-mini |
| Anthropic | `anthropic` | Claude models |
| Groq | `groq` | Fast inference |
| Ollama | `ollama` | Local models |
| HuggingFace | `hf` / `huggingface` | HF Inference API |
| aiXplain | `aixplain` | aiXplain platform |

Default NLG model: `gpt-5`. Override with `--model` flag.

---

## Configuration

`config.py` contains central configuration:
- Dataset paths (`results/final_report2025_us/`, `results/final_report2025_eu/`)
- Model name mappings
- Date constants
- Output directory structure

### Coverage Window

- **US dataset**: January 2025 to February 2026 (14 monthly reports)
- **EU dataset**: Same window
- `horizon_months` = months remaining from analysis date to Feb 2026
  - Jan 2025 → 13, Feb 2025 → 12, ..., Feb 2026 → 0
  - This is the **investment horizon** (how far forward the recommendation looks), NOT the total dataset size

---

## CLI Usage — NLG (run_nlg.sh)

### Entry Point

```bash
./run_nlg.sh [options]
```

Run `./run_nlg.sh -h` for full help.

### Common Commands

```bash
# List available samples
./run_nlg.sh --list-samples --source-model gpt-5-mini

# Single month — default multi-agent pipeline
./run_nlg.sh --workflow default --source-model gpt-5-mini --analysis-date 2025-01-31

# Single month — end-to-end (single LLM call)
./run_nlg.sh --workflow e2e --source-model gpt-5-mini --analysis-date 2025-01-31

# All months in sequence (auto-chains previous month output)
./run_nlg.sh --workflow default --source-model gpt-5-mini --sequence

# Override NLG model
./run_nlg.sh --workflow default --source-model gpt-5-mini --analysis-date 2025-01-31 --model gpt-5-mini

# EU dataset
./run_nlg.sh --dataset-path results/final_report2025_eu --source-model gpt-5-mini --list-samples

# Irish language output
./run_nlg.sh --workflow default --source-model gpt-5-mini --analysis-date 2025-01-31 --language ga
```

### Key Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--workflow` | Pipeline type: `default`, `unified_worker`, `e2e` | `default` |
| `--source-model` | Upstream analysis model to read | Required |
| `--model` | NLG generation model | `gpt-5` |
| `--provider` | LLM provider | `openai` |
| `--language` | Output language: `en` or `ga` | `en` |
| `--analysis-date` | Single month to generate (YYYY-MM-DD) | — |
| `--sequence` | Run all months, chaining outputs | — |
| `--previous-reports-path` | Explicit path to prior NLG outputs | Auto-discovered |
| `--dataset-path` | Override dataset directory | US default |
| `--list-samples` | List available samples and exit | — |

---

## CLI Usage — Upstream Analysis (run.sh)

### Entry Point

```bash
./run.sh [options]
```

### Common Commands

```bash
# Run all US tickers, all months, agent mode, gpt-5-mini
./run.sh --all-tickers --mode agent --model gpt-5-mini

# Run a single ticker
./run.sh --ticker AAPL --mode agent --model gpt-5-mini

# Run specific tickers
./run.sh --tickers AAPL,MSFT,GOOG --mode agent --model gpt-5-mini

# Enable reflection (agent revises its own answers)
./run.sh --all-tickers --mode agent --model gpt-5-mini --reflection

# Workflow mode
./run.sh --all-tickers --mode workflow --model gpt-5-mini

# Both modes sequentially
./run.sh --all-tickers --mode both --model gpt-5-mini

# Restrict analysis date range
./run.sh --all-tickers --mode agent --model gpt-5-mini \
  --analysis-start-date 2025-01-31 --analysis-end-date 2025-06-30

# EU region
./run.sh --region eu --all-tickers --mode agent --model gpt-5-mini

# Run EU pipeline first, then analysis
./run.sh --region eu --run-eu-pipeline --all-tickers --mode agent --model gpt-5-mini

# Analysis only (skip evaluation)
./run.sh --all-tickers --mode agent --model gpt-5-mini --analysis-only

# Evaluation only (skip analysis, just evaluate existing results)
./run.sh --all-tickers --mode agent --model gpt-5-mini --eval-only

# Enable MCP (Model Context Protocol) for database querying
./run.sh --all-tickers --mode agent --model gpt-5-mini --mcp

# Run N times for statistical analysis
./run.sh --all-tickers --mode agent --model gpt-5-mini --n-times 3
```

### Key Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--region` | `us` or `eu` | `us` |
| `--mode` | `agent`, `workflow`, or `both` | — |
| `--model` | LLM model name | `gpt-5-mini` |
| `--ticker` | Single ticker to analyze | — |
| `--tickers` | Comma-separated tickers | — |
| `--all-tickers` | Run all tickers in the region | — |
| `--reflection` | Enable agent self-revision | Off |
| `--mcp` | Enable Model Context Protocol | Off |
| `--reasoning` | Agent reasoning verbosity | — |
| `--max-turns` | Max agent turns per analysis | `30` |
| `--n-times` | Repeat runs N times | `1` |
| `--analysis-start-date` | Restrict start of analysis window | — |
| `--analysis-end-date` | Restrict end of analysis window | — |
| `--analysis-only` | Skip evaluation phase | — |
| `--eval-only` | Skip analysis, only evaluate | — |
| `--run-eu-pipeline` | Run EU data pipeline before analysis | — |
| `--eval-mode` | `summary`, `folder`, `table2`, or `none` | — |

---

## Output Format

### Output Directory Structure

```
results/nlg/{region}/{source-model}/{branch}/{provider}/{nlg-model}/{language}/{workflow}/
```

Example:
```
results/nlg/us/gpt-5-mini/workflow_False/openai/gpt-5/en/default/
├── 2025-01-31.json    # Full execution state (all agent steps, metadata)
├── 2025-01-31.txt     # Final report text only
├── 2025-02-28.json
├── 2025-02-28.txt
└── ...
```

### JSON Output Fields

- `final_response` — The finished report text
- `history_of_steps` — Full trace of agent actions
- `worker_attempts` — How many attempts each worker needed
- `review` — Final guardrail feedback
- Metadata: model, provider, workflow, language, analysis_date, timestamps

### Previous Report Auto-Chaining

- **Sequence runs** (`--sequence`): Each month's output is automatically passed as `previous_report` to the next month
- **Single runs** (`--analysis-date`): Auto-discovers previous month's output from the NLG output directory via `_find_previous_month_report()`

---

## Evaluation

### Upstream Analysis Evaluation

The upstream pipeline evaluates predicted indicators against ROIC.ai gold-standard benchmarks:

- **Gold benchmark**: `data/processed/benchmarks/roic_gold_benchmark_<DATE>.csv`
- **Evaluator**: `openai-agent/experiments/final_report2025/evaluate.py`
- **Eval modes**:
  - `summary` — High-level accuracy summary
  - `folder` — Per-folder detailed metrics
  - `table2` — Detailed per-indicator accuracy tables
- **Output**: `results/validation/{us,eu}/` containing `table1_*_summary.csv` and `table2_*_rows_*.csv`

### NLG Evaluation

The NLG guardrail system provides inline evaluation during generation:
- **Content Ordering guardrail**: Checks fact preservation — no facts dropped or invented
- **Text Structuring guardrail**: Validates XML tag structure and completeness
- **Surface Realization guardrail**: Returns structured JSON with verdicts on report identity, naming discipline, linguistic quality, factuality, analytical quality, numerical discipline

---

## Key Design Decisions

1. **System prompts avoid template variables**: LangChain's `ChatPromptTemplate` treats `{variable}` in system messages as required inputs. All data-specific values are passed through the user-level `input_prompt` instead.

2. **SafeFormatDict for graceful degradation**: Missing template keys pass through as `{key}` rather than raising `KeyError`.

3. **Guardrail routing advances on stuck workers**: If a worker hits its attempt limit but later stages haven't run, the orchestrator advances to the next stage rather than terminating early.

4. **Document-level generation**: Prompts explicitly require institutional-grade, comprehensive reports — not brief summaries. Every indicator must be verbalized.

5. **Temporal continuity**: Previous month's report is fed as context to maintain narrative consistency across the 14-month series.

6. **Bilingual support**: English and Irish variants of all generation prompts (SR, unified worker, e2e). Language selected via `--language` flag.

7. **Ablation-ready architecture**: Workflow variants (no_guardrail, no_finalizer, no_orchestrator, single_module) are built into `workflow.py` for systematic research comparison.

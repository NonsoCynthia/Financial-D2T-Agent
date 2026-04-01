#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./run.sh [options]

Main options:
  --region us|eu                     Data/DB region (default: us)
  --run-eu-pipeline                  Run scripts_eu pipeline before EU agent/workflow run
  --analysis-only                    Run agent/workflow only; skip evaluation
  --eval-only                        Run evaluation only; skip agent/workflow

Run options (passed to run_one_ticker.py):
  --mode agent|workflow|both         Default: both
  --model MODEL                      Default: gpt-5-mini
  --ticker TICKER                    Single ticker
  --tickers A,B,C                    Comma-separated tickers
  --all-tickers                      Use all tickers from final_report2025 config
                                     (for --region eu, this resolves to default EU tickers)
  --n-times N                        Default: 1
  --max-turns N                      Default: 30
  --reasoning low|medium|high        Compatibility flag; analyst reasoning is fixed to medium
  --verbosity low|medium|high        Default: medium
  --reflection | --no-reflection     Default: no-reflection
  --mcp | --no-mcp                   Agent MCP toggle
  --write-folder PATH                Results root (default by region)
  --analysis-start-date YYYY-MM-DD   Override analysis start date
  --analysis-end-date YYYY-MM-DD     Override analysis end date
  --inter-run-sleep-seconds N        Pause between runs (default from code: 10)

Evaluation options:
  --eval-mode summary|folder|table2|none   Default: summary
  --results-root PATH                 Used by summary mode
  --pred-folder PATH                  Used by folder/table2 mode
  --gold-csv PATH                     Used by summary/folder mode
  --gold-benchmark-csv PATH           Table2 gold benchmark CSV (long/wide format)
  --gold-date-match exact|on_or_before
  --gold-source-priority S1,S2,S3
  --gold-fixed-date YYYY-MM-DD
  --ratio-clip-quantile Q
  --out-csv PATH
  --summary-out-csv PATH              Used by table2 mode
  --build-roic-gold | --no-build-roic-gold
  --cap-analysis-to-gold | --no-cap-analysis-to-gold
  --roic-dumps-dir PATH               Input folder for ROIC JSON dumps
  --roic-gold-report-json PATH        Quality report written when auto-building
  --roic-source-name NAME             Source label in auto-built CSV (default: roic.ai)

Notes:
  - Reasoning policy is enforced in code: analyst=medium, manager=high.
  - With --mode both and eval mode folder/table2, this script evaluates both
    workflow and agent folders by default.
  - If --pred-folder is provided, folder/table2 evaluation runs only once using it.
  - In table2 mode, if --gold-benchmark-csv is not provided, the script defaults
    to data[_eu]/processed/benchmarks/roic_gold_benchmark_<today>.csv and can build it
    from ROIC dumps automatically when missing.
  - In table2 mode, analysis end date is capped to the gold max date by default.
    Disable with --no-cap-analysis-to-gold.

Examples:
  ./run.sh --region us --mode both --all-tickers --n-times 1
  ./run.sh --region eu --run-eu-pipeline --mode both --tickers A5G.IR,ASML.AS
  ./run.sh --region us --mode workflow --ticker AAPL --analysis-only --n-times 1 --max-turns 8 --analysis-start-date 2025-12-01 --analysis-end-date 2025-12-31 --inter-run-sleep-seconds 0
  ./run.sh --eval-only --eval-mode summary --results-root /abs/results
  ./run.sh --eval-only --eval-mode table2 --pred-folder /abs/preds --gold-benchmark-csv /abs/gold.csv --ratio-clip-quantile 0.01
  ./run.sh --eval-only --eval-mode table2 --pred-folder /abs/preds --build-roic-gold --roic-dumps-dir /abs/roic_json_dumps_monthly_last_year
USAGE
}

# Thin wrapper around `run_pipeline.py`.
# This prefers the conda env in `CONDA_ENV` and falls back to plain `python`.
#
# Examples:
#   ./run.sh --help
#   ./run.sh --region us --mode workflow --ticker AAPL --analysis-only --n-times 1 --max-turns 8
#   ./run.sh --region eu --run-eu-pipeline --mode both --tickers A5G.IR,ASML.AS
#   ./run.sh --eval-only --eval-mode summary --results-root "$PWD/results/final_report2025_us"
#   ./run.sh --eval-only --eval-mode table2 --pred-folder "$PWD/results/final_report2025_us/gpt-5-mini/workflow_False"
#
# Full CLI parsing and defaults live in `run_pipeline.py`.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_PY="${PROJECT_ROOT}/run_pipeline.py"
CONDA_ENV_NAME="${CONDA_ENV:-finance}"
CONDA_BIN="${CONDA_BIN:-/home/chinonso/anaconda3/bin/conda}"

cd "${PROJECT_ROOT}"

for arg in "$@"; do
  case "${arg}" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

if [[ "${CONDA_DEFAULT_ENV:-}" == "${CONDA_ENV_NAME}" ]]; then
  exec python "${RUNNER_PY}" "$@"
fi

if command -v conda >/dev/null 2>&1; then
  exec conda run --no-capture-output -n "${CONDA_ENV_NAME}" python "${RUNNER_PY}" "$@"
fi

if [[ -x "${CONDA_BIN}" ]]; then
  exec "${CONDA_BIN}" run --no-capture-output -n "${CONDA_ENV_NAME}" python "${RUNNER_PY}" "$@"
fi

exec python "${RUNNER_PY}" "$@"

# Quick references:
# - Reasoning policy is enforced in code: analyst=medium, manager=high.
# - Use --analysis-only for generation and --eval-only for evaluation-only mode.
# - For a fast single-month run:
# ./run.sh \
#   --region us \
#   --mode workflow \
#   --ticker TSLA \
#   --analysis-only \
#   --n-times 1 \
#   --max-turns 8 \
#   --analysis-start-date 2025-01-01 \
#   --analysis-end-date 2026-02-25 \
#   --inter-run-sleep-seconds 0

# EU
# ./run.sh \
#   --region eu \
#   --mode workflow \
#   --ticker ASML.AS \
#   --analysis-only \
#   --n-times 1 \
#   --max-turns 8 \
#   --analysis-start-date 2025-01-01 \
#   --analysis-end-date 2026-02-26 \
#   --inter-run-sleep-seconds 0

# Table 1 (run from repo root: Financial-D2T-Agent)
# ./run.sh \
#   --region us \
#   --eval-only \
#   --eval-mode summary \
#   --results-root "$PWD/results/final_report2025_us" \
#   --out-csv "$PWD/results/validation/us/table1_us_summary.csv"

# ./run.sh \
#   --region eu \
#   --eval-only \
#   --eval-mode summary \
#   --results-root "$PWD/results/final_report2025_eu" \
#   --out-csv "$PWD/results/validation/eu/table1_eu_summary.csv"


# Table 2
# ./run.sh \
#   --region us \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --gold-benchmark-csv "$PWD/data/processed/benchmarks/roic_gold_benchmark_2026-02-25.csv" \
#   --out-csv "$PWD/results/validation/us/table2_us_rows_workflow_false.csv" \
#   --summary-out-csv "$PWD/results/validation/us/table2_us_summary_workflow_false.csv"

# ./run.sh \
#   --region us \
#   --model gpt-5 \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --reflection \
#   --gold-benchmark-csv "$PWD/data/processed/benchmarks/roic_gold_benchmark_2026-02-25.csv"

# ./run.sh \
#   --region us \
#   --model gpt-5-mini \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --reflection \
#   --gold-benchmark-csv "$PWD/data/processed/benchmarks/roic_gold_benchmark_2026-02-25.csv"

# ./run.sh \
#   --region us \
#   --model gpt-5-mini \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --no-reflection \
#   --gold-benchmark-csv "$PWD/data/processed/benchmarks/roic_gold_benchmark_2026-02-25.csv"

# ./run.sh \
#   --region us \
#   --model gpt-5 \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --no-reflection \
#   --gold-benchmark-csv "$PWD/data/processed/benchmarks/roic_gold_benchmark_2026-02-25.csv"


# ./run.sh \
#   --region eu \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --gold-benchmark-csv "$PWD/data_eu/processed/benchmarks/roic_gold_benchmark_2026-02-26.csv" \
#   --out-csv "$PWD/results/validation/eu/table2_eu_rows_workflow_false.csv" \
#   --summary-out-csv "$PWD/results/validation/eu/table2_eu_summary_workflow_false.csv"

# Add --reflection to evaluate *_True folders and get *_true.csv suffixes.
# ./run.sh --region us --mode workflow --all-tickers --no-reflection --analysis-only --n-times 1 --model gpt-5
# ./run.sh --region us --mode workflow --all-tickers --reflection --analysis-only --n-times 1 --model gpt-5


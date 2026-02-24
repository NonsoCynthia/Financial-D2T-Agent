#!/usr/bin/env bash
set -euo pipefail

# Wrapper for run_one_ticker.py with sensible defaults.
#
# Examples:
#   ./run.sh --ticker TSLA
#   ./run.sh --mode agent --model gpt-5-mini --tickers TSLA,AAPL
#   ./run.sh --mode both --all-tickers --n-times 1 --reflection
# ./run.sh --mode agent --model gpt-5-mini --tickers TSLA,AAPL 
# ./run.sh --mode agent --model gpt-5-mini --tickers TSLA,AAPL --reflection
# ./run.sh --mode workflow --model gpt-5-mini --tickers TSLA,AAPL
# ./run.sh --mode workflow --model gpt-5-mini --tickers TSLA,AAPL --reflection


# Optional env override:
#   CONDA_ENV=finance ./run.sh --ticker TSLA

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

CONDA_ENV="${CONDA_ENV:-finance}"

if [[ $# -eq 0 ]]; then
  set -- --ticker TSLA --max-turns 20 --n-times 12
fi

source /home/chinonso/anaconda3/etc/profile.d/conda.sh
conda activate "${CONDA_ENV}"

# python -u "${SCRIPT_DIR}/run_one_ticker.py" "$@"


# python \
# /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/openai-agent/experiments/final_report2025/evaluate.py \
# --mode summary \
# --results-root /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/results/final_report2025_us_test_one_ticker \
# --gold-csv /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/data/processed/panel/daily_panel_prices_returns_fundamentals.csv \
# --out-csv /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/results/final_report2025_us_test_one_ticker/summary_indicator.csv \
# --manager-out-csv /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/results/final_report2025_us_test_one_ticker/summary_manager.csv

python \
/home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/openai-agent/experiments/final_report2025/evaluate.py \
--mode folder \
--pred-folder /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/results/final_report2025_us_test_one_ticker/gpt-5-mini/workflow_False \
--gold-csv /home/chinonso/PHD_PROJECTS/Financial-D2T-Agent/data/processed/panel/daily_panel_prices_returns_fundamentals.csv


# python scripts/run_gold_benchmark.py \
#   --ticker TSLA \
#   --asof 2026-02-23 \
#   --pred data/predictions/TSLA_2026-02-23.json \
#   --roic_apikey YOUR_ROIC_KEY \
#   --google_exchange NASDAQ
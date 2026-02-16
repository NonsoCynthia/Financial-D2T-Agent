#!/usr/bin/env bash
set -euo pipefail
source /home/chinonso/anaconda3/etc/profile.d/conda.sh
conda activate finance

MODEL="gpt-5-mini"
INDICATOR_REASONING="medium"
MANAGER_REASONING="high"
REASONING="high"
MAX_MONTHS="12"
TEST_START="2025-01-02"
TEST_END="2025-12-31"
# TICKERS="TSLA,AMZN,NIO,MSFT,AAPL,GOOG,NFLX,COIN"  # change to a single ticker for quick tests
TICKERS="TSLA"
SERVER_PATH="finAgents/server_us_finance.py"
AGENT_OUT="results/experiments/monthly_agent_workflow"
# WORKFLOW_OUT="results/experiments/monthly_normal_workflow"
WORKFLOW_OUT="results/experiments/monthly_workflow"

# # Agent pipeline (tools + managers)
# python run_agent_monthly.py \
#   --tickers "$TICKERS" \
#   --max_months "$MAX_MONTHS" \
#   --model "$MODEL" \
#   --reasoning_effort "$REASONING" \
#   --test_start "$TEST_START" \
#   --test_end "$TEST_END" \
#   --server_path "$SERVER_PATH" \
#   --out_dir "$AGENT_OUT"

# # Workflow baseline (preferred)
# python run_workflow_monthly.py \
#   --tickers "$TICKERS" \
#   --model "$MODEL" \
#   --indicator_reasoning_effort "$INDICATOR_REASONING" \
#   --manager_reasoning_effort "$MANAGER_REASONING" \
#   --strict_paper \
#   --use_code_interpreter \
#   --max_months "$MAX_MONTHS" \
#   --lookback_months 12 \
#   --test_start "$TEST_START" \
#   --test_end "$TEST_END" \
#   --out_dir "$WORKFLOW_OUT"

# # Evaluate agent outputs — uncomment to run
# python run_eval_monthly.py \
#   --mode agent \
#   --pred_dir "$AGENT_OUT" \
#   --gold_csv data/processed/panel/monthly_panel_prices_returns_fundamentals.csv

# # Evaluate workflow outputs — uncomment to run
# python run_eval_monthly.py \
#   --mode workflow \
#   --pred_dir "$WORKFLOW_OUT" \
#   --gold_csv data/processed/panel/monthly_panel_prices_returns_fundamentals.csv

# Optional Yahoo spot-check for one month (paper-style single-month validation)
python scripts/07_yahoo_gold_spotcheck.py \
  --pred_dir "$WORKFLOW_OUT" \
  --mode workflow \
  --month "2025-04" \
  --tolerance 0.30


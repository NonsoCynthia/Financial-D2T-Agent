#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# run_nlg.sh — Multi-Stock Monthly Report Generator (M-SMRG) NLG Runner
# ---------------------------------------------------------------------------
# Thin wrapper around run_nlg.py.
# Run from the repo root or call this file directly.
# ---------------------------------------------------------------------------

show_help() {
cat <<'HELP'
================================================================================
  run_nlg.sh — Multi-Stock Monthly Report Generator (M-SMRG) NLG Runner
================================================================================

DESCRIPTION
  Generate comprehensive, document-level monthly equity reports from structured
  financial data using multi-agent NLG pipelines or single-shot (e2e) generation.

  Allowed data sources:
    results/final_report2025_us   (US equities, default)
    results/final_report2025_eu   (EU equities)

USAGE
  ./run_nlg.sh [OPTIONS] <TARGET>

TARGETS (exactly one required)
  --list-samples                 List available generation samples and exit
  --sample-id ID                 Run a specific sample by numeric ID
  --analysis-date YYYY-MM-DD     Run the sample for a specific analysis month
  --sequence                     Run all loaded samples in chronological order,
                                 auto-chaining each month's output as context
                                 for the next

WORKFLOW (--workflow)
  default          Multi-agent pipeline: orchestrator -> content ordering ->
                   text structuring -> surface realization -> guardrail ->
                   finalizer.  Produces the most thorough output.  (default)
  unified_worker   Same orchestrator flow but a single unified worker handles
                   all three stages.
  no_orchestrator_no_guardrail_no_finalizer
                   Fixed CO -> TS -> SR worker chain only. No orchestrator,
                   guardrail, or finalizer.
  no_orchestrator_no_finalizer
                   Fixed CO -> guardrail -> TS -> guardrail -> SR -> guardrail.
                   No orchestrator or finalizer.
  no_guardrail_no_finalizer
                   Orchestrator plus specialized workers, but no guardrail or
                   finalizer.
  e2e              Single LLM call — no multi-agent stages.  Fastest, but
                   less structured.

COMMON OPTIONS
  --source-model MODEL       Upstream analysis model folder to read from.
                              This selects the source results folder, e.g.
                              results/final_report2025_us/gpt-5-mini.
                              It is not the NLG generation model.
  --model MODEL              NLG model used for generation (default: gpt-5).
                              Override to use a smaller/cheaper model, e.g.
                              --model gpt-5-mini or --model gpt-5.1
  --provider PROVIDER        LLM provider (default: openai).
                              Choices: openai, ollama, anthropic, groq, hf,
                              huggingface, aixplain
  --language LANG            Output language (default: en).
                              Choices: en, ga (Irish/Gaeilge)
  --temperature FLOAT        Sampling temperature (default: 0.0)

DATA & FILTERING
  --dataset-path PATH        Path to the upstream analysis results directory.
                              Default: results/final_report2025_us
  --dataset-kind KIND        Dataset format (default: auto).
                              Choices: auto, financial_multi_stock_monthly
  --source-arch ARCH         Upstream analysis branch type (default: workflow).
                              Choices: workflow, agent
  --source-reflection        Use the reflection-enabled upstream branch
  --source-no-reflection     Use the non-reflection upstream branch (default)
  --tickers TICKERS          Comma-separated ticker filter, e.g. AAPL,TSLA,NVDA
  --ticker TICKER            Target ticker when using --analysis-date
  --start-date DATE          Filter samples starting from this date
  --end-date DATE            Filter samples up to this date
  --max-months-per-ticker N  Max months to include per ticker
  --min-stocks-per-month N   Min stocks required per month bundle (default: 1)
  --previous-reports-path P  Path to directory/file with previous NLG reports
                              for continuity context

OUTPUT & SAVING
  --output-dir DIR           Override the default output directory
  --save-prefix PREFIX       Custom filename prefix for saved outputs
  --no-save                  Do not save results to disk
  --catalog-limit N          Limit --list-samples output to N rows (0 = all)

RUNTIME
  --max-iteration N          Max LangGraph recursion limit (default: 100)
  --model-kwargs-json JSON   Extra JSON kwargs for e2e model runs
  --python PATH              Python interpreter to use (default: python)

EXAMPLES

  # List all available samples for the gpt-5-mini source model
  ./run_nlg.sh --list-samples --source-model gpt-5-mini

  # Run the default multi-agent pipeline for January 2025
  ./run_nlg.sh --workflow default --source-model gpt-5-mini \
               --analysis-date 2025-01-31

  # Same month, but use gpt-5-mini as the NLG model too
  ./run_nlg.sh --workflow default --source-model gpt-5-mini \
               --analysis-date 2025-01-31 --model gpt-5-mini

  # Single-shot (e2e) generation for January 2025
  ./run_nlg.sh --workflow e2e --source-model gpt-5-mini \
               --analysis-date 2025-01-31

  # Keep the gpt-5-mini source dataset, but generate with gpt-5.1
  ./run_nlg.sh --workflow e2e --source-model gpt-5-mini \
               --model gpt-5.1 --analysis-date 2025-01-31

  # Run all months in sequence (auto-chains previous month output)
  ./run_nlg.sh --workflow default --source-model gpt-5-mini --sequence

  # E2E sequence across all months
  ./run_nlg.sh --workflow e2e --source-model gpt-5 --sequence

  # EU dataset, specific sample ID
  ./run_nlg.sh --dataset-path results/final_report2025_eu \
               --source-model gpt-5-mini --sample-id 3

  # Use a custom Python interpreter
  ./run_nlg.sh --python python3 --source-model gpt-5-mini \
               --sample-id 1 --save-prefix my_run

NOTES
  - When running a single month, the script auto-discovers the previous
    month's NLG output in the output directory for continuity context.
  - When using --sequence, each month's output is automatically fed as
    context to the next month.
  - Output files (JSON + TXT) are saved under:
      results/nlg/<region>/<source-model>/<branch>/<provider>/<nlg-model>/
                  <language>/<workflow>/
================================================================================
HELP
}

# ---------------------------------------------------------------------------
# Setup: resolve project root and defaults
# ---------------------------------------------------------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"     # override with: ./run_nlg.sh --python python3 ...
RUNNER_PY="${PROJECT_ROOT}/run_nlg.py"  # the Python entry point this script wraps
ARGS=()

# ---------------------------------------------------------------------------
# Check for -h / --help before anything else
# ---------------------------------------------------------------------------
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      show_help
      exit 0
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Parse shell-level flags (--python); everything else is forwarded to Python
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      # Use a specific Python interpreter, e.g. --python python3.11
      [[ $# -ge 2 ]] || { echo "--python requires a value" >&2; exit 2; }
      PYTHON_BIN="$2"
      shift 2
      ;;
    *)
      # Collect all other args to pass through to run_nlg.py
      ARGS+=("$1")
      shift
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Run the NLG pipeline
# ---------------------------------------------------------------------------
# Ensure the project root is on PYTHONPATH so imports resolve correctly
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
cd "${PROJECT_ROOT}"

# Hand off to the Python runner with all collected arguments
exec "${PYTHON_BIN}" "${RUNNER_PY}" "${ARGS[@]}"

# ===========================================================================
# QUICK REFERENCE — copy-paste commands
# ===========================================================================
#
# --- Step 1: See what samples are available ---
#   ./run_nlg.sh --list-samples --source-model gpt-5-mini
#   ./run_nlg.sh --list-samples --source-model gpt-5-mini --catalog-limit 5
#
# --- Step 2a: Run a SINGLE month (default multi-agent pipeline) ---
# ./run_nlg.sh --workflow default --source-model gpt-5 --analysis-date 2025-01-31
# ./run_nlg.sh --workflow default --source-model gpt-5 --source-no-reflection --analysis-date 2025-01-31
# ./run_nlg.sh --workflow default --source-model gpt-5 --source-reflection --analysis-date 2025-01-31 
#
# --- Step 2b: Run a SINGLE month (e2e single-shot generation) ---
#   ./run_nlg.sh --workflow e2e --source-model gpt-5-mini --analysis-date 2025-01-31
#
# --- Step 2c: Run ablation studies ---
#   ./run_nlg.sh --workflow no_orchestrator_no_guardrail_no_finalizer --source-model gpt-5 --source-reflection --analysis-date 2025-01-31
#   ./run_nlg.sh --workflow no_orchestrator_no_finalizer --source-model gpt-5 --source-reflection --sequence --analysis-date 2025-01-31
#   ./run_nlg.sh --workflow no_guardrail_no_finalizer --source-model gpt-5 --source-reflection --sequence --analysis-date 2025-01-31
#
# --- Step 3: Run ALL months in sequence (previous month auto-chains) ---
# ./run_nlg.sh --workflow default --source-model gpt-5 --sequence
# ./run_nlg.sh --workflow default --source-model gpt-5 --sequence --source-reflection
# ./run_nlg.sh --workflow e2e --source-model gpt-5 --sequence --source-reflection
#
# --- Use a different NLG model (e.g. gpt-5-mini instead of gpt-5) ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini \
#                --analysis-date 2025-01-31 --model gpt-5-mini
#
# --- Use a different provider ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini \
#                --analysis-date 2025-01-31 --provider anthropic
#
# --- EU dataset ---
#   ./run_nlg.sh --dataset-path results/final_report2025_eu \
#                --source-model gpt-5-mini --list-samples
#
# --- Run by sample ID ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini --sample-id 1
#
# --- Filter to specific tickers ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini \
#                --sequence --tickers AAPL,TSLA,GOOG
#
# --- Custom output directory and save prefix ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini \
#                --analysis-date 2025-01-31 --output-dir my_results \
#                --save-prefix experiment_v2
#
# --- Generate report in Irish (Gaeilge) ---
#   ./run_nlg.sh --workflow default --source-model gpt-5-mini \
#                --analysis-date 2025-01-31 --language ga
# ===========================================================================
# Completed 14 sequence step(s).
# - 2025-01-31: multi_stock_2025-01-31
# - 2025-02-28: multi_stock_2025-02-28
# - 2025-03-31: multi_stock_2025-03-31
# - 2025-04-30: multi_stock_2025-04-30
# - 2025-05-31: multi_stock_2025-05-31
# - 2025-06-30: multi_stock_2025-06-30
# - 2025-07-31: multi_stock_2025-07-31
# - 2025-08-31: multi_stock_2025-08-31
# - 2025-09-30: multi_stock_2025-09-30
# - 2025-10-31: multi_stock_2025-10-31
# - 2025-11-30: multi_stock_2025-11-30
# - 2025-12-31: multi_stock_2025-12-31
# - 2026-01-31: multi_stock_2026-01-31
# - 2026-02-25: multi_stock_2026-02-25
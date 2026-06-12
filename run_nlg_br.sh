#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# run_nlg_br.sh — Brazilian Portuguese NLG Runner
# ---------------------------------------------------------------------------
# Thin wrapper around run_nlg_brazilian_manager.py.
# Run from the repo root or call this file directly.
# ---------------------------------------------------------------------------

show_help() {
cat <<'HELP'
================================================================================
  run_nlg_br.sh — Brazilian Portuguese Financial NLG Runner
================================================================================

DESCRIPTION
  Generate Brazilian Portuguese monthly equity reports from AIDA-BR manager
  results. Each generated report uses a 12-month investment horizon unless the
  manager decision text explicitly states otherwise. The expected source dataset is:

    data_br/completo_gpt5mini

  Each ticker/date bundle may include:
    YYYY-MM-DD_analyst_0.json
    YYYY-MM-DD_manager_0.json
    YYYY-MM-DD_material_facts_0.txt

USAGE
  ./run_nlg_br.sh [OPTIONS] <TARGET>

TARGETS (exactly one required)
  --list-samples                 List available Brazilian monthly samples
  --sample-id ID                 Run one sample by numeric ID
  --analysis-date YYYY-MM-DD     Run one Brazilian analysis date
  --sequence                     Run all loaded samples in chronological order

WORKFLOW (--workflow)
  default          Multi-agent pipeline: orchestrator -> content ordering ->
                   text structuring -> surface realization -> guardrail ->
                   finalizer. (default)
  unified_worker   Orchestrator flow with a single unified worker.
  no_orchestrator_no_guardrail_no_finalizer
                   Fixed CO -> TS -> SR worker chain only.
  no_orchestrator_no_finalizer
                   Fixed CO -> guardrail -> TS -> guardrail -> SR -> guardrail.
  no_guardrail_no_finalizer
                   Orchestrator plus specialized workers, no guardrail/finalizer.
  e2e              Single LLM call, no multi-agent stages.

COMMON OPTIONS
  --dataset-path PATH        Brazilian source dataset path.
                              Default: data_br/completo_gpt5mini
  --model MODEL              NLG model used for generation (default: gpt-5)
  --provider PROVIDER        LLM provider (default: openai)
  --temperature FLOAT        Sampling temperature (default: 0.0)
  --reasoning-effort LEVEL   Reasoning effort for supported OpenAI models
                              (default: high)

DATA & FILTERING
  --tickers TICKERS          Comma-separated ticker filter, e.g.
                              ALUP11,PETR4,VALE3
  --start-date DATE          Filter samples starting from this date
  --end-date DATE            Filter samples up to this date
  --min-stocks-per-month N   Min stocks required per month bundle (default: 1)

OUTPUT & SAVING
  --output-dir DIR           Output root (default: results/nlg_brazilian_manager)
  --save-prefix PREFIX       Custom filename prefix for saved outputs
  --no-save                  Do not save results to disk
  --catalog-limit N          Limit --list-samples output to N rows (0 = all)

RUNTIME
  --model-kwargs-json JSON   Extra JSON kwargs for e2e model runs
  --python PATH              Python interpreter to use (default: python)

EXAMPLES

  # List available Brazilian samples
  ./run_nlg_br.sh --list-samples --catalog-limit 5

  # Run the default multi-agent pipeline for December 2025
  ./run_nlg_br.sh --workflow default --analysis-date 2025-12-01

  # Single-shot e2e generation
  ./run_nlg_br.sh --workflow e2e --analysis-date 2025-12-01

  # Run all comparable NLG architectures for one month
  for wf in default unified_worker no_orchestrator_no_guardrail_no_finalizer \
            no_orchestrator_no_finalizer no_guardrail_no_finalizer e2e; do
    ./run_nlg_br.sh --workflow "$wf" --analysis-date 2025-12-01
  done

  # Use an external checkout of the AIDA-BR repository
  ./run_nlg_br.sh \
    --dataset-path /path/to/workflow-vs-agent-fundamentals-br/results_v2/completo_gpt5mini \
    --workflow default \
    --analysis-date 2025-12-01

NOTES
  - Outputs are saved under:
      results/nlg_brazilian_manager/<workflow>/
  - When using --sequence, existing saved reports in that workflow directory
    are skipped and reused as continuity context for the next month. This lets
    you resume any partial run without regenerating completed samples.
  - The Brazilian source folder does not need workflow_False/workflow_True.
    Comparability comes from running the same NLG architectures over the same
    fixed Brazilian source dataset.
================================================================================
HELP
}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
RUNNER_PY="${PROJECT_ROOT}/pipeline/run_nlg_brazilian_manager.py"
ARGS=()

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      show_help
      exit 0
      ;;
  esac
done

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      [[ $# -ge 2 ]] || { echo "--python requires a value" >&2; exit 2; }
      PYTHON_BIN="$2"
      shift 2
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

export PYTHONPATH="${PROJECT_ROOT}/pipeline:${PROJECT_ROOT}/evaluation:${PROJECT_ROOT}:${PYTHONPATH:-}"
cd "${PROJECT_ROOT}"

exec "${PYTHON_BIN}" "${RUNNER_PY}" "${ARGS[@]}"

# ===========================================================================
# QUICK REFERENCE — copy-paste commands
# ===========================================================================
#
# --- See available Brazilian samples ---
#   ./run_nlg_br.sh --list-samples --catalog-limit 5
#
# --- Local default dataset path ---
#   The AIDA-BR input has been copied into this project at:
#     data_br/completo_gpt5mini
#   Because this is the default, you can run without --dataset-path:
#     ./run_nlg_br.sh --list-samples --catalog-limit 3
#
# --- If the AIDA-BR repo is outside this project, pass its results_v2 path ---
#   ./run_nlg_br.sh \
#     --dataset-path /path/to/workflow-vs-agent-fundamentals-br/results_v2/completo_gpt5mini \
#     --list-samples --catalog-limit 5
#
# --- Test the FIRST month with a single-shot e2e report ---
#   ./run_nlg_br.sh --sample-id 1 --workflow e2e --provider openai
#
# --- Read the first-month e2e text output ---
#   less results/nlg_brazilian_manager/e2e/pt_br_manager_report_br_manager_2024-01-02.txt
#
# --- Test the FIRST month with the full multi-agent pipeline ---
#   ./run_nlg_br.sh --sample-id 1 --workflow default --provider openai
#
# --- Read the first-month default multi-agent text output ---
#   less results/nlg_brazilian_manager/default/pt_br_manager_report_br_manager_2024-01-02.txt
#
# --- Run a specific analysis date ---
#   ./run_nlg_br.sh --analysis-date 2025-12-01 --workflow default --provider openai
#
# --- Run all Brazilian months in sequence ---
#   ./run_nlg_br.sh --sequence --workflow default --provider openai
#
# --- Resume all Brazilian months for each comparable NLG architecture ---
#   for wf in default unified_worker no_orchestrator_no_guardrail_no_finalizer \
#             no_orchestrator_no_finalizer no_guardrail_no_finalizer e2e; do
#     ./run_nlg_br.sh --sequence --workflow "$wf" --provider openai
#   done
#
# --- Run all comparable NLG architectures for one month ---
#   for wf in default unified_worker no_orchestrator_no_guardrail_no_finalizer \
#             no_orchestrator_no_finalizer no_guardrail_no_finalizer e2e; do
#     ./run_nlg_br.sh --workflow "$wf" --analysis-date 2025-12-01 --provider openai
#   done
#
# --- Use a different NLG model for e2e ---
#   ./run_nlg_br.sh --sample-id 1 --workflow e2e --provider openai --model gpt-5-mini
#
# --- Note on multi-agent model defaults ---
#   The default multi-agent pipeline uses the per-agent overrides in main.py:
#     orchestrator=gpt-5.4-mini
#     content ordering=gpt-5
#     text structuring=gpt-5
#     surface realization=gpt-5
#     guardrail=gpt-5
#     finalizer=gpt-5-mini
# ===========================================================================

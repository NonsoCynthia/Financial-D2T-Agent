#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# run_llm_as_judge.sh — Controlled LLM-as-judge runner
# ---------------------------------------------------------------------------
# Thin wrapper around run_multimodel_judge.py.
# Run from the repo root or call this file directly.
# ---------------------------------------------------------------------------

show_help() {
cat <<'HELP'
================================================================================
  run_llm_as_judge.sh — Controlled LLM-as-judge Runner
================================================================================

DESCRIPTION
  Run automated LLM-as-judge scoring for generated NLG reports with controls for
  judge model, workflow, reflection branch, date, sample name, or one JSON file.

USAGE
  ./run_llm_as_judge.sh [OPTIONS]

COMMON OPTIONS
  --judge NAME                  Judge to run. May be repeated.
                                Choices: gpt5, claude_haiku_45, gemini_25
                                Default: all configured judges
  --collection NAME             Output collection to judge.
                                Choices: nlg, nlg_brazilian_manager, all
                                Default: nlg
  --source-reflection VALUE     true, false, or all
                                Default: all. Applies to nlg only.
  --workflow NAME               Workflow to run. May be repeated.
                                Examples: default, default_old, e2e,
                                          no_guardrail_no_finalizer,
                                          no_orchestrator_no_finalizer
  --analysis-date YYYY-MM-DD    Run only one analysis date.
  --sample-name NAME            Run only one sample name.
  --json-file PATH              Run only one generated-report JSON file.
  --list-files                  List matching generated-report JSON files only.
  --overwrite                   Overwrite cached outputs written by this runner.
  --missing-only                Run only missing judge/sample JSONs.
                                Preserves existing judgments and skips summaries.
  --skip-summaries              Run judgments without rebuilding summary CSVs.
  --fresh-gpt5                  Re-run GPT-5 instead of loading existing GPT-5
                                judge results from results/validation/llm_judge.
  --max-retries N               Retry attempts per judge/sample.
  --aggregate-only              Rebuild paper tables from cached judge JSONs.
                                Does not call any LLM API.
  --python PATH                 Python interpreter to use.
                                Default: python

EXAMPLES
  # Show files available under the default/reflection condition
  ./run_llm_as_judge.sh --list-files --source-reflection true --workflow default

  # Run one English generated-report file
  ./run_llm_as_judge.sh \
    --json-file results/nlg/final_report2025_us/gpt-5/workflow_True/openai/gpt-5/en/default/default_gpt-5_workflow_True_gpt-5_multi_stock_2025-01-31.json

  # Run one date for one workflow
  ./run_llm_as_judge.sh --collection nlg --source-reflection true --workflow e2e --analysis-date 2025-01-31

  # Run US workflow_True for default and e2e
  ./run_llm_as_judge.sh --collection nlg --source-reflection true --workflow default --workflow e2e

  # Run US workflow_False for default_old
  ./run_llm_as_judge.sh --collection nlg --source-reflection false --workflow default_old

  # Run Brazilian manager outputs
  ./run_llm_as_judge.sh --collection nlg_brazilian_manager --workflow default --workflow e2e

  # Run one Brazilian manager month
  ./run_llm_as_judge.sh --collection nlg_brazilian_manager --workflow default --analysis-date 2024-01-02

  # Run only Claude Haiku on default and e2e
  ./run_llm_as_judge.sh --judge claude_haiku_45 --workflow default --workflow e2e

  # Use the finance conda environment interpreter directly
  ./run_llm_as_judge.sh --python /home/chinonso/anaconda3/envs/finance/bin/python --list-files

  # Recompute paper-ready tables from all cached judgments
  ./run_llm_as_judge.sh --aggregate-only

  # Complete every missing judgment without replacing existing files
  ./run_llm_as_judge.sh --missing-only --collection all

  # Regenerate every judgment fresh, but do not compute paper summaries
  ./run_llm_as_judge.sh --collection all --fresh-gpt5 --overwrite --skip-summaries

NOTES
  - This wrapper currently targets run_multimodel_judge.py.
  - The Python runner writes outputs under:
      results/validation/llm_judge_multimodel/
================================================================================
HELP
}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
RUNNER_PY="${PROJECT_ROOT}/run_multimodel_judge.py"
ARGS=()
AGGREGATE_ONLY=false

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
    --aggregate-only)
      AGGREGATE_ONLY=true
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
cd "${PROJECT_ROOT}"

if [[ "${AGGREGATE_ONLY}" == true ]]; then
  if [[ ${#ARGS[@]} -gt 0 ]]; then
    echo "--aggregate-only cannot be combined with judge-run options" >&2
    exit 2
  fi
  exec "${PYTHON_BIN}" "${PROJECT_ROOT}/aggregate_llm_judge_results.py"
fi

exec "${PYTHON_BIN}" "${RUNNER_PY}" "${ARGS[@]}"

# ===========================================================================
# QUICK REFERENCE
# ===========================================================================
#
# Preview US workflow_True default/e2e files:
# ./run_llm_as_judge.sh --list-files --collection nlg --source-reflection true --workflow default --workflow e2e
#
# Preview US workflow_False default/e2e files:
# ./run_llm_as_judge.sh --list-files --collection nlg --source-reflection false --workflow default --workflow e2e
#
# Run all US workflow_True default/e2e files with all three judges:
# ./run_llm_as_judge.sh --judge gpt5 --judge gemini_25 --judge claude_haiku_45 --collection nlg --source-reflection true --workflow default --workflow e2e --fresh-gpt5 --overwrite
#
# Run one US month:
# ./run_llm_as_judge.sh --judge gpt5 --judge gemini_25 --judge claude_haiku_45 --collection nlg --source-reflection true --workflow default --workflow e2e --analysis-date 2025-01-31 --fresh-gpt5 --overwrite
#
# Run US default_old:
# ./run_llm_as_judge.sh --collection nlg --source-reflection true --workflow default_old
#
# Run US ablation workflows:
# ./run_llm_as_judge.sh --collection nlg --source-reflection true --workflow no_guardrail_no_finalizer --workflow no_orchestrator_no_finalizer
#
# Preview Brazilian manager files:
# ./run_llm_as_judge.sh --list-files --collection nlg_brazilian_manager --workflow default --workflow e2e
#
# Run all Brazilian manager default/e2e files:
# ./run_llm_as_judge.sh --judge gpt5 --judge gemini_25 --judge claude_haiku_45 --collection nlg_brazilian_manager --workflow default --workflow e2e --fresh-gpt5 --overwrite
#
# Run one Brazilian manager month:
# ./run_llm_as_judge.sh --judge gpt5 --judge gemini_25 --judge claude_haiku_45 --collection nlg_brazilian_manager --workflow default --workflow e2e --analysis-date 2024-01-02
#
# Run one JSON file:
# ./run_llm_as_judge.sh --json-file path/to/generated_report.json
#
# Recompute all paper tables from cached results without calling an LLM:
# ./run_llm_as_judge.sh --aggregate-only
#
# Run every currently missing judgment, preserving cached results:
# ./run_llm_as_judge.sh --missing-only --collection all
#
# Regenerate all judgments from the APIs without computing summaries:
# ./run_llm_as_judge.sh --collection all --fresh-gpt5 --overwrite --skip-summaries
#
# Current output roots:
# results/validation/llm_judge_multimodel/<judge>/workflow_True/openai/gpt-5/en/<workflow>/
# results/validation/llm_judge_multimodel/<judge>/workflow_False/openai/gpt-5/en/<workflow>/
# results/validation/llm_judge_multimodel/<judge>/nlg_brazilian_manager/pt_br/<workflow>/

# ./run_llm_as_judge.sh --aggregate-only

# rm Financial-D2T-Agent/llm-as-judge.ipynb
# rm Financial-D2T-Agent/llm-as-judge-multi-model-robustness.ipynb
# rm Financial-D2T-Agent/new_llm-as-judge-multi-model-robustness.ipynb
# rm Financial-D2T-Agent/newer_llm_as_judge.ipynb
# rm -rf Financial-D2T-Agent/results/validation/llm_judge_old
# rm -rf Financial-D2T-Agent/results/validation/llm_judge_multi_model
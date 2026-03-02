#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${PROJECT_ROOT}/openai-agent"
RUN_ONE="${AGENT_DIR}/run_one_ticker.py"
EVAL_PY="${AGENT_DIR}/experiments/final_report2025/evaluate.py"
ROIC_GOLD_BUILDER="${PROJECT_ROOT}/scripts/09_build_roic_gold_benchmark.py"
EU_PIPELINE="${PROJECT_ROOT}/scripts_eu/run_eu_pipeline.py"

CONDA_ENV="${CONDA_ENV:-finance}"

REGION="us"                  # us | eu
RUN_ANALYSIS=1
RUN_EVAL=1
RUN_EU_PIPELINE=0

MODE="both"                  # agent | workflow | both
MODEL="gpt-5-mini"
N_TIMES=1
MAX_TURNS=30
REASONING="medium"           # compatibility only; analyst reasoning is always medium
VERBOSITY="medium"           # low | medium | high
REFLECTION=0
MCP_FLAG=""                  # --mcp | --no-mcp | ""

WRITE_FOLDER=""
RESULTS_ROOT=""
PRED_FOLDER=""
VALIDATION_REGION_DIR=""

EVAL_MODE="summary"          # summary | folder | table2 | none
GOLD_CSV=""
GOLD_BENCHMARK_CSV=""
GOLD_DATE_MATCH="${GOLD_DATE_MATCH:-exact}"
GOLD_SOURCE_PRIORITY="${GOLD_SOURCE_PRIORITY:-roic.ai,gurufocus,yahoo,gold}"
GOLD_FIXED_DATE="${GOLD_FIXED_DATE:-}"
RATIO_CLIP_QUANTILE="${RATIO_CLIP_QUANTILE:-0.0}"
OUT_CSV=""
SUMMARY_OUT_CSV=""
OUT_CSV_SET_BY_USER=0
SUMMARY_OUT_CSV_SET_BY_USER=0
AUTO_BUILD_ROIC_GOLD=1
AUTO_CAP_ANALYSIS_TO_GOLD=1
ROIC_DUMPS_DIR=""
ROIC_GOLD_REPORT_JSON=""
ROIC_SOURCE_NAME="roic.ai"
ANALYSIS_START_DATE_OVERRIDE=""
ANALYSIS_END_DATE_OVERRIDE=""
INTER_RUN_SLEEP_SECONDS_OVERRIDE=""
# US_TICKERS = "TSLA,AMZN,NIO,MSFT,AAPL,GOOG,NFLX,COIN"

declare -a TICKER_ARGS=()
EU_DEFAULT_TICKERS="KRZ.IR,A5G.IR,BIRG.IR,ASML.AS,SAP.DE,MC.PA,NOVO-B.CO,SIE.DE,OR.PA,NESN.SW"
TODAY_YMD="$(date +%F)"

die() {
  echo "$*" >&2
  exit 2
}

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

suffix_path() {
  local path="$1"
  local suffix="$2"

  if [[ -z "${path}" ]]; then
    echo ""
    return
  fi

  local dir base stem ext
  dir="$(dirname "${path}")"
  base="$(basename "${path}")"

  if [[ "${base}" == *.* ]]; then
    stem="${base%.*}"
    ext=".${base##*.}"
  else
    stem="${base}"
    ext=""
  fi

  echo "${dir}/${stem}_${suffix}${ext}"
}

default_pred_folder_for_arch() {
  local arch="$1"
  echo "${WRITE_FOLDER}/${MODEL}/${arch}_${REFLECTION_BOOL}"
}

table2_output_path() {
  local kind="$1"   # rows | summary
  local arch="$2"   # workflow | agent
  local reflection="$3"  # true | false
  echo "${VALIDATION_REGION_DIR}/table2_${REGION}_${kind}_${arch}_${reflection}.csv"
}

run_eval_one() {
  local arch_label="$1"
  local pred_folder="$2"
  local out_csv="$3"
  local summary_out_csv="$4"

  local -a eval_cmd=()
  case "${EVAL_MODE}" in
    folder)
      eval_cmd=(
        python "${EVAL_PY}"
        --mode folder
        --pred-folder "${pred_folder}"
        --gold-csv "${GOLD_CSV}"
      )
      if [[ -n "${out_csv}" ]]; then
        eval_cmd+=(--out-csv "${out_csv}")
      fi
      ;;
    table2)
      eval_cmd=(
        python "${EVAL_PY}"
        --mode table2
        --pred-folder "${pred_folder}"
        --gold-benchmark-csv "${GOLD_BENCHMARK_CSV}"
        --date-match "${GOLD_DATE_MATCH}"
        --source-priority "${GOLD_SOURCE_PRIORITY}"
        --ratio-clip-quantile "${RATIO_CLIP_QUANTILE}"
      )
      if [[ -n "${GOLD_FIXED_DATE}" ]]; then
        eval_cmd+=(--fixed-date "${GOLD_FIXED_DATE}")
      fi
      if [[ -n "${out_csv}" ]]; then
        eval_cmd+=(--out-csv "${out_csv}")
      fi
      if [[ -n "${summary_out_csv}" ]]; then
        eval_cmd+=(--summary-out-csv "${summary_out_csv}")
      fi
      ;;
    *)
      die "Internal error: unsupported per-folder eval mode '${EVAL_MODE}'"
      ;;
  esac

  echo "Evaluating ${arch_label}: ${pred_folder}"
  echo "+ ${eval_cmd[*]}"
  "${eval_cmd[@]}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)
      REGION="$2"
      shift 2
      ;;
    --run-eu-pipeline)
      RUN_EU_PIPELINE=1
      shift
      ;;
    --analysis-only)
      RUN_EVAL=0
      shift
      ;;
    --eval-only)
      RUN_ANALYSIS=0
      shift
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --ticker|--tickers)
      TICKER_ARGS+=("$1" "$2")
      shift 2
      ;;
    --all-tickers)
      TICKER_ARGS+=("$1")
      shift
      ;;
    --n-times)
      N_TIMES="$2"
      shift 2
      ;;
    --max-turns)
      MAX_TURNS="$2"
      shift 2
      ;;
    --reasoning)
      REASONING="$2"
      shift 2
      ;;
    --verbosity)
      VERBOSITY="$2"
      shift 2
      ;;
    --reflection)
      REFLECTION=1
      shift
      ;;
    --no-reflection)
      REFLECTION=0
      shift
      ;;
    --mcp)
      MCP_FLAG="--mcp"
      shift
      ;;
    --no-mcp)
      MCP_FLAG="--no-mcp"
      shift
      ;;
    --write-folder)
      WRITE_FOLDER="$2"
      shift 2
      ;;
    --analysis-start-date)
      ANALYSIS_START_DATE_OVERRIDE="$2"
      shift 2
      ;;
    --analysis-end-date)
      ANALYSIS_END_DATE_OVERRIDE="$2"
      shift 2
      ;;
    --inter-run-sleep-seconds)
      INTER_RUN_SLEEP_SECONDS_OVERRIDE="$2"
      shift 2
      ;;
    --eval-mode)
      EVAL_MODE="$2"
      shift 2
      ;;
    --results-root)
      RESULTS_ROOT="$2"
      shift 2
      ;;
    --pred-folder)
      PRED_FOLDER="$2"
      shift 2
      ;;
    --gold-csv)
      GOLD_CSV="$2"
      shift 2
      ;;
    --gold-benchmark-csv)
      GOLD_BENCHMARK_CSV="$2"
      shift 2
      ;;
    --gold-date-match)
      GOLD_DATE_MATCH="$2"
      shift 2
      ;;
    --gold-source-priority)
      GOLD_SOURCE_PRIORITY="$2"
      shift 2
      ;;
    --gold-fixed-date)
      GOLD_FIXED_DATE="$2"
      shift 2
      ;;
    --ratio-clip-quantile)
      RATIO_CLIP_QUANTILE="$2"
      shift 2
      ;;
    --out-csv)
      OUT_CSV="$2"
      OUT_CSV_SET_BY_USER=1
      shift 2
      ;;
    --summary-out-csv)
      SUMMARY_OUT_CSV="$2"
      SUMMARY_OUT_CSV_SET_BY_USER=1
      shift 2
      ;;
    --build-roic-gold)
      AUTO_BUILD_ROIC_GOLD=1
      shift
      ;;
    --no-build-roic-gold)
      AUTO_BUILD_ROIC_GOLD=0
      shift
      ;;
    --cap-analysis-to-gold)
      AUTO_CAP_ANALYSIS_TO_GOLD=1
      shift
      ;;
    --no-cap-analysis-to-gold)
      AUTO_CAP_ANALYSIS_TO_GOLD=0
      shift
      ;;
    --roic-dumps-dir)
      ROIC_DUMPS_DIR="$2"
      shift 2
      ;;
    --roic-gold-report-json)
      ROIC_GOLD_REPORT_JSON="$2"
      shift 2
      ;;
    --roic-source-name)
      ROIC_SOURCE_NAME="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

if [[ "${REGION}" != "us" && "${REGION}" != "eu" ]]; then
  die "--region must be us or eu"
fi

if [[ "${MODE}" != "agent" && "${MODE}" != "workflow" && "${MODE}" != "both" ]]; then
  die "--mode must be one of: agent, workflow, both"
fi

if [[ "${EVAL_MODE}" != "summary" && "${EVAL_MODE}" != "folder" && "${EVAL_MODE}" != "table2" && "${EVAL_MODE}" != "none" ]]; then
  die "--eval-mode must be one of: summary, folder, table2, none"
fi

if [[ "${RUN_ANALYSIS}" -eq 0 && "${RUN_EVAL}" -eq 0 ]]; then
  echo "Nothing to do: both analysis and evaluation are disabled."
  exit 0
fi

REFLECTION_BOOL="False"
if [[ "${REFLECTION}" -eq 1 ]]; then
  REFLECTION_BOOL="True"
fi
REFLECTION_BOOL_LOWER="$(printf '%s' "${REFLECTION_BOOL}" | tr '[:upper:]' '[:lower:]')"

if [[ -z "${WRITE_FOLDER}" ]]; then
  if [[ "${REGION}" == "eu" ]]; then
    WRITE_FOLDER="${PROJECT_ROOT}/results/final_report2025_eu"
  else
    WRITE_FOLDER="${PROJECT_ROOT}/results/final_report2025_us"
  fi
fi

if [[ -z "${GOLD_CSV}" ]]; then
  if [[ "${REGION}" == "eu" ]]; then
    GOLD_CSV="${PROJECT_ROOT}/data_eu/processed/panel/daily_panel_prices_returns_fundamentals.csv"
  else
    GOLD_CSV="${PROJECT_ROOT}/data/processed/panel/daily_panel_prices_returns_fundamentals.csv"
  fi
fi

VALIDATION_REGION_DIR="${PROJECT_ROOT}/results/validation/${REGION}"
if [[ -z "${OUT_CSV}" ]]; then
  case "${EVAL_MODE}" in
    summary)
      OUT_CSV="${VALIDATION_REGION_DIR}/table1_${REGION}_summary.csv"
      ;;
    folder)
      OUT_CSV="${VALIDATION_REGION_DIR}/folder_${REGION}_rows.csv"
      ;;
    table2)
      if [[ "${MODE}" == "workflow" || "${MODE}" == "agent" ]]; then
        OUT_CSV="$(table2_output_path "rows" "${MODE}" "${REFLECTION_BOOL_LOWER}")"
      fi
      ;;
  esac
fi
if [[ "${EVAL_MODE}" == "table2" && -z "${SUMMARY_OUT_CSV}" ]]; then
  if [[ "${MODE}" == "workflow" || "${MODE}" == "agent" ]]; then
    SUMMARY_OUT_CSV="$(table2_output_path "summary" "${MODE}" "${REFLECTION_BOOL_LOWER}")"
  fi
fi

if [[ "${EVAL_MODE}" == "table2" && ( "${RUN_EVAL}" -eq 1 || ( "${RUN_ANALYSIS}" -eq 1 && "${AUTO_CAP_ANALYSIS_TO_GOLD}" -eq 1 ) ) ]]; then
  if [[ -z "${GOLD_BENCHMARK_CSV}" ]]; then
    if [[ "${REGION}" == "eu" ]]; then
      GOLD_BENCHMARK_CSV="${PROJECT_ROOT}/data_eu/processed/benchmarks/roic_gold_benchmark_${TODAY_YMD}.csv"
    else
      GOLD_BENCHMARK_CSV="${PROJECT_ROOT}/data/processed/benchmarks/roic_gold_benchmark_${TODAY_YMD}.csv"
    fi
  fi

  if [[ "${AUTO_BUILD_ROIC_GOLD}" -eq 1 && ! -f "${GOLD_BENCHMARK_CSV}" ]]; then
    if [[ -z "${ROIC_DUMPS_DIR}" ]]; then
      if [[ "${REGION}" == "eu" ]]; then
        roic_primary_dir="${PROJECT_ROOT}/data_eu/roic_json_dumps_monthly_last_year"
        roic_fallback_base="${PROJECT_ROOT}/data_eu"
      else
        roic_primary_dir="${PROJECT_ROOT}/data/roic_json_dumps_monthly_last_year"
        roic_fallback_base="${PROJECT_ROOT}/data"
      fi

      if [[ -d "${roic_primary_dir}" ]]; then
        ROIC_DUMPS_DIR="${roic_primary_dir}"
      else
        latest_roic_dump_dir="$(find "${roic_fallback_base}" -maxdepth 1 -type d -name 'roic_*_json_dumps' | sort | tail -n 1)"
        if [[ -n "${latest_roic_dump_dir}" ]]; then
          ROIC_DUMPS_DIR="${latest_roic_dump_dir}"
        fi
      fi
    fi

    if [[ -z "${ROIC_GOLD_REPORT_JSON}" ]]; then
      if [[ "${GOLD_BENCHMARK_CSV}" == *.csv ]]; then
        ROIC_GOLD_REPORT_JSON="${GOLD_BENCHMARK_CSV%.csv}_report.json"
      else
        ROIC_GOLD_REPORT_JSON="${GOLD_BENCHMARK_CSV}_report.json"
      fi
    fi

    if [[ ! -d "${ROIC_DUMPS_DIR}" ]]; then
      die "Table2 gold CSV not found and no ROIC dump directory was found. Provide --gold-benchmark-csv or --roic-dumps-dir."
    fi
  fi
fi

if [[ "${REGION}" == "eu" ]]; then
  if [[ "${#TICKER_ARGS[@]}" -eq 0 ]]; then
    TICKER_ARGS=(--tickers "${EU_DEFAULT_TICKERS}")
  else
    for i in "${!TICKER_ARGS[@]}"; do
      if [[ "${TICKER_ARGS[$i]}" == "--all-tickers" ]]; then
        TICKER_ARGS=(--tickers "${EU_DEFAULT_TICKERS}")
        break
      fi
    done
  fi
fi

if [[ "${REGION}" == "eu" ]]; then
  export US_DB_PATH="${PROJECT_ROOT}/data_eu/processed/mcp/fundamental_analysis.db"
else
  unset US_DB_PATH || true
fi

cd "${AGENT_DIR}"
if [[ -f "/home/chinonso/anaconda3/etc/profile.d/conda.sh" ]]; then
  source /home/chinonso/anaconda3/etc/profile.d/conda.sh
elif command -v conda >/dev/null 2>&1; then
  eval "$(conda shell.bash hook)"
else
  die "Unable to initialize conda. Ensure conda is installed and available."
fi
conda activate "${CONDA_ENV}"

if [[ "${EVAL_MODE}" == "table2" && "${AUTO_BUILD_ROIC_GOLD}" -eq 1 && ! -f "${GOLD_BENCHMARK_CSV}" && ( "${RUN_EVAL}" -eq 1 || ( "${RUN_ANALYSIS}" -eq 1 && "${AUTO_CAP_ANALYSIS_TO_GOLD}" -eq 1 ) ) ]]; then
  build_gold_cmd=(
    python "${ROIC_GOLD_BUILDER}"
    --in-dir "${ROIC_DUMPS_DIR}"
    --out-csv "${GOLD_BENCHMARK_CSV}"
    --report-json "${ROIC_GOLD_REPORT_JSON}"
    --source-name "${ROIC_SOURCE_NAME}"
  )
  echo "+ ${build_gold_cmd[*]}"
  "${build_gold_cmd[@]}"
fi

if [[ "${EVAL_MODE}" == "table2" && ! -f "${GOLD_BENCHMARK_CSV}" && ( "${RUN_EVAL}" -eq 1 || ( "${RUN_ANALYSIS}" -eq 1 && "${AUTO_CAP_ANALYSIS_TO_GOLD}" -eq 1 ) ) ]]; then
  die "Table2 gold benchmark CSV not found: ${GOLD_BENCHMARK_CSV}"
fi

if [[ -n "${ANALYSIS_START_DATE_OVERRIDE}" ]]; then
  export ANALYSIS_START_DATE="${ANALYSIS_START_DATE_OVERRIDE}"
  echo "Using ANALYSIS_START_DATE=${ANALYSIS_START_DATE}"
fi
if [[ -n "${ANALYSIS_END_DATE_OVERRIDE}" ]]; then
  export ANALYSIS_END_DATE="${ANALYSIS_END_DATE_OVERRIDE}"
  echo "Using ANALYSIS_END_DATE=${ANALYSIS_END_DATE}"
fi

if [[ "${RUN_ANALYSIS}" -eq 1 && "${EVAL_MODE}" == "table2" && "${AUTO_CAP_ANALYSIS_TO_GOLD}" -eq 1 && -f "${GOLD_BENCHMARK_CSV}" ]]; then
  gold_max_date="$(
    python - "$GOLD_BENCHMARK_CSV" <<'PY'
import sys
import pandas as pd

p = sys.argv[1]
df = pd.read_csv(p, low_memory=False)
norm = {str(c).strip().lower().replace(" ", "").replace("_", ""): c for c in df.columns}
date_col = None
for cand in ("date", "asofdate", "asof", "as_of_date", "targetdate"):
    key = cand.replace("_", "")
    if key in norm:
        date_col = norm[key]
        break
if date_col is None:
    print("")
    raise SystemExit(0)
dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
if dates.empty:
    print("")
    raise SystemExit(0)
print(dates.max().strftime("%Y-%m-%d"))
PY
  )"

  if [[ -n "${gold_max_date}" ]]; then
    current_end="${ANALYSIS_END_DATE:-${TODAY_YMD}}"
    if [[ "${current_end}" > "${gold_max_date}" ]]; then
      export ANALYSIS_END_DATE="${gold_max_date}"
      echo "Capping ANALYSIS_END_DATE to gold max date: ${ANALYSIS_END_DATE}"
    elif [[ -z "${ANALYSIS_END_DATE:-}" ]]; then
      export ANALYSIS_END_DATE="${gold_max_date}"
      echo "Setting ANALYSIS_END_DATE to gold max date: ${ANALYSIS_END_DATE}"
    fi
  else
    echo "Warning: unable to derive max date from ${GOLD_BENCHMARK_CSV}; skipping analysis-date cap."
  fi
fi

if [[ -n "${INTER_RUN_SLEEP_SECONDS_OVERRIDE}" ]]; then
  export INTER_RUN_SLEEP_SECONDS="${INTER_RUN_SLEEP_SECONDS_OVERRIDE}"
  echo "Using INTER_RUN_SLEEP_SECONDS=${INTER_RUN_SLEEP_SECONDS}"
fi

analysis_start_display="${ANALYSIS_START_DATE:-2025-01-01}"
analysis_end_display="${ANALYSIS_END_DATE:-${TODAY_YMD}}"
inter_run_sleep_display="${INTER_RUN_SLEEP_SECONDS:-10}"
echo "Run plan: region=${REGION} mode=${MODE} model=${MODEL} n_times=${N_TIMES} max_turns=${MAX_TURNS} eval_mode=${EVAL_MODE}"
echo "Run plan: analysis_window=${analysis_start_display}..${analysis_end_display} inter_run_sleep_seconds=${inter_run_sleep_display} reflection=${REFLECTION_BOOL}"
echo "Run plan: reasoning_policy=analyst:medium manager:high"
if [[ "${EVAL_MODE}" == "table2" ]]; then
  echo "Run plan: table2_ratio_clip_quantile=${RATIO_CLIP_QUANTILE}"
  echo "Run plan: table2_gold_benchmark_csv=${GOLD_BENCHMARK_CSV}"
  if [[ "${MODE}" == "both" ]]; then
    echo "Run plan: table2_out_csv=per-arch auto naming (table2_${REGION}_{rows|summary}_{workflow|agent}_${REFLECTION_BOOL_LOWER}.csv)"
  else
    echo "Run plan: table2_out_csv=${OUT_CSV}"
    echo "Run plan: table2_summary_out_csv=${SUMMARY_OUT_CSV}"
  fi
  echo "Run plan: table2_auto_build_roic_gold=${AUTO_BUILD_ROIC_GOLD}"
  echo "Run plan: table2_cap_analysis_to_gold=${AUTO_CAP_ANALYSIS_TO_GOLD}"
  if [[ -n "${ROIC_DUMPS_DIR}" ]]; then
    echo "Run plan: table2_roic_dumps_dir=${ROIC_DUMPS_DIR}"
  fi
fi
if [[ "${#TICKER_ARGS[@]}" -gt 0 ]]; then
  echo "Run plan: ticker_args=${TICKER_ARGS[*]}"
fi

if [[ "${REGION}" == "eu" && "${RUN_EU_PIPELINE}" -eq 1 ]]; then
  echo "+ python ${EU_PIPELINE} --all"
  python "${EU_PIPELINE}" --all
fi

if [[ "${RUN_ANALYSIS}" -eq 1 ]]; then
  if [[ "${#TICKER_ARGS[@]}" -eq 0 ]]; then
    TICKER_ARGS=(--all-tickers)
  fi

  run_cmd=(
    python "${RUN_ONE}"
    "${TICKER_ARGS[@]}"
    --mode "${MODE}"
    --model "${MODEL}"
    --n-times "${N_TIMES}"
    --max-turns "${MAX_TURNS}"
    --reasoning "${REASONING}"
    --verbosity "${VERBOSITY}"
    --write-folder "${WRITE_FOLDER}"
  )

  if [[ "${REFLECTION}" -eq 1 ]]; then
    run_cmd+=(--reflection)
  fi
  if [[ "${REGION}" == "eu" ]]; then
    run_cmd+=(--allow-unknown-tickers)
  fi
  if [[ -n "${MCP_FLAG}" ]]; then
    run_cmd+=("${MCP_FLAG}")
  fi

  echo "+ ${run_cmd[*]}"
  "${run_cmd[@]}"
fi

if [[ "${RUN_EVAL}" -eq 1 && "${EVAL_MODE}" != "none" ]]; then
  if [[ "${EVAL_MODE}" == "summary" ]]; then
    if [[ -z "${RESULTS_ROOT}" ]]; then
      RESULTS_ROOT="${WRITE_FOLDER}"
    fi
    eval_cmd=(
      python "${EVAL_PY}"
      --mode summary
      --results-root "${RESULTS_ROOT}"
      --gold-csv "${GOLD_CSV}"
    )
    if [[ -n "${OUT_CSV}" ]]; then
      eval_cmd+=(--out-csv "${OUT_CSV}")
    fi
    echo "+ ${eval_cmd[*]}"
    "${eval_cmd[@]}"
  else
    if [[ -n "${PRED_FOLDER}" ]]; then
      if [[ ! -d "${PRED_FOLDER}" ]]; then
        die "Prediction folder not found: ${PRED_FOLDER}"
      fi
      run_eval_one "custom" "${PRED_FOLDER}" "${OUT_CSV}" "${SUMMARY_OUT_CSV}"
    else
      if [[ "${MODE}" == "both" ]]; then
        for arch in workflow agent; do
          pred_folder_arch="$(default_pred_folder_for_arch "${arch}")"
          if [[ ! -d "${pred_folder_arch}" ]]; then
            echo "Skipping ${arch} evaluation: folder not found at ${pred_folder_arch}"
            continue
          fi
          if [[ "${EVAL_MODE}" == "table2" ]]; then
            if [[ "${OUT_CSV_SET_BY_USER}" -eq 1 ]]; then
              out_arch="$(suffix_path "${OUT_CSV}" "${arch}_${REFLECTION_BOOL_LOWER}")"
            else
              out_arch="$(table2_output_path "rows" "${arch}" "${REFLECTION_BOOL_LOWER}")"
            fi

            if [[ "${SUMMARY_OUT_CSV_SET_BY_USER}" -eq 1 ]]; then
              summary_arch="$(suffix_path "${SUMMARY_OUT_CSV}" "${arch}_${REFLECTION_BOOL_LOWER}")"
            else
              summary_arch="$(table2_output_path "summary" "${arch}" "${REFLECTION_BOOL_LOWER}")"
            fi
          else
            out_arch="$(suffix_path "${OUT_CSV}" "${arch}")"
            summary_arch="$(suffix_path "${SUMMARY_OUT_CSV}" "${arch}")"
          fi
          run_eval_one "${arch}" "${pred_folder_arch}" "${out_arch}" "${summary_arch}"
        done
      else
        arch="${MODE}"
        pred_folder_arch="$(default_pred_folder_for_arch "${arch}")"
        if [[ ! -d "${pred_folder_arch}" ]]; then
          die "Prediction folder not found: ${pred_folder_arch}"
        fi
        run_eval_one "${arch}" "${pred_folder_arch}" "${OUT_CSV}" "${SUMMARY_OUT_CSV}"
      fi
    fi
  fi
fi

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
#   --region eu \
#   --mode workflow \
#   --eval-only \
#   --eval-mode table2 \
#   --gold-benchmark-csv "$PWD/data_eu/processed/benchmarks/roic_gold_benchmark_2026-02-26.csv" \
#   --out-csv "$PWD/results/validation/eu/table2_eu_rows_workflow_false.csv" \
#   --summary-out-csv "$PWD/results/validation/eu/table2_eu_summary_workflow_false.csv"

# Add --reflection to evaluate *_True folders and get *_true.csv suffixes.

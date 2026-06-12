# Financial-D2T-Agent

A multi-agent data-to-text (D2T) system for generating professional monthly equity research reports from structured financial data. Built as part of PhD research on multi-agent NLG architectures, supporting English (US) and Brazilian Portuguese output.

---

## Project Structure

```
Financial-D2T-Agent/
├── run.sh                  # Upstream stock analysis pipeline
├── run_nlg.sh              # English NLG generation (all workflow variants)
├── run_nlg_br.sh           # Brazilian Portuguese NLG generation
├── run_llm_as_judge.sh     # LLM-as-judge evaluation and paper result generation
│
├── pipeline/               # Core NLG generation scripts
│   ├── config.py           # Tickers, dates, model config
│   ├── load_data.py        # US data loading and prompt context builder
│   ├── load_data_brazilian_manager.py
│   ├── main.py             # Multi-agent graph definition (LangGraph)
│   ├── run_pipeline.py     # Upstream stock analysis entry point
│   ├── run_nlg.py          # English NLG entry point
│   └── run_nlg_brazilian_manager.py
│
├── evaluation/             # LLM-judge evaluation and paper result generation
│   ├── run_multimodel_judge.py         # GPT-5 / Gemini 2.5 Pro / Claude Haiku judge runner
│   ├── run_human_eval.py               # Human evaluation runner
│   ├── generate_annotation_excel.py    # Human evaluation workbook builder
│   ├── aggregate_llm_judge_results.py  # Aggregates cached judge JSON → DataFrames
│   ├── analyze_default_e2e_correlations.py
│   ├── analyze_llm_judge_correlations.py
│   ├── generate_paper_style_metrics_csv.py          # English (reflection=True) + BR paper CSVs
│   ├── generate_workflow_true_five_system_metrics_csv.py  # 5-system ablation paper CSV
│   └── LLM_JUDGE_CORRELATION_METHOD.md
│
├── agents/                 # Agent definitions, prompts, LangGraph workflow
├── scripts/                # US data extraction scripts
├── scripts_eu/             # EU data extraction scripts
├── data/                   # US source data
├── data_br/                # Brazilian Portuguese source data
├── data_eu/                # EU source data
├── results/                # All generated outputs and evaluation results
│   └── validation/llm_judge_multimodel/paper_results/
│       ├── paper_style_default_e2e_reflection_true_metrics.csv
│       ├── paper_style_workflow_true_five_system_metrics.csv
│       └── paper_style_default_e2e_br_metrics.csv
├── notebooks/              # Exploratory notebooks
├── statistics/             # Token and data statistics CSVs
├── docs/                   # Detailed project documentation (project.md)
└── mcp/                    # MCP server definitions
```

---

## Shell Scripts

All four scripts must be run from the project root directory. They automatically set `PYTHONPATH` to include the `pipeline/` and `evaluation/` subdirectories.

### `run.sh` — Upstream Stock Analysis

Runs the financial data extraction and upstream analysis pipeline (price data, SEC fundamentals, signal computation).

```bash
./run.sh
# With a specific conda environment:
CONDA_ENV=myenv ./run.sh
```

### `run_nlg.sh` — English NLG Generation

Generates monthly equity reports in English for a given workflow variant and source model.

```bash
# List available samples
./run_nlg.sh --list-samples --source-model gpt-5-mini

# Run a single sample
./run_nlg.sh --workflow default --source-model gpt-5-mini --sample-id 3 --source-reflection

# Run all samples sequentially
./run_nlg.sh --workflow default --source-model gpt-5-mini --sequence

# Key flags:
#   --workflow         default | e2e | default_old | no_orchestrator_no_finalizer |
#                      no_guardrail_no_finalizer | no_orchestrator_no_guardrail_no_finalizer
#   --source-model     upstream model folder (e.g. gpt-5-mini, gpt-5)
#   --source-reflection / --source-no-reflection
#   --sequence         run all samples one after another (no parallelism)
#   --python PATH      specify Python interpreter
```

### `run_nlg_br.sh` — Brazilian Portuguese NLG Generation

Generates monthly equity reports in Brazilian Portuguese using the manager-only workflow.

```bash
# Run all Brazilian Portuguese samples
./run_nlg_br.sh --workflow default

# With a specific dataset path:
./run_nlg_br.sh --workflow default --dataset-path data_br/my_dataset
```

### `run_llm_as_judge.sh` — LLM-as-Judge Evaluation

Runs three LLM judges (GPT-5, Gemini 2.5 Pro, Claude Haiku 4.5) over generated reports, and generates paper-ready result CSVs.

```bash
# Run the judge on specific conditions
./run_llm_as_judge.sh --collection nlg --source-reflection true --workflow default --workflow e2e

# Run on all workflows across all samples
./run_llm_as_judge.sh --collection nlg --source-reflection true

# Brazilian Portuguese
./run_llm_as_judge.sh --collection nlg_brazilian_manager

# Generate paper CSVs from cached judge results (no new judging)
./run_llm_as_judge.sh --aggregate-only       # English (reflection=True) + Brazilian Portuguese
./run_llm_as_judge.sh --workflow-true-analysis  # 5-system ablation

# List available files without running
./run_llm_as_judge.sh --list-files --collection nlg --source-reflection true

# Key flags:
#   --collection          nlg | nlg_brazilian_manager
#   --source-reflection   true | false | all
#   --workflow            one or more workflow names
#   --analysis-date       YYYY-MM-DD (filter to a specific report date)
#   --aggregate-only      skip judging, regenerate paper CSVs only
#   --workflow-true-analysis  skip judging, regenerate 5-system ablation CSV only
```

---

## Paper Result CSVs

Located in `results/validation/llm_judge_multimodel/paper_results/`:

| File | Content | Research Question |
|---|---|---|
| `paper_style_default_e2e_reflection_true_metrics.csv` | English default vs e2e, source_reflection=True | RQ2 |
| `paper_style_workflow_true_five_system_metrics.csv` | 5-system ablation, English, source_reflection=True | RQ3 |
| `paper_style_default_e2e_br_metrics.csv` | Brazilian Portuguese default vs e2e | RQ2/RQ4 |

Each file contains three metric types:
- `system_ranking` — ensemble mean, SD, Wilcoxon p-value, CLD group letter per system
- `iaa_item_pearson` — pairwise inter-judge Pearson r across report items
- `iaa_system_pearson` — pairwise inter-judge Pearson r across system means

The 5-system file additionally contains:
- `system_pairwise_wilcoxon` — all 10 pairwise Wilcoxon tests with Holm-Bonferroni correction

---

## Requirements

```bash
pip install -r requirements.txt
```

A `.env` file in the home directory or project root is required for API keys:
```
OPENAI_API_KEY=...
ANTHROPIC_API_KEY=...
GOOGLE_API_KEY=...
```

---

## Further Documentation

- [`docs/project.md`](docs/project.md) — full architecture, data flow, prompt design, CLI reference
- [`evaluation/LLM_JUDGE_CORRELATION_METHOD.md`](evaluation/LLM_JUDGE_CORRELATION_METHOD.md) — inter-judge agreement methodology

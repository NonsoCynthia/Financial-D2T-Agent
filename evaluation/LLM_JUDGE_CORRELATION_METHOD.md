# Default versus E2E Correlation Analysis

## Research Question

The analysis measures whether `default` and `e2e` produce similar evaluation
scores for the same report sample. English and Brazilian Portuguese are
analysed separately.

This is a workflow correlation analysis. It is not a correlation between the
three LLM judges.

## Pairing

Each observation pairs:

- the `default` score for one sample; and
- the `e2e` score for that same sample.

English observations are also matched on `source_reflection`, preventing a
reflection-enabled report from being paired with a reflection-disabled report.
This gives 28 English pairs per evaluator: 14 samples with reflection disabled
and 14 samples with reflection enabled.

Brazilian Portuguese has no reflection branch and gives 24 pairs per evaluator.

Results are calculated separately for:

- GPT-5;
- Gemini 2.5 Pro;
- Claude Haiku 4.5; and
- a three-judge ensemble, calculated by averaging the three judge scores for
  each sample and workflow before pairing.

## Statistical Analysis

For each evaluator and evaluation dimension, the analysis calculates:

- Pearson correlation between paired `default` and `e2e` scores;
- Spearman rank correlation between paired scores;
- two-sided p-values; and
- a paired, two-sided Wilcoxon signed-rank test comparing score distributions.

The Wilcoxon test uses the Pratt treatment of zero differences. A correlation
is reported as `not estimable` if either workflow has zero score variance.

## Significance Letters

There are only two workflows, so only `A` and `B` are required:

- values sharing `A` are not significantly different;
- values marked `A` and `B` differ at p < 0.05;
- the workflow with the higher mean receives `A` when the difference is
  significant.

## Command

```bash
./run_llm_as_judge.sh --correlation-analysis
```

Or run the Python file directly:

```bash
python generate_paper_style_metrics_csv.py
```

## Outputs

Outputs are saved under:

```text
results/validation/llm_judge_multimodel/paper_results/
```

- `paper_style_default_e2e_metrics.csv`: system rankings, significance groups,
  and Pearson inter-judge agreement metrics without raw reports, prompts,
  justifications, or local filesystem paths.

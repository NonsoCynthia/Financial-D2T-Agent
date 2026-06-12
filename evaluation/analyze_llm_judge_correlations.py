#!/usr/bin/env python3
"""Create reproducible, paper-ready inter-judge analysis.

The script requires one score from each configured judge for every report. It:

1. pairs judges on the same collection, workflow, report, and analysis date;
2. calculates two-sided Pearson and Spearman correlations;
3. compares paired Likert scores with two-sided Wilcoxon signed-rank tests;
4. applies Holm correction to the three judge comparisons within each
   corpus/dimension;
5. constructs compact A/B/C significance groups; and
6. writes CSV, Markdown, HTML, and Word outputs.

Run from the project root:
    python analyze_llm_judge_correlations.py
"""

from __future__ import annotations

from itertools import combinations, product
import html
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pandas as pd
from scipy import stats

import aggregate_llm_judge_results as aggregate


OUTPUT_DIR = aggregate.PAPER_DIR
DIMENSIONS = aggregate.SCORE_COLUMNS + ["mean_score"]
DIMENSION_LABELS = {
    "no_omissions_score": "No-Omissions",
    "no_additions_score": "No-Additions",
    "grammaticality_score": "Grammaticality",
    "coherence_score": "Coherence",
    "fluency_score": "Fluency",
    "mean_score": "Overall",
}
JUDGE_LABELS = {
    "gpt5": "GPT-5",
    "gemini_25": "Gemini 2.5 Pro",
    "claude_haiku_45": "Claude Haiku 4.5",
}
JUDGE_ORDER = list(JUDGE_LABELS)
PAIR_ORDER = list(combinations(JUDGE_ORDER, 2))


def corpus_groups(results: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("English", results[results["collection"] == "nlg"]),
        (
            "Brazilian Portuguese",
            results[results["collection"] == "nlg_brazilian_manager"],
        ),
        ("Combined", results),
    ]


def paired_scores(group: pd.DataFrame, dimension: str) -> pd.DataFrame:
    index = [
        "collection", "source_reflection", "workflow", "sample_name", "analysis_date"
    ]
    wide = group.pivot(index=index, columns="judge", values=dimension)
    return wide[JUDGE_ORDER].dropna()


def correlation_table(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for corpus, group in corpus_groups(results):
        for dimension in DIMENSIONS:
            wide = paired_scores(group, dimension)
            for judge_a, judge_b in PAIR_ORDER:
                x = wide[judge_a]
                y = wide[judge_b]
                pearson = stats.pearsonr(x, y)
                spearman = stats.spearmanr(x, y)
                rows.append(
                    {
                        "corpus": corpus,
                        "dimension": DIMENSION_LABELS[dimension],
                        "judge_a": JUDGE_LABELS[judge_a],
                        "judge_b": JUDGE_LABELS[judge_b],
                        "n": len(wide),
                        "pearson_r": pearson.statistic,
                        "pearson_p": pearson.pvalue,
                        "spearman_rho": spearman.statistic,
                        "spearman_p": spearman.pvalue,
                    }
                )
    return pd.DataFrame(rows)


def holm_adjust(p_values: dict[tuple[str, str], float]) -> dict[tuple[str, str], float]:
    ordered = sorted(p_values, key=p_values.get)
    adjusted: dict[tuple[str, str], float] = {}
    running_max = 0.0
    total = len(ordered)
    for rank, pair in enumerate(ordered):
        value = min(1.0, (total - rank) * p_values[pair])
        running_max = max(running_max, value)
        adjusted[pair] = running_max
    return adjusted


def compact_letters(
    means: dict[str, float],
    significant: dict[tuple[str, str], bool],
) -> dict[str, str]:
    """Find a minimal A/B/C display satisfying pairwise significance decisions."""
    letters = ("A", "B", "C")
    subsets = [
        frozenset(letters[index] for index in range(3) if mask & (1 << index))
        for mask in range(1, 8)
    ]
    ranked = sorted(means, key=lambda judge_name: (-means[judge_name], judge_name))
    best: tuple[tuple[int, int, tuple[str, ...]], dict[str, str]] | None = None

    for assignment in product(subsets, repeat=len(ranked)):
        mapping = dict(zip(ranked, assignment))
        valid = True
        for judge_a, judge_b in combinations(ranked, 2):
            pair = tuple(sorted((judge_a, judge_b)))
            share_letter = bool(mapping[judge_a] & mapping[judge_b])
            if significant[pair] == share_letter:
                valid = False
                break
        if not valid:
            continue

        used = set().union(*assignment)
        rendered = {
            judge_name: "".join(letter for letter in letters if letter in mapping[judge_name])
            for judge_name in ranked
        }
        objective = (
            len(used),
            sum(len(value) for value in assignment),
            tuple(rendered[judge_name] for judge_name in ranked),
        )
        if best is None or objective < best[0]:
            best = objective, rendered

    if best is None:
        raise RuntimeError("Could not construct compact letter display")

    # Relabel letters by the highest-ranked judge carrying each letter.
    rendered = best[1]
    ordered_letters = []
    for judge_name in ranked:
        for letter in rendered[judge_name]:
            if letter not in ordered_letters:
                ordered_letters.append(letter)
    relabel = {old: letters[index] for index, old in enumerate(ordered_letters)}
    return {
        judge_name: "".join(relabel[letter] for letter in rendered[judge_name])
        for judge_name in ranked
    }


def significance_table(results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows = []
    test_rows = []
    for corpus, group in corpus_groups(results):
        for dimension in DIMENSIONS:
            wide = paired_scores(group, dimension)
            means = {judge_name: float(wide[judge_name].mean()) for judge_name in JUDGE_ORDER}
            p_values = {}
            raw_tests = {}
            for judge_a, judge_b in PAIR_ORDER:
                differences = wide[judge_a] - wide[judge_b]
                if np.allclose(differences, 0):
                    statistic, p_value = 0.0, 1.0
                else:
                    test = stats.wilcoxon(
                        wide[judge_a],
                        wide[judge_b],
                        alternative="two-sided",
                        zero_method="pratt",
                    )
                    statistic, p_value = test.statistic, test.pvalue
                pair = tuple(sorted((judge_a, judge_b)))
                p_values[pair] = float(p_value)
                raw_tests[pair] = float(statistic)

            adjusted = holm_adjust(p_values)
            significant = {pair: p < 0.05 for pair, p in adjusted.items()}
            letters = compact_letters(means, significant)

            row = {
                "corpus": corpus,
                "dimension": DIMENSION_LABELS[dimension],
                "n": len(wide),
            }
            for judge_name in JUDGE_ORDER:
                mean = means[judge_name]
                sd = float(wide[judge_name].std(ddof=1))
                row[f"{judge_name}_mean"] = mean
                row[f"{judge_name}_sd"] = sd
                row[f"{judge_name}_group"] = letters[judge_name]
                row[JUDGE_LABELS[judge_name]] = (
                    f"{mean:.3f} +/- {sd:.3f} {letters[judge_name]}"
                )
            summary_rows.append(row)

            for judge_a, judge_b in PAIR_ORDER:
                pair = tuple(sorted((judge_a, judge_b)))
                test_rows.append(
                    {
                        "corpus": corpus,
                        "dimension": DIMENSION_LABELS[dimension],
                        "judge_a": JUDGE_LABELS[judge_a],
                        "judge_b": JUDGE_LABELS[judge_b],
                        "n": len(wide),
                        "wilcoxon_statistic": raw_tests[pair],
                        "p_raw": p_values[pair],
                        "p_holm": adjusted[pair],
                        "significant_0.05": significant[pair],
                    }
                )
    return pd.DataFrame(summary_rows), pd.DataFrame(test_rows)


def p_text(p_value: float) -> str:
    if pd.isna(p_value):
        return "not estimable"
    return "<0.001" if p_value < 0.001 else f"{p_value:.3f}"


def correlation_text(coefficient: float, p_value: float) -> str:
    if pd.isna(coefficient):
        return "not estimable"
    return f"{coefficient:.3f} ({p_text(p_value)})"


def write_main_paper_table(
    correlations: pd.DataFrame,
    significance: pd.DataFrame,
) -> None:
    overall_correlations = correlations[correlations["dimension"] == "Overall"]
    overall_significance = significance[significance["dimension"] == "Overall"]
    lines = [
        "# Paper Table: Inter-Judge Agreement and Score Differences",
        "",
        "## Overall-score correlations",
        "",
        "| Corpus | Judge pair | N | Pearson r (p) | Spearman rho (p) |",
        "|---|---|---:|---:|---:|",
    ]
    for _, row in overall_correlations.iterrows():
        lines.append(
            f"| {row['corpus']} | {row['judge_a']} vs {row['judge_b']} | "
            f"{int(row['n'])} | "
            f"{correlation_text(row['pearson_r'], row['pearson_p'])} | "
            f"{correlation_text(row['spearman_rho'], row['spearman_p'])} |"
        )

    lines.extend(
        [
            "",
            "## Overall judge scores",
            "",
            "| Corpus | N | GPT-5 | Gemini 2.5 Pro | Claude Haiku 4.5 |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for _, row in overall_significance.iterrows():
        lines.append(
            f"| {row['corpus']} | {int(row['n'])} | {row['GPT-5']} | "
            f"{row['Gemini 2.5 Pro']} | {row['Claude Haiku 4.5']} |"
        )
    lines.extend(
        [
            "",
            "Note: Scores are mean +/- SD on the 1-7 Likert scale. Within each "
            "row, values sharing a letter are not significantly different; values "
            "with no letter in common differ at Holm-adjusted p < 0.05 based on "
            "paired Wilcoxon signed-rank tests. Pearson and Spearman p-values are "
            "two-sided. Combined-corpus correlations should be interpreted alongside "
            "the language-specific results because between-corpus score differences "
            "can influence pooled correlations.",
            "",
        ]
    )
    (OUTPUT_DIR / "PAPER_TABLE_CORRELATIONS.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def html_table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(value)}</th>" for value in headers)
    body = "\n".join(
        "<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def write_document(
    correlations: pd.DataFrame,
    significance: pd.DataFrame,
) -> None:
    """Write an HTML report and convert it to DOCX when LibreOffice is available."""
    overall = correlations[correlations["dimension"] == "Overall"]
    correlation_rows = [
        [
            row["corpus"],
            f"{row['judge_a']} vs {row['judge_b']}",
            str(int(row["n"])),
            correlation_text(row["pearson_r"], row["pearson_p"]),
            correlation_text(row["spearman_rho"], row["spearman_p"]),
        ]
        for _, row in overall.iterrows()
    ]
    score_rows = [
        [
            row["corpus"],
            str(int(row["n"])),
            row["GPT-5"],
            row["Gemini 2.5 Pro"],
            row["Claude Haiku 4.5"],
        ]
        for _, row in significance[significance["dimension"] == "Overall"].iterrows()
    ]

    dimension_sections = []
    for corpus in ("English", "Brazilian Portuguese", "Combined"):
        corpus_scores = significance[significance["corpus"] == corpus]
        rows = [
            [
                row["dimension"],
                str(int(row["n"])),
                row["GPT-5"],
                row["Gemini 2.5 Pro"],
                row["Claude Haiku 4.5"],
            ]
            for _, row in corpus_scores.iterrows()
        ]
        dimension_sections.extend(
            [
                f"<h2>{html.escape(corpus)} dimension-level scores</h2>",
                html_table(
                    ["Dimension", "N", "GPT-5", "Gemini 2.5 Pro", "Claude Haiku 4.5"],
                    rows,
                ),
            ]
        )

    document = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>LLM-as-Judge Correlation Analysis</title>
<style>
@page {{ size: A4 landscape; margin: 1.5cm; }}
body {{ font-family: Arial, sans-serif; font-size: 10pt; line-height: 1.35; color: #111; }}
h1 {{ font-size: 18pt; }}
h2 {{ font-size: 13pt; margin-top: 18pt; }}
h3 {{ font-size: 11pt; }}
table {{ border-collapse: collapse; width: 100%; margin: 8pt 0 14pt; }}
th, td {{ border: 1px solid #777; padding: 5pt; vertical-align: top; }}
th {{ background: #e9eef3; font-weight: bold; }}
.note {{ font-size: 9pt; }}
</style>
</head>
<body>
<h1>Inter-Judge Correlation Analysis for LLM-as-Judge Evaluation</h1>
<p><strong>Data:</strong> 522 judgments from three judges, paired across 174 reports:
126 English reports and 48 Brazilian Portuguese reports.</p>

<h2>Statistical methodology</h2>
<p>Pearson's product-moment correlation and Spearman's rank correlation were
calculated for each pair of judges using scores paired on the same collection,
workflow, report, and analysis date. Tests were two-sided. Differences in
judge score distributions were assessed with paired, two-sided Wilcoxon
signed-rank tests using the Pratt treatment of zero differences. The three
pairwise judge comparisons within each corpus and evaluation dimension were
corrected using Holm's procedure at alpha = 0.05.</p>
<p>Compact letter displays summarize significant differences. Values sharing
at least one letter are not significantly different after Holm correction.
Values with no letter in common are significantly different. Correlations are
reported as not estimable when either judge has zero variance.</p>

<h2>Table 1. Overall-score inter-judge correlations</h2>
{html_table(
    ["Corpus", "Judge pair", "N", "Pearson r (p)", "Spearman rho (p)"],
    correlation_rows,
)}

<h2>Table 2. Overall judge scores and significance groups</h2>
{html_table(
    ["Corpus", "N", "GPT-5", "Gemini 2.5 Pro", "Claude Haiku 4.5"],
    score_rows,
)}
<p class="note">Values are mean +/- SD on the 1-7 Likert scale. Combined
correlations should be interpreted with the language-specific results because
between-corpus score differences can influence pooled correlations.</p>

{''.join(dimension_sections)}

<h2>Reproducibility</h2>
<p>Run <code>./run_llm_as_judge.sh --correlation-analysis</code> from the
project root. Source code: <code>analyze_llm_judge_correlations.py</code>.
Machine-readable results are written to
<code>results/validation/llm_judge_multimodel/paper_results/</code>.</p>
</body>
</html>
"""
    html_path = OUTPUT_DIR / "LLM_JUDGE_CORRELATION_ANALYSIS.html"
    html_path.write_text(document, encoding="utf-8")

    office = shutil.which("libreoffice") or shutil.which("soffice")
    if not office:
        print("LibreOffice not found; HTML document generated, DOCX skipped.")
        return
    temporary_directories = [
        "/tmp/llm_judge_libreoffice_profile",
        "/tmp/llm_judge_document_home",
        "/tmp/llm_judge_document_config",
        "/tmp/llm_judge_document_cache",
        "/tmp/llm_judge_document_runtime",
    ]
    for directory in temporary_directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            office,
            "--headless",
            "-env:UserInstallation=file:///tmp/llm_judge_libreoffice_profile",
            "--convert-to",
            "docx:Office Open XML Text",
            "--outdir",
            str(OUTPUT_DIR),
            str(html_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "HOME": "/tmp/llm_judge_document_home",
            "XDG_CONFIG_HOME": "/tmp/llm_judge_document_config",
            "XDG_CACHE_HOME": "/tmp/llm_judge_document_cache",
            "XDG_RUNTIME_DIR": "/tmp/llm_judge_document_runtime",
        },
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "LibreOffice DOCX conversion failed: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )


def write_markdown(correlations: pd.DataFrame, significance: pd.DataFrame) -> None:
    lines = [
        "# Inter-Judge Correlation Analysis",
        "",
        "Pearson r and Spearman rho are calculated from paired report scores. "
        "All tests are two-sided.",
        "",
    ]
    for corpus in ("English", "Brazilian Portuguese", "Combined"):
        lines.extend(
            [
                f"## {corpus}: Correlations",
                "",
                "| Dimension | Judge pair | N | Pearson r (p) | Spearman rho (p) |",
                "|---|---|---:|---:|---:|",
            ]
        )
        subset = correlations[correlations["corpus"] == corpus]
        for _, row in subset.iterrows():
            lines.append(
                f"| {row['dimension']} | {row['judge_a']} vs {row['judge_b']} | "
                f"{int(row['n'])} | "
                f"{correlation_text(row['pearson_r'], row['pearson_p'])} | "
                f"{correlation_text(row['spearman_rho'], row['spearman_p'])} |"
            )
        lines.extend(
            [
                "",
                f"## {corpus}: Judge Scores and Significance Groups",
                "",
                "| Dimension | N | GPT-5 | Gemini 2.5 Pro | Claude Haiku 4.5 |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        subset = significance[significance["corpus"] == corpus]
        for _, row in subset.iterrows():
            lines.append(
                f"| {row['dimension']} | {int(row['n'])} | {row['GPT-5']} | "
                f"{row['Gemini 2.5 Pro']} | {row['Claude Haiku 4.5']} |"
            )
        lines.extend(
            [
                "",
                "Values are mean +/- SD. Within each row, judges sharing a letter "
                "are not significantly different; judges with no letter in common "
                "differ at Holm-adjusted p < 0.05 using paired Wilcoxon tests.",
                "",
            ]
        )
    (OUTPUT_DIR / "CORRELATION_ANALYSIS.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = aggregate.load_cached_results()
    expected = sum(
        len(samples) * len(aggregate.ALL_JUDGES)
        for samples in aggregate.expected_samples().values()
    )
    if len(results) != expected:
        raise RuntimeError(
            f"Expected {expected} judgments, but found {len(results)}. "
            "Complete missing judgments before correlation analysis."
        )

    correlations = correlation_table(results)
    significance, pairwise_tests = significance_table(results)
    correlations.to_csv(OUTPUT_DIR / "correlations_pearson_spearman.csv", index=False)
    significance.to_csv(OUTPUT_DIR / "judge_significance_groups.csv", index=False)
    pairwise_tests.to_csv(OUTPUT_DIR / "judge_pairwise_wilcoxon.csv", index=False)
    write_markdown(correlations, significance)
    write_main_paper_table(correlations, significance)
    write_document(correlations, significance)

    print(f"Judgments analysed: {len(results)}")
    print(f"Paired reports: {len(results) // len(aggregate.ALL_JUDGES)}")
    print(f"Saved: {OUTPUT_DIR / 'correlations_pearson_spearman.csv'}")
    print(f"Saved: {OUTPUT_DIR / 'judge_significance_groups.csv'}")
    print(f"Saved: {OUTPUT_DIR / 'judge_pairwise_wilcoxon.csv'}")
    print(f"Saved: {OUTPUT_DIR / 'CORRELATION_ANALYSIS.md'}")
    print(f"Saved: {OUTPUT_DIR / 'PAPER_TABLE_CORRELATIONS.md'}")
    print(f"Saved: {OUTPUT_DIR / 'LLM_JUDGE_CORRELATION_ANALYSIS.html'}")
    docx_path = OUTPUT_DIR / "LLM_JUDGE_CORRELATION_ANALYSIS.docx"
    if docx_path.exists():
        print(f"Saved: {docx_path}")


if __name__ == "__main__":
    main()

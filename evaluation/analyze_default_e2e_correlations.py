#!/usr/bin/env python3
"""Compare default and e2e scores by paired sample in each language.

For each judge and for the three-judge ensemble, this script pairs the default
and e2e outputs belonging to the same sample. English pairs also match on the
source-reflection condition. It calculates Pearson and Spearman correlations
and tests default/e2e score differences with paired Wilcoxon signed-rank tests.
"""

from __future__ import annotations

import html
import os
from pathlib import Path
import shutil
import subprocess
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ConstantInputWarning

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
EVALUATORS = {
    "gpt5": "GPT-5",
    "gemini_25": "Gemini 2.5 Pro",
    "claude_haiku_45": "Claude Haiku 4.5",
    "ensemble": "Three-judge ensemble",
}


def language_groups(results: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    selected = results[results["workflow"].isin(["default", "e2e"])].copy()
    return [
        ("English", selected[selected["collection"] == "nlg"]),
        (
            "Brazilian Portuguese",
            selected[selected["collection"] == "nlg_brazilian_manager"],
        ),
    ]


def add_ensemble(group: pd.DataFrame) -> pd.DataFrame:
    identifiers = [
        "collection",
        "source_reflection",
        "workflow",
        "sample_name",
        "analysis_date",
    ]
    ensemble = group.groupby(identifiers, dropna=False)[DIMENSIONS].mean().reset_index()
    ensemble["judge"] = "ensemble"
    return pd.concat([group, ensemble], ignore_index=True, sort=False)


def paired_workflows(group: pd.DataFrame, evaluator: str, dimension: str) -> pd.DataFrame:
    index = ["collection", "source_reflection", "sample_name", "analysis_date"]
    selected = group[group["judge"] == evaluator]
    wide = selected.pivot(index=index, columns="workflow", values=dimension)
    return wide[["default", "e2e"]].dropna()


def safe_correlations(default: pd.Series, e2e: pd.Series) -> tuple[float, float, float, float]:
    if default.nunique() < 2 or e2e.nunique() < 2:
        return np.nan, np.nan, np.nan, np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        pearson = stats.pearsonr(default, e2e)
        spearman = stats.spearmanr(default, e2e)
    return (
        float(pearson.statistic),
        float(pearson.pvalue),
        float(spearman.statistic),
        float(spearman.pvalue),
    )


def paired_test(default: pd.Series, e2e: pd.Series) -> tuple[float, float]:
    differences = default - e2e
    if np.allclose(differences, 0):
        return 0.0, 1.0
    test = stats.wilcoxon(
        default,
        e2e,
        alternative="two-sided",
        zero_method="pratt",
    )
    return float(test.statistic), float(test.pvalue)


def workflow_letters(default_mean: float, e2e_mean: float, p_value: float) -> tuple[str, str]:
    if p_value >= 0.05:
        return "A", "A"
    if default_mean >= e2e_mean:
        return "A", "B"
    return "B", "A"


def analyse(results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    correlation_rows = []
    difference_rows = []
    for language, raw_group in language_groups(results):
        group = add_ensemble(raw_group)
        for evaluator, evaluator_label in EVALUATORS.items():
            for dimension in DIMENSIONS:
                paired = paired_workflows(group, evaluator, dimension)
                pearson_r, pearson_p, spearman_rho, spearman_p = safe_correlations(
                    paired["default"], paired["e2e"]
                )
                correlation_rows.append(
                    {
                        "language": language,
                        "evaluator": evaluator_label,
                        "dimension": DIMENSION_LABELS[dimension],
                        "n_pairs": len(paired),
                        "pearson_r": pearson_r,
                        "pearson_p": pearson_p,
                        "spearman_rho": spearman_rho,
                        "spearman_p": spearman_p,
                    }
                )

                statistic, p_value = paired_test(paired["default"], paired["e2e"])
                default_mean = float(paired["default"].mean())
                e2e_mean = float(paired["e2e"].mean())
                default_group, e2e_group = workflow_letters(
                    default_mean, e2e_mean, p_value
                )
                difference_rows.append(
                    {
                        "language": language,
                        "evaluator": evaluator_label,
                        "dimension": DIMENSION_LABELS[dimension],
                        "n_pairs": len(paired),
                        "default_mean": default_mean,
                        "default_sd": float(paired["default"].std(ddof=1)),
                        "default_group": default_group,
                        "e2e_mean": e2e_mean,
                        "e2e_sd": float(paired["e2e"].std(ddof=1)),
                        "e2e_group": e2e_group,
                        "mean_difference_default_minus_e2e": float(
                            (paired["default"] - paired["e2e"]).mean()
                        ),
                        "wilcoxon_statistic": statistic,
                        "p_value": p_value,
                        "significant_0.05": p_value < 0.05,
                    }
                )
    return pd.DataFrame(correlation_rows), pd.DataFrame(difference_rows)


def p_text(value: float) -> str:
    if pd.isna(value):
        return "not estimable"
    return "<0.001" if value < 0.001 else f"{value:.3f}"


def correlation_text(coefficient: float, p_value: float) -> str:
    if pd.isna(coefficient):
        return "not estimable"
    return f"{coefficient:.3f} ({p_text(p_value)})"


def score_text(mean: float, sd: float, group: str) -> str:
    return f"{mean:.3f} +/- {sd:.3f} {group}"


def markdown_table(correlations: pd.DataFrame, differences: pd.DataFrame) -> str:
    lines = [
        "# Default versus E2E Correlation Analysis",
        "",
        "Default and e2e scores are paired on the same sample within each language. "
        "English pairs also match on source-reflection status.",
        "",
    ]
    for language in ("English", "Brazilian Portuguese"):
        lines.extend(
            [
                f"## {language}: Pearson and Spearman correlations",
                "",
                "| Evaluator | Dimension | N | Pearson r (p) | Spearman rho (p) |",
                "|---|---|---:|---:|---:|",
            ]
        )
        subset = correlations[correlations["language"] == language]
        for _, row in subset.iterrows():
            lines.append(
                f"| {row['evaluator']} | {row['dimension']} | "
                f"{int(row['n_pairs'])} | "
                f"{correlation_text(row['pearson_r'], row['pearson_p'])} | "
                f"{correlation_text(row['spearman_rho'], row['spearman_p'])} |"
            )
        lines.extend(
            [
                "",
                f"## {language}: default versus e2e score differences",
                "",
                "| Evaluator | Dimension | N | Default | E2E | Wilcoxon p |",
                "|---|---|---:|---:|---:|---:|",
            ]
        )
        subset = differences[differences["language"] == language]
        for _, row in subset.iterrows():
            lines.append(
                f"| {row['evaluator']} | {row['dimension']} | "
                f"{int(row['n_pairs'])} | "
                f"{score_text(row['default_mean'], row['default_sd'], row['default_group'])} | "
                f"{score_text(row['e2e_mean'], row['e2e_sd'], row['e2e_group'])} | "
                f"{p_text(row['p_value'])} |"
            )
        lines.extend(
            [
                "",
                "Values are mean +/- SD. Within a row, values sharing a letter are "
                "not significantly different; A and B indicate a significant paired "
                "difference at p < 0.05 using a two-sided Wilcoxon signed-rank test.",
                "",
            ]
        )
    return "\n".join(lines)


def html_table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(value)}</th>" for value in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def write_document(correlations: pd.DataFrame, differences: pd.DataFrame) -> None:
    sections = []
    for language in ("English", "Brazilian Portuguese"):
        overall_corr = correlations[
            (correlations["language"] == language)
            & (correlations["dimension"] == "Overall")
        ]
        correlation_rows = [
            [
                row["evaluator"],
                str(int(row["n_pairs"])),
                correlation_text(row["pearson_r"], row["pearson_p"]),
                correlation_text(row["spearman_rho"], row["spearman_p"]),
            ]
            for _, row in overall_corr.iterrows()
        ]
        overall_diff = differences[
            (differences["language"] == language)
            & (differences["dimension"] == "Overall")
        ]
        difference_rows = [
            [
                row["evaluator"],
                str(int(row["n_pairs"])),
                score_text(row["default_mean"], row["default_sd"], row["default_group"]),
                score_text(row["e2e_mean"], row["e2e_sd"], row["e2e_group"]),
                p_text(row["p_value"]),
            ]
            for _, row in overall_diff.iterrows()
        ]
        all_corr = correlations[correlations["language"] == language]
        appendix_rows = [
            [
                row["evaluator"],
                row["dimension"],
                str(int(row["n_pairs"])),
                correlation_text(row["pearson_r"], row["pearson_p"]),
                correlation_text(row["spearman_rho"], row["spearman_p"]),
            ]
            for _, row in all_corr.iterrows()
        ]
        sections.extend(
            [
                f"<h2>{language}</h2>",
                "<h3>Overall-score default/e2e correlations</h3>",
                html_table(
                    ["Evaluator", "N", "Pearson r (p)", "Spearman rho (p)"],
                    correlation_rows,
                ),
                "<h3>Overall default/e2e score differences</h3>",
                html_table(
                    ["Evaluator", "N", "Default", "E2E", "Wilcoxon p"],
                    difference_rows,
                ),
                "<h3>Dimension-level correlations</h3>",
                html_table(
                    ["Evaluator", "Dimension", "N", "Pearson r (p)", "Spearman rho (p)"],
                    appendix_rows,
                ),
            ]
        )

    document = f"""<!doctype html>
<html><head><meta charset="utf-8">
<title>Default versus E2E Correlation Analysis</title>
<style>
@page {{ size: A4 landscape; margin: 1.5cm; }}
body {{ font-family: Arial, sans-serif; font-size: 10pt; line-height: 1.35; }}
h1 {{ font-size: 18pt; }} h2 {{ font-size: 14pt; margin-top: 18pt; }}
h3 {{ font-size: 11pt; }}
table {{ border-collapse: collapse; width: 100%; margin: 7pt 0 14pt; }}
th, td {{ border: 1px solid #777; padding: 5pt; vertical-align: top; }}
th {{ background: #e9eef3; }}
</style></head><body>
<h1>Default versus E2E Correlation Analysis</h1>
<p>This analysis correlates default and e2e scores for the same report sample.
English observations are additionally paired within the same source-reflection
condition. Results are presented separately for English and Brazilian
Portuguese.</p>
<h2>Methods</h2>
<p>For each evaluator and the three-judge ensemble, two-sided Pearson and
Spearman correlations were calculated between paired default and e2e scores.
Paired score differences were tested with two-sided Wilcoxon signed-rank tests
using the Pratt treatment of zero differences. Values sharing a letter are not
significantly different; A and B indicate p &lt; 0.05. Correlations are labelled
not estimable when default or e2e scores have zero variance.</p>
{''.join(sections)}
<h2>Reproducibility</h2>
<p>Run <code>./run_llm_as_judge.sh --correlation-analysis</code>. The source is
<code>analyze_default_e2e_correlations.py</code>.</p>
</body></html>"""
    html_path = OUTPUT_DIR / "DEFAULT_VS_E2E_CORRELATION_ANALYSIS.html"
    html_path.write_text(document, encoding="utf-8")

    office = shutil.which("libreoffice") or shutil.which("soffice")
    if not office:
        return
    temp_dirs = [
        "/tmp/default_e2e_libreoffice_profile",
        "/tmp/default_e2e_document_home",
        "/tmp/default_e2e_document_config",
        "/tmp/default_e2e_document_cache",
        "/tmp/default_e2e_document_runtime",
    ]
    for directory in temp_dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            office,
            "--headless",
            "-env:UserInstallation=file:///tmp/default_e2e_libreoffice_profile",
            "--convert-to",
            "docx:Office Open XML Text",
            "--outdir",
            str(OUTPUT_DIR),
            str(html_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "HOME": "/tmp/default_e2e_document_home",
            "XDG_CONFIG_HOME": "/tmp/default_e2e_document_config",
            "XDG_CACHE_HOME": "/tmp/default_e2e_document_cache",
            "XDG_RUNTIME_DIR": "/tmp/default_e2e_document_runtime",
        },
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = aggregate.load_cached_results()
    correlations, differences = analyse(results)

    expected_pairs = {
        ("English", evaluator): 28 for evaluator in EVALUATORS
    } | {
        ("Brazilian Portuguese", evaluator): 24 for evaluator in EVALUATORS
    }
    observed = correlations.groupby(["language", "evaluator"])["n_pairs"].min()
    for key, expected in expected_pairs.items():
        if observed.get((key[0], EVALUATORS[key[1]]), 0) != expected:
            raise RuntimeError(f"Incomplete paired workflow data for {key}")

    correlations.to_csv(
        OUTPUT_DIR / "default_e2e_correlations_pearson_spearman.csv", index=False
    )
    differences.to_csv(
        OUTPUT_DIR / "default_e2e_paired_wilcoxon.csv", index=False
    )
    markdown = markdown_table(correlations, differences)
    (OUTPUT_DIR / "DEFAULT_VS_E2E_CORRELATION_ANALYSIS.md").write_text(
        markdown, encoding="utf-8"
    )
    write_document(correlations, differences)

    print("Paired English samples per evaluator: 28")
    print("Paired Brazilian Portuguese samples per evaluator: 24")
    for filename in (
        "default_e2e_correlations_pearson_spearman.csv",
        "default_e2e_paired_wilcoxon.csv",
        "DEFAULT_VS_E2E_CORRELATION_ANALYSIS.md",
        "DEFAULT_VS_E2E_CORRELATION_ANALYSIS.html",
        "DEFAULT_VS_E2E_CORRELATION_ANALYSIS.docx",
    ):
        path = OUTPUT_DIR / filename
        if path.exists():
            print(f"Saved: {path}")


if __name__ == "__main__":
    main()

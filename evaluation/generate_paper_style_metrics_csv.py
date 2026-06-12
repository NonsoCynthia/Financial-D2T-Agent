#!/usr/bin/env python3
"""Generate paper-style default/e2e rankings and Pearson IAA in one CSV.

Output sections:
  system_ranking      Ensemble mean and A/B group for default and e2e.
  iaa_item_pearson    Pairwise judge Pearson correlation across report items.
  iaa_system_pearson  Pairwise correlation across system means. With only two
                      systems, this is descriptive and not inferential.
"""

from __future__ import annotations

from itertools import combinations
import math
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ConstantInputWarning

import aggregate_llm_judge_results as aggregate
import analyze_default_e2e_correlations as workflow_analysis


OUTPUT_PATH_EN = aggregate.PAPER_DIR / "paper_style_default_e2e_reflection_true_metrics.csv"
OUTPUT_PATH_BR = aggregate.PAPER_DIR / "paper_style_default_e2e_br_metrics.csv"
NUMERIC_COLS = ["mean", "sd", "pearson_r", "p_value"]


def round_sig(x: object, sig: int = 3) -> object:
    if pd.isna(x) or x == 0:
        return x
    v = float(x)
    if abs(v) < 1e-10:
        return 0.0
    return round(v, sig - int(math.floor(math.log10(abs(v)))) - 1)
JUDGE_LABELS = {
    "gpt5": "GPT-5",
    "gemini_25": "Gemini 2.5 Pro",
    "claude_haiku_45": "Claude Haiku 4.5",
}
JUDGE_PAIRS = list(combinations(JUDGE_LABELS, 2))


def safe_pearson(x: pd.Series, y: pd.Series) -> tuple[float, float]:
    paired = pd.concat([x, y], axis=1).dropna()
    if len(paired) < 3 or paired.iloc[:, 0].nunique() < 2 or paired.iloc[:, 1].nunique() < 2:
        return np.nan, np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        result = stats.pearsonr(paired.iloc[:, 0], paired.iloc[:, 1])
    return float(result.statistic), float(result.pvalue)


def base_row(metric_type: str, language: str, dimension: str) -> dict[str, object]:
    return {
        "metric_type": metric_type,
        "language": language,
        "dimension": dimension,
        "system": "",
        "mean": np.nan,
        "sd": np.nan,
        "group": "",
        "n": np.nan,
        "judge_a": "",
        "judge_b": "",
        "pearson_r": np.nan,
        "p_value": np.nan,
        "significant_0.05": "",
        "significant_0.001": "",
        "interpretation": "",
    }


_LANGUAGE_MAP = {
    "nlg": "English",
    "nlg_brazilian_manager": "Brazilian Portuguese",
}


def ranking_rows(results: pd.DataFrame) -> list[dict[str, object]]:
    """Build system_ranking rows without calling workflow_analysis.analyse(),
    so empty language groups after source_reflection filtering are skipped."""
    identifiers = [
        "collection", "source_reflection", "workflow", "sample_name", "analysis_date",
    ]
    dimensions = list(workflow_analysis.DIMENSION_LABELS)
    ensemble = (
        results[results["workflow"].isin(["default", "e2e"])]
        .groupby(identifiers, dropna=False)[dimensions]
        .mean()
        .reset_index()
    )
    rows = []
    for collection, language in _LANGUAGE_MAP.items():
        lang = ensemble[ensemble["collection"] == collection]
        if lang.empty:
            continue
        pivot_index = [
            "collection", "source_reflection", "sample_name", "analysis_date",
        ]
        for dim_col, dim_label in workflow_analysis.DIMENSION_LABELS.items():
            wide = lang.pivot(
                index=pivot_index, columns="workflow", values=dim_col
            )
            if "default" not in wide.columns or "e2e" not in wide.columns:
                continue
            paired = wide[["default", "e2e"]].dropna()
            if len(paired) < 2:
                continue
            diff = paired["default"] - paired["e2e"]
            if np.allclose(diff, 0):
                statistic, p_value = 0.0, 1.0
            else:
                test = stats.wilcoxon(
                    paired["default"], paired["e2e"], alternative="two-sided"
                )
                statistic, p_value = float(test.statistic), float(test.pvalue)
            default_mean = float(paired["default"].mean())
            e2e_mean = float(paired["e2e"].mean())
            sig_05 = p_value < 0.05
            if not sig_05:
                default_group, e2e_group = "A", "A"
            elif default_mean >= e2e_mean:
                default_group, e2e_group = "A", "B"
            else:
                default_group, e2e_group = "B", "A"
            for system, mean, sd, group in [
                ("default", default_mean, float(paired["default"].std(ddof=1)), default_group),
                ("e2e", e2e_mean, float(paired["e2e"].std(ddof=1)), e2e_group),
            ]:
                output = base_row("system_ranking", language, dim_label)
                output.update(
                    system=system,
                    mean=mean,
                    sd=sd,
                    group=group,
                    n=len(paired),
                    p_value=p_value,
                    interpretation=(
                        "Systems sharing a letter are not significantly different; "
                        "different letters indicate paired Wilcoxon p<0.05."
                    ),
                )
                output["significant_0.05"] = sig_05
                output["significant_0.001"] = p_value < 0.001
                rows.append(output)
    return rows


def selected_language_data(results: pd.DataFrame, language: str) -> pd.DataFrame:
    collection = "nlg" if language == "English" else "nlg_brazilian_manager"
    return results[
        (results["collection"] == collection)
        & results["workflow"].isin(["default", "e2e"])
    ].copy()


def item_iaa_rows(results: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    identifiers = [
        "collection",
        "source_reflection",
        "workflow",
        "sample_name",
        "analysis_date",
    ]
    for language in ("English", "Brazilian Portuguese"):
        data = selected_language_data(results, language)
        if data.empty:
            continue
        for dimension_column, dimension_label in workflow_analysis.DIMENSION_LABELS.items():
            wide = data.pivot(
                index=identifiers,
                columns="judge",
                values=dimension_column,
            )
            pair_coefficients = []
            pair_p_values = []
            for judge_a, judge_b in JUDGE_PAIRS:
                coefficient, p_value = safe_pearson(wide[judge_a], wide[judge_b])
                output = base_row("iaa_item_pearson", language, dimension_label)
                output.update(
                    n=len(wide[[judge_a, judge_b]].dropna()),
                    judge_a=JUDGE_LABELS[judge_a],
                    judge_b=JUDGE_LABELS[judge_b],
                    pearson_r=coefficient,
                    p_value=p_value,
                    interpretation="Pearson correlation across paired report scores.",
                )
                output["significant_0.05"] = (
                    bool(p_value < 0.05) if not pd.isna(p_value) else ""
                )
                output["significant_0.001"] = (
                    bool(p_value < 0.001) if not pd.isna(p_value) else ""
                )
                rows.append(output)
                if not pd.isna(coefficient):
                    pair_coefficients.append(coefficient)
                if not pd.isna(p_value):
                    pair_p_values.append(p_value)

            output = base_row("iaa_item_pearson_mean_pairwise", language, dimension_label)
            output.update(
                n=len(wide),
                judge_a="All three judges",
                judge_b="Mean pairwise",
                pearson_r=(
                    float(np.tanh(np.mean(np.arctanh(
                        np.clip(pair_coefficients, -0.999999, 0.999999)
                    ))))
                    if pair_coefficients else np.nan
                ),
                interpretation=(
                    "Fisher-z mean of the available pairwise Pearson "
                    "correlations; use pairwise rows and p-values for inference."
                ),
            )
            rows.append(output)
    return rows


def system_iaa_rows(results: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    for language in ("English", "Brazilian Portuguese"):
        data = selected_language_data(results, language)
        if data.empty:
            continue
        for dimension_column, dimension_label in workflow_analysis.DIMENSION_LABELS.items():
            means = (
                data.groupby(["judge", "workflow"])[dimension_column]
                .mean()
                .unstack("judge")
            )
            for judge_a, judge_b in JUDGE_PAIRS:
                coefficient, p_value = safe_pearson(means[judge_a], means[judge_b])
                output = base_row("iaa_system_pearson", language, dimension_label)
                output.update(
                    n=len(means),
                    judge_a=JUDGE_LABELS[judge_a],
                    judge_b=JUDGE_LABELS[judge_b],
                    pearson_r=coefficient,
                    p_value=p_value,
                    interpretation=(
                        "Not inferential: only two systems (default and e2e). "
                        "At least three systems are needed for a useful "
                        "system-level correlation."
                    ),
                )
                output["significant_0.05"] = ""
                rows.append(output)
    return rows


def _drop_incomplete_collections(results: pd.DataFrame) -> pd.DataFrame:
    """Remove collections that no longer have both default and e2e after filtering."""
    has_both = (
        results[results["workflow"].isin(["default", "e2e"])]
        .groupby("collection")["workflow"]
        .nunique()
    )
    valid = has_both[has_both == 2].index
    return results[results["collection"].isin(valid)].copy()


def main() -> None:
    aggregate.PAPER_DIR.mkdir(parents=True, exist_ok=True)
    all_results = aggregate.load_cached_results()

    en_results = _drop_incomplete_collections(
        all_results[all_results["source_reflection"] == True]
    )
    rows = ranking_rows(en_results) + item_iaa_rows(en_results) + system_iaa_rows(en_results)
    output = pd.DataFrame(rows)
    for col in NUMERIC_COLS:
        if col in output.columns:
            output[col] = output[col].apply(round_sig)
    output.to_csv(OUTPUT_PATH_EN, index=False)
    print(f"Saved {len(output)} rows (English, reflection_true): {OUTPUT_PATH_EN}")
    print(output.groupby("metric_type").size().to_string())

    br_results = all_results[all_results["collection"] == "nlg_brazilian_manager"].copy()
    rows = ranking_rows(br_results) + item_iaa_rows(br_results) + system_iaa_rows(br_results)
    output = pd.DataFrame(rows)
    for col in NUMERIC_COLS:
        if col in output.columns:
            output[col] = output[col].apply(round_sig)
    output.to_csv(OUTPUT_PATH_BR, index=False)
    print(f"Saved {len(output)} rows (Brazilian Portuguese): {OUTPUT_PATH_BR}")
    print(output.groupby("metric_type").size().to_string())


if __name__ == "__main__":
    main()

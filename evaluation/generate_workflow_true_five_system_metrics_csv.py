#!/usr/bin/env python3
"""Generate a separate English workflow_True five-system analysis table."""

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
from analyze_llm_judge_correlations import holm_adjust


SYSTEMS = [
    "default",
    "default_old",
    "no_orchestrator_no_finalizer",
    "no_guardrail_no_finalizer",
    "no_orchestrator_no_guardrail_no_finalizer",
]
JUDGE_LABELS = {
    "gpt5": "GPT-5",
    "gemini_25": "Gemini 2.5 Pro",
    "claude_haiku_45": "Claude Haiku 4.5",
}
JUDGE_PAIRS = list(combinations(JUDGE_LABELS, 2))
OUTPUT_PATH = aggregate.PAPER_DIR / "paper_style_workflow_true_five_system_metrics.csv"
NUMERIC_COLS = ["mean", "sd", "pearson_r", "p_value", "p_holm"]


def round_sig(x: object, sig: int = 3) -> object:
    if pd.isna(x) or x == 0:
        return x
    v = float(x)
    if abs(v) < 1e-10:
        return 0.0
    return round(v, sig - int(math.floor(math.log10(abs(v)))) - 1)


def safe_pearson(x: pd.Series, y: pd.Series) -> tuple[float, float, int]:
    paired = pd.concat([x, y], axis=1).dropna()
    if (
        len(paired) < 3
        or paired.iloc[:, 0].nunique() < 2
        or paired.iloc[:, 1].nunique() < 2
    ):
        return np.nan, np.nan, len(paired)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        result = stats.pearsonr(paired.iloc[:, 0], paired.iloc[:, 1])
    return float(result.statistic), float(result.pvalue), len(paired)


def base_row(metric_type: str, dimension: str) -> dict[str, object]:
    return {
        "metric_type": metric_type,
        "language": "English",
        "source_reflection": True,
        "dimension": dimension,
        "system": "",
        "system_a": "",
        "system_b": "",
        "mean": np.nan,
        "sd": np.nan,
        "group": "",
        "n": np.nan,
        "judge_a": "",
        "judge_b": "",
        "pearson_r": np.nan,
        "p_value": np.nan,
        "p_holm": np.nan,
        "significant_0.05": "",
        "significant_0.001": "",
        "interpretation": "",
    }


def selected_results(results: pd.DataFrame) -> pd.DataFrame:
    selected = results[
        (results["collection"] == "nlg")
        & (results["source_reflection"] == True)
        & results["workflow"].isin(SYSTEMS)
    ].copy()
    coverage = selected.groupby(["workflow", "judge"])["sample_name"].nunique()
    missing = [
        (system, judge, int(coverage.get((system, judge), 0)))
        for system in SYSTEMS
        for judge in JUDGE_LABELS
        if coverage.get((system, judge), 0) != 14
    ]
    if missing:
        raise RuntimeError(f"Incomplete workflow_True coverage: {missing}")
    return selected


def complete_ensemble(data: pd.DataFrame) -> pd.DataFrame:
    identifiers = ["workflow", "sample_name", "analysis_date"]
    dimensions = list(workflow_analysis.DIMENSION_LABELS)
    counts = data.groupby(identifiers)["judge"].nunique()
    if not (counts == len(JUDGE_LABELS)).all():
        raise RuntimeError("Every workflow/sample must have all three judge scores")
    ensemble = data.groupby(identifiers)[dimensions].mean().reset_index()
    return ensemble


def compact_system_letters(
    systems: list[str],
    significant: dict[tuple[str, str], bool],
    means: dict[str, float],
) -> dict[str, str]:
    """Construct a minimal compact-letter display for pairwise decisions."""
    ranked = sorted(systems, key=lambda system: (-means[system], SYSTEMS.index(system)))
    compatible_sets = []
    for mask in range(1, 1 << len(ranked)):
        members = frozenset(
            ranked[index] for index in range(len(ranked)) if mask & (1 << index)
        )
        if all(
            not significant[tuple(sorted(pair))]
            for pair in combinations(members, 2)
        ):
            compatible_sets.append(members)

    required = {frozenset((system,)) for system in ranked}
    required.update(
        frozenset(pair)
        for pair in combinations(ranked, 2)
        if not significant[tuple(sorted(pair))]
    )

    best = None
    for count in range(1, len(compatible_sets) + 1):
        for chosen in combinations(compatible_sets, count):
            covered = {
                requirement
                for requirement in required
                if any(requirement.issubset(group) for group in chosen)
            }
            if covered != required:
                continue
            objective = (
                count,
                sum(len(group) for group in chosen),
                tuple(
                    tuple(ranked.index(system) for system in ranked if system in group)
                    for group in chosen
                ),
            )
            if best is None or objective < best[0]:
                best = objective, chosen
        if best is not None:
            break
    if best is None:
        raise RuntimeError("Could not construct compact system letters")

    ordered_groups = sorted(
        best[1],
        key=lambda group: min(ranked.index(system) for system in group),
    )
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return {
        system: "".join(
            letters[index]
            for index, group in enumerate(ordered_groups)
            if system in group
        )
        for system in ranked
    }


def ranking_and_test_rows(
    ensemble: pd.DataFrame,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    ranking_rows = []
    test_rows = []
    for dimension_column, dimension_label in workflow_analysis.DIMENSION_LABELS.items():
        wide = ensemble.pivot(
            index=["sample_name", "analysis_date"],
            columns="workflow",
            values=dimension_column,
        )[SYSTEMS].dropna()
        means = {system: float(wide[system].mean()) for system in SYSTEMS}
        raw_p_values = {}
        statistics = {}
        for system_a, system_b in combinations(SYSTEMS, 2):
            difference = wide[system_a] - wide[system_b]
            if np.allclose(difference, 0):
                statistic, p_value = 0.0, 1.0
            else:
                test = stats.wilcoxon(
                    wide[system_a],
                    wide[system_b],
                    alternative="two-sided",
                    zero_method="pratt",
                )
                statistic, p_value = float(test.statistic), float(test.pvalue)
            pair = tuple(sorted((system_a, system_b)))
            statistics[pair] = statistic
            raw_p_values[pair] = p_value

        adjusted = holm_adjust(raw_p_values)
        significant = {pair: value < 0.05 for pair, value in adjusted.items()}
        letters = compact_system_letters(SYSTEMS, significant, means)

        for system in SYSTEMS:
            row = base_row("system_ranking", dimension_label)
            row.update(
                system=system,
                mean=means[system],
                sd=float(wide[system].std(ddof=1)),
                group=letters[system],
                n=len(wide),
                interpretation=(
                    "Three-judge ensemble mean. Systems sharing a letter are not "
                    "significantly different after Holm-adjusted paired Wilcoxon tests."
                ),
            )
            ranking_rows.append(row)

        for system_a, system_b in combinations(SYSTEMS, 2):
            pair = tuple(sorted((system_a, system_b)))
            row = base_row("system_pairwise_wilcoxon", dimension_label)
            row.update(
                system_a=system_a,
                system_b=system_b,
                n=len(wide),
                p_value=raw_p_values[pair],
                p_holm=adjusted[pair],
                interpretation=(
                    f"Paired Wilcoxon statistic={statistics[pair]:.6g}; "
                    "p_holm controls the ten system comparisons within this dimension."
                ),
            )
            row["significant_0.05"] = significant[pair]
            row["significant_0.001"] = adjusted[pair] < 0.001
            test_rows.append(row)
    return ranking_rows, test_rows


def item_iaa_rows(data: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    index = ["workflow", "sample_name", "analysis_date"]
    for dimension_column, dimension_label in workflow_analysis.DIMENSION_LABELS.items():
        wide = data.pivot(index=index, columns="judge", values=dimension_column)
        coefficients = []
        for judge_a, judge_b in JUDGE_PAIRS:
            coefficient, p_value, n = safe_pearson(wide[judge_a], wide[judge_b])
            row = base_row("iaa_item_pearson", dimension_label)
            row.update(
                n=n,
                judge_a=JUDGE_LABELS[judge_a],
                judge_b=JUDGE_LABELS[judge_b],
                pearson_r=coefficient,
                p_value=p_value,
                interpretation=(
                    "Pearson inter-judge correlation across 70 matched "
                    "workflow/sample outputs."
                ),
            )
            row["significant_0.05"] = (
                bool(p_value < 0.05) if not pd.isna(p_value) else ""
            )
            row["significant_0.001"] = (
                bool(p_value < 0.001) if not pd.isna(p_value) else ""
            )
            rows.append(row)
            if not pd.isna(coefficient):
                coefficients.append(coefficient)

        row = base_row("iaa_item_pearson_mean_pairwise", dimension_label)
        row.update(
            n=len(wide),
            judge_a="All three judges",
            judge_b="Mean pairwise",
            pearson_r=(
                float(
                    np.tanh(
                        np.mean(
                            np.arctanh(
                                np.clip(coefficients, -0.999999, 0.999999)
                            )
                        )
                    )
                )
                if coefficients
                else np.nan
            ),
            interpretation=(
                "Fisher-z mean of available pairwise item-level Pearson correlations."
            ),
        )
        rows.append(row)
    return rows


def system_iaa_rows(data: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    for dimension_column, dimension_label in workflow_analysis.DIMENSION_LABELS.items():
        means = (
            data.groupby(["workflow", "judge"])[dimension_column]
            .mean()
            .unstack("judge")
            .reindex(SYSTEMS)
        )
        for judge_a, judge_b in JUDGE_PAIRS:
            coefficient, p_value, n = safe_pearson(means[judge_a], means[judge_b])
            row = base_row("iaa_system_pearson", dimension_label)
            row.update(
                n=n,
                judge_a=JUDGE_LABELS[judge_a],
                judge_b=JUDGE_LABELS[judge_b],
                pearson_r=coefficient,
                p_value=p_value,
                interpretation=(
                    "Pearson inter-judge correlation across the five system means."
                ),
            )
            row["significant_0.05"] = (
                bool(p_value < 0.05) if not pd.isna(p_value) else ""
            )
            row["significant_0.001"] = (
                bool(p_value < 0.001) if not pd.isna(p_value) else ""
            )
            rows.append(row)
    return rows


def main() -> None:
    aggregate.PAPER_DIR.mkdir(parents=True, exist_ok=True)
    data = selected_results(aggregate.load_cached_results())
    ensemble = complete_ensemble(data)
    rankings, tests = ranking_and_test_rows(ensemble)
    rows = rankings + tests + item_iaa_rows(data) + system_iaa_rows(data)
    output = pd.DataFrame(rows)
    for col in NUMERIC_COLS:
        if col in output.columns:
            output[col] = output[col].apply(round_sig)
    output.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(output)} rows: {OUTPUT_PATH}")
    print(output.groupby("metric_type").size().to_string())


if __name__ == "__main__":
    main()

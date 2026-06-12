#!/usr/bin/env python3
"""Build paper-ready tables from cached multimodel LLM-judge JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

import run_multimodel_judge as judge


ROOT = judge.MULTIMODEL_ROOT
PAPER_DIR = ROOT / "paper_results"
SCORE_COLUMNS = [
    f"{dimension.lower().replace('-', '_')}_score"
    for dimension in judge.DIMENSION_NAMES
]
ALL_JUDGES = list(judge.JUDGES)
PUBLIC_RESULT_COLUMNS = [
    "sample_name",
    "analysis_date",
    "judge",
    "judge_label",
    *SCORE_COLUMNS,
    "mean_score",
    "collection",
    "source_reflection",
    "workflow",
]


def result_condition(path: Path) -> tuple[str, bool | None, str]:
    parts = path.relative_to(ROOT).parts
    if len(parts) >= 7 and parts[1].startswith("workflow_"):
        return "nlg", parts[1] == "workflow_True", parts[-2]
    if len(parts) >= 5 and parts[1] == "nlg_brazilian_manager":
        return "nlg_brazilian_manager", None, parts[-2]
    raise ValueError(f"Unrecognised judge result path: {path}")


def load_cached_results() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(ROOT.glob("*/*/**/*.json")):
        try:
            collection, source_reflection, workflow = result_condition(path)
        except ValueError:
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        row = judge.flatten(raw)
        row.update(
            collection=collection,
            source_reflection=source_reflection,
            workflow=workflow,
            result_file=str(path.relative_to(judge.PROJECT_ROOT)),
        )
        rows.append(row)

    results = pd.DataFrame(rows)
    if results.empty:
        raise RuntimeError(f"No cached judge JSON files found under {ROOT}")

    duplicate_columns = [
        "collection", "source_reflection", "workflow", "sample_name", "judge"
    ]
    duplicates = results.duplicated(duplicate_columns, keep=False)
    if duplicates.any():
        files = results.loc[duplicates, duplicate_columns + ["result_file"]]
        raise RuntimeError(f"Duplicate cached judgments found:\n{files.to_string(index=False)}")
    return results


def expected_samples() -> dict[tuple[str, bool | None, str], set[str]]:
    expected: dict[tuple[str, bool | None, str], set[str]] = {}
    for condition in judge.CONDITIONS:
        records = judge.build_records(*condition)
        expected[condition] = {record["sample_name"] for record in records}
    return expected


def build_completeness(
    results: pd.DataFrame,
    expected: dict[tuple[str, bool | None, str], set[str]],
) -> pd.DataFrame:
    rows = []
    for condition, sample_names in expected.items():
        collection, source_reflection, workflow = condition
        condition_rows = results[
            (results["collection"] == collection)
            & (results["source_reflection"].fillna("NA") == (
                source_reflection if source_reflection is not None else "NA"
            ))
            & (results["workflow"] == workflow)
        ]
        for judge_name in ALL_JUDGES:
            completed = set(
                condition_rows.loc[
                    condition_rows["judge"] == judge_name, "sample_name"
                ]
            )
            missing = sorted(sample_names - completed)
            unexpected = sorted(completed - sample_names)
            rows.append(
                {
                    "collection": collection,
                    "source_reflection": source_reflection,
                    "workflow": workflow,
                    "judge": judge_name,
                    "judge_label": judge.JUDGES[judge_name]["label"],
                    "expected_samples": len(sample_names),
                    "completed_samples": len(completed & sample_names),
                    "coverage_percent": 100 * len(completed & sample_names) / len(sample_names),
                    "complete": not missing and not unexpected,
                    "missing_samples": ";".join(missing),
                    "unexpected_samples": ";".join(unexpected),
                }
            )
    return pd.DataFrame(rows)


def mean_ci(values: pd.Series) -> tuple[float, float, float, float]:
    data = values.dropna().astype(float).to_numpy()
    mean = float(data.mean())
    if len(data) < 2:
        return mean, float("nan"), float("nan"), float("nan")
    sd = float(data.std(ddof=1))
    margin = float(stats.t.ppf(0.975, len(data) - 1) * sd / np.sqrt(len(data)))
    return mean, sd, mean - margin, mean + margin


def summarise(
    results: pd.DataFrame,
    group_columns: list[str],
) -> pd.DataFrame:
    rows = []
    for keys, group in results.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys))
        row["n_samples"] = group["sample_name"].nunique()
        for column in SCORE_COLUMNS + ["mean_score"]:
            mean, sd, lower, upper = mean_ci(group[column])
            row[column] = mean
            row[f"{column}_sd"] = sd
            row[f"{column}_ci_lower"] = lower
            row[f"{column}_ci_upper"] = upper
        rows.append(row)
    return pd.DataFrame(rows)


def build_complete_case_ensemble(results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    identifiers = [
        "collection", "source_reflection", "workflow", "sample_name", "analysis_date"
    ]
    counts = (
        results.groupby(identifiers, dropna=False)["judge"]
        .nunique()
        .rename("n_judges")
        .reset_index()
    )
    complete_ids = counts[counts["n_judges"] == len(ALL_JUDGES)][identifiers]
    complete = results.merge(complete_ids, on=identifiers, how="inner")
    ensemble = (
        complete.groupby(identifiers, dropna=False)[SCORE_COLUMNS]
        .mean()
        .reset_index()
    )
    ensemble["mean_score"] = ensemble[SCORE_COLUMNS].mean(axis=1)
    return ensemble, counts


def main() -> None:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    results = load_cached_results()
    expected = expected_samples()
    completeness = build_completeness(results, expected)
    per_judge = summarise(
        results,
        ["collection", "source_reflection", "workflow", "judge", "judge_label"],
    )
    ensemble, judge_counts = build_complete_case_ensemble(results)
    ensemble_summary = summarise(
        ensemble,
        ["collection", "source_reflection", "workflow"],
    )

    outputs = {
        "all_judgments.csv": results[PUBLIC_RESULT_COLUMNS],
        "completeness.csv": completeness,
        "summary_per_judge.csv": per_judge,
        "complete_case_ensemble.csv": ensemble,
        "summary_complete_case_ensemble.csv": ensemble_summary,
        "judges_per_sample.csv": judge_counts,
    }
    for filename, frame in outputs.items():
        path = PAPER_DIR / filename
        frame.to_csv(path, index=False)
        print(f"Saved {len(frame):>4} rows: {path.relative_to(judge.PROJECT_ROOT)}")

    complete_conditions = completeness.groupby(
        ["collection", "source_reflection", "workflow"], dropna=False
    )["complete"].all()
    print(
        f"\nCached judgments: {len(results)}"
        f"\nFully complete conditions: {int(complete_conditions.sum())}/{len(complete_conditions)}"
        f"\nComplete three-judge samples: {len(ensemble)}"
    )
    incomplete = completeness[~completeness["complete"]]
    if not incomplete.empty:
        print("\nIncomplete judge-condition rows:")
        print(
            incomplete[
                [
                    "collection", "source_reflection", "workflow", "judge_label",
                    "completed_samples", "expected_samples", "missing_samples",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()

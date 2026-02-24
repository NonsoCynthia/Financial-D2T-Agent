from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass(frozen=True)
class Table2Row:
    """One row in the Table 2 style benchmarking output."""
    indicator: str
    predicted: Optional[float]
    gold: Optional[float]
    normalised_error: Optional[float]


def build_table2(
    predicted: Dict[str, Optional[float]],
    gold: Dict[str, Optional[float]],
    errors: Dict[str, Optional[float]],
) -> List[Table2Row]:
    """Build Table 2 rows from predicted values, gold values, and per-indicator errors."""
    rows: List[Table2Row] = []
    for k in sorted(set(predicted.keys()) | set(gold.keys())):
        rows.append(
            Table2Row(
                indicator=k,
                predicted=predicted.get(k),
                gold=gold.get(k),
                normalised_error=errors.get(k),
            )
        )
    return rows


def write_table2_csv(rows: List[Table2Row], out_path: Path) -> Path:
    """Write Table 2 rows to a CSV file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Indicator", "Predicted", "Gold", "NormalisedError"])
        for r in rows:
            w.writerow([r.indicator, r.predicted, r.gold, r.normalised_error])
    return out_path

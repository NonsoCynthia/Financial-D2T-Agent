from __future__ import annotations

from typing import Dict, Optional, Tuple


def normalised_error(pred: Optional[float], gold: Optional[float], eps: float = 1e-12) -> Optional[float]:
    """Compute absolute relative error |pred - gold| / max(|gold|, eps).

    Returns None if pred or gold is missing.
    Uses an epsilon guard so that near-zero gold values do not explode.
    """
    if pred is None or gold is None:
        return None
    denom = abs(gold)
    if denom < eps:
        denom = eps
    return abs(pred - gold) / denom


def merge_gold_sources(
    roic_values: Dict[str, Optional[float]],
    google_values: Dict[str, Optional[float]],
) -> Dict[str, Optional[float]]:
    """Merge two gold sources into one benchmark dict, preferring ROIC where available."""
    merged = dict(google_values)
    for k, v in roic_values.items():
        if v is not None:
            merged[k] = v
        else:
            merged.setdefault(k, None)
    return merged


def compute_errors_per_indicator(
    predicted: Dict[str, Optional[float]],
    gold: Dict[str, Optional[float]],
) -> Dict[str, Optional[float]]:
    """Compute normalised errors for each indicator present in either predicted or gold dicts."""
    keys = sorted(set(predicted.keys()) | set(gold.keys()))
    out: Dict[str, Optional[float]] = {}
    for k in keys:
        out[k] = normalised_error(pred=predicted.get(k), gold=gold.get(k))
    return out
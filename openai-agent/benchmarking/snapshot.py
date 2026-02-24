from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .gold_sources import GoldSnapshot


def utc_timestamp() -> str:
    """Return an ISO-8601 UTC timestamp suitable for filenames and audit logs."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_snapshot(snapshot: GoldSnapshot, out_dir: Path) -> Path:
    """Persist a GoldSnapshot as JSON for reproducible benchmarking."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{snapshot.ticker}_{snapshot.asof}_{snapshot.source}_{utc_timestamp()}.json"
    path = out_dir / fname
    payload: Dict[str, Any] = asdict(snapshot)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def load_snapshot(path: Path) -> GoldSnapshot:
    """Load a previously saved GoldSnapshot JSON file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return GoldSnapshot(
        source=data["source"],
        ticker=data["ticker"],
        asof=data["asof"],
        values=data["values"],
        raw=data["raw"],
    )
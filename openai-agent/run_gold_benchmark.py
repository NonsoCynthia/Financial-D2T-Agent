from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

from financial_d2t_agent.benchmarking.errors import compute_errors_per_indicator, merge_gold_sources
from financial_d2t_agent.benchmarking.gold_sources import fetch_google_finance_snapshot, fetch_roic_snapshot
from financial_d2t_agent.benchmarking.snapshot import save_snapshot
from financial_d2t_agent.benchmarking.table2 import build_table2, write_table2_csv


def load_predicted_indicators(path: Path) -> Dict[str, Optional[float]]:
    """Load your agent-produced indicators from JSON.

    Expected JSON shape is a flat dict like:
    {
      "Assets": 123.0,
      "CashAndEquivalents": 45.0,
      "ShareholderEquity": 67.0,
      "P_E": 12.3,
      "P_B": 4.5
    }

    If your current output differs, adapt this function only, not the rest of the pipeline.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return {k: (None if v is None else float(v)) for k, v in data.items()}
    raise ValueError(f"Unsupported prediction format in {path}")


def main() -> None:
    """Run gold-standard benchmarking, snapshot sources, and write Table 2 CSV."""
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", required=True)
    p.add_argument("--asof", required=True, help="A date label for your run, for example 2026-02-23")
    p.add_argument("--pred", required=True, type=Path, help="Path to agent JSON predictions for this ticker and date")
    p.add_argument("--roic_apikey", default=None, help="ROIC.ai API key. Prefer using an env var in your own wrapper.")
    p.add_argument("--google_exchange", default="NASDAQ", help="Google Finance exchange, for example NASDAQ or NYSE")
    p.add_argument("--snapshot_dir", default=Path("data/benchmarks/snapshots"), type=Path)
    p.add_argument("--out_csv", default=None, help="Output CSV path. Defaults to data/benchmarks/table2/")
    args = p.parse_args()

    predicted = load_predicted_indicators(path=args.pred)

    if not args.roic_apikey:
        raise SystemExit("Missing --roic_apikey. ROIC.ai requires an API key. :contentReference[oaicite:1]{index=1}")

    roic = fetch_roic_snapshot(ticker=args.ticker, apikey=args.roic_apikey, asof=args.asof)
    google = fetch_google_finance_snapshot(ticker=args.ticker, exchange=args.google_exchange, asof=args.asof)

    save_snapshot(roic, args.snapshot_dir)
    save_snapshot(google, args.snapshot_dir)

    gold = merge_gold_sources(roic.values, google.values)
    errors = compute_errors_per_indicator(predicted, gold)

    rows = build_table2(predicted, gold, errors)

    if args.out_csv:
        out_csv = Path(args.out_csv)
    else:
        out_csv = Path("data/benchmarks/table2") / f"table2_{args.ticker}_{args.asof}.csv"

    write_table2_csv(rows, out_csv)
    print(f"Wrote: {out_csv}")


if __name__ == "__main__":
    main()
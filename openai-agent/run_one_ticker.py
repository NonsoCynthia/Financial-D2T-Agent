#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run selected tickers through the final_report2025 agent/workflow pipeline."
    )
    ticker_group = parser.add_mutually_exclusive_group(required=True)
    ticker_group.add_argument("--ticker", help="Single ticker to run (e.g., TSLA).")
    ticker_group.add_argument(
        "--tickers",
        help="Comma-separated tickers to run (e.g., TSLA,AAPL,MSFT).",
    )
    ticker_group.add_argument(
        "--all-tickers",
        action="store_true",
        help="Run all configured tickers from config.",
    )
    parser.add_argument(
        "--mode",
        choices=["agent", "workflow", "both"],
        default="workflow",
        help="Pipeline mode to run.",
    )
    parser.add_argument(
        "--model",
        default="gpt-5-mini",
        help="Model name from experiments.Model enum values.",
    )
    parser.add_argument(
        "--reflection",
        action="store_true",
        help="Enable reflection guardrail pass.",
    )
    parser.add_argument(
        "--n-times",
        type=int,
        default=1,
        help="Number of attempts per ticker.",
    )
    parser.add_argument(
        "--max-turns",
        type=int,
        default=30,
        help="Max turns per run.",
    )
    parser.add_argument(
        "--write-folder",
        default="result/debug",
        help="Optional output root folder override.",
    )
    parser.add_argument(
        "--reasoning",
        choices=["low", "medium", "high"],
        default="medium",
        help="Deprecated: analyst reasoning is fixed to medium by policy.",
    )
    parser.add_argument(
        "--verbosity",
        choices=["low", "medium", "high"],
        default="medium",
        help="Verbosity level for the run.",
    )
    parser.add_argument(
        "--allow-unknown-tickers",
        action="store_true",
        help="Allow tickers not preconfigured in final_report2025 config (useful for EU/custom symbols).",
    )
    mcp_group = parser.add_mutually_exclusive_group()
    mcp_group.add_argument(
        "--mcp",
        dest="use_mcp",
        action="store_true",
        help="Enable MCP server integration for agent mode.",
    )
    mcp_group.add_argument(
        "--no-mcp",
        dest="use_mcp",
        action="store_false",
        help="Disable MCP server integration for agent mode.",
    )
    parser.set_defaults(use_mcp=None)
    args = parser.parse_args()

    if args.n_times < 1:
        parser.error("--n-times must be >= 1")
    if args.max_turns < 1:
        parser.error("--max-turns must be >= 1")

    return args


def load_env(project_root: Path) -> None:
    # Explicit dotenv paths avoid python-dotenv frame inspection issues.
    from dotenv import load_dotenv

    candidates = [
        project_root / ".env",
        project_root.parent / ".env",
        Path.cwd() / ".env",
    ]
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        load_dotenv(dotenv_path=resolved, override=False)


def main() -> int:
    args = parse_args()

    openai_agent_dir = Path(__file__).resolve().parent
    project_root = openai_agent_dir.parent

    load_env(project_root=project_root)

    from experiments import ExperimentMetadata, Intensity, Model
    from experiments import StockInput
    import experiments.final_report2025.config as config_mod
    from financial_agents.us_indicator_schema import IndicatorOutput

    configured_by_ticker = {s.stock_id.upper(): s for s in config_mod.STOCKS}
    requested_tickers: list[str]
    if args.all_tickers:
        requested_tickers = sorted(configured_by_ticker.keys())
    elif args.ticker:
        requested_tickers = [args.ticker.strip().upper()]
    else:
        requested_tickers = [t.strip().upper() for t in (args.tickers or "").split(",") if t.strip()]

    if not requested_tickers:
        print("No tickers provided.", file=sys.stderr)
        return 2

    missing = [t for t in requested_tickers if t not in configured_by_ticker]
    if missing and not args.allow_unknown_tickers:
        available = ", ".join(sorted(configured_by_ticker.keys()))
        print(
            f"Unknown ticker(s): {', '.join(missing)}. Available tickers: {available}",
            file=sys.stderr,
        )
        return 2

    selected = []
    for t in requested_tickers:
        if t in configured_by_ticker:
            selected.append(configured_by_ticker[t])
            continue
        selected.append(
            StockInput(
                name=t,
                cnpj="N/A",
                stock_id=t,
            )
        )

    try:
        model = Model(args.model)
    except ValueError:
        options = ", ".join(m.value for m in Model)
        print(
            f"Invalid --model '{args.model}'. Available model values: {options}",
            file=sys.stderr,
        )
        return 2

    # Keep imports in sync and limit run to selected tickers.
    config_mod.STOCKS[:] = selected

    write_folder = args.write_folder.strip()
    if not write_folder:
        write_folder = str(project_root / "results" / "final_report2025_us_test_one_ticker")

    if args.reasoning != "medium":
        print("Ignoring --reasoning value; analyst reasoning is fixed to medium.")

    experiment = ExperimentMetadata(
        model=model,
        write_folder=write_folder,
        max_turns=args.max_turns,
        structured_output=IndicatorOutput.model_json_schema(),
        reasoning=Intensity.MEDIUM,
        verbosity=Intensity(args.verbosity),
        reflection=bool(args.reflection),
    )

    modes = [args.mode] if args.mode != "both" else ["workflow", "agent"]
    for mode in modes:
        if mode == "agent":
            use_mcp_agent = True if args.use_mcp is None else bool(args.use_mcp)
            os.environ["USE_MCP_AGENT"] = "1" if use_mcp_agent else "0"
        else:
            os.environ.pop("USE_MCP_AGENT", None)

        if mode == "workflow":
            import experiments.final_report2025.workflow as run_mod
        else:
            import experiments.final_report2025.agent as run_mod

        if hasattr(run_mod, "STOCKS") and isinstance(run_mod.STOCKS, list):
            run_mod.STOCKS[:] = selected

        print(
            f"Running mode={mode} tickers={','.join(requested_tickers)} model={model.value} "
            f"reflection={args.reflection} n_times={args.n_times} max_turns={args.max_turns}"
        )
        run_mod.run(experiment_metadata=experiment, n_times=args.n_times)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

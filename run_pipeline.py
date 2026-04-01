#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import date
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parent
AGENT_DIR = PROJECT_ROOT / "openai-agent"
RUN_ONE = AGENT_DIR / "run_one_ticker.py"
EVAL_PY = AGENT_DIR / "experiments" / "final_report2025" / "evaluate.py"
ROIC_GOLD_BUILDER = PROJECT_ROOT / "scripts" / "09_build_roic_gold_benchmark.py"
EU_PIPELINE = PROJECT_ROOT / "scripts_eu" / "run_eu_pipeline.py"

EU_DEFAULT_TICKERS = "KRZ.IR,A5G.IR,BIRG.IR,ASML.AS,SAP.DE,MC.PA,NOVO-B.CO,SIE.DE,OR.PA,NESN.SW"
TODAY_YMD = date.today().isoformat()


def suffix_path(path: Optional[str], suffix: str) -> str:
    if not path:
        return ""

    src = Path(path)
    if src.suffix:
        return str(src.with_name(f"{src.stem}_{suffix}{src.suffix}"))
    return str(src.with_name(f"{src.name}_{suffix}"))


def safe_slug(value: str) -> str:
    text = str(value or "").strip()
    for char in ("/", "\\", " "):
        text = text.replace(char, "_")
    return text


def table2_output_path(
    validation_region_dir: Path,
    region: str,
    kind: str,
    model: str,
    arch: str,
    reflection: str,
) -> str:
    model_slug = safe_slug(model)
    return str(validation_region_dir / f"table2_{region}_{kind}_{model_slug}_{arch}_{reflection}.csv")


def default_pred_folder_for_arch(write_folder: str, model: str, arch: str, reflection_bool: str) -> str:
    return str(Path(write_folder) / model / f"{arch}_{reflection_bool}")


def resolve_path(value: Optional[str], base_dir: Path) -> Optional[str]:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def _canonical_path(value: str) -> str:
    try:
        return str(Path(value).resolve())
    except Exception:
        return value


def prepare_openai_agent_env(env: dict[str, str]) -> dict[str, str]:
    clean_env = env.copy()
    current = clean_env.get("PYTHONPATH", "")
    project_root = _canonical_path(str(PROJECT_ROOT))
    agent_dir = str(AGENT_DIR.resolve())

    kept_entries: list[str] = []
    seen: set[str] = set()
    for entry in current.split(os.pathsep):
        if not entry:
            continue
        canonical = _canonical_path(entry)
        if canonical == project_root:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        kept_entries.append(entry)

    if agent_dir not in {_canonical_path(entry) for entry in kept_entries}:
        kept_entries.insert(0, agent_dir)

    clean_env["PYTHONPATH"] = os.pathsep.join(kept_entries)
    return clean_env


def print_command(cmd: list[str]) -> None:
    print(f"+ {shlex.join(cmd)}")


def run_command(cmd: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    print_command(cmd)
    subprocess.run(cmd, check=True, cwd=str(cwd), env=env)


def derive_gold_max_date(csv_path: str) -> str:
    import pandas as pd

    df = pd.read_csv(csv_path, low_memory=False)
    norm = {str(c).strip().lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    date_col = None
    for cand in ("date", "asofdate", "asof", "as_of_date", "targetdate"):
        key = cand.replace("_", "")
        if key in norm:
            date_col = norm[key]
            break
    if date_col is None:
        return ""

    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if dates.empty:
        return ""
    return dates.max().strftime("%Y-%m-%d")


def find_latest_roic_dump_dir(base_dir: Path) -> Optional[Path]:
    candidates = sorted(
        path for path in base_dir.iterdir()
        if path.is_dir() and fnmatch(path.name, "roic_*_json_dumps")
    )
    return candidates[-1] if candidates else None


def ensure_parent(path: Optional[str]) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the final_report2025 analysis/evaluation pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--region", choices=["us", "eu"], default="us")
    parser.add_argument("--run-eu-pipeline", action="store_true")
    parser.add_argument("--analysis-only", dest="run_eval", action="store_false")
    parser.add_argument("--eval-only", dest="run_analysis", action="store_false")
    parser.set_defaults(run_analysis=True, run_eval=True)

    parser.add_argument("--mode", choices=["agent", "workflow", "both"], default="both")
    parser.add_argument("--model", default="gpt-5-mini")
    parser.add_argument("--ticker")
    parser.add_argument("--tickers")
    parser.add_argument("--all-tickers", action="store_true")
    parser.add_argument("--n-times", type=int, default=1)
    parser.add_argument("--max-turns", type=int, default=30)
    parser.add_argument("--reasoning", choices=["low", "medium", "high"], default="medium")
    parser.add_argument("--verbosity", choices=["low", "medium", "high"], default="medium")
    parser.add_argument("--reflection", dest="reflection", action="store_true")
    parser.add_argument("--no-reflection", dest="reflection", action="store_false")
    parser.set_defaults(reflection=False)

    mcp_group = parser.add_mutually_exclusive_group()
    mcp_group.add_argument("--mcp", dest="mcp_flag", action="store_const", const="--mcp")
    mcp_group.add_argument("--no-mcp", dest="mcp_flag", action="store_const", const="--no-mcp")
    parser.set_defaults(mcp_flag="")

    parser.add_argument("--write-folder")
    parser.add_argument("--analysis-start-date")
    parser.add_argument("--analysis-end-date")
    parser.add_argument("--inter-run-sleep-seconds")

    parser.add_argument("--eval-mode", choices=["summary", "folder", "table2", "none"], default="summary")
    parser.add_argument("--results-root")
    parser.add_argument("--pred-folder")
    parser.add_argument("--gold-csv")
    parser.add_argument("--gold-benchmark-csv")
    parser.add_argument("--gold-date-match", default=os.getenv("GOLD_DATE_MATCH", "exact"))
    parser.add_argument(
        "--gold-source-priority",
        default=os.getenv("GOLD_SOURCE_PRIORITY", "roic.ai,gurufocus,yahoo,gold"),
    )
    parser.add_argument("--gold-fixed-date", default=os.getenv("GOLD_FIXED_DATE", ""))
    parser.add_argument("--ratio-clip-quantile", default=os.getenv("RATIO_CLIP_QUANTILE", "0.0"))
    parser.add_argument("--out-csv")
    parser.add_argument("--summary-out-csv")
    parser.add_argument("--build-roic-gold", dest="auto_build_roic_gold", action="store_true")
    parser.add_argument("--no-build-roic-gold", dest="auto_build_roic_gold", action="store_false")
    parser.set_defaults(auto_build_roic_gold=True)
    parser.add_argument("--cap-analysis-to-gold", dest="auto_cap_analysis_to_gold", action="store_true")
    parser.add_argument("--no-cap-analysis-to-gold", dest="auto_cap_analysis_to_gold", action="store_false")
    parser.set_defaults(auto_cap_analysis_to_gold=True)
    parser.add_argument("--roic-dumps-dir")
    parser.add_argument("--roic-gold-report-json")
    parser.add_argument("--roic-source-name", default="roic.ai")
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.ticker and args.tickers:
        parser.error("--ticker and --tickers cannot be used together.")
    if args.all_tickers and (args.ticker or args.tickers):
        parser.error("--all-tickers cannot be combined with --ticker or --tickers.")
    if args.n_times < 1:
        parser.error("--n-times must be >= 1.")
    if args.max_turns < 1:
        parser.error("--max-turns must be >= 1.")


def resolve_ticker_args(args: argparse.Namespace) -> list[str]:
    if args.region == "eu":
        if args.ticker:
            return ["--ticker", args.ticker]
        if args.tickers:
            return ["--tickers", args.tickers]
        return ["--tickers", EU_DEFAULT_TICKERS]

    if args.ticker:
        return ["--ticker", args.ticker]
    if args.tickers:
        return ["--tickers", args.tickers]
    if args.all_tickers:
        return ["--all-tickers"]
    return []


def resolve_defaults(args: argparse.Namespace) -> argparse.Namespace:
    args.out_csv_user_set = args.out_csv is not None
    args.summary_out_csv_user_set = args.summary_out_csv is not None

    if not args.write_folder:
        if args.region == "eu":
            args.write_folder = str((PROJECT_ROOT / "results" / "final_report2025_eu").resolve())
        else:
            args.write_folder = str((PROJECT_ROOT / "results" / "final_report2025_us").resolve())
    else:
        args.write_folder = resolve_path(args.write_folder, AGENT_DIR)

    if not args.gold_csv:
        if args.region == "eu":
            args.gold_csv = str((PROJECT_ROOT / "data_eu" / "processed" / "panel" / "daily_panel_prices_returns_fundamentals.csv").resolve())
        else:
            args.gold_csv = str((PROJECT_ROOT / "data" / "processed" / "panel" / "daily_panel_prices_returns_fundamentals.csv").resolve())
    else:
        args.gold_csv = resolve_path(args.gold_csv, AGENT_DIR)

    args.results_root = resolve_path(args.results_root, AGENT_DIR)
    args.pred_folder = resolve_path(args.pred_folder, AGENT_DIR)
    args.gold_benchmark_csv = resolve_path(args.gold_benchmark_csv, AGENT_DIR)
    args.roic_dumps_dir = resolve_path(args.roic_dumps_dir, AGENT_DIR)
    args.roic_gold_report_json = resolve_path(args.roic_gold_report_json, AGENT_DIR)
    args.out_csv = resolve_path(args.out_csv, AGENT_DIR)
    args.summary_out_csv = resolve_path(args.summary_out_csv, AGENT_DIR)

    validation_region_dir = (PROJECT_ROOT / "results" / "validation" / args.region).resolve()
    validation_region_dir.mkdir(parents=True, exist_ok=True)
    args.validation_region_dir = validation_region_dir

    if not args.out_csv:
        if args.eval_mode == "summary":
            args.out_csv = str(validation_region_dir / f"table1_{args.region}_summary.csv")
        elif args.eval_mode == "folder":
            args.out_csv = str(validation_region_dir / f"folder_{args.region}_rows.csv")
        elif args.eval_mode == "table2" and args.mode in {"workflow", "agent"}:
            args.out_csv = table2_output_path(
                validation_region_dir,
                args.region,
                "rows",
                args.model,
                args.mode,
                "true" if args.reflection else "false",
            )

    if args.eval_mode == "table2" and not args.summary_out_csv and args.mode in {"workflow", "agent"}:
        args.summary_out_csv = table2_output_path(
            validation_region_dir,
            args.region,
            "summary",
            args.model,
            args.mode,
            "true" if args.reflection else "false",
        )

    return args


def resolve_table2_gold_inputs(args: argparse.Namespace) -> argparse.Namespace:
    need_table2_assets = args.eval_mode == "table2" and (
        args.run_eval or (args.run_analysis and args.auto_cap_analysis_to_gold)
    )
    if not need_table2_assets:
        return args

    if not args.gold_benchmark_csv:
        if args.region == "eu":
            args.gold_benchmark_csv = str(
                (PROJECT_ROOT / "data_eu" / "processed" / "benchmarks" / f"roic_gold_benchmark_{TODAY_YMD}.csv").resolve()
            )
        else:
            args.gold_benchmark_csv = str(
                (PROJECT_ROOT / "data" / "processed" / "benchmarks" / f"roic_gold_benchmark_{TODAY_YMD}.csv").resolve()
            )

    if args.auto_build_roic_gold and not Path(args.gold_benchmark_csv).is_file():
        if not args.roic_dumps_dir:
            if args.region == "eu":
                roic_primary_dir = PROJECT_ROOT / "data_eu" / "roic_json_dumps_monthly_last_year"
                roic_fallback_base = PROJECT_ROOT / "data_eu"
            else:
                roic_primary_dir = PROJECT_ROOT / "data" / "roic_json_dumps_monthly_last_year"
                roic_fallback_base = PROJECT_ROOT / "data"

            if roic_primary_dir.is_dir():
                args.roic_dumps_dir = str(roic_primary_dir.resolve())
            elif roic_fallback_base.is_dir():
                latest = find_latest_roic_dump_dir(roic_fallback_base)
                if latest is not None:
                    args.roic_dumps_dir = str(latest.resolve())

        if not args.roic_gold_report_json:
            gold_path = Path(args.gold_benchmark_csv)
            if gold_path.suffix == ".csv":
                args.roic_gold_report_json = str(gold_path.with_suffix("").with_name(f"{gold_path.stem}_report").with_suffix(".json"))
            else:
                args.roic_gold_report_json = f"{args.gold_benchmark_csv}_report.json"

        if not args.roic_dumps_dir or not Path(args.roic_dumps_dir).is_dir():
            raise SystemExit(
                "Table2 gold CSV not found and no ROIC dump directory was found. "
                "Provide --gold-benchmark-csv or --roic-dumps-dir."
            )

    return args


def maybe_build_table2_gold(args: argparse.Namespace, env: dict[str, str]) -> None:
    need_table2_assets = args.eval_mode == "table2" and (
        args.run_eval or (args.run_analysis and args.auto_cap_analysis_to_gold)
    )
    if not need_table2_assets:
        return

    if args.auto_build_roic_gold and not Path(args.gold_benchmark_csv).is_file():
        ensure_parent(args.gold_benchmark_csv)
        ensure_parent(args.roic_gold_report_json)
        cmd = [
            sys.executable,
            str(ROIC_GOLD_BUILDER),
            "--in-dir",
            args.roic_dumps_dir,
            "--out-csv",
            args.gold_benchmark_csv,
            "--report-json",
            args.roic_gold_report_json,
            "--source-name",
            args.roic_source_name,
        ]
        run_command(cmd, cwd=AGENT_DIR, env=env)

    if not Path(args.gold_benchmark_csv).is_file():
        raise SystemExit(f"Table2 gold benchmark CSV not found: {args.gold_benchmark_csv}")


def apply_runtime_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()

    if args.region == "eu":
        env["US_DB_PATH"] = str((PROJECT_ROOT / "data_eu" / "processed" / "mcp" / "fundamental_analysis.db").resolve())
    else:
        env.pop("US_DB_PATH", None)

    if args.analysis_start_date:
        env["ANALYSIS_START_DATE"] = args.analysis_start_date
        print(f"Using ANALYSIS_START_DATE={env['ANALYSIS_START_DATE']}")

    if args.analysis_end_date:
        env["ANALYSIS_END_DATE"] = args.analysis_end_date
        print(f"Using ANALYSIS_END_DATE={env['ANALYSIS_END_DATE']}")

    if args.inter_run_sleep_seconds:
        env["INTER_RUN_SLEEP_SECONDS"] = args.inter_run_sleep_seconds
        print(f"Using INTER_RUN_SLEEP_SECONDS={env['INTER_RUN_SLEEP_SECONDS']}")

    return env


def maybe_cap_analysis_to_gold(args: argparse.Namespace, env: dict[str, str]) -> None:
    if not (args.run_analysis and args.eval_mode == "table2" and args.auto_cap_analysis_to_gold):
        return
    if not args.gold_benchmark_csv or not Path(args.gold_benchmark_csv).is_file():
        return

    gold_max_date = derive_gold_max_date(args.gold_benchmark_csv)
    if not gold_max_date:
        print(f"Warning: unable to derive max date from {args.gold_benchmark_csv}; skipping analysis-date cap.")
        return

    current_end = env.get("ANALYSIS_END_DATE", TODAY_YMD)
    if current_end > gold_max_date:
        env["ANALYSIS_END_DATE"] = gold_max_date
        print(f"Capping ANALYSIS_END_DATE to gold max date: {env['ANALYSIS_END_DATE']}")
    elif "ANALYSIS_END_DATE" not in env:
        env["ANALYSIS_END_DATE"] = gold_max_date
        print(f"Setting ANALYSIS_END_DATE to gold max date: {env['ANALYSIS_END_DATE']}")


def print_run_plan(args: argparse.Namespace, ticker_args: list[str], env: dict[str, str]) -> None:
    reflection_bool = "True" if args.reflection else "False"
    reflection_lower = reflection_bool.lower()
    analysis_start_display = env.get("ANALYSIS_START_DATE", "2025-01-01")
    analysis_end_display = env.get("ANALYSIS_END_DATE", TODAY_YMD)
    inter_run_sleep_display = env.get("INTER_RUN_SLEEP_SECONDS", "10")

    print(
        f"Run plan: region={args.region} mode={args.mode} model={args.model} "
        f"n_times={args.n_times} max_turns={args.max_turns} eval_mode={args.eval_mode}"
    )
    print(
        f"Run plan: analysis_window={analysis_start_display}..{analysis_end_display} "
        f"inter_run_sleep_seconds={inter_run_sleep_display} reflection={reflection_bool}"
    )
    print("Run plan: reasoning_policy=analyst:medium manager:high")

    if args.eval_mode == "table2":
        print(f"Run plan: table2_ratio_clip_quantile={args.ratio_clip_quantile}")
        print(f"Run plan: table2_gold_benchmark_csv={args.gold_benchmark_csv}")
        if args.mode == "both":
            print(
                "Run plan: table2_out_csv=per-arch auto naming "
                f"(table2_{args.region}_{{rows|summary}}_{{workflow|agent}}_{reflection_lower}.csv)"
            )
        else:
            print(f"Run plan: table2_out_csv={args.out_csv}")
            print(f"Run plan: table2_summary_out_csv={args.summary_out_csv}")
        print(f"Run plan: table2_auto_build_roic_gold={1 if args.auto_build_roic_gold else 0}")
        print(f"Run plan: table2_cap_analysis_to_gold={1 if args.auto_cap_analysis_to_gold else 0}")
        if args.roic_dumps_dir:
            print(f"Run plan: table2_roic_dumps_dir={args.roic_dumps_dir}")

    if ticker_args:
        print(f"Run plan: ticker_args={' '.join(ticker_args)}")


def run_analysis(args: argparse.Namespace, ticker_args: list[str], env: dict[str, str]) -> None:
    effective_ticker_args = ticker_args[:] if ticker_args else ["--all-tickers"]
    analysis_env = prepare_openai_agent_env(env)

    cmd = [
        sys.executable,
        str(RUN_ONE),
        *effective_ticker_args,
        "--mode",
        args.mode,
        "--model",
        args.model,
        "--n-times",
        str(args.n_times),
        "--max-turns",
        str(args.max_turns),
        "--reasoning",
        args.reasoning,
        "--verbosity",
        args.verbosity,
        "--write-folder",
        args.write_folder,
    ]

    if args.reflection:
        cmd.append("--reflection")
    if args.region == "eu":
        cmd.append("--allow-unknown-tickers")
    if args.mcp_flag:
        cmd.append(args.mcp_flag)

    run_command(cmd, cwd=AGENT_DIR, env=analysis_env)


def run_summary_eval(args: argparse.Namespace, env: dict[str, str]) -> None:
    results_root = args.results_root or args.write_folder
    ensure_parent(args.out_csv)
    eval_env = prepare_openai_agent_env(env)
    cmd = [
        sys.executable,
        str(EVAL_PY),
        "--mode",
        "summary",
        "--results-root",
        results_root,
        "--gold-csv",
        args.gold_csv,
    ]
    if args.out_csv:
        cmd.extend(["--out-csv", args.out_csv])
    run_command(cmd, cwd=AGENT_DIR, env=eval_env)


def run_eval_one(
    args: argparse.Namespace,
    *,
    label: str,
    pred_folder: str,
    out_csv: Optional[str],
    summary_out_csv: Optional[str],
    env: dict[str, str],
) -> None:
    eval_env = prepare_openai_agent_env(env)
    if args.eval_mode == "folder":
        ensure_parent(out_csv)
        cmd = [
            sys.executable,
            str(EVAL_PY),
            "--mode",
            "folder",
            "--pred-folder",
            pred_folder,
            "--gold-csv",
            args.gold_csv,
        ]
        if out_csv:
            cmd.extend(["--out-csv", out_csv])
    elif args.eval_mode == "table2":
        ensure_parent(out_csv)
        ensure_parent(summary_out_csv)
        cmd = [
            sys.executable,
            str(EVAL_PY),
            "--mode",
            "table2",
            "--pred-folder",
            pred_folder,
            "--gold-benchmark-csv",
            args.gold_benchmark_csv,
            "--date-match",
            args.gold_date_match,
            "--source-priority",
            args.gold_source_priority,
            "--ratio-clip-quantile",
            str(args.ratio_clip_quantile),
        ]
        if args.gold_fixed_date:
            cmd.extend(["--fixed-date", args.gold_fixed_date])
        if out_csv:
            cmd.extend(["--out-csv", out_csv])
        if summary_out_csv:
            cmd.extend(["--summary-out-csv", summary_out_csv])
    else:
        raise SystemExit(f"Internal error: unsupported per-folder eval mode '{args.eval_mode}'")

    print(f"Evaluating {label}: {pred_folder}")
    run_command(cmd, cwd=AGENT_DIR, env=eval_env)


def run_folder_or_table2_eval(args: argparse.Namespace, env: dict[str, str]) -> None:
    reflection_bool = "True" if args.reflection else "False"
    reflection_lower = reflection_bool.lower()

    if args.pred_folder:
        pred_folder = args.pred_folder
        if not Path(pred_folder).is_dir():
            raise SystemExit(f"Prediction folder not found: {pred_folder}")
        run_eval_one(
            args,
            label="custom",
            pred_folder=pred_folder,
            out_csv=args.out_csv,
            summary_out_csv=args.summary_out_csv,
            env=env,
        )
        return

    if args.mode == "both":
        for arch in ("workflow", "agent"):
            pred_folder_arch = default_pred_folder_for_arch(args.write_folder, args.model, arch, reflection_bool)
            if not Path(pred_folder_arch).is_dir():
                print(f"Skipping {arch} evaluation: folder not found at {pred_folder_arch}")
                continue

            if args.eval_mode == "table2":
                if args.out_csv_user_set and args.out_csv:
                    out_arch = suffix_path(args.out_csv, f"{safe_slug(args.model)}_{arch}_{reflection_lower}")
                else:
                    out_arch = table2_output_path(
                        args.validation_region_dir,
                        args.region,
                        "rows",
                        args.model,
                        arch,
                        reflection_lower,
                    )

                if args.summary_out_csv_user_set and args.summary_out_csv:
                    summary_arch = suffix_path(
                        args.summary_out_csv,
                        f"{safe_slug(args.model)}_{arch}_{reflection_lower}",
                    )
                else:
                    summary_arch = table2_output_path(
                        args.validation_region_dir,
                        args.region,
                        "summary",
                        args.model,
                        arch,
                        reflection_lower,
                    )
            else:
                out_arch = suffix_path(args.out_csv, arch)
                summary_arch = suffix_path(args.summary_out_csv, arch)

            run_eval_one(
                args,
                label=arch,
                pred_folder=pred_folder_arch,
                out_csv=out_arch,
                summary_out_csv=summary_arch,
                env=env,
            )
        return

    arch = args.mode
    pred_folder_arch = default_pred_folder_for_arch(args.write_folder, args.model, arch, reflection_bool)
    if not Path(pred_folder_arch).is_dir():
        raise SystemExit(f"Prediction folder not found: {pred_folder_arch}")

    run_eval_one(
        args,
        label=arch,
        pred_folder=pred_folder_arch,
        out_csv=args.out_csv,
        summary_out_csv=args.summary_out_csv,
        env=env,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    args = resolve_defaults(args)
    args = resolve_table2_gold_inputs(args)

    if not args.run_analysis and not args.run_eval:
        print("Nothing to do: both analysis and evaluation are disabled.")
        return 0

    env = apply_runtime_env(args)
    maybe_build_table2_gold(args, env)
    maybe_cap_analysis_to_gold(args, env)

    ticker_args = resolve_ticker_args(args)
    print_run_plan(args, ticker_args, env)

    if args.region == "eu" and args.run_eu_pipeline:
        run_command([sys.executable, str(EU_PIPELINE), "--all"], cwd=AGENT_DIR, env=env)

    if args.run_analysis:
        run_analysis(args, ticker_args, env)

    if args.run_eval and args.eval_mode != "none":
        if args.eval_mode == "summary":
            run_summary_eval(args, env)
        else:
            run_folder_or_table2_eval(args, env)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

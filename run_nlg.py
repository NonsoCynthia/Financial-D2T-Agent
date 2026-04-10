#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DEFAULT_DATASET_PATH = "results/final_report2025_us"
ALLOWED_NLG_RESULTS_ROOTS = (
    (PROJECT_ROOT / "results" / "final_report2025_us").resolve(),
    (PROJECT_ROOT / "results" / "final_report2025_eu").resolve(),
)
VALID_WORKFLOWS = (
    "default",
    "unified_worker",
    "no_orchestrator_no_guardrail_no_finalizer",
    "no_orchestrator_no_finalizer",
    "no_guardrail_no_finalizer",
    "e2e",
)
VALID_LANGUAGES = ("en", "ga")
VALID_PROVIDERS = ("openai", "ollama", "anthropic", "groq", "hf", "huggingface", "aixplain")
VALID_DATASET_KINDS = ("auto", "financial_multi_stock_monthly")
SOURCE_MODEL_HINTS = ("gpt-5", "gpt-5-mini")
VALID_SOURCE_ARCHES = ("workflow", "agent")


def canonical_provider(value: str) -> str:
    lowered = value.strip().lower()
    if lowered == "huggingface":
        return "hf"
    return lowered


def parse_csv_tickers(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    tickers = [item.strip().upper() for item in value.split(",") if item.strip()]
    return tickers or None


def parse_model_kwargs(raw: Optional[str]) -> Optional[dict[str, Any]]:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid --model-kwargs-json: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("--model-kwargs-json must decode to a JSON object.")
    return payload


def safe_slug(value: str) -> str:
    text = (value or "sample").strip()
    for char in ("/", "\\", " "):
        text = text.replace(char, "_")
    return text


def resolve_cli_path(value: str) -> Path:
    raw = Path(value)
    if raw.is_absolute():
        return raw.resolve()
    return (PROJECT_ROOT / raw).resolve()


def write_text_report(directory: Path, prefix: str, sample_slug: str, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}_{safe_slug(sample_slug)}.txt"
    path.write_text((text or "").strip() + "\n", encoding="utf-8")
    return path


def result_paths(output_dir: Path, save_prefix: str, sample_slug: str) -> tuple[Path, Path]:
    safe_sample_slug = safe_slug(sample_slug)
    return (
        output_dir / f"{save_prefix}_{safe_sample_slug}.json",
        output_dir / f"{save_prefix}_{safe_sample_slug}.txt",
    )


def load_existing_report_text(
    output_dir: Path,
    save_prefix: str,
    sample_slug: str,
) -> tuple[Optional[str], Optional[Path]]:
    json_path, text_path = result_paths(output_dir, save_prefix, sample_slug)

    if json_path.is_file():
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload = None
        if isinstance(payload, dict):
            report_text = (
                payload.get("final_response")
                or payload.get("generated_text")
                or payload.get("report")
            )
            if isinstance(report_text, str) and report_text.strip():
                return report_text.strip(), json_path

    if text_path.is_file():
        try:
            report_text = text_path.read_text(encoding="utf-8").strip()
        except OSError:
            report_text = ""
        if report_text:
            return report_text, text_path

    return None, None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the monthly multi-stock report generator (M-SMRG) NLG workflows.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--list-samples", action="store_true", help="List available generation samples.")
    target.add_argument("--sample-id", type=int, help="Run a specific sample id.")
    target.add_argument(
        "--analysis-date",
        help="Run the sample for a specific analysis month. Use with --ticker for per-ticker selection when applicable.",
    )
    target.add_argument("--sequence", action="store_true", help="Run all loaded samples in chronological order.")

    parser.add_argument("--ticker", help="Target ticker when resolving a sample by --analysis-date.")
    parser.add_argument("--workflow", default="default", choices=VALID_WORKFLOWS)
    parser.add_argument("--provider", default="openai", choices=VALID_PROVIDERS)
    parser.add_argument(
        "--model",
        default="gpt-5",
        help="NLG model to use for the selected provider. Override anytime via --model.",
    )
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--model-kwargs-json", help="Extra JSON kwargs for e2e model runs.")
    parser.add_argument("--language", default="en", choices=VALID_LANGUAGES)
    parser.add_argument(
        "--dataset-path",
        default=DEFAULT_DATASET_PATH,
        help=(
            "Path inside results/final_report2025_us or results/final_report2025_eu used as the NLG source. "
            "When this points at a region or model root, use --source-model and optionally "
            "--source-arch/--source-reflection to select one upstream analysis branch."
        ),
    )
    parser.add_argument("--dataset-kind", default="auto", choices=VALID_DATASET_KINDS)
    parser.add_argument(
        "--source-model",
        help=(
            "Upstream run.sh model folder to read from when --dataset-path is a region or model root. "
            "This selects a source results directory such as results/final_report2025_us/gpt-5-mini, "
            "not the NLG generation model controlled by --model."
        ),
    )
    parser.add_argument(
        "--source-arch",
        choices=VALID_SOURCE_ARCHES,
        default="workflow",
        help="Upstream analysis branch to read when --dataset-path does not already point at workflow_*/agent_*.",
    )
    parser.add_argument("--source-reflection", dest="source_reflection", action="store_true")
    parser.add_argument("--source-no-reflection", dest="source_reflection", action="store_false")
    parser.set_defaults(source_reflection=False)
    parser.add_argument("--previous-reports-path")
    parser.add_argument("--filings-index-path")
    parser.add_argument("--roic-dumps-dir")
    parser.add_argument("--tickers", help="Comma-separated ticker filter, e.g. AAPL,TSLA,NVDA.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--max-months-per-ticker", type=int)
    parser.add_argument("--min-stocks-per-month", type=int, default=1)
    parser.add_argument("--max-iteration", type=int, default=100)
    parser.add_argument("--output-dir")
    parser.add_argument("--save-prefix")
    parser.add_argument("--catalog-limit", type=int, default=0)
    parser.add_argument("--no-save", dest="save", action="store_false")
    parser.set_defaults(save=True)
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.ticker and not args.analysis_date:
        parser.error("--ticker requires --analysis-date.")
    if args.sample_id and args.ticker:
        parser.error("--ticker cannot be combined with --sample-id.")
    source_info = resolve_dataset_source(parser, args)
    dataset_path = source_info["resolved_dataset_path"]
    if not any(_is_subpath(path=dataset_path, root=root) for root in ALLOWED_NLG_RESULTS_ROOTS):
        parser.error(
            "--dataset-path must point inside results/final_report2025_us "
            "or results/final_report2025_eu "
            "for the NLG runner."
        )
    if args.dataset_kind not in {"auto", "financial_multi_stock_monthly"}:
        parser.error(
            "--dataset-kind must be 'auto' or 'financial_multi_stock_monthly' "
            "for the NLG runner."
        )
    args.dataset_path = str(dataset_path)
    args.dataset_path_display = source_info["dataset_path_display"]
    args.source_region = source_info["source_region"]
    args.source_model = source_info["source_model"]
    args.source_branch = source_info["source_branch"]
    args.source_arch = source_info["source_arch"]
    args.source_reflection = source_info["source_reflection"]


def resolve_dataset_source(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> dict[str, Any]:
    dataset_path = resolve_cli_path(args.dataset_path)
    matched_root = next(
        (root for root in ALLOWED_NLG_RESULTS_ROOTS if _is_subpath(path=dataset_path, root=root)),
        None,
    )
    if matched_root is None:
        parser.error(
            "--dataset-path must point inside results/final_report2025_us "
            "or results/final_report2025_eu."
        )

    relative_parts = dataset_path.relative_to(matched_root).parts
    available_models = _available_source_models(matched_root)
    path_source_model = relative_parts[0] if relative_parts and relative_parts[0] in available_models else None
    path_source_branch = None
    if len(relative_parts) >= 2 and _parse_source_branch_name(relative_parts[1]) is not None:
        path_source_branch = relative_parts[1]

    if args.source_model and args.source_model not in available_models:
        if available_models:
            available_text = ", ".join(repr(name) for name in available_models)
        else:
            available_text = "none"
        nlg_hint = ""
        if str(args.source_model).startswith("gpt-"):
            nlg_hint = " If you meant the NLG generation model, pass it via --model instead."
        parser.error(
            f"--source-model {args.source_model!r} does not match an available upstream analysis folder under "
            f"{matched_root}. Available source models: {available_text}.{nlg_hint}"
        )

    if path_source_model and args.source_model and args.source_model != path_source_model:
        parser.error(
            f"--source-model {args.source_model!r} conflicts with --dataset-path "
            f"which already points inside model folder {path_source_model!r}."
        )

    source_model = path_source_model or args.source_model
    if source_model is None:
        if len(available_models) == 1:
            source_model = available_models[0]
        elif available_models:
            parser.error(
                "--dataset-path points at a region root with multiple model folders "
                f"{available_models}. Pass --source-model to choose one."
            )
        else:
            hinted_models = ", ".join(repr(name) for name in SOURCE_MODEL_HINTS)
            parser.error(
                f"No supported source model folders were found under {matched_root}. "
                "Expected folders that contain analysis branches like workflow_False or agent_True. "
                f"Common examples: {hinted_models}."
            )

    model_root = matched_root / source_model
    if not model_root.is_dir():
        parser.error(f"Source model folder not found: {model_root}")

    source_branch = path_source_branch
    if source_branch is None:
        wanted_branch = f"{args.source_arch}_{'True' if args.source_reflection else 'False'}"
        wanted_branch_root = model_root / wanted_branch
        if wanted_branch_root.is_dir():
            source_branch = wanted_branch
        else:
            available_branches = _available_source_branches(model_root)
            if len(available_branches) == 1:
                source_branch = available_branches[0]
            elif available_branches:
                parser.error(
                    f"Source model folder {model_root} does not contain {wanted_branch!r}. "
                    f"Available branches: {available_branches}. "
                    "Adjust --source-arch/--source-reflection or pass a deeper --dataset-path."
                )
            else:
                parser.error(
                    f"No supported analysis branch folders were found under {model_root}. "
                    "Expected folders like workflow_False or agent_True."
                )

    branch_info = _parse_source_branch_name(source_branch)
    if branch_info is None:
        parser.error(f"Unsupported source branch name: {source_branch}")
    source_arch, source_reflection = branch_info

    if dataset_path == matched_root or dataset_path == model_root:
        resolved_dataset_path = model_root / source_branch
    else:
        resolved_dataset_path = dataset_path

    try:
        dataset_path_display = str(resolved_dataset_path.relative_to(PROJECT_ROOT))
    except ValueError:
        dataset_path_display = str(resolved_dataset_path)

    return {
        "resolved_dataset_path": resolved_dataset_path,
        "dataset_path_display": dataset_path_display,
        "source_region": matched_root.name,
        "source_model": source_model,
        "source_branch": source_branch,
        "source_arch": source_arch,
        "source_reflection": source_reflection,
    }


def _is_subpath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _parse_source_branch_name(value: str) -> Optional[tuple[str, bool]]:
    match = re.match(r"^(workflow|agent)_(True|False)$", str(value).strip())
    if match is None:
        return None
    return match.group(1), match.group(2) == "True"


def _available_source_models(region_root: Path) -> list[str]:
    model_names: list[str] = []
    for child in sorted(region_root.iterdir()):
        if not child.is_dir():
            continue
        if _available_source_branches(child):
            model_names.append(child.name)
    return model_names


def _available_source_branches(model_root: Path) -> list[str]:
    branch_names: list[str] = []
    for child in sorted(model_root.iterdir()):
        if child.is_dir() and _parse_source_branch_name(child.name) is not None:
            branch_names.append(child.name)
    return branch_names


def configure_provider_model(provider: str, model_override: Optional[str], temperature: float) -> None:
    from agents.llm_model import model_name

    provider_conf = model_name.get(provider, {}).copy()
    if model_override:
        key = "model_id" if provider == "aixplain" else "model_name"
        provider_conf[key] = model_override
    if "temperature" in provider_conf or temperature != 0.0:
        provider_conf["temperature"] = temperature
    model_name[provider] = provider_conf


def get_effective_nlg_model_label(provider: str) -> str:
    from agents.llm_model import model_name

    provider_conf = model_name.get(provider, {})
    key = "model_id" if provider == "aixplain" else "model_name"
    raw_value = provider_conf.get(key) or provider
    return safe_slug(str(raw_value))


def build_output_dir(
    args: argparse.Namespace,
    provider: str,
    nlg_model_label: str,
) -> Path:
    if args.output_dir:
        return Path(args.output_dir).resolve()
    return (
        PROJECT_ROOT
        / "results"
        / "nlg"
        / args.source_region
        / args.source_model
        / args.source_branch
        / provider
        / nlg_model_label
        / args.language
        / args.workflow
    ).resolve()


def build_runner(args: argparse.Namespace, provider: str, output_dir: Path):
    from main import D2TAgentExperimentRunner

    return D2TAgentExperimentRunner(
        provider=provider,
        language=args.language,
        dataset_path=args.dataset_path,
        dataset_kind=args.dataset_kind,
        filings_index_path=args.filings_index_path,
        roic_dumps_dir=args.roic_dumps_dir,
        previous_reports_path=args.previous_reports_path,
        tickers=parse_csv_tickers(args.tickers),
        start_date=args.start_date,
        end_date=args.end_date,
        max_months_per_ticker=args.max_months_per_ticker,
        min_stocks_per_month=args.min_stocks_per_month,
        max_iteration=args.max_iteration,
        output_dir=str(output_dir),
    )


def list_samples(args: argparse.Namespace) -> int:
    from load_data import load_generation_samples

    samples = load_generation_samples(
        dataset_path=args.dataset_path,
        dataset_kind=args.dataset_kind,
        filings_index_path=args.filings_index_path,
        roic_dumps_dir=args.roic_dumps_dir,
        previous_reports_path=args.previous_reports_path,
        tickers=parse_csv_tickers(args.tickers),
        start_date=args.start_date,
        end_date=args.end_date,
        max_months_per_ticker=args.max_months_per_ticker,
        min_stocks_per_month=args.min_stocks_per_month,
    )

    print(
        f"Loaded {len(samples)} samples from {args.dataset_path_display} "
        f"(dataset_kind={args.dataset_kind}, source_model={args.source_model}, source_branch={args.source_branch})."
    )
    rows = samples if args.catalog_limit <= 0 else samples[: args.catalog_limit]
    if not rows:
        print("No samples found.")
        return 0

    header = f"{'ID':>4}  {'DATE':<10}  {'TICKERS':<7}  {'TICKER LIST':<40}  SAMPLE NAME"
    print(header)
    print("-" * len(header))
    for sample in rows:
        tickers_value = sample.get("tickers") or []
        ticker_list = ",".join(tickers_value) if isinstance(tickers_value, list) else str(sample.get("ticker", ""))
        print(
            f"{int(sample['sample_id']):>4}  "
            f"{str(sample.get('analysis_date', ''))[:10]:<10}  "
            f"{str(sample.get('ticker_count', len(tickers_value) or 1)):<7}  "
            f"{ticker_list[:40]:<40}  "
            f"{sample.get('sample_name', '')}"
        )
    return 0


def resolve_sample_id(runner, args: argparse.Namespace) -> int:
    if args.sample_id:
        return args.sample_id
    if args.analysis_date and args.ticker:
        return runner.find_sample_id(ticker=args.ticker, analysis_date=args.analysis_date)
    if args.analysis_date:
        return runner.find_month_sample_id(analysis_date=args.analysis_date)
    raise SystemExit("No runnable target resolved.")


def save_e2e_result(
    result: dict[str, Any],
    output_dir: Path,
    save_prefix: str,
) -> tuple[Path, Path]:
    from load_data import save_result_to_json

    sample_meta = result["sample_metadata"]
    sample_slug = safe_slug(str(sample_meta.get("sample_name", f"sample{sample_meta.get('sample_id', 'x')}")))
    json_filename = f"{save_prefix}_{sample_slug}.json"
    save_result_to_json(result, filename=json_filename, directory=str(output_dir))
    text_path = write_text_report(output_dir, save_prefix, sample_slug, str(result.get("generated_text", "")))
    return output_dir / json_filename, text_path


def save_workflow_text_result(
    state: dict[str, Any],
    output_dir: Path,
    save_prefix: str,
) -> Path:
    sample_meta = state.get("sample_metadata", {}) or {}
    sample_slug = safe_slug(str(sample_meta.get("sample_name", f"sample{sample_meta.get('sample_id', 'x')}")))
    return write_text_report(output_dir, save_prefix, sample_slug, str(state.get("final_response", "")))


def run_sequence(
    runner,
    args: argparse.Namespace,
    provider: str,
    output_dir: Path,
    save_prefix: str,
    model_kwargs: Optional[dict[str, Any]],
) -> int:
    from load_data import save_result_to_json

    run_config = {
        "provider": provider,
        "workflow": args.workflow,
        "language": args.language,
        "dataset_path": args.dataset_path,
        "dataset_kind": args.dataset_kind,
        "tickers": parse_csv_tickers(args.tickers) or [],
        "start_date": args.start_date,
        "end_date": args.end_date,
        "max_months_per_ticker": args.max_months_per_ticker,
        "min_stocks_per_month": args.min_stocks_per_month,
        "save_prefix": save_prefix,
        "output_dir": str(output_dir),
    }

    summary_rows: list[dict[str, Any]] = []
    previous_report: Optional[str] = None

    for sample in runner.samples:
        sample_id = int(sample["sample_id"])
        sample_name = str(sample.get("sample_name", f"sample{sample_id}"))
        existing_report, existing_path = load_existing_report_text(
            output_dir=output_dir,
            save_prefix=save_prefix,
            sample_slug=sample_name,
        )

        if existing_report is not None:
            print(
                f"Skipping sample_id={sample_id} (sample={sample_name}); "
                f"found existing output: {existing_path}"
            )
            previous_report = existing_report
            row: dict[str, Any] = {
                "sample_id": sample_id,
                "sample_name": sample.get("sample_name"),
                "analysis_date": sample.get("analysis_date"),
            }
            if args.workflow == "e2e":
                row["generated_text"] = existing_report
            else:
                row["final_response"] = existing_report
            summary_rows.append(row)
            continue

        if args.workflow == "e2e":
            result = runner.run_end_to_end(
                sample_id=sample_id,
                provider=provider,
                temperature=args.temperature,
                extra_model_kwargs=model_kwargs,
                previous_report=previous_report,
            )
            previous_report = str(result.get("generated_text", "")).strip() or previous_report
            sample_meta = result["sample_metadata"]
            if args.save:
                json_path, text_path = save_e2e_result(result, output_dir, save_prefix)
                print(f"Saved sample_id={sample_id} outputs:")
                print(f"- JSON result: {json_path}")
                print(f"- Text report: {text_path}")

            summary_rows.append(
                {
                    "sample_id": sample_meta.get("sample_id"),
                    "sample_name": sample_meta.get("sample_name"),
                    "analysis_date": sample_meta.get("analysis_date"),
                    "generated_text": result.get("generated_text", ""),
                }
            )
            continue

        state = runner.run_sample(
            sample_id=sample_id,
            workflow=args.workflow,
            save=args.save,
            save_prefix=save_prefix,
            previous_report=previous_report,
        )
        previous_report = str(state.get("final_response", "")).strip() or previous_report
        sample_meta = state.get("sample_metadata", {}) or sample
        if args.save:
            text_path = save_workflow_text_result(state, output_dir, save_prefix)
            print(f"Saved sample_id={sample_id} text report: {text_path}")

        summary_rows.append(
            {
                "sample_id": sample_id,
                "analysis_date": sample_meta.get("analysis_date"),
                "sample_name": sample_meta.get("sample_name"),
                "final_response": state.get("final_response", ""),
            }
        )

    if args.save:
        save_result_to_json(
            {"config": run_config, "results": summary_rows},
            filename=f"{save_prefix}_sequence_summary.json",
            directory=str(output_dir),
        )
    print(f"Completed {len(summary_rows)} sequence step(s).")
    for row in summary_rows:
        print(f"- {row['analysis_date']}: {row['sample_name']}")
    return 0


def _find_previous_month_report(
    output_dir: Path,
    analysis_date: str,
) -> Optional[str]:
    """
    Search the output directory for a previous month's NLG report to use as
    continuity context. Looks for JSON files from the month immediately before
    `analysis_date` and extracts the final report text.
    """
    from datetime import datetime, timedelta

    try:
        dt = datetime.strptime(str(analysis_date).strip()[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    # Compute previous month's last day
    first_of_current = dt.replace(day=1)
    prev_last_day = first_of_current - timedelta(days=1)
    prev_date_str = prev_last_day.strftime("%Y-%m-%d")

    if not output_dir.is_dir():
        return None

    # Search for JSON files containing the previous month's date
    for json_file in sorted(output_dir.glob("*.json")):
        if prev_date_str not in json_file.name:
            continue
        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        # Extract report text from known keys
        report_text = (
            payload.get("final_response")
            or payload.get("generated_text")
            or payload.get("report")
        )
        if report_text and isinstance(report_text, str) and len(report_text.strip()) > 50:
            print(f"[AUTO-CHAIN] Using previous month report from: {json_file.name}")
            return report_text.strip()

    # Also check .txt files
    for txt_file in sorted(output_dir.glob("*.txt")):
        if prev_date_str not in txt_file.name:
            continue
        try:
            text = txt_file.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if len(text) > 50:
            print(f"[AUTO-CHAIN] Using previous month report from: {txt_file.name}")
            return text

    return None


def run_single(
    runner,
    args: argparse.Namespace,
    provider: str,
    output_dir: Path,
    save_prefix: str,
    model_kwargs: Optional[dict[str, Any]],
) -> int:
    sample_id = resolve_sample_id(runner, args)
    sample_meta = runner.get_sample_metadata(sample_id)
    sample_slug = safe_slug(str(sample_meta.get("sample_name", f"sample{sample_id}")))

    # Auto-discover previous month's report for continuity context
    previous_report = _find_previous_month_report(
        output_dir,
        str(sample_meta.get("analysis_date", "")),
    )

    if args.workflow == "e2e":
        result = runner.run_end_to_end(
            sample_id=sample_id,
            provider=provider,
            temperature=args.temperature,
            extra_model_kwargs=model_kwargs,
            previous_report=previous_report,
        )
        if args.save:
            json_path, text_path = save_e2e_result(result, output_dir, save_prefix)
            print("Output files:")
            print(f"- JSON result: {json_path}")
            print(f"- Text report: {text_path}")
        print(result.get("generated_text", "").strip())
        return 0

    state = runner.run_sample(
        sample_id=sample_id,
        workflow=args.workflow,
        save=args.save,
        save_prefix=save_prefix,
        previous_report=previous_report,
    )
    if args.save:
        text_path = write_text_report(
            output_dir,
            save_prefix,
            sample_slug,
            str(state.get("final_response", "")),
        )
        json_path = state.get("saved_state_path")
        print("Output files:")
        if json_path:
            print(f"- JSON state: {json_path}")
        print(f"- Text report: {text_path}")
    print(str(state.get("final_response", "")).strip())
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    provider = canonical_provider(args.provider)
    model_kwargs = parse_model_kwargs(args.model_kwargs_json)

    if model_kwargs and args.workflow != "e2e":
        print(
            "Ignoring --model-kwargs-json for workflow-based runs; only the e2e path uses extra model kwargs directly.",
            file=sys.stderr,
        )

    if args.list_samples:
        return list_samples(args)

    configure_provider_model(provider, args.model, args.temperature)
    nlg_model_label = get_effective_nlg_model_label(provider)
    output_dir = build_output_dir(args, provider, nlg_model_label)
    save_prefix = args.save_prefix or safe_slug(
        f"{args.workflow}_{args.source_model}_{args.source_branch}_{nlg_model_label}"
    )
    print(
        f"Using source dataset {args.dataset_path_display} "
        f"(source_model={args.source_model}, source_branch={args.source_branch})."
    )
    print(
        f"NLG outputs will be written under {output_dir} "
        f"with save_prefix={save_prefix}."
    )
    runner = build_runner(args, provider, output_dir)

    if args.sequence:
        return run_sequence(runner, args, provider, output_dir, save_prefix, model_kwargs)
    return run_single(runner, args, provider, output_dir, save_prefix, model_kwargs)


if __name__ == "__main__":
    raise SystemExit(main())

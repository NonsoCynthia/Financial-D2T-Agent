#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional


PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DEFAULT_DATASET_PATH = "data_br/completo_gpt5mini"
DEFAULT_OUTPUT_DIR = "results/nlg_brazilian_manager"
VALID_PROVIDERS = ("openai", "ollama", "anthropic", "groq", "hf", "huggingface", "aixplain")
VALID_WORKFLOWS = (
    "default",
    "unified_worker",
    "no_orchestrator_no_guardrail_no_finalizer",
    "no_orchestrator_no_finalizer",
    "no_guardrail_no_finalizer",
    "e2e",
)


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
    raw = Path(value).expanduser()
    if raw.is_absolute():
        return raw.resolve()
    return (PROJECT_ROOT / raw).resolve()


def result_paths(output_dir: Path, save_prefix: str, sample_slug: str) -> tuple[Path, Path]:
    safe_prefix = safe_slug(save_prefix)
    safe_sample_slug = safe_slug(sample_slug)
    return (
        output_dir / f"{safe_prefix}_{safe_sample_slug}.json",
        output_dir / f"{safe_prefix}_{safe_sample_slug}.txt",
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
        description="Generate Brazilian Portuguese NLG reports from AIDA-BR manager outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--list-samples", action="store_true", help="List available monthly samples.")
    target.add_argument("--sample-id", type=int, help="Run one sample id.")
    target.add_argument("--analysis-date", help="Run one analysis date, e.g. 2025-12-01.")
    target.add_argument("--sequence", action="store_true", help="Run every loaded monthly sample.")

    parser.add_argument("--dataset-path", default=DEFAULT_DATASET_PATH)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workflow", default="default", choices=VALID_WORKFLOWS)
    parser.add_argument("--provider", default="openai", choices=VALID_PROVIDERS)
    parser.add_argument("--model", default="gpt-5")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--model-kwargs-json")
    parser.add_argument("--tickers", help="Comma-separated ticker filter, e.g. EQTL3,ENEV3.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--min-stocks-per-month", type=int, default=1)
    parser.add_argument("--catalog-limit", type=int, default=0)
    parser.add_argument("--save-prefix", default="pt_br_manager_report")
    parser.add_argument("--no-save", dest="save", action="store_false")
    parser.set_defaults(save=True)
    return parser


def load_samples(args: argparse.Namespace) -> list[dict[str, Any]]:
    from load_data_brazilian_manager import load_brazilian_manager_samples

    return load_brazilian_manager_samples(
        dataset_root=resolve_cli_path(args.dataset_path),
        tickers=parse_csv_tickers(args.tickers),
        start_date=args.start_date,
        end_date=args.end_date,
        min_stocks_per_month=args.min_stocks_per_month,
    )


def list_samples(args: argparse.Namespace, samples: list[dict[str, Any]]) -> int:
    dataset_path = resolve_cli_path(args.dataset_path)
    print(f"Loaded {len(samples)} Brazilian manager samples from {dataset_path}.")
    rows = samples if args.catalog_limit <= 0 else samples[: args.catalog_limit]
    if not rows:
        print("No samples found.")
        return 0

    header = f"{'ID':>4}  {'DATE':<10}  {'TICKERS':<7}  TICKER LIST"
    print(header)
    print("-" * len(header))
    for sample in rows:
        tickers = sample.get("tickers") or []
        ticker_list = ",".join(tickers) if isinstance(tickers, list) else ""
        print(
            f"{int(sample['sample_id']):>4}  "
            f"{str(sample.get('analysis_date', ''))[:10]:<10}  "
            f"{str(sample.get('ticker_count', len(tickers))):<7}  "
            f"{ticker_list}"
        )
    return 0


def resolve_samples_to_run(args: argparse.Namespace, samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if args.sequence:
        return samples
    if args.sample_id:
        for sample in samples:
            if int(sample["sample_id"]) == int(args.sample_id):
                return [sample]
        raise SystemExit(f"sample_id {args.sample_id} not found.")
    if args.analysis_date:
        matches = [sample for sample in samples if sample.get("analysis_date") == args.analysis_date]
        if matches:
            return matches
        raise SystemExit(f"analysis_date {args.analysis_date!r} not found.")
    raise SystemExit("No runnable target resolved.")


def build_model_config(args: argparse.Namespace) -> dict[str, Any]:
    from agents.llm_model import resolve_model_config

    provider = "hf" if args.provider == "huggingface" else args.provider
    conf = resolve_model_config(
        provider=provider,
        model_override=args.model,
        temperature=args.temperature,
        reasoning_effort=args.reasoning_effort,
    )
    extra = parse_model_kwargs(args.model_kwargs_json)
    if extra:
        conf.update(extra)
    return conf


def generate_report(args: argparse.Namespace, sample: dict[str, Any]) -> dict[str, Any]:
    from agents.llm_model import UnifiedModel, extract_text_output, model_label_from_config
    from agents.agent_prompts_brazilian_manager import PT_BR_MANAGER_REPORT_PROMPT

    provider = "hf" if args.provider == "huggingface" else args.provider
    conf = build_model_config(args)
    model_label = model_label_from_config(conf)
    prompt_chars = len(str(sample.get("prompt_context", "")))
    print(
        f"Invoking {provider}/{model_label or args.model} for e2e "
        f"with {sample.get('ticker_count', 'unknown')} stock(s), "
        f"prompt_chars={prompt_chars}. This may take a while."
    )
    llm = UnifiedModel(provider=provider, **conf).model_(PT_BR_MANAGER_REPORT_PROMPT)
    raw_output = llm.invoke({"input": sample["prompt_context"]})
    print("Model call completed; extracting and saving report.")
    generated_text = extract_text_output(raw_output)
    return {
        "generated_text": generated_text,
        "query": sample["prompt_context"],
        "sample_metadata": sample,
        "provider": provider,
        "model": model_label,
        "language": "pt-BR",
        "raw_output": repr(raw_output),
    }


class BrazilianManagerExperimentRunner:
    def __init__(self, args: argparse.Namespace, samples: list[dict[str, Any]], output_dir: Path) -> None:
        from main import D2TAgentExperimentRunner

        class _Runner(D2TAgentExperimentRunner):
            def _load_samples(self, path: str) -> list[dict[str, Any]]:
                return samples

        self.runner = _Runner(
            provider="hf" if args.provider == "huggingface" else args.provider,
            language="pt_br",
            dataset_path=str(resolve_cli_path(args.dataset_path)),
            dataset_kind="brazilian_manager_monthly",
            tickers=parse_csv_tickers(args.tickers),
            start_date=args.start_date,
            end_date=args.end_date,
            min_stocks_per_month=args.min_stocks_per_month,
            max_iteration=100,
            output_dir=str(output_dir),
        )


def run_workflow_report(
    args: argparse.Namespace,
    samples: list[dict[str, Any]],
    sample: dict[str, Any],
    previous_report: Optional[str],
) -> dict[str, Any]:
    output_dir = build_workflow_output_dir(args)
    wrapper = BrazilianManagerExperimentRunner(args=args, samples=samples, output_dir=output_dir)
    state = wrapper.runner.run_sample(
        sample_id=int(sample["sample_id"]),
        workflow=args.workflow,
        save=False,
        previous_report=previous_report,
    )
    final_text = str(state.get("final_response", "")).strip()
    return {
        "final_response": final_text,
        "generated_text": final_text,
        "sample_metadata": sample,
        "provider": "hf" if args.provider == "huggingface" else args.provider,
        "language": "pt-BR",
        "workflow": args.workflow,
        "token_usage": state.get("token_usage", {}),
        "state_summary": {
            "response": state.get("response"),
            "iteration_count": state.get("iteration_count"),
            "history_of_steps": state.get("history_of_steps", []),
        },
    }


def build_workflow_output_dir(args: argparse.Namespace) -> Path:
    return resolve_cli_path(args.output_dir) / safe_slug(args.workflow)


def save_report(args: argparse.Namespace, result: dict[str, Any]) -> tuple[Path, Path]:
    from load_data_brazilian_manager import save_result_to_json

    output_dir = build_workflow_output_dir(args)
    sample = result["sample_metadata"]
    sample_slug = safe_slug(str(sample.get("sample_name", f"sample{sample.get('sample_id', 'x')}")))
    prefix = safe_slug(args.save_prefix)
    json_path, text_path = result_paths(output_dir, prefix, sample_slug)

    json_path = save_result_to_json(
        result,
        filename=json_path.name,
        directory=output_dir,
    )
    text_path.write_text(str(result.get("generated_text", "")).strip() + "\n", encoding="utf-8")
    return json_path, text_path


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    samples = load_samples(args)

    if args.list_samples:
        return list_samples(args, samples)

    selected_samples = resolve_samples_to_run(args, samples)
    summary: list[dict[str, Any]] = []
    previous_report: Optional[str] = None
    for sample in selected_samples:
        sample_id = int(sample["sample_id"])
        sample_name = str(sample.get("sample_name", f"sample{sample_id}"))
        if args.sequence and args.save:
            existing_report, existing_path = load_existing_report_text(
                output_dir=build_workflow_output_dir(args),
                save_prefix=args.save_prefix,
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
                    "tickers": sample.get("tickers", []),
                }
                if args.workflow == "e2e":
                    row["generated_text"] = existing_report
                else:
                    row["final_response"] = existing_report
                    row["generated_text"] = existing_report
                summary.append(row)
                continue

        print(
            f"Generating pt-BR report for sample_id={sample_id} "
            f"date={sample['analysis_date']} workflow={args.workflow}."
        )
        if args.workflow == "e2e":
            result = generate_report(args, sample)
        else:
            result = run_workflow_report(args, samples=samples, sample=sample, previous_report=previous_report)
        previous_report = str(result.get("generated_text", "")).strip() or previous_report
        if args.save:
            json_path, text_path = save_report(args, result)
            print(f"Saved JSON: {json_path}")
            print(f"Saved text: {text_path}")
        summary.append(
            {
                "sample_id": sample["sample_id"],
                "analysis_date": sample["analysis_date"],
                "tickers": sample["tickers"],
                "generated_text": result["generated_text"],
            }
        )

    if args.sequence and args.save:
        from load_data_brazilian_manager import save_result_to_json

        save_result_to_json(
            {"results": summary},
            filename=f"{safe_slug(args.save_prefix)}_sequence_summary.json",
            directory=build_workflow_output_dir(args),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

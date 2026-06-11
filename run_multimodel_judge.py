"""
run_multimodel_judge.py

Runs GPT-5, Claude Sonnet 3.7, and Gemini 2.5 as LLM judges across all US NLG conditions.

GPT-5 results are loaded from the existing llm_judge/ folder (no re-run).
Claude and Gemini results are computed fresh and cached under llm_judge_multimodel/.

Outputs:
  results/validation/llm_judge_multimodel/{judge}/{condition}/  — per-sample JSONs
  results/validation/llm_judge_multimodel/summary_all_judges.csv — combined comparison
  results/validation/llm_judge_multimodel/paired_ttests.csv      — paired t-tests

Usage:
  /home/chinonso/anaconda3/bin/python3 run_multimodel_judge.py
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field
from scipy import stats

load_dotenv(Path.home() / ".env")
load_dotenv()

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from load_data import build_multi_stock_prompt_context, load_generation_samples

# ---------------------------------------------------------------------------
# Conditions to evaluate (mirrors the Results section in llm-as-judge.ipynb)
# ---------------------------------------------------------------------------

Condition = tuple[str, bool | None, str]

US_WORKFLOWS = [
    "default",
    "default_old",
    "e2e",
    "no_orchestrator_no_guardrail_no_finalizer",
    "no_orchestrator_no_finalizer",
    "no_guardrail_no_finalizer",
]

BR_WORKFLOWS = ["default", "e2e"]

CONDITIONS: list[Condition] = [
    ("nlg", False, "default"),
    ("nlg", False, "default_old"),
    ("nlg", False, "e2e"),
    ("nlg", True,  "default"),
    ("nlg", True,  "default_old"),
    ("nlg", True,  "e2e"),
    ("nlg", True,  "no_orchestrator_no_guardrail_no_finalizer"),
    ("nlg", True,  "no_orchestrator_no_finalizer"),
    ("nlg", True,  "no_guardrail_no_finalizer"),
    ("nlg_brazilian_manager", None, "default"),
    ("nlg_brazilian_manager", None, "e2e"),
]

# ---------------------------------------------------------------------------
# Judge configuration
# ---------------------------------------------------------------------------

OVERWRITE = False
MAX_RETRIES = 3

JUDGES = {
    "gpt5": {
        "label":   "GPT-5",
        "enabled": bool(os.getenv("OPENAI_API_KEY")),
        "existing_results_root": PROJECT_ROOT / "results" / "validation" / "llm_judge" / "us" / "gpt-5",
    },
    "claude_haiku_45": {
        "label":   "Claude Haiku 4.5",
        "enabled": bool(os.getenv("ANTHROPIC_API_KEY")),
        "model":   os.getenv("CLAUDE_JUDGE_MODEL", "claude-haiku-4-5-20251001"),
    },
    "gemini_25": {
        "label":   "Gemini 2.5 Pro",
        "enabled": bool(os.getenv("AIXPLAIN_API_KEY") or os.getenv("TEAM_API_KEY")),
        "model":   os.getenv("GEMINI_25_AIXPLAIN_MODEL_ID",
                             os.getenv("AIXPLAIN_GEMINI_MODEL_ID", "google/gemini-2.5-pro/google")),
    },
}

MULTIMODEL_ROOT = PROJECT_ROOT / "results" / "validation" / "llm_judge_multimodel"
MULTIMODEL_ROOT.mkdir(parents=True, exist_ok=True)

REGION       = "us"
SOURCE_MODEL = "gpt-5"
NLG_PROVIDER = "openai"
NLG_MODEL    = "gpt-5"
LANGUAGE     = "en"

DIMENSION_NAMES = ["No-Omissions", "No-Additions", "Grammaticality", "Coherence", "Fluency"]

# ---------------------------------------------------------------------------
# Pydantic schema (same as llm-as-judge.ipynb)
# ---------------------------------------------------------------------------

class DimensionScore(BaseModel):
    Justification: str = Field(min_length=1)
    Score: int = Field(ge=1, le=7)


class JudgeScorecard(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    no_omissions:  DimensionScore = Field(alias="No-Omissions")
    no_additions:  DimensionScore = Field(alias="No-Additions")
    grammaticality: DimensionScore = Field(alias="Grammaticality")
    coherence:     DimensionScore = Field(alias="Coherence")
    fluency:       DimensionScore = Field(alias="Fluency")

# ---------------------------------------------------------------------------
# Judge instructions (from llm-as-judge.ipynb)
# ---------------------------------------------------------------------------

JUDGE_INSTRUCTIONS_EN = """You are evaluating how well a Generated Report realises a given Input Bundle for a financial data-to-text task.

Your task:
1. Read the Input Data and Generated Report carefully. The Input Data is the same text shown to human evaluators: for US English reports it is Subject | Predicate | Value triples; for Brazilian Portuguese reports it is structured input text.
2. For each of the five Dimensions below, assign a score from 1 (lowest) to 7 (highest).
3. For each Dimension, give a short justification (one or two sentences).
4. Return only a single JSON object in the exact format specified. Do not include any extra text.

Dimensions:
No-Omissions: To what degree is ALL the information in the Input Data present in the Generated Report. Judge this dimension only against the Input Data. Do not penalise omissions of information that appears only in outside knowledge. If an indicator appears in the Input Data with value 0, do not penalise the report for omitting that zero-valued indicator.
No-Additions: To what degree does the Generated Report include ONLY information supported by the Input Data. Qualitative inferences directly derivable from the Input Data are permitted. Penalise specific numeric figures or factual claims that cannot be found in the Input Data or derived from it by simple arithmetic. If a zero-valued structured indicator conflicts with a non-zero value stated in RecommendationJustification or manager text within the Input Data, treat the zero as a missing placeholder and treat the non-zero textual value as supported evidence.
Grammaticality: To what degree is the Generated Report free of grammatical errors, considering form only.
Coherence: To what degree is the Generated Report well structured and organised into a coherent body of information about the covered stocks, from the perspective of meaning only.
Fluency: To what degree does the Generated Report read smoothly and naturally as professional financial prose, without abrupt or awkward phrasing.

Important notes:
- No-Omissions and No-Additions are judged only with respect to the Input Data. Do not use outside knowledge.
- For No-Omissions, do not penalise the report for omitting indicators whose value is 0 in the Input Data. Zero-valued indicators may be uninformative placeholders in this dataset.
- If a structured indicator has value 0 but RecommendationJustification or manager text in the Input Data states a non-zero value for the same concept, treat the zero as a placeholder/missing structured value. Do not penalise the report for using the non-zero textual value.
- Grammaticality, Coherence, and Fluency are intrinsic properties of the Generated Report. Judge them against the report text itself, not against the Input Data.
- Scores must be integers in the set (1, 2, 3, 4, 5, 6, 7).
- Judge each Dimension independently.
- Do not award 7 by default unless the condition for that score is fully satisfied.

Return your assessment in this exact JSON format with no additional keys and no extra text:
{
  "No-Omissions": {"Justification": "", "Score": 1},
  "No-Additions": {"Justification": "", "Score": 1},
  "Grammaticality": {"Justification": "", "Score": 1},
  "Coherence": {"Justification": "", "Score": 1},
  "Fluency": {"Justification": "", "Score": 1}
}"""

JUDGE_INSTRUCTIONS_PT_BR = """Você está avaliando quão bem um Relatório Gerado realiza um Conjunto de Entrada em uma tarefa financeira de geração de texto a partir de dados.

Sua tarefa:
1. Leia cuidadosamente os Dados de Entrada e o Relatório Gerado. Os Dados de Entrada são o mesmo texto mostrado aos avaliadores humanos: para relatórios em inglês dos EUA, são triplas Subject | Predicate | Value; para relatórios em português brasileiro, é texto estruturado de entrada.
2. Para cada uma das cinco Dimensões abaixo, atribua uma pontuação de 1 (menor) a 7 (maior).
3. Para cada Dimensão, forneça uma justificativa breve (uma ou duas frases).
4. Retorne apenas um único objeto JSON no formato exato especificado. Não inclua nenhum texto adicional.

Dimensões:
No-Omissions: Em que grau TODAS as informações dos Dados de Entrada estão presentes no Relatório Gerado. Julgue esta dimensão apenas em relação aos Dados de Entrada. Não penalize omissões de informações que apareçam apenas em conhecimento externo. Se um indicador aparecer nos Dados de Entrada com valor 0, não penalize o relatório por omitir esse indicador de valor zero.
No-Additions: Em que grau o Relatório Gerado inclui APENAS informações sustentadas pelos Dados de Entrada. Inferências qualitativas diretamente deriváveis dos Dados de Entrada são permitidas. Penalize números específicos ou afirmações factuais que não possam ser encontrados nos Dados de Entrada ou derivados deles por aritmética simples. Se um indicador estruturado de valor zero entrar em conflito com um valor diferente de zero declarado em RecommendationJustification ou no texto do gerente dentro dos Dados de Entrada, trate o zero como placeholder ausente e trate o valor textual diferente de zero como evidência sustentada.
Grammaticality: Em que grau o Relatório Gerado está livre de erros gramaticais, considerando apenas a forma.
Coherence: Em que grau o Relatório Gerado está bem estruturado e organizado em um corpo coerente de informações sobre as ações cobertas, apenas do ponto de vista do significado.
Fluency: Em que grau o Relatório Gerado é lido de forma fluida e natural como prosa financeira profissional, sem formulações abruptas ou estranhas.

Observações importantes:
- No-Omissions e No-Additions devem ser julgadas apenas em relação aos Dados de Entrada. Não use conhecimento externo.
- Para No-Omissions, não penalize o relatório por omitir indicadores cujo valor seja 0 nos Dados de Entrada. Indicadores de valor zero podem ser placeholders pouco informativos neste conjunto de dados.
- Se um indicador estruturado tiver valor 0, mas RecommendationJustification ou o texto do gerente nos Dados de Entrada declarar um valor diferente de zero para o mesmo conceito, trate o zero como placeholder/valor estruturado ausente. Não penalize o relatório por usar o valor textual diferente de zero.
- Grammaticality, Coherence e Fluency são propriedades intrínsecas do Relatório Gerado. Julgue-as em relação ao próprio texto do relatório, não em relação aos Dados de Entrada.
- As pontuações devem ser números inteiros no conjunto (1, 2, 3, 4, 5, 6, 7).
- Julgue cada Dimensão de forma independente.
- Não atribua 7 por padrão, a menos que a condição para essa pontuação esteja plenamente satisfeita.

Retorne sua avaliação neste formato JSON exato, sem chaves adicionais e sem texto extra:
{
  "No-Omissions": {"Justification": "", "Score": 1},
  "No-Additions": {"Justification": "", "Score": 1},
  "Grammaticality": {"Justification": "", "Score": 1},
  "Coherence": {"Justification": "", "Score": 1},
  "Fluency": {"Justification": "", "Score": 1}
}"""


def judge_instructions_for_collection(collection: str) -> tuple[str, str]:
    if collection == "nlg_brazilian_manager":
        return JUDGE_INSTRUCTIONS_PT_BR, "pt_br"
    return JUDGE_INSTRUCTIONS_EN, "en"

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def nlg_output_dir(collection: str, source_reflection: bool | None, workflow: str) -> Path:
    if collection == "nlg_brazilian_manager":
        return PROJECT_ROOT / "results" / "nlg_brazilian_manager" / workflow
    base_dir = (
        PROJECT_ROOT / "results" / "nlg"
        / f"final_report2025_{REGION}"
        / SOURCE_MODEL
        / f"workflow_{source_reflection}"
        / NLG_PROVIDER / NLG_MODEL / LANGUAGE
    )
    if workflow == "no_orchestrator_no_guardrail_no_finalizer":
        return base_dir / "no_orchestrator_no_finalizer" / workflow
    return base_dir / workflow


def source_dataset_dir(source_reflection: bool) -> Path:
    return (
        PROJECT_ROOT / "results"
        / f"final_report2025_{REGION}"
        / SOURCE_MODEL
        / f"workflow_{source_reflection}"
    )


def gpt5_existing_dir(source_reflection: bool, workflow: str) -> Path:
    return (
        PROJECT_ROOT / "results" / "validation" / "llm_judge"
        / REGION / SOURCE_MODEL
        / f"workflow_{source_reflection}"
        / NLG_PROVIDER / NLG_MODEL / LANGUAGE / workflow
    )


def judge_output_dir(judge_name: str, collection: str, source_reflection: bool | None, workflow: str) -> Path:
    if collection == "nlg_brazilian_manager":
        return (
            MULTIMODEL_ROOT / judge_name
            / "nlg_brazilian_manager"
            / "pt_br" / workflow
        )
    return (
        MULTIMODEL_ROOT / judge_name
        / f"workflow_{source_reflection}"
        / NLG_PROVIDER / NLG_MODEL / LANGUAGE / workflow
    )


def safe_slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (text or "sample").strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run LLM-as-judge scoring with optional condition and sample filters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--judge",
        action="append",
        choices=sorted(JUDGES),
        help="Judge to run. May be repeated. Defaults to all configured judges.",
    )
    parser.add_argument(
        "--collection",
        choices=("nlg", "nlg_brazilian_manager", "all"),
        default="nlg",
        help="Generated-output collection to judge.",
    )
    parser.add_argument(
        "--source-reflection",
        choices=("true", "false", "all"),
        default="all",
        help="Limit to reflection-enabled, non-reflection, or all US source branches. Ignored for Brazilian manager outputs.",
    )
    parser.add_argument(
        "--workflow",
        action="append",
        choices=sorted({workflow for _, _, workflow in CONDITIONS}),
        help="Workflow to run. May be repeated. Defaults to all workflows.",
    )
    parser.add_argument(
        "--analysis-date",
        help="Run only one analysis date, e.g. 2025-01-31.",
    )
    parser.add_argument(
        "--sample-name",
        help="Run only one sample name.",
    )
    parser.add_argument(
        "--json-file",
        help="Run only the generated-report JSON file at this path.",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List matching generated-report JSON files and exit without judging.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite cached judge outputs where this runner writes them.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help=(
            "Run only judge/sample pairs without a cached multimodel JSON. "
            "Existing judgments are untouched and summary tables are not rebuilt."
        ),
    )
    parser.add_argument(
        "--skip-summaries",
        action="store_true",
        help="Run judgments but do not rebuild aggregate summary CSV files.",
    )
    parser.add_argument(
        "--fresh-gpt5",
        action="store_true",
        help="Run GPT-5 fresh instead of loading existing GPT-5 judge results.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help="Maximum retries per sample/judge.",
    )
    return parser.parse_args()


def selected_conditions(args: argparse.Namespace) -> list[Condition]:
    conditions = CONDITIONS
    if args.collection != "all":
        conditions = [c for c in conditions if c[0] == args.collection]
    if args.source_reflection != "all":
        wanted = args.source_reflection == "true"
        conditions = [
            c for c in conditions
            if c[0] != "nlg" or c[1] == wanted
        ]
    if args.workflow:
        workflows = set(args.workflow)
        conditions = [c for c in conditions if c[2] in workflows]
    return conditions


def resolve_project_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve(strict=False)


def filter_records(records: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.analysis_date:
        records = [r for r in records if r["analysis_date"] == args.analysis_date]
    if args.sample_name:
        records = [r for r in records if r["sample_name"] == args.sample_name]
    if args.json_file:
        target = resolve_project_path(args.json_file)
        records = [
            r for r in records
            if Path(r["json_path"]).resolve(strict=False) == target
        ]
    return records


def format_us_reference_input(payload: dict[str, Any]) -> str:
    """Match generate_annotation_excel.py:format_us_input."""
    meta = payload.get("sample_metadata") or {}
    analysis_date = str(payload.get("analysis_date") or meta.get("analysis_date") or "")
    report_subject = f"M_SMRG_{analysis_date}" if analysis_date else "M_SMRG"
    header_triples = [
        (report_subject, "report_type", "Monthly Equity Review"),
        (report_subject, "analysis_date", analysis_date),
        (report_subject, "price_reference_date", analysis_date),
        (report_subject, "coverage_window_end_date", str(payload.get("end_date") or "")),
        (report_subject, "investment_horizon_months", str(payload.get("horizon_months") or "")),
        (
            report_subject,
            "analyst_note",
            "This report is generated from structured financial data. All recommendations are model-derived. This document does not constitute regulated investment advice. Past performance is not indicative of future results.",
        ),
    ]
    triples = (
        payload.get("data")
        or payload.get("data_input")
        or meta.get("data_input")
        or []
    )
    lines: list[str] = []
    for subject, predicate, value in header_triples:
        if value:
            lines.append(f"{subject} | {predicate} | {value}")
    for triple in triples:
        if isinstance(triple, (list, tuple)) and len(triple) == 3:
            subject, predicate, value = triple
            lines.append(f"{subject} | {predicate} | {value}")
    return "\n".join(lines) or "(no triples found)"


def format_br_reference_input(payload: dict[str, Any]) -> str:
    """Match generate_annotation_excel.py:format_br_input."""
    meta = payload.get("sample_metadata") or {}
    query = (
        payload.get("query")
        or payload.get("input")
        or payload.get("prompt")
        or meta.get("prompt_context")
        or ""
    )
    return str(query).strip()

# ---------------------------------------------------------------------------
# Data loading (mirrors llm-as-judge.ipynb)
# ---------------------------------------------------------------------------

def build_brazilian_records(workflow: str) -> list[dict[str, Any]]:
    nlg_dir = nlg_output_dir("nlg_brazilian_manager", None, workflow)
    if not nlg_dir.exists():
        raise FileNotFoundError(f"Brazilian NLG output not found: {nlg_dir}")

    rows: list[dict[str, Any]] = []
    for json_path in sorted(nlg_dir.glob("*.json")):
        if "sequence_summary" in json_path.name:
            continue
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        txt_path = json_path.with_suffix(".txt")
        meta = payload.get("sample_metadata") or {}
        generated_text = (
            payload.get("generated_text")
            or payload.get("final_response")
            or (txt_path.read_text(encoding="utf-8") if txt_path.exists() else "")
        ).strip()
        rows.append({
            "sample_name": str(meta.get("sample_name") or payload.get("sample_name") or json_path.stem),
            "analysis_date": str(meta.get("analysis_date") or payload.get("analysis_date") or "")[:10],
            "generated_text": generated_text,
            "reference_input": format_br_reference_input(payload),
            "tickers": meta.get("tickers") or [],
            "ticker_count": meta.get("ticker_count") or "",
            "previous_report": meta.get("previous_report") or "",
            "json_path": json_path,
        })

    rows.sort(key=lambda r: r["analysis_date"])
    generated_by_date = {r["analysis_date"]: r for r in rows}
    coverage_end_date = rows[-1]["analysis_date"] if rows else ""

    records: list[dict[str, Any]] = []
    previous_row: dict[str, Any] | None = None
    for row in rows:
        prev_text = "N/A"
        if previous_row:
            prev_text = previous_row["generated_text"]
        elif row["previous_report"]:
            prev_text = row["previous_report"]

        tickers = row["tickers"]
        tickers_str = ", ".join(tickers) if isinstance(tickers, list) else str(tickers)
        analysis_ts = pd.to_datetime(row["analysis_date"], errors="coerce")
        end_ts = pd.to_datetime(coverage_end_date, errors="coerce")
        horizon = ""
        if not pd.isna(analysis_ts) and not pd.isna(end_ts):
            horizon = str(max((end_ts.year - analysis_ts.year) * 12 + (end_ts.month - analysis_ts.month), 0))

        records.append({
            "sample_name": row["sample_name"],
            "analysis_date": row["analysis_date"],
            "previous_analysis_date": previous_row["analysis_date"] if previous_row else None,
            "generated_text": row["generated_text"],
            "previous_generated_text": prev_text,
            "reference_input": row["reference_input"],
            "prompt_context": row["reference_input"],
            "report_metadata": {
                "analysis_date": row["analysis_date"],
                "tickers": tickers_str,
                "ticker_count": str(row["ticker_count"] or (len(tickers) if isinstance(tickers, list) else "")),
                "end_date": coverage_end_date,
                "horizon_months": horizon,
            },
            "json_path": row["json_path"],
        })
        previous_row = row

    return records


def build_records(collection: str, source_reflection: bool | None, workflow: str) -> list[dict[str, Any]]:
    if collection == "nlg_brazilian_manager":
        return build_brazilian_records(workflow)
    if source_reflection is None:
        raise ValueError("source_reflection is required for nlg collection")

    ds_dir  = source_dataset_dir(source_reflection)
    nlg_dir = nlg_output_dir(collection, source_reflection, workflow)

    if not ds_dir.exists():
        raise FileNotFoundError(f"Source dataset not found: {ds_dir}")
    if not nlg_dir.exists():
        raise FileNotFoundError(f"NLG output not found: {nlg_dir}")

    samples = load_generation_samples(str(ds_dir), dataset_kind="auto", min_stocks_per_month=1)
    sample_index = {str(s["sample_name"]): s for s in samples}

    rows: list[dict[str, Any]] = []
    for json_path in sorted(nlg_dir.glob("*.json")):
        if "sequence_summary" in json_path.name:
            continue
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        txt_path = json_path.with_suffix(".txt")
        meta = payload.get("sample_metadata") or {}
        sample_name  = str(meta.get("sample_name") or payload.get("sample_name") or json_path.stem)
        analysis_date = str(meta.get("analysis_date") or payload.get("analysis_date") or "")[:10]
        generated_text = (
            payload.get("generated_text")
            or payload.get("final_response")
            or (txt_path.read_text(encoding="utf-8") if txt_path.exists() else "")
        ).strip()
        rows.append({
            "sample_name": sample_name,
            "analysis_date": analysis_date,
            "generated_text": generated_text,
            "reference_input": format_us_reference_input(payload),
            "json_path": json_path,
        })

    rows.sort(key=lambda r: r["analysis_date"])
    generated_by_date = {r["analysis_date"]: r for r in rows}

    coverage_end_date = max(
        (str(s.get("analysis_date", ""))[:10] for s in sample_index.values()),
        default=""
    )
    if coverage_end_date:
        ts = pd.to_datetime(coverage_end_date, errors="coerce")
        if not pd.isna(ts):
            coverage_end_date = (ts + pd.offsets.MonthEnd(0)).date().isoformat()

    records: list[dict[str, Any]] = []
    for row in rows:
        sample = sample_index.get(row["sample_name"])
        if not sample:
            print(f"  WARNING: no source sample for {row['sample_name']}")
            continue

        prev_date = str(sample.get("previous_analysis_date") or "")[:10]
        prev_text = "N/A"
        if prev_date and prev_date in generated_by_date:
            prev_text = generated_by_date[prev_date]["generated_text"]
        elif isinstance(sample.get("previous_report"), str) and sample["previous_report"].strip():
            prev_text = sample["previous_report"].strip()

        prompt_context = build_multi_stock_prompt_context(
            analysis_date=str(sample.get("analysis_date", "")),
            stock_rows=sample.get("stocks", []),
            previous_report=prev_text or "N/A",
        )

        # Report metadata for the judge input header
        tickers = sample.get("tickers") or []
        tickers_str = ", ".join(tickers) if isinstance(tickers, list) else str(tickers)
        analysis_ts = pd.to_datetime(row["analysis_date"], errors="coerce")
        end_ts      = pd.to_datetime(coverage_end_date, errors="coerce")
        horizon = ""
        if not pd.isna(analysis_ts) and not pd.isna(end_ts):
            horizon = str(max((end_ts.year - analysis_ts.year) * 12 + (end_ts.month - analysis_ts.month), 0))

        records.append({
            "sample_name":           row["sample_name"],
            "analysis_date":         row["analysis_date"],
            "previous_analysis_date": prev_date or None,
            "generated_text":        row["generated_text"],
            "previous_generated_text": prev_text,
            "reference_input":       row["reference_input"],
            "prompt_context":        prompt_context,
            "report_metadata": {
                "analysis_date":  row["analysis_date"],
                "tickers":        tickers_str,
                "ticker_count":   str(len(tickers) if isinstance(tickers, list) else ""),
                "end_date":       coverage_end_date,
                "horizon_months": horizon,
            },
            "json_path": row["json_path"],
        })

    return records


def build_judge_input(record: dict[str, Any]) -> str:
    return (
        f"INPUT DATA:\n"
        f"{record.get('reference_input') or '(no reference input found)'}\n\n"
        f"GENERATED REPORT:\n"
        f"{record['generated_text']}"
    )

# ---------------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------------

def parse_scorecard(raw_text: str) -> JudgeScorecard:
    text = (raw_text or "").strip()
    candidates = [text]
    if text.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", text)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        candidates.append(cleaned.strip())
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(text[start:end + 1])

    last_err: Exception | None = None
    for candidate in dict.fromkeys(candidates):  # deduplicate, preserve order
        if not candidate:
            continue
        try:
            return JudgeScorecard.model_validate_json(candidate)
        except Exception as exc:
            last_err = exc
    raise ValueError(f"Could not parse scorecard: {last_err}\nRaw: {raw_text[:300]}")

# ---------------------------------------------------------------------------
# Judge clients
# ---------------------------------------------------------------------------

def call_gpt5(judge_input: str, judge_instructions: str) -> JudgeScorecard:
    from openai import OpenAI
    client = OpenAI()
    response = client.responses.parse(
        model="gpt-5",
        instructions=judge_instructions,
        input=judge_input,
        text_format=JudgeScorecard,
        reasoning={"effort": "high"},
        text={"verbosity": "low"},
        store=False,
    )
    parsed = response.output_parsed
    if parsed is None:
        raw = getattr(response, "output_text", "") or ""
        parsed = parse_scorecard(raw)
    return parsed


def call_claude(judge_input: str, model: str, judge_instructions: str) -> JudgeScorecard:
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=judge_instructions,
        messages=[{"role": "user", "content": judge_input}],
    )
    raw = response.content[0].text
    return parse_scorecard(raw)


def call_gemini(judge_input: str, model_id: str, judge_instructions: str) -> JudgeScorecard:
    api_key = os.getenv("AIXPLAIN_API_KEY") or os.getenv("TEAM_API_KEY")
    from aixplain import Aixplain
    aix = Aixplain(api_key)
    model = aix.Model.get(model_id)
    full_prompt = f"{judge_instructions}\n\n{judge_input}"
    try:
        result = model.run(text=full_prompt, temperature=0.0, max_tokens=8192)
    except TypeError:
        result = model.run(text=full_prompt)
    # Extract text from aiXplain result
    raw = ""
    if hasattr(result, "data"):
        data = result.data
        if isinstance(data, str):
            raw = data
        elif isinstance(data, dict):
            raw = (
                data.get("output")
                or data.get("text")
                or data.get("response")
                or data.get("content")
                or ""
            )
        else:
            raw = (
                getattr(data, "output", "")
                or getattr(data, "text", "")
                or getattr(data, "response", "")
                or getattr(data, "content", "")
            )
    raw = raw or getattr(result, "output", "") or getattr(result, "text", "") or str(result)
    return parse_scorecard(raw)

# ---------------------------------------------------------------------------
# Core judging loop
# ---------------------------------------------------------------------------

def judge_one(
    record: dict[str, Any],
    judge_name: str,
    collection: str,
    source_reflection: bool | None,
    workflow: str,
) -> dict[str, Any] | None:
    """Run a single judge on a single record. Returns result dict or None on total failure."""
    out_dir = judge_output_dir(judge_name, collection, source_reflection, workflow)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_slug(record['sample_name'])}.json"

    if out_path.exists() and not OVERWRITE:
        return json.loads(out_path.read_text(encoding="utf-8"))

    judge_input = build_judge_input(record)
    judge_instructions, prompt_language = judge_instructions_for_collection(collection)
    cfg = JUDGES[judge_name]
    last_err: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if judge_name == "gpt5":
                scorecard = call_gpt5(judge_input, judge_instructions)
            elif judge_name == "claude_haiku_45":
                scorecard = call_claude(judge_input, cfg["model"], judge_instructions)
            elif judge_name == "gemini_25":
                scorecard = call_gemini(judge_input, cfg["model"], judge_instructions)
            else:
                raise ValueError(f"Unknown judge: {judge_name}")

            result = {
                "sample_name":    record["sample_name"],
                "analysis_date":  record["analysis_date"],
                "judge":          judge_name,
                "judge_label":    cfg["label"],
                "prompt_language": prompt_language,
                "attempt":        attempt,
                "scores":         scorecard.model_dump(by_alias=True),
            }
            out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            return result

        except Exception as exc:
            last_err = exc
            print(f"    [retry {attempt}/{MAX_RETRIES}] {type(exc).__name__}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(3 * attempt)

    print(f"    FAILED after {MAX_RETRIES} attempts: {last_err}")
    return None


def load_existing_gpt5(source_reflection: bool, workflow: str) -> list[dict[str, Any]]:
    """Load pre-computed GPT-5 results from the existing llm_judge/ folder."""
    src_dir = gpt5_existing_dir(source_reflection, workflow)
    results = []
    for p in sorted(src_dir.glob("*.json")):
        if p.name in ("all_judgements.json", "summary.csv"):
            continue
        raw = json.loads(p.read_text(encoding="utf-8"))
        if "error" in raw or "scores" not in raw:
            continue
        # Normalise to our output schema
        results.append({
            "sample_name":   raw["sample_name"],
            "analysis_date": raw["analysis_date"],
            "judge":         "gpt5",
            "judge_label":   "GPT-5",
            "attempt":       raw.get("judge_attempt", 1),
            "scores":        raw["scores"],
        })
    return results


def flatten(result: dict[str, Any]) -> dict[str, Any]:
    row = {
        "sample_name":   result["sample_name"],
        "analysis_date": result["analysis_date"],
        "judge":         result["judge"],
        "judge_label":   result["judge_label"],
        "prompt_language": result.get("prompt_language", ""),
    }
    numeric: list[float] = []
    for dim in DIMENSION_NAMES:
        key = dim.lower().replace("-", "_")
        val = result["scores"][dim]
        score = int(val["Score"]) if isinstance(val, dict) else int(val)
        row[f"{key}_score"] = score
        if isinstance(val, dict):
            row[f"{key}_justification"] = val.get("Justification", "")
        numeric.append(score)
    row["mean_score"] = sum(numeric) / len(numeric)
    return row

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def run_condition(
    collection: str,
    source_reflection: bool | None,
    workflow: str,
    judge_names: list[str],
    args: argparse.Namespace,
) -> pd.DataFrame:
    label = (
        f"nlg_brazilian_manager/{workflow}"
        if collection == "nlg_brazilian_manager"
        else f"workflow_{source_reflection}/{workflow}"
    )
    print(f"\n{'='*60}")
    print(f"Condition: {label}")
    print(f"{'='*60}")

    records = filter_records(build_records(collection, source_reflection, workflow), args)
    print(f"  Records loaded: {len(records)}")
    if not records:
        return pd.DataFrame()

    if args.list_files:
        for record in records:
            rel_path = Path(record["json_path"]).relative_to(PROJECT_ROOT)
            print(f"  {record['analysis_date']}  {record['sample_name']}  {rel_path}")
        return pd.DataFrame()

    all_rows: list[dict[str, Any]] = []
    selected_sample_names = {record["sample_name"] for record in records}

    for judge_name in judge_names:
        cfg = JUDGES[judge_name]
        if not cfg["enabled"]:
            print(f"\n  [{cfg['label']}] SKIPPED (not configured)")
            continue

        print(f"\n  [{cfg['label']}]")

        # GPT-5: load from existing results, copy to multimodel folder for consistency
        records_for_judge = records
        if args.missing_only:
            out_dir = judge_output_dir(
                judge_name, collection, source_reflection, workflow
            )
            records_for_judge = [
                record for record in records
                if not (
                    out_dir / f"{safe_slug(record['sample_name'])}.json"
                ).exists()
            ]
            cached_count = len(records) - len(records_for_judge)
            print(
                f"    Cached: {cached_count}; missing: {len(records_for_judge)}"
            )
            if not records_for_judge:
                continue

        if (
            collection == "nlg"
            and judge_name == "gpt5"
            and not args.fresh_gpt5
            and not args.missing_only
        ):
            existing = [
                r for r in load_existing_gpt5(source_reflection, workflow)
                if r["sample_name"] in selected_sample_names
            ]
            if existing:
                # Mirror into multimodel folder so everything is in one place
                out_dir = judge_output_dir("gpt5", collection, source_reflection, workflow)
                out_dir.mkdir(parents=True, exist_ok=True)
                for r in existing:
                    p = out_dir / f"{safe_slug(r['sample_name'])}.json"
                    if not p.exists() or OVERWRITE:
                        p.write_text(json.dumps(r, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"    Loaded {len(existing)} existing results")
                all_rows.extend(flatten(r) for r in existing)
                continue
            else:
                print(f"    No existing GPT-5 results found for {label}, running fresh")

        for idx, record in enumerate(records_for_judge, 1):
            result = judge_one(record, judge_name, collection, source_reflection, workflow)
            if result:
                all_rows.append(flatten(result))
                print(f"    [{idx}/{len(records_for_judge)}] {record['sample_name']} ✓")
            else:
                print(f"    [{idx}/{len(records_for_judge)}] {record['sample_name']} ✗ (failed)")

    return pd.DataFrame(all_rows)


def bootstrap_ci(scores: np.ndarray, n: int = 5000, ci: float = 0.95) -> tuple[float, float]:
    means = [np.random.choice(scores, len(scores), replace=True).mean() for _ in range(n)]
    a = (1 - ci) / 2
    return float(np.percentile(means, a * 100)), float(np.percentile(means, (1 - a) * 100))


def build_comparison_table(all_results: pd.DataFrame) -> pd.DataFrame:
    score_cols = [f"{d.lower().replace('-','_')}_score" for d in DIMENSION_NAMES]
    rows = []
    for (collection, source_reflection, workflow), group in all_results.groupby(
        ["collection", "source_reflection", "workflow"], dropna=False
    ):
        for judge_name in JUDGES:
            jg = group[group["judge"] == judge_name]
            if jg.empty:
                continue
            row = {
                "collection": collection,
                "source_reflection": source_reflection,
                "workflow": workflow,
                "judge": judge_name,
                "judge_label": JUDGES[judge_name]["label"],
                "n_samples": len(jg),
            }
            for col in score_cols:
                scores = jg[col].dropna().values
                lo, hi = bootstrap_ci(scores)
                row[col]               = scores.mean()
                row[f"{col}_ci_lower"] = lo
                row[f"{col}_ci_upper"] = hi
            row["mean_score"]          = jg["mean_score"].mean()
            lo, hi = bootstrap_ci(jg["mean_score"].dropna().values)
            row["mean_score_ci_lower"] = lo
            row["mean_score_ci_upper"] = hi
            rows.append(row)
    return pd.DataFrame(rows)


def build_ensemble_comparison(all_results: pd.DataFrame) -> pd.DataFrame:
    """Average scores across all enabled judges per sample, then summarise per condition."""
    score_cols = [f"{d.lower().replace('-','_')}_score" for d in DIMENSION_NAMES]
    ensemble = (
        all_results
        .groupby(["collection", "source_reflection", "workflow", "sample_name", "analysis_date"],
                 as_index=False)[score_cols]
        .mean()
    )
    ensemble["mean_score"] = ensemble[score_cols].mean(axis=1)

    rows = []
    for (collection, sr, wf), grp in ensemble.groupby(["collection", "source_reflection", "workflow"], dropna=False):
        row = {"collection": collection, "source_reflection": sr, "workflow": wf, "n_samples": len(grp)}
        for col in score_cols:
            scores = grp[col].dropna().values
            lo, hi = bootstrap_ci(scores)
            row[col]               = scores.mean()
            row[f"{col}_ci_lower"] = lo
            row[f"{col}_ci_upper"] = hi
        row["mean_score"]          = grp["mean_score"].mean()
        lo, hi = bootstrap_ci(grp["mean_score"].dropna().values)
        row["mean_score_ci_lower"] = lo
        row["mean_score_ci_upper"] = hi
        rows.append(row)
    return pd.DataFrame(rows)


def build_ttests(all_results: pd.DataFrame) -> pd.DataFrame:
    score_cols = [f"{d.lower().replace('-','_')}_score" for d in DIMENSION_NAMES] + ["mean_score"]
    all_results = all_results[all_results["collection"] == "nlg"]
    if all_results.empty:
        return pd.DataFrame()

    # Use ensemble (mean across judges) for t-tests
    ensemble = (
        all_results
        .groupby(["source_reflection", "workflow", "sample_name"],
                 as_index=False)[score_cols[:-1]]
        .mean()
    )
    ensemble["mean_score"] = ensemble[score_cols[:-1]].mean(axis=1)

    test_pairs = [
        ((False, "e2e"),    (False, "default"), "e2e vs default (no reflection)"),
        ((True,  "e2e"),    (True,  "default"), "e2e vs default (with reflection)"),
        ((True,  "default"), (False, "default"), "default: reflection vs no reflection"),
        ((True,  "e2e"),    (False, "e2e"),     "e2e: reflection vs no reflection"),
        ((False, "e2e"),    (True,  "default"), "best e2e vs best default"),
    ]

    rows = []
    for (sr_a, wf_a), (sr_b, wf_b), label in test_pairs:
        df_a = ensemble[(ensemble["source_reflection"] == sr_a) & (ensemble["workflow"] == wf_a)]
        df_b = ensemble[(ensemble["source_reflection"] == sr_b) & (ensemble["workflow"] == wf_b)]
        shared = set(df_a["sample_name"]) & set(df_b["sample_name"])
        if len(shared) < 3:
            continue
        df_a = df_a[df_a["sample_name"].isin(shared)].sort_values("sample_name")
        df_b = df_b[df_b["sample_name"].isin(shared)].sort_values("sample_name")
        for col in score_cols:
            a = df_a[col].values
            b = df_b[col].values
            diff = a - b
            t, p = stats.ttest_rel(a, b)
            rows.append({
                "comparison":       label,
                "dimension":        col,
                "mean_A":           a.mean(),
                "mean_B":           b.mean(),
                "mean_diff (A-B)":  diff.mean(),
                "std_diff":         diff.std(ddof=1),
                "t_stat":           round(t, 3),
                "p_value":          round(p, 4),
                "significant_p05":  p < 0.05,
                "significant_p10":  p < 0.10,
            })
    return pd.DataFrame(rows)


def main() -> None:
    global MAX_RETRIES, OVERWRITE

    args = parse_args()
    if args.missing_only and args.overwrite:
        raise SystemExit("--missing-only cannot be combined with --overwrite")
    MAX_RETRIES = args.max_retries
    OVERWRITE = args.overwrite
    judge_names = args.judge or list(JUDGES)
    conditions = selected_conditions(args)

    if not conditions:
        print("No matching conditions selected.")
        return

    print("Enabled judges:")
    for name in judge_names:
        cfg = JUDGES[name]
        status = "ENABLED" if cfg["enabled"] else "DISABLED"
        print(f"  {status}  {cfg['label']}")

    all_frames: list[pd.DataFrame] = []

    for collection, source_reflection, workflow in conditions:
        df = run_condition(collection, source_reflection, workflow, judge_names, args)
        if not df.empty:
            df["collection"]        = collection
            df["source_reflection"] = source_reflection
            df["workflow"]          = workflow
            all_frames.append(df)

    if args.list_files:
        print("\nDone listing files.")
        return

    if args.missing_only or args.skip_summaries:
        print(
            "\nJudge run finished. Summary tables were not rebuilt.\n"
            "After verifying that every judgment succeeded, run:\n"
            "  ./run_llm_as_judge.sh --aggregate-only"
        )
        return

    if not all_frames:
        print("\nNo results collected.")
        return

    all_results = pd.concat(all_frames, ignore_index=True)

    # Save raw results
    raw_path = MULTIMODEL_ROOT / "all_results_raw.csv"
    all_results.to_csv(raw_path, index=False)
    print(f"\nRaw results saved: {raw_path.relative_to(PROJECT_ROOT)}")

    # Per-judge comparison table
    comparison = build_comparison_table(all_results)
    score_cols  = [f"{d.lower().replace('-','_')}_score" for d in DIMENSION_NAMES]

    print("\n=== MEAN SCORES PER JUDGE PER CONDITION (95% CI) ===\n")
    display_rows = []
    for _, row in comparison.iterrows():
        dr = {
            "collection": row["collection"],
            "reflection": str(row["source_reflection"]),
            "workflow":   row["workflow"],
            "judge":      row["judge_label"],
            "n":          int(row["n_samples"]),
        }
        for col in score_cols:
            dr[col] = f"{row[col]:.3f} [{row[f'{col}_ci_lower']:.3f}, {row[f'{col}_ci_upper']:.3f}]"
        dr["mean"] = f"{row['mean_score']:.3f} [{row['mean_score_ci_lower']:.3f}, {row['mean_score_ci_upper']:.3f}]"
        display_rows.append(dr)
    print(pd.DataFrame(display_rows).to_string(index=False))

    comp_path = MULTIMODEL_ROOT / "summary_per_judge.csv"
    comparison.to_csv(comp_path, index=False)
    print(f"\nPer-judge summary saved: {comp_path.relative_to(PROJECT_ROOT)}")

    # Ensemble comparison
    ensemble_comp = build_ensemble_comparison(all_results)
    print("\n=== ENSEMBLE MEAN (averaged across judges) ===\n")
    ens_display = []
    for _, row in ensemble_comp.iterrows():
        dr = {
            "collection": row["collection"],
            "reflection": str(row["source_reflection"]),
            "workflow":   row["workflow"],
            "n":          int(row["n_samples"]),
        }
        for col in score_cols:
            dr[col] = f"{row[col]:.3f}"
        dr["mean"] = f"{row['mean_score']:.3f}"
        ens_display.append(dr)
    print(pd.DataFrame(ens_display).to_string(index=False))

    ens_path = MULTIMODEL_ROOT / "summary_ensemble.csv"
    ensemble_comp.to_csv(ens_path, index=False)
    print(f"\nEnsemble summary saved: {ens_path.relative_to(PROJECT_ROOT)}")

    # Paired t-tests
    ttest_df = build_ttests(all_results)
    if not ttest_df.empty:
        ttest_path = MULTIMODEL_ROOT / "paired_ttests.csv"
        ttest_df.to_csv(ttest_path, index=False)
        print(f"\nPaired t-tests saved: {ttest_path.relative_to(PROJECT_ROOT)}")
        sig = ttest_df[ttest_df["significant_p05"]]
        if sig.empty:
            print("No statistically significant differences found (p < 0.05).")
        else:
            print(f"\nSignificant differences (p < 0.05):\n{sig[['comparison','dimension','mean_diff (A-B)','p_value']].to_string(index=False)}")

    print("\nDone.")


if __name__ == "__main__":
    main()

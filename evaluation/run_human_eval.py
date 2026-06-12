"""
run_human_eval.py

Human evaluation of generated financial reports using a 1–5 Likert scale.

Two evaluation prompt variants — English (EN) and Brazilian Portuguese (PT-BR) —
with identical criteria so scores are directly comparable across languages.

Dimensions (same in both languages):
  No-Omissions | No-Additions | Grammaticality | Coherence | Fluency

Outputs → results/validation/human_eval/{annotator}/{condition}/
          results/validation/human_eval/all_results_raw.csv  (after aggregation)

Usage:
  python run_human_eval.py
  python run_human_eval.py --annotator alice --overwrite
  python run_human_eval.py --condition us_default --annotator bob
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "pipeline"))
sys.path.insert(0, str(PROJECT_ROOT / "evaluation"))

# ---------------------------------------------------------------------------
# Conditions
# ---------------------------------------------------------------------------

# Each entry: (condition_key, language, nlg_folder, label)
# Adjust this list to control which conditions are included.
EVAL_CONDITIONS: list[dict[str, Any]] = [
    {
        "key":      "us_default",
        "language": "en",
        "label":    "US · default (workflow_True)",
        "nlg_dir":  PROJECT_ROOT / "results" / "nlg" / "final_report2025_us"
                    / "gpt-5" / "workflow_True" / "openai" / "gpt-5" / "en" / "default",
    },
    {
        "key":      "br_default",
        "language": "pt_br",
        "label":    "BR-PT · default",
        "nlg_dir":  PROJECT_ROOT / "results" / "nlg_brazilian_manager" / "default",
    },
]

DIMENSIONS = ["No-Omissions", "No-Additions", "Grammaticality", "Coherence", "Fluency"]

HUMAN_EVAL_ROOT = PROJECT_ROOT / "results" / "validation" / "human_eval"

# ---------------------------------------------------------------------------
# Evaluation prompts  (identical criteria, two languages)
# ---------------------------------------------------------------------------

INSTRUCTIONS: dict[str, str] = {

    "en": textwrap.dedent("""\
        ┌─────────────────────────────────────────────────────────────────┐
        │              HUMAN EVALUATION — FINANCIAL REPORT NLG           │
        └─────────────────────────────────────────────────────────────────┘

        You will read a Generated Report and rate it on five dimensions.
        Score each dimension from 1 (lowest) to 5 (highest).
        You may optionally add a short justification (press Enter to skip).

        Dimensions and scoring anchors
        ───────────────────────────────────────────────────────────────────
        NO-OMISSIONS
          Does the report cover ALL information from the input bundle?
          1 = Major data fields are missing
          3 = Most fields present; minor omissions
          5 = All structured data fields appear in the report

        NO-ADDITIONS
          Does the report include ONLY information from the input bundle?
          Inferences directly derivable from the figures are acceptable.
          1 = Many facts not traceable to the bundle
          3 = Occasional unsupported claims
          5 = No facts outside the bundle or simple derivations

        GRAMMATICALITY
          Is the report free of grammatical errors? (form only)
          1 = Frequent errors that impede reading
          3 = Noticeable errors, but overall readable
          5 = No grammatical errors

        COHERENCE
          Is the report well-structured and logically organised? (meaning)
          1 = Disorganised; hard to follow
          3 = Mostly coherent; some structural issues
          5 = Clear, logical organisation throughout

        FLUENCY
          Does the report read smoothly as professional financial prose?
          1 = Awkward, unnatural phrasing throughout
          3 = Mostly fluent; occasional awkward phrasing
          5 = Reads naturally as professional financial prose
        ───────────────────────────────────────────────────────────────────
    """),

    "pt_br": textwrap.dedent("""\
        ┌─────────────────────────────────────────────────────────────────┐
        │         AVALIAÇÃO HUMANA — GERAÇÃO DE TEXTO FINANCEIRO         │
        └─────────────────────────────────────────────────────────────────┘

        Você lerá um Relatório Gerado e o avaliará em cinco dimensões.
        Atribua uma nota de 1 (mínimo) a 5 (máximo) para cada dimensão.
        Você pode adicionar uma justificativa breve (pressione Enter para pular).

        Dimensões e âncoras de pontuação
        ───────────────────────────────────────────────────────────────────
        SEM OMISSÕES (No-Omissions)
          O relatório cobre TODAS as informações do conjunto de entrada?
          1 = Campos de dados importantes estão ausentes
          3 = A maioria dos campos está presente; omissões menores
          5 = Todos os campos de dados estruturados aparecem no relatório

        SEM ADIÇÕES (No-Additions)
          O relatório inclui APENAS informações do conjunto de entrada?
          Inferências diretamente deriváveis dos dados são aceitáveis.
          1 = Muitos fatos não rastreáveis ao conjunto de entrada
          3 = Algumas afirmações sem suporte ocasional
          5 = Nenhum fato além do conjunto ou de derivações simples

        GRAMATICALIDADE (Grammaticality)
          O relatório está livre de erros gramaticais? (forma apenas)
          1 = Erros frequentes que dificultam a leitura
          3 = Erros perceptíveis, mas geralmente legível
          5 = Nenhum erro gramatical

        COERÊNCIA (Coherence)
          O relatório está bem estruturado e organizado logicamente? (significado)
          1 = Desorganizado; difícil de seguir
          3 = Majoritariamente coerente; alguns problemas estruturais
          5 = Organização clara e lógica em todo o texto

        FLUÊNCIA (Fluency)
          O relatório é lido de forma fluida como prosa financeira profissional?
          1 = Fraseado desajeitado e pouco natural ao longo do texto
          3 = Majoritariamente fluente; fraseado ocasionalmente desajeitado
          5 = Lido naturalmente como prosa financeira profissional
        ───────────────────────────────────────────────────────────────────
    """),
}

# Dimension display labels per language (maps canonical name → displayed label)
DIM_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "No-Omissions":  "No-Omissions",
        "No-Additions":  "No-Additions",
        "Grammaticality":"Grammaticality",
        "Coherence":     "Coherence",
        "Fluency":       "Fluency",
    },
    "pt_br": {
        "No-Omissions":  "Sem Omissões (No-Omissions)",
        "No-Additions":  "Sem Adições (No-Additions)",
        "Grammaticality":"Gramaticalidade (Grammaticality)",
        "Coherence":     "Coerência (Coherence)",
        "Fluency":       "Fluência (Fluency)",
    },
}

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def safe_slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (text or "sample").strip())


def wrap(text: str, width: int = 100) -> str:
    lines = []
    for paragraph in text.splitlines():
        lines.append(textwrap.fill(paragraph, width=width) if paragraph.strip() else "")
    return "\n".join(lines)


def page_text(text: str, lines_per_page: int = 40) -> None:
    all_lines = text.splitlines()
    for i in range(0, len(all_lines), lines_per_page):
        print("\n".join(all_lines[i: i + lines_per_page]))
        if i + lines_per_page < len(all_lines):
            try:
                input("  [Enter for more…]")
            except (EOFError, KeyboardInterrupt):
                break


def ask_score(prompt: str, lang: str) -> int:
    while True:
        raw = input(prompt).strip()
        if raw in {"1", "2", "3", "4", "5"}:
            return int(raw)
        if lang == "pt_br":
            print("  Por favor, insira um número de 1 a 5.")
        else:
            print("  Please enter a number from 1 to 5.")


def ask_justification(lang: str) -> str:
    if lang == "pt_br":
        raw = input("  Justificativa (opcional, Enter para pular): ").strip()
    else:
        raw = input("  Justification (optional, Enter to skip): ").strip()
    return raw

# ---------------------------------------------------------------------------
# Sample loading
# ---------------------------------------------------------------------------

def load_samples(condition: dict[str, Any]) -> list[dict[str, Any]]:
    nlg_dir: Path = condition["nlg_dir"]
    samples = []
    for jp in sorted(nlg_dir.glob("*.json")):
        if "sequence_summary" in jp.name:
            continue
        payload = json.loads(jp.read_text(encoding="utf-8"))
        meta = payload.get("sample_metadata") or {}
        name = str(
            meta.get("sample_name") or payload.get("sample_name") or jp.stem
        )
        date = str(
            meta.get("analysis_date") or payload.get("analysis_date") or ""
        )[:10]
        text = (
            payload.get("generated_text")
            or payload.get("final_response")
            or (jp.with_suffix(".txt").read_text(encoding="utf-8")
                if jp.with_suffix(".txt").exists() else "")
        ).strip()
        if not text:
            continue
        samples.append({
            "sample_name":   name,
            "analysis_date": date,
            "generated_text": text,
            "condition_key": condition["key"],
            "language":      condition["language"],
        })
    samples.sort(key=lambda s: s["analysis_date"])
    return samples

# ---------------------------------------------------------------------------
# Core evaluation loop
# ---------------------------------------------------------------------------

def evaluate_sample(
    sample: dict[str, Any],
    annotator: str,
    condition: dict[str, Any],
    overwrite: bool,
) -> dict[str, Any] | None:
    lang = condition["language"]
    out_dir = HUMAN_EVAL_ROOT / annotator / condition["key"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_slug(sample['sample_name'])}.json"

    if out_path.exists() and not overwrite:
        return json.loads(out_path.read_text(encoding="utf-8"))

    # ── Display report ────────────────────────────────────────────────────
    sep = "═" * 70
    print(f"\n{sep}")
    print(f"  Sample : {sample['sample_name']}")
    print(f"  Date   : {sample['analysis_date']}")
    print(f"  Lang   : {lang}")
    print(sep)
    page_text(wrap(sample["generated_text"]))
    print(f"\n{sep}\n")

    # ── Collect scores ────────────────────────────────────────────────────
    scores: dict[str, dict[str, Any]] = {}
    dim_labels = DIM_LABELS[lang]

    for dim in DIMENSIONS:
        label = dim_labels[dim]
        score = ask_score(f"  {label} [1–5]: ", lang)
        just  = ask_justification(lang)
        scores[dim] = {"Score": score, "Justification": just}

    result = {
        "sample_name":   sample["sample_name"],
        "analysis_date": sample["analysis_date"],
        "condition_key": condition["key"],
        "language":      lang,
        "annotator":     annotator,
        "judge":         f"human_{annotator}",
        "judge_label":   f"Human ({annotator})",
        "timestamp":     datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "scores":        scores,
    }
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result

# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def flatten(result: dict[str, Any]) -> dict[str, Any]:
    row = {
        "condition_key":  result["condition_key"],
        "language":       result["language"],
        "sample_name":    result["sample_name"],
        "analysis_date":  result["analysis_date"],
        "annotator":      result["annotator"],
        "judge":          result["judge"],
        "judge_label":    result["judge_label"],
    }
    nums: list[float] = []
    for dim in DIMENSIONS:
        key = dim.lower().replace("-", "_")
        val = result["scores"][dim]
        score = int(val["Score"]) if isinstance(val, dict) else int(val)
        row[f"{key}_score"] = score
        if isinstance(val, dict):
            row[f"{key}_justification"] = val.get("Justification", "")
        nums.append(score)
    row["mean_score"] = sum(nums) / len(nums)
    return row


def aggregate_results(annotator: str) -> pd.DataFrame:
    rows = []
    for cond in EVAL_CONDITIONS:
        out_dir = HUMAN_EVAL_ROOT / annotator / cond["key"]
        if not out_dir.exists():
            continue
        for p in sorted(out_dir.glob("*.json")):
            raw = json.loads(p.read_text(encoding="utf-8"))
            rows.append(flatten(raw))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    csv_path = HUMAN_EVAL_ROOT / f"all_results_{annotator}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nAggregated results saved: {csv_path.relative_to(PROJECT_ROOT)}")
    return df

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Human evaluation — 1–5 Likert scale")
    p.add_argument("--annotator", default="annotator1",
                   help="Annotator identifier (default: annotator1)")
    p.add_argument("--condition", default=None,
                   help="Evaluate only this condition key (e.g. us_default, br_default)")
    p.add_argument("--overwrite", action="store_true",
                   help="Re-score already-saved samples")
    p.add_argument("--aggregate-only", action="store_true",
                   help="Skip evaluation; just aggregate saved results to CSV")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    annotator: str = args.annotator.strip()
    HUMAN_EVAL_ROOT.mkdir(parents=True, exist_ok=True)

    conditions = (
        [c for c in EVAL_CONDITIONS if c["key"] == args.condition]
        if args.condition else EVAL_CONDITIONS
    )
    if not conditions:
        sys.exit(f"Unknown condition '{args.condition}'. "
                 f"Valid keys: {[c['key'] for c in EVAL_CONDITIONS]}")

    if args.aggregate_only:
        df = aggregate_results(annotator)
        if not df.empty:
            score_cols = [f"{d.lower().replace('-','_')}_score" for d in DIMENSIONS]
            print(df.groupby(["condition_key", "language"])[score_cols + ["mean_score"]].mean().round(3))
        return

    all_rows: list[dict[str, Any]] = []

    for cond in conditions:
        lang = cond["language"]
        print(INSTRUCTIONS[lang])

        if lang == "pt_br":
            input(f"  Pronto? Pressione Enter para começar a condição: {cond['label']}\n")
        else:
            input(f"  Ready? Press Enter to start condition: {cond['label']}\n")

        samples = load_samples(cond)
        if not samples:
            print(f"  No samples found in {cond['nlg_dir']}")
            continue

        total   = len(samples)
        scored  = sum(
            1 for s in samples
            if (HUMAN_EVAL_ROOT / annotator / cond["key"] / f"{safe_slug(s['sample_name'])}.json").exists()
        )
        pending = total - scored if not args.overwrite else total

        print(f"  Condition : {cond['label']}")
        print(f"  Samples   : {total} total, {scored} already scored, {pending} to do\n")

        for i, sample in enumerate(samples, 1):
            out_path = HUMAN_EVAL_ROOT / annotator / cond["key"] / f"{safe_slug(sample['sample_name'])}.json"
            if out_path.exists() and not args.overwrite:
                all_rows.append(flatten(json.loads(out_path.read_text(encoding="utf-8"))))
                continue

            if lang == "pt_br":
                print(f"\n  [{i}/{total}] Avaliando: {sample['sample_name']}")
            else:
                print(f"\n  [{i}/{total}] Evaluating: {sample['sample_name']}")

            try:
                result = evaluate_sample(sample, annotator, cond, args.overwrite)
            except KeyboardInterrupt:
                print("\n\nEvaluation paused. Progress saved. Re-run to continue.")
                break

            if result:
                all_rows.append(flatten(result))
                nums = [result["scores"][d]["Score"] for d in DIMENSIONS]
                mean = sum(nums) / len(nums)
                if lang == "pt_br":
                    print(f"  Salvo. Média = {mean:.2f}")
                else:
                    print(f"  Saved. Mean = {mean:.2f}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        csv_path = HUMAN_EVAL_ROOT / f"all_results_{annotator}.csv"
        df.to_csv(csv_path, index=False)
        score_cols = [f"{d.lower().replace('-','_')}_score" for d in DIMENSIONS]
        print(f"\n{'='*60}")
        print(f"Results saved: {csv_path.relative_to(PROJECT_ROOT)}")
        print("\nMean scores by condition:")
        print(df.groupby(["condition_key", "language"])[score_cols + ["mean_score"]].mean().round(3))


if __name__ == "__main__":
    main()

import os
import calendar
from datetime import datetime
from typing import List, Literal, Optional, Dict, Any

from IPython.display import Image, display
from langchain_core.runnables.graph_mermaid import MermaidDrawMethod

from agents.llm_model import UnifiedModel, model_name
from agents.utilities.token_tracker import token_tracker, track_response

from load_data import (
    build_multi_stock_prompt_context,
    load_generation_samples,
    save_result_to_json,
)
from agents.agent_prompts import (
    END_TO_END_GENERATION_PROMPT_EN,
    END_TO_END_GENERATION_PROMPT_GA,
    input_prompt,
)
from agents.agents_modules.workflow import (
    build_agent_workflow,
    build_agent_workflow_unified,
)

Language = Literal["en", "ga"]
WorkflowName = Literal[
    "default",
    "unified_worker",
]

LEGACY_INPUT_PROMPT = """You are an agent designed to generate text from structured data.
Use all provided facts, omit nothing material, and do not hallucinate.

Data:
{data}

Output: """


DEFAULT_AGENT_MODEL_OVERRIDES: Dict[str, str] = {
    "orchestrator": "gpt-5.4-mini",          # upgraded: needs to diagnose failures
    "content ordering": "gpt-5",
    "text structuring": "gpt-5",
    "surface realization": "gpt-5",
    "guardrail": "gpt-5.4-mini",      # acceptable, monitor for missed errors
    "finalizer": "gpt-5-mini",        # fine for copy-editing
    "unified_worker": "gpt-5",
}

DEFAULT_AGENT_REASONING_EFFORT_OVERRIDES: Dict[str, Optional[str]] = {
    "orchestrator": "medium",         # added: needs to reason through failures
    "content ordering": "high",
    "text structuring": "high",
    "surface realization": "high",
    "guardrail": "medium",            # added: needs careful cross-referencing
    "finalizer": None,                # fine as is
    "unified_worker": "high",         # added: single agent doing all three tasks
}
DEFAULT_E2E_REASONING_EFFORT: Optional[str] = "high"


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _extract_text_segments(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        segments: list[str] = []
        for item in value:
            segments.extend(_extract_text_segments(item))
        return segments
    if isinstance(value, dict):
        if value.get("type") == "reasoning":
            return []
        for key in ("text", "output_text"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                return [candidate]
        if "content" in value:
            return _extract_text_segments(value.get("content"))
        return []

    block_type = getattr(value, "type", None)
    if block_type == "reasoning":
        return []

    text_value = getattr(value, "text", None)
    if isinstance(text_value, str):
        return [text_value]

    output_text_value = getattr(value, "output_text", None)
    if isinstance(output_text_value, str):
        return [output_text_value]

    nested_content = getattr(value, "content", None)
    if nested_content is not None and nested_content is not value:
        return _extract_text_segments(nested_content)

    return []


def extract_generated_text(raw_output: Any) -> str:
    content = getattr(raw_output, "content", raw_output)
    text_segments = [segment.strip() for segment in _extract_text_segments(content) if str(segment).strip()]
    if text_segments:
        return "\n".join(text_segments).strip()
    return str(raw_output).strip()


class D2TAgentExperimentRunner:
    """
    Utility class to run data to text generation experiments with the
    multi agent system and its ablation architectures.

    Typical usage:
    - Instantiate the runner
    - Pick a sample id (1 based)
    - Pick a workflow
    - Call run_sample(...)
    """

    def __init__(
        self,
        provider: str = "openai",
        language: Language = "en",
        dataset_path: str = "results/final_report2025_us",
        dataset_kind: str = "auto",
        filings_index_path: Optional[str] = None,
        roic_dumps_dir: Optional[str] = None,
        previous_reports_path: Optional[str] = None,
        tickers: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_months_per_ticker: Optional[int] = None,
        min_stocks_per_month: int = 1,
        max_iteration: int = 100,
        output_dir: str = "results",
        agent_model_overrides: Optional[Dict[str, str]] = None,
        agent_reasoning_overrides: Optional[Dict[str, Optional[str]]] = None,
        e2e_reasoning_effort: Optional[str] = DEFAULT_E2E_REASONING_EFFORT,
    ) -> None:
        self.provider = provider
        self.language = language
        self.dataset_path = dataset_path
        self.dataset_kind = dataset_kind
        self.filings_index_path = filings_index_path
        self.roic_dumps_dir = roic_dumps_dir
        self.previous_reports_path = previous_reports_path
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.max_months_per_ticker = max_months_per_ticker
        self.min_stocks_per_month = min_stocks_per_month
        self.max_iteration = max_iteration
        self.output_dir = output_dir
        self.agent_model_overrides = {
            **DEFAULT_AGENT_MODEL_OVERRIDES,
            **(agent_model_overrides or {}),
        }
        self.agent_reasoning_overrides = {
            **DEFAULT_AGENT_REASONING_EFFORT_OVERRIDES,
            **(agent_reasoning_overrides or {}),
        }
        self.e2e_reasoning_effort = e2e_reasoning_effort

        os.makedirs(self.output_dir, exist_ok=True)

        self.samples: List[Dict[str, Any]] = self._load_samples(self.dataset_path)
        self.coverage_end_date = self._infer_coverage_end_date()
        self.triplesets: List[Any] = [sample["data_input"] for sample in self.samples]
        self.architectures: Dict[WorkflowName, Any] = self._build_workflows()

    def _load_samples(self, path: str) -> List[Dict[str, Any]]:
        samples = load_generation_samples(
            dataset_path=path,
            dataset_kind=self.dataset_kind,
            filings_index_path=self.filings_index_path,
            roic_dumps_dir=self.roic_dumps_dir,
            previous_reports_path=self.previous_reports_path,
            tickers=self.tickers,
            start_date=self.start_date,
            end_date=self.end_date,
            max_months_per_ticker=self.max_months_per_ticker,
            min_stocks_per_month=self.min_stocks_per_month,
        )
        print(
            f"Loaded {len(samples)} generation samples from {path} "
            f"(dataset_kind={self.dataset_kind})."
        )
        return samples

    def _build_workflows(self) -> Dict[WorkflowName, Any]:
        """
        Build and cache all workflow variants for the given provider and language.
        """
        print(f"Building workflows with provider={self.provider}, language={self.language}.")

        workflows: Dict[WorkflowName, Any] = {}

        # Default architecture
        workflows["default"] = build_agent_workflow(
            provider=self.provider,
            language=self.language,
            agent_model_overrides=self.agent_model_overrides,
            agent_reasoning_overrides=self.agent_reasoning_overrides,
        )
        
        # New unified worker architecture
        workflows["unified_worker"] = build_agent_workflow_unified(
            provider=self.provider,
            language=self.language,
            agent_model_overrides=self.agent_model_overrides,
            agent_reasoning_overrides=self.agent_reasoning_overrides,
        )

        print("Workflows built:", list(workflows.keys()))
        return workflows

    @property
    def inspect_data(self):
        data = self.triplesets
        num_samples = len(self.triplesets)
        triple_lengths = [{i: len(j)} for i, j in enumerate(self.triplesets, start=1)]
        return data, num_samples, triple_lengths

    @property
    def sample_catalog(self) -> List[Dict[str, Any]]:
        catalog: List[Dict[str, Any]] = []
        for sample in self.samples:
            catalog.append(
                {
                    "sample_id": sample["sample_id"],
                    "sample_name": sample.get("sample_name"),
                    "ticker": sample.get("ticker"),
                    "analysis_date": sample.get("analysis_date"),
                    "tickers": sample.get("tickers"),
                    "ticker_count": sample.get("ticker_count"),
                }
            )
        return catalog

    def get_sample_metadata(self, sample_id: int) -> Dict[str, Any]:
        if sample_id < 1 or sample_id > len(self.samples):
            raise IndexError(
                f"sample_id {sample_id} is out of range. "
                f"Valid range is 1 to {len(self.samples)}."
            )
        return self.samples[sample_id - 1]

    def find_sample_id(self, ticker: str, analysis_date: str) -> int:
        wanted_ticker = str(ticker).strip().upper()
        wanted_date = str(analysis_date).strip()[:10]

        for sample in self.samples:
            sample_date = str(sample.get("analysis_date", "")).strip()[:10]
            if sample_date != wanted_date:
                continue

            if str(sample.get("ticker", "")).strip().upper() == wanted_ticker:
                return int(sample["sample_id"])

            tickers = {
                str(t).strip().upper()
                for t in (sample.get("tickers") or [])
            }
            if wanted_ticker in tickers:
                return int(sample["sample_id"])

        raise KeyError(
            f"No sample found for ticker='{wanted_ticker}' and analysis_date='{wanted_date}'."
        )

    def find_month_sample_id(self, analysis_date: str) -> int:
        wanted_date = str(analysis_date).strip()[:10]
        for sample in self.samples:
            if str(sample.get("analysis_date", "")).strip()[:10] == wanted_date:
                return int(sample["sample_id"])
        raise KeyError(f"No sample found for analysis_date='{wanted_date}'.")

    def _compute_report_metadata(
        self,
        sample_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        analysis_date_str = str((sample_meta or {}).get("analysis_date", "")).strip()[:10]
        horizon_months = ""
        end_date_str = self.coverage_end_date

        if analysis_date_str and end_date_str:
            try:
                a_year, a_month = int(analysis_date_str[:4]), int(analysis_date_str[5:7])
                e_year, e_month = int(end_date_str[:4]), int(end_date_str[5:7])
                remaining = (e_year - a_year) * 12 + (e_month - a_month)
                horizon_months = str(max(remaining, 0))
            except Exception:
                horizon_months = ""

        return {
            "analysis_date": analysis_date_str,
            "horizon_months": horizon_months,
            "end_date": end_date_str,
        }

    def _infer_coverage_end_date(self) -> str:
        candidate_dates: List[str] = []

        if self.end_date:
            candidate_dates.append(str(self.end_date).strip()[:10])

        for sample in self.samples:
            value = str(sample.get("analysis_date", "")).strip()[:10]
            if value:
                candidate_dates.append(value)

        if not candidate_dates:
            return ""

        latest = max(candidate_dates)
        try:
            parsed = datetime.fromisoformat(latest)
            month_end_day = calendar.monthrange(parsed.year, parsed.month)[1]
            return parsed.replace(day=month_end_day).date().isoformat()
        except Exception:
            return latest

    def build_query(
        self,
        data: Any,
        custom_prefix: Optional[str] = None,
        sample_meta: Optional[Dict[str, Any]] = None,
        previous_report: Optional[str] = None,
    ) -> str:
        """
        Build the natural language query that seeds the orchestrator.

        Uses the global input_prompt template:

            Data: {data}
        """
        template = custom_prefix if custom_prefix is not None else input_prompt
        if (
            custom_prefix is None
            and not (sample_meta or {}).get("ticker")
            and not (sample_meta or {}).get("tickers")
        ):
            template = LEGACY_INPUT_PROMPT
        prompt_data = data
        if sample_meta and sample_meta.get("sample_type") == "multi_stock_monthly":
            prompt_data = build_multi_stock_prompt_context(
                analysis_date=str(sample_meta.get("analysis_date", "")),
                stock_rows=sample_meta.get("stocks", []),
                previous_report=(
                    previous_report
                    if previous_report is not None
                    else str(sample_meta.get("previous_report", "N/A"))
                ),
            )
        elif sample_meta and sample_meta.get("prompt_context"):
            prompt_data = sample_meta["prompt_context"]

        tickers_value = sample_meta.get("tickers") if sample_meta else None
        report_meta = self._compute_report_metadata(sample_meta)

        values: Dict[str, Any] = {
            "data": prompt_data,
            "ticker": (sample_meta or {}).get("ticker", ""),
            "analysis_date": report_meta["analysis_date"],
            "sample_name": (sample_meta or {}).get("sample_name", ""),
            "tickers": ", ".join(tickers_value) if isinstance(tickers_value, list) else "",
            "ticker_count": (sample_meta or {}).get("ticker_count", ""),
            "horizon_months": report_meta["horizon_months"],
            "end_date": report_meta["end_date"],
            "previous_report": (
                previous_report
                if previous_report is not None
                else (sample_meta or {}).get("previous_report", "N/A")
            ),
        }
        query = template.format_map(_SafeFormatDict(values))
        return query

    def build_initial_state(
        self,
        data: Any,
        query: str,
        sample_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Construct the initial ExecutionState for LangGraph.
        """
        report_meta = self._compute_report_metadata(sample_meta)
        initial_state: Dict[str, Any] = {
            "data_input": data,
            "user_prompt": query,
            "sample_metadata": sample_meta or {},
            "analysis_date": report_meta["analysis_date"],
            "horizon_months": report_meta["horizon_months"],
            "end_date": report_meta["end_date"],
            "raw_data": data,
            "history_of_steps": [],
            "final_response": "",
            "next_agent": "",
            "next_agent_payload": "",
            "current_step": 0,
            "iteration_count": 0,
            "max_iteration": self.max_iteration,
            # new fields
            "worker_attempts": {},      # Counter for no. of worker attempts e.g. {"surface realization": 2, ...}
            "last_worker": "",
            # "max_worker_attempts": 3,        # global cap. number of runs per worker"
            "max_worker_attempts": {
                        "content ordering": 3,
                        "text structuring": 3,
                        "surface realization": 3,
                    },
        }
        return initial_state

    def get_workflow(self, workflow: WorkflowName = "default") -> Any:
        if workflow not in self.architectures:
            raise ValueError(
                f"Unknown workflow '{workflow}'. "
                f"Available: {list(self.architectures.keys())}"
            )
        return self.architectures[workflow]

    def run_sample(
        self,
        sample_id: int,
        workflow: WorkflowName = "default",
        save: bool = True,
        save_prefix: Optional[str] = None,
        previous_report: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run a chosen workflow on a single sample.

        Parameters
        ----------
        sample_id:
            1 based sample id (sample 1, sample 2, etc).
        workflow:
            Which architecture to use, for example "default" or "no_orchestrator".
        save:
            Whether to save the full state as JSON.
        save_prefix:
            Optional prefix for the JSON filename. If None, uses workflow name.

        Returns
        -------
        state:
            Final state returned by the LangGraph workflow.
        """
        if sample_id < 1 or sample_id > len(self.triplesets):
            raise IndexError(
                f"sample_id {sample_id} is out of range. "
                f"Valid range is 1 to {len(self.triplesets)}."
            )

        index = sample_id - 1
        sample_meta = self.samples[index]
        data = sample_meta["data_input"]
        print(
            f"Running workflow='{workflow}' on sample_id={sample_id} "
            f"(sample={sample_meta.get('sample_name', index)})."
        )

        query = self.build_query(
            data=data,
            sample_meta=sample_meta,
            previous_report=previous_report,
        )
        initial_state = self.build_initial_state(data=data, query=query, sample_meta=sample_meta)

        graph = self.get_workflow(workflow)

        token_tracker.reset()
        state = graph.invoke(
            initial_state,
            config={"recursion_limit": initial_state["max_iteration"]},
        )
        state["token_usage"] = token_tracker.as_dict()
        print(token_tracker.summary_str())

        if save:
            if save_prefix is None:
                save_prefix = workflow
            sample_slug = sample_meta.get("sample_name", f"sample{sample_id}")
            filename = f"{save_prefix}_{sample_slug}.json"
            save_result_to_json(
                state,
                dataset_folder="",
                filename=filename,
                directory=self.output_dir,
            )
            save_path = os.path.join(self.output_dir, filename)
            state["saved_state_path"] = save_path
            print(f"Saved state to {save_path}.")

        return state
    
    def run_end_to_end(
        self,
        sample_id: int,
        provider: Optional[str] = None,
        temperature: float = 0.0,
        extra_model_kwargs: Optional[Dict[str, Any]] = None,
        previous_report: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run a single end to end data to text generation using a single LLM
        with the END_TO_END_GENERATION_PROMPT in the configured language.

        Returns a dict with:
          - "generated_text": final text from the model
          - "query": the textual prompt given to the model
          - "data": the raw triples for this sample
          - "raw_output": the full object returned by the LLM
        """
        if sample_id < 1 or sample_id > len(self.triplesets):
            raise IndexError(
                f"sample_id {sample_id} is out of range. "
                f"Valid range is 1 to {len(self.triplesets)}."
            )

        index = sample_id - 1
        sample_meta = self.samples[index]
        data = sample_meta["data_input"]

        # Build the textual input that will be fed as {input} to the LLM
        query = self.build_query(
            data=data,
            sample_meta=sample_meta,
            previous_report=previous_report,
        )

        # Choose provider and model config
        provider = provider or self.provider
        base_conf = model_name.get(provider.lower(), {}).copy()
        base_conf["temperature"] = temperature
        if provider.lower() == "openai" and self.e2e_reasoning_effort is not None:
            base_conf["reasoning_effort"] = self.e2e_reasoning_effort
        if extra_model_kwargs:
            base_conf.update(extra_model_kwargs)

        # Pick the correct end to end prompt based on language
        if self.language == "ga":
            system_prompt = END_TO_END_GENERATION_PROMPT_GA
        else:
            system_prompt = END_TO_END_GENERATION_PROMPT_EN

        # Build the chat model with the chosen system prompt
        llm = UnifiedModel(provider=provider, **base_conf).model_(system_prompt)

        # Invoke the model
        token_tracker.reset()
        raw_output = llm.invoke({"input": query})
        track_response(raw_output)
        print(token_tracker.summary_str())
        generated_text = extract_generated_text(raw_output)

        return {
            "generated_text": generated_text,
            "query": query,
            "data": data,
            "sample_metadata": sample_meta,
            "raw_output": raw_output,
            "token_usage": token_tracker.as_dict(),
        }

    def run_month(
        self,
        ticker: str,
        analysis_date: str,
        workflow: WorkflowName = "default",
        save: bool = True,
        save_prefix: Optional[str] = None,
        previous_report: Optional[str] = None,
    ) -> Dict[str, Any]:
        sample_id = self.find_sample_id(ticker=ticker, analysis_date=analysis_date)
        return self.run_sample(
            sample_id=sample_id,
            workflow=workflow,
            save=save,
            save_prefix=save_prefix,
            previous_report=previous_report,
        )

    def run_analysis_month(
        self,
        analysis_date: str,
        workflow: WorkflowName = "default",
        save: bool = True,
        save_prefix: Optional[str] = None,
        previous_report: Optional[str] = None,
    ) -> Dict[str, Any]:
        sample_id = self.find_month_sample_id(analysis_date=analysis_date)
        return self.run_sample(
            sample_id=sample_id,
            workflow=workflow,
            save=save,
            save_prefix=save_prefix,
            previous_report=previous_report,
        )

    def run_sequence(
        self,
        workflow: WorkflowName = "default",
        save: bool = True,
        save_prefix: Optional[str] = None,
        sample_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        ordered_ids = sample_ids or [sample["sample_id"] for sample in self.samples]
        previous_report: Optional[str] = None
        outputs: List[Dict[str, Any]] = []

        for sample_id in ordered_ids:
            state = self.run_sample(
                sample_id=sample_id,
                workflow=workflow,
                save=save,
                save_prefix=save_prefix,
                previous_report=previous_report,
            )
            sample_meta = self.get_sample_metadata(sample_id)
            previous_report = str(state.get("final_response", "")).strip() or previous_report
            outputs.append(
                {
                    "sample_id": sample_id,
                    "analysis_date": sample_meta.get("analysis_date"),
                    "sample_name": sample_meta.get("sample_name"),
                    "final_response": state.get("final_response", ""),
                }
            )

        return outputs

    def run_end_to_end_sequence(
        self,
        provider: Optional[str] = None,
        temperature: float = 0.0,
        extra_model_kwargs: Optional[Dict[str, Any]] = None,
        sample_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        ordered_ids = sample_ids or [sample["sample_id"] for sample in self.samples]
        previous_report: Optional[str] = None
        outputs: List[Dict[str, Any]] = []

        for sample_id in ordered_ids:
            result = self.run_end_to_end(
                sample_id=sample_id,
                provider=provider,
                temperature=temperature,
                extra_model_kwargs=extra_model_kwargs,
                previous_report=previous_report,
            )
            previous_report = str(result.get("generated_text", "")).strip() or previous_report
            outputs.append(result)

        return outputs


    def show_workflow_graph(
        self,
        workflow: WorkflowName = "default",
        xray: bool = True,
    ) -> None:
        """
        Display a Mermaid diagram of the selected workflow in Jupyter.
        """
        graph = self.get_workflow(workflow)
        png_bytes = graph.get_graph(xray=xray).draw_mermaid_png(
            draw_method=MermaidDrawMethod.API
        )
        display(Image(png_bytes))

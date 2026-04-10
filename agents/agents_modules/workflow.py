__author__ = "chinonsocynthiaosuji"

"""
Author: Chinonso Cynthia Osuji
Date: 01/10/2025
Description:
    Define the workflow for the data-to-text agent system with ablations.
"""

import re
from typing import List, Dict, Union, Any, Literal, Optional
from langgraph.graph import START, END, StateGraph
from agents.utilities.utils import ExecutionState
from agents.agents_modules.orchestrator import TaskOrchestrator
from agents.agents_modules.worker import TaskWorker
from agents.agents_modules.guardrail import TaskGuardrail
from agents.agents_modules.finalizer import TaskFinalizer
from agents.agents_modules.task import UnifiedTaskWorker
from agents.agent_prompts import (
    CONTENT_ORDERING_PROMPT,
    TEXT_STRUCTURING_PROMPT,
    SURFACE_REALIZATION_PROMPT_EN,
    SURFACE_REALIZATION_PROMPT_GA,
)

LanguageCode = Literal["en", "ga"]

# Default surface realization prompt used by legacy code
SURFACE_REALIZATION_PROMPT = SURFACE_REALIZATION_PROMPT_EN

# Worker role maps per language
WORKER_ROLES_EN: Dict[str, str] = {
    "content ordering": CONTENT_ORDERING_PROMPT,
    "text structuring": TEXT_STRUCTURING_PROMPT,
    "surface realization": SURFACE_REALIZATION_PROMPT_EN,
}

WORKER_ROLES_GA: Dict[str, str] = {
    "content ordering": CONTENT_ORDERING_PROMPT,  # still in English for now
    "text structuring": TEXT_STRUCTURING_PROMPT,  # still in English for now
    "surface realization": SURFACE_REALIZATION_PROMPT_GA,
}

# Backwards compatible default
WORKER_ROLES = WORKER_ROLES_EN
EXPECTED_WORKER_ORDER: List[str] = [
    "content ordering",
    "text structuring",
    "surface realization",
]


def get_worker_roles(language: LanguageCode) -> Dict[str, str]:
    if language == "ga":
        return WORKER_ROLES_GA
    return WORKER_ROLES_EN


def get_surface_prompt(language: LanguageCode) -> str:
    if language == "ga":
        return SURFACE_REALIZATION_PROMPT_GA
    return SURFACE_REALIZATION_PROMPT_EN


def _is_correct_review(raw_text: str) -> bool:
    text = str(raw_text or "").strip()
    if not text:
        return False

    overall_match = re.search(r"OVERALL:\s*(.+)$", text, re.MULTILINE | re.IGNORECASE)
    candidate = overall_match.group(1).strip() if overall_match else text
    candidate = candidate.splitlines()[0].strip()

    return bool(re.match(r"^CORRECT\b", candidate, re.IGNORECASE))


def add_workers_(
    worker_prompts: Dict[str, str],
    graph: StateGraph,
    tools: List[Any],
    user_prompt: Union[str, Dict[str, Any]],
    provider: str,
    worker_model_overrides: Dict[str, str] | None = None,
    worker_reasoning_overrides: Dict[str, Optional[str]] | None = None,
    worker_success_next_agents: Dict[str, str] | None = None,
) -> List[str]:
    added: List[str] = []
    for name, prompt in worker_prompts.items():
        model = TaskWorker.init(
            description=prompt,
            tools=tools,
            context=user_prompt,
            provider=provider,
            model_name_override=(worker_model_overrides or {}).get(name),
            reasoning_effort=(worker_reasoning_overrides or {}).get(name),
        )
        graph.add_node(
            name,
            TaskWorker.execute(
                model,
                role=name,
                success_next_agent=(worker_success_next_agents or {}).get(
                    name,
                    "guardrail",
                ),
            ),
        )
        added.append(name)
    return added


def add_workers(
    worker_prompts: Dict[str, str],
    graph: StateGraph,
    tools: List[Any],
    provider: str,
    worker_model_overrides: Dict[str, str] | None = None,
    worker_reasoning_overrides: Dict[str, Optional[str]] | None = None,
    worker_success_next_agents: Dict[str, str] | None = None,
) -> List[str]: 
    added: List[str] = []
    for name, prompt in worker_prompts.items():

        def node_fn(state, prompt=prompt, name=name):
            user_prompt = state.get("user_prompt", "")
            model = TaskWorker.init(
                description=prompt,
                tools=tools,
                context=user_prompt,
                provider=provider,
                model_name_override=(worker_model_overrides or {}).get(name),
                reasoning_effort=(worker_reasoning_overrides or {}).get(name),
            )
            return TaskWorker.execute(
                model,
                role=name,
                success_next_agent=(worker_success_next_agents or {}).get(
                    name,
                    "guardrail",
                ),
            )(state)

        graph.add_node(name, node_fn)
        added.append(name)
    return added


# def guardrail_routing(state: ExecutionState) -> Literal["orchestrator", "finalizer"]:
#     expected = {"content ordering", "text structuring", "surface realization"}
#     done = {
#         step.agent_name.strip().lower()
#         for step in state.get("history_of_steps", [])
#         if getattr(step, "agent_name", None)
#         and step.agent_name.strip().lower() in expected
#     }
#     review = state.get("review", "").strip().lower()

#     # 1. Explicit rerun instructions always win
#     if "rerun surface realization with feedback" in review:
#         return "orchestrator"

#     # 2. Finalize only if we are sure it is correct
#     if expected.issubset(done) and "incorrect" not in review and "correct" in review:
#         return "finalizer"

#     # 3. Otherwise keep orchestrating
#     return "orchestrator"

def guardrail_routing(state: ExecutionState) -> Literal["orchestrator", "finalizer"]:
    expected = {"content ordering", "text structuring", "surface realization"}
    review = state.get("review", "").strip().lower()
    review_raw = str(state.get("review", "") or "")
    passed_stages = {
        str(name).strip().lower()
        for name in (state.get("passed_stages", []) or [])
        if str(name).strip()
    }
    passed_stages = {name for name in passed_stages if name in expected}

    review_is_correct = _is_correct_review(review_raw)

    # Explicit rerun instructions still win if we have not exhausted attempts
    if "rerun surface realization with feedback" in review:
        return "orchestrator"

    # Finalize only if all three workers have run and the review says correct
    if expected.issubset(passed_stages) and review_is_correct:
        return "finalizer"

    # Otherwise keep orchestrating
    return "orchestrator"


def worker_routing(state: ExecutionState) -> Literal["guardrail", "orchestrator"]:
    next_agent = str(state.get("next_agent", "") or "").strip().lower()
    if next_agent == "orchestrator":
        return "orchestrator"
    return "guardrail"


def _worker_model_overrides(
    worker_roles: Dict[str, str],
    agent_model_overrides: Dict[str, str] | None,
) -> Dict[str, str]:
    return {
        name: model
        for name, model in (agent_model_overrides or {}).items()
        if name in worker_roles
    }


def _worker_reasoning_overrides(
    worker_roles: Dict[str, str],
    agent_reasoning_overrides: Dict[str, Optional[str]] | None,
) -> Dict[str, Optional[str]]:
    return {
        name: effort
        for name, effort in (agent_reasoning_overrides or {}).items()
        if name in worker_roles
    }


def _add_standard_workers(
    flow: StateGraph,
    worker_roles: Dict[str, str],
    provider: str,
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
    worker_success_next_agents: Dict[str, str] | None = None,
) -> List[str]:
    return add_workers_(
        worker_roles,
        flow,
        [],
        "",
        provider,
        worker_model_overrides=_worker_model_overrides(
            worker_roles,
            agent_model_overrides,
        ),
        worker_reasoning_overrides=_worker_reasoning_overrides(
            worker_roles,
            agent_reasoning_overrides,
        ),
        worker_success_next_agents=worker_success_next_agents,
    )


def _ordered_unique_stages(values: Any) -> List[str]:
    seen = {
        str(name).strip().lower()
        for name in (values or [])
        if str(name).strip()
    }
    return [name for name in EXPECTED_WORKER_ORDER if name in seen]


def _latest_worker_output(state: ExecutionState) -> str:
    history = state.get("history_of_steps", []) or []
    preferred_order = [
        "surface realization",
        "text structuring",
        "content ordering",
    ]
    for preferred_name in preferred_order:
        for step in reversed(history):
            step_name = str(getattr(step, "agent_name", "") or "").strip().lower()
            if step_name == preferred_name:
                return str(getattr(step, "agent_output", "") or "")
    return str(state.get("final_response", "") or "")


def final_response_passthrough(state: ExecutionState) -> Dict[str, Any]:
    """
    Terminal node for no-finalizer ablations.
    It does not call an LLM; it exposes the latest worker output as the result.
    """
    return {
        "final_response": _latest_worker_output(state),
        "history_of_steps": state.get("history_of_steps", []) or [],
    }


def accept_worker_output_without_guardrail(state: ExecutionState) -> Dict[str, Any]:
    """
    Bookkeeping node for no-guardrail ablations.
    It marks the latest worker stage as complete without evaluating the output.
    """
    last_worker = str(state.get("last_worker", "") or "").strip().lower()
    passed_stages = _ordered_unique_stages(state.get("passed_stages", []))
    closed_stages = _ordered_unique_stages(state.get("closed_stages", []))

    if last_worker in EXPECTED_WORKER_ORDER:
        if last_worker not in passed_stages:
            passed_stages.append(last_worker)
        if last_worker not in closed_stages:
            closed_stages.append(last_worker)

    return {
        "next_agent": "orchestrator",
        "review": "",
        "history_of_steps": state.get("history_of_steps", []) or [],
        "iteration_count": state.get("iteration_count", 0),
        "max_iteration": state.get("max_iteration", 50),
        "worker_attempts": state.get("worker_attempts", {}) or {},
        "last_worker": last_worker,
        "passed_stages": _ordered_unique_stages(passed_stages),
        "closed_stages": _ordered_unique_stages(closed_stages),
    }



def build_agent_workflow(
    provider: str = "ollama",
    language: LanguageCode = "en",
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
) -> StateGraph:
    flow = StateGraph(ExecutionState)

    # workers = list(WORKER_ROLES.keys())

    # Pick language specific worker prompts
    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")

    # Orchestrator
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(
            TaskOrchestrator.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("orchestrator"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("orchestrator"),
            )
        ),
    )

    # Workers
    tools = []
    user_prompt = ""
    worker_model_overrides = {
        name: model
        for name, model in (agent_model_overrides or {}).items()
        if name in worker_roles
    }
    worker_reasoning_overrides = {
        name: effort
        for name, effort in (agent_reasoning_overrides or {}).items()
        if name in worker_roles
    }
    add_workers_(
        worker_roles,
        flow,
        tools,
        user_prompt,
        provider,
        worker_model_overrides=worker_model_overrides,
        worker_reasoning_overrides=worker_reasoning_overrides,
    ) #remove user prompt
    # add_workers(WORKER_ROLES, flow, tools, provider) #Add user prompt

    # guardrail & Finalizer
    flow.add_node(
        "guardrail",
        TaskGuardrail.evaluate(
            TaskGuardrail.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("guardrail"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("guardrail"),
            )
        ),
    )
    flow.add_node(
        "finalizer",
        TaskFinalizer.compile(
            TaskFinalizer.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("finalizer"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("finalizer"),
            )
        ),
    )

    # Routing
    routes = {name: name for name in workers}
    routes.update({"finish": "finalizer"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)
    for w in workers:
        flow.add_conditional_edges(
            w,
            worker_routing,
            {"guardrail": "guardrail", "orchestrator": "orchestrator"},
        )
    flow.add_conditional_edges("guardrail", guardrail_routing) #Original
    # flow.add_edge("guardrail", "orchestrator") # Always to orchestrator
    flow.add_edge("finalizer", END)

    return flow.compile()


def build_agent_workflow_unified(
    provider: str = "ollama",
    language: LanguageCode = "en",
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
):
    """
    Ablation: single unified worker that performs content ordering,
    text structuring and surface realization using one prompt.

    Architecture:
      orchestrator + 1 unified worker node + guardrail + finalizer

    The orchestrator still chooses between logical stages
    ('content ordering', 'text structuring', 'surface realization'),
    but all of them are routed to the same LangGraph node.
    """
    flow = StateGraph(ExecutionState)

    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    # Orchestrator entry
    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(
            TaskOrchestrator.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("orchestrator"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("orchestrator"),
            )
        ),
    )

    # Unified worker node
    unified_model = UnifiedTaskWorker.init(
        provider=provider,
        language=language,
        model_name_override=(agent_model_overrides or {}).get("unified_worker"),
        reasoning_effort=(agent_reasoning_overrides or {}).get("unified_worker"),
    )

    flow.add_node("task", UnifiedTaskWorker.execute(unified_model, language=language))

    # Guardrail and finalizer
    flow.add_node(
        "guardrail",
        TaskGuardrail.evaluate(
            TaskGuardrail.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("guardrail"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("guardrail"),
            )
        ),
    )
    flow.add_node(
        "finalizer",
        TaskFinalizer.compile(
            TaskFinalizer.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("finalizer"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("finalizer"),
            )
        ),
    )

    # Routing: all logical stages go to the same node "task"
    routes = {name: "task" for name in workers}
    routes.update({"finish": "finalizer"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)

    flow.add_conditional_edges(
        "task",
        worker_routing,
        {"guardrail": "guardrail", "orchestrator": "orchestrator"},
    )
    flow.add_conditional_edges("guardrail", guardrail_routing)
    flow.add_edge("finalizer", END)

    return flow.compile()


#################################################################################################################
# The codes below are for ablation studies
#################################################################################################################

def build_agent_workflow_single_module(provider: str = "ollama", language: LanguageCode = "en",):
    """
    Ablation 1.
    Use a single generic worker module for content ordering, text structuring
    and surface realization instead of three specialized worker prompts.

    Architecture:
      orchestrator + 3 worker roles (CO, TS, SR) + guardrail + finalizer
    But all three worker roles share the same description prompt, so they are
    effectively the same module.
    """
    flow = StateGraph(ExecutionState)

    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    # Orchestrator entry
    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(TaskOrchestrator.init(provider)),
    )

    # Workers.
    tools: List[Any] = []
    user_prompt: Union[str, Dict[str, Any]] = ""
    single_prompt = get_surface_prompt(language)

    unified_worker_prompts: Dict[str, str] = {
        name: single_prompt for name in worker_roles.keys()
    }
    add_workers_(unified_worker_prompts, flow, tools, user_prompt, provider)

    # Guardrail and Finalizer stay as in the default
    flow.add_node("guardrail", TaskGuardrail.evaluate(TaskGuardrail.init(provider)))
    flow.add_node("finalizer", TaskFinalizer.compile(TaskFinalizer.init(provider)))

    # Routing identical to default
    routes = {name: name for name in workers}
    routes.update({"finish": "finalizer"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)

    for w in workers:
        flow.add_edge(w, "guardrail")

    flow.add_conditional_edges("guardrail", guardrail_routing)
    flow.add_edge("finalizer", END)

    return flow.compile()


def build_agent_workflow_no_guardrail(provider: str = "ollama", language: LanguageCode = "en",):
    """
    Ablation 2.
    Remove the Guardrail LLM module.
    Workers send their output directly back to the orchestrator, and the
    orchestrator decides when to finish and hand off to the finalizer.
    """
    flow = StateGraph(ExecutionState)

    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    # Orchestrator entry
    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(TaskOrchestrator.init(provider)),
    )

    # Workers: same as default, including separate CO, TS, SR roles
    tools: List[Any] = []
    user_prompt: Union[str, Dict[str, Any]] = ""
    add_workers_(worker_roles, flow, tools, user_prompt, provider)

    # No Guardrail LLM. Replace with a simple passthrough node so that
    # we do not have to change TaskWorker logic which returns next_agent="guardrail".
    def noop_guardrail(state: ExecutionState) -> ExecutionState:
        state["review"] = "Guardrail disabled for this ablation."
        return state

    flow.add_node("guardrail", noop_guardrail)

    # Finalizer remains the same
    flow.add_node("finalizer", TaskFinalizer.compile(TaskFinalizer.init(provider)))

    # Routing.
    routes = {name: name for name in workers}
    routes.update({"finish": "finalizer"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)

    # Workers go to the noop guardrail, which always goes straight back to orchestrator.
    for w in workers:
        flow.add_edge(w, "guardrail")

    flow.add_edge("guardrail", "orchestrator")
    flow.add_edge("finalizer", END)

    return flow.compile()


def build_agent_workflow_no_finalizer(provider: str = "ollama", language: LanguageCode = "en",):
    """
    Ablation 3.
    Remove the Finalizer LLM module.
    The system stops once the orchestrator signals completion.
    The last worker output is treated as the final output.
    """
    flow = StateGraph(ExecutionState)

    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    # Orchestrator entry
    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(TaskOrchestrator.init(provider)),
    )

    # Workers unchanged
    tools: List[Any] = []
    user_prompt: Union[str, Dict[str, Any]] = ""
    add_workers_(worker_roles, flow, tools, user_prompt, provider)

    # Guardrail unchanged
    flow.add_node("guardrail", TaskGuardrail.evaluate(TaskGuardrail.init(provider)))

    # No Finalizer LLM. Use a simple identity node that just terminates the graph.
    def noop_finalizer(state: ExecutionState) -> ExecutionState:
        return state

    flow.add_node("finalizer", noop_finalizer)

    # Routing largely matches default.
    routes = {name: name for name in workers}
    routes.update({"finish": "finalizer"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)

    for w in workers:
        flow.add_edge(w, "guardrail")

    flow.add_conditional_edges("guardrail", guardrail_routing)

    flow.add_edge("finalizer", END)

    return flow.compile()


def build_agent_workflow_no_orchestrator(provider: str = "ollama", language: LanguageCode = "en",):
    """
    Ablation 4.
    Remove the Orchestrator.
    Run content ordering, text structuring and surface realization as a
    fixed workflow: CO -> TS -> SR, with guardrail and finalizer still present.

    The workers no longer depend on state["next_agent"] for routing.
    The execution order is purely given by the graph structure.
    """
    flow = StateGraph(ExecutionState)

    worker_roles = get_worker_roles(language)

    # Fixed worker order
    ordered_roles: List[str] = ["content ordering", "text structuring", "surface realization",]

    # Entry point is now the first worker, not the orchestrator
    first_role = ordered_roles[0]
    flow.add_edge(START, first_role)
    flow.set_entry_point(first_role)

    # Workers with their usual prompts
    tools: List[Any] = []
    user_prompt: Union[str, Dict[str, Any]] = ""
    add_workers_(worker_roles, flow, tools, user_prompt, provider)

    # Guardrail instances, one per stage
    guardrail_agent = TaskGuardrail.init(provider)

    flow.add_node("guardrail_co", TaskGuardrail.evaluate(guardrail_agent))
    flow.add_node("guardrail_ts", TaskGuardrail.evaluate(guardrail_agent))
    flow.add_node("guardrail_sr", TaskGuardrail.evaluate(guardrail_agent))

    # Finalizer as in the default configuration
    flow.add_node("finalizer", TaskFinalizer.compile(TaskFinalizer.init(provider)))

    # Deterministic workflow:
    # START -> CO -> guardrail_co -> TS -> guardrail_ts -> SR -> guardrail_sr -> finalizer -> END
    flow.add_edge("content ordering", "guardrail_co")
    flow.add_edge("guardrail_co", "text structuring")

    flow.add_edge("text structuring", "guardrail_ts")
    flow.add_edge("guardrail_ts", "surface realization")

    flow.add_edge("surface realization", "guardrail_sr")
    flow.add_edge("guardrail_sr", "finalizer")

    flow.add_edge("finalizer", END)

    return flow.compile()


def build_agent_workflow_no_orchestrator_no_guardrail_no_finalizer(
    provider: str = "ollama",
    language: LanguageCode = "en",
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
):
    """
    Ablation study 1.
    Remove orchestration, guardrail evaluation, and finalizer polishing.

    Architecture:
      content ordering -> text structuring -> surface realization -> END
    """
    flow = StateGraph(ExecutionState)
    worker_roles = get_worker_roles(language)

    first_role = EXPECTED_WORKER_ORDER[0]
    flow.add_edge(START, first_role)
    flow.set_entry_point(first_role)

    _add_standard_workers(
        flow,
        worker_roles,
        provider,
        agent_model_overrides=agent_model_overrides,
        agent_reasoning_overrides=agent_reasoning_overrides,
        worker_success_next_agents={
            "content ordering": "text structuring",
            "text structuring": "surface realization",
            "surface realization": "terminal_output",
        },
    )
    flow.add_node("terminal_output", final_response_passthrough)

    flow.add_edge("content ordering", "text structuring")
    flow.add_edge("text structuring", "surface realization")
    flow.add_edge("surface realization", "terminal_output")
    flow.add_edge("terminal_output", END)

    return flow.compile()


def build_agent_workflow_no_orchestrator_no_finalizer(
    provider: str = "ollama",
    language: LanguageCode = "en",
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
):
    """
    Ablation study 2.
    Remove the orchestrator and finalizer, but keep guardrail evaluation after
    each fixed worker stage.

    Architecture:
      content ordering -> guardrail -> text structuring -> guardrail ->
      surface realization -> guardrail -> END
    """
    flow = StateGraph(ExecutionState)
    worker_roles = get_worker_roles(language)

    first_role = EXPECTED_WORKER_ORDER[0]
    flow.add_edge(START, first_role)
    flow.set_entry_point(first_role)

    _add_standard_workers(
        flow,
        worker_roles,
        provider,
        agent_model_overrides=agent_model_overrides,
        agent_reasoning_overrides=agent_reasoning_overrides,
    )

    guardrail_agent = TaskGuardrail.init(
        provider,
        model_name_override=(agent_model_overrides or {}).get("guardrail"),
        reasoning_effort=(agent_reasoning_overrides or {}).get("guardrail"),
    )
    flow.add_node("guardrail_co", TaskGuardrail.evaluate(guardrail_agent))
    flow.add_node("guardrail_ts", TaskGuardrail.evaluate(guardrail_agent))
    flow.add_node("guardrail_sr", TaskGuardrail.evaluate(guardrail_agent))
    flow.add_node("terminal_output", final_response_passthrough)

    flow.add_edge("content ordering", "guardrail_co")
    flow.add_edge("guardrail_co", "text structuring")

    flow.add_edge("text structuring", "guardrail_ts")
    flow.add_edge("guardrail_ts", "surface realization")

    flow.add_edge("surface realization", "guardrail_sr")
    flow.add_edge("guardrail_sr", "terminal_output")
    flow.add_edge("terminal_output", END)

    return flow.compile()


def build_agent_workflow_no_guardrail_no_finalizer(
    provider: str = "ollama",
    language: LanguageCode = "en",
    agent_model_overrides: Dict[str, str] | None = None,
    agent_reasoning_overrides: Dict[str, Optional[str]] | None = None,
):
    """
    Ablation study 3.
    Keep the orchestrator and specialized workers, but remove guardrail
    evaluation and finalizer polishing.

    Architecture:
      orchestrator -> worker -> accept-without-guardrail -> orchestrator
      ... -> terminal output -> END
    """
    flow = StateGraph(ExecutionState)
    worker_roles = get_worker_roles(language)
    workers = list(worker_roles.keys())

    flow.add_edge(START, "orchestrator")
    flow.set_entry_point("orchestrator")
    flow.add_node(
        "orchestrator",
        TaskOrchestrator.execute(
            TaskOrchestrator.init(
                provider,
                model_name_override=(agent_model_overrides or {}).get("orchestrator"),
                reasoning_effort=(agent_reasoning_overrides or {}).get("orchestrator"),
            )
        ),
    )

    _add_standard_workers(
        flow,
        worker_roles,
        provider,
        agent_model_overrides=agent_model_overrides,
        agent_reasoning_overrides=agent_reasoning_overrides,
        worker_success_next_agents={
            name: "accept_worker_output" for name in worker_roles
        },
    )
    flow.add_node("accept_worker_output", accept_worker_output_without_guardrail)
    flow.add_node("terminal_output", final_response_passthrough)

    routes = {name: name for name in workers}
    routes.update({"finish": "terminal_output"})
    flow.add_conditional_edges("orchestrator", lambda state: state["next_agent"], routes)

    for worker_name in workers:
        flow.add_edge(worker_name, "accept_worker_output")

    flow.add_edge("accept_worker_output", "orchestrator")
    flow.add_edge("terminal_output", END)

    return flow.compile()


# display(Image(process_flow.get_graph(xray=True).draw_mermaid_png()))

__author__='chinonsocynthiaosuji'

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    This module defines utility classes and types used in the agent's execution pipeline.
"""

import re
from enum import Enum
from uuid import uuid4
from typing import Annotated, Dict, List, Text, Any, Optional, Union, TypedDict, Tuple
from pydantic import BaseModel, Field


class AgentStepOutput(BaseModel):
    """Internally used to store result steps in the agent's response."""
    agent_name: Text
    agent_input: Union[Text, Dict[str, Any]]
    agent_output: Union[List, Dict, Text]
    rationale: Optional[Union[List, Dict, Text]] = None
    tool_steps: Optional[List[Any]] = None 

    
class ExecutionState(TypedDict, total=False):  # set total=False to make all keys optional
    """Holds evolving state across agent pipeline execution."""

    # Raw structured data to be verbalised, separated from the natural language user prompt
    data_input: Union[Text, List, Dict[str, Any]]

    # User facing instruction or high level request
    user_prompt: Union[Text, Dict[str, Any]]                    # User input to the agent system
    sample_metadata: Dict[str, Any]                             # Per-sample metadata carried through the workflow
    analysis_date: str                                          # Current sample analysis date
    horizon_months: str                                         # Remaining horizon used by report prompts/finalizer
    end_date: str                                               # Coverage window end date used by report prompts/finalizer

    # Orchestration bookkeeping
    final_response: str                                         # Final agent response
    next_agent: str                                             # Next agent in the sequence
    next_agent_payload: str                                     # Inputs to the agents in the sequence
    review: str                                                 # Guardrail feedback
    iteration_count: int                                        # Count of all steps done
    max_iteration: int                                          # Recursion limit set in langChain
    current_step: int                                           # Current step count
    history_of_steps: List[AgentStepOutput]                     # A list of all the agent interactions

    # New fields for limiting iterations and guardrail calls
    max_worker_attempts: Union[int, Dict[str, int]]  # global or per worker cap
    worker_attempts: Dict[str, int]                   # Per worker attempt counters
    last_worker: str                                  # Name of the last worker that ran
    passed_stages: List[str]                          # Stages explicitly approved by guardrail
    closed_stages: List[str]                          # Stages passed or exhausted and no longer reopenable
    response: str                                     # Optional routing/status signal
    token_usage: Dict[str, Any]                       # Accumulated totals plus optional per-agent breakdown

__author__='chinonsocynthiaosuji'

"""
Author: Chinonso Cynthia Osuji
Date: 10/07/2025
Description:
    Utility functions for agent workflows, including variable substitution and step summarization in a uniquely formatted template.
"""

import re, json
from typing import List, Text, Union, Dict
from agents.utilities.utils import AgentStepOutput
from langchain_core.exceptions import OutputParserException



def apply_variable_substitution(template: Text, substitutions: Union[Text, Dict[Text, Text]]) -> Text:
    """
    Substitute placeholders in the form {variable} within a template string.

    Args:
        template (Text): A string containing placeholders.
        substitutions (Union[Text, Dict[Text, Text]]): Substitution values.

    Returns:
        Text: The formatted string with placeholders replaced.
    """
    if isinstance(substitutions, str):
        # If a string, only support {user_prompt} placeholder
        return template.replace("{user_prompt}", substitutions)
    elif isinstance(substitutions, dict):
        # Replace all {var} with substitutions[var] or blank if missing
        def repl(match):
            var = match.group(1)
            return substitutions.get(var, "")
        pattern = re.compile(r"(?<!{){([^{}]+)}(?!})")
        return pattern.sub(repl, template)
    else:
        return template


def summarize_agent_steps(step_log: List[AgentStepOutput]) -> List[Text]:
    """
    Generate a uniquely formatted summary of agent execution steps,
    excluding guardrail steps and using UID-tagged blocks.

    Args:
        step_log (List[AgentStepOutput]): List of agent interaction records.

    Returns:
        List[Text]: A list of UID-formatted step summaries.
    """
    summary = []
    step_counter = 1

    for entry in step_log:
        agent = entry.agent_name.lower()

        if agent == "guardrail":
            continue

        if agent == "orchestrator":
            try:
                role, role_input = re.findall(r"(.*)\(input='(.*)'\)", entry.agent_output)[0]
            except Exception:
                role, role_input = "FINISH", entry.agent_output

            agent_type = "orchestrator"
            uid = f"{agent_type.upper()}_{step_counter}"
            if role == "FINISH":
                block = (
                    f"##=== BEGIN:{uid} ===##\n"
                    f"-- AGENT TYPE: {agent_type}\n"
                    f"-- AGENT NAME: {entry.agent_name}\n"
                    f"-- SIGNAL: FINISH\n"
                    f"-- RESPONSE START --\n{role_input}\n-- RESPONSE END --\n"
                    f"##=== END:{uid} ===##"
                )
            else:
                block = (
                    f"##=== BEGIN:{uid} ===##\n"
                    f"-- AGENT TYPE: {agent_type}\n"
                    f"-- AGENT NAME: {entry.agent_name}\n"
                    f"-- ROUTED TO: {role}\n"
                    f"-- INPUT START --\n{role_input}\n-- INPUT END --\n"
                    f"##=== END:{uid} ===##"
                )
        else:
            agent_type = entry.agent_name.lower()
            uid = f"{agent_type.upper()}_{step_counter}"

            if agent_type == "surface realization":
                block = (
                    f"##=== BEGIN:{uid} ===##\n"
                    f"-- AGENT TYPE: {agent_type}\n"
                    f"-- AGENT NAME: {entry.agent_name}\n"
                    f"-- INPUT START --\n{entry.agent_input}\n-- INPUT END --\n"
                    f"-- OUTPUT START --\n{entry.agent_output}\n-- OUTPUT END --\n"
                    "Finalizer Agent: Carefully review the output provided above by the surface realization agent. "
                    "Edit and refine the text as a human would, ensuring maximum fluency, semantic adequacy, coherence, and naturalness. "
                    "Your task is to produce the best possible final text, correcting any errors or awkwardness if present.\n"
                    f"##=== END:{uid} ===##"
                )
            else:
                block = (
                    f"##=== BEGIN:{uid} ===##\n"
                    f"-- AGENT TYPE: {agent_type}\n"
                    f"-- AGENT NAME: {entry.agent_name}\n"
                    f"-- INPUT START --\n{entry.agent_input}\n-- INPUT END --\n"
                    f"-- OUTPUT START --\n{entry.agent_output}\n-- OUTPUT END --\n"
                    f"##=== END:{uid} ===##"
                )

        summary.append(block)
        step_counter += 1

    return summary


# custom parsing error handler
def _handle_parsing_errors(e: OutputParserException) -> str:
            """
            Make the JSON agent tolerant of extra text around the JSON.
            """
            raw = getattr(e, "llm_output", "") or ""
            if not raw.strip():
                return json.dumps(
                    {"action": "Final Answer", "action_input": "PARSING_ERROR"}
                )

            # Cleanup raw output
            cleaned = raw.replace("```json", "```").replace("```", "")
            cleaned = re.sub(r"^\s*Action\s*:\s*", "", cleaned, flags=re.IGNORECASE)

            # Extract content starting from the first JSON bracket
            idxs = [i for i in [cleaned.find("{"), cleaned.find("[")] if i != -1]
            if idxs:
                cleaned = cleaned[min(idxs) :]

            # Drop comment lines to prevent parse errors
            cleaned_no_comments = "\n".join(
                line for line in cleaned.splitlines()
                if not line.lstrip().startswith("//")
            ).strip()

            try:
                parsed_inner = json.loads(cleaned_no_comments)
            except Exception:
                # Fallback. treat whole output as the final answer string
                return json.dumps(
                    {
                        "action": "Final Answer",
                        "action_input": raw.strip(),
                    }
                )

            # If inner JSON is already an action object, use it
            if isinstance(parsed_inner, dict) and "action" in parsed_inner and "action_input" in parsed_inner:
                return json.dumps(parsed_inner)

            # Otherwise wrap the content
            return json.dumps(
                {
                    "action": "Final Answer",
                    "action_input": parsed_inner,
                }
            )

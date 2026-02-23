from agents import Agent, Tool
from agents.mcp import MCPServerStreamableHttp
from agents.model_settings import ModelSettings
from typing import Any


def get_agent(
    name: str,
    instructions: str,
    tools: list[Tool],
    servers: list[MCPServerStreamableHttp],
    model: str = "gpt-4.1-mini",
    model_settings: ModelSettings = ModelSettings(tool_choice="required"),
    output_type: Any | None = None,
) -> Agent:
    agent = Agent(
        name=name,
        model=model,
        instructions=instructions,
        tools=tools,
        mcp_servers=servers,
        model_settings=model_settings,
        output_type=output_type,
    )
    return agent

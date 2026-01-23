finAgents/
  __init__.py

  server_us_finance.py
  run_multi_agent.py

  financial_agents/
    __init__.py
    financial_analyst.py
    financial_manager.py

  agent_tools/

What each file does:
`server_us_finance`.py: MCP server. Exposes tools to query your SQLite artefacts: prices, returns, companyfacts, panel.
`financial_agents/financial_analyst.py`: Analyst agent builder. Instructions and output schema for indicators.
`financial_agents/financial_manager.py`: Manager agent builder. Instructions and output schema for buy or sell decision.
`run_multi_agent.py`: Starts the MCP server over stdio, runs analyst first, then manager, prints outputs.

<!-- mkdir -p agents/financial_agents
touch agents/__init__.py agents/financial_agents/__init__.py -->
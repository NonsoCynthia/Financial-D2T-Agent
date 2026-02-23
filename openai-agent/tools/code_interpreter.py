from agents import function_tool
from typing_extensions import TypedDict


class CodeInterpreterInput(TypedDict):
    code: str


@function_tool
def code_interpreter(inp: CodeInterpreterInput) -> dict:
    """
    Execute Python code and return stdout/stderr.

    The code passed to this function is executed in isolation.
    The result MUST be printed at the end.
    """
    import subprocess
    import sys

    try:
        result = subprocess.run([sys.executable, "-c", inp.get("code")], capture_output=True)
        report = f"StdOut:\n{result.stdout}\nStdErr:\n{result.stderr}"
        return {"status": "success", "report": report}
    except Exception as e:
        return {"status": "error", "report": f"Failed to run code: {e}"}

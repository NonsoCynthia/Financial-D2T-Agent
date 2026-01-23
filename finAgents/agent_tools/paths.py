from pathlib import Path
from typing import Dict


def repo_root() -> Path:
    """
    Resolve the repository root folder.

    This assumes this file lives in agents/agent_tools/.
    """
    return Path(__file__).resolve().parents[2]


def data_paths() -> Dict[str, Path]:
    """
    Return the paths to the SQLite artefacts produced by your pipeline.
    """
    root = repo_root()
    return {
        "prices_db": root / "data" / "raw" / "prices_us.db",
        "facts_db": root / "data" / "raw" / "sec" / "sec_companyfacts.db",
        "panel_db": root / "data" / "processed" / "panel" / "panel.db",
    }

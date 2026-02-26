#!/usr/bin/env python3
"""
Compatibility wrapper.

The ROIC benchmark builder was moved to:
  scripts/09_build_roic_gold_benchmark.py

This file remains so older commands keep working.
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    target = project_root / "scripts" / "09_build_roic_gold_benchmark.py"
    runpy.run_path(str(target), run_name="__main__")

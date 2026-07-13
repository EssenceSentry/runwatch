#!/usr/bin/env python3
"""Run the repository-local agent skill installer from a conventional path."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    """Delegate to the installer stored beside the repository skills."""
    installer = (
        Path(__file__).resolve().parents[1] / "agent_skills" / "install_skills.py"
    )
    sys.argv[0] = str(installer)
    runpy.run_path(str(installer), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

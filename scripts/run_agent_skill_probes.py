"""Run the repository-local Runwatch agent-skill probe."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Probe:
    name: str
    script: Path
    pythonpath: tuple[Path, ...]


PROBES = (
    Probe(
        name="runwatch",
        script=Path("agent_skills/runwatch/scripts/runwatch_skill_probe.py"),
        pythonpath=(Path("src"),),
    ),
)


def _pythonpath(paths: tuple[Path, ...]) -> str:
    return os.pathsep.join(str(ROOT / path) for path in paths)


def run_probe(probe: Probe) -> int:
    print(f"\n=== agent skill probe: {probe.name} ===", flush=True)
    env = os.environ.copy()
    env["PYTHONPATH"] = _pythonpath(probe.pythonpath)
    completed = subprocess.run(
        [sys.executable, str(ROOT / probe.script)],
        cwd=ROOT,
        env=env,
        check=False,
    )
    return int(completed.returncode)


def main() -> int:
    failures: list[str] = []
    for probe in PROBES:
        if run_probe(probe) != 0:
            failures.append(probe.name)

    if failures:
        print("\nFailed agent skill probes: " + ", ".join(failures), file=sys.stderr)
        return 1

    print("\nAll agent skill probes passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

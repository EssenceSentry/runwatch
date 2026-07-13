#!/usr/bin/env python3
"""Check that GitHub quality-gate lint targets match local pre-commit targets."""

from __future__ import annotations

import argparse
import shlex
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
PRE_COMMIT_CONFIG = ROOT / ".pre-commit-config.yaml"
QUALITY_GATE_WORKFLOW = ROOT / ".github" / "workflows" / "quality-gate.yml"

PRE_COMMIT_HOOKS = {
    "ruff": ("runwatch-ruff",),
    "black": ("runwatch-black",),
}
QUALITY_GATE_STEPS = {
    "ruff": ("ruff", "Ruff"),
    "black": ("black", "Black"),
}


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected mapping in {path.relative_to(ROOT)}")
    return payload


def _local_hook_entries() -> dict[str, str]:
    config = _load_yaml(PRE_COMMIT_CONFIG)
    hooks: dict[str, str] = {}
    for repo in config.get("repos", []):
        if not isinstance(repo, dict):
            continue
        for hook in repo.get("hooks", []):
            if not isinstance(hook, dict):
                continue
            hook_id = hook.get("id")
            entry = hook.get("entry")
            if isinstance(hook_id, str) and isinstance(entry, str):
                hooks[hook_id] = entry
    return hooks


def _pre_commit_targets(tool: str) -> set[str]:
    hooks = _local_hook_entries()
    targets: set[str] = set()
    for hook_id in PRE_COMMIT_HOOKS[tool]:
        entry = hooks.get(hook_id)
        if entry is None:
            raise ValueError(f"Missing pre-commit hook {hook_id!r}")
        targets.update(_targets_from_command(entry, tool))
    return targets


def _quality_gate_targets(tool: str) -> set[str]:
    workflow = _load_yaml(QUALITY_GATE_WORKFLOW)
    jobs = workflow.get("jobs")
    if not isinstance(jobs, dict):
        raise ValueError("quality-gate workflow must define a jobs mapping")
    job_id, step_name = QUALITY_GATE_STEPS[tool]
    job = jobs.get(job_id)
    if not isinstance(job, dict):
        raise ValueError(f"quality-gate workflow is missing job {job_id!r}")
    steps = job.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"quality-gate job {job_id!r} must define steps")
    for step in steps:
        if not isinstance(step, dict) or step.get("name") != step_name:
            continue
        run = step.get("run")
        if not isinstance(run, str):
            raise ValueError(f"quality-gate step {step_name!r} must define run")
        return set(_targets_from_command(run, tool))
    raise ValueError(f"quality-gate job {job_id!r} is missing step {step_name!r}")


def _targets_from_command(command: str, tool: str) -> list[str]:
    tokens = shlex.split(command)
    marker = "check" if tool == "ruff" else "--check"
    try:
        marker_index = tokens.index(marker)
    except ValueError as exc:
        raise ValueError(f"{tool} command is missing {marker!r}: {command}") from exc
    return tokens[marker_index + 1 :]


def _parity_errors(tool: str) -> list[str]:
    local = _pre_commit_targets(tool)
    remote = _quality_gate_targets(tool)
    errors: list[str] = []
    missing_in_ci = sorted(local - remote)
    missing_locally = sorted(remote - local)
    if missing_in_ci:
        errors.append(
            f"{tool}: targets present in pre-commit but missing from quality-gate: "
            + ", ".join(missing_in_ci)
        )
    if missing_locally:
        errors.append(
            f"{tool}: targets present in quality-gate but missing from pre-commit: "
            + ", ".join(missing_locally)
        )
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tool",
        choices=sorted(PRE_COMMIT_HOOKS),
        action="append",
        help="Tool target list to check. Defaults to all supported tools.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tools = args.tool or sorted(PRE_COMMIT_HOOKS)
    errors = [error for tool in tools for error in _parity_errors(tool)]
    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"Quality-gate parity checks passed for {len(tools)} tool(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

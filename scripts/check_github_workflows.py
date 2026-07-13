#!/usr/bin/env python3
"""Validate basic GitHub Actions workflow structure."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError

ROOT = Path(__file__).resolve().parents[1]
EXPRESSION_OPEN = re.compile(r"\$\{\{")
EXPRESSION_CLOSE = re.compile(r"\}\}")


class WorkflowLoader(yaml.BaseLoader):
    """YAML loader that keeps Actions keys as strings and rejects duplicates."""


def _construct_mapping(
    loader: WorkflowLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


WorkflowLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _workflow_files() -> list[str]:
    workflows = ROOT / ".github" / "workflows"
    return sorted(
        path.relative_to(ROOT).as_posix()
        for pattern in ("*.yml", "*.yaml")
        for path in workflows.glob(pattern)
    )


def _as_mapping(value: object) -> dict[str, object] | None:
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return value
    return None


def _as_list(value: object) -> list[object] | None:
    if isinstance(value, list):
        return value
    return None


def _needs_values(value: object) -> tuple[list[str], bool]:
    if value is None:
        return [], True
    if isinstance(value, str):
        return [value], True
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value), True
    return [], False


def _check_expression_balance(path: Path, errors: list[str]) -> None:
    text = path.read_text(encoding="utf-8")
    opens = len(EXPRESSION_OPEN.findall(text))
    closes = len(EXPRESSION_CLOSE.findall(text))
    if opens != closes:
        errors.append(
            f"{path.relative_to(ROOT).as_posix()}: has unbalanced GitHub expression markers"
        )


def _check_job_steps(
    *,
    relpath: str,
    job_id: str,
    job: dict[str, object],
    errors: list[str],
) -> None:
    if "uses" in job:
        return
    if "runs-on" not in job:
        errors.append(f"{relpath}: job {job_id!r} must define 'runs-on' or 'uses'")
    steps = _as_list(job.get("steps"))
    if not steps:
        errors.append(f"{relpath}: job {job_id!r} must define a non-empty steps list")
        return
    for index, raw_step in enumerate(steps, start=1):
        step = _as_mapping(raw_step)
        if step is None:
            errors.append(f"{relpath}: job {job_id!r} step {index} must be a mapping")
            continue
        if "run" not in step and "uses" not in step:
            errors.append(
                f"{relpath}: job {job_id!r} step {index} must define 'run' or 'uses'"
            )


def check_workflow(path: Path) -> list[str]:
    relpath = path.relative_to(ROOT).as_posix()
    errors: list[str] = []
    try:
        workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=WorkflowLoader)
    except Exception as exc:
        return [f"{relpath}: {exc}"]

    top = _as_mapping(workflow)
    if top is None:
        return [f"{relpath}: workflow must be a mapping"]
    if "on" not in top:
        errors.append(f"{relpath}: workflow must define 'on'")
    jobs = _as_mapping(top.get("jobs"))
    if jobs is None:
        errors.append(f"{relpath}: workflow must define a jobs mapping")
        return errors
    if not jobs:
        errors.append(f"{relpath}: workflow must define at least one job")
        return errors

    job_ids = set(jobs)
    for job_id, raw_job in jobs.items():
        job = _as_mapping(raw_job)
        if job is None:
            errors.append(f"{relpath}: job {job_id!r} must be a mapping")
            continue
        needs, valid_needs = _needs_values(job.get("needs"))
        if not valid_needs:
            errors.append(
                f"{relpath}: job {job_id!r} needs must be a string or list of strings"
            )
        for needed in needs:
            if needed not in job_ids:
                errors.append(f"{relpath}: job {job_id!r} needs unknown job {needed!r}")
        _check_job_steps(relpath=relpath, job_id=job_id, job=job, errors=errors)

    _check_expression_balance(path, errors)
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Workflow files to check. Defaults to .github/workflows/*.yml.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = args.paths or _workflow_files()
    errors: list[str] = []
    for raw_path in paths:
        path = ROOT / raw_path
        if path.exists():
            errors.extend(check_workflow(path))

    if errors:
        for error in errors:
            print(error)
        return 1

    print(f"GitHub workflow checks passed for {len(paths)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

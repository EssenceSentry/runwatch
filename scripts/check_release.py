#!/usr/bin/env python3
"""Validate Runwatch release metadata and its Git tag."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - exercised by the Python 3.10 CI environment
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = ROOT / "pyproject.toml"
EXPECTED_PROJECT_NAME = "runwatch-notebook"


def project_metadata(path: Path = PYPROJECT_PATH) -> tuple[str, str]:
    """Return the validated distribution name and version from ``pyproject.toml``."""

    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    project_value: Any = raw.get("project")
    if not isinstance(project_value, dict):
        raise ValueError("pyproject.toml must define a project table")
    project = project_value
    name = project.get("name")
    version = project.get("version")
    if name != EXPECTED_PROJECT_NAME:
        raise ValueError(
            f"Expected project name {EXPECTED_PROJECT_NAME!r}, found {name!r}"
        )
    if not isinstance(version, str) or not version.strip():
        raise ValueError("project.version must be a non-empty string")
    if version != version.strip():
        raise ValueError("project.version must not contain surrounding whitespace")
    return name, version


def validate_release_tag(tag: str, version: str) -> None:
    """Require a release tag in the exact ``v<project.version>`` form."""

    expected = f"v{version}"
    if tag != expected:
        raise ValueError(f"Release tag must be {expected!r}, found {tag!r}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag",
        help="GitHub release tag to compare with project.version",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        name, version = project_metadata()
        if args.tag is not None:
            validate_release_tag(args.tag, version)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"Release metadata check failed: {error}", file=sys.stderr)
        return 1
    suffix = f" and tag {args.tag}" if args.tag is not None else ""
    print(f"Release metadata is valid for {name} {version}{suffix}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

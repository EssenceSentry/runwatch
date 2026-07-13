#!/usr/bin/env python3
"""Validate the repository-local Runwatch skill and its UI metadata."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = ROOT / "agent_skills" / "runwatch"
SKILL_PATH = SKILL_ROOT / "SKILL.md"
OPENAI_PATH = SKILL_ROOT / "agents" / "openai.yaml"
FRONTMATTER = re.compile(r"\A---\n(?P<yaml>.*?)\n---\n", re.DOTALL)
LINK = re.compile(r"\[[^]]+\]\((?P<target>[^)]+)\)")


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a mapping with string keys")
    return value


def validate_skill() -> list[str]:
    errors: list[str] = []
    text = SKILL_PATH.read_text(encoding="utf-8")
    match = FRONTMATTER.match(text)
    if match is None:
        return ["SKILL.md must begin with YAML frontmatter"]

    metadata = _mapping(yaml.safe_load(match.group("yaml")), label="frontmatter")
    if set(metadata) != {"name", "description"}:
        errors.append("SKILL.md frontmatter must contain only name and description")
    if metadata.get("name") != SKILL_ROOT.name:
        errors.append("skill name must match its directory name")
    description = metadata.get("description")
    if not isinstance(description, str) or not description.strip():
        errors.append("skill description must be a non-empty string")

    for link in LINK.finditer(text):
        target = link.group("target")
        if "://" in target or target.startswith("#"):
            continue
        if not (SKILL_ROOT / target).is_file():
            errors.append(f"SKILL.md link does not exist: {target}")

    config = _mapping(yaml.safe_load(OPENAI_PATH.read_text()), label="openai.yaml")
    interface = _mapping(config.get("interface"), label="openai.yaml interface")
    required = {"display_name", "short_description", "default_prompt"}
    missing = sorted(required - set(interface))
    if missing:
        errors.append("openai.yaml interface is missing: " + ", ".join(missing))
    short_description = interface.get("short_description")
    if not isinstance(short_description, str) or not 25 <= len(short_description) <= 64:
        errors.append("short_description must be between 25 and 64 characters")
    default_prompt = interface.get("default_prompt")
    if not isinstance(default_prompt, str) or "$runwatch" not in default_prompt:
        errors.append("default_prompt must mention $runwatch")

    combined = text + OPENAI_PATH.read_text(encoding="utf-8")
    if "auto_classifier" in combined:
        errors.append("skill must use the standalone runwatch namespace")
    return errors


def main() -> int:
    try:
        errors = validate_skill()
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"skill validation failed: {exc}")
        return 1
    if errors:
        for error in errors:
            print(error)
        return 1
    print("Runwatch skill validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

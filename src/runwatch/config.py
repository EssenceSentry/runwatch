from __future__ import annotations

import os
import re
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import yaml

from .models import ResourceEvent, RunwatchConfig
from .resources import validate_resource_event

_ENVIRONMENT_REFERENCE = re.compile(
    r"\$\$|\$(?:\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}|"
    r"(?P<plain>[A-Za-z_][A-Za-z0-9_]*))"
)


def _expand_environment_variables(text: str) -> str:
    referenced = {
        match.group("braced") or match.group("plain")
        for match in _ENVIRONMENT_REFERENCE.finditer(text)
        if match.group("braced") or match.group("plain")
    }
    _require_environment_variables(referenced)

    def substitute(match: re.Match[str]) -> str:
        name = match.group("braced") or match.group("plain")
        return "$" if name is None else os.environ[name]

    return _ENVIRONMENT_REFERENCE.sub(substitute, text)


def _require_environment_variables(referenced: set[str]) -> None:
    missing = sorted(name for name in referenced if name not in os.environ)
    if missing:
        names = ", ".join(missing)
        raise ValueError(
            f"Runwatch config references unset environment variable(s): {names}"
        )


def _config_environment_references(value: object) -> set[str]:
    if isinstance(value, str):
        return {
            match.group("braced") or match.group("plain")
            for match in _ENVIRONMENT_REFERENCE.finditer(value)
            if match.group("braced") or match.group("plain")
        }
    if isinstance(value, list):
        references: set[str] = set()
        for item in cast(list[object], value):
            references.update(_config_environment_references(item))
        return references
    if isinstance(value, dict):
        references = set()
        for item in cast(dict[object, object], value).values():
            references.update(_config_environment_references(item))
        return references
    return set()


def _expand_config_value(value: object) -> object:
    if isinstance(value, str):
        return _expand_environment_variables(value)
    if isinstance(value, list):
        return [_expand_config_value(item) for item in cast(list[object], value)]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {key: _expand_config_value(item) for key, item in mapping.items()}
    return value


def validate_config_resources(config: RunwatchConfig) -> None:
    for registration in config.resources:
        validate_resource_event(
            ResourceEvent(
                resource=registration.resource,
                lifecycle=registration.lifecycle,
            )
        )


def load_config(path: Path | None) -> RunwatchConfig:
    if path is None:
        return RunwatchConfig()
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Runwatch config must be a mapping, got {type(raw).__name__}")
    mapping = cast(dict[object, object], raw)
    _require_environment_variables(_config_environment_references(mapping))
    raw = _expand_config_value(mapping)
    config = RunwatchConfig.model_validate(raw)
    validate_config_resources(config)
    return config


def dump_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    template = (
        files("runwatch").joinpath("default_config.yaml").read_text(encoding="utf-8")
    )
    # Keep the shipped template honest when defaults or field names change.
    parsed = RunwatchConfig.model_validate(yaml.safe_load(template))
    validate_config_resources(parsed)
    path.write_text(template, encoding="utf-8")

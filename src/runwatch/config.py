from __future__ import annotations

import os
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml

from .models import ResourceEvent, RunwatchConfig
from .resources import validate_resource_event


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
    text = os.path.expandvars(path.read_text(encoding="utf-8"))
    raw: Any = yaml.safe_load(text) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Runwatch config must be a mapping, got {type(raw).__name__}")
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

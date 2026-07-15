"""Strict parsing and serialization models for per-run manifests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal, TypeAlias, cast

from pydantic import BaseModel, ConfigDict, Field, StrictBool, ValidationError

from .models import RunwatchConfig
from .notification_config import compatible_runwatch_config
from .schema_versions import RUN_MANIFEST_SCHEMA_VERSION


class InvalidRunManifest(RuntimeError):
    """A run manifest that cannot be interpreted without unsafe coercion."""


class _ManifestBase(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: Annotated[str, Field(min_length=1)]
    name: Annotated[str, Field(min_length=1)]
    notebook_path: Annotated[str, Field(min_length=1)]
    source_path: Annotated[str, Field(min_length=1)]
    output_path: Annotated[str, Field(min_length=1)]
    working_dir: Annotated[str, Field(min_length=1)]
    config: RunwatchConfig


class _RunManifestV2(_ManifestBase):
    schema_version: Literal[2]
    cleanup_on_success: StrictBool = False


class _RunManifestV3(_ManifestBase):
    schema_version: Literal[3] = RUN_MANIFEST_SCHEMA_VERSION
    cleanup_on_success: StrictBool


RunManifest: TypeAlias = _RunManifestV2 | _RunManifestV3


def read_run_manifest(path: Path) -> RunManifest:
    """Read a supported run manifest without coercing destructive policy fields.

    Parameters
    ----------
    path:
        Path to ``run-manifest.json``.

    Returns
    -------
    RunManifest
        A strictly validated legacy-v2 or current-v3 manifest.

    Raises
    ------
    InvalidRunManifest
        If JSON, version dispatch, or field validation fails.
    """

    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InvalidRunManifest(
            f"Invalid Runwatch manifest {path}: {error}"
        ) from error
    if not isinstance(decoded, dict):
        raise InvalidRunManifest("Runwatch manifest must be a JSON object")
    value = cast(dict[str, object], decoded)
    version = value.get("schema_version")
    model: type[_RunManifestV2] | type[_RunManifestV3]
    if type(version) is not int:
        raise InvalidRunManifest(
            f"Unsupported Runwatch manifest schema {version!r}; expected 2 or "
            f"{RUN_MANIFEST_SCHEMA_VERSION}"
        )
    if version == 2:
        model = _RunManifestV2
    elif version == RUN_MANIFEST_SCHEMA_VERSION:
        model = _RunManifestV3
    else:
        raise InvalidRunManifest(
            f"Unsupported Runwatch manifest schema {version!r}; expected 2 or "
            f"{RUN_MANIFEST_SCHEMA_VERSION}"
        )
    compatible = dict(value)
    if version == 2:
        compatible["config"] = compatible_runwatch_config(value.get("config"))
    try:
        return model.model_validate(compatible)
    except ValidationError as error:
        raise InvalidRunManifest(
            f"Invalid Runwatch manifest {path}: {error}"
        ) from error

from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass
from typing import Any, Literal, cast

from ..models import ResourceObservation, ResourceStatus
from ..schema_versions import S3_PROGRESS_MANIFEST_SCHEMA_VERSION
from .base import AwsResourceAdapter, ResourceOperationError
from .s3 import parse_s3_uri

_MAX_MANIFEST_BYTES = 1_048_576
_RESERVED_METRICS = {"bucket", "key", "exists", "completed", "total"}

_ManifestStatus = Literal["running", "completed", "failed"]


@dataclass(frozen=True)
class _Manifest:
    status: _ManifestStatus
    completed: int | float
    total: int | float | None
    message: str | None
    metrics: dict[str, Any]


def _manifest_number(value: Any, *, positive: bool, field: str) -> int | float:
    valid_type = isinstance(value, (int, float)) and not isinstance(value, bool)
    if not valid_type or not math.isfinite(float(value)):
        qualifier = "positive" if positive else "nonnegative"
        raise ResourceOperationError(f"Manifest {field} must be a {qualifier} number")
    if (positive and value <= 0) or (not positive and value < 0):
        qualifier = "positive" if positive else "nonnegative"
        raise ResourceOperationError(f"Manifest {field} must be a {qualifier} number")
    return cast(int | float, value)


def _valid_scalar(value: Any) -> bool:
    if value is None or isinstance(value, (str, bool)):
        return True
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


async def _get_manifest_object(
    client: Any, *, bucket: str, key: str
) -> dict[str, Any] | None:
    try:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(client.get_object, Bucket=bucket, Key=key),
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise


async def _read_manifest_bytes(response: dict[str, Any]) -> bytes:
    body = response["Body"]
    try:
        content_length = response.get("ContentLength")
        if content_length is not None and int(content_length) > _MAX_MANIFEST_BYTES:
            raise ResourceOperationError("Runwatch manifest exceeds the 1 MiB limit")
        raw = cast(
            bytes,
            await asyncio.to_thread(body.read, _MAX_MANIFEST_BYTES + 1),
        )
        if len(raw) > _MAX_MANIFEST_BYTES:
            raise ResourceOperationError("Runwatch manifest exceeds the 1 MiB limit")
        return raw
    finally:
        await asyncio.to_thread(body.close)


def _decode_manifest(raw: bytes, location: str) -> dict[str, Any]:
    try:
        decoded = json.loads(raw)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResourceOperationError(f"Invalid JSON manifest at {location}") from error
    if not isinstance(decoded, dict):
        raise ResourceOperationError(
            "Runwatch manifests require schema_version="
            f"{S3_PROGRESS_MANIFEST_SCHEMA_VERSION}"
        )
    return cast(dict[str, Any], decoded)


def _manifest_status(payload: dict[str, Any]) -> _ManifestStatus:
    schema_version = payload.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != S3_PROGRESS_MANIFEST_SCHEMA_VERSION
    ):
        raise ResourceOperationError(
            "Runwatch manifests require schema_version="
            f"{S3_PROGRESS_MANIFEST_SCHEMA_VERSION}"
        )
    status = payload.get("status")
    if status not in {"running", "completed", "failed"}:
        raise ResourceOperationError(
            "Manifest status must be running, completed, or failed"
        )
    return cast(_ManifestStatus, status)


def _manifest_progress(
    payload: dict[str, Any],
) -> tuple[int | float, int | float | None]:
    completed = _manifest_number(
        payload.get("completed"), positive=False, field="completed"
    )
    total_value = payload.get("total")
    if total_value is None:
        return completed, None
    total = _manifest_number(total_value, positive=True, field="total")
    if completed > total:
        raise ResourceOperationError("Manifest completed must not exceed total")
    return completed, total


def _manifest_message(payload: dict[str, Any]) -> str | None:
    message = payload.get("message")
    if message is not None and not isinstance(message, str):
        raise ResourceOperationError("Manifest message must be a string")
    return message


def _manifest_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    metrics_value = payload.get("metrics", {})
    if not isinstance(metrics_value, dict):
        raise ResourceOperationError("Manifest metrics must contain scalar values")
    metrics = cast(dict[str, Any], metrics_value)
    if any(not _valid_scalar(value) for value in metrics.values()):
        raise ResourceOperationError("Manifest metrics must contain scalar values")
    collisions = _RESERVED_METRICS.intersection(metrics)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ResourceOperationError(
            f"Manifest metrics cannot override reserved fields: {names}"
        )
    return metrics


def _validated_manifest(payload: dict[str, Any]) -> _Manifest:
    status = _manifest_status(payload)
    completed, total = _manifest_progress(payload)
    return _Manifest(
        status=status,
        completed=completed,
        total=total,
        message=_manifest_message(payload),
        metrics=_manifest_metrics(payload),
    )


def _manifest_observation(
    manifest: _Manifest,
    response: dict[str, Any],
    *,
    bucket: str,
    key: str,
) -> ResourceObservation:
    mapped = {
        "running": ResourceStatus.RUNNING,
        "completed": ResourceStatus.COMPLETED,
        "failed": ResourceStatus.FAILED,
    }[manifest.status]
    return ResourceObservation(
        status=mapped,
        terminal=manifest.status in {"completed", "failed"},
        message=manifest.message,
        metrics={
            "bucket": bucket,
            "key": key,
            "exists": True,
            "completed": manifest.completed,
            "total": manifest.total,
            **manifest.metrics,
        },
        raw={"etag": response.get("ETag"), "status": manifest.status},
    )


class S3ManifestAdapter(AwsResourceAdapter):
    provider = "aws"
    resource_type = "s3_manifest"
    supports_blocking = True

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        bucket, key = parse_s3_uri(resource["external_id"])
        client = self.aws.client("s3", resource.get("region"))
        response = await _get_manifest_object(client, bucket=bucket, key=key)
        if response is None:
            return ResourceObservation(
                status=ResourceStatus.PENDING,
                message="Manifest does not exist yet",
                metrics={"bucket": bucket, "key": key, "exists": False},
            )
        raw = await _read_manifest_bytes(response)
        manifest = _validated_manifest(_decode_manifest(raw, resource["external_id"]))
        return _manifest_observation(manifest, response, bucket=bucket, key=key)

from __future__ import annotations

import math
from typing import Any, Literal

from .emit import emit_resource
from .models import Ownership, ResourceEvent, ResourceLifecycle, ResourceSpec
from .resources.cloudwatch import validate_cloudwatch_metric_configuration
from .resources.cloudwatch_logs import validate_cloudwatch_logs_configuration
from .resources.s3 import parse_s3_uri

_S3_PREFIX_RESERVED_METADATA = {
    "expected_count",
    "completion_marker",
    "max_pages",
    "full_rescan_seconds",
}
_SAGEMAKER_RESERVED_METADATA = {"output_prefixes", "log_group"}


def _validate_s3_uri(uri: str) -> None:
    try:
        parse_s3_uri(uri)
    except RuntimeError as error:
        raise ValueError(str(error)) from error


def emit_sagemaker_processing_job(
    job_name: str,
    *,
    region: str | None = None,
    account_id: str | None = None,
    logical_key: str | None = None,
    output_prefixes: list[str] | None = None,
    log_group: str = "/aws/sagemaker/ProcessingJobs",
    blocking: bool = True,
    stop_on_cancel: bool = True,
    poll_interval_seconds: float | None = None,
    final_log_drain_seconds: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    collisions = _SAGEMAKER_RESERVED_METADATA.intersection(metadata)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"metadata cannot override SageMaker fields: {names}")
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="aws",
            type="sagemaker_processing_job",
            id=job_name,
            logical_key=logical_key or job_name,
            region=region,
            account_id=account_id,
            ownership=Ownership.EXCLUSIVE,
            metadata={
                "output_prefixes": output_prefixes or [],
                "log_group": log_group,
                **metadata,
            },
        ),
        lifecycle=ResourceLifecycle(
            blocking=blocking,
            stop_on_cancel=stop_on_cancel,
            retain_logs=True,
            poll_interval_seconds=poll_interval_seconds,
            final_log_drain_seconds=final_log_drain_seconds,
        ),
    )
    return emit_resource(event, text=f"SageMaker Processing job: {job_name}")


def emit_s3_prefix(
    uri: str,
    *,
    region: str | None = None,
    logical_key: str | None = None,
    expected_count: int | None = None,
    completion_marker: str | None = None,
    blocking: bool = False,
    poll_interval_seconds: float | None = None,
    max_pages: int = 100,
    full_rescan_seconds: float = 300.0,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_s3_uri(uri)
    raw_expected_count: Any = expected_count
    raw_max_pages: Any = max_pages
    raw_rescan_seconds: Any = full_rescan_seconds
    if raw_expected_count is not None and (
        isinstance(raw_expected_count, bool)
        or not isinstance(raw_expected_count, int)
        or raw_expected_count < 0
    ):
        raise ValueError("expected_count must be nonnegative")
    if blocking and expected_count is None and not completion_marker:
        raise ValueError(
            "blocking S3 prefixes require expected_count or completion_marker"
        )
    if (
        isinstance(raw_max_pages, bool)
        or not isinstance(raw_max_pages, int)
        or raw_max_pages <= 0
    ):
        raise ValueError("max_pages must be positive")
    if (
        isinstance(raw_rescan_seconds, bool)
        or not isinstance(raw_rescan_seconds, (int, float))
        or not math.isfinite(float(raw_rescan_seconds))
        or raw_rescan_seconds < 0
    ):
        raise ValueError("full_rescan_seconds must be nonnegative")
    metadata = metadata or {}
    collisions = _S3_PREFIX_RESERVED_METADATA.intersection(metadata)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"metadata cannot override S3 prefix fields: {names}")
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="aws",
            type="s3_prefix",
            id=uri,
            logical_key=logical_key or uri,
            region=region,
            ownership=Ownership.BORROWED,
            metadata={
                "expected_count": expected_count,
                "completion_marker": completion_marker,
                "max_pages": max_pages,
                "full_rescan_seconds": full_rescan_seconds,
                **metadata,
            },
        ),
        lifecycle=ResourceLifecycle(
            blocking=blocking,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    return emit_resource(event, text=f"S3 prefix: {uri}")


def emit_s3_manifest(
    uri: str,
    *,
    region: str | None = None,
    logical_key: str | None = None,
    blocking: bool = True,
    poll_interval_seconds: float | None = None,
) -> dict[str, Any]:
    _validate_s3_uri(uri)
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="aws",
            type="s3_manifest",
            id=uri,
            logical_key=logical_key or uri,
            region=region,
            ownership=Ownership.BORROWED,
        ),
        lifecycle=ResourceLifecycle(
            blocking=blocking,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    return emit_resource(event, text=f"S3 manifest: {uri}")


def emit_cloudwatch_metric(
    *,
    namespace: str,
    metric_name: str,
    dimensions: dict[str, str] | None = None,
    region: str | None = None,
    logical_key: str | None = None,
    statistic: Literal[
        "Average", "Sum", "Minimum", "Maximum", "SampleCount"
    ] = "Average",
    period_seconds: int = 60,
    lookback_seconds: int = 900,
    poll_interval_seconds: float | None = 60.0,
) -> dict[str, Any]:
    identity = f"{namespace}/{metric_name}"
    metric_metadata = {
        "namespace": namespace,
        "metric_name": metric_name,
        "dimensions": dimensions or {},
        "statistic": statistic,
        "period_seconds": period_seconds,
        "lookback_seconds": lookback_seconds,
    }
    try:
        validate_cloudwatch_metric_configuration(metric_metadata)
    except RuntimeError as error:
        raise ValueError(str(error)) from error
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="aws",
            type="cloudwatch_metric",
            id=identity,
            logical_key=logical_key or identity,
            region=region,
            ownership=Ownership.BORROWED,
            metadata=metric_metadata,
        ),
        lifecycle=ResourceLifecycle(
            blocking=False,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    return emit_resource(event, text=f"CloudWatch metric: {identity}")


def emit_cloudwatch_logs(
    *,
    log_group: str,
    stream_prefix: str = "",
    region: str | None = None,
    logical_key: str | None = None,
    poll_interval_seconds: float | None = 5.0,
    max_streams: int | None = None,
) -> dict[str, Any]:
    identity = f"{log_group}:{stream_prefix}"
    logs_metadata = {
        "log_group": log_group,
        "stream_prefix": stream_prefix,
        "max_streams": max_streams,
    }
    try:
        validate_cloudwatch_logs_configuration(logs_metadata)
    except RuntimeError as error:
        raise ValueError(str(error)) from error
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="aws",
            type="cloudwatch_logs",
            id=identity,
            logical_key=logical_key or identity,
            region=region,
            ownership=Ownership.BORROWED,
            metadata=logs_metadata,
        ),
        lifecycle=ResourceLifecycle(
            blocking=False,
            retain_logs=True,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    return emit_resource(event, text=f"CloudWatch logs: {identity}")

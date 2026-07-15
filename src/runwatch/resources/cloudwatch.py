from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ..models import ResourceEvent, ResourceObservation, ResourceStatus
from .base import AwsResourceAdapter, ResourceOperationError

_STATISTICS = {"Average", "Sum", "Minimum", "Maximum", "SampleCount"}
_MAX_TRACKED_METRIC_SAMPLES = 1_440


def _validate_required_text(metadata: dict[str, Any], field: str) -> None:
    value = metadata.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ResourceOperationError(f"aws.cloudwatch_metric {field} must not be empty")


def _positive_integer(metadata: dict[str, Any], field: str, default: int) -> int:
    value = metadata.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ResourceOperationError(
            f"aws.cloudwatch_metric {field} must be a positive integer"
        )
    return value


def _validate_period(period: int) -> None:
    if period not in {1, 5, 10, 20, 30} and period % 60:
        raise ResourceOperationError(
            "aws.cloudwatch_metric period_seconds must be 1, 5, 10, 20, 30, or a multiple of 60"
        )


def _validate_dimensions(dimensions: Any) -> None:
    if not isinstance(dimensions, dict):
        raise ResourceOperationError(
            "aws.cloudwatch_metric dimensions must be a mapping with at most 30 entries"
        )
    typed_dimensions = cast(dict[Any, Any], dimensions)
    if len(typed_dimensions) > 30:
        raise ResourceOperationError(
            "aws.cloudwatch_metric dimensions must be a mapping with at most 30 entries"
        )
    invalid = any(
        not isinstance(key, str)
        or not key.strip()
        or not isinstance(value, str)
        or not value.strip()
        for key, value in typed_dimensions.items()
    )
    if invalid:
        raise ResourceOperationError(
            "aws.cloudwatch_metric dimension names and values must not be empty"
        )


def validate_cloudwatch_metric_configuration(metadata: dict[str, Any]) -> None:
    for field in ("namespace", "metric_name"):
        _validate_required_text(metadata, field)
    statistic = metadata.get("statistic", "Average")
    if statistic not in _STATISTICS:
        raise ResourceOperationError("aws.cloudwatch_metric statistic is not supported")
    period = _positive_integer(metadata, "period_seconds", 60)
    _validate_period(period)
    lookback = _positive_integer(metadata, "lookback_seconds", 900)
    if (lookback + period - 1) // period > 1_440:
        raise ResourceOperationError(
            "aws.cloudwatch_metric lookback produces more than 1440 datapoints"
        )
    _validate_dimensions(metadata.get("dimensions", {}))


def _metric_series_identity(
    namespace: str,
    metric_name: str,
    statistic: str,
    period: int,
    dimensions: dict[str, str],
) -> str:
    return json.dumps(
        [namespace, metric_name, statistic, period, sorted(dimensions.items())],
        separators=(",", ":"),
    )


def _metric_sample(point: dict[str, Any], statistic: str) -> dict[str, Any]:
    return {
        "timestamp": point["Timestamp"].isoformat(),
        "value": point.get(statistic),
        "unit": point.get("Unit"),
    }


def _new_metric_samples(
    series: list[dict[str, Any]],
    cursor: dict[str, Any],
    *,
    identity: str,
) -> list[dict[str, Any]]:
    raw_seen: object = (
        cursor.get("metric_sample_fingerprints", {})
        if cursor.get("metric_series_identity") == identity
        else {}
    )
    seen = (
        {
            str(timestamp): str(fingerprint)
            for timestamp, fingerprint in cast(dict[object, object], raw_seen).items()
            if isinstance(timestamp, str) and isinstance(fingerprint, str)
        }
        if isinstance(raw_seen, dict)
        else {}
    )
    changed: list[dict[str, Any]] = []
    for sample in series:
        timestamp = str(sample["timestamp"])
        fingerprint = json.dumps(
            [sample.get("value"), sample.get("unit")],
            separators=(",", ":"),
        )
        if seen.get(timestamp) != fingerprint:
            changed.append(sample)
        seen[timestamp] = fingerprint
    retained_timestamps = sorted(seen)[-_MAX_TRACKED_METRIC_SAMPLES:]
    cursor["metric_series_identity"] = identity
    cursor["metric_sample_fingerprints"] = {
        timestamp: seen[timestamp] for timestamp in retained_timestamps
    }
    return changed[-_MAX_TRACKED_METRIC_SAMPLES:]


def _metric_payload(
    namespace: str,
    metric_name: str,
    statistic: str,
    series: list[dict[str, Any]],
) -> dict[str, Any]:
    latest = series[-1] if series else None
    return {
        "namespace": namespace,
        "metric_name": metric_name,
        "statistic": statistic,
        "latest": latest,
        "latest_value": latest.get("value") if latest else None,
        "latest_timestamp": latest.get("timestamp") if latest else None,
        "latest_unit": latest.get("unit") if latest else None,
        "datapoint_count": len(series),
        "series": series,
    }


class CloudWatchMetricAdapter(AwsResourceAdapter):
    provider = "aws"
    resource_type = "cloudwatch_metric"

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        validate_cloudwatch_metric_configuration(event.resource.metadata)

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        metadata = resource.get("metadata", {})
        validate_cloudwatch_metric_configuration(metadata)
        namespace = metadata["namespace"]
        metric_name = metadata["metric_name"]
        statistic = metadata.get("statistic", "Average")
        period = int(metadata.get("period_seconds", 60))
        lookback = int(metadata.get("lookback_seconds", 900))
        dimension_values = cast(dict[str, str], metadata.get("dimensions", {}))
        dimensions = [
            {"Name": key, "Value": value} for key, value in dimension_values.items()
        ]
        end = datetime.now(timezone.utc)
        start = end - timedelta(seconds=lookback)
        client = self.aws.client("cloudwatch", resource.get("region"))
        response = await asyncio.to_thread(
            client.get_metric_statistics,
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=end,
            Period=period,
            Statistics=[statistic],
        )
        points = sorted(
            response.get("Datapoints", []), key=lambda item: item["Timestamp"]
        )
        samples_by_timestamp = {
            str(point["Timestamp"].isoformat()): _metric_sample(point, statistic)
            for point in points
        }
        series = [samples_by_timestamp[key] for key in sorted(samples_by_timestamp)]
        changed = _new_metric_samples(
            series,
            cursor,
            identity=_metric_series_identity(
                str(namespace),
                str(metric_name),
                str(statistic),
                period,
                dimension_values,
            ),
        )
        return ResourceObservation(
            status=ResourceStatus.RUNNING,
            terminal=False,
            metrics=_metric_payload(
                str(namespace), str(metric_name), str(statistic), series
            ),
            history_metrics=_metric_payload(
                str(namespace), str(metric_name), str(statistic), changed
            ),
        )

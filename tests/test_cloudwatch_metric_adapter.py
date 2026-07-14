from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.cloudwatch import CloudWatchMetricAdapter


class FakeCloudWatch:
    def __init__(self, points: list[dict[str, Any]]) -> None:
        self.points = points
        self.request: dict[str, Any] = {}

    def get_metric_statistics(self, **kwargs: Any) -> dict[str, Any]:
        self.request = kwargs
        return {"Datapoints": self.points}


class FakeAws:
    def __init__(self, points: list[dict[str, Any]]) -> None:
        self.cloudwatch = FakeCloudWatch(points)

    def client(self, service: str, region: str | None = None) -> FakeCloudWatch:
        assert service == "cloudwatch"
        assert region == "us-east-1"
        return self.cloudwatch


@pytest.mark.asyncio
async def test_metric_adapter_sorts_series_and_reports_latest(tmp_path: Path) -> None:
    early = datetime(2026, 1, 1, tzinfo=timezone.utc)
    late = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    aws = FakeAws(
        [
            {"Timestamp": late, "Sum": 9.0, "Unit": "Count"},
            {"Timestamp": early, "Sum": 3.0, "Unit": "Count"},
        ]
    )
    adapter = CloudWatchMetricAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    observation = await adapter.inspect(
        {
            "region": "us-east-1",
            "metadata": {
                "namespace": "Pipeline",
                "metric_name": "Rows",
                "statistic": "Sum",
                "period_seconds": 60,
                "lookback_seconds": 300,
                "dimensions": {"RunId": "abc"},
            },
        },
        {},
    )

    assert observation.status is ResourceStatus.RUNNING
    assert observation.metrics["latest_value"] == 9.0
    assert [point["value"] for point in observation.metrics["series"]] == [3.0, 9.0]
    assert observation.history_metrics == observation.metrics
    assert aws.cloudwatch.request["Dimensions"] == [{"Name": "RunId", "Value": "abc"}]


@pytest.mark.asyncio
async def test_metric_history_emits_only_new_or_revised_timestamp_samples(
    tmp_path: Path,
) -> None:
    early = datetime(2026, 1, 1, tzinfo=timezone.utc)
    late = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
    aws = FakeAws(
        [
            {"Timestamp": early, "Sum": 3.0, "Unit": "Count"},
            {"Timestamp": late, "Sum": 9.0, "Unit": "Count"},
        ]
    )
    adapter = CloudWatchMetricAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    resource: dict[str, Any] = {
        "region": "us-east-1",
        "metadata": {
            "namespace": "Pipeline",
            "metric_name": "Rows",
            "statistic": "Sum",
        },
    }
    cursor: dict[str, Any] = {}

    first = await adapter.inspect(resource, cursor)
    unchanged = await adapter.inspect(resource, cursor)
    aws.cloudwatch.points[0]["Sum"] = 4.0
    revised = await adapter.inspect(resource, cursor)
    resource["metadata"]["period_seconds"] = 300
    new_period = await adapter.inspect(resource, cursor)

    assert first.history_metrics is not None
    assert [point["value"] for point in first.history_metrics["series"]] == [3.0, 9.0]
    assert unchanged.metrics["series"] == first.metrics["series"]
    assert unchanged.history_metrics is not None
    assert unchanged.history_metrics["series"] == []
    assert revised.history_metrics is not None
    assert revised.history_metrics["series"] == [
        {"timestamp": early.isoformat(), "value": 4.0, "unit": "Count"}
    ]
    assert new_period.history_metrics is not None
    assert [point["value"] for point in new_period.history_metrics["series"]] == [
        4.0,
        9.0,
    ]


@pytest.mark.asyncio
async def test_metric_dedup_cursor_has_bounded_retention(tmp_path: Path) -> None:
    points = [
        {
            "Timestamp": datetime.fromtimestamp(index, timezone.utc),
            "Average": float(index),
        }
        for index in range(1_500)
    ]
    adapter = CloudWatchMetricAdapter(
        aws=FakeAws(points),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    cursor: dict[str, Any] = {}

    observation = await adapter.inspect(
        {
            "region": "us-east-1",
            "metadata": {
                "namespace": "Pipeline",
                "metric_name": "Rows",
                "period_seconds": 1,
                "lookback_seconds": 1_440,
            },
        },
        cursor,
    )

    assert len(observation.metrics["series"]) == 1_500
    assert observation.history_metrics is not None
    assert len(observation.history_metrics["series"]) == 1_440
    assert len(cursor["metric_sample_fingerprints"]) == 1_440


@pytest.mark.asyncio
async def test_metric_adapter_handles_empty_series(tmp_path: Path) -> None:
    adapter = CloudWatchMetricAdapter(
        aws=FakeAws([]),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    observation = await adapter.inspect(
        {
            "region": "us-east-1",
            "metadata": {"namespace": "Pipeline", "metric_name": "Rows"},
        },
        {},
    )
    assert observation.metrics["latest"] is None
    assert observation.metrics["datapoint_count"] == 0
    assert observation.history_metrics is not None
    assert observation.history_metrics["series"] == []

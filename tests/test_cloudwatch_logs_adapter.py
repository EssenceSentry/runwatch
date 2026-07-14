from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from runwatch.models import AwsSettings
from runwatch.resources.cloudwatch_logs import CloudWatchLogsAdapter


class FakeLogs:
    def __init__(self) -> None:
        self.describe_requests: list[dict[str, Any]] = []

    def describe_log_streams(self, **kwargs: Any) -> dict[str, Any]:
        self.describe_requests.append(kwargs)
        if "nextToken" not in kwargs:
            return {
                "logStreams": [{"logStreamName": "job/one"}],
                "nextToken": "streams-page-2",
            }
        return {"logStreams": [{"logStreamName": "job/two"}]}

    def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        stream = kwargs["logStreamName"]
        return {
            "events": [{"timestamp": 1, "message": f"from {stream}"}],
            "nextForwardToken": f"after-{stream}",
        }


class FakeAws:
    def __init__(self) -> None:
        self.logs = FakeLogs()

    def client(self, service: str, region: str | None = None) -> FakeLogs:
        assert service == "logs"
        return self.logs


@pytest.mark.asyncio
async def test_log_stream_discovery_is_paginated_and_cursors_are_saved(
    tmp_path: Path,
) -> None:
    aws = FakeAws()
    adapter = CloudWatchLogsAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(max_log_streams=2),
        working_dir=tmp_path,
    )
    cursor: dict[str, Any] = {}
    observation = await adapter.inspect(
        {
            "region": "us-east-1",
            "metadata": {"log_group": "/example", "stream_prefix": "job/"},
        },
        cursor,
    )

    assert len(aws.logs.describe_requests) == 2
    assert aws.logs.describe_requests[1]["nextToken"] == "streams-page-2"
    assert observation.metrics["stream_count"] == 2
    assert len(observation.log_lines) == 2
    assert cursor["log_tokens"] == {
        "job/one": "after-job/one",
        "job/two": "after-job/two",
    }


@pytest.mark.asyncio
async def test_bounded_log_stream_discovery_rotates_across_polls(
    tmp_path: Path,
) -> None:
    aws = FakeAws()
    adapter = CloudWatchLogsAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(max_log_streams=1),
        working_dir=tmp_path,
    )
    resource = {
        "metadata": {
            "log_group": "/example",
            "stream_prefix": "job/",
            "max_streams": 1,
        }
    }
    cursor: dict[str, Any] = {"log_tokens": {"deleted/stream": "stale-token"}}

    first = await adapter.inspect(resource, cursor)
    second = await adapter.inspect(resource, cursor)

    assert "job/one" in first.log_lines[0]
    assert first.metrics["stream_discovery_truncated"] is True
    assert "job/two" in second.log_lines[0]
    assert second.metrics["stream_rotation_active"] is True
    assert "stream_discovery_token" not in cursor
    assert "deleted/stream" not in cursor["log_tokens"]


class BusyStreamsLogs:
    def __init__(self) -> None:
        self.read_requests: list[dict[str, Any]] = []

    def describe_log_streams(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "logStreams": [
                {"logStreamName": "job/one"},
                {"logStreamName": "job/two"},
            ]
        }

    def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.read_requests.append(kwargs)
        stream = kwargs["logStreamName"]
        limit = kwargs["limit"]
        return {
            "events": [
                {"timestamp": index, "message": f"{stream}-{index}"}
                for index in range(limit)
            ],
            "nextForwardToken": f"after-{stream}",
        }


@pytest.mark.asyncio
async def test_log_line_budget_is_shared_across_busy_streams(tmp_path: Path) -> None:
    aws = FakeAws()
    logs = BusyStreamsLogs()
    aws.logs = logs  # type: ignore[assignment]
    adapter = CloudWatchLogsAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(max_log_streams=2, max_log_lines_per_poll=4),
        working_dir=tmp_path,
    )

    observation = await adapter.inspect(
        {
            "region": "us-east-1",
            "metadata": {"log_group": "/example", "stream_prefix": "job/"},
        },
        {},
    )

    assert [request["limit"] for request in logs.read_requests] == [2, 2]
    assert sum("job/one" in line for line in observation.log_lines) == 2
    assert sum("job/two" in line for line in observation.log_lines) == 2

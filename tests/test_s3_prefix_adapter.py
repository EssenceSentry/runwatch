# pyright: reportUnknownVariableType=false
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.s3 import S3PrefixAdapter


class FakePaginator:
    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "Contents": [{"Key": "output/one", "Size": 11}],
                "IsTruncated": True,
            }
        ]


class FakeS3:
    def get_paginator(self, operation: str) -> FakePaginator:
        assert operation == "list_objects_v2"
        return FakePaginator()


class FakeAws:
    def client(self, service: str, region: str | None = None) -> FakeS3:
        assert service == "s3"
        return FakeS3()


@pytest.mark.asyncio
async def test_prefix_reports_lower_bound_when_page_limit_truncates_scan(
    tmp_path: Path,
) -> None:
    adapter = S3PrefixAdapter(
        aws=FakeAws(),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    observation = await adapter.inspect(
        {
            "external_id": "s3://bucket/output/",
            "metadata": {"max_pages": 1},
        },
        {},
    )

    assert observation.status is ResourceStatus.RUNNING
    assert observation.metrics["object_count"] == 1
    assert observation.metrics["truncated"] is True
    assert "lower bounds" in str(observation.message)


class IncrementalPaginator:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def paginate(self, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        self.requests.append(kwargs)
        if "StartAfter" not in kwargs:
            return (
                {
                    "Contents": [
                        {"Key": "output/a", "Size": 2},
                        {"Key": "output/b", "Size": 3},
                    ],
                    "IsTruncated": False,
                },
            )
        return ({"Contents": [], "IsTruncated": False},)


class IncrementalS3:
    def __init__(self) -> None:
        self.paginator = IncrementalPaginator()

    def get_paginator(self, operation: str) -> IncrementalPaginator:
        assert operation == "list_objects_v2"
        return self.paginator


class IncrementalAws:
    def __init__(self) -> None:
        self.s3 = IncrementalS3()

    def client(self, service: str, region: str | None = None) -> IncrementalS3:
        return self.s3


@pytest.mark.asyncio
async def test_unchanged_prefix_uses_incremental_scan_after_initial_reconciliation(
    tmp_path: Path,
) -> None:
    aws = IncrementalAws()
    adapter = S3PrefixAdapter(
        aws=aws,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    resource = {
        "external_id": "s3://bucket/output/",
        "metadata": {"max_pages": 10, "full_rescan_seconds": 300},
    }
    cursor: dict[str, Any] = {}
    first = await adapter.inspect(resource, cursor)
    second = await adapter.inspect(resource, cursor)

    assert first.metrics["scan_mode"] == "full"
    assert second.metrics["scan_mode"] == "incremental"
    assert second.metrics["object_count"] == 2
    assert aws.s3.paginator.requests[1]["StartAfter"] == "output/b"


@pytest.mark.asyncio
async def test_failed_full_rescan_does_not_destroy_last_good_cursor(
    tmp_path: Path,
) -> None:
    class BrokenPaginator:
        def paginate(self, **kwargs: Any):
            raise RuntimeError("temporary S3 failure")

    class BrokenS3:
        def get_paginator(self, operation: str) -> BrokenPaginator:
            return BrokenPaginator()

    class BrokenAws:
        def client(self, service: str, region: str | None = None) -> BrokenS3:
            return BrokenS3()

    adapter = S3PrefixAdapter(
        aws=BrokenAws(),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    cursor: dict[str, Any] = {
        "prefix_scan": {
            "initialized": True,
            "object_count": 7,
            "total_bytes": 70,
            "last_key": "output/seven",
            "marker_found": False,
            "last_full_scan": 1.0,
        }
    }
    before = {"prefix_scan": dict(cursor["prefix_scan"])}

    with pytest.raises(RuntimeError, match="temporary"):
        await adapter.inspect(
            {
                "external_id": "s3://bucket/output/",
                "metadata": {"full_rescan_seconds": 0},
            },
            cursor,
        )

    assert cursor == before

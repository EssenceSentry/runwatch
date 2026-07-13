# pyright: reportUnknownLambdaType=false
from __future__ import annotations

import io
import json
import math
from pathlib import Path
from typing import Any

import pytest

from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.s3_manifest import S3ManifestAdapter


class FakeS3:
    payload: dict[str, Any]

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return {"Body": io.BytesIO(json.dumps(self.payload).encode()), "ETag": "etag"}


class FakeAws:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.s3 = FakeS3()
        self.s3.payload = payload

    def client(self, service: str, region: str | None = None) -> Any:
        return self.s3


class TrackingBody:
    def __init__(self, payload: bytes, *, read_error: Exception | None = None) -> None:
        self.payload = payload
        self.read_error = read_error
        self.read_calls = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        self.read_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return self.payload if size < 0 else self.payload[:size]

    def close(self) -> None:
        self.closed = True


class ResponseAws:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response

    def client(self, service: str, region: str | None = None) -> Any:
        return type(
            "S3",
            (),
            {"get_object": lambda instance, **kwargs: self.response},
        )()


def _adapter_for_response(
    tmp_path: Path, response: dict[str, Any]
) -> S3ManifestAdapter:
    return S3ManifestAdapter(
        aws=ResponseAws(response),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )


@pytest.mark.asyncio
async def test_manifest_completed_state(tmp_path: Path) -> None:
    adapter = S3ManifestAdapter(
        aws=FakeAws(
            {
                "schema_version": 1,
                "status": "completed",
                "completed": 10,
                "total": 10,
                "message": "done",
                "metrics": {"rows": 200},
            }
        ),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    observation = await adapter.inspect(
        {"external_id": "s3://bucket/progress.json"}, {}
    )
    assert observation.status is ResourceStatus.COMPLETED
    assert observation.terminal
    assert observation.metrics["rows"] == 200


@pytest.mark.asyncio
async def test_manifest_body_is_closed_after_success(tmp_path: Path) -> None:
    body = TrackingBody(
        json.dumps(
            {
                "schema_version": 1,
                "status": "running",
                "completed": 1,
            }
        ).encode()
    )
    adapter = _adapter_for_response(tmp_path, {"Body": body, "ETag": "etag"})

    await adapter.inspect({"external_id": "s3://bucket/progress.json"}, {})

    assert body.closed
    assert body.read_calls == 1


@pytest.mark.asyncio
async def test_manifest_body_is_closed_when_content_length_is_too_large(
    tmp_path: Path,
) -> None:
    body = TrackingBody(b"{}")
    adapter = _adapter_for_response(
        tmp_path,
        {"Body": body, "ContentLength": 1_048_577},
    )

    with pytest.raises(RuntimeError, match="1 MiB"):
        await adapter.inspect({"external_id": "s3://bucket/progress.json"}, {})

    assert body.closed
    assert body.read_calls == 0


@pytest.mark.asyncio
async def test_manifest_body_is_closed_when_read_exceeds_limit(tmp_path: Path) -> None:
    body = TrackingBody(b"x" * 1_048_577)
    adapter = _adapter_for_response(tmp_path, {"Body": body})

    with pytest.raises(RuntimeError, match="1 MiB"):
        await adapter.inspect({"external_id": "s3://bucket/progress.json"}, {})

    assert body.closed
    assert body.read_calls == 1


@pytest.mark.asyncio
async def test_manifest_body_is_closed_when_read_fails(tmp_path: Path) -> None:
    body = TrackingBody(b"", read_error=OSError("read failed"))
    adapter = _adapter_for_response(tmp_path, {"Body": body})

    with pytest.raises(OSError, match="read failed"):
        await adapter.inspect({"external_id": "s3://bucket/progress.json"}, {})

    assert body.closed
    assert body.read_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "schema_version"),
        ({"schema_version": 2}, "schema_version"),
        ({"schema_version": 1, "status": "unknown", "completed": 0}, "status"),
        ({"schema_version": 1, "status": "running", "completed": -1}, "completed"),
        (
            {"schema_version": 1, "status": "running", "completed": 0, "total": 0},
            "total",
        ),
        (
            {
                "schema_version": 1,
                "status": "running",
                "completed": 0,
                "metrics": {"nested": {"x": 1}},
            },
            "scalar",
        ),
        (
            {"schema_version": 1, "status": "running", "completed": True},
            "completed",
        ),
        (
            {
                "schema_version": 1,
                "status": "running",
                "completed": math.inf,
            },
            "completed",
        ),
        (
            {
                "schema_version": 1,
                "status": "running",
                "completed": 2,
                "total": 1,
            },
            "exceed",
        ),
        (
            {
                "schema_version": 1,
                "status": "running",
                "completed": 0,
                "metrics": {"bucket": "override"},
            },
            "reserved",
        ),
        (
            {
                "schema_version": 1,
                "status": "running",
                "completed": 0,
                "metrics": {"rate": math.nan},
            },
            "scalar",
        ),
    ],
)
async def test_manifest_validation_errors(
    tmp_path: Path, payload: Any, message: str
) -> None:
    adapter = S3ManifestAdapter(
        aws=FakeAws(payload),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    with pytest.raises(RuntimeError, match=message):
        await adapter.inspect({"external_id": "s3://bucket/progress.json"}, {})


@pytest.mark.asyncio
async def test_missing_manifest_is_pending(tmp_path: Path) -> None:
    class MissingS3:
        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "NoSuchKey"}}  # type: ignore[attr-defined]
            raise error

    adapter = S3ManifestAdapter(
        aws=type(
            "Aws",
            (),
            {"client": lambda self, service, region=None: MissingS3()},
        )(),  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    observation = await adapter.inspect(
        {"external_id": "s3://bucket/progress.json"}, {}
    )
    assert observation.status is ResourceStatus.PENDING

# pyright: reportPrivateUsage=false, reportUnknownParameterType=false, reportMissingParameterType=false
from __future__ import annotations

import copy
import html
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from runwatch._backends import default_output, resolve_backend
from runwatch._marimo_document import WORKER, MarimoDocument
from runwatch._marimo_output import MarimoOutputs
from runwatch._marimo_session import MarimoSession, MarimoTransportError
from runwatch._marimo_snapshot import render_snapshot, snapshot_view, static_html
from runwatch._marimo_worker import inspect_document
from runwatch.emit import EVENT_MIME_TYPE, FALLBACK_PREFIX
from runwatch.models import MarimoSettings, NotebookSettings, RunwatchConfig
from runwatch.validation import validate_execution

SOURCE = """import marimo
app = marimo.App()
@app.cell
def _():
    from pathlib import Path
    Path("must-not-execute").touch()
    return
"""


def test_validation_is_static_and_requires_a_prepared_environment(
    tmp_path: Path,
) -> None:
    path = tmp_path / "native.py"
    path.write_text(SOURCE)
    report = validate_execution(path, RunwatchConfig(), working_dir=tmp_path)
    assert report["valid"], report
    assert report["backend"] == "marimo"
    assert not (tmp_path / "must-not-execute").exists()
    for declaration in (
        'dependencies = ["nonexistent-runwatch-test-package"]',
        'requires-python = ">=9"',
        'dependencies = ["marimo<0.1"]',
    ):
        path.write_text(f"# /// script\n# {declaration}\n# ///\n" + SOURCE)
        report = validate_execution(path, RunwatchConfig(), working_dir=tmp_path)
        assert not report["valid"]
        assert not (tmp_path / "must-not-execute").exists()


@pytest.mark.parametrize(
    "source",
    [
        "print('script')",
        SOURCE + "\n@app.cell\ndef _():\n    from pathlib import Path\n    return\n",
        "import marimo\napp=marimo.App()\n@app.cell\ndef _():\n    x = y\n    return x,\n@app.cell\ndef _():\n    y = x\n    return y,\n",
    ],
)
def test_structural_errors_are_rejected_before_execution(
    tmp_path: Path, source: str
) -> None:
    path = tmp_path / "invalid.py"
    path.write_text(source)
    with pytest.raises(Exception):
        inspect_document(path)


def test_selection_environment_and_identity_guards(tmp_path: Path) -> None:
    path = tmp_path / "source.py"
    path.write_text(SOURCE)
    document = MarimoDocument(**inspect_document(path))
    fresh = copy.deepcopy(document)
    fresh.cells[0]["code"] = "answer = 42"
    document.validate_resume(fresh)
    for field, value in (
        ("structure", dict[str, Any]()),
        ("environment_hash", "changed"),
        ("python", "another-python"),
    ):
        changed = copy.deepcopy(fresh)
        setattr(changed, field, value)
        with pytest.raises(RuntimeError, match="restart"):
            document.validate_resume(changed)
    for index in (-1, 1):
        with pytest.raises(ValueError):
            document.selection(index)
    assert document.selection(None) == document.selection(0)
    assert resolve_backend(path, NotebookSettings()) == "marimo"
    assert default_output(path, tmp_path, "marimo") == path.with_suffix(
        ".executed.html"
    )
    with pytest.raises(ValueError, match="kernel_name"):
        resolve_backend(path, NotebookSettings(kernel_name="python3"))
    with pytest.raises(ValueError, match="requires"):
        resolve_backend(path, NotebookSettings(backend="jupyter"))
    with pytest.raises(ValueError, match="Marimo backend"):
        resolve_backend(
            path.with_suffix(".ipynb"),
            NotebookSettings(marimo=MarimoSettings(python_executable="custom")),
        )


def test_optional_backend_imports_without_marimo_installed(tmp_path: Path) -> None:
    path = tmp_path / "source.py"
    path.write_text(SOURCE)
    result = subprocess.run(
        [sys.executable, "-S", str(WORKER), "inspect", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "marimo" in json.loads(result.stdout)["error"]
    assert not (tmp_path / "must-not-execute").exists()


def test_wrapped_mime_fragmented_streams_and_deduplication() -> None:
    outputs = MarimoOutputs()
    payload = {
        "schema_version": 2,
        "event": "progress",
        "event_id": "one",
        "completed": 1,
        "total": 2,
    }
    wrapper = {"mime_type": EVENT_MIME_TYPE, "payload": payload}
    line = FALLBACK_PREFIX + json.dumps(wrapper) + "\n"
    results: list[Any] = []
    for chunk in [line[:4], line[4:24], line[24:-2], line[-2:]]:
        results.extend(outputs.normalize("cell", {"channel": "stdout", "data": chunk}))
    assert len(results) == 1
    assert results[0]["data"][EVENT_MIME_TYPE] == payload
    bundle = {EVENT_MIME_TYPE: payload, "text/plain": "protocol"}
    rich = f"<marimo-mime-renderer data-mime='{html.escape(json.dumps('application/vnd.marimo+mimebundle'))}' data-data='{html.escape(json.dumps(bundle))}'></marimo-mime-renderer>"
    assert outputs.normalize("cell", {"mimetype": "text/html", "data": rich}) == []
    outputs.attempts["cell"] = 2
    assert len(outputs.normalize("cell", {"mimetype": "text/html", "data": rich})) == 1
    assert (
        outputs.normalize("cell", {"channel": "stdout", "data": "ordinary"})[0]["text"]
        == "ordinary"
    )
    outputs.normalize("cell", {"channel": "stderr", "data": FALLBACK_PREFIX + "broken"})
    assert "broken" in outputs.flush("cell")[0]["text"]


def test_static_table_and_scriptless_snapshot() -> None:
    value = html.escape(json.dumps(json.dumps([{"a": "<script>unsafe</script>"}])))
    table = static_html(
        f"<marimo-ui-element><marimo-table data-data='{value}'></marimo-table></marimo-ui-element>"
    )
    assert "<table>" in table and "&lt;script&gt;" in table
    payload = {
        "schema_version": 1,
        "backend": "marimo",
        "cells": [
            {
                "id": "cell",
                "code": "x=1",
                "outputs": [
                    {
                        "output_type": "display_data",
                        "data": {"text/html": table + "<script>alert(1)</script>"},
                        "metadata": {},
                    }
                ],
            }
        ],
    }
    rendered = render_snapshot(json.dumps(payload).encode())
    assert "<script" not in rendered
    with pytest.raises(ValueError):
        snapshot_view({"schema_version": 99})


async def ignore_message(_message: dict[str, Any]) -> None:
    pass


@pytest.mark.asyncio
async def test_ambiguous_transport_is_never_retried(tmp_path: Path) -> None:
    count = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        return httpx.Response(
            200, text='event: stdout\ndata: {"data":"side effect already happened"}\n\n'
        )

    session = MarimoSession(tmp_path, tmp_path, NotebookSettings(), ignore_message)
    session.client = httpx.AsyncClient(
        base_url="http://127.0.0.1", transport=httpx.MockTransport(handler)
    )
    try:
        with pytest.raises(MarimoTransportError, match="uncertain"):
            await session.execute("side_effect()")
        assert count == 1
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_transient_assets_are_materialized_and_cached(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(
            200, headers={"content-type": "image/png"}, content=b"image"
        )

    session = MarimoSession(tmp_path, tmp_path, NotebookSettings(), ignore_message)
    session.client = httpx.AsyncClient(
        base_url="http://127.0.0.1", transport=httpx.MockTransport(handler)
    )
    try:
        first = await session.materialize('<img src="./@file/image.png">')
        assert "data:image/png;base64," in first
        assert await session.materialize('<img src="./@file/image.png">') == first
        escaped = await session.materialize(
            "<marimo-table data-data='&quot;./@file/image.png&quot;'></marimo-table>"
        )
        assert "data:image/png;base64," in escaped
        assert len(calls) == 1
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_surviving_owned_runtime_is_reaped_before_reconstruction(
    tmp_path: Path,
) -> None:
    first = MarimoSession(tmp_path, tmp_path, NotebookSettings(), ignore_message)
    second = MarimoSession(tmp_path, tmp_path, NotebookSettings(), ignore_message)
    try:
        await first.start(SOURCE)
        assert first.kernel_is_alive()
        assert not (tmp_path / "must-not-execute").exists()
        await second.start(SOURCE)
        assert not first.kernel_is_alive()
        assert second.kernel_is_alive()
        await first.close()
        assert second.kernel_is_alive()
        assert second.identity_path.exists()
    finally:
        await first.close()
        await second.close()

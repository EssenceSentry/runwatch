# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import nbformat
import pytest
from bs4 import BeautifulSoup

import runwatch._notebook_snapshot as snapshot_module
from runwatch._notebook_snapshot import (
    NotebookSnapshotChanged,
    NotebookSnapshotRenderer,
    NotebookSnapshotRenderError,
    NotebookSnapshotTooLarge,
)


def _write_notebook(path: Path, marker: str) -> None:
    nbformat.write(
        nbformat.v4.new_notebook(
            cells=[
                nbformat.v4.new_markdown_cell(f"# {marker}"),
                nbformat.v4.new_code_cell(
                    "print('saved')",
                    outputs=[nbformat.v4.new_output("stream", text=f"{marker}\n")],
                ),
            ]
        ),
        path,
    )


@pytest.mark.asyncio
async def test_renderer_selects_authoritative_lifecycle_snapshot(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.ipynb"
    partial = tmp_path / "executed.partial.ipynb"
    final = tmp_path / "executed.ipynb"
    _write_notebook(source, "source marker")
    renderer = NotebookSnapshotRenderer(
        source_path=source,
        partial_output_path=partial,
        output_path=final,
    )

    initial = await renderer.render(use_final=False)
    assert initial.description.kind == "source"
    assert "source marker" in initial.html

    _write_notebook(final, "stale final marker")
    _write_notebook(partial, "checkpoint marker")
    running = await renderer.render(use_final=False)
    assert running.description.kind == "checkpoint"
    assert "checkpoint marker" in running.html
    assert "stale final marker" not in running.html

    completed = await renderer.render(use_final=True)
    assert completed.description.kind == "final"
    assert "stale final marker" in completed.html


@pytest.mark.asyncio
async def test_renderer_refreshes_metadata_when_final_bytes_match_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.ipynb"
    partial = tmp_path / "executed.partial.ipynb"
    final = tmp_path / "executed.ipynb"
    _write_notebook(source, "source marker")
    _write_notebook(partial, "settled marker")
    renderer = NotebookSnapshotRenderer(
        source_path=source,
        partial_output_path=partial,
        output_path=final,
    )
    original = snapshot_module._render_notebook
    calls = 0

    def counted(payload: bytes) -> str:
        nonlocal calls
        calls += 1
        return original(payload)

    monkeypatch.setattr(snapshot_module, "_render_notebook", counted)
    checkpoint = await renderer.render(use_final=False)
    final.write_bytes(partial.read_bytes())
    final_mtime = partial.stat().st_mtime + 10
    os.utime(final, (final_mtime, final_mtime))

    completed = await renderer.render(use_final=True)

    assert completed.description.kind == "final"
    assert completed.description.updated_at != checkpoint.description.updated_at
    assert completed.digest == checkpoint.digest
    assert completed.html == checkpoint.html
    assert calls == 1


@pytest.mark.asyncio
async def test_renderer_is_single_flight_and_caches_by_content_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.ipynb"
    _write_notebook(source, "generation one")
    renderer = NotebookSnapshotRenderer(
        source_path=source,
        partial_output_path=tmp_path / "partial.ipynb",
        output_path=tmp_path / "final.ipynb",
    )
    original = snapshot_module._render_notebook
    calls = 0

    def counted(payload: bytes) -> str:
        nonlocal calls
        calls += 1
        return original(payload)

    monkeypatch.setattr(snapshot_module, "_render_notebook", counted)
    first, concurrent = await asyncio.gather(
        renderer.render(use_final=False),
        renderer.render(use_final=False),
    )
    assert first.digest == concurrent.digest
    assert calls == 1

    cached = await renderer.render(
        use_final=False,
        expected_digest=first.digest,
    )
    assert cached is first
    assert calls == 1

    _write_notebook(source, "generation two")
    second = await renderer.render(use_final=False)
    assert second.digest != first.digest
    assert calls == 2
    with pytest.raises(NotebookSnapshotChanged):
        await renderer.render(
            use_final=False,
            expected_digest=first.digest,
        )


@pytest.mark.asyncio
async def test_renderer_sanitizes_active_and_interactive_content(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.ipynb"
    png = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
        "/x8AAusB9Y9Zl1sAAAAASUVORK5CYII="
    )
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell("""
# Safe heading
<script>window.evil = true</script>
<iframe src="https://evil.example/frame"></iframe>
<form action="https://evil.example/post"><button>Send</button></form>
<a href="https://evil.example/link" onclick="evil()">remote link</a>
<img src="https://evil.example/pixel" onerror="evil()">
<details><summary>Toggle</summary><p>Always visible</p></details>
<video src="https://evil.example/video" controls autoplay></video>
"""),
            nbformat.v4.new_code_cell(
                "display(plot)",
                outputs=[
                    nbformat.v4.new_output(
                        "display_data",
                        data={
                            "application/vnd.plotly.v1+json": {"data": []},
                            "application/javascript": "fetch('https://evil.example')",
                            "text/plain": "Plotly object",
                        },
                    ),
                    nbformat.v4.new_output(
                        "display_data",
                        data={
                            "text/html": "<table><tr><td>kept table</td></tr></table>",
                            "image/png": png,
                        },
                    ),
                ],
            ),
            nbformat.v4.new_raw_cell("<script>raw evil</script>"),
        ],
        metadata={"widgets": {"application/vnd.jupyter.widget-state+json": {}}},
    )
    nbformat.write(notebook, source)
    renderer = NotebookSnapshotRenderer(
        source_path=source,
        partial_output_path=tmp_path / "partial.ipynb",
        output_path=tmp_path / "final.ipynb",
    )

    html = (await renderer.render(use_final=False)).html

    assert "Safe heading" in html
    assert "kept table" in html
    assert "data:image/png;base64" in html
    assert "Interactive output omitted from this read-only snapshot." in html
    assert "runwatch-notebook-read-only" in html
    assert "<script" not in html.lower()
    assert "<iframe" not in html.lower()
    assert "<form" not in html.lower()
    assert "href=" not in html.lower()
    rendered = BeautifulSoup(html, "html.parser")
    heading = rendered.find("h1")
    assert heading is not None and "Safe heading" in heading.get_text()
    assert rendered.find("table") is not None
    assert not rendered.find_all(["script", "iframe", "form", "object", "embed"])
    for element in rendered.find_all(True):
        assert not any(
            attribute.lower().startswith("on") for attribute in element.attrs
        )
        assert not {"href", "action", "formaction", "srcdoc"}.intersection(
            element.attrs
        )
        source_value = element.attrs.get("src")
        assert source_value is None or str(source_value).startswith("data:")
    details = rendered.find("details")
    summary = rendered.find("summary")
    video = rendered.find("video")
    assert details is not None and "open" in details.attrs
    assert summary is not None and summary.attrs.get("tabindex") == "-1"
    assert video is not None and not {
        "autoplay",
        "controls",
        "loop",
        "src",
    }.intersection(video.attrs)
    assert "raw evil" not in html
    assert "widget-state" not in html


@pytest.mark.asyncio
async def test_renderer_bounds_inputs_and_reports_invalid_notebooks_safely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.ipynb"
    source.write_bytes(b"not a notebook")
    renderer = NotebookSnapshotRenderer(
        source_path=source,
        partial_output_path=tmp_path / "partial.ipynb",
        output_path=tmp_path / "final.ipynb",
    )
    with pytest.raises(NotebookSnapshotRenderError) as invalid:
        await renderer.render(use_final=False)
    assert str(source) not in str(invalid.value)

    monkeypatch.setattr(snapshot_module, "_MAX_NOTEBOOK_BYTES", 4)
    with pytest.raises(NotebookSnapshotTooLarge) as oversized:
        await NotebookSnapshotRenderer(
            source_path=source,
            partial_output_path=tmp_path / "partial.ipynb",
            output_path=tmp_path / "final.ipynb",
        ).render(use_final=False)
    assert str(source) not in str(oversized.value)


@pytest.mark.asyncio
async def test_renderer_bounds_rendered_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.ipynb"
    _write_notebook(source, "bounded output")
    monkeypatch.setattr(snapshot_module, "_MAX_RENDERED_BYTES", 10)

    with pytest.raises(NotebookSnapshotTooLarge):
        await NotebookSnapshotRenderer(
            source_path=source,
            partial_output_path=tmp_path / "partial.ipynb",
            output_path=tmp_path / "final.ipynb",
        ).render(use_final=False)


def test_snapshot_limits_are_large_enough_for_normal_notebooks() -> None:
    limits = {
        "input": snapshot_module._MAX_NOTEBOOK_BYTES,
        "output": snapshot_module._MAX_RENDERED_BYTES,
    }
    assert limits == {
        "input": 50 * 1024 * 1024,
        "output": 100 * 1024 * 1024,
    }

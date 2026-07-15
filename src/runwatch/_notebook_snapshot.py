from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast

import nbformat
from bs4 import BeautifulSoup
from bs4.element import Tag
from nbconvert import HTMLExporter
from nbformat import NotebookNode

_SnapshotKind = Literal["source", "checkpoint", "final"]
_MAX_NOTEBOOK_BYTES = 50 * 1024 * 1024
_MAX_RENDERED_BYTES = 100 * 1024 * 1024
_INTERACTIVE_MIME_TYPES = frozenset(
    {
        "application/javascript",
        "application/vnd.jupyter.widget-view+json",
        "application/vnd.plotly.v1+json",
        "application/vnd.vega.v5+json",
        "application/vnd.vegalite.v4+json",
        "application/vnd.vegalite.v5+json",
    }
)
_READ_ONLY_STYLE = """
<style id="runwatch-notebook-read-only">
  a, button, input, select, textarea, summary, audio, video,
  [contenteditable], [role="button"] {
    pointer-events: none !important;
  }
  input, select, textarea, [contenteditable] {
    user-select: text !important;
    -webkit-user-modify: read-only !important;
  }
</style>
"""
_INTERACTIVE_PLACEHOLDER = "Interactive output omitted from this read-only snapshot."


class NotebookSnapshotUnavailable(RuntimeError):
    pass


class NotebookSnapshotRenderError(RuntimeError):
    pass


class NotebookSnapshotTooLarge(RuntimeError):
    pass


class NotebookSnapshotChanged(RuntimeError):
    pass


@dataclass(frozen=True)
class NotebookSnapshotDescription:
    kind: _SnapshotKind
    updated_at: str


@dataclass(frozen=True)
class _NotebookSnapshotDocument:
    description: NotebookSnapshotDescription
    digest: str
    payload: bytes


@dataclass(frozen=True)
class RenderedNotebookSnapshot:
    description: NotebookSnapshotDescription
    digest: str
    html: str


class NotebookSnapshotRenderer:
    def __init__(
        self,
        *,
        source_path: Path,
        partial_output_path: Path,
        output_path: Path,
    ) -> None:
        self.source_path = source_path
        self.partial_output_path = partial_output_path
        self.output_path = output_path
        self._render_lock = asyncio.Lock()
        self._cached: RenderedNotebookSnapshot | None = None

    async def render(
        self,
        *,
        use_final: bool,
        expected_digest: str | None = None,
    ) -> RenderedNotebookSnapshot:
        async with self._render_lock:
            if self._cached is not None and expected_digest == self._cached.digest:
                return self._cached
            try:
                document = await asyncio.to_thread(
                    self._load_document,
                    use_final=use_final,
                )
            except (NotebookSnapshotUnavailable, NotebookSnapshotTooLarge):
                raise
            except Exception as error:
                raise NotebookSnapshotRenderError(
                    "Notebook snapshot could not be read " f"({type(error).__name__})"
                ) from error
            if expected_digest is not None and document.digest != expected_digest:
                raise NotebookSnapshotChanged("A newer notebook snapshot is available")
            if self._cached is not None and document.digest == self._cached.digest:
                if document.description == self._cached.description:
                    return self._cached
                rendered = RenderedNotebookSnapshot(
                    description=document.description,
                    digest=document.digest,
                    html=self._cached.html,
                )
                self._cached = rendered
                return rendered
            try:
                html = await asyncio.to_thread(
                    _render_notebook,
                    document.payload,
                )
            except NotebookSnapshotTooLarge:
                raise
            except Exception as error:
                raise NotebookSnapshotRenderError(
                    f"Notebook snapshot could not be rendered ({type(error).__name__})"
                ) from error
            rendered = RenderedNotebookSnapshot(
                description=document.description,
                digest=document.digest,
                html=html,
            )
            self._cached = rendered
            return rendered

    def _load_document(self, *, use_final: bool) -> _NotebookSnapshotDocument:
        for kind, path in self._candidates(use_final=use_final):
            try:
                with path.open("rb") as handle:
                    stat = os.fstat(handle.fileno())
                    if stat.st_size > _MAX_NOTEBOOK_BYTES:
                        raise NotebookSnapshotTooLarge(
                            "Notebook snapshot exceeds the 50 MiB input limit"
                        )
                    payload = handle.read(_MAX_NOTEBOOK_BYTES + 1)
            except FileNotFoundError:
                continue
            if len(payload) > _MAX_NOTEBOOK_BYTES:
                raise NotebookSnapshotTooLarge(
                    "Notebook snapshot exceeds the 50 MiB input limit"
                )
            return _NotebookSnapshotDocument(
                description=NotebookSnapshotDescription(
                    kind=kind,
                    updated_at=_timestamp(stat.st_mtime),
                ),
                digest=hashlib.sha256(payload).hexdigest(),
                payload=payload,
            )
        raise NotebookSnapshotUnavailable("Notebook snapshot is not available yet")

    def _candidates(self, *, use_final: bool) -> tuple[tuple[_SnapshotKind, Path], ...]:
        durable: tuple[tuple[_SnapshotKind, Path], ...] = (
            ("checkpoint", self.partial_output_path),
            ("source", self.source_path),
        )
        if not use_final:
            return durable
        return (("final", self.output_path), *durable)


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _render_notebook(payload: bytes) -> str:
    notebook = nbformat.reads(payload.decode("utf-8"), as_version=4)
    metadata = notebook.get("metadata")
    if isinstance(metadata, dict):
        cast(dict[str, Any], metadata).pop("widgets", None)
    _replace_interactive_outputs(notebook)
    exporter = HTMLExporter(
        template_name="lab",
        sanitize_html=False,
        embed_images=False,
        exclude_raw=True,
    )
    rendered, _resources = exporter.from_notebook_node(notebook)
    html = rendered
    if "</head>" in html:
        html = html.replace("</head>", f"{_READ_ONLY_STYLE}</head>", 1)
    else:
        html = f"{_READ_ONLY_STYLE}{html}"
    html = _harden_rendered_html(html)
    if len(html.encode("utf-8")) > _MAX_RENDERED_BYTES:
        raise NotebookSnapshotTooLarge(
            "Rendered notebook exceeds the 100 MiB output limit"
        )
    return html


def _harden_rendered_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    _remove_active_elements(soup)
    for element in soup.find_all(True):
        _neutralize_element(element)
    return str(soup)


def _remove_active_elements(soup: BeautifulSoup) -> None:
    for element in soup.find_all(
        ["script", "iframe", "object", "embed", "base", "form", "link"]
    ):
        element.decompose()
    for element in soup.find_all("meta"):
        if str(element.get("http-equiv", "")).lower() == "refresh":
            element.decompose()


def _neutralize_element(element: Tag) -> None:
    navigation_attributes = {
        "href",
        "xlink:href",
        "action",
        "formaction",
        "srcdoc",
        "srcset",
        "imagesrcset",
        "ping",
        "manifest",
        "target",
    }
    for attribute in tuple(element.attrs):
        if (
            attribute.lower().startswith("on")
            or attribute.lower() in navigation_attributes
        ):
            element.attrs.pop(attribute, None)
    for attribute in ("src", "poster", "background"):
        _remove_remote_attribute(element, attribute)
    if element.name in {"button", "input", "select", "textarea"}:
        element.attrs["disabled"] = ""
    if (
        element.name
        in {
            "a",
            "audio",
            "button",
            "input",
            "select",
            "summary",
            "textarea",
            "video",
        }
        or element.attrs.get("role") == "button"
    ):
        element.attrs["tabindex"] = "-1"
    if element.name == "details":
        element.attrs["open"] = ""
    if element.name in {"audio", "video"}:
        for attribute in ("autoplay", "controls", "loop"):
            element.attrs.pop(attribute, None)
    if "contenteditable" in element.attrs:
        element.attrs["contenteditable"] = "false"
        element.attrs["tabindex"] = "-1"
    element.attrs.pop("autofocus", None)


def _remove_remote_attribute(element: Tag, attribute: str) -> None:
    value = element.attrs.get(attribute)
    if value is not None and not str(value).strip().lower().startswith("data:"):
        element.attrs.pop(attribute, None)


def _replace_interactive_outputs(notebook: NotebookNode) -> None:
    for cell in notebook.cells:
        if cell.get("cell_type") != "code":
            continue
        for output in cell.get("outputs", []):
            _replace_interactive_output(output)


def _replace_interactive_output(output: NotebookNode) -> None:
    raw_data = output.get("data")
    if not isinstance(raw_data, dict):
        return
    data = cast(dict[str, Any], raw_data)
    interactive = _INTERACTIVE_MIME_TYPES.intersection(data)
    if not interactive:
        return
    for mime_type in interactive:
        data.pop(mime_type, None)
    html = data.get("text/html")
    if isinstance(html, str) and "<script" in html.lower():
        data.pop("text/html", None)
    if not _has_static_fallback(data):
        data["text/plain"] = _INTERACTIVE_PLACEHOLDER


def _has_static_fallback(data: dict[str, Any]) -> bool:
    return any(
        mime_type.startswith("image/") or mime_type == "text/html" for mime_type in data
    )

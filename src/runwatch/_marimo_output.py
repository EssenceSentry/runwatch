"""Normalize native outputs and structured events without invoking a renderer runtime."""

from __future__ import annotations

import json
from collections import OrderedDict
from typing import Any, cast

import nbformat
from bs4 import BeautifulSoup
from nbformat import NotebookNode

from .emit import EVENT_MIME_TYPE, FALLBACK_PREFIX, RESOURCE_MIME_TYPE

_EVENT_TYPES = (RESOURCE_MIME_TYPE, EVENT_MIME_TYPE)


class MarimoOutputs:
    def __init__(self) -> None:
        self.buffers: dict[tuple[str, str], str] = {}
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.attempts: dict[str, int] = {}
        self._scope = ""

    def normalize(self, cell_id: str, output: dict[str, Any]) -> list[NotebookNode]:
        self._scope = f"{cell_id}:{self.attempts.get(cell_id, 0)}:"
        channel = str(output.get("channel", "output"))
        data = output.get("data", "")
        if channel in {"stdout", "stderr"}:
            return self._stream(cell_id, channel, str(data))
        if channel == "marimo-error":
            return [
                nbformat.v4.new_output(
                    "error",
                    ename="MarimoError",
                    evalue=str(data),
                    traceback=[str(data)],
                )
            ]
        mime = str(output.get("mimetype", "text/plain"))
        if mime == "text/html":
            return self._html(str(data))
        if mime in _EVENT_TYPES:
            return self._event(mime, data)
        if mime == "application/vnd.marimo+mimebundle":
            return self._bundle(data)
        return [nbformat.v4.new_output("display_data", data={mime: data}, metadata={})]

    def _stream(self, cell_id: str, channel: str, text: str) -> list[NotebookNode]:
        key = (cell_id, channel)
        text = self.buffers.pop(key, "") + text
        results: list[NotebookNode] = []
        lines = text.splitlines(keepends=True)
        for line in lines:
            if line.startswith(FALLBACK_PREFIX) and not line.endswith("\n"):
                self.buffers[key] = line[-1_048_576:]
            elif line.startswith(FALLBACK_PREFIX):
                try:
                    wrapper = json.loads(line[len(FALLBACK_PREFIX) :])
                    results.extend(
                        self._event(wrapper["mime_type"], wrapper["payload"])
                    )
                except (ValueError, KeyError, TypeError):
                    results.append(
                        nbformat.v4.new_output("stream", name=channel, text=line)
                    )
            elif FALLBACK_PREFIX.startswith(line) and not line.endswith("\n"):
                self.buffers[key] = line
            else:
                results.append(
                    nbformat.v4.new_output("stream", name=channel, text=line)
                )
        return results

    def flush(self, cell_id: str) -> list[NotebookNode]:
        self._scope = f"{cell_id}:{self.attempts.get(cell_id, 0)}:"
        result: list[NotebookNode] = []
        for channel in ("stdout", "stderr"):
            value = self.buffers.pop((cell_id, channel), "")
            if value:
                result.extend(self._stream(cell_id, channel, value + "\n"))
        return result

    def _event(self, mime: str, payload: Any) -> list[NotebookNode]:
        if mime not in _EVENT_TYPES or not isinstance(payload, dict):
            return []
        value = cast(dict[str, Any], payload)
        event_id = str(value.get("event_id") or json.dumps(value, sort_keys=True))
        event_id = self._scope + event_id
        if event_id in self.seen:
            return []
        self.seen[event_id] = None
        if len(self.seen) > 100_000:
            self.seen.popitem(last=False)
        return [nbformat.v4.new_output("display_data", data={mime: value}, metadata={})]

    def _bundle(self, data: Any) -> list[NotebookNode]:
        if isinstance(data, str):
            data = json.loads(data)
        bundle = dict(cast(dict[str, Any], data))
        result: list[NotebookNode] = []
        structured = False
        for mime in _EVENT_TYPES:
            if mime in bundle:
                structured = True
                result.extend(self._event(mime, bundle.pop(mime)))
        if structured:
            bundle.pop("text/plain", None)
        if bundle:
            result.append(
                nbformat.v4.new_output("display_data", data=bundle, metadata={})
            )
        return result

    def _html(self, text: str) -> list[NotebookNode]:
        soup = BeautifulSoup(text, "html.parser")
        result: list[NotebookNode] = []
        for element in soup.find_all("marimo-mime-renderer"):
            try:
                mime = json.loads(str(element.get("data-mime", '""')))
                data = json.loads(str(element.get("data-data", "null")))
                if mime == "application/vnd.marimo+mimebundle":
                    result.extend(self._bundle(data))
                    element.decompose()
            except (ValueError, TypeError):
                continue
        remaining = str(soup)
        if (
            soup.get_text(strip=True)
            or soup.find(["img", "svg", "table"])
            or soup.find(lambda tag: tag.name.startswith("marimo-"))
        ):
            result.append(
                nbformat.v4.new_output(
                    "display_data", data={"text/html": remaining}, metadata={}
                )
            )
        return result

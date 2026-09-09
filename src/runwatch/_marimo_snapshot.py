"""Native, source-bound output snapshots and scriptless static presentation."""

from __future__ import annotations

import base64
import json
from typing import Any, cast

import nbformat
from bs4 import BeautifulSoup
from bs4.element import Tag
from nbformat import NotebookNode


def snapshot_view(payload: dict[str, Any]) -> NotebookNode:
    if payload.get("schema_version") != 1 or payload.get("backend") != "marimo":
        raise ValueError("Unsupported Marimo snapshot schema")
    cells: list[NotebookNode] = []
    for cell in payload["cells"]:
        outputs = [nbformat.from_dict(output) for output in cell.get("outputs", [])]
        for output in outputs:
            data = output.get("data", {})
            if "text/html" in data:
                data["text/html"] = static_html(str(data["text/html"]))
        cells.append(
            nbformat.v4.new_code_cell(
                cell["code"],
                id=cell["id"],
                outputs=outputs,
                execution_count=cell.get("attempt") or None,
            )
        )
    return nbformat.v4.new_notebook(cells=cells)


def static_html(value: str) -> str:
    soup = BeautifulSoup(value, "html.parser")
    for element in reversed(
        list(soup.find_all(lambda tag: tag.name.startswith("marimo-")))
    ):
        if element.parent is None:
            continue
        if element.name == "marimo-table":
            replacement = _static_table(soup, str(element.get("data-data", "null")))
            if replacement is not None:
                element.replace_with(replacement)
                continue
        if element.get_text(strip=True) or element.find(["img", "table", "svg", "pre"]):
            element.unwrap()
            continue
        replacement = soup.new_tag("pre")
        replacement.string = "Interactive output omitted from this read-only snapshot."
        element.replace_with(replacement)
    return str(soup)


def _static_table(soup: BeautifulSoup, value: str) -> Tag | None:
    try:
        rows = json.loads(value)
        if isinstance(rows, str):
            if rows.startswith("data:"):
                rows = base64.b64decode(rows.split(",", 1)[1]).decode()
            rows = json.loads(rows)
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return None
        rows = cast(list[dict[str, Any]], rows)
        table = soup.new_tag("table")
        header = soup.new_tag("tr")
        columns: list[str] = list(rows[0])
        for column in columns:
            cell = soup.new_tag("th")
            cell.string = column
            header.append(cell)
        table.append(header)
        for row in rows[:1000]:
            record = soup.new_tag("tr")
            for column in columns:
                cell = soup.new_tag("td")
                cell.string = str(row.get(column, ""))
                record.append(cell)
            table.append(record)
        return table
    except (ValueError, TypeError, KeyError, AttributeError):
        return None


def render_snapshot(payload: bytes) -> str:
    from ._notebook_snapshot import (
        _render_notebook,  # pyright: ignore[reportPrivateUsage]
    )

    return _render_notebook(payload)


def read_snapshot(payload: bytes) -> NotebookNode:
    return snapshot_view(json.loads(payload))

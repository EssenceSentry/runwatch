"""Static native source and identity handling, independent of the live session."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import nbformat
from nbformat import NotebookNode

from ._backends import NotebookDocument, python_executable
from .models import NotebookSettings

WORKER = Path(__file__).with_name("_marimo_worker.py")


@dataclass
class MarimoDocument:
    source: str
    cells: list[dict[str, Any]]
    structure: dict[str, Any]
    environment_hash: str
    version: str
    python: str

    @classmethod
    def read(
        cls, path: Path, settings: NotebookSettings, working_dir: Path
    ) -> MarimoDocument:
        result = subprocess.run(
            [python_executable(settings), str(WORKER), "inspect", str(path)],
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=settings.startup_timeout_seconds,
            check=False,
        )
        try:
            payload = json.loads(result.stdout)
        except (ValueError, TypeError) as error:
            raise RuntimeError(
                "Could not inspect Marimo; install the marimo extra in the selected Python environment"
            ) from error
        if result.returncode or "error" in payload:
            raise ValueError(str(payload.get("error", "Marimo inspection failed")))
        return cls(**payload)

    def records(self) -> list[dict[str, Any]]:
        return [
            {
                "cell_index": index,
                "cell_id": cell["id"],
                "cell_type": "code",
                "label": cell["name"] if cell["name"] != "_" else f"Cell {index + 1}",
                "source": cell["code"],
                "source_hash": hashlib.sha256(cell["code"].encode()).hexdigest(),
            }
            for index, cell in enumerate(self.cells)
        ]

    def view(self) -> NotebookNode:
        # nbformat is only an in-memory output container shared with the Jupyter
        # presentation pipeline. Marimo source is never executed as an ipynb.
        return nbformat.v4.new_notebook(
            cells=[
                nbformat.v4.new_code_cell(
                    cell["code"],
                    id=cell["id"],
                    metadata={
                        "runwatch": {"label": record["label"]},
                        "marimo": {"name": cell["name"], "parents": cell["parents"]},
                    },
                )
                for cell, record in zip(self.cells, self.records(), strict=True)
            ]
        )

    def validate_resume(self, fresh: NotebookDocument) -> None:
        if not isinstance(fresh, MarimoDocument) or self.structure != fresh.structure:
            raise RuntimeError(
                "Notebook structure or environment metadata changed; restart is required"
            )
        if (
            self.environment_hash != fresh.environment_hash
            or self.python != fresh.python
        ):
            raise RuntimeError("Python environment changed; restart is required")
        old_codes = [cell["code"] for cell in self.cells]
        self._validate_mapping(fresh, old_codes)
        for index, cell in enumerate(fresh.cells):
            code = cell["code"]
            if code != old_codes[index] and code in old_codes:
                raise RuntimeError(
                    "Cell reordering or ambiguous identity requires restart"
                )
            if cell["kind"] != "CellDef" and code != old_codes[index]:
                raise RuntimeError(
                    "Setup or top-level definition edits require restart"
                )

    def _validate_mapping(self, fresh: MarimoDocument, old_codes: list[str]) -> None:
        previous_changed = False
        for index, cell in enumerate(fresh.cells):
            changed = cell["code"] != old_codes[index] and cell["name"] == "_"
            if changed and previous_changed:
                raise RuntimeError(
                    "Adjacent unnamed cell edits have ambiguous identity; restart is required"
                )
            previous_changed = changed

    def selection(self, index: int | None) -> set[str]:
        if index is None:
            return {cell["id"] for cell in self.cells}
        if index < 0 or index >= len(self.cells):
            raise ValueError("from-cell is outside the Marimo notebook")
        selected = {self.cells[index]["id"]}
        while True:
            expanded = selected | {
                c["id"] for c in self.cells if selected.intersection(c["parents"])
            }
            if expanded == selected:
                break
            selected = expanded
        while True:
            expanded = selected | {
                parent
                for c in self.cells
                if c["id"] in selected
                for parent in c["parents"]
            }
            if expanded == selected:
                return selected
            selected = expanded

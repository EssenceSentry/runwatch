"""Jupyter document validation, independent of runtime execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import nbformat
from nbformat import NotebookNode

from ._backends import NotebookDocument
from ._runner_core import cell_records


@dataclass
class JupyterDocument:
    notebook: NotebookNode

    @classmethod
    def read(cls, path: Path) -> JupyterDocument:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        return cls(notebook)

    @property
    def source(self) -> str:
        return nbformat.writes(self.notebook)

    def records(self) -> list[dict[str, Any]]:
        return cell_records(self.notebook)

    def view(self) -> NotebookNode:
        return self.notebook

    def validate_resume(self, fresh: NotebookDocument, from_index: int = 0) -> None:
        if not isinstance(fresh, JupyterDocument):
            raise RuntimeError("Changing notebook backend requires a new run")
        if len(fresh.notebook.cells) != len(self.notebook.cells):
            raise RuntimeError("Cell additions/removals require restart")
        for index, (old, new) in enumerate(
            zip(self.notebook.cells, fresh.notebook.cells, strict=True)
        ):
            if old.id != new.id:
                raise RuntimeError("Cell reordering or replacement requires restart")
            if index < from_index and str(old.source) != str(new.source):
                raise RuntimeError(
                    f"Already-executed cell {index + 1} changed; restart is required"
                )

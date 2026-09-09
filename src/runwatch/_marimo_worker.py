"""Version-specific Marimo bridge, executed by the selected notebook interpreter.

Private Marimo imports are confined to this worker and scratchpad commands. Loading
a document here compiles its cells and graph, but never executes notebook code.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path
from typing import Any, cast


def module(name: str) -> Any:
    return importlib.import_module(name)


def environment() -> dict[str, Any]:
    version = importlib.metadata.version("marimo")
    if (
        not module("packaging.specifiers")
        .SpecifierSet(">=0.24.0,<0.25")
        .contains(version)
    ):
        raise RuntimeError(f"Unsupported Marimo {version}; expected >=0.24.0,<0.25")
    distributions = sorted(
        (str(dist.metadata["Name"]), dist.version)
        for dist in importlib.metadata.distributions()
    )
    digest = hashlib.sha256(
        json.dumps([sys.version, sys.prefix, distributions]).encode()
    ).hexdigest()
    return {"version": version, "python": sys.executable, "environment_hash": digest}


def check_requirements(source: str) -> None:
    metadata = module("marimo._utils.scripts").read_pyproject_from_script(source)
    if metadata is None:
        return
    specifiers = module("packaging.specifiers")
    required_python = metadata.get("requires-python")
    if required_python and not specifiers.SpecifierSet(required_python).contains(
        platform.python_version()
    ):
        raise ValueError(f"Notebook requires Python {required_python}")
    _check_installed(metadata.get("dependencies", []), set())


def _check_installed(values: list[str], seen: set[str]) -> None:
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        requirement = module("packaging.requirements").Requirement(value)
        if requirement.marker and not requirement.marker.evaluate():
            continue
        distribution = importlib.metadata.distribution(requirement.name)
        if requirement.url or not requirement.specifier.contains(distribution.version):
            raise ValueError(f"Prepared environment does not satisfy {value}")
        extras = set(distribution.metadata.get_all("Provides-Extra") or [])
        if not requirement.extras.issubset(extras):
            raise ValueError(f"Unknown dependency extras in {value}")
        _check_extra_requirements(distribution, requirement.extras, seen)


def _check_extra_requirements(
    distribution: Any, extras: set[str], seen: set[str]
) -> None:
    if not extras:
        return
    selected: list[str] = []
    for value in cast(list[str], distribution.requires or []):
        requirement = module("packaging.requirements").Requirement(value)
        if requirement.marker is None or any(
            requirement.marker.evaluate({"extra": extra}) for extra in extras
        ):
            requirement.marker = None
            selected.append(str(requirement))
    _check_installed(selected, seen)


def inspect_document(path: Path) -> dict[str, Any]:
    info = environment()
    source = path.read_text(encoding="utf-8")
    check_requirements(source)
    loader = module("marimo._ast.load")
    loaded = loader.get_notebook_status(str(path))
    if loaded.status not in {"valid", "has_warnings"} or loaded.notebook is None:
        raise ValueError("Source is not a valid Marimo notebook")
    app = loader.load_notebook_ir(loaded.notebook, filepath=str(path))
    internal = module("marimo._ast.app").InternalApp(app)
    graph = internal.graph  # Static compilation and cycle/redefinition checks only.
    ids = list(app._cell_manager.cell_ids())
    cells: list[dict[str, Any]] = []
    for cell_id, definition in zip(ids, loaded.notebook.cells, strict=True):
        cells.append(
            {
                "id": str(cell_id),
                "name": definition.name,
                "code": definition.code,
                "config": definition.options,
                "kind": type(definition).__name__,
                "parents": sorted(graph.parents.get(cell_id, ())),
            }
        )
    return {
        **info,
        "source": source,
        "cells": cells,
        "structure": {
            "header": loaded.notebook.header.value if loaded.notebook.header else "",
            "app": loaded.notebook.app.options,
            "cells": [(c["name"], c["config"], c["kind"]) for c in cells],
        },
    }


def serve(arguments: list[str]) -> None:
    environment()
    manager = module("marimo._config.manager")
    original = manager.SecurityConfigManager.get_config

    def controlled_config(self: Any, *, hide_secrets: bool = True) -> dict[str, Any]:
        config = cast(dict[str, Any], original(self, hide_secrets=hide_secrets))
        config["runtime"] = {
            **config.get("runtime", {}),
            "auto_instantiate": False,
            "on_cell_change": "lazy",
            "auto_reload": "off",
            "auto_install": False,
            "cache_cells": False,
            "dotenv": [],
        }
        config["save"] = {**config.get("save", {}), "format_on_save": False}
        config["experimental"] = {**config.get("experimental", {}), "rtc_v2": False}
        return config

    manager.SecurityConfigManager.get_config = controlled_config
    module("marimo._cli.cli").main(args=arguments, prog_name="marimo")


def main() -> int:
    try:
        if sys.argv[1] == "serve":
            serve(sys.argv[2:])
        else:
            print(json.dumps(inspect_document(Path(sys.argv[2]))))
    except Exception as error:
        print(json.dumps({"error": f"{type(error).__name__}: {error}"}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Scratchpad-only code for the tested Marimo code-mode interface."""

from __future__ import annotations

import json
from textwrap import dedent

RESULT_PREFIX = "__RUNWATCH_MARIMO_RESULT__="

INVENTORY = dedent("""
import marimo._code_mode as cm
import hashlib as _rw_hashlib
import json as _rw_json
import os as _rw_os
import sys as _rw_sys
from pathlib import Path as _rw_Path
async with cm.get_context() as ctx:
    _rw_cells = []
    for _rw_cell in ctx.cells:
        _rw_errors = [{"type": type(e.exception).__name__ if e.exception is not None else "GraphError", "message": e.msg} for e in _rw_cell.errors]
        if _rw_cell.status == "exception" and str(_rw_cell.id) in getattr(__import__("marimo._runtime.control_flow", fromlist=["_"]), "_runwatch_stops", set()):
            _rw_errors = [{"type": "MarimoStopError", "message": "Cell intentionally stopped with mo.stop"}]
        _rw_cells.append({"id": str(_rw_cell.id), "name": _rw_cell.name, "code": _rw_cell.code, "status": "disabled" if _rw_cell.config.disabled else str(_rw_cell.status), "errors": _rw_errors, "parents": sorted(ctx.graph.parents.get(_rw_cell.id, ()))})
    _rw_modules = {}
    for _rw_mod in tuple(_rw_sys.modules.values()):
        _rw_file = getattr(_rw_mod, "__file__", None)
        if not isinstance(_rw_file, str) or not _rw_file.endswith(".py"):
            continue
        _rw_path = _rw_Path(_rw_file)
        if str(_rw_sys.base_prefix) in _rw_file or "__marimo__cell" in _rw_file:
            continue
        try:
            _rw_modules[str(_rw_path.resolve())] = _rw_hashlib.sha256(_rw_path.read_bytes()).hexdigest()
        except OSError:
            pass
    print("__RUNWATCH_MARIMO_RESULT__=" + _rw_json.dumps({"cells": _rw_cells, "modules": _rw_modules, "pid": _rw_os.getpid()}))
""")

CAPABILITIES = dedent("""
import marimo._code_mode as cm
assert callable(cm.get_context)
async with cm.get_context() as ctx:
    assert callable(ctx.run_cell) and callable(ctx.edit_cell)
    assert hasattr(ctx.graph, "parents") and hasattr(ctx.graph, "ancestors")
    assert ctx._kernel.reactive_execution_mode == "lazy"
    assert ctx._kernel.user_config["runtime"]["auto_reload"] == "off"
""")


def execute_cells(cell_ids: list[str], edits: dict[str, str] | None = None) -> str:
    return dedent(f"""
        import marimo._runtime.control_flow as _rw_flow
        getattr(_rw_flow, "_runwatch_stops", set()).difference_update({cell_ids!r})
        import marimo._code_mode as cm
        async with cm.get_context() as ctx:
            for _rw_id, _rw_code in {edits or {}!r}.items():
                _rw_previous = ctx.cells[_rw_id].code
                ctx.edit_cell(_rw_id, code=_rw_code)
            for _rw_id in {cell_ids!r}:
                ctx.run_cell(_rw_id)
    """)


def parse_result(stdout: str) -> dict[str, object]:
    for line in reversed(stdout.splitlines()):
        if line.startswith(RESULT_PREFIX):
            result: dict[str, object] = json.loads(line[len(RESULT_PREFIX) :])
            return result
    raise RuntimeError("Marimo inspection ended without a structured result")


def native_progress_bootstrap(min_interval: float) -> str:
    # Executed in an isolated namespace; no injected variables enter the graph.
    body = dedent("""
        import json, time, uuid
        from marimo._plugins.stateless.status._progress import _Progress
        if not getattr(_Progress, "_runwatch_patched", False):
            original_update = _Progress.update_progress
            original_init = _Progress.__init__
            def emit(bar, force=False):
                now = time.monotonic()
                last = getattr(bar, "_runwatch_last", 0)
                finished = bar.total is not None and bar.current >= bar.total
                if not force and not finished and now - last < MIN_INTERVAL:
                    return
                bar._runwatch_last = now
                payload = {"schema_version": 2, "event": "progress", "event_id": str(uuid.uuid4()), "completed": max(0, bar.current), "total": bar.total, "message": bar.title, "metrics": {"source": "marimo", "progress_id": bar._runwatch_id, "elapsed_seconds": max(0, time.time()-bar.start_time), "closed": bool(bar.closed or finished)}}
                print("__RUNWATCH_EVENT_JSON__=" + json.dumps({"mime_type": "application/vnd.runwatch.event+json", "payload": payload}))
            def init(self, *args, **kwargs):
                original_init(self, *args, **kwargs)
                self._runwatch_id = uuid.uuid4().hex
                emit(self, True)
            def update(self, *args, **kwargs):
                original_update(self, *args, **kwargs)
                try:
                    emit(self)
                except Exception:
                    pass
            _Progress.__init__ = init
            _Progress.update_progress = update
            _Progress._runwatch_patched = True
    """).replace("MIN_INTERVAL", repr(min_interval))
    return f"exec({body!r}, {{}})"


STOP_BOOTSTRAP = "exec(" + repr(dedent("""
import marimo._runtime.control_flow as _rw_flow
from marimo._runtime.context import get_context as _rw_context
if not hasattr(_rw_flow, "_runwatch_stops"):
    _rw_flow._runwatch_stops = set()
    _rw_original_stop = _rw_flow.MarimoStopError.__init__
    def _rw_stop(self, output):
        _rw_original_stop(self, output)
        _rw_flow._runwatch_stops.add(str(_rw_context().cell_id))
    _rw_flow.MarimoStopError.__init__ = _rw_stop
""")) + ", {})"

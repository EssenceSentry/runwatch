"""Run a disposable notebook twice to compare live repair and a fresh kernel."""

from __future__ import annotations

import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import nbformat


def paused(state: Path) -> bool:
    database = state / "runwatch.sqlite3"
    if not database.exists():
        return False
    with sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True) as connection:
        try:
            row = connection.execute("SELECT status FROM runs").fetchone()
        except sqlite3.OperationalError:
            return False
    return row is not None and row[0] == "paused"


def demonstrate(root: Path, mode: str) -> None:
    root.mkdir()
    counter = root / "executions.txt"
    state = root / "run"
    executable = shutil.which("runwatch")
    if executable is None:
        raise SystemExit("Install Runwatch with its supervisor extra first.")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = str(sock.getsockname()[1])
    env = dict(os.environ, RECOVERY_COUNTER=str(counter), RECOVERY_MODE=mode)
    notebook = root / "example.ipynb"
    shutil.copyfile(Path(__file__).with_name("example.ipynb"), notebook)
    config = root / "config.yaml"
    config.write_text(
        "schema_version: 2\nserver:\n  open_browser: false\n  linger_seconds: 0\n"
    )
    print(f"\n{'LIVE KERNEL' if mode == 'live' else 'LOST KERNEL'}", flush=True)
    print("$ runwatch execute example.ipynb --run-dir run", flush=True)
    with (root / "supervisor.log").open("w") as log:
        runner = subprocess.Popen(
            [
                executable,
                "execute",
                str(notebook),
                "--config",
                str(config),
                "--run-dir",
                str(state),
                "--port",
                port,
                "--keep-run",
            ],
            cwd=root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 120
            while not paused(state):
                if runner.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"Notebook never paused. See {log.name}")
                time.sleep(0.1)
            print(
                f"Preparation executions: {counter.read_text().count('prepare')}",
                flush=True,
            )
            checkpoint = nbformat.read(state / "executed.partial.ipynb", as_version=4)
            failure = checkpoint.cells[-1].outputs
            print(
                "Final cell paused:",
                next(
                    (o.ename for o in failure if o.output_type == "error"),
                    "kernel exited",
                ),
                flush=True,
            )
            changed = nbformat.read(state / "source.ipynb", as_version=4)
            changed.cells[-1].source = 'print("Result:", sum(batch))'
            nbformat.write(changed, state / "source.ipynb")
            print('Repair run/source.ipynb: print("Result:", sum(batch))', flush=True)
            action = "resume" if mode == "live" else "restart"
            print(f"$ runwatch {action} run", flush=True)
            subprocess.run(
                [executable, action, str(state)],
                check=True,
                cwd=root,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=60,
            )
            if runner.wait(timeout=120) != 0:
                raise RuntimeError(f"Recovery failed. See {log.name}")
            finished = nbformat.read(state / "executed.ipynb", as_version=4)
            result = finished.cells[-1].outputs[0].text.strip()
            count = counter.read_text().count("prepare")
            assert result == "Result: 499500" and count == (1 if mode == "live" else 2)
            print(result, flush=True)
            print(f"Preparation executions: {count}", flush=True)
        finally:
            if runner.poll() is None:
                runner.terminate()
                runner.wait(timeout=30)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        raise SystemExit("Run replay.py without arguments.")
    root = Path(tempfile.mkdtemp(prefix="runwatch-recovery-"))
    try:
        for scenario in ("live", "lost"):
            demonstrate(root / scenario, scenario)
    except Exception:
        print(f"Recovery artifacts retained in {root}", file=sys.stderr)
        raise
    else:
        shutil.rmtree(root)

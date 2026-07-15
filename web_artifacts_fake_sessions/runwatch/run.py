#!/usr/bin/env python3
"""Replay the local Runwatch notebook session."""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from socket import socket
from urllib.parse import quote
from uuid import uuid4

SESSION_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SESSION_ROOT.parents[1]
NOTEBOOK_PATH = SESSION_ROOT / "session.ipynb"
CONFIG_PATH = SESSION_ROOT / "runwatch.yaml"
LINKED_DASHBOARD_PATH = SESSION_ROOT / "linked_dashboard.html"
DEFAULT_NTFY_BASE_URL = "https://ntfy.sh"
NTFY_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def port_number(value: str) -> int:
    parsed = positive_int(value)
    if parsed > 65_535:
        raise argparse.ArgumentTypeError("must not exceed 65535")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


def ntfy_topic(value: str) -> str:
    if not NTFY_TOPIC_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "must contain only letters, numbers, hyphens, and underscores"
        )
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--share",
        choices=("none", "lan", "cloudflared"),
        default="lan",
        help="Dashboard sharing mode. Defaults to lan for phone monitoring.",
    )
    parser.add_argument("--host", help="Optional Runwatch server bind host.")
    parser.add_argument(
        "--port",
        type=port_number,
        help="Dashboard port. Defaults to an available local port.",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the dashboard in the default browser.",
    )
    parser.add_argument(
        "--qr",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print the dashboard pairing QR code.",
    )
    parser.add_argument(
        "--batches",
        type=positive_int,
        default=300,
        help="Number of simulated work batches. Defaults to 300.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=nonnegative_float,
        default=1.0,
        help="Delay between simulated work batches. Defaults to one second.",
    )
    parser.add_argument(
        "--name",
        default="runwatch-fake-session",
        help="Run name shown in the dashboard.",
    )
    parser.add_argument(
        "--ntfy",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable ntfy notifications and the dashboard app link.",
    )
    parser.add_argument(
        "--ntfy-base-url",
        default=os.environ.get("RUNWATCH_NTFY_BASE_URL", DEFAULT_NTFY_BASE_URL),
        help="ntfy server base URL. Defaults to https://ntfy.sh.",
    )
    parser.add_argument(
        "--ntfy-topic",
        type=ntfy_topic,
        default=os.environ.get("RUNWATCH_NTFY_TOPIC"),
        help="Private ntfy topic. Defaults to a new unguessable topic per replay.",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip Runwatch preflight validation before replaying.",
    )
    return parser.parse_args()


def run_replay(command: list[str], environment: dict[str, str]) -> int:
    """Run the replay in its own process group and forward terminal signals."""

    replay = subprocess.Popen(  # noqa: S603
        command,
        env=environment,
        start_new_session=True,
    )
    forwarded_signal: signal.Signals | None = None
    forwarded_at = 0.0
    force_stop = False
    previous_handlers: dict[signal.Signals, signal.Handlers] = {}

    def forward_signal(signum: int, _frame: object) -> None:
        nonlocal force_stop, forwarded_at, forwarded_signal
        received = signal.Signals(signum)
        if forwarded_signal is None:
            forwarded_signal = received
            forwarded_at = time.monotonic()
            with contextlib.suppress(ProcessLookupError):
                os.killpg(replay.pid, received)
            return
        force_stop = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)  # type: ignore
        signal.signal(signum, forward_signal)

    try:
        while True:
            try:
                return int(replay.wait(timeout=0.25))
            except subprocess.TimeoutExpired:
                if forwarded_signal is None:
                    continue
                if not force_stop and time.monotonic() - forwarded_at < 30:
                    continue
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(replay.pid, signal.SIGTERM)
                try:
                    replay.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(replay.pid, signal.SIGKILL)
                    replay.wait()
                return 128 + int(forwarded_signal)
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main() -> int:
    args = parse_args()
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("uv is required to replay the Runwatch fake session")

    runtime_root = SESSION_ROOT / ".runtime"
    working_dir = runtime_root / "workspace"
    runs_root = runtime_root / "runs"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    replay_id = uuid4().hex
    linked_dashboard_root = (
        working_dir / ".runwatch" / "linked-dashboard" / replay_id[:8]
    )
    working_dir.mkdir(parents=True, exist_ok=True)
    runs_root.mkdir(parents=True, exist_ok=True)
    linked_dashboard_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(LINKED_DASHBOARD_PATH, linked_dashboard_root / "index.html")

    replay_notebook_path = runtime_root / f"session-{replay_id[:8]}.ipynb"
    shutil.copyfile(NOTEBOOK_PATH, replay_notebook_path)
    run_dir = runs_root / f"{timestamp}-{replay_id[:8]}"
    base_command = [
        uv,
        "run",
        "--project",
        str(REPO_ROOT),
        "runwatch",
    ]
    environment = os.environ.copy()
    environment["RUNWATCH_SIMULATION_BATCHES"] = str(args.batches)
    environment["RUNWATCH_SIMULATION_DELAY_SECONDS"] = str(args.delay_seconds)
    environment["RUNWATCH_MASCOT_SHOWCASE"] = "1"
    linked_dashboard_port = available_port()
    environment["RUNWATCH_SIMULATION_DASHBOARD_URL"] = (
        f"http://127.0.0.1:{linked_dashboard_port}"
    )
    environment["RUNWATCH_SIMULATION_DASHBOARD_STATUS_PATH"] = str(
        linked_dashboard_root / "status.json"
    )
    if args.ntfy:
        topic = args.ntfy_topic or f"runwatch-{replay_id}"
        base_url = str(args.ntfy_base_url).rstrip("/")
        environment["RUNWATCH_NTFY_BASE_URL"] = base_url
        environment["RUNWATCH_NTFY_TOPIC"] = topic
        print(f"ntfy subscription: {base_url}/{quote(topic, safe='-_')}", flush=True)
    else:
        environment["RUNWATCH_NTFY_BASE_URL"] = ""
        environment["RUNWATCH_NTFY_TOPIC"] = ""
    linked_dashboard = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "http.server",
            str(linked_dashboard_port),
            "--bind",
            "127.0.0.1",
            "--directory",
            str(linked_dashboard_root),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=environment,
    )
    try:
        port = int(args.port) if args.port else available_port()

        if not args.skip_validate:
            validation = subprocess.run(  # noqa: S603
                [
                    *base_command,
                    "validate",
                    str(replay_notebook_path),
                    "--config",
                    str(CONFIG_PATH),
                    "--working-dir",
                    str(working_dir),
                ],
                check=False,
                env=environment,
            )
            if validation.returncode:
                return int(validation.returncode)

        command = [
            *base_command,
            "execute",
            str(replay_notebook_path),
            "--config",
            str(CONFIG_PATH),
            "--working-dir",
            str(working_dir),
            "--run-dir",
            str(run_dir),
            "--name",
            str(args.name),
            "--share",
            str(args.share),
            "--port",
            str(port),
            "--browser" if args.open else "--no-browser",
            "--qr" if args.qr else "--no-qr",
        ]
        if args.host:
            command.extend(("--host", str(args.host)))
        print(f"Replay runtime: {run_dir}", flush=True)
        print(
            "Linked dashboard: open it from the Runwatch resource card",
            flush=True,
        )
        return run_replay(command, environment)
    except KeyboardInterrupt:
        return 130
    finally:
        linked_dashboard.terminate()
        try:
            linked_dashboard.wait(timeout=5)
        except subprocess.TimeoutExpired:
            linked_dashboard.kill()
            linked_dashboard.wait()
        replay_notebook_path.unlink(missing_ok=True)
        shutil.rmtree(linked_dashboard_root, ignore_errors=True)


def available_port() -> int:
    with socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from collections.abc import Iterable
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psutil
from pydantic import BaseModel

from ._fs import PRIVATE_DIRECTORY_MODE, PRIVATE_FILE_MODE
from .models import (
    SCHEMA_VERSION,
    ActionKind,
    ActionStatus,
    CellStatus,
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceStatus,
    RunStatus,
    utc_now,
)


class UnsupportedRunSchema(RuntimeError):
    pass


class CorruptRunState(RuntimeError):
    pass


class ResourceEventConflict(RuntimeError):
    pass


_NOTIFICATION_EVENT_CURSOR_KEY = "_notification_event_cursor"
_NOTIFICATION_ROUTING_REQUIRED_KEY = "_notification_routing_required"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError as error:
        raise CorruptRunState("Persisted Runwatch JSON is corrupt") from error


def source_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RunStore:
    """Versioned SQLite state and action journal for one or more Runwatch runs."""

    def __init__(
        self,
        path: Path,
        *,
        max_observations_per_resource: int = 10_000,
        max_observation_bytes_per_resource: int = 8_388_608,
        max_log_lines_per_resource: int = 2_000,
        max_log_bytes_per_resource: int = 2_097_152,
        max_events_per_run: int = 10_000,
        max_event_bytes_per_run: int = 8_388_608,
        max_resource_payload_bytes: int = 2_097_152,
    ) -> None:
        try:
            path.parent.mkdir(parents=True, mode=PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        else:
            path.parent.chmod(PRIVATE_DIRECTORY_MODE)
        self._reserve_private_database(path)
        self.path = path
        self.max_observations_per_resource = max_observations_per_resource
        self.max_observation_bytes_per_resource = max_observation_bytes_per_resource
        self.max_log_lines_per_resource = max_log_lines_per_resource
        self.max_log_bytes_per_resource = max_log_bytes_per_resource
        self.max_events_per_run = max_events_per_run
        self.max_event_bytes_per_run = max_event_bytes_per_run
        self.max_resource_payload_bytes = max_resource_payload_bytes
        self._connection = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._initialize_or_validate_schema()

    @staticmethod
    def _reserve_private_database(path: Path) -> None:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        try:
            descriptor = os.open(path, flags, PRIVATE_FILE_MODE)
        except FileExistsError:
            path.chmod(PRIVATE_FILE_MODE)
        else:
            os.close(descriptor)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _initialize_or_validate_schema(self) -> None:
        tables = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        if not tables:
            self._create_schema()
            return
        if "runwatch_meta" not in tables:
            raise UnsupportedRunSchema(
                "This run directory uses the unsupported Runwatch 0.1 schema; "
                "start a new run with Runwatch schema version 2"
            )
        row = self._connection.execute(
            "SELECT value FROM runwatch_meta WHERE key = 'schema_version'"
        ).fetchone()
        version = int(row["value"]) if row else 0
        if version != SCHEMA_VERSION:
            raise UnsupportedRunSchema(
                f"Unsupported Runwatch schema version {version}; expected {SCHEMA_VERSION}"
            )
        required = {
            "runs",
            "cells",
            "resources",
            "resource_observations",
            "events",
            "actions",
            "notification_intents",
            "notification_deliveries",
        }
        missing = sorted(required - tables)
        if missing:
            raise CorruptRunState(
                "Runwatch database is missing required table(s): " + ", ".join(missing)
            )
        integrity = self._connection.execute("PRAGMA quick_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            detail = integrity[0] if integrity else "no result"
            raise CorruptRunState(f"Runwatch database integrity check failed: {detail}")

    def _create_schema(self) -> None:
        self._connection.executescript("""
            CREATE TABLE runwatch_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                notebook_path TEXT NOT NULL,
                source_path TEXT NOT NULL,
                output_path TEXT NOT NULL,
                working_dir TEXT NOT NULL,
                run_dir TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                current_cell_index INTEGER,
                failed_cell_index INTEGER,
                failed_attempt INTEGER,
                kernel_epoch INTEGER NOT NULL DEFAULT 0,
                kernel_id TEXT,
                kernel_pid INTEGER,
                process_pid INTEGER,
                process_started_at REAL,
                process_token TEXT,
                server_port INTEGER,
                source_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE cells (
                run_id TEXT NOT NULL,
                cell_index INTEGER NOT NULL,
                cell_id TEXT NOT NULL,
                cell_type TEXT NOT NULL,
                label TEXT,
                source TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                kernel_epoch INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                ended_at TEXT,
                elapsed_seconds REAL,
                error_name TEXT,
                error_value TEXT,
                traceback_json TEXT NOT NULL DEFAULT '[]',
                output_tail_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (run_id, cell_index),
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE resources (
                internal_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                logical_key TEXT,
                cell_index INTEGER,
                attempt INTEGER,
                kernel_epoch INTEGER,
                provider TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                external_id TEXT NOT NULL,
                region TEXT,
                account_id TEXT,
                ownership TEXT NOT NULL,
                lifecycle_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                cursor_json TEXT NOT NULL DEFAULT '{}',
                supports_stop INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                terminal INTEGER NOT NULL DEFAULT 0,
                monitor_closed INTEGER NOT NULL DEFAULT 0,
                disposition TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                message TEXT,
                metrics_json TEXT NOT NULL DEFAULT '{}',
                log_tail_json TEXT NOT NULL DEFAULT '[]',
                raw_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                terminal_at TEXT,
                UNIQUE (run_id, event_id),
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );

            CREATE UNIQUE INDEX idx_resources_logical
                ON resources(run_id, provider, resource_type, logical_key)
                WHERE logical_key IS NOT NULL AND disposition = 'active';
            CREATE INDEX idx_resources_attempt
                ON resources(run_id, cell_index, attempt, kernel_epoch);

            CREATE TABLE resource_observations (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                internal_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                metrics_json TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
                FOREIGN KEY (internal_id) REFERENCES resources(internal_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_observations_resource_seq
                ON resource_observations(internal_id, seq DESC);

            CREATE TABLE events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_events_run_seq ON events(run_id, seq DESC);

            CREATE TABLE actions (
                action_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                expected_kernel_epoch INTEGER,
                expected_cell_attempt INTEGER,
                expected_source_hash TEXT,
                requested_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                message TEXT,
                result_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_actions_run_status
                ON actions(run_id, status, requested_at);

            CREATE TABLE notification_intents (
                intent_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                dedup_key TEXT,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                data_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL,
                last_reported_status TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX idx_notification_intents_dedup
                ON notification_intents(run_id, dedup_key)
                WHERE dedup_key IS NOT NULL;
            CREATE INDEX idx_notification_intents_report
                ON notification_intents(run_id, status, last_reported_status);

            CREATE TABLE notification_deliveries (
                delivery_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                delivered_at TEXT,
                UNIQUE (intent_id, kind, destination),
                FOREIGN KEY (intent_id) REFERENCES notification_intents(intent_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            );
            CREATE INDEX idx_notification_deliveries_due
                ON notification_deliveries(run_id, status, next_attempt_at);
            """)
        self._connection.execute(
            "INSERT INTO runwatch_meta (key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        self._connection.commit()

    def initialize_run(
        self,
        *,
        run_id: str,
        name: str,
        notebook_path: Path,
        source_path: Path,
        output_path: Path,
        working_dir: Path,
        run_dir: Path,
        source_digest: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now().isoformat()
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO runs (
                    run_id, name, notebook_path, source_path, output_path, working_dir,
                    run_dir, status, source_hash, created_at, updated_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    name,
                    str(notebook_path),
                    str(source_path),
                    str(output_path),
                    str(working_dir),
                    str(run_dir),
                    RunStatus.CREATED.value,
                    source_digest,
                    now,
                    now,
                    json_dumps(metadata or {}),
                ),
            )
            self._connection.commit()

    def initialize_cells(self, run_id: str, cells: Iterable[dict[str, Any]]) -> None:
        rows = [
            (
                run_id,
                int(cell["cell_index"]),
                str(cell["cell_id"]),
                str(cell["cell_type"]),
                cell.get("label"),
                str(cell.get("source", "")),
                str(cell["source_hash"]),
                CellStatus.PENDING.value,
            )
            for cell in cells
        ]
        with self._lock:
            self._connection.executemany(
                """
                INSERT INTO cells (
                    run_id, cell_index, cell_id, cell_type, label, source, source_hash, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._connection.commit()

    def replace_cells_for_restart(
        self,
        run_id: str,
        cells: Iterable[dict[str, Any]],
        kernel_epoch: int,
        from_cell: int,
    ) -> None:
        rows = [
            (
                run_id,
                int(cell["cell_index"]),
                str(cell["cell_id"]),
                str(cell["cell_type"]),
                cell.get("label"),
                str(cell.get("source", "")),
                str(cell["source_hash"]),
                (
                    CellStatus.NOT_REPLAYED.value
                    if int(cell["cell_index"]) < from_cell
                    else CellStatus.PENDING.value
                ),
                kernel_epoch,
            )
            for cell in cells
        ]
        with self._lock:
            self._connection.execute("DELETE FROM cells WHERE run_id = ?", (run_id,))
            self._connection.executemany(
                """
                INSERT INTO cells (
                    run_id, cell_index, cell_id, cell_type, label, source, source_hash,
                    status, kernel_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._connection.commit()

    def get_run(self, run_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            if run_id is None:
                row = self._connection.execute(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
        if row is None:
            raise KeyError(run_id or "latest run")
        return self._decode_run(row)

    def update_run_status(
        self,
        run_id: str,
        status: RunStatus,
        *,
        message: str | None = None,
        current_cell_index: int | None | object = ...,
        failed_cell_index: int | None | object = ...,
        failed_attempt: int | None | object = ...,
        started: bool = False,
        ended: bool = False,
    ) -> None:
        fields = ["status = ?", "updated_at = ?"]
        values: list[Any] = [status.value, utc_now().isoformat()]
        if message is not None:
            fields.append("message = ?")
            values.append(message)
        for name, value in (
            ("current_cell_index", current_cell_index),
            ("failed_cell_index", failed_cell_index),
            ("failed_attempt", failed_attempt),
        ):
            if value is not ...:
                fields.append(f"{name} = ?")
                values.append(value)
        if started:
            fields.append("started_at = COALESCE(started_at, ?)")
            values.append(utc_now().isoformat())
        if ended:
            fields.append("ended_at = ?")
            values.append(utc_now().isoformat())
        values.append(run_id)
        with self._lock:
            self._connection.execute(
                f"UPDATE runs SET {', '.join(fields)} WHERE run_id = ?", values
            )
            self._connection.commit()

    def update_process(
        self,
        run_id: str,
        *,
        process_pid: int,
        server_port: int,
        process_started_at: float | None = None,
        process_token: str | None = None,
    ) -> None:
        started_at = process_started_at or process_start_time(process_pid)
        with self._lock:
            self._connection.execute(
                """
                UPDATE runs SET process_pid = ?, process_started_at = ?,
                    process_token = ?, server_port = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    process_pid,
                    started_at,
                    process_token,
                    server_port,
                    utc_now().isoformat(),
                    run_id,
                ),
            )
            self._connection.commit()

    def clear_process(self, run_id: str, *, process_token: str | None = None) -> None:
        values: list[Any] = [utc_now().isoformat(), run_id]
        token_clause = ""
        if process_token is not None:
            token_clause = " AND process_token = ?"
            values.append(process_token)
        with self._lock:
            self._connection.execute(
                """
                UPDATE runs SET process_pid = NULL, process_started_at = NULL,
                    process_token = NULL, server_port = NULL, updated_at = ?
                WHERE run_id = ?
                """ + token_clause,
                values,
            )
            self._connection.commit()

    def begin_kernel_epoch(self, run_id: str) -> int:
        with self._lock:
            self._connection.execute(
                "UPDATE runs SET kernel_epoch = kernel_epoch + 1, updated_at = ? WHERE run_id = ?",
                (utc_now().isoformat(), run_id),
            )
            row = self._connection.execute(
                "SELECT kernel_epoch FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            self._connection.commit()
        if row is None:
            raise KeyError(run_id)
        return int(row["kernel_epoch"])

    def update_kernel(
        self, run_id: str, *, kernel_id: str | None, kernel_pid: int | None
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE runs SET kernel_id = ?, kernel_pid = ?, updated_at = ? WHERE run_id = ?
                """,
                (kernel_id, kernel_pid, utc_now().isoformat(), run_id),
            )
            self._connection.commit()

    def update_source(
        self, run_id: str, digest: str, cells: Iterable[dict[str, Any]]
    ) -> None:
        with self._lock:
            for cell in cells:
                self._connection.execute(
                    """
                    UPDATE cells SET source = ?, source_hash = ?, label = ?
                    WHERE run_id = ? AND cell_index = ?
                    """,
                    (
                        str(cell["source"]),
                        str(cell["source_hash"]),
                        cell.get("label"),
                        run_id,
                        int(cell["cell_index"]),
                    ),
                )
            self._connection.execute(
                "UPDATE runs SET source_hash = ?, updated_at = ? WHERE run_id = ?",
                (digest, utc_now().isoformat(), run_id),
            )
            self._connection.commit()

    def reset_cells_for_restart(
        self, run_id: str, from_cell: int, kernel_epoch: int
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE cells
                SET status = CASE WHEN cell_index < ? THEN ? ELSE ? END,
                    kernel_epoch = ?, started_at = NULL, ended_at = NULL,
                    elapsed_seconds = NULL, error_name = NULL, error_value = NULL,
                    traceback_json = '[]', output_tail_json = '[]'
                WHERE run_id = ?
                """,
                (
                    from_cell,
                    CellStatus.NOT_REPLAYED.value,
                    CellStatus.PENDING.value,
                    kernel_epoch,
                    run_id,
                ),
            )
            self._connection.commit()

    def begin_cell_attempt(
        self,
        run_id: str,
        cell_index: int,
        source: str,
        source_digest: str,
        kernel_epoch: int,
    ) -> int:
        now = utc_now().isoformat()
        with self._lock:
            self._connection.execute(
                """
                UPDATE cells SET attempt = attempt + 1, source = ?, source_hash = ?, status = ?,
                    kernel_epoch = ?, started_at = ?, ended_at = NULL, elapsed_seconds = NULL,
                    error_name = NULL, error_value = NULL, traceback_json = '[]',
                    output_tail_json = '[]'
                WHERE run_id = ? AND cell_index = ?
                """,
                (
                    source,
                    source_digest,
                    CellStatus.RUNNING.value,
                    kernel_epoch,
                    now,
                    run_id,
                    cell_index,
                ),
            )
            row = self._connection.execute(
                "SELECT attempt FROM cells WHERE run_id = ? AND cell_index = ?",
                (run_id, cell_index),
            ).fetchone()
            self._connection.commit()
        if row is None:
            raise KeyError(f"Unknown cell {cell_index}")
        return int(row["attempt"])

    def mark_cell_skipped(self, run_id: str, cell_index: int) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE cells SET status = ?, ended_at = ?, elapsed_seconds = 0 WHERE run_id = ? AND cell_index = ?",
                (CellStatus.SKIPPED.value, utc_now().isoformat(), run_id, cell_index),
            )
            self._connection.commit()

    def complete_cell(
        self,
        run_id: str,
        cell_index: int,
        *,
        status: CellStatus,
        elapsed_seconds: float,
        error_name: str | None = None,
        error_value: str | None = None,
        traceback: list[str] | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE cells SET status = ?, ended_at = ?, elapsed_seconds = ?, error_name = ?,
                    error_value = ?, traceback_json = ?
                WHERE run_id = ? AND cell_index = ?
                """,
                (
                    status.value,
                    utc_now().isoformat(),
                    elapsed_seconds,
                    error_name,
                    error_value,
                    json_dumps(traceback or []),
                    run_id,
                    cell_index,
                ),
            )
            self._connection.commit()

    def append_cell_output(
        self,
        run_id: str,
        cell_index: int,
        output: dict[str, Any],
        *,
        max_items: int = 60,
    ) -> None:
        with self._lock:
            row = self._connection.execute(
                "SELECT output_tail_json FROM cells WHERE run_id = ? AND cell_index = ?",
                (run_id, cell_index),
            ).fetchone()
            if row is None:
                return
            values = json_loads(row["output_tail_json"], [])
            values.append(output)
            self._connection.execute(
                "UPDATE cells SET output_tail_json = ? WHERE run_id = ? AND cell_index = ?",
                (json_dumps(values[-max_items:]), run_id, cell_index),
            )
            self._connection.commit()

    def register_resource(
        self,
        *,
        run_id: str,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
    ) -> tuple[str, bool]:
        self._validate_resource_payload_size(
            {
                "lifecycle": event.lifecycle.model_dump(mode="json"),
                "metadata": event.resource.metadata,
            }
        )
        with self._lock:
            duplicate = self._connection.execute(
                "SELECT * FROM resources WHERE run_id = ? AND event_id = ?",
                (run_id, event.event_id),
            ).fetchone()
            if duplicate is not None:
                expected_identity = (
                    event.resource.provider,
                    event.resource.type,
                    event.resource.id,
                    event.resource.logical_key,
                    event.resource.region,
                    event.resource.account_id,
                    event.resource.ownership.value,
                    event.lifecycle.model_dump(mode="json"),
                    event.resource.metadata,
                )
                stored_identity = (
                    duplicate["provider"],
                    duplicate["resource_type"],
                    duplicate["external_id"],
                    duplicate["logical_key"],
                    duplicate["region"],
                    duplicate["account_id"],
                    duplicate["ownership"],
                    json_loads(duplicate["lifecycle_json"], {}),
                    json_loads(duplicate["metadata_json"], {}),
                )
                if stored_identity != expected_identity:
                    raise ResourceEventConflict(
                        f"Resource event {event.event_id} was reused with a different identity"
                    )
                return str(duplicate["internal_id"]), False
            try:
                if event.resource.logical_key:
                    existing = self._connection.execute(
                        """
                        SELECT * FROM resources
                        WHERE run_id = ? AND provider = ? AND resource_type = ?
                            AND logical_key = ? AND disposition = ?
                        """,
                        (
                            run_id,
                            event.resource.provider,
                            event.resource.type,
                            event.resource.logical_key,
                            ResourceDisposition.ACTIVE.value,
                        ),
                    ).fetchone()
                    if existing is not None:
                        if existing["external_id"] == event.resource.id:
                            self._refresh_reconciled_resource(
                                existing["internal_id"],
                                event=event,
                                cell_index=cell_index,
                                attempt=attempt,
                                kernel_epoch=kernel_epoch,
                                supports_stop=supports_stop,
                            )
                            self._connection.commit()
                            return str(existing["internal_id"]), False
                        self._connection.execute(
                            """
                            UPDATE resources
                            SET disposition = ?, monitor_closed = 1, updated_at = ?,
                                version = version + 1
                            WHERE internal_id = ?
                            """,
                            (
                                ResourceDisposition.SUPERSEDED.value,
                                utc_now().isoformat(),
                                existing["internal_id"],
                            ),
                        )
                internal_id = str(uuid4())
                now = utc_now().isoformat()
                self._connection.execute(
                    """
                    INSERT INTO resources (
                        internal_id, run_id, event_id, logical_key, cell_index, attempt,
                        kernel_epoch, provider, resource_type, external_id, region, account_id,
                        ownership, lifecycle_json, metadata_json, supports_stop, status,
                        disposition, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        internal_id,
                        run_id,
                        event.event_id,
                        event.resource.logical_key,
                        cell_index,
                        attempt,
                        kernel_epoch,
                        event.resource.provider,
                        event.resource.type,
                        event.resource.id,
                        event.resource.region,
                        event.resource.account_id,
                        event.resource.ownership.value,
                        json_dumps(event.lifecycle),
                        json_dumps(event.resource.metadata),
                        int(supports_stop),
                        ResourceStatus.REGISTERED.value,
                        ResourceDisposition.ACTIVE.value,
                        now,
                        now,
                    ),
                )
                self._connection.commit()
                return internal_id, True
            except Exception:
                self._connection.rollback()
                raise

    def _refresh_reconciled_resource(
        self,
        internal_id: str,
        *,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
    ) -> None:
        self._connection.execute(
            """
            UPDATE resources
            SET event_id = ?, cell_index = ?, attempt = ?, kernel_epoch = ?,
                region = ?, account_id = ?, ownership = ?, lifecycle_json = ?,
                metadata_json = ?, supports_stop = ?, updated_at = ?,
                version = version + 1
            WHERE internal_id = ?
            """,
            (
                event.event_id,
                cell_index,
                attempt,
                kernel_epoch,
                event.resource.region,
                event.resource.account_id,
                event.resource.ownership.value,
                json_dumps(event.lifecycle),
                json_dumps(event.resource.metadata),
                int(supports_stop),
                utc_now().isoformat(),
                internal_id,
            ),
        )

    def get_resource(self, internal_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM resources WHERE internal_id = ?", (internal_id,)
            ).fetchone()
        return self._decode_resource(row) if row else None

    def list_resources(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM resources WHERE run_id = ? ORDER BY created_at",
                (run_id,),
            ).fetchall()
        return [self._decode_resource(row) for row in rows]

    def resource_cursor(self, internal_id: str) -> dict[str, Any]:
        resource = self.get_resource(internal_id)
        return dict(resource.get("cursor", {})) if resource else {}

    def save_resource_cursor(self, internal_id: str, cursor: dict[str, Any]) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE resources SET cursor_json = ?, updated_at = ? WHERE internal_id = ?",
                (json_dumps(cursor), utc_now().isoformat(), internal_id),
            )
            self._connection.commit()

    def update_resource_observation(
        self, run_id: str, internal_id: str, observation: ResourceObservation
    ) -> None:
        self._update_resource_observation(
            run_id,
            internal_id,
            observation,
            cursor=None,
            terminal_disposition=None,
        )

    def record_resource_inspection(
        self,
        run_id: str,
        internal_id: str,
        observation: ResourceObservation,
        cursor: dict[str, Any],
    ) -> None:
        """Atomically commit an adapter cursor and everything it observed."""

        self._update_resource_observation(
            run_id,
            internal_id,
            observation,
            cursor=cursor,
            terminal_disposition=None,
        )

    def record_resource_stop_inspection(
        self,
        run_id: str,
        internal_id: str,
        observation: ResourceObservation,
        cursor: dict[str, Any],
        disposition: ResourceDisposition,
    ) -> None:
        """Atomically commit stop inspection state and its terminal disposition."""

        self._update_resource_observation(
            run_id,
            internal_id,
            observation,
            cursor=cursor,
            terminal_disposition=disposition,
        )

    def _update_resource_observation(
        self,
        run_id: str,
        internal_id: str,
        observation: ResourceObservation,
        *,
        cursor: dict[str, Any] | None,
        terminal_disposition: ResourceDisposition | None,
    ) -> None:
        now = utc_now().isoformat()
        with self._lock:
            try:
                row = self._resource_persistence_row(internal_id)
                if row is None:
                    return
                log_tail = self._updated_log_tail(row, observation)
                cursor_json = (
                    row["cursor_json"] if cursor is None else json_dumps(cursor)
                )
                self._validate_resource_payload_size(
                    {
                        "cursor": json_loads(cursor_json, {}),
                        "metrics": observation.metrics,
                        "raw": observation.raw,
                    }
                )
                self._update_resource_from_observation(
                    internal_id,
                    observation,
                    cursor_json=cursor_json,
                    log_tail=log_tail,
                    now=now,
                    terminal_disposition=terminal_disposition,
                )
                self._insert_resource_observation(
                    run_id, internal_id, observation, now=now
                )
                self._prune_resource_observations(internal_id)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    def _resource_persistence_row(self, internal_id: str) -> sqlite3.Row | None:
        return self._connection.execute(
            """
            SELECT cursor_json, log_tail_json FROM resources WHERE internal_id = ?
            """,
            (internal_id,),
        ).fetchone()

    def _updated_log_tail(
        self, row: sqlite3.Row, observation: ResourceObservation
    ) -> list[str]:
        log_tail = json_loads(row["log_tail_json"], [])
        log_tail.extend(observation.log_lines)
        return self._bounded_log_tail(log_tail)

    def _bounded_log_tail(self, lines: list[str]) -> list[str]:
        selected: list[str] = []
        used = 0
        for line in reversed(lines[-self.max_log_lines_per_resource :]):
            encoded = line.encode("utf-8")
            remaining = self.max_log_bytes_per_resource - used
            if remaining <= 0:
                break
            if len(encoded) > remaining:
                if not selected:
                    selected.append(
                        encoded[-remaining:].decode("utf-8", errors="replace")
                    )
                break
            selected.append(line)
            used += len(encoded)
        return list(reversed(selected))

    def _validate_resource_payload_size(self, value: Any) -> None:
        size = len(json_dumps(value).encode("utf-8"))
        if size > self.max_resource_payload_bytes:
            raise ValueError(
                "Resource payload exceeds storage.max_resource_payload_bytes "
                f"({size} > {self.max_resource_payload_bytes})"
            )

    def _update_resource_from_observation(
        self,
        internal_id: str,
        observation: ResourceObservation,
        *,
        cursor_json: str,
        log_tail: list[str],
        now: str,
        terminal_disposition: ResourceDisposition | None,
    ) -> None:
        apply_disposition = int(
            observation.terminal and terminal_disposition is not None
        )
        disposition = (
            terminal_disposition.value
            if terminal_disposition is not None
            else ResourceDisposition.ACTIVE.value
        )
        self._connection.execute(
            """
            UPDATE resources SET cursor_json = ?, status = ?, terminal = ?, message = ?,
                metrics_json = ?, log_tail_json = ?, raw_json = ?, updated_at = ?,
                terminal_at = CASE WHEN ? THEN COALESCE(terminal_at, ?) ELSE terminal_at END,
                disposition = CASE WHEN ? THEN ? ELSE disposition END,
                monitor_closed = CASE WHEN ? THEN 1 ELSE monitor_closed END,
                version = version + CASE
                    WHEN status != ? OR terminal != ?
                        OR (? AND disposition != ?) THEN 1 ELSE 0 END
            WHERE internal_id = ?
            """,
            (
                cursor_json,
                observation.status.value,
                int(observation.terminal),
                observation.message,
                json_dumps(observation.metrics),
                json_dumps(log_tail),
                json_dumps(observation.raw),
                now,
                int(observation.terminal),
                now,
                apply_disposition,
                disposition,
                apply_disposition,
                observation.status.value,
                int(observation.terminal),
                apply_disposition,
                disposition,
                internal_id,
            ),
        )

    def _insert_resource_observation(
        self,
        run_id: str,
        internal_id: str,
        observation: ResourceObservation,
        *,
        now: str,
    ) -> None:
        history_metrics = (
            observation.metrics
            if observation.history_metrics is None
            else observation.history_metrics
        )
        self._connection.execute(
            """
            INSERT INTO resource_observations
                (run_id, internal_id, timestamp, status, message, metrics_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                internal_id,
                now,
                observation.status.value,
                observation.message,
                json_dumps(history_metrics),
            ),
        )

    def _prune_resource_observations(self, internal_id: str) -> None:
        self._connection.execute(
            """
            DELETE FROM resource_observations
            WHERE internal_id = ? AND seq NOT IN (
                SELECT seq FROM (
                    SELECT seq,
                        ROW_NUMBER() OVER (ORDER BY seq DESC) AS row_number,
                        SUM(
                            LENGTH(CAST(metrics_json AS BLOB))
                            + LENGTH(CAST(COALESCE(message, '') AS BLOB))
                        ) OVER (ORDER BY seq DESC) AS cumulative_bytes
                    FROM resource_observations WHERE internal_id = ?
                )
                WHERE row_number = 1 OR (
                    row_number <= ? AND cumulative_bytes <= ?
                )
            )
            """,
            (
                internal_id,
                internal_id,
                self.max_observations_per_resource,
                self.max_observation_bytes_per_resource,
            ),
        )

    def resource_observations(
        self, internal_id: str, limit: int = 300
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM resource_observations WHERE internal_id = ?
                ORDER BY seq DESC LIMIT ?
                """,
                (internal_id, limit),
            ).fetchall()
        values = [
            {
                "seq": int(row["seq"]),
                "timestamp": row["timestamp"],
                "status": row["status"],
                "message": row["message"],
                "metrics": json_loads(row["metrics_json"], {}),
            }
            for row in reversed(rows)
        ]
        return values

    def downsampled_resource_observations(
        self, internal_id: str, max_points: int
    ) -> list[dict[str, Any]]:
        """Return a bounded, evenly spaced series including first and last points."""

        if max_points < 2:
            raise ValueError("max_points must be at least 2")
        with self._lock:
            count_row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM resource_observations WHERE internal_id = ?",
                (internal_id,),
            ).fetchone()
            count = int(count_row["count"]) if count_row else 0
            if count <= max_points:
                rows = self._connection.execute(
                    """
                    SELECT * FROM resource_observations WHERE internal_id = ?
                    ORDER BY seq
                    """,
                    (internal_id,),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    """
                    WITH RECURSIVE sample(point) AS (
                        SELECT 0
                        UNION ALL
                        SELECT point + 1 FROM sample WHERE point + 1 < ?
                    ), ranked AS (
                        SELECT *, ROW_NUMBER() OVER (ORDER BY seq) AS point_number,
                            COUNT(*) OVER () AS point_count
                        FROM resource_observations WHERE internal_id = ?
                    )
                    SELECT ranked.* FROM ranked JOIN sample
                    ON ranked.point_number = 1 + CAST(
                        sample.point * (ranked.point_count - 1) / (? - 1) AS INTEGER
                    )
                    ORDER BY ranked.seq
                    """,
                    (max_points, internal_id, max_points),
                ).fetchall()
        return [
            {
                "seq": int(row["seq"]),
                "timestamp": row["timestamp"],
                "status": row["status"],
                "message": row["message"],
                "metrics": json_loads(row["metrics_json"], {}),
            }
            for row in rows
        ]

    def downsampled_run_resource_observations(
        self, run_id: str, max_points: int
    ) -> dict[str, list[dict[str, Any]]]:
        """Return bounded observation histories for every resource in one query."""

        if max_points < 2:
            raise ValueError("max_points must be at least 2")
        sample_values = ", ".join(f"({point})" for point in range(max_points))
        query = f"""
            WITH sample(point) AS (VALUES {sample_values}),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY internal_id ORDER BY seq
                    ) AS point_number,
                    COUNT(*) OVER (PARTITION BY internal_id) AS point_count
                FROM resource_observations
                WHERE run_id = ?
            ), selected AS (
                SELECT * FROM ranked WHERE point_count <= ?
                UNION ALL
                SELECT ranked.* FROM ranked JOIN sample
                    ON ranked.point_number = 1 + CAST(
                        sample.point * (ranked.point_count - 1) / (? - 1)
                        AS INTEGER
                    )
                WHERE ranked.point_count > ?
            )
            SELECT * FROM selected ORDER BY internal_id, seq
        """
        with self._lock:
            rows = self._connection.execute(
                query, (run_id, max_points, max_points, max_points)
            ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["internal_id"]), []).append(
                {
                    "seq": int(row["seq"]),
                    "timestamp": row["timestamp"],
                    "status": row["status"],
                    "message": row["message"],
                    "metrics": json_loads(row["metrics_json"], {}),
                }
            )
        return grouped

    def set_resource_status(
        self, internal_id: str, status: ResourceStatus, *, message: str | None = None
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE resources SET status = ?, message = COALESCE(?, message),
                    updated_at = ?, version = version + CASE WHEN status != ? THEN 1 ELSE 0 END
                WHERE internal_id = ?
                """,
                (
                    status.value,
                    message,
                    utc_now().isoformat(),
                    status.value,
                    internal_id,
                ),
            )
            self._connection.commit()

    def request_resource_stop(
        self, internal_id: str, disposition: ResourceDisposition
    ) -> dict[str, Any]:
        """Persist stop intent, atomically confirming a resource already terminal."""

        now = utc_now().isoformat()
        with self._lock:
            try:
                self._connection.execute(
                    """
                    UPDATE resources
                    SET status = CASE WHEN terminal THEN status ELSE ? END,
                        disposition = CASE WHEN terminal THEN ? ELSE disposition END,
                        monitor_closed = CASE WHEN terminal THEN 1 ELSE monitor_closed END,
                        updated_at = ?,
                        version = version + CASE
                            WHEN (NOT terminal AND status != ?)
                                OR (terminal AND disposition != ?) THEN 1 ELSE 0 END
                    WHERE internal_id = ?
                    """,
                    (
                        ResourceStatus.STOPPING.value,
                        disposition.value,
                        now,
                        ResourceStatus.STOPPING.value,
                        disposition.value,
                        internal_id,
                    ),
                )
                row = self._connection.execute(
                    "SELECT * FROM resources WHERE internal_id = ?", (internal_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(internal_id)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return self._decode_resource(row)

    def set_resource_disposition(
        self, internal_id: str, disposition: ResourceDisposition
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE resources SET disposition = ?, updated_at = ?,
                    version = version + CASE WHEN disposition != ? THEN 1 ELSE 0 END
                WHERE internal_id = ?
                """,
                (
                    disposition.value,
                    utc_now().isoformat(),
                    disposition.value,
                    internal_id,
                ),
            )
            self._connection.commit()

    def mark_resource_monitor_closed(self, internal_id: str) -> None:
        with self._lock:
            self._connection.execute(
                "UPDATE resources SET monitor_closed = 1, updated_at = ? WHERE internal_id = ?",
                (utc_now().isoformat(), internal_id),
            )
            self._connection.commit()

    def create_action(
        self,
        run_id: str,
        kind: ActionKind,
        *,
        payload: dict[str, Any] | None = None,
        expected_kernel_epoch: int | None = None,
        expected_cell_attempt: int | None = None,
        expected_source_hash: str | None = None,
    ) -> str:
        action_id = str(uuid4())
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO actions (
                    action_id, run_id, kind, status, payload_json, expected_kernel_epoch,
                    expected_cell_attempt, expected_source_hash, requested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    action_id,
                    run_id,
                    kind.value,
                    ActionStatus.REQUESTED.value,
                    json_dumps(payload or {}),
                    expected_kernel_epoch,
                    expected_cell_attempt,
                    expected_source_hash,
                    utc_now().isoformat(),
                ),
            )
            self._connection.commit()
        return action_id

    def claim_next_action(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM actions WHERE run_id = ? AND status = ?
                ORDER BY requested_at LIMIT 1
                """,
                (run_id, ActionStatus.REQUESTED.value),
            ).fetchone()
            if row is None:
                return None
            updated = self._connection.execute(
                """
                UPDATE actions SET status = ?, started_at = ?
                WHERE action_id = ? AND status = ?
                """,
                (
                    ActionStatus.EXECUTING.value,
                    utc_now().isoformat(),
                    row["action_id"],
                    ActionStatus.REQUESTED.value,
                ),
            )
            self._connection.commit()
            if updated.rowcount != 1:
                return None
            claimed = self._connection.execute(
                "SELECT * FROM actions WHERE action_id = ?", (row["action_id"],)
            ).fetchone()
        return self._decode_action(claimed) if claimed else None

    def claim_action(self, action_id: str) -> dict[str, Any] | None:
        """Atomically claim one known requested action."""

        with self._lock:
            updated = self._connection.execute(
                """
                UPDATE actions SET status = ?, started_at = ?
                WHERE action_id = ? AND status = ?
                """,
                (
                    ActionStatus.EXECUTING.value,
                    utc_now().isoformat(),
                    action_id,
                    ActionStatus.REQUESTED.value,
                ),
            )
            self._connection.commit()
            if updated.rowcount != 1:
                return None
            row = self._connection.execute(
                "SELECT * FROM actions WHERE action_id = ?", (action_id,)
            ).fetchone()
        return self._decode_action(row) if row else None

    def recover_incomplete_actions(self, run_id: str) -> int:
        """Return crash-interrupted actions to the durable request queue."""

        recovered = 0
        with self._lock:
            rows = self._connection.execute(
                "SELECT action_id, payload_json FROM actions WHERE run_id = ? AND status = ?",
                (run_id, ActionStatus.EXECUTING.value),
            ).fetchall()
            try:
                for row in rows:
                    payload = dict(json_loads(row["payload_json"], {}))
                    payload["recovered"] = True
                    self._connection.execute(
                        """
                        UPDATE actions SET status = ?, started_at = NULL, payload_json = ?,
                            message = 'Recovered after the previous Runwatch process stopped'
                        WHERE action_id = ? AND status = ?
                        """,
                        (
                            ActionStatus.REQUESTED.value,
                            json_dumps(payload),
                            row["action_id"],
                            ActionStatus.EXECUTING.value,
                        ),
                    )
                    recovered += 1
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return recovered

    def finish_action(
        self,
        action_id: str,
        status: ActionStatus,
        *,
        message: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        if not status.terminal:
            raise ValueError("finish_action requires a terminal action status")
        with self._lock:
            updated = self._connection.execute(
                """
                UPDATE actions SET status = ?, message = ?, result_json = ?, finished_at = ?
                WHERE action_id = ? AND status IN (?, ?)
                """,
                (
                    status.value,
                    message,
                    json_dumps(result or {}),
                    utc_now().isoformat(),
                    action_id,
                    ActionStatus.REQUESTED.value,
                    ActionStatus.EXECUTING.value,
                ),
            )
            self._connection.commit()
            if updated.rowcount != 1:
                existing = self._connection.execute(
                    "SELECT status FROM actions WHERE action_id = ?", (action_id,)
                ).fetchone()
                if existing is None:
                    raise KeyError(action_id)
                if existing["status"] != status.value:
                    raise RuntimeError(
                        f"Action {action_id} is already {existing['status']}"
                    )

    def get_action(self, action_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM actions WHERE action_id = ?", (action_id,)
            ).fetchone()
        return self._decode_action(row) if row else None

    def list_actions(self, run_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM actions WHERE run_id = ? ORDER BY requested_at DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [self._decode_action(row) for row in rows]

    def append_event(
        self, run_id: str, event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp = utc_now().isoformat()
        with self._lock:
            cursor = self._connection.execute(
                "INSERT INTO events (run_id, timestamp, type, payload_json) VALUES (?, ?, ?, ?)",
                (run_id, timestamp, event_type, json_dumps(payload)),
            )
            self._prune_events(run_id)
            self._connection.commit()
        sequence = cursor.lastrowid
        if sequence is None:
            raise RuntimeError("SQLite did not return an event sequence")
        return {
            "seq": int(sequence),
            "run_id": run_id,
            "timestamp": timestamp,
            "type": event_type,
            "payload": payload,
        }

    def _prune_events(self, run_id: str) -> None:
        protected_after = self._notification_pruning_cursor(run_id)
        self._connection.execute(
            """
            DELETE FROM events
            WHERE run_id = ?
              AND (? IS NULL OR seq <= ?)
              AND seq NOT IN (
                SELECT seq FROM (
                    SELECT seq,
                        ROW_NUMBER() OVER (ORDER BY seq DESC) AS row_number,
                        SUM(
                            LENGTH(CAST(payload_json AS BLOB))
                            + LENGTH(CAST(type AS BLOB))
                        ) OVER (ORDER BY seq DESC) AS cumulative_bytes
                    FROM events WHERE run_id = ?
                )
                WHERE row_number = 1 OR (
                    row_number <= ? AND cumulative_bytes <= ?
                )
            )
            """,
            (
                run_id,
                protected_after,
                protected_after,
                run_id,
                self.max_events_per_run,
                self.max_event_bytes_per_run,
            ),
        )

    def _notification_pruning_cursor(self, run_id: str) -> int | None:
        row = self._connection.execute(
            "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run {run_id}")
        metadata, cursor = self._notification_metadata(row)
        required = metadata.get(_NOTIFICATION_ROUTING_REQUIRED_KEY, False)
        if not isinstance(required, bool):
            raise CorruptRunState("Notification routing requirement is invalid")
        return cursor if required else None

    def recent_events(self, run_id: str, limit: int = 120) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM events WHERE run_id = ? ORDER BY seq DESC LIMIT ?",
                (run_id, limit),
            ).fetchall()
        return [
            {
                "seq": int(row["seq"]),
                "run_id": row["run_id"],
                "timestamp": row["timestamp"],
                "type": row["type"],
                "payload": json_loads(row["payload_json"], {}),
            }
            for row in reversed(rows)
        ]

    def events_after(
        self, run_id: str, sequence: int, *, limit: int = 1_000
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM events WHERE run_id = ? AND seq > ?
                ORDER BY seq LIMIT ?
                """,
                (run_id, sequence, limit),
            ).fetchall()
        return [
            {
                "seq": int(row["seq"]),
                "run_id": row["run_id"],
                "timestamp": row["timestamp"],
                "type": row["type"],
                "payload": json_loads(row["payload_json"], {}),
            }
            for row in rows
        ]

    def notification_event_cursor(self, run_id: str) -> int:
        """Return the last durable event consumed by notification routing."""

        with self._lock:
            row = self._connection.execute(
                "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run {run_id}")
        _metadata, cursor = self._notification_metadata(row)
        return cursor

    def require_notification_event_routing(self, run_id: str) -> bool:
        """Protect unconsumed events from retention pruning for this run."""

        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"Unknown run {run_id}")
                metadata, _cursor = self._notification_metadata(row)
                current = metadata.get(_NOTIFICATION_ROUTING_REQUIRED_KEY, False)
                if not isinstance(current, bool):
                    raise CorruptRunState("Notification routing requirement is invalid")
                if current:
                    self._connection.commit()
                    return False
                metadata[_NOTIFICATION_ROUTING_REQUIRED_KEY] = True
                self._update_run_metadata(run_id, metadata)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return True

    @staticmethod
    def _notification_metadata(
        row: sqlite3.Row,
    ) -> tuple[dict[str, Any], int]:
        metadata_value = json_loads(row["metadata_json"], {})
        if not isinstance(metadata_value, dict):
            raise CorruptRunState("Run metadata must be a JSON object")
        metadata = cast(dict[str, Any], metadata_value)
        value = metadata.get(_NOTIFICATION_EVENT_CURSOR_KEY, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CorruptRunState("Notification event cursor is invalid")
        return metadata, value

    def normalize_notification_event_cursor(self, run_id: str) -> tuple[int, bool]:
        """Clamp an impossible future notification cursor to the event high-water."""

        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"Unknown run {run_id}")
                metadata, current = self._notification_metadata(row)
                maximum_row = self._connection.execute(
                    "SELECT MAX(seq) AS maximum FROM events WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                maximum = int(maximum_row["maximum"] or 0)
                if current <= maximum:
                    self._connection.commit()
                    return current, False
                metadata[_NOTIFICATION_EVENT_CURSOR_KEY] = maximum
                self._update_run_metadata(run_id, metadata)
                self._connection.commit()
                return maximum, True
            except Exception:
                self._connection.rollback()
                raise

    def advance_notification_event_cursor(self, run_id: str, sequence: object) -> bool:
        """Monotonically record a durable event consumed by notification routing."""

        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ValueError("Notification event sequence must be nonnegative")
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"Unknown run {run_id}")
                metadata, current = self._notification_metadata(row)
                if sequence <= current:
                    self._connection.commit()
                    return False
                metadata[_NOTIFICATION_EVENT_CURSOR_KEY] = sequence
                self._update_run_metadata(run_id, metadata)
                self._prune_events(run_id)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return True

    def _update_run_metadata(self, run_id: str, metadata: dict[str, Any]) -> None:
        self._connection.execute(
            "UPDATE runs SET metadata_json = ?, updated_at = ? WHERE run_id = ?",
            (json_dumps(metadata), utc_now().isoformat(), run_id),
        )

    def enqueue_notification(
        self,
        *,
        run_id: str,
        title: str,
        message: str,
        data: dict[str, Any],
        dedup_key: str | None,
        destinations: Iterable[tuple[str, str]],
    ) -> dict[str, Any]:
        """Persist one notification intent and its independent destinations.

        A successful deduplicated intent is immutable. A terminally failed intent is
        rearmed only when the caller explicitly enqueues that deduplication key again.
        """

        unique_destinations = list(dict.fromkeys(destinations))
        if not unique_destinations:
            raise ValueError("A notification requires at least one destination")
        now = utc_now().isoformat()
        created = False
        rearmed = False
        with self._lock:
            try:
                row = None
                if dedup_key is not None:
                    row = self._connection.execute(
                        """
                        SELECT * FROM notification_intents
                        WHERE run_id = ? AND dedup_key = ?
                        """,
                        (run_id, dedup_key),
                    ).fetchone()
                if row is not None and row["status"] == "succeeded":
                    result = self._decode_notification_intent(row)
                    result.update({"created": False, "rearmed": False})
                    return result
                if row is None:
                    intent_id = str(uuid4())
                    self._connection.execute(
                        """
                        INSERT INTO notification_intents (
                            intent_id, run_id, dedup_key, title, message, data_json,
                            status, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                        """,
                        (
                            intent_id,
                            run_id,
                            dedup_key,
                            title,
                            message,
                            json_dumps(data),
                            now,
                            now,
                        ),
                    )
                    created = True
                else:
                    intent_id = str(row["intent_id"])
                    if row["status"] == "failed":
                        self._connection.execute(
                            """
                            UPDATE notification_intents
                            SET title = ?, message = ?, data_json = ?, status = 'pending',
                                last_reported_status = NULL, completed_at = NULL,
                                updated_at = ?
                            WHERE intent_id = ?
                            """,
                            (title, message, json_dumps(data), now, intent_id),
                        )
                        self._connection.execute(
                            """
                            UPDATE notification_deliveries
                            SET status = 'pending', attempt_count = 0,
                                next_attempt_at = ?, last_error = NULL,
                                delivered_at = NULL, updated_at = ?
                            WHERE intent_id = ? AND status = 'failed'
                            """,
                            (now, now, intent_id),
                        )
                        rearmed = True
                for kind, destination in unique_destinations:
                    self._connection.execute(
                        """
                        INSERT OR IGNORE INTO notification_deliveries (
                            delivery_id, intent_id, run_id, kind, destination, status,
                            next_attempt_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                        """,
                        (
                            str(uuid4()),
                            intent_id,
                            run_id,
                            kind,
                            destination,
                            now,
                            now,
                            now,
                        ),
                    )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            stored = self._connection.execute(
                "SELECT * FROM notification_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
        if stored is None:
            raise RuntimeError("Notification intent disappeared after it was enqueued")
        result = self._decode_notification_intent(stored)
        result.update({"created": created, "rearmed": rearmed})
        return result

    def recover_notification_deliveries(self, run_id: str) -> int:
        """Make crash-interrupted deliveries eligible for another attempt."""

        now = utc_now().isoformat()
        with self._lock:
            try:
                intent_rows = self._connection.execute(
                    """
                    SELECT DISTINCT intent_id FROM notification_deliveries
                    WHERE run_id = ? AND status = 'sending'
                    """,
                    (run_id,),
                ).fetchall()
                updated = self._connection.execute(
                    """
                    UPDATE notification_deliveries
                    SET status = 'pending',
                        attempt_count = CASE
                            WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END,
                        next_attempt_at = ?, updated_at = ?
                    WHERE run_id = ? AND status = 'sending'
                    """,
                    (now, now, run_id),
                )
                for intent_row in intent_rows:
                    self._connection.execute(
                        """
                        UPDATE notification_intents
                        SET status = 'pending', completed_at = NULL, updated_at = ?
                        WHERE intent_id = ?
                        """,
                        (now, intent_row["intent_id"]),
                    )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return int(updated.rowcount)

    def recover_claimed_notification_delivery(
        self,
        delivery_id: str,
        *,
        max_attempts: int,
        retry_delay_seconds: float,
        error: str,
    ) -> str | None:
        """Recover one delivery whose worker failed after claiming it.

        The attempt remains counted because the destination may already have accepted the
        request. Stable idempotency headers make a bounded retry safe for destinations that
        honor them. Once the configured attempt limit is reached, the claim is failed
        terminally instead of remaining stuck in ``sending``.
        """

        now_value = utc_now()
        now = now_value.isoformat()
        with self._lock:
            try:
                delivery = self._connection.execute(
                    "SELECT * FROM notification_deliveries WHERE delivery_id = ?",
                    (delivery_id,),
                ).fetchone()
                if delivery is None:
                    raise KeyError(f"Unknown notification delivery {delivery_id}")
                if delivery["status"] != "sending":
                    return None
                intent_id = str(delivery["intent_id"])
                if int(delivery["attempt_count"]) >= max_attempts:
                    status = "failed"
                    next_attempt = str(delivery["next_attempt_at"])
                else:
                    status = "pending"
                    next_attempt = (
                        now_value + timedelta(seconds=retry_delay_seconds)
                    ).isoformat()
                self._connection.execute(
                    """
                    UPDATE notification_deliveries
                    SET status = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
                    WHERE delivery_id = ? AND status = 'sending'
                    """,
                    (status, error, next_attempt, now, delivery_id),
                )
                intent_status, completed_at = self._notification_intent_state(
                    intent_id, now
                )
                self._connection.execute(
                    """
                    UPDATE notification_intents
                    SET status = ?, completed_at = ?, updated_at = ?
                    WHERE intent_id = ?
                    """,
                    (intent_status, completed_at, now, intent_id),
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return status

    def notification_outbox_state(self, run_id: str) -> dict[str, int]:
        """Return bounded cleanup-relevant counts for a run's durable outbox."""

        with self._lock:
            intent = self._connection.execute(
                """
                SELECT COUNT(*) AS count FROM notification_intents
                WHERE run_id = ? AND status IN ('pending', 'partial')
                """,
                (run_id,),
            ).fetchone()
            deliveries = self._connection.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 'sending' THEN 1 ELSE 0 END) AS sending
                FROM notification_deliveries
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        pending = int(deliveries["pending"] or 0) if deliveries is not None else 0
        sending = int(deliveries["sending"] or 0) if deliveries is not None else 0
        return {
            "nonterminal_intents": int(intent["count"] if intent is not None else 0),
            "nonterminal_deliveries": pending + sending,
            "pending_deliveries": pending,
            "sending_deliveries": sending,
        }

    def claim_due_notification_deliveries(
        self, run_id: str, *, limit: int = 32
    ) -> list[dict[str, Any]]:
        now = utc_now().isoformat()
        claimed: list[sqlite3.Row] = []
        with self._lock:
            try:
                rows = self._connection.execute(
                    """
                    SELECT delivery_id FROM notification_deliveries
                    WHERE run_id = ? AND status = 'pending' AND next_attempt_at <= ?
                    ORDER BY next_attempt_at, created_at LIMIT ?
                    """,
                    (run_id, now, limit),
                ).fetchall()
                for row in rows:
                    updated = self._connection.execute(
                        """
                        UPDATE notification_deliveries
                        SET status = 'sending', attempt_count = attempt_count + 1,
                            updated_at = ?
                        WHERE delivery_id = ? AND status = 'pending'
                        """,
                        (now, row["delivery_id"]),
                    )
                    if updated.rowcount == 1:
                        delivery = self._connection.execute(
                            """
                            SELECT d.*, i.title, i.message, i.data_json
                            FROM notification_deliveries AS d
                            JOIN notification_intents AS i ON i.intent_id = d.intent_id
                            WHERE d.delivery_id = ?
                            """,
                            (row["delivery_id"],),
                        ).fetchone()
                        if delivery is not None:
                            claimed.append(delivery)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return [self._decode_notification_delivery(row) for row in claimed]

    def finish_notification_delivery(
        self,
        delivery_id: str,
        *,
        succeeded: bool,
        max_attempts: int,
        retry_delay_seconds: float,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Record one attempt and recompute its notification intent state."""

        now_value = utc_now()
        now = now_value.isoformat()
        with self._lock:
            try:
                delivery = self._connection.execute(
                    "SELECT * FROM notification_deliveries WHERE delivery_id = ?",
                    (delivery_id,),
                ).fetchone()
                if delivery is None:
                    raise KeyError(f"Unknown notification delivery {delivery_id}")
                intent_id = str(delivery["intent_id"])
                self._finish_notification_attempt(
                    delivery,
                    delivery_id=delivery_id,
                    succeeded=succeeded,
                    max_attempts=max_attempts,
                    retry_delay_seconds=retry_delay_seconds,
                    error=error,
                    now_value=now_value,
                )
                intent_status, completed_at = self._notification_intent_state(
                    intent_id, now
                )
                self._connection.execute(
                    """
                    UPDATE notification_intents
                    SET status = ?, completed_at = ?, updated_at = ?
                    WHERE intent_id = ?
                    """,
                    (intent_status, completed_at, now, intent_id),
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            intent = self._connection.execute(
                "SELECT * FROM notification_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
        if intent is None:
            raise RuntimeError("Notification intent disappeared during delivery")
        return self._decode_notification_intent(intent)

    def _finish_notification_attempt(
        self,
        delivery: sqlite3.Row,
        *,
        delivery_id: str,
        succeeded: bool,
        max_attempts: int,
        retry_delay_seconds: float,
        error: str | None,
        now_value: datetime,
    ) -> None:
        if delivery["status"] != "sending":
            return
        now = now_value.isoformat()
        if succeeded:
            self._connection.execute(
                """
                UPDATE notification_deliveries
                SET status = 'succeeded', last_error = NULL,
                    delivered_at = ?, updated_at = ?
                WHERE delivery_id = ?
                """,
                (now, now, delivery_id),
            )
            return
        failure = error or "Notification delivery failed"
        if int(delivery["attempt_count"]) >= max_attempts:
            self._connection.execute(
                """
                UPDATE notification_deliveries
                SET status = 'failed', last_error = ?, updated_at = ?
                WHERE delivery_id = ?
                """,
                (failure, now, delivery_id),
            )
            return
        next_attempt = (now_value + timedelta(seconds=retry_delay_seconds)).isoformat()
        self._connection.execute(
            """
            UPDATE notification_deliveries
            SET status = 'pending', last_error = ?,
                next_attempt_at = ?, updated_at = ?
            WHERE delivery_id = ?
            """,
            (failure, next_attempt, now, delivery_id),
        )

    def _notification_intent_state(
        self, intent_id: str, now: str
    ) -> tuple[str, str | None]:
        states = self._connection.execute(
            """
            SELECT status, last_error FROM notification_deliveries
            WHERE intent_id = ?
            """,
            (intent_id,),
        ).fetchall()
        if states and all(row["status"] == "succeeded" for row in states):
            return "succeeded", now
        if states and not any(
            row["status"] in {"pending", "sending"} for row in states
        ):
            return "failed", now
        if any(row["last_error"] is not None for row in states):
            return "partial", None
        return "pending", None

    def notification_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM notification_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        return self._decode_notification_intent(row) if row else None

    def notification_deliveries(self, intent_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM notification_deliveries
                WHERE intent_id = ? ORDER BY created_at, delivery_id
                """,
                (intent_id,),
            ).fetchall()
        return [self._decode_notification_delivery(row) for row in rows]

    def unreported_notification_intents(
        self, run_id: str, *, limit: int = 32
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM notification_intents
                WHERE run_id = ? AND status IN ('partial', 'succeeded', 'failed')
                  AND (last_reported_status IS NULL OR last_reported_status <> status)
                ORDER BY updated_at LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return [self._decode_notification_intent(row) for row in rows]

    def mark_notification_reported(self, intent_id: str, status: str) -> bool:
        with self._lock:
            updated = self._connection.execute(
                """
                UPDATE notification_intents
                SET last_reported_status = ?, updated_at = ?
                WHERE intent_id = ? AND status = ?
                  AND (last_reported_status IS NULL OR last_reported_status <> ?)
                """,
                (status, utc_now().isoformat(), intent_id, status, status),
            )
            self._connection.commit()
        return updated.rowcount == 1

    def snapshot(self, run_id: str, *, chart_points: int = 300) -> dict[str, Any]:
        run = self.get_run(run_id)
        with self._lock:
            cell_rows = self._connection.execute(
                "SELECT * FROM cells WHERE run_id = ? ORDER BY cell_index", (run_id,)
            ).fetchall()
        resources = self.list_resources(run_id)
        observation_histories = self.downsampled_run_resource_observations(
            run_id, chart_points
        )
        for resource in resources:
            resource["observations"] = observation_histories.get(
                str(resource["internal_id"]), []
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "run": run,
            "cells": [self._decode_cell(row) for row in cell_rows],
            "resources": resources,
            "events": self.recent_events(run_id),
            "actions": self.list_actions(run_id),
        }

    @staticmethod
    def _decode_run(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["metadata"] = json_loads(value.pop("metadata_json"), {})
        return value

    @staticmethod
    def _decode_cell(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["traceback"] = json_loads(value.pop("traceback_json"), [])
        value["output_tail"] = json_loads(value.pop("output_tail_json"), [])
        return value

    @staticmethod
    def _decode_resource(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["lifecycle"] = json_loads(value.pop("lifecycle_json"), {})
        value["metadata"] = json_loads(value.pop("metadata_json"), {})
        value["cursor"] = json_loads(value.pop("cursor_json"), {})
        value["metrics"] = json_loads(value.pop("metrics_json"), {})
        value["log_tail"] = json_loads(value.pop("log_tail_json"), [])
        value["raw"] = json_loads(value.pop("raw_json"), {})
        for key in ("terminal", "monitor_closed", "supports_stop"):
            value[key] = bool(value[key])
        return value

    @staticmethod
    def _decode_action(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["payload"] = json_loads(value.pop("payload_json"), {})
        value["result"] = json_loads(value.pop("result_json"), {})
        return value

    @staticmethod
    def _decode_notification_intent(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["data"] = json_loads(value.pop("data_json"), {})
        return value

    @staticmethod
    def _decode_notification_delivery(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        if "data_json" in value:
            value["data"] = json_loads(value.pop("data_json"), {})
        return value


def process_start_time(pid: int | None) -> float | None:
    if not pid or pid <= 0:
        return None
    try:
        return float(psutil.Process(pid).create_time())
    except (psutil.Error, OSError):
        return None


def process_is_alive(pid: int | None, expected_started_at: float | None = None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        process = psutil.Process(pid)
        if not process.is_running():
            return False
        if process.status() in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
            return False
        if expected_started_at is not None:
            return abs(float(process.create_time()) - expected_started_at) < 0.01
    except (psutil.Error, OSError):
        return False
    return True


def controller_is_alive(run: dict[str, Any]) -> bool:
    return process_is_alive(run.get("process_pid"), run.get("process_started_at"))

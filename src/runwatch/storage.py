from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psutil
from pydantic import BaseModel

from ._fs import PRIVATE_DIRECTORY_MODE, PRIVATE_FILE_MODE
from .models import (
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
from .schema_versions import DATABASE_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION


class UnsupportedRunSchema(RuntimeError):
    pass


class CorruptRunState(RuntimeError):
    pass


class ResourceEventConflict(RuntimeError):
    pass


_NOTIFICATION_EVENT_CURSOR_KEY = "_notification_event_cursor"
_NOTIFICATION_ROUTING_REQUIRED_KEY = "_notification_routing_required"
_NOTIFICATION_ROUTING_FAILURE_KEY = "_notification_routing_failure"
_NOTIFICATION_EGRESS_SCHEMA_KEY = "_notification_egress_schema_version"
_NOTIFICATION_EGRESS_SCHEMA_VERSION = 1
_TERMINAL_EVENT_KEY = "_terminal_event"
_RESOURCE_DOMAIN_EVENT_TEXT_MAX_BYTES = 64
_RESOURCE_DOMAIN_EVENT_INDEX_LIMIT = 2_147_483_647
_SQLITE_INTEGER_MAX = 9_223_372_036_854_775_807
_RESOURCE_OBSERVATION_EVENT_ID_JSON_BYTES = 48
_RESOURCE_OBSERVATION_EVENT_MESSAGE_JSON_BYTES = 32
_NOTIFICATION_ROUTING_EVENT_TYPE_JSON_BYTES = 64
_NOTIFICATION_ROUTING_ERROR_TYPE_JSON_BYTES = 64
_RUN_MESSAGE_MAX_BYTES = 16_384
_CELL_ERROR_NAME_MAX_BYTES = 256
_CELL_ERROR_VALUE_MAX_BYTES = 16_384
_CELL_TRACEBACK_MAX_LINES = 50
_CELL_TRACEBACK_LINE_JSON_BYTES = 4_096
_CELL_EVENT_ERROR_NAME_JSON_BYTES = 96
_CELL_EVENT_ERROR_VALUE_JSON_BYTES = 192
_CELL_EVENT_TRACEBACK_LINES = 2
_CELL_EVENT_TRACEBACK_LINE_JSON_BYTES = 128
_TERMINAL_EVENT_ERROR_TYPE_JSON_BYTES = 96
_TERMINAL_EVENT_ERROR_JSON_BYTES = 384
_TERMINAL_EVENT_OUTPUT_PATH_JSON_BYTES = 384
_TERMINAL_EVENT_RESOURCE_ID_JSON_BYTES = 80
_TERMINAL_EVENT_RESOURCE_SAMPLE_SIZE = 5
_SAFE_LEGACY_NOTIFICATION_DATA: dict[str, Any] = {
    "schema_version": 1,
    "kind": "legacy",
    "note": "Retained notification details were removed for safety",
}
_NOTIFICATION_DATA_FIELDS: dict[str, frozenset[str]] = {
    "periodic_status": frozenset(
        {
            "schema_version",
            "kind",
            "name",
            "status",
            "current_cell_index",
            "active_resource_count",
            "elapsed_seconds",
        }
    ),
    "cell_failed": frozenset({"schema_version", "kind", "cell_index", "error_type"}),
    "section_started": frozenset(
        {
            "schema_version",
            "kind",
            "heading",
            "heading_level",
            "cell_index",
        }
    ),
    "resource_failed": frozenset(
        {
            "schema_version",
            "kind",
            "provider",
            "resource_type",
            "display_id",
            "status",
        }
    ),
    "run_succeeded": frozenset(
        {"schema_version", "kind", "status", "reason", "elapsed_seconds"}
    ),
    "run_failed": frozenset(
        {"schema_version", "kind", "status", "reason", "elapsed_seconds"}
    ),
    "run_cancelled": frozenset(
        {"schema_version", "kind", "status", "reason", "elapsed_seconds"}
    ),
    "legacy": frozenset({"schema_version", "kind", "note"}),
}


def _unique_notification_destinations(
    values: Iterable[tuple[str, str]],
) -> list[tuple[str, str]]:
    destinations = list(dict.fromkeys(values))
    if any(not kind or not destination for kind, destination in destinations):
        raise ValueError("Notification destinations require a kind and destination")
    return destinations


def _notification_topology(
    values: Iterable[tuple[str, str]],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for kind, _destination in values:
        counts[kind] = counts.get(kind, 0) + 1
    return tuple(sorted(counts.items()))


def _notification_rotation_pairs(
    actual: list[tuple[str, str]],
    current: list[tuple[str, str]] | None,
    desired: list[tuple[str, str]],
) -> list[tuple[str, str, str]]:
    if not actual:
        return []
    if current is None:
        if _notification_topology(actual) != _notification_topology(desired):
            raise ValueError(
                "Persisted notification destinations do not match the desired topology"
            )
        current = actual
    if _notification_topology(current) != _notification_topology(desired):
        raise ValueError(
            "Persisted notification destinations do not match the desired topology"
        )
    if not set(actual).issubset(current):
        raise ValueError(
            "Persisted notification destinations do not match the desired topology"
        )
    pairs: list[tuple[str, str, str]] = []
    for kind, _count in _notification_topology(desired):
        actual_values = sorted(
            destination for item_kind, destination in actual if item_kind == kind
        )
        current_values = [
            destination for item_kind, destination in current if item_kind == kind
        ]
        targets = [
            destination for item_kind, destination in desired if item_kind == kind
        ]
        replacements = dict(zip(current_values, targets, strict=True))
        pairs.extend((kind, old, replacements[old]) for old in actual_values)
    return pairs


def _current_notification_data(raw: str) -> bool:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(value, dict):
        return False
    raw_mapping = cast(dict[object, object], value)
    mapping: dict[str, object] = {
        key: item for key, item in raw_mapping.items() if isinstance(key, str)
    }
    if len(mapping) != len(raw_mapping) or mapping.get("schema_version") != 1:
        return False
    kind = mapping.get("kind")
    allowed = _NOTIFICATION_DATA_FIELDS.get(kind) if isinstance(kind, str) else None
    return allowed is not None and set(mapping).issubset(allowed)


def _terminal_notification_alias_group(
    dedup_key: object,
) -> tuple[str, str] | None:
    if not isinstance(dedup_key, str):
        return None
    parts = dedup_key.split(":")
    if len(parts) == 3 and parts[0] == "run-terminal":
        status = parts[1]
        if status in {"succeeded", "failed", "cancelled"} and parts[2]:
            return status, parts[2]
    if len(parts) == 2 and parts[0] in {"run-succeeded", "run-cancelled"}:
        return parts[0].removeprefix("run-"), parts[1]
    if len(parts) == 3 and parts[0] == "run-failed" and parts[1] and parts[2]:
        return "failed", parts[2]
    return None


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


def _bounded_utf8_text(value: str, max_bytes: int) -> str:
    """Truncate text to a valid UTF-8 byte budget."""

    if max_bytes < 1:
        raise ValueError("UTF-8 byte limits must be positive")
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    suffix = "…".encode("utf-8")
    if max_bytes < len(suffix):
        return encoded[:max_bytes].decode("utf-8", errors="ignore")
    prefix = encoded[: max_bytes - len(suffix)].decode("utf-8", errors="ignore")
    return prefix + "…"


def _bounded_json_text(value: str, max_bytes: int) -> str:
    """Truncate text so its serialized JSON string fits a byte budget."""

    if max_bytes < len(json_dumps("").encode("utf-8")):
        raise ValueError("JSON string byte limits must fit an empty string")
    if len(json_dumps(value).encode("utf-8")) <= max_bytes:
        return value

    suffix = "…"
    low = 0
    high = len(value)
    best = ""
    while low <= high:
        middle = (low + high) // 2
        candidate = value[:middle] + suffix
        if len(json_dumps(candidate).encode("utf-8")) <= max_bytes:
            best = candidate
            low = middle + 1
        else:
            high = middle - 1
    return best


def _bounded_event_index(value: int | None) -> int | None:
    if value is None:
        return None
    return max(
        -_RESOURCE_DOMAIN_EVENT_INDEX_LIMIT,
        min(value, _RESOURCE_DOMAIN_EVENT_INDEX_LIMIT),
    )


def _terminal_event_kernel_epoch(
    payload: dict[str, Any],
) -> tuple[int | None, bool]:
    value = payload.get("kernel_epoch")
    kernel_epoch = (
        _bounded_event_index(value)
        if isinstance(value, int) and not isinstance(value, bool)
        else None
    )
    return kernel_epoch, kernel_epoch != value


def _project_runner_error_terminal_event(
    payload: dict[str, Any],
    kernel_epoch: int | None,
    kernel_epoch_truncated: bool,
) -> dict[str, Any]:
    raw_error_type = str(payload.get("error_type", ""))
    raw_error = str(payload.get("error", ""))
    error_type = _bounded_json_text(
        raw_error_type, _TERMINAL_EVENT_ERROR_TYPE_JSON_BYTES
    )
    error = _bounded_json_text(raw_error, _TERMINAL_EVENT_ERROR_JSON_BYTES)
    return {
        "kernel_epoch": kernel_epoch,
        "error_type": error_type,
        "error": error,
        "projection_truncated": (
            kernel_epoch_truncated
            or error_type != raw_error_type
            or error != raw_error
            or set(payload) != {"kernel_epoch", "error_type", "error"}
        ),
    }


def _project_external_failure_terminal_event(
    payload: dict[str, Any],
    kernel_epoch: int | None,
    kernel_epoch_truncated: bool,
) -> dict[str, Any]:
    raw_resource_ids = payload.get("resource_ids", [])
    resource_id_values = (
        cast(list[object], raw_resource_ids)
        if isinstance(raw_resource_ids, list)
        else []
    )
    resource_ids = [str(resource_id) for resource_id in resource_id_values]
    sample = resource_ids[:_TERMINAL_EVENT_RESOURCE_SAMPLE_SIZE]
    bounded_sample = [
        _bounded_json_text(resource_id, _TERMINAL_EVENT_RESOURCE_ID_JSON_BYTES)
        for resource_id in sample
    ]
    return {
        "kernel_epoch": kernel_epoch,
        "failure_count": len(resource_ids),
        "resource_ids_sample": bounded_sample,
        "projection_truncated": (
            kernel_epoch_truncated
            or not isinstance(raw_resource_ids, list)
            or len(resource_ids) > len(sample)
            or bounded_sample != sample
            or set(payload) != {"kernel_epoch", "resource_ids"}
        ),
    }


def _project_succeeded_terminal_event(
    payload: dict[str, Any],
    kernel_epoch: int | None,
    kernel_epoch_truncated: bool,
) -> dict[str, Any]:
    raw_output_path = str(payload.get("output_path", ""))
    output_path = _bounded_json_text(
        raw_output_path, _TERMINAL_EVENT_OUTPUT_PATH_JSON_BYTES
    )
    return {
        "kernel_epoch": kernel_epoch,
        "output_path": output_path,
        "projection_truncated": (
            kernel_epoch_truncated
            or output_path != raw_output_path
            or set(payload) != {"kernel_epoch", "output_path"}
        ),
    }


def _project_cancelled_terminal_event(
    payload: dict[str, Any],
    kernel_epoch: int | None,
    kernel_epoch_truncated: bool,
) -> dict[str, Any]:
    projected: dict[str, Any] = {"kernel_epoch": kernel_epoch}
    if "kernel_state_lost" in payload:
        projected["kernel_state_lost"] = bool(payload["kernel_state_lost"])
    if "offline" in payload:
        projected["offline"] = bool(payload["offline"])
    projected["projection_truncated"] = (
        kernel_epoch_truncated
        or set(payload) - {"kernel_epoch", "kernel_state_lost", "offline"} != set()
    )
    return projected


def _project_external_timeout_terminal_event(
    payload: dict[str, Any],
    kernel_epoch: int | None,
    kernel_epoch_truncated: bool,
) -> dict[str, Any]:
    return {
        "kernel_epoch": kernel_epoch,
        "projection_truncated": (
            kernel_epoch_truncated or set(payload) != {"kernel_epoch"}
        ),
    }


_TerminalEventProjector = Callable[[dict[str, Any], int | None, bool], dict[str, Any]]
_TERMINAL_EVENT_PROJECTORS: dict[str, _TerminalEventProjector] = {
    "run.runner_error": _project_runner_error_terminal_event,
    "run.failed_external": _project_external_failure_terminal_event,
    "run.succeeded": _project_succeeded_terminal_event,
    "run.cancelled": _project_cancelled_terminal_event,
    "run.external_timeout": _project_external_timeout_terminal_event,
}
_ALLOWED_TERMINAL_EVENTS: dict[RunStatus, frozenset[str]] = {
    RunStatus.SUCCEEDED: frozenset({"run.succeeded"}),
    RunStatus.CANCELLED: frozenset({"run.cancelled"}),
    RunStatus.FAILED: frozenset(
        {"run.runner_error", "run.failed_external", "run.external_timeout"}
    ),
}


def _utf8_suffix(value: str, max_bytes: int) -> str:
    """Return the largest valid suffix that fits a UTF-8 byte budget."""

    if max_bytes <= 0:
        return ""
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[-max_bytes:].decode("utf-8", errors="ignore")


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
        max_event_payload_bytes: int = 2_097_152,
        max_resource_payload_bytes: int = 4_194_304,
        max_notification_record_bytes: int = 524_288,
        max_delivery_error_bytes: int = 4_096,
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
        self.max_event_payload_bytes = max_event_payload_bytes
        self.max_resource_payload_bytes = max_resource_payload_bytes
        self.max_notification_record_bytes = max_notification_record_bytes
        self.max_delivery_error_bytes = max_delivery_error_bytes
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
                "start a new run with Runwatch schema version 3"
            )
        row = self._connection.execute(
            "SELECT value FROM runwatch_meta WHERE key = 'schema_version'"
        ).fetchone()
        version = int(row["value"]) if row else 0
        if version not in {2, DATABASE_SCHEMA_VERSION}:
            raise UnsupportedRunSchema(
                "Unsupported Runwatch database schema version "
                f"{version}; expected {DATABASE_SCHEMA_VERSION}"
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
        if version == 2:
            self._migrate_schema_v2_to_v3()
        integrity = self._connection.execute("PRAGMA quick_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            detail = integrity[0] if integrity else "no result"
            raise CorruptRunState(f"Runwatch database integrity check failed: {detail}")

    def _migrate_schema_v2_to_v3(self) -> None:
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            self._connection.execute(
                "ALTER TABLE runs ADD COLUMN finalization_complete "
                "INTEGER NOT NULL DEFAULT 0"
            )
            self._connection.execute("ALTER TABLE runs ADD COLUMN finalized_at TEXT")
            self._connection.execute(
                "UPDATE runwatch_meta SET value = ? WHERE key = 'schema_version'",
                (str(DATABASE_SCHEMA_VERSION),),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

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
                finalization_complete INTEGER NOT NULL DEFAULT 0,
                finalized_at TEXT,
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
            (str(DATABASE_SCHEMA_VERSION),),
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
        if status.terminal:
            raise ValueError("Terminal run states must be committed with finish_run")
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
        if status in {RunStatus.STARTING, RunStatus.RESTARTING}:
            fields.extend(
                ["finalization_complete = 0", "finalized_at = NULL", "ended_at = NULL"]
            )
        values.append(run_id)
        with self._lock:
            self._connection.execute(
                f"UPDATE runs SET {', '.join(fields)} WHERE run_id = ?", values
            )
            self._connection.commit()

    def finish_run(
        self,
        run_id: str,
        status: RunStatus,
        *,
        message: str,
        event_type: str,
        event_payload: dict[str, Any],
        current_cell_index: int | None | object = ...,
        failed_cell_index: int | None | object = ...,
        failed_attempt: int | None | object = ...,
    ) -> dict[str, Any]:
        """Atomically commit one terminal run transition and its domain event."""

        if not status.terminal:
            raise ValueError("finish_run requires a terminal status")
        allowed_events = _ALLOWED_TERMINAL_EVENTS.get(status, frozenset())
        if event_type not in allowed_events:
            raise ValueError(
                f"Terminal status {status.value} cannot be committed with event "
                f"{event_type!r}"
            )
        now = utc_now().isoformat()
        message = _bounded_utf8_text(message, _RUN_MESSAGE_MAX_BYTES)
        fields = [
            "status = ?",
            "message = ?",
            "updated_at = ?",
            "ended_at = ?",
            "finalization_complete = 0",
            "finalized_at = NULL",
        ]
        values: list[Any] = [status.value, message, now, now]
        for name, value in (
            ("current_cell_index", current_cell_index),
            ("failed_cell_index", failed_cell_index),
            ("failed_attempt", failed_attempt),
        ):
            if value is not ...:
                fields.append(f"{name} = ?")
                values.append(value)
        terminal_values = tuple(item.value for item in RunStatus if item.terminal)
        terminal_placeholders = ", ".join("?" for _ in terminal_values)
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT status, kernel_epoch, metadata_json FROM runs "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(run_id)
                current_status = RunStatus(row["status"])
                if current_status.terminal:
                    raise RuntimeError(
                        f"Run {run_id} is already terminal with status "
                        f"{current_status.value}"
                    )
                kernel_epoch = int(row["kernel_epoch"])
                supplied_epoch = event_payload.get("kernel_epoch")
                corrected_supplied_epoch = "kernel_epoch" in event_payload and (
                    isinstance(supplied_epoch, bool)
                    or not isinstance(supplied_epoch, int)
                    or supplied_epoch != kernel_epoch
                )
                derived_payload = dict(event_payload)
                derived_payload["kernel_epoch"] = kernel_epoch
                projected_payload = self._terminal_event_payload(
                    event_type, derived_payload
                )
                if corrected_supplied_epoch:
                    projected_payload["projection_truncated"] = True
                metadata_value = json_loads(row["metadata_json"], {})
                if not isinstance(metadata_value, dict):
                    raise CorruptRunState("Run metadata must be a JSON object")
                metadata = cast(dict[str, Any], metadata_value)
                metadata[_TERMINAL_EVENT_KEY] = {
                    "type": event_type,
                    "kernel_epoch": kernel_epoch,
                }
                terminal_fields = [*fields, "metadata_json = ?"]
                terminal_values_list = [
                    *values,
                    json_dumps(metadata),
                    run_id,
                    *terminal_values,
                ]
                updated = self._connection.execute(
                    f"UPDATE runs SET {', '.join(terminal_fields)} "
                    f"WHERE run_id = ? AND status NOT IN ({terminal_placeholders})",
                    terminal_values_list,
                )
                if updated.rowcount != 1:
                    changed = self._connection.execute(
                        "SELECT status FROM runs WHERE run_id = ?", (run_id,)
                    ).fetchone()
                    if changed is None:
                        raise KeyError(run_id)
                    raise RuntimeError(
                        f"Run {run_id} is already terminal with status "
                        f"{changed['status']}"
                    )
                event = self._append_event_uncommitted(
                    run_id, event_type, projected_payload, timestamp=now
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return event

    @staticmethod
    def _terminal_event_payload(
        event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Project essential terminal events into the configured minimum cap."""

        projector = _TERMINAL_EVENT_PROJECTORS.get(event_type)
        if projector is None:
            return payload
        kernel_epoch, kernel_epoch_truncated = _terminal_event_kernel_epoch(payload)
        return projector(payload, kernel_epoch, kernel_epoch_truncated)

    def terminal_event_for_state(
        self, run_id: str, status: RunStatus, kernel_epoch: int
    ) -> dict[str, Any] | None:
        """Return the terminal event matching one durable run state.

        Current stores retain the terminal event identity in run metadata so event
        retention cannot erase the failure reason. Legacy stores fall back to their
        retained event journal.
        """

        if not status.terminal:
            raise ValueError("terminal_event_for_state requires a terminal status")
        if isinstance(kernel_epoch, bool) or kernel_epoch < 0:
            raise ValueError("Terminal event kernel epoch must be nonnegative")
        allowed_events = _ALLOWED_TERMINAL_EVENTS[status]
        with self._lock:
            row = self._connection.execute(
                "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise KeyError(run_id)
            metadata_value = json_loads(row["metadata_json"], {})
            if not isinstance(metadata_value, dict):
                raise CorruptRunState("Run metadata must be a JSON object")
            metadata = cast(dict[str, Any], metadata_value)
            terminal_value = metadata.get(_TERMINAL_EVENT_KEY)
            if isinstance(terminal_value, dict):
                terminal = cast(dict[object, object], terminal_value)
                event_type = terminal.get("type")
                event_epoch = terminal.get("kernel_epoch")
                if (
                    isinstance(event_type, str)
                    and event_type in allowed_events
                    and isinstance(event_epoch, int)
                    and not isinstance(event_epoch, bool)
                    and event_epoch == kernel_epoch
                ):
                    return {
                        "run_id": run_id,
                        "type": event_type,
                        "payload": {"kernel_epoch": kernel_epoch},
                    }
            placeholders = ", ".join("?" for _ in allowed_events)
            rows = self._connection.execute(
                f"SELECT * FROM events WHERE run_id = ? "
                f"AND type IN ({placeholders}) ORDER BY seq DESC",
                (run_id, *sorted(allowed_events)),
            ).fetchall()
        for event_row in rows:
            payload_value = json_loads(event_row["payload_json"], {})
            if not isinstance(payload_value, dict):
                continue
            payload = cast(dict[str, Any], payload_value)
            event_epoch = payload.get("kernel_epoch")
            if (
                isinstance(event_epoch, bool)
                or not isinstance(event_epoch, int)
                or event_epoch != kernel_epoch
            ):
                continue
            return {
                "seq": int(event_row["seq"]),
                "run_id": event_row["run_id"],
                "timestamp": event_row["timestamp"],
                "type": event_row["type"],
                "payload": payload,
            }
        return None

    def mark_run_finalized(self, run_id: str, expected_status: RunStatus) -> None:
        """Record that the runner and supervisor post-run work returned normally."""

        if not expected_status.terminal:
            raise ValueError("Only a terminal run can be finalized")
        now = utc_now().isoformat()
        with self._lock:
            updated = self._connection.execute(
                """
                UPDATE runs SET finalization_complete = 1, finalized_at = ?, updated_at = ?
                WHERE run_id = ? AND status = ? AND ended_at IS NOT NULL
                """,
                (now, now, run_id, expected_status.value),
            )
            self._connection.commit()
        if updated.rowcount != 1:
            raise RuntimeError(
                f"Run {run_id} is not terminal as {expected_status.value}"
            )

    def request_run_cancellation(
        self,
        run_id: str,
        *,
        message: str,
        event_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Atomically persist a first cancellation request and its event."""

        now = utc_now().isoformat()
        terminal_values = tuple(item.value for item in RunStatus if item.terminal)
        terminal_placeholders = ", ".join("?" for _ in terminal_values)
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT status FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(run_id)
                current = RunStatus(row["status"])
                if current.terminal or current is RunStatus.CANCELLING:
                    self._connection.commit()
                    return None
                updated = self._connection.execute(
                    f"""
                    UPDATE runs SET status = ?, message = ?, updated_at = ?
                    WHERE run_id = ? AND status NOT IN ({terminal_placeholders})
                    """,
                    (
                        RunStatus.CANCELLING.value,
                        message,
                        now,
                        run_id,
                        *terminal_values,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError(f"Run {run_id} changed while cancellation began")
                event = self._append_event_uncommitted(
                    run_id, "run.cancel_requested", event_payload, timestamp=now
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return event

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
                """
                UPDATE runs SET kernel_epoch = kernel_epoch + 1, updated_at = ?,
                    finalization_complete = 0, finalized_at = NULL, ended_at = NULL
                WHERE run_id = ?
                """,
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

    def pause_failed_cell(
        self,
        run_id: str,
        cell_index: int,
        *,
        attempt: int,
        kernel_epoch: int,
        elapsed_seconds: float,
        error_name: str,
        error_value: str,
        traceback: list[str],
        kernel_dead: bool,
    ) -> dict[str, Any]:
        """Atomically persist a failed cell, paused run, and failure event."""

        now = utc_now().isoformat()
        persisted_error_name = _bounded_utf8_text(
            error_name, _CELL_ERROR_NAME_MAX_BYTES
        )
        persisted_error_value = _bounded_utf8_text(
            error_value, _CELL_ERROR_VALUE_MAX_BYTES
        )
        selected_traceback = traceback[-_CELL_TRACEBACK_MAX_LINES:]
        persisted_traceback = [
            _bounded_json_text(line, _CELL_TRACEBACK_LINE_JSON_BYTES)
            for line in selected_traceback
        ]
        persisted_details_truncated = (
            persisted_error_name != error_name
            or persisted_error_value != error_value
            or len(selected_traceback) != len(traceback)
            or persisted_traceback != selected_traceback
        )
        event_error_name = _bounded_json_text(
            persisted_error_name, _CELL_EVENT_ERROR_NAME_JSON_BYTES
        )
        event_error_value = _bounded_json_text(
            persisted_error_value, _CELL_EVENT_ERROR_VALUE_JSON_BYTES
        )
        event_traceback_source = persisted_traceback[-_CELL_EVENT_TRACEBACK_LINES:]
        event_traceback = [
            _bounded_json_text(line, _CELL_EVENT_TRACEBACK_LINE_JSON_BYTES)
            for line in event_traceback_source
        ]
        message = _bounded_utf8_text(
            f"Cell {cell_index + 1} failed: "
            f"{persisted_error_name}: {persisted_error_value}",
            _RUN_MESSAGE_MAX_BYTES,
        )
        terminal_values = tuple(item.value for item in RunStatus if item.terminal)
        terminal_placeholders = ", ".join("?" for _ in terminal_values)
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                cell = self._connection.execute(
                    """
                    UPDATE cells SET status = ?, ended_at = ?, elapsed_seconds = ?,
                        error_name = ?, error_value = ?, traceback_json = ?
                    WHERE run_id = ? AND cell_index = ? AND attempt = ?
                        AND kernel_epoch = ?
                    """,
                    (
                        CellStatus.FAILED.value,
                        now,
                        elapsed_seconds,
                        persisted_error_name,
                        persisted_error_value,
                        json_dumps(persisted_traceback),
                        run_id,
                        cell_index,
                        attempt,
                        kernel_epoch,
                    ),
                )
                if cell.rowcount != 1:
                    raise RuntimeError(
                        f"Cell {cell_index} attempt changed before failure persistence"
                    )
                run = self._connection.execute(
                    f"""
                    UPDATE runs SET status = ?, message = ?, current_cell_index = ?,
                        failed_cell_index = ?, failed_attempt = ?, updated_at = ?
                    WHERE run_id = ? AND status NOT IN ({terminal_placeholders})
                    """,
                    (
                        RunStatus.PAUSED.value,
                        message,
                        cell_index,
                        cell_index,
                        attempt,
                        now,
                        run_id,
                        *terminal_values,
                    ),
                )
                if run.rowcount != 1:
                    raise RuntimeError(
                        f"Run {run_id} became terminal before cell failure persistence"
                    )
                event = self._append_event_uncommitted(
                    run_id,
                    "cell.failed",
                    {
                        "cell_index": _bounded_event_index(cell_index),
                        "attempt": _bounded_event_index(attempt),
                        "kernel_epoch": _bounded_event_index(kernel_epoch),
                        "error_name": event_error_name,
                        "error_value": event_error_value,
                        "traceback": event_traceback,
                        "kernel_dead": kernel_dead,
                        "projection_truncated": (
                            persisted_details_truncated
                            or event_error_name != persisted_error_name
                            or event_error_value != persisted_error_value
                            or len(event_traceback_source) != len(persisted_traceback)
                            or event_traceback != event_traceback_source
                            or _bounded_event_index(cell_index) != cell_index
                            or _bounded_event_index(attempt) != attempt
                            or _bounded_event_index(kernel_epoch) != kernel_epoch
                        ),
                    },
                    timestamp=now,
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return event

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
        """Persist a resource without publishing its domain event.

        Runtime registration uses :meth:`register_resource_with_event`. This two-value
        helper remains available for low-level state construction and migrations.
        """

        internal_id, created, _event = self._register_resource(
            run_id=run_id,
            event=event,
            cell_index=cell_index,
            attempt=attempt,
            kernel_epoch=kernel_epoch,
            supports_stop=supports_stop,
            append_domain_event=False,
        )
        return internal_id, created

    def register_resource_with_event(
        self,
        *,
        run_id: str,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
    ) -> tuple[str, bool, dict[str, Any]]:
        """Atomically persist a resource transition and its bounded domain event."""

        internal_id, created, persisted_event = self._register_resource(
            run_id=run_id,
            event=event,
            cell_index=cell_index,
            attempt=attempt,
            kernel_epoch=kernel_epoch,
            supports_stop=supports_stop,
            append_domain_event=True,
        )
        assert persisted_event is not None
        return internal_id, created, persisted_event

    def _register_resource(
        self,
        *,
        run_id: str,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
        append_domain_event: bool,
    ) -> tuple[str, bool, dict[str, Any] | None]:
        self._validate_resource_payload_size(
            event.model_dump(mode="json"), record_type="registration"
        )
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                duplicate = self._connection.execute(
                    "SELECT * FROM resources WHERE run_id = ? AND event_id = ?",
                    (run_id, event.event_id),
                ).fetchone()
                superseded_internal_id: str | None = None
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
                    internal_id = str(duplicate["internal_id"])
                    created = False
                elif event.resource.logical_key:
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
                    if (
                        existing is not None
                        and existing["external_id"] == event.resource.id
                    ):
                        internal_id = str(existing["internal_id"])
                        self._refresh_reconciled_resource(
                            internal_id,
                            event=event,
                            cell_index=cell_index,
                            attempt=attempt,
                            kernel_epoch=kernel_epoch,
                            supports_stop=supports_stop,
                        )
                        created = False
                    else:
                        if existing is not None:
                            superseded_internal_id = str(existing["internal_id"])
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
                                    superseded_internal_id,
                                ),
                            )
                        internal_id = self._insert_resource(
                            run_id=run_id,
                            event=event,
                            cell_index=cell_index,
                            attempt=attempt,
                            kernel_epoch=kernel_epoch,
                            supports_stop=supports_stop,
                        )
                        created = True
                else:
                    internal_id = self._insert_resource(
                        run_id=run_id,
                        event=event,
                        cell_index=cell_index,
                        attempt=attempt,
                        kernel_epoch=kernel_epoch,
                        supports_stop=supports_stop,
                    )
                    created = True

                persisted_event = None
                if append_domain_event:
                    persisted_event = self._append_event_uncommitted(
                        run_id,
                        "resource.registered" if created else "resource.reconciled",
                        self._resource_domain_event_payload(
                            internal_id=internal_id,
                            event=event,
                            cell_index=cell_index,
                            attempt=attempt,
                            kernel_epoch=kernel_epoch,
                            supports_stop=supports_stop,
                            superseded_internal_id=superseded_internal_id,
                        ),
                        timestamp=utc_now().isoformat(),
                    )
                self._connection.commit()
                return internal_id, created, persisted_event
            except Exception:
                self._connection.rollback()
                raise

    def _insert_resource(
        self,
        *,
        run_id: str,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
    ) -> str:
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
        return internal_id

    @staticmethod
    def _bounded_resource_domain_event_index(value: int | None) -> int | None:
        return _bounded_event_index(value)

    @classmethod
    def _resource_domain_event_payload(
        cls,
        *,
        internal_id: str,
        event: ResourceEvent,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
        supports_stop: bool,
        superseded_internal_id: str | None,
    ) -> dict[str, Any]:
        text = {
            "event_id": event.event_id,
            "provider": event.resource.provider,
            "type": event.resource.type,
            "id": event.resource.id,
            "logical_key": event.resource.logical_key,
        }
        bounded_text = {
            key: (
                _bounded_json_text(value, _RESOURCE_DOMAIN_EVENT_TEXT_MAX_BYTES)
                if value is not None
                else None
            )
            for key, value in text.items()
        }
        indices = {
            "cell_index": cell_index,
            "attempt": attempt,
            "kernel_epoch": kernel_epoch,
        }
        bounded_indices = {
            key: cls._bounded_resource_domain_event_index(value)
            for key, value in indices.items()
        }
        return {
            "internal_id": internal_id,
            "event_id": bounded_text["event_id"],
            "cell_index": bounded_indices["cell_index"],
            "attempt": bounded_indices["attempt"],
            "kernel_epoch": bounded_indices["kernel_epoch"],
            "resource": {
                "provider": bounded_text["provider"],
                "type": bounded_text["type"],
                "id": bounded_text["id"],
                "logical_key": bounded_text["logical_key"],
                "ownership": event.resource.ownership.value,
            },
            "lifecycle": {
                "monitor": event.lifecycle.monitor,
                "blocking": event.lifecycle.blocking,
                "stop_on_cancel": event.lifecycle.stop_on_cancel,
                "retain_logs": event.lifecycle.retain_logs,
            },
            "supports_stop": supports_stop,
            "superseded_internal_id": superseded_internal_id,
            "projection_truncated": (
                bounded_text != text
                or any(bounded_indices[key] != value for key, value in indices.items())
            ),
        }

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
        self._validate_resource_payload_size(cursor, record_type="cursor")
        with self._lock:
            self._connection.execute(
                "UPDATE resources SET cursor_json = ?, updated_at = ? WHERE internal_id = ?",
                (json_dumps(cursor), utc_now().isoformat(), internal_id),
            )
            self._connection.commit()

    def update_resource_observation(
        self, run_id: str, internal_id: str, observation: ResourceObservation
    ) -> dict[str, Any]:
        return self._update_resource_observation(
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
    ) -> dict[str, Any]:
        """Atomically commit an adapter cursor and everything it observed."""

        return self._update_resource_observation(
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
    ) -> dict[str, Any]:
        """Atomically commit stop inspection state and its terminal disposition."""

        return self._update_resource_observation(
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
    ) -> dict[str, Any]:
        now = utc_now().isoformat()
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._resource_persistence_row(internal_id)
                if row is None:
                    raise KeyError(internal_id)
                log_tail = self._updated_log_tail(row, observation)
                cursor_json = (
                    row["cursor_json"] if cursor is None else json_dumps(cursor)
                )
                cursor_value = json_loads(cursor_json, {})
                self._validate_resource_payload_size(cursor_value, record_type="cursor")
                history_metrics = (
                    observation.metrics
                    if observation.history_metrics is None
                    else observation.history_metrics
                )
                self._validate_resource_payload_size(
                    {
                        "message": observation.message,
                        "current_metrics": observation.metrics,
                        "history_metrics": history_metrics,
                        "raw": observation.raw,
                        "log_lines": observation.log_lines,
                        "persisted_log_tail": log_tail,
                    },
                    record_type="observation",
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
                event = self._append_event_uncommitted(
                    run_id,
                    "resource.observed",
                    self._resource_observation_event_payload(internal_id, observation),
                    timestamp=now,
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return event

    @staticmethod
    def _resource_observation_event_payload(
        internal_id: str, observation: ResourceObservation
    ) -> dict[str, Any]:
        raw_internal_id = str(internal_id)
        bounded_internal_id = _bounded_json_text(
            raw_internal_id, _RESOURCE_OBSERVATION_EVENT_ID_JSON_BYTES
        )
        raw_message = observation.message
        bounded_message = (
            None
            if raw_message is None
            else _bounded_json_text(
                raw_message, _RESOURCE_OBSERVATION_EVENT_MESSAGE_JSON_BYTES
            )
        )
        raw_metric_count = len(observation.metrics)
        raw_log_line_count = len(observation.log_lines)
        metric_count = min(raw_metric_count, _RESOURCE_DOMAIN_EVENT_INDEX_LIMIT)
        new_log_line_count = min(raw_log_line_count, _RESOURCE_DOMAIN_EVENT_INDEX_LIMIT)
        return {
            "internal_id": bounded_internal_id,
            "status": observation.status.value,
            "terminal": observation.terminal,
            "message": bounded_message,
            "metric_count": metric_count,
            "new_log_line_count": new_log_line_count,
            "projection_truncated": (
                bounded_internal_id != raw_internal_id
                or bounded_message != raw_message
                or bool(observation.metrics)
                or bool(observation.log_lines)
                or bool(observation.raw)
                or observation.history_metrics is not None
                or metric_count != raw_metric_count
                or new_log_line_count != raw_log_line_count
            ),
        }

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
                    selected.append(_utf8_suffix(line, remaining))
                break
            selected.append(line)
            used += len(encoded)
        return list(reversed(selected))

    def _validate_resource_payload_size(self, value: Any, *, record_type: str) -> None:
        self._validated_json_payload(
            value,
            max_bytes=self.max_resource_payload_bytes,
            setting="max_resource_payload_bytes",
            record_type=f"Resource {record_type}",
        )

    @staticmethod
    def _validated_json_payload(
        value: Any,
        *,
        max_bytes: int,
        setting: str,
        record_type: str,
    ) -> str:
        serialized = json_dumps(value)
        size = len(serialized.encode("utf-8"))
        if size > max_bytes:
            raise ValueError(
                f"{record_type} exceeds storage.{setting} ({size} > {max_bytes})"
            )
        return serialized

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
                monitor_closed = CASE WHEN ? THEN 0 ELSE monitor_closed END,
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
                int(observation.terminal),
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
        resource, _event = self.request_resource_stop_with_event(
            internal_id, disposition
        )
        return resource

    def request_resource_stop_with_event(
        self, internal_id: str, disposition: ResourceDisposition
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Persist stop intent, atomically confirming a resource already terminal."""

        now = utc_now().isoformat()
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                previous = self._connection.execute(
                    "SELECT * FROM resources WHERE internal_id = ?", (internal_id,)
                ).fetchone()
                if previous is None:
                    raise KeyError(internal_id)
                already_stopping = bool(
                    not previous["terminal"]
                    and previous["status"] == ResourceStatus.STOPPING.value
                )
                self._connection.execute(
                    """
                    UPDATE resources
                    SET status = CASE WHEN terminal THEN status ELSE ? END,
                        disposition = CASE WHEN terminal THEN ? ELSE disposition END,
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
                event = None
                if not already_stopping:
                    event = self._append_event_uncommitted(
                        str(row["run_id"]),
                        "resource.stop_requested",
                        {
                            "internal_id": internal_id,
                            "external_id": str(row["external_id"]),
                            "already_terminal": bool(row["terminal"]),
                        },
                        timestamp=now,
                    )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return self._decode_resource(row), event

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
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                event = self._append_event_uncommitted(
                    run_id, event_type, payload, timestamp=timestamp
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return event

    def _append_event_uncommitted(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        timestamp: str,
    ) -> dict[str, Any]:
        payload_json = self._validated_json_payload(
            payload,
            max_bytes=self.max_event_payload_bytes,
            setting="max_event_payload_bytes",
            record_type="Event payload",
        )
        cursor = self._connection.execute(
            "INSERT INTO events (run_id, timestamp, type, payload_json) VALUES (?, ?, ?, ?)",
            (run_id, timestamp, event_type, payload_json),
        )
        self._prune_events(run_id)
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
                metadata.pop(_NOTIFICATION_ROUTING_FAILURE_KEY, None)
                self._update_run_metadata(run_id, metadata)
                self._prune_events(run_id)
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return True

    def record_notification_routing_failure(
        self,
        run_id: str,
        sequence: object,
        source_event_type: str,
        error_type: str,
        max_attempts: int,
    ) -> dict[str, Any]:
        """Persist one bounded routing retry or atomically dead-letter its event."""

        sequence = self._validated_notification_routing_sequence(sequence)
        self._validate_notification_routing_max_attempts(max_attempts)
        bounded_source_type = _bounded_json_text(
            source_event_type, _NOTIFICATION_ROUTING_EVENT_TYPE_JSON_BYTES
        )
        bounded_error_type = _bounded_json_text(
            error_type, _NOTIFICATION_ROUTING_ERROR_TYPE_JSON_BYTES
        )
        now = utc_now().isoformat()
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
                    return {"attempt": 0, "dead_lettered": False, "event": None}
                attempt = self._next_notification_routing_attempt(metadata, sequence)
                if attempt < max_attempts:
                    metadata[_NOTIFICATION_ROUTING_FAILURE_KEY] = {
                        "event_seq": sequence,
                        "source_event_type": bounded_source_type,
                        "error_type": bounded_error_type,
                        "attempt": attempt,
                    }
                    self._update_run_metadata(run_id, metadata)
                    self._connection.commit()
                    return {
                        "attempt": attempt,
                        "dead_lettered": False,
                        "event": None,
                    }
                metadata[_NOTIFICATION_EVENT_CURSOR_KEY] = sequence
                metadata.pop(_NOTIFICATION_ROUTING_FAILURE_KEY, None)
                self._update_run_metadata(run_id, metadata)
                event = self._append_event_uncommitted(
                    run_id,
                    "notification.event_dead_lettered",
                    self._notification_dead_letter_payload(
                        sequence=sequence,
                        source_event_type=source_event_type,
                        bounded_source_type=bounded_source_type,
                        error_type=error_type,
                        bounded_error_type=bounded_error_type,
                        attempt=attempt,
                    ),
                    timestamp=now,
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
        return {"attempt": attempt, "dead_lettered": True, "event": event}

    @staticmethod
    def _validated_notification_routing_sequence(sequence: object) -> int:
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 0
            or sequence > _SQLITE_INTEGER_MAX
        ):
            raise ValueError("Notification event sequence must be a SQLite integer")
        return sequence

    @staticmethod
    def _validate_notification_routing_max_attempts(max_attempts: int) -> None:
        if isinstance(max_attempts, bool) or max_attempts < 1:
            raise ValueError("Notification routing max_attempts must be positive")

    @staticmethod
    def _next_notification_routing_attempt(
        metadata: dict[str, Any], sequence: int
    ) -> int:
        failure_value = metadata.get(_NOTIFICATION_ROUTING_FAILURE_KEY)
        if failure_value is None:
            return 1
        if not isinstance(failure_value, dict):
            raise CorruptRunState("Notification routing failure metadata is invalid")
        failure = cast(dict[object, object], failure_value)
        if failure.get("event_seq") != sequence:
            return 1
        failure_attempt = failure.get("attempt")
        if (
            isinstance(failure_attempt, bool)
            or not isinstance(failure_attempt, int)
            or failure_attempt < 1
            or failure_attempt >= _RESOURCE_DOMAIN_EVENT_INDEX_LIMIT
        ):
            raise CorruptRunState("Notification routing failure attempt is invalid")
        return failure_attempt + 1

    @staticmethod
    def _notification_dead_letter_payload(
        *,
        sequence: int,
        source_event_type: str,
        bounded_source_type: str,
        error_type: str,
        bounded_error_type: str,
        attempt: int,
    ) -> dict[str, Any]:
        return {
            "event_seq": sequence,
            "source_event_type": bounded_source_type,
            "error_type": bounded_error_type,
            "attempt": attempt,
            "projection_truncated": (
                bounded_source_type != source_event_type
                or bounded_error_type != error_type
            ),
        }

    def _update_run_metadata(self, run_id: str, metadata: dict[str, Any]) -> None:
        self._connection.execute(
            "UPDATE runs SET metadata_json = ?, updated_at = ? WHERE run_id = ?",
            (json_dumps(metadata), utc_now().isoformat(), run_id),
        )

    def notification_run_summary(self, run_id: str) -> dict[str, Any]:
        """Return the small state projection needed by periodic notifications."""

        with self._lock:
            run = self._connection.execute(
                """
                SELECT name, status, current_cell_index, started_at, updated_at
                FROM runs WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            resources = self._connection.execute(
                """
                SELECT COUNT(*) AS count FROM resources
                WHERE run_id = ? AND terminal = 0 AND disposition = ?
                """,
                (run_id, ResourceDisposition.ACTIVE.value),
            ).fetchone()
        if run is None:
            raise KeyError(f"Unknown run {run_id}")
        elapsed_seconds: float | None = None
        if run["started_at"] and run["updated_at"]:
            try:
                elapsed_seconds = max(
                    0.0,
                    (
                        datetime.fromisoformat(str(run["updated_at"]))
                        - datetime.fromisoformat(str(run["started_at"]))
                    ).total_seconds(),
                )
            except ValueError:
                elapsed_seconds = None
        return {
            "name": str(run["name"]),
            "status": str(run["status"]),
            "current_cell_index": run["current_cell_index"],
            "active_resource_count": int(resources["count"] if resources else 0),
            "elapsed_seconds": elapsed_seconds,
        }

    def notification_configuration(self, run_id: str) -> dict[str, Any] | None:
        """Return the notification config last reconciled into durable state."""

        with self._lock:
            row = self._connection.execute(
                "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run {run_id}")
        metadata, _cursor = self._notification_metadata(row)
        config = metadata.get("config")
        if not isinstance(config, dict):
            return None
        notifications = cast(dict[str, Any], config).get("notifications", {})
        if not isinstance(notifications, dict):
            raise CorruptRunState("Persisted notification config must be an object")
        return dict(cast(dict[str, Any], notifications))

    def existing_notification_dedup_key(
        self, run_id: str, dedup_keys: Iterable[str]
    ) -> str | None:
        """Return the first requested canonical or legacy key already persisted."""

        ordered = list(dict.fromkeys(key for key in dedup_keys if key))
        if not ordered:
            return None
        placeholders = ",".join("?" for _key in ordered)
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT dedup_key, status FROM notification_intents
                WHERE run_id = ? AND dedup_key IN ({placeholders})
                """,
                (run_id, *ordered),
            ).fetchall()
        succeeded = {
            str(row["dedup_key"]) for row in rows if row["status"] == "succeeded"
        }
        if succeeded:
            return next(key for key in ordered if key in succeeded)
        existing = {str(row["dedup_key"]) for row in rows}
        return next((key for key in ordered if key in existing), None)

    def notification_delivery_topology(
        self, run_id: str
    ) -> tuple[tuple[str, int], ...]:
        """Return persisted destination-kind counts without exposing credentials."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT kind, COUNT(DISTINCT destination) AS count
                FROM notification_deliveries WHERE run_id = ? GROUP BY kind
                """,
                (run_id,),
            ).fetchall()
        return tuple(sorted((str(row["kind"]), int(row["count"])) for row in rows))

    def reconcile_notification_configuration(
        self,
        run_id: str,
        *,
        current_destinations: Iterable[tuple[str, str]] | None,
        desired_destinations: Iterable[tuple[str, str]],
        desired_configuration: dict[str, Any],
    ) -> dict[str, int]:
        """Atomically rotate same-topology destinations and scrub legacy records."""

        current = (
            None
            if current_destinations is None
            else _unique_notification_destinations(current_destinations)
        )
        desired = _unique_notification_destinations(desired_destinations)
        self._validated_json_payload(
            [
                {"kind": kind, "destination": destination}
                for kind, destination in desired
            ],
            max_bytes=self.max_notification_record_bytes,
            setting="max_notification_record_bytes",
            record_type="Notification rotation destinations",
        )
        if not desired:
            return self.purge_notification_state(
                run_id, desired_configuration=desired_configuration
            )
        now = utc_now().isoformat()
        with self._lock:
            try:
                self._connection.execute("PRAGMA secure_delete=ON")
                self._connection.execute("BEGIN IMMEDIATE")
                consolidated = self._consolidate_terminal_notification_aliases(
                    run_id, now
                )
                actual = self._actual_notification_destinations(run_id)
                pairs = _notification_rotation_pairs(actual, current, desired)
                changed = self._rewrite_notification_destinations(run_id, pairs)
                configuration_changed = current is not None and current != desired
                migration_required = self._notification_egress_migration_required(
                    run_id
                )
                if changed or configuration_changed:
                    self._reset_rotated_notification_deliveries(run_id, now)
                elif migration_required:
                    self._clear_notification_delivery_errors(run_id, now)
                if changed or configuration_changed or migration_required:
                    self._recompute_notification_intents(run_id, now)
                sanitized = self._sanitize_legacy_notification_intents(run_id, now)
                scrubbed = 0
                if changed or configuration_changed or migration_required:
                    scrubbed = self._scrub_notification_diagnostic_events(run_id)
                self._write_notification_configuration_metadata(
                    run_id,
                    desired_configuration=desired_configuration,
                    routing_required=True,
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            if (
                changed
                or configuration_changed
                or migration_required
                or consolidated
                or sanitized
                or scrubbed
            ):
                self._compact_notification_credentials_best_effort()
        return {
            "rotated_destinations": changed,
            "consolidated_intents": consolidated,
            "sanitized_intents": sanitized,
            "scrubbed_events": scrubbed,
        }

    def purge_notification_state(
        self,
        run_id: str,
        *,
        desired_configuration: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Delete notification outbox state and disable event replay routing."""

        configuration = desired_configuration or {}
        with self._lock:
            try:
                self._connection.execute("PRAGMA secure_delete=ON")
                self._connection.execute("BEGIN IMMEDIATE")
                counts = self._notification_purge_counts(run_id)
                maximum = self._notification_event_high_water(run_id)
                self._connection.execute(
                    "DELETE FROM notification_intents WHERE run_id = ?", (run_id,)
                )
                scrubbed = self._scrub_notification_diagnostic_events(run_id)
                self._write_notification_configuration_metadata(
                    run_id,
                    desired_configuration=configuration,
                    routing_required=False,
                    event_cursor=maximum,
                    purge_keys=True,
                )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            self._compact_notification_credentials_best_effort()
        return {
            **counts,
            "scrubbed_events": scrubbed,
        }

    def _consolidate_terminal_notification_aliases(self, run_id: str, now: str) -> int:
        rows = self._connection.execute(
            """
            SELECT intent_id, dedup_key, status, created_at
            FROM notification_intents
            WHERE run_id = ? AND dedup_key IS NOT NULL
            """,
            (run_id,),
        ).fetchall()
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            group = _terminal_notification_alias_group(row["dedup_key"])
            if group is not None:
                groups.setdefault(group, []).append(row)
        removed = 0
        for aliases in groups.values():
            if len(aliases) < 2:
                continue
            authoritative = min(aliases, key=self._notification_alias_priority)
            authoritative_id = str(authoritative["intent_id"])
            for alias in aliases:
                alias_id = str(alias["intent_id"])
                if alias_id == authoritative_id:
                    continue
                self._merge_succeeded_notification_deliveries(
                    authoritative_id, alias_id, now
                )
                self._connection.execute(
                    "DELETE FROM notification_intents WHERE intent_id = ?",
                    (alias_id,),
                )
                removed += 1
            self._recompute_one_notification_intent(authoritative_id, now)
        return removed

    @staticmethod
    def _notification_alias_priority(row: sqlite3.Row) -> tuple[bool, bool, str, str]:
        dedup_key = str(row["dedup_key"])
        return (
            row["status"] != "succeeded",
            not dedup_key.startswith("run-terminal:"),
            str(row["created_at"]),
            str(row["intent_id"]),
        )

    def _merge_succeeded_notification_deliveries(
        self, authoritative_id: str, alias_id: str, now: str
    ) -> None:
        deliveries = self._connection.execute(
            """
            SELECT kind, destination, delivered_at
            FROM notification_deliveries
            WHERE intent_id = ? AND status = 'succeeded'
            """,
            (alias_id,),
        ).fetchall()
        for delivery in deliveries:
            self._connection.execute(
                """
                UPDATE notification_deliveries
                SET status = 'succeeded', last_error = NULL,
                    delivered_at = COALESCE(delivered_at, ?), updated_at = ?
                WHERE intent_id = ? AND kind = ? AND destination = ?
                """,
                (
                    delivery["delivered_at"],
                    now,
                    authoritative_id,
                    delivery["kind"],
                    delivery["destination"],
                ),
            )

    def _recompute_one_notification_intent(self, intent_id: str, now: str) -> None:
        status, completed_at = self._notification_intent_state(intent_id, now)
        self._connection.execute(
            """
            UPDATE notification_intents
            SET status = ?, completed_at = ?,
                last_reported_status = CASE
                    WHEN status = ? THEN last_reported_status ELSE NULL END,
                updated_at = ?
            WHERE intent_id = ?
            """,
            (status, completed_at, status, now, intent_id),
        )

    def _actual_notification_destinations(self, run_id: str) -> list[tuple[str, str]]:
        rows = self._connection.execute(
            """
            SELECT DISTINCT kind, destination FROM notification_deliveries
            WHERE run_id = ? ORDER BY kind, destination
            """,
            (run_id,),
        ).fetchall()
        return [(str(row["kind"]), str(row["destination"])) for row in rows]

    def _rewrite_notification_destinations(
        self,
        run_id: str,
        pairs: list[tuple[str, str, str]],
    ) -> int:
        replacements = [pair for pair in pairs if pair[1] != pair[2]]
        temporary: list[tuple[str, str, str]] = []
        for index, (kind, old, new) in enumerate(replacements):
            placeholder = f"runwatch-rotation:{uuid4().hex}:{index}"
            self._connection.execute(
                """
                UPDATE notification_deliveries SET destination = ?
                WHERE run_id = ? AND kind = ? AND destination = ?
                """,
                (placeholder, run_id, kind, old),
            )
            temporary.append((kind, placeholder, new))
        for kind, placeholder, new in temporary:
            self._connection.execute(
                """
                UPDATE notification_deliveries SET destination = ?
                WHERE run_id = ? AND kind = ? AND destination = ?
                """,
                (new, run_id, kind, placeholder),
            )
        return len(replacements)

    def _reset_rotated_notification_deliveries(self, run_id: str, now: str) -> None:
        self._connection.execute(
            """
            UPDATE notification_deliveries
            SET status = CASE WHEN status = 'succeeded' THEN status ELSE 'pending' END,
                attempt_count = CASE WHEN status = 'succeeded' THEN attempt_count ELSE 0 END,
                next_attempt_at = CASE WHEN status = 'succeeded' THEN next_attempt_at ELSE ? END,
                last_error = NULL,
                delivered_at = CASE WHEN status = 'succeeded' THEN delivered_at ELSE NULL END,
                updated_at = ?
            WHERE run_id = ?
            """,
            (now, now, run_id),
        )

    def _clear_notification_delivery_errors(self, run_id: str, now: str) -> None:
        self._connection.execute(
            """
            UPDATE notification_deliveries
            SET last_error = NULL, updated_at = ?
            WHERE run_id = ? AND last_error IS NOT NULL
            """,
            (now, run_id),
        )

    def _recompute_notification_intents(self, run_id: str, now: str) -> None:
        rows = self._connection.execute(
            "SELECT intent_id, status FROM notification_intents WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        for row in rows:
            status, completed_at = self._notification_intent_state(
                str(row["intent_id"]), now
            )
            self._connection.execute(
                """
                UPDATE notification_intents
                SET status = ?, completed_at = ?,
                    last_reported_status = CASE
                        WHEN status = ? THEN last_reported_status ELSE NULL END,
                    updated_at = ?
                WHERE intent_id = ?
                """,
                (status, completed_at, status, now, row["intent_id"]),
            )

    def _sanitize_legacy_notification_intents(self, run_id: str, now: str) -> int:
        rows = self._connection.execute(
            "SELECT intent_id, data_json FROM notification_intents WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        sanitized = 0
        for row in rows:
            if _current_notification_data(row["data_json"]):
                continue
            self._connection.execute(
                """
                UPDATE notification_intents
                SET title = 'Runwatch notification',
                    message = 'A retained Runwatch notification is ready.',
                    data_json = ?, updated_at = ?
                WHERE intent_id = ?
                """,
                (json_dumps(_SAFE_LEGACY_NOTIFICATION_DATA), now, row["intent_id"]),
            )
            sanitized += 1
        return sanitized

    def _scrub_notification_diagnostic_events(self, run_id: str) -> int:
        updated = self._connection.execute(
            """
            UPDATE events SET payload_json = '{}'
            WHERE run_id = ? AND type LIKE 'notification.%'
            """,
            (run_id,),
        )
        return int(updated.rowcount)

    def _write_notification_configuration_metadata(
        self,
        run_id: str,
        *,
        desired_configuration: dict[str, Any],
        routing_required: bool,
        event_cursor: int | None = None,
        purge_keys: bool = False,
    ) -> None:
        row = self._connection.execute(
            "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run {run_id}")
        metadata, _cursor = self._notification_metadata(row)
        if purge_keys:
            for key in list(metadata):
                if key.startswith("_notification_"):
                    metadata.pop(key)
        config_value = metadata.get("config")
        config = (
            dict(cast(dict[str, Any], config_value))
            if isinstance(config_value, dict)
            else {}
        )
        config["notifications"] = desired_configuration
        metadata["config"] = config
        metadata[_NOTIFICATION_ROUTING_REQUIRED_KEY] = routing_required
        metadata[_NOTIFICATION_EGRESS_SCHEMA_KEY] = _NOTIFICATION_EGRESS_SCHEMA_VERSION
        if event_cursor is not None:
            metadata[_NOTIFICATION_EVENT_CURSOR_KEY] = event_cursor
        self._update_run_metadata(run_id, metadata)

    def _notification_egress_migration_required(self, run_id: str) -> bool:
        row = self._connection.execute(
            "SELECT metadata_json FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown run {run_id}")
        metadata, _cursor = self._notification_metadata(row)
        return metadata.get(_NOTIFICATION_EGRESS_SCHEMA_KEY) != (
            _NOTIFICATION_EGRESS_SCHEMA_VERSION
        )

    def _notification_purge_counts(self, run_id: str) -> dict[str, int]:
        row = self._connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM notification_intents WHERE run_id = ?) AS intents,
                (SELECT COUNT(*) FROM notification_deliveries WHERE run_id = ?) AS deliveries
            """,
            (run_id, run_id),
        ).fetchone()
        return {
            "deleted_intents": int(row["intents"] if row else 0),
            "deleted_deliveries": int(row["deliveries"] if row else 0),
        }

    def _notification_event_high_water(self, run_id: str) -> int:
        row = self._connection.execute(
            "SELECT MAX(seq) AS maximum FROM events WHERE run_id = ?", (run_id,)
        ).fetchone()
        return int(row["maximum"] or 0) if row else 0

    def _compact_notification_credentials_best_effort(self) -> None:
        try:
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        except sqlite3.DatabaseError:
            pass
        try:
            self._connection.execute("VACUUM")
            self._connection.commit()
        except sqlite3.DatabaseError:
            self._connection.rollback()

    def enqueue_rolling_notification(
        self,
        *,
        run_id: str,
        title: str,
        message: str,
        data: dict[str, Any],
        dedup_key: str,
        destinations: Iterable[tuple[str, str]],
    ) -> dict[str, Any]:
        """Keep one reported-or-active intent for a rolling notification slot."""

        return self.enqueue_notification(
            run_id=run_id,
            title=title,
            message=message,
            data=data,
            dedup_key=dedup_key,
            destinations=destinations,
            rolling=True,
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
        rolling: bool = False,
        rearm_failed: bool = True,
    ) -> dict[str, Any]:
        """Persist one notification intent and its independent destinations.

        A successful deduplicated intent is immutable. A terminally failed intent is
        rearmed only when the caller explicitly enqueues that deduplication key again.
        """

        unique_destinations = list(dict.fromkeys(destinations))
        if not unique_destinations:
            raise ValueError("A notification requires at least one destination")
        self._validated_json_payload(
            {
                "title": title,
                "message": message,
                "data": data,
                "dedup_key": dedup_key,
                "rolling": rolling,
                "destinations": [
                    {"kind": kind, "destination": destination}
                    for kind, destination in unique_destinations
                ],
            },
            max_bytes=self.max_notification_record_bytes,
            setting="max_notification_record_bytes",
            record_type="Notification record",
        )
        data_json = json_dumps(data)
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
                row, rolling_result = self._prepare_rolling_notification(row, rolling)
                if rolling_result is not None:
                    return rolling_result
                if row is not None and (
                    row["status"] == "succeeded"
                    or (row["status"] == "failed" and not rearm_failed)
                ):
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
                            data_json,
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
                            (title, message, data_json, now, intent_id),
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

    def _prepare_rolling_notification(
        self, row: sqlite3.Row | None, rolling: bool
    ) -> tuple[sqlite3.Row | None, dict[str, Any] | None]:
        if row is None or not rolling:
            return row, None
        terminal = row["status"] in {"succeeded", "failed"}
        reported = row["last_reported_status"] == row["status"]
        if terminal and reported:
            self._connection.execute(
                "DELETE FROM notification_intents WHERE intent_id = ?",
                (row["intent_id"],),
            )
            return None, None
        result = self._decode_notification_intent(row)
        result.update({"created": False, "rearmed": False})
        return row, result

    def recover_notification_deliveries(
        self,
        run_id: str,
        *,
        max_attempts: int,
        error: str,
    ) -> int:
        """Recover ambiguous interrupted attempts without refunding retry budget."""

        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        error = _bounded_utf8_text(error, self.max_delivery_error_bytes)
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
                    SET status = CASE
                            WHEN attempt_count >= ? THEN 'failed' ELSE 'pending' END,
                        last_error = ?,
                        next_attempt_at = CASE
                            WHEN attempt_count >= ? THEN next_attempt_at ELSE ? END,
                        updated_at = ?
                    WHERE run_id = ? AND status = 'sending'
                    """,
                    (max_attempts, error, max_attempts, now, now, run_id),
                )
                for intent_row in intent_rows:
                    intent_id = str(intent_row["intent_id"])
                    intent_status, completed_at = self._notification_intent_state(
                        intent_id, now
                    )
                    self._connection.execute(
                        """
                        UPDATE notification_intents SET status = ?, completed_at = ?,
                            last_reported_status = CASE
                                WHEN status = ? THEN last_reported_status ELSE NULL END,
                            updated_at = ?
                        WHERE intent_id = ?
                        """,
                        (intent_status, completed_at, intent_status, now, intent_id),
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

        error = _bounded_utf8_text(error, self.max_delivery_error_bytes)
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

        error = (
            _bounded_utf8_text(error, self.max_delivery_error_bytes)
            if error is not None
            else None
        )
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
            "schema_version": RUN_SNAPSHOT_SCHEMA_VERSION,
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
        value["finalization_complete"] = bool(value["finalization_complete"])
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

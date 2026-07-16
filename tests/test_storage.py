# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from runwatch.models import (
    ActionKind,
    ActionStatus,
    CellStatus,
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunStatus,
)
from runwatch.storage import (
    CorruptRunState,
    ResourceEventConflict,
    RunStore,
    UnsupportedRunSchema,
    process_is_alive,
    process_start_time,
)


def initialize(
    store: RunStore,
    root: Path,
    *,
    metadata: dict[str, Any] | None = None,
) -> None:
    source = root / "source.ipynb"
    source.write_text("{}", encoding="utf-8")
    store.initialize_run(
        run_id="run",
        name="demo",
        notebook_path=source,
        source_path=source,
        output_path=root / "out.ipynb",
        working_dir=root,
        run_dir=root,
        source_digest="digest",
        metadata=metadata,
    )


def test_store_preserves_existing_parent_permissions_and_secures_database(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "project-directory"
    parent.mkdir(mode=0o755)
    parent.chmod(0o755)

    store = RunStore(parent / "state.sqlite3")

    assert stat.S_IMODE(parent.stat().st_mode) == 0o755
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    store.close()

    private_parent = tmp_path / "new-private-directory"
    private_store = RunStore(private_parent / "state.sqlite3")

    assert stat.S_IMODE(private_parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(private_store.path.stat().st_mode) == 0o600
    private_store.close()


def test_v2_store_persists_actions_cursors_and_bounded_observations(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_observations_per_resource=100)
    initialize(store, tmp_path)
    event = ResourceEvent(
        resource=ResourceSpec(provider="fake", type="metric", id="x", logical_key="x")
    )
    internal_id, created = store.register_resource(
        run_id="run",
        event=event,
        cell_index=1,
        attempt=2,
        kernel_epoch=3,
        supports_stop=False,
    )
    assert created
    store.save_resource_cursor(internal_id, {"offset": 4})
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(status=ResourceStatus.RUNNING, metrics={"value": 2}),
    )
    action_id = store.create_action("run", ActionKind.RESUME, expected_kernel_epoch=3)
    claimed = store.claim_next_action("run")
    assert claimed and claimed["action_id"] == action_id
    assert store.recover_incomplete_actions("run") == 1
    recovered = store.claim_next_action("run")
    assert recovered and recovered["action_id"] == action_id
    store.finish_action(action_id, ActionStatus.COMPLETED, result={"ok": True})
    snapshot = store.snapshot("run")
    assert snapshot["schema_version"] == 2
    assert snapshot["resources"][0]["cursor"] == {"offset": 4}
    assert snapshot["resources"][0]["observations"][0]["metrics"] == {"value": 2}
    assert snapshot["actions"][0]["result"] == {"ok": True}
    store.close()


def test_observation_history_projection_keeps_rich_current_metrics(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="metric")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(
            status=ResourceStatus.RUNNING,
            metrics={"latest_value": 3.0, "series": [{"timestamp": "t", "value": 3.0}]},
            history_metrics={"latest_value": 3.0},
        ),
    )

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["metrics"]["series"] == [{"timestamp": "t", "value": 3.0}]
    assert store.resource_observations(internal_id)[0]["metrics"] == {
        "latest_value": 3.0
    }
    store.close()


def test_snapshot_loads_all_resource_histories_with_one_query(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    for identifier in ("first", "second"):
        internal_id, _ = store.register_resource(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(provider="fake", type="metric", id=identifier)
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
            supports_stop=False,
        )
        for value in range(5):
            store.update_resource_observation(
                "run",
                internal_id,
                ResourceObservation(
                    status=ResourceStatus.RUNNING, metrics={"value": value}
                ),
            )

    statements: list[str] = []
    store._connection.set_trace_callback(statements.append)  # noqa: SLF001
    snapshot = store.snapshot("run", chart_points=3)
    store._connection.set_trace_callback(None)  # noqa: SLF001

    history_queries = [
        statement
        for statement in statements
        if "FROM resource_observations" in statement
    ]
    assert len(history_queries) == 1
    assert [len(resource["observations"]) for resource in snapshot["resources"]] == [
        3,
        3,
    ]
    for resource in snapshot["resources"]:
        assert resource["observations"][0]["metrics"] == {"value": 0}
        assert resource["observations"][-1]["metrics"] == {"value": 4}
    store.close()


def test_terminal_run_and_event_roll_back_together_on_event_failure(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    store.update_run_status("run", RunStatus.RUNNING, started=True)
    store._connection.execute("""
        CREATE TRIGGER reject_run_succeeded BEFORE INSERT ON events
        WHEN NEW.type = 'run.succeeded'
        BEGIN SELECT RAISE(ABORT, 'injected terminal event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    with pytest.raises(sqlite3.IntegrityError, match="injected terminal event failure"):
        store.finish_run(
            "run",
            RunStatus.SUCCEEDED,
            message="completed",
            event_type="run.succeeded",
            event_payload={"kernel_epoch": 0},
        )

    rolled_back = store.get_run("run")
    assert rolled_back["status"] == RunStatus.RUNNING.value
    assert rolled_back["ended_at"] is None
    assert rolled_back["finalization_complete"] is False
    assert "_terminal_event" not in rolled_back["metadata"]
    assert store.recent_events("run") == []

    store._connection.execute(  # noqa: SLF001 - deliberate fault removal
        "DROP TRIGGER reject_run_succeeded"
    )
    store._connection.commit()  # noqa: SLF001 - deliberate fault removal
    event = store.finish_run(
        "run",
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )

    committed = store.get_run("run")
    assert committed["status"] == RunStatus.SUCCEEDED.value
    assert committed["ended_at"] is not None
    assert committed["finalization_complete"] is False
    assert event["type"] == "run.succeeded"
    assert [item["type"] for item in store.recent_events("run")] == ["run.succeeded"]
    store.close()


@pytest.mark.parametrize(
    ("status", "event_type"),
    [
        (RunStatus.SUCCEEDED, "run.runner_error"),
        (RunStatus.FAILED, "run.succeeded"),
        (RunStatus.CANCELLED, "run.external_timeout"),
        (RunStatus.FAILED, "run.future_failure"),
    ],
)
def test_terminal_status_rejects_unknown_or_mismatched_event_types(
    tmp_path: Path, status: RunStatus, event_type: str
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)

    with pytest.raises(ValueError, match="cannot be committed"):
        store.finish_run(
            "run",
            status,
            message="terminal",
            event_type=event_type,
            event_payload={"kernel_epoch": 99},
        )

    run = store.get_run("run")
    assert run["status"] == RunStatus.CREATED.value
    assert "_terminal_event" not in run["metadata"]
    assert store.recent_events("run") == []
    store.close()


def test_terminal_event_identity_uses_durable_epoch_and_survives_event_retention(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_events_per_run=1)
    initialize(store, tmp_path)
    assert store.begin_kernel_epoch("run") == 1

    terminal = store.finish_run(
        "run",
        RunStatus.FAILED,
        message="timeout",
        event_type="run.external_timeout",
        event_payload={"kernel_epoch": 99},
    )
    assert terminal["payload"]["kernel_epoch"] == 1
    assert terminal["payload"]["projection_truncated"] is True
    assert store.get_run("run")["metadata"]["_terminal_event"] == {
        "type": "run.external_timeout",
        "kernel_epoch": 1,
    }

    store.append_event("run", "run.runner_error", {"kernel_epoch": 1})
    assert [event["type"] for event in store.recent_events("run")] == [
        "run.runner_error"
    ]
    recovered = store.terminal_event_for_state("run", RunStatus.FAILED, 1)
    assert recovered is not None
    assert recovered["type"] == "run.external_timeout"
    assert recovered["payload"] == {"kernel_epoch": 1}

    metadata = store.get_run("run")["metadata"]
    metadata.pop("_terminal_event")
    store._connection.execute(  # noqa: SLF001 - legacy metadata fixture
        "UPDATE runs SET metadata_json = ? WHERE run_id = 'run'",
        (json.dumps(metadata),),
    )
    store._connection.commit()  # noqa: SLF001 - legacy metadata fixture
    legacy = store.terminal_event_for_state("run", RunStatus.FAILED, 1)
    assert legacy is not None and legacy["type"] == "run.runner_error"
    assert store.terminal_event_for_state("run", RunStatus.FAILED, 2) is None
    store.close()


def test_oversized_runner_error_terminalizes_with_bounded_projection(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    canary = "RUNNER_ERROR_TAIL_CANARY"
    huge_error = ("\x00" * 100_000) + canary
    huge_error_type = ("E" * 10_000) + canary

    event = store.finish_run(
        "run",
        RunStatus.FAILED,
        message=f"Runner failure: {huge_error_type}: {huge_error}",
        event_type="run.runner_error",
        event_payload={
            "kernel_epoch": 2**100,
            "error_type": huge_error_type,
            "error": huge_error,
        },
    )

    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 1_024
    assert canary.encode() not in serialized
    assert event["payload"]["projection_truncated"] is True
    run = store.get_run("run")
    assert run["status"] == RunStatus.FAILED.value
    assert len(str(run["message"]).encode("utf-8")) <= 16_384
    store.close()


def test_oversized_external_failure_terminalizes_with_count_and_sample(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    canary = "EXTERNAL_FAILURE_TAIL_CANARY"
    resource_ids = [f"resource-{index}-" + ("r" * 100) for index in range(2_000)]
    resource_ids[-1] += canary

    event = store.finish_run(
        "run",
        RunStatus.FAILED,
        message="Blocking resources failed",
        event_type="run.failed_external",
        event_payload={"kernel_epoch": 3, "resource_ids": resource_ids},
    )

    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 1_024
    assert canary.encode() not in serialized
    assert event["payload"]["failure_count"] == 2_000
    assert len(event["payload"]["resource_ids_sample"]) == 5
    assert event["payload"]["projection_truncated"] is True
    assert store.get_run("run")["status"] == RunStatus.FAILED.value
    store.close()


def test_oversized_success_output_path_uses_bounded_terminal_projection(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    canary = "OUTPUT_PATH_TAIL_CANARY"
    output_path = ("/deep-output" * 10_000) + canary

    event = store.finish_run(
        "run",
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 1, "output_path": output_path},
    )

    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 1_024
    assert canary.encode() not in serialized
    assert event["payload"]["projection_truncated"] is True
    assert store.get_run("run")["status"] == RunStatus.SUCCEEDED.value
    store.close()


def test_failed_cell_run_pause_and_event_roll_back_together_on_event_failure(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    store.initialize_cells(
        "run",
        [
            {
                "cell_index": 0,
                "cell_id": "cell-0",
                "cell_type": "code",
                "source": "raise ValueError('boom')",
                "source_hash": "source-digest",
            }
        ],
    )
    store.update_run_status("run", RunStatus.RUNNING, current_cell_index=0)
    attempt = store.begin_cell_attempt(
        "run",
        0,
        "raise ValueError('boom')",
        "source-digest",
        0,
    )
    store._connection.execute("""
        CREATE TRIGGER reject_cell_failed BEFORE INSERT ON events
        WHEN NEW.type = 'cell.failed'
        BEGIN SELECT RAISE(ABORT, 'injected cell event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    with pytest.raises(sqlite3.IntegrityError, match="injected cell event failure"):
        store.pause_failed_cell(
            "run",
            0,
            attempt=attempt,
            kernel_epoch=0,
            elapsed_seconds=0.5,
            error_name="ValueError",
            error_value="boom",
            traceback=["trace"],
            kernel_dead=False,
        )

    rolled_back = store.snapshot("run")
    assert rolled_back["run"]["status"] == RunStatus.RUNNING.value
    assert rolled_back["run"]["failed_cell_index"] is None
    assert rolled_back["cells"][0]["status"] == CellStatus.RUNNING.value
    assert rolled_back["cells"][0]["error_name"] is None
    assert store.recent_events("run") == []

    store._connection.execute(  # noqa: SLF001 - deliberate fault removal
        "DROP TRIGGER reject_cell_failed"
    )
    store._connection.commit()  # noqa: SLF001 - deliberate fault removal
    event = store.pause_failed_cell(
        "run",
        0,
        attempt=attempt,
        kernel_epoch=0,
        elapsed_seconds=0.5,
        error_name="ValueError",
        error_value="boom",
        traceback=["trace"],
        kernel_dead=False,
    )

    committed = store.snapshot("run")
    assert committed["run"]["status"] == RunStatus.PAUSED.value
    assert committed["run"]["failed_cell_index"] == 0
    assert committed["cells"][0]["status"] == CellStatus.FAILED.value
    assert committed["cells"][0]["error_name"] == "ValueError"
    assert event["type"] == "cell.failed"
    store.close()


def test_oversized_cell_failure_commits_bounded_pause_projection(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    store.initialize_cells(
        "run",
        [
            {
                "cell_index": 0,
                "cell_id": "cell-0",
                "cell_type": "code",
                "source": "raise RuntimeError(detail)",
                "source_hash": "source-digest",
            }
        ],
    )
    store.update_run_status("run", RunStatus.RUNNING, current_cell_index=0)
    attempt = store.begin_cell_attempt(
        "run",
        0,
        "raise RuntimeError(detail)",
        "source-digest",
        0,
    )
    canary = "CELL_FAILURE_TAIL_CANARY"
    huge_detail = ("x" * 100_000) + canary
    traceback = [f"frame-{index}" for index in range(100)] + [
        ("\x00" * 100_000) + canary
    ]

    event = store.pause_failed_cell(
        "run",
        0,
        attempt=attempt,
        kernel_epoch=0,
        elapsed_seconds=0.5,
        error_name=("RuntimeError" * 1_000) + canary,
        error_value=huge_detail,
        traceback=traceback,
        kernel_dead=False,
    )

    snapshot = store.snapshot("run")
    assert snapshot["run"]["status"] == RunStatus.PAUSED.value
    assert snapshot["cells"][0]["status"] == CellStatus.FAILED.value
    assert len(snapshot["cells"][0]["error_value"].encode("utf-8")) <= 16_384
    assert len(snapshot["cells"][0]["traceback"]) == 50
    assert all(
        len(json.dumps(line, ensure_ascii=False).encode("utf-8")) <= 4_096
        for line in snapshot["cells"][0]["traceback"]
    )
    failures = [item for item in snapshot["events"] if item["type"] == "cell.failed"]
    assert len(failures) == 1
    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 1_024
    assert canary.encode() not in serialized
    assert event["payload"]["projection_truncated"] is True
    store.close()


def test_resource_observation_and_event_roll_back_together_on_event_failure(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="logs", id="stream")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.save_resource_cursor(internal_id, {"token": "page-1"})
    store._connection.execute("""
        CREATE TRIGGER reject_resource_observed BEFORE INSERT ON events
        WHEN NEW.type = 'resource.observed'
        BEGIN SELECT RAISE(ABORT, 'injected resource event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault
    observation = ResourceObservation(
        status=ResourceStatus.COMPLETED,
        terminal=True,
        metrics={"page": 2},
        log_lines=["page-2"],
    )

    with pytest.raises(sqlite3.IntegrityError, match="injected resource event failure"):
        store.record_resource_inspection(
            "run", internal_id, observation, {"token": "page-2"}
        )

    rolled_back = store.get_resource(internal_id)
    assert rolled_back is not None
    assert rolled_back["status"] == ResourceStatus.REGISTERED.value
    assert rolled_back["terminal"] is False
    assert rolled_back["cursor"] == {"token": "page-1"}
    assert rolled_back["log_tail"] == []
    assert store.resource_observations(internal_id) == []
    assert store.recent_events("run") == []

    store._connection.execute(  # noqa: SLF001 - deliberate fault removal
        "DROP TRIGGER reject_resource_observed"
    )
    store._connection.commit()  # noqa: SLF001 - deliberate fault removal
    event = store.record_resource_inspection(
        "run", internal_id, observation, {"token": "page-2"}
    )

    committed = store.get_resource(internal_id)
    assert committed is not None
    assert committed["status"] == ResourceStatus.COMPLETED.value
    assert committed["terminal"] is True
    assert committed["cursor"] == {"token": "page-2"}
    assert committed["log_tail"] == ["page-2"]
    assert len(store.resource_observations(internal_id)) == 1
    assert event["type"] == "resource.observed"
    store.close()


def test_resource_registration_event_uses_bounded_allowlisted_projection(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    secret = "REGISTRATION_METADATA_CANARY" * 4_000
    registration = ResourceEvent(
        event_id="event-" + ("e" * 500),
        resource=ResourceSpec(
            provider="provider-" + ("p" * 500),
            type="type-" + ("t" * 500),
            id="identifier-" + ("i" * 500),
            logical_key="logical-" + ("l" * 500),
            metadata={"secret": secret},
        ),
    )

    internal_id, created, event = store.register_resource_with_event(
        run_id="run",
        event=registration,
        cell_index=1,
        attempt=2,
        kernel_epoch=3,
        supports_stop=False,
    )

    assert created is True
    assert event["type"] == "resource.registered"
    assert event["payload"]["projection_truncated"] is True
    assert "metadata" not in event["payload"]["resource"]
    serialized = json.dumps(event["payload"], ensure_ascii=False).encode("utf-8")
    assert len(serialized) <= 1_024
    assert secret.encode() not in serialized
    resource = store.get_resource(internal_id)
    assert resource is not None and resource["metadata"] == {"secret": secret}

    duplicate_id, duplicate_created, reconciled = store.register_resource_with_event(
        run_id="run",
        event=registration,
        cell_index=1,
        attempt=2,
        kernel_epoch=3,
        supports_stop=False,
    )
    assert duplicate_id == internal_id
    assert duplicate_created is False
    assert reconciled["type"] == "resource.reconciled"
    assert len(store.list_resources("run")) == 1
    store.close()


def test_resource_registration_projection_bounds_json_escaped_control_characters(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=1_024)
    initialize(store, tmp_path)
    controls = "\x00\x01\n\t" * 1_000
    registration = ResourceEvent(
        event_id="event-" + controls,
        resource=ResourceSpec(
            provider="provider-" + controls,
            type="type-" + controls,
            id="identifier-" + controls,
            logical_key="logical-" + controls,
        ),
    )

    internal_id, created, event = store.register_resource_with_event(
        run_id="run",
        event=registration,
        cell_index=1,
        attempt=2,
        kernel_epoch=3,
        supports_stop=False,
    )

    assert created is True
    assert event["payload"]["projection_truncated"] is True
    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 1_024
    for value in (
        event["payload"]["event_id"],
        event["payload"]["resource"]["provider"],
        event["payload"]["resource"]["type"],
        event["payload"]["resource"]["id"],
        event["payload"]["resource"]["logical_key"],
    ):
        assert len(json.dumps(value, ensure_ascii=False).encode("utf-8")) <= 64
    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["external_id"] == registration.resource.id
    store.close()


def test_new_resource_rolls_back_when_bounded_event_exceeds_cap(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_event_payload_bytes=256)
    initialize(store, tmp_path)

    with pytest.raises(ValueError, match="max_event_payload_bytes"):
        store.register_resource_with_event(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(provider="fake", type="job", id="new")
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
            supports_stop=False,
        )

    assert store.list_resources("run") == []
    assert store.recent_events("run") == []
    store.close()


def test_reconciled_resource_rolls_back_when_event_append_fails(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _created = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            event_id="original-event",
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="same-job",
                logical_key="logical-job",
                metadata={"revision": 1},
            ),
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=1,
        supports_stop=False,
    )
    before = store.get_resource(internal_id)
    assert before is not None
    store._connection.execute("""
        CREATE TRIGGER reject_resource_reconciled BEFORE INSERT ON events
        WHEN NEW.type = 'resource.reconciled'
        BEGIN SELECT RAISE(ABORT, 'injected reconcile event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    with pytest.raises(sqlite3.IntegrityError, match="reconcile event failure"):
        store.register_resource_with_event(
            run_id="run",
            event=ResourceEvent(
                event_id="replacement-event",
                resource=ResourceSpec(
                    provider="fake",
                    type="job",
                    id="same-job",
                    logical_key="logical-job",
                    metadata={"revision": 2},
                ),
            ),
            cell_index=4,
            attempt=5,
            kernel_epoch=6,
            supports_stop=True,
        )

    assert store.get_resource(internal_id) == before
    assert store.recent_events("run") == []
    store.close()


def test_superseded_resource_and_replacement_roll_back_with_event_failure(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    original_id, _created = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="job", id="old", logical_key="logical-job"
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    before = store.get_resource(original_id)
    assert before is not None
    store._connection.execute("""
        CREATE TRIGGER reject_resource_registered BEFORE INSERT ON events
        WHEN NEW.type = 'resource.registered'
        BEGIN SELECT RAISE(ABORT, 'injected registration event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    with pytest.raises(sqlite3.IntegrityError, match="registration event failure"):
        store.register_resource_with_event(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(
                    provider="fake",
                    type="job",
                    id="replacement",
                    logical_key="logical-job",
                )
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
            supports_stop=False,
        )

    assert store.list_resources("run") == [before]
    assert store.recent_events("run") == []
    store.close()


def test_resource_stop_intent_and_event_roll_back_together_on_event_failure(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="job")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=True,
    )
    before = store.get_resource(internal_id)
    assert before is not None
    store._connection.execute("""
        CREATE TRIGGER reject_resource_stop BEFORE INSERT ON events
        WHEN NEW.type = 'resource.stop_requested'
        BEGIN SELECT RAISE(ABORT, 'injected stop event failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    with pytest.raises(sqlite3.IntegrityError, match="injected stop event failure"):
        store.request_resource_stop_with_event(
            internal_id, ResourceDisposition.CANCELLED
        )

    rolled_back = store.get_resource(internal_id)
    assert rolled_back is not None
    assert rolled_back["status"] == ResourceStatus.REGISTERED.value
    assert rolled_back["disposition"] == ResourceDisposition.ACTIVE.value
    assert rolled_back["version"] == before["version"]
    assert store.recent_events("run") == []

    store._connection.execute(  # noqa: SLF001 - deliberate fault removal
        "DROP TRIGGER reject_resource_stop"
    )
    store._connection.commit()  # noqa: SLF001 - deliberate fault removal
    committed, event = store.request_resource_stop_with_event(
        internal_id, ResourceDisposition.CANCELLED
    )

    assert committed["status"] == ResourceStatus.STOPPING.value
    assert committed["disposition"] == ResourceDisposition.ACTIVE.value
    assert committed["version"] == before["version"] + 1
    assert event is not None and event["type"] == "resource.stop_requested"
    store.close()


def test_update_run_status_rejects_terminal_transitions(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)

    for status in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED):
        with pytest.raises(ValueError, match="finish_run"):
            store.update_run_status("run", status)

    assert store.get_run("run")["status"] == RunStatus.CREATED.value
    assert store.recent_events("run") == []
    store.close()


def test_v2_database_migration_keeps_existing_terminal_runs_unfinalized(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path)
    store._connection.execute(  # noqa: SLF001 - construct legacy schema fixture
        "UPDATE runs SET status = ?, ended_at = ? WHERE run_id = 'run'",
        (RunStatus.SUCCEEDED.value, "2026-01-01T00:00:00+00:00"),
    )
    store._connection.execute(  # noqa: SLF001 - construct legacy schema fixture
        "ALTER TABLE runs DROP COLUMN finalized_at"
    )
    store._connection.execute(  # noqa: SLF001 - construct legacy schema fixture
        "ALTER TABLE runs DROP COLUMN finalization_complete"
    )
    store._connection.execute(  # noqa: SLF001 - construct legacy schema fixture
        "UPDATE runwatch_meta SET value = '2' WHERE key = 'schema_version'"
    )
    store._connection.commit()  # noqa: SLF001 - construct legacy schema fixture
    store.close()

    migrated = RunStore(path)
    run = migrated.get_run("run")

    assert run["status"] == RunStatus.SUCCEEDED.value
    assert run["ended_at"] == "2026-01-01T00:00:00+00:00"
    assert run["finalization_complete"] is False
    assert run["finalized_at"] is None
    version = migrated._connection.execute(  # noqa: SLF001 - schema assertion
        "SELECT value FROM runwatch_meta WHERE key = 'schema_version'"
    ).fetchone()
    assert version is not None and version["value"] == "3"
    migrated.close()


def test_terminal_stop_inspection_is_one_atomic_recoverable_commit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="logs", id="stream")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.save_resource_cursor(internal_id, {"token": "page-1"})
    store._connection.execute("""
        CREATE TRIGGER reject_observation BEFORE INSERT ON resource_observations
        BEGIN SELECT RAISE(ABORT, 'injected observation failure'); END
        """)  # noqa: SLF001 - deliberate transaction fault
    store._connection.commit()  # noqa: SLF001 - deliberate transaction fault

    observation = ResourceObservation(
        status=ResourceStatus.STOPPED,
        terminal=True,
        metrics={"page": 2},
        log_lines=["line-from-page-2"],
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected observation failure"):
        store.record_resource_stop_inspection(
            "run",
            internal_id,
            observation,
            {"token": "page-2"},
            ResourceDisposition.CANCELLED,
        )
    store.close()

    recovered = RunStore(path)
    resource = recovered.get_resource(internal_id)
    assert resource is not None
    assert resource["cursor"] == {"token": "page-1"}
    assert resource["status"] == "registered"
    assert resource["terminal"] is False
    assert resource["disposition"] == "active"
    assert resource["log_tail"] == []
    assert recovered.resource_observations(internal_id) == []

    recovered._connection.execute(  # noqa: SLF001 - deliberate fault removal
        "DROP TRIGGER reject_observation"
    )
    recovered._connection.commit()  # noqa: SLF001 - deliberate fault removal
    recovered.record_resource_stop_inspection(
        "run",
        internal_id,
        observation,
        {"token": "page-2"},
        ResourceDisposition.CANCELLED,
    )
    committed = recovered.get_resource(internal_id)
    assert committed is not None
    assert committed["cursor"] == {"token": "page-2"}
    assert committed["log_tail"] == ["line-from-page-2"]
    assert committed["status"] == "stopped"
    assert committed["terminal"] is True
    assert committed["disposition"] == "cancelled"
    assert committed["monitor_closed"] is False
    assert len(recovered.resource_observations(internal_id)) == 1
    recovered.close()


def test_stop_request_atomically_confirms_a_prior_terminal_monitor_observation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="finished")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=True,
    )
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(status=ResourceStatus.STOPPED, terminal=True),
    )
    before = store.get_resource(internal_id)
    assert before is not None and before["disposition"] == "active"

    requested = store.request_resource_stop(internal_id, ResourceDisposition.CANCELLED)
    assert requested["status"] == "stopped"
    assert requested["terminal"] is True
    assert requested["disposition"] == "cancelled"
    assert requested["monitor_closed"] is False
    store.close()

    reopened = RunStore(path)
    confirmed = reopened.get_resource(internal_id)
    assert confirmed is not None and confirmed["disposition"] == "cancelled"
    reopened.close()


def test_v01_database_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "old.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()
    with pytest.raises(UnsupportedRunSchema, match=r"0\.1 schema"):
        RunStore(path)


def test_resource_event_conflict_is_transactionally_fail_closed(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    first = ResourceEvent(
        event_id="event-a",
        resource=ResourceSpec(
            provider="fake", type="job", id="job-a", logical_key="build"
        ),
    )
    unrelated = ResourceEvent(
        event_id="event-b",
        resource=ResourceSpec(
            provider="fake", type="job", id="job-b", logical_key="other"
        ),
    )
    first_id, _ = store.register_resource(
        run_id="run",
        event=first,
        cell_index=0,
        attempt=1,
        kernel_epoch=1,
        supports_stop=False,
    )
    store.register_resource(
        run_id="run",
        event=unrelated,
        cell_index=0,
        attempt=1,
        kernel_epoch=1,
        supports_stop=False,
    )

    conflict = ResourceEvent(
        event_id="event-b",
        resource=ResourceSpec(
            provider="fake", type="job", id="job-c", logical_key="build"
        ),
    )
    with pytest.raises(ResourceEventConflict, match="different identity"):
        store.register_resource(
            run_id="run",
            event=conflict,
            cell_index=0,
            attempt=2,
            kernel_epoch=2,
            supports_stop=False,
        )

    store.append_event("run", "commit-probe", {})
    assert store.get_resource(first_id)["disposition"] == "active"  # type: ignore[index]
    assert len(store.list_resources("run")) == 2
    store.close()


def test_observation_metrics_do_not_churn_stop_control_version(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="x")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    original = store.get_resource(internal_id)
    assert original is not None
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(status=ResourceStatus.RUNNING, metrics={"value": 1}),
    )
    running = store.get_resource(internal_id)
    assert running is not None and running["version"] == original["version"] + 1
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(status=ResourceStatus.RUNNING, metrics={"value": 2}),
    )
    metric_only = store.get_resource(internal_id)
    assert metric_only is not None and metric_only["version"] == running["version"]
    store.close()


def test_dashboard_observations_are_downsampled_across_full_history(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="x")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    for value in range(10):
        store.update_resource_observation(
            "run",
            internal_id,
            ResourceObservation(
                status=ResourceStatus.RUNNING, metrics={"value": value}
            ),
        )
    points = store.snapshot("run", chart_points=3)["resources"][0]["observations"]
    assert len(points) == 3
    assert points[0]["metrics"]["value"] == 0
    assert points[-1]["metrics"]["value"] == 9
    with pytest.raises(ValueError, match="at least 2"):
        store.downsampled_resource_observations(internal_id, 1)
    store.close()


def test_events_are_bounded_and_replayable(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_events_per_run=3)
    initialize(store, tmp_path)
    for value in range(6):
        store.append_event("run", "probe", {"value": value})
    events = store.recent_events("run", limit=20)
    assert [event["payload"]["value"] for event in events] == [3, 4, 5]
    assert [event["payload"]["value"] for event in store.events_after("run", 4)] == [
        4,
        5,
    ]
    store.close()


def test_histories_and_log_tail_are_bounded_by_encoded_bytes(tmp_path: Path) -> None:
    store = RunStore(
        tmp_path / "state.sqlite3",
        max_observations_per_resource=100,
        max_observation_bytes_per_resource=90,
        max_log_lines_per_resource=100,
        max_log_bytes_per_resource=10,
        max_events_per_run=100,
        max_event_bytes_per_run=90,
    )
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="bytes")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    for value in range(5):
        store.update_resource_observation(
            "run",
            internal_id,
            ResourceObservation(
                status=ResourceStatus.RUNNING,
                metrics={"value": value, "blob": "x" * 40},
                log_lines=[f"line-{value}-long"],
            ),
        )
        store.append_event("run", "probe", {"value": value, "blob": "x" * 40})

    observations = store.resource_observations(internal_id, limit=100)
    events = store.recent_events("run", limit=100)
    resource = store.get_resource(internal_id)
    assert observations[-1]["metrics"]["value"] == 4
    assert events[-1]["payload"]["value"] == 4
    assert len(observations) < 5
    assert len(events) < 5
    assert resource is not None
    assert sum(len(line.encode("utf-8")) for line in resource["log_tail"]) <= 10
    assert resource["log_tail"] == ["ine-4-long"]
    store.close()


def test_resource_payload_byte_limit_rejects_oversized_metadata(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_resource_payload_bytes=256)
    initialize(store, tmp_path)
    with pytest.raises(ValueError, match="max_resource_payload_bytes"):
        store.register_resource(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(
                    provider="fake",
                    type="metric",
                    id="oversized",
                    metadata={"blob": "x" * 1_000},
                )
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
            supports_stop=False,
        )
    assert store.list_resources("run") == []
    store.close()


def test_notification_event_cursor_is_durable_and_monotonic(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path, metadata={"operator_note": "preserve me"})
    first = store.append_event("run", "first", {})
    second = store.append_event("run", "second", {})

    assert store.notification_event_cursor("run") == 0
    assert store.advance_notification_event_cursor("run", int(first["seq"]))
    assert not store.advance_notification_event_cursor("run", int(first["seq"]))
    assert not store.advance_notification_event_cursor("run", 0)
    assert store.notification_event_cursor("run") == first["seq"]
    assert store.advance_notification_event_cursor("run", int(second["seq"]))
    assert store.get_run("run")["metadata"]["operator_note"] == "preserve me"
    store.close()

    reopened = RunStore(path)
    assert reopened.notification_event_cursor("run") == second["seq"]
    reopened.close()


@pytest.mark.parametrize("sequence", [True, False, 1.5, "1", None])
def test_notification_event_cursor_writer_rejects_non_integer_sequences(
    tmp_path: Path,
    sequence: Any,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path, metadata={"operator_note": "preserve me"})

    with pytest.raises(ValueError, match="nonnegative"):
        store.advance_notification_event_cursor("run", sequence)
    assert store.notification_event_cursor("run") == 0
    assert store.get_run("run")["metadata"] == {"operator_note": "preserve me"}
    store.close()


def test_notification_event_cursor_is_monotonic_across_store_connections(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    higher_store = RunStore(path)
    initialize(
        higher_store,
        tmp_path,
        metadata={"notifications": {"enabled": True}, "operator_note": "keep"},
    )
    lower_store = RunStore(path)
    higher_finished = threading.Event()

    def advance_higher() -> bool:
        try:
            return higher_store.advance_notification_event_cursor("run", 20)
        finally:
            higher_finished.set()

    def advance_lower() -> bool:
        assert higher_finished.wait(timeout=1)
        return lower_store.advance_notification_event_cursor("run", 10)

    with ThreadPoolExecutor(max_workers=2) as executor:
        higher = executor.submit(advance_higher)
        lower = executor.submit(advance_lower)
        assert higher.result(timeout=1)
        assert not lower.result(timeout=1)

    assert higher_store.notification_event_cursor("run") == 20
    assert lower_store.notification_event_cursor("run") == 20
    assert lower_store.get_run("run")["metadata"] == {
        "notifications": {"enabled": True},
        "operator_note": "keep",
        "_notification_event_cursor": 20,
    }
    lower_store.close()
    higher_store.close()


def test_notification_event_cursor_rejects_unknown_run_and_negative_sequence(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)

    with pytest.raises(KeyError, match="Unknown run missing"):
        store.notification_event_cursor("missing")
    with pytest.raises(KeyError, match="Unknown run missing"):
        store.advance_notification_event_cursor("missing", 1)
    with pytest.raises(ValueError, match="nonnegative"):
        store.advance_notification_event_cursor("run", -1)
    assert store.notification_event_cursor("run") == 0
    store.close()


@pytest.mark.parametrize("cursor", [True, -1, 1.5, "later"])
def test_notification_event_cursor_rejects_invalid_persisted_values(
    tmp_path: Path,
    cursor: object,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    store._connection.execute(  # noqa: SLF001 - deliberate corruption probe
        "UPDATE runs SET metadata_json = ? WHERE run_id = 'run'",
        (json.dumps({"_notification_event_cursor": cursor}),),
    )
    store._connection.commit()  # noqa: SLF001 - deliberate corruption probe

    with pytest.raises(CorruptRunState, match="cursor is invalid"):
        store.notification_event_cursor("run")
    store.close()


def test_notification_routing_failures_persist_and_dead_letter_once(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path, max_event_payload_bytes=256)
    initialize(store, tmp_path)
    store.require_notification_event_routing("run")
    source = store.append_event("run", "probe.poison", {"value": 1})
    source_type = "probe.\x00" * 100
    error_type = "Unexpected\nError" * 100

    first = store.record_notification_routing_failure(
        "run", source["seq"], source_type, error_type, max_attempts=2
    )
    assert first == {"attempt": 1, "dead_lettered": False, "event": None}
    failure = store.get_run("run")["metadata"]["_notification_routing_failure"]
    assert failure["event_seq"] == source["seq"]
    assert failure["attempt"] == 1
    assert (
        len(
            json.dumps(failure["source_event_type"], ensure_ascii=False).encode("utf-8")
        )
        <= 64
    )
    assert (
        len(json.dumps(failure["error_type"], ensure_ascii=False).encode("utf-8")) <= 64
    )
    store.close()

    recovered = RunStore(path, max_event_payload_bytes=256)
    second = recovered.record_notification_routing_failure(
        "run", source["seq"], source_type, error_type, max_attempts=2
    )
    assert second["attempt"] == 2
    assert second["dead_lettered"] is True
    dead_letter = second["event"]
    assert dead_letter is not None
    assert dead_letter["type"] == "notification.event_dead_lettered"
    assert dead_letter["payload"]["event_seq"] == source["seq"]
    assert dead_letter["payload"]["projection_truncated"] is True
    assert (
        len(
            json.dumps(
                dead_letter["payload"], ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        )
        <= 256
    )
    assert recovered.notification_event_cursor("run") == source["seq"]
    assert "_notification_routing_failure" not in recovered.get_run("run")["metadata"]
    assert [
        event["type"]
        for event in recovered.recent_events("run")
        if event["type"] == "notification.event_dead_lettered"
    ] == ["notification.event_dead_lettered"]

    duplicate = recovered.record_notification_routing_failure(
        "run", source["seq"], source_type, error_type, max_attempts=2
    )
    assert duplicate == {"attempt": 0, "dead_lettered": False, "event": None}

    assert recovered.advance_notification_event_cursor("run", dead_letter["seq"])
    next_event = recovered.append_event("run", "probe.next", {})
    recovered.record_notification_routing_failure(
        "run", next_event["seq"], "probe.next", "RuntimeError", max_attempts=3
    )
    assert "_notification_routing_failure" in recovered.get_run("run")["metadata"]
    assert recovered.advance_notification_event_cursor("run", next_event["seq"])
    assert "_notification_routing_failure" not in recovered.get_run("run")["metadata"]
    recovered.close()


def test_corrupt_json_and_incomplete_v2_schema_are_rejected(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    store._connection.execute(  # noqa: SLF001 - deliberate corruption probe
        "UPDATE runs SET metadata_json = '{' WHERE run_id = 'run'"
    )
    store._connection.commit()  # noqa: SLF001 - deliberate corruption probe
    with pytest.raises(CorruptRunState, match="JSON"):
        store.get_run("run")
    store.close()

    path = tmp_path / "incomplete.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE runwatch_meta (key TEXT PRIMARY KEY, value TEXT)")
    connection.execute("INSERT INTO runwatch_meta VALUES ('schema_version', '2')")
    connection.commit()
    connection.close()
    with pytest.raises(CorruptRunState, match="missing required"):
        RunStore(path)


def test_process_identity_rejects_reused_pid_identity() -> None:
    started_at = process_start_time(os.getpid())
    assert started_at is not None
    assert process_is_alive(os.getpid(), started_at)
    assert not process_is_alive(os.getpid(), started_at - 100)


def test_action_recovers_after_abrupt_controller_process_exit(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path)
    action_id = store.create_action("run", ActionKind.RESUME)
    store.close()

    script = """
import os
import sys
from pathlib import Path
from runwatch.storage import RunStore
store = RunStore(Path(sys.argv[1]))
assert store.claim_action(sys.argv[2]) is not None
os._exit(17)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), action_id],
        check=False,
        env=os.environ.copy(),
    )
    assert result.returncode == 17

    recovered = RunStore(path)
    assert recovered.recover_incomplete_actions("run") == 1
    action = recovered.get_action(action_id)
    assert action is not None
    assert action["status"] == ActionStatus.REQUESTED.value
    assert action["payload"]["recovered"] is True
    recovered.close()


def test_full_resource_identity_and_standalone_cursor_use_utf8_byte_cap(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_resource_payload_bytes=1_024)
    initialize(store, tmp_path)

    with pytest.raises(ValueError, match="Resource registration.*max_resource"):
        store.register_resource(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(
                    provider="fake",
                    type="metric",
                    id="é" * 600,
                )
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
            supports_stop=False,
        )
    assert store.list_resources("run") == []

    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="small")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.save_resource_cursor(internal_id, {"offset": 1})
    with pytest.raises(ValueError, match="Resource cursor.*max_resource"):
        store.save_resource_cursor(internal_id, {"token": "é" * 600})
    assert store.resource_cursor(internal_id) == {"offset": 1}
    store.close()


@pytest.mark.parametrize(
    "field",
    ["message", "metrics", "history_metrics", "raw", "log_lines"],
)
def test_aggregate_observation_cap_covers_every_persisted_payload(
    tmp_path: Path,
    field: str,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_resource_payload_bytes=1_024)
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="small")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.save_resource_cursor(internal_id, {"offset": 1})
    values: dict[str, Any] = {
        "status": ResourceStatus.RUNNING,
        field: "é" * 600 if field == "message" else {"blob": "é" * 600},
    }
    if field == "log_lines":
        values[field] = ["é" * 600]
    observation = ResourceObservation.model_validate(values)

    with pytest.raises(ValueError, match="Resource observation.*max_resource"):
        store.record_resource_inspection("run", internal_id, observation, {"offset": 2})

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["status"] == ResourceStatus.REGISTERED.value
    assert resource["cursor"] == {"offset": 1}
    assert resource["metrics"] == {}
    assert resource["raw"] == {}
    assert resource["log_tail"] == []
    assert store.resource_observations(internal_id) == []
    assert store.recent_events("run") == []
    store.close()


def test_large_valid_resource_observation_uses_small_event_projection(
    tmp_path: Path,
) -> None:
    store = RunStore(
        tmp_path / "state.sqlite3",
        max_resource_payload_bytes=8_192,
        max_event_payload_bytes=256,
    )
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="small")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.save_resource_cursor(internal_id, {"offset": 1})
    observation = ResourceObservation(
        status=ResourceStatus.RUNNING,
        message="é" * 200,
        metrics={"value": 2, "detail": "m" * 1_000},
        log_lines=["log-" + ("l" * 1_000)],
        raw={"provider_detail": "r" * 1_000},
    )

    event = store.record_resource_inspection(
        "run", internal_id, observation, {"offset": 2}
    )

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["status"] == ResourceStatus.RUNNING.value
    assert resource["cursor"] == {"offset": 2}
    assert resource["metrics"] == observation.metrics
    assert resource["raw"] == observation.raw
    assert resource["log_tail"] == observation.log_lines
    assert len(store.resource_observations(internal_id)) == 1
    assert event["payload"]["internal_id"] == internal_id
    assert event["payload"]["metric_count"] == 2
    assert event["payload"]["new_log_line_count"] == 1
    assert event["payload"]["projection_truncated"] is True
    assert "metrics" not in event["payload"]
    assert "new_log_lines" not in event["payload"]
    serialized = json.dumps(
        event["payload"], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    assert len(serialized) <= 256

    with pytest.raises(ValueError, match="max_event_payload_bytes"):
        store.append_event("run", "probe", {"text": "é" * 200})
    assert [item["type"] for item in store.recent_events("run")] == [
        "resource.observed"
    ]
    store.close()


@pytest.mark.parametrize("field", ["title", "message", "data", "destinations"])
def test_notification_record_cap_rejects_all_columns_atomically(
    tmp_path: Path,
    field: str,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_notification_record_bytes=512)
    initialize(store, tmp_path)
    title = "Runwatch"
    message = "status"
    data: dict[str, Any] = {"kind": "status"}
    destinations = [("webhook", "https://hooks.example/runwatch")]
    if field == "title":
        title = "é" * 400
    elif field == "message":
        message = "é" * 400
    elif field == "data":
        data = {"blob": "é" * 400}
    else:
        destinations = [("webhook", "https://hooks.example/" + "é" * 400)]

    with pytest.raises(ValueError, match="max_notification_record_bytes"):
        store.enqueue_notification(
            run_id="run",
            title=title,
            message=message,
            data=data,
            dedup_key="status",
            destinations=destinations,
        )

    assert store.notification_outbox_state("run") == {
        "nonterminal_intents": 0,
        "nonterminal_deliveries": 0,
        "pending_deliveries": 0,
        "sending_deliveries": 0,
    }
    store.close()


def test_failed_notification_can_be_observed_without_rearming(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    destinations = [("webhook", "https://hooks.example/runwatch")]
    intent = store.enqueue_notification(
        run_id="run",
        title="Original",
        message="Original failure",
        data={"reason": "original"},
        dedup_key="terminal",
        destinations=destinations,
    )
    claimed = store.claim_due_notification_deliveries("run")
    assert len(claimed) == 1
    store.finish_notification_delivery(
        claimed[0]["delivery_id"],
        succeeded=False,
        max_attempts=1,
        retry_delay_seconds=1,
        error="failed",
    )
    failed = store.notification_intent(intent["intent_id"])
    assert failed is not None and failed["status"] == "failed"

    unchanged = store.enqueue_notification(
        run_id="run",
        title="Replacement",
        message="Replacement failure",
        data={"reason": "replacement"},
        dedup_key="terminal",
        destinations=destinations,
        rearm_failed=False,
    )
    assert unchanged["created"] is False
    assert unchanged["rearmed"] is False
    assert unchanged["status"] == "failed"
    assert unchanged["title"] == "Original"
    assert unchanged["data"] == {"reason": "original"}
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["status"] == "failed"
    assert delivery["attempt_count"] == 1

    rearmed = store.enqueue_notification(
        run_id="run",
        title="Replacement",
        message="Replacement failure",
        data={"reason": "replacement"},
        dedup_key="terminal",
        destinations=destinations,
    )
    assert rearmed["rearmed"] is True
    assert rearmed["status"] == "pending"
    assert rearmed["title"] == "Replacement"
    assert rearmed["data"] == {"reason": "replacement"}
    store.close()


def test_delivery_errors_are_truncated_to_valid_utf8_bytes(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_delivery_error_bytes=17)
    initialize(store, tmp_path)
    intent = store.enqueue_notification(
        run_id="run",
        title="Runwatch",
        message="failed",
        data={"kind": "failure"},
        dedup_key="failure",
        destinations=[
            ("webhook", "https://hooks.example/one"),
            ("webhook", "https://hooks.example/two"),
        ],
    )
    claimed = store.claim_due_notification_deliveries("run")
    assert len(claimed) == 2

    store.finish_notification_delivery(
        claimed[0]["delivery_id"],
        succeeded=False,
        max_attempts=1,
        retry_delay_seconds=1,
        error="💥" * 20,
    )
    store.recover_claimed_notification_delivery(
        claimed[1]["delivery_id"],
        max_attempts=1,
        retry_delay_seconds=1,
        error="🔥" * 20,
    )

    deliveries = store.notification_deliveries(intent["intent_id"])
    assert len(deliveries) == 2
    for delivery in deliveries:
        error = delivery["last_error"]
        assert isinstance(error, str)
        assert len(error.encode("utf-8")) <= 17
        assert "�" not in error
    store.close()


def test_terminal_dedup_lookup_prefers_any_succeeded_legacy_alias(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3")
    initialize(store, tmp_path)
    keys = ("run-terminal:succeeded:3", "run-succeeded:3")
    intents = [
        store.enqueue_notification(
            run_id="run",
            title="terminal",
            message="terminal",
            data={},
            dedup_key=key,
            destinations=[("webhook", "https://hooks.example/terminal")],
        )
        for key in keys
    ]
    with store._lock:
        store._connection.execute(
            "UPDATE notification_intents SET status = 'failed' WHERE intent_id = ?",
            (intents[0]["intent_id"],),
        )
        store._connection.execute(
            "UPDATE notification_intents SET status = 'succeeded' WHERE intent_id = ?",
            (intents[1]["intent_id"],),
        )
        store._connection.commit()

    assert store.existing_notification_dedup_key("run", keys) == "run-succeeded:3"
    store.close()


def test_repeated_crash_recovery_does_not_refund_notification_attempts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    store = RunStore(path)
    initialize(store, tmp_path)
    intent = store.enqueue_notification(
        run_id="run",
        title="Runwatch",
        message="ambiguous delivery",
        data={},
        dedup_key="crash-loop",
        destinations=[("webhook", "https://hooks.example/ambiguous")],
    )
    delivery_id: str | None = None

    for attempt in range(1, 4):
        claimed = store.claim_due_notification_deliveries("run")
        assert len(claimed) == 1
        claimed_delivery_id = str(claimed[0]["delivery_id"])
        if delivery_id is None:
            delivery_id = claimed_delivery_id
        assert claimed_delivery_id == delivery_id
        assert claimed[0]["intent_id"] == intent["intent_id"]
        assert claimed[0]["attempt_count"] == attempt
        store.close()  # Simulate a crash after the destination may have accepted it.

        store = RunStore(path)
        assert (
            store.recover_notification_deliveries(
                "run",
                max_attempts=3,
                error="ambiguous accepted-before-crash outcome",
            )
            == 1
        )
        delivery = store.notification_deliveries(intent["intent_id"])[0]
        assert delivery["delivery_id"] == delivery_id
        assert delivery["attempt_count"] == attempt
        assert delivery["status"] == ("failed" if attempt == 3 else "pending")

    assert store.claim_due_notification_deliveries("run") == []
    assert (
        store.recover_notification_deliveries(
            "run", max_attempts=3, error="must not rearm a terminal delivery"
        )
        == 0
    )
    terminal = store.notification_intent(intent["intent_id"])
    assert terminal is not None
    assert terminal["status"] == "failed"
    store.close()


def test_log_tail_byte_truncation_never_introduces_replacement_overflow(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "state.sqlite3", max_log_bytes_per_resource=4)
    initialize(store, tmp_path)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="metric", id="small")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(
            status=ResourceStatus.RUNNING,
            log_lines=["a🙂b"],
        ),
    )

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert sum(len(line.encode("utf-8")) for line in resource["log_tail"]) <= 4
    assert "�" not in "".join(resource["log_tail"])
    store.close()

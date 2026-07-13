# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
import os
import sqlite3
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
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
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
    assert committed["monitor_closed"] is True
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
    assert requested["monitor_closed"] is True
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

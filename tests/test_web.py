# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import nbformat
import pytest
from fastapi.testclient import TestClient

from runwatch.dashboard_links import DashboardLinkManager
from runwatch.models import (
    ActionKind,
    NotificationSettings,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceLifecycle,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunStatus,
    RunwatchConfig,
)
from runwatch.supervisor import RunSupervisor
from runwatch.web import (
    _dashboard_event_signal,
    _delivery_batch,
    _event_stream,
    _events_after,
    _ntfy_deep_link,
    create_app,
)

_WEB_ARTIFACTS = Path(__file__).resolve().parents[1] / "web_artifacts"


@pytest.mark.asyncio
async def test_linked_dashboard_open_is_authenticated_and_snapshot_is_secret_free(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    internal_id, _created = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="local",
                type="dashboard",
                id="http://127.0.0.1:8501",
                logical_key="training-ui",
                ownership=Ownership.EXTERNAL,
                metadata={"name": "Training UI"},
            ),
            lifecycle=ResourceLifecycle(blocking=False),
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=False,
    )
    links = DashboardLinkManager(
        access_token="secret-token",
        share="none",
        cloudflared_binary="cloudflared",
        bus=supervisor.bus,
    )
    supervisor.attach_dashboard_links(links)
    await links.reconcile(supervisor.store.list_resources(supervisor.run_id))
    await asyncio.sleep(0)
    client = TestClient(create_app(supervisor, "secret-token"))

    assert client.get(f"/api/resources/{internal_id}/open").status_code == 401
    client.get("/?token=secret-token")
    snapshot = client.get("/api/state").json()
    link = snapshot["resources"][0]["link"]
    assert link["href"] == f"/api/resources/{internal_id}/open"
    assert "secret-token" not in str(link)
    assert "127.0.0.1" not in str(link)
    opened = client.get(f"/api/resources/{internal_id}/open", follow_redirects=False)
    assert opened.status_code == 303
    assert opened.headers["location"] == "http://127.0.0.1:8501"
    assert opened.headers["cache-control"] == "no-store"

    await supervisor.close()


def test_dashboard_auth_and_reduced_remote_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RUNWATCH_MASCOT_SHOWCASE", raising=False)
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    app = create_app(supervisor, "secret-token")
    client = TestClient(app)
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/api/state").status_code == 401
    assert (
        client.get(
            "/api/state", headers={"Authorization": "Bearer secret-token"}
        ).status_code
        == 200
    )
    response = client.get("/?token=secret-token", follow_redirects=False)
    assert response.status_code == 200
    assert response.history == []
    assert response.headers["set-cookie"].startswith(
        "runwatch_access=secret-token; HttpOnly;"
    )
    assert response.headers["cache-control"] == "no-store"
    asset_version = app.state.runwatch_asset_version
    assert len(asset_version) == 12
    assert (
        f"/static/common/neumorphic-gloss-components.css?v={asset_version}"
        in response.text
    )
    assert f"/static/runwatch/styles.css?v={asset_version}" in response.text
    assert f"/static/runwatch/app.js?v={asset_version}" in response.text
    assert f"/static/runwatch/mascot/phrases.json?v={asset_version}" in response.text
    assert f"/static/runwatch/mascot/ready.png?v={asset_version}" in response.text
    assert 'data-showcase="false"' in response.text
    assert "pixability" not in response.text.lower()
    assert (
        client.get("/static/common/neumorphic-gloss-components.css").status_code == 200
    )
    assert client.get("/static/runwatch/styles.css").status_code == 200
    assert client.get("/static/runwatch/app.js").status_code == 200
    mascot_catalog = client.get("/static/runwatch/mascot/phrases.json")
    assert mascot_catalog.status_code == 200
    assert mascot_catalog.headers["content-type"].startswith("application/json")
    mascot_image = client.get("/static/runwatch/mascot/ready.png")
    assert mascot_image.status_code == 200
    assert mascot_image.headers["content-type"] == "image/png"
    assert client.get("/static/runwatch/mascot/not-a-dog.png").status_code == 404
    assert client.get("/static/common/pixability.jpg").status_code == 404
    assert client.get("/static/runwatch/index.html").status_code == 404
    assert "default-src 'self'" in response.headers["content-security-policy"]
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-frame-options"] == "DENY"
    assert "Remote notebook assistant" not in response.text
    state_response = client.get("/api/state")
    assert state_response.status_code == 200
    assert state_response.headers["cache-control"] == "no-store"
    assert client.post("/api/actions/cancel", json={}).status_code == 404
    assert client.get("/notifications/ntfy/open").status_code == 404
    supervisor.store.close()


def test_dashboard_exposes_authenticated_ntfy_app_handoff(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(
            notifications=NotificationSettings(
                ntfy_base_url="https://ntfy.example/base",
                ntfy_topic="phone topic",
            )
        ),
    )
    client = TestClient(create_app(supervisor, "secret-token"))

    assert client.get("/notifications/ntfy/open").status_code == 401
    dashboard = client.get("/?token=secret-token")
    assert 'href="/notifications/ntfy/open"' in dashboard.text
    assert 'aria-label="Open ntfy"' in dashboard.text
    assert 'class="ntfy-logo"' in dashboard.text
    assert 'class="ntfy-terminal-prompt"' in dashboard.text
    assert 'class="ntfy-terminal-cursor"' in dashboard.text
    assert "Open ntfy" in dashboard.text
    styles = client.get("/static/runwatch/styles.css")
    assert styles.status_code == 200
    assert "background: #338574" in styles.text
    handoff = client.get("/notifications/ntfy/open", follow_redirects=False)
    assert handoff.status_code == 303
    assert handoff.headers["location"] == ("ntfy://ntfy.example/base/phone%20topic")
    assert handoff.headers["cache-control"] == "no-store"
    supervisor.store.close()


@pytest.mark.asyncio
async def test_dashboard_state_and_sse_are_allowlisted_presentations(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "sensitive.ipynb"
    nbformat.write(
        nbformat.v4.new_notebook(
            cells=[nbformat.v4.new_code_cell("SOURCE_SECRET = 'hidden'")]
        ),
        notebook_path,
    )
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(
            notifications=NotificationSettings(
                webhook_urls=["https://hooks.example/run?token=WEBHOOK_SECRET"]
            )
        ),
    )
    internal_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="local",
                type="line_count",
                id=str(tmp_path / "private" / "output.jsonl"),
                account_id="ACCOUNT_SECRET",
                metadata={"credential": "METADATA_SECRET"},
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=False,
    )
    supervisor.store.save_resource_cursor(internal_id, {"token": "CURSOR_SECRET"})
    supervisor.store.update_resource_observation(
        supervisor.run_id,
        internal_id,
        ResourceObservation(
            status=ResourceStatus.RUNNING,
            metrics={"line_count": 4, "credential": "METRIC_SECRET"},
            raw={"provider_token": "RAW_SECRET"},
        ),
    )
    supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.RESUME,
        payload={"credential": "ACTION_SECRET"},
    )
    event = await supervisor.bus.publish(
        "resource.observed",
        {
            "internal_id": internal_id,
            "status": "running",
            "credential": "EVENT_SECRET",
        },
    )

    client = TestClient(create_app(supervisor, "secret-token"))
    client.get("/?token=secret-token")
    response = client.get("/api/state")
    snapshot = response.json()
    serialized = response.text

    assert response.headers["cache-control"] == "no-store"
    assert snapshot["schema_version"] == 1
    assert "actions" not in snapshot
    assert set(snapshot["capabilities"]) == {"controller_live"}
    assert "source" not in snapshot["cells"][0]
    assert "metadata" not in snapshot["run"]
    resource = snapshot["resources"][0]
    assert resource["external_id"] == "output.jsonl"
    assert resource["metrics"] == {"line_count": 4}
    assert not {"cursor", "raw", "account_id"}.intersection(resource)
    for secret in (
        "SOURCE_SECRET",
        "WEBHOOK_SECRET",
        "ACCOUNT_SECRET",
        "METADATA_SECRET",
        "METRIC_SECRET",
        "CURSOR_SECRET",
        "RAW_SECRET",
        "ACTION_SECRET",
        "EVENT_SECRET",
    ):
        assert secret not in serialized

    signal = _dashboard_event_signal(event)
    assert signal == {
        "seq": event["seq"],
        "timestamp": event["timestamp"],
        "type": "resource.observed",
    }
    supervisor.store.close()


def test_ntfy_deep_link_marks_plain_http_as_insecure() -> None:
    assert (
        _ntfy_deep_link(
            NotificationSettings(
                ntfy_base_url="http://192.168.1.3:8080",
                ntfy_topic="runs",
            )
        )
        == "ntfy://192.168.1.3:8080/runs?secure=false"
    )


def test_dashboard_stop_queues_versioned_sagemaker_cancellation(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    internal_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="sagemaker_processing_job",
                id="processing-job",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(blocking=True, stop_on_cancel=True),
        ),
        cell_index=1,
        attempt=2,
        kernel_epoch=0,
        supports_stop=True,
    )
    supervisor.store.update_process(
        supervisor.run_id, process_pid=os.getpid(), server_port=8765
    )
    resource = supervisor.store.get_resource(internal_id)
    assert resource is not None
    client = TestClient(create_app(supervisor, "secret-token"))
    client.get("/?token=secret-token", follow_redirects=True)

    response = client.post(
        f"/api/resources/{internal_id}/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": resource["version"],
        },
    )

    assert response.status_code == 202
    action = supervisor.store.get_action(response.json()["action_id"])
    assert action and action["status"] == "requested"
    assert action["payload"]["expected_version"] == resource["version"]
    supervisor.store.close()


def test_dashboard_stop_rejects_invalid_or_stale_resources(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    client = TestClient(create_app(supervisor, "secret-token"))
    client.get("/?token=secret-token", follow_redirects=True)

    invalid = client.post(
        "/api/resources/missing/stop",
        json={"confirmation": "no", "expected_version": 1},
    )
    assert invalid.status_code == 400
    missing = client.post(
        "/api/resources/missing/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": 1,
        },
    )
    assert missing.status_code == 404

    borrowed_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="sagemaker_processing_job",
                id="borrowed-job",
                ownership=Ownership.BORROWED,
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=True,
    )
    borrowed = supervisor.store.get_resource(borrowed_id)
    assert borrowed is not None
    stale = client.post(
        f"/api/resources/{borrowed_id}/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": borrowed["version"] - 1,
        },
    )
    assert stale.status_code == 409
    wrong_owner = client.post(
        f"/api/resources/{borrowed_id}/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": borrowed["version"],
        },
    )
    assert "exclusive" in wrong_owner.json()["detail"]

    terminal_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="sagemaker_processing_job",
                id="terminal-job",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=True,
    )
    supervisor.store.update_resource_observation(
        supervisor.run_id,
        terminal_id,
        ResourceObservation(status=ResourceStatus.COMPLETED, terminal=True),
    )
    terminal = supervisor.store.get_resource(terminal_id)
    assert terminal is not None
    response = client.post(
        f"/api/resources/{terminal_id}/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": terminal["version"],
        },
    )
    assert "Terminal resources" in response.json()["detail"]

    active_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="sagemaker_processing_job",
                id="active-job",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=True,
    )
    active = supervisor.store.get_resource(active_id)
    assert active is not None
    supervisor.store.update_process(
        supervisor.run_id, process_pid=999_999_999, server_port=8765
    )
    response = client.post(
        f"/api/resources/{active_id}/stop",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": active["version"],
        },
    )
    assert "not live" in response.json()["detail"]
    supervisor.store.close()


def test_dashboard_rejects_superseded_resource_stop(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    internal_id, _ = supervisor.store.register_resource(
        run_id=supervisor.run_id,
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="sagemaker_processing_job",
                id="old-job",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=0,
        supports_stop=True,
    )
    supervisor.store.set_resource_disposition(
        internal_id, ResourceDisposition.SUPERSEDED
    )
    resource = supervisor.store.get_resource(internal_id)
    assert resource is not None
    supervisor.store.update_process(
        supervisor.run_id, process_pid=os.getpid(), server_port=8765
    )
    response = TestClient(create_app(supervisor, "secret")).post(
        f"/api/resources/{internal_id}/stop?token=secret",
        json={
            "confirmation": "STOP RESOURCE AND CANCEL RUN",
            "expected_version": resource["version"],
        },
    )
    assert response.status_code == 409
    assert "active" in response.json()["detail"]
    assert supervisor.store.list_actions(supervisor.run_id) == []
    supervisor.store.close()


@pytest.mark.asyncio
async def test_sse_replays_events_after_last_event_id(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    first = await supervisor.bus.publish("first", {"value": 1})
    second = await supervisor.bus.publish("second", {"value": 2})

    async def connected() -> bool:
        return False

    request = SimpleNamespace(
        headers={"last-event-id": str(first["seq"])},
        app=SimpleNamespace(state=SimpleNamespace(runwatch_supervisor=supervisor)),
        is_disconnected=connected,
    )
    stream = _event_stream(request)  # type: ignore[arg-type]
    try:
        assert "connected" in await anext(stream)
        replay = await anext(stream)
        assert f"id: {second['seq']}" in replay
        assert '"type": "second"' in replay
        assert '"value"' not in replay
    finally:
        await stream.aclose()
        supervisor.store.close()


@pytest.mark.asyncio
async def test_sse_invalid_last_event_id_replays_from_oldest_retained_event(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    first = await supervisor.bus.publish("first", {"value": 1})

    async def connected() -> bool:
        return False

    request = SimpleNamespace(
        headers={"last-event-id": "not-a-sequence"},
        app=SimpleNamespace(state=SimpleNamespace(runwatch_supervisor=supervisor)),
        is_disconnected=connected,
    )
    stream = _event_stream(request)  # type: ignore[arg-type]
    try:
        assert "connected" in await anext(stream)
        replay = await anext(stream)
        assert f"id: {first['seq']}" in replay
        assert '"type": "first"' in replay
    finally:
        await stream.aclose()
        supervisor.store.close()


@pytest.mark.asyncio
async def test_sse_future_last_event_id_is_clamped_before_subscribing(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    baseline = await supervisor.bus.publish("baseline", {})

    async def connected() -> bool:
        return False

    request = SimpleNamespace(
        headers={"last-event-id": str(int(baseline["seq"]) + 1_000_000)},
        app=SimpleNamespace(state=SimpleNamespace(runwatch_supervisor=supervisor)),
        is_disconnected=connected,
    )
    stream = _event_stream(request)  # type: ignore[arg-type]
    try:
        assert "connected" in await anext(stream)
        future = await supervisor.bus.publish("future", {"value": 2})
        delivered = await asyncio.wait_for(anext(stream), timeout=0.5)
        assert f"id: {future['seq']}" in delivered
        assert '"type": "future"' in delivered
    finally:
        await stream.aclose()
        supervisor.store.close()


def test_sse_replay_reads_every_persisted_page(tmp_path: Path) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    try:
        persisted = [
            supervisor.store.append_event(
                supervisor.run_id, "progress", {"index": index}
            )
            for index in range(1_005)
        ]

        replay = _events_after(supervisor, 0)

        assert len(replay) == 1_005
        assert replay[0]["seq"] == persisted[0]["seq"]
        assert replay[-1]["seq"] == persisted[-1]["seq"]
    finally:
        supervisor.store.close()


def test_sse_gap_recovery_is_ordered_and_deduplicates_queued_events(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )
    try:
        first = supervisor.store.append_event(supervisor.run_id, "first", {})
        second = supervisor.store.append_event(supervisor.run_id, "second", {})
        third = supervisor.store.append_event(supervisor.run_id, "third", {})

        recovered = _delivery_batch(supervisor, third, int(first["seq"]))

        assert [event["seq"] for event in recovered] == [
            second["seq"],
            third["seq"],
        ]
        assert _delivery_batch(supervisor, second, int(third["seq"])) == []
    finally:
        supervisor.store.close()


def test_stop_confirmation_lists_only_other_provider_stoppable_resources() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")
    assert "item.internal_id !== id" in script
    assert "!item.monitor_closed" in script
    assert "item.status !== 'stopping'" in script
    assert "item.ownership === 'exclusive'" in script
    assert "item.supports_stop" in script
    assert "other resource(s) eligible for provider stop" in script


def test_dashboard_formats_metric_units_and_timestamps() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")
    assert "const fmtBytes" in script
    assert "const fmtTimestamp" in script
    assert "fmtBytes(metrics.total_bytes)" in script
    assert "relativeTime(metrics.latest_modified_at" in script


def test_dashboard_renders_linked_dashboard_actions_without_public_targets() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")
    dashboard_renderer = script.split("function renderDashboardResource", 1)[1].split(
        "function sagemakerBody", 1
    )[0]
    assert "Open ${escapeHtml(label)}" in dashboard_renderer
    assert 'href="${escapeHtml(link.href)}"' in script
    assert "Preparing ${label}" in dashboard_renderer
    assert "http_status" not in dashboard_renderer
    assert "response_time_seconds" not in dashboard_renderer
    assert "technicalDetails" not in dashboard_renderer
    assert "link.public_base" not in script
    assert "link.token" not in script


def test_dashboard_prioritizes_user_signals_and_collapses_diagnostics() -> None:
    runwatch_root = _WEB_ARTIFACTS / "runwatch"
    template = (runwatch_root / "index.html").read_text(encoding="utf-8")
    script = (runwatch_root / "app.js").read_text(encoding="utf-8")

    assert 'id="heartbeat"' in template
    assert 'id="issue-count"' in template
    assert 'id="remote-count"' in template
    assert 'id="cell-highlights"' in template
    assert '<details class="full-timeline panel surface">' in template
    assert '<details class="developer-diagnostics panel surface">' in template
    assert 'id="kernel-epoch"' not in template
    assert "function meaningfulEvents" in script
    assert "events.slice(-100)" in script


def test_dashboard_mascot_catalog_covers_every_run_status() -> None:
    mascot_root = _WEB_ARTIFACTS / "runwatch/mascot"
    catalog = json.loads((mascot_root / "phrases.json").read_text(encoding="utf-8"))

    assert set(catalog) == {status.value for status in RunStatus}
    referenced_images: set[str] = set()
    for status in RunStatus:
        entry = catalog[status.value]
        assert set(entry) == {"image", "phrases"}
        assert isinstance(entry["image"], str)
        assert (mascot_root / entry["image"]).is_file()
        referenced_images.add(entry["image"])
        phrases = entry["phrases"]
        assert len(phrases) == 10
        assert len(set(phrases)) == len(phrases)
        assert all(isinstance(phrase, str) and phrase.strip() for phrase in phrases)

    assert referenced_images == {
        "alert.png",
        "confused.png",
        "inspecting.png",
        "ready.png",
        "running.png",
        "sleeping.png",
        "success.png",
        "waiting.png",
    }


def test_dashboard_mascot_narrator_is_status_driven_and_mobile_safe() -> None:
    runwatch_root = _WEB_ARTIFACTS / "runwatch"
    template = (runwatch_root / "index.html").read_text(encoding="utf-8")
    script = (runwatch_root / "app.js").read_text(encoding="utf-8")
    styles = (runwatch_root / "styles.css").read_text(encoding="utf-8")

    assert 'id="mascot-narrator"' in template
    assert 'id="mascot-message"' in template
    assert 'id="mascot-image"' in template
    assert 'id="mascot-showcase-label"' in template
    assert "data-showcase=\"{{ 'true' if mascot_showcase else 'false' }}\"" in template
    assert 'aria-live="polite"' in template
    assert "function loadMascotCatalog()" in script
    assert "function maybeShowMascot(run, options = {})" in script
    assert "function chooseMascotPhrase(status, entry)" in script
    assert "function startMascotShowcase()" in script
    assert "function showNextMascotShowcaseState()" in script
    assert "if (mascotShowcaseEnabled()) startMascotShowcase();" in script
    assert "MASCOT_SHOWCASE_SECONDS = 6" in script
    assert "showcaseLabel.textContent = friendlyStatus(status);" in script
    assert "Mascot preview" not in script
    assert all(f"'{status.value}'" in script for status in RunStatus)
    assert "mascotState.lastTerminalStatus === status" in script
    assert "maybeShowMascot(run);" in script
    assert ".mascot-narrator.is-visible" in styles
    assert ".mascot-showcase-label" in styles
    assert "@keyframes mascot-float" in styles
    assert "max-width: calc(100vw - 112px)" in styles
    assert "@media (prefers-reduced-motion: reduce)" in styles


def test_dashboard_can_enable_the_fake_session_mascot_showcase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RUNWATCH_MASCOT_SHOWCASE", "1")
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(),
    )

    response = TestClient(create_app(supervisor, "secret-token")).get(
        "/?token=secret-token"
    )

    assert response.status_code == 200
    assert 'data-showcase="true"' in response.text


def test_dashboard_scopes_tqdm_progress_and_prefers_the_outermost_bar() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")

    assert "function latestProgress(events, current)" in script
    assert "payload.cell_index === current.cell_index" in script
    assert "payload.attempt === current.attempt" in script
    assert "latest.payload.metrics.position || 0" in script
    assert "candidate.metrics?.progress_id === progressId" in script
    assert "payload.metrics?.closed === true" in script


def test_dashboard_uses_resource_specific_summaries_and_terminal_semantics() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")

    assert "function sagemakerBody" in script
    assert "metrics.instance_count" in script
    assert "metrics.instance_type" in script
    assert "metrics.volume_size_gb" in script
    assert "function fileCountBody" in script
    assert "function lineCountBody" in script
    assert "function systemBody" in script
    assert "function resourcePriority" in script
    assert "resource.monitor_closed" in script
    assert "RUN_TERMINAL.has(run.status)" in script
    assert "primitiveMetrics" not in script


def test_runwatch_web_artifact_is_unbranded_and_uses_shared_components() -> None:
    runwatch_root = _WEB_ARTIFACTS / "runwatch"
    template = (runwatch_root / "index.html").read_text(encoding="utf-8")
    bundle = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            runwatch_root / "index.html",
            runwatch_root / "styles.css",
            runwatch_root / "app.js",
        )
    )

    assert "/static/common/neumorphic-gloss-components.css" in template
    assert "pixability" not in bundle.lower()
    assert "pixability.jpg" not in bundle.lower()

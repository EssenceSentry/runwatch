# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import nbformat
import pytest
from fastapi.testclient import TestClient

from runwatch.dashboard_links import DashboardLinkManager
from runwatch.models import (
    NotificationSettings,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceLifecycle,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunwatchConfig,
)
from runwatch.supervisor import RunSupervisor
from runwatch.web import (
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

    await supervisor.close()


def test_dashboard_auth_and_reduced_remote_surface(tmp_path: Path) -> None:
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
    assert "pixability" not in response.text.lower()
    assert (
        client.get("/static/common/neumorphic-gloss-components.css").status_code == 200
    )
    assert client.get("/static/runwatch/styles.css").status_code == 200
    assert client.get("/static/runwatch/app.js").status_code == 200
    assert client.get("/static/common/pixability.jpg").status_code == 404
    assert client.get("/static/runwatch/index.html").status_code == 404
    assert "default-src 'self'" in response.headers["content-security-policy"]
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-frame-options"] == "DENY"
    assert "Remote notebook assistant" not in response.text
    assert client.get("/api/state").status_code == 200
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
    handoff = client.get("/notifications/ntfy/open", follow_redirects=False)
    assert handoff.status_code == 303
    assert handoff.headers["location"] == ("ntfy://ntfy.example/base/phone%20topic")
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
    assert "r.internal_id!==id" in script
    assert "r.status!=='stopping'" in script
    assert "r.ownership==='exclusive'&&r.supports_stop" in script
    assert "other resource(s) eligible for provider stop" in script


def test_dashboard_formats_metric_units_and_timestamps() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")
    assert "const fmtBytes" in script
    assert "key === 'bytes' || key.endsWith('_bytes')" in script
    assert "key.endsWith('_at') || key.endsWith('_timestamp')" in script
    assert "fmtMetricValue(k,v)" in script


def test_dashboard_renders_linked_dashboard_actions_without_public_targets() -> None:
    script = (_WEB_ARTIFACTS / "runwatch/app.js").read_text(encoding="utf-8")
    assert "Open ${escapeHtml(link.label||'dashboard')}" in script
    assert 'href="${escapeHtml(link.href)}"' in script
    assert "Preparing dashboard link" in script
    assert "link.public_base" not in script
    assert "link.token" not in script


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

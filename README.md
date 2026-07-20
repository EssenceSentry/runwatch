# Runwatch

[![quality-gate](https://github.com/EssenceSentry/runwatch/actions/workflows/quality-gate.yml/badge.svg?branch=main)](https://github.com/EssenceSentry/runwatch/actions/workflows/quality-gate.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](.pre-commit-config.yaml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Runwatch is `nbclient` on steroids: durable notebook execution, structured AWS and local
resource monitoring, restart/replay, and a mobile-friendly dashboard. The dashboard is
observational except for one deliberately narrow remote action: stopping an owned,
stoppable resource cancels the run.

Runwatch is agent-agnostic. Codex, Claude, or a human edits the run-owned `source.ipynb`
with normal `nbformat` APIs and uses the local CLI to resume or restart the run.

## Highlights

- Cell-by-cell `nbclient` execution with a live kernel.
- Atomic write-back of settled cell sources, execution counts, and outputs to the
  notebook passed to `runwatch execute`.
- Failure pause that preserves kernel state while monitoring continues.
- Manual recovery after kernel death or a stopped Runwatch process.
- Immutable input, editable source, rolling partial output, and final executed notebook.
- Durable SQLite actions for live and stopped-process control, kernel epochs, source
  hashes, adapter cursors, observations, logs, notifications, and bounded events.
- Built-in resource adapters for:
  - SageMaker Processing jobs;
  - S3 prefixes and Runwatch manifests;
  - CloudWatch metrics and logs;
  - host/kernel CPU and memory plus optional NVIDIA GPU metrics;
  - local file counts and incremental line counts;
  - health-checked links to other localhost dashboards.
- FastAPI dashboard with SSE, pairing-token authentication, LAN sharing, and optional
  `cloudflared` quick tunnels.
- Webhook and `ntfy` notifications backed by durable event replay and a
  per-destination retry outbox. Delivery is at least once; retries carry stable
  `Idempotency-Key` and `X-Runwatch-Intent-ID` headers so receivers can deduplicate a
  request accepted immediately before a Runwatch crash. Unexpected event-routing
  failures use bounded backoff and one durable dead-letter record instead of pinning
  event retention indefinitely.
- Versioned notification presentations exclude resolved configuration and raw
  operational payloads. Response bodies are not read, redirects are not followed,
  periodic reminders use one rolling intent, and network plain HTTP requires an
  explicit opt-in.
- Notification-aware removal of successful or cancelled run state after the dashboard closes, with
  incomplete outboxes retained for `runwatch open` recovery and `--keep-run` available
  for retained provenance.

Runwatch versions its persisted and wire contracts independently. New run manifests
and SQLite databases use schema version 3; configuration and kernel resource events use
schema version 2; S3 progress manifests use schema version 1. Legacy schema-version-2
run directories reopen conservatively. Runwatch intentionally does not migrate 0.1 run
directories.

## Installation

Python 3.10 or newer is required. Runwatch supports local POSIX filesystems on Linux
and macOS; those are the platforms exercised in CI. Windows and shared/network
filesystems are not currently supported execution targets.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[supervisor]'
```

Notebook kernels that only emit generic Runwatch protocol events can use the
Pydantic-only base install. The CLI, notebook runner, dashboard, notifications, and
built-in AWS/local adapters require the `supervisor` extra shown above.

Install optional NVIDIA monitoring support with:

```bash
python -m pip install -e '.[supervisor,gpu]'
```

For development:

```bash
uv sync --extra supervisor --extra test --extra dev --extra docs
uv run pytest tests
uv run ruff check src tests
```

The selected notebook kernel must be able to import `runwatch` when cells emit resources.

Third-party supervisor adapters can extend Runwatch without adding provider-specific
code to this package. See [Third-party resource adapters](docs/resource-events.md#third-party-resource-adapters)
for the entry-point contract and adapter API.

## First run

```bash
runwatch init-config runwatch.yaml
runwatch validate notebook.ipynb --config runwatch.yaml
runwatch execute notebook.ipynb --config runwatch.yaml
```

`init-config` writes an annotated starter file. `validate` checks nbformat structure,
kernel availability, paths, sharing prerequisites, configured adapters, and terminal
conditions without starting a kernel, dashboard, or provider resource. Resources
emitted dynamically by cells cannot be predicted during preflight.

Runwatch prints the run directory, editable notebook, pairing URL, and terminal QR code.
By default the dashboard remains available for 90 seconds after the run reaches a
terminal state, then closes automatically. If the run succeeded or was cancelled,
Runwatch removes that run directory and removes the empty `.runwatch/runs` and
`.runwatch` parents. When
notifications are configured, cleanup first waits up to
`notifications.terminal_drain_timeout_seconds` for terminal event routing and delivery
attempts. If the outbox is still nonterminal, the run is retained and the CLI
prints a reason plus `runwatch open RUN_DIR`; opening it restarts notification delivery
without rerunning the notebook. The recovery controller conservatively retains the run
when that dashboard closes because it did not itself observe normal notebook
finalization. After confirming delivery, remove the retained state explicitly if it is
no longer needed. Set `server.linger_seconds: 0` to close immediately, set it to `null`
to keep the dashboard open until Ctrl+C, or use `--keep-run` to retain successful or
cancelled state after the original execution's dashboard closes.

For a no-AWS replay with live progress, local metrics, file monitoring, log tailing,
and final notebook results, use the repository's
[Runwatch fake session](web_artifacts_fake_sessions/runwatch/README.md):

```bash
web_artifacts_fake_sessions/runwatch/run.sh
```

Every run contains:

```text
.runwatch/runs/<timestamp>-<notebook>-<id>/
├── access-token.txt
├── input.ipynb               # immutable original snapshot
├── source.ipynb              # agent/human editable nbformat document
├── executed.partial.ipynb    # runner-owned rolling checkpoint
├── executed.ipynb            # final executed notebook
├── writeback-state.json      # conflict guard for the user-owned notebook
├── run-manifest.json
└── runwatch.sqlite3
```

Each per-run directory is restricted to the current user (`0700`) and new run-state
files are created with mode `0600`. Retained state is nevertheless sensitive: it can
contain notebook source and output, tracebacks, local paths, resource identifiers and
logs, notification destinations and payloads, actions, and the dashboard bearer token.
The mode of the original notebook passed to `execute` is preserved during write-back.

## Failure and recovery

After every settled cell attempt, Runwatch atomically writes the executed notebook
state back to the notebook passed to `execute`. This includes repaired source,
execution counts, outputs, and tracebacks. If that notebook changes outside Runwatch,
write-back stops rather than overwriting the external edit, and the run-owned partial
checkpoint is retained.

Rolling checkpoints serialize an immutable notebook generation on the event-loop
thread before filesystem I/O. Requests that arrive during a write remain pending, and
transient checkpoint failures are journaled and retried with bounded backoff until the
worker recovers. Checkpoint and write-back publication use a temporary file, file
`fsync`, atomic replacement, and parent-directory `fsync` where the local filesystem
supports them.

Original-notebook conflict detection is a best-effort portable compare-and-replace. It
checks a content and metadata fingerprint again immediately before `os.replace`, which
detects external saves during preparation and makes the remaining race very small. No
portable filesystem primitive can make the final comparison and rename indivisible, so
another writer in that last window can still be overwritten. Avoid editing the original
notebook while Runwatch is executing; edit the run-owned `source.ipynb` for recovery.

When a cell fails or reaches its configured timeout, Runwatch persists its outputs and
traceback, pauses the notebook, and keeps the kernel and resource monitors alive. A
timed-out kernel is interrupted and synchronized before live resume is allowed. Edit
`source.ipynb` with normal `nbformat`:

```python
from pathlib import Path

import nbformat

path = Path(".runwatch/runs/.../source.ipynb")
notebook = nbformat.read(path, as_version=4)
notebook.cells[6].source = "result = repaired_input()"
nbformat.write(notebook, path)
```

If only the failed or future cells changed, resume in the live kernel:

```bash
runwatch resume .runwatch/runs/...
```

If imported source or an already-executed cell changed, create a new kernel and replay:

```bash
runwatch restart .runwatch/runs/...
```

Replay starts at cell zero. An explicit override may start later, but the operator owns
reconstruction of any missing kernel state:

```bash
runwatch restart .runwatch/runs/... --from-cell 4
```

`N` is zero-based and must identify a cell in the current `source.ipynb`.

If the Runwatch process is no longer live, `resume` first journals the recovery action,
reopens the persisted run, restores resource monitors and cursors, starts a new kernel
epoch, and replays from cell zero. A crash during recovery leaves an action that the
next invocation can safely recover.

Cancellation is bounded rather than relying on one cooperative interrupt. Runwatch
persists `cancelling`, then advances through kernel interrupt, graceful shutdown,
provisioner terminate, and provisioner kill using the four `notebook.cancel_*_grace_seconds`
settings. Stage failures are journaled, repeated cancellation requests are idempotent,
and an execution client that still does not return is detached so the run can settle as
`cancelled` instead of hanging indefinitely.

For an agent-oriented dossier:

```bash
runwatch context .runwatch/runs/... --format markdown
runwatch context .runwatch/runs/... --json
```

## Resource emission

Resources are emitted as structured Jupyter MIME output immediately after they are
created or selected. Runwatch injects the active run, cell, attempt, and kernel epoch.

### SageMaker Processing

```python
from runwatch import aws

sagemaker.create_processing_job(**request)
aws.emit_owned_sagemaker_processing_job(
    request["ProcessingJobName"],
    region="us-east-1",
    logical_key="feature-build",
    output_prefixes=["s3://bucket/run/output/"],
)
```

SageMaker Processing is blocking but borrowed and observation-only by default. Use
`emit_owned_sagemaker_processing_job` only for a job the current run created and may
stop; that explicit helper claims exclusive ownership and enables provider stop during
run cancellation. Existing calls that explicitly pass `stop_on_cancel=True` continue
to opt into exclusive ownership. Set `ownership="exclusive"` with
`stop_on_cancel=False` when manual stop should be available without joining the
cancellation cascade.

### S3 prefix

```python
aws.emit_s3_prefix(
    "s3://bucket/run/output/",
    expected_count=400,
    completion_marker="_SUCCESS",
    blocking=True,
    full_rescan_seconds=300,
)
```

A blocking prefix must define an expected count or completion marker. Truncated scans
continue from a persisted key on the next poll instead of restarting at page one. After
the initial reconciliation, polls scan only keys after the last observed key and perform
a full reconciliation every `full_rescan_seconds` (five minutes by default). Incremental
counts therefore assume append-only keys between reconciliations; the dashboard exposes
the scan mode, reconciliation time, and lower-bound warning. Set
`full_rescan_seconds=0` when every poll must be a full exact scan.

### S3 manifest

```python
aws.emit_s3_manifest("s3://bucket/run/progress.json", blocking=True)
```

The manifest is a small JSON document:

```json
{
  "schema_version": 1,
  "status": "running",
  "completed": 180,
  "total": 400,
  "message": "Building features",
  "metrics": {"rows": 9000000}
}
```

`status` must be `running`, `completed`, or `failed`; metrics must be finite scalar
values and cannot override reserved manifest fields. Manifests are capped at 1 MiB.

### CloudWatch

```python
aws.emit_cloudwatch_metric(
    namespace="MyPipeline",
    metric_name="ChannelsProcessed",
    dimensions={"RunId": "abc"},
)

aws.emit_cloudwatch_logs(
    log_group="/aws/my-pipeline",
    stream_prefix="run-abc/",
)
```

CloudWatch metrics and logs are always nonblocking. Metric cards retain the full current
lookback window, while observation history stores only new or revised timestamp samples
and bounds its deduplication cursor to 1,440 entries. Log stream discovery rotates across
bounded pages when more streams exist than can be displayed at once, shares each poll's
line budget across busy streams, and prunes tokens after a complete discovery rotation.
Terminal SageMaker jobs drain paginated logs until caught up or report an explicit
truncation.

### Local system, files, and dashboards

```python
from runwatch import local

local.emit_system_metrics(include_host=True, include_kernel=True, gpu="all")

local.emit_file_count(
    "artifacts/parts",
    pattern="*.parquet",
    expected_count=400,
    blocking=True,
)

local.emit_line_count(
    "logs/inference.log",
    expected_lines=10000,
    tail_lines=100,
    blocking=True,
)

local.emit_dashboard(
    "http://127.0.0.1:8501",
    name="Training dashboard",
    health_path="/_stcore/health",
    logical_key="training-dashboard",
)
```

CPU and memory use `psutil`. NVIDIA metrics use optional NVML support and degrade to an
`nvidia_available=false` metric when unavailable. File-count scans stream metadata into
exact count, total-byte, and newest-modification-time aggregates without retaining one
stat object per file. That settlement signature is an aggregate, not content identity;
content edits that preserve all three values may not reset settlement. Line monitoring
is optimized for append-only logs: it detects replacement, truncation, changes near the
committed offset, and non-growing files whose mtime changes, while keeping partial-line
memory bounded. An earlier in-place edit combined with later file growth can remain
undetected if it does not touch the offset fingerprint. Conversely, a metadata-only mtime
change on a non-growing file is conservatively treated as a rewrite and may replay the
bounded log tail. `line_count` includes records terminated by LF or CRLF; a CR is counted
once a following byte proves it is not the start of CRLF. A final unterminated fragment,
including a trailing lone CR, is excluded until completed and is reported through
`partial_line_pending`, `partial_line_buffered_bytes`, and `partial_line_truncated`.

`emit_dashboard` registers an already-running local web application. Runwatch monitors
its health and adds an **Open Training dashboard** action to the resource card. With
`--share lan`, that action opens a separate authenticated LAN reverse proxy; with
`--share cloudflared`, it opens a separate authenticated quick tunnel. HTTP streaming,
redirects, cookies, WebSockets, and SSE pass through at the proxy root, which avoids
breaking applications that do not support a configurable path prefix.

Only explicit `localhost` and loopback IP URLs are accepted. Linked dashboards are
external, nonblocking, and observation-only: Runwatch neither starts nor stops the
application. The registration survives Runwatch recovery, while its proxy port and
tunnel URL are recreated for the current process and are never persisted in dashboard
state. Stop the linked application through its own controls or local process manager.

### Notebook progress

```python
from runwatch import emit_progress

emit_progress(180, total=400, unit="partitions", message="Building features")
```

Python kernels also mirror `tqdm`, `tqdm.auto`, and `tqdm.notebook` bars into the
same dashboard progress area without notebook changes:

```python
from tqdm.auto import tqdm

for item in tqdm(items, desc="Building features", unit="items"):
    process(item)
```

Runwatch preserves tqdm's normal notebook output and emits structured updates at most
twice per second by default. Updates reuse one hidden notebook display per bar, so a
long loop does not append an output for every refresh. The dashboard scopes progress
to the current cell and prefers the outermost bar when bars are nested. Set
`notebook.capture_tqdm: false` to disable automatic capture, or adjust
`notebook.tqdm_min_interval_seconds`. Progress created in a separate process is outside
the notebook kernel and is not captured automatically.

## Dashboard and remote stop

The dashboard shows notebook state, reported progress, outputs, tracebacks, resource
metrics and charts, log tails, and the durable event journal.

The dashboard header provides an **Open notebook** action beside the ntfy action. It
opens a separate, authenticated, read-only rendering of the saved notebook in a new
tab. While the notebook is executing, the view uses `executed.partial.ipynb`; after
notebook cells finish, it uses the final output; before the first checkpoint, it renders
the saved source. The page does not update automatically: the browser's normal refresh
control (including pull-to-refresh on mobile) loads the newest durable save. The page
reports the snapshot timestamp and settled-cell count. On mobile, scrolling down in the
notebook collapses that metadata header; selecting its compact summary expands it again.

This full-notebook view is intentionally different from the bounded dashboard timeline:
it can contain all notebook source and saved outputs. Runwatch removes active and
navigation-capable HTML, omits JavaScript-only outputs, and isolates the rendering in a
sandboxed frame with scripts and network access disabled. The child retains only its
same-origin identity so the trusted wrapper can observe scroll position for the compact
mobile header. Treat access to it as access to the notebook itself.

The browser API is an explicit presentation model rather than a dump of SQLite state.
It allowlists display-safe run, cell, resource, metric, and event fields and omits
notebook source, resolved configuration, notification endpoints, provider cursors and
raw responses, controller tokens, and dedicated internal-path fields. Output,
traceback, log, and chart payloads are bounded, but their user-generated text can still
contain sensitive values. SSE carries only sequence, timestamp, and event type as an
invalidation signal; the browser then refreshes the sanitized snapshot. Dashboard
pages, snapshots, SSE, and authenticated redirects use `Cache-Control: no-store`.

Registered localhost dashboards appear as normal resource cards. Their Open action is
available only while the authenticated share is ready and uses the same pairing session
as the Runwatch dashboard.

Only active, exclusive resources whose adapter supports stop show a Stop button. The
confirmation lists the selected job and every other resource affected by cancellation.
Confirming the action interrupts the notebook, stops all eligible owned resources, and
finishes the run as `cancelled`. The provider resource is revalidated and accepts the
stop request before notebook cancellation begins; a stale or superseded resource has no
cancellation side effect.

The equivalent local command is:

```bash
runwatch resource stop RUN_DIR RESOURCE_ID
```

## Other CLI commands

```text
runwatch status RUN_DIR [--json]
runwatch validate NOTEBOOK [--config PATH] [--json]
runwatch events RUN_DIR [--follow] [--json]
runwatch open RUN_DIR
runwatch notifications rotate RUN_DIR --config PATH
runwatch notifications purge RUN_DIR --yes
runwatch version
```

`open` serves persisted state without starting notebook execution and retries durable
notification delivery. It conservatively retains the run when the dashboard closes:
only the controller that observed normal notebook finalization may authorize automatic
run cleanup.

`status`, `context`, and `events` emit bounded presentation schema version 1 in JSON
mode. They expose recovery-relevant lifecycle fields and safe summaries, not the raw
SQLite snapshot, resolved notification configuration, controller credentials, or raw
event payloads.

Notification credential maintenance is offline and lock-fenced. `notifications
rotate` reads only the notification settings from `--config`, atomically records them
as desired state in the run manifest, then rewrites every persisted delivery for the
same webhook/ntfy topology. It also rearms failed deliveries, clears legacy transport
errors, and sanitizes pre-presentation intents. To change topology, purge the old
outbox first; a subsequent rotate may enable the new topology because no old delivery
rows remain. `notifications purge --yes` disables routing without replay, removes the
outbox, scrubs notification diagnostics, and best-effort compacts SQLite.

Runwatch validates resource lifecycle semantics at every entry point—not just in the
convenience emitters. Metrics, logs, and system monitors are always nonblocking;
conditional S3 and local-file monitors require a concrete terminal condition; and
`stop_on_cancel` requires an exclusive adapter that implements stop.

## Sharing and security

Runwatch requires CurveZMQ encryption for the local Jupyter manager-to-kernel
channels and fails kernel startup rather than falling back to plaintext TCP. The
kernel must advertise Curve support; IPython kernels require `ipykernel>=7.3`.

Localhost is the default. For a trusted LAN:

```bash
runwatch execute notebook.ipynb --share lan
```

For a temporary public tunnel, install `cloudflared` and use `--share cloudflared`.
Runwatch prints both the public pairing URL and a local loopback pairing URL; the QR
code contains only the public URL.
The pairing URL is a bearer credential. It is excluded from notifications by default
and should be treated as a secret. See the
[security guide](docs/security.md).

## Agent workflow

Runwatch does not launch or communicate with agents. A repository-local skill under
`agent_skills/runwatch/` teaches Codex or Claude to inspect `runwatch context`, edit
`source.ipynb`, choose resume versus restart, and verify the resulting state.

Install or update that skill for Codex, VS Code Copilot, and Claude Code with:

```bash
uv run python scripts/install_skills.py update --target all
```

The installer copies the skill to the personal skill directories for each selected
agent. Pass `runwatch` instead of `update` for a non-replacing first installation, or
use `--target codex`, `--target copilot`, or `--target claude` for one destination.

## License

MIT. See [LICENSE](LICENSE).

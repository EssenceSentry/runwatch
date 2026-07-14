# Runwatch

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
  request accepted immediately before a Runwatch crash.
- Automatic removal of successful run state after the dashboard closes, with
  `--keep-run` available for retained provenance.

Runwatch uses state schema version 2 and intentionally does not migrate 0.1 run
directories.

## Installation

Python 3.10 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

Install optional NVIDIA monitoring support with:

```bash
python -m pip install -e '.[gpu]'
```

For development:

```bash
uv sync --extra test --extra dev --extra docs
uv run pytest tests
uv run ruff check src tests
```

The selected notebook kernel must be able to import `runwatch` when cells emit resources.

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
terminal state, then closes automatically. If the run succeeded, Runwatch removes that
run directory and removes the empty `.runwatch/runs` and `.runwatch` parents. Set
`server.linger_seconds: 0` to close immediately, set it to `null` to keep the dashboard
open until Ctrl+C, or use `--keep-run` to retain successful state after the dashboard
closes.

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

## Failure and recovery

After every settled cell attempt, Runwatch atomically writes the executed notebook
state back to the notebook passed to `execute`. This includes repaired source,
execution counts, outputs, and tracebacks. If that notebook changes outside Runwatch,
write-back stops rather than overwriting the external edit, and the run-owned partial
checkpoint is retained.

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
aws.emit_sagemaker_processing_job(
    request["ProcessingJobName"],
    region="us-east-1",
    logical_key="feature-build",
    output_prefixes=["s3://bucket/run/output/"],
)
```

SageMaker Processing is blocking and exclusively owned by default. It is the only
initial resource type that supports provider stop.

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

CloudWatch metrics and logs are always nonblocking. Log stream discovery rotates across
bounded pages when more streams exist than can be displayed at once. Terminal
SageMaker jobs drain paginated logs until caught up or report an explicit truncation.

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
`nvidia_available=false` metric when unavailable. File and line monitors tolerate
concurrent renames, detect replacement/truncation/rewrite, and keep partial-line memory
bounded.

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
runwatch version
```

`open` serves persisted state without starting notebook execution.

Runwatch validates resource lifecycle semantics at every entry point—not just in the
convenience emitters. Metrics, logs, and system monitors are always nonblocking;
conditional S3 and local-file monitors require a concrete terminal condition; and
`stop_on_cancel` requires an exclusive adapter that implements stop.

## Sharing and security

Localhost is the default. For a trusted LAN:

```bash
runwatch execute notebook.ipynb --share lan
```

For a temporary public tunnel, install `cloudflared` and use `--share cloudflared`.
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

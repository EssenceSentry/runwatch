# Structured resource and progress events

Runwatch schema version 2 recognizes:

```text
application/vnd.runwatch.resource+json
application/vnd.runwatch.event+json
```

Every display also includes `text/plain`, keeping executed notebooks readable outside
Runwatch.

## Resource event

```json
{
  "schema_version": 2,
  "event_id": "uuid",
  "event": "resource_created",
  "resource": {
    "provider": "aws",
    "type": "sagemaker_processing_job",
    "id": "job-name",
    "logical_key": "feature-build",
    "region": "us-east-1",
    "account_id": null,
    "ownership": "exclusive",
    "metadata": {}
  },
  "lifecycle": {
    "monitor": true,
    "blocking": true,
    "stop_on_cancel": true,
    "retain_logs": true,
    "poll_interval_seconds": null,
    "final_log_drain_seconds": null
  }
}
```

The provider identity is `(provider, account_id, region, type, id)`. A non-null
`logical_key` reconciles replay of the same provider resource while retaining its
cursor. If replay emits a new provider `id` for that key, Runwatch preserves the old
row as `superseded` and monitors a fresh active row. A logical key does not make
provider creation itself idempotent; notebook code should still prefer deterministic
provider identifiers.

Ownership is either `exclusive`, `borrowed`, or `external`. Manual stop requires
exclusive ownership and adapter support. `stop_on_cancel=true` opts the resource into
the provider-stop cascade when the overall run is cancelled.

Lifecycle validation is adapter-aware and applies to notebook events and static config.
SageMaker Processing and S3 manifests have native terminal states. S3 prefixes, local
file counts, and local line counts may block only with a configured completion
condition. CloudWatch metrics/logs and system metrics cannot block. `stop_on_cancel`
requires an exclusive resource whose adapter supports stop.

The `local.dashboard` resource is an external, nonblocking registration for an
already-running loopback web application. Its provider ID is the validated localhost
URL, while optional metadata supplies a display name, health path, exact expected HTTP
status, and request timeout. Runwatch persists the registration and health observations
but recreates LAN proxies and Cloudflare tunnels after process recovery.

```python
from runwatch import local

local.emit_dashboard(
    "http://127.0.0.1:8501",
    name="Training dashboard",
    health_path="/_stcore/health",
    logical_key="training-dashboard",
)
```

Runwatch injects `run_id`, `cell_index`, `attempt`, and `kernel_epoch` while consuming
the event. Notebook code cannot supply those execution fields.

## Progress event

```json
{
  "schema_version": 2,
  "event_id": "uuid",
  "event": "progress",
  "completed": 180,
  "total": 400,
  "unit": "partitions",
  "message": "Building features",
  "metrics": {"rows": 9000000}
}
```

Completed values must be nonnegative, totals must be positive, and completed must not
exceed total.

Python kernels automatically translate standard, auto, async, and notebook tqdm bars
to this event shape when `notebook.capture_tqdm` is enabled. Tqdm metadata is carried in
`metrics` as `source`, `progress_id`, `position`, `rate`, `elapsed_seconds`, and
`closed`. Runwatch applies `notebook.tqdm_min_interval_seconds` in addition to tqdm's
own display throttling and always emits initial and terminal states. Display IDs keep
the executed notebook bounded to one structured output per bar.

## S3 progress manifest

The S3 manifest adapter reads a separate versioned JSON object:

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

Status is `running`, `completed`, or `failed`; metrics must be scalar.

## S3 prefix cursors

S3 prefix monitors persist aggregate counts and the last lexicographic key. Limited
scans continue from that key on the next poll. Once reconciled, normal polls use
`StartAfter` and periodically perform a full scan to account for deletion, overwrite,
or insertion before the cursor. Until that reconciliation, aggregate counts assume an
append-only prefix. Observations expose `scan_mode`, `scan_in_progress`,
`counts_reconciled_at`, and `full_rescan_seconds`.

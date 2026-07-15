# Structured resource and progress events

Runwatch kernel resource-event schema version 2 recognizes:

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

## Third-party resource adapters

Provider integrations can live in separately distributed packages. Register each
adapter class under the `runwatch.adapters` entry-point group using the exact
`provider.resource_type` handled by the class:

```toml
[project.entry-points."runwatch.adapters"]
"example.batch_job" = "example_runwatch:BatchJobAdapter"
```

An adapter subclasses `ResourceAdapter`, declares its identity and capabilities, and
implements asynchronous inspection:

```python
from typing import Any

from runwatch.models import ResourceObservation, ResourceStatus
from runwatch.resources import ResourceAdapter


class BatchJobAdapter(ResourceAdapter):
    provider = "example"
    resource_type = "batch_job"
    supports_blocking = True

    async def inspect(
        self,
        resource: dict[str, Any],
        cursor: dict[str, Any],
    ) -> ResourceObservation:
        status = await inspect_batch_job(resource["external_id"], cursor)
        return ResourceObservation(
            status=ResourceStatus.COMPLETED if status.done else ResourceStatus.RUNNING,
            terminal=status.done,
            message=status.message,
            metrics={"completed": status.completed},
        )
```

Runwatch discovers entry points lazily and rejects duplicate names, non-adapter
classes, or a class whose `provider.resource_type` does not match its entry-point name.
The adapter constructor receives an `AdapterContext` as `self.context`. It exposes the
run `working_dir`, supervisor-owned namespaced `settings`, and
`service(name, factory)` for lazily sharing a process-local client between adapter
instances. Provider packages own their dependencies and client setup; they should not
place credentials or raw provider responses in observations.

For a blocking resource, a provider terminal observation does not settle the resource
until its configured final log drain finishes and Runwatch durably closes the monitor.
If the controller stops in that interval, reopening the run restores the terminal
monitor and retries the final inspection before run finalization. Nonblocking monitors
remain best effort and do not delay run completion.

Set `supports_blocking = True` only when the resource has a real terminal condition.
Override `has_terminal_condition()` when that depends on registration metadata.
Set `supports_stop = True` and implement `stop()` only when Runwatch can safely stop
exclusively owned resources. `validate_registration()` can enforce any additional
provider-neutral lifecycle constraints. `close()` may release adapter-owned clients.
The supervisor extra and the third-party adapter package must be installed in the
controller environment; a notebook kernel that only emits generic protocol events can
continue to use the dependency-light base package.

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

# Architecture

Runwatch is a single local supervisor process with four authority domains:

```text
user notebook ↔ conflict-guarded cell write-back
  → source.ipynb
  → NotebookRunner / nbclient / kernel epoch
  → RunSupervisor + durable SQLite actions
  → ResourceManager + typed adapters
  → authenticated mobile dashboard, CLI, notifications
```

The kernel is a child process and may be replaced. SQLite and the run artifacts are the
durable authority for run state; AWS remains authoritative for AWS resource state.

## Notebook lifecycle

```text
created → starting → running
                       ├─ failure → paused → resume in live kernel
                       │                    └→ restart/replay in a new kernel epoch
                       ├─ cells done → waiting_external
                       └─ remote resource stop → cancelling → cancelled
        → succeeded | failed | cancelled
```

`input.ipynb` is immutable. `source.ipynb` is a normal, output-free nbformat v4 file
that humans and agents may edit. The runner owns partial and final executed notebooks.
At every settled cell boundary it atomically publishes the current executed notebook
back to the user-owned notebook. A durable hash guard refuses publication when that
notebook changed outside Runwatch, preserving both the external edit and the partial
checkpoint.

Rolling checkpoint requests use monotonic generations. The event-loop thread serializes
an immutable notebook snapshot before a worker performs filesystem I/O, and a request
that arrives during that write remains pending for the next generation. A transient
rolling-checkpoint failure emits a durable diagnostic and retries with bounded backoff;
recovery emits a matching diagnostic instead of silently losing the worker.

Write-back conflict detection is intentionally described as best effort. Runwatch
fingerprints the destination content and metadata, writes and synchronizes a replacement,
then checks the fingerprint again immediately before atomic replacement. This catches
observable concurrent saves but cannot eliminate the final comparison/rename window:
portable local filesystems do not offer an atomic content-compare-and-replace primitive.
Operators should edit the run-owned `source.ipynb`, not the original notebook, while a
run is active.

A cell execution timeout follows the same paused recovery path after Runwatch
interrupts and synchronizes the kernel. If synchronization cannot be proven, the
action requires a new kernel instead of offering an unsafe live resume.

Run cancellation first persists `cancelling`, then escalates an active cell through
kernel interrupt, graceful kernel shutdown, provisioner terminate, and provisioner kill.
Each stage has a configurable grace period. Stage failures are events rather than an
unbounded wait, repeated requests reuse the same cancellation work, and Runwatch can
detach from a client await that remains stuck after process-level escalation. The cell
is recorded as interrupted and normal cancellation still stops eligible owned resources
before the run settles as `cancelled`.

A live resume accepts edits only to the failed and future cells. Structural changes or
edits to an already executed cell require restart. Restart reads the complete source
notebook and starts a new kernel epoch. Replay begins at zero unless the local operator
explicitly selects a later start cell.

For Python kernels, each kernel epoch installs optional tqdm instrumentation before the
first user cell. It wraps tqdm's standard and notebook display frontends without
replacing their native output. Structured progress uses a Jupyter display ID, so later
refreshes update one notebook output in place. The runner consumes both initial
`display_data` and later `update_display_data` messages, validates them through the
normal progress-event model, and keeps instrumentation failures isolated from notebook
execution.

## Durable actions

Local CLI recovery and resource stops insert SQLite actions before side effects,
including when the original controller process is gone. Each action is
bound to expected kernel, attempt, source, or resource versions and moves through:

```text
requested → executing → completed | rejected | failed
```

Only one supervisor owns a run lock. The lock and run record bind a PID to its process
start time, host, boot identity, and controller token so PID reuse or a previous boot
cannot impersonate the old owner. A lock from another host is never reclaimed as a
local stale lock. Pending actions and resource-monitor cursors survive process restart.

Notification intents and per-destination delivery attempts also live in SQLite. A slow
or failing webhook cannot block another destination, failed deliveries use bounded
exponential backoff, and an attempt interrupted by process shutdown returns to a
recoverable state. Worker failures are collected and journaled during close rather than
being discarded.

The notification outbox stores a versioned presentation rather than an internal event
or snapshot. Event-specific presenters allowlist small run, cell, and resource
summaries before enqueueing. Delivery streams only response headers, does not follow
redirects, and persists typed error categories without request URLs or response bodies.
Periodic status uses one replaceable, reported intent slot and a direct aggregate query
instead of loading the recovery snapshot.

The run manifest is the desired-state journal for notification credential maintenance.
An offline rotation writes it first, then one SQLite transaction consolidates legacy
terminal deduplication aliases, maps actual delivery rows to a same-shape destination
set, preserves already successful per-destination delivery, resets the remaining
attempts, sanitizes legacy presentations, and updates the metadata copy of the config.
`NotificationManager.start()` performs this reconciliation before its no-destination
fast path and before recovering or claiming work, which completes a crash interrupted
between the manifest and database steps. Purge advances the routing cursor to the event
high-water mark while disabling routing, so enabling notifications later never replays
pre-purge events.

Successful run directories are temporary operational state. The default 90-second
post-terminal linger keeps the final dashboard state observable. Cleanup then waits a
separate bounded interval for notification routing and every outbox item to reach a
terminal result. A nonterminal outbox or drain error retains the successful run, emits
`run.cleanup_retained`, and gives the operator `runwatch open RUN_DIR` to restart the
workers without rerunning the notebook. The recovery controller conservatively retains
the run when `open` closes; it did not itself observe normal notebook finalization and
therefore cannot authorize automatic successful-run cleanup.

When cleanup is eligible, the controller keeps its run lock and publishes a sibling
cleanup fence before deleting the run directory. The sibling survives that deletion and
prevents a successor from acquiring ownership until destructive cleanup is complete.
Only then is the fence released and empty Runwatch parent directories removed. Paused,
failed, cancelled, interrupted, write-back-conflicted, and explicitly retained runs
remain available for inspection or recovery.

## Persistence and filesystem model

Per-run directories are mode `0700`; newly created run artifacts, the lock, manifest,
token, and SQLite database are mode `0600`. Atomic notebook and manifest publication
writes an exclusive temporary file, synchronizes its contents, replaces the destination,
and synchronizes the parent directory where supported. Replacing the original notebook
preserves its existing mode.

SQLite uses WAL mode with `synchronous=NORMAL`. Together with atomic artifact writes,
this is designed for ordinary process crashes on local POSIX filesystems. It is not a
claim of strict durability across sudden host power loss, storage-controller failure,
filesystem corruption, or broken `fsync` semantics. Runwatch currently supports local
Linux and macOS filesystems, which are exercised in CI. Windows and shared/network
filesystems are unsupported; the lock protocol deliberately refuses to infer that a
foreign-host record is stale and is not a distributed lease.

## Resource protocol

Notebook cells emit `application/vnd.runwatch.resource+json`. The event carries a
provider/type identity, logical reconciliation key, ownership, lifecycle, and typed
metadata. The runner supplies the cell attempt and kernel epoch.

Adapters implement inspection and may optionally implement stop. Observations and
cursors are persisted after each poll. Observation history and resource log tails are
bounded by both row count and encoded byte size. Independent hard per-record UTF-8 byte
ceilings reject oversized full resource registrations, standalone cursors, aggregate
observations, event payloads, and notification records at the final SQLite boundary;
delivery diagnostics are truncated to valid UTF-8 within their own ceiling. CloudWatch
metric cards keep the full current lookback, while history
persists only new or revised timestamp samples rather than duplicating the lookback on
every poll. History is evenly downsampled across the retained range for mobile charts.
The general event journal is likewise bounded by count and bytes; high-volume cell
output uses coalesced transient refresh events instead of growing SQLite without limit.
Aggregate byte settings are retention targets, while the per-record settings are hard
admission limits. The newest admissible observation or event remains available even
when it alone exceeds its aggregate retention target, and core notebook state, durable
actions, and the notification outbox are not discarded to enforce a total database
size. Unrouted notification-source events are also protected until the durable
notification cursor consumes them, so a routing backlog may temporarily exceed the
event target rather than lose an at-least-once notification.

Each adapter also declares whether it can safely block and validates conditional
terminal metadata. The same validation runs for static configuration, public emitters,
and raw notebook events, preventing nonterminal metrics or logs from gating a run.

Only SageMaker Processing initially supports stop. Confirming a remote stop requests
normal run cancellation, interrupts the notebook, and stops all other eligible owned
resources. Eligibility is checked centrally before cancellation. Observation-only
metric/log changes do not invalidate a stop confirmation, while provider-state,
ownership, disposition, and terminal-state changes do.

`local.dashboard` is a special external observation resource. Its adapter health-checks
an explicit loopback URL. The share runtime creates one root-mounted authenticated
reverse proxy per active registration: it binds to the LAN for LAN mode, or binds to
loopback and receives its own quick tunnel for Cloudflare mode. The durable resource is
restored from SQLite, but proxy ports and tunnel URLs are process-owned ephemeral state.
Runwatch snapshots expose only a same-origin Open route, which performs a fresh resource
and link-state check before redirecting the paired browser.

Terminal SageMaker log collection follows CloudWatch pagination until tokens stabilize
or the configured page/line bound is reached. A bounded drain is explicitly reported
as truncated instead of silently presented as complete.

## Dashboard boundary

The durable snapshot is an internal recovery model. `/api/state` maps it into explicit
dashboard response models and allowlists only presentation fields. Notebook source,
resolved configuration, notification destinations, provider cursors and raw responses,
controller credentials, and dedicated internal-path fields do not cross that boundary.
Displayed cell output and tracebacks, resource logs, chart series, and event text are
bounded but remain user-controlled content and can contain sensitive values.

`/notebook` is an explicit, authenticated exception to that bounded presentation
boundary. On demand, it selects the current run's authoritative durable notebook
(partial checkpoint while cells are active, final output after cells finish, or source
before the first checkpoint), renders it in memory, and binds the wrapper and iframe to
one content digest. The rendering is never persisted and is refreshed only by a new
page request. Its child route has a separate CSP so it can be framed by the Runwatch
wrapper while remaining scriptless, networkless, and sandboxed. The frame keeps its
same-origin identity, but not script capability, so the trusted wrapper can observe
downward scrolling and compact its mobile metadata header. All other Runwatch documents
retain frame denial.

SSE is an invalidation channel, not a second persistence API. Initial and replayed
events contain only sequence, timestamp, and type; the browser reloads the sanitized
snapshot for details. The dashboard document, state API, SSE response, and authenticated
redirects all set `Cache-Control: no-store`.

## Process recovery

`runwatch resume RUN_DIR` queues a live resume when the original process owns the run.
If it is gone, the command reacquires the lock, restores adapters from SQLite, starts a
new kernel epoch, and replays from cell zero. Arbitrary Python memory is reconstructed
through replay; Runwatch does not attempt whole-process serialization.

## Agent boundary

Runwatch is agent-agnostic. Codex and Claude use their native filesystem, shell, and
remote applications. A repository skill explains the stable Runwatch CLI and nbformat
workflow; no agent subprocess, transcript, or repair proposal is stored by Runwatch.
The `status`, `context`, and `events` commands expose versioned CLI presentation models;
they never serialize the internal recovery snapshot or raw event payloads.

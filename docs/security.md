# Security notes

Runwatch is a personal, single-user operations console, not an Internet-hardened
multi-user service.

## Controls

- Per-run directories are restricted to the current user (`0700`), and new run-state
  files, lock records, manifests, tokens, and the SQLite database use mode `0600`.
  Original-notebook write-back preserves the notebook's existing mode.
- Local Jupyter manager-to-kernel channels require CurveZMQ encryption. Kernel
  startup fails closed when the selected kernelspec does not advertise Curve support.
- Random pairing token created atomically with `0600` permissions and validated before
  reuse.
- Token exchange for an `HttpOnly`, `SameSite=Strict` cookie.
- No permissive CORS policy.
- Dashboard state uses explicit presentation models. It excludes notebook source,
  resolved configuration, notification endpoints, provider cursors/raw responses,
  controller credentials, and dedicated internal-path fields while bounding displayed
  output, logs, tracebacks, chart series, and event text.
- SSE carries only an event sequence, timestamp, and type as an invalidation signal.
  Dashboard documents, snapshots, SSE, and authenticated redirects use
  `Cache-Control: no-store`.
- The full notebook snapshot is a separate authenticated view, not part of the bounded
  dashboard state API. Rendering is content-digest cached in memory, input and output
  sizes are bounded, and the parent page binds its metadata and iframe to the same
  saved generation.
- Rendered notebooks are sanitized and stripped of scripts, frames, forms, remote
  media, navigation targets, event handlers, raw cells, widget state, and
  JavaScript-only output bundles. The child response additionally uses an iframe
  sandbox and a route-specific CSP that disables scripts, connections, forms, objects,
  and child frames. It grants `allow-same-origin` without `allow-scripts` only so the
  trusted parent can observe downward scrolling and collapse the mobile metadata
  header; it grants no form, navigation, popup, download, or modal capability.
- The dashboard has one mutation: stop an exclusive, adapter-stoppable resource.
- Detailed stop confirmation includes the cascading cancellation scope.
- Optimistic resource versions reject stale stop confirmations.
- AWS credentials remain server-side.
- Pairing URLs are excluded from notifications.
- Notification routing replays persisted events and retries each destination
  independently. Delivery is at least once, with stable `Idempotency-Key` and
  `X-Runwatch-Intent-ID` headers for receiver-side deduplication.
- Notification and agent-CLI output use dedicated allowlisted presentation schemas.
  Webhooks do not receive resolved configuration, raw event payloads, tracebacks,
  logs, metrics, provider responses, account IDs, controller credentials, or internal
  paths. Delivery errors contain only a bounded error category and optional HTTP
  status; response bodies and credential-bearing request URLs are never retained.
- Notification response bodies are not read, redirects are not followed, encoded
  webhook payloads are bounded, and non-loopback plain HTTP destinations require the
  explicit `notifications.allow_insecure_http` opt-in.
- Periodic notifications use a single rolling durable intent and a lightweight state
  summary, so long-running jobs do not accumulate successful reminder history.
- Unexpected event-routing failures use a durable bounded retry counter and one
  allowlisted dead-letter event. Replay cannot repeatedly reset a destination that has
  already exhausted its delivery attempts.
- Offline `runwatch notifications rotate` uses manifest-first desired state and a
  same-topology transaction to replace credentials in every pending and terminal
  delivery row. Startup finishes an interrupted rotation before recovery workers can
  claim old destinations. Legacy terminal deduplication aliases are consolidated before
  delivery, and a one-time egress migration removes historical URL/body-bearing errors.
- Offline `runwatch notifications purge --yes` deletes intents and deliveries, disables
  routing at the current event high-water mark, scrubs notification diagnostic payloads
  and metadata, and enables SQLite secure deletion before best-effort WAL truncation and
  compaction. Filesystem snapshots, backups, and storage-device history remain outside
  Runwatch's deletion guarantee.
- Successful cleanup waits boundedly for notification routing and outbox attempts to
  become terminal. Incomplete delivery retains the run for `runwatch open` recovery.
- Run ownership verifies PID, process start time, host, boot identity, and controller
  identity. Destructive successful cleanup remains fenced until deletion completes.
- Configuration rejects unresolved environment-variable placeholders, and configured
  webhook and ntfy base URLs must be absolute HTTP(S) URLs.
- CSP, frame denial, no-referrer, MIME-sniffing, and restrictive permissions-policy
  response headers are enabled.
- Stop eligibility is checked before notebook cancellation and rejects inactive,
  terminal, borrowed, unsupported, or stale resources.
- Linked dashboards accept only explicit localhost or loopback HTTP(S) targets. Their
  LAN and Cloudflare shares use a dedicated pairing-token proxy; the proxy removes the
  Runwatch query token, bearer token, and reserved auth cookie before forwarding.
- Linked-dashboard public URLs and tokens are not included in state snapshots or
  persisted resource metadata. The resource card receives only an authenticated,
  same-origin Open route.

## Limitations

- A pairing URL is a bearer credential and can leak through browser history, copied
  messages, logs, or screenshots.
- `no-store` reduces cache retention but does not make a pairing URL safe to disclose.
- When Cloudflare sharing and ntfy are both configured, a replacement pairing URL is
  deliberately sent to that ntfy topic as a clickable target. Use a private topic and
  treat access to it as equivalent to access to the dashboard.
  An authenticated dashboard intentionally displays bounded notebook output,
  tracebacks, resource identifiers and metrics, and log tails. That user-controlled
  text can itself contain paths, secrets, or other sensitive values.
- The notebook snapshot intentionally exposes the complete saved notebook source and
  outputs to the paired browser. Sanitization prevents active browser behavior; it is
  not secret detection or redaction. Source, text, tables, images, tracebacks, and
  rendered output may contain credentials or sensitive data.
- LAN mode is plain HTTP; a quick tunnel is transport, not complete identity.
- There is no user identity, role separation, token-revocation UI, explicit CSRF token,
  rate limiting, or tamper-resistant audit.
- Notebook code and local agents are trusted and may access the host credentials.
- Retained run state is sensitive. It can contain notebook source and output,
  tracebacks, paths, resource identifiers and logs, notification destinations and
  payloads, durable actions, and the dashboard bearer token. File modes are access
  control for an ordinary single-user host, not encryption or protection from the
  account owner, privileged users, host compromise, backups, or disk forensics.
- Run names, explicitly safe logical resource labels, and bounded error types remain
  visible in presentations. Runwatch does not attempt to discover arbitrary secrets in
  every user-authored name; keep credentials out of display labels.
- Rotation intentionally preserves destination topology while an outbox exists. Purge
  first when webhook count or ntfy presence must change, then rotate from the empty
  outbox to record the replacement topology.
- A local user able to edit the run directory can alter its source or SQLite state.
- A linked dashboard is trusted local code. Once paired, its complete native interface
  is exposed through the proxy, including any mutations that application provides.
- Runwatch authenticates access to a linked dashboard but does not add application-level
  authorization, rewrite its HTML, or guarantee compatibility with absolute URLs that
  deliberately point at another origin.

Prefer localhost, a private overlay network, or trusted LAN access. Use temporary public
tunnels only when necessary. Shared deployments need an identity-aware proxy, TLS,
CSRF protection, rate limiting, constrained IAM, and a separate low-privilege resource
control service.

Runwatch's locking and durability model is for local POSIX filesystems on Linux and
macOS. Shared/network filesystems and Windows are unsupported. SQLite WAL with
`synchronous=NORMAL`, atomic replacement, and file/directory synchronization protect
ordinary process-crash recovery; they do not promise survival of every sudden power
loss, filesystem corruption, storage-controller failure, or filesystem that violates
normal `fsync` and atomic-rename semantics.

Monitoring generally needs SageMaker describe, CloudWatch metric/log read, and S3 read
permissions. Remote stop additionally needs `sagemaker:StopProcessingJob`. Constrain
that permission by ARN or run tags where the account supports it.

# Security notes

Runwatch is a personal, single-user operations console, not an Internet-hardened
multi-user service.

## Controls

- Random pairing token created atomically with `0600` permissions and validated before
  reuse.
- Token exchange for an `HttpOnly`, `SameSite=Strict` cookie.
- No permissive CORS policy.
- The dashboard has one mutation: stop an exclusive, adapter-stoppable resource.
- Detailed stop confirmation includes the cascading cancellation scope.
- Optimistic resource versions reject stale stop confirmations.
- AWS credentials remain server-side.
- Pairing URLs are excluded from notifications.
- Notification routing replays persisted events and retries each destination
  independently. Delivery is at least once, with stable `Idempotency-Key` and
  `X-Runwatch-Intent-ID` headers for receiver-side deduplication.
- Run ownership verifies PID plus process start time and controller identity.
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
- LAN mode is plain HTTP; a quick tunnel is transport, not complete identity.
- There is no user identity, role separation, token-revocation UI, explicit CSRF token,
  rate limiting, or tamper-resistant audit.
- Notebook code and local agents are trusted and may access the host credentials.
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

Monitoring generally needs SageMaker describe, CloudWatch metric/log read, and S3 read
permissions. Remote stop additionally needs `sagemaker:StopProcessingJob`. Constrain
that permission by ARN or run tags where the account supports it.

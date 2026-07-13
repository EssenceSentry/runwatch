# Replayable fake sessions

Runwatch's checked-in fake session exercises the real notebook executor, dashboard,
local monitors, LAN pairing flow, and ntfy handoff without creating AWS resources.

```bash
web_artifacts_fake_sessions/runwatch/run.sh
```

The launcher runs in the foreground, prints the authoritative tokenized dashboard URL
and QR code, and stores mutable state under the session's ignored `.runtime/` directory.
See [the session guide](runwatch/README.md) for phone, notification, and replay options.

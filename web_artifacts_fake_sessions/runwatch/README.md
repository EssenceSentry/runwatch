# Runwatch fake session

Replays a self-contained notebook against the real Runwatch executor and dashboard.
The notebook emits structured progress, host and kernel metrics, local file counts,
incremental line counts, bounded log tails, and a final results summary. It creates no
cloud resources and requires no AWS credentials.

The launcher also starts a small results dashboard on loopback. The notebook registers
it with `local.emit_dashboard`, and the Runwatch resource card exposes an authenticated
**Open Simulation results** action through the selected LAN or Cloudflare sharing mode.
The results page updates while batches complete and remains available while Runwatch is
running.

From this directory:

```bash
./run.sh
```

Or from the repository root:

```bash
web_artifacts_fake_sessions/runwatch/run.sh
```

The launcher validates the notebook, starts a fresh durable Runwatch run, binds the
dashboard to the trusted LAN, prints the pairing URL and QR code, and keeps the
dashboard available until interrupted. It also creates a private, unguessable ntfy
topic for the replay. After opening the dashboard on the phone, use **Open ntfy app**
to subscribe; terminal run notifications are then delivered to that topic. Runtime
runs and notebook-generated files are isolated under `.runtime/` and ignored by Git.
By default, the simulated work lasts about five minutes. During fake sessions only,
the mascot cycles through every Runwatch status and labels the simulated state; the
dashboard's actual run status and metrics remain unchanged.

Useful variants:

```bash
# Run a shorter 20-second smoke replay.
./run.sh --batches 20 --delay-seconds 1

# Reuse an existing private ntfy topic.
./run.sh --ntfy-topic my-private-runwatch-topic

# Keep the replay completely local and disable external notifications.
./run.sh --share none --no-ntfy

# Use a fixed dashboard port without printing a QR code.
./run.sh --port 8765 --no-qr
```

Press `Ctrl+C` when finished. Each replay receives a new run directory, so previous
runs remain available under `.runtime/runs/` for recovery testing.

The phone and Mac must be on the same trusted network, and guest-network client
isolation or a host firewall can still block LAN access. The default replay makes HTTPS
requests to `ntfy.sh`; use `--no-ntfy` to disable them.

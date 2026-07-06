# Architecture

This document explains how `iter` is built and why, for anyone reading the
source or extending it. For day-to-day usage, see [SKILL.md](SKILL.md) or
the [website](https://iter.birukjember.tech).

## Problem

Local dev servers accumulate: a `npm run dev` here, a `cargo run` there, a
forgotten `python manage.py runserver` from three weeks ago. Ports drift
between runs, nothing dies on its own, and killed processes don't restart
themselves by design — the two failure modes this project targets are
**port amnesia** (which port did that project use last time?) and
**zombie processes** (dev servers that run forever, unmonitored, eating
memory and file descriptors).

`iter` solves both with one idea: every server the user cares about runs
behind a **stable local proxy port** that never changes across restarts,
and the daemon watches real byte traffic through that proxy to decide when
a backend has gone idle.

## Components

```
┌─────────┐   UDS, newline-delimited JSON    ┌──────────────────────┐
│ iter CLI │ ───────────────────────────────▶ │       daemon          │
└─────────┘ ◀─────────────────────────────── │ (background process) │
                                               └──────────────────────┘
                                                  │            │
                                          spawns  │            │ proxies
                                                   ▼            ▼
                                          backend process   stable port
                                          (npm/cargo/...)    (raw TCP)
```

- **CLI** (`main.rs`, `client.rs`) — a thin `clap`-based frontend. Every
  subcommand connects to the daemon's Unix domain socket, sends a
  `Request`, prints the `Response`. If no daemon is running, the CLI
  spawns one (detached, `setsid`-style) and retries the connection.
- **Daemon** (`daemon.rs`) — the long-lived process. Owns the registry of
  managed servers, accepts CLI connections on the UDS, and on startup
  reconciles against the last-persisted state (see below).
- **Server supervisor** (`server.rs`) — one Tokio task per managed server.
  Spawns the backend command, binds the stable port, runs the proxy loop,
  and tears the whole thing down on idle-kill or explicit stop.
- **Protocol** (`protocol.rs`) — the `Request`/`Response` enum contract
  between CLI and daemon, serialized as one JSON object per line.
- **State** (`state.rs`) — atomic persistence of the registry to
  `~/.iter/state.json` so a killed daemon can be reconciled on restart.
- **Logs** (`logs.rs`) — per-server log files under `~/.iter/logs/`, plus
  rotation.
- **Paths** (`paths.rs`) — all `~/.iter/*` path construction in one place.

## The proxy is byte-level, not HTTP-aware

The stable-port listener does not parse HTTP, WebSocket frames, or any
other protocol — it accepts a TCP connection, opens a matching connection
to the backend's real (ephemeral) port, and pumps bytes in both
directions with `tokio::io::copy`-style loops. This is deliberate:

- It works for anything that speaks TCP — HTTP/1.1, HTTP/2, WebSocket
  upgrades, gRPC, raw sockets — without protocol-specific code paths.
- It keeps the daemon's trusted surface area small: the only thing it
  ever does with backend bytes is count them and move them.

Every non-empty read or write on either side of the pump updates a shared
`Instant` (behind the server's registry entry). That timestamp — not a
fixed timer — is what idle-kill checks against.

## Idle-kill, precisely

A background check (per managed server) compares `Instant::now()` against
the last-activity timestamp on an interval. If the gap exceeds the
server's configured `--idle` duration, the supervisor:

1. Marks the entry `IdleKilled` in the registry.
2. Sends the backend a graceful termination signal, then force-kills after
   a bounded grace period if it hasn't exited.
3. Persists the updated registry to disk.
4. **Does not restart it.** A killed server stays killed until a user
   explicitly runs `iter restart <name>`. This is a hard product
   guarantee, not a bug — auto-restart would silently reintroduce the
   zombie-process problem this tool exists to prevent.

There is no fixed-interval kill timer anywhere in the codebase — only
traffic-derived idleness. A server under constant load never gets killed
no matter how long it's been running.

## Persistence and crash recovery

The daemon can die — `kill -9`, a crash, a reboot — while backend
processes it spawned keep running as orphans. Losing the daemon must not
mean losing track of those orphans, so:

- On every state-changing event (start, stop, idle-kill, crash-detect),
  the daemon writes a full snapshot of the registry to
  `~/.iter/state.json` using a write-to-temp-then-rename, so a crash
  mid-write can never corrupt the file the next daemon reads.
- On startup, before accepting any CLI connections, the daemon loads that
  snapshot and reconciles each entry:
  - If the entry was `running` and its recorded pid is still alive
    (checked via a zero-signal `kill(pid, 0)` probe), the daemon
    **adopts** it: rebinds the stable port, resumes supervision
    (idle-kill, stop, log rotation) against the *existing* backend
    process without restarting it.
  - If the pid is gone, the entry is registered as `Crashed` — visible in
    `iter list`, restartable with `iter restart`, but not silently
    resurrected.
  - Anything that was already `Stopped` or `IdleKilled` is restored as
    such.

### Why backend logs are written directly to files, not piped

An earlier version piped each backend's stdout/stderr through the daemon
process (`Stdio::piped()` + a pump task copying into the log file). That
works fine until the daemon dies: the pipe's read end disappears with it,
and the *orphaned* backend process's next attempt to write a log line
blocks on a broken pipe — wedging the backend's request handling with it,
even though the process itself is still "alive" by every liveness check.

The fix was to give each backend **direct file descriptors**
(`Stdio::from(File)`) opened in append mode onto its log file, independent
of the daemon's lifetime. Two independently-opened `O_APPEND` file
descriptors on the same inode is safe under POSIX — writes remain atomic
per write(2) call — so stdout and stderr can each hold their own fd
without a pump task or coordination through the daemon at all. This also
means log writing keeps working exactly as before during the
adoption window when a new daemon hasn't reconciled yet.

Log rotation had to change to match: since the daemon no longer owns the
write path, rename-based rotation (moving the current file aside) would
silently orphan the backend's fd onto the renamed file forever. Rotation
instead truncates the log file in place (read the content, write it to
`<name>.old`, then `set_len(0)` on the same path/fd) — safe with an
external writer because the inode and open fd never change.

## Panic safety

The design goal is: a malformed request, a backend that misbehaves, or an
unexpected I/O error on one connection must never bring down the daemon
or another server's supervision task.

- Every per-connection and per-server unit of work runs in its own
  `tokio::spawn`ed task. A panic inside one task is contained by Tokio's
  task boundary and does not propagate to the daemon's main loop or to
  other servers.
- External input (socket bytes, JSON payloads, server names used as file
  path components) is never trusted enough to justify `.unwrap()`,
  `.expect()`, or unchecked indexing. `validate_name()` in `protocol.rs`
  rejects anything outside `[A-Za-z0-9._-]` before a name is ever used to
  construct a filesystem path, closing off path traversal.
- Fallible I/O (state persistence, log rotation) logs and continues
  rather than treating disk errors as fatal — a failed persist should
  degrade the crash-recovery guarantee, not take down a working daemon.

## Directory layout on disk

```
~/.iter/
├── daemon.sock       # Unix domain socket, CLI <-> daemon
├── daemon.log        # daemon's own log (rotated by rename; daemon owns the fd)
├── state.json         # atomic snapshot of the registry
├── state.json.tmp     # write-then-rename staging file
└── logs/
    ├── <name>.log      # backend stdout+stderr, live
    └── <name>.log.old  # previous rotation
```

# iter — supervise local dev servers behind stable ports

This file documents `iter` for any CLI coding agent (Claude Code, Codex,
etc.) that starts dev servers on the user's behalf. It mirrors
[SKILL.md](SKILL.md), which some agents load under a different convention.

`iter` is a small CLI + background daemon that runs dev servers (npm run
dev, cargo run, python manage.py runserver, rails server, etc.) behind a
**stable proxy port that never changes**, auto-kills them after a period of
real network inactivity, and never auto-restarts a killed server. It works
for any TCP-based process in any language — it never parses the protocol,
only raw bytes and process lifecycle.

Use this when starting a dev server on the user's behalf so that:
- repeated runs of the same project always land on the same port (no more
  hunting for "which port did it pick this time").
- forgotten servers don't run forever — they get killed after idle time and
  stay dead until someone asks for them again.
- two different projects that both want port 3000 don't collide — each
  gets its own stable port that you choose.

There is no need to manually start a daemon; the CLI starts it in the
background automatically the first time any command is run.

## Before starting a server

Always run `iter list` first to see what's already managed. If a server
with the name/project you want is already `running`, don't start a
duplicate — just use its existing stable port. If it's `idle-killed` or
`stopped`, use `iter restart <name>` instead of `iter start` (a `start`
with a name that's already registered will fail with a clear error).

Pick a `<name>` that identifies the project (e.g. the directory or repo
name — `my-app-frontend`, `billing-api`). Pick a `--port` that's unlikely
to collide — check `iter list` for ports already claimed, and prefer a
per-project convention (e.g. 3000-range for one project, 4000-range for
another) rather than reusing the language's default dev port.

## Commands

### `iter start <name> --port <stable_port> [--idle <minutes>] [--cwd <dir>] [--port-env <VAR>] -- <command...>`

Starts a managed server.

- `<name>` — unique identifier for this server. Fails if already in use.
- `--port` — the stable port users/browsers actually connect to. `iter`
  picks the real backend port automatically; this stable port never
  changes across restarts. Fails if already claimed by another managed
  server.
- `--idle` — minutes of zero traffic before the backend is killed
  (default 30).
- `--cwd` — working directory to run the command in (default: current
  directory).
- `--port-env` — env var used to tell the command which port to bind to
  (default `PORT`).
- `<command...>` — the command to run, after `--`. Any argument containing
  the literal text `{port}` gets that substring replaced with the real
  backend port, for tools that take a `--port <n>` flag instead of reading
  an env var.

Examples:

```
iter start my-app --port 3000 -- npm run dev
iter start api --port 8000 --idle 15 -- python manage.py runserver {port}
iter start static-site --port 5500 --cwd ~/projects/site -- python3 -m http.server {port}
iter start worker --port 9000 --port-env APP_PORT -- cargo run
```

### `iter stop <name>`

Fully stops the server (kills the backend process and tears down the
proxy). The registry entry stays around in `iter list` as `stopped` so it
can be brought back later with `iter restart`.

### `iter restart <name>`

Restarts a `stopped`, `idle-killed`, or `crashed` server on the same
stable port with a freshly allocated backend port. Fails if the server is
currently running.

### `iter list`

Table of every managed server: name, stable port, backend port, status
(`running` / `idle-killed` / `stopped` / `crashed`), time remaining before
idle-kill, PID, and the underlying command. Always check this before
starting a new server to avoid duplicate names/ports.

### `iter logs <name> [-n <lines>]`

Recent stdout/stderr for a server (default last 100 lines). Useful for
checking whether a server actually started successfully or crashed.

### `iter shutdown-all`

Stops every managed server and the daemon itself. Use when the user wants
a clean slate (e.g. before shutting down their machine, or to reset all
state during troubleshooting).

## Notes for agents

- Idle detection is based on real TCP bytes flowing through the proxy in
  either direction, not connection count or a fixed timer — a server with
  an open but silent connection (e.g. an idle WebSocket) will still be
  killed if no bytes actually move.
- A killed server never restarts itself. If the user says "start my app"
  and `iter list` shows it `idle-killed`, use `iter restart`, not `iter
  start`.
- `iter start`/`iter restart` return a non-zero exit code with a message on
  `stderr` on failure (name conflict, port conflict, bad command, etc.) —
  check the exit code before assuming the server came up.

## Surviving a daemon crash or reboot

The daemon persists every server's config and last-known status to
`~/.iter/state.json` after every change (start, stop, restart, idle-kill,
crash). If the daemon itself dies unexpectedly (crash, `kill -9`, machine
reboot), the next `iter` command transparently starts a fresh daemon, which
reads that file and reconciles it against reality before accepting any
requests:

- if a server was `running` and its pid is still alive, the daemon
  **re-adopts** it — re-binding the same stable port and resuming the
  proxy/idle-tracking against the still-running backend, with no
  interruption to already-open connections beyond the brief window while
  the new daemon starts.
- if a server was `running` but its pid is gone, it's marked `crashed` —
  visible via `iter list`, restartable via `iter restart`.
- servers that were already `stopped`/`idle-killed`/`crashed` stay that way.

Per-server logs are written directly by the child process to its log file
(not piped through the daemon), specifically so an adopted backend's
logging keeps working even though the daemon that originally spawned it is
gone.

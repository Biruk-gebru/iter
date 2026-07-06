# iter

Start it. Forget it.

`iter` is a small CLI + background daemon that supervises local dev
servers (`npm run dev`, `cargo run`, `python manage.py runserver`, `rails
server`, ...) behind a **stable proxy port that never changes**, kills
them after real network idleness — never a fixed timer — and never
auto-restarts a killed server. It works with anything that speaks TCP: it
never parses the protocol, only raw bytes and process lifecycle.

Website: [iter.birukjember.tech](https://iter.birukjember.tech)

## Why

Local dev servers accumulate. Ports drift between runs, nothing dies on
its own, and forgotten processes run forever eating memory and file
descriptors. `iter` fixes both:

- **Stable ports** — `iter start web --port 5173 -- npm run dev` always
  puts `web` on `:5173`, run after run, project after project.
- **Idle-kill on real traffic** — the proxy tracks actual bytes flowing
  through it in either direction. A server goes quiet, it gets killed
  after your configured idle window. A server under constant load never
  does, no matter how long it's been running.
- **No auto-restart** — a killed server stays dead until you explicitly
  run `iter restart`. No zombie processes, no surprise revivals.

## Install

### Prebuilt binary (macOS / Linux, no Rust required)

```
curl -fsSL "https://github.com/Biruk-gebru/iter/releases/latest/download/iter-$(uname -m | sed 's/arm64/aarch64/')-$([ "$(uname -s)" = Darwin ] && echo apple-darwin || echo unknown-linux-gnu).tar.gz" | tar xz && sudo install iter /usr/local/bin/iter
```

This downloads a self-contained, statically-built binary for your
platform from [Releases](https://github.com/Biruk-gebru/iter/releases) —
**no Rust toolchain, no `cargo`, no runtime dependency on Rust at all**.
It's a normal native executable, same as any Go or C binary you'd
download and run. Covers Intel/Apple Silicon Macs and x86_64/arm64 Linux.
There's no native Windows build yet — Windows users can run the Linux
binary under WSL.

### Build from source (requires Rust)

```
cargo install --git https://github.com/Biruk-gebru/iter
```

Only this path needs `cargo`/`rustc` installed — it compiles the binary
locally instead of downloading a prebuilt one.

## Quick start

```
iter start web --port 5173 -- npm run dev
```

That's it — no separate daemon-management step. The first `iter` command
you ever run starts the background daemon automatically. `web` is now
reachable on `:5173` and will stay on that port every time you start it
again, from any project.

```
iter list                  # see every managed server, status, idle countdown
iter logs web -n 200        # recent stdout/stderr
iter restart web             # bring a stopped/idle-killed server back
iter stop web                 # fully stop it
iter shutdown-all              # stop everything, including the daemon
```

Run `iter --help` for the full command reference.

## Using iter with a coding agent

If you use Claude Code, Codex, or another CLI coding agent to manage your
dev servers:

```
iter init-agent
```

writes a short block into your agent's global config
(`~/.claude/CLAUDE.md`, `~/.codex/AGENTS.md`) so it automatically prefers
`iter` over running servers directly, in every project, from then on. Add
`--project` to scope it to the current repo's `AGENTS.md` instead. Run
`iter agents` any time to print the full usage reference an agent needs —
it's embedded in the binary, so it works even without the repo cloned.

## How it works

The stable-port listener is a raw byte-level TCP proxy — it doesn't parse
HTTP, WebSocket frames, or anything else, so it transparently supports
HTTP/1.1, HTTP/2, WebSocket upgrades, gRPC, or any other TCP protocol.
Every read/write through it updates a shared "last activity" timestamp;
idle-kill compares that against `Instant::now()`, not a timer. State is
persisted to `~/.iter/state.json` after every change, so if the daemon
itself dies (crash, `kill -9`, reboot), the next `iter` command starts a
fresh daemon that reconciles against the last snapshot and re-adopts any
backend process still alive, rather than losing track of it.

Full internals — proxy design, persistence/crash-recovery, panic-safety
rules, on-disk layout — are documented in
[ARCHITECTURE.md](ARCHITECTURE.md).

## License

MIT

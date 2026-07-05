use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

use crate::paths;
use crate::protocol::ServerInfo;
use crate::state::{self, PersistedServer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Running,
    IdleKilled,
    Stopped,
    Crashed,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Running => "running",
            Status::IdleKilled => "idle-killed",
            Status::Stopped => "stopped",
            Status::Crashed => "crashed",
        }
    }
}

pub struct ServerConfig {
    pub name: String,
    pub stable_port: u16,
    pub idle: Duration,
    pub cwd: PathBuf,
    pub port_env: String,
    pub command: Vec<String>,
}

struct SharedInfo {
    status: Status,
    backend_port: Option<u16>,
    pid: Option<u32>,
}

pub struct ServerEntry {
    pub config: ServerConfig,
    shared: StdMutex<SharedInfo>,
    last_activity: StdMutex<Instant>,
    stop_tx: StdMutex<Option<oneshot::Sender<()>>>,
    stopping: AtomicBool,
}

pub type Registry = Arc<AsyncMutex<HashMap<String, Arc<ServerEntry>>>>;

pub fn new_registry() -> Registry {
    Arc::new(AsyncMutex::new(HashMap::new()))
}

impl ServerEntry {
    pub fn info(&self) -> ServerInfo {
        let shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        let remaining_secs = if shared.status == Status::Running {
            let elapsed = self
                .last_activity
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .elapsed();
            Some(self.config.idle.saturating_sub(elapsed).as_secs())
        } else {
            None
        };
        ServerInfo {
            name: self.config.name.clone(),
            stable_port: self.config.stable_port,
            backend_port: shared.backend_port,
            status: shared.status.as_str().to_string(),
            remaining_secs,
            pid: shared.pid,
            command: self.config.command.join(" "),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.shared.lock().unwrap_or_else(|e| e.into_inner()).status,
            Status::Running
        )
    }

    pub fn snapshot(&self) -> PersistedServer {
        let shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        PersistedServer {
            name: self.config.name.clone(),
            stable_port: self.config.stable_port,
            idle_secs: self.config.idle.as_secs(),
            cwd: self.config.cwd.to_string_lossy().to_string(),
            port_env: self.config.port_env.clone(),
            command: self.config.command.clone(),
            status: shared.status.as_str().to_string(),
            backend_port: shared.backend_port,
            pid: shared.pid,
        }
    }
}

/// Checks whether a process with the given pid is still alive, without
/// requiring us to own it as a child (used to detect orphaned backends
/// left behind by a previous daemon that died or was killed).
fn pid_alive(pid: u32) -> bool {
    // Signal 0 performs no action but still fails with ESRCH if the pid
    // doesn't exist, which is the standard way to probe liveness.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

fn kill_pid(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

/// Owns either a real child process we spawned, or just the pid of an
/// orphan adopted from a previous daemon run. The orphan variant can't be
/// `wait()`-ed on directly since we never had a `Child` handle for it, so
/// its liveness is polled instead.
enum ChildHandle {
    Owned(Child),
    Orphan(u32),
}

impl ChildHandle {
    /// Waits for the process to exit. For an owned child this is a real
    /// `wait()`; for an adopted orphan it's a bounded liveness poll, since
    /// there is no portable "notify me when this arbitrary pid exits"
    /// primitive without owning the process.
    async fn wait(&mut self) {
        match self {
            ChildHandle::Owned(c) => {
                let _ = c.wait().await;
            }
            ChildHandle::Orphan(pid) => loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if !pid_alive(*pid) {
                    return;
                }
            },
        }
    }

    fn start_kill(&mut self) {
        match self {
            ChildHandle::Owned(c) => {
                let _ = c.start_kill();
            }
            ChildHandle::Orphan(pid) => kill_pid(*pid),
        }
    }

    async fn reap(&mut self) {
        match self {
            ChildHandle::Owned(c) => {
                let _ = tokio::time::timeout(Duration::from_secs(3), c.wait()).await;
            }
            ChildHandle::Orphan(pid) => {
                for _ in 0..30 {
                    if !pid_alive(*pid) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

fn substitute_port(tokens: &[String], port: u16) -> Vec<String> {
    tokens
        .iter()
        .map(|t| t.replace("{port}", &port.to_string()))
        .collect()
}

/// Ask the OS for a free ephemeral port by binding to port 0, reading back
/// the assigned port, then releasing the listener so the child process can
/// bind to it itself. There is an inherent, unavoidable race between the
/// release and the child's bind; this is the standard "ask the kernel for a
/// free port" pattern and the race window is extremely small in practice.
async fn allocate_backend_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|e| format!("failed to allocate a backend port: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("failed to read allocated port: {e}"))?
        .port();
    drop(listener);
    Ok(port)
}

pub enum StartError {
    NameInUse,
    PortInUse(String),
    Other(String),
}

/// Starts a brand-new managed server (used by both `start` and `restart`).
/// Binds the stable listener synchronously so port conflicts are reported
/// immediately to the caller rather than discovered later inside a
/// detached task.
pub async fn launch(
    registry: &Registry,
    config: ServerConfig,
    daemon_log: Arc<AsyncMutex<crate::logs::LogWriter>>,
) -> Result<Arc<ServerEntry>, StartError> {
    {
        let map = registry.lock().await;
        if map.contains_key(&config.name) {
            return Err(StartError::NameInUse);
        }
        for other in map.values() {
            if other.config.stable_port == config.stable_port {
                return Err(StartError::PortInUse(other.config.name.clone()));
            }
        }
    }

    let backend_port = allocate_backend_port().await.map_err(StartError::Other)?;

    let stable_listener = TcpListener::bind(("127.0.0.1", config.stable_port))
        .await
        .map_err(|e| {
            StartError::Other(format!(
                "failed to bind stable port {}: {e}",
                config.stable_port
            ))
        })?;

    if config.command.is_empty() {
        return Err(StartError::Other("command must not be empty".to_string()));
    }
    let argv = substitute_port(&config.command, backend_port);
    let program = &argv[0];
    let mut cmd = Command::new(program);
    if argv.len() > 1 {
        cmd.args(&argv[1..]);
    }
    cmd.current_dir(&config.cwd);
    cmd.env(&config.port_env, backend_port.to_string());
    cmd.stdin(Stdio::null());

    // Redirect stdout/stderr straight to the log file's own file descriptor
    // rather than piping them through the daemon and copying bytes over in
    // a pump task. This is deliberate: if stdout/stderr were piped, the
    // read end lives in *our* process, so if the daemon dies (crash, `kill
    // -9`) the child's write end becomes a broken pipe and the child can
    // wedge or crash the next time it tries to log a line — exactly the
    // kind of failure this adoption feature exists to survive. A direct
    // file redirection has no reader to lose; the child keeps writing to
    // disk regardless of whether our daemon is alive.
    let (stdout_stdio, stderr_stdio) = match open_log_stdio(&config.name) {
        Ok(v) => v,
        Err(e) => {
            drop(stable_listener);
            return Err(StartError::Other(e));
        }
    };
    cmd.stdout(stdout_stdio);
    cmd.stderr(stderr_stdio);

    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            drop(stable_listener);
            return Err(StartError::Other(format!(
                "failed to spawn '{program}': {e}"
            )));
        }
    };
    let pid = child.id();

    let mut child_handle = ChildHandle::Owned(child);

    let (stop_tx, stop_rx) = oneshot::channel();
    let entry = Arc::new(ServerEntry {
        config,
        shared: StdMutex::new(SharedInfo {
            status: Status::Running,
            backend_port: Some(backend_port),
            pid,
        }),
        last_activity: StdMutex::new(Instant::now()),
        stop_tx: StdMutex::new(Some(stop_tx)),
        stopping: AtomicBool::new(false),
    });

    {
        let mut map = registry.lock().await;
        // Re-check for a racing concurrent start with the same name/port
        // that landed while we were spawning the process.
        if map.contains_key(&entry.config.name) {
            child_handle.start_kill();
            drop(stable_listener);
            return Err(StartError::NameInUse);
        }
        map.insert(entry.config.name.clone(), entry.clone());
    }
    persist(registry, &daemon_log).await;

    tokio::spawn(supervise(
        entry.clone(),
        stable_listener,
        child_handle,
        stop_rx,
        registry.clone(),
        daemon_log,
    ));

    Ok(entry)
}

/// Re-registers a backend process that was left running by a previous
/// daemon instance (detected via the persisted state file at startup). We
/// never had a `Child` handle for it, so it's monitored by pid liveness
/// instead of `wait()`, and its stdout/stderr are not recaptured (they were
/// already redirected to its log file by the daemon that originally spawned
/// it, or are simply lost if that daemon didn't persist far enough).
pub async fn adopt(
    registry: &Registry,
    config: ServerConfig,
    backend_port: u16,
    pid: u32,
    daemon_log: Arc<AsyncMutex<crate::logs::LogWriter>>,
) {
    let name = config.name.clone();
    let stable_listener = match TcpListener::bind(("127.0.0.1", config.stable_port)).await {
        Ok(l) => l,
        Err(e) => {
            let mut log = daemon_log.lock().await;
            log.write_line(&format!(
                "[{name}] could not reclaim stable port {} for orphaned pid {pid} (left running, unmanaged): {e}",
                config.stable_port
            ))
            .await;
            insert_inert(registry, config, Status::Crashed, Some(backend_port), None).await;
            return;
        }
    };

    let (stop_tx, stop_rx) = oneshot::channel();
    let entry = Arc::new(ServerEntry {
        config,
        shared: StdMutex::new(SharedInfo {
            status: Status::Running,
            backend_port: Some(backend_port),
            pid: Some(pid),
        }),
        last_activity: StdMutex::new(Instant::now()),
        stop_tx: StdMutex::new(Some(stop_tx)),
        stopping: AtomicBool::new(false),
    });

    {
        let mut map = registry.lock().await;
        map.insert(name.clone(), entry.clone());
    }
    {
        let mut log = daemon_log.lock().await;
        log.write_line(&format!(
            "[{name}] reclaimed orphaned process pid {pid} on backend port {backend_port} after daemon restart"
        ))
        .await;
    }

    tokio::spawn(supervise(
        entry,
        stable_listener,
        ChildHandle::Orphan(pid),
        stop_rx,
        registry.clone(),
        daemon_log,
    ));
}

/// Registers a server entry that is not currently running (already
/// stopped, idle-killed, or crashed) without spawning any supervisor task.
/// Used when reconciling persisted state for servers that weren't left
/// with a live backend process.
pub async fn insert_inert(
    registry: &Registry,
    config: ServerConfig,
    status: Status,
    backend_port: Option<u16>,
    pid: Option<u32>,
) {
    let name = config.name.clone();
    let entry = Arc::new(ServerEntry {
        config,
        shared: StdMutex::new(SharedInfo {
            status,
            backend_port,
            pid,
        }),
        last_activity: StdMutex::new(Instant::now()),
        stop_tx: StdMutex::new(None),
        stopping: AtomicBool::new(false),
    });
    let mut map = registry.lock().await;
    map.insert(name, entry);
}

/// Best-effort persistence: failures are logged but never propagated,
/// since the in-memory registry stays authoritative for the current daemon
/// run regardless of whether the on-disk mirror could be updated.
async fn persist(registry: &Registry, daemon_log: &Arc<AsyncMutex<crate::logs::LogWriter>>) {
    if let Err(e) = state::save(registry).await {
        let mut log = daemon_log.lock().await;
        log.write_line(&format!("failed to persist state: {e}"))
            .await;
    }
}

/// Opens two independent, append-mode file descriptors onto the same
/// per-server log file for the child's stdout and stderr. Two separate
/// `O_APPEND` file descriptors on the same file are safe to write from
/// concurrently — each `write()` atomically seeks to the current end of
/// file first — so stdout and stderr lines interleave correctly without a
/// pump task or in-process buffering.
fn open_log_stdio(name: &str) -> Result<(Stdio, Stdio), String> {
    let path = paths::server_log_path(name)?;
    let open_one = || -> Result<Stdio, String> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map(Stdio::from)
            .map_err(|e| format!("failed to open log file {path:?}: {e}"))
    };
    Ok((open_one()?, open_one()?))
}

enum StopReason {
    Idle,
    Stopped,
    Crashed,
}

async fn supervise(
    entry: Arc<ServerEntry>,
    listener: TcpListener,
    mut child: ChildHandle,
    mut stop_rx: oneshot::Receiver<()>,
    registry: Registry,
    daemon_log: Arc<AsyncMutex<crate::logs::LogWriter>>,
) {
    let mut idle_interval = tokio::time::interval(Duration::from_secs(5));
    let backend_port = entry
        .shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .backend_port
        .unwrap_or(0);

    let reason = loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let activity = ActivityHandle(entry.clone());
                        tokio::spawn(handle_connection(stream, backend_port, activity));
                    }
                    Err(e) => {
                        let mut log = daemon_log.lock().await;
                        log.write_line(&format!(
                            "[{}] proxy accept error: {e}",
                            entry.config.name
                        ))
                        .await;
                    }
                }
            }
            _ = idle_interval.tick() => {
                let elapsed = entry.last_activity.lock().unwrap_or_else(|e| e.into_inner()).elapsed();
                if elapsed >= entry.config.idle {
                    break StopReason::Idle;
                }
            }
            _ = child.wait() => {
                if entry.stopping.load(Ordering::SeqCst) {
                    break StopReason::Stopped;
                }
                let mut log = daemon_log.lock().await;
                log.write_line(&format!("[{}] process exited", entry.config.name)).await;
                break StopReason::Crashed;
            }
            _ = &mut stop_rx => {
                break StopReason::Stopped;
            }
        }
    };

    entry.stopping.store(true, Ordering::SeqCst);
    child.start_kill();
    child.reap().await;
    drop(listener);

    let new_status = match reason {
        StopReason::Idle => Status::IdleKilled,
        StopReason::Stopped => Status::Stopped,
        StopReason::Crashed => Status::Crashed,
    };
    {
        let mut shared = entry.shared.lock().unwrap_or_else(|e| e.into_inner());
        shared.status = new_status;
        shared.pid = None;
    }

    // The registry entry (running, idle-killed, stopped, or crashed) is
    // kept either way so `iter restart` and `iter list` still see it, and
    // the on-disk mirror reflects the transition immediately in case the
    // daemon itself goes away before the next change.
    persist(&registry, &daemon_log).await;
}

struct ActivityHandle(Arc<ServerEntry>);

impl ActivityHandle {
    fn touch(&self) {
        if let Ok(mut guard) = self.0.last_activity.lock() {
            *guard = Instant::now();
        }
    }
}

async fn handle_connection(client: TcpStream, backend_port: u16, activity: ActivityHandle) {
    let mut backend = None;
    for attempt in 0..5u8 {
        match TcpStream::connect(("127.0.0.1", backend_port)).await {
            Ok(s) => {
                backend = Some(s);
                break;
            }
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {}
        }
    }
    let backend = match backend {
        Some(b) => b,
        None => return,
    };

    let activity = Arc::new(activity);
    let (client_r, client_w) = client.into_split();
    let (backend_r, backend_w) = backend.into_split();

    let a1 = activity.clone();
    let a2 = activity.clone();
    let t1 = tokio::spawn(pump(client_r, backend_w, a1));
    let t2 = tokio::spawn(pump(backend_r, client_w, a2));
    let _ = t1.await;
    let _ = t2.await;
}

async fn pump<R, W>(mut src: R, mut dst: W, activity: Arc<ActivityHandle>)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match src.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        if dst.write_all(&buf[..n]).await.is_err() {
            break;
        }
        activity.touch();
    }
    let _ = dst.shutdown().await;
}

/// Stops a running or idle-killed server in place, transitioning it to
/// `Stopped`. Waits for the supervisor task to actually finish tearing down
/// the proxy listener and child process before returning, so the caller can
/// rely on the port being free for a subsequent `start`/`restart`.
pub async fn stop(entry: &Arc<ServerEntry>) -> Result<(), String> {
    let tx = entry
        .stop_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    match tx {
        Some(tx) => {
            entry.stopping.store(true, Ordering::SeqCst);
            let _ = tx.send(());
            // Poll briefly for the supervisor to finish; it always
            // transitions out of Running within a few seconds.
            for _ in 0..100 {
                if !entry.is_active() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        }
        None => Ok(()),
    }
}

/// Reconciles persisted state from a previous daemon run against reality.
/// For each persisted server: if it was running and its pid is still alive,
/// the backend process is an orphan left behind by a dead daemon and gets
/// re-adopted (its stable port is re-bound and it's supervised again); if
/// its pid is gone, it's registered as `crashed`; anything already
/// inactive (stopped/idle-killed/crashed) is registered as-is. Called once
/// at daemon startup, before the control socket starts accepting requests.
pub async fn reconcile(
    registry: &Registry,
    persisted: Vec<PersistedServer>,
    daemon_log: Arc<AsyncMutex<crate::logs::LogWriter>>,
) {
    for p in persisted {
        let config = ServerConfig {
            name: p.name,
            stable_port: p.stable_port,
            idle: Duration::from_secs(p.idle_secs),
            cwd: PathBuf::from(p.cwd),
            port_env: p.port_env,
            command: p.command,
        };

        if p.status == Status::Running.as_str() {
            match (p.pid, p.backend_port) {
                (Some(pid), Some(backend_port)) if pid_alive(pid) => {
                    adopt(registry, config, backend_port, pid, daemon_log.clone()).await;
                }
                _ => {
                    let mut log = daemon_log.lock().await;
                    log.write_line(&format!(
                        "[{}] was running before daemon restart but its process is gone; marking crashed",
                        config.name
                    ))
                    .await;
                    drop(log);
                    insert_inert(registry, config, Status::Crashed, p.backend_port, None).await;
                }
            }
            continue;
        }

        let status = match p.status.as_str() {
            "idle-killed" => Status::IdleKilled,
            "crashed" => Status::Crashed,
            _ => Status::Stopped,
        };
        insert_inert(registry, config, status, p.backend_port, None).await;
    }

    persist(registry, &daemon_log).await;
}

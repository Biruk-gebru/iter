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

use crate::logs::LogWriter;
use crate::protocol::ServerInfo;

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
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            drop(stable_listener);
            return Err(StartError::Other(format!(
                "failed to spawn '{program}': {e}"
            )));
        }
    };
    let pid = child.id();

    let log_writer = match LogWriter::open(&config.name).await {
        Ok(w) => Arc::new(AsyncMutex::new(w)),
        Err(e) => {
            let _ = child.start_kill();
            drop(stable_listener);
            return Err(StartError::Other(e));
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    if let Some(out) = stdout {
        spawn_log_pump(out, log_writer.clone(), "out");
    }
    if let Some(err) = stderr {
        spawn_log_pump(err, log_writer.clone(), "err");
    }

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
            let _ = child.start_kill();
            drop(stable_listener);
            return Err(StartError::NameInUse);
        }
        map.insert(entry.config.name.clone(), entry.clone());
    }

    tokio::spawn(supervise(
        entry.clone(),
        stable_listener,
        child,
        stop_rx,
        registry.clone(),
        daemon_log,
    ));

    Ok(entry)
}

fn spawn_log_pump<R>(reader: R, writer: Arc<AsyncMutex<LogWriter>>, stream_name: &'static str)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let mut w = writer.lock().await;
                    w.write_line(&format!("[{stream_name}] {line}")).await;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });
}

enum StopReason {
    Idle,
    Stopped,
    Crashed,
}

async fn supervise(
    entry: Arc<ServerEntry>,
    listener: TcpListener,
    mut child: Child,
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
            status = child.wait() => {
                if entry.stopping.load(Ordering::SeqCst) {
                    break StopReason::Stopped;
                }
                let mut log = daemon_log.lock().await;
                match status {
                    Ok(s) => log.write_line(&format!("[{}] process exited: {s}", entry.config.name)).await,
                    Err(e) => log.write_line(&format!("[{}] wait() failed: {e}", entry.config.name)).await,
                }
                break StopReason::Crashed;
            }
            _ = &mut stop_rx => {
                break StopReason::Stopped;
            }
        }
    };

    entry.stopping.store(true, Ordering::SeqCst);
    let _ = child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
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

    if matches!(new_status, Status::Crashed) {
        // A crashed process leaves the name/port reserved but idle; the
        // registry entry is kept so `iter restart` and `iter list` still
        // see it, matching how idle-kill and explicit stop behave.
        let map = registry.lock().await;
        debug_assert!(map.contains_key(&entry.config.name));
    }
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

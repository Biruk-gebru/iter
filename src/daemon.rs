use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex as AsyncMutex;

use crate::logs::{self, LogWriter};
use crate::paths;
use crate::protocol::{validate_name, Request, Response, ServerInfo, StartRequest};
use crate::server::{self, Registry, ServerConfig, StartError};

pub async fn run() -> Result<(), String> {
    paths::ensure_dirs()?;
    let socket_path = paths::socket_path()?;
    // Remove a stale socket from a previous, no-longer-running daemon.
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    let listener = UnixListener::bind(&socket_path)
        .map_err(|e| format!("failed to bind control socket {socket_path:?}: {e}"))?;

    let daemon_log_path = paths::daemon_log_path()?;
    let daemon_log = Arc::new(AsyncMutex::new(
        LogWriter::open_at(&daemon_log_path)
            .await
            .map_err(|e| format!("failed to open daemon log: {e}"))?,
    ));

    {
        let mut log = daemon_log.lock().await;
        log.write_line("daemon starting").await;
    }

    let registry = server::new_registry();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let registry = registry.clone();
                        let daemon_log = daemon_log.clone();
                        let shutdown_tx = shutdown_tx.clone();
                        tokio::spawn(async move {
                            handle_client(stream, registry, daemon_log, shutdown_tx).await;
                        });
                    }
                    Err(e) => {
                        let mut log = daemon_log.lock().await;
                        log.write_line(&format!("accept error: {e}")).await;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                let mut log = daemon_log.lock().await;
                log.write_line("shutdown-all requested, stopping all servers").await;
                drop(log);
                stop_all(&registry).await;
                let _ = std::fs::remove_file(&socket_path);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_client(
    stream: UnixStream,
    registry: Registry,
    daemon_log: Arc<AsyncMutex<LogWriter>>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    line.clear();
    let n = match reader.read_line(&mut line).await {
        Ok(n) => n,
        Err(_) => return,
    };
    if n == 0 {
        return;
    }
    let request: Request = match serde_json::from_str(line.trim_end()) {
        Ok(r) => r,
        Err(e) => {
            let resp = Response::Error {
                message: format!("bad request: {e}"),
            };
            let _ = send(&mut write_half, &resp).await;
            return;
        }
    };

    let response = match request {
        Request::Ping => Response::Pong,
        Request::Start(req) => handle_start(&registry, &daemon_log, req).await,
        Request::Stop { name } => handle_stop(&registry, &name).await,
        Request::Restart { name } => handle_restart(&registry, &daemon_log, &name).await,
        Request::List => handle_list(&registry).await,
        Request::Logs { name, lines } => handle_logs(&name, lines).await,
        Request::ShutdownAll => {
            let _ = shutdown_tx.send(()).await;
            Response::Ok
        }
    };

    let _ = send(&mut write_half, &response).await;
}

async fn send(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &Response,
) -> Result<(), ()> {
    let mut bytes = serde_json::to_vec(resp).map_err(|_| ())?;
    bytes.push(b'\n');
    write_half.write_all(&bytes).await.map_err(|_| ())?;
    Ok(())
}

async fn handle_start(
    registry: &Registry,
    daemon_log: &Arc<AsyncMutex<LogWriter>>,
    req: StartRequest,
) -> Response {
    if let Err(e) = validate_name(&req.name) {
        return Response::Error { message: e };
    }
    let cwd = PathBuf::from(&req.cwd);
    let config = ServerConfig {
        name: req.name,
        stable_port: req.stable_port,
        idle: Duration::from_secs(req.idle_minutes.saturating_mul(60)),
        cwd,
        port_env: req.port_env,
        command: req.command,
    };
    match server::launch(registry, config, daemon_log.clone()).await {
        Ok(_) => Response::Ok,
        Err(StartError::NameInUse) => Response::Error {
            message: "a server with that name already exists".to_string(),
        },
        Err(StartError::PortInUse(owner)) => Response::Error {
            message: format!("stable port is already claimed by server '{owner}'"),
        },
        Err(StartError::Other(msg)) => Response::Error { message: msg },
    }
}

async fn handle_stop(registry: &Registry, name: &str) -> Response {
    let entry = {
        let map = registry.lock().await;
        map.get(name).cloned()
    };
    match entry {
        None => Response::Error {
            message: format!("no managed server named '{name}'"),
        },
        Some(entry) => match server::stop(&entry).await {
            Ok(()) => Response::Ok,
            Err(e) => Response::Error { message: e },
        },
    }
}

async fn handle_restart(
    registry: &Registry,
    daemon_log: &Arc<AsyncMutex<LogWriter>>,
    name: &str,
) -> Response {
    let entry = {
        let map = registry.lock().await;
        map.get(name).cloned()
    };
    let entry = match entry {
        None => {
            return Response::Error {
                message: format!("no managed server named '{name}'"),
            }
        }
        Some(e) => e,
    };
    if entry.is_active() {
        return Response::Error {
            message: format!("server '{name}' is already running; stop it first"),
        };
    }
    // Ensure it is fully torn down (idempotent if already stopped) and
    // remove it so `launch` can re-register the name cleanly.
    let _ = server::stop(&entry).await;
    {
        let mut map = registry.lock().await;
        map.remove(name);
    }
    let config = ServerConfig {
        name: entry.config.name.clone(),
        stable_port: entry.config.stable_port,
        idle: entry.config.idle,
        cwd: entry.config.cwd.clone(),
        port_env: entry.config.port_env.clone(),
        command: entry.config.command.clone(),
    };
    match server::launch(registry, config, daemon_log.clone()).await {
        Ok(_) => Response::Ok,
        Err(StartError::NameInUse) => Response::Error {
            message: "a server with that name already exists".to_string(),
        },
        Err(StartError::PortInUse(owner)) => Response::Error {
            message: format!("stable port is already claimed by server '{owner}'"),
        },
        Err(StartError::Other(msg)) => Response::Error { message: msg },
    }
}

async fn handle_list(registry: &Registry) -> Response {
    let map = registry.lock().await;
    let servers: Vec<ServerInfo> = map.values().map(|e| e.info()).collect();
    Response::Servers { servers }
}

async fn handle_logs(name: &str, lines: usize) -> Response {
    if validate_name(name).is_err() {
        return Response::Error {
            message: "invalid server name".to_string(),
        };
    }
    match logs::tail_lines(name, lines).await {
        Ok(lines) => Response::LogLines { lines },
        Err(e) => Response::Error { message: e },
    }
}

async fn stop_all(registry: &Registry) {
    let entries: Vec<_> = {
        let map = registry.lock().await;
        map.values().cloned().collect()
    };
    for entry in entries {
        let _ = server::stop(&entry).await;
    }
}

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::paths;
use crate::protocol::{Request, Response};

async fn try_connect() -> Option<UnixStream> {
    let path = paths::socket_path().ok()?;
    UnixStream::connect(&path).await.ok()
}

/// Ensures the background daemon is running, starting it transparently in
/// the background on first use so the user never has to invoke `iter
/// daemon` themselves.
pub async fn ensure_daemon() -> Result<(), String> {
    if try_connect().await.is_some() {
        return Ok(());
    }

    paths::ensure_dirs()?;
    let exe = std::env::current_exe().map_err(|e| format!("failed to find own executable: {e}"))?;
    let daemon_log = paths::daemon_log_path()?;
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&daemon_log)
        .map_err(|e| format!("failed to open daemon log {daemon_log:?}: {e}"))?;
    let log_file_err = log_file
        .try_clone()
        .map_err(|e| format!("failed to duplicate daemon log handle: {e}"))?;

    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log_file))
        .stderr(std::process::Stdio::from(log_file_err))
        .spawn()
        .map_err(|e| format!("failed to start daemon: {e}"))?;

    for _ in 0..100 {
        if try_connect().await.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("daemon did not start within 5 seconds; check ~/.iter/daemon.log".to_string())
}

pub async fn send(req: &Request) -> Result<Response, String> {
    let path = paths::socket_path()?;
    let stream = UnixStream::connect(&path)
        .await
        .map_err(|e| format!("failed to connect to daemon: {e}"))?;
    let (read_half, mut write_half) = stream.into_split();

    let mut bytes =
        serde_json::to_vec(req).map_err(|e| format!("failed to encode request: {e}"))?;
    bytes.push(b'\n');
    write_half
        .write_all(&bytes)
        .await
        .map_err(|e| format!("failed to send request: {e}"))?;

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .await
        .map_err(|e| format!("failed to read response: {e}"))?;
    if n == 0 {
        return Err("daemon closed the connection without responding".to_string());
    }
    serde_json::from_str(line.trim_end()).map_err(|e| format!("bad response from daemon: {e}"))
}

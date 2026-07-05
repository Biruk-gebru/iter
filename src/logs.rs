use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader};

use crate::paths;

/// Cap log files at ~5 MiB before rotating to a single `.old` backup. Keeps
/// disk usage bounded for long-running dev servers (and the daemon log
/// itself) without pulling in a full log-rotation dependency.
const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024;

pub struct LogWriter {
    path: PathBuf,
    old_path: PathBuf,
    file: File,
    written_since_check: u64,
}

impl LogWriter {
    pub async fn open(name: &str) -> Result<Self, String> {
        let path = paths::server_log_path(name)?;
        let old_path = paths::server_log_old_path(name)?;
        Self::open_at_with_old(path, old_path).await
    }

    /// Opens a log file at an arbitrary path (used for the daemon's own
    /// log, which isn't tied to a managed server name).
    pub async fn open_at(path: &Path) -> Result<Self, String> {
        let mut old_path = path.as_os_str().to_os_string();
        old_path.push(".old");
        Self::open_at_with_old(path.to_path_buf(), PathBuf::from(old_path)).await
    }

    async fn open_at_with_old(path: PathBuf, old_path: PathBuf) -> Result<Self, String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| format!("failed to open log file {path:?}: {e}"))?;
        Ok(Self {
            path,
            old_path,
            file,
            written_since_check: 0,
        })
    }

    pub async fn write_line(&mut self, line: &str) {
        let mut buf = line.as_bytes().to_vec();
        buf.push(b'\n');
        // Best-effort: a failed log write must never bring down the
        // supervisor task, so errors are swallowed here.
        if self.file.write_all(&buf).await.is_err() {
            return;
        }
        self.written_since_check += buf.len() as u64;
        if self.written_since_check > 256 * 1024 {
            self.written_since_check = 0;
            self.rotate_if_needed().await;
        }
    }

    async fn rotate_if_needed(&mut self) {
        let meta = match fs::metadata(&self.path).await {
            Ok(m) => m,
            Err(_) => return,
        };
        if meta.len() < MAX_LOG_BYTES {
            return;
        }
        // Best-effort rotation: if any step fails we just keep appending to
        // the existing (oversized) file rather than risk losing logs.
        if fs::rename(&self.path, &self.old_path).await.is_err() {
            return;
        }
        if let Ok(f) = OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .open(&self.path)
            .await
        {
            self.file = f;
        }
    }
}

/// Reads the last `n` lines from a server's log, pulling from the rotated
/// `.old` file too if the current file alone doesn't have enough lines.
pub async fn tail_lines(name: &str, n: usize) -> Result<Vec<String>, String> {
    let path = paths::server_log_path(name)?;
    let mut current = read_lines_capped(&path, n).await;
    if current.len() < n {
        let old_path = paths::server_log_old_path(name)?;
        let mut old = read_lines_capped(&old_path, n - current.len()).await;
        old.extend(current);
        current = old;
    }
    Ok(current)
}

async fn read_lines_capped(path: &std::path::Path, n: usize) -> Vec<String> {
    if n == 0 {
        return Vec::new();
    }
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    read_last_lines(reader, n).await
}

async fn read_last_lines<R: AsyncRead + Unpin>(reader: BufReader<R>, n: usize) -> Vec<String> {
    use tokio::io::AsyncBufReadExt;
    let mut lines = reader.lines();
    let mut ring: VecDeque<String> = VecDeque::with_capacity(n.min(10_000));
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if ring.len() == n {
                    ring.pop_front();
                }
                ring.push_back(line);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    ring.into_iter().collect()
}

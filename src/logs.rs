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
    /// Opens a log file at an arbitrary path (used for the daemon's own
    /// log; per-server logs are written directly by the child process, not
    /// through a `LogWriter` — see `open_log_stdio` in `server.rs`).
    pub async fn open_at(path: &Path) -> Result<Self, String> {
        let mut old_os = path.as_os_str().to_os_string();
        old_os.push(".old");
        let old_path = PathBuf::from(old_os);
        let path = path.to_path_buf();
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

/// Periodically rotates any per-server log file that has grown past the
/// size cap. Per-server logs are written directly by the child process's
/// own file descriptor (not through `LogWriter`), so they can't be rotated
/// by renaming — the child's fd would keep writing into the renamed file,
/// not a fresh one. Instead the current content is copied to `.old` and
/// the file is truncated in place with `set_len(0)`, which every existing
/// `O_APPEND` writer (including a managed child) sees correctly on its
/// next write, since the kernel recomputes the append offset from the
/// file's current length each time.
pub async fn rotate_server_logs() {
    let dir = match paths::logs_dir() {
        Ok(d) => d,
        Err(_) => return,
    };
    let mut entries = match fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(_) => return,
    };
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(_) => break,
        };
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "log") {
            continue;
        }
        let meta = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.len() >= MAX_LOG_BYTES {
            rotate_in_place(&path).await;
        }
    }
}

async fn rotate_in_place(path: &Path) {
    let mut old_os = path.as_os_str().to_os_string();
    old_os.push(".old");
    let old_path = PathBuf::from(old_os);

    // Best-effort: any failure here just leaves the oversized file in
    // place for the next rotation pass rather than losing log data.
    let contents = match fs::read(path).await {
        Ok(c) => c,
        Err(_) => return,
    };
    if fs::write(&old_path, &contents).await.is_err() {
        return;
    }
    if let Ok(f) = OpenOptions::new().write(true).open(path).await {
        let _ = f.set_len(0).await;
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

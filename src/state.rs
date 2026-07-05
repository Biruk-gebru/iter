use serde::{Deserialize, Serialize};

use crate::paths;
use crate::server::Registry;

/// On-disk representation of one managed server, written after every state
/// change so a restarted daemon can reconcile against reality instead of
/// starting with empty bookkeeping.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedServer {
    pub name: String,
    pub stable_port: u16,
    pub idle_secs: u64,
    pub cwd: String,
    pub port_env: String,
    pub command: Vec<String>,
    pub status: String,
    pub backend_port: Option<u16>,
    pub pid: Option<u32>,
}

/// Snapshots the full registry and writes it to `~/.iter/state.json`. Writes
/// to a temp file and renames into place so a crash mid-write can never
/// leave a truncated/corrupt state file behind. Best-effort: a failure to
/// persist is logged by the caller but never propagated as fatal, since the
/// daemon's in-memory state remains authoritative for the current run.
pub async fn save(registry: &Registry) -> Result<(), String> {
    let snapshot: Vec<PersistedServer> = {
        let map = registry.lock().await;
        map.values().map(|entry| entry.snapshot()).collect()
    };

    let path = paths::state_path()?;
    let tmp_path = paths::state_tmp_path()?;
    let bytes =
        serde_json::to_vec_pretty(&snapshot).map_err(|e| format!("failed to encode state: {e}"))?;

    tokio::fs::write(&tmp_path, &bytes)
        .await
        .map_err(|e| format!("failed to write {tmp_path:?}: {e}"))?;
    tokio::fs::rename(&tmp_path, &path)
        .await
        .map_err(|e| format!("failed to rename {tmp_path:?} to {path:?}: {e}"))?;
    Ok(())
}

/// Loads the last-persisted state. A missing file (first run, or after a
/// clean `shutdown-all`) is treated as "nothing to reconcile", not an
/// error. A corrupt file is logged to stderr (captured in the daemon log
/// once the daemon's own log redirection is set up by the caller that
/// spawned it) and treated the same way, rather than blocking startup.
pub async fn load() -> Vec<PersistedServer> {
    let path = match paths::state_path() {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    match serde_json::from_slice::<Vec<PersistedServer>>(&bytes) {
        Ok(servers) => servers,
        Err(e) => {
            eprintln!("iter: ignoring corrupt state file {path:?}: {e}");
            Vec::new()
        }
    }
}

use std::path::PathBuf;

pub fn home_dir() -> Result<PathBuf, String> {
    std::env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| "HOME environment variable is not set".to_string())
}

pub fn iter_dir() -> Result<PathBuf, String> {
    Ok(home_dir()?.join(".iter"))
}

pub fn socket_path() -> Result<PathBuf, String> {
    Ok(iter_dir()?.join("iter.sock"))
}

pub fn daemon_log_path() -> Result<PathBuf, String> {
    Ok(iter_dir()?.join("daemon.log"))
}

pub fn state_path() -> Result<PathBuf, String> {
    Ok(iter_dir()?.join("state.json"))
}

pub fn state_tmp_path() -> Result<PathBuf, String> {
    Ok(iter_dir()?.join("state.json.tmp"))
}

pub fn logs_dir() -> Result<PathBuf, String> {
    Ok(iter_dir()?.join("logs"))
}

pub fn server_log_path(name: &str) -> Result<PathBuf, String> {
    Ok(logs_dir()?.join(format!("{name}.log")))
}

pub fn server_log_old_path(name: &str) -> Result<PathBuf, String> {
    Ok(logs_dir()?.join(format!("{name}.log.old")))
}

pub fn ensure_dirs() -> Result<(), String> {
    let dir = iter_dir()?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("failed to create {dir:?}: {e}"))?;
    let logs = logs_dir()?;
    std::fs::create_dir_all(&logs).map_err(|e| format!("failed to create {logs:?}: {e}"))?;
    Ok(())
}

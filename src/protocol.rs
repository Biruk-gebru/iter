use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Ping,
    Start(StartRequest),
    Stop { name: String },
    Restart { name: String },
    List,
    Logs { name: String, lines: usize },
    ShutdownAll,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartRequest {
    pub name: String,
    pub stable_port: u16,
    pub idle_minutes: u64,
    pub cwd: String,
    pub port_env: String,
    pub command: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error { message: String },
    Servers { servers: Vec<ServerInfo> },
    LogLines { lines: Vec<String> },
    Pong,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub stable_port: u16,
    pub backend_port: Option<u16>,
    pub status: String,
    pub remaining_secs: Option<u64>,
    pub pid: Option<u32>,
    pub command: String,
}

/// Names are used to build log file paths on disk, so they must be
/// restricted to a safe character set to prevent path traversal.
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name must not be empty".to_string());
    }
    if name.len() > 64 {
        return Err("name must be at most 64 characters".to_string());
    }
    let valid = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if !valid || name == "." || name == ".." {
        return Err("name may only contain letters, digits, '-', '_', and '.'".to_string());
    }
    Ok(())
}

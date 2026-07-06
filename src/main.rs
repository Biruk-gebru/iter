mod client;
mod daemon;
mod init_agent;
mod logs;
mod paths;
mod protocol;
mod server;
mod state;

use clap::{Parser, Subcommand};
use protocol::{Request, Response, StartRequest};

const AGENTS_MD: &str = include_str!("../AGENTS.md");

#[derive(Parser)]
#[command(
    name = "iter",
    version,
    about = "Supervise local dev servers behind stable proxy ports",
    long_about = "iter runs local dev servers (npm run dev, cargo run, python \
manage.py runserver, ...) behind a stable proxy port that never changes across \
restarts, kills them after real network idleness (never a fixed timer), and \
never auto-restarts a killed server.\n\nThere is no separate daemon-management \
step: any command below starts the background daemon automatically on first \
use.",
    after_help = "EXAMPLES:\n    \
    iter start web --port 5173 -- npm run dev\n    \
    iter start api --port 8000 --idle 15 -- python manage.py runserver {port}\n    \
    iter list\n    \
    iter logs web -n 200\n    \
    iter restart web\n    \
    iter stop web\n    \
    iter init-agent\n\n\
For coding-agent integration (Claude Code, Codex, etc.), run `iter agents` for \
embedded usage docs, or `iter init-agent` to wire them into your agent's config \
automatically.\n\n\
Docs: https://github.com/Biruk-gebru/iter"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a managed server behind a stable proxy port.
    Start {
        /// Unique name for this server.
        name: String,
        /// Stable port that clients connect to (never changes).
        #[arg(long)]
        port: u16,
        /// Minutes of no traffic before the backend is killed.
        #[arg(long, default_value_t = 30)]
        idle: u64,
        /// Working directory to run the command in (defaults to cwd).
        #[arg(long)]
        cwd: Option<String>,
        /// Environment variable used to pass the backend port to the command.
        #[arg(long, default_value = "PORT")]
        port_env: String,
        /// The command to run, e.g. `-- npm run dev`. Supports a literal
        /// `{port}` placeholder in any argument.
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Fully stop a managed server, freeing its port and name.
    Stop { name: String },
    /// Restart a stopped or idle-killed server on the same stable port.
    Restart { name: String },
    /// List all managed servers.
    List,
    /// Show recent stdout/stderr for a managed server.
    Logs {
        name: String,
        #[arg(short = 'n', long, default_value_t = 100)]
        lines: usize,
    },
    /// Run the background daemon (not normally invoked directly).
    Daemon,
    /// Stop every managed server and the daemon itself.
    #[command(name = "shutdown-all")]
    ShutdownAll,
    /// Print embedded usage docs for coding agents (Claude Code, Codex, etc).
    Agents,
    /// Wire iter usage instructions into your coding agent's config.
    #[command(name = "init-agent")]
    InitAgent {
        /// Write to this project's AGENTS.md instead of your global agent config.
        #[arg(long)]
        project: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    if matches!(cli.command, Command::Agents) {
        print!("{AGENTS_MD}");
        return;
    }

    if let Command::InitAgent { project } = cli.command {
        if let Err(e) = init_agent::run(project) {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
        return;
    }

    if matches!(cli.command, Command::Daemon) {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("failed to start async runtime: {e}");
                std::process::exit(1);
            }
        };
        if let Err(e) = rt.block_on(daemon::run()) {
            eprintln!("daemon error: {e}");
            std::process::exit(1);
        }
        return;
    }

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start async runtime: {e}");
            std::process::exit(1);
        }
    };
    let result = rt.block_on(run_client(cli.command));
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run_client(command: Command) -> Result<(), String> {
    client::ensure_daemon().await?;

    let request = match command {
        Command::Daemon | Command::Agents | Command::InitAgent { .. } => {
            unreachable!("handled before entering async client path")
        }
        Command::Start {
            name,
            port,
            idle,
            cwd,
            port_env,
            command,
        } => {
            if command.is_empty() {
                return Err(
                    "no command given; usage: iter start <name> --port <p> -- <command...>"
                        .to_string(),
                );
            }
            let cwd = match cwd {
                Some(c) => c,
                None => std::env::current_dir()
                    .map_err(|e| format!("failed to read current directory: {e}"))?
                    .to_string_lossy()
                    .to_string(),
            };
            Request::Start(StartRequest {
                name,
                stable_port: port,
                idle_minutes: idle,
                cwd,
                port_env,
                command,
            })
        }
        Command::Stop { name } => Request::Stop { name },
        Command::Restart { name } => Request::Restart { name },
        Command::List => Request::List,
        Command::Logs { name, lines } => Request::Logs { name, lines },
        Command::ShutdownAll => Request::ShutdownAll,
    };

    let response = client::send(&request).await?;
    print_response(response)
}

fn print_response(response: Response) -> Result<(), String> {
    match response {
        Response::Ok => {
            println!("ok");
            Ok(())
        }
        Response::Pong => {
            println!("pong");
            Ok(())
        }
        Response::Error { message } => Err(message),
        Response::Servers { servers } => {
            print_servers(&servers);
            Ok(())
        }
        Response::LogLines { lines } => {
            for line in lines {
                println!("{line}");
            }
            Ok(())
        }
    }
}

fn print_servers(servers: &[protocol::ServerInfo]) {
    if servers.is_empty() {
        println!("no managed servers");
        return;
    }
    println!(
        "{:<16} {:<8} {:<8} {:<12} {:<10} {:<8} COMMAND",
        "NAME", "PORT", "BACKEND", "STATUS", "REMAINING", "PID"
    );
    for s in servers {
        let backend = s
            .backend_port
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string());
        let remaining = s
            .remaining_secs
            .map(format_remaining)
            .unwrap_or_else(|| "-".to_string());
        let pid = s
            .pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<16} {:<8} {:<8} {:<12} {:<10} {:<8} {}",
            s.name, s.stable_port, backend, s.status, remaining, pid, s.command
        );
    }
}

fn format_remaining(secs: u64) -> String {
    let mins = secs / 60;
    let rem = secs % 60;
    format!("{mins}m{rem:02}s")
}

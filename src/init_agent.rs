use std::path::PathBuf;

const BLOCK_START: &str = "<!-- iter:agent-instructions:start -->";
const BLOCK_END: &str = "<!-- iter:agent-instructions:end -->";

const BLOCK_BODY: &str = "\
## iter — local dev server supervisor

If `iter` is installed, use it (instead of running dev servers directly)
whenever starting a local dev server on the user's behalf: `iter start
<name> --port <p> -- <command...>`. Run `iter list` first to check for an
already-running server before starting a duplicate, and `iter restart
<name>` to bring back a stopped/idle-killed one instead of `iter start`.
Full usage reference: run `iter agents` or `iter --help`.
";

fn wrapped_block() -> String {
    format!("{BLOCK_START}\n{BLOCK_BODY}{BLOCK_END}\n")
}

/// Insert or replace the iter block in `content`, appending it if absent.
fn upsert_block(content: &str) -> String {
    let out = if let (Some(start), Some(end)) = (content.find(BLOCK_START), content.find(BLOCK_END))
    {
        let end = end + BLOCK_END.len();
        let mut out = String::with_capacity(content.len());
        out.push_str(&content[..start]);
        out.push_str(wrapped_block().trim_end());
        out.push('\n');
        out.push_str(&content[end..]);
        out
    } else {
        let mut out = content.to_string();
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&wrapped_block());
        out
    };
    out.trim_end().to_string() + "\n"
}

fn write_block(path: &PathBuf) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("failed to create {parent:?}: {e}"))?;
    }
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let updated = upsert_block(&existing);
    std::fs::write(path, updated).map_err(|e| format!("failed to write {path:?}: {e}"))?;
    Ok(())
}

fn global_targets() -> Result<Vec<PathBuf>, String> {
    let home = crate::paths::home_dir()?;
    Ok(vec![
        home.join(".claude").join("CLAUDE.md"),
        home.join(".codex").join("AGENTS.md"),
    ])
}

fn project_targets() -> Result<Vec<PathBuf>, String> {
    let cwd =
        std::env::current_dir().map_err(|e| format!("failed to read current directory: {e}"))?;
    Ok(vec![cwd.join("AGENTS.md")])
}

pub fn run(project: bool) -> Result<(), String> {
    let targets = if project {
        project_targets()?
    } else {
        global_targets()?
    };

    for path in &targets {
        write_block(path)?;
        println!("updated {}", path.display());
    }

    if project {
        println!("\nAdded iter instructions to this project's AGENTS.md.");
    } else {
        println!("\nAdded iter instructions to your global agent config files.");
        println!("They'll apply automatically in every project from now on.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_to_empty_file() {
        let out = upsert_block("");
        assert!(out.contains(BLOCK_START));
        assert!(out.contains(BLOCK_END));
    }

    #[test]
    fn appends_to_existing_content_without_double_blank_lines() {
        let out = upsert_block("# My project\n\nSome notes.\n");
        assert!(out.starts_with("# My project\n\nSome notes.\n"));
        assert!(out.contains(BLOCK_START));
    }

    #[test]
    fn replaces_existing_block_in_place() {
        let original = format!(
            "before\n\n{}\nold content\n{}\nafter\n",
            BLOCK_START, BLOCK_END
        );
        let out = upsert_block(&original);
        assert!(out.contains("before"));
        assert!(out.contains("after"));
        assert!(!out.contains("old content"));
        assert_eq!(out.matches(BLOCK_START).count(), 1);
    }

    #[test]
    fn is_idempotent() {
        let once = upsert_block("");
        let twice = upsert_block(&once);
        assert_eq!(once, twice);
    }
}

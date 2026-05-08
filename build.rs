use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    let cargo_version = env::var("CARGO_PKG_VERSION").unwrap();
    rerun_on_git_state();
    let version = compute_version(&cargo_version).unwrap_or_else(|| cargo_version.clone());
    println!("cargo:rustc-env=JMA_VERSION={version}");
}

fn rerun_on_git_state() {
    if !Path::new(".git/HEAD").exists() {
        return;
    }
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Ok(head) = fs::read_to_string(".git/HEAD")
        && let Some(ref_path) = head.strip_prefix("ref: ").map(str::trim)
    {
        println!("cargo:rerun-if-changed=.git/{ref_path}");
    }
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

fn compute_version(cargo_version: &str) -> Option<String> {
    let tagged = Command::new("git")
        .args(["describe", "--tags", "--exact-match", "HEAD"])
        .output()
        .ok()?;
    if tagged.status.success() {
        return Some(cargo_version.to_string());
    }

    let sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !sha.status.success() {
        return None;
    }
    let sha = String::from_utf8(sha.stdout).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        return None;
    }
    Some(format!("{cargo_version} ({sha})"))
}

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    let cargo_version = env::var("CARGO_PKG_VERSION").unwrap();
    rerun_on_git_state();
    let version = compute_version().unwrap_or_else(|| cargo_version.clone());
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

fn compute_version() -> Option<String> {
    let tagged = Command::new("git")
        .args(["describe", "--tags", "HEAD"])
        .output()
        .ok()?;

    if !tagged.status.success() {
        return None;
    }
    let version_string = String::from_utf8(tagged.stdout).ok()?;
    let version_string = version_string.trim();

    Some(version_string.to_string())
}

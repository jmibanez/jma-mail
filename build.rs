use std::env;
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
    // Cargo treats a watched path that does not exist as permanently
    // stale and rebuilds the crate on every build, so only watch paths
    // that are guaranteed (HEAD, refs/) or checked (packed-refs) to
    // exist. Individual loose refs come and go as pack-refs prunes
    // them; the refs/ directory watch stands in for all of them.
    // Commits, tag changes, and ref packing all touch something under
    // it, and `git describe --tags` reads HEAD plus the tag set.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
    if Path::new(".git/packed-refs").exists() {
        println!("cargo:rerun-if-changed=.git/packed-refs");
    }
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

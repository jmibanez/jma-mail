//! Long-running Stalwart fixture for bench tools. Spawns the
//! container via the shared E2E fixture machinery in
//! `tests/common/mod.rs`, optionally IMAP-APPENDs a maildir corpus
//! into INBOX (and any sibling folders the corpus contains), writes
//! a shell-sourceable connection-info file, then blocks on SIGTERM
//! or Ctrl-C. Container teardown happens automatically when this
//! process exits because `JmapFixture` holds the testcontainers
//! handle.
//!
//! Companion to `examples/gen-bench-maildir` (which produces the
//! corpus) and the `tools/bench-*.sh` scripts (which spawn this
//! example in their testcontainer-mode dispatch path). Two-stage
//! lifecycle so the scripts don't need to embed testcontainers or
//! async-imap themselves: start the example, poll for the info
//! file, run the bench cells against the advertised URL/bearer,
//! SIGTERM the example.
//!
//! The info file is written *after* seeding completes, so its
//! appearance is the signal that the server is fully ready -- not
//! just listening. Seeding a corpus of a few hundred messages over
//! plaintext-loopback IMAP takes a couple of seconds; thousands
//! takes proportionally longer. Bench scripts should poll with a
//! generous timeout.

#[path = "../tests/common/mod.rs"]
mod common;

use anyhow::{Context, Result};
use async_imap::Client as ImapClient;
use clap::Parser;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;

use common::{JmapFixture, spawn_stalwart};

#[derive(Parser, Debug)]
#[command(
    name = "bench-server",
    about = "Long-running Stalwart fixture for bench tools: spawn + seed + block on SIGTERM."
)]
struct Args {
    /// Maildir-shaped corpus to seed the fixture from. Walks
    /// <root>/<folder>/{cur,new}/, IMAP-APPENDs each file into the
    /// corresponding folder (creating folders as needed). Omit to
    /// spawn an empty server.
    #[arg(long)]
    seed_from: Option<PathBuf>,

    /// Write a shell-sourceable connection-info file at this path
    /// once the fixture (and seeding, if requested) is fully ready.
    /// The file exports JMA_BENCH_SESSION_URL, JMA_BENCH_BEARER,
    /// and JMA_BENCH_ACCOUNT_EMAIL.
    #[arg(long)]
    info_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(seed_root) = &args.seed_from
        && !seed_root.is_dir()
    {
        anyhow::bail!("--seed-from is not a directory: {}", seed_root.display());
    }

    eprintln!("[bench-server] spawning Stalwart...");
    let fx = spawn_stalwart()
        .await
        .context("spawning Stalwart container")?;
    eprintln!("[bench-server] Stalwart ready at {}", fx.session_url);

    if let Some(seed_root) = &args.seed_from {
        let messages = collect_maildir_messages(seed_root)
            .with_context(|| format!("walking corpus at {}", seed_root.display()))?;
        let count = messages.len();
        eprintln!(
            "[bench-server] seeding {count} messages via IMAP from {}",
            seed_root.display()
        );
        seed_imap(&fx, messages)
            .await
            .context("IMAP-seeding fixture from corpus")?;
        eprintln!("[bench-server] seeding complete");
    }

    write_info(&fx, &args.info_path)
        .with_context(|| format!("writing info file {}", args.info_path.display()))?;
    eprintln!(
        "[bench-server] connection info written to {}",
        args.info_path.display()
    );
    eprintln!("[bench-server] ready; SIGTERM or Ctrl-C to shut down");

    // Block on either SIGTERM (the bench scripts' graceful-stop
    // signal) or SIGINT (Ctrl-C during ad-hoc invocation). The
    // fixture handle drops at function exit, which triggers
    // testcontainers' teardown.
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    tokio::select! {
        _ = sigterm.recv() => eprintln!("[bench-server] SIGTERM"),
        _ = sigint.recv() => eprintln!("[bench-server] SIGINT"),
    }
    eprintln!("[bench-server] shutting down");
    Ok(())
}

#[derive(Debug)]
struct SeedEntry {
    folder: String,
    eml: Vec<u8>,
    seen: bool,
}

/// Walk a maildir tree and collect every file under
/// `<root>/<folder>/cur/` (seen) or `<root>/<folder>/new/` (unseen)
/// as raw .eml bytes paired with the folder name. Folder names
/// come from the top-level directory entries verbatim, so the
/// caller's choice of layout (single INBOX, multi-folder corpus)
/// drives the IMAP destination set with no further mapping.
///
/// Folder names and per-folder file paths are sorted before
/// iteration so the resulting IMAP UID assignment is deterministic
/// across hosts / filesystems for a given corpus; `fs::read_dir`
/// itself returns entries in filesystem-dependent order which
/// would otherwise break cross-machine reproducibility.
fn collect_maildir_messages(root: &Path) -> Result<Vec<SeedEntry>> {
    let mut folder_paths: Vec<PathBuf> = fs::read_dir(root)
        .with_context(|| format!("reading {}", root.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.path())
        .collect();
    folder_paths.sort();

    let mut messages = Vec::new();
    for folder_path in folder_paths {
        let folder_name = folder_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        for (sub, seen) in [("cur", true), ("new", false)] {
            let sub_path = folder_path.join(sub);
            if !sub_path.is_dir() {
                continue;
            }
            let mut msg_paths: Vec<PathBuf> = fs::read_dir(&sub_path)
                .with_context(|| format!("reading {}", sub_path.display()))?
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| e.path())
                .collect();
            msg_paths.sort();
            for msg_path in msg_paths {
                let bytes = fs::read(&msg_path)
                    .with_context(|| format!("reading {}", msg_path.display()))?;
                messages.push(SeedEntry {
                    folder: folder_name.clone(),
                    eml: bytes,
                    seen,
                });
            }
        }
    }
    Ok(messages)
}

/// Default number of concurrent IMAP sessions used for the APPEND
/// phase. Empirically 8 is the sweet spot against the RocksDB
/// fixture this binary is built to seed: RocksDB's writer
/// concurrency scales near-linearly with CPU count, so client
/// parallelism past 1 buys real throughput. 8 workers on an
/// 8-CPU host VM completes a 100k-message seed in ~422s; 16
/// workers on the same VM buys only ~7% more (~392s); 16 workers
/// on a 16-CPU VM trims further to ~319s. The recommended Colima
/// allocation is 8 CPU / 8 GB, which matches this default. On
/// smaller hosts (1-4 CPUs) override with a lower
/// TESTCONTAINER_SEED_PARALLELISM to avoid CPU over-subscription.
/// `tools/_testcontainer.sh` sets the same default and exports it
/// so bench-script invocations inherit it; this constant is the
/// standalone fallback when bench-server is invoked directly.
const DEFAULT_SEED_PARALLELISM: usize = 8;

/// How many progress lines each worker emits across its chunk.
/// Ten gives an obvious "still alive" cadence in server.log without
/// drowning it for big corpora; tens of thousands of APPENDs land
/// under a hundred status lines per worker.
const PROGRESS_STEPS_PER_WORKER: usize = 10;

/// CREATE every distinct folder in a single up-front session, then
/// APPEND every message across N concurrent sessions in parallel.
/// The CREATE pass runs serially in its own session to avoid the
/// race where two workers both try to CREATE the same folder; the
/// existing "already exists" tolerance handles INBOX and any
/// folders that survived a prior run against a persistent fixture.
/// `\Seen` is applied to messages originally in `cur/`; `new/`
/// messages are APPENDed without flags so they land in jma's
/// "delivered, awaiting first read" state.
async fn seed_imap(fx: &JmapFixture, messages: Vec<SeedEntry>) -> Result<()> {
    let total = messages.len();
    if total == 0 {
        return Ok(());
    }

    let folders: BTreeSet<String> = messages.iter().map(|m| m.folder.clone()).collect();
    create_folders(fx, &folders).await?;

    let parallelism = std::env::var("TESTCONTAINER_SEED_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_SEED_PARALLELISM)
        .min(total)
        .max(1);

    let chunks = split_chunks(messages, parallelism);
    eprintln!(
        "[bench-server] APPEND {total} messages across {} concurrent session(s)",
        chunks.len()
    );

    // JoinSet gives fail-fast semantics: the moment one worker
    // returns Err we abort the rest, so a misconfigured auth or a
    // server-side crash surfaces immediately instead of waiting
    // for the other 7 workers to finish their chunks against a
    // doomed fixture. abort_all is best-effort -- workers between
    // network reads observe the abort promptly; ones mid-APPEND
    // wait for the syscall to return -- but we don't await those,
    // so the function still returns on the first error.
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for (idx, chunk) in chunks.into_iter().enumerate() {
        let host = fx.imap_host.clone();
        let port = fx.imap_port;
        let user = fx.account_email.clone();
        let pass = fx.account_password.clone();
        set.spawn(async move {
            append_chunk(
                host,
                port,
                user,
                pass,
                idx,
                chunk,
                PROGRESS_STEPS_PER_WORKER,
            )
            .await
        });
    }

    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => {
                set.abort_all();
                return Err(e);
            }
            Err(e) => {
                set.abort_all();
                return Err(anyhow::Error::new(e).context("worker join"));
            }
        }
    }
    Ok(())
}

async fn create_folders(fx: &JmapFixture, folders: &BTreeSet<String>) -> Result<()> {
    let stream = TcpStream::connect((fx.imap_host.as_str(), fx.imap_port))
        .await
        .context("connect to fixture IMAP for CREATE pass")?;
    let client = ImapClient::new(stream);
    let mut session = client
        .login(&fx.account_email, &fx.account_password)
        .await
        .map_err(|(e, _)| anyhow::anyhow!("CREATE-pass IMAP login: {e}"))?;
    for folder in folders {
        if let Err(e) = session.create(folder).await {
            // "already exists" is the common case (INBOX is
            // pre-created by Stalwart and any folder we previously
            // CREATE'd on this fixture instance). Other errors --
            // auth scope, listener config, server crashes -- get
            // surfaced here so they aren't masked when APPEND
            // fails downstream.
            eprintln!("[bench-server] CREATE {folder} -> {e} (continuing)");
        }
    }
    let _ = session.logout().await;
    Ok(())
}

async fn append_chunk(
    host: String,
    port: u16,
    user: String,
    pass: String,
    worker_idx: usize,
    chunk: Vec<SeedEntry>,
    progress_steps: usize,
) -> Result<()> {
    let stream = TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("worker {worker_idx}: connect to IMAP"))?;
    let client = ImapClient::new(stream);
    let mut session = client
        .login(&user, &pass)
        .await
        .map_err(|(e, _)| anyhow::anyhow!("worker {worker_idx}: IMAP login: {e}"))?;

    let chunk_total = chunk.len();
    let step = chunk_total.div_ceil(progress_steps).max(1);
    // Track per-batch and cumulative throughput so the seed log
    // shows whether the rate stays flat or degrades as the corpus
    // grows. `cum` is the average from the start of this worker;
    // `batch` is the rate over just the most recent step. If the
    // two diverge (batch much smaller than cum) something on the
    // server side is doing work that scales with corpus size.
    let start = std::time::Instant::now();
    let mut last_progress_at = start;
    let mut last_progress_count = 0usize;

    for (i, entry) in chunk.iter().enumerate() {
        let flags = if entry.seen { Some("(\\Seen)") } else { None };
        session
            .append(&entry.folder, flags, None, entry.eml.as_slice())
            .await
            .with_context(|| format!("worker {worker_idx}: APPEND msg {i} to {}", entry.folder))?;
        let one_based = i + 1;
        if one_based % step == 0 && one_based < chunk_total {
            let now = std::time::Instant::now();
            let interval_secs = now.duration_since(last_progress_at).as_secs_f64();
            let interval_msgs = one_based - last_progress_count;
            let batch_rate = if interval_secs > 0.0 {
                interval_msgs as f64 / interval_secs
            } else {
                f64::INFINITY
            };
            let elapsed = now.duration_since(start).as_secs_f64();
            let cum_rate = if elapsed > 0.0 {
                one_based as f64 / elapsed
            } else {
                f64::INFINITY
            };
            eprintln!(
                "[bench-server] worker {worker_idx}: {one_based}/{chunk_total} \
                 (t={elapsed:.1}s, batch={batch_rate:.0}/s, cum={cum_rate:.0}/s)"
            );
            last_progress_at = now;
            last_progress_count = one_based;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let final_rate = if elapsed > 0.0 {
        chunk_total as f64 / elapsed
    } else {
        f64::INFINITY
    };
    let _ = session.logout().await;
    eprintln!(
        "[bench-server] worker {worker_idx}: done ({chunk_total} APPENDs in {elapsed:.1}s, {final_rate:.0}/s)"
    );
    Ok(())
}

/// Split a Vec into roughly equal-sized chunks by move (no
/// per-chunk clone of the underlying SeedEntry data). The last
/// chunk may be smaller when the total doesn't divide evenly.
fn split_chunks<T>(mut v: Vec<T>, n_chunks: usize) -> Vec<Vec<T>> {
    let n = n_chunks.max(1);
    let chunk_size = v.len().div_ceil(n).max(1);
    let mut out = Vec::with_capacity(n);
    while !v.is_empty() {
        // .min(v.len()) is load-bearing on the last iteration:
        // when the total doesn't divide evenly, the tail chunk has
        // fewer than chunk_size elements and split_off would panic
        // without the clamp.
        let split_at = chunk_size.min(v.len());
        let rest = v.split_off(split_at);
        out.push(v);
        v = rest;
    }
    out
}

/// Write a shell-sourceable env-style file. Bench scripts `source`
/// this to get the three values they substitute into the generated
/// `config.toml` (`[account].email` / `[account].token` /
/// `[account].session_url`). Plain double-quoting is safe because
/// the wizard-generated bearer is pure ASCII alphanumeric and the
/// session URL has no shell-special characters. The debug_asserts
/// pin that contract on the fixture side -- if a future fixture
/// change introduces a `"`, `\`, or newline, the failure surfaces
/// here (where the fix is obvious) rather than at sourced-bash
/// parse time (where it isn't).
fn write_info(fx: &JmapFixture, path: &Path) -> Result<()> {
    let unsafe_chars = ['"', '\\', '\n'];
    debug_assert!(
        !fx.session_url.contains(unsafe_chars),
        "fixture session_url contains a shell-unsafe character"
    );
    debug_assert!(
        !fx.bearer.contains(unsafe_chars),
        "fixture bearer contains a shell-unsafe character"
    );
    debug_assert!(
        !fx.account_email.contains(unsafe_chars),
        "fixture account_email contains a shell-unsafe character"
    );
    let body = format!(
        "JMA_BENCH_SESSION_URL=\"{}\"\n\
         JMA_BENCH_BEARER=\"{}\"\n\
         JMA_BENCH_ACCOUNT_EMAIL=\"{}\"\n",
        fx.session_url, fx.bearer, fx.account_email
    );
    fs::write(path, body)?;
    Ok(())
}

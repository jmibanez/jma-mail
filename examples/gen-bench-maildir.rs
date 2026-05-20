//! Generate a maildir tree of synthetic .eml messages for benchmark
//! input. Output is a maildir root with one or more folders, each
//! holding `cur/`, `new/`, and `tmp/` subdirectories. Messages are
//! distributed round-robin across folders, mostly into `cur/` (with
//! `:2,S` flag info marking them seen) and a minority into `new/`
//! (no info section), matching the on-disk shape jma's own delivery
//! path produces.
//!
//! Each message gets a unique Message-ID and a maildir filename of
//! the form `<unixtime>.M<counter>.jma-bench[:2,S]`. The base
//! timestamp and the per-message increment are fixed, so structure
//! (filenames, folder assignment, cur/new placement, Date headers)
//! is deterministic regardless of seed. The RNG drives only content:
//! From-name and address come from the `fake` name/email dictionaries,
//! Subject is a 3-7 word lorem sentence with trailing period stripped,
//! and body is sampled from a 59/30/8/1/2
//! short/medium/large/extra-large/jumbo size distribution targeting
//! real-world local-maildir shapes (typical median ~7-40 KB, p90
//! ~40-120 KB, p99 ~0.5-1.1 MB, with a fat tail driven by attachment
//! and inline-image traffic). Medium covers 2-20 KB, large 20-200 KB,
//! extra-large 10-64 KB (overlaps the lower half of large for
//! parser-buffer density), and the 2% jumbo tier produces ~1 MB
//! bodies so the corpus carries an attachment-class tail that
//! testcontainer-style synthetic blobs (typically <1 KB) lack.
//! Sizing jumbo at 2% rather than 1% pins the corpus's strict P99
//! statistic inside the jumbo tier. Same `--seed` reproduces the
//! same corpus byte-for-byte.
//!
//! Intended consumers: `tools/bench-rss.sh`, `tools/bench-power.sh`,
//! `tools/bench-power-watch.sh`. Run once to produce a stable
//! corpus, then pass the output path as the `<maildir-source>`
//! positional argument. The bench scripts copy the tree into
//! `$BENCH_DIR/maildir` once and reuse it across runs.
//!
//! This binary does not talk to a JMAP server. If a bench requires
//! the same content to also exist server-side (testcontainer flow),
//! that's a separate tool's job.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use fake::Fake;
use fake::faker::internet::en::FreeEmail;
use fake::faker::lorem::en::{Paragraph, Sentence};
use fake::faker::name::en::Name;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs;
use std::path::PathBuf;

/// Stable base for the unixtime portion of maildir filenames.
/// 2026-01-01 00:00:00 UTC -- recent enough that an MUA inspecting
/// the corpus doesn't see "ancient" mail, far enough back that
/// per-message increments stay well below the current epoch.
const BASE_UNIXTIME: u64 = 1_767_225_600;

/// Folder names cycled through when `--folders` > 1. Mirrors the
/// standard role set most JMAP servers (Stalwart, Fastmail) advertise
/// out of the box; the first slot is the inbox which most benchmarks
/// care about.
const FOLDER_POOL: &[&str] = &["INBOX", "Archive", "Drafts", "Sent", "Junk", "Trash"];

#[derive(Parser, Debug)]
#[command(
    name = "gen-bench-maildir",
    about = "Generate a maildir tree of synthetic emails for benchmark input."
)]
struct Args {
    /// Number of messages to generate across all folders.
    #[arg(long, default_value_t = 1000)]
    count: usize,

    /// Maildir root to write to. Created if missing; refused if
    /// non-empty unless --allow-non-empty is set.
    #[arg(long)]
    out: PathBuf,

    /// Number of folders to spread messages across (cycled through
    /// FOLDER_POOL). The first folder is always INBOX.
    #[arg(long, default_value_t = 1)]
    folders: usize,

    /// RNG seed for content variation. Structure (filenames, folder
    /// assignment) is deterministic regardless; only header/body
    /// rotation varies with this.
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Fraction of messages placed in `new/` rather than `cur/`,
    /// expressed as a percentage. Defaults to 10 -- mostly-seen
    /// matches a long-lived account's typical shape.
    #[arg(long, default_value_t = 10)]
    new_pct: u32,

    /// Allow writing into a non-empty `--out` directory. Files at
    /// paths the generator writes get overwritten; stale files from
    /// a prior larger run are left in place (this flag does not
    /// clear the directory). For a fully fresh corpus, delete
    /// `--out` first.
    #[arg(long)]
    allow_non_empty: bool,
}

/// Generate lorem paragraphs into a single body string until the
/// total byte count reaches a target drawn uniformly from
/// `[min_target, max_target)`. Paragraphs are joined with the
/// `\r\n\r\n` separator the rest of the generator uses; the +4
/// accounts for that separator when tracking running total. The
/// loop overshoots the chosen target by at most one paragraph's
/// length (a few hundred bytes), so the resulting body sits very
/// close to the target window rather than wherever paragraph-count
/// sampling variance happens to land.
fn body_until<R: Rng>(rng: &mut R, min_target: usize, max_target: usize) -> String {
    let target = rng.random_range(min_target..max_target);
    let mut chunks: Vec<String> = Vec::new();
    let mut total: usize = 0;
    while total < target {
        let p: String = Paragraph(4..7).fake_with_rng(rng);
        total += p.len() + 4;
        chunks.push(p);
    }
    chunks.join("\r\n\r\n")
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.folders == 0 || args.folders > FOLDER_POOL.len() {
        anyhow::bail!(
            "--folders must be between 1 and {} (got {})",
            FOLDER_POOL.len(),
            args.folders
        );
    }
    if args.new_pct > 100 {
        anyhow::bail!("--new-pct must be 0..=100 (got {})", args.new_pct);
    }

    if args.out.exists() {
        let empty = fs::read_dir(&args.out)
            .with_context(|| format!("reading {}", args.out.display()))?
            .next()
            .is_none();
        if !empty && !args.allow_non_empty {
            anyhow::bail!(
                "{} is non-empty; pass --allow-non-empty to write into it",
                args.out.display()
            );
        }
    }
    fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;

    let folders: Vec<&str> = FOLDER_POOL.iter().take(args.folders).copied().collect();
    for folder in &folders {
        for sub in ["cur", "new", "tmp"] {
            let p = args.out.join(folder).join(sub);
            fs::create_dir_all(&p).with_context(|| format!("creating {}", p.display()))?;
        }
    }

    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut cur_count = 0usize;
    let mut new_count = 0usize;
    let mut per_folder: Vec<usize> = vec![0; folders.len()];

    for i in 0..args.count {
        let folder_idx = i % folders.len();
        let folder = folders[folder_idx];
        per_folder[folder_idx] += 1;

        // Place every Nth message in new/ where N = floor(100/new_pct).
        // Yields approximately new_pct% of messages in new/, exact
        // only when new_pct evenly divides 100. Determined by the
        // counter rather than the RNG so placement is reproducible
        // across runs regardless of --seed.
        let in_new = match 100u32.checked_div(args.new_pct) {
            Some(stride) => i % (stride.max(1) as usize) == 0,
            None => false,
        };

        let unixtime = BASE_UNIXTIME + (i as u64);
        let filename = if in_new {
            format!("{unixtime}.M{i}.jma-bench")
        } else {
            format!("{unixtime}.M{i}.jma-bench:2,S")
        };

        // Draw order (Name -> FreeEmail -> Sentence -> tier -> body)
        // is part of the seed contract: inserting another faker
        // earlier in this block shifts every byte downstream for the
        // same --seed. Cheap to honor, expensive to debug if violated.
        let from_name: String = Name().fake_with_rng(&mut rng);
        let from_addr: String = FreeEmail().fake_with_rng(&mut rng);
        // Subjects: 3-7 lorem words, capitalized and trimmed. Sentence
        // returns "Word word word." with a trailing period; strip it so
        // the subject reads like a real subject line.
        let subject_raw: String = Sentence(3..8).fake_with_rng(&mut rng);
        let subject = subject_raw.trim_end_matches('.').to_string();

        // Body size distribution. Tier byte ranges track measured
        // real-world local-maildir shapes (typical median ~7-40 KB,
        // p90 ~40-120 KB, p99 ~0.5-1.1 MB; the heavy tail comes
        // from attachments and inline-image traffic). The
        // 59/30/8/1/2 split keeps a single-sentence majority that
        // matches the "many short replies and notifications"
        // character of a real account (and leaves the corpus P50
        // in the short tier), while the per-tier byte windows pull
        // p75 onward (12 KB / 35 KB at p75 / p90 in the generated
        // corpus) into ranges that exercise the chunked-write,
        // parser-buffer, and attachment paths realistically.
        // Earlier paragraph-count-based sizing produced p90 < 1 KB,
        // which couldn't surface buffer-pressure deltas in the
        // streaming-download path.
        //
        // Jumbo is 2% (not 1%) so the corpus's strict P99 statistic
        // lands inside the jumbo tier rather than at the boundary
        // below it -- with only 1% jumbo, RNG variance on the
        // jumbo-tier hit count can pull a[P99] back into the large
        // tier (sub-200 KB), which doesn't match the real-world
        // shape.
        //
        // All non-short tiers use generate-until-byte-target
        // (`body_until`) because paragraph-count-based sizing
        // doesn't converge tightly: fake's Paragraph length has a
        // heavy tail driven by per-sentence word-count variance,
        // and a fixed N can produce anywhere from a small fraction
        // to a large multiple of the intended size. Short stays as
        // a single `Sentence` call because the variance band is
        // small relative to the target there.
        //
        // Boundaries are RNG-driven so --seed still locks the corpus.
        let tier = rng.random_range(0u8..100);
        let body = if tier < 59 {
            // ~59% short: 1 lorem sentence (~80-200 bytes)
            Sentence(8..20).fake_with_rng(&mut rng)
        } else if tier < 89 {
            // ~30% medium: 2-20 KB. Body-of-the-distribution range
            // for a typical reply chain with quoted history.
            body_until(&mut rng, 2_000, 20_000)
        } else if tier < 97 {
            // ~8% large: 20-200 KB. HTML-formatted threads, inline
            // images, mailing-list digests.
            body_until(&mut rng, 20_000, 200_000)
        } else if tier < 98 {
            // ~1% extra-large: 10-64 KB. Overlaps the lower half of
            // the large tier on purpose -- adds density in the
            // parser-buffer-stress range that a single-percent
            // sample of large would otherwise under-cover.
            body_until(&mut rng, 10_000, 64_000)
        } else {
            // ~2% jumbo: 1.0-1.3 MB. Pins the corpus's strict
            // P99 statistic inside this tier so jumbo-class
            // I/O actually shows up in P99-headlined metrics.
            body_until(&mut rng, 1_000_000, 1_300_000)
        };

        let msgid = format!("<bench-{i}@jma-bench>");

        // Spread Date headers across the corpus so sort-by-date and
        // thread-by-date paths in MUAs and benches see meaningful
        // ordering. Derived from the same per-message unixtime that
        // names the maildir file.
        let date: DateTime<Utc> = Utc
            .timestamp_opt(unixtime as i64, 0)
            .single()
            .expect("BASE_UNIXTIME + counter stays in the valid timestamp range");
        let date_header = date.format("%a, %d %b %Y %H:%M:%S %z").to_string();

        // `To:` is hardcoded to the Stalwart fixture's admin
        // account so corpora generated here drop cleanly into a
        // testcontainer seed without rewriting headers. Don't
        // parameterize this without also updating the fixture's
        // `ACCOUNT_EMAIL` in `tests/common/mod.rs`.
        let eml = format!(
            "From: \"{from_name}\" <{from_addr}>\r\n\
             To: admin@example.org\r\n\
             Subject: {subject}\r\n\
             Message-ID: {msgid}\r\n\
             Date: {date_header}\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=us-ascii\r\n\
             \r\n\
             {body}\r\n"
        );

        let subdir = if in_new { "new" } else { "cur" };
        let path = args.out.join(folder).join(subdir).join(&filename);
        fs::write(&path, eml.as_bytes()).with_context(|| format!("writing {}", path.display()))?;

        if in_new {
            new_count += 1;
        } else {
            cur_count += 1;
        }
    }

    println!("wrote {} messages to {}", args.count, args.out.display());
    println!("  cur/: {cur_count}");
    println!("  new/: {new_count}");
    for (i, folder) in folders.iter().enumerate() {
        println!("  {}: {}", folder, per_folder[i]);
    }
    Ok(())
}

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`jmapsync` is a Rust CLI that bidirectionally syncs email between a JMAP server (targeting Fastmail) and a local Maildir — like `mbsync`/`isync` but speaking JMAP. Single binary, async (tokio), state in SQLite.

## Build / Test

Apple clang is needed because `aws-lc-sys` (transitive via `jmap-client`'s rustls stack) doesn't build under gcc on macOS. **Always run cargo with `CC=/usr/bin/cc`** if CC is set to something other than clang on macOS:

```
CC=/usr/bin/cc cargo check
CC=/usr/bin/cc cargo test
CC=/usr/bin/cc cargo build --release
```

Single test: `CC=/usr/bin/cc cargo test --lib maildir_ops::headers::tests::parses_folded_value`.

If rust-analyzer / flymake reports diagnostics that contradict a clean `cargo check`, trust `cargo check` — the LSP cache goes stale across large multi-file refactors. Re-run `cargo check` to confirm.

## Running

Auth uses an **API token (Bearer)**, not Basic auth and not OAuth. Tokens come from the config file or `JMAPSYNC_TOKEN` env var. Config defaults to `~/.config/jmapsync/config.toml`; `jmapsync init` writes a template.

Subcommands: `sync` (default, bidirectional), `pull` (server→local), `push` (local→server), `watch` (daemon: SSE + filesystem notify), `init`, `status`, `mailboxes`. Top-level flags: `-c <path>`, `-n` (dry-run), `-v`/`-vv`/`-vvv`, `-q`.

## Architecture

The orchestration lives in `src/sync/` and is layered:

- **`engine.rs`** is the entry point for all three sync modes (`sync`, `pull_only`, `push_only`). It resolves which mailboxes to sync, runs the **dedupe pass** at the top of every cycle, fetches remote changes via `Email/changes`, scans local maildirs for changes, builds a reconciliation `SyncPlan`, then dispatches to `pull` and `push`.
- **`pull.rs`** has two paths: `initial_pull` (full `Email/query` + `Email/get` per mailbox) and `delta_pull` (loop on `Email/changes` until `has_more_changes` is false). Both go through an `ingest_email` helper that consults `message_map` first, then the in-memory `LocalIndex`, before downloading. State is persisted only after the pull completes.
- **`push.rs`** uploads local-only messages via `Email/import`, applies flag changes via `Email/set` keywords, and handles destroys.
- **`reconcile.rs`** turns remote changes + local changes + DB state into a `SyncPlan`, applying the configured `ConflictStrategy` (server-wins / local-wins / newest-wins).

### Idempotency model — read this before touching pull/push

Two distinct identifiers anchor the system:

- JMAP `email_id` — stable per server, lives in `message_map`.
- RFC 5322 `Message-ID` — stable across both sides, survives a state DB wipe.

The state DB is **disposable**. Nuking `state.db` and re-running must converge on the existing maildir without re-downloading or duplicating files. That property is enforced by:

1. `maildir_ops::dedupe::dedupe_and_index` runs at the start of every sync cycle, walks each synced folder, groups files by Message-ID (parsed from headers in `maildir_ops::headers`), and **deletes the newest by mtime** within each group — newer copies are presumed to be jmapsync-introduced duplicates from a prior aborted run. Returns a `LocalIndex` keyed on Message-ID.
2. `pull::ingest_email` checks `message_map` (by JMAP id) → `LocalIndex` (by Message-ID) → download. The Message-ID lookup is what saves us after a DB wipe.

Don't add a code path that writes to a maildir without going through `ingest_email` (or an equivalent that consults both indices).

### JMAP boundary quirks

- `Email/changes` since the literal `"0"` is **not** how you bootstrap state. Fastmail (and the spec) allows the server to refuse with `cannotCalculateChanges`. Use `jmap::email::get_current_state` (Email/get with empty ids) to read the current state after an initial pull.
- The `jmap-client` crate formats errors via `Display` — substring-match on `"Cannot calculate changes"` (the human-readable form), not the JSON name `cannotCalculateChanges`. The recovery path on this error is: write empty string to `jmap_state`, which `queries::get_jmap_state` filters back to `None`, which routes to `initial_pull` on next run.
- Maildir stores bare LF; JMAP `Email/import` rejects bare newlines. `jmap::email::import_email` calls `normalize_crlf` before handing bytes to `email_import`. Don't normalize at maildir read time — keep on-disk format native so other MUAs work.

### State DB (`src/state/`)

SQLite, schema in `db.rs::SCHEMA`. Tables: `jmap_state` (per-entity sync cursor), `message_map` (JMAP↔maildir mapping, indexed on `maildir_id`, `message_id`, `mailbox_id`), `mailbox_map`, `local_state` (filesystem snapshot for change detection). All access goes through `queries.rs` — don't `prepare` ad-hoc SQL elsewhere.

### Daemon mode (`src/daemon/`)

`runner.rs` runs an initial sync, then concurrently spawns `eventsource.rs` (SSE listener on the JMAP `eventSourceUrl` from the session) and `watcher.rs` (filesystem `notify` with debouncing). Both feed a `tokio::sync::mpsc` channel of `SyncTrigger`s; the main loop drains and re-runs `engine::sync` per trigger.

## Git / commit conventions

This repo uses **LKML-style commit messages**: subsystem-prefixed subject (`sync:`, `jmap:`, `state:`, `maildir:`, `daemon:`), blank line, body that explains the **problem before the fix**, wrapped at ~72 chars. One logical change per commit. See `git log` for examples.

**Always run `cargo fmt` before committing** so the diff stays formatting-noise-free.

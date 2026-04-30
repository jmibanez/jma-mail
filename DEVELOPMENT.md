# DEVELOPMENT.md

A working tour of `jmapsync`'s internals, for those who want to work on the codebase. Skim the first three sections to get oriented; the later sections drill into `src/sync/` (the only part of the codebase with non-trivial logic).

## What this is

`jmapsync` is a Rust CLI that bidirectionally syncs email between a JMAP server (targeting Fastmail) and a local Maildir -- like `mbsync`/`isync` but speaking JMAP. Single binary, async (tokio), state in SQLite.

Currently, `jmapsync` only supports **API token (Bearer)** auth. OAuth might be implemented in the future, but that would somehow entail saving an OAuth API private key; for now API tokens work. Tokens come from the config file or `JMAPSYNC_TOKEN`.

See [README.md](README.md) for more info on subcommands and flags.

## Build / test

`aws-lc-sys` (transitive via `jmap-client`'s rustls stack) doesn't build under gcc on macOS, so cargo needs Apple clang. If your environment sets `CC` to something else, override it:

```
CC=/usr/bin/cc cargo check
CC=/usr/bin/cc cargo test
CC=/usr/bin/cc cargo build --release
```

Single test, by name path:

```
CC=/usr/bin/cc cargo test --lib maildir_ops::headers::tests::parses_folded_value
```

## Source layout

- `src/main.rs` -- CLI entry point and subcommand dispatch.
- `src/config.rs` -- TOML config schema and template.
- `src/jmap/` -- JMAP client wrapper. `session.rs` opens the   connection, `email.rs` is the per-method veneer over `jmap-client`, `mailbox.rs` handles `Mailbox/get`, `retry.rs` classifies errors as transient vs hard, `types.rs` holds the internal `EmailObject` shape.
- `src/maildir_ops/` -- anything that touches the filesystem. `store.rs` wraps `maildirpp` for read/write, `flags.rs` translates between maildir suffix flags and JMAP keywords, `scan.rs` walks a folder and emits `LocalChange`s, `dedupe.rs` does the Message-ID-based duplicate sweep, `headers.rs` parses `Message-ID` out of a maildir file.
- `src/state/` -- SQLite. Schema in `db.rs::SCHEMA`. Tables:
  - `jmap_state` -- per-entity sync cursor (one row per `(account, entity)` like `("acct", "Email")`).
  - `message_map` -- JMAP<->maildir binding, indexed on `maildir_id`, `message_id`, `mailbox_id`.
  - `mailbox_map` -- server mailbox metadata.
  - `local_state` -- filesystem snapshot for change detection. All access goes through `queries.rs`. Don't `prepare` ad-hoc SQL elsewhere; if you need a new query, add it there.
- `src/sync/` -- orchestration. See [Sync internals](#sync-internals) below.
- `src/daemon/` -- `watch` mode. `runner.rs` runs an initial sync then concurrently spawns `eventsource.rs` (SSE listener on the JMAP `eventSourceUrl` from the session) and `watcher.rs` (filesystem `notify` with debouncing). Both feed a `tokio::sync::mpsc` channel of `SyncTrigger`s; the main loop drains and re-runs `engine::sync` per trigger. `hook.rs` runs `post_arrival_command` after sync cycles that downloaded mail, coalescing overlapping triggers.

## Identifiers

The system juggles several opaque IDs from different namespaces. Knowing which is which makes everything in `src/sync/` and `src/state/` easier to read.

**Server-side (JMAP).** Opaque strings the server hands out; we never parse or generate them.

- `jmap_email_id` -- per-email-object id. Primary key of `message_map`. Returned by `Email/get` and `Email/changes`. Always 1:1 with a server email, so any index keyed on it holds a single record.
- `jmap_blob_id` -- the body bytes of an email. Distinct from `jmap_email_id` because the same blob could in principle be referenced by multiple email objects (drafts/sent). Cached on `MessageRecord` so we don't need an extra round-trip to re-download.
- `jmap_thread_id` -- the conversation thread an email belongs to. Cached on `MessageRecord` for the same reason.
- `jmap_mailbox_id` -- per-folder id. Primary key of `mailbox_map`. Each `EmailObject.mailbox_ids` is a `HashMap<jmap_mailbox_id, bool>` describing membership.

**Local (filesystem).**

- `maildir_id` -- the unique prefix of a maildir filename, e.g. `1734567890.M123P456.host` from `cur/1734567890.M123P456.host:2,S`. Identifies **one file on disk**. Each file is born from exactly one ingest action (download or adopt), which writes exactly one `message_map` row with that file's id, so the maildir_id -> MessageRecord relationship is 1:1 by construction. The schema indexes the column but doesn't constrain it as unique because the column is also nullable for never-bound rows; the 1:1 invariant is enforced by code, not by SQLite. Primary key of `local_state`.
- `maildir_folder` -- the folder name on disk (e.g. `"INBOX"`, `"Archive"`). The `INBOX` magic alias is applied here, so this string need not match the JMAP display name (`"Inbox"`, `"Indbakke"`). Joined with `jmap_mailbox_id` in the `mailboxes: Vec<(String, String)>` that every sync phase indexes against.

**Cross-cutting.**

- `Message-ID` -- the RFC 5322 header. The **only** identifier shared by both sides. Parsed out of the maildir file by `maildir_ops::headers`; reported by JMAP as `EmailObject.message_id` (a `Vec<String>`, since RFC 5322 technically allows multiple values). NOT unique in `message_map`: the same Message-ID can legitimately appear in multiple rows (same email delivered to two folders, or sent messages stored in both Sent and the conversation thread folder). This is the identifier that survives a state DB wipe and lets us rebind known files without re-downloading.

## Data model

The sync pipeline moves a small set of internal types between phases. Each one lives in a specific module, has a specific role, and gets translated into another type at module boundaries. Knowing the cast helps when reading any handler.

**JMAP-side types** (`src/jmap/types.rs`). Hand-rolled rather than re-exported from `jmap-client` so the rest of the codebase doesn't depend on the upstream crate's shape.

- `EmailObject` -- `{ id, blob_id, thread_id, mailbox_ids: HashMap<jmap_mailbox_id, bool>, keywords: HashMap<String, bool>, message_id: Option<Vec<String>>, subject }`. Produced by `Email/get` and `Email/changes`-then-`Email/get`; consumed by `reconcile`. The `mailbox_ids` map's bool is always `true` (JMAP's convention for set membership); we keep the type as-is to match the wire format.
- `MailboxObject` -- mailbox metadata from `Mailbox/get`. Consumed by `resolve_mailboxes` and immediately translated into a `MailboxRecord` for the DB.
- `ChangesResponse` -- `{ old_state, new_state, created, updated, destroyed, has_more_changes }`. The shape `Email/changes` returns; loop control for `fetch_remote_state`.
- `SessionInfo` -- session URLs and account id, captured at connect time.

**State-side types** (`src/state/queries.rs`). One struct per row-shape; every read or write goes through these.

- `MessageRecord` -- one `message_map` row. Carries everything needed to act on an email without re-querying: both server identifiers (`jmap_email_id`, `jmap_blob_id`, `jmap_thread_id`, `mailbox_id`), both local identifiers (`maildir_id`, `maildir_folder`), the cross-cutting `message_id`, and a flags pair (`flags` for the maildir suffix view, `jmap_keywords` for the JSON-serialised JMAP view). Many fields are `Option` because a row can exist in a partially-bound state during in-flight phases.
- `MailboxRecord` -- one `mailbox_map` row. Mostly a snapshot of `MailboxObject` plus the resolved `maildir_folder` (with the `INBOX` magic alias applied).

**Maildir-side types** (`src/maildir_ops/`). What `scan` and `dedupe` produce for `reconcile` to chew on.

- `LocalChange` (enum) -- one of:
  - `NewMessage { maildir_id, folder, flags, path, message_id }` -- a file appeared that wasn't in `local_state`. The `message_id` is `Option` because the header may be missing or unreadable; reconcile treats `None` as "not safe to dedupe against the server" and falls through to a plain upload.
  - `FlagsChanged { maildir_id, folder, old_flags, new_flags }` -- the maildir filename suffix changed.
  - `DeletedMessage { maildir_id, folder }` -- a `local_state` row has no corresponding file on disk.
- `LocalEntry` -- `{ folder, maildir_id, path }`. One on-disk file's location.
- `LocalIndex` -- `{ by_message_id: HashMap<message_id, Vec<LocalEntry>> }`. Built once per cycle by `dedupe_and_index` after dedupe deletes within-folder duplicates. The `Vec` here, like `known_by_message_id`, exists because the same Message-ID can legitimately live in several folders.

**Plan types** (`src/sync/plan.rs`). The bridge between `reconcile` and `execute`. Detailed in [`plan.rs`](#plan-rs--the-action-vocabulary) and [`reconcile.rs`](#reconcile-rs--building-the-plan); summarised here for the cast list:

- `SyncAction` (enum) -- the action vocabulary. Each variant carries every field its handler needs, so execute never re-queries.
- `SyncPlan` -- `{ actions: Vec<SyncAction>, new_email_state, new_mailbox_state }`. The plan plus the JMAP cursor to commit on success.
- `SyncDirection` -- `Both | PullOnly | PushOnly`. Top-level mode set by the CLI subcommand.
- `ActionDirection` -- `Pull | Push | Both`. Per-action classification used by `into_filtered`. The asymmetry (`Both` exists for actions but not modes) is on purpose: only `AdoptLocalMessage` is `Both`, and it's a hybrid that survives every direction filter.

**Reconcile context** (`src/sync/reconcile.rs`). `ReconcileCtx<'a>` is a private struct that bundles the immutable inputs every helper in `reconcile` needs (the three `known_by_*` indices, the `LocalIndex`, the `mailboxes` slice, the conflict strategy, plus a few derived sets). Threaded by `&` through helpers so each one stays short on parameters.

## Idempotency model

Read this before touching anything in `src/sync/` or `src/maildir_ops/`.

Two distinct identifiers anchor the system:

- **JMAP `email_id`** -- stable per server, lives in `message_map`.
- **RFC 5322 `Message-ID`** -- stable across both sides, survives a state DB wipe.

The state DB is **disposable**. Nuking `state.db` and re-running must converge on the existing maildir without re-downloading or duplicating files. That property is enforced by:

1. `maildir_ops::dedupe::dedupe_and_index` runs at the start of every sync cycle. It walks each synced folder, groups files by Message-ID (parsed from headers in `maildir_ops::headers`), and **deletes the newest by mtime** within each group. Newer copies are presumed to be jmapsync-introduced duplicates from a prior aborted run. Returns a `LocalIndex` keyed on Message-ID.

2. `reconcile` consults `message_map` (by JMAP id) -> `LocalIndex` (by Message-ID) before emitting a `DownloadMessage`. The Message-ID lookup is what saves us after a DB wipe: a known server email whose Message-ID we already have on disk is adopted into `message_map` rather than re-downloaded.

Don't add a code path that writes to a maildir outside `execute::execute` (or `dedupe`'s deletion of duplicates). Every
other writer would skip the Message-ID anchor.

## JMAP boundary quirks

The `jmap-client` crate is convenient but a few server behaviors need explicit handling:

- **`Email/changes` since `"0"` is not a bootstrap.** Fastmail (and the spec) allows the server to refuse with `cannotCalculateChanges`. Use `jmap::email::get_current_state` (`Email/get` with empty ids) to read the current state after an initial pull instead.
- **Error matching is on the human-readable string.** `jmap-client` formats errors via `Display` -- substring-match on `"Cannot calculate changes"`, not the JSON name `cannotCalculateChanges`. The recovery path on this error is: write empty string to `jmap_state`, which `queries::get_jmap_state` filters back to `None`, which routes to the initial-pull branch on the next cycle.
- **Maildir stores bare LF; `Email/import` rejects bare newlines.** `jmap::email::import_email` calls `normalize_crlf` before handing bytes to `email_import`. Don't normalize at maildir read time -- keep on-disk format native so other MUAs work.

## Sync internals

The orchestration lives in `src/sync/` and is layered:

- `engine.rs` -- the entry point for all three sync modes (`sync`, `pull_only`, `push_only`). Resolves which mailboxes to sync, runs the dedupe pass, fetches remote changes, scans local maildirs, hands everything to `reconcile`, filters the resulting plan by direction, hands it to `execute`.
- `plan.rs` -- the action vocabulary (`SyncAction`) and container (`SyncPlan`).
- `reconcile.rs` -- pure function from `(remote_emails, remote_destroyed, local_changes, DB indices, conflict strategy) -> SyncPlan`.
- `execute.rs` -- walks the plan in dependency order and performs the actual JMAP / filesystem / DB writes.

### The sync cycle

One call to `engine::sync` (or `pull_only` / `push_only`) goes through five phases. Phases 1-3 are pure data-gathering and
planning; phase 5 is the only place we mutate anything.

1. **Resolve mailboxes.** `resolve_mailboxes` queries `Mailbox/get`, honours `[sync].mailboxes`, applies the `INBOX` magic alias, and upserts each into `mailbox_map`. Returns a `Vec<(jmap_id, folder_name)>` that every later phase indexes against.

2. **Scan local + dedupe + collect remote changes.**
   - `dedupe::dedupe_and_index` runs first (see [Idempotency model](#idempotency-model)).
   - `scan::scan_folder` walks each folder, compares against `local_state`, emits `LocalChange::{NewMessage, FlagsChanged, DeletedMessage}`. `NewMessage` carries the parsed Message-ID when the file has one -- reconcile uses it for adoption and move detection.
   - `fetch_remote_state` either loops `Email/changes` from the persisted cursor (delta path) or runs `Email/query` per mailbox followed by `Email/get` (initial path). The `cannotCalculateChanges` error wipes the cursor and falls back to the initial path. Returns `(remote_emails, remote_destroyed, new_state, used_initial_path)`.

3. **Build indices and reconcile.** `build_known_indices` materialises three views over `message_map`, each keyed on a different identifier (see [Identifiers](#identifiers)):
   - `known_by_jmap: HashMap<jmap_email_id, MessageRecord>` -- 1:1, since `jmap_email_id` is the primary key.
   - `known_by_maildir: HashMap<maildir_id, MessageRecord>` -- 1:1, since each on-disk file maps to one `message_map` row.
   - `known_by_message_id: HashMap<message_id, Vec<MessageRecord>>` -- 1:N, since the same RFC 5322 Message-ID can appear in multiple folders.

   `reconcile::reconcile` consumes all of the above plus the configured `ConflictStrategy` and returns a `SyncPlan`.

4. **Filter by direction.** `SyncPlan::into_filtered(direction)` splits the plan into `(kept, dropped)`. `AdoptLocalMessage` is always kept regardless of direction (see [documentation on `plan.rs`, next](#plan-rs--the-action-vocabulary)). Dropped actions are logged so a `pull` or `push` user sees what was suppressed.

5. **Execute.** `execute::execute` walks the plan in a fixed order (adopt -> download -> local-flags -> local-move -> local-delete -> upload -> remote-set), then persists `new_email_state` into `jmap_state`.

### `plan.rs` -- the action vocabulary

`SyncAction` is the discrete unit reconcile emits and execute consumes. Each variant exists to model exactly one observable outcome; the vocabulary is deliberately small but distinct so reconcile can reason about them locally and execute can dispatch on shape.

#### Pull-side actions (server -> local)

- `DownloadMessage` -- server has an email we don't. Fetch the blob, store it in the maildir, write `message_map` and `local_state`.
- `UpdateLocalFlags` -- server keywords differ from our recorded flags; rewrite the maildir filename suffix and update the DB.
- `DeleteLocal` -- server says the JMAP id is destroyed and we have it locally. Unlink the file and clear DB rows.
- `MoveLocal` -- server claims the email lives in a different mailbox than our local copy. Move the file across maildirs and update the folder column.

#### Push-side actions (local -> server)

- `UploadMessage` -- local-only file. `Email/import` it and bind the resulting JMAP id.
- `UpdateRemoteKeywords` -- local flag change, server unchanged. Push keywords via `Email/set`.
- `DestroyRemote` -- file gone locally and we know the JMAP id. Tell the server to destroy.
- `MoveRemote` -- paired-delete-plus-new across folders is recognised as a move; emit a single mailbox-membership update instead of destroy+import.

#### The hybrid action

- `AdoptLocalMessage` -- pure DB write that binds an existing local file to a known JMAP id. No bytes move. Two cases:
  - Server claims a Message-ID we already have on disk (after a state DB wipe, or after a half-completed prior run).
  - User dropped a file into a maildir for a message that already exists upstream (would otherwise hit `alreadyExists` on import).

  Adoption belongs to neither side, so `direction()` returns `Both` and `into_filtered` always keeps it: it's strictly forward progress in either pull-only or push-only mode and never costs a network round-trip.

  `old_maildir_id: Option<String>` on `AdoptLocalMessage` covers the cross-folder local-move case: the same JMAP id is being rebound from an old maildir_id (about to be removed by the move's paired `DeletedMessage`) to a new one. Execute deletes the stale `local_state` row before upserting the new binding so the next scan doesn't re-emit `DeletedMessage` for the orphan.

#### `SyncPlan`

Holds the action vector plus the JMAP `Email` state cursor that should be persisted *if and only if* the cycle completes. Per-action counters (`download_count`, `adopt_count`, ...) drive the dry-run display. `into_filtered` is the one non-trivial method:

```rust
match (sync_direction, action.direction()) {
    (Both, _)              => keep,
    (_, Both)              => keep,   // adoption
    (PullOnly, Pull)       => keep,
    (PushOnly, Push)       => keep,
    _                      => drop,
}
```

### `reconcile.rs` -- building the plan

Reconcile is a pure function. It does no I/O and emits no actions of its own beyond pushing into `SyncPlan::actions`. The structure is:

```
reconcile()
|-- derive local_flag_changes, local_deletes, destroyed_set, news_by_message_id
|-- move pre-pass            (detect cross-folder local moves)
|-- process_remote_emails    (download / adopt / flag-update / move-local / conflict)
|-- process_remote_destroys  (DeleteLocal for any locally-bound id)
|-- emit_detected_moves      (MoveRemote + AdoptLocalMessage(old_maildir_id))
\-- process_local_changes    (upload / flag-push / destroy-remote)
```

Every helper takes an immutable `&ReconcileCtx<'a>` so the parameter lists stay bounded as new branches are added. Mutable per-cycle state (`adopted_maildir_ids`, `deletes_overruled_by_server`, the move bookkeeping sets) is threaded explicitly because the order of the passes matters.

#### The move pre-pass

`scan::scan_folder` doesn't know about cross-folder moves: when the user moves a file from `INBOX/cur/X` to `Archive/cur/Y`, scan emits `DeletedMessage(X)` and `NewMessage(Y, message_id=M)`. Reconcile pairs them by Message-ID:

1. For each `DeletedMessage` whose record has a Message-ID, look for a `NewMessage` with the same Message-ID in a different folder.
2. If found, build a `DetectedMove` capturing both the JMAP-side move (old/new mailbox ids) and the local-side rebind (old/new maildir_ids, prior/new flags).
3. Mark both ends as consumed so `process_local_changes` won't emit a separate `DestroyRemote` and `UploadMessage`.

`emit_detected_moves` then writes three actions per detected move:

- `MoveRemote` -- change the server-side mailbox membership.
- `AdoptLocalMessage { old_maildir_id: Some(...) }` -- rebind in DB, drop the orphan local_state row.
- `UpdateRemoteKeywords` -- only if flags also changed during the move.

The whole point of this pass is to avoid the `DestroyRemote + UploadMessage` shape, which would lose the JMAP id, the thread id, and the keyword history.

#### `process_remote_emails`

For each email the server returned:

1. **Skipped** if it's also in `destroyed_set` (handled separately).
2. **Skipped** if it isn't in any synced mailbox (with debug log).
3. **Path 1 -- known by JMAP id** -> `handle_known_remote`. This is the steady-state branch: resolve any flag conflict, emit `UpdateLocalFlags` if server flags drifted, emit `MoveLocal` if the server's mailbox membership disagrees with ours.
4. **Path 2 -- Message-ID matches a local file** -> `try_adopt_remote`. First check `known_by_message_id` (DB), then the in-memory `local_index` from the dedupe pass. The latter is what saves us after a `state.db` wipe. Emits `AdoptLocalMessage` and marks the maildir_id as adopted so phase 4 won't double-emit an upload.
5. **Path 3 -- nothing local** -> `DownloadMessage`.

#### `handle_known_remote` and conflict resolution

Two conflict shapes are handled inline:

- **delete-vs-update** -- local `DeletedMessage` for a JMAP id the server is still updating. `resolve_delete_conflict`:
  - `ServerWins`: re-download (restores the file). The orphan `local_state` row gets cleaned by the next scan cycle.
  - `LocalWins`: skip the remote-side action; `process_local_changes` will emit `DestroyRemote` unimpeded.

- **flag-vs-flag** -- both sides changed flags since last sync. `resolve_flag_conflict` picks one side, the loser is dropped, the winner is emitted as the corresponding action.

`deletes_overruled_by_server` is the cross-pass channel for the `ServerWins` delete case: when path 1 decides server wins,
`process_local_changes::handle_local_delete` must skip the `DestroyRemote` it would otherwise emit. Without this set the local loop would happily destroy the remote we just decided to re-download.

#### `process_local_changes`

After remote-side actions are settled, walk `local_changes` again:

- `NewMessage`: skip if `consumed_news` (move pre-pass) or `adopted_maildir_ids` (a remote pass already adopted this file).
  Otherwise check Message-ID against `known_by_message_id`:
  - Match in the *same* folder -> adopt.
  - Match in a *different* folder, with no paired delete -> warn and skip. This is a user-introduced duplicate that would hit `alreadyExists` on import every cycle and never converge.
  - No match -> `UploadMessage`.
- `FlagsChanged`: emit `UpdateRemoteKeywords` unless the server also changed flags (already handled in `handle_known_remote`).
- `DeletedMessage`: emit `DestroyRemote` unless `consumed_deletes` (move pre-pass), `destroyed_set` (server already destroyed it), or `deletes_overruled_by_server` says so.

#### Invariants reconcile maintains

- Reconcile never reads the network or the filesystem; all inputs are passed in. This makes it cheap to add tests.
- A given local maildir_id appears in at most one push-side action per plan (the `consumed_*` and `adopted_maildir_ids` sets enforce this).
- A given JMAP email_id appears in at most one pull-side action per plan, with the exception that delete-vs-update conflicts in `ServerWins` mode emit `DownloadMessage` even though there's a `DeletedMessage` in `local_changes` for the same id (the local loop is suppressed via `deletes_overruled_by_server`).
- `AdoptLocalMessage` is emitted before the `NewMessage` loop runs, so adoption pre-empts upload deterministically.

### `execute.rs` -- performing the side-effects

Execute is the only place that mutates anything. It buckets the plan by variant, then drains buckets in a fixed order:

```
adopt -> download -> local-flags -> local-move -> local-delete ->
upload -> remote-keywords -> remote-move -> remote-destroy
```

This ordering is load-bearing in a few places:

- **Adopt before everything.** Subsequent actions (`UpdateLocalFlags`, `MoveLocal`) may reference the maildir_id / jmap_email_id being bound; they only succeed if `message_map` already has the row.
- **Pull side before push side.** We want the local DB to settle to its post-pull state before we tell the server about local changes -- both because the conflict resolution in reconcile assumed the pull would land first, and because failed pushes leave the DB in a state where the next cycle can still see the original local changes.
- **Remote-set last and batched.** `apply_remote_set` collapses every `UpdateRemoteKeywords`, `MoveRemote`, and `DestroyRemote` into a single `Email/set` call (JMAP allows arbitrary `update` and `destroy` entries in one method call). This minimises round trips and keeps the per-id failure handling uniform: the call returns `failed_updates` and `failed_destroys` sets, and we mirror successes into the DB while skipping the failures.

#### Per-handler responsibilities

Each handler is a small function that destructures the action, performs the I/O, and writes the DB. The pattern is consistent:

```rust
let SyncAction::Foo { fields, .. } = action else { continue };
do_io(fields)?;
queries::upsert_message(conn, &MessageRecord { ... })?;
queries::upsert_local_state(conn, ...)?;
info!("...");
```

A handful of handler-specific notes:

- `adopt_messages` -- when `old_maildir_id` is set (cross-folder local move), call `delete_local_state(old)` first so the orphan row from the source folder is removed before the new binding is written.
- `update_local_flags` -- flag changes go to disk via `store::set_flags` (which renames the file), then DB. If the disk rename fails the DB is left untouched and the cycle continues (the next cycle will re-derive the change).
- `upload_messages` -- extracts flags from the on-disk filename rather than trusting the action payload, because the maildir filename is the single source of truth for flag state. Catches `alreadyExists` and warns rather than failing the cycle: hitting this means reconcile's adoption guard didn't fire (typically the Message-ID was unparseable), and bailing on a single bad message shouldn't kill the run.
- `apply_remote_set` -- see above. Per-id failures are silently skipped from the DB-mirror pass so the local DB never claims the server is in a state it isn't.

#### Download concurrency and rate-limit halving

`run_downloads` is the only handler that fans out concurrently:

- Configured concurrency (`config.sync.download_concurrency`) is clamped at startup to the server's advertised `maxConcurrentRequests` (Fastmail: 10).
- Each pass spawns up to `n` concurrent `Email/blob` downloads via `buffer_unordered`.
- If any download returns a transient error (rate limit, 5xx), succeeded entries are committed, failed entries stay in `pending`, concurrency is halved, the loop sleeps 500ms and retries.
- On a hard error from any download, the cycle returns the error after committing whatever did succeed.
- If a pass returns no successes and no rate-limit, we bail with "stream stalled" rather than looping forever.

The halving is one-way per cycle; it doesn't ratchet back up on success. This is intentional -- a flaky link that hit the cap once is likely to hit it again, and the next cycle starts fresh from the configured value anyway.

#### State persistence

After every handler returns, `execute` writes `new_email_state` (if present) into `jmap_state`. This is the only place the JMAP cursor advances -- if any handler returned an error, we never reach this write and the next cycle re-runs `Email/changes` from the prior cursor. The DB writes that *did* happen before the error are kept, which is fine because they're all idempotent against a re-run.

### Adding a new SyncAction

When you need to model a new outcome, the checklist is:

1. Add a variant to `SyncAction` with all the fields needed to execute it without re-querying.
2. Decide its `direction()` -- Pull, Push, or Both. If Both, it must be byte-cheap (no network), since it survives every direction filter.
3. If it should appear in the dry-run summary, add a `*_count` helper and a `Display` arm.
4. Add a bucket in `execute::execute` and slot it into the dependency order. Think about whether it should run before or after adopt.
5. Add a handler. Follow the destructure-then-IO-then-DB pattern; keep DB writes idempotent.
6. Emit the variant from reconcile in exactly one place if you can, so the conditions under which it fires are localised.

Things to avoid:

- Don't write to a maildir outside `execute` (or `dedupe`'s deletion of duplicates). Every other writer would skip the Message-ID check that anchors the idempotency model.
- Don't add an `Email/set` call outside `apply_remote_set` -- keep remote mutations batched.
- Don't advance `jmap_state` from anywhere except the tail of `execute::execute`.

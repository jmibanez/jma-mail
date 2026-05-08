# DEVELOPMENT.md

A working tour of `jma`'s internals, for those who want to work on the codebase. Skim the first three sections to get oriented; the later sections drill into `src/sync/` (the only part of the codebase with non-trivial logic).

## What this is

`jma` (binary; crate name `jma-mail`) is a Rust CLI that bidirectionally syncs email between a JMAP server (targeting Fastmail) and a local Maildir -- like `mbsync`/`isync` but speaking JMAP. Single binary, async (tokio), state in SQLite.

Currently, `jma` only supports **API token (Bearer)** auth. OAuth might be implemented in the future, but that would somehow entail saving an OAuth API private key; for now API tokens work. Tokens are resolved per-account: the OS keychain (entry keyed on the account's email, set via `jma auth set-token --account <email>`) wins over the in-file `[account].token` fallback. `src/auth.rs` wraps `keyring-core` with the platform-native backend (Keychain on macOS, Secret Service on Linux, Credential Manager on Windows).

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

- `src/main.rs` -- subcommand dispatch and lock acquisition.
- `src/cli.rs` -- clap definitions for subcommands and global flags.
- `src/config.rs` -- TOML config schema and template.
- `src/auth.rs` -- bearer-token storage in the OS keychain (per-email scoping; platform backends behind `keyring-core`).
- `src/ids.rs` -- newtype wrappers for the various string IDs (see [ID newtypes](#id-newtypes)).
- `src/jmap/` -- JMAP client wrapper. `session.rs` opens the connection, `discovery.rs` resolves the session URL via DNS SRV / well-known per RFC 8620 section 2.2, `limits.rs` clamps every server-advertised cap against a hardcoded ceiling (so a hostile or buggy server can't induce DoS via absurd values), `email.rs` is the per-method veneer over `jmap-client`, `mailbox.rs` handles `Mailbox/get`, `retry.rs` classifies errors as transient vs hard, `types.rs` holds the internal `EmailObject` shape.
- `src/maildir_ops/` -- anything that touches the filesystem. `store.rs` wraps the `maildir` crate for read/write, `flags.rs` translates between maildir suffix flags and JMAP keywords, `scan.rs` walks a folder and emits `LocalChange`s, `dedupe.rs` does the Message-ID-based duplicate sweep, `headers.rs` parses `Message-ID` out of a maildir file, `layout.rs` resolves a JMAP mailbox tree onto a single on-disk folder name under the configured `FolderLayout` and applies any matching `[[rename_rules]]`, `lock.rs` takes the per-maildir advisory lock.
- `src/state/` -- SQLite. Schema in `db.rs::SCHEMA`. Tables:
  - `jmap_state` -- per-entity sync cursor (one row per `(account, entity)` like `("acct", "Email")`).
  - `message_map` -- JMAP<->maildir binding, indexed on `maildir_id`, `message_id`, `mailbox_id`.
  - `mailbox_map` -- server mailbox metadata.
  - `local_state` -- filesystem snapshot for change detection.
  - `jmap_discovery` -- cached session URL keyed on the account's email domain; populated by `session::connect` after autodiscovery, cleared and rediscovered on a structural connect failure.

  All access goes through `queries.rs`. Don't `prepare` ad-hoc SQL elsewhere; if you need a new query, add it there.
- `src/sync/` -- orchestration. See [Sync internals](#sync-internals) below.
- `src/daemon/` -- `watch` mode. `runner.rs` runs an initial sync then concurrently spawns `eventsource.rs` (SSE listener on the JMAP `eventSourceUrl` from the session) and `watcher.rs` (filesystem `notify` with debouncing). Both feed a `tokio::sync::mpsc` channel of `SyncTrigger`s; the main loop drains and re-runs `SyncEngine::sync` per trigger. `hook.rs` runs `post_arrival_command` after sync cycles that downloaded mail, coalescing overlapping triggers.

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

### ID newtypes

Each identifier above has a corresponding newtype in `src/ids.rs`: `JmapEmailId`, `JmapBlobId`, `JmapThreadId`, `JmapMailboxId`, `JmapAccountId`, `MaildirId`, `MessageId`. They are all `String`-backed and generated by a single `define_id_newtype!` macro so the trait surface stays uniform: `From<String>`/`From<&str>`, `AsRef<str>`, `Borrow<str>` (so `HashMap<NewType, V>::get(&str)` works without allocating), `Display`, rusqlite `ToSql`/`FromSql`, and serde `transparent`. None of them implement `Default` or `PartialEq<&str>`, on purpose -- both would silently re-admit the bare-string slop the newtypes are meant to prevent.

The point is that the compiler now distinguishes a server `Email/id` from a folder name from a maildir basename, even though all three would happily fit in a `String`. Every internal slot that used to be `String` -- `MessageRecord` columns, `SyncAction` payload fields, `DetectedMove`, `ReconcileCtx` index keys and per-cycle tracking sets, the engine/daemon `account_id` plumbing -- is typed at its newtype.

There are exactly two boundaries that still take/return bare strings, and conversion happens *at those boundaries*:

- **jmap-client** -- the upstream crate's APIs take `Into<String>`, return `&str`, and serialize JSON keys as plain strings. `EmailSetOp::SetMailboxes.target_mailbox_ids` is `Vec<String>` for the same reason. We cross with `String::from(&id)` / `id.as_ref()` at the call site.
- **rusqlite + raw SQL** -- `queries::{get,set}_jmap_state` keeps `account_id: &str`. The newtype's `ToSql`/`FromSql` impls cover *column* values, but param boundaries against literal SQL stay string-typed because they're not bound to any one ID kind. We cross with `id.as_ref()`.

Stay typed everywhere else. If you find yourself writing `String::from(...)` mid-pipeline, the right fix is almost always to change the slot's type, not to add another conversion.

## Data model

The sync pipeline moves a small set of internal types between phases. Each one lives in a specific module, has a specific role, and gets translated into another type at module boundaries. Knowing the cast helps when reading any handler.

**JMAP-side types** (`src/jmap/types.rs`). Hand-rolled rather than re-exported from `jmap-client` so the rest of the codebase doesn't depend on the upstream crate's shape.

- `EmailObject` -- `{ id, blob_id, thread_id, mailbox_ids: HashMap<jmap_mailbox_id, bool>, keywords: HashMap<String, bool>, message_id: Option<Vec<String>>, subject }`. Produced by `Email/get` and `Email/changes`-then-`Email/get`; consumed by `reconcile`. The `mailbox_ids` map's bool is always `true` (JMAP's convention for set membership); we keep the type as-is to match the wire format.
- `MailboxObject` -- mailbox metadata from `Mailbox/get`. Consumed by `resolve_mailboxes` and immediately translated into a `MailboxRecord` for the DB.
- `ChangesResponse` -- `{ old_state, new_state, created, updated, destroyed, has_more_changes }`. The shape `Email/changes` returns; loop control for `fetch_remote_state`.
- `SessionInfo` -- session URLs and account id, captured at connect time.

**State-side types** (`src/state/queries.rs`). One struct per row-shape; every read or write goes through these.

- `MessageRecord` -- one `message_map` row. Carries everything needed to act on an email without re-querying: both server identifiers (`jmap_email_id`, `jmap_blob_id`, `jmap_thread_id`, `mailbox_id`), both local identifiers (`maildir_id`, `maildir_folder`), the cross-cutting `message_id`, and a flags pair (`flags` for the maildir suffix view, `jmap_keywords` for the JSON-serialised JMAP view). `message_id` is non-`Option` (and `NOT NULL` in the DB schema): scan and reconcile both refuse to write a row without one. The `maildir_*`, `jmap_blob_id`, and `jmap_thread_id` fields remain `Option` because a row can be JMAP-known but not yet bound to an on-disk file during in-flight phases (and the blob/thread ids aren't always carried through every code path that constructs a `MessageRecord`).
- `MailboxRecord` -- one `mailbox_map` row. Mostly a snapshot of `MailboxObject` plus the resolved `maildir_folder` (with the `INBOX` magic alias applied).

**Maildir-side types** (`src/maildir_ops/`). What `scan` and `dedupe` produce for `reconcile` to chew on.

- `LocalChange` (enum) -- one of:
  - `NewMessage { maildir_id, folder, flags, path, message_id, size_bytes }` -- a file appeared that wasn't in `local_state`. `message_id` is non-`Option`: `scan::scan_folder` refuses to construct a `NewMessage` for a file without a parseable Message-ID header (it `error!`s and skips the file), so reconcile never has to defend against the missing-anchor case. `size_bytes` is the on-disk size captured at scan time, used by reconcile to refuse oversized uploads at plan time without doing its own I/O.
  - `FlagsChanged { maildir_id, folder, old_flags, new_flags }` -- the maildir filename suffix changed.
  - `DeletedMessage { maildir_id, folder }` -- a `local_state` row has no corresponding file on disk.
- `LocalEntry` -- `{ folder, maildir_id }`. One on-disk file's location.
- `LocalIndex` -- `{ by_message_id: HashMap<message_id, Vec<LocalEntry>> }`. Assembled once per cycle by `SyncEngine::run` from the `on_kept` callback `dedupe` invokes per surviving file. The `Vec` here, like `known_by_message_id`, exists because the same Message-ID can legitimately live in several folders.

**Plan types** (`src/sync/plan.rs`). The bridge between `reconcile` and `execute`. Detailed in [`plan.rs`](#plan-rs--the-action-vocabulary) and [`reconcile.rs`](#reconcile-rs--building-the-plan); summarised here for the cast list:

- `LocalId` / `RemoteId` / `BoundId` -- typed message-identity wrappers. Each pairs an opaque id with its RFC 5322 `Message-ID` (non-`Option`: scan and reconcile both refuse to construct one of these for a message without a parseable Message-ID, so by the time downstream code holds one the anchor is guaranteed). `Display` formats `id (<msg-id>)`. `LocalId` carries a `MaildirId`; `RemoteId` carries a `JmapEmailId`; `BoundId` carries both (a message bound on both sides) and exposes `as_remote()`. Every `SyncAction` variant takes the identity it actually needs (download takes `RemoteId`, adopt/local-flags/local-move/local-delete take `BoundId`, upload takes `LocalId`, remote-keyword/destroy/move take `RemoteId`), so log lines just `{}` the field instead of formatting the id-pair by hand. `AdoptLocalMessage::old_maildir_id` is `Option<MaildirId>` rather than a `LocalId` bundle: it's a DB-cleanup hint (the row to drop on a cross-folder rebind), not an identity worth logging.
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

1. `maildir_ops::dedupe::dedupe` runs at the start of every sync cycle. It walks each synced folder, groups files by Message-ID (parsed from headers in `maildir_ops::headers`), and **deletes the newest by mtime** within each group. Newer copies are presumed to be jma-introduced duplicates from a prior aborted run. Fires an `on_kept` callback once per surviving file; `SyncEngine::run` uses that callback to build a `LocalIndex` keyed on Message-ID.

2. `reconcile` consults `message_map` (by JMAP id) -> `LocalIndex` (by Message-ID) before emitting a `DownloadMessage`. The Message-ID lookup is what saves us after a DB wipe: a known server email whose Message-ID we already have on disk is adopted into `message_map` rather than re-downloaded.

3. **Both ingest boundaries refuse Message-ID-less messages.** Local files without a parseable `Message-ID` header are skipped with `error!` by `maildir_ops::scan::scan_folder` and `maildir_ops::dedupe::dedupe`. Remote emails whose `Email/get` response carries no `messageId` and aren't already bound by JMAP id are skipped at `sync::reconcile::process_remote_emails`. The known-JMAP-id path still applies flag/move updates -- the existing `message_map` row is itself the anchor, so a server that strips Message-ID from updates doesn't break flag sync. The result: no row enters `message_map`, and no `LocalChange::NewMessage` is emitted, without an anchor.

Don't add a code path that writes to a maildir outside `Executor::execute` (or `dedupe`'s deletion of duplicates). Every other writer would skip the Message-ID check.

### Schema versioning: nuke-and-resync as the migration story

There is no in-place migration system. Whenever the SQLite schema or the invariants the code expects of existing rows change in a way the previous binary's writes would violate (e.g. tightening a column from `NULL`-able to `NOT NULL`, adding a uniqueness constraint, changing what a string-encoded field represents), bump `state::db::SCHEMA_VERSION`. The version is stored as SQLite's `PRAGMA user_version`.

On open, the binary compares the on-disk version against `SCHEMA_VERSION`:

- **Match** -- proceed.
- **Mismatch** under a mutating command (`sync`, `pull`, `push`, `watch`) -- `open_or_recreate` `warn!`s and unlinks `state.db` plus its `-wal` and `-shm` siblings, then recreates an empty schema. The mutating command holds the state DB lock at this point, so no concurrent jma sharing this DB (same config, or — once the multi-account refactor lands — sibling per-account drivers against a shared DB) can race with the unlink. The disposability invariant is what makes this safe: the maildir + JMAP server are the source of truth, and the dedupe pass plus Message-ID-anchored adoption rebind every existing local file without re-downloading bytes.
- **Mismatch** under a read-only command (`status`, `mailboxes`, `auth rediscover`) -- `open` refuses with an actionable error pointing the user at `jma sync`. Read-only paths don't hold the state DB lock and so can't safely nuke; deferring to the next mutating run keeps the locking invariant intact.

Both directions of mismatch (older binary, newer DB; or newer binary, older DB) take the same auto-nuke path. Disposability cuts both ways. A pre-versioning DB (`user_version = 0` with populated tables) is treated as stale.

When you bump `SCHEMA_VERSION`, you don't need to write migration code -- but you DO need to mention the field/invariant change in the commit message, and ideally add a regression test that constructs an old-schema DB on disk and checks `open_or_recreate` rebuilds correctly.

## Concurrency model

Two distinct surfaces need protection from concurrent mutation, plus write-coherence inside the cycle. They get different mechanisms because they have different threat models.

### Maildir mutual exclusion

`maildir_ops::lock::acquire_lock` takes a `flock`-backed advisory lock on `<maildir_root>/.jma.lock`. Mutating jma invocations (`sync`, `pull`, `push`, `watch`) against the same maildir can't both run; the second fails with the first's PID in the error message. Read-only commands (`status`, `mailboxes`) and maildir-less commands (`init`, `auth`) skip this.

The lock keys on the maildir root, not the state DB, because the maildir is the shared mutation surface across processes. Two configs pointing at *different* state DBs but the *same* maildir would otherwise race -- both could write the same Message-ID under different filenames and clobber each other. Per-account-Maildir-root is the universal convention across mbsync, OfflineIMAP, getmail, etc., so this serializes exactly what needs serializing without artificially preventing legitimate multi-account setups (different accounts -> different roots -> different locks).

### State DB lock (the unlink gate)

`state::db::acquire_lock` takes a separate `flock` on `<db_path>.lock`. This lock specifically gates `open_or_recreate`'s schema-mismatch unlink path: without it, a process that decides the on-disk schema is stale would `unlink` the DB out from under any concurrent process that opened the same DB but doesn't realise it's about to be deleted (writes vanish into the orphaned inode; a later checkpoint can corrupt or panic).

The maildir lock alone doesn't cover this, because two processes can hold *different* maildir locks while sharing a state DB -- which is exactly the topology the multi-account refactor produces (per-account daemon drivers, each with their own maildir, all pointing at one widened-PK DB).

Note that with the maildir-relative default for `[state].db_path`, the state DB lives at `<maildir_root>/.jma.db` and the maildir lock structurally covers what the DB lock guards -- any process that could open this DB must already hold the maildir lock by construction. The DB lock is only load-bearing when `[state].db_path` is set explicitly to a path outside the maildir, where two configs can share a DB while owning different maildirs. Today both locks are taken unconditionally regardless of topology; gating the DB lock on config shape is a candidate simplification but not a current one.

**Lock-acquisition order: maildir lock first, then state DB lock**, consistently across every mutating call site (see `acquire_mutator_locks` in `src/main.rs`). Consistent order is what prevents deadlock between two contending pairs. Both locks are held for process lifetime; the kernel releases them when the fds close at process exit.

### State DB write coherence

The locks above protect against destruction; per-batch transactions plus SQLite's WAL writer-lock protect against interleaved writes:

1. **Intra-cycle write atomicity** -- `src/sync/execute.rs` wraps each mutation phase in a SQLite transaction via `Connection::unchecked_transaction()`. Pure-DB phases (`adopt_messages`, `apply_move_pair_adopts`, the post-network section of `apply_remote_set`) take one transaction per phase, so a panic mid-loop rolls the whole phase back. FS-mutating phases (`update_local_flags`, `move_local_messages`, `delete_local_messages`, `upload_messages`, `run_downloads`) take one transaction per iteration around the paired DB writes that follow each successful FS op, so a row's `message_map` and `local_state` never disagree even if the second DB write fails. Side benefit: SQLite's WAL writer-lock serializes any other writer on the file from the first write through commit; the eventual multi-account refactor (one engine per account against a shared DB) inherits this serialization for free.

2. **External writers** -- a deliberate bypass (e.g. `sqlite3 state.db "UPDATE ..."` typed in error, or any tool that opens the DB without going through the state DB lock) is not prevented. Per-batch transactions block such writers from interleaving *within* a phase, but they can still interleave between phases. The recovery story is "next sync cycle re-reconciles from server state"; the design accepts this rather than holding cycle-spanning transactions across network I/O (which would balloon the WAL during long initial syncs and lose Ctrl-C-mid-cycle partial-progress recovery).

## JMAP boundary quirks

The `jmap-client` crate is convenient but a few server behaviors need explicit handling:

- **`Email/changes` since `"0"` is not a bootstrap.** Fastmail (and the spec) allows the server to refuse with `cannotCalculateChanges`. Use `jmap::email::get_current_state` (`Email/get` with empty ids) to read the current state after an initial pull instead. Detect this error via `jmap::email::is_cannot_calculate_changes`, which walks the anyhow chain and downcasts to the typed `jmap_client::Error::Method(MethodErrorType::CannotCalculateChanges)` -- never substring-match Display strings, since a `.context(...)` wrapper at any layer breaks that. Recovery: write the empty string to `jmap_state`, which `queries::get_jmap_state` filters back to `None`, which routes to the initial-pull branch on the next cycle.
- **Maildir stores bare LF; `Email/import` rejects bare newlines.** `jmap::email::import_email` calls `normalize_crlf` before handing bytes to `email_import`. Don't normalize at maildir read time -- keep on-disk format native so other MUAs work.
- **`mailbox_id(id, bool)` patches can't remove memberships.** `jmap-client` 0.4.1 types its `mailboxIds` patch map as `bool`, so the only values it can emit are `true` and `false`. Per RFC 8621 §4.1.1 `mailboxIds` is `Id[Boolean]` whose values are always `true`, and per RFC 8620 §5.3 a key is removed by patching its value to `null` -- which the typed-`bool` map cannot serialize. Strict servers (Fastmail) reject the `false`-valued patch as `notUpdated`. For moves, use the full-replacement `mailboxIds(...)` setter instead, which sends the entire target set in one go.
- **Session URL resolution disables redirects.** `session::connect` resolves the URL via explicit `[account].session_url` override, then the `jmap_discovery` cache, then RFC 8620 section 2.2 autodiscovery (DNS SRV `_jmap._tcp.<domain>`, fallback `https://<domain>/.well-known/jmap`). The well-known probe runs with `redirect::Policy::none()`: per RFC 8620 section 2.1 the canonical session resource requires an authenticated GET, and following a 30x would land us there unauthenticated and get a 401. On a structural connect failure against a cached URL we clear the cache row and rediscover.

### Server-cap clamping

The JMAP server is a trust boundary: per RFC 8620 section 2 it can advertise any value it likes for its session capabilities, including absurd ones. A hostile or ill-configured upstream could otherwise force us to bundle megabytes of metadata into a single `Email/get` response, OOM us with a too-large upload, or escape the maildir tree with an oversized mailbox name. Every server-advertised limit therefore goes through `src/jmap/limits.rs`, which `min`'s the value against a hardcoded ceiling (`MAX_SET_BATCH_SIZE`, `MAX_GET_BATCH_SIZE`, `MAX_UPLOAD_FILE_SIZE`, `MAX_MAILBOX_NAME_LEN`) or, for the concurrency knobs, against the user-configured value. The server can only **lower** what we'd otherwise do; it can never raise it. Direct reads of `client.session().core_capabilities()...` elsewhere are a smell -- if you find yourself reaching for one, add or extend an accessor in `limits.rs` and route through it.

## Sync internals

The orchestration lives in `src/sync/` and is layered:

- `engine.rs` -- the entry point for all three sync modes (`sync`, `pull_only`, `push_only`). Resolves which mailboxes to sync, runs the dedupe pass, fetches remote changes, scans local maildirs, hands everything to `reconcile`, filters the resulting plan by direction, hands it to `execute`.
- `plan.rs` -- the action vocabulary (`SyncAction`) and container (`SyncPlan`).
- `reconcile.rs` -- pure function from `(remote_emails, remote_destroyed, local_changes, DB indices, conflict strategy) -> SyncPlan`.
- `execute.rs` -- walks the plan in dependency order and performs the actual JMAP / filesystem / DB writes.

### The sync cycle

One call to `SyncEngine::sync` (or `pull_only` / `push_only`) goes through five phases. Phases 1-3 are pure data-gathering and
planning; phase 5 is the only place we mutate anything.

1. **Resolve mailboxes.** `resolve_mailboxes` queries `Mailbox/get`, honours `[sync].mailboxes`, applies the `INBOX` magic alias, and upserts each into `mailbox_map`. Returns a `Vec<(jmap_id, folder_name)>` that every later phase indexes against.

2. **Scan local + dedupe + collect remote changes.**
   - `dedupe::dedupe` runs first (see [Idempotency model](#idempotency-model)).
   - `scan::scan_folder` walks each folder, compares against `local_state`, emits `LocalChange::{NewMessage, FlagsChanged, DeletedMessage}`. `NewMessage` carries the parsed Message-ID when the file has one -- reconcile uses it for adoption and move detection.
   - `fetch_remote_state` either loops `Email/changes` from the persisted cursor (delta path) or runs `Email/query` per mailbox followed by `Email/get` (initial path). The `cannotCalculateChanges` error wipes the cursor and falls back to the initial path. Returns `(remote_emails, remote_destroyed, new_state, used_initial_path)`.

3. **Build indices and reconcile.** `build_known_indices` materialises three views over `message_map`, each keyed on a different identifier (see [Identifiers](#identifiers)):
   - `known_by_jmap: HashMap<jmap_email_id, MessageRecord>` -- 1:1, since `jmap_email_id` is the primary key.
   - `known_by_maildir: HashMap<maildir_id, MessageRecord>` -- 1:1, since each on-disk file maps to one `message_map` row.
   - `known_by_message_id: HashMap<message_id, Vec<MessageRecord>>` -- 1:N, since the same RFC 5322 Message-ID can appear in multiple folders.

   `reconcile::reconcile` consumes these plus the dedupe pass's `LocalIndex`, the `[(jmap_id, folder_name)]` mailbox list from phase 1, the configured `ConflictStrategy`, and the effective `maxSizeUpload` cap (used to refuse oversized local files at plan time). The full input set is bundled as `ReconcileInput` in `src/sync/reconcile.rs`. Returns a `SyncPlan`.

4. **Filter by direction.** `SyncPlan::into_filtered(direction)` splits the plan into `(kept, dropped)`. `AdoptLocalMessage` is always kept regardless of direction (see [documentation on `plan.rs`, next](#plan-rs--the-action-vocabulary)). Dropped actions are logged so a `pull` or `push` user sees what was suppressed.

5. **Execute.** `Executor::execute` walks the plan in a fixed bucket order, performs the JMAP / filesystem / DB writes, and ratchets the JMAP cursor at the tail. See [`execute.rs`](#executers--performing-the-side-effects) for the full bucket order, load-bearing dependencies, and the chain-validation step that decides whether the cursor advances.

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

  `old_maildir_id: Option<MaildirId>` on `AdoptLocalMessage` covers the cross-folder local-move case: the same JMAP id is being rebound from an old maildir_id (about to be removed by the move's paired `DeletedMessage`) to a new one. Execute deletes the stale `local_state` row before upserting the new binding so the next scan doesn't re-emit `DeletedMessage` for the orphan.

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
adopt (unconditional) -> download -> local-flags -> local-move ->
local-delete -> upload -> remote-set (keywords + moves + destroys, batched)
-> adopt (move-paired)
```

This ordering is load-bearing in a few places:

- **Adopt before everything (mostly).** Unconditional adopts -- those that bind a maildir file to a server email without an associated remote-side move -- run first, because subsequent actions (`UpdateLocalFlags`, `MoveLocal`) may reference the maildir_id / jmap_email_id being bound and only succeed if `message_map` already has the row.
- **Move-paired adopts run last, after the remote-set batch.** When `AdoptLocalMessage` carries an `old_maildir_id` it is the DB half of a cross-folder local move whose JMAP half was emitted as a `MoveRemote`. Committing the adopt up front would advance the DB to the destination folder; if the paired `MoveRemote` then fails per-id inside `apply_remote_set`, the next reconcile cycle would see a divergence (server still in source, DB in destination) and emit a backwards `MoveLocal` that undoes the user's move. Deferring until after `apply_remote_set` lets `apply_move_pair_adopts` consult the call's `failed_updates` set and skip the adopt for any move-pair the server rejected.
- **Pull side before push side.** We want the local DB to settle to its post-pull state before we tell the server about local changes -- both because the conflict resolution in reconcile assumed the pull would land first, and because failed pushes leave the DB in a state where the next cycle can still see the original local changes.
- **Remote-set batched.** `apply_remote_set` collapses every `UpdateRemoteKeywords`, `MoveRemote`, and `DestroyRemote` into a single `Email/set` call (JMAP allows arbitrary `update` and `destroy` entries in one method call). This minimises round trips and keeps the per-id failure handling uniform: the call returns `failed_updates` and `failed_destroys` sets, and we mirror successes into the DB while skipping the failures.

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
- `upload_messages` -- extracts flags from the on-disk filename rather than trusting the action payload, because the maildir filename is the single source of truth for flag state. Catches `alreadyExists` and warns rather than failing the cycle: hitting this means reconcile's adoption guard didn't fire (typically a race with another writer between scan and import), and bailing on a single bad message shouldn't kill the run.
- `apply_remote_set` -- see above. Per-id failures are silently skipped from the DB-mirror pass so the local DB never claims the server is in a state it isn't.

#### Download and upload concurrency

Both `run_downloads` and `upload_messages` fan out via `buffer_unordered`, capped at the effective per-handler concurrency from `limits::` (`config.sync.{download,upload}_concurrency` clamped to the server's `maxConcurrentRequests` / `maxConcurrentUpload`). DB writes happen serially after the stream drains, since `rusqlite::Connection` isn't `Send` and can't cross await points inside the parallel futures. Each successful `Email/import` also contributes its `oldState` / `newState` pair to the cursor ratchet (see [State persistence](#state-persistence)); `Email/blob` is a pure data fetch so downloads don't.

Downloads get a rate-limit halving loop: a transient failure within a pass commits the successes, halves concurrency, sleeps 500ms, and retries the unfinished entries; the halving is one-way per cycle, since a link that hit the cap once is likely to hit it again, and the next cycle resets from the configured value. A hard error short-circuits after committing whatever did succeed; a pass with no progress and no transient signal bails with "stream stalled" rather than spinning. Uploads don't halve -- they drain the stream regardless, and the first observed error becomes the cycle's hard error.

#### State persistence

After every handler returns, `execute` writes `new_email_state` (if present) into `jmap_state`. This is the only place the JMAP cursor advances -- if any handler returned an error, we never reach this write and the next cycle re-runs `Email/changes` from the prior cursor. The DB writes that *did* happen before the error are kept, which is fine because they're all idempotent against a re-run.

The cursor we write is not unconditionally `new_email_state`. Per RFC 8620 section 7.1, every state-advancing call (`Email/import`, `Email/set`) returns its own `oldState` / `newState` pair, and a cursor can only legitimately move along a chain whose edges are all present. `execute` collects each call's pair into a chain map keyed on `oldState`, then walks forward from the cursor reconcile produced. If every edge is consumed in a single contiguous walk, no third-party write landed between our calls and we ratchet the cursor to the walk's end; if any edge is missing or unreachable, we leave the cursor alone and let the next cycle's `Email/changes` resume from the unmoved point.

### Adding a new SyncAction

When you need to model a new outcome, the checklist is:

1. Add a variant to `SyncAction` with all the fields needed to execute it without re-querying.
2. Decide its `direction()` -- Pull, Push, or Both. If Both, it must be byte-cheap (no network), since it survives every direction filter.
3. If it should appear in the dry-run summary, add a `*_count` helper and a `Display` arm.
4. Add a bucket in `Executor::execute` and slot it into the dependency order. Think about whether it should run before or after adopt.
5. Add a handler. Follow the destructure-then-IO-then-DB pattern; keep DB writes idempotent.
6. Emit the variant from reconcile in exactly one place if you can, so the conditions under which it fires are localised.

Things to avoid:

- Don't write to a maildir outside `execute` (or `dedupe`'s deletion of duplicates). Every other writer would skip the Message-ID check that anchors the idempotency model.
- Don't add an `Email/set` call outside `apply_remote_set` -- keep remote mutations batched.
- Don't advance `jmap_state` from anywhere except the tail of `Executor::execute`.

## Local Maildir layout

A maildir is classically a single flat `cur` / `new` / `tmp` triplet, with no native concept of a folder hierarchy, so different MUAs have invented different conventions for projecting one onto the on-disk shape. jma supports three, picked via `[sync].folder_layout`:

- **`Flat`** -- mbsync's `Flatten=<sep>` convention. Parent/child becomes `<root>/parent.child/{cur,new,tmp}` with a user-chosen `[sync].hierarchy_separator` (default `.`).
- **`MaildirPP`** -- the Courier/Dovecot Maildir++ convention: every synced folder is prefixed with a single `.` at the root, so a child `Sent` of parent `[Airmail]` becomes `<root>/.[Airmail].Sent/`.
- **`Fs`** -- Dovecot's `LAYOUT=fs`. Hierarchy materialises as a recursive directory tree: `<root>/parent/child/{cur,new,tmp}`.

The translation lives in `maildir_ops::layout::resolve_folder_path`. `mailbox_map.maildir_folder` and every downstream `maildir_folder` string (in `LocalChange`, `MessageRecord`, etc.) is the post-resolution name -- callers don't see the JMAP hierarchy directly. The `INBOX` magic alias documented in [Identifiers](#identifiers) is applied at the same point: regardless of the configured layout, the inbox-role mailbox lands in a folder named `INBOX` (or `.INBOX` under MaildirPP) so other tools handed the maildir get the conventional name rather than the locale-specific JMAP display string.

### User-driven rename rules

Top-level `[[rename_rules]]` tables in `config.toml` let the user pin specific folder paths to specific on-disk names; see the README section for the user-facing shape. Internally:

- The on-disk shape (`MaildirRenameRule` in `src/config.rs`) is the serde-deserialised form: `MapDirectly { source_folder_path, renamed_name }` or `Pattern { source_folder_pattern, rename_pattern }`. Both pattern strings are still raw at this point.
- `compile_maildir_rename_rules` runs at `Config::load` time. For `Pattern` rules it compiles `source_folder_pattern` via the `regex` crate; the compile is itself the validation step (a malformed regex fails `Config::load`, surfacing immediately rather than at first sync). The compiled-in-place result lives on `Config` as a `#[serde(skip)]` `Vec<CompiledRenameRule>`. `Regex` is `Arc`-shared internally, so cloning compiled rules into the runtime view is cheap.
- `FolderLayoutDefinition::from_config` clones the compiled vec into the runtime layout struct. `resolve_folder_path` then consults the rules via `maybe_match_rename_rules` *before* applying the layout's separator join: if any rule matches the canonical `/`-joined segments, its output is returned verbatim and the per-segment join is skipped. First match wins.
- `MAX_MAILBOX_NAME_LEN` is enforced on the rule's output too -- the cap check sits inside the rename branch in `resolve_folder_path` so a runaway substitution can't escape it. Past the cap, the rule's right-hand side is otherwise passed through as-is by design: a rule producing path separators is the feature (a flat `foo.bar.baz` rewritten to `foo/bar/baz` to expand into a subtree).

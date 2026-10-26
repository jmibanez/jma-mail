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

### Real Stalwart flag gate

`tests/e2e_flag_sync.rs` is ignored by the ordinary test run but explicitly
included in the E2E workflow's locked, serial `e2e_* --ignored` invocation.
The default backend is the existing Docker Stalwart/MySQL snapshot:

```sh
cargo test --locked --test e2e_flag_sync -- --ignored --nocapture --test-threads=1
```

For native Stalwart v0.16 on Linux, use the isolated runner (requires enabled
unprivileged user/network/mount namespaces). A tested Guix invocation is:

```sh
guix shell git gcc-toolchain openssl pkg-config stalwart util-linux iproute2 python -- bash <<'SH'
set -e
export LD_LIBRARY_PATH="$(dirname "$(cc -print-file-name=libgcc_s.so.1)")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
tools/test-stalwart-native.sh
SH
```

The runner defaults to `e2e_flag_sync`. Pass Cargo test-target names or quoted
patterns to select more suites, for example `tools/test-stalwart-native.sh 'e2e_*'`
for all seven Stalwart targets. Every selected binary and its tests run serially.

The runner builds outside the sandbox, then executes the compiled test binaries
with loopback-only networking and a private tmpfs /var. Native fixture startup
refuses an unsafe host environment. `JMA_TEST_NATIVE_STALWART` selects the
executable; it is not a URL or permission to use an existing server. Every
fixture has fresh temporary config/storage, clears inherited Stalwart config
environment, and stops/waits for its child before deleting its directory.

`tests/common/native.rs` bootstraps fresh RocksDB with typed JSON through
`x:Bootstrap/set` **updating** the singleton. That returns generated fixture
admin credentials; restart into the normal account, mint a Bearer via
`x:ApiKey/set`, replace default listeners with high loopback HTTP/IMAP ports,
enable test-only plaintext IMAP authentication, and restart again. Before seeding,
the fixture also creates `x:Search` with the Docker snapshot's `indexEmailFields`
map, including `headers: true`, requiring positive creation ACK and persisted
readback after restart. Bootstrap defaults leave headers unindexed: mailbox pulls
and flag tests can pass while rebind header queries return no matches. Bootstrap
listeners and default log paths are confined to the private namespaces. The
Docker path and its container teardown order remain unchanged. Existing public
fixture fields/seeding helpers still support examples/bench-server.rs.

IMAP APPEND seeds Seen/Flagged/custom-keyword messages independently of jma's
mutation path. Tests run real SyncEngine cycles, fresh independent Email/get
and full-keyword oracle writes (never SDK keyword(false)), actual filenames,
SQLite observations/baselines and later idle checks. Coverage includes local
removals plus a tracked move into new/, server pulls/promotion, both missing-peer
shapes, custom preservation, identity/blob/thread/body stability and real
stateMismatch rejection. This focused F/S gate is separate from the exhaustive
six-bit stateful model; neither proves arbitrary cross-writer linearizability.
Native v0.16.4 execution does not establish the Docker/MySQL backend, a hosted CI
result, or required-branch status.

## Source layout

- `src/main.rs` -- subcommand dispatch and lock acquisition.
- `src/cli.rs` -- clap definitions for subcommands and global flags.
- `src/config.rs` -- TOML config schema, plus the rendering of both the fully-commented `jma init --no-interactive` template and the wizard's filled-in output via a single shared `render_config_toml` helper. `Config::load`'s missing-file error names `jma init` explicitly so users hitting first-run land on the right command.
- `src/wizard.rs` -- interactive setup wizard invoked by `jma init` when no config exists at the target path. Collects email/token/maildir/layout/conflict-strategy via `dialoguer` (`Input`, `Password`, arrow-key `Select`), checks the OS keychain after the email prompt and reuses an existing entry if one is already there (the iCloud / 1Password / Bitwarden sync case for second-machine bootstraps), otherwise stores the new token in the keychain with an in-file fallback if the keychain rejects the write. Writes the canonical template with the answered slots substituted. The wizard is never triggered implicitly -- non-init commands fail with an actionable error when the config is missing.
- `src/auth.rs` -- bearer-token storage in the OS keychain (per-email scoping; platform backends behind `keyring-core`).
- `src/ids.rs` -- newtype wrappers for the various string IDs (see [ID newtypes](#id-newtypes)).
- `src/domain.rs` -- neutral domain types shared across layers (`MailboxObject`, `MailboxFolderBinding`, `MaybeReference`, `MailboxIndex`). Depends only on `ids` + `std`; produced by `jmap`, consumed by `maildir_ops` and `sync`, so it lives below both peers rather than inside either (see [Data model](#data-model)).
- `src/jmap/` -- JMAP client wrapper. `session.rs` opens the connection, `discovery.rs` resolves the session URL via DNS SRV / well-known per RFC 8620 section 2.2, `limits.rs` clamps every server-advertised cap against a hardcoded ceiling (so a hostile or buggy server can't induce DoS via absurd values), `email.rs` is the per-method veneer over `jmap-client`, `mailbox.rs` handles `Mailbox/get`, `retry.rs` classifies errors as transient vs hard, `types.rs` holds the internal `EmailObject` shape.
- `src/maildir_ops/` -- anything that touches the filesystem. `store.rs` wraps message writes, `entries.rs` supplies strict non-creating cur/new inventory, `flags.rs` translates between maildir suffix flags and JMAP keywords, `scan.rs` walks a folder and emits `LocalChange`s, `sync/dedupe.rs` does the Message-ID-based duplicate sweep, `headers.rs` parses `Message-ID` out of a maildir file, `layout.rs` resolves a JMAP mailbox tree onto a single on-disk folder name under the configured `FolderLayout` and applies any matching `[[rename_rules]]`, `lock.rs` takes the per-maildir advisory lock, `drift.rs` classifies maildir-vs-DB drift into the four classes `status` and `janitor prune` consume (see [Maildir/DB drift and prune](#maildirdb-drift-and-janitor-prune)), `removal.rs` owns `remove_maildir_tree` -- the single guarded folder delete (path-escape guard, rescue of unmapped local-only files before deletion, then the state-DB cascade) that every in-crate destroy path routes through.
- `src/state/` -- SQLite. Schema defined inline in `db.rs`; version constant is `db.rs::SCHEMA_VERSION`. Tables:
  - `jmap_state` -- per-entity sync cursor, one row per `(account, entity)`. `("acct", "Email")` ratchets in `execute`; `("acct", "Mailbox")` is stamped at the cycle tail from the `Mailbox/get` state, gated on the cycle's staged `mailbox_map` writes having landed so the cursor never advances past a cache that is still catching up. The Mailbox cursor also gates the fetch: a `Mailbox/changes` delta touching no structural property (empty, or only `totalEmails`/`unreadEmails`/`totalThreads`/`unreadThreads` per RFC 8621 section 2.2) lets `resolve_mailboxes` rebuild the set from `mailbox_map` and skip the full `Mailbox/get`; `janitor prune` clears it when it drops a `mailbox_map` row out of band, since the server-state cursor cannot otherwise see that local mutation. Both seed the daemon's SSE state-change dedup cache so the first event after connect isn't a redundant trigger.
  - `message_map` -- JMAP<->maildir binding, indexed on `maildir_id`, `message_id`, `jmap_mailbox_id`.
  - `mailbox_map` -- server mailbox metadata.
  - `local_state` -- raw filesystem observations for change detection, not proof of successful reconciliation.
  - `message_flag_sync` -- nullable acknowledged six-flag baseline, review/error state, and optional in-flight flag receipt. Access is owned by `state/flag_sync.rs`.
  - `jmap_discovery` -- cached session URL keyed on the account's email domain; populated by `session::connect` after autodiscovery, cleared and rediscovered on a structural connect failure.
  - `folder_checkpoint` -- per-folder `(cur_mtime, new_mtime, cur_count, new_count)` snapshot written at the tail of every successful Full cycle. Phase 0 reads it to skip the dedupe walk for folders whose on-disk state hasn't changed; see [The sync cycle](#the-sync-cycle).

  Access goes through `queries.rs` or the narrowly scoped `flag_sync.rs` history API. Keep SQL in the state layer.
- `src/sync/` -- orchestration (engine, plan, reconcile, execute), the shared flag decision in `flag_resolution.rs`, explicit targeted authority in `flag_repair.rs`, cached selected-history reporting in `status.rs`, and the daemon's self-write cache in `self_writes.rs`. See [Sync internals](#sync-internals) below.
- `src/janitor/` -- maintenance tasks invokable from the engine's Phase 0 and from `jma janitor <task>` CLI subcommands. Today: `dedupe`, `remotededupe`, `rebindfolders`, `prune` (issue #8). Reserved-but-empty: `db_gc`. Each task is a thin orchestration over the underlying primitive (e.g. `janitor::dedupe::run` wraps `sync::dedupe::plan_dedupe` + `apply_dedupe` with the empty-folder-list short-circuit); `prune` is offline drift remediation built on `maildir_ops::drift` + `removal` (see [Maildir/DB drift and prune](#maildirdb-drift-and-janitor-prune)).
- `src/profile.rs` -- per-run profiling layer. Aggregates `tracing` spans and events from the sync engine and maildir ops into a phase-by-phase summary (timings, blob throughput, file-op counts, RSS deltas). Output is opt-in via `--profile` (stderr table) and `--profile-json <PATH>` (NDJSON per cycle in daemon mode).
- `src/daemon/` -- `watch` mode. `runner.rs` owns the FS watcher and long-lived engine, performs the initial full sync, then runs SSE sessions. `eventsource.rs` listens on the JMAP `eventSourceUrl`; `watcher.rs` debounces filesystem notifications and classifies batches via `classify.rs` before emitting. Both feed a `tokio::sync::mpsc` channel of `SyncTrigger`s. `retry.rs` owns the independent retained-work deadline and permanent-rejection mask across sessions. The trigger loop dispatches path-driven local cycles, normal full cycles, or full retry cycles. See [Trigger pipeline](#trigger-pipeline). `hook.rs` runs `post_arrival_command` after cycles that downloaded mail, coalescing overlapping triggers.

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

- `Message-ID` -- the client-supplied RFC 5322 header used for best-effort cross-side correlation. Parsed out of the maildir file by `maildir_ops::headers`; reported by JMAP as `EmailObject.message_id` (a `Vec<MessageId>`). It may be absent, constant across unrelated messages, or reused across content-distinct draft versions. NOT unique in `message_map`: multiple files/server objects can carry the same header. It can help rebind existing files after a DB wipe, but is not proof of byte identity, a unique match, or destructive authority.

### ID newtypes

Each identifier above has a corresponding newtype in `src/ids.rs`: `JmapEmailId`, `JmapBlobId`, `JmapThreadId`, `JmapMailboxId`, `JmapAccountId`, `MaildirId`, `MessageId`. They are all `String`-backed and generated by a single `define_id_newtype!` macro so the trait surface stays uniform: `From<String>`/`From<&str>`, `AsRef<str>`, `Borrow<str>` (so `HashMap<NewType, V>::get(&str)` works without allocating), `Display`, rusqlite `ToSql`/`FromSql`, and serde `transparent`. None of them implement `Default` or `PartialEq<&str>`, on purpose -- both would silently re-admit the bare-string slop the newtypes are meant to prevent.

The point is that the compiler now distinguishes a server `Email/id` from a folder name from a maildir basename, even though all three would happily fit in a `String`. Every internal slot that used to be `String` -- `MessageRecord` columns, `SyncAction` payload fields, `DetectedMove`, `ReconcileCtx` index keys and per-cycle tracking sets, the engine/daemon `account_id` plumbing -- is typed at its newtype.

There are exactly two boundaries that still take/return bare strings, and conversion happens *at those boundaries*:

- **jmap-client** -- the upstream crate's APIs take `Into<String>`, return `&str`, and serialize JSON keys as plain strings. We cross with `String::from(&id)` / `id.as_ref()` at the actual call site -- e.g. `set_email_batch` newtypes its `EmailSetOp::SetMailboxes.target_mailbox_ids` as `Vec<JmapMailboxId>` and only crosses to bare strings at the `set.update(...).mailbox_ids(target_mailbox_ids.iter())` invocation.
- **rusqlite + raw SQL** -- `queries::{get,set}_jmap_state` keeps `account_id: &str`. The newtype's `ToSql`/`FromSql` impls cover *column* values, but param boundaries against literal SQL stay string-typed because they're not bound to any one ID kind. We cross with `id.as_ref()`.

Stay typed everywhere else. If you find yourself writing `String::from(...)` mid-pipeline, the right fix is almost always to change the slot's type, not to add another conversion.

## Data model

The sync pipeline moves a small set of internal types between phases. Each one lives in a specific module, has a specific role, and gets translated into another type at module boundaries. Knowing the cast helps when reading any handler.

**Domain types** (`src/domain.rs`). Neutral across layers: `jmap` produces them at the wire boundary, `maildir_ops` consumes them to lay folders out on disk, and `sync` threads them through reconcile and execute. Nothing about their shape is JMAP-wire-specific (contrast `EmailObject` below), so they live in a leaf module that depends only on `ids` -- keeping the peer layers off each other.

- `MailboxObject` -- mailbox metadata from `Mailbox/get`. Consumed by `resolve_mailboxes` and immediately translated into a `MailboxRecord` for the DB.
- `MailboxFolderBinding` -- the in-cycle pairing of a JMAP mailbox id with its on-disk `maildir_folder`, the server-side `server_name`, and a computed `remote_path` (slash-joined parent chain, e.g. `Personal/Archive`). `resolve_mailboxes` constructs these in topological order so each child's `remote_path` is `parent.remote_path + '/' + node.server_name`; the value is stamped on the binding and persisted on `mailbox_map.remote_path` for between-cycle reads. JMAP scopes mailbox uniqueness to `(parent_id, name)` per RFC 8621 section 2, so the bare `server_name` is ambiguous across the hierarchy while this path is not.
- `MaybeReference<T>` -- the type of `MailboxFolderBinding.jmap_mailbox_id`: either a resolved `Value` or a symbolic `Reference` an executor-side resolution step swaps in once an earlier create action returns. Today no producer constructs a `Reference`, so every binding's id is a resolved `Value`.
- `MailboxIndex` -- the O(1) two-way lookup (`jmap_mailbox_id` <-> binding and `maildir_folder` <-> binding) over a set of `MailboxFolderBinding`s, with `insert` the sole mutator so the two maps cannot drift. `sync::MailboxBindings` composes one as its `index` field and delegates its lookup readers (`by_id`, `by_folder`, `iter`, `ids`, `folders`, `len`, `is_empty`) to it; the per-cycle resolution bookkeeping `MailboxBindings` adds (new mailboxes, renames, orphans, staged cache writes) rides alongside the index rather than inside it. `maildir_ops::scan` (`scan_paths`, `discover_unbound_folders`) takes a `&MailboxIndex` directly -- it only needs `by_folder` -- so the filesystem layer resolves folders to mailbox ids without depending on `sync`; the engine hands it `MailboxBindings::index()`.

**JMAP-side types** (`src/jmap/types.rs`). Hand-rolled rather than re-exported from `jmap-client` so the rest of the codebase doesn't depend on the upstream crate's shape.

- `EmailObject` -- `{ id, blob_id, thread_id, mailbox_ids: HashMap<JmapMailboxId, bool>, keywords: HashMap<String, bool>, message_id: Option<Vec<MessageId>>, subject, received_at: Option<i64>, size }`. Produced by `Email/get` and `Email/changes`-then-`Email/get`; consumed by `reconcile`. The `mailbox_ids` map's bool is always `true` (JMAP's convention for set membership); we keep the type as-is to match the wire format.
- `ChangesResponse` -- `{ old_state, new_state, created, updated, destroyed, has_more_changes }`. The shape `Email/changes` returns; loop control for `fetch_remote_state`.
- `SessionInfo` -- session URLs and account id, captured at connect time.

**State-side types** (`src/state/queries.rs`). One struct per row-shape; every read or write goes through these.

- `MessageRecord` -- one `message_map` row. Carries everything needed to act on an email without re-querying: server identifiers (`jmap_email_id`, `jmap_blob_id`, `jmap_thread_id`, `jmap_mailbox_id`), the local `maildir_id`, the cross-cutting `message_id`, and a server-observation pair (`flags` for its mapped six-flag projection, `jmap_keywords` for the full JSON-serialised keyword set). The raw filename suffix, including unowned letters, is observed separately in `local_state`; acknowledgement is separate again in `message_flag_sync`. The on-disk folder name is *not* stored on the row -- it's derived by looking `jmap_mailbox_id` up against `mailbox_map`'s `maildir_folder` column (or against the in-memory `MailboxBindings` table during a sync cycle), so the binding lives in exactly one place. `message_id` is non-`Option` (and `NOT NULL` in the DB schema): scan and reconcile both refuse to write a row without one. The `maildir_id`, `jmap_blob_id`, and `jmap_thread_id` fields remain `Option` because a row can be JMAP-known but not yet bound to an on-disk file during in-flight phases (and the blob/thread ids aren't always carried through every code path that constructs a `MessageRecord`).
- `MailboxRecord` -- one `mailbox_map` row. Mostly a snapshot of `MailboxObject` plus the resolved `maildir_folder` (with the `INBOX` magic alias applied) and a cached `remote_path: Option<String>` -- the slash-joined server-name path (e.g. `Personal/Archive`) that disambiguates same-named mailboxes at different points in the hierarchy. JMAP scopes its mailbox uniqueness to `(parent_id, name)` per RFC 8621 section 2, so the bare `name` column is ambiguous while this path is not. The column is nullable; older DBs upgraded via the runtime additive `ALTER` read NULL on existing rows until the next `resolve_mailboxes` upsert stamps a value. Reads tolerate the gap; writes always supply one.

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
- `SyncPlan` -- `{ actions: Vec<SyncAction>, new_email_state }`. The plan plus the JMAP cursor to commit on success.
- `SyncDirection` -- `Both | PullOnly | PushOnly`. Top-level mode set by the CLI subcommand.
- `ActionDirection` -- `Pull | Push | Both`. Per-action classification used by `into_filtered`. The asymmetry (`Both` exists for actions but not modes) is on purpose: adoption and observation/history metadata are `Both` and survive every direction filter. They must not acknowledge a filtered-out mutation.

**Reconcile context** (`src/sync/reconcile.rs`). `ReconcileCtx<'a>` is a private struct that bundles the immutable inputs every helper in `reconcile` needs (the three `known_by_*` indices, the `LocalIndex`, the `mailboxes` slice, the conflict strategy, plus a few derived sets). Threaded by `&` through helpers so each one stays short on parameters.

## Idempotency model

Read this before touching anything in `src/sync/` or `src/maildir_ops/`.

Two distinct identifiers anchor the system:

- **JMAP `email_id`** -- stable per server, lives in `message_map`.
- **RFC 5322 `Message-ID`** -- an unreliable client-supplied correlation hint that may survive a state DB wipe; not a unique identity or content digest.

The state DB is **rebuildable, not a lossless journal**. Recreating it starts a new reconciliation from the current maildir and server state. Message-ID-based adoption is intended to reuse existing files rather than download another copy; it cannot reconstruct the history of changes made before the reset. The current recovery mechanisms are:

1. `sync::dedupe::plan_dedupe` groups files by Message-ID, then partitions
   each group by complete content. Size/SHA-256 are prefilters followed by a
   byte-for-byte comparison; the oldest mtime survives within each identical
   class. Content-distinct revisions remain on disk and each contributes a
   KeptEntry. Cross-folder copies stay separate, and extending-prefix cur/new
   promotion pairs retain their existing exemption. The plan is non-mutating.
   `janitor::dedupe::run` additionally vetoes deletion of any ID in
   `message_map` or `local_state`, preserving live synchronization anchors even
   when byte-identical. Only remaining deletions run on apply; previews do not
   delete files.

2. Reconcile consults bindings and LocalIndex before downloading. The index
   is built from dedupe-kept entries even when message_map is nonempty, so
   partially committed bootstraps can still discover their unbound files.
   Checkpoints narrow the walk; it is not a permanent whole-tree index. One
   oldest local content representative per Message-ID/folder may upload;
   siblings remain locally for review rather than competing for Fastmail's
   one-import-per-Message-ID rule. Message-ID-based adoption reuses supported,
   unambiguous matches; it is not proof of byte identity or lost-history recovery.

3. **Both ingest boundaries still refuse Message-ID-less messages.** This is jma's current identity-policy limitation, not a transport requirement: [RFC 5322 section 3.6.4](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.6.4) recommends the header with SHOULD, not MUST. Local files without a parseable `Message-ID` header are skipped with `error!` by `maildir_ops::scan::scan_folder` and `sync::dedupe::plan_dedupe`, leaving the bytes untouched. Remote emails whose `Email/get` response carries no `messageId` and aren't already bound by JMAP id are skipped at `sync::reconcile::process_remote_emails`; they stay on the server, but the cycle may advance its cursor, so this is not queued headerless ingestion. The known-JMAP-id path still applies flag/move updates -- the existing `message_map` row is itself the anchor, so a server that strips Message-ID from updates doesn't break flag sync. No synthetic header or headerless identity is introduced.

Don't add a code path that writes to a maildir outside `Executor::execute` (or `apply_dedupe`'s deletion of duplicates). Every other writer would skip the Message-ID check.

### PR #21 integration and live-binding authority

Content-safe dedupe preserves more copies; that does not authorize adopting an
untracked copy over a still-live cached Maildir ID or treating its suffix as a
new edit of the bound message. Same-folder adoption first prefers an exact
Maildir-ID binding, preserves extra copies when an anchor is live, and defers
multiple unbound candidates when the old anchor is missing. Remote dedupe
likewise refuses a group with multiple locally bound Email IDs: uniqueness of
Maildir IDs does not imply uniqueness of Message-ID. These cases preserve
both files/objects and their flag histories instead of guessing or panicking.

Path cycles preflight new/relocated identities and disappeared known identities
before Phase 0. They widen to Full so dedupe and LocalIndex participate as well
as the scan. Ordinary known-ID flag renames remain path-scoped; unknown new-side
files remain observation-only. Missing unknown paths and excluded paths do not widen merely
because their filenames are unfamiliar. The older post-scan deletion
widening remains a conservative race-window fallback.

Permanent per-message import rejections alreadyExists/invalidEmail are detected
through the anyhow source chain, not outer Display text, and preserve the
local file without recording an upload. Account/transport/protocol failures
still propagate. Skipped files may be reassessed on later Full cycles; this is
not a durable body-payload retry or suppression queue.

Remote-dedupe audit options retain member subject/receivedAt, size, SHA-256 and
an optional bounded terminal-safe content prefix. Reporting never promotes a
refused group into deletion eligibility. Email/get requests subject and
receivedAt (optional Unix seconds) for that report; they are observations, not
flag acknowledgement history. `janitor destroyids` accepts an externally
reviewed, account-wide exact ID manifest, refuses the entire batch if any ID
has a message_map row, verifies fresh presence and requires --yes to destroy.
Explicit maintenance uses its existing unconditioned Email/set path; normal
reconciliation still requires its planning-state guard.

### Schema versioning: nuke-and-resync as the migration story

There is no in-place migration system. Whenever the SQLite schema or the invariants the code expects of existing rows change in a way the previous binary's writes would violate (e.g. tightening a column from `NULL`-able to `NOT NULL`, adding a uniqueness constraint, changing what a string-encoded field represents), bump `state::db::SCHEMA_VERSION`. The version is stored as SQLite's `PRAGMA user_version`.

On open, the binary compares the on-disk version against `SCHEMA_VERSION`:

- **Match** -- proceed.
- **Mismatch** under a mutating command (`sync`, `pull`, `push`, `watch`) -- `open_or_recreate` logs a warning and unlinks `state.db` plus its `-wal` and `-shm` siblings, then recreates an empty schema. There is no confirmation prompt or automatic backup. The caller is required to hold the state DB lock, preventing another cooperating jma from using the DB during the unlink. This lock prevents that race; it does not make discarding synchronization history lossless.
- **Mismatch** under a read-only command (`status`, `mailboxes`, `auth rediscover`) -- `open` refuses with backup/rebuild guidance. Read-only paths don't hold the state DB lock and cannot safely unlink it.
- **Maintenance and targeted repair** -- `open_preserving_history` refuses a mismatch even while holding the locks. Compatible additive migrations run, but binding/history rows are never discarded before dedupe, reviewed-destroy or repair preflight. This includes previews. Ordinary sync/pull/push/watch rebuild policy is unchanged.

Both directions of mismatch (older binary, newer DB; or newer binary, older DB) take the same auto-nuke path for ordinary sync mutators. Disposability cuts both ways. A pre-versioning DB (`user_version = 0` with populated tables) is treated as stale.

When you bump `SCHEMA_VERSION`, the current implementation rebuilds rather than migrates. Mention the field/invariant change and the recovery consequences in the commit message, and test the old-schema open path. Do not infer that every future state addition can safely be discarded merely because the current open path does so.

### What a DB rebuild can recover

The unlink itself removes only the DB and its WAL/SHM files, not maildir messages. The subsequent sync is a normal mutating sync: it can adopt, download, upload, deduplicate, change flags, or propagate deletions according to the inputs and configured policy. It is not a read-only reconstruction operation.

Current data does not uniquely determine the lost history. For example, local flags `S` and remote flags `FS` can mean either that both sides previously had `FS` and the user unflagged locally, or that both previously had `S` and another client flagged remotely. Without the previous synchronized snapshots, those histories are indistinguishable. A fresh adoption may use the configured conflict strategy, but cannot promise the same result as finishing the interrupted reconciliation.

Rebuilding therefore does not promise to preserve unacknowledged flag intent, replay an interrupted operation, restore a destroyed JMAP Email id, or recover server-only metadata from a local filename. Adoption also requires a usable Message-ID and an unambiguous supported match; Message-ID alone is not proof of byte identity.

Before a reset or an upgrade/downgrade that may trigger one, stop concurrent mutators and back up the DB with a consistent SQLite backup (or copy it only after closing all connections, including any WAL state). Preserve the maildir as well. Treat a rebuild as a fresh reconciliation with explicit policy, not as transparent crash recovery. Normal restart recovery with the DB intact is a separate guarantee and must be tested separately.

### Flag observations, acknowledgement, and restart recovery

`state/flag_sync.rs` separates three things that old flag synchronization
conflated:

- `local_state.flags`: last observed raw filename suffix.
- `message_map.flags` / `jmap_keywords`: last observed server projection
  and full keyword set, including unowned keywords.
- `message_flag_sync.baseline`: last acknowledged common mapped flag set;
  NULL means initial reconciliation is unfinished, not an empty flag set.

Cold adoption records observations and a NULL baseline when they disagree.
It does not acknowledge a rejected push, failed local rename, or operation
excluded by a direction filter. Observed agreement can initialize a baseline.
The shared bound-message pass uses actual scan observations and the baseline
on later cycles, even when neither side emitted a new delta. One-sided
changes propagate; competing changes use the configured whole-flag-set
policy, not a union. Local suffix extensions survive owned-bit replacement.

Before flag I/O, the executor commits a small receipt containing the captured
local suffix/identity/location and mapped target. Successful remote keyword
acknowledgement or local rename commits that processed target with the
corresponding mirrors, not any newer MUA edit. A failed acknowledgement DB
transaction leaves the receipt intact. A fresh remote target can establish
completion of an older push (source projection equals target), allowing a
newer local edit to propagate. It does **not** prove completion of a pull:
its required local rename may have failed. Confirmed per-ID rejections clear
the receipt but retain the previous/NULL baseline; missing acknowledgements
and transport failures retain it. Failure classification is committed
separately from successful-ID mirrors, so one ID's ACK rollback cannot
turn another ID's confirmed rejection into an ambiguous receipt. Typed
permanent rejections are marked blocked, distinct from retryable rejections
and ambiguous outcomes.

An unacknowledged pull can also have renamed successfully before its ACK
transaction failed. If the filename now matches that old pull target but fresh
remote flags differ, the old suffix must not be pushed back as an invented MUA
edit. When normal resolution would do that (including a server return to the
old baseline), the shared resolver retains the baseline/receipt and marks
explicit review. Current values cannot distinguish our rename from a
coincident local edit, so `repair-flags` supplies the missing authority. This
is not virtual pull acknowledgement: safe fresh pulls and distinct newer local
mapped edits still proceed normally, and fresh agreement can still initialize
a common baseline. Preparing a replacement pull from an older unacknowledged
pull target marks review before overwriting its receipt; positive ACK clears
it. If that replacement also fails or is interrupted, review preserves the
older own-effect ambiguity instead of permitting a later repush when the
server returns to the baseline. Review keeps its explanatory error instead of
being relabeled as a legacy-only failure on the next cycle. Stateful SAME-DB tests in
`flag_tests/pull_ack.rs` cover both unsafe pushes and automatic safe-pull
recovery, followed by independent fresh verification and later idle cycles.

`store::set_flags_if_unchanged` requires a unique live path and the planner's
exact raw suffix, refuses existing targets, and syncs renamed directories on
Unix. A stale plan cannot deliberately find the latest suffix and overwrite
it. Non-Seen changes keep a new-side file in `new/`; adding Seen promotes it;
clearing Seen in `cur/` does not make it new. This is snapshot validation,
not global linearizability against non-cooperating filesystem writers.

The table is additive at schema version 2: both opening APIs initialize it
without rebuilding existing v2 bindings/cursors. Missing history rows are
legacy/unverified. Fresh Email/get snapshots are kept separate from actual
Email/changes events so verification does not fabricate delete-vs-update
conflicts. Confirmed agreement initializes history; a still-confirmed old
common observation can identify a local-only edit. Other legacy disagreement
is deferred for explicit review, not automatically overwritten. Do not mix
old and new mutating binaries against one DB: older binaries do not maintain
this history. Reset/rebuild still discards it and invokes bootstrap policy;
this is not a lossless migration or a general durable operation queue.

`pending_flag_actions`, `blocked_flag_actions` and `unverified_flag_actions`
keep unfinished work from being reported as already synchronized, including
path no-ops and previews. `flag_sync::summarize` and `load_for_mailboxes` are
scoped to resolved selected bindings (plus managed local orphans), not the
whole DB. Excluded histories cannot force full scans, pollute counts, or
break a selected cycle by containing an invalid receipt. Queries normalize
unowned suffix letters and retain known errors even when old cached views
agree. Missing history is unverified, not a settled or empty baseline.

Retryable pending/legacy work widens Paths to Full. `SyncOutcome` separates
retryable remote/local effects and deferred observations from permanent
remote rejections and explicit review deferrals. An effect withheld by a
membership or direction gate is not an attempted local failure. The engine
computes scoped history status; the executor no longer counts the entire DB.
Console and daemon/TUI reporting show unfinished/blocked work rather than
claiming idle. `sync/status.rs` supplies SELECT-only cached history queries
for `jma status`; configured selection uses cached remote paths and warns
about unclassifiable bound mailboxes. It does not verify remote state or
acknowledge histories. Existing DB-opening/schema initialization is unchanged.

Full/manual cycles can reassess retained discrepancies. Watch mode also has
an event-independent retry deadline (see below). Email-state preconditions
and fresh generation replanning are described below; targeted legacy
authority repair uses the separate flags-only path below. Neither a timer nor
a conditional write recovers discarded DB history.
Unresolved flag histories hold destructive local-orphan handling, while mere
metadata observations are not user-change blockers. Moves and adoption use
the same baseline decision as ordinary updates. A move uses the observed
destination suffix and full fresh remote keywords (or the verified cache),
not a filename-derived replacement for server metadata. Ordinary adoption
also prefers fresh full metadata over its cached record when available.

Move-paired destination binding and local flag effects require an actually
submitted, positively acknowledged membership update, including under direction
filters. Placement and full observations commit first; a required destination
flag rename acknowledges flags only after it succeeds. Failure retains the
old baseline and a destination snapshot receipt so a reopened DB can finish
without a fresh edit or an old-source rename. Push receipts also capture the
moved destination, not the stale binding. Failed or excluded placement holds
the Email cursor so replay retains its fresh metadata. Flag-only receipts do
not acknowledge mailbox placement. Truly competing selected mailbox
destinations are deferred for review rather than arbitrarily counter-moved.
When folder policy replaces a move with a counter-move, its destination flag
pull is deferred, not allowed into the ordinary pre-membership flag phase.
Every counter holds the Email cursor and requests a fresh full replan, even
when mapped flags agree: the discarded adoption may contain custom-only
metadata which must not disappear behind an advanced cursor.

Counter-moves take the actual destination BoundId from the unique paired
adoption, never the cached old basename. `store::move_message` requires one
live source across cur/new, refuses target identity collisions and returns the
raw moved suffix. The executor atomically rebinds that Maildir ID, removes the
old local observation and stores the actual suffix, preserving server keyword
observations and the flag baseline separately. It retains the existing move
to-cur convention, including for a new-side source. Failed counters withhold
dependent folder deletion and keep the cycle retryable; missing/ambiguous
counter destinations hold the orphan for review rather than guessing.

Deletion presence inventory now retains candidate folders as well as IDs.
`engine::DeferredLocalDeletes` feeds fresh per-cycle `held_local_folders` into
the planner independently of the adoption-only LocalIndex (which is usually
empty in steady state). An unresolved changed-ID new-side candidate therefore
vetoes both Email destruction and removal of the folder containing it across
idle cycles/reopen. This presence veto does not authorize adopting or importing
unknown new mail. Ordinary promotion to cur can make it a supported move pair;
unresolved copies remain visibly deferred. These are not durable payload jobs
or a content-digest identity proof. Regression cases are in
`flag_tests/maildir_cases.rs`, with collision/duplicate controls in store.rs.

### Explicit targeted flag repair

`sync/flag_repair.rs::inspect` powers `jma repair-flags --authority
local|server EMAIL_ID...`, with preview by default and explicit `--apply`.
It never calls `SyncEngine::run`: no dedupe, body transfer, imports, destroys,
mailbox resolution writes or placement repair. It captures Email state before
fresh metadata gets, verifies live configured selection, unique cached-folder
identity, exact file path/raw suffix and Message-ID, blob/thread and sentinel
anchors. Missing/duplicate/moved/multiple-mailbox/partial-Maildir targets are
refused. The conservative inventory reads bounded headers across cached
folders, including excluded ones to detect ambiguity; it is not a body digest.

Inspection performs SELECTs only. Its private, consuming `FlagRepair` preview
revalidates server/account, selection, bindings, histories, files (including
inode/device on Unix) and Email state before writes. Apply constructs only
ObserveMessage, RecordFlagState, UpdateLocalFlags and UpdateRemoteKeywords
for the explicit targets. It passes the captured guard to the executor but
leaves `new_email_state=None`: targeted inspection cannot advance the global
Email cursor. Keyword patches preserve fresh full custom metadata; local
renames preserve unowned suffix letters. The shared local flag handler no
longer calls ensure_maildir; a vanished source fails without creating folders.

Explicit authority does not infer history from a reviewed candidate baseline.
Before disagreeing effects, apply retains the old/NULL baseline and sets review.
Receipt preparation/failure preserves review; only successful rename/positive
conditional ACK (or later verified agreement) clears it. Thus an interrupted
repair cannot be undone by ordinary sync under the opposite global conflict
policy. Re-preview and explicit rerun are required for remaining disagreement;
there is no new durable authority queue and no all-target transaction. Existing
receipt ambiguity and partial-chunk acknowledgement rules still apply. CLI
incomplete apply returns nonzero; it does not automatically replay stale input.

The command takes maildir then DB locks for preview and apply and requires an
existing DB, using `db::open_preserving_history`, never `open_or_recreate`.
Compatible additive migrations and session discovery-cache writes still apply; preview's
SELECT-only guarantee concerns repair observations/history, not those existing
open/connect behaviors or the lock files. It does not fix folder identity or
recreate missing directories. A DB rebuild still discards flag history.
Stateful regressions live in `flag_tests/repair.rs`; `tests/flag_repair.rs`
executes the real CLI against a strict stateful metadata/keyword-only peer.

### Email snapshot guards and generation replanning

A reconciliation Email/set is conditioned on the **Email object state for the
same account**, captured before its remote observations. Incremental cycles
use the final Email/changes state before Email/get. Initial enumeration now
captures `get_current_state` **before** query/get, not after: a later cookie
could both authorize stale metadata and skip an intervening change on the
next delta cycle. Query-state and session-state strings are not substitutes.

The guard is passed separately to `Executor::execute(plan, planning_state)`.
`SyncPlan.new_email_state` is a cursor target and may intentionally be None
for unfinished placement or suppressed work; it must not silently disable
conditional writes. Actual Email/set without a non-empty planning state is
refused before its flag receipts or network call. Pure-local/mailbox-only plans and
execution-time-vetoed destroys need no Email guard. Explicit maintenance and
independent oracle writes retain the unconditioned `set_email_batch` helper;
reconciliation uses `set_email_batch_if_in_state`.

Own Email/import state edges advance the guard only if they form a verified
continuous chain from the planning state. An unrelated/unreachable edge or
missing chain half cannot authorize the old plan with a newer token. Each
Email/set chunk carries `ifInState`; a following chunk uses newState only
when oldState matches its guard and both response states are present. Missing
continuation state stops the batch for fresh reconciliation, never an
unguarded fallback. This follows RFC 8620 sections 5.3 and 7.1.1; it is
optimistic concurrency, not a cross-entity/filesystem transaction or a claim
of global linearizability against arbitrary writers.

`stateMismatch` is a typed method-level rejection, not a transport-retry
condition. Earlier positive chunk acknowledgements are preserved and mirrored;
current rejected and never-submitted IDs are not mirrored. A clean rejection
or unsent operation clears its flag receipt but retains the old/NULL baseline.
If an earlier HTTP/parse attempt could have applied, a later mismatch does
**not** erase that ambiguous receipt. Typed method errors other than
`serverPartialFail` guarantee no visible effects under RFC 8620 section 3.6.2;
clean serverFail retries cannot become evidence of an older own write. A
terminal clean method rejection preserves earlier ACKs, parks the cursor,
and postpones dependent destructive work until fresh policy is available.
It uses normal retryable/permanent classification rather than inventing
ambiguity or immediately retrying a rejected per-ID operation.

`SyncOutcome.replan_required` keeps the cursor parked and reports remaining
generation work. Queued downstream mailbox destruction is postponed; only
positively acknowledged move pairs may finish their placement/flag steps.
For same-ID resurrection of a locally deleted mailbox, the planner excludes
old bindings that CreateLocalMailbox will clear, so ghost paths cannot
suppress the required downloads. Different-ID local-orphan history checks
remain intact.

`SyncEngine::run` permits three generation attempts, rebuilding a full plan
from fresh files, observations and retained baselines. Direction filters stay
unchanged. Successfully mirrored effects are not replayed; completed counts
are accumulated while pending/status comes from the final attempt, and the
final console summary uses those totals. Explicit per-ID failures keep their
normal backoff/permission handling rather than being immediately retried by
the generation loop. Exhaustion remains visibly incomplete and retryable by
the daemon or another Full cycle. Tests in `sync/flag_tests/generation.rs`
exercise these races against the stateful protocol model and real files/DB;
real Stalwart remains a separate validation gate.

## Concurrency model

Two distinct surfaces need protection from concurrent mutation, plus write-coherence inside the cycle. They get different mechanisms because they have different threat models.

### Maildir mutual exclusion

`maildir_ops::lock::acquire_lock` takes a `flock`-backed advisory lock on `<maildir_root>/.jma.lock`. Mutating jma invocations (`sync`, `pull`, `push`, `watch`, `repair-flags`, including repair preview) against the same maildir can't both run; the second fails with the first's PID in the error message. Read-only commands (`status`, `mailboxes`) and maildir-less commands (`init`, `auth`) skip this.

The lock keys on the maildir root, not the state DB, because the maildir is the shared mutation surface across processes. Two configs pointing at *different* state DBs but the *same* maildir would otherwise race -- both could write the same Message-ID under different filenames and clobber each other. Per-account-Maildir-root is the universal convention across mbsync, OfflineIMAP, getmail, etc., so this serializes exactly what needs serializing without artificially preventing legitimate multi-account setups (different accounts -> different roots -> different locks).

### State DB lock (the unlink gate)

`state::db::acquire_lock` takes a separate `flock` on `<db_path>.lock`. This lock specifically gates `open_or_recreate`'s schema-mismatch unlink path: without it, a process that decides the on-disk schema is stale would `unlink` the DB out from under any concurrent process that opened the same DB but doesn't realise it's about to be deleted (writes vanish into the orphaned inode; a later checkpoint can corrupt or panic).

The maildir lock alone doesn't cover this, because two processes can hold *different* maildir locks while sharing a state DB -- e.g. two configs whose maildir roots differ but whose explicit `[state].db_path` points at the same DB.

Note that with the maildir-relative default for `[state].db_path`, the state DB lives at `<maildir_root>/.jma.db` and the maildir lock structurally covers what the DB lock guards -- any process that could open this DB must already hold the maildir lock by construction. The DB lock is only load-bearing when `[state].db_path` is set explicitly to a path outside the maildir, where two configs can share a DB while owning different maildirs. Today both locks are taken unconditionally regardless of topology; gating the DB lock on config shape is a candidate simplification but not a current one.

**Lock-acquisition order: maildir lock first, then state DB lock**, consistently across every mutating call site (see `acquire_mutator_locks` in `src/main.rs`). Consistent order is what prevents deadlock between two contending pairs. Both locks are held for process lifetime; the kernel releases them when the fds close at process exit.

### State DB write coherence

The locks above protect against destruction; per-batch transactions plus SQLite's WAL writer-lock protect against interleaved writes:

1. **Intra-cycle write atomicity** -- `src/sync/execute.rs` wraps each mutation phase in a SQLite transaction via `Connection::unchecked_transaction()`. Pure-DB phases (`adopt_messages`, `apply_move_pair_adopts`, the post-network section of `apply_remote_set`) take one transaction per phase, so a panic mid-loop rolls the whole phase back. FS-mutating phases (`update_local_flags`, `move_local_messages`, `delete_local_messages`, `upload_messages`, `download_messages`) take one transaction per iteration around the paired DB writes that follow each successful FS op, so a row's `message_map` and `local_state` never disagree even if the second DB write fails. Side benefit: SQLite's WAL writer-lock serializes any other writer on the file from the first write through commit.

2. **External writers** -- a deliberate bypass (e.g. `sqlite3 state.db "UPDATE ..."` typed in error, or any tool that opens the DB without going through the state DB lock) is not prevented. Per-batch transactions block such writers from interleaving *within* a phase, but they can still interleave between phases. The recovery story is "next sync cycle re-reconciles from server state"; the design accepts this rather than holding cycle-spanning transactions across network I/O (which would balloon the WAL during long initial syncs and lose Ctrl-C-mid-cycle partial-progress recovery).

## JMAP boundary quirks

The `jmap-client` crate is convenient but a few server behaviors need explicit handling:

- **`Email/changes` since `"0"` is not a bootstrap.** Fastmail (and the spec) allows the server to refuse with `cannotCalculateChanges`. Use `jmap::email::get_current_state` (`Email/get` with empty ids) to read the current state after an initial pull instead. Detect this error via `jmap::email::is_cannot_calculate_changes`, which walks the anyhow chain and downcasts to the typed `jmap_client::Error::Method(MethodErrorType::CannotCalculateChanges)` -- never substring-match Display strings, since a `.context(...)` wrapper at any layer breaks that. Recovery: write the empty string to `jmap_state`, which `queries::get_jmap_state` filters back to `None`, which routes to the initial-pull branch on the next cycle.
- **Transient errors are matched on typed `jmap_client::Error` variants, not Display strings.** `jmap::retry::is_transient_jmap_error` dispatches on the typed variant: `Transport` via reqwest's `is_timeout`/`is_connect` (plus a dead-connection io-kind walk for resume-from-sleep / NIC-reset cases), `Problem` via `ProblemDetails::status()` (typed `Option<u32>`), `Method` via `MethodErrorType::{ServerUnavailable, ServerFail, ServerPartialFail}`, `Set` via `SetErrorType::{RateLimit, OverQuota}`. Note the asymmetry: rate-limit / quota errors can surface as an HTTP-429 `ProblemDetails` (request-level, the RFC 8620 `urn:ietf:params:jmap:error:limit` shape carries one) or as `SetErrorType::{RateLimit, OverQuota}` (set-level), never as a `MethodErrorType` variant -- don't go looking for one. The `Problem` arm dispatches purely on status (429 / 5xx -> transient), not on the JMAP type URI: the same `limit` type covers both per-request-size 400s (hard) and rate-limit 429s (transient), and the status correctly partitions them. The `is_transient_substring` fallback is reached only when the anyhow chain doesn't carry a typed `jmap_client::Error` -- in practice, test fixtures synthesized via `anyhow!`. Extend the typed branches in new code, not the substring list.
- **`Server(String)` is the defensive arm, observation-free.** `jmap-client` 0.4.1 builds this variant at `client.rs:426` whenever a non-2xx response's `Content-Type` isn't `application/problem+json`, discarding the typed `StatusCode` in the process. `is_transient_server_payload` parses the leading token of `http::StatusCode`'s Display to recover the code. Documented Fastmail errors come back as problem+json -> `Problem`, so production traffic against Fastmail shouldn't reach this arm; it exists because the construction site in `jmap-client` exists, to catch the case where an intermediary (CDN, load balancer) returns a naked 5xx without a problem+json body during a regional outage.
- **Maildir stores bare LF; `Email/import` rejects bare newlines.** `jmap::email::import_email` calls `normalize_crlf` before handing bytes to `email_import`. Don't normalize at maildir read time -- keep on-disk format native so other MUAs work.
- **`mailbox_id(id, bool)` patches can't remove memberships.** `jmap-client` 0.4.1 types its `mailboxIds` patch map as `bool`, so the only values it can emit are `true` and `false`. Per RFC 8621 section 4.1.1 `mailboxIds` is `Id[Boolean]` whose values are always `true`, and per RFC 8620 section 5.3 a key is removed by patching its value to `null` -- which the typed-`bool` map cannot serialize. Strict servers (Fastmail) reject the `false`-valued patch as `notUpdated`. For moves, use the full-replacement `mailboxIds(...)` setter instead, which sends the entire target set in one go.
- **Session URL resolution disables redirects.** `session::connect` resolves the URL via explicit `[account].session_url` override, then the `jmap_discovery` cache, then RFC 8620 section 2.2 autodiscovery (DNS SRV `_jmap._tcp.<domain>`, fallback `https://<domain>/.well-known/jmap`). The well-known probe runs with `redirect::Policy::none()`: per RFC 8620 section 2.1 the canonical session resource requires an authenticated GET, and following a 30x would land us there unauthenticated and get a 401. On a structural connect failure against a cached URL we clear the cache row and rediscover.
- **`Client` is `Send + Sync` but `!Clone`.** `jmap-client` 0.4.1 holds a `parking_lot::Mutex<Arc<Session>>` internally, so the type can't be cloned. `Client::download(&self, ...)` and `Client::build()` both take `&self`, so multiple in-flight calls from a single `&Client` reference are safe without an `Arc` wrapper -- the download fan-out in `execute::download_messages` relies on this. Wrap in `Arc<Client>` only when calls need to outlive a borrow (e.g., `tokio::spawn`'d tasks on the daemon's runtime).

### Server-cap clamping

The JMAP server is a trust boundary: per RFC 8620 section 2 it can advertise any value it likes for its session capabilities, including absurd ones. A hostile or ill-configured upstream could otherwise force us to bundle megabytes of metadata into a single `Email/get` response, OOM us with a too-large upload, or escape the maildir tree with an oversized mailbox name. Every server-advertised limit therefore goes through `src/jmap/limits.rs`, which `min`'s the value against a hardcoded ceiling (`MAX_SET_BATCH_SIZE`, `MAX_GET_BATCH_SIZE`, `MAX_UPLOAD_FILE_SIZE`, `MAX_MAILBOX_NAME_LEN`) or, for the concurrency knobs, against the user-configured value. The server can only **lower** what we'd otherwise do; it can never raise it. Direct reads of `client.session().core_capabilities()...` elsewhere are a smell -- if you find yourself reaching for one, add or extend an accessor in `limits.rs` and route through it.

## Sync internals

The orchestration lives in `src/sync/` and is layered:

- `engine.rs` -- the entry point for all three sync modes (`sync`, `pull_only`, `push_only`). Resolves which mailboxes to sync, runs the dedupe pass, fetches remote changes, scans local maildirs, hands everything to `reconcile`, filters the resulting plan by direction, hands it to `execute`.
- `plan.rs` -- the action vocabulary (`SyncAction`) and container (`SyncPlan`).
- `reconcile.rs` -- pure function from `(remote_emails, remote_destroyed, local_changes, DB indices, conflict strategy) -> SyncPlan`.
- `execute.rs` -- walks the plan in dependency order and performs the actual JMAP / filesystem / DB writes.

### The sync cycle

One call to `SyncEngine::sync` (or `pull_only` / `push_only`) goes through six phases. Phases 1-3 are pure data-gathering and
planning; phase 5 is the only place we mutate the maildir or server, and phase 6 records the cycle's outcome for the next cycle's Phase 0 gate.

1. **Resolve mailboxes.** `resolve_mailboxes` sources the mailbox set (`Mailbox/get`, or a rebuild from `mailbox_map` when the `Mailbox` cursor's `Mailbox/changes` shows no structural change), honours `[sync].mailboxes`, applies the `INBOX` magic alias, and upserts each into `mailbox_map`. Returns a `Vec<(jmap_id, folder_name)>` that every later phase indexes against.

2. **Scan local + dedupe + collect remote changes.**
   - `janitor::dedupe::run` runs first (see [Idempotency model](#idempotency-model)), narrowed to the folders `compute_dirty_folders` flagged. On a Full cycle that's the folders whose current `(cur_mtime_ns, new_mtime_ns, cur_count, new_count)` snapshot differs from the row recorded by the last successful cycle, plus any folder with no row at all (first sync, post-recovery wipe, freshly added folder). On a Paths cycle it's the empty set: `janitor::dedupe::run` short-circuits on empty input and returns `DedupePlan::default()`, which flows through the rest of the cycle as a no-op (the watcher's authoritative event set already drives the necessary recheck cadence). The same `janitor::dedupe::run` powers the offline `jma janitor dedupe` CLI subcommand, which passes every folder in `mailbox_map` instead of the dirty subset.
   - `scan::scan_folder` walks each folder, compares against `local_state`, emits `LocalChange::{NewMessage, FlagsChanged, DeletedMessage}`. `NewMessage` carries the parsed Message-ID when the file has one -- reconcile uses it for adoption and move detection.
   - `fetch_remote_state` either loops `Email/changes` from the persisted cursor (delta path) or runs `Email/query` per mailbox followed by `Email/get` (initial path). The `cannotCalculateChanges` error wipes the cursor and falls back to the initial path. Returns `(remote_emails, remote_destroyed, new_state, used_initial_path)`.

3. **Build indices and reconcile.** `build_known_indices` materialises three views over `message_map`, each keyed on a different identifier (see [Identifiers](#identifiers)):
   - `known_by_jmap: HashMap<jmap_email_id, MessageRecord>` -- 1:1, since `jmap_email_id` is the primary key.
   - `known_by_maildir: HashMap<maildir_id, MessageRecord>` -- 1:1, since each on-disk file maps to one `message_map` row.
   - `known_by_message_id: HashMap<message_id, Vec<MessageRecord>>` -- 1:N, since the same RFC 5322 Message-ID can appear in multiple folders.

   `reconcile::reconcile` consumes these plus the dedupe pass's `LocalIndex`, the `[(jmap_id, folder_name)]` mailbox list from phase 1, the configured `ConflictStrategy`, and the effective `maxSizeUpload` cap (used to refuse oversized local files at plan time). The full input set is bundled as `ReconcileInput` in `src/sync/reconcile.rs`. Returns a `SyncPlan`.

4. **Filter by direction.** `SyncPlan::into_filtered(direction)` splits the plan into `(kept, dropped)`. `AdoptLocalMessage` is always kept regardless of direction (see [documentation on `plan.rs`, next](#plan-rs--the-action-vocabulary)). Dropped actions are logged so a `pull` or `push` user sees what was suppressed.

5. **Execute.** `Executor::execute` walks the plan in a fixed bucket order, performs the JMAP / filesystem / DB writes, and ratchets the JMAP cursor at the tail. See [`execute.rs`](#executers--performing-the-side-effects) for the full bucket order, load-bearing dependencies, and the chain-validation step that decides whether the cursor advances.

6. **Record folder checkpoints.** On Full cycles, `record_folder_checkpoints` snapshots `(cur_mtime_ns, new_mtime_ns, cur_count, new_count)` for every synced folder and upserts the row in `folder_checkpoint`. The next cycle's Phase 0 compares against these rows to decide which folders to walk. Failures here are non-fatal -- a missing or stale row just causes the next cycle to re-walk dedupe for that folder. The write happens outside the executor's cursor-advance transaction; if the process dies between the two, the next cycle replays cleanly off the cursor and conservatively re-walks dedupe. Paths cycles skip this phase entirely (they also skipped phase 0).

### `plan.rs` -- the action vocabulary

`SyncAction` is the discrete unit reconcile emits and execute consumes. Each variant exists to model exactly one observable outcome; the vocabulary is deliberately small but distinct so reconcile can reason about them locally and execute can dispatch on shape.

#### Pull-side actions (server -> local)

- `DownloadMessage` -- server has an email we don't. Fetch the blob, store it in the maildir, write `message_map` and `local_state`.
- `UpdateLocalFlags` -- server keywords differ from our recorded flags; rewrite the maildir filename suffix and update the DB.
- `DeleteLocal` -- server says the JMAP id is destroyed and we have it locally. Unlink the file and clear DB rows.
- `MoveLocal` -- server claims the email lives in a different mailbox than our local copy. Move the file across maildirs and update the folder column.
- `CreateLocalMailbox` -- server has a mailbox we don't have a maildir for. `ensure_maildir` and stamp the `.jma.mapping` sentinel.
- `RenameLocalMailbox` -- cached `mailbox_map.maildir_folder` disagrees with the freshly resolved name (server rename or parent move). `fs::rename` the maildir, rewrite `local_state.maildir_folder`, refresh the sentinel at the new path. Idempotent on the source-missing/target-present recovery branch.

#### Push-side actions (local -> server)

- `UploadMessage` -- local-only file. `Email/import` it and bind the resulting JMAP id.
- `UpdateRemoteKeywords` -- local flag change, server unchanged. Push keywords via `Email/set`.
- `DestroyRemote` -- file gone locally and we know the JMAP id. Tell the server to destroy.
- `MoveRemote` -- paired-delete-plus-new across folders is recognised as a move; emit a single mailbox-membership update instead of destroy+import.
- `CreateRemoteMailbox` -- folder-level push primitive symmetric with `CreateLocalMailbox`. Issues `Mailbox/set { create }` with name, optional parent, and optional role; the server-assigned id is picked up by the next cycle's `Mailbox/get` and bound through `resolve_mailboxes`.
- `RenameRemoteMailbox` -- push primitive symmetric with `RenameLocalMailbox`. Emitted when the per-cycle `.jma.mapping` sentinel walk finds a tracked mailbox id at a disk path the cache and server disagree with (user `mv`-ed the maildir locally, or `conflict_strategy = LocalWins` overrules a server-side rename). Issues `Mailbox/set { update }` with the new name and parent_id derived from the disk path by `decompose_disk_path_to_jmap`, which decomposes the layout-formatted path via `layout::decompose_folder_string` and resolves the parent segment against `mailbox_map` via a pre-cycle `cached_by_folder` snapshot. Rule-mapped paths and unresolvable parents are refused with a warn/debug log; the next cycle re-evaluates. The executor advances `mailbox_map` only after the JMAP call succeeds, so under `--dry-run` or per-action failure the cache stays anchored to the pre-rename folder and the next cycle re-emits the action.

#### The hybrid action

- `AdoptLocalMessage` -- pure DB write that binds an existing local file to a known JMAP id. No bytes move. Two cases:
  - Server claims a Message-ID we already have on disk (after a state DB wipe, or after a half-completed prior run).
  - User dropped a file into a maildir for a message that already exists upstream (would otherwise hit `alreadyExists` on import).

  Adoption belongs to neither side, so `direction()` returns `Both` and `into_filtered` keeps it. Ordinary adoption is pure DB work. Move-paired adoption is conditional: execute must still verify that its membership update was submitted and acknowledged; retaining it through a direction filter does not establish completion.

  `old_maildir_id: Option<MaildirId>` on `AdoptLocalMessage` covers the cross-folder local-move case: the same JMAP id is being rebound from an old maildir_id (about to be removed by the move's paired `DeletedMessage`) to a new one. Execute deletes the stale `local_state` row before upserting the new binding so the next scan doesn't re-emit `DeletedMessage` for the orphan.

  `old_jmap_email_id: Option<JmapEmailId>` is the remote-side analog and covers the destroy+create-with-shared-Message-ID rebind: the server destroyed Email A and created Email B carrying the same wire-format Message-ID header, and reconcile has paired them so the same maildir_id can be rebound from A to B. `commit_adopt` drops A's `message_map` row inside the same txn before upserting B, so the partial unique index on `message_map(maildir_id)` never observes both rows holding the same value. `try_adopt_remote` populates a `consumed_remote_destroys` set when it emits one of these rebinds, and `process_remote_destroys` skips ids in that set so the paired destroy doesn't emit a `DeleteLocal` against the file we just rebound.

#### Direction-neutral observation/history actions

`ObserveMessage` persists full server metadata and raw local observations
without acknowledging a mutation. `RecordFlagState` initializes or recovers
history independently of an effect. `DeferredFlagState` exposes unresolved
history/presence instead of silently producing an idle plan. All survive
direction filters, but an excluded push/pull remains unsettled. Dry-run
executes none of their writes.

#### Local orphans

A *local orphan* is a maildir on disk whose remote counterpart has been deleted server-side. `MailboxBindings::local_orphans()` exposes per-cycle `LocalOrphanRecord`s, populated as a side-effect of `resolve_mailboxes`. Detection feeds two paths: the cache-vs-server diff (cached `mailbox_map` row whose id is missing from the fresh `Mailbox/get`) and the sentinel-driven recovery walk (`.jma.mapping` files on disk whose id is also not in the live server set; `server_name` and `parent_jmap_mailbox_id` are recovered from the sentinel itself, covering fully-nuked DBs and partial-cache states alike). The cache row drops either way -- the state DB is disposable. The record's `binding.jmap_mailbox_id` is a tombstone (the server no longer serves it); useful for identifying DB rows that still point at the dead id. Detections log at `info!` from `resolve_mailboxes`.

#### Remote orphans

A *remote orphan* is the inverse: a server-known mailbox whose local maildir and sentinel are absent on disk. `MailboxBindings::remote_orphans()` exposes per-cycle `RemoteOrphanRecord`s, populated from `LocalChange::LocalFolderDeleted` events that scan emits for absent-maildir bindings. The detector observes absence; the cause (user delete, external script, backup restore that excluded the maildir, etc.) is the consumer's concern. Two structural exclusions prevent misfire: scan consults `MailboxBindings::is_new_mailbox` (a binding whose maildir is absent because `CreateLocalMailbox` has not yet run) and `MailboxBindings::sentinel_survives_for` (a binding whose `.jma.mapping` sentinel relocated elsewhere on disk, indicating a rename rather than a delete). The record's `binding.jmap_mailbox_id` is live -- the server still serves it; the on-disk side is the one that has vanished. Each record carries a `server_email_count` populated by `Email/query` with `limit: 0` after detection.

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
|-- process_local_changes    (upload / destroy-remote)
|-- reconcile_bound_flags    (observations + baseline -> flag decision)
\-- orphan finalizers       (folder conflict-policy decisions)
```

Every helper takes an immutable `&ReconcileCtx<'a>` so the parameter lists stay bounded as new branches are added. Mutable per-cycle state (`adopted_maildir_ids`, `deletes_overruled_by_server`, `consumed_remote_destroys`, the move bookkeeping sets) is threaded explicitly because the order of the passes matters. `consumed_remote_destroys` is the hand-off between `process_remote_emails` (where the destroy+create-with-shared-Message-ID rebind is detected and the source JMAP id is marked consumed) and `process_remote_destroys` (which skips ids in the set so the paired delete doesn't undo the rebind).

#### The move pre-pass

`scan::scan_folder` doesn't know about cross-folder moves: when the user moves a file from `INBOX/cur/X` to `Archive/cur/Y`, scan emits `DeletedMessage(X)` and `NewMessage(Y, message_id=M)`. Reconcile pairs them by Message-ID:

1. For each `DeletedMessage` whose record has a Message-ID, look for a `NewMessage` with the same Message-ID in a different folder.
2. If found, build a `DetectedMove` capturing the source/destination binding, old/new maildir_ids and actual destination suffix. Index these IDs before remote processing so a simultaneous server update cannot produce a stale-source `MoveLocal`.
3. Mark both ends as consumed so `process_local_changes` won't emit a separate `DestroyRemote` and `UploadMessage`.

`emit_detected_moves` uses `flag_resolution::decide` with the acknowledged
baseline, destination snapshot, full remote keywords and whole-set policy.
It emits:

- `MoveRemote` -- change server-side mailbox membership.
- `AdoptLocalMessage { old_maildir_id: Some(...) }` -- conditionally rebind and drop the old local_state row. Keywords are the full remote observation after applying only the selected owned-bit patch, preserving custom metadata.
- `UpdateRemoteKeywords` or `UpdateLocalFlags` when the shared decision requires a flag effect. Local flag work addresses the destination and is deferred until membership succeeds.

Competing selected remote/local mailbox destinations are deferred, with the
Email cursor held for replay. The pass avoids `DestroyRemote + UploadMessage`,
which would lose the JMAP id, thread id and keyword history. Genuine new
adoption uses bootstrap policy; adoption with retained history uses the same
baseline decision instead of reapplying bootstrap authority.

#### `process_remote_emails`

For each email the server returned:

1. **Skipped** if it's also in `destroyed_set` (handled separately).
2. **Skipped** if it isn't in any synced mailbox (with debug log).
3. **Path 1 -- known by JMAP id** -> `handle_known_remote`. Handle delete-vs-update and emit `MoveLocal` if server membership disagrees with ours. Ordinary flags and metadata are reconciled later by `reconcile_bound_flags`.
4. **Path 2 -- Message-ID matches a local file** -> `try_adopt_remote`. First check `known_by_message_id` (DB), then the in-memory `local_index` from the dedupe pass. The latter is what saves us after a `state.db` wipe. Emits `AdoptLocalMessage` and marks the maildir_id as adopted so phase 4 won't double-emit an upload.
5. **Path 3 -- nothing local** -> `DownloadMessage`.

#### `handle_known_remote` and conflict resolution

Delete-vs-update is handled inline; ordinary flag conflicts use the shared baseline pass:

- **delete-vs-update** -- local `DeletedMessage` for a JMAP id the server is still updating. `resolve_delete_conflict`:
  - `ServerWins`: re-download (restores the file). The orphan `local_state` row gets cleaned by the next scan cycle.
  - `LocalWins`: skip the remote-side action; `process_local_changes` will emit `DestroyRemote` unimpeded.

- **flag-vs-flag** -- `flag_resolution::decide` compares both mapped projections with the acknowledged baseline, chooses a whole set, and returns updated history separately from the required effect. `reconcile_bound_flags` emits one flag effect plus independent observation/history actions. Custom-only server metadata changes refresh the cache even when mapped flags agree. Moves and adoption use the same resolver through `emit_adoption_flag_reconciliation`; full moved metadata is committed after membership acknowledgement.

`deletes_overruled_by_server` is the cross-pass channel for the `ServerWins` delete case: when path 1 decides server wins,
`process_local_changes::handle_local_delete` must skip the `DestroyRemote` it would otherwise emit. Without this set the local loop would happily destroy the remote we just decided to re-download.

#### `process_local_changes`

After remote-side actions are settled, walk `local_changes` again:

- `NewMessage`: skip if `consumed_news` (move pre-pass) or `adopted_maildir_ids` (a remote pass already adopted this file).
  Otherwise check Message-ID against `known_by_message_id`:
  - Match in the *same* folder -> adopt.
  - Match in a *different* folder, with no paired delete -> warn and skip. This is a user-introduced duplicate that would hit `alreadyExists` on import every cycle and never converge.
  - No match -> `UploadMessage`.
- `FlagsChanged`: leave emission to `reconcile_bound_flags`, which also sees retained pending work without a new delta.
- `DeletedMessage`: emit `DestroyRemote` unless `consumed_deletes` (move pre-pass), `destroyed_set` (server already destroyed it), or `deletes_overruled_by_server` says so.

The local scan observes suffixes in both `cur/` and `new/`. Tracked new-side files participate in flag and move classification; unknown new-side files are not implicitly uploaded. Classification uses the tracked state across mailboxes, not only the destination folder's rows. A path-only result containing a deletion candidate is widened to a full inventory before reconciliation. If a live identity candidate still prevents establishing a unique move or a real deletion, the engine defers the destructive action and reports `deferred_actions` rather than claiming an idle synchronized state. The executor repeats the presence check before adding remote destroys to a request; unreadable inventory fails closed, and a live candidate prevents the request and its DB cleanup.

#### Invariants reconcile maintains

- Reconcile never reads the network or the filesystem; all inputs are passed in. This makes it cheap to add tests.
- A given local maildir_id appears in at most one push-side action per plan (the `consumed_*` and `adopted_maildir_ids` sets enforce this).
- A given JMAP email_id appears in at most one pull-side action per plan, with the exception that delete-vs-update conflicts in `ServerWins` mode emit `DownloadMessage` even though there's a `DeletedMessage` in `local_changes` for the same id (the local loop is suppressed via `deletes_overruled_by_server`).
- `AdoptLocalMessage` is emitted before the `NewMessage` loop runs, so adoption pre-empts upload deterministically.

### `execute.rs` -- performing the side-effects

Execute is the only place that mutates anything. It buckets the plan by variant, then drains buckets in a fixed order:

```
observe / record flag history -> adopt (unconditional) -> download -> local-flags -> local-move ->
local-delete -> upload -> remote-set (keywords + moves + destroys, batched)
-> adopt/observe placement (move-paired) -> destination local-flags
```

This ordering is load-bearing in a few places:

- **Adopt before everything (mostly).** Unconditional adopts -- those that bind a maildir file to a server email without an associated remote-side move -- run first, because subsequent actions (`UpdateLocalFlags`, `MoveLocal`) may reference the maildir_id / jmap_email_id being bound and only succeed if `message_map` already has the row.
- **Move-paired placement and local flags run after remote-set.** `old_maildir_id` identifies the conditional adoption and its paired destination flag effect. Execute intersects actually submitted move IDs with the adapter's acknowledged results; neither an excluded move nor a rejected/missing ACK authorizes adoption or a destination rename. `apply_move_pair_adopts` commits placement and full observations first, acknowledging flags only on actual agreement (a completed push or no-effect case). Required destination renames then use snapshot validation and acknowledge separately. A rename failure retains destination binding/observations plus unfinished flag history, so the next cycle need not rediscover a move or touch an old source. No later adoption overwrites a successful rename. Unfinished placement holds the Email cursor and counts as deferred work.
- **Ordinary pull side before push side.** The local DB settles to its post-pull state before ordinary pushes. Move-paired destination flag pulls are the explicit exception: membership must succeed before they can land.
- **Remote-set batched.** `apply_remote_set` collects every `UpdateRemoteKeywords`, `MoveRemote`, and `DestroyRemote` for `set_email_batch`. That function merges keyword patches and mailbox replacements for each Email id into one update before splitting by `maxObjectsInSet`; each combined update and each destroy counts as one object. `keywords::KeywordPatch` distinguishes Set/Remove mutations from full keyword observations. `jmap/email_set.rs` sends explicit JSON patches because jmap-client 0.4.1 cannot encode null-valued Email patches: Set becomes true, Remove becomes null, and pointer segments are escaped. A move and its flag changes remain in the same per-object update. The adapter retains connected-client authentication, API URL, timeout, typed JMAP responses and retry classification; HTTP redirects are refused rather than forwarding credentials. An id must appear in the positive updated/destroyed acknowledgement before its operation is mirrored; rejection or missing acknowledgement remains a failed id.

Flag-sync regression tests in `src/sync/flag_tests.rs` run actual `SyncEngine` cycles against real temporary Maildirs and a stateful JMAP model. The model applies patches, rejects invalid keyword values, and exposes fresh `Email/get` results; assertions compare those results with filenames, DB views, and later idle cycles. These tests do not replace the Docker-backed Stalwart tests, which remain a separate integration check.

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
- `update_local_flags` -- persist the receipt, validate the captured suffix with `store::set_flags_if_unchanged`, rename, then commit mirrors and acknowledgement together. Failure leaves history pending, without acknowledging the failed effect.
- `upload_messages` -- imports the captured `UploadMessage.flags` snapshot and mirrors/acknowledges that same snapshot. It does not re-parse a later filename. A moved-away path is skipped if unreadable. Catches `alreadyExists` and warns rather than failing the cycle.
- `apply_remote_set` -- persist flag receipts before I/O; after positive acknowledgement, commit keyword mirrors and processed baselines together. Rejected/ambiguous IDs do not advance the baseline; rejection details are retained for recovery/status.

#### Download and upload concurrency

Uploads (`upload_messages`) fan out via `buffer_unordered`, capped at the effective concurrency from `limits::` (`config.sync.upload_concurrency` clamped to the server's `maxConcurrentUpload`). DB writes happen serially after the stream drains, since `rusqlite::Connection` isn't `Send` and can't cross await points inside the parallel futures. Each successful `Email/import` contributes its `oldState` / `newState` pair to the cursor ratchet (see [State persistence](#state-persistence)).

Downloads (`download_messages`) use a producer-consumer shape instead: a spawned task does the parallel `buffer_unordered` blob fetch and streams each response body straight into a fresh file in the target maildir's `tmp/`, then pushes the open `TemporaryMailFile` handle into a bounded mpsc channel; the calling task drains the channel inline, performs the barrier-fsync + rename to `new/`/`cur/` via `store::finalize_message`, and commits the per-message DB transaction. Body bytes never sit in memory -- peak per-blob memory is one HTTP chunk (~16 KB) plus the open-file handle. The split exists because `Connection` isn't `Send`, but unlike the upload path it overlaps store-and-DB work with subsequent fetches instead of waiting for the whole stream to drain. The barrier-fsync and the SQLite WAL fsync stay on the same (consumer) task, so the durability ordering (file content barrier'd before the DB row publishes) is preserved per-message. Concurrency is clamped the same way (`config.sync.download_concurrency` against `maxConcurrentRequests`). `Email/blob` is a pure data fetch so downloads don't ratchet the cursor.

Downloads get a rate-limit halving loop: a transient failure within a pass commits the successes, halves concurrency, sleeps 500ms, and retries the unfinished entries; the halving is one-way per cycle, since a link that hit the cap once is likely to hit it again, and the next cycle resets from the configured value. A hard error short-circuits after committing whatever did succeed; a pass with no progress and no transient signal bails with "stream stalled" rather than spinning. Uploads don't halve -- they drain the stream regardless, and the first observed error becomes the cycle's hard error.

#### State persistence

After every handler returns, `execute` writes `new_email_state` (if present) into `jmap_state`. This is the only place the JMAP cursor advances -- if any handler returned an error, we never reach this write and the next cycle re-runs `Email/changes` from the prior cursor. The DB writes that *did* happen before the error are kept, which is fine because they're all idempotent against a re-run.

The cursor we write is not unconditionally `new_email_state`. Per RFC 8620 section 7.1, every state-advancing call (`Email/import`, `Email/set`) returns its own `oldState` / `newState` pair, and a cursor can only legitimately move along a chain whose edges are all present. `execute` collects each call's pair into a chain map keyed on `oldState`, then walks forward from the cursor reconcile produced. If every edge is consumed in a single contiguous walk, no third-party write landed between our calls and we ratchet the cursor to the walk's end; if any edge is missing or unreachable, we leave the cursor alone and let the next cycle's `Email/changes` resume from the unmoved point.

`resolve_email_cursor` also holds the cursor (writes nothing this cycle) when a pull-side filesystem op *warn-and-skipped* -- a `MoveLocal` or `UpdateLocalFlags` whose on-disk rename failed, so the handler logged, continued, and left its paired DB write unwritten. Those handlers don't propagate the failure (one bad file shouldn't kill the cycle), so without this the cursor would advance past the `Email/changes` delta that produced the skipped action: the next delta would run from the advanced cursor and never re-report the email, and the local scan couldn't re-derive it either (the file and its `local_state` row are both unchanged), stranding the change silently. Holding the cursor -- detected via a shortfall between each handler's attempted and succeeded counts -- lets the next cycle re-apply the action idempotently. This is what makes the per-handler "next cycle re-derives" promise true on the delta path, not just the initial-pull path.

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

## Daemon internals

### Trigger pipeline

`watch` mode receives external change signals from two sources and funnels them, plus a daemon-owned retained-work deadline, into one sync cycle at a time. There are three layers between a kernel FS event and `engine.run`, and they each smooth a different slice of the noise; a self-write cache rides alongside layer 1 to suppress fsevents echoes of jma's own disk writes:

```
FS kernel events -> notify-debouncer-mini -> [self-write cache] -> mpsc trigger channel -> coalescing window -> engine.run
                    ([watch].debounce_secs)  ([watch].self_write_                          ([watch].coalesce_
                                              ttl_secs)                                     window_ms)
SSE state events ---------------------------------------------------/
```

**Layer 1: per-path debouncing** (`notify-debouncer-mini`, configured via `[watch].debounce_secs`, default 2s). The crate keeps one global wakeup timer plus a per-path event map. When the timer fires, every path whose `update.elapsed() >= timeout` is drained and emitted in the same batch; surviving paths re-arm a fresh deadline off the youngest survivor. With `batch_mode = true` (the crate default), an arriving event never shortens an existing deadline, so paths batch together against the deadline that was set by the *first* event of the current burst rather than each path setting its own. Practical consequence: a flurry of writes against the same file collapses into one emit; writes against different paths usually emit together but can land in adjacent ticks (see below).

**Layer 2: trigger-loop coalescing** (`runner.rs`, configured via `[watch].coalesce_window_ms`, default 500ms). After the first `SyncTrigger` arrives on the mpsc channel, the loop holds open a small window before starting the cycle. Each subsequent trigger resets the window. The cycle normally runs once the channel is quiet for the full window, but a due retry caps that wait and a timer-first cycle bypasses coalescing. This layer also decides the trigger shape via dominance, broadest scan first: `Initial` beats `Manual` beats `RemoteChange` beats `LocalStructuralChange` beats `LocalChange`. `LocalChange` paths concatenate across every absorbed `LocalChange`; a merge landing on `LocalStructuralChange` unions the `candidates` from every absorbed structural trigger and folds every absorbed trigger's message paths -- structural and plain `LocalChange` alike -- into the merged trigger's `message_paths`, so a folder-level event doesn't lose the FS-event paths from the same batch.

Before the watcher emits a `LocalStructuralChange` trigger, `daemon::classify::batch_fires` judges the batch's structural candidates on two cheap signals: a candidate fires if it is a `cur`/`new`/`tmp` path event, or if it is a maildir on disk via `store::is_maildir` (a folder that appears by create, rename, or move stats as a maildir, which a rename delivers atomically without trio events; this arm is unguarded, since a renamed folder carries its sentinel with it and gating here would suppress the rename). The trio arm is gated on the folder's `.jma.mapping` sentinel: a missing trio always fires (a folder being torn down), but an intact trio fires only when its parent folder has no sentinel, i.e. jma has not bound it yet. An intact trio under a sentinel'd (already-bound) folder is treated as churn noise rather than a create or teardown -- FSEvents on macOS can coalesce heavy in-`cur/` churn (renames and hardlinks that never leave the folder, as in Gnus/nnmaildir's bookkeeping) up into a directory-level event naming `cur` itself, and without the gate that echo reads as a folder appearing. Everything else -- sidecar files, foreign index directories like `.notmuch/` that churn temp files, junk directory trees, bare `mkdir`s, missing non-trio paths -- is noise. This is accurate rather than precise: no binding lookup and no subtree walk, at the cost of two structural changes that carry neither signal at the candidate path and wait for the next full scan -- a folder moved out of the tree, and a nested tree moved in under a bare (non-maildir) parent. A batch whose candidates are all noise degrades at the watcher to a plain `LocalChange` over its surviving message paths, or to nothing at all, so folder-level filesystem noise never enters the trigger channel and every trigger that reaches the runner starts a cycle.

The two layers exist because they smooth different things. `debounce_secs` smooths each filesystem path's event stream; `coalesce_window_ms` smooths *across* paths and across signal sources (FS + SSE). Neither subsumes the other: layer 1 on its own cannot merge two debouncer batches that fire on adjacent ticks, nor can it merge an FS batch with an SSE state event; layer 2 on its own would see one mpsc send per kernel event and have to dedupe everything that the per-path crate already collapses.

#### Retained-work retries

`daemon/retry.rs::OperationRetry` keeps one Tokio-monotonic deadline on the
`WatchDaemon`, not on an SSE session. Bootstrap and ordinary cycle outcomes
arm it for retryable message work. Backoff is 1s, 2s, 4s ... capped at 60s,
separate from transport reconnect backoff. Exhausted transient startup RPCs
also arm it, so recovery does not depend on a first external event. A
transport reconnect retains the deadline and rejection mask. Typed SQLite
BUSY/LOCKED errors also schedule a fresh cycle without rebuilding the JMAP
session; constraint/corruption errors are not guessed retryable from their
text. Other hard cycle errors pause automatic retries until another
external/manual cycle; hard connect/configuration errors still abort rather
than spinning.

A timer selects `ScanScope::Retry`: a fresh full managed inventory and
bidirectional reconciliation, not replay of a serialized stale mutation.
Retry-only flag decisions skip blocked/review disagreement but can recognize
fresh agreement. A daemon-lifetime set of confirmed permanent Email/set
rejections also suppresses mailbox-only updates and destroys, with their
paired destination bindings/local effects. Suppression holds the Email
cursor and reports blocked work. Missing positive ACK is ambiguous/retryable,
not a confirmed permanent rejection. Ordinary event/manual Full cycles may
reassess permissions; restart performs a normal Full bootstrap. The mask is
not persisted as a general operation queue.

A path no-op cannot clear or postpone a deadline. For successful outcomes,
only a completed, non-preview, bidirectional full check may disarm it when
no retryable work remains. Blocked/review-only work does not arm a timer. If a deadline expires
during path coalescing, the merged external cycle is promoted to normal Full
so it neither loses fresh local edits nor consumes a mailbox-only retry as a
path no-op. A timer-first cycle runs without waiting for a quiet FS stream.

This is retained flag/message-effect recovery, not a new retry guarantee for
all body imports or mailbox RPCs. Retry cycles retain normal managed-folder
policy; they are not the targeted flag-only repair command. Tests drive the
real runner/engine/HTTP model with controlled producers (no accidental FS/SSE
follow-up), plus paused-clock scheduling tests. Real Stalwart/OS producer
coverage remains a separate gate.

#### Self-write cache

`SelfWriteCache` (`src/sync/self_writes.rs`) sits between the per-path debouncer and the mpsc trigger channel and exists to drop fsevents echoes of jma's own disk writes. Two suppression layers do this together:

- The watcher's all-live-new filter (no cache involved) catches the simplest case: a batch whose every path is a live `new/` file. Per `scan_paths`' contract those events are seen-only -- jma's delivery already updated the DB -- so the trigger never reaches the runner.
- The cache catches the harder case: an MUA promotes `new/<id>:2,F` to `cur/<id>:2,F` preserving flags. Both source and destination paths arrive in the same batch, the all-live-new filter keeps the batch (a `cur/` path is present), and the runner would otherwise wake, run a path-driven scan, classify as no-op, and pay the `Email/changes` round-trip on the way out. Each delivery records two cache entries: the actually-delivered path, and (for `new/` deliveries) the predicted same-flag `cur/` path the MUA might rename to. When the watcher sees a batch whose every path matches a non-expired cache entry, it drops the batch and evicts those entries.

The path-exact key matters: a flag-changing promotion (e.g. MUA adds `S` on read, producing `cur/<id>:2,FS`) won't match the predicted `cur/<id>:2,F` entry, so the batch falls through to a real cycle that pushes the new flag to the server. Eviction-on-match matters too: once an entry has caught the echo it was predicted for, a *subsequent* event on the same path within the TTL has to fall through (e.g. an MUA renames the just-promoted `cur/` file to add `S`).

TTL defaults to `2 * (debounce_secs + coalesce_window_ms)` rounded to the nearest second (5s under default knobs), with a 1s floor. The 2x multiple is wide enough for a push-aware MUA to notice the `new/` delivery and promote within the next debounce cycle. Polling MUAs (Gnus's nnmaildir, others on a manual-fetch schedule) may need to override `[watch].self_write_ttl_secs` upward; setting it to 0 effectively disables the cache and is useful when debugging a self-suppression report.

Limitation: a user deletion of a freshly jma-delivered file *within* the TTL window will be silently swallowed (the watcher matches the cache entry and drops the batch). The next full scan -- Initial, RemoteChange, or a manual sync -- catches the drift, so the deletion eventually propagates. This is the price of the cache; the alternative (no cache) means every same-flag promotion costs an `Email/changes` round-trip.

#### The split-batch corner case

Per-path debouncing means two events that fire microseconds apart on different paths can land in *different* emit batches. Concretely: a cross-folder rename produces a source-path event at t=0 and a destination-path event at t=0.001. The first event arms the global deadline at t=2.0. At t=2.0 the wakeup fires; the source path's `update.elapsed()` is exactly 2s so it emits, and on the way out of the drain the deadline is reset and re-armed at `dest.update + timeout` = t=2.001 (the source already drained, the dest is the only survivor). At t=2.001 the next wakeup fires and the dest emits. Two callback invocations, two mpsc sends, two `SyncTrigger`s.

Without layer 2, those two triggers drive two separate cycles. Under `ScanScope::Paths` (the path-driven scan, which trusts the event stream as the change set), the source-cycle sees `DeletedMessage(src)` alone and reconcile cannot pair it with the missing `NewMessage(dst)`; the move degrades into `DestroyRemote` followed by a fresh `UploadMessage`, losing the JMAP id, thread, and keyword history of the original. This is what `coalesce_window_ms` exists to fix: 500ms is comfortably larger than the inter-tick gap, so both halves arrive in the same coalesced batch and `scan_paths` sees them in one call.

Raising `debounce_secs` doesn't fix this -- the split is a per-path-timing artifact, not a window-too-narrow problem, so raising the value just relocates the same boundary and the inter-tick split can recur there. `coalesce_window_ms` is the layer that actually addresses it.

#### Steady-state latency

The latency floor from a first FS event to the start of the cycle is `debounce_secs + coalesce_window_ms` (~2.5s with defaults). Under `batch_mode = true` the debouncer's docstring caps per-event delay at 2x `debounce_secs`, so the worst-case floor with defaults is closer to ~4.5s. Continuous activity extends both windows further (each layer's timer resets on new input).

#### Tuning expectations

All three knobs are advanced/debug-only and intentionally undocumented in the user-facing README. Defaults handle the common case. Reasonable reasons to override:

- `debounce_secs`: an MUA whose write flurries genuinely span more than 2s, or load-shedding when the watcher fires too aggressively.
- `coalesce_window_ms`: chasing a split-batch report that survives the default 500ms (raise it), or shaving latency off a non-production diagnostic run (lower it; tests can set it to 0 to disable the wait entirely).
- `self_write_ttl_secs`: an MUA whose `new/`->`cur/` promotion latency consistently exceeds the derived default (raise to suppress promotion echoes), or debugging the self-suppression behavior (set to 0 to bypass the cache entirely).

If you find yourself wanting to recommend a tuning change to a user, the right move is usually to investigate whether the default actually broke or whether something upstream (an MUA bug, a non-atomic move, fsevents drops) is the real cause.

## Maildir semantics

### Partial Maildirs and strict inventory

Either cur/ or new/ makes a Maildir present; an empty peer removed by a cleanup
tool does not make its live messages disappear. `store::try_open_maildir`
uses that shape without creating directories. Authoritative reads use
`maildir_ops::entries::read`: the pinned Maildir iterator silently suppresses
*all* initial read_dir errors, not just NotFound. The shared reader opens both
present peers before yielding entries, skips only missing peers and dot files,
keeps raw suffixes and typed IDs, and propagates other open/entry errors.
Bare new-side IDs remain valid; malformed cur-side filenames are errors.

Scan, dedupe/adoption indexing, orphan catalogs, rescue-before-delete, prune
counts and flag/move snapshots use this reader. An unreadable peer cannot
become an empty inventory or a prune Safe label. Reads and previews do not heal
folders. Checkpoints represent a missing peer with zero mtime/count, so later
creation dirties the folder; present unreadable sides remain errors. Both
checkpoint gates recognize either peer, allowing new-only files to participate
in rebuild adoption rather than being downloaded again.

Only an actual message write may create its required target peer (for example
new-to-cur Seen promotion). The helper creates that leaf only, not the mailbox
or its ancestors, and syncs its parent on Unix. A missing mailbox still fails
snapshot validation. Non-Seen updates in new/ and cur-only renames do not
create unrelated peers. The explicit repair-flags command intentionally keeps
its stricter complete-Maildir validation; ordinary sync handles partial shapes.
Tests cover local pushes/previews, remote pulls, rebuild adoption, checkpoints,
and present-but-unreadable-peer refusals. This does not change Message-ID's
identity limits, DB rebuild semantics or unrelated best-effort sampling tools.

### Format conventions

Three under-specified parts of the maildir format have shaped specific choices in `maildir_ops`. The in-code docstrings cover *what* jma does; this section covers the surrounding ecosystem that explains *why*.

- **`new/` vs `cur/`: ecosystem is split between placement- and flag-driven.** The maildir(5) spec says deliveries land in `new/` and are promoted to `cur/` on first read, but is silent on whether placement or `:2,S` should be canonical once a file lives in `cur/`. Mutt and NeoMutt are placement-driven by default (`maildir_check_cur=false`): a no-`S` file in `cur/` reads as "Old", and a file in `new/` is unread regardless of suffix. mblaze takes the same shape. Dovecot (an IMAP server, not a MUA) writes strictly the other way -- any flag at all routes the message directly to `cur/`, so it never produces `new/<id>:2,<flags>` -- but its reader accepts that shape from other tools and renames it verbatim. OfflineIMAP and mbsync route by `S` and preserve the `:2,<flags>` suffix in both subdirs deliberately (OfflineIMAP #80 was closed wontfix in 2015 with an explicit "we DO WANT flags even if it's new" rationale). `maildir_ops::store::store_message` follows the OfflineIMAP/mbsync precedent; the failure mode of *not* preserving the suffix in `new/` (MUA promotion drops non-`S` flags, jma then pushes the erasure to the server) is spelled out in its docstring.

- **Durability fences: `F_BARRIERFSYNC` is stricter than MDA precedent.** Every MDA we surveyed (procmail, maildrop, getmail, Dovecot LDA, mbsync, OfflineIMAP) does one `fsync(2)` on the `tmp/` file before `rename(2)`ing it into `new/` or `cur/`. Only Dovecot additionally fsyncs the parent directory once per delivery transaction. None use `fcntl(F_FULLFSYNC)` on macOS -- the ecosystem accepts that a power loss may lose the most recent delivery. jma uses `F_BARRIERFSYNC` on Apple via the `barrier_fsync` feature of the pinned `maildir` crate fork (`Cargo.toml`), which is stricter than MDA precedent: it guarantees ordering (file content durable before the rename publishes it) without paying the full `F_FULLFSYNC` latency. `maildir_ops::store::store_message` is the sanctioned entry point for the buffered path; `open_tmp` + `finalize_message` for the streaming download path. Any other writer that bypasses these would also bypass the barrier-fsync.

- **Custom keywords have no portable on-disk representation that MUAs respect.** The spec reserves the `:2,...` suffix for six standard flags (`P R S T D F`); anything else is per-program convention with no consensus. Dovecot (an IMAP server, not a MUA) writes a `dovecot-keywords` index file in each maildir mapping lowercase suffix letters to keyword names, but it's Dovecot-internal IMAP-flag tracking, not interchange: of the MUAs we surveyed (notmuch, mu/mu4e, mutt, NeoMutt, Gnus's nnmaildir, Wanderlust, aerc, Sylpheed/Claws, alot), none read or write it. NeoMutt preserves unknown suffix characters verbatim but assigns them no meaning. notmuch, mu, Gnus, and Wanderlust route custom tags into private indexes alongside the maildir, never into the filename. mutt and aerc drop or reject non-standard chars. Courier IMAP (also a server, not a MUA) uses an incompatible `courierimapkeywords/` format -- Dovecot ships a migration tool precisely because the two don't interoperate. `maildir_ops::flags` maps only the standard six between JMAP keywords and the filename suffix (see `FLAG_MAPPINGS`); server-only keywords (`$imported`, `$hasattachment`, user labels) stay in `message_map.jmap_keywords` and don't appear on disk. We explored writing `dovecot-keywords` and concluded against it: the file is only useful to a cohabiting Dovecot server (no MUA on the maildir would see the keywords), and Dovecot's per-mailbox-dynamic letter-to-name assignment makes an external writer hard to keep coordinated with Dovecot's own index. The work is gated on a real Dovecot-cohabitation use case, not a general user want.

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

## Maildir/DB drift and `janitor prune`

The maildir tree and `mailbox_map` can disagree in ways a sync cycle won't reconcile on its own -- the user shrank `[sync].mailboxes`, `mkdir`'d a folder by hand, deleted a maildir, or moved one before the next sync pushed the rename. `maildir_ops::drift::compute_drift` is the single offline classifier of that disagreement; `status` renders it and `janitor prune` acts on it, so the two can never disagree about what drift exists.

### The four drift classes

`compute_drift(conn, maildir_root, mailboxes, case_insensitive)` reads `mailbox_map`, the on-disk folders (`find_maildir_folders`), and the `.jma.mapping` sentinels, and returns `DriftClasses`:

- **`disk_only`** -- a maildir folder with no `mailbox_map` row (an untracked stray).
- **`db_only`** -- a row whose maildir is gone from disk: neither `<folder>/cur` nor `<folder>/new` exists, per `store::is_maildir` (an orphaned cache row).
- **`config_dropped`** -- a folder present on both sides whose mailbox is no longer selected by `[sync].mailboxes`. Uses the same path-based `jmap::mailbox::get_selected_mailboxes` the engine uses, but over the cached `mailbox_map.remote_path` rather than the live server paths -- so `status`/`prune` match what `sync` would select given the last-synced paths. A server-side rename not yet reconciled (or a row with no cached path, which is left unflagged) can diverge until the next sync refreshes the cache. Empty when the config syncs every mailbox.
- **`rename_in_flight`** -- a disk orphan whose `.jma.mapping` id matches a db orphan's id: a local `mv` not yet pushed. Pairing the two pulls them out of `disk_only`/`db_only` so a rename reads as a rename, not as an unrelated stray + orphan. The four classes are disjoint.

### `janitor prune`: plan then apply

`prune::plan` turns `DriftClasses` into a `PrunePlan` of `PruneEntry`s, each carrying a `PruneSafety`. Two orthogonal force axes, each gating one class shape:

- Disk-bearing classes (`DiskOnlyStray`, `ConfigDropped`): `Safe` when the maildir is empty, else `NeedsForceNonEmpty`.
- `DbOnlyOrphan` (DB-cascade-only, the maildir is already gone): `Safe` when the folder is no longer in the sync set, else `NeedsForceInConfig` -- clearing rows for a still-selected folder would just make the next cycle re-download it.
- `rename_in_flight` is never prunable; it lands in `PrunePlan::skipped_renames` so a preview can explain the skip.

`prune::apply` removes each entry whose label is `Safe` or whose matching force flag (`--force-non-empty` / `--force-in-config`) is set; it records force-gated entries as `skipped` and permitted-but-unremovable ones (path-escape refusal, rescue failure, fs error) as `deferred`. Every removal routes through `removal::remove_maildir_tree` (path guard, rescue of unmapped files, NotFound-tolerant disk removal, and the `message_map`/`local_state`/`folder_checkpoint` cascade); `apply` then drops the `mailbox_map` row itself, the one piece `remove_maildir_tree` leaves to its caller. A `DiskOnlyStray` has no row, so it passes `None` for the mailbox id -- rescue then treats every file as unmapped and the `message_map` cascade is skipped.

### The disposability boundary

The state DB and the `.jma.mapping` sentinels are disposable -- a nuke-and-resync rebuilds them (see [Idempotency model](#idempotency-model)). Mapped maildir mail is *not*: a synced message may carry local-only flag changes, or be the last copy of something deleted server-side. That is why a non-empty removal always needs an explicit `--force-non-empty` rather than being treated as safe-because-cached, and why `remove_maildir_tree` rescues unmapped (local-only) files to a rescue maildir before deleting rather than destroying them.

# jma -- Sync JMAP mailboxes to local Maildirs

[![CI](https://github.com/jmibanez/jma-mail/actions/workflows/ci.yaml/badge.svg)](https://github.com/jmibanez/jma-mail/actions/workflows/ci.yaml)[![E2E](https://github.com/jmibanez/jma-mail/actions/workflows/e2e.yaml/badge.svg)](https://github.com/jmibanez/jma-mail/actions/workflows/e2e.yaml)

`jma` (the binary; crate name `jma-mail`) is JM's Mail Agent: it syncs your local Maildir mailboxes with a [JMAP](https://jmap.io) mail server such as Fastmail, like [`isync`/`mbsync`](https://isync.sourceforge.io/mbsync.html) but for JMAP instead of IMAP.

`jma` supports syncing a Maildir mailbox that was downloaded via `mbsync` or `offlineimap` -- point it to the directory containing your Maildir mailboxes and it should pick up where `mbsync` or `offlineimap` left off.

This project is in a very early state, though I'm using it for my own mail. **Use it at your own risk**.


## Quick Start

```console

# Pre-populate the config file, and initialize a Maildir structure in ~/Mail/Fastmail (the default) if it doesn't exist
$ jma init

# Edit the config file in ~/.config/jma/config.toml

# Run a bidirectional sync (below invocation is equivalent to running `jma sync`)
$ jma

# ... Or, if you want push email plus watching for any local changes to sync to upstream
$ jma watch
```


## Installation

Currently, this project doesn't yet have releases. You need to install this manually via Cargo:

```shell
cargo install --git https://github.com/jmibanez/jma-mail
```

## A note on mailboxes

`jma` respects the naming of your mailboxes, with the exception of inboxes. To match the behavior of `mbsync`, `jma` will use the Maildir mailbox named `INBOX` against the JMAP folder with the `inbox` role. This means that if your folder is localized on your provider, your local Maildir inbox will always be named `INBOX` regardless.

Note this also applies to the names of the mailboxes in `[sync].mailboxes` (see below), so if your mailbox is e.g. named `Posteingang` it will be synced locally as `INBOX`.

If you want finer-grained control over how specific folders are mapped to on-disk names -- for example, to flatten Gmail's `[Gmail]/Sent` to a tidier `GmailSent` -- see [Maildir rename rules](#maildir-rename-rules-rename_rules).

## How jma Tracks State

`jma` tracks state between your local Maildirs and the upstream JMAP server on an SQLite database. By default the database lives at `<sync.maildir_path>/.jma.db` -- a hidden file at the maildir root, so state and data move together when you copy or relocate the maildir, and multiple accounts each get their own DB without sharing a path. Set `[state].db_path` to override (for example, to keep state on a local-only path when the maildir lives on a synced or networked volume). State is intentionally disposable: you should be able to delete its state and rerun a `jma pull` to reconverge.

If you delete the state DB and run `jma pull`, `jma` will re-walk the server. It parses the `Message-ID` of each existing local message and "adopts" messages if they exist on the server, instead of re-downloading them. If you previously killed `jma` in the middle of a previous initial `pull` and then deleted the state DB, `jma` does The Right Thing and continues where it left off (i.e. it doesn't redownload previously downloaded messages).

So what's stored in the state DB?

  * A bidirectional `message_map`, mapping between a JMAP email and its local maildir file. `jma` stores the name of the local Maildir file and its corresponding `message_id` as a fast lookup cache
  * A mapping between JMAP mailbox IDs and local Maildir folder names in `mailbox_map`, including role and parent
  * `jmap_state` containing per-entity sync cursors so `jma` only needs to ask for changes since the last sync
  * And `local_state`, which is a snapshot of the local state (flags, size, mtime) so any local filesystem changes are quickly detected

None of the state DB's contents are required to do a `sync`, `push`, or `pull`.

### Breaking Changes

If there are any changes that break state tracking, as mentioned above you can simply delete the state DB and re-run `jma`. For most cases, `jma` marks its SQLite state database with a schema version -- if there's a mismatch, it will automatically nuke the state DB and do a full sync to catch up.

### Concurrency: One Mutator at a Time

Mutating commands (`sync`, `pull`, `push`, `watch`) take **two** exclusive OS-level advisory locks before doing any work:

1. `<maildir_root>/.jma.lock` -- guards the maildir against concurrent mutation by any other jma process pointed at the same maildir, even if their state DB paths differ.
2. `<db_path>.lock` (e.g. `<maildir_root>/.jma.db.lock`) -- guards the state DB against the schema-mismatch unlink/recreate path racing a concurrent process that opened the same DB.

If another instance already holds either lock, the second invocation fails fast with a message naming the holder's PID. The two locks cover different topologies: same maildir / different DB (lock 1 catches it), and same DB / different maildir (lock 2 catches it). The locks are always acquired in the same order -- maildir first, then state DB -- so two contending processes can't deadlock.

Read-only commands (`mailboxes`) and commands that don't touch the maildir or DB (`init`, `auth`) do **not** take either lock and can run alongside a `watch` daemon.

The locks are held by the kernel, not by the contents of the files, so they're released automatically when the holding process exits -- including on crash or `kill -9`. The `.lock` files themselves may stick around on disk; that's fine, the next invocation will reuse them. You should never need to delete them by hand, but it's safe to do so when no jma process is running.


## Configuration

Configuration for `jma` lives in `~/.config/jma/config.toml`. The key knobs to put in are your account details, the path to your Maildir mailboxes, and which mailboxes to sync.

### Authentication and Account `[account]`

`jma` currently only supports Bearer tokens, AKA API tokens. On Fastmail, go to **Settings -> Privacy & Security (under Account) -> Manage API tokens** to generate a token, then provide it to `jma` in one of two ways (checked in this order):

  1. The OS-native secret store, via `jma auth set-token --account <email>`. The token goes into macOS Keychain, Linux Secret Service (D-Bus), or Windows Credential Manager -- never on disk in the clear. Per-account scoping (the keyring entry is keyed on `<email>`) keeps multi-account configs from sharing or overwriting each other's credentials. Pipe a token in (`pbpaste | jma auth set-token --account foo@example.com`) or let it prompt. `jma auth clear-token --account <email>` removes it. The auth subcommands don't read the config -- the email you pass is what `[account].email` must be later for sync to find the token; a typo is detected when sync runs and reports "no token for ...".
  2. The `token` key in the `[account]` block of the config file. Plaintext fallback for headless servers (no Keychain, no D-Bus session bus) where the keychain isn't available.

Other `[account]` keys:

  * `email`: The email address for this account. Required. The domain part is used to discover the JMAP session URL via DNS SRV (`_jmap._tcp.<domain>`) and `/.well-known/jmap`, per RFC 8620 section 2.2.
  * `session_url`: Optional explicit JMAP session URL. When unset (the default), the URL is autodiscovered from the `email` domain via DNS SRV and `/.well-known/jmap`, and the result is cached in the state DB. Set this only to override autodiscovery -- e.g. when your provider doesn't publish the discovery records, or to pin a specific endpoint during testing. To force a re-discovery (for example, when your provider changes their session endpoint), run `jma auth rediscover`.

### Sync `[sync]`

This section configures which Maildirs `jma` will sync to, and how it syncs. The important knobs here are `maildir_path` which should point to the root of your Maildir mailboxes that you want to sync (e.g. `~/Mail/Fastmail`, or `~/Mail/my-provider`). By default `jma init` will also populate `mailboxes` with the common IMAP/JMAP mailboxes (INBOX, Archive, Sent, Drafts, Trash) and none of your custom folders/mailboxes -- if you want to populate _all_ mailboxes from upstream, unset this key or set it to an empty list.

  * `maildir_path`: The path to the Maildir root you want to sync
  * `mailboxes`: The specific mailboxes you want to sync. Set this to `[]` (an empty list) or leave this empty to sync all mailboxes. Note that this respects the inbox role; if your upstream mailbox with the inbox role is named e.g. `Posteingang` (DE) it will be synced to the local folder `INBOX`.
  * `case_insensitive_match`: When checking mailbox names against upstream, it can happen that your local Maildirs do not match because of different cases (e.g. when you're on a case-insensitive, case preserving filesystem, and your mailbox is named `foo` but upstream is saved as `Foo`). Set this to `true` to ignore case; default is to respect case.
  * `download_concurrency`: How many blobs (messages) to download concurrently. By default this is 8, but it is clamped by what your upstream specifies as its maximum supported concurrent requests (i.e. whichever is lower wins)
  * `conflict_strategy`: How to resolve conflicts:
     * `server-wins` -- The state on the server wins. Any local changes are discarded
     * `local-wins`  -- Your local Maildir state wins. The server is updated to reflect your local Maildir
  
### State `[state]`

This section only has one key, `db_path`, which tells `jma` where to persist its state. Leave it unset to use the default (`<sync.maildir_path>/.jma.db`); set it to override -- e.g. when the maildir lives on a synced or networked volume and you want the SQLite files on a local-only path.

### Push Email Config `[watch]`

`jma` supports JMAP's EventSource for push emails. This section has knobs around that feature. You should probably leave the defaults in, but if you want to tweak things this is the place:

  * `post_arrival_command`: A shell command that `jma` will invoke when it observes new mail. If you use a mail indexer such as `mu` or `notmuch`, put the indexing command here -- e.g. set this to `notmuch new` for `notmuch`.
  * `ping_interval`: How often (in seconds) to request the server send heartbeat pings on the EventSource (SSE) stream. Per RFC 8620 section 7.3 the server is allowed to clamp this value; some providers (Fastmail, notably) use a longer interval than requested. The actual interval the server uses drives the daemon's stream watchdog -- see `watch` below.

### Maildir rename rules `[[rename_rules]]`

By default `jma` projects each JMAP folder onto disk using `[sync].folder_layout` and `[sync].hierarchy_separator`. If you want certain folders to land under different on-disk names -- to flatten Gmail's `[Gmail]/Sent` to a tidier `GmailSent`, or to reshape a whole subtree -- add one or more `[[rename_rules]]` tables. Two kinds:

  * **Direct mapping.** Match an exact folder path; replace it with a literal name.

    ```toml
    [[rename_rules]]
    type = "map-directly"
    source_folder_path = "[Gmail]/Sent"
    renamed_name = "GmailSent"
    ```

  * **Pattern mapping.** Match a [Rust `regex`](https://docs.rs/regex)-syntax expression against the folder path; produce the new name via capture-group substitution. TOML literal strings (single quotes) avoid having to backslash-escape.

    ```toml
    [[rename_rules]]
    type = "pattern"
    source_folder_pattern = '\[Gmail\]/(.*)'
    rename_pattern = 'Gmail.\1'
    ```

A few things to know:

  * **Rules match against the canonical `parent/child` path with `/` separators**, regardless of `[sync].hierarchy_separator`. So a parent `[Gmail]` with a child `Sent` is matched as `[Gmail]/Sent` even if your hierarchy separator is `.`.
  * **First match wins.** Rules are evaluated in the order they appear in the config; the first that matches a folder path is used.
  * **The rule output is the final on-disk folder name.** When a rule fires, the layout's separator-joining is skipped -- the rule's right-hand side is what lands on disk. By design: a single rule can expand a flat JMAP name into a subtree (e.g. `foo.bar.baz` to `foo/bar/baz`) or collapse one the other way.
  * **Pattern regexes are validated at config load.** A typo in `source_folder_pattern` is reported when `jma` starts, not on the first sync.

## Commands and Options

Note: By default, invoking `jma` without any subcommands runs `sync`.

```console
JM's Mail Agent: bidirectional JMAP-to-Maildir email sync

Usage: jma [OPTIONS] [COMMAND]

Commands:
  sync       Run bidirectional sync (default if no command given)
  pull       One-way sync: server -> local only
  push       One-way sync: local -> server only
  watch      Daemon mode: continuous sync on server + local changes
  init       Initialize config file and local maildir structure
  mailboxes  List remote mailboxes and their local mapping
  status     Show sync staleness, cursor health, and maildir drift
  auth       Manage account credentials and the JMAP discovery cache
  help       Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>  Config file path [default: ~/.config/jma/config.toml]
  -v, --verbose...       Increase verbosity: -v info, -vv debug, -vvv all crates, -vvvv trace
  -n, --dry-run          Show what would be done without making changes
  -q, --quiet            Suppress all output except errors
  -h, --help             Print help
  -V, --version          Print version
```

### init : Initialize config

`init` is what you should run first to both create a default config on disk and create the local Maildir structure for your mailboxes (if you don't already have it). See [Configuration](#configuration) above for more details on the config.

### pull : Pull upstream changes into your local Maildir

`pull` pulls down all upstream changes into your local Maildir mailboxes. Run this subcommand if you only want to download mail (e.g. if you want to archive your Mailboxes locally). If there are any conflicts however, your config's `conflict_strategy` will be applied.

### push : Push local Maildir state to upstream

`push` pushes all local Maildir state (new messages, flag changes, etc.) to your upstream server. If there are any conflicts however, your config's `conflict_strategy` will be applied.

### sync : Bi-Directional Sync

`sync` is generally the command you want, and is equivalent to running `mbsync -a` or `mbsync` against a specific channel. By default, if you run `jma` without any subcommands it is equivalent to running `jma sync`.

```console
$ jma sync
Running jma version 0.1.0
Syncing 7 mailboxes
Downloading 4 messages
Sync complete (4 downloaded, 1 uploaded, 2 flag updates, 0 moved, 0 deleted)
```

The summary line tallies what landed this cycle, summed across both directions where applicable.

### watch : Push email and continuous sync

`watch` is the daemon mode: it does an initial bidirectional sync, then stays running and re-syncs whenever it sees a change on either side. Useful as a long-running process under launchd, systemd-user, or a tmux pane. `Ctrl-C` (or any `SIGTERM`/`SIGINT`) exits.

Two trigger sources feed it:

  * **JMAP EventSource (SSE).** A long-running HTTPS connection to your server's push endpoint. The server pushes a `state` event whenever an entity (Email, Mailbox) advances; `jma` runs a sync cycle in response. This is what gives you push email.
  * **Filesystem watcher.** Watches the maildir root via the OS's native filesystem-events API (`inotify` on Linux, `FSEvents` on macOS). Local edits (a message marked read by your MUA, a move between folders, a delete) trigger a sync cycle so changes propagate upstream. Events are debounced and coalesced before triggering a cycle so a burst of edits collapses into one sync.

After every cycle that downloaded new mail, the optional `[watch].post_arrival_command` shell command runs (use this to kick off `mu index`, `notmuch new`, etc.).

#### Reconnect on failure

The daemon recovers from two classes of failure on its own:

  * **Persistent JMAP transport errors.** If a sync cycle exhausts its in-call retry budget against a 5xx storm or sustained connectivity loss, the daemon rebuilds the JMAP session (re-resolving the bearer token from the keychain on the way) and continues. Reconnect attempts grow exponentially up to 60 seconds between tries; backoff resets on the first successful sync after recovery. Hard errors (e.g. a 401 from a rotated bearer token) propagate so the daemon dies loudly rather than spinning -- if you see this, fix the credential and restart.
  * **Silently-dead SSE streams.** The daemon expects regular events on the EventSource: state changes when there's activity, periodic pings otherwise. If no event of any kind arrives within the negotiated ping interval plus 5 seconds of slack, the stream is treated as dead -- the usual failure mode after a laptop wakes from sleep, a NAT entry expires, or a proxy times the connection out without closing it -- and the daemon reconnects. Per RFC 8620 section 7.3 the server picks the actual ping interval (it MAY clamp our requested `[watch].ping_interval` upward) and reports it on each ping event; that reported value is what the watchdog uses. Until the first ping arrives, the watchdog budgets the spec-mandated maximum of 300 seconds, so a slow first ping won't cause a spurious reconnect.

### mailboxes : Status info

`mailboxes` is a read-only subcommand that shows the mapping between your server's mailboxes and your own local Maildir mailboxes. It also displays which mailbox corresponds to which role (drafts, sent, etc):

```console
$ jma mailboxes
On-disk name                                Total   Unread  Role
----------------------------------------------------------------------
* INBOX                                       556        3  inbox
* Archive                                      71        0  archive
* Drafts                                        8        0  drafts
* Misc                                          3        0
* Priority                                      0        0
* Sent                                          1        0  sent
* Spam                                        431        6  junk
* Trash                                         0        0  trash

* = synced

```

### status : Sync staleness, cursor health, and maildir drift

`status` is a read-only, offline subcommand for the questions that aren't answered by `mailboxes` (which goes to the server) or by counting files on disk. It opens the state DB without taking any locks, so it's safe to run alongside an in-progress `sync` or `watch`.

```console
$ jma status
Configured account: foo@example.com
Maildir root: /Users/foo/Mail
State DB: /Users/foo/Mail/.jma.db (132.4 KB, schema v1)

JMAP cursors (account a1b2c3d4e):
  Last cursor write: 2026-05-06 14:22:01 (12m ago)
  Email cursor: healthy
  Mailbox cursor: healthy

Maildir vs DB drift (under /Users/foo/Mail):
  Folders on disk not in DB: SomeNewFolder
  Folders in DB not on disk: (none)
```

What each section is for:

  * **Last cursor write.** "When did the JMAP cursor for this account last advance?" Answers the staleness question without going online. The semantic is "last cursor write," not "last sync attempt completed" -- a sync run that produces no state change is invisible here, since we don't write a row when there's nothing to write. Reasonable proxy for "is this account stale, should I run sync?" but not a sync-attempt log.
  * **Cursor health.** Each tracked entity (`Email`, `Mailbox`) is either `healthy` (a state cookie is set) or `FORCED RESYNC`. The latter means the cursor was tripped by a `cannotCalculateChanges` error from the server, and the next mutating run (`sync`/`pull`/`watch`) will do a full re-pull instead of a delta. Surfaced here so a slow next cycle isn't a surprise.
  * **Maildir vs DB drift.** Folders on disk that aren't in `mailbox_map`, and folders in `mailbox_map` that aren't on disk. Diagnostic for "I `mkdir`'d a folder, why hasn't jma noticed" and the symmetric "the DB thinks I have a folder I don't." Compares immediate non-hidden subdirectories of the maildir root.
  * **DB metadata.** Path, size, and schema version of the state DB. Useful for bug reports and for noticing when the DB has gotten unexpectedly large.

The JMAP account ID is shown raw rather than mapped to an email -- mapping back would need either a JMAP roundtrip (which would defeat the offline-capable design) or a new column to remember the email-to-ID binding. The configured email prints once at the top so you can match the section to the account it belongs to.

### auth : Manage account credentials and the JMAP discovery cache

`auth` is a small set of administrative subcommands for the OS keychain entries and the JMAP discovery cache. All three actions take `--account <email>`, which scopes them to a specific account by its `[account].email`. The `auth` subcommands don't read the config file, so a typo in `--account` isn't caught here -- the next sync run will report "no token for ..." instead.

  * `auth set-token --account <email>` -- Read a bearer token from stdin and store it in the OS keychain (macOS Keychain, Linux Secret Service, Windows Credential Manager) under that email. See [Authentication and Account `[account]`](#authentication-and-account-account) above for how the stored entry feeds into sync.
  * `auth clear-token --account <email>` -- Remove the bearer token for that email from the OS keychain. Idempotent; clearing a non-existent entry is a no-op.
  * `auth rediscover --account <email>` -- Clear the cached JMAP session URL for this account's email domain and re-run autodiscovery (DNS SRV `_jmap._tcp.<domain>`, then `/.well-known/jmap`), printing the result. Use this when your provider changes their session endpoint. If `[account].session_url` is set explicitly in the config, sync bypasses the discovery cache anyway -- `rediscover` still updates the cache, but the new value only takes effect once you remove the override; the command warns you about this.

`auth` does not take the maildir or state-DB locks, so it can run alongside a `watch` daemon on the same account. Token rotations land in the keychain immediately; the running daemon will pick the new token up on its next reconnect (see [watch](#watch--push-email-and-continuous-sync)).

## A short note on this project's name

Originally, I named this project `jmapsync` -- short, descriptive, and evocative of the original inspiration for it, [`isync`/`mbsync`](https://isync.sourceforge.io/mbsync.html). However, I found out a bit later that there is already [an existing project called `jmapsync`](https://codeberg.org/derat/jmapsync) that basically does the same thing, although this project does have a different featureset. So, might as well rename. 

You can honestly think of `jma` as either meaning "JMAP Mail Agent" or "JM's Mail Agent". Either works :)

## Copyright, License

Copyright (C) JM Ibañez 2026. This project is licensed under the [BSD 3-clause license](LICENSE).

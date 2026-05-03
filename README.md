# jmapsync: mbsync, but for JMAP

`jmapsync` syncs your local Maildir mailboxes with a [JMAP](https://jmap.io) mail server such as Fastmail, like [`isync`/`mbsync`](https://isync.sourceforge.io/mbsync.html) but for JMAP instead of IMAP.

`jmapsync` supports syncing a Maildir mailbox that was downloaded via `mbsync` or `offlineimap` -- point it to the directory containing your Maildir mailboxes and it should pick up where `mbsync` or `offlineimap` left off.

This project is in a very early state, though I'm using it for my own mail. **Use it at your own risk**.


## Quick Start

```console

# Pre-populate the config file, and initialize a Maildir structure in ~/Mail/Fastmail (the default) if it doesn't exist
$ jmapsync init

# Edit the config file in ~/.config/jmapsync/config.toml

# Run a bidirectional sync (below invocation is equivalent to running `jmapsync sync`)
$ jmapsync

# ... Or, if you want push email plus watching for any local changes to sync to upstream
$ jmapsync watch
```


## Installation

Currently, this project doesn't yet have releases. You need to install this manually via Cargo:

```shell
cargo install --path .
```

## A note on mailboxes

`jmapsync` respects the naming of your mailboxes, with the exception of inboxes. To match the behavior of `mbsync`, `jmapsync` will use the Maildir mailbox named `INBOX` against the JMAP folder with the `inbox` role. This means that if your folder is localized on your provider, your local Maildir inbox will always be named `INBOX` regardless.

Note this also applies to the names of the mailboxes in `[sync].mailboxes` (see below), so if your mailbox is e.g. named `Posteingang` it will be synced locally as `INBOX`.

## How jmapsync Tracks State

`jmapsync` tracks state between your local Maildirs and the upstream JMAP server on an SQLite database in `[state].db_path` (by default in `~/.local/share/jmapsync/state.db`). State is intentionally disposable: you should be able to delete its state and rerun a `jmapsync pull` to reconverge.

If you delete `state.db` and run `jmapsync pull`, `jmapsync` will re-walk the server. It parses the `Message-ID` of each existing local message and "adopts" messages if they exist on the server, instead of re-downloading them. If you previously killed `jmapsync` in the middle of a previous initial `pull` and then deleted `state.db`, `jmapsync` does The Right Thing and continues where it left off (i.e. it doesn't redownload previously downloaded messages).

So what's stored in `state.db`?

  * A bidirectional `message_map`, mapping between a JMAP email and its local maildir file. `jmapsync` stores the name of the local Maildir file and its corresponding `message_id` as a fast lookup cache
  * A mapping between JMAP mailbox IDs and local Maildir folder names in `mailbox_map`, including role and parent
  * `jmap_state` containing per-entity sync cursors so `jmapsync` only needs to ask for changes since the last sync
  * And `local_state`, which is a snapshot of the local state (flags, size, mtime) so any local filesystem changes are quickly detected

None of `state.db`'s contents are required to do a `pull`.

### Breaking Changes

If there are any changes that break state tracking, as mentioned above you can simply delete the state DB and re-run `jmapsync`.

### Concurrency: One Mutator at a Time

Mutating commands (`sync`, `pull`, `push`, `watch`) take an exclusive OS-level advisory lock on `<db_path>.lock` (e.g. `~/.local/share/jmapsync/state.db.lock`) before doing any work. If another instance already holds the lock, the second invocation fails fast with a message naming the holder's PID. This prevents two instances from racing on the SQLite state DB or double-uploading local-only messages to the server.

Read-only commands (`status`, `mailboxes`) and commands that don't touch the state DB (`init`, `auth`) do **not** take the lock and can run alongside a `watch` daemon.

The lock is held by the kernel, not by the contents of the file, so it's released automatically when the holding process exits — including on crash or `kill -9`. The `.lock` file itself may stick around on disk; that's fine, the next invocation will reuse it. You should never need to delete it by hand, but it's safe to do so when no jmapsync process is running.


## Configuration

Configuration for `jmapsync` lives in `~/.config/jmapsync/config.toml`. The key knobs to put in are your account details, the path to your Maildir mailboxes, and which mailboxes to sync.

### Authentication and Account `[account]`

`jmapsync` currently only supports Bearer tokens, AKA API tokens. On Fastmail, go to **Settings -> Privacy & Security (under Account) -> Manage API tokens** to generate a token, then provide it to `jmapsync` in one of three ways (checked in this order):

  1. The `JMAPSYNC_TOKEN` environment variable. Best for CI and scripted use.
  2. The OS-native secret store, via `jmapsync auth set-token`. The token goes into macOS Keychain, Linux Secret Service (D-Bus), or Windows Credential Manager — never on disk in the clear. Recommended for interactive use. Pipe a token in (`pbpaste | jmapsync auth set-token`) or let it prompt. `jmapsync auth clear-token` removes it.
  3. The `token` key in the `[account]` section of the config file. Plaintext fallback for headless servers (no Keychain, no D-Bus session bus) and CI environments where the env var isn't a fit.

Other `[account]` keys:

  * `email`: The email address for this account. Required. The domain part is used to discover the JMAP session URL via DNS SRV (`_jmap._tcp.<domain>`) and `/.well-known/jmap`, per RFC 8620 section 2.2.
  * `session_url`: Optional explicit JMAP session URL. When unset (the default), the URL is autodiscovered from the `email` domain via DNS SRV and `/.well-known/jmap`, and the result is cached in the state DB. Set this only to override autodiscovery -- e.g. when your provider doesn't publish the discovery records, or to pin a specific endpoint during testing. To force a re-discovery (for example, when your provider changes their session endpoint), run `jmapsync auth rediscover`.

### Sync `[sync]`

This section configures which Maildirs `jmapsync` will sync to, and how it syncs. The important knobs here are `maildir_path` which should point to the root of your Maildir mailboxes that you want to sync (e.g. `~/Mail/Fastmail`, or `~/Mail/my-provider`). By default `jmapsync init` will also populate `mailboxes` with the common IMAP/JMAP mailboxes (INBOX, Archive, Sent, Drafts, Trash) and none of your custom folders/mailboxes -- if you want to populate _all_ mailboxes from upstream, unset this key or set it to an empty list.

  * `maildir_path`: The path to the Maildir root you want to sync
  * `mailboxes`: The specific mailboxes you want to sync. Set this to `[]` (an empty list) or leave this empty to sync all mailboxes. Note that this respects the inbox role; if your upstream mailbox with the inbox role is named e.g. `Posteingang` (DE) it will be synced to the local folder `INBOX`.
  * `case_insensitive_match`: When checking mailbox names against upstream, it can happen that your local Maildirs do not match because of different cases (e.g. when you're on a case-insensitive, case preserving filesystem, and your mailbox is named `foo` but upstream is saved as `Foo`). Set this to `true` to ignore case; default is to respect case.
  * `download_concurrency`: How many blobs (messages) to download concurrently. By default this is 8, but it is clamped by what your upstream specifies as its maximum supported concurrent requests (i.e. whichever is lower wins)
  * `conflict_strategy`: How to resolve conflicts:
     * `server-wins` -- The state on the server wins. Any local changes are discarded
     * `local-wins`  -- Your local Maildir state wins. The server is updated to reflect your local Maildir
  
### State `[state]`

This section only has one key, `db_path`, which tells `jmapsync` where to persist its state.

### Push Email Config `[watch]`

`jmapsync` supports JMAP's EventSource for push emails. This section has knobs around that feature. You should probably leave the defaults in, but if you want to tweak things this is the place:

  * `post_arrival_command`: A shell command that `jmapsync` will invoke when it observes new mail. If you use a mail indexer such as `mu` or `notmuch`, put the indexing command here -- e.g. set this to `notmuch new` for `notmuch`.
  * `debounce_secs`: How long in seconds to coalesce filesystem events together. A smaller value means `jmapsync` will more readily sync any local changes, at the cost of chattier updates. A larger value means `jmapsync` will wait approximately that long and batch all local changes in that timeframe.
  * `ping_interval`: How often will the upstream send a ping to `jmapsync`. Tweak this if you observe buffering proxies along your connection that delay delivery of notifications.

## Commands and Options

Note: By default, invoking `jmapsync` without any subcommands runs `sync`.

```console
Bidirectional JMAP-to-Maildir email sync

Usage: jmapsync [OPTIONS] [COMMAND]

Commands:
  sync       Run bidirectional sync (default if no command given)
  pull       One-way sync: server -> local only
  push       One-way sync: local -> server only
  watch      Daemon mode: watch for push events + local changes, sync continuously
  init       Initialize config file and local maildir structure
  status     Show sync state info
  mailboxes  List remote mailboxes and their local mapping
  help       Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>  Config file path [default: ~/.config/jmapsync/config.toml]
  -v, --verbose...       Increase logging verbosity (-v, -vv, -vvv)
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

`sync` is generally the command you want, and is equivalent to running `mbsync -a` or `mbsync` against a specific channel. By default, if you run `jmapsync` without any subcommands it is equivalent to running `jmapsync sync`.

### status, mailboxes : Status info

`status` and `mailboxes` are read-only status subcommands. `status` shows what `jmapsync` thinks of the current sync state -- which mailboxes are already synced from the last time a sync was ran, and which mailboxes need to be synced as they have local changes. Run `status` if you need to get an idea of what mailboxes `sync` will operate on.

`mailboxes` shows the mapping between your server's mailboxes and your own local Maildir mailboxes. It also displays which mailbox corresponds to which role (drafts, sent, etc):

```console
$ jmapsync mailboxes
2026-04-28T07:09:08.040128Z  INFO jmapsync::jmap::session: Connecting to JMAP server at https://api.fastmail.com/jmap/session
2026-04-28T07:09:08.781767Z  INFO jmapsync::jmap::session: JMAP session established for foo@example.com
2026-04-28T07:09:09.473870Z  INFO jmapsync::jmap::mailbox: Fetched 8 mailboxes (state: J391861)
Name                                        Total   Unread  Role
----------------------------------------------------------------------
* Inbox                                       556        3  inbox
* Archive                                      71        0  archive
* Drafts                                        8        0  drafts
* Misc                                          3        0
* Priority                                      0        0
* Sent                                          1        0  sent
* Spam                                        431        6  junk
* Trash                                         0        0  trash

* = synced

```


## Copyright, License

Copyright (C) JM Ibañez 2026. This project is licensed under the [BSD 3-clause license](LICENSE).

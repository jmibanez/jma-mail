# CLAUDE.md

Operational rules for Claude Code working in this repository. For
project architecture, idempotency model, JMAP boundary quirks, and a
detailed walk through `src/sync/`, read `DEVELOPMENT.md` — it is the
single source of truth for "how this codebase works." Don't duplicate
that material here.

## Build / test commands

`aws-lc-sys` (transitive via `jmap-client`'s rustls) doesn't build
under gcc on macOS. **Always run cargo with `CC=/usr/bin/cc`** if
`CC` is set to anything other than clang:

```
CC=/usr/bin/cc cargo check
CC=/usr/bin/cc cargo test
CC=/usr/bin/cc cargo build --release
```

Single test: `CC=/usr/bin/cc cargo test --lib maildir_ops::headers::tests::parses_folded_value`.

If rust-analyzer / flymake reports diagnostics that contradict a
clean `cargo check`, trust `cargo check` — the LSP cache goes stale
across large multi-file refactors. Re-run `cargo check` to confirm.

## Working files

Only operate on files that have been checked in, unless you're
creating a new file or have been instructed explicitly to operate on
a specific file. Untracked files may be the user's in-progress work
or contain secrets — leave them alone.

## Keeping DEVELOPMENT.md current

`DEVELOPMENT.md` is only useful if it tracks the code. Update it in
the same commit as any architecture-level change — module
restructures, function or type renames the doc references,
sync-pipeline reshapes, schema-version bumps, lock-ordering tweaks,
new load-bearing modules. When you notice existing drift, fix it
inline rather than letting it accumulate. Periodic catch-up audits
are a fallback, not the primary mechanism.

## Commit conventions

LKML-style commit messages: subsystem-prefixed subject (`sync:`,
`jmap:`, `state:`, `maildir:`, `daemon:`), blank line, body that
explains the **problem before the fix**, wrapped at ~72 chars. One
logical change per commit. See `git log` for examples.

**Always run `cargo fmt` before committing** so the diff stays
formatting-noise-free.

## Referring to commits

When mentioning a commit in conversation, summaries, or commit
bodies, identify it by its **commit ID** (short SHA is fine). Don't
refer to commits by their ordinal position in a session plan
("Commit 3"), by a task number that produced them ("the commit from
task #15"), or by any other handle that only resolves inside the
ephemeral context of one session. Those references are dead weight to
anyone — including future-you — reading the history without that
session's plan in hand.

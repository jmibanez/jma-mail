//! Explicit, targeted flag authority without running the ordinary sync pipeline.
//!
//! Inspection is SELECT-only. Apply uses only observation/history and flag
//! actions; it never advances an account cursor or repairs placement. Failed
//! explicit effects retain review, requiring an explicit rerun (or fresh
//! agreement), not reinterpretation under the global conflict policy.

use anyhow::{Context, Result, bail, ensure};
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use crate::config::Config;
use crate::domain::{MailboxFolderBinding, MailboxObject, MaybeReference};
use crate::ids::{JmapAccountId, JmapEmailId, MaildirId, MessageId};
use crate::jmap::{email, limits, mailbox};
use crate::maildir_ops::flags::{flags_to_keyword_patch, keywords_to_flags, merge_mapped_flags};
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::{layout, sentinel};
use crate::state::flag_sync::{self, FlagSyncState};
use crate::state::queries::{self, MailboxRecord, MessageRecord};
use crate::sync::engine::SyncOutcome;
use crate::sync::execute::Executor;
use crate::sync::plan::{BoundId, SyncAction, SyncPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum FlagAuthority {
    Local,
    Server,
}

impl fmt::Display for FlagAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Local => "local",
            Self::Server => "server",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnapshot {
    path: PathBuf,
    maildir_id: MaildirId,
    message_id: Option<MessageId>,
    raw_flags: String,
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

struct Target {
    record: MessageRecord,
    binding: Arc<MailboxFolderBinding>,
    local: LocalSnapshot,
    cached_local: (String, String),
    remote: crate::jmap::types::EmailObject,
    history: Option<FlagSyncState>,
}

/// A non-durable preview. Private inputs prevent callers from substituting
/// arbitrary actions or changing authority after inspection. Apply consumes it
/// and revalidates files, bindings, history, selection and Email state.
pub struct FlagRepair {
    authority: FlagAuthority,
    account_id: JmapAccountId,
    api_url: String,
    planning_state: String,
    root: PathBuf,
    targets: Vec<Target>,
}

impl fmt::Display for FlagRepair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Flag repair preview: authority={} (six mapped flags only)",
            self.authority
        )?;
        for target in &self.targets {
            let desired = target.desired(self.authority);
            let remote_flags = keywords_to_flags(&target.remote.keywords);
            writeln!(
                f,
                "{} (<{}>)",
                target.record.jmap_email_id,
                anchor(&target.record.message_id)
            )?;
            writeln!(
                f,
                "  local: {:?}; raw suffix={:?}",
                target.local.path, target.local.raw_flags
            )?;
            let keywords: BTreeMap<_, _> = target.remote.keywords.iter().collect();
            writeln!(
                f,
                "  fresh remote: flags={remote_flags:?}; keywords={}",
                serde_json::to_string(&keywords).map_err(|_| fmt::Error)?
            )?;
            match &target.history {
                Some(history) => writeln!(
                    f,
                    "  history: baseline={}; review={}; blocked={}; receipt={}; error={:?}",
                    history
                        .baseline
                        .as_ref()
                        .map_or_else(|| "NULL".into(), |b| format!("{b:?}")),
                    history.review,
                    history.blocked,
                    serde_json::to_string(&history.attempt).map_err(|_| fmt::Error)?,
                    history.error
                )?,
                None => writeln!(
                    f,
                    "  history: legacy/unverified; baseline=NULL; cached candidates: local={:?}, server={:?} (not acknowledged)",
                    target.cached_local.1, target.record.flags
                )?,
            }
            writeln!(
                f,
                "  target={desired:?}; effect={}",
                if flag_sync::mapped_flags(&target.local.raw_flags) == remote_flags {
                    "none (acknowledge agreement)"
                } else if self.authority == FlagAuthority::Local {
                    "conditional remote keyword patch"
                } else {
                    "snapshot-checked local flag rename"
                }
            )?;
        }
        write!(
            f,
            "No body, mailbox, placement or account-cursor changes. Use --apply to apply; interrupted repairs require an explicit rerun."
        )
    }
}

impl Target {
    fn desired(&self, authority: FlagAuthority) -> String {
        match authority {
            FlagAuthority::Local => flag_sync::mapped_flags(&self.local.raw_flags),
            FlagAuthority::Server => keywords_to_flags(&self.remote.keywords),
        }
    }

    fn observation(&self) -> SyncAction {
        SyncAction::ObserveMessage {
            id: self.id(),
            binding: self.binding.clone(),
            keywords: self.remote.keywords.clone(),
            filename_flags: self.local.raw_flags.clone(),
            jmap_blob_id: self.record.jmap_blob_id.clone(),
            jmap_thread_id: self.record.jmap_thread_id.clone(),
        }
    }

    fn id(&self) -> BoundId {
        BoundId {
            jmap_email_id: self.record.jmap_email_id.clone(),
            maildir_id: self.local.maildir_id.clone(),
            message_id: self.record.message_id.clone(),
        }
    }
}

/// Fresh metadata only: capture the Email state BEFORE the target gets. No
/// dedupe, mailbox resolution writes, body transfers or ordinary engine run.
pub async fn inspect(
    client: &Client,
    conn: &Connection,
    config: &Config,
    ids: &[JmapEmailId],
    authority: FlagAuthority,
) -> Result<FlagRepair> {
    ensure!(
        !ids.is_empty(),
        "Select at least one explicit JMAP Email ID"
    );
    let mut unique = HashSet::new();
    ensure!(
        ids.iter()
            .all(|id| !id.as_ref().is_empty() && unique.insert(id)),
        "Email IDs must be non-empty and unique"
    );
    let root = config.canonical_maildir_root()?;
    let planning_state = email::get_current_state(client).await?;
    ensure!(
        !planning_state.is_empty(),
        "Server returned an empty Email state"
    );
    let (live, _) = mailbox::get_all(client).await?;
    let remote = email::get_by_ids(client, ids).await?;
    let cached = queries::get_all_mailboxes(conn)?;
    let inventory = inventory(&root, &cached)?;
    let validation = Validation {
        client,
        conn,
        config,
        root: &root,
        cached: &cached,
        live: &live,
        inventory: &inventory,
    };
    let mut targets = Vec::new();
    for id in ids {
        let matches: Vec<_> = remote.iter().filter(|email| &email.id == id).collect();
        ensure!(
            matches.len() == 1,
            "Email {id} is missing or ambiguous on the server"
        );
        targets.push(
            validation
                .target(matches[0].clone())
                .with_context(|| format!("Refusing flag repair for {id}"))?,
        );
    }
    Ok(FlagRepair {
        authority,
        account_id: client.default_account_id().into(),
        api_url: client.session().api_url().to_owned(),
        planning_state,
        root,
        targets,
    })
}

impl FlagRepair {
    /// The CLI must hold the usual maildir-then-DB mutator locks. Invalid or
    /// stale targets fail before any history/effect is written. A later I/O or
    /// ACK failure can leave partial progress; review protects unfinished work.
    pub async fn apply(
        self,
        client: Arc<Client>,
        conn: &Connection,
        config: &Config,
    ) -> Result<SyncOutcome> {
        ensure!(
            client.default_account_id() == self.account_id.as_ref()
                && client.session().api_url() == self.api_url,
            "Repair server/account changed since inspection"
        );
        ensure!(
            config.canonical_maildir_root()? == self.root,
            "Repair maildir root changed since inspection"
        );
        let (live, _) = mailbox::get_all(&client).await?;
        let cached = queries::get_all_mailboxes(conn)?;
        let inventory = inventory(&self.root, &cached)?;
        let validation = Validation {
            client: &client,
            conn,
            config,
            root: &self.root,
            cached: &cached,
            live: &live,
            inventory: &inventory,
        };
        for target in &self.targets {
            let current = validation.target(target.remote.clone())?;
            ensure!(
                current.local == target.local
                    && current.history == target.history
                    && current.cached_local == target.cached_local
                    && same_record(&current.record, &target.record)
                    && current.binding == target.binding,
                "Email {} changed since inspection; rerun repair preview",
                target.record.jmap_email_id
            );
        }
        ensure!(
            email::get_current_state(&client).await? == self.planning_state,
            "Email state changed since inspection; rerun repair preview"
        );

        let mut plan = SyncPlan {
            actions: Vec::new(),
            new_email_state: None,
        };
        for target in self.targets {
            let desired = target.desired(self.authority);
            let local = flag_sync::mapped_flags(&target.local.raw_flags);
            let remote = keywords_to_flags(&target.remote.keywords);
            let mut history = target.history.clone().unwrap_or_default();
            if local == remote {
                history = FlagSyncState {
                    baseline: Some(desired.clone()),
                    ..Default::default()
                };
            } else {
                // Explicit authority is not the global conflict strategy. Keep
                // review until an actual effect/ACK clears it, including after
                // SAME-DB reopen with the opposite global policy.
                history.review = true;
                history.blocked = true;
                history.error = Some(
                    "Explicit flag repair unfinished; rerun repair-flags with an authority".into(),
                );
            }
            plan.actions.push(SyncAction::RecordFlagState {
                id: target.id().as_remote(),
                state: history,
            });
            plan.actions.push(target.observation());
            if local == remote {
                continue;
            }
            match self.authority {
                FlagAuthority::Local => plan.actions.push(SyncAction::UpdateRemoteKeywords {
                    id: target.id().as_remote(),
                    keywords: flags_to_keyword_patch(&desired),
                    filename_flags: target.local.raw_flags.clone(),
                }),
                FlagAuthority::Server => plan.actions.push(SyncAction::UpdateLocalFlags {
                    id: target.id(),
                    binding: target.binding.clone(),
                    expected_flags: target.local.raw_flags.clone(),
                    new_flags: merge_mapped_flags(&target.local.raw_flags, &desired),
                    keywords: target.remote.keywords.clone(),
                    jmap_blob_id: target.remote.blob_id.clone(),
                    jmap_thread_id: target.remote.thread_id.clone(),
                }),
            }
        }
        // This plan contains ONLY the four flag/observation variants above.
        // None withholds the global cursor: we examined only selected targets.
        Executor::new(client, conn, config, None)?
            .execute(plan, Some(&self.planning_state))
            .await
    }
}

fn anchor(id: &MessageId) -> &str {
    id.as_ref().trim_matches(['<', '>'])
}

fn same_record(a: &MessageRecord, b: &MessageRecord) -> bool {
    a.jmap_email_id == b.jmap_email_id
        && a.maildir_id == b.maildir_id
        && a.jmap_mailbox_id == b.jmap_mailbox_id
        && a.message_id == b.message_id
        && a.jmap_blob_id == b.jmap_blob_id
        && a.jmap_thread_id == b.jmap_thread_id
        && a.flags == b.flags
        && a.jmap_keywords == b.jmap_keywords
}

struct Validation<'a> {
    client: &'a Client,
    conn: &'a Connection,
    config: &'a Config,
    root: &'a Path,
    cached: &'a [MailboxRecord],
    live: &'a [MailboxObject],
    inventory: &'a [LocalSnapshot],
}

impl Validation<'_> {
    fn target(&self, remote: crate::jmap::types::EmailObject) -> Result<Target> {
        let record = queries::get_message_by_jmap_id(self.conn, &remote.id)?
            .context("Email has no cached binding")?;
        let mid = record
            .maildir_id
            .as_ref()
            .context("Email has no bound local identity")?;
        let mailbox = self
            .cached
            .iter()
            .find(|mb| mb.jmap_mailbox_id == record.jmap_mailbox_id)
            .context("Binding has no cached mailbox")?;
        ensure!(
            self.cached
                .iter()
                .filter(|mb| mb.maildir_folder == mailbox.maildir_folder)
                .count()
                == 1,
            "Cached folder has multiple mailbox identities"
        );
        let by_id: HashMap<_, _> = self.live.iter().map(|mb| (mb.id.clone(), mb)).collect();
        let live = by_id
            .get(&record.jmap_mailbox_id)
            .context("Bound mailbox is no longer live")?;
        let paths = mailbox::build_remote_paths(&by_id);
        let inputs: Vec<_> = self
            .live
            .iter()
            .map(|mb| mailbox::MailboxSelectionInput {
                path: &paths[&mb.id],
                role: mb.role.as_deref(),
            })
            .collect();
        let selected = mailbox::get_selected_mailboxes(
            &self.config.sync.mailboxes,
            &inputs,
            self.config.sync.case_insensitive_match,
            true,
        );
        ensure!(
            selected.contains(&paths[&live.id]),
            "Bound mailbox is excluded by configured selection"
        );
        ensure!(
            layout::resolve_folder_path(
                live,
                &by_id,
                &layout::FolderLayoutDefinition::from_config(
                    self.config,
                    limits::max_size_mailbox_name(self.client)
                )
            )? == mailbox.maildir_folder,
            "Cached folder is stale or renamed; repair does not change placement"
        );
        let folder_path = checked_folder(self.root, &mailbox.maildir_folder)?;
        let mapping = sentinel::read(&folder_path)?
            .context("Missing or invalid mailbox identity sentinel")?;
        ensure!(
            mapping.jmap_mailbox_id == live.id
                && mapping.parent_jmap_mailbox_id == live.parent_id
                && mapping.server_name == live.name,
            "Mailbox sentinel disagrees with live binding"
        );
        ensure!(
            folder_path.join("cur").is_dir() && folder_path.join("new").is_dir(),
            "Partial Maildir is unsupported by flag repair; no directories will be created"
        );
        let cached_local =
            queries::get_local_state(self.conn, mid)?.context("Missing local observation")?;
        ensure!(
            cached_local.0 == mailbox.maildir_folder,
            "Local observation refers to a stale folder"
        );
        ensure!(
            remote.mailbox_ids.len() == 1 && remote.mailbox_ids.get(&live.id) == Some(&true),
            "Remote placement is moved, unsupported or ambiguous"
        );
        ensure!(
            record.jmap_blob_id.as_ref() == Some(&remote.blob_id)
                && record.jmap_thread_id.as_ref() == Some(&remote.thread_id),
            "Remote blob/thread identity differs from cached binding"
        );
        let remote_ids = remote
            .message_id
            .as_ref()
            .context("Remote Message-ID anchor is missing")?;
        ensure!(
            remote_ids.len() == 1 && anchor(&remote_ids[0]) == anchor(&record.message_id),
            "Remote Message-ID anchor disagrees with binding"
        );
        let candidates: Vec<_> = self
            .inventory
            .iter()
            .filter(|file| {
                &file.maildir_id == mid
                    || file
                        .message_id
                        .as_ref()
                        .is_some_and(|id| anchor(id) == anchor(&record.message_id))
            })
            .collect();
        ensure!(
            candidates.len() == 1,
            "Expected one live cached-folder identity candidate, found {}",
            candidates.len()
        );
        let local = candidates[0];
        ensure!(
            &local.maildir_id == mid
                && local.path.parent().and_then(Path::parent) == Some(folder_path.as_path())
                && local
                    .message_id
                    .as_ref()
                    .is_some_and(|id| anchor(id) == anchor(&record.message_id)),
            "Local file is missing, moved or has a different Message-ID anchor"
        );
        let history = flag_sync::get(self.conn, &remote.id)?;
        Ok(Target {
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value(live.id.clone()),
                maildir_folder: mailbox.maildir_folder.clone(),
                server_name: live.name.clone(),
                remote_path: paths[&live.id].clone(),
            }),
            record,
            local: local.clone(),
            cached_local,
            remote,
            history,
        })
    }
}

fn checked_folder(root: &Path, folder: &str) -> Result<PathBuf> {
    ensure!(
        !folder.is_empty()
            && Path::new(folder)
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "Unsafe cached maildir folder {folder:?}"
    );
    let path = root.join(folder);
    if path.exists() {
        ensure!(
            path.canonicalize()?.starts_with(root),
            "Cached maildir folder escapes configured root"
        );
    }
    Ok(path)
}

/// Conservative cached-folder inventory, including excluded folders: another
/// live candidate is a refusal, never an adoption/dedupe decision. Header
/// anchors are not content digests. Symlinks are refused, not followed.
fn inventory(root: &Path, mailboxes: &[MailboxRecord]) -> Result<Vec<LocalSnapshot>> {
    let mut files = Vec::new();
    let mut folders = HashSet::new();
    for mailbox in mailboxes {
        if !folders.insert(&mailbox.maildir_folder) {
            continue;
        }
        let folder = checked_folder(root, &mailbox.maildir_folder)?;
        for subdir in ["cur", "new"] {
            let directory = folder.join(subdir);
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("Cannot inspect {}", directory.display()));
                }
            };
            ensure!(
                directory.canonicalize()?.starts_with(root),
                "Maildir subdirectory escapes configured root"
            );
            for entry in entries {
                let entry = entry?;
                let metadata = entry.path().symlink_metadata()?;
                ensure!(
                    !metadata.file_type().is_symlink(),
                    "Symlink in Maildir inventory: {:?}",
                    entry.path()
                );
                if !metadata.is_file() {
                    continue;
                }
                let name = entry.file_name();
                let name = name.to_str().context("Non-UTF-8 Maildir filename")?;
                let (id, raw_flags) = name.split_once(":2,").unwrap_or((name, ""));
                if id.is_empty() {
                    bail!("Empty Maildir identity");
                }
                files.push(LocalSnapshot {
                    path: entry.path(),
                    maildir_id: id.into(),
                    raw_flags: raw_flags.into(),
                    message_id: parse_message_id_from_file(&entry.path())?,
                    len: metadata.len(),
                    modified: metadata.modified()?,
                    #[cfg(unix)]
                    device: {
                        use std::os::unix::fs::MetadataExt;
                        metadata.dev()
                    },
                    #[cfg(unix)]
                    inode: {
                        use std::os::unix::fs::MetadataExt;
                        metadata.ino()
                    },
                });
            }
        }
    }
    Ok(files)
}

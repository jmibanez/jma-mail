use anyhow::{Context, Result};
use maildir::Maildir;
use std::path::Path;
use tracing::debug;

/// Ensure a maildir folder exists with cur/new/tmp subdirectories.
pub fn ensure_maildir(path: &Path) -> Result<Maildir> {
    let md = Maildir::from(path.to_path_buf());
    md.create_dirs()
        .with_context(|| format!("Failed to create maildir dirs at {}", path.display()))?;
    Ok(md)
}

/// Store a raw email message into a maildir's cur/ with the given flags.
/// Returns the maildir unique ID assigned to the message.
pub fn store_message(maildir: &Maildir, data: &[u8], flags: &str) -> Result<String> {
    let id = maildir
        .store_cur_with_flags(data, flags)
        .context("Failed to store message in maildir")?;
    debug!("Stored message {} with flags '{}'", id, flags);
    Ok(id)
}

/// Delete a message from a maildir by its unique ID.
pub fn delete_message(maildir: &Maildir, id: &str) -> Result<()> {
    maildir
        .delete(id)
        .with_context(|| format!("Failed to delete message {}", id))?;
    debug!("Deleted message {}", id);
    Ok(())
}

/// Set flags on a message (replaces all existing flags).
pub fn set_flags(maildir: &Maildir, id: &str, flags: &str) -> Result<()> {
    maildir
        .set_flags(id, flags)
        .with_context(|| format!("Failed to set flags on message {}", id))?;
    debug!("Set flags '{}' on message {}", flags, id);
    Ok(())
}

/// Move a message from one maildir to another.
pub fn move_message(from: &Maildir, to: &Maildir, id: &str) -> Result<()> {
    from.move_to(id, to)
        .with_context(|| format!("Failed to move message {}", id))?;
    debug!("Moved message {} to {:?}", id, to.path());
    Ok(())
}

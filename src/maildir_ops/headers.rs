use anyhow::Result;
use mailparse::{MailHeaderMap, parse_headers};
use std::path::Path;

use crate::ids::MessageId;

/// Read a message file and extract its `Message-ID` header value.
/// The returned id has surrounding `<...>` stripped, or is the raw
/// value if no angle brackets are present.
pub fn parse_message_id_from_file(path: &Path) -> Result<Option<MessageId>> {
    // 64 KiB caps the read at a size that comfortably fits any
    // reasonable header block. We deliberately avoid routing through
    // `MailEntry::headers()` because that loads the whole file —
    // for a message with a multi-MB attachment we'd read megabytes
    // just to pull one header.
    let raw = read_header_bytes(path, 64 * 1024)?;
    Ok(parse_message_id(&raw))
}

fn read_header_bytes(path: &Path, max: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut buf = vec![0u8; max];
    let n = f.read(&mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

/// Extract the Message-ID from a byte slice that begins with the
/// message's headers. Returns the angle-bracket-stripped value, or
/// `None` if the header is missing, empty, or the byte slice contains
/// a stray CR not followed by LF. Callers treat `None` the same way
/// regardless of cause: the file lacks a usable idempotency anchor
/// and is dropped at the scan boundary.
pub fn parse_message_id(raw: &[u8]) -> Option<MessageId> {
    let (headers, _) = parse_headers(raw).ok()?;
    let value = headers.get_first_value("Message-ID")?;
    extract_msgid_value(&value)
}

fn extract_msgid_value(value: &str) -> Option<MessageId> {
    let s = value.trim();
    if let (Some(lt), Some(gt)) = (s.find('<'), s.rfind('>'))
        && gt > lt
    {
        return Some(MessageId::from(&s[lt + 1..gt]));
    }
    if s.is_empty() {
        None
    } else {
        Some(MessageId::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_message_id() {
        let raw = b"Subject: hi\r\nMessage-ID: <abc@example.com>\r\n\r\nbody";
        assert_eq!(
            parse_message_id(raw),
            Some(MessageId::from("abc@example.com"))
        );
    }

    #[test]
    fn parses_lf_only() {
        let raw = b"Subject: hi\nMessage-Id: <id-2@host>\n\nbody";
        assert_eq!(parse_message_id(raw), Some(MessageId::from("id-2@host")));
    }

    #[test]
    fn parses_folded_value() {
        let raw = b"Message-ID:\r\n <wrapped@x>\r\nSubject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), Some(MessageId::from("wrapped@x")));
    }

    #[test]
    fn missing_header_returns_none() {
        let raw = b"Subject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), None);
    }
}

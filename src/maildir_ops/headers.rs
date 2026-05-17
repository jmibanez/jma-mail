use anyhow::Result;
use mailparse::{MailHeaderMap, parse_headers};
use std::path::Path;

use crate::ids::MessageId;

/// First read size for `parse_message_id_from_file`. Comfortably fits
/// the header block of any well-traveled mailing-list post; messages
/// with longer header sections trigger the escalation path up to
/// `HEADER_HARD_CAP`.
const HEADER_SOFT_CAP: usize = 64 * 1024;

/// Hard ceiling for the header read. A message whose headers section
/// exceeds this size yields no Message-ID anchor: the parser would
/// otherwise risk handing back a value truncated mid-string, and any
/// such partial value can collide with another truncated-prefix
/// message in the dedupe index.
const HEADER_HARD_CAP: usize = 1024 * 1024;

/// Read a message file and extract its `Message-ID` header value.
/// The returned id has surrounding `<...>` stripped, or is the raw
/// value if no angle brackets are present.
///
/// The read starts at `HEADER_SOFT_CAP` and escalates up to
/// `HEADER_HARD_CAP` if the headers/body separator isn't reached in
/// the first pass -- covers long forwarded `Received:` chains and
/// other legitimately header-heavy mail without paying that cost
/// for the common case. If the headers section exceeds the hard cap
/// (pathological mail, hostile input), the file is treated as having
/// no usable anchor: handing callers a truncated Message-ID value
/// would let two distinct messages collide on the same prefix in
/// the dedupe index.
pub fn parse_message_id_from_file(path: &Path) -> Result<Option<MessageId>> {
    let raw = read_headers_section(path, HEADER_SOFT_CAP, HEADER_HARD_CAP)?;
    // Without an end-of-headers marker, the buffer is either truncated
    // at the hard cap or the file is malformed. Either way, anything
    // mailparse extracts could be a partial value -- refuse to anchor.
    if !contains_end_of_headers(&raw) {
        return Ok(None);
    }
    Ok(parse_message_id(&raw))
}

/// Read `path` until the end-of-headers marker (CRLF CRLF or LF LF)
/// is seen, escalating from `soft_cap` up to `hard_cap`. Returns the
/// bytes actually read; the caller checks whether EOH is present to
/// decide whether the parse is trustworthy.
fn read_headers_section(path: &Path, soft_cap: usize, hard_cap: usize) -> Result<Vec<u8>> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = vec![0u8; soft_cap];
    let n = read_fill(&mut f, &mut buf)?;
    buf.truncate(n);

    // EOF inside the soft cap or EOH already in hand -- no further
    // reading possible or necessary.
    if n < soft_cap || contains_end_of_headers(&buf) {
        return Ok(buf);
    }

    while buf.len() < hard_cap {
        let prior = buf.len();
        let next_len = (prior + soft_cap).min(hard_cap);
        buf.resize(next_len, 0);
        let r = read_fill(&mut f, &mut buf[prior..])?;
        buf.truncate(prior + r);
        if r == 0 {
            break;
        }
        // Overlap by 3 bytes so a CRLF CRLF straddling the chunk
        // boundary isn't missed.
        let scan_from = prior.saturating_sub(3);
        if contains_end_of_headers(&buf[scan_from..]) {
            break;
        }
    }
    Ok(buf)
}

/// Like `Read::read` but loops over short reads so a regular-file
/// short return doesn't make us think we hit EOF.
fn read_fill(f: &mut std::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    use std::io::Read;
    let mut total = 0;
    while total < buf.len() {
        match f.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

/// True iff `buf` contains the headers/body separator. Maildir files
/// on disk can be CRLF (RFC 5322 wire format kept verbatim) or
/// LF-only (some MUAs normalize) -- check both.
fn contains_end_of_headers(buf: &[u8]) -> bool {
    let crlf = b"\r\n\r\n";
    let lf = b"\n\n";
    buf.windows(crlf.len()).any(|w| w == crlf) || buf.windows(lf.len()).any(|w| w == lf)
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
        // `<>` carries no idempotency anchor; reject so empty
        // MessageIds don't collide across messages in dedupe and
        // LocalIndex.
        let inner = s[lt + 1..gt].trim();
        if inner.is_empty() {
            return None;
        }
        return Some(MessageId::from(inner));
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

    /// `<>` violates RFC 5322 (`msg-id` requires non-empty `id-left`
    /// and `id-right`). Returning `MessageId::from("")` would collide
    /// every such message onto one dedupe / LocalIndex bucket; the
    /// parser must reject it so callers see no usable anchor.
    #[test]
    fn empty_angle_brackets_returns_none() {
        let raw = b"Message-ID: <>\r\nSubject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), None);
    }

    /// Whitespace-only inside angle brackets has the same collision
    /// shape as the bare `<>` case and is treated the same.
    #[test]
    fn whitespace_only_angle_brackets_returns_none() {
        let raw = b"Message-ID: <   >\r\nSubject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), None);
    }

    /// Header present with no value at all -- same `MessageId::from("")`
    /// collision risk as `<>`, just via a different parse path
    /// (no-angle-brackets branch instead of the empty-inner-content
    /// branch).
    #[test]
    fn empty_value_returns_none() {
        let raw = b"Message-ID:\r\nSubject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), None);
    }

    /// Header present with a whitespace-only value -- trims to empty
    /// and shares the collision risk above.
    #[test]
    fn whitespace_only_value_returns_none() {
        let raw = b"Message-ID:    \r\nSubject: hi\r\n\r\nbody";
        assert_eq!(parse_message_id(raw), None);
    }

    /// CRLF CRLF is the on-wire shape; the helper must recognise it.
    #[test]
    fn end_of_headers_detects_crlf() {
        assert!(contains_end_of_headers(b"Subject: hi\r\n\r\nbody"));
    }

    /// LF LF shows up in maildir files some MUAs write without CR.
    #[test]
    fn end_of_headers_detects_lf_only() {
        assert!(contains_end_of_headers(b"Subject: hi\n\nbody"));
    }

    #[test]
    fn end_of_headers_absent_in_header_only_buffer() {
        assert!(!contains_end_of_headers(b"Subject: hi\r\nFrom: x@y"));
    }

    fn write_file(path: &std::path::Path, content: &[u8]) {
        use std::io::Write;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(content).unwrap();
    }

    /// Soft-cap exhausted but headers continue past it: the read
    /// escalates and the real Message-ID is still recoverable.
    /// Pins the M12 fix's central promise -- legitimate large-header
    /// mail (long forwarded Received chains) isn't silently dropped.
    #[test]
    fn extends_read_to_recover_message_id_past_soft_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big-headers.eml");
        let mut content = Vec::new();
        // Fill past the 1 KiB soft cap (used in test) with filler
        // Received: lines, then plant the real Message-ID after.
        for i in 0..200u32 {
            content.extend_from_slice(format!("Received: by host-{:03}.example\r\n", i).as_bytes());
        }
        content.extend_from_slice(b"Message-ID: <real-anchor@example.com>\r\n");
        content.extend_from_slice(b"Subject: hi\r\n\r\nbody");
        write_file(&path, &content);

        let raw = read_headers_section(&path, 1024, 16 * 1024).unwrap();
        assert!(contains_end_of_headers(&raw));
        assert_eq!(
            parse_message_id(&raw),
            Some(MessageId::from("real-anchor@example.com"))
        );
    }

    /// Headers section exceeds the hard cap: read stops at the cap,
    /// EOH is absent, `parse_message_id_from_file` returns None.
    /// Without this guard, a truncated Message-ID value would be
    /// returned and could collide with another file's truncated
    /// prefix in the dedupe index -- the M12 destructive path.
    #[test]
    fn refuses_to_anchor_when_headers_exceed_hard_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("monster-headers.eml");
        let mut content = Vec::new();
        // 4 KiB of filler far exceeds the 1 KiB hard cap used here.
        for i in 0..100u32 {
            content.extend_from_slice(format!("Received: by host-{:03}.example\r\n", i).as_bytes());
        }
        content.extend_from_slice(b"Message-ID: <buried@example.com>\r\n");
        content.extend_from_slice(b"Subject: hi\r\n\r\nbody");
        write_file(&path, &content);

        let raw = read_headers_section(&path, 512, 1024).unwrap();
        assert!(!contains_end_of_headers(&raw));
    }

    /// Public entry point returns `Ok(None)` on a file whose buffer
    /// never reaches end-of-headers. Pins the guard at
    /// `parse_message_id_from_file` that turns "EOH absent" into
    /// "no anchor" -- without it, a truncated Message-ID value
    /// would reach the dedupe index and risk the M12 collision.
    /// Uses a short file so the helper-cap path isn't even needed:
    /// the guard alone has to fire.
    #[test]
    fn parse_message_id_from_file_returns_none_when_eoh_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-eoh.eml");
        write_file(&path, b"Message-ID: <partial@example");
        assert_eq!(parse_message_id_from_file(&path).unwrap(), None);
    }

    /// CRLF CRLF marker landing right on a chunk boundary -- the
    /// scan must not miss it (the 3-byte overlap on the boundary
    /// scan is what recognizes the pattern), otherwise a clean
    /// message with a long header block would be punted as if its
    /// EOH never appeared.
    #[test]
    fn end_of_headers_straddles_chunk_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boundary.eml");
        let mut content = Vec::new();
        // Pick filler sized so the EOH sequence straddles the
        // 64-byte soft cap.
        content.extend_from_slice(&[b'X'; 62]);
        content.extend_from_slice(b"\r\n");
        content.extend_from_slice(b"\r\nbody");
        write_file(&path, &content);

        let raw = read_headers_section(&path, 64, 256).unwrap();
        assert!(contains_end_of_headers(&raw));
    }
}

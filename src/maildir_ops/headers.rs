use anyhow::Result;
use std::path::Path;

use crate::ids::MessageId;

/// Read a message file and extract its `Message-ID` header value.
/// The returned id has surrounding `<...>` stripped, or is the raw
/// value if no angle brackets are present.
pub fn parse_message_id_from_file(path: &Path) -> Result<Option<MessageId>> {
    // Read only enough bytes to comfortably cover header section.
    // 64 KiB is plenty for any reasonable message header block.
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

/// Parse a Message-ID header value out of a byte slice that begins with the
/// message's headers. Returns the angle-bracket-stripped value, or `None` if
/// the header is missing.
pub fn parse_message_id(raw: &[u8]) -> Option<MessageId> {
    let header_end = find_header_end(raw).unwrap_or(raw.len());
    let headers = &raw[..header_end];

    let mut i = 0;
    while i < headers.len() {
        let line_end = headers[i..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| i + p)
            .unwrap_or(headers.len());

        let mut line_slice = &headers[i..line_end];
        if line_slice.last() == Some(&b'\r') {
            line_slice = &line_slice[..line_slice.len() - 1];
        }

        if header_name_matches(line_slice, b"Message-ID") {
            // Capture the value, possibly across folded continuation lines.
            let mut value: Vec<u8> = line_slice[b"Message-ID:".len()..].to_vec();

            // Folded headers: subsequent lines starting with WSP belong to this
            // header.
            let mut j = line_end + 1;
            while j < headers.len() && (headers[j] == b' ' || headers[j] == b'\t') {
                let next_end = headers[j..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|p| j + p)
                    .unwrap_or(headers.len());
                let mut cont = &headers[j..next_end];
                if cont.last() == Some(&b'\r') {
                    cont = &cont[..cont.len() - 1];
                }
                value.push(b' ');
                value.extend_from_slice(cont);
                j = next_end + 1;
            }

            return extract_msgid_value(&value);
        }

        i = line_end + 1;
    }

    None
}

fn header_name_matches(line: &[u8], name: &[u8]) -> bool {
    if line.len() < name.len() + 1 {
        return false;
    }
    if !line[..name.len()].eq_ignore_ascii_case(name) {
        return false;
    }
    // The character right after the name must be ':' (after optional WSP).
    let mut k = name.len();
    while k < line.len() && (line[k] == b' ' || line[k] == b'\t') {
        k += 1;
    }
    k < line.len() && line[k] == b':'
}

fn extract_msgid_value(value: &[u8]) -> Option<MessageId> {
    let s = std::str::from_utf8(value).ok()?.trim();
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

/// Find the byte offset where the headers end (the empty line separator).
/// Returns the offset of the start of the body, or None if no separator found.
fn find_header_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .or_else(|| raw.windows(2).position(|w| w == b"\n\n"))
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

    #[test]
    fn find_header_end_crlf() {
        assert_eq!(find_header_end(b"a\r\n\r\nb"), Some(1));
    }

    #[test]
    fn find_header_end_lf() {
        assert_eq!(find_header_end(b"a\n\nb"), Some(1));
    }

    #[test]
    fn find_header_end_prefers_crlf_over_lf() {
        // Both separators present; CRLF CRLF must win even when LF LF
        // appears earlier in the buffer.
        assert_eq!(find_header_end(b"\n\nfoo\r\n\r\nbody"), Some(5));
    }

    #[test]
    fn find_header_end_too_short_for_crlf_falls_back_to_lf() {
        // 3 bytes can't fit \r\n\r\n; must still find \n\n.
        assert_eq!(find_header_end(b"\n\n!"), Some(0));
    }

    #[test]
    fn find_header_end_separator_at_end_of_buffer() {
        // Pins the upper boundary: a separator whose last byte is also
        // the last byte of the buffer must still be found.
        assert_eq!(find_header_end(b"abc\r\n\r\n"), Some(3));
        assert_eq!(find_header_end(b"abc\n\n"), Some(3));
    }

    #[test]
    fn find_header_end_no_separator() {
        assert_eq!(find_header_end(b""), None);
        assert_eq!(find_header_end(b"a"), None);
        assert_eq!(find_header_end(b"abc"), None);
        assert_eq!(find_header_end(b"no separator here"), None);
    }
}

//! Reversible names for NFSv4 servers exposing legacy byte names.
//!
//! RFC 7530 §12.7 calls for UTF-8 names. Some existing servers nonetheless
//! return arbitrary bytes; losing those bytes would make LOOKUP ambiguous.

const PREFIX: &str = "~nfs-rs-raw:";
const HEX: &[u8; 16] = b"0123456789ABCDEF";

use std::borrow::Cow;

pub(crate) fn display(raw: &[u8]) -> String {
    if let Ok(name) = std::str::from_utf8(raw)
        && !name.starts_with(PREFIX)
    {
        return name.to_owned();
    }
    let mut result = String::with_capacity(PREFIX.len() + 2 * raw.len());
    result.push_str(PREFIX);
    for byte in raw {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

pub(crate) fn wire(name: &str) -> Cow<'_, [u8]> {
    let Some(hex) = name.strip_prefix(PREFIX) else {
        return Cow::Borrowed(name.as_bytes());
    };
    if hex.is_empty() || hex.len() % 2 != 0 {
        return Cow::Borrowed(name.as_bytes());
    }
    let mut decoded = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let (Some(high), Some(low)) = (nibble(pair[0]), nibble(pair[1])) else {
            return Cow::Borrowed(name.as_bytes());
        };
        decoded.push((high << 4) | low);
    }
    Cow::Owned(decoded)
}

fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_legacy_and_utf8_names_without_collisions() {
        for raw in [
            &b"plain"[..],
            "中文 & #".as_bytes(),
            &b"\xd6\xd0\xce\xc4"[..],
            "~nfs-rs-raw:D6D0CEC4".as_bytes(),
        ] {
            assert_eq!(wire(&display(raw)), raw);
        }
        assert_ne!(
            display(b"\xd6\xd0\xce\xc4"),
            display("~nfs-rs-raw:D6D0CEC4".as_bytes())
        );
        assert_eq!(wire("~nfs-rs-raw:literal").as_ref(), b"~nfs-rs-raw:literal");
        assert!(matches!(wire("ordinary.txt"), Cow::Borrowed(_)));
        assert!(matches!(wire("~nfs-rs-raw:FF"), Cow::Owned(_)));
        assert_eq!(wire("~nfs-rs-raw:ff").as_ref(), b"\xff");
        assert_eq!(wire("~nfs-rs-raw:FG").as_ref(), b"~nfs-rs-raw:FG");
    }
}

//! Reversible names for NFSv4 servers exposing legacy byte names.
//!
//! RFC 7530 §12.7 calls for UTF-8 names. Some existing servers nonetheless
//! return arbitrary bytes; losing those bytes would make LOOKUP ambiguous.

const PREFIX: &str = "~nfs-rs-raw:";

pub(crate) fn display(raw: &[u8]) -> String {
    if let Ok(name) = std::str::from_utf8(raw)
        && !name.starts_with(PREFIX)
    {
        return name.to_owned();
    }
    let mut result = String::from(PREFIX);
    for byte in raw {
        use std::fmt::Write;
        let _ = write!(result, "{byte:02X}");
    }
    result
}

pub(crate) fn wire(name: &str) -> Vec<u8> {
    let Some(hex) = name.strip_prefix(PREFIX) else {
        return name.as_bytes().to_vec();
    };
    if hex.is_empty() || hex.len() % 2 != 0 {
        return name.as_bytes().to_vec();
    }
    let decoded: Option<Vec<u8>> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).ok()?;
            u8::from_str_radix(text, 16).ok()
        })
        .collect();
    decoded.unwrap_or_else(|| name.as_bytes().to_vec())
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
        assert_eq!(wire("~nfs-rs-raw:literal"), b"~nfs-rs-raw:literal");
    }
}

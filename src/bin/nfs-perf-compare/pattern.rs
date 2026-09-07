use bytes::Bytes;

use super::cli::CHUNK;

pub const ALIGN: usize = 4096;

/// Over-allocate and return (vec, offset) such that vec[offset..offset+len] is 4096-aligned.
pub fn aligned_bytes(len: usize) -> (Vec<u8>, usize) {
    let v = vec![0u8; len + ALIGN];
    let offset = (ALIGN - (v.as_ptr() as usize % ALIGN)) % ALIGN;
    (v, offset)
}

pub fn pattern_block() -> Bytes {
    let len = CHUNK as usize;
    let (mut v, off) = aligned_bytes(len);
    for (i, b) in v[off..off + len].iter_mut().enumerate() {
        *b = ((i * 17 + 29) % 251) as u8;
    }
    Bytes::from(v).slice(off..off + len)
}

/// Pattern bytes for the absolute file range [offset, offset+len). Zero-copy
/// unless the range wraps around the 1 MiB period (e.g. 1020 KiB chunks).
pub fn pattern_at(block: &Bytes, offset: u64, len: usize) -> Bytes {
    let pos = (offset % CHUNK) as usize;
    if pos + len <= block.len() {
        return block.slice(pos..pos + len);
    }
    let mut out = bytes::BytesMut::with_capacity(len);
    let mut remaining = len;
    let mut p = pos;
    while remaining > 0 {
        let n = remaining.min(block.len() - p);
        out.extend_from_slice(&block[p..p + n]);
        remaining -= n;
        p = (p + n) % block.len();
    }
    out.freeze()
}

pub fn verify(block: &Bytes, offset: u64, chunk: &[u8]) -> bool {
    let mut pos = (offset % CHUNK) as usize;
    let mut rest = chunk;
    while !rest.is_empty() {
        let n = rest.len().min(block.len() - pos);
        if rest[..n] != block[pos..pos + n] {
            return false;
        }
        rest = &rest[n..];
        pos = (pos + n) % block.len();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_is_aligned_and_periodic() {
        let b = pattern_block();
        assert_eq!(b.as_ptr() as usize % ALIGN, 0);
        assert_eq!(b[0], 29);
        assert_eq!(b[1], 46);
        assert!(verify(&b, 0, &b));
        assert!(verify(&b, CHUNK * 7 + 100, &b[100..]));
        // non-1MiB chunking (NFSv4.1 negotiates 1020 KiB payloads) must round-trip
        let chunk = 1_044_480u64;
        for i in 0..5u64 {
            let data = pattern_at(&b, i * chunk, chunk as usize);
            assert_eq!(data.len(), chunk as usize);
            assert!(verify(&b, i * chunk, &data), "chunk {i}");
        }
        let mut bad = b.to_vec();
        bad[5] ^= 1;
        assert!(!verify(&b, 0, &bad));
    }
}

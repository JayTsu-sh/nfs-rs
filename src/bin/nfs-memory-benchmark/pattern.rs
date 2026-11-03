use bytes::Bytes;

pub const MIB: usize = 1024 * 1024;

/// Two identical periods let every <=1 MiB range share the same allocation,
/// including 1020 KiB NFSv4.1 chunks crossing the first period's boundary.
pub struct Pattern(Bytes);

impl Pattern {
    pub fn new() -> Self {
        Self(Bytes::from(
            (0..2 * MIB)
                .map(|i| ((i % MIB) % 251) as u8)
                .collect::<Vec<_>>(),
        ))
    }

    pub fn at(&self, offset: u64, len: usize) -> Bytes {
        assert!(len <= MIB);
        let start = (offset % MIB as u64) as usize;
        self.0.slice(start..start + len)
    }

    pub fn verify(&self, offset: u64, data: &[u8]) -> bool {
        data.len() <= MIB && self.at(offset, data.len()).as_ref() == data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiated_chunk_sizes_share_one_periodic_backing() {
        let pattern = Pattern::new();
        for chunk in [MIB, 1020 * 1024] {
            for index in 0..257 {
                let offset = index * chunk as u64;
                let part = pattern.at(offset, chunk);
                let start = (offset % MIB as u64) as usize;
                assert_eq!(part.as_ptr(), pattern.0[start..].as_ptr());
                assert_eq!(part[0], (start % 251) as u8);
                assert_eq!(part[chunk - 1], (((start + chunk - 1) % MIB) % 251) as u8);
            }
        }
        assert_eq!(pattern.at(MIB as u64 - 2, 4).as_ref(), &[147, 148, 0, 1]);
        assert!(pattern.verify(MIB as u64 - 2, &[147, 148, 0, 1]));
        assert!(!pattern.verify(MIB as u64 - 2, &[147, 148, 0, 2]));
    }
}

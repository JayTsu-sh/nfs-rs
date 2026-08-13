//! Common NFSv4 `stateid4` representation.

/// Opaque NFSv4 stateid (16 bytes: seqid u32 + other [u8; 12]) fenced by a
/// client-engine generation. The wire value is common; generation ownership
/// remains with each minor-version engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateId {
    pub raw: [u8; 16],
    pub generation: u64,
}

impl StateId {
    /// Anonymous stateid (all zeros).
    pub(crate) fn anonymous() -> Self {
        Self {
            raw: [0; 16],
            generation: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: &[u8; 16]) -> Self {
        Self::from_bytes_at(bytes, 1)
    }

    pub(crate) fn from_bytes_at(bytes: &[u8; 16], generation: u64) -> Self {
        Self {
            raw: *bytes,
            generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StateId;
    use bytes::Bytes;

    #[test]
    fn rfc7530_stateid4_golden_vector_is_16_wire_bytes() {
        let raw = [
            0x01, 0x02, 0x03, 0x04, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
            0x1a, 0x1b,
        ];
        let stateid = StateId::from_bytes_at(&raw, 7);
        assert_eq!(stateid.raw, raw);
        assert_eq!(&stateid.raw[..4], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(&stateid.raw[4..], &raw[4..]);
        assert_eq!(stateid.generation, 7);

        let decoded =
            crate::nfs4::fastxdr::stateid4::<Bytes>::try_from(Bytes::copy_from_slice(&raw))
                .expect("RFC 7530 stateid4 literal must decode");
        assert_eq!(decoded.seqid, 0x0102_0304);
        assert_eq!(decoded.other.as_ref(), &raw[4..]);
    }
}

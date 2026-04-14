//! NFSv4.1 ACL (Access Control List) encode/decode.
//!
//! NFSv4 ACLs are carried as FATTR4_ACL (attribute #12) inside GETATTR/SETATTR,
//! not as separate RPC procedures. See RFC 7530 §6.2.

use bytes::{Buf, Bytes};

use super::attrs::{decode_getattr_envelope, decode_utf8str};
use crate::error::{NfsError, Result};
use crate::mount::{AceFlags, AceMask, AceType, Acl, AclSupport, NfsAce};

/// Maximum number of ACEs we'll decode from a single response.
/// Prevents unbounded allocation from a malformed server response.
const MAX_ACES: usize = 8192;

// ─── AceType conversion ────────────────────────────────────────────────────────

impl TryFrom<u32> for AceType {
    type Error = NfsError;

    fn try_from(v: u32) -> Result<Self> {
        match v {
            0 => Ok(AceType::AccessAllowed),
            1 => Ok(AceType::AccessDenied),
            2 => Ok(AceType::SystemAudit),
            3 => Ok(AceType::SystemAlarm),
            _ => Err(NfsError::Xdr(format!("unknown ACE type {}", v))),
        }
    }
}

// ─── Decode ────────────────────────────────────────────────────────────────────

/// Decode a single nfsace4 from the wire.
/// Wire format: type(u32) + flag(u32) + access_mask(u32) + who(utf8str_mixed).
pub(crate) fn decode_nfsace4(buf: &mut Bytes) -> Result<NfsAce> {
    if buf.remaining() < 12 {
        return Err(NfsError::Xdr("nfsace4 truncated".to_string()));
    }
    let ace_type = AceType::try_from(buf.get_u32())?;
    let flags = AceFlags(buf.get_u32());
    let access_mask = AceMask(buf.get_u32());
    let who = decode_utf8str(buf)?;
    Ok(NfsAce { ace_type, flags, access_mask, who })
}

/// Decode a variable-length array of nfsace4.
/// Wire format: count(u32) + count * nfsace4.
pub(crate) fn decode_acl(buf: &mut Bytes) -> Result<Acl> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("ACL count truncated".to_string()));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_ACES {
        return Err(NfsError::Xdr(format!("ACL has {} entries, max {}", count, MAX_ACES)));
    }
    let mut aces = Vec::with_capacity(count);
    for _ in 0..count {
        aces.push(decode_nfsace4(buf)?);
    }
    Ok(Acl { aces })
}

/// Skip over an ACL in the attribute value stream without allocating.
/// Used by `decode_fattr4_to_attr` when ACL appears in the bitmap but
/// we only need standard attributes.
pub(super) fn skip_acl(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("ACL count truncated".to_string()));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_ACES {
        return Err(NfsError::Xdr(format!("ACL has {} entries, max {}", count, MAX_ACES)));
    }
    for _ in 0..count {
        // nfsace4: type(4) + flag(4) + access_mask(4) + who(var)
        if buf.remaining() < 12 {
            return Err(NfsError::Xdr("nfsace4 truncated".to_string()));
        }
        buf.advance(12);
        // Skip utf8str: len(4) + padded data
        if buf.remaining() < 4 {
            return Err(NfsError::Xdr("nfsace4 who length truncated".to_string()));
        }
        let len = buf.get_u32() as usize;
        let padded = (len + 3) & !3;
        if buf.remaining() < padded {
            return Err(NfsError::Xdr("nfsace4 who data truncated".to_string()));
        }
        buf.advance(padded);
    }
    Ok(())
}

// ─── Encode ────────────────────────────────────────────────────────────────────

/// Encode a single nfsace4 to XDR wire format.
fn encode_nfsace4(ace: &NfsAce, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(ace.ace_type as u32).to_be_bytes());
    buf.extend_from_slice(&ace.flags.0.to_be_bytes());
    buf.extend_from_slice(&ace.access_mask.0.to_be_bytes());
    // utf8str_mixed: len(4) + data + pad
    let who_bytes = ace.who.as_bytes();
    buf.extend_from_slice(&(who_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(who_bytes);
    let pad = (4 - who_bytes.len() % 4) % 4;
    for _ in 0..pad {
        buf.push(0);
    }
}

/// Encode an ACL as a variable-length array of nfsace4.
fn encode_acl(acl: &Acl, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(acl.aces.len() as u32).to_be_bytes());
    for ace in &acl.aces {
        encode_nfsace4(ace, buf);
    }
}

/// Encode an ACL into (attrmask, attr_vals) for use in SETATTR.
/// Sets bit 12 (FATTR4_ACL) in word 0 of the bitmap.
pub(super) fn encode_setattr_acl(acl: &Acl) -> (Vec<u32>, Vec<u8>) {
    let word0: u32 = 1 << 12; // FATTR4_ACL
    let mut vals = Vec::new();
    encode_acl(acl, &mut vals);
    (vec![word0], vals)
}

// ─── GETATTR response decoders ─────────────────────────────────────────────────

/// Parse a GETATTR response that requested FATTR4_ACL (bit 12) and decode the ACL.
pub(super) fn decode_getattr_acl(data: &mut Bytes) -> Result<Acl> {
    let (bitmap, mut vals) = decode_getattr_envelope(data)?;
    let word0 = bitmap.first().copied().unwrap_or(0);
    if word0 & (1 << 12) == 0 {
        return Err(NfsError::Xdr("server did not return FATTR4_ACL".to_string()));
    }
    // Guard: attributes are encoded in bit-number order. If any bits below 12 are set,
    // their values precede the ACL data in attr_vals and would cause a misparse.
    if word0 & ((1 << 12) - 1) != 0 {
        return Err(NfsError::Xdr("server returned unexpected attributes before FATTR4_ACL".to_string()));
    }
    decode_acl(&mut vals)
}

/// Parse a GETATTR response that requested FATTR4_ACLSUPPORT (bit 13).
pub(super) fn decode_getattr_aclsupport(data: &mut Bytes) -> Result<AclSupport> {
    let (bitmap, mut vals) = decode_getattr_envelope(data)?;
    let word0 = bitmap.first().copied().unwrap_or(0);
    if word0 & (1 << 13) == 0 {
        return Err(NfsError::Xdr("server did not return FATTR4_ACLSUPPORT".to_string()));
    }
    // Guard: if any bits below 13 are set, their values precede ACLSUPPORT data.
    if word0 & ((1 << 13) - 1) != 0 {
        return Err(NfsError::Xdr("server returned unexpected attributes before FATTR4_ACLSUPPORT".to_string()));
    }
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr("ACLSUPPORT value truncated".to_string()));
    }
    Ok(AclSupport(vals.get_u32()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ace(ace_type: AceType, flags: u32, mask: u32, who: &str) -> NfsAce {
        NfsAce {
            ace_type,
            flags: AceFlags(flags),
            access_mask: AceMask(mask),
            who: who.to_string(),
        }
    }

    fn encode_one_ace(ace: &NfsAce) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_nfsace4(ace, &mut buf);
        buf
    }

    #[test]
    fn ace_type_try_from_valid() {
        assert_eq!(AceType::try_from(0).unwrap(), AceType::AccessAllowed);
        assert_eq!(AceType::try_from(1).unwrap(), AceType::AccessDenied);
        assert_eq!(AceType::try_from(2).unwrap(), AceType::SystemAudit);
        assert_eq!(AceType::try_from(3).unwrap(), AceType::SystemAlarm);
    }

    #[test]
    fn ace_type_try_from_invalid() {
        assert!(AceType::try_from(4).is_err());
        assert!(AceType::try_from(999).is_err());
    }

    #[test]
    fn roundtrip_single_ace() {
        let ace = make_ace(AceType::AccessAllowed, 0x01, 0x1F01FF, "OWNER@");
        let encoded = encode_one_ace(&ace);
        let mut bytes = Bytes::from(encoded);
        let decoded = decode_nfsace4(&mut bytes).unwrap();
        assert_eq!(decoded, ace);
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn roundtrip_ace_with_padding() {
        // "AB" is 2 bytes, needs 2 bytes padding to reach 4-byte alignment
        let ace = make_ace(AceType::AccessDenied, 0x40, 0x20000, "AB");
        let encoded = encode_one_ace(&ace);
        // 12 (fixed) + 4 (len) + 4 (padded "AB\0\0") = 20
        assert_eq!(encoded.len(), 20);
        let mut bytes = Bytes::from(encoded);
        let decoded = decode_nfsace4(&mut bytes).unwrap();
        assert_eq!(decoded, ace);
    }

    #[test]
    fn roundtrip_acl_empty() {
        let acl = Acl { aces: vec![] };
        let mut buf = Vec::new();
        encode_acl(&acl, &mut buf);
        assert_eq!(buf, 0u32.to_be_bytes());
        let mut bytes = Bytes::from(buf);
        let decoded = decode_acl(&mut bytes).unwrap();
        assert_eq!(decoded, acl);
    }

    #[test]
    fn roundtrip_acl_multiple() {
        let acl = Acl {
            aces: vec![
                make_ace(AceType::AccessAllowed, 0, AceMask::READ_DATA | AceMask::EXECUTE, "OWNER@"),
                make_ace(AceType::AccessAllowed, AceFlags::IDENTIFIER_GROUP, AceMask::READ_DATA, "GROUP@"),
                make_ace(AceType::AccessDenied, 0, AceMask::WRITE_DATA, "EVERYONE@"),
            ],
        };
        let mut buf = Vec::new();
        encode_acl(&acl, &mut buf);
        let mut bytes = Bytes::from(buf);
        let decoded = decode_acl(&mut bytes).unwrap();
        assert_eq!(decoded, acl);
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn decode_nfsace4_truncated() {
        let mut bytes = Bytes::from_static(&[0u8; 8]); // need at least 12
        assert!(decode_nfsace4(&mut bytes).is_err());
    }

    #[test]
    fn decode_acl_count_truncated() {
        let mut bytes = Bytes::from_static(&[0u8; 2]); // need 4 for count
        assert!(decode_acl(&mut bytes).is_err());
    }

    #[test]
    fn decode_acl_exceeds_max() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_ACES as u32 + 1).to_be_bytes()));
        let mut bytes = Bytes::from(buf);
        assert!(decode_acl(&mut bytes).is_err());
    }

    #[test]
    fn skip_acl_empty() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes()); // count = 0
        let mut bytes = Bytes::from(buf);
        skip_acl(&mut bytes).unwrap();
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn skip_acl_with_entries() {
        let acl = Acl {
            aces: vec![
                make_ace(AceType::AccessAllowed, 0, 0x1F, "OWNER@"),
                make_ace(AceType::AccessDenied, 0, 0x02, "EVERYONE@"),
            ],
        };
        let mut buf = Vec::new();
        encode_acl(&acl, &mut buf);
        // Append a sentinel byte to verify skip_acl stops at the right place
        buf.push(0xFF);
        let mut bytes = Bytes::from(buf);
        skip_acl(&mut bytes).unwrap();
        assert_eq!(bytes.remaining(), 1);
        assert_eq!(bytes[0], 0xFF);
    }

    #[test]
    fn special_who_strings() {
        for who in &["OWNER@", "GROUP@", "EVERYONE@", "INTERACTIVE@", "NETWORK@", "BATCH@", "ANONYMOUS@", "AUTHENTICATED@", "SERVICE@"] {
            let ace = make_ace(AceType::AccessAllowed, 0, AceMask::READ_DATA, who);
            let encoded = encode_one_ace(&ace);
            let mut bytes = Bytes::from(encoded);
            let decoded = decode_nfsace4(&mut bytes).unwrap();
            assert_eq!(decoded.who, *who);
        }
    }

    #[test]
    fn encode_setattr_acl_bitmap() {
        let acl = Acl {
            aces: vec![make_ace(AceType::AccessAllowed, 0, 0x1F, "OWNER@")],
        };
        let (attrmask, vals) = encode_setattr_acl(&acl);
        assert_eq!(attrmask, vec![1u32 << 12]);
        // vals should start with count=1
        assert_eq!(&vals[..4], &1u32.to_be_bytes());
    }

    #[test]
    fn ace_flags_contains() {
        let flags = AceFlags(AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT);
        assert!(flags.contains(AceFlags::FILE_INHERIT));
        assert!(flags.contains(AceFlags::DIRECTORY_INHERIT));
        assert!(!flags.contains(AceFlags::INHERIT_ONLY));
    }

    #[test]
    fn ace_mask_contains() {
        let mask = AceMask(AceMask::READ_DATA | AceMask::WRITE_DATA);
        assert!(mask.contains(AceMask::READ_DATA));
        assert!(mask.contains(AceMask::WRITE_DATA));
        assert!(!mask.contains(AceMask::EXECUTE));
    }

    #[test]
    fn acl_support_supports() {
        let support = AclSupport(AclSupport::ALLOW | AclSupport::DENY);
        assert!(support.supports(AclSupport::ALLOW));
        assert!(support.supports(AclSupport::DENY));
        assert!(!support.supports(AclSupport::AUDIT));
    }

    #[test]
    fn decode_getattr_acl_response() {
        let acl = Acl {
            aces: vec![make_ace(AceType::AccessAllowed, 0, AceMask::READ_DATA, "OWNER@")],
        };
        // Build a GETATTR response with bit 12 set
        let mut resp = Vec::new();
        // bitmap: 1 word, bit 12 set
        resp.extend_from_slice(&1u32.to_be_bytes()); // bitmap_len = 1
        resp.extend_from_slice(&(1u32 << 12).to_be_bytes()); // word0 with bit 12
        // attr_vals
        let mut acl_data = Vec::new();
        encode_acl(&acl, &mut acl_data);
        resp.extend_from_slice(&(acl_data.len() as u32).to_be_bytes());
        resp.extend_from_slice(&acl_data);

        let mut bytes = Bytes::from(resp);
        let decoded = decode_getattr_acl(&mut bytes).unwrap();
        assert_eq!(decoded, acl);
    }

    #[test]
    fn decode_getattr_acl_missing_bit() {
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes()); // bitmap_len = 1
        resp.extend_from_slice(&0u32.to_be_bytes()); // no bits set
        resp.extend_from_slice(&0u32.to_be_bytes()); // attr_vals len = 0
        let mut bytes = Bytes::from(resp);
        assert!(decode_getattr_acl(&mut bytes).is_err());
    }

    #[test]
    fn decode_getattr_aclsupport_response() {
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes()); // bitmap_len = 1
        resp.extend_from_slice(&(1u32 << 13).to_be_bytes()); // word0 with bit 13
        let support_val = AclSupport::ALLOW | AclSupport::DENY;
        resp.extend_from_slice(&4u32.to_be_bytes()); // attr_vals len = 4
        resp.extend_from_slice(&support_val.to_be_bytes());
        let mut bytes = Bytes::from(resp);
        let support = decode_getattr_aclsupport(&mut bytes).unwrap();
        assert_eq!(support, AclSupport(support_val));
    }
}

//! NFSv4 fattr4 bitmap and common attribute decoding.
//!
//! NFSv4 attributes are encoded as a bitmap (which attributes are present)
//! followed by a variable-length opaque blob containing the attribute values
//! in bitmap order. The common numbering, including FILEHANDLE=19 and
//! FILEID=20, is defined by RFC 7530 §5.8 and retained by later minor versions.

use bytes::{Buf, Bytes};

use super::acl::{decode_acl, skip_acl};
use super::attrnum;
use crate::Time;
use crate::error::{NfsError, Result};
use crate::mount::Attr;

// ─── Common NFSv4 attribute numbers (RFC 7530 §5.8) ────────────────────────
// Attribute number = bit position in the bitmap. Word 0 = bits 0-31, Word 1 = bits 32-63.
//
// Word 0: 0=supported_attrs, 1=type, 2=fh_expire_type, 3=change, 4=size,
//   5=link_support, 6=symlink_support, 7=named_attr, 8=fsid, 9=unique_handles,
//   10=lease_time, 11=rdattr_error, 12=acl, 13=aclsupport, 14=archive,
//   15=cansettime, 16=case_insensitive, 17=case_preserving, 18=chown_restricted,
//   19=FILEHANDLE, 20=fileid, 21=files_avail, 22=files_free,
//   23=files_total, 24=fs_locations, 25=hidden, 26=homogeneous,
//   27=maxfilesize, 28=maxlink, 29=maxname, 30=maxread, 31=maxwrite
//
// Word 1 (attr# 32-63): 32=mimetype, 33=mode, 34=no_trunc, 35=numlinks,
//   36=owner, 37=owner_group, 38=quota_avail_hard, 39=quota_avail_soft,
//   40=quota_used, 41=rawdev, 42=space_avail, 43=space_free, 44=space_total,
//   45=space_used, 46=system, 47=time_access, 48=time_access_set,
//   49=time_backup, 50=time_create, 51=time_delta, 52=time_metadata,
//   53=time_modify, 54=time_modify_set, 55=mounted_on_fileid

/// Standard bitmap for GETATTR to populate crate::Attr fields.
///
/// Common NFSv4 attribute numbers (RFC 7530 §5.8):
///   type(1), size(4), fsid(8), ACL(12), filehandle(19), fileid(20),
///   mode(33), numlinks(35), owner(36), owner_group(37),
///   rawdev(41), space_used(45), time_access(47),
///   time_metadata(52), time_modify(53)
pub(crate) fn standard_getattr_bitmap() -> [u32; 2] {
    let word0: u32 = (1 << 1)  | // type
        (1 << 4)  | // size
        (1 << 8)  | // fsid
        (1 << attrnum::ACL) | // ACL
        (1 << attrnum::FILEHANDLE) | // filehandle
        (1 << attrnum::FILEID); // fileid

    let word1: u32 = (1 << 1)  | // mode (attr 33)
        (1 << 3)  | // numlinks (attr 35)
        (1 << 4)  | // owner (attr 36)
        (1 << 5)  | // owner_group (attr 37)
        (1 << 9)  | // rawdev (attr 41)
        (1 << 13) | // space_used (attr 45)
        (1 << 15) | // time_access (attr 47)
        (1 << 20) | // time_metadata (attr 52)
        (1 << 21); // time_modify (attr 53)

    [word0, word1]
}

/// Encode the common RFC 7530 writable attributes in bitmap order.
pub(crate) fn encode_setattr(
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<Time>,
    mtime: Option<Time>,
) -> (Vec<u32>, Vec<u8>) {
    let mut word0 = 0u32;
    let mut word1 = 0u32;
    let mut values = Vec::new();
    if let Some(value) = size {
        word0 |= 1 << 4;
        values.extend_from_slice(&value.to_be_bytes());
    }
    if let Some(value) = mode {
        word1 |= 1 << 1;
        values.extend_from_slice(&value.to_be_bytes());
    }
    for (value, bit) in [(uid, 4u32), (gid, 5u32)] {
        if let Some(value) = value {
            word1 |= 1 << bit;
            let value = value.to_string();
            let bytes = value.as_bytes();
            values.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            values.extend_from_slice(bytes);
            values.resize(values.len() + (4 - bytes.len() % 4) % 4, 0);
        }
    }
    for (value, bit) in [(atime, 16u32), (mtime, 22u32)] {
        if let Some(value) = value {
            word1 |= 1 << bit;
            values.extend_from_slice(&1u32.to_be_bytes());
            values.extend_from_slice(&(value.seconds as i64).to_be_bytes());
            values.extend_from_slice(&value.nseconds.to_be_bytes());
        }
    }
    let mask = if word1 != 0 {
        vec![word0, word1]
    } else if word0 != 0 {
        vec![word0]
    } else {
        Vec::new()
    };
    (mask, values)
}

/// Check if a specific attribute bit is set in a bitmap.
fn bitmap_has(bitmap: &[u32], attr_num: u32) -> bool {
    let word_idx = (attr_num / 32) as usize;
    let bit_idx = attr_num % 32;
    if word_idx >= bitmap.len() {
        return false;
    }
    (bitmap[word_idx] & (1 << bit_idx)) != 0
}

/// Split an fattr4 into its returned bitmap and bounded attribute-value payload.
pub(crate) fn decode_fattr4_envelope(data: &mut Bytes, label: &str) -> Result<(Vec<u32>, Bytes)> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(format!("{label} bitmap length truncated")));
    }
    let count = data.get_u32() as usize;
    if count > 16 || data.remaining() < count.saturating_mul(4) {
        return Err(NfsError::Xdr(format!("{label} bitmap is invalid")));
    }
    let bitmap = (0..count).map(|_| data.get_u32()).collect();
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(format!("{label} value length truncated")));
    }
    let length = data.get_u32() as usize;
    if data.remaining() < length {
        return Err(NfsError::Xdr(format!("{label} values truncated")));
    }
    Ok((bitmap, data.split_to(length)))
}

pub(crate) fn fattr4_has(bitmap: &[u32], attr_num: u32) -> bool {
    bitmap_has(bitmap, attr_num)
}

/// Skip `n` bytes from the attribute values buffer with bounds checking.
fn skip_fixed(vals: &mut Bytes, n: usize, attr_name: &str) -> Result<()> {
    if vals.remaining() < n {
        return Err(NfsError::Xdr(format!(
            "fattr4 {} truncated (need {} have {})",
            attr_name,
            n,
            vals.remaining()
        )));
    }
    vals.advance(n);
    Ok(())
}

/// Skip a `settime4` union: how(u32), then nfstime4(12) if `how == SET_TO_CLIENT_TIME(1)`.
fn skip_settime4(vals: &mut Bytes, attr_name: &str) -> Result<()> {
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr(format!(
            "fattr4 {} settime4 truncated",
            attr_name
        )));
    }
    let how = vals.get_u32();
    if how == 1 {
        // SET_TO_CLIENT_TIME4: followed by nfstime4 (12 bytes)
        skip_fixed(vals, 12, attr_name)?;
    }
    Ok(())
}

/// Decode an XDR opaque<> (variable-length): len(u32) + data + pad to 4.
fn decode_opaque(vals: &mut Bytes, attr_name: &str) -> Result<Bytes> {
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr(format!(
            "fattr4 {} opaque length truncated",
            attr_name
        )));
    }
    let len = vals.get_u32() as usize;
    let padded = (len + 3) & !3;
    if vals.remaining() < padded {
        return Err(NfsError::Xdr(format!(
            "fattr4 {} opaque data truncated (need {} have {})",
            attr_name,
            padded,
            vals.remaining()
        )));
    }
    let data = vals.slice(..len);
    vals.advance(padded);
    Ok(data)
}

/// Skip a `bitmap4` (variable-length bitmap): count(u32) + count * u32 words.
fn skip_bitmap4(vals: &mut Bytes, attr_name: &str) -> Result<()> {
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr(format!(
            "fattr4 {} bitmap count truncated",
            attr_name
        )));
    }
    let count = vals.get_u32() as usize;
    skip_fixed(vals, count * 4, attr_name)
}

/// Skip `fs_locations4`: root(pathname4) + locations<>.
/// pathname4 = component4<> = utf8str_cs<> (count + N strings).
/// fs_location4 = server(utf8str_cis<>) + rootpath(pathname4).
fn skip_fs_locations(vals: &mut Bytes) -> Result<()> {
    // pathname4 root: count(u32) + count * utf8str
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr(
            "fattr4 fs_locations root truncated".to_string(),
        ));
    }
    let root_count = vals.get_u32() as usize;
    for _ in 0..root_count {
        let _ = decode_utf8str(vals)?;
    }
    // locations<>: count(u32) + count * fs_location4
    if vals.remaining() < 4 {
        return Err(NfsError::Xdr(
            "fattr4 fs_locations locations count truncated".to_string(),
        ));
    }
    let loc_count = vals.get_u32() as usize;
    for _ in 0..loc_count {
        // fs_location4.server: utf8str_cis<> = count(u32) + count * utf8str
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr(
                "fattr4 fs_location4 server count truncated".to_string(),
            ));
        }
        let srv_count = vals.get_u32() as usize;
        for _ in 0..srv_count {
            let _ = decode_utf8str(vals)?;
        }
        // fs_location4.rootpath: pathname4
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr(
                "fattr4 fs_location4 rootpath count truncated".to_string(),
            ));
        }
        let path_count = vals.get_u32() as usize;
        for _ in 0..path_count {
            let _ = decode_utf8str(vals)?;
        }
    }
    Ok(())
}

/// Decode fattr4 attribute values into a `crate::Attr`.
///
/// `bitmap` is the response bitmap, `vals` is the opaque attr_vals data.
/// Attributes are decoded in bitmap order (lowest bit number first).
/// Unhandled attributes are skipped so that the buffer stays aligned.
///
/// Uses common NFSv4 attribute numbering (RFC 7530): FILEHANDLE=19, fileid=20,
/// mode=33, numlinks=35, owner=36, etc.
pub(crate) fn decode_fattr4_to_attr(bitmap: &[u32], vals: &mut Bytes) -> Result<Attr> {
    let mut attr = Attr::default();

    // ── Word 0 attributes (0-31) in strict numeric order ──

    // Attr 0: supported_attrs (bitmap4, variable)
    if bitmap_has(bitmap, 0) {
        skip_bitmap4(vals, "supported_attrs")?;
    }
    // Attr 1: type (nfs_ftype4 = uint32)
    if bitmap_has(bitmap, 1) {
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr("fattr4 type truncated".to_string()));
        }
        attr.type_ = vals.get_u32();
    }
    // Attr 2: fh_expire_type (uint32)
    if bitmap_has(bitmap, 2) {
        skip_fixed(vals, 4, "fh_expire_type")?;
    }
    // Attr 3: change (uint64)
    if bitmap_has(bitmap, 3) {
        skip_fixed(vals, 8, "change")?;
    }
    // Attr 4: size (uint64)
    if bitmap_has(bitmap, 4) {
        if vals.remaining() < 8 {
            return Err(NfsError::Xdr("fattr4 size truncated".to_string()));
        }
        attr.filesize = vals.get_u64();
    }
    // Attr 5: link_support (bool = uint32)
    if bitmap_has(bitmap, 5) {
        skip_fixed(vals, 4, "link_support")?;
    }
    // Attr 6: symlink_support (bool)
    if bitmap_has(bitmap, 6) {
        skip_fixed(vals, 4, "symlink_support")?;
    }
    // Attr 7: named_attr (bool)
    if bitmap_has(bitmap, 7) {
        skip_fixed(vals, 4, "named_attr")?;
    }
    // Attr 8: fsid (fsid4 = major:uint64 + minor:uint64)
    if bitmap_has(bitmap, 8) {
        if vals.remaining() < 16 {
            return Err(NfsError::Xdr("fattr4 fsid truncated".to_string()));
        }
        let major = vals.get_u64();
        let _minor = vals.get_u64();
        attr.fsid = major;
    }
    // Attr 9: unique_handles (bool)
    if bitmap_has(bitmap, 9) {
        skip_fixed(vals, 4, "unique_handles")?;
    }
    // Attr 10: lease_time (uint32)
    if bitmap_has(bitmap, 10) {
        skip_fixed(vals, 4, "lease_time")?;
    }
    // Attr 11: rdattr_error (nfsstat4 = uint32)
    if bitmap_has(bitmap, 11) {
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr("fattr4 rdattr_error truncated".to_string()));
        }
        let code = vals.get_u32();
        if code != 0 {
            return Err(NfsError::RdattrError(code));
        }
    }
    // Attr 12: ACL — decode into Attr
    if bitmap_has(bitmap, attrnum::ACL) {
        attr.acl = Some(decode_acl(vals)?);
    }
    // Attr 13: aclsupport (uint32)
    if bitmap_has(bitmap, attrnum::ACLSUPPORT) {
        skip_fixed(vals, 4, "aclsupport")?;
    }
    // Attr 14: archive (bool)
    if bitmap_has(bitmap, 14) {
        skip_fixed(vals, 4, "archive")?;
    }
    // Attr 15: cansettime (bool)
    if bitmap_has(bitmap, 15) {
        skip_fixed(vals, 4, "cansettime")?;
    }
    // Attr 16: case_insensitive (bool)
    if bitmap_has(bitmap, 16) {
        skip_fixed(vals, 4, "case_insensitive")?;
    }
    // Attr 17: case_preserving (bool)
    if bitmap_has(bitmap, 17) {
        skip_fixed(vals, 4, "case_preserving")?;
    }
    // Attr 18: chown_restricted (bool)
    if bitmap_has(bitmap, 18) {
        skip_fixed(vals, 4, "chown_restricted")?;
    }
    // Attr 19: filehandle (nfs_fh4 = opaque<>, variable-length)
    if bitmap_has(bitmap, attrnum::FILEHANDLE) {
        attr.filehandle = decode_opaque(vals, "filehandle")?;
    }
    // Attr 20: fileid (uint64)
    if bitmap_has(bitmap, attrnum::FILEID) {
        if vals.remaining() < 8 {
            return Err(NfsError::Xdr("fattr4 fileid truncated".to_string()));
        }
        attr.fileid = vals.get_u64();
    }
    // Attr 21: files_avail (uint64)
    if bitmap_has(bitmap, 21) {
        skip_fixed(vals, 8, "files_avail")?;
    }
    // Attr 22: files_free (uint64)
    if bitmap_has(bitmap, 22) {
        skip_fixed(vals, 8, "files_free")?;
    }
    // Attr 23: files_total (uint64)
    if bitmap_has(bitmap, 23) {
        skip_fixed(vals, 8, "files_total")?;
    }
    // Attr 24: fs_locations (complex variable)
    if bitmap_has(bitmap, 24) {
        skip_fs_locations(vals)?;
    }
    // Attr 25: hidden (bool)
    if bitmap_has(bitmap, 25) {
        skip_fixed(vals, 4, "hidden")?;
    }
    // Attr 26: homogeneous (bool)
    if bitmap_has(bitmap, 26) {
        skip_fixed(vals, 4, "homogeneous")?;
    }
    // Attr 27: maxfilesize (uint64)
    if bitmap_has(bitmap, 27) {
        skip_fixed(vals, 8, "maxfilesize")?;
    }
    // Attr 28: maxlink (uint32)
    if bitmap_has(bitmap, 28) {
        skip_fixed(vals, 4, "maxlink")?;
    }
    // Attr 29: maxname (uint32)
    if bitmap_has(bitmap, 29) {
        skip_fixed(vals, 4, "maxname")?;
    }
    // Attr 30: maxread (uint64)
    if bitmap_has(bitmap, 30) {
        skip_fixed(vals, 8, "maxread")?;
    }
    // Attr 31: maxwrite (uint64)
    if bitmap_has(bitmap, 31) {
        skip_fixed(vals, 8, "maxwrite")?;
    }

    // ── Word 1 attributes (32-63) in strict numeric order ──

    // Attr 32: mimetype (utf8str, variable)
    if bitmap_has(bitmap, 32) {
        let _ = decode_utf8str(vals)?;
    }
    // Attr 33: mode (mode4 = uint32)
    if bitmap_has(bitmap, attrnum::MODE) {
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr("fattr4 mode truncated".to_string()));
        }
        attr.file_mode = vals.get_u32();
    }
    // Attr 34: no_trunc (bool)
    if bitmap_has(bitmap, 34) {
        skip_fixed(vals, 4, "no_trunc")?;
    }
    // Attr 35: numlinks (uint32)
    if bitmap_has(bitmap, 35) {
        if vals.remaining() < 4 {
            return Err(NfsError::Xdr("fattr4 numlinks truncated".to_string()));
        }
        attr.nlink = vals.get_u32();
    }
    // Attr 36: owner (utf8str_cs) — store raw string AND numeric uid
    if bitmap_has(bitmap, 36) {
        let owner_str = decode_utf8str(vals)?;
        attr.uid = parse_numeric_owner(&owner_str);
        attr.owner = owner_str;
    }
    // Attr 37: owner_group (utf8str_cs) — store raw string AND numeric gid
    if bitmap_has(bitmap, 37) {
        let group_str = decode_utf8str(vals)?;
        attr.gid = parse_numeric_owner(&group_str);
        attr.owner_group = group_str;
    }
    // Attr 38: quota_avail_hard (uint64)
    if bitmap_has(bitmap, 38) {
        skip_fixed(vals, 8, "quota_avail_hard")?;
    }
    // Attr 39: quota_avail_soft (uint64)
    if bitmap_has(bitmap, 39) {
        skip_fixed(vals, 8, "quota_avail_soft")?;
    }
    // Attr 40: quota_used (uint64)
    if bitmap_has(bitmap, 40) {
        skip_fixed(vals, 8, "quota_used")?;
    }
    // Attr 41: rawdev (specdata4 = uint32 + uint32)
    if bitmap_has(bitmap, 41) {
        if vals.remaining() < 8 {
            return Err(NfsError::Xdr("fattr4 rawdev truncated".to_string()));
        }
        attr.spec_data[0] = vals.get_u32();
        attr.spec_data[1] = vals.get_u32();
    }
    // Attr 42: space_avail (uint64)
    if bitmap_has(bitmap, 42) {
        skip_fixed(vals, 8, "space_avail")?;
    }
    // Attr 43: space_free (uint64)
    if bitmap_has(bitmap, 43) {
        skip_fixed(vals, 8, "space_free")?;
    }
    // Attr 44: space_total (uint64)
    if bitmap_has(bitmap, 44) {
        skip_fixed(vals, 8, "space_total")?;
    }
    // Attr 45: space_used (uint64)
    if bitmap_has(bitmap, 45) {
        if vals.remaining() < 8 {
            return Err(NfsError::Xdr("fattr4 space_used truncated".to_string()));
        }
        attr.used = vals.get_u64();
    }
    // Attr 46: system (bool)
    if bitmap_has(bitmap, 46) {
        skip_fixed(vals, 4, "system")?;
    }
    // Attr 47: time_access (nfstime4)
    if bitmap_has(bitmap, 47) {
        attr.atime = decode_nfstime4(vals)?;
    }
    // Attr 48: time_access_set (settime4, variable)
    if bitmap_has(bitmap, 48) {
        skip_settime4(vals, "time_access_set")?;
    }
    // Attr 49: time_backup (nfstime4)
    if bitmap_has(bitmap, 49) {
        skip_fixed(vals, 12, "time_backup")?;
    }
    // Attr 50: time_create (nfstime4)
    if bitmap_has(bitmap, 50) {
        skip_fixed(vals, 12, "time_create")?;
    }
    // Attr 51: time_delta (nfstime4)
    if bitmap_has(bitmap, 51) {
        skip_fixed(vals, 12, "time_delta")?;
    }
    // Attr 52: time_metadata (nfstime4, maps to ctime)
    if bitmap_has(bitmap, 52) {
        attr.ctime = decode_nfstime4(vals)?;
    }
    // Attr 53: time_modify (nfstime4)
    if bitmap_has(bitmap, 53) {
        attr.mtime = decode_nfstime4(vals)?;
    }
    // Attr 54: time_modify_set (settime4, variable)
    if bitmap_has(bitmap, 54) {
        skip_settime4(vals, "time_modify_set")?;
    }
    // Attr 55: mounted_on_fileid (uint64)
    if bitmap_has(bitmap, 55) {
        skip_fixed(vals, 8, "mounted_on_fileid")?;
    }

    // ── Word 2 attributes (64-95, NFSv4.1 extensions) ──
    // Attr 56: dir_notif_delay (nfstime4)
    if bitmap_has(bitmap, 56) {
        skip_fixed(vals, 12, "dir_notif_delay")?;
    }
    // Attr 57: dirent_notif_delay (nfstime4)
    if bitmap_has(bitmap, 57) {
        skip_fixed(vals, 12, "dirent_notif_delay")?;
    }
    // Attrs 58-59: dacl/sacl (nfsacl41, complex ACL)
    if bitmap_has(bitmap, 58) {
        skip_acl(vals)?;
    }
    if bitmap_has(bitmap, 59) {
        skip_acl(vals)?;
    }
    // Attr 60: change_policy (uint64 + uint64 = 16 bytes)
    if bitmap_has(bitmap, 60) {
        skip_fixed(vals, 16, "change_policy")?;
    }

    Ok(attr)
}

/// Decode the GETATTR response envelope: bitmap length + bitmap words + attr_vals opaque.
/// Returns `(bitmap, attr_vals_bytes)`. Shared by `decode_getattr_response` and `acl.rs`.
pub(super) fn decode_getattr_envelope(data: &mut Bytes) -> Result<(Vec<u32>, Bytes)> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("GETATTR bitmap length truncated".to_string()));
    }
    let bitmap_len = data.get_u32() as usize;
    if bitmap_len > 16 {
        return Err(NfsError::Xdr(format!(
            "GETATTR bitmap has {} words, max 16",
            bitmap_len
        )));
    }
    let mut bitmap = vec![0u32; bitmap_len];
    for word in &mut bitmap {
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("GETATTR bitmap word truncated".to_string()));
        }
        *word = data.get_u32();
    }
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETATTR attr_vals length truncated".to_string(),
        ));
    }
    let vals_len = data.get_u32() as usize;
    if data.remaining() < vals_len {
        return Err(NfsError::Xdr("GETATTR attr_vals truncated".to_string()));
    }
    let vals = data.split_to(vals_len);
    // Pad alignment (attr_vals is XDR opaque, padded to 4)
    let pad = (4 - vals_len % 4) % 4;
    if data.remaining() >= pad {
        data.advance(pad);
    }
    Ok((bitmap, vals))
}

/// Decode a full GETATTR response (bitmap + attr_vals opaque) into an Attr.
pub(crate) fn decode_getattr_response(data: &mut Bytes) -> Result<Attr> {
    let (bitmap, mut vals) = decode_getattr_envelope(data)?;
    let result = decode_fattr4_to_attr(&bitmap, &mut vals);
    if let Err(ref e) = result {
        // Log bitmap detail on failure to aid debugging
        let set_bits: Vec<u32> = (0u32..((bitmap.len() as u32) * 32))
            .filter(|&bit| bitmap_has(&bitmap, bit))
            .collect();
        tracing::warn!(
            "GETATTR decode failed: {} (bitmap={:?} set_attrs={:?} vals_len={})",
            e,
            bitmap,
            set_bits,
            vals.len()
        );
    }
    result
}

fn decode_nfstime4(buf: &mut Bytes) -> Result<Time> {
    if buf.remaining() < 12 {
        return Err(NfsError::Xdr("nfstime4 truncated".to_string()));
    }
    let seconds = buf.get_i64();
    let nseconds = buf.get_u32();
    // Clamp i64 seconds to u32 range (crate::Time uses u32).
    // Negative timestamps (pre-1970) become 0; timestamps after 2106 are capped.
    let clamped = if seconds < 0 {
        0u32
    } else if seconds > u32::MAX as i64 {
        u32::MAX
    } else {
        seconds as u32
    };
    Ok(Time {
        seconds: clamped,
        nseconds,
    })
}

pub(super) fn decode_utf8str(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("utf8str length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("utf8str data truncated".to_string()));
    }
    let bytes = buf.slice(..len);
    buf.advance(padded);
    String::from_utf8(bytes.to_vec())
        .map_err(|_| NfsError::Xdr("invalid UTF-8 in string attribute".to_string()))
}

/// Parse an owner string to uid/gid.
/// NFSv4 encodes owners as "name@domain" or numeric strings (RFC 5661 §5.9).
/// 数字形式直接解析；`root` 按 POSIX 惯例映射到 0；其余无法映射的名字 → nobody。
fn parse_numeric_owner(s: &str) -> u32 {
    // Try parsing as plain number first
    if let Ok(n) = s.parse::<u32>() {
        return n;
    }
    // Extract the name part before '@'
    if let Some(name) = s.split('@').next() {
        // Numeric-string form, e.g. "1000@domain"
        if let Ok(n) = name.parse::<u32>() {
            return n;
        }
        // Well-known: root is uid/gid 0 on all Unix systems (POSIX)
        if name == "root" {
            return 0;
        }
    }
    // Unknown owner → nobody
    65534
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_bitmap_has_expected_bits() {
        let bm = standard_getattr_bitmap();
        // Word 0 (common RFC 7530 numbering)
        assert!(bitmap_has(&bm, 1)); // type
        assert!(bitmap_has(&bm, 4)); // size
        assert!(bitmap_has(&bm, 8)); // fsid
        assert!(bitmap_has(&bm, 12)); // ACL
        assert!(bitmap_has(&bm, 19)); // filehandle
        assert!(bitmap_has(&bm, 20)); // fileid
        // Word 1 (common RFC 7530 numbering)
        assert!(bitmap_has(&bm, 33)); // mode
        assert!(bitmap_has(&bm, 35)); // numlinks
        assert!(bitmap_has(&bm, 36)); // owner
        assert!(bitmap_has(&bm, 37)); // owner_group
        assert!(bitmap_has(&bm, 41)); // rawdev
        assert!(bitmap_has(&bm, 45)); // space_used
        assert!(bitmap_has(&bm, 47)); // time_access
        assert!(bitmap_has(&bm, 52)); // time_metadata
        assert!(bitmap_has(&bm, 53)); // time_modify
        // Should NOT have these
        assert!(!bitmap_has(&bm, 0)); // supported_attrs
        assert!(!bitmap_has(&bm, 3)); // change
        assert!(!bitmap_has(&bm, 34)); // no_trunc
    }

    #[test]
    fn parse_numeric_owner_values() {
        assert_eq!(parse_numeric_owner("1000"), 1000);
        assert_eq!(parse_numeric_owner("0"), 0);
        assert_eq!(parse_numeric_owner("1000@example.com"), 1000);
        assert_eq!(parse_numeric_owner("root"), 0);
        assert_eq!(parse_numeric_owner("root@netapp.com"), 0);
        assert_eq!(parse_numeric_owner("nobody@localdomain"), 65534);
    }

    #[test]
    fn parse_numeric_owner_edge_cases() {
        assert_eq!(parse_numeric_owner("65534"), 65534);
        assert_eq!(parse_numeric_owner("4294967295"), 4294967295); // u32::MAX
        assert_eq!(parse_numeric_owner(""), 65534);
        assert_eq!(parse_numeric_owner("@domain"), 65534);
    }

    #[test]
    fn bitmap_has_empty() {
        let bm: [u32; 0] = [];
        assert!(!bitmap_has(&bm, 0));
        assert!(!bitmap_has(&bm, 32));
        assert!(!bitmap_has(&bm, 100));
    }

    #[test]
    fn bitmap_has_single_word() {
        let bm = [0x00000002u32]; // bit 1 only
        assert!(!bitmap_has(&bm, 0));
        assert!(bitmap_has(&bm, 1));
        assert!(!bitmap_has(&bm, 2));
        assert!(!bitmap_has(&bm, 32)); // out of range
    }

    #[test]
    fn bitmap_has_two_words() {
        let bm = [0u32, 0x00000001]; // bit 32 only
        assert!(!bitmap_has(&bm, 0));
        assert!(!bitmap_has(&bm, 31));
        assert!(bitmap_has(&bm, 32));
        assert!(!bitmap_has(&bm, 33));
    }

    fn build_fattr4_type_only(ftype: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        // bitmap: [bit 1 set] = type
        buf.extend_from_slice(&1u32.to_be_bytes()); // bitmap len = 1
        buf.extend_from_slice(&0x00000002u32.to_be_bytes()); // bit 1
        // attr_vals opaque
        let vals = ftype.to_be_bytes();
        buf.extend_from_slice(&(vals.len() as u32).to_be_bytes());
        buf.extend_from_slice(&vals);
        buf
    }

    #[test]
    fn decode_getattr_type_only() {
        let buf = build_fattr4_type_only(1); // NF4REG
        let mut bytes = Bytes::from(buf);
        let attr = decode_getattr_response(&mut bytes).unwrap();
        assert_eq!(attr.type_, 1);
        assert_eq!(attr.filesize, 0); // not requested
    }

    #[test]
    fn decode_getattr_type_directory() {
        let buf = build_fattr4_type_only(2); // NF4DIR
        let mut bytes = Bytes::from(buf);
        let attr = decode_getattr_response(&mut bytes).unwrap();
        assert_eq!(attr.type_, 2);
    }

    #[test]
    fn decode_fattr4_size_and_fileid() {
        // RFC 7530: bit 4 (size) + bit 20 (fileid) in word 0
        let bitmap = [(1u32 << 4) | (1 << 20)];
        let mut vals = Vec::new();
        vals.extend_from_slice(&12345u64.to_be_bytes()); // size
        vals.extend_from_slice(&99u64.to_be_bytes()); // fileid
        let mut v = Bytes::from(vals);
        let attr = decode_fattr4_to_attr(&bitmap, &mut v).unwrap();
        assert_eq!(attr.filesize, 12345);
        assert_eq!(attr.fileid, 99);
    }

    #[test]
    fn decode_fattr4_mode_and_nlinks() {
        // RFC 7530: word1 bit 1 (mode=33) + bit 3 (numlinks=35)
        let bitmap = [0u32, (1 << 1) | (1 << 3)];
        let mut vals = Vec::new();
        vals.extend_from_slice(&0o755u32.to_be_bytes()); // mode
        vals.extend_from_slice(&3u32.to_be_bytes()); // nlinks
        let mut v = Bytes::from(vals);
        let attr = decode_fattr4_to_attr(&bitmap, &mut v).unwrap();
        assert_eq!(attr.file_mode, 0o755);
        assert_eq!(attr.nlink, 3);
    }

    #[test]
    fn decode_fattr4_time_access() {
        // RFC 7530: word1 bit 15 = time_access (attr 47)
        let bitmap = [0u32, 1 << 15];
        let mut vals = Vec::new();
        vals.extend_from_slice(&1700000000i64.to_be_bytes()); // seconds
        vals.extend_from_slice(&123456789u32.to_be_bytes()); // nseconds
        let mut v = Bytes::from(vals);
        let attr = decode_fattr4_to_attr(&bitmap, &mut v).unwrap();
        assert_eq!(attr.atime.nseconds, 123456789);
    }

    #[test]
    fn decode_getattr_empty_bitmap() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes()); // bitmap len = 0
        buf.extend_from_slice(&0u32.to_be_bytes()); // attr_vals len = 0
        let mut bytes = Bytes::from(buf);
        let attr = decode_getattr_response(&mut bytes).unwrap();
        assert_eq!(attr, Attr::default());
    }

    #[test]
    fn decode_getattr_truncated_bitmap() {
        let buf = vec![0u8; 2]; // too short for bitmap len
        let mut bytes = Bytes::from(buf);
        assert!(decode_getattr_response(&mut bytes).is_err());
    }

    #[test]
    fn decode_nfstime4_valid() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000i64.to_be_bytes());
        buf.extend_from_slice(&500u32.to_be_bytes());
        let mut bytes = Bytes::from(buf);
        let t = decode_nfstime4(&mut bytes).unwrap();
        assert_eq!(t.seconds, 1000);
        assert_eq!(t.nseconds, 500);
    }

    #[test]
    fn decode_nfsv41_readdirplus_response() {
        // Captured from an NFSv4.1 server using the common RFC 7530 bitmap numbering:
        //   word0: type(1), size(4), fsid(8), acl(12), filehandle(19)
        //   word1: no_trunc(34), numlinks(35), owner(36), space_total(44),
        //          time_delta(51), time_metadata(52)
        //
        // Response bitmap = [0x00081112, 0x00181C1C] but we re-derive from data.
        // Attrs in server response: 1,4,8,12,19, 34,35,36,44,51,52
        let bitmap_word0: u32 = (1 << 1)  | // type
            (1 << 4)  | // size
            (1 << 8)  | // fsid
            (1 << 12) | // acl
            (1 << 19); // filehandle
        let bitmap_word1: u32 = (1 << 2)  | // no_trunc (attr 34)
            (1 << 3)  | // numlinks (attr 35)
            (1 << 4)  | // owner (attr 36)
            (1 << 12) | // space_total (attr 44)
            (1 << 19) | // time_delta (attr 51)
            (1 << 20); // time_metadata (attr 52)
        let bitmap = [bitmap_word0, bitmap_word1];

        let hex: Vec<u8> = vec![
            // type=2 (NF4DIR)
            0x00, 0x00, 0x00, 0x02, // size=81
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x51, // fsid major=0, minor=0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, // ACL: count=3
            0x00, 0x00, 0x00, 0x03, // ACE 1: ALLOW OWNER@ mask=0x001601e7
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x01, 0xe7, 0x00, 0x00,
            0x00, 0x06, 0x4f, 0x57, 0x4e, 0x45, 0x52, 0x40, 0x00, 0x00,
            // ACE 2: ALLOW GROUP@ mask=0x001200a1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x00, 0xa1, 0x00, 0x00,
            0x00, 0x06, 0x47, 0x52, 0x4f, 0x55, 0x50, 0x40, 0x00, 0x00,
            // ACE 3: ALLOW EVERYONE@ mask=0x001200a1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x00, 0xa1, 0x00, 0x00,
            0x00, 0x09, 0x45, 0x56, 0x45, 0x52, 0x59, 0x4f, 0x4e, 0x45, 0x40, 0x00, 0x00, 0x00,
            // filehandle: len=20, data=0100018100...87718b63
            0x00, 0x00, 0x00, 0x14, 0x01, 0x00, 0x01, 0x81, 0x00, 0x00, 0x00, 0x00, 0x75, 0xd0,
            0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x87, 0x71, 0x8b, 0x63, // no_trunc=1
            0x00, 0x00, 0x00, 0x01, // numlinks=8
            0x00, 0x00, 0x00, 0x08, // owner: len=1, "0"
            0x00, 0x00, 0x00, 0x01, 0x30, 0x00, 0x00, 0x00, // space_total=65,250,787,328
            0x00, 0x00, 0x00, 0x0f, 0x31, 0x40, 0x00, 0x00,
            // time_delta: 0s 1000000ns (1ms granularity)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40,
            // time_metadata: 0x69d0a53a seconds, 0x9a36 nseconds
            0x00, 0x00, 0x00, 0x00, 0x69, 0xd0, 0xa5, 0x3a, 0x00, 0x00, 0x9a, 0x36,
        ];
        assert_eq!(hex.len(), 180);

        let mut vals = Bytes::from(hex);
        let attr = decode_fattr4_to_attr(&bitmap, &mut vals).unwrap();
        assert_eq!(vals.remaining(), 0, "all bytes consumed");

        assert_eq!(attr.type_, 2); // NF4DIR
        assert_eq!(attr.filesize, 81);
        assert_eq!(attr.fsid, 0);
        assert_eq!(attr.acl.as_ref().map(|a| a.aces.len()), Some(3));
        assert_eq!(attr.filehandle.len(), 20);
        assert_eq!(attr.nlink, 8);
        assert_eq!(attr.owner, "0");
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.ctime.seconds, 0x69d0a53a); // time_metadata
        assert_eq!(attr.ctime.nseconds, 0x9a36);
    }

    #[test]
    fn decode_nfstime4_truncated() {
        let buf = vec![0u8; 8]; // need 12
        let mut bytes = Bytes::from(buf);
        assert!(decode_nfstime4(&mut bytes).is_err());
    }
}

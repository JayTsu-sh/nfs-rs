//! Wire-level definitions shared by NFSv4 minor versions.

pub(crate) mod acl;
pub(crate) mod attrs;
pub(crate) mod compound;
pub(crate) mod fastxdr;
pub(crate) mod stateid;

pub use fastxdr::nfsstat4 as Nfs4ErrorCode;
impl std::error::Error for Nfs4ErrorCode {}

impl std::fmt::Display for Nfs4ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Nfs4ErrorCode::NFS4_OK => write!(f, "call completed successfully"),
            Nfs4ErrorCode::NFS4ERR_PERM => write!(f, "permission denied"),
            Nfs4ErrorCode::NFS4ERR_NOENT => write!(f, "no such file or directory"),
            Nfs4ErrorCode::NFS4ERR_IO => write!(f, "i/o error"),
            Nfs4ErrorCode::NFS4ERR_NXIO => write!(f, "no such device or address"),
            Nfs4ErrorCode::NFS4ERR_ACCESS => write!(f, "access denied"),
            Nfs4ErrorCode::NFS4ERR_EXIST => write!(f, "file exists"),
            Nfs4ErrorCode::NFS4ERR_XDEV => write!(f, "cross-device link"),
            Nfs4ErrorCode::NFS4ERR_NOTDIR => write!(f, "not a directory"),
            Nfs4ErrorCode::NFS4ERR_ISDIR => write!(f, "is a directory"),
            Nfs4ErrorCode::NFS4ERR_INVAL => write!(f, "invalid argument"),
            Nfs4ErrorCode::NFS4ERR_FBIG => write!(f, "file too large"),
            Nfs4ErrorCode::NFS4ERR_NOSPC => write!(f, "no space left on device"),
            Nfs4ErrorCode::NFS4ERR_ROFS => write!(f, "read-only file system"),
            Nfs4ErrorCode::NFS4ERR_MLINK => write!(f, "too many hard links"),
            Nfs4ErrorCode::NFS4ERR_NAMETOOLONG => write!(f, "name too long"),
            Nfs4ErrorCode::NFS4ERR_NOTEMPTY => write!(f, "directory not empty"),
            Nfs4ErrorCode::NFS4ERR_DQUOT => write!(f, "quota exceeded"),
            Nfs4ErrorCode::NFS4ERR_STALE => write!(f, "stale file handle"),
            Nfs4ErrorCode::NFS4ERR_BADHANDLE => write!(f, "illegal file handle"),
            Nfs4ErrorCode::NFS4ERR_BAD_COOKIE => write!(f, "stale cookie"),
            Nfs4ErrorCode::NFS4ERR_NOTSUPP => write!(f, "operation not supported"),
            Nfs4ErrorCode::NFS4ERR_TOOSMALL => write!(f, "buffer too small"),
            Nfs4ErrorCode::NFS4ERR_SERVERFAULT => write!(f, "server fault"),
            Nfs4ErrorCode::NFS4ERR_BADTYPE => write!(f, "bad type"),
            Nfs4ErrorCode::NFS4ERR_DELAY => write!(f, "server busy, retry later"),
            Nfs4ErrorCode::NFS4ERR_SAME => write!(f, "no change"),
            Nfs4ErrorCode::NFS4ERR_DENIED => write!(f, "lock denied"),
            Nfs4ErrorCode::NFS4ERR_EXPIRED => write!(f, "lock/stateid expired"),
            Nfs4ErrorCode::NFS4ERR_LOCKED => write!(f, "file locked"),
            Nfs4ErrorCode::NFS4ERR_GRACE => write!(f, "server in grace period"),
            Nfs4ErrorCode::NFS4ERR_FHEXPIRED => write!(f, "file handle expired"),
            Nfs4ErrorCode::NFS4ERR_SHARE_DENIED => write!(f, "share denied"),
            Nfs4ErrorCode::NFS4ERR_WRONGSEC => write!(f, "wrong security flavor"),
            Nfs4ErrorCode::NFS4ERR_CLID_INUSE => write!(f, "client ID in use"),
            Nfs4ErrorCode::NFS4ERR_MOVED => write!(f, "filesystem moved"),
            Nfs4ErrorCode::NFS4ERR_NOFILEHANDLE => write!(f, "no current filehandle"),
            Nfs4ErrorCode::NFS4ERR_MINOR_VERS_MISMATCH => write!(f, "minor version mismatch"),
            Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID => write!(f, "stale client ID"),
            Nfs4ErrorCode::NFS4ERR_STALE_STATEID => write!(f, "stale state ID"),
            Nfs4ErrorCode::NFS4ERR_OLD_STATEID => write!(f, "old state ID"),
            Nfs4ErrorCode::NFS4ERR_BAD_STATEID => write!(f, "bad state ID"),
            Nfs4ErrorCode::NFS4ERR_BAD_SEQID => write!(f, "bad sequence ID"),
            Nfs4ErrorCode::NFS4ERR_NOT_SAME => write!(f, "verify/nverify mismatch"),
            Nfs4ErrorCode::NFS4ERR_LOCK_RANGE => write!(f, "lock range not supported"),
            Nfs4ErrorCode::NFS4ERR_SYMLINK => write!(f, "symlink encountered"),
            Nfs4ErrorCode::NFS4ERR_RESTOREFH => write!(f, "no saved filehandle"),
            Nfs4ErrorCode::NFS4ERR_LEASE_MOVED => write!(f, "lease moved"),
            Nfs4ErrorCode::NFS4ERR_ATTRNOTSUPP => write!(f, "attribute not supported"),
            Nfs4ErrorCode::NFS4ERR_NO_GRACE => write!(f, "not in grace period"),
            Nfs4ErrorCode::NFS4ERR_RECLAIM_BAD => write!(f, "reclaim error"),
            Nfs4ErrorCode::NFS4ERR_RECLAIM_CONFLICT => write!(f, "reclaim conflict"),
            Nfs4ErrorCode::NFS4ERR_BADXDR => write!(f, "bad XDR"),
            Nfs4ErrorCode::NFS4ERR_LOCKS_HELD => write!(f, "locks still held"),
            Nfs4ErrorCode::NFS4ERR_OPENMODE => write!(f, "open mode conflict"),
            Nfs4ErrorCode::NFS4ERR_BADOWNER => write!(f, "bad owner"),
            Nfs4ErrorCode::NFS4ERR_BADCHAR => write!(f, "bad character in name"),
            Nfs4ErrorCode::NFS4ERR_BADNAME => write!(f, "bad name"),
            Nfs4ErrorCode::NFS4ERR_BAD_RANGE => write!(f, "bad lock range"),
            Nfs4ErrorCode::NFS4ERR_LOCK_NOTSUPP => write!(f, "lock not supported"),
            Nfs4ErrorCode::NFS4ERR_OP_ILLEGAL => write!(f, "illegal operation"),
            Nfs4ErrorCode::NFS4ERR_DEADLOCK => write!(f, "deadlock"),
            Nfs4ErrorCode::NFS4ERR_FILE_OPEN => write!(f, "file is open"),
            Nfs4ErrorCode::NFS4ERR_ADMIN_REVOKED => write!(f, "admin revoked"),
            Nfs4ErrorCode::NFS4ERR_CB_PATH_DOWN => write!(f, "callback path down"),
            // NFSv4.1-specific errors
            Nfs4ErrorCode::NFS4ERR_BADIOMODE => write!(f, "bad I/O mode"),
            Nfs4ErrorCode::NFS4ERR_BADLAYOUT => write!(f, "bad layout"),
            Nfs4ErrorCode::NFS4ERR_BAD_SESSION_DIGEST => write!(f, "bad session digest"),
            Nfs4ErrorCode::NFS4ERR_BADSESSION => write!(f, "bad session"),
            Nfs4ErrorCode::NFS4ERR_BADSLOT => write!(f, "bad slot"),
            Nfs4ErrorCode::NFS4ERR_COMPLETE_ALREADY => write!(f, "reclaim complete already"),
            Nfs4ErrorCode::NFS4ERR_CONN_NOT_BOUND_TO_SESSION => {
                write!(f, "connection not bound to session")
            }
            Nfs4ErrorCode::NFS4ERR_DELEG_ALREADY_WANTED => write!(f, "delegation already wanted"),
            Nfs4ErrorCode::NFS4ERR_BACK_CHAN_BUSY => write!(f, "backchannel busy"),
            Nfs4ErrorCode::NFS4ERR_LAYOUTTRYLATER => write!(f, "layout try later"),
            Nfs4ErrorCode::NFS4ERR_LAYOUTUNAVAILABLE => write!(f, "layout unavailable"),
            Nfs4ErrorCode::NFS4ERR_NOMATCHING_LAYOUT => write!(f, "no matching layout"),
            Nfs4ErrorCode::NFS4ERR_RECALLCONFLICT => write!(f, "recall conflict"),
            Nfs4ErrorCode::NFS4ERR_UNKNOWN_LAYOUTTYPE => write!(f, "unknown layout type"),
            Nfs4ErrorCode::NFS4ERR_SEQ_MISORDERED => write!(f, "sequence misordered"),
            Nfs4ErrorCode::NFS4ERR_SEQUENCE_POS => write!(f, "sequence position error"),
            Nfs4ErrorCode::NFS4ERR_REQ_TOO_BIG => write!(f, "request too big"),
            Nfs4ErrorCode::NFS4ERR_REP_TOO_BIG => write!(f, "reply too big"),
            Nfs4ErrorCode::NFS4ERR_REP_TOO_BIG_TO_CACHE => write!(f, "reply too big to cache"),
            Nfs4ErrorCode::NFS4ERR_RETRY_UNCACHED_REP => write!(f, "retry uncached reply"),
            Nfs4ErrorCode::NFS4ERR_UNSAFE_COMPOUND => write!(f, "unsafe compound"),
            Nfs4ErrorCode::NFS4ERR_TOO_MANY_OPS => write!(f, "too many operations"),
            Nfs4ErrorCode::NFS4ERR_OP_NOT_IN_SESSION => write!(f, "operation not in session"),
            Nfs4ErrorCode::NFS4ERR_HASH_ALG_UNSUPP => write!(f, "hash algorithm unsupported"),
            Nfs4ErrorCode::NFS4ERR_CLIENTID_BUSY => write!(f, "client ID busy"),
            Nfs4ErrorCode::NFS4ERR_PNFS_IO_HOLE => write!(f, "pNFS I/O hole"),
            Nfs4ErrorCode::NFS4ERR_SEQ_FALSE_RETRY => write!(f, "sequence false retry"),
            Nfs4ErrorCode::NFS4ERR_BAD_HIGH_SLOT => write!(f, "bad high slot"),
            Nfs4ErrorCode::NFS4ERR_DEADSESSION => write!(f, "dead session"),
            Nfs4ErrorCode::NFS4ERR_ENCR_ALG_UNSUPP => write!(f, "encryption algorithm unsupported"),
            Nfs4ErrorCode::NFS4ERR_PNFS_NO_LAYOUT => write!(f, "pNFS no layout"),
            Nfs4ErrorCode::NFS4ERR_NOT_ONLY_OP => write!(f, "not the only operation"),
            Nfs4ErrorCode::NFS4ERR_WRONG_CRED => write!(f, "wrong credentials"),
            Nfs4ErrorCode::NFS4ERR_WRONG_TYPE => write!(f, "wrong type"),
            Nfs4ErrorCode::NFS4ERR_DIRDELEG_UNAVAIL => {
                write!(f, "directory delegation unavailable")
            }
            Nfs4ErrorCode::NFS4ERR_REJECT_DELEG => write!(f, "delegation rejected"),
            Nfs4ErrorCode::NFS4ERR_RETURNCONFLICT => write!(f, "return conflict"),
            Nfs4ErrorCode::NFS4ERR_DELEG_REVOKED => write!(f, "delegation revoked"),
        }
    }
}

/// RFC 7530 common attribute numbers.
pub(crate) mod attrnum {
    pub(crate) const ACL: u32 = 12;
    pub(crate) const ACLSUPPORT: u32 = 13;
    pub(crate) const DACL: u32 = 58;
    pub(crate) const SACL: u32 = 59;
    pub(crate) const FILEHANDLE: u32 = 19;
    pub(crate) const FILEID: u32 = 20;
    pub(crate) const MODE: u32 = 33;
}

#[cfg(test)]
mod tests {
    use super::{Nfs4ErrorCode, attrnum};

    #[test]
    fn rfc7530_common_attribute_numbers_are_stable() {
        assert_eq!(attrnum::ACL, 12);
        assert_eq!(attrnum::ACLSUPPORT, 13);
        assert_eq!(attrnum::FILEHANDLE, 19);
        assert_eq!(attrnum::FILEID, 20);
        assert_eq!(attrnum::MODE, 33);
    }

    #[test]
    fn rfc7530_status_discriminants_match_the_wire_registry() {
        assert_eq!(Nfs4ErrorCode::NFS4_OK as u32, 0);
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BAD_STATEID as u32, 10025);
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BAD_SEQID as u32, 10026);
    }

    #[test]
    fn nfsv41_status_extensions_remain_explicit() {
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BADIOMODE as u32, 10049);
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BADSESSION as u32, 10052);
        assert_eq!(Nfs4ErrorCode::NFS4ERR_SEQ_MISORDERED as u32, 10063);
    }

    #[test]
    fn shared_attribute_documentation_does_not_claim_a_v41_numbering_shift() {
        let source = include_str!("attrs.rs").to_ascii_lowercase();
        assert!(!source.contains("new in nfsv4.1"));
        assert!(!source.contains("shifted +1 from v4.0"));
        assert!(!source.contains("nfsv4.1 numbering"));
    }
}

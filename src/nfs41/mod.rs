//! Shared NFSv4.1/v4.2 client implementation (RFC 8881, RFC 7862 §2).
//!
//! Two-layer design matching NFSv3:
//! - Inner `Mount41`: protocol-specific implementation
//! - Outer `Mount41Wrapper`: implements `crate::Mount` trait

mod acl_ops;
pub(crate) mod callback;
pub(crate) mod compound;
mod dir_ops;
mod getattr;
pub(crate) mod layout;
pub(crate) mod lease;
mod lookup;
pub(crate) mod mount;
pub(crate) mod pnfs_io;
mod read;
mod readdir;
#[cfg(test)]
mod retry_tests;
pub(crate) mod session;
mod setattr;
pub(crate) mod state;
#[cfg(test)]
mod test_support;
pub(crate) mod v42_ops;
#[cfg(test)]
mod v42_tests;
pub(crate) mod v42_xattr;
#[cfg(test)]
mod version_tests;
mod write;
mod write_owner;
mod xattr;

// Re-export error code type for NfsError::Nfs4
pub use crate::nfs4::Nfs4ErrorCode;

/// NFSv4 program number (same as v3, different version)
pub(crate) const NFS4_PROGRAM: u32 = 100003;
/// NFSv4 RPC version
pub(crate) const NFS4_VERSION: u32 = 4;

/// A client ID and its sessions belong to one minor version (RFC 8178 §8.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum MinorVersion {
    #[default]
    V1 = 1,
    V2 = 2,
}

impl MinorVersion {
    pub(crate) fn version(self) -> crate::NFSVersion {
        match self {
            Self::V1 => crate::NFSVersion::NFSv4p1,
            Self::V2 => crate::NFSVersion::NFSv4p2,
        }
    }
}
/// COMPOUND procedure number (the only procedure in NFSv4)
pub(crate) const NFS4_COMPOUND_PROC: u32 = 1;
/// NULL procedure number
pub(crate) const NFS4_NULL_PROC: u32 = 0;
/// Default NFS port for v4.x (no portmapper needed)
pub(crate) const NFS4_DEFAULT_PORT: u16 = 2049;
pub(crate) const ONE_ATTEMPT: crate::rpc::ReplayPolicy = crate::rpc::ReplayPolicy::ONE_ATTEMPT;
pub(crate) const BOOTSTRAP_REPLAY: crate::rpc::ReplayPolicy =
    crate::rpc::ReplayPolicy::byte_identical(2);

#[cfg(test)]
mod tests {
    use super::Nfs4ErrorCode;

    #[test]
    fn nfs4_ok_display() {
        assert_eq!(
            Nfs4ErrorCode::NFS4_OK.to_string(),
            "call completed successfully"
        );
    }

    #[test]
    fn nfs4_common_errors_display() {
        assert_eq!(Nfs4ErrorCode::NFS4ERR_PERM.to_string(), "permission denied");
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_NOENT.to_string(),
            "no such file or directory"
        );
        assert_eq!(Nfs4ErrorCode::NFS4ERR_ACCESS.to_string(), "access denied");
        assert_eq!(Nfs4ErrorCode::NFS4ERR_EXIST.to_string(), "file exists");
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_STALE.to_string(),
            "stale file handle"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_NOSPC.to_string(),
            "no space left on device"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_ROFS.to_string(),
            "read-only file system"
        );
    }

    #[test]
    fn nfs4_session_errors_display() {
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BADSESSION.to_string(), "bad session");
        assert_eq!(Nfs4ErrorCode::NFS4ERR_BADSLOT.to_string(), "bad slot");
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_SEQ_MISORDERED.to_string(),
            "sequence misordered"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_DEADSESSION.to_string(),
            "dead session"
        );
    }

    #[test]
    fn nfs4_state_errors_display() {
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_BAD_STATEID.to_string(),
            "bad state ID"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_EXPIRED.to_string(),
            "lock/stateid expired"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_GRACE.to_string(),
            "server in grace period"
        );
        assert_eq!(Nfs4ErrorCode::NFS4ERR_LOCKED.to_string(), "file locked");
    }

    #[test]
    fn nfs4_pnfs_errors_display() {
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_LAYOUTTRYLATER.to_string(),
            "layout try later"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_LAYOUTUNAVAILABLE.to_string(),
            "layout unavailable"
        );
        assert_eq!(
            Nfs4ErrorCode::NFS4ERR_NOMATCHING_LAYOUT.to_string(),
            "no matching layout"
        );
    }

    #[test]
    fn nfs4_error_is_error_trait() {
        let err: Box<dyn std::error::Error> = Box::new(Nfs4ErrorCode::NFS4ERR_IO);
        assert!(err.to_string().contains("i/o error"));
    }

    #[test]
    fn nfs4_error_copy_clone() {
        let e = Nfs4ErrorCode::NFS4ERR_NOENT;
        let e2 = e; // Copy
        #[allow(clippy::clone_on_copy)]
        let e3 = e.clone(); // Clone (intentional — verifying Clone impl exists)
        assert_eq!(e, e2);
        assert_eq!(e, e3);
    }

    #[test]
    fn nfs4_constants() {
        assert_eq!(super::NFS4_PROGRAM, 100003);
        assert_eq!(super::NFS4_VERSION, 4);
        assert_eq!(super::MinorVersion::V1 as u32, 1);
        assert_eq!(super::NFS4_COMPOUND_PROC, 1);
        assert_eq!(super::NFS4_NULL_PROC, 0);
        assert_eq!(super::NFS4_DEFAULT_PORT, 2049);
    }
}

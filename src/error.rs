// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

/// Structured error type for NFS operations.
///
/// Replaces the previous `std::io::Error` + `ErrorKind::Other` pattern,
/// preserving NFS protocol error codes for programmatic matching.
#[derive(Error, Debug)]
pub enum NfsError {
    /// Underlying I/O or network error.
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// NFS3 protocol error (nfsstat3).
    #[error("NFS3 error: {0}")]
    Nfs3(crate::nfs3::ErrorCode),

    /// NFS4 protocol error (nfsstat4).
    #[error("NFS4 error: {0}")]
    Nfs4(crate::nfs41::Nfs4ErrorCode),

    /// MOUNT protocol error (mount_mountstat3).
    #[error("Mount error: {0}")]
    Mount(crate::nfs3::MountErrorCode),

    /// RPC-level error (rejected, mismatch, truncated response).
    #[error("RPC error: {0}")]
    Rpc(String),

    /// XDR encoding/decoding error.
    #[error("XDR error: {0}")]
    Xdr(String),

    /// Operation not supported by this NFS version.
    #[error("{0}")]
    Unsupported(String),

    /// Invalid input (bad URL, bad parameters).
    #[error("{0}")]
    InvalidInput(String),

    /// Server returned rdattr_error for an entry (READDIRPLUS per-entry error).
    #[error("rdattr_error: server returned nfsstat4 {0} for entry attributes")]
    RdattrError(u32),
}

impl NfsError {
    /// 目标已存在（NFS3ERR_EXIST / NFS4ERR_EXIST，errno 17）
    pub fn is_exist(&self) -> bool {
        matches!(
            self,
            NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_EXIST)
                | NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_EXIST)
        )
    }

    /// 目标不存在（NFS3ERR_NOENT / NFS4ERR_NOENT，errno 2）
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT)
                | NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_NOENT)
        )
    }

    /// Returns the corresponding `std::io::ErrorKind` for backward compatibility
    /// with code that matches on error kinds.
    pub fn kind(&self) -> std::io::ErrorKind {
        match self {
            NfsError::Io(io) => io.kind(),
            NfsError::Nfs3(_) => std::io::ErrorKind::Other,
            NfsError::Nfs4(_) => std::io::ErrorKind::Other,
            NfsError::Mount(_) => std::io::ErrorKind::Other,
            NfsError::Rpc(_) => std::io::ErrorKind::Other,
            NfsError::Xdr(_) => std::io::ErrorKind::Other,
            NfsError::Unsupported(_) => std::io::ErrorKind::Unsupported,
            NfsError::InvalidInput(_) => std::io::ErrorKind::InvalidInput,
            NfsError::RdattrError(_) => std::io::ErrorKind::Other,
        }
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, NfsError>;

/// Convert NfsError back to std::io::Error for backward compatibility.
impl From<NfsError> for std::io::Error {
    fn from(e: NfsError) -> Self {
        match e {
            NfsError::Io(io) => io,
            NfsError::Nfs3(code) => std::io::Error::other(code),
            NfsError::Nfs4(code) => std::io::Error::other(code),
            NfsError::Mount(code) => std::io::Error::other(code),
            NfsError::Rpc(msg) => std::io::Error::other(msg),
            NfsError::Xdr(msg) => std::io::Error::other(msg),
            NfsError::Unsupported(msg) => std::io::Error::new(std::io::ErrorKind::Unsupported, msg),
            NfsError::InvalidInput(msg) => std::io::Error::new(std::io::ErrorKind::InvalidInput, msg),
            NfsError::RdattrError(code) => {
                std::io::Error::other(format!("rdattr_error: nfsstat4 {}", code))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nfs3_error_display() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_PERM);
        assert!(err.to_string().contains("NFS3 error"));
    }

    #[test]
    fn nfs4_error_display() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_PERM);
        assert!(err.to_string().contains("NFS4 error"));
        assert!(err.to_string().contains("permission denied"));
    }

    #[test]
    fn nfs4_error_kind_is_other() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_STALE);
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn mount_error_display() {
        let err = NfsError::Mount(crate::nfs3::MountErrorCode::MNT3ERR_PERM);
        assert!(err.to_string().contains("Mount error"));
    }

    #[test]
    fn rpc_error_display() {
        let err = NfsError::Rpc("bad response".to_string());
        assert_eq!(err.to_string(), "RPC error: bad response");
    }

    #[test]
    fn xdr_error_display() {
        let err = NfsError::Xdr("truncated".to_string());
        assert_eq!(err.to_string(), "XDR error: truncated");
    }

    #[test]
    fn unsupported_display() {
        let err = NfsError::Unsupported("NFSv4 required".to_string());
        assert_eq!(err.to_string(), "NFSv4 required");
    }

    #[test]
    fn invalid_input_display() {
        let err = NfsError::InvalidInput("bad URL".to_string());
        assert_eq!(err.to_string(), "bad URL");
    }

    #[test]
    fn io_error_transparent_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection lost");
        let err = NfsError::Io(io_err);
        assert_eq!(err.to_string(), "connection lost");
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        let nfs_err: NfsError = io_err.into();
        assert!(matches!(nfs_err, NfsError::Io(_)));
    }

    #[test]
    fn kind_io_preserves_inner() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = NfsError::Io(io_err);
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn kind_nfs3_is_other() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT);
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn kind_unsupported() {
        let err = NfsError::Unsupported("test".to_string());
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    }

    #[test]
    fn kind_invalid_input() {
        let err = NfsError::InvalidInput("test".to_string());
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn is_exist_nfs3() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_EXIST);
        assert!(err.is_exist());
    }

    #[test]
    fn is_exist_nfs4() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_EXIST);
        assert!(err.is_exist());
    }

    #[test]
    fn is_exist_false_for_other() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(!err.is_exist());
    }

    #[test]
    fn is_not_found_nfs3() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT);
        assert!(err.is_not_found());
    }

    #[test]
    fn is_not_found_nfs4() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(err.is_not_found());
    }

    #[test]
    fn is_not_found_false_for_exist() {
        let err = NfsError::Nfs4(crate::nfs41::Nfs4ErrorCode::NFS4ERR_EXIST);
        assert!(!err.is_not_found());
    }

    #[test]
    fn into_io_error_roundtrip() {
        let nfs_err = NfsError::Rpc("test rpc error".to_string());
        let io_err: std::io::Error = nfs_err.into();
        assert_eq!(io_err.kind(), std::io::ErrorKind::Other);
        assert!(io_err.to_string().contains("test rpc error"));
    }

    #[test]
    fn into_io_error_preserves_io() {
        let original = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let nfs_err = NfsError::Io(original);
        let io_err: std::io::Error = nfs_err.into();
        assert_eq!(io_err.kind(), std::io::ErrorKind::BrokenPipe);
    }
}

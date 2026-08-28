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

use crate::mount::NFSVersion;
use thiserror::Error;

/// Retry/recovery safety of an NFS operation after an authoritative result is unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationOutcome {
    /// The request is known not to have taken effect.
    DefiniteFailure,
    /// Repeating the operation is safe because it is read-only or idempotent.
    SafeToRetry,
    /// The request may have taken effect; blind retry can duplicate a mutation.
    Uncertain,
}

/// Protocol-level side-effect class used for migration recovery decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationClass {
    ReadOnly,
    SessionControl,
    ReplaySensitive,
}

/// Recovery action recommended to callers without requiring string parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    Retry,
    Reopen,
    Remount,
    VerifyThenResume,
    DoNotRetry,
}

/// Whether the request crossed the transport send boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestTransmission {
    /// The core proved that no request bytes were sent.
    NotSent,
    /// The request crossed the send boundary and may have reached the server.
    Sent,
}

#[derive(Debug)]
struct TransportFailure {
    transmission: RequestTransmission,
    source: Box<NfsError>,
}

impl std::fmt::Display for TransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for TransportFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// Opaque, bounded request identity. It deliberately excludes file handles and payload bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestId {
    kind: RequestIdKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RequestIdKind {
    #[allow(dead_code)]
    Nfs40Owner { owner: u64, sequence_id: u32 },
    Nfs41Session {
        session_id: [u8; 16],
        slot_id: u32,
        sequence_id: u32,
    },
}

impl RequestId {
    pub(crate) fn nfs41(session_id: [u8; 16], slot_id: u32, sequence_id: u32) -> Self {
        Self {
            kind: RequestIdKind::Nfs41Session {
                session_id,
                slot_id,
                sequence_id,
            },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn nfs40(owner: u64, sequence_id: u32) -> Self {
        Self {
            kind: RequestIdKind::Nfs40Owner { owner, sequence_id },
        }
    }
}

/// Protocol-neutral context for classifying a request with an uncertain outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub operation: String,
    pub protocol: NFSVersion,
    pub request_id: Option<RequestId>,
}

/// Structured disposition for an operation whose normal result was not returned.
#[derive(Error, Debug)]
#[error("{outcome:?} NFS operation {operation_class:?}; recovery={recovery:?}")]
pub struct OperationOutcomeError {
    pub outcome: OperationOutcome,
    pub operation_class: OperationClass,
    pub transmission: RequestTransmission,
    pub recovery: RecoveryAction,
    pub completed_bytes: Option<u64>,
    #[source]
    pub source: Box<NfsError>,
    context: RequestContext,
}

impl OperationOutcomeError {
    pub fn new(
        outcome: OperationOutcome,
        operation_class: OperationClass,
        recovery: RecoveryAction,
        context: RequestContext,
        source: NfsError,
    ) -> Self {
        Self {
            outcome,
            operation_class,
            transmission: RequestTransmission::Sent,
            recovery,
            completed_bytes: None,
            source: Box::new(source),
            context,
        }
    }

    pub fn context(&self) -> &RequestContext {
        &self.context
    }

    /// Records bytes completed by authoritative preceding chunks. The current
    /// uncertain chunk must never be included in this count.
    pub fn with_completed_bytes(mut self, completed_bytes: u64) -> Self {
        self.completed_bytes = Some(completed_bytes);
        self
    }
}

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
    Nfs4(crate::nfs4::Nfs4ErrorCode),

    /// NFSv4 byte-range lock conflict, including the server-reported range.
    #[error("NFS4 lock denied: type {lock_type}, offset {offset}, length {length}")]
    LockDenied {
        lock_type: u32,
        offset: u64,
        length: u64,
        owner: bytes::Bytes,
    },

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

    /// An operation failed without an authoritative result and carries retry guidance.
    #[error(transparent)]
    OperationOutcome(#[from] Box<OperationOutcomeError>),
}

impl NfsError {
    pub(crate) fn transport(transmission: RequestTransmission, source: NfsError) -> Self {
        let kind = source.kind();
        Self::Io(std::io::Error::new(
            kind,
            TransportFailure {
                transmission,
                source: Box::new(source),
            },
        ))
    }

    /// Returns transport send-boundary evidence when it is available.
    pub fn request_transmission(&self) -> Option<RequestTransmission> {
        match self {
            Self::Io(error) => error
                .get_ref()
                .and_then(|source| source.downcast_ref::<TransportFailure>())
                .map(|failure| failure.transmission),
            Self::OperationOutcome(error) => Some(error.transmission),
            _ => None,
        }
    }

    /// Wraps a failure for which the caller proved that the current request did
    /// not cross the transport send boundary.
    pub fn before_send_failure(
        operation_class: OperationClass,
        context: RequestContext,
        completed_bytes: Option<u64>,
        source: NfsError,
    ) -> Self {
        let mut error = OperationOutcomeError::new(
            OperationOutcome::DefiniteFailure,
            operation_class,
            RecoveryAction::Retry,
            context,
            source,
        );
        error.transmission = RequestTransmission::NotSent;
        error.completed_bytes = completed_bytes;
        Self::OperationOutcome(Box::new(error))
    }

    /// Returns structured retry/verification guidance when the result is non-authoritative.
    pub fn operation_outcome(&self) -> Option<&OperationOutcomeError> {
        match self {
            Self::OperationOutcome(error) => Some(error),
            _ => None,
        }
    }

    /// 目标已存在（NFS3ERR_EXIST / NFS4ERR_EXIST，errno 17）
    pub fn is_exist(&self) -> bool {
        matches!(
            self,
            NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_EXIST)
                | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_EXIST)
        )
    }

    /// 目标不存在（NFS3ERR_NOENT / NFS4ERR_NOENT，errno 2）
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT)
                | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOENT)
        )
    }

    /// Returns the corresponding `std::io::ErrorKind` for backward compatibility
    /// with code that matches on error kinds.
    pub fn kind(&self) -> std::io::ErrorKind {
        match self {
            NfsError::Io(io) => io.kind(),
            NfsError::Nfs3(_) => std::io::ErrorKind::Other,
            NfsError::Nfs4(_) => std::io::ErrorKind::Other,
            NfsError::LockDenied { .. } => std::io::ErrorKind::WouldBlock,
            NfsError::Mount(_) => std::io::ErrorKind::Other,
            NfsError::Rpc(_) => std::io::ErrorKind::Other,
            NfsError::Xdr(_) => std::io::ErrorKind::Other,
            NfsError::Unsupported(_) => std::io::ErrorKind::Unsupported,
            NfsError::InvalidInput(_) => std::io::ErrorKind::InvalidInput,
            NfsError::RdattrError(_) => std::io::ErrorKind::Other,
            NfsError::OperationOutcome(_) => std::io::ErrorKind::Other,
        }
    }
}

pub(crate) fn classify_sent_nfs41_error(
    operation_class: OperationClass,
    context: RequestContext,
    source: NfsError,
) -> NfsError {
    let replay_protocol_error = matches!(
        &source,
        NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_RETRY_UNCACHED_REP)
            | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_SEQ_FALSE_RETRY)
    );
    classify_non_authoritative_error(operation_class, context, source, replay_protocol_error)
}

pub(crate) fn classify_sent_nfs3_error(
    operation_class: OperationClass,
    context: RequestContext,
    source: NfsError,
) -> NfsError {
    classify_non_authoritative_error(operation_class, context, source, false)
}

pub(crate) fn classify_sent_nfs40_error(
    operation_class: OperationClass,
    context: RequestContext,
    source: NfsError,
) -> NfsError {
    classify_non_authoritative_error(operation_class, context, source, false)
}

fn classify_non_authoritative_error(
    operation_class: OperationClass,
    context: RequestContext,
    source: NfsError,
    replay_protocol_error: bool,
) -> NfsError {
    if source.request_transmission() == Some(RequestTransmission::NotSent) {
        return NfsError::before_send_failure(operation_class, context, None, source);
    }

    let lacks_authoritative_result = replay_protocol_error
        || source.request_transmission() == Some(RequestTransmission::Sent)
        || matches!(
            &source,
            NfsError::Io(_) | NfsError::Rpc(_) | NfsError::Xdr(_)
        );
    if !lacks_authoritative_result {
        return source;
    }
    let (outcome, recovery) = match operation_class {
        OperationClass::ReadOnly => (OperationOutcome::SafeToRetry, RecoveryAction::Retry),
        OperationClass::SessionControl => (OperationOutcome::Uncertain, RecoveryAction::Remount),
        OperationClass::ReplaySensitive => (
            OperationOutcome::Uncertain,
            RecoveryAction::VerifyThenResume,
        ),
    };
    NfsError::OperationOutcome(Box::new(OperationOutcomeError::new(
        outcome,
        operation_class,
        recovery,
        context,
        source,
    )))
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
            error @ NfsError::LockDenied { .. } => {
                std::io::Error::new(std::io::ErrorKind::WouldBlock, error)
            }
            NfsError::Mount(code) => std::io::Error::other(code),
            NfsError::Rpc(msg) => std::io::Error::other(msg),
            NfsError::Xdr(msg) => std::io::Error::other(msg),
            NfsError::Unsupported(msg) => std::io::Error::new(std::io::ErrorKind::Unsupported, msg),
            NfsError::InvalidInput(msg) => {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
            }
            NfsError::RdattrError(code) => {
                std::io::Error::other(format!("rdattr_error: nfsstat4 {}", code))
            }
            NfsError::OperationOutcome(error) => std::io::Error::other(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn nfs3_error_display() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_PERM);
        assert!(err.to_string().contains("NFS3 error"));
    }

    #[test]
    fn nfs4_error_display() {
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_PERM);
        assert!(err.to_string().contains("NFS4 error"));
        assert!(err.to_string().contains("permission denied"));
    }

    #[test]
    fn nfs4_error_kind_is_other() {
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_STALE);
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
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_EXIST);
        assert!(err.is_exist());
    }

    #[test]
    fn is_exist_false_for_other() {
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(!err.is_exist());
    }

    #[test]
    fn is_not_found_nfs3() {
        let err = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT);
        assert!(err.is_not_found());
    }

    #[test]
    fn is_not_found_nfs4() {
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOENT);
        assert!(err.is_not_found());
    }

    #[test]
    fn is_not_found_false_for_exist() {
        let err = NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_EXIST);
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

    fn context() -> RequestContext {
        RequestContext {
            operation: "write".to_string(),
            protocol: NFSVersion::NFSv4p1,
            request_id: Some(RequestId::nfs41([7; 16], 3, 9)),
        }
    }

    #[test]
    fn sent_read_only_transport_failure_is_safe_to_retry() {
        let error = classify_sent_nfs41_error(
            OperationClass::ReadOnly,
            context(),
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "reply timeout",
            )),
        );
        let outcome = error
            .operation_outcome()
            .expect("outcome must be structured");
        assert_eq!(outcome.outcome, OperationOutcome::SafeToRetry);
        assert_eq!(outcome.recovery, RecoveryAction::Retry);
        assert_eq!(outcome.context(), &context());
        assert!(outcome.source().is_some());
    }

    #[test]
    fn sent_modifying_transport_failure_is_uncertain() {
        let error = classify_sent_nfs41_error(
            OperationClass::ReplaySensitive,
            context(),
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "lost after send",
            )),
        );
        let outcome = error
            .operation_outcome()
            .expect("outcome must be structured");
        assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
        assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
        assert_eq!(outcome.operation_class, OperationClass::ReplaySensitive);
    }

    #[test]
    fn replay_protocol_errors_have_operation_aware_outcomes() {
        for code in [
            crate::nfs4::Nfs4ErrorCode::NFS4ERR_RETRY_UNCACHED_REP,
            crate::nfs4::Nfs4ErrorCode::NFS4ERR_SEQ_FALSE_RETRY,
        ] {
            let read = classify_sent_nfs41_error(
                OperationClass::ReadOnly,
                context(),
                NfsError::Nfs4(code),
            );
            assert_eq!(
                read.operation_outcome().map(|error| error.outcome),
                Some(OperationOutcome::SafeToRetry)
            );

            let write = classify_sent_nfs41_error(
                OperationClass::ReplaySensitive,
                context(),
                NfsError::Nfs4(code),
            );
            assert_eq!(
                write.operation_outcome().map(|error| error.outcome),
                Some(OperationOutcome::Uncertain)
            );
        }
    }

    #[test]
    fn authoritative_protocol_failure_remains_definite_and_unwrapped() {
        let error = classify_sent_nfs41_error(
            OperationClass::ReplaySensitive,
            context(),
            NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_ACCESS),
        );
        assert!(error.operation_outcome().is_none());
        assert!(matches!(
            error,
            NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_ACCESS)
        ));
    }

    #[test]
    fn outcome_error_preserves_source_without_payload_context() {
        let error = classify_sent_nfs41_error(
            OperationClass::ReplaySensitive,
            context(),
            NfsError::Rpc("truncated authoritative reply".to_string()),
        );
        let outcome = error
            .operation_outcome()
            .expect("outcome must be structured");
        assert!(
            matches!(&*outcome.source, NfsError::Rpc(message) if message.contains("truncated"))
        );
        let debug = format!("{outcome:?}");
        assert!(!debug.contains("file handle"));
        assert!(!debug.contains("payload"));
    }

    #[test]
    fn modifying_failure_before_send_is_definite_and_preserves_completed_bytes() {
        let error = NfsError::before_send_failure(
            OperationClass::ReplaySensitive,
            context(),
            Some(4096),
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "request was not sent",
            )),
        );
        let outcome = error
            .operation_outcome()
            .expect("outcome must be structured");

        assert_eq!(outcome.outcome, OperationOutcome::DefiniteFailure);
        assert_eq!(outcome.transmission, RequestTransmission::NotSent);
        assert_eq!(outcome.completed_bytes, Some(4096));
        assert_eq!(outcome.recovery, RecoveryAction::Retry);
    }

    #[test]
    fn sent_modifying_failure_records_sent_transmission() {
        let error = classify_sent_nfs41_error(
            OperationClass::ReplaySensitive,
            context(),
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "lost after send",
            )),
        );
        let outcome = error
            .operation_outcome()
            .expect("outcome must be structured");

        assert_eq!(outcome.transmission, RequestTransmission::Sent);
        assert_eq!(outcome.completed_bytes, None);
    }

    #[test]
    fn uncertain_chunked_operation_preserves_only_confirmed_bytes() {
        let error = OperationOutcomeError::new(
            OperationOutcome::Uncertain,
            OperationClass::ReplaySensitive,
            RecoveryAction::VerifyThenResume,
            context(),
            NfsError::Rpc("reply lost for current chunk".to_string()),
        )
        .with_completed_bytes(8192);

        assert_eq!(error.completed_bytes, Some(8192));
        assert_eq!(error.transmission, RequestTransmission::Sent);
    }

    #[test]
    fn sent_nfs3_mutation_without_reply_is_uncertain() {
        let mut nfs3_context = context();
        nfs3_context.protocol = NFSVersion::NFSv3;
        nfs3_context.request_id = None;
        let error = classify_sent_nfs3_error(
            OperationClass::ReplaySensitive,
            nfs3_context,
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "NFSv3 reply timeout",
            )),
        );
        let outcome = error
            .operation_outcome()
            .expect("sent NFSv3 mutation must have a structured outcome");

        assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
        assert_eq!(outcome.transmission, RequestTransmission::Sent);
        assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
    }

    #[test]
    fn nfs3_transport_evidence_preserves_before_send_failure() {
        let mut nfs3_context = context();
        nfs3_context.protocol = NFSVersion::NFSv3;
        nfs3_context.request_id = None;
        let error = classify_sent_nfs3_error(
            OperationClass::ReplaySensitive,
            nfs3_context,
            NfsError::transport(
                RequestTransmission::NotSent,
                NfsError::Rpc("connection was not ready".to_string()),
            ),
        );
        let outcome = error
            .operation_outcome()
            .expect("transport evidence must be preserved");

        assert_eq!(outcome.outcome, OperationOutcome::DefiniteFailure);
        assert_eq!(outcome.transmission, RequestTransmission::NotSent);
        assert_eq!(outcome.recovery, RecoveryAction::Retry);
    }
}

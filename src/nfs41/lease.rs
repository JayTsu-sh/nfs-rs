//! NFSv4.1 lease renewal via background SEQUENCE calls.
//!
//! RFC 5661 §8.3: The server maintains a lease for each client. If the lease
//! expires, the server may revoke the client's state (open files, locks, etc.).
//! The lease is renewed by any SEQUENCE operation, so a background task
//! periodically sends COMPOUND(SEQUENCE) to keep the session alive.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Buf;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::compound::{CompoundBuilder, CompoundResponse, OpNum};
use super::fastxdr::nfsstat4;
use super::session::SessionHolder;
use crate::error::{NfsError, Result};
use crate::rpc;
use crate::rpc::auth::Auth;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseHealth {
    Healthy = 0,
    TransportFailure = 1,
    ProtocolFailure = 2,
    IdentityMismatch = 3,
    SessionFailure = 4,
}

impl LeaseHealth {
    fn from_error(error: &NfsError) -> Self {
        match error {
            NfsError::Nfs4(
                nfsstat4::NFS4ERR_BADSESSION
                | nfsstat4::NFS4ERR_DEADSESSION
                | nfsstat4::NFS4ERR_SEQ_MISORDERED
                | nfsstat4::NFS4ERR_BADSLOT,
            ) => Self::SessionFailure,
            NfsError::Io(_) | NfsError::Rpc(_) => Self::TransportFailure,
            NfsError::Xdr(message) if message.contains("mismatch") => Self::IdentityMismatch,
            _ => Self::ProtocolFailure,
        }
    }

    fn load(value: &AtomicU8) -> Self {
        match value.load(Ordering::Acquire) {
            0 => Self::Healthy,
            1 => Self::TransportFailure,
            2 => Self::ProtocolFailure,
            3 => Self::IdentityMismatch,
            _ => Self::SessionFailure,
        }
    }
}

fn validate_renewal_response(
    response: &CompoundResponse,
    session_id: &[u8; 16],
    sequence_id: u32,
    slot_id: u32,
    highest_slot_id: u32,
) -> Result<u32> {
    response.check_status()?;
    if response.tag != "renew" {
        return Err(NfsError::Xdr("renewal tag mismatch".to_string()));
    }
    if response.results.len() != 1 {
        return Err(NfsError::Xdr(format!(
            "renewal response has {} operations, expected 1",
            response.results.len()
        )));
    }
    let sequence = response.op_ok(0)?;
    if sequence.opcode != OpNum::Sequence as u32 {
        return Err(NfsError::Xdr(format!(
            "renewal result 0 opcode mismatch: got {}, expected {}",
            sequence.opcode,
            OpNum::Sequence as u32
        )));
    }
    let mut data = sequence.data.clone();
    if data.remaining() != 36 {
        return Err(NfsError::Xdr(format!(
            "renewal SEQUENCE result length {}, expected 36",
            data.remaining()
        )));
    }
    let mut echoed_session = [0; 16];
    data.copy_to_slice(&mut echoed_session);
    let echoed_sequence = data.get_u32();
    let echoed_slot = data.get_u32();
    let echoed_highest = data.get_u32();
    let target_highest = data.get_u32();
    let status_flags = data.get_u32();
    if &echoed_session != session_id {
        return Err(NfsError::Xdr(
            "renewal SEQUENCE session mismatch".to_string(),
        ));
    }
    if echoed_sequence != sequence_id {
        return Err(NfsError::Xdr(format!(
            "renewal SEQUENCE sequence mismatch: got {echoed_sequence}, expected {sequence_id}"
        )));
    }
    if echoed_slot != slot_id {
        return Err(NfsError::Xdr(format!(
            "renewal SEQUENCE slot mismatch: got {echoed_slot}, expected {slot_id}"
        )));
    }
    if echoed_highest != highest_slot_id || target_highest > highest_slot_id {
        return Err(NfsError::Xdr(format!(
            "renewal SEQUENCE highest-slot mismatch: echoed {echoed_highest}, target {target_highest}, expected maximum {highest_slot_id}"
        )));
    }
    Ok(status_flags)
}

/// Background lease renewal task.
pub(crate) struct LeaseRenewal {
    handle: JoinHandle<()>,
    health: Arc<AtomicU8>,
}

impl LeaseRenewal {
    /// Start a background task that sends COMPOUND(SEQUENCE) every `interval`.
    ///
    /// The interval should be less than the server's lease time (typically 30-90s).
    /// Accepts `Arc<SessionHolder>` so that post-recovery session replacements are
    /// picked up automatically: each renewal iteration reads the current session
    /// from the holder rather than holding a stale `Arc<Session>`.
    pub fn start(
        rpc: rpc::Client,
        auth: Auth,
        session_holder: Arc<SessionHolder>,
        interval: Duration,
    ) -> Self {
        let health = Arc::new(AtomicU8::new(LeaseHealth::Healthy as u8));
        let task_health = Arc::clone(&health);
        let handle = tokio::spawn(async move {
            debug!(interval_secs = interval.as_secs(), "lease renewal started");
            loop {
                tokio::time::sleep(interval).await;
                // Always get the latest session so post-recovery renewals use the
                // new session ID rather than the old stale one.
                let session = session_holder.get().await;
                match session.acquire_slot().await {
                    Ok(mut slot) => {
                        let sequence_id = slot.sequence_id;
                        let slot_id = slot.slot_id;
                        let highest_slot_id = session.highest_slot_id();
                        let session_id = *session.id();
                        let builder = CompoundBuilder::new("renew").sequence(
                            &session_id,
                            sequence_id,
                            slot_id,
                            highest_slot_id,
                        );
                        let mut buf = Vec::new();
                        builder.encode_with_header(&auth, &mut buf);
                        slot.fence_on_drop();
                        let result = rpc
                            .call(buf, 1, Duration::from_secs(5))
                            .await
                            .and_then(CompoundResponse::decode)
                            .and_then(|response| {
                                validate_renewal_response(
                                    &response,
                                    &session_id,
                                    sequence_id,
                                    slot_id,
                                    highest_slot_id,
                                )
                            });
                        match result {
                            Ok(status_flags) => {
                                slot.advance();
                                slot.resolve();
                                task_health.store(LeaseHealth::Healthy as u8, Ordering::Release);
                                debug!(
                                    status_flags,
                                    "lease renewal: validated SEQUENCE successful"
                                );
                            }
                            Err(error) => {
                                let health = LeaseHealth::from_error(&error);
                                task_health.store(health as u8, Ordering::Release);
                                if matches!(error, NfsError::Nfs4(_)) {
                                    slot.resolve();
                                }
                                warn!(
                                    error = %error,
                                    health = ?health,
                                    "lease renewal: SEQUENCE validation failed"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        let health = LeaseHealth::from_error(&error);
                        task_health.store(health as u8, Ordering::Release);
                        warn!(
                            error = %error,
                            health = ?health,
                            "lease renewal: failed to acquire slot"
                        );
                    }
                };
            }
        });
        Self { handle, health }
    }

    pub fn health(&self) -> LeaseHealth {
        LeaseHealth::load(&self.health)
    }

    /// Stop the background renewal task.
    pub fn stop(&self) {
        self.handle.abort();
    }
}

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::compound::OpResponse;
    use super::*;

    const SESSION_ID: [u8; 16] = [0x31; 16];
    const SEQUENCE_ID: u32 = 9;
    const SLOT_ID: u32 = 2;
    const HIGHEST_SLOT_ID: u32 = 7;

    fn sequence_data() -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&SESSION_ID);
        data.extend_from_slice(&SEQUENCE_ID.to_be_bytes());
        data.extend_from_slice(&SLOT_ID.to_be_bytes());
        data.extend_from_slice(&HIGHEST_SLOT_ID.to_be_bytes());
        data.extend_from_slice(&HIGHEST_SLOT_ID.to_be_bytes());
        data.extend_from_slice(&0x400u32.to_be_bytes());
        data
    }

    fn response(status: nfsstat4, opcode: u32, data: Vec<u8>) -> CompoundResponse {
        CompoundResponse {
            tag: "renew".to_string(),
            status,
            results: vec![OpResponse {
                opcode,
                status,
                data: Bytes::from(data),
            }],
            session_generation: 1,
        }
    }

    fn validate(response: &CompoundResponse) -> Result<u32> {
        validate_renewal_response(response, &SESSION_ID, SEQUENCE_ID, SLOT_ID, HIGHEST_SLOT_ID)
    }

    #[test]
    fn valid_sequence_response_returns_status_flags() {
        let response = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, sequence_data());
        assert_eq!(validate(&response).unwrap(), 0x400);
    }

    #[test]
    fn non_ok_sequence_status_matrix_is_rejected() {
        for status in [
            nfsstat4::NFS4ERR_BADSESSION,
            nfsstat4::NFS4ERR_DEADSESSION,
            nfsstat4::NFS4ERR_SEQ_MISORDERED,
            nfsstat4::NFS4ERR_BADSLOT,
        ] {
            let response = response(status, OpNum::Sequence as u32, Vec::new());
            assert!(matches!(validate(&response), Err(NfsError::Nfs4(code)) if code == status));
        }
    }

    #[test]
    fn malformed_response_matrix_is_rejected() {
        let mut wrong_tag = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, sequence_data());
        wrong_tag.tag = "stale".to_string();
        assert!(validate(&wrong_tag).is_err());

        let mut no_results = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, sequence_data());
        no_results.results.clear();
        assert!(validate(&no_results).is_err());

        let wrong_opcode = response(nfsstat4::NFS4_OK, OpNum::GetAttr as u32, sequence_data());
        assert!(validate(&wrong_opcode).is_err());

        for len in [0, 4, 35, 37] {
            let truncated = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, vec![0; len]);
            assert!(validate(&truncated).is_err());
        }
    }

    #[test]
    fn echoed_identity_mismatch_matrix_is_rejected() {
        for offset in [0usize, 16, 20, 24] {
            let mut data = sequence_data();
            data[offset] ^= 1;
            let response = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, data);
            assert!(validate(&response).is_err(), "offset {offset} must fail");
        }

        let mut target_too_high = sequence_data();
        target_too_high[28..32].copy_from_slice(&(HIGHEST_SLOT_ID + 1).to_be_bytes());
        let response = response(nfsstat4::NFS4_OK, OpNum::Sequence as u32, target_too_high);
        assert!(validate(&response).is_err());
    }

    #[test]
    fn renewal_health_classifies_recovery_signals() {
        assert_eq!(
            LeaseHealth::from_error(&NfsError::Nfs4(nfsstat4::NFS4ERR_BADSESSION)),
            LeaseHealth::SessionFailure
        );
        assert_eq!(
            LeaseHealth::from_error(&NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timeout"
            ))),
            LeaseHealth::TransportFailure
        );
        assert_eq!(
            LeaseHealth::from_error(&NfsError::Xdr("slot mismatch".to_string())),
            LeaseHealth::IdentityMismatch
        );
    }
}

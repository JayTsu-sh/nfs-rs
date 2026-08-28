//! NFSv4.1 mount flow and `Mount` trait implementation.
//!
//! Mount flow (no portmapper, no separate MOUNT protocol):
//! 1. TCP connect to port 2049 (or user-specified nfsport)
//! 2. EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE (via Session::establish)
//! 3. PUTROOTFH + LOOKUP*n + GETFH to navigate to the export path
//! 4. GETATTR to query rsize/wsize limits

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, Bytes};
use tracing::{debug, info, warn};

use super::callback::{CallbackState, RecallNotification};
use super::compound::{CompoundBuilder, CompoundResponse};
use super::layout::{DsConnection, LayoutManager};
use super::lease::{LeaseHealth, LeaseRenewal};
use super::session::{ClientIdentity, Session, SessionHolder, validate_sequence_result};
use super::state::StateManager;
use super::{NFS4_DEFAULT_PORT, NFS4_NULL_PROC, NFS4_PROGRAM, NFS4_VERSION};
use crate::error::{
    NfsError, OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction,
    RequestContext, Result, classify_sent_nfs41_error,
};
use crate::mount::{self, NFSVersion, Nfs41CallbackStats, Nfs41ChannelLimits};
use crate::nfs4::fastxdr::nfsstat4;
use crate::rpc;
use crate::rpc::auth::Auth;

// Timeout for metadata operations.
const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
// Base timeout for data operations, scaled by payload size.
const DATA_TIMEOUT_BASE_SECS: u64 = 10;
const MIN_BANDWIDTH_BYTES_PER_SEC: u64 = 1_250_000;
static NEXT_MOUNT_ISSUER: AtomicU64 = AtomicU64::new(1);
// Retry counts
const NFS_RETRIES: usize = 10;
const NFS_REPLAY: crate::rpc::ReplayPolicy = crate::rpc::ReplayPolicy::byte_identical(NFS_RETRIES);
// Equal-jitter exponential backoff for NFS4ERR_DELAY/GRACE retries.
// Without jitter, concurrent workers (e.g. integrity-check parallel LOOKUPs)
// resync to the same retry instant and sustain the thundering-herd that
// triggered the error. Total expected DELAY wait across 17 attempts ≈ 46s
// (max < 61s), covering observed 60+ s server-busy windows.
pub(super) const DELAY_RETRY_MAX: usize = 16;
const DELAY_RETRY_BASE_MS: u64 = 200;
const DELAY_RETRY_CAP_MS: u64 = 5000;
const GRACE_RETRY_BASE_MS: u64 = 1000;
const GRACE_RETRY_CAP_MS: u64 = 8000;
const RPC_CALL_PREFIX_SIZE: usize = 8; // XID + message type; record marker is transport framing.
const MIN_RPC_REPLY_ENVELOPE_SIZE: usize = 24;

fn enforce_request_size(session: &Session, header_len: usize, data_len: usize) -> Result<()> {
    let encoded = header_len
        .checked_add(RPC_CALL_PREFIX_SIZE)
        .and_then(|size| size.checked_add(data_len))
        .and_then(|size| size.checked_add((4 - (data_len & 3)) & 3))
        .ok_or_else(|| NfsError::Rpc("encoded COMPOUND request size overflow".to_string()))?;
    let maximum = usize::try_from(session.max_request_size())
        .map_err(|_| NfsError::Rpc("channel request limit exceeds usize".to_string()))?;
    enforce_encoded_size("request", encoded, maximum)
}

fn enforce_response_size(session: &Session, response_len: usize) -> Result<()> {
    let maximum = usize::try_from(session.max_response_size())
        .map_err(|_| NfsError::Rpc("channel response limit exceeds usize".to_string()))?;
    let encoded = response_len
        .checked_add(MIN_RPC_REPLY_ENVELOPE_SIZE)
        .ok_or_else(|| NfsError::Rpc("encoded COMPOUND response size overflow".to_string()))?;
    enforce_encoded_size("response", encoded, maximum)
}

fn enforce_encoded_size(kind: &str, actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        return Err(NfsError::Rpc(format!(
            "encoded COMPOUND {kind} size {actual} exceeds channel maximum {maximum}"
        )));
    }
    Ok(())
}

fn enforce_response_operations(
    session: &Session,
    response: &CompoundResponse,
    requested: usize,
) -> Result<()> {
    let negotiated = usize::try_from(session.max_operations())
        .map_err(|_| NfsError::Rpc("channel operation limit exceeds usize".to_string()))?;
    if response.results.len() > negotiated || response.results.len() > requested {
        return Err(NfsError::Xdr(format!(
            "COMPOUND response contains {} operations; requested {requested}, channel maximum {negotiated}",
            response.results.len()
        )));
    }
    Ok(())
}

fn bind_connection_request(auth: &Auth, session_id: &[u8; 16]) -> Vec<u8> {
    let mut buf = Vec::new();
    CompoundBuilder::new("bind_conn")
        .bind_conn_to_session(session_id, 3, false)
        .encode_with_header(auth, &mut buf);
    buf
}

fn validate_bound_connection(response: Bytes, expected_session_id: &[u8; 16]) -> Result<u32> {
    let response = CompoundResponse::decode(response)?;
    let op = response.op_ok(0)?;
    let mut data = op.data.clone();
    if data.remaining() < 20 {
        return Err(NfsError::Xdr(
            "BIND_CONN_TO_SESSION result truncated".to_string(),
        ));
    }
    let mut confirmed_session_id = [0; 16];
    data.copy_to_slice(&mut confirmed_session_id);
    let direction = data.get_u32();
    if &confirmed_session_id != expected_session_id {
        return Err(NfsError::Rpc(
            "BIND_CONN_TO_SESSION returned a mismatched session ID".to_string(),
        ));
    }
    if direction & 2 == 0 {
        return Err(NfsError::Rpc(format!(
            "BIND_CONN_TO_SESSION did not restore the required backchannel: direction {direction}"
        )));
    }
    Ok(direction)
}

async fn bind_connection(
    client: &rpc::Client,
    auth: &Auth,
    session_id: &[u8; 16],
    during_reconnect: bool,
) -> Result<u32> {
    let request = bind_connection_request(auth, session_id);
    let response = if during_reconnect {
        client
            .call_during_reconnect(request, METADATA_TIMEOUT)
            .await?
    } else {
        client
            .call(request, super::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await?
    };
    validate_bound_connection(response, session_id)
}

fn request_context(
    tag: &str,
    session_id: &[u8; 16],
    slot_id: u32,
    sequence_id: u32,
) -> RequestContext {
    RequestContext {
        operation: tag.chars().take(64).collect(),
        protocol: NFSVersion::NFSv4p1,
        request_id: Some(crate::error::RequestId::nfs41(
            *session_id,
            slot_id,
            sequence_id,
        )),
    }
}

fn sent_error(
    operation_class: OperationClass,
    context: &RequestContext,
    error: NfsError,
) -> NfsError {
    classify_sent_nfs41_error(operation_class, context.clone(), error)
}

fn backoff_jitter_ms(attempt: usize, base_ms: u64, cap_ms: u64) -> u64 {
    let base = base_ms
        .saturating_mul(1u64.checked_shl(attempt as u32).unwrap_or(u64::MAX))
        .min(cap_ms);
    let half = base / 2;
    if half == 0 {
        return base;
    }
    rand::random_range(half..base)
}

pub(super) fn delay_with_jitter_ms(attempt: usize) -> u64 {
    backoff_jitter_ms(attempt, DELAY_RETRY_BASE_MS, DELAY_RETRY_CAP_MS)
}

pub(super) fn grace_with_jitter_ms(attempt: usize) -> u64 {
    backoff_jitter_ms(attempt, GRACE_RETRY_BASE_MS, GRACE_RETRY_CAP_MS)
}

// SEQ4_STATUS flag constants (RFC 5661 §2.10.6.2)
const SEQ4_STATUS_CB_PATH_DOWN: u32 = 0x0000_0001;
const SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED: u32 = 0x0000_0008;
const SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED: u32 = 0x0000_0010;
const SEQ4_STATUS_ADMIN_STATE_REVOKED: u32 = 0x0000_0020;
const SEQ4_STATUS_RECALLABLE_STATE_REVOKED: u32 = 0x0000_0040;
const SEQ4_STATUS_LEASE_MOVED: u32 = 0x0000_0080;
const SEQ4_STATUS_RESTART_RECLAIM_NEEDED: u32 = 0x0000_0100;
const SEQ4_STATUS_CB_PATH_DOWN_SESSION: u32 = 0x0000_0200;
const SEQ4_STATUS_BACKCHANNEL_FAULT: u32 = 0x0000_0400;
const SEQ4_STATUS_DEVID_CHANGED: u32 = 0x0000_0800;
const SEQ4_STATUS_DEVID_DELETED: u32 = 0x0000_1000;

/// Bitmask of all state-revocation flags.
const SEQ4_STATUS_STATE_REVOKED: u32 = SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED
    | SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED
    | SEQ4_STATUS_ADMIN_STATE_REVOKED
    | SEQ4_STATUS_RECALLABLE_STATE_REVOKED;

/// Bitmask of all backchannel-related flags.
const SEQ4_STATUS_CB_FLAGS: u32 =
    SEQ4_STATUS_CB_PATH_DOWN | SEQ4_STATUS_CB_PATH_DOWN_SESSION | SEQ4_STATUS_BACKCHANNEL_FAULT;

fn data_timeout(data_size: usize) -> std::time::Duration {
    let transfer_secs = data_size as u64 / MIN_BANDWIDTH_BYTES_PER_SEC;
    std::time::Duration::from_secs(DATA_TIMEOUT_BASE_SECS + transfer_secs)
}

/// Inner NFSv4.1 mount state.
pub(crate) struct Mount41 {
    pub(crate) rpc: rpc::Client,
    pub(crate) auth: Auth,
    pub(crate) root_fh: Bytes,
    /// Holder for atomic session replacement on BADSESSION/DEADSESSION recovery.
    pub(crate) session_holder: Arc<SessionHolder>,
    /// Active session identity and bounded replay cache for backchannel callbacks.
    callback_state: Arc<CallbackState>,
    /// Serializes session recovery publication for this mount.
    recovery_lock: tokio::sync::Mutex<()>,
    /// 客户端身份标识，re-establishment 时复用，避免 EXCHANGE_ID 互相销毁 session。
    pub(crate) client_identity: ClientIdentity,
    pub(crate) state: StateManager,
    pub(crate) lease_renewal: LeaseRenewal,
    pub(crate) layout_manager: Arc<LayoutManager>,
    /// 解析后的 MDS 地址；pNFS 路径用于判定 DS 是否就是 MDS（复用主 session）。
    pub(crate) server_addr: std::net::SocketAddr,
    /// Handle to the recall handler task; aborted on umount.
    pub(crate) recall_handle: Option<tokio::task::JoinHandle<()>>,
    /// 归还通道发送端：OPEN 收到不请自来的 delegation 时主动入队
    /// DELEGRETURN（复用 recall handler），避免 server 后续 CB_RECALL
    /// 时阻塞其它 client 对同一文件的访问。
    pub(crate) recall_tx: tokio::sync::mpsc::Sender<RecallNotification>,
    pub(crate) retain_delegations: bool,
    pub(crate) rsize: u32,
    pub(crate) wsize: u32,
}

impl Mount41 {
    /// Check SEQ4_STATUS flags from SEQUENCE result and take corrective action.
    ///
    /// RFC 5661 §2.10.6.2: the server reports session/state conditions via
    /// `status_flags` in SEQUENCE4resok. This method parses those flags and
    /// triggers the appropriate recovery (clearing state, layouts, etc.).
    async fn handle_seq_status(&self, flags: u32) {
        if flags == 0 {
            return; // fast path: no flags set
        }

        if flags & SEQ4_STATUS_STATE_REVOKED != 0 {
            warn!(flags, "server revoked state — clearing open state");
            self.state.clear().await;
        }
        if flags & (SEQ4_STATUS_DEVID_CHANGED | SEQ4_STATUS_DEVID_DELETED) != 0 {
            warn!(flags, "pNFS device changed/deleted — evicting layout cache");
            self.layout_manager.clear().await;
        }
        if flags & SEQ4_STATUS_CB_FLAGS != 0 {
            // RFC 8881 §18.46.3. CB_PATH_DOWN / CB_PATH_DOWN_SESSION mean the
            // backchannel path is down (recover by re-binding a connection);
            // BACKCHANNEL_FAULT means the server hit an unrecoverable backchannel
            // fault (slot/seq desync) and mandates destroying & rebuilding the
            // session. With the in-connection backchannel now serviced, these
            // should not normally fire; full re-bind / DESTROY_SESSION recovery is
            // a follow-up. For now surface the specific condition.
            if flags & SEQ4_STATUS_BACKCHANNEL_FAULT != 0 {
                warn!(
                    flags,
                    "server reports BACKCHANNEL_FAULT (slot/seq desync); session should be rebuilt"
                );
            } else {
                warn!(flags, "server reports backchannel path down (CB_PATH_DOWN)");
            }
        }
        if flags & SEQ4_STATUS_LEASE_MOVED != 0 {
            warn!("server reports LEASE_MOVED");
        }
        if flags & SEQ4_STATUS_RESTART_RECLAIM_NEEDED != 0 {
            // RFC 5661 §8.4.2.1: server restarted — all client state is gone.
            // Clear local caches and send RECLAIM_COMPLETE. We cannot reclaim
            // existing open files (no VFS layer), so they will get stale-stateid
            // errors; callers should re-open. RECLAIM_COMPLETE is spawned in the
            // background to avoid slot contention with the caller's in-flight slot.
            warn!(
                "server reports RESTART_RECLAIM_NEEDED — clearing all state, sending RECLAIM_COMPLETE"
            );
            self.state.clear().await;
            self.layout_manager.clear().await;
            let rpc = self.rpc.clone();
            let auth = self.auth.clone();
            let session_holder = self.session_holder.clone();
            tokio::spawn(async move {
                // Wait briefly for the caller's slot to be released.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let sess = session_holder.get().await;
                match sess.acquire_slot().await {
                    Ok(mut slot) => {
                        let seq_id = slot.current_sequence_id();
                        let sess_id = *sess.id();
                        let highest = sess.highest_slot_id();
                        let slot_id = slot.slot_id;
                        let builder = super::compound::CompoundBuilder::new("reclaim_complete")
                            .sequence(&sess_id, seq_id, slot_id, highest)
                            .reclaim_complete(false);
                        let mut buf = Vec::new();
                        builder.encode_with_header(&auth, &mut buf);
                        slot.fence_on_drop();
                        let response = rpc
                            .call(buf, super::ONE_ATTEMPT, std::time::Duration::from_secs(10))
                            .await
                            .and_then(CompoundResponse::decode);
                        match response {
                            Ok(response) => {
                                let sequence_ok = response.op_ok(0).is_ok();
                                let status = response.check_status();
                                if sequence_ok {
                                    slot.advance();
                                }
                                if sequence_ok && status.is_ok() {
                                    slot.resolve();
                                    warn!("RECLAIM_COMPLETE sent successfully");
                                    return;
                                }
                                let error = match status {
                                    Err(error) => error,
                                    Ok(()) => NfsError::Xdr(
                                        "RECLAIM_COMPLETE missing valid SEQUENCE".to_string(),
                                    ),
                                };
                                warn!(error = %error, "RECLAIM_COMPLETE failed");
                                if !matches!(
                                    error,
                                    NfsError::Nfs4(nfsstat4::NFS4ERR_RETRY_UNCACHED_REP)
                                        | NfsError::Nfs4(nfsstat4::NFS4ERR_SEQ_FALSE_RETRY)
                                ) {
                                    slot.resolve();
                                }
                            }
                            Err(error) => {
                                warn!(error = %error, "RECLAIM_COMPLETE failed");
                            }
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "RECLAIM_COMPLETE: failed to acquire slot");
                    }
                }
            });
        }
    }

    /// Send a COMPOUND with SEQUENCE prepended (metadata timeout).
    /// Retries on NFS4ERR_DELAY/GRACE; re-establishes session on BADSESSION/DEADSESSION.
    pub(crate) async fn compound(
        &self,
        tag: &str,
        build_ops: impl Fn(CompoundBuilder) -> CompoundBuilder + Send + Sync,
    ) -> Result<CompoundResponse> {
        self.compound_inner(tag, METADATA_TIMEOUT, None, &build_ops)
            .await
    }

    /// Send a COMPOUND with SEQUENCE (data-transfer timeout scaled by payload size).
    pub(crate) async fn compound_data(
        &self,
        tag: &str,
        data_size: usize,
        build_ops: impl Fn(CompoundBuilder) -> CompoundBuilder + Send + Sync,
    ) -> Result<CompoundResponse> {
        self.compound_inner(tag, data_timeout(data_size), None, &build_ops)
            .await
    }

    /// Send a COMPOUND with SEQUENCE for writes, using zero-copy data transfer.
    /// The write data is sent separately via `rpc::Client::call_with_data()`.
    pub(crate) async fn compound_write(
        &self,
        tag: &str,
        data: bytes::Bytes,
        build_ops: impl Fn(CompoundBuilder) -> CompoundBuilder + Send + Sync,
    ) -> Result<CompoundResponse> {
        self.compound_inner(tag, data_timeout(data.len()), Some(data), &build_ops)
            .await
    }

    /// Unified COMPOUND execution with retry/recovery logic.
    /// When `write_data` is Some, uses `rpc.call_with_data()` for zero-copy writes.
    async fn compound_inner(
        &self,
        tag: &str,
        timeout: std::time::Duration,
        write_data: Option<bytes::Bytes>,
        build_ops: &(dyn Fn(CompoundBuilder) -> CompoundBuilder + Send + Sync),
    ) -> Result<CompoundResponse> {
        for attempt in 0..=DELAY_RETRY_MAX {
            let sess = self.session_holder.get().await;
            let mut slot = sess.acquire_slot().await?;

            let seq_id = slot.current_sequence_id();
            let builder = CompoundBuilder::new(tag).sequence(
                sess.id(),
                seq_id,
                slot.slot_id,
                sess.highest_slot_id(),
            );
            let builder = build_ops(builder);
            if let Some(required) = builder.required_generation()
                && required != sess.generation()
            {
                return Err(NfsError::OperationOutcome(Box::new(
                    OperationOutcomeError::new(
                        OperationOutcome::DefiniteFailure,
                        builder.operation_class(),
                        RecoveryAction::Reopen,
                        request_context(tag, sess.id(), slot.slot_id, seq_id),
                        NfsError::Rpc(format!(
                            "stale session generation {required}, active {}",
                            sess.generation()
                        )),
                    ),
                )));
            }
            let builder = builder.apply_sequence_cache_policy(sess.max_cached_response_size())?;
            builder.enforce_max_operations(sess.max_operations())?;
            let request_op_count = builder.op_count();
            let operation_class = builder.operation_class();
            let context = request_context(tag, sess.id(), slot.slot_id, seq_id);
            if operation_class != OperationClass::ReadOnly {
                slot.fence_on_drop();
            }
            let mut buf = Vec::new();
            builder.encode_with_header(&self.auth, &mut buf);
            enforce_request_size(&sess, buf.len(), write_data.as_ref().map_or(0, Bytes::len))?;

            let response_bytes = if let Some(ref data) = write_data {
                self.rpc
                    .call_with_data(buf, data.clone(), NFS_REPLAY, timeout)
                    .await
            } else {
                self.rpc.call(buf, NFS_REPLAY, timeout).await
            }
            .map_err(|error| sent_error(operation_class, &context, error))?;
            enforce_response_size(&sess, response_bytes.len())
                .map_err(|error| sent_error(operation_class, &context, error))?;
            let mut resp = CompoundResponse::decode(response_bytes)
                .map_err(|error| sent_error(operation_class, &context, error))?;
            enforce_response_operations(&sess, &resp, request_op_count)
                .map_err(|error| sent_error(operation_class, &context, error))?;
            resp.session_generation = sess.generation();
            // Validate the wire identity before advancing or changing slot limits.
            let sequence_result = if let Some(sequence_op) = resp.results.first()
                && sequence_op.opcode == super::compound::OpNum::Sequence as u32
                && sequence_op.status == nfsstat4::NFS4_OK
            {
                let sequence =
                    match validate_sequence_result(sequence_op, sess.id(), seq_id, slot.slot_id) {
                        Ok(sequence) => sequence,
                        Err(error) => {
                            slot.fence_on_drop();
                            return Err(sent_error(operation_class, &context, error));
                        }
                    };
                if let Err(error) = sess.update_sequence_slot_limits(
                    sequence.highest_slot_id,
                    sequence.target_highest_slot_id,
                ) {
                    slot.fence_on_drop();
                    return Err(sent_error(operation_class, &context, error));
                }
                slot.advance();
                Some(sequence)
            } else {
                None
            };
            match resp.check_status() {
                Err(NfsError::Nfs4(nfsstat4::NFS4ERR_DELAY)) if attempt < DELAY_RETRY_MAX => {
                    let delay_ms = delay_with_jitter_ms(attempt);
                    warn!(tag, attempt, delay_ms, "NFS4ERR_DELAY, retrying with jitter");
                    slot.resolve();
                    drop(slot);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                Err(NfsError::Nfs4(nfsstat4::NFS4ERR_GRACE)) if attempt < DELAY_RETRY_MAX => {
                    let delay_ms = grace_with_jitter_ms(attempt);
                    warn!(tag, attempt, delay_ms, "NFS4ERR_GRACE, waiting for server grace period");
                    slot.resolve();
                    drop(slot);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                Err(NfsError::Nfs4(nfsstat4::NFS4ERR_BADSESSION))
                | Err(NfsError::Nfs4(nfsstat4::NFS4ERR_DEADSESSION))
                // RFC 5661 §2.10.6.1: SEQ_MISORDERED — RPC retransmit after server
                // evicted reply-cache entry. Re-establish to reset slot sequence numbers.
                | Err(NfsError::Nfs4(nfsstat4::NFS4ERR_SEQ_MISORDERED))
                    if attempt < DELAY_RETRY_MAX =>
                {
                    warn!(tag, attempt, "session invalid — re-establishing");
                    let expected_generation = sess.generation();
                    slot.resolve();
                    drop(slot);
                    let _recovery = self.recovery_lock.lock().await;
                    if self.session_holder.get().await.generation() != expected_generation {
                        warn!(
                            tag,
                            attempt,
                            expected_generation,
                            "session already recovered by another request"
                        );
                        continue;
                    }
                    match Session::establish(&self.rpc, &self.auth, &self.client_identity).await {
                        Ok(new_session) => {
                            let next_generation = expected_generation.saturating_add(1);
                            self.state.transition_to(next_generation).await;
                            self.layout_manager.transition_to(next_generation).await;
                            if !self
                                .session_holder
                                .replace_with_callback_if_current(
                                    expected_generation,
                                    new_session,
                                    &self.callback_state,
                                )
                                .await?
                            {
                                return Err(NfsError::Rpc(
                                    "session generation changed during recovery publication"
                                        .to_string(),
                                ));
                            }
                            warn!(tag, attempt, "session re-established, retrying with new session");
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            continue;
                        }
                        Err(e) => {
                            return Err(NfsError::Rpc(format!(
                                "session re-establishment failed: {}",
                                e
                            )));
                        }
                    }
                }
                Err(error) => {
                    let error = sent_error(operation_class, &context, error);
                    if error.operation_outcome().is_none() {
                        slot.resolve();
                    }
                    return Err(error);
                }
                Ok(()) => {}
            }
            resp.op_ok(0)
                .map_err(|error| sent_error(operation_class, &context, error))?; // SEQUENCE
            if let Some(sequence) = sequence_result {
                self.handle_seq_status(sequence.status_flags).await;
            }
            slot.resolve();
            return Ok(resp);
        }
        Err(NfsError::Rpc(
            "NFS4ERR_DELAY/GRACE retry exhausted".to_string(),
        ))
    }

    /// Send a COMPOUND to a pNFS data server with SEQUENCE on the DS's own
    /// session (RFC 8881 §13.1: DS 上的 READ/WRITE 同样要求 session)。
    pub(crate) async fn compound_ds(
        ds: &DsConnection,
        auth: &Auth,
        tag: &str,
        data_size: usize,
        build_ops: impl FnOnce(CompoundBuilder) -> CompoundBuilder,
    ) -> Result<CompoundResponse> {
        let mut slot = ds.session.acquire_slot().await?;
        let builder = CompoundBuilder::new(tag).sequence(
            ds.session.id(),
            slot.current_sequence_id(),
            slot.slot_id,
            ds.session.highest_slot_id(),
        );
        let builder = build_ops(builder)
            .apply_sequence_cache_policy(ds.session.max_cached_response_size())?;
        builder.enforce_max_operations(ds.session.max_operations())?;
        let request_op_count = builder.op_count();
        let operation_class = builder.operation_class();
        let context = request_context(
            tag,
            ds.session.id(),
            slot.slot_id,
            slot.current_sequence_id(),
        );
        if operation_class != OperationClass::ReadOnly {
            slot.fence_on_drop();
        }
        let mut buf = Vec::new();
        builder.encode_with_header(auth, &mut buf);
        enforce_request_size(&ds.session, buf.len(), 0)?;
        let timeout = data_timeout(data_size);
        let response_bytes = ds
            .client
            .call(buf, NFS_REPLAY, timeout)
            .await
            .map_err(|error| sent_error(operation_class, &context, error))?;
        enforce_response_size(&ds.session, response_bytes.len())
            .map_err(|error| sent_error(operation_class, &context, error))?;
        let mut resp = CompoundResponse::decode(response_bytes)
            .map_err(|error| sent_error(operation_class, &context, error))?;
        enforce_response_operations(&ds.session, &resp, request_op_count)
            .map_err(|error| sent_error(operation_class, &context, error))?;
        resp.session_generation = ds.session.generation();
        // RFC 5661 §2.10.6.1: advance sequence ID whenever SEQUENCE succeeded.
        if let Err(error) = resp.op_ok(0) {
            let error = sent_error(operation_class, &context, error);
            if error.operation_outcome().is_none() {
                slot.resolve();
            }
            return Err(error);
        }
        let sequence = match validate_sequence_result(
            &resp.results[0],
            ds.session.id(),
            slot.current_sequence_id(),
            slot.slot_id,
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                slot.fence_on_drop();
                return Err(sent_error(operation_class, &context, error));
            }
        };
        if let Err(error) = ds
            .session
            .update_sequence_slot_limits(sequence.highest_slot_id, sequence.target_highest_slot_id)
        {
            slot.fence_on_drop();
            return Err(sent_error(operation_class, &context, error));
        }
        slot.advance();
        if let Err(error) = resp.check_status() {
            let error = sent_error(operation_class, &context, error);
            if error.operation_outcome().is_none() {
                slot.resolve();
            }
            return Err(error);
        }
        slot.resolve();
        Ok(resp)
    }

    /// Send a COMPOUND to a DS with zero-copy write data (SEQUENCE on DS session).
    pub(crate) async fn compound_ds_write(
        ds: &DsConnection,
        auth: &Auth,
        tag: &str,
        data: Bytes,
        build_ops: impl FnOnce(CompoundBuilder) -> CompoundBuilder,
    ) -> Result<CompoundResponse> {
        let mut slot = ds.session.acquire_slot().await?;
        let builder = CompoundBuilder::new(tag).sequence(
            ds.session.id(),
            slot.current_sequence_id(),
            slot.slot_id,
            ds.session.highest_slot_id(),
        );
        let builder = build_ops(builder)
            .apply_sequence_cache_policy(ds.session.max_cached_response_size())?;
        builder.enforce_max_operations(ds.session.max_operations())?;
        let request_op_count = builder.op_count();
        let operation_class = builder.operation_class();
        let context = request_context(
            tag,
            ds.session.id(),
            slot.slot_id,
            slot.current_sequence_id(),
        );
        if operation_class != OperationClass::ReadOnly {
            slot.fence_on_drop();
        }
        let mut buf = Vec::new();
        builder.encode_with_header(auth, &mut buf);
        enforce_request_size(&ds.session, buf.len(), data.len())?;
        let timeout = data_timeout(data.len());
        let response_bytes = ds
            .client
            .call_with_data(buf, data, NFS_REPLAY, timeout)
            .await
            .map_err(|error| sent_error(operation_class, &context, error))?;
        enforce_response_size(&ds.session, response_bytes.len())
            .map_err(|error| sent_error(operation_class, &context, error))?;
        let mut resp = CompoundResponse::decode(response_bytes)
            .map_err(|error| sent_error(operation_class, &context, error))?;
        enforce_response_operations(&ds.session, &resp, request_op_count)
            .map_err(|error| sent_error(operation_class, &context, error))?;
        resp.session_generation = ds.session.generation();
        if let Err(error) = resp.op_ok(0) {
            let error = sent_error(operation_class, &context, error);
            if error.operation_outcome().is_none() {
                slot.resolve();
            }
            return Err(error);
        }
        let sequence = match validate_sequence_result(
            &resp.results[0],
            ds.session.id(),
            slot.current_sequence_id(),
            slot.slot_id,
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                slot.fence_on_drop();
                return Err(sent_error(operation_class, &context, error));
            }
        };
        if let Err(error) = ds
            .session
            .update_sequence_slot_limits(sequence.highest_slot_id, sequence.target_highest_slot_id)
        {
            slot.fence_on_drop();
            return Err(sent_error(operation_class, &context, error));
        }
        slot.advance();
        if let Err(error) = resp.check_status() {
            let error = sent_error(operation_class, &context, error);
            if error.operation_outcome().is_none() {
                slot.resolve();
            }
            return Err(error);
        }
        slot.resolve();
        Ok(resp)
    }
}

/// Outer wrapper implementing the public `crate::Mount` trait.
#[derive(Debug)]
pub(crate) struct Mount41Wrapper {
    m: Mount41,
    issuer: u64,
}

impl std::fmt::Debug for Mount41 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mount41")
            .field("root_fh_len", &self.root_fh.len())
            .field("rsize", &self.rsize)
            .field("wsize", &self.wsize)
            .field("lease_health", &self.lease_renewal.health())
            .field("state", &"StateManager")
            .field("layout_manager", &"LayoutManager")
            .finish()
    }
}

// ─── Mount entry point ──────────────────────────────────────────────────────

pub(crate) async fn mount(args: &crate::MountArgs) -> Result<Box<dyn crate::Mount>> {
    let nfsport = if args.nfsport != 0 {
        args.nfsport
    } else {
        NFS4_DEFAULT_PORT
    };
    // Use tokio async DNS to avoid blocking the worker thread (H8)
    let addrs: Vec<std::net::SocketAddr> =
        tokio::net::lookup_host(format!("{}:{}", args.host, nfsport))
            .await
            .map_err(NfsError::Io)?
            .collect();
    debug!(host = %args.host, port = nfsport, addr_count = addrs.len(), "resolved NFSv4.1 server addresses");

    let auth = Auth::new_unix("nfs-rs", args.uid, args.gid);

    for addr in &addrs {
        match mount_on_addr(addr, args, &auth).await {
            Ok(mount) => return Ok(mount),
            Err(e) => {
                warn!(addr = %addr, error = %e, "NFSv4.1 mount attempt failed");
                continue;
            }
        }
    }
    Err(NfsError::Rpc(
        "NFSv4.1 mount failed on all addresses".to_string(),
    ))
}

async fn mount_on_addr(
    addr: &std::net::SocketAddr,
    args: &crate::MountArgs,
    auth: &Auth,
) -> Result<Box<dyn crate::Mount>> {
    info!(addr = %addr, dirpath = %args.dirpath, "connecting for NFSv4.1 mount");

    // 1. TCP connect
    let nfs_mux = rpc::StreamMux::connect(*addr, args.noresvport).await?;
    let client = rpc::Client::new(nfs_mux, None); // no separate mount connection

    // 2. Establish session (EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE)
    // 每个 mount 实例生成唯一的 ClientIdentity，避免 co_ownerid 冲突导致服务端销毁其他 session
    let client_identity = ClientIdentity::new();
    let raw_session = Session::establish(&client, auth, &client_identity).await?;
    let session_holder = Arc::new(SessionHolder::new(raw_session));
    let session = session_holder.get().await;

    // 3. Navigate to export path: PUTROOTFH + LOOKUP*n + GETFH
    let root_fh = navigate_to_export(&client, &session, auth, &args.dirpath).await?;
    info!(
        fh_len = root_fh.len(),
        "navigated to export, got root file handle"
    );

    // 4. Get filesystem limits via GETATTR
    let (rsize, wsize, renewal_interval) =
        get_fs_limits(&client, &session, auth, &root_fh, args.rsize, args.wsize).await?;
    info!(
        rsize,
        wsize,
        renewal_secs = renewal_interval.as_secs(),
        "negotiated transfer sizes"
    );

    let layout_manager = Arc::new(LayoutManager::new(args.noresvport));

    // Set up the NFSv4.1 backchannel on this fore-channel connection. In v4.1 the
    // server sends CB_COMPOUND callbacks back over the *same* connection, so we
    // register a handler with the RPC layer rather than opening a separate
    // listener (the v4.0 model). RFC 8881 §2.10.3.1, §18.34.
    let (recall_tx, recall_rx) = tokio::sync::mpsc::channel(32);
    let callback_state = CallbackState::new_negotiated(
        *session.id(),
        session.generation(),
        session.backchannel_max_requests(),
        session.backchannel_max_request_size(),
        session.backchannel_max_operations(),
    );
    client.enable_backchannel(super::callback::make_backchannel_handler(
        Arc::clone(&callback_state),
        recall_tx.clone(),
    ));

    // A connection is not NFSv4.1-ready until BOTH has been confirmed.
    let direction = bind_connection(&client, auth, session.id(), false).await?;
    info!(direction, "backchannel bound via BIND_CONN_TO_SESSION");
    let reconnect_auth = auth.clone();
    let reconnect_sessions = Arc::clone(&session_holder);
    client.set_reconnect_handler(move |client, connection_generation| {
        let auth = reconnect_auth.clone();
        let sessions = Arc::clone(&reconnect_sessions);
        async move {
            let session = sessions.get().await;
            let session_generation = session.generation();
            let direction = bind_connection(&client, &auth, session.id(), true).await?;
            info!(
                connection_generation,
                session_generation, direction, "NFSv4.1 connection rebound and ready"
            );
            Ok(())
        }
    })?;

    // Always run the recall handler: OPEN 一律带 WANT_NO_DELEG，正常情况下服务器
    // 不会授予 delegation；若不合规服务器仍强塞并 CB_RECALL，则用 recall 报文里
    // 自带的 (fh, stateid) 无状态地直接 DELEGRETURN，无需本地跟踪。
    let recall_handle = Some(tokio::spawn(handle_recalls(
        recall_rx,
        client.clone(),
        auth.clone(),
        session_holder.clone(),
        layout_manager.clone(),
        Arc::clone(&callback_state),
    )));

    // Start background lease renewal using COMPOUND(SEQUENCE) (interval = server lease_time / 2)
    let lease_renewal = LeaseRenewal::start(
        client.clone(),
        auth.clone(),
        session_holder.clone(),
        renewal_interval,
    );

    let m = Mount41 {
        rpc: client,
        auth: auth.clone(),
        root_fh,
        session_holder,
        callback_state,
        recovery_lock: tokio::sync::Mutex::new(()),
        client_identity,
        state: StateManager::new(),
        layout_manager,
        server_addr: *addr,
        lease_renewal,
        recall_handle,
        recall_tx,
        retain_delegations: args.retain_delegations,
        rsize,
        wsize,
    };

    Ok(Box::new(Mount41Wrapper {
        m,
        issuer: NEXT_MOUNT_ISSUER.fetch_add(1, Ordering::Relaxed),
    }))
}

/// Background task that processes recall notifications (delegation + pNFS layout).
///
/// 两类召回都走"无状态归还"模式：
/// - CB_RECALL：客户端从不主动持有 delegation（OPEN 带 WANT_NO_DELEG），
///   报文自带 (fh, stateid)，直接原样 DELEGRETURN。
/// - CB_LAYOUTRECALL：callback 已回 NFS4_OK（承诺归还），此处驱逐本地
///   layout 缓存并用报文里的 stateid 发 LAYOUTRETURN（RFC 8881 §12.5.5.1）。
async fn handle_recalls(
    mut recall_rx: tokio::sync::mpsc::Receiver<RecallNotification>,
    rpc: rpc::Client,
    auth: Auth,
    session_holder: Arc<SessionHolder>,
    layout_manager: Arc<LayoutManager>,
    callback_state: Arc<CallbackState>,
) {
    while let Some(notification) = recall_rx.recv().await {
        match notification {
            RecallNotification::Delegation { stateid, fh, .. } => {
                // 部分 server（如 ONTAP）无视 WANT_NO_DELEG 仍授予 delegation，
                // 之后逐个 recall——多文件负载下每文件一条，属已知常态行为而非
                // 异常，降为 debug 避免刷屏
                debug!(
                    fh_len = fh.len(),
                    "server granted a delegation despite WANT_NO_DELEG; returning it"
                );
                send_recall_return(&rpc, &auth, &session_holder, |b| {
                    b.putfh(&fh).delegreturn(&stateid)
                })
                .await;
            }
            RecallNotification::LayoutFile {
                stateid,
                fh,
                offset,
                length,
                iomode,
            } => {
                callback_state.record_layout_recall();
                let _io_guard = layout_manager.write_file_io(&fh).await;
                debug!(
                    fh_len = fh.len(),
                    offset, length, "returning recalled layout"
                );
                info!(
                    event = "pnfs_layout_recall_received",
                    fh_len = fh.len(),
                    offset,
                    length,
                    "serializing pNFS file-layout recall"
                );
                // A single ordered COMPOUND ensures LAYOUTRETURN is not
                // executed if an earlier LAYOUTCOMMIT fails.
                let dirty = layout_manager.snapshot_dirty(&fh).await;
                let returned = send_recall_return(&rpc, &auth, &session_holder, |b| {
                    let b = b.putfh(&fh);
                    let b = if let Some(dirty) = dirty {
                        b.layoutcommit(
                            dirty.start,
                            dirty.end - dirty.start,
                            false,
                            &stateid,
                            Some(dirty.end - 1),
                            1, // LAYOUT4_NFSV4_1_FILES
                        )
                    } else {
                        b
                    };
                    b.layoutreturn(
                        false, // reclaim
                        1,     // LAYOUT4_NFSV4_1_FILES
                        iomode, 1, // LAYOUTRETURN4_FILE
                        offset, length, &stateid,
                    )
                })
                .await;
                let acknowledged = match dirty {
                    Some(dirty) if returned => layout_manager.acknowledge_dirty(&fh, dirty).await,
                    Some(_) => false,
                    None => returned,
                };
                if returned && acknowledged {
                    layout_manager.remove_layout(&fh).await;
                    callback_state.record_layout_return();
                    info!(event = "pnfs_layout_recall_returned", fh_len = fh.len());
                } else {
                    warn!(
                        fh_len = fh.len(),
                        "retaining recalled layout after LAYOUTRETURN failure"
                    );
                }
            }
            RecallNotification::LayoutAll => {
                // FSID/ALL 召回：客户端不跟踪 fsid 映射，保守地逐个归还全部缓存 layout
                let layouts = layout_manager.all_layouts().await;
                debug!(count = layouts.len(), "returning all layouts on recall");
                for (fh, layout) in &layouts {
                    let _io_guard = layout_manager.write_file_io(fh).await;
                    let dirty = layout_manager.snapshot_dirty(fh).await;
                    let iomode = if layout.segments.len() == 1 {
                        layout.segments[0].iomode as u32
                    } else {
                        3 // LAYOUTIOMODE4_ANY
                    };
                    let returned = send_recall_return(&rpc, &auth, &session_holder, |b| {
                        let b = b.putfh(fh);
                        let b = if let Some(dirty) = dirty {
                            b.layoutcommit(
                                dirty.start,
                                dirty.end - dirty.start,
                                false,
                                &layout.stateid,
                                Some(dirty.end - 1),
                                1, // LAYOUT4_NFSV4_1_FILES
                            )
                        } else {
                            b
                        };
                        b.layoutreturn(
                            false,
                            1, // LAYOUT4_NFSV4_1_FILES
                            iomode,
                            1, // LAYOUTRETURN4_FILE
                            0,
                            0xFFFF_FFFF_FFFF_FFFF,
                            &layout.stateid,
                        )
                    })
                    .await;
                    let acknowledged = match dirty {
                        Some(dirty) if returned => {
                            layout_manager.acknowledge_dirty(fh, dirty).await
                        }
                        Some(_) => false,
                        None => returned,
                    };
                    if returned && acknowledged {
                        layout_manager.remove_layout(fh).await;
                    } else {
                        warn!(
                            fh_len = fh.len(),
                            "retaining recalled layout after LAYOUTRETURN failure"
                        );
                    }
                }
            }
        }
    }
    debug!("recall handler exiting");
}

/// 在 recall handler 中发送一个 SEQUENCE + 归还类 op 的 COMPOUND（best-effort）。
/// 失败仅记日志：归还是尽力而为，server 侧有超时兜底。
async fn send_recall_return(
    rpc: &rpc::Client,
    auth: &Auth,
    session_holder: &Arc<SessionHolder>,
    build_ops: impl FnOnce(CompoundBuilder) -> CompoundBuilder,
) -> bool {
    let mut succeeded = false;
    // Always get the latest session from the holder so that post-recovery
    // returns use the current session ID rather than a stale one.
    let session = session_holder.get().await;
    match session.acquire_slot().await {
        Ok(mut slot) => {
            let builder = CompoundBuilder::new("recall_return").sequence(
                session.id(),
                slot.sequence_id,
                slot.slot_id,
                session.highest_slot_id(),
            );
            let builder = match build_ops(builder)
                .apply_sequence_cache_policy(session.max_cached_response_size())
            {
                Ok(builder) => builder,
                Err(e) => {
                    warn!(error = %e, "recall return cache policy rejected");
                    return false;
                }
            };
            if let Err(e) = builder.enforce_max_operations(session.max_operations()) {
                warn!(error = %e, "recall return operation limit rejected");
                return false;
            }
            let request_op_count = builder.op_count();
            let operation_class = builder.operation_class();
            let context = request_context(
                "recall_return",
                session.id(),
                slot.slot_id,
                slot.sequence_id,
            );
            let mut buf = Vec::new();
            builder.encode_with_header(auth, &mut buf);
            if let Err(e) = enforce_request_size(&session, buf.len(), 0) {
                warn!(error = %e, "recall return request limit rejected");
                return false;
            }
            slot.fence_on_drop();
            let response = rpc
                .call(buf, super::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| sent_error(operation_class, &context, error))
                .and_then(|bytes| {
                    enforce_response_size(&session, bytes.len())?;
                    let response = CompoundResponse::decode(bytes)
                        .map_err(|error| sent_error(operation_class, &context, error))?;
                    enforce_response_operations(&session, &response, request_op_count)?;
                    Ok(response)
                });
            match response {
                Ok(response)
                    if response.results.len() == request_op_count
                        && response.op_ok(0).is_ok()
                        && response.check_status().is_ok() =>
                {
                    match validate_sequence_result(
                        &response.results[0],
                        session.id(),
                        slot.sequence_id,
                        slot.slot_id,
                    ) {
                        Ok(sequence) => {
                            if session
                                .update_sequence_slot_limits(
                                    sequence.highest_slot_id,
                                    sequence.target_highest_slot_id,
                                )
                                .is_ok()
                            {
                                slot.advance();
                                slot.resolve();
                                debug!("recall return sent");
                                succeeded = true;
                            }
                        }
                        Err(error) => warn!(error = %error, "recall return SEQUENCE mismatch"),
                    }
                }
                Ok(response) => {
                    if let Ok(sequence_op) = response.op_ok(0)
                        && let Ok(sequence) = validate_sequence_result(
                            sequence_op,
                            session.id(),
                            slot.sequence_id,
                            slot.slot_id,
                        )
                        && session
                            .update_sequence_slot_limits(
                                sequence.highest_slot_id,
                                sequence.target_highest_slot_id,
                            )
                            .is_ok()
                    {
                        slot.advance();
                    }
                    let error = match response.check_status() {
                        Err(error) => sent_error(operation_class, &context, error),
                        Ok(()) => sent_error(
                            operation_class,
                            &context,
                            NfsError::Xdr("recall return missing valid SEQUENCE".to_string()),
                        ),
                    };
                    if error.operation_outcome().is_none() {
                        slot.resolve();
                    }
                    warn!(error = %error, "recall return failed");
                }
                Err(error) => {
                    warn!(error = %error, "recall return failed");
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to acquire slot for recall return");
        }
    };
    succeeded
}

/// Navigate to the export path using PUTROOTFH + LOOKUP chain + GETFH.
async fn navigate_to_export(
    rpc: &rpc::Client,
    session: &Session,
    auth: &Auth,
    dirpath: &str,
) -> Result<Bytes> {
    let mut slot = session.acquire_slot().await?;
    let sequence_id = slot.sequence_id;
    let mut builder = CompoundBuilder::new("navigate")
        .sequence(
            session.id(),
            slot.sequence_id,
            slot.slot_id,
            session.highest_slot_id(),
        )
        .putrootfh();

    // Split path into components and add LOOKUP for each
    let components: Vec<&str> = dirpath.split('/').filter(|c| !c.is_empty()).collect();
    for component in &components {
        builder = builder.lookup(component);
    }
    builder = builder.getfh();
    builder.enforce_max_operations(session.max_operations())?;
    let request_op_count = builder.op_count();

    let mut buf = Vec::new();
    builder.encode_with_header(auth, &mut buf);
    enforce_request_size(session, buf.len(), 0)?;
    let response_bytes = rpc
        .call(
            buf,
            super::BOOTSTRAP_REPLAY,
            std::time::Duration::from_secs(10),
        )
        .await?;
    enforce_response_size(session, response_bytes.len())?;
    let resp = CompoundResponse::decode(response_bytes)?;
    enforce_response_operations(session, &resp, request_op_count)?;
    resp.check_status()?;
    let sequence =
        validate_sequence_result(resp.op_ok(0)?, session.id(), sequence_id, slot.slot_id)
            .inspect_err(|_| slot.fence_on_drop())?;
    if let Err(error) = session
        .update_sequence_slot_limits(sequence.highest_slot_id, sequence.target_highest_slot_id)
    {
        slot.fence_on_drop();
        return Err(error);
    }
    resp.op_ok(1)?; // PUTROOTFH

    // Check each LOOKUP result
    for i in 0..components.len() {
        resp.op_ok(2 + i)?;
    }

    // GETFH result: index = 2 + components.len()
    let getfh_op = resp.op_ok(2 + components.len())?;
    let mut data = getfh_op.data.clone();
    // Decode nfs_fh4: opaque<NFS4_FHSIZE>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("GETFH result missing fh length".to_string()));
    }
    let fh_len = data.get_u32() as usize;
    let padded = (fh_len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("GETFH result fh data truncated".to_string()));
    }
    let fh = data.slice(..fh_len);
    data.advance(padded);
    slot.advance();

    Ok(fh)
}

/// Query filesystem attributes to determine rsize/wsize limits and lease time.
/// NFSv4.1: FATTR4_LEASE_TIME = attr #10, FATTR4_MAXREAD = attr #30, FATTR4_MAXWRITE = attr #31
async fn get_fs_limits(
    rpc: &rpc::Client,
    session: &Session,
    auth: &Auth,
    root_fh: &Bytes,
    requested_rsize: u32,
    requested_wsize: u32,
) -> Result<(u32, u32, std::time::Duration)> {
    // NFSv4.1: lease_time=10, maxread=30, maxwrite=31
    let bitmap = [(1u32 << 10) | (1u32 << 30) | (1u32 << 31)]; // word0 only

    let mut slot = session.acquire_slot().await?;
    let sequence_id = slot.sequence_id;
    let builder = CompoundBuilder::new("fsinfo")
        .sequence(
            session.id(),
            slot.sequence_id,
            slot.slot_id,
            session.highest_slot_id(),
        )
        .putfh(root_fh)
        .getattr(&bitmap);
    builder.enforce_max_operations(session.max_operations())?;
    let request_op_count = builder.op_count();

    let mut buf = Vec::new();
    builder.encode_with_header(auth, &mut buf);
    enforce_request_size(session, buf.len(), 0)?;
    let response_bytes = rpc
        .call(
            buf,
            super::BOOTSTRAP_REPLAY,
            std::time::Duration::from_secs(10),
        )
        .await?;
    enforce_response_size(session, response_bytes.len())?;
    let resp = CompoundResponse::decode(response_bytes)?;
    enforce_response_operations(session, &resp, request_op_count)?;
    resp.check_status()?;
    let sequence =
        validate_sequence_result(resp.op_ok(0)?, session.id(), sequence_id, slot.slot_id)
            .inspect_err(|_| slot.fence_on_drop())?;
    if let Err(error) = session
        .update_sequence_slot_limits(sequence.highest_slot_id, sequence.target_highest_slot_id)
    {
        slot.fence_on_drop();
        return Err(error);
    }
    resp.op_ok(1)?; // PUTFH
    let getattr_op = resp.op_ok(2)?; // GETATTR
    slot.advance();

    // Decode fattr4: bitmap + attr_vals
    let mut data = getattr_op.data.clone();
    // bitmap
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
    let mut resp_bitmap = vec![0u32; bitmap_len];
    for word in &mut resp_bitmap {
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("GETATTR bitmap word truncated".to_string()));
        }
        *word = data.get_u32();
    }
    // attr_vals opaque
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETATTR attr_vals length truncated".to_string(),
        ));
    }
    let vals_len = data.get_u32() as usize;
    if data.remaining() < vals_len {
        return Err(NfsError::Xdr("GETATTR attr_vals truncated".to_string()));
    }
    let mut vals = data.slice(..vals_len);

    // Check which attrs are in the response bitmap (NFSv4.1: 10=lease_time, 30=maxread, 31=maxwrite)
    let has_lease_time = bitmap_len > 0 && (resp_bitmap[0] & (1 << 10)) != 0;
    let has_maxread = bitmap_len > 0 && (resp_bitmap[0] & (1 << 30)) != 0;
    let has_maxwrite = bitmap_len > 0 && (resp_bitmap[0] & (1 << 31)) != 0;

    // Decode attr values in bitmap order (bit 10 < bit 29 < bit 30)
    let mut server_lease_secs: u32 = 90; // RFC 5661 §8.3: typical default
    let mut server_maxread = u64::MAX;
    let mut server_maxwrite = u64::MAX;

    if has_lease_time && vals.remaining() >= 4 {
        server_lease_secs = vals.get_u32();
    }
    if has_maxread && vals.remaining() >= 8 {
        server_maxread = vals.get_u64();
    }
    if has_maxwrite && vals.remaining() >= 8 {
        server_maxwrite = vals.get_u64();
    }

    // Renewal interval = lease_time / 2, clamped to [5s, 45s]
    let renewal_secs = (server_lease_secs / 2).clamp(5, 45);
    let renewal_interval = std::time::Duration::from_secs(renewal_secs as u64);

    // Clamp requested sizes to server limits (same logic as v3).
    // A WRITE request also contains RPC, authentication, COMPOUND, SEQUENCE,
    // PUTFH, and WRITE metadata. Reserve conservative headroom so the encoded
    // request stays below the fore-channel ca_maxrequestsize negotiated by
    // CREATE_SESSION (RFC 5661 §2.10.1).
    let rsize_max: u32 = 4_194_304; // 4 MiB
    let wsize_max: u32 = 4_194_304;
    let rsize_min: u32 = 8192;
    let wsize_min: u32 = 8192;

    let rsize = effective_rsize(
        requested_rsize,
        server_maxread,
        rsize_max,
        rsize_min,
        session.max_response_size(),
    )?;
    let wsize = effective_wsize(
        requested_wsize,
        server_maxwrite,
        wsize_max,
        wsize_min,
        session.max_request_size(),
    )?;

    Ok((rsize, wsize, renewal_interval))
}

const NFS41_WRITE_REQUEST_HEADROOM: u32 = 4 * 1024;
const NFS41_READ_RESPONSE_HEADROOM: u32 = 4 * 1024;

fn effective_rsize(
    requested_rsize: u32,
    server_maxread: u64,
    client_rsize_max: u32,
    client_rsize_min: u32,
    session_max_response_size: u32,
) -> Result<u32> {
    let session_payload_limit = session_max_response_size
        .checked_sub(NFS41_READ_RESPONSE_HEADROOM)
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            NfsError::Rpc(format!(
                "NFSv4.1 session max response size {} is too small for READ overhead",
                session_max_response_size
            ))
        })?;
    Ok(requested_rsize
        .min(server_maxread.min(client_rsize_max as u64) as u32)
        .max(client_rsize_min)
        .min(session_payload_limit))
}

fn effective_wsize(
    requested_wsize: u32,
    server_maxwrite: u64,
    client_wsize_max: u32,
    client_wsize_min: u32,
    session_max_request_size: u32,
) -> Result<u32> {
    let session_payload_limit = session_max_request_size
        .checked_sub(NFS41_WRITE_REQUEST_HEADROOM)
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            NfsError::Rpc(format!(
                "NFSv4.1 session max request size {} is too small for WRITE overhead",
                session_max_request_size
            ))
        })?;

    Ok(requested_wsize
        .min(server_maxwrite.min(client_wsize_max as u64) as u32)
        .max(client_wsize_min)
        .min(session_payload_limit))
}

// ─── Mount trait implementation ──────────────────────────────────────────────
// Each operation delegates to impl methods on Mount41 defined in separate files:
// lookup.rs, getattr.rs, read.rs, write.rs, dir_ops.rs, readdir.rs, setattr.rs

#[async_trait::async_trait]
impl crate::Mount for Mount41Wrapper {
    fn capabilities(&self) -> crate::MountCapabilities {
        crate::MountCapabilities {
            acl: true,
            // Server-effective named attributes are not negotiated yet.
            named_attributes: false,
            locks: true,
            callbacks: true,
            delegation_retention: self.m.retain_delegations,
            pnfs: self.m.session_holder.pnfs_mds(),
            session_diagnostics: true,
        }
    }

    fn health(&self) -> crate::MountHealth {
        let lease_healthy = self.m.lease_renewal.health() == LeaseHealth::Healthy;
        crate::MountHealth {
            lifecycle: if lease_healthy {
                crate::MountLifecycleState::Ready
            } else {
                crate::MountLifecycleState::Recovering
            },
            lease_healthy: Some(lease_healthy),
            // The current callback executor exposes counters but no liveness
            // signal. Unknown is safer than reporting a synthetic healthy state.
            callback_healthy: None,
            ..crate::MountHealth::default()
        }
    }

    fn get_max_read_size(&self) -> u32 {
        self.m.rsize
    }

    fn get_max_write_size(&self) -> u32 {
        self.m.wsize
    }

    async fn nfs41_channel_limits(&self) -> Option<Nfs41ChannelLimits> {
        let session = self.m.session_holder.get().await;
        Some(Nfs41ChannelLimits {
            max_request_size: session.max_request_size(),
            max_response_size: session.max_response_size(),
            max_cached_response_size: session.max_cached_response_size(),
            max_operations: session.max_operations(),
            max_requests: session.max_requests(),
            effective_highest_slot_id: session.effective_highest_slot_id(),
        })
    }

    async fn nfs41_callback_stats(&self) -> Option<Nfs41CallbackStats> {
        let (layout_recalls_received, layout_returns_completed) =
            self.m.callback_state.layout_recall_stats();
        Some(Nfs41CallbackStats {
            layout_recalls_received,
            layout_returns_completed,
        })
    }

    fn version(&self) -> NFSVersion {
        NFSVersion::NFSv4p1
    }

    async fn getfh(&self) -> Bytes {
        self.m.root_fh.clone()
    }

    async fn null(&self) -> Result<()> {
        // NFSv4 NULL is procedure 0 (same as v3), no COMPOUND needed
        let mut buf = Vec::new();
        crate::nfs3::rpc_header(NFS4_PROGRAM, NFS4_VERSION, NFS4_NULL_PROC, &self.m.auth)
            .encode(&mut buf);
        self.m.rpc.call(buf, NFS_REPLAY, METADATA_TIMEOUT).await?;
        Ok(())
    }

    async fn close(&self, fh: Bytes) -> Result<()> {
        self.m.close_file(fh).await
    }

    async fn delegreturn(&self, stateid: u64) -> Result<()> {
        // 客户端 OPEN 一律带 WANT_NO_DELEG，从不持有 delegation，无可归还。
        Err(NfsError::Rpc(format!(
            "no delegation held (client opens with WANT_NO_DELEG); stateid prefix {:#x}",
            stateid
        )))
    }

    async fn delegpurge(&self, _clientid: u64) -> Result<()> {
        Err(NfsError::Unsupported(
            "DELEGPURGE not supported".to_string(),
        ))
    }

    async fn umount(&self) -> Result<()> {
        // Stop recall handler (the backchannel handler itself lives in the RPC
        // layer and is torn down when the connection closes).
        if let Some(ref handle) = self.m.recall_handle {
            handle.abort();
        }
        // Stop lease renewal
        self.m.lease_renewal.stop();
        // Return pNFS layouts before CLOSE. Each file is exclusively fenced so
        // LAYOUTCOMMIT -> LAYOUTRETURN cannot race a foreground WRITE/CLOSE.
        self.m.layoutreturn_all().await?;
        // CLOSE all open files before destroying session
        let open_files = self.m.state.drain().await;
        for (fh, sid) in &open_files {
            let _ = self
                .m
                .compound("close", |b| {
                    b.require_generation(sid.generation)
                        .putfh(fh)
                        .close(0, &sid.raw)
                })
                .await;
        }
        // Tear down DS connections: destroy each DS session/client-id (best-effort)
        for (addr, ds) in self.m.layout_manager.drain_data_servers().await {
            debug!(addr = %addr, "destroying DS session");
            ds.destroy(&self.m.auth).await;
        }
        // DESTROY_SESSION — send without SEQUENCE (H3: cannot use session to
        // destroy itself). Use a bare COMPOUND with only DESTROY_SESSION.
        let current_sess = self.m.session_holder.get().await;
        let client_id = current_sess.client_id();
        {
            let builder =
                CompoundBuilder::new("destroy_session").destroy_session(current_sess.id());
            let mut buf = Vec::new();
            builder.encode_with_header(&self.m.auth, &mut buf);
            if let Err(e) = self
                .m
                .rpc
                .call(buf, super::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
            {
                debug!(error = %e, "DESTROY_SESSION failed (may already be destroyed)");
            }
        }
        // DESTROY_CLIENTID (no session needed)
        let builder = CompoundBuilder::new("destroy_clientid").destroy_client_id(client_id);
        let mut buf = Vec::new();
        builder.encode_with_header(&self.m.auth, &mut buf);
        let _ = self
            .m
            .rpc
            .call(buf, super::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await;
        Ok(())
    }

    // ─── Operations — delegate to impl Mount41 in sub-files ───────────

    async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<mount::ObjRes> {
        self.m.lookup(dir_fh, filename).await
    }
    async fn lookup_path(&self, path: &str) -> Result<mount::ObjRes> {
        self.m.lookup_path(path).await
    }
    async fn getattr(&self, fh: Bytes) -> Result<mount::Attr> {
        self.m.getattr(fh).await
    }
    async fn getattr_path(&self, path: &str) -> Result<mount::Attr> {
        self.m.getattr_path(path).await
    }
    async fn access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        self.m.access(fh, mode).await
    }
    async fn access_path(&self, path: &str, mode: u32) -> Result<u32> {
        self.m.access_path(path, mode).await
    }
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        self.m.read(fh, offset, count).await
    }
    async fn read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        self.m.read_path(path, offset, count).await
    }
    async fn readlink(&self, fh: Bytes) -> Result<String> {
        self.m.readlink(fh).await
    }
    async fn readlink_path(&self, path: &str) -> Result<String> {
        self.m.readlink_path(path).await
    }
    async fn readdir(&self, dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        self.m.readdir(dir_fh).await
    }
    async fn readdir_path(&self, dir_path: &str) -> Result<mount::ReaddirStream<'_>> {
        self.m.readdir_path(dir_path).await
    }
    async fn readdirplus(&self, dir_fh: Bytes) -> mount::ReaddirplusStream<'_> {
        self.m.readdirplus(dir_fh).await
    }
    async fn readdirplus_path(&self, dir_path: &str) -> Result<mount::ReaddirplusStream<'_>> {
        self.m.readdirplus_path(dir_path).await
    }
    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        self.m.write(fh, offset, data).await
    }
    async fn write_path(&self, path: &str, offset: u64, data: Bytes) -> Result<u32> {
        self.m.write_path(path, offset, data).await
    }
    async fn open(&self, dir_fh: Bytes, filename: &str, access: u32) -> Result<mount::ObjRes> {
        self.m.open(dir_fh, filename, access).await
    }
    async fn open_path(&self, path: &str, access: u32) -> Result<mount::ObjRes> {
        self.m.open_path(path, access).await
    }
    async fn create(
        &self,
        dir_fh: Bytes,
        filename: &str,
        mode: Option<u32>,
    ) -> Result<mount::ObjRes> {
        self.m.create(dir_fh, filename, mode).await
    }
    async fn create_path(&self, path: &str, mode: Option<u32>) -> Result<mount::ObjRes> {
        self.m.create_path(path, mode).await
    }
    async fn mkdir(&self, dir_fh: Bytes, dirname: &str, mode: u32) -> Result<mount::ObjRes> {
        self.m.mkdir(dir_fh, dirname, mode).await
    }
    async fn mkdir_path(&self, path: &str, mode: u32) -> Result<mount::ObjRes> {
        self.m.mkdir_path(path, mode).await
    }
    async fn remove(&self, dir_fh: Bytes, filename: &str) -> Result<()> {
        self.m.remove(dir_fh, filename).await
    }
    async fn remove_path(&self, path: &str) -> Result<()> {
        self.m.remove_path(path).await
    }
    async fn rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()> {
        self.m.rmdir(dir_fh, dirname).await
    }
    async fn rmdir_path(&self, path: &str) -> Result<()> {
        self.m.rmdir_path(path).await
    }
    async fn rename(
        &self,
        from_dir_fh: Bytes,
        from_name: &str,
        to_dir_fh: Bytes,
        to_name: &str,
    ) -> Result<()> {
        self.m
            .rename(from_dir_fh, from_name, to_dir_fh, to_name)
            .await
    }
    async fn rename_path(&self, from_path: &str, to_path: &str) -> Result<()> {
        self.m.rename_path(from_path, to_path).await
    }
    async fn link(&self, src_fh: Bytes, dst_dir_fh: Bytes, dst_name: &str) -> Result<mount::Attr> {
        self.m.link(src_fh, dst_dir_fh, dst_name).await
    }
    async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<mount::Attr> {
        self.m.link_path(src_path, dst_path).await
    }
    async fn symlink(
        &self,
        target: &str,
        dst_dir_fh: Bytes,
        dst_name: &str,
    ) -> Result<mount::ObjRes> {
        self.m.symlink(target, dst_dir_fh, dst_name).await
    }
    async fn symlink_path(&self, target: &str, dst_path: &str) -> Result<mount::ObjRes> {
        self.m.symlink_path(target, dst_path).await
    }
    async fn symlink_with_attrs(
        &self,
        target: &str,
        dst_dir_fh: Bytes,
        dst_name: &str,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<crate::Time>,
        mtime: Option<crate::Time>,
    ) -> Result<mount::ObjRes> {
        self.m
            .symlink_with_attrs(target, dst_dir_fh, dst_name, uid, gid, atime, mtime)
            .await
    }
    async fn setattr(
        &self,
        fh: Bytes,
        guard_ctime: Option<crate::Time>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<crate::Time>,
        mtime: Option<crate::Time>,
    ) -> Result<()> {
        self.m
            .setattr(fh, guard_ctime, mode, uid, gid, size, atime, mtime)
            .await
    }
    async fn setattr_path(
        &self,
        path: &str,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<crate::Time>,
        mtime: Option<crate::Time>,
    ) -> Result<()> {
        self.m
            .setattr_path(path, specify_guard, mode, uid, gid, size, atime, mtime)
            .await
    }
    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
        self.m.commit(fh, offset, count).await
    }
    async fn commit_path(&self, path: &str, offset: u64, count: u32) -> Result<()> {
        self.m.commit_path(path, offset, count).await
    }
    async fn lock(&self, fh: Bytes, lock_type: u32, offset: u64, length: u64) -> Result<Bytes> {
        self.m.lock(fh, lock_type, offset, length).await
    }
    async fn lock_test(&self, fh: Bytes, lock_type: u32, offset: u64, length: u64) -> Result<()> {
        self.m.lock_test(fh, lock_type, offset, length).await
    }
    async fn lock_stateful(
        &self,
        fh: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<crate::LockToken> {
        let generation = self.m.session_holder.get().await.generation();
        let stateid = self.m.lock(fh.clone(), lock_type, offset, length).await?;
        if self.m.session_holder.get().await.generation() != generation {
            return Err(NfsError::OperationOutcome(Box::new(
                OperationOutcomeError::new(
                    OperationOutcome::Uncertain,
                    OperationClass::ReplaySensitive,
                    RecoveryAction::Reopen,
                    RequestContext {
                        operation: "lock".to_string(),
                        protocol: NFSVersion::NFSv4p1,
                        request_id: None,
                    },
                    NfsError::Rpc("session generation changed while acquiring lock".to_string()),
                ),
            )));
        }
        Ok(crate::LockToken::new(
            fh,
            stateid,
            lock_type,
            offset,
            length,
            self.issuer,
            generation,
        ))
    }
    async fn unlock_stateful(&self, token: crate::LockToken) -> Result<()> {
        let generation = self.m.session_holder.get().await.generation();
        if token.issuer != self.issuer || token.generation != generation {
            return Err(NfsError::InvalidInput(
                "lock token belongs to another mount or recovery generation".to_string(),
            ));
        }
        self.m
            .locku(
                token.fh,
                token.stateid,
                token.lock_type,
                token.offset,
                token.length,
            )
            .await
    }
    async fn locku(
        &self,
        fh: Bytes,
        lock_stateid: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<()> {
        self.m
            .locku(fh, lock_stateid, lock_type, offset, length)
            .await
    }
    async fn getacl(&self, fh: Bytes) -> Result<mount::Acl> {
        self.m.getacl(fh).await
    }
    async fn setacl(&self, fh: Bytes, acl: &mount::Acl) -> Result<()> {
        self.m.setacl(fh, acl).await
    }
    async fn aclsupport(&self, fh: Bytes) -> Result<mount::AclSupport> {
        self.m.aclsupport(fh).await
    }
    async fn getxattr(&self, fh: Bytes, name: &str) -> Result<Bytes> {
        self.m.getxattr(fh, name).await
    }
    async fn setxattr(&self, fh: Bytes, name: &str, value: Bytes) -> Result<()> {
        self.m.setxattr(fh, name, value).await
    }
    async fn listxattr(&self, fh: Bytes) -> Result<Vec<String>> {
        self.m.listxattr(fh).await
    }
    async fn removexattr(&self, fh: Bytes, name: &str) -> Result<()> {
        self.m.removexattr(fh, name).await
    }

    // ─── FS info operations (inline — no sub-file needed) ────────────

    async fn fsinfo(&self) -> Result<mount::FSInfo> {
        // Query filesystem attrs via GETATTR on root fh
        // NFSv4.1: maxfilesize(27), maxread(30), maxwrite(31)
        // word1: time_delta(#51 = bit 19)
        let bitmap = [(1u32 << 27) | (1 << 30) | (1 << 31), 1u32 << 19];
        let resp = self
            .m
            .compound("fsinfo", |b| b.putfh(&self.m.root_fh).getattr(&bitmap))
            .await?;
        resp.op_ok(1)?; // PUTFH
        let op = resp.op_ok(2)?; // GETATTR
        let mut data = op.data.clone();

        // Parse fattr4: bitmap + attr_vals
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("fsinfo bitmap truncated".to_string()));
        }
        let bm_len = data.get_u32() as usize;
        let mut bm = vec![0u32; bm_len];
        for w in &mut bm {
            if data.remaining() < 4 {
                return Err(NfsError::Xdr("fsinfo bitmap word truncated".to_string()));
            }
            *w = data.get_u32();
        }
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("fsinfo vals length truncated".to_string()));
        }
        let vals_len = data.get_u32() as usize;
        if data.remaining() < vals_len {
            return Err(NfsError::Xdr("fsinfo vals data truncated".to_string()));
        }
        let mut vals = data.split_to(vals_len);

        let bm_has = |attr: u32| -> bool {
            let w = (attr / 32) as usize;
            w < bm.len() && (bm[w] & (1 << (attr % 32))) != 0
        };

        let mut maxfilesize = u64::MAX;
        let mut maxread = self.m.rsize as u64;
        let mut maxwrite = self.m.wsize as u64;
        let mut time_delta = crate::Time {
            seconds: 0,
            nseconds: 1,
        };

        // Decode in attribute number order
        if bm_has(27) && vals.remaining() >= 8 {
            maxfilesize = vals.get_u64();
        }
        if bm_has(30) && vals.remaining() >= 8 {
            maxread = vals.get_u64();
        }
        if bm_has(31) && vals.remaining() >= 8 {
            maxwrite = vals.get_u64();
        }
        if bm_has(51) && vals.remaining() >= 12 {
            let secs = vals.get_i64();
            let nsecs = vals.get_u32();
            time_delta = crate::Time {
                seconds: secs as u32,
                nseconds: nsecs,
            };
        }

        Ok(mount::FSInfo {
            attr: None,
            rtmax: maxread.min(self.m.rsize as u64) as u32,
            rtpref: maxread.min(self.m.rsize as u64) as u32,
            rtmult: 4096,
            wtmax: maxwrite.min(self.m.wsize as u64) as u32,
            wtpref: maxwrite.min(self.m.wsize as u64) as u32,
            wtmult: 4096,
            dtpref: 8192,
            maxfilesize,
            time_delta,
            properties: 0x001b, // FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS | FSF3_CANSETTIME
        })
    }
    async fn fsstat(&self) -> Result<mount::FSStat> {
        // NFSv4.1: files_avail(21), files_free(22), files_total(23),
        //          space_avail(42), space_free(43), space_total(44)
        let bitmap = [
            (1u32 << 21) | (1 << 22) | (1 << 23),
            (1u32 << (42 - 32)) | (1 << (43 - 32)) | (1 << (44 - 32)),
        ];
        let resp = self
            .m
            .compound("fsstat", |b| b.putfh(&self.m.root_fh).getattr(&bitmap))
            .await?;
        resp.op_ok(1)?;
        let op = resp.op_ok(2)?;
        let mut data = op.data.clone();
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("fsstat bitmap truncated".to_string()));
        }
        let bm_len = data.get_u32() as usize;
        if bm_len > 16 {
            return Err(NfsError::Xdr(format!(
                "fsstat bitmap has {} words, max 16",
                bm_len
            )));
        }
        let mut bm = vec![0u32; bm_len];
        for w in &mut bm {
            if data.remaining() < 4 {
                return Err(NfsError::Xdr("fsstat bitmap word truncated".to_string()));
            }
            *w = data.get_u32();
        }
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("fsstat vals truncated".to_string()));
        }
        let vals_len = data.get_u32() as usize;
        if data.remaining() < vals_len {
            return Err(NfsError::Xdr("fsstat vals data truncated".to_string()));
        }
        let mut vals = data.split_to(vals_len);
        let mut stat = mount::FSStat::default();
        let bm_has = |attr: u32| -> bool {
            let w = (attr / 32) as usize;
            w < bm.len() && (bm[w] & (1 << (attr % 32))) != 0
        };
        if bm_has(21) && vals.remaining() >= 8 {
            stat.afiles = vals.get_u64();
        }
        if bm_has(22) && vals.remaining() >= 8 {
            stat.ffiles = vals.get_u64();
        }
        if bm_has(23) && vals.remaining() >= 8 {
            stat.tfiles = vals.get_u64();
        }
        if bm_has(42) && vals.remaining() >= 8 {
            stat.abytes = vals.get_u64();
        }
        if bm_has(43) && vals.remaining() >= 8 {
            stat.fbytes = vals.get_u64();
        }
        if bm_has(44) && vals.remaining() >= 8 {
            stat.tbytes = vals.get_u64();
        }
        Ok(stat)
    }
    async fn pathconf(&self, fh: Bytes) -> Result<mount::Pathconf> {
        Ok(self.pathconf_with_support(fh).await?.values)
    }

    async fn pathconf_with_support(&self, fh: Bytes) -> Result<mount::SupportedPathconf> {
        let target_fh = if fh.is_empty() {
            self.m.root_fh.clone()
        } else {
            fh
        };
        // Include supported_attrs(0) and fsid(8) so capability discovery stays
        // scoped to this object and does not add a second RPC.
        // word1: no_trunc(#34 = bit 2)
        let bitmap = [
            (1u32 << 0) | (1 << 8) | (1 << 16) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
            1u32 << 2,
        ];
        let resp = self
            .m
            .compound("pathconf", |b| b.putfh(&target_fh).getattr(&bitmap))
            .await?;
        resp.op_ok(1)?; // PUTFH
        let op = resp.op_ok(2)?; // GETATTR
        let mut data = op.data.clone();

        if data.remaining() < 4 {
            return Err(NfsError::Xdr("pathconf bitmap truncated".to_string()));
        }
        let bm_len = data.get_u32() as usize;
        let mut bm = vec![0u32; bm_len];
        for w in &mut bm {
            if data.remaining() < 4 {
                return Err(NfsError::Xdr("pathconf bitmap word truncated".to_string()));
            }
            *w = data.get_u32();
        }
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("pathconf vals truncated".to_string()));
        }
        let vals_len = data.get_u32() as usize;
        if data.remaining() < vals_len {
            return Err(NfsError::Xdr("pathconf vals truncated".to_string()));
        }
        let mut vals = data.split_to(vals_len);

        let bm_has = |attr: u32| -> bool {
            let w = (attr / 32) as usize;
            w < bm.len() && (bm[w] & (1 << (attr % 32))) != 0
        };

        if bm_has(0) {
            if vals.remaining() < 4 {
                return Err(NfsError::Xdr("supported_attrs truncated".to_string()));
            }
            let words = vals.get_u32() as usize;
            let bytes = words.saturating_mul(4);
            if vals.remaining() < bytes {
                return Err(NfsError::Xdr("supported_attrs truncated".to_string()));
            }
            vals.advance(bytes);
        }
        let fsid = if bm_has(8) {
            if vals.remaining() < 16 {
                return Err(NfsError::Xdr("fsid truncated".to_string()));
            }
            Some((vals.get_u64(), vals.get_u64()))
        } else {
            None
        };

        let available = mount::PathconfSupport {
            linkmax: bm_has(28),
            name_max: bm_has(29),
            no_trunc: bm_has(34),
            chown_restricted: bm_has(18),
            case_insensitive: bm_has(16),
            case_preserving: bm_has(17),
        };
        let mut pc = mount::Pathconf {
            attr: None,
            linkmax: 32767,
            name_max: 255,
            no_trunc: true,
            chown_restricted: true,
            case_insensitive: false,
            case_preserving: true,
        };

        // Decode in NFSv4.1 attr number order
        if bm_has(16) && vals.remaining() >= 4 {
            pc.case_insensitive = vals.get_u32() != 0;
        }
        if bm_has(17) && vals.remaining() >= 4 {
            pc.case_preserving = vals.get_u32() != 0;
        }
        if bm_has(18) && vals.remaining() >= 4 {
            pc.chown_restricted = vals.get_u32() != 0;
        }
        if bm_has(28) && vals.remaining() >= 4 {
            pc.linkmax = vals.get_u32();
        }
        if bm_has(29) && vals.remaining() >= 4 {
            pc.name_max = vals.get_u32();
        }
        if bm_has(34) && vals.remaining() >= 4 {
            pc.no_trunc = vals.get_u32() != 0;
        }

        Ok(mount::SupportedPathconf {
            values: pc,
            available,
            fsid,
        })
    }
    async fn pathconf_path(&self, path: &str) -> Result<mount::Pathconf> {
        let obj = self.m.lookup_path(path).await?;
        self.pathconf(obj.fh).await
    }
    async fn exports(&self) -> Result<Vec<mount::ExportEntry>> {
        Err(NfsError::Unsupported(
            "NFSv4.1 does not have a separate EXPORT procedure".to_string(),
        ))
    }
}

// ─── Decode helpers (used by sub-files) ──────────────────────────────────────

pub(super) fn decode_fh(data: &mut Bytes) -> Result<Bytes> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("fh length truncated".to_string()));
    }
    let len = data.get_u32() as usize;
    let padded = (len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("fh data truncated".to_string()));
    }
    let fh = data.slice(..len);
    data.advance(padded);
    Ok(fh)
}

pub(super) fn decode_string_from_bytes(data: &mut Bytes) -> Result<String> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("string length truncated".to_string()));
    }
    let len = data.get_u32() as usize;
    let padded = (len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("string data truncated".to_string()));
    }
    let s = String::from_utf8_lossy(&data.slice(..len)).to_string();
    data.advance(padded);
    Ok(s)
}

pub(super) fn extract_stateid(data: &mut Bytes) -> Result<[u8; 16]> {
    if data.remaining() < 16 {
        return Err(NfsError::Xdr("stateid4 truncated".to_string()));
    }
    let mut sid = [0u8; 16];
    data.copy_to_slice(&mut sid);
    Ok(sid)
}

/// 在 `extract_stateid` 之后继续解析 OPEN4resok 剩余部分，提取 server
/// 授予的 delegation stateid（RFC 8881 §18.16.3）。
///
/// 布局：change_info4(20) + rflags(4) + attrmask(bitmap4) + open_delegation4。
/// OPEN_DELEGATE_READ(1)/WRITE(2) 返回 Some(deleg stateid)；NONE(0)、
/// NONE_EXT(3) 或任何截断都返回 None（best-effort，解析失败不影响 OPEN）。
pub(super) fn extract_open_delegation(data: &mut Bytes) -> Option<[u8; 16]> {
    // change_info4: atomic(4) + before(8) + after(8)
    if data.remaining() < 20 + 4 + 4 {
        return None;
    }
    data.advance(20);
    let _rflags = data.get_u32();
    // attrmask: bitmap4 = count + count 个 u32
    let bitmap_len = data.get_u32() as usize;
    if bitmap_len > 16 || data.remaining() < bitmap_len * 4 + 4 {
        return None;
    }
    data.advance(bitmap_len * 4);
    let delegation_type = data.get_u32();
    match delegation_type {
        // OPEN_DELEGATE_READ / OPEN_DELEGATE_WRITE：紧接 stateid4
        1 | 2 if data.remaining() >= 16 => {
            let mut sid = [0u8; 16];
            data.copy_to_slice(&mut sid);
            Some(sid)
        }
        _ => None,
    }
    // NOTE: the rest of the old file content (readdir helpers, ensure_open, maybe_close,
    // skip_fattr4_inline, decode_entry_fattr4, encode_setattr) has been moved to
    // readdir.rs, setattr.rs, and state management helpers.
    // This replaces ~900 lines of inline implementations with ~90 lines of delegating code.
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind_response(session_id: [u8; 16], direction: u32) -> Bytes {
        let mut response = Vec::new();
        response.extend_from_slice(&0u32.to_be_bytes()); // COMPOUND status
        response.extend_from_slice(&0u32.to_be_bytes()); // empty tag
        response.extend_from_slice(&1u32.to_be_bytes()); // result count
        response.extend_from_slice(&41u32.to_be_bytes()); // OP_BIND_CONN_TO_SESSION
        response.extend_from_slice(&0u32.to_be_bytes()); // operation status
        response.extend_from_slice(&session_id);
        response.extend_from_slice(&direction.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes()); // RDMA mode false
        Bytes::from(response)
    }

    #[test]
    fn bind_validation_requires_matching_session_and_backchannel() {
        let session_id = [9; 16];
        assert_eq!(
            validate_bound_connection(bind_response(session_id, 3), &session_id).unwrap(),
            3
        );
        assert!(validate_bound_connection(bind_response([8; 16], 3), &session_id).is_err());
        assert!(validate_bound_connection(bind_response(session_id, 1), &session_id).is_err());
        let mut truncated = bind_response(session_id, 3).to_vec();
        truncated.truncate(truncated.len() - 8);
        assert!(validate_bound_connection(Bytes::from(truncated), &session_id).is_err());
    }

    #[test]
    fn effective_wsize_preserves_server_maxwrite_negotiation() {
        let wsize = effective_wsize(1_048_576, 64 * 1024, 4_194_304, 8192, 1_048_576).unwrap();
        assert_eq!(wsize, 64 * 1024);
    }

    #[test]
    fn effective_wsize_reserves_session_request_headroom() {
        let session_max_request_size = 1_048_576;
        let wsize = effective_wsize(
            1_048_576,
            4_194_304,
            4_194_304,
            8192,
            session_max_request_size,
        )
        .unwrap();
        assert_eq!(
            wsize,
            session_max_request_size - NFS41_WRITE_REQUEST_HEADROOM
        );
        assert!(wsize < session_max_request_size);
    }

    #[test]
    fn effective_wsize_never_exceeds_small_session_limit() {
        let session_max_request_size = NFS41_WRITE_REQUEST_HEADROOM + 4096;
        let wsize = effective_wsize(
            1_048_576,
            4_194_304,
            4_194_304,
            8192,
            session_max_request_size,
        )
        .unwrap();
        assert_eq!(wsize, 4096);
        assert!(wsize < session_max_request_size);
    }

    #[test]
    fn effective_wsize_rejects_session_without_payload_capacity() {
        let result = effective_wsize(
            1_048_576,
            4_194_304,
            4_194_304,
            8192,
            NFS41_WRITE_REQUEST_HEADROOM,
        );
        assert!(result.is_err());
    }

    #[test]
    fn effective_rsize_reserves_negotiated_response_headroom() {
        let maximum = 1_048_576;
        let rsize = effective_rsize(4_194_304, u64::MAX, 4_194_304, 8192, maximum).unwrap();
        assert_eq!(rsize, maximum - NFS41_READ_RESPONSE_HEADROOM);
        assert!(rsize < maximum);
    }

    #[test]
    fn effective_rsize_rejects_response_without_payload_capacity() {
        assert!(effective_rsize(8192, u64::MAX, 4_194_304, 8192, 4096).is_err());
    }

    #[test]
    fn negotiated_encoded_size_limits_accept_exact_boundary_only() {
        assert!(enforce_encoded_size("request", 0, 1).is_ok());
        assert!(enforce_encoded_size("request", 1, 1).is_ok());
        assert!(enforce_encoded_size("request", 2, 1).is_err());
        assert!(enforce_encoded_size("response", 4095, 4096).is_ok());
        assert!(enforce_encoded_size("response", 4096, 4096).is_ok());
        assert!(enforce_encoded_size("response", 4097, 4096).is_err());
        assert!(usize::MAX.checked_add(1).is_none());
    }

    #[test]
    fn jitter_lands_in_half_to_full_range() {
        for _ in 0..200 {
            let d = backoff_jitter_ms(0, 200, 5000);
            assert!((100..200).contains(&d), "attempt=0 produced {} ms", d);
        }
    }

    #[test]
    fn jitter_caps_at_cap_ms() {
        for _ in 0..200 {
            let d = backoff_jitter_ms(20, 200, 5000);
            assert!((2500..5000).contains(&d), "attempt=20 produced {} ms", d);
        }
    }

    #[test]
    fn jitter_does_not_panic_on_huge_attempt() {
        // checked_shl + saturating_mul + min(cap) must absorb shifts past 63.
        for attempt in [32usize, 63, 64, 100, usize::MAX] {
            let d = backoff_jitter_ms(attempt, 200, 5000);
            assert!(d > 0 && d < 5000, "attempt={} produced {}", attempt, d);
        }
    }

    /// 构造 extract_stateid 之后的 OPEN4resok 剩余字节。
    fn open_resok_tail(delegation_type: u32, deleg_sid: Option<[u8; 16]>) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 20]); // change_info4
        buf.extend_from_slice(&0u32.to_be_bytes()); // rflags
        buf.extend_from_slice(&2u32.to_be_bytes()); // attrmask: 2 words
        buf.extend_from_slice(&[0u8; 8]);
        buf.extend_from_slice(&delegation_type.to_be_bytes());
        if let Some(sid) = deleg_sid {
            buf.extend_from_slice(&sid);
        }
        Bytes::from(buf)
    }

    #[test]
    fn open_delegation_none() {
        let mut data = open_resok_tail(0, None);
        assert_eq!(extract_open_delegation(&mut data), None);
    }

    #[test]
    fn open_delegation_read_and_write() {
        for dtype in [1u32, 2] {
            let mut data = open_resok_tail(dtype, Some([7u8; 16]));
            assert_eq!(extract_open_delegation(&mut data), Some([7u8; 16]));
        }
    }

    #[test]
    fn open_delegation_truncated() {
        // delegation_type=2 但缺 stateid
        let mut data = open_resok_tail(2, None);
        assert_eq!(extract_open_delegation(&mut data), None);
        // 整体截断
        let mut short = Bytes::from_static(&[0u8; 10]);
        assert_eq!(extract_open_delegation(&mut short), None);
    }
}

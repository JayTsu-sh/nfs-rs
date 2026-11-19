//! NFSv4.0 stateid recovery: lane refresh, reclaim, and GRACE/DELAY backoff.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use futures::stream::TryStreamExt as _;

use super::compound::{
    CallbackAddress, CompoundBuilder, DelegationGrant, NewLockArgs, OpenArgs, OpenReclaimArgs,
    SetClientIdArgs, decode_confirm_response, decode_lock_response, decode_open_response,
    decode_setclientid_response, decode_stateid_response,
};
use super::lease::LeaseState;
use super::mount::{ClientIdentity, METADATA_TIMEOUT, return_delegation};
use super::state::{LockLane, LockState, OpenState, OwnerLane};
use crate::error::{NfsError, OperationClass, RequestContext, Result};
use crate::mount::{MountLifecycleState, NFSVersion};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

pub(super) const MAX_CONCURRENT_RECLAIMS: usize = 16;
/// RFC 7530 §9.6.2: after NFS4ERR_GRACE the client should retry "on the order
/// of several seconds" rather than driving a recovery per I/O request.
pub(super) const RECOVERY_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StateidRecovery {
    Bad,
    Stale,
    Old,
}

pub(super) fn stateid_recovery(error: &NfsError) -> Option<StateidRecovery> {
    match error {
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID) => Some(StateidRecovery::Bad),
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID) => Some(StateidRecovery::Stale),
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID) => Some(StateidRecovery::Old),
        _ => None,
    }
}

pub(super) fn stateid_hex(stateid: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(32);
    for byte in stateid {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Inputs shared by the fresh-OPEN helpers.
pub(super) struct FreshOpenSpec<'a> {
    pub(super) dir_fh: &'a Bytes,
    pub(super) name: &'a str,
    pub(super) owner_wire: &'a [u8],
    pub(super) access: u32,
    pub(super) client_id: u64,
}

pub(super) struct FreshOpen {
    pub(super) stateid: [u8; 16],
    pub(super) next_seqid: u32,
    pub(super) fh: Bytes,
    pub(super) delegation: Option<DelegationGrant>,
}

pub(super) struct FreshOpenAttempt {
    pub(super) result: Result<FreshOpen>,
    /// Open-owner seqid carried by the last OPEN that was actually sent.
    pub(super) sent_seqid: u32,
}

/// Issue a normal `CLAIM_NULL` OPEN for a lane, without resolving a path again.
///
/// RFC 7530 §9.6.3.4.5 allows a client whose reclaim was rejected by the FSAL
/// to re-establish state with a normal OPEN. The directory handle and name are
/// the ones retained on the lane at open time.
async fn fresh_open_lane(
    rpc: &rpc::Client,
    auth: &Auth,
    spec: &FreshOpenSpec<'_>,
    seqid: u32,
    operation: &str,
) -> Result<FreshOpen> {
    let request = CompoundBuilder::new(operation)
        .putfh(spec.dir_fh)
        .open(OpenArgs {
            seqid,
            share_access: spec.access,
            client_id: spec.client_id,
            owner: spec.owner_wire,
            filename: spec.name,
            create: false,
        })
        .getfh()
        .encode_with_header(auth);
    let response = rpc
        .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await?;
    let (opened, fh) = decode_open_response(response)?;
    let mut next_seqid = seqid.wrapping_add(1);
    let mut stateid = opened.stateid;
    if opened.confirm_required {
        let request = CompoundBuilder::new("open_confirm")
            .putfh(&fh)
            .open_confirm(&stateid, next_seqid)
            .encode_with_header(auth);
        let response = rpc
            .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await?;
        stateid = decode_stateid_response(response, 20, "OPEN_CONFIRM")?;
        next_seqid = next_seqid.wrapping_add(1);
    }
    Ok(FreshOpen {
        stateid,
        next_seqid,
        fh,
        delegation: opened.delegation,
    })
}

/// Fresh OPEN bounded to two attempts: a released open-owner restarts at
/// seqid 0, while a still-current open-owner expects the lane's next seqid.
pub(super) async fn fresh_open_lane_with_seqid(
    rpc: &rpc::Client,
    auth: &Auth,
    spec: &FreshOpenSpec<'_>,
    next_seqid: u32,
    operation: &str,
) -> FreshOpenAttempt {
    match fresh_open_lane(rpc, auth, spec, 0, operation).await {
        Err(NfsError::Nfs4(
            crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID
            | crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID
            | crate::Nfs4ErrorCode::NFS4ERR_BAD_SEQID,
        )) if next_seqid != 0 => FreshOpenAttempt {
            result: fresh_open_lane(rpc, auth, spec, next_seqid, operation).await,
            sent_seqid: next_seqid,
        },
        result => FreshOpenAttempt {
            result,
            sent_seqid: 0,
        },
    }
}

pub(super) async fn establish_identity(
    rpc: &rpc::Client,
    auth: &Auth,
    identity: &ClientIdentity,
    callback_addr: Option<&str>,
) -> Result<u64> {
    let callback = callback_addr
        .map(|addr| CallbackAddress::tcp(addr, 1))
        .unwrap_or(CallbackAddress::DISABLED);
    let identity_request = CompoundBuilder::new("identity")
        .setclientid(SetClientIdArgs {
            verifier: identity.verifier,
            owner: identity.owner.as_bytes(),
            callback,
        })
        .encode_with_header(auth);
    let identity_response = rpc
        .call(
            identity_request,
            ReplayPolicy::ONE_ATTEMPT,
            METADATA_TIMEOUT,
        )
        .await?;
    let (client_id, confirm_verifier) = decode_setclientid_response(identity_response)?;

    let confirm_request = CompoundBuilder::new("confirm")
        .setclientid_confirm(client_id, confirm_verifier)
        .encode_with_header(auth);
    let confirm_response = rpc
        .call(confirm_request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await?;
    decode_confirm_response(confirm_response)?;
    Ok(client_id)
}

#[derive(Clone)]
pub(super) struct RecoveryContext {
    pub(super) rpc: rpc::Client,
    pub(super) auth: Auth,
    pub(super) identity: ClientIdentity,
    pub(super) client_id: Arc<AtomicU64>,
    pub(super) issuer: u64,
    pub(super) state: Arc<OpenState>,
    pub(super) locks: Arc<LockState>,
    pub(super) lease: Arc<LeaseState>,
    pub(super) callback_addr: Option<String>,
}

impl RecoveryContext {
    pub(super) async fn recover(&self) -> Result<()> {
        let renewed_client_id = establish_identity(
            &self.rpc,
            &self.auth,
            &self.identity,
            self.callback_addr.as_deref(),
        )
        .await?;
        self.client_id.store(renewed_client_id, Ordering::Release);
        self.lease.mark_reclaiming();

        // One lane's reclaim failing for an authoritative reason (removed,
        // replaced, or not reclaimable) must not discard state that other lanes
        // can still recover. Busy statuses abort the pass instead, because the
        // remaining lanes were never attempted and must be kept for the retry.
        stream::iter(
            self.state
                .snapshot()
                .await
                .into_iter()
                .map(Ok::<_, NfsError>),
        )
        .try_for_each_concurrent(Some(MAX_CONCURRENT_RECLAIMS), |lane| async move {
            match self.reclaim_open_lane(&lane, renewed_client_id).await {
                Ok(()) => Ok(()),
                Err(error) if error.is_server_busy() => Err(error),
                Err(error) if is_authoritative_lane_loss(&error) => {
                    let (owner, fh) = {
                        let open = lane.lock().await;
                        (open.owner, open.fh.clone())
                    };
                    tracing::warn!(
                        target: "nfs_rs::stateid",
                        owner,
                        error = %error,
                        "NFSv4.0 open reclaim failed for one lane; dropping only that lane"
                    );
                    self.state.remove(owner, &fh).await;
                    Ok(())
                }
                Err(error) => Err(error),
            }
        })
        .await?;

        let mut lock_groups = BTreeMap::<u64, Vec<_>>::new();
        for lane in self.locks.snapshot().await {
            let open_owner = lane.lock().await.open_owner;
            lock_groups.entry(open_owner).or_default().push(lane);
        }
        stream::iter(lock_groups.into_values().map(Ok::<_, NfsError>))
            .try_for_each_concurrent(Some(MAX_CONCURRENT_RECLAIMS), |lanes| async move {
                for lane in lanes {
                    match self.reclaim_lock_lane(&lane, renewed_client_id).await {
                        Ok((old_stateid, new_stateid)) => {
                            self.locks
                                .rekey(old_stateid, new_stateid, Arc::clone(&lane))
                                .await;
                        }
                        Err(error) if error.is_server_busy() => return Err(error),
                        Err(error) if is_authoritative_lane_loss(&error) => {
                            let (owner, stateid) = {
                                let lock = lane.lock().await;
                                (lock.owner, lock.stateid)
                            };
                            tracing::warn!(
                                target: "nfs_rs::stateid",
                                owner,
                                error = %error,
                                "NFSv4.0 lock reclaim failed for one lane; dropping only that lock"
                            );
                            self.locks.remove(&stateid).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Reclaim one open lane.
    ///
    /// A lane that is already closing is released by CLOSE, not by reclaim;
    /// waiting for its I/O fence here can deadlock with the closer, which holds
    /// the fence and waits for this same lane.
    async fn reclaim_open_lane(
        &self,
        lane: &Arc<tokio::sync::Mutex<OwnerLane>>,
        client_id: u64,
    ) -> Result<()> {
        let mut open = lane.lock().await;
        if open.closing {
            return Ok(());
        }
        let _in_flight_settled = Arc::clone(&open.io_fence).write_owned().await;
        let owner_wire = format!("nfs-rs-{:016x}-{:016x}", self.issuer, open.owner);
        let reclaim = self
            .rpc
            .call(
                CompoundBuilder::new("reclaim-open")
                    .putfh(&open.fh)
                    .open_reclaim(OpenReclaimArgs {
                        seqid: 0,
                        share_access: open.access,
                        client_id,
                        owner: owner_wire.as_bytes(),
                    })
                    .getfh()
                    .encode_with_header(&self.auth),
                ReplayPolicy::ONE_ATTEMPT,
                METADATA_TIMEOUT,
            )
            .await
            .and_then(decode_open_response);
        let needs_fallback = match &reclaim {
            // An incompatible success leaves the lane with state it cannot use,
            // so re-establish the open with a normal OPEN.
            Ok((reclaimed, fh)) => reclaimed.confirm_required || *fh != open.fh,
            // RFC 7530 §9.6.3.4.5: only a definitive rejection of the reclaim
            // justifies a normal OPEN. Busy statuses mean the request was not
            // executed and must keep their retryable semantics.
            Err(NfsError::Nfs4(status)) => !is_busy_status(*status),
            // Io/Rpc/Xdr mean the transmission outcome is unknown; keep the
            // existing Uncertain handling instead of guessing a rejection.
            Err(_) => false,
        };
        match reclaim {
            Ok((reclaimed, fh)) if !needs_fallback => {
                return_delegation(&self.rpc, &self.auth, &fh, reclaimed.delegation).await?;
                open.stateid = reclaimed.stateid;
                open.next_seqid = 1;
            }
            Ok(_) => {
                self.reopen_after_rejected_reclaim(&mut open, owner_wire.as_bytes(), client_id)
                    .await?;
            }
            Err(NfsError::Nfs4(status)) if !is_busy_status(status) => {
                self.reopen_after_rejected_reclaim(&mut open, owner_wire.as_bytes(), client_id)
                    .await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// RFC 7530 §9.6.3.4.5: a reclaim rejected by the FSAL does not end
    /// recovery. Re-establish the open with a normal `CLAIM_NULL` OPEN and
    /// keep the lane's stateid and seqid in sync.
    async fn reopen_after_rejected_reclaim(
        &self,
        open: &mut OwnerLane,
        owner_wire: &[u8],
        client_id: u64,
    ) -> Result<()> {
        let spec = FreshOpenSpec {
            dir_fh: &open.dir_fh,
            name: &open.name,
            owner_wire,
            access: open.access,
            client_id,
        };
        let attempt = fresh_open_lane_with_seqid(
            &self.rpc,
            &self.auth,
            &spec,
            open.next_seqid,
            "reclaim-fallback-open",
        )
        .await;
        let fresh = match attempt.result {
            Ok(fresh) => fresh,
            Err(error) => {
                // RFC 7530 §9.1.7: a failed OPEN consumes its open-owner seqid
                // unless the status is in the exception table. Record it so the
                // retry after a busy recovery (GRACE/DELAY) does not replay a
                // consumed seqid and get stuck on NFS4ERR_BAD_SEQID.
                if matches!(&error, NfsError::Nfs4(status) if status_consumes_seqid(*status)) {
                    open.next_seqid = attempt.sent_seqid.wrapping_add(1);
                }
                return Err(error);
            }
        };
        if fresh.fh != open.fh {
            return Err(NfsError::Xdr(
                "NFSv4.0 fresh OPEN after rejected reclaim returned a different filehandle".into(),
            ));
        }
        let _ = return_delegation(&self.rpc, &self.auth, &fresh.fh, fresh.delegation).await;
        tracing::debug!(
            target: "nfs_rs::stateid",
            operation = "reclaim",
            owner = open.owner,
            "NFSv4.0 reclaim was rejected; open state re-established with a fresh OPEN"
        );
        open.stateid = fresh.stateid;
        open.next_seqid = fresh.next_seqid;
        Ok(())
    }

    /// Reclaim one lock lane, returning the stateids so the lock index can be
    /// re-keyed by the caller.
    async fn reclaim_lock_lane(
        &self,
        lane: &Arc<tokio::sync::Mutex<LockLane>>,
        client_id: u64,
    ) -> Result<([u8; 16], [u8; 16])> {
        let (open_owner, old_stateid) = {
            let lock = lane.lock().await;
            (lock.open_owner, lock.stateid)
        };
        let open_lane = self.state.by_owner(open_owner).await.ok_or_else(|| {
            NfsError::LostOpenState(format!(
                "NFSv4.0 reclaim LOCK lost its open-owner {open_owner}"
            ))
        })?;
        let mut open = open_lane.lock().await;
        let _in_flight_settled = Arc::clone(&open.io_fence).write_owned().await;
        let mut lock = lane.lock().await;
        let response = self
            .rpc
            .call(
                CompoundBuilder::new("reclaim-lock")
                    .putfh(&lock.fh)
                    .lock_new(NewLockArgs {
                        lock_type: lock.lock_type,
                        reclaim: true,
                        offset: lock.offset,
                        length: lock.length,
                        open_seqid: open.next_seqid,
                        open_stateid: &open.stateid,
                        lock_seqid: 0,
                        client_id,
                        owner: &lock.owner_wire,
                    })
                    .encode_with_header(&self.auth),
                ReplayPolicy::ONE_ATTEMPT,
                METADATA_TIMEOUT,
            )
            .await?;
        let new_stateid = decode_lock_response(response, 12, "LOCK")?;
        open.next_seqid = open.next_seqid.wrapping_add(1);
        lock.stateid = new_stateid;
        lock.next_seqid = 1;
        Ok((old_stateid, new_stateid))
    }

    pub(super) async fn recover_or_lose(&self) -> Result<()> {
        // A mount that already lost its state (or is unmounting) must not be
        // revived by the delayed retry scheduled below.
        match self.lease.health().lifecycle {
            MountLifecycleState::LostState => {
                return Err(NfsError::LostOpenState(
                    "NFSv4.0 recovery skipped: mount already lost its state".into(),
                ));
            }
            MountLifecycleState::Closed | MountLifecycleState::Closing => {
                return Err(NfsError::LostOpenState(
                    "NFSv4.0 recovery skipped: mount is closing".into(),
                ));
            }
            _ => {}
        }
        // One recovery at a time: SETCLIENTID must not be issued twice for the
        // same client identity, and the reclaim pass reads the whole state map.
        let _recovery = self.lease.recovery_guard().await;
        if let Some(status) = self.lease.active_recovery_backoff() {
            return Err(NfsError::Nfs4(status));
        }
        match self.attempt_recovery().await {
            // RFC 7530 §9.6.2: NFS4ERR_GRACE is a retry signal, not a reason to
            // discard state. Keep every lane and fail fast until the backoff
            // lapses; a fresh OPEN during grace would otherwise wipe opens and
            // locks that must be kept. Do not resume the mount or extend the
            // lease deadline: recovery is still incomplete and RENEW owns the
            // lost-lease decision. RENEW does not retry on its own, so one task
            // owns the retries until the mount settles.
            Err(error) if error.is_server_busy() => {
                if self.lease.claim_retry_task() {
                    tokio::spawn(retry_recovery(self.clone()));
                }
                Err(error)
            }
            result => result,
        }
    }

    /// One recovery attempt with the recovery mutex held: on success the mount
    /// resumes, on a busy server the backoff window is refreshed, and any other
    /// failure loses the state.
    async fn attempt_recovery(&self) -> Result<()> {
        match self.recover().await {
            Ok(()) => {
                // A finishing recovery must not revive a mount that started
                // unmounting while the reclaim pass was in flight.
                if matches!(
                    self.lease.health().lifecycle,
                    MountLifecycleState::Closing | MountLifecycleState::Closed
                ) {
                    return Err(NfsError::LostOpenState(
                        "NFSv4.0 recovery finished after unmount started".into(),
                    ));
                }
                self.lease.clear_recovery_backoff();
                self.lease.mark_recovered();
                Ok(())
            }
            Err(error) if error.is_server_busy() => {
                let status = nfs4_status(&error).unwrap_or(crate::Nfs4ErrorCode::NFS4ERR_GRACE);
                self.lease
                    .defer_recovery(tokio::time::Instant::now() + RECOVERY_BACKOFF, status);
                Err(error)
            }
            Err(error) => {
                // Losing state supersedes any busy backoff: later callers must
                // observe lost state, not a stale NFS4ERR_GRACE.
                self.lease.clear_recovery_backoff();
                // Never rewrite the lifecycle of a mount that already unmounted:
                // `Closed` is terminal, and `Closing` still owns its cleanup.
                if !matches!(
                    self.lease.health().lifecycle,
                    MountLifecycleState::Closing | MountLifecycleState::Closed
                ) {
                    self.lease.mark_lost().await;
                    self.state.clear().await;
                    self.locks.clear().await;
                }
                Err(NfsError::OperationOutcome(Box::new(
                    crate::error::OperationOutcomeError::new(
                        crate::error::OperationOutcome::Uncertain,
                        OperationClass::ReplaySensitive,
                        crate::error::RecoveryAction::Reopen,
                        RequestContext {
                            operation: "nfs40_lease_recovery".into(),
                            protocol: NFSVersion::NFSv4p0,
                            request_id: None,
                        },
                        error,
                    ),
                )))
            }
        }
    }
}

/// A retry task stops once recovery can no longer help: the lease window has
/// lapsed (the server may have discarded this client's state) or the mount has
/// already settled (resumed, lost, or unmounted).
pub(super) fn retry_settled(lease: &LeaseState) -> bool {
    tokio::time::Instant::now() >= lease.lease_deadline()
        || matches!(
            lease.health().lifecycle,
            MountLifecycleState::Ready
                | MountLifecycleState::LostState
                | MountLifecycleState::Closed
                | MountLifecycleState::Closing
        )
}

/// Retries recovery once each backoff window lapses until the mount settles.
/// GRACE/DELAY would otherwise leave the mount gated (Reclaiming) with no one
/// to retry: RENEW only triggers recovery on NFS4ERR_EXPIRED/STALE_CLIENTID.
pub(super) async fn retry_recovery(context: RecoveryContext) {
    // The slot is claimed by whoever spawns this task; dropping the guard here
    // lets a later busy failure start a fresh retry task.
    let _slot = RetryTaskSlot(Arc::clone(&context.lease));
    loop {
        tokio::time::sleep(RECOVERY_BACKOFF + Duration::from_millis(100)).await;
        let _recovery = context.lease.recovery_guard().await;
        if retry_settled(&context.lease) {
            return;
        }
        // Another recovery refreshed the window; wait it out instead of
        // issuing SETCLIENTID concurrently with the work in flight.
        if context.lease.active_recovery_backoff().is_some() {
            continue;
        }
        match context.attempt_recovery().await {
            Ok(()) => return,
            Err(error) if error.is_server_busy() => continue,
            Err(_) => return,
        }
    }
}

/// Releases the single-owner retry slot when the retry task exits.
struct RetryTaskSlot(Arc<LeaseState>);

impl Drop for RetryTaskSlot {
    fn drop(&mut self) {
        self.0.release_retry_task();
    }
}

/// RFC 7530 §9.1.7: every owner-sequenced request consumes its seqid except
/// for this exception list. OLD_STATEID is deliberately not an exception, so a
/// rejected request must still advance the client's seqid before a retry.
pub(super) fn status_consumes_seqid(status: crate::Nfs4ErrorCode) -> bool {
    use crate::Nfs4ErrorCode::{
        NFS4ERR_BAD_SEQID, NFS4ERR_BAD_STATEID, NFS4ERR_BADXDR, NFS4ERR_MOVED,
        NFS4ERR_NOFILEHANDLE, NFS4ERR_STALE_CLIENTID, NFS4ERR_STALE_STATEID,
    };
    // NFS4ERR_RESOURCE (10018) belongs to the RFC exception list but is not
    // modelled by the generated enum.
    const NFS4ERR_RESOURCE: u32 = 10018;
    !(matches!(
        status,
        NFS4ERR_BADXDR
            | NFS4ERR_BAD_SEQID
            | NFS4ERR_BAD_STATEID
            | NFS4ERR_MOVED
            | NFS4ERR_NOFILEHANDLE
            | NFS4ERR_STALE_CLIENTID
            | NFS4ERR_STALE_STATEID
    ) || status as u32 == NFS4ERR_RESOURCE)
}

pub(super) fn consumes_owner_seqid(result: &Result<impl Sized>) -> bool {
    match result {
        Ok(_) | Err(NfsError::LockDenied { .. }) => true,
        Err(NfsError::Nfs4(status)) => status_consumes_seqid(*status),
        Err(_) => false,
    }
}

/// Transient server-busy statuses: the operation was not executed and the
/// caller may retry it once the server is ready.
fn is_busy_status(status: crate::Nfs4ErrorCode) -> bool {
    matches!(
        status,
        crate::Nfs4ErrorCode::NFS4ERR_DELAY | crate::Nfs4ErrorCode::NFS4ERR_GRACE
    )
}

/// A definitive reply that invalidates the state this lane represents.
///
/// Busy statuses are handled before this check. Every NFSv4 status is the
/// server's authoritative answer for that lane, so only that lane is dropped
/// and the rest of the reclaim pass continues. Transport, XDR and encoding
/// failures leave the transmission outcome unknown; they abort the pass
/// instead of discarding state on a guess.
pub(super) fn is_authoritative_lane_loss(error: &NfsError) -> bool {
    matches!(error, NfsError::Nfs4(_) | NfsError::LostOpenState(_))
}

/// Innermost NFSv4 status, unwrapping busy and uncertain-outcome wrappers.
fn nfs4_status(error: &NfsError) -> Option<crate::Nfs4ErrorCode> {
    match error {
        NfsError::Nfs4(status) => Some(*status),
        NfsError::ServerBusy(details) => nfs4_status(&details.source),
        NfsError::OperationOutcome(details) => nfs4_status(&details.source),
        NfsError::FileClose(failures) => failures.iter().find_map(|f| nfs4_status(&f.error)),
        _ => None,
    }
}

/// Decode the seqid carried by the first four bytes of a stateid.
pub(super) fn stateid_seqid(stateid: &[u8; 16]) -> u32 {
    u32::from_be_bytes([stateid[0], stateid[1], stateid[2], stateid[3]])
}

/// Overwrite a stateid's seqid, keeping the `other` field intact.
pub(super) fn set_stateid_seqid(stateid: &mut [u8; 16], seqid: u32) {
    stateid[..4].copy_from_slice(&seqid.to_be_bytes());
}

/// Advance a stateid's seqid by one, returning the new value.
pub(super) fn bump_stateid_seqid(stateid: &mut [u8; 16]) -> u32 {
    let next = stateid_seqid(stateid).wrapping_add(1);
    set_stateid_seqid(stateid, next);
    next
}

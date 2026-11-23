//! NFSv4.0 stateid recovery: bounded lane refresh, reclaim, and GRACE/DELAY backoff.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use futures::stream::StreamExt as _;

use super::compound::{
    CallbackAddress, CompoundBuilder, DelegationGrant, LockOpOutcome, NewLockArgs, OpenArgs,
    OpenOpOutcome, OpenReclaimArgs, SetClientIdArgs, StateidOpOutcome, decode_confirm_response,
    decode_lock_op_outcome, decode_open_op_outcome, decode_setclientid_response,
    decode_stateid_op_outcome,
};
use super::lease::LeaseState;
use super::mount::{ClientIdentity, METADATA_TIMEOUT, return_delegation};
use super::state::{LockLane, LockState, OpenState, OwnerLane};
use crate::error::{NfsError, OperationClass, RequestContext, Result};
use crate::mount::{MountLifecycleState, NFSVersion};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

pub(super) const MAX_CONCURRENT_RECLAIMS: usize = 16;
/// In-place lane refresh is attempted once; a second stateid failure is
/// reported as exhausted instead of looping on the same lane.
const MAX_STATEID_RETRIES: u8 = 1;
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
    // Outcome wrappers keep the authoritative status in `source`; classify it
    // so a definite STALE_CLIENTID after a restart is not mistaken for a
    // generic replay-sensitive failure.
    if let NfsError::OperationOutcome(details) = error {
        return stateid_recovery(&details.source);
    }
    match error {
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID) => Some(StateidRecovery::Bad),
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID) => Some(StateidRecovery::Stale),
        // A restart invalidates the whole client id; operations carrying an old
        // stateid then answer STALE_CLIENTID (or EXPIRED) rather than
        // STALE_STATEID. Treat both as the same "earlier server instance"
        // recovery so callers get the typed LostOpenState contract.
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID)
        | NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_EXPIRED) => Some(StateidRecovery::Stale),
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
    /// Next unconsumed open-owner seqid after that attempt.
    pub(super) next_seqid: u32,
}

impl FreshOpenAttempt {
    /// Records any seqid the last request consumed without replaying the
    /// untouched value when the attempt did not consume anything.
    pub(super) fn record_consumption(&self, open: &mut OwnerLane) {
        if self.next_seqid != self.sent_seqid {
            open.next_seqid = self.next_seqid;
        }
    }
}

/// Issue a normal `CLAIM_NULL` OPEN for a lane, without resolving a path again.
///
/// RFC 7530 §9.6.3.4.5 allows a client whose reclaim was rejected by the FSAL
/// to re-establish state with a normal OPEN. The directory handle and name are
/// the ones retained on the lane at open time. The returned attempt reports the
/// next unconsumed seqid so a PUTFH failure (which never ran the OPEN) does not
/// advance the cursor.
pub(super) async fn fresh_open_lane(
    rpc: &rpc::Client,
    auth: &Auth,
    spec: &FreshOpenSpec<'_>,
    seqid: u32,
    operation: &str,
) -> FreshOpenAttempt {
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
    let response = match rpc
        .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return FreshOpenAttempt {
                result: Err(error),
                sent_seqid: seqid,
                next_seqid: seqid,
            };
        }
    };
    let outcome = match decode_open_op_outcome(response) {
        Ok(outcome) => outcome,
        Err(error) => {
            return FreshOpenAttempt {
                result: Err(error),
                sent_seqid: seqid,
                next_seqid: seqid,
            };
        }
    };
    let mut next_seqid = if open_op_consumes_seqid(&outcome) {
        seqid.wrapping_add(1)
    } else {
        seqid
    };
    let (opened, fh) = match outcome.into_result() {
        Ok(value) => value,
        Err(error) => {
            return FreshOpenAttempt {
                result: Err(error),
                sent_seqid: seqid,
                next_seqid,
            };
        }
    };
    let mut stateid = opened.stateid;
    if opened.confirm_required {
        let request = CompoundBuilder::new("open_confirm")
            .putfh(&fh)
            .open_confirm(&stateid, next_seqid)
            .encode_with_header(auth);
        let response = match rpc
            .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return FreshOpenAttempt {
                    result: Err(error),
                    sent_seqid: seqid,
                    next_seqid,
                };
            }
        };
        let confirm = match decode_stateid_op_outcome(response, 20, "OPEN_CONFIRM") {
            Ok(outcome) => outcome,
            Err(error) => {
                return FreshOpenAttempt {
                    result: Err(error),
                    sent_seqid: seqid,
                    next_seqid,
                };
            }
        };
        if stateid_op_consumes_seqid(&confirm) {
            next_seqid = next_seqid.wrapping_add(1);
        }
        match confirm.into_result() {
            Ok(confirmed) => stateid = confirmed,
            Err(error) => {
                return FreshOpenAttempt {
                    result: Err(error),
                    sent_seqid: seqid,
                    next_seqid,
                };
            }
        }
    }
    FreshOpenAttempt {
        result: Ok(FreshOpen {
            stateid,
            next_seqid,
            fh,
            delegation: opened.delegation,
        }),
        sent_seqid: seqid,
        next_seqid,
    }
}

/// Fresh OPEN bounded to two attempts: a released open-owner restarts at
/// seqid 0, while a still-current open-owner expects the lane's next seqid.
///
/// Only NFS4ERR_BAD_SEQID proves the owner is still alive. BAD/OLD_STATEID on a
/// seqid-0 OPEN do not, so they must not be retried with the lane's cursor: the
/// lane is refreshed by the caller instead of guessing the server's seqid.
pub(super) async fn fresh_open_lane_with_seqid(
    rpc: &rpc::Client,
    auth: &Auth,
    spec: &FreshOpenSpec<'_>,
    next_seqid: u32,
    operation: &str,
) -> FreshOpenAttempt {
    let attempt = fresh_open_lane(rpc, auth, spec, 0, operation).await;
    match &attempt.result {
        Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_BAD_SEQID)) if next_seqid != 0 => {
            fresh_open_lane(rpc, auth, spec, next_seqid, operation).await
        }
        _ => attempt,
    }
}

/// Best-effort CLOSE for a just-created open state that will not be kept.
///
/// The handle is about to be discarded, so a failure only leaves server state
/// behind until the lease expires; it must not mask the caller's own error.
pub(super) async fn close_fresh_open(
    rpc: &rpc::Client,
    auth: &Auth,
    fh: &Bytes,
    stateid: &[u8; 16],
    seqid: u32,
) {
    let request = CompoundBuilder::new("open-discard")
        .putfh(fh)
        .close(seqid, stateid)
        .encode_with_header(auth);
    if let Err(error) = rpc
        .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await
    {
        tracing::debug!(
            target: "nfs_rs::stateid",
            error = %error,
            "NFSv4.0 could not discard a fresh OPEN; the server state expires with the lease"
        );
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
        // Lanes reclaimed under a previous client id are not reclaimable under a
        // new one, so the ledger only applies while the identity is unchanged.
        if renewed_client_id != self.client_id.load(Ordering::Acquire) {
            self.lease.clear_reclaimed();
        }
        self.client_id.store(renewed_client_id, Ordering::Release);
        if !self.lease.mark_reclaiming() {
            return Err(NfsError::LostOpenState(
                "NFSv4.0 recovery aborted: the mount started unmounting during identity re-establishment"
                    .into(),
            ));
        }

        // One lane's reclaim failing for an authoritative reason (removed,
        // replaced, or not reclaimable) must not discard state that other lanes
        // can still recover. Busy statuses keep every remaining lane for the
        // retry. The pass always waits for all lanes: cancelling in-flight
        // reclaims could leave server state that the lanes never record.
        let open_results = stream::iter(self.state.snapshot().await)
            .map(|lane| async move {
                let result = self.reclaim_open_lane(&lane, renewed_client_id).await;
                (lane, result)
            })
            .buffer_unordered(MAX_CONCURRENT_RECLAIMS)
            .collect::<Vec<_>>()
            .await;
        let mut busy_error = None;
        let mut hard_error = None;
        for (lane, result) in open_results {
            match result {
                Ok(()) => {}
                Err(error) if error.is_server_busy() => {
                    busy_error.get_or_insert(error);
                }
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
                }
                Err(error) => {
                    hard_error.get_or_insert(error);
                }
            }
        }
        // Busy wins over hard: GRACE/DELAY means the server will accept the
        // reclaim later, so the pass keeps every lane and retries instead of
        // discarding state because another lane hit a transport failure. The
        // retry is bounded by the lease deadline in `retry_settled`.
        if let Some(error) = busy_error {
            return Err(error);
        }
        if let Some(error) = hard_error {
            return Err(error);
        }

        let mut lock_groups = BTreeMap::<u64, Vec<_>>::new();
        for lane in self.locks.snapshot().await {
            let open_owner = lane.lock().await.open_owner;
            lock_groups.entry(open_owner).or_default().push(lane);
        }
        // Lanes sharing an open owner must be reclaimed in order, so each group
        // stays sequential while groups run concurrently. As with opens, the
        // pass waits for every group before deciding the outcome.
        let lock_results = stream::iter(lock_groups.into_values())
            .map(|lanes| async move {
                for lane in lanes {
                    match self.reclaim_lock_lane(&lane, renewed_client_id).await {
                        Ok(true) => {}
                        Ok(false) => {}
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
            .buffer_unordered(MAX_CONCURRENT_RECLAIMS)
            .collect::<Vec<_>>()
            .await;
        let mut busy_error = None;
        let mut hard_error = None;
        for result in lock_results {
            match result {
                Ok(()) => {}
                Err(error) if error.is_server_busy() => {
                    busy_error.get_or_insert(error);
                }
                Err(error) => {
                    hard_error.get_or_insert(error);
                }
            }
        }
        // Same priority as the open pass: a busy reply keeps every lane for the
        // retry, and a hard failure is only reported when nothing was busy.
        if let Some(error) = busy_error {
            return Err(error);
        }
        if let Some(error) = hard_error {
            return Err(error);
        }
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
        // Same fence-before-lane order as close_lane: the closer holds the
        // fence and waits for this lane, so waiting for the fence while holding
        // the lane would deadlock.
        let io_fence = {
            let open = lane.lock().await;
            if open.closing {
                return Ok(());
            }
            // A pass retried after GRACE/DELAY must not reclaim this lane again:
            // the owner already exists and a second CLAIM_PREVIOUS with seqid 0
            // is answered with NFS4ERR_BAD_SEQID, which would drop a usable
            // lane.
            if self.lease.open_reclaimed(open.owner) {
                return Ok(());
            }
            Arc::clone(&open.io_fence)
        };
        let _in_flight_settled = io_fence.write_owned().await;
        let mut open = lane.lock().await;
        if open.closing || !self.state.is_current(open.owner, lane).await {
            // Closed or removed while this reclaim waited: do not resurrect
            // server state the client no longer tracks.
            return Ok(());
        }
        let owner_wire = open_owner_wire(self.issuer, open.owner);
        // The first reclaim of a session starts at seqid 0, but a busy reply
        // still consumes that seqid (§9.1.7). Keep the cursor in LeaseState so
        // the retry in the same session sends the next unconsumed value instead
        // of replaying a request whose cached DELAY/GRACE the server returns
        // forever.
        let reclaim_seqid = self.lease.open_reclaim_seqid(open.owner);
        let reclaim = self
            .rpc
            .call(
                CompoundBuilder::new("reclaim-open")
                    .putfh(&open.fh)
                    .open_reclaim(OpenReclaimArgs {
                        seqid: reclaim_seqid,
                        share_access: open.access,
                        client_id,
                        owner: owner_wire.as_bytes(),
                    })
                    .getfh()
                    .encode_with_header(&self.auth),
                ReplayPolicy::ONE_ATTEMPT,
                METADATA_TIMEOUT,
            )
            .await;
        let (reclaim, consumed) = match reclaim {
            Ok(response) => match decode_open_op_outcome(response) {
                Ok(outcome) => {
                    let consumed = open_op_consumes_seqid(&outcome);
                    (outcome.into_result(), consumed)
                }
                Err(error) => (Err(error), false),
            },
            Err(error) => (Err(error), false),
        };
        if consumed {
            self.lease
                .set_open_reclaim_seqid(open.owner, reclaim_seqid.wrapping_add(1));
        }
        match reclaim {
            // An incompatible success leaves the lane with state it cannot use,
            // so only a compatible result is kept as-is.
            Ok((reclaimed, fh)) if !reclaimed.confirm_required && fh == open.fh => {
                // The recovered open state does not depend on the delegation
                // being returned; a failed DELEGRETURN must not discard state
                // that other lanes can still reclaim.
                if let Err(error) =
                    return_delegation(&self.rpc, &self.auth, &fh, reclaimed.delegation).await
                {
                    tracing::warn!(
                        target: "nfs_rs::stateid",
                        owner = open.owner,
                        error = %error,
                        "NFSv4.0 reclaim could not return the recovered delegation"
                    );
                }
                open.stateid = reclaimed.stateid;
                open.next_seqid = reclaim_seqid.wrapping_add(1);
                // Keep the reclaim ledger aligned with the lane cursor even
                // though the successful outcome already advanced it above: a
                // later pass of this session must never resend the
                // CLAIM_PREVIOUS with a seqid the server already consumed.
                self.lease
                    .set_open_reclaim_seqid(open.owner, reclaim_seqid.wrapping_add(1));
            }
            Ok((reclaimed, fh)) => {
                // The reclaim succeeded, but its state cannot back this lane
                // (confirmation required, or the path now resolves to a
                // different object). Re-running a normal OPEN would leave the
                // reclaimed open and any delegation orphaned on the server.
                // Return the delegation, release the state the reclaim created,
                // then report an authoritative per-lane loss so only this open
                // is dropped.
                open.next_seqid = reclaim_seqid.wrapping_add(1);
                let _ = return_delegation(&self.rpc, &self.auth, &fh, reclaimed.delegation).await;
                let cleanup_seqid = self.lease.open_reclaim_seqid(open.owner);
                let _ = super::mount::close_unpublished_state(
                    &self.rpc,
                    &self.auth,
                    &fh,
                    cleanup_seqid,
                    &reclaimed.stateid,
                )
                .await;
                return Err(NfsError::LostOpenState(
                    "NFSv4.0 reclaim OPEN returned incompatible state".into(),
                ));
            }
            // RFC 7530 §9.6.3.4.5: only a definitive rejection of the reclaim
            // justifies a normal OPEN. Busy statuses mean the request was not
            // executed and must keep their retryable semantics; Io/Rpc/Xdr mean
            // the transmission outcome is unknown, so the pass keeps its
            // Uncertain handling instead of guessing a rejection. A client-id
            // failure is not a per-lane rejection and must abort the session.
            Err(NfsError::Nfs4(status))
                if !is_busy_status(status) && !is_session_wide_status(status) =>
            {
                if consumed {
                    open.next_seqid = reclaim_seqid.wrapping_add(1);
                }
                self.reopen_after_rejected_reclaim(&mut open, owner_wire.as_bytes(), client_id)
                    .await?;
            }
            Err(error) => return Err(error),
        }
        self.lease.mark_open_reclaimed(open.owner);
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
        // Busy reclaim attempts consume open-owner seqids and advance the
        // reclaim ledger without touching the lane cursor. Once this recovery
        // session consumed anything, the ledger is the authoritative next seqid;
        // only when nothing was consumed can the pre-loss lane cursor still be
        // the value the server expects (the owner survived the trigger).
        let reclaim_cursor = self.lease.open_reclaim_seqid(open.owner);
        let seed = if reclaim_cursor == 0 {
            open.next_seqid
        } else {
            reclaim_cursor
        };
        let attempt =
            fresh_open_lane_with_seqid(&self.rpc, &self.auth, &spec, seed, "reclaim-fallback-open")
                .await;
        // RFC 7530 §9.1.7: record only what the last OPEN actually consumed; a
        // PUTFH failure leaves the cursor untouched. Doing this before matching
        // the result keeps the failure path from replaying a consumed seqid.
        attempt.record_consumption(open);
        if attempt.next_seqid != attempt.sent_seqid {
            // The fallback OPEN advances the same open-owner sequence the
            // reclaim ledger tracks. Keep them in sync, otherwise the next
            // recovery pass resends the CLAIM_PREVIOUS with a seqid the server
            // already consumed and replays its cached GRACE/DELAY once more.
            self.lease
                .set_open_reclaim_seqid(open.owner, open.next_seqid);
        }
        let fresh = match attempt.result {
            Ok(fresh) => fresh,
            Err(error) => {
                return Err(error);
            }
        };
        if fresh.fh != open.fh {
            // The path now resolves to a different object (replaced or
            // re-created by another client). This lane cannot be recovered, but
            // it says nothing about the other lanes: report it as an
            // authoritative per-lane loss so the reclaim pass drops only this
            // open instead of invalidating the whole mount. Release the state
            // just created for the replacement object, otherwise the dropped
            // lane leaks a server open until the lease expires.
            let _ = return_delegation(&self.rpc, &self.auth, &fresh.fh, fresh.delegation).await;
            close_fresh_open(
                &self.rpc,
                &self.auth,
                &fresh.fh,
                &fresh.stateid,
                fresh.next_seqid,
            )
            .await;
            return Err(NfsError::LostOpenState(
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

    /// Reclaim one lock lane, re-keying the lock index while the lane is still
    /// held so a concurrent LOCKU cannot retire the new stateid first.
    ///
    /// `false` means the lane was already reclaimed by an earlier attempt of the
    /// same session (a pass retried after GRACE/DELAY). A retry sends the next
    /// unconsumed open/lock seqids: GRACE and DELAY are outside the §9.1.7
    /// exception table, so the failed request already advanced both owners.
    async fn reclaim_lock_lane(
        &self,
        lane: &Arc<tokio::sync::Mutex<LockLane>>,
        client_id: u64,
    ) -> Result<bool> {
        let (open_owner, lock_owner, old_stateid) = {
            let lock = lane.lock().await;
            (lock.open_owner, lock.owner, lock.stateid)
        };
        if self.lease.lock_reclaimed(lock_owner) {
            return Ok(false);
        }
        let open_lane = self.state.by_owner(open_owner).await.ok_or_else(|| {
            NfsError::LostOpenState(format!(
                "NFSv4.0 reclaim LOCK lost its open-owner {open_owner}"
            ))
        })?;
        // Same fence-before-lane order as close_lane and the OPEN reclaim.
        let io_fence = {
            let open = open_lane.lock().await;
            if open.closing || !self.state.is_current(open_owner, &open_lane).await {
                return Ok(false);
            }
            Arc::clone(&open.io_fence)
        };
        let _in_flight_settled = io_fence.write_owned().await;
        let mut open = open_lane.lock().await;
        if open.closing || !self.state.is_current(open_owner, &open_lane).await {
            return Ok(false);
        }
        let mut lock = lane.lock().await;
        if !self.locks.is_current(&old_stateid, lane).await {
            // A LOCKU released this lock while the reclaim waited; reclaiming
            // it would resurrect state the caller closed.
            return Ok(false);
        }
        let open_seqid = open.next_seqid;
        // A reclaim lock owner starts at seqid 0; a consuming busy reply
        // advances the cursor for the retry in the same session.
        let lock_seqid = self.lease.lock_reclaim_seqid(lock_owner);
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
                        open_seqid,
                        open_stateid: &open.stateid,
                        lock_seqid,
                        client_id,
                        owner: &lock.owner_wire,
                    })
                    .encode_with_header(&self.auth),
                ReplayPolicy::ONE_ATTEMPT,
                METADATA_TIMEOUT,
            )
            .await;
        let (decoded, consumed) = match response {
            Ok(response) => match decode_lock_op_outcome(response, 12, "LOCK") {
                Ok(outcome) => {
                    let consumed = lock_op_consumes_seqid(&outcome);
                    (outcome.into_result(), consumed)
                }
                Err(error) => (Err(error), false),
            },
            Err(error) => (Err(error), false),
        };
        // A processed LOCK consumes both owner seqids; only the §9.1.7
        // exception statuses leave them untouched. A PUTFH failure never ran
        // the LOCK, so it must not advance either cursor.
        if consumed {
            open.next_seqid = open_seqid.wrapping_add(1);
        }
        let new_stateid = match decoded {
            Ok(stateid) => stateid,
            Err(error) => {
                if consumed {
                    self.lease
                        .set_lock_reclaim_seqid(lock_owner, lock_seqid.wrapping_add(1));
                }
                return Err(error);
            }
        };
        lock.stateid = new_stateid;
        lock.next_seqid = lock_seqid.wrapping_add(1);
        // Re-key while the lane lock is still held: releasing it first would let
        // a concurrent LOCKU remove the new key, after which this re-key would
        // resurrect a lane whose stateid is already retired.
        self.locks
            .rekey(old_stateid, new_stateid, Arc::clone(lane))
            .await;
        drop(lock);
        self.lease.mark_lock_reclaimed(lock_owner);
        Ok(true)
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
        // The lifecycle may have become terminal while this recovery was
        // waiting for its turn (e.g. the previous holder lost state). Re-check
        // so a queued attempt cannot reclaim into a mount that is already lost.
        if matches!(
            self.lease.health().lifecycle,
            MountLifecycleState::LostState
                | MountLifecycleState::Closing
                | MountLifecycleState::Closed
        ) {
            return Err(NfsError::LostOpenState(
                "NFSv4.0 recovery skipped: mount became terminal while waiting".into(),
            ));
        }
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
                self.lease.clear_recovery_backoff();
                // Every lane is usable again, so the next recovery session must
                // reclaim them from scratch.
                self.lease.clear_reclaimed();
                // A finishing recovery must not revive a mount that started
                // unmounting or crossed the lease deadline while the reclaim
                // pass was in flight. Take the publication lock so a concurrent
                // RENEW timeout (`mark_lost`) cannot interleave with this
                // transition, then re-validate the lifecycle.
                let publication = self.lease.publication_guard().await;
                if !self.lease.mark_recovered() {
                    drop(publication);
                    // The reclaim pass may have repopulated the map after the
                    // state was declared lost. A terminal mount must not keep
                    // reclaimable lanes that later operations could mistake for
                    // live state.
                    self.state.clear().await;
                    self.locks.clear().await;
                    return Err(NfsError::LostOpenState(
                        "NFSv4.0 recovery finished after the mount lost its state or started unmounting"
                            .into(),
                    ));
                }
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
                self.lease.clear_reclaimed();
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

#[cfg(test)]
pub(super) fn consumes_owner_seqid(result: &Result<impl Sized>) -> bool {
    match result {
        Ok(_) | Err(NfsError::LockDenied { .. }) => true,
        Err(NfsError::Nfs4(status)) => status_consumes_seqid(*status),
        Err(_) => false,
    }
}

/// RFC 7530 §9.1.7 consumption of a two-operation `PUTFH + stateid op` reply.
pub(super) fn stateid_op_consumes_seqid(outcome: &StateidOpOutcome<[u8; 16]>) -> bool {
    match outcome {
        StateidOpOutcome::Completed(_) | StateidOpOutcome::Executed(_) => true,
        StateidOpOutcome::Rejected(error) => error_consumes_seqid(error),
        StateidOpOutcome::NotExecuted(_) => false,
    }
}

pub(super) fn open_op_consumes_seqid(outcome: &OpenOpOutcome) -> bool {
    match outcome {
        OpenOpOutcome::Completed(_, _) | OpenOpOutcome::Executed(_) => true,
        OpenOpOutcome::Rejected(error) => error_consumes_seqid(error),
        OpenOpOutcome::NotExecuted(_) => false,
    }
}

pub(super) fn lock_op_consumes_seqid(outcome: &LockOpOutcome) -> bool {
    match outcome {
        LockOpOutcome::Granted(_) | LockOpOutcome::Executed(_) | LockOpOutcome::Denied(_) => true,
        LockOpOutcome::Rejected(error) => error_consumes_seqid(error),
        LockOpOutcome::NotExecuted(_) => false,
    }
}

fn error_consumes_seqid(error: &NfsError) -> bool {
    match error {
        NfsError::Nfs4(status) => status_consumes_seqid(*status),
        NfsError::LockDenied { .. } => true,
        _ => false,
    }
}

/// Wire form of an open-owner name, shared by recovery and I/O so both address
/// the same server-side owner.
pub(super) fn open_owner_wire(issuer: u64, owner: u64) -> String {
    format!("nfs-rs-{issuer:016x}-{owner:016x}")
}

/// Shared bounded-retry step for stateid-carrying operations.
///
/// Classifies a stateid failure and consumes one refresh from the caller's
/// budget. `retries` counts refreshes already performed, so a second stateid
/// failure is reported as exhausted instead of refreshing the lane again. The
/// error is returned to the caller, which may still need it to report a lane
/// that vanished between the two attempts.
pub(super) fn classify_stateid_failure(
    operation: &str,
    retries: &mut u8,
    error: NfsError,
) -> Result<(StateidRecovery, NfsError)> {
    let Some(trigger) = stateid_recovery(&error) else {
        return Err(error);
    };
    if *retries >= MAX_STATEID_RETRIES {
        return Err(state_recovery_exhausted(operation, trigger, error));
    }
    *retries += 1;
    Ok((trigger, error))
}

/// Classify a failure raised while refreshing a lane.
///
/// An authoritative protocol status explains why the lane could not be refreshed
/// and keeps its own meaning: busy statuses (GRACE, DELAY) stay retryable, and
/// statuses such as NOENT or ACCESS keep their errno mapping. A closed open
/// state is a local lifecycle answer, not an exhausted recovery either. Only
/// failures without a definitive outcome (transport, encoding) are reported as
/// exhausted recovery.
pub(super) fn recovery_failure(
    operation: &str,
    trigger: StateidRecovery,
    source: NfsError,
) -> NfsError {
    // The lifecycle/generation gate keeps its own action.
    if matches!(
        &source,
        NfsError::OperationOutcome(_) | NfsError::ClosedResource(_)
    ) {
        return source;
    }
    // A stateid status raised by the recovery attempt itself means the bounded
    // recovery failed; asking the caller for another in-place refresh would
    // just repeat the same request.
    if stateid_recovery(&source).is_none()
        && (is_authoritative_lane_loss(&source) || is_session_wide_failure(&source))
    {
        return source;
    }
    state_recovery_exhausted(operation, trigger, source)
}

pub(super) fn state_recovery_exhausted(
    operation: &str,
    trigger: StateidRecovery,
    source: NfsError,
) -> NfsError {
    let status = match trigger {
        StateidRecovery::Bad => crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID,
        StateidRecovery::Stale => crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID,
        StateidRecovery::Old => crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID,
    };
    tracing::warn!(
        target: "nfs_rs::stateid",
        operation,
        status = %status,
        error = %source,
        "NFSv4.0 stateid recovery exhausted"
    );
    NfsError::StateRecoveryExhausted(Box::new(crate::StateRecoveryError {
        operation: operation.to_string(),
        status,
        source: Box::new(source),
    }))
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
    match error {
        // Client-id and lease statuses invalidate the whole client identity,
        // not just this lane. Dropping the lane would silently discard state
        // that a session-level recovery can still reclaim.
        NfsError::Nfs4(status) => !is_session_wide_status(*status),
        // A DENIED reclaim LOCK is the server's definitive answer for this
        // lock (for example another client reclaimed the conflicting range
        // first). It says nothing about the other lanes, so drop only this
        // lock instead of losing the whole mount.
        NfsError::LockDenied { .. } => true,
        NfsError::LostOpenState(_) => true,
        _ => false,
    }
}

/// Statuses that invalidate the whole client identity for every lane.
fn is_session_wide_status(status: crate::Nfs4ErrorCode) -> bool {
    matches!(
        status,
        crate::Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID | crate::Nfs4ErrorCode::NFS4ERR_EXPIRED
    )
}

fn is_session_wide_failure(error: &NfsError) -> bool {
    matches!(error, NfsError::Nfs4(status) if is_session_wide_status(*status))
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

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream;
use futures::stream::TryStreamExt as _;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use super::callback::{CallbackService, CallbackState, CallbackWorker};
use super::compound::{
    CompoundBuilder, DelegationGrant, DelegationKind, NewLockArgs, OpenArgs, StateidOpOutcome,
    create_succeeded_before_compound_failure, decode_access_response, decode_commit_response,
    decode_create_response, decode_delegreturn_response,
    decode_getattr_response as decode_getattr_compound, decode_link_response,
    decode_lock_op_outcome, decode_lockt_response, decode_lookup_getattr_response,
    decode_open_response, decode_read_response, decode_readdir_response, decode_readlink_response,
    decode_release_lockowner_response, decode_remove_response, decode_rename_response,
    decode_setattr_response, decode_stateid_op_outcome, decode_stateid_response,
    decode_write_response, open_succeeded_before_compound_failure,
};
use super::lease::{LeaseRenewal, LeaseState, RecoveryHandler};
use super::recovery::{
    FreshOpenSpec, RecoveryContext, StateidRecovery, classify_stateid_failure, close_fresh_open,
    establish_identity, fresh_open_lane_with_seqid, lock_op_consumes_seqid, recovery_failure,
    state_recovery_exhausted, stateid_hex, stateid_op_consumes_seqid, status_consumes_seqid,
};
use super::state::{
    LockLane, LockState, LostLock, OpenState, OwnerLane, decode_owner, encode_owner,
};
use crate::dialer::Endpoint;
use crate::error::{NfsError, OperationClass, RequestContext, Result, classify_sent_nfs40_error};
use crate::mount::{self, MountLifecycleState, NFSVersion};
use crate::nfs4::acl;
use crate::nfs4::attrs::{
    decode_fattr4_envelope, decode_getattr_response, encode_setattr, fattr4_has,
    standard_getattr_bitmap,
};
use crate::nfs4::compound::decode_navigation_response;
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};
use crate::{Mount, MountArgs};

const NFS4_DEFAULT_PORT: u16 = 2049;
pub(super) const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const SAFE_REPLAY: ReplayPolicy = ReplayPolicy::byte_identical(2);
const CALLBACK_GETATTR_BITMAP: [u32; 1] = [0x18];
struct DetachedSettlement<T: 'static> {
    future: Option<Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>>,
}

impl<T: 'static> DetachedSettlement<T> {
    fn new(future: impl Future<Output = Result<T>> + Send + 'static) -> Self {
        Self {
            future: Some(Box::pin(crate::retry::inherit(future))),
        }
    }
}

impl<T: 'static> Future for DetachedSettlement<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(Err(NfsError::Rpc(
                "NFSv4.0 settlement polled after completion".into(),
            )));
        };
        let result = future.as_mut().poll(context);
        if result.is_ready() {
            self.future = None;
        }
        result
    }
}

impl<T: 'static> Drop for DetachedSettlement<T> {
    fn drop(&mut self) {
        let Some(future) = self.future.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(async move {
                let _ = future.await;
            }));
        }
    }
}

fn decode_callback_attributes(mut data: Bytes) -> Result<(u64, u64)> {
    let (bitmap, mut values) = decode_fattr4_envelope(&mut data, "delegation getattr")?;
    if bitmap != CALLBACK_GETATTR_BITMAP || values.remaining() != 16 {
        return Err(NfsError::Xdr(
            "delegation GETATTR did not return exactly change and size".into(),
        ));
    }
    Ok((values.get_u64(), values.get_u64()))
}

async fn query_callback_attributes(
    rpc: &rpc::Client,
    auth: &Auth,
    fh: &Bytes,
) -> Result<(u64, u64)> {
    let request = CompoundBuilder::new("delegation-getattr")
        .putfh(fh)
        .getattr(&CALLBACK_GETATTR_BITMAP)
        .encode_with_header(auth);
    let response = rpc.call(request, SAFE_REPLAY, METADATA_TIMEOUT).await?;
    decode_callback_attributes(decode_getattr_compound(response)?)
}
/// The gate error returned while a lost lock has not been reacquired.
///
/// Recovery may keep an open after its lock could not be reclaimed. Serving I/O
/// through it would silently drop the application out of its critical section,
/// so reads and writes on that handle fail explicitly until the caller
/// reacquires a lock or closes it.
fn lost_lock_error(operation: &'static str, lost: &LostLock) -> NfsError {
    let cause = if lost.status == crate::Nfs4ErrorCode::NFS4ERR_DENIED {
        NfsError::LockDenied {
            lock_type: lost.lock_type,
            offset: lost.offset,
            length: lost.length,
            owner: lost.owner.clone(),
        }
    } else {
        NfsError::Nfs4(lost.status)
    };
    NfsError::StateRecoveryExhausted(Box::new(crate::StateRecoveryError {
        operation: operation.to_string(),
        status: lost.status,
        source: Box::new(cause),
    }))
}

struct Mount40 {
    rpc: rpc::Client,
    auth: Auth,
    root_fh: Bytes,
    client_id: Arc<AtomicU64>,
    issuer: u64,
    next_owner: AtomicU64,
    state: Arc<OpenState>,
    locks: Arc<LockState>,
    lease: Arc<LeaseState>,
    _renewal: Option<LeaseRenewal>,
    _callback: Option<CallbackService>,
    _callback_worker: Option<CallbackWorker>,
    callback_state: Option<Arc<CallbackState>>,
    dircount: u32,
    maxcount: u32,
    rsize: u32,
    wsize: u32,
    acl_supported: bool,
}

#[derive(Clone)]
pub(super) struct ClientIdentity {
    pub(super) verifier: [u8; 8],
    pub(super) owner: String,
}

impl ClientIdentity {
    fn new() -> Self {
        Self {
            verifier: rand::random(),
            owner: format!("nfs-rs-v4.0-{:016x}", rand::random::<u64>()),
        }
    }
}

impl Debug for Mount40 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Mount40")
            .field("root_fh_len", &self.root_fh.len())
            .field("client_identity", &"confirmed")
            .finish()
    }
}

pub(crate) async fn mount(args: &MountArgs) -> Result<Box<dyn Mount>> {
    let port = if args.nfsport == 0 {
        NFS4_DEFAULT_PORT
    } else {
        args.nfsport
    };
    let addrs = args
        .dialer()
        .resolve(&args.host, port)
        .await
        .map_err(NfsError::Io)?;
    let auth = Auth::new_unix("nfs-rs", args.uid, args.gid);
    let mut last_error = None;
    for addr in addrs {
        match mount_on_addr(&addr, args, auth.clone()).await {
            Ok(mount) => return Ok(Box::new(mount)),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| NfsError::Rpc("NFSv4.0 resolved no addresses".to_string())))
}

async fn mount_on_addr(addr: &Endpoint, args: &MountArgs, auth: Auth) -> Result<Mount40> {
    let mux = rpc::StreamMux::dial(args.dialer(), addr.clone()).await?;
    let rpc = rpc::Client::new(mux, None);

    let callback = if args.retain_delegations {
        // The callback listener is reached by the server directly, so it is
        // bound for the server's resolved address.
        let server = addr.socket_addr().ok_or_else(|| {
            NfsError::InvalidInput("delegation callbacks need a resolved server address".into())
        })?;
        Some(CallbackService::bind_for(server).await?)
    } else {
        None
    };
    let callback_addr = callback
        .as_ref()
        .map(|service| service.universal_addr().to_string());
    let callback_state = callback.as_ref().map(CallbackService::state);
    let callback_worker = match (&callback, &callback_state) {
        (Some(service), Some(state)) => Some(CallbackWorker::start(
            service.take_recall_receiver()?,
            rpc.clone(),
            auth.clone(),
            Arc::clone(state),
        )),
        _ => None,
    };
    let identity_rpc = rpc.clone();
    let identity_auth = auth.clone();
    let identity_config = ClientIdentity::new();
    let identity_for_task = identity_config.clone();
    let callback_for_task = callback_addr.clone();
    let identity = tokio::spawn(async move {
        establish_identity(
            &identity_rpc,
            &identity_auth,
            &identity_for_task,
            callback_for_task.as_deref(),
        )
        .await
    });
    let client_id = identity
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 identity task failed: {error}")))??;
    let client_id = Arc::new(AtomicU64::new(client_id));

    let components: Vec<&str> = args
        .dirpath
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    let mut navigation = CompoundBuilder::new("navigate").putrootfh();
    for component in &components {
        navigation = navigation.lookup(component);
    }
    let response = rpc
        .call(
            navigation.getfh().encode_with_header(&auth),
            SAFE_REPLAY,
            METADATA_TIMEOUT,
        )
        .await?;
    let root_fh = decode_navigation_response(response, components.len())?;
    let (lease_time, rsize, wsize, acl_supported, maxread_published) =
        query_mount_parameters(&rpc, &auth, &root_fh).await?;
    let (dircount, maxcount) = crate::mount::directory_reply_limits(
        args.readdir_buffer,
        maxread_published.then_some(rsize),
    );
    let generation = 1;
    let lease = LeaseState::ready(generation, lease_time);
    let issuer = rand::random();
    let state = Arc::new(OpenState::default());
    let locks = Arc::new(LockState::default());
    let reconnect_lease = Arc::clone(&lease);
    rpc.set_reconnect_handler(move |_client, _generation| {
        let lease = Arc::clone(&reconnect_lease);
        async move {
            lease.mark_reconnecting();
            // Probe the client id now: a restart answers STALE_CLIENTID and
            // starts recovery immediately instead of after the next RENEW tick.
            lease.wake_renewal();
            Ok(())
        }
    })?;
    let recovery_context = RecoveryContext {
        rpc: rpc.clone(),
        auth: auth.clone(),
        identity: identity_config,
        client_id: Arc::clone(&client_id),
        issuer,
        state: Arc::clone(&state),
        locks: Arc::clone(&locks),
        lease: Arc::clone(&lease),
        callback_addr,
    };
    let recovery_callback_state = callback_state.clone();
    let recovery: RecoveryHandler = Arc::new(move || {
        let context = recovery_context.clone();
        let callback_state = recovery_callback_state.clone();
        Box::pin(async move {
            if let Some(callback_state) = callback_state {
                callback_state.invalidate_delegations(context.lease.generation())?;
            }
            context.recover_or_lose().await
        })
    });
    let renewal = LeaseRenewal::start(
        rpc.clone(),
        auth.clone(),
        Arc::clone(&client_id),
        Duration::from_secs(u64::from(lease_time / 3).max(1)),
        Arc::clone(&lease),
        Some(recovery),
    );

    Ok(Mount40 {
        rpc,
        auth,
        root_fh,
        client_id,
        issuer,
        next_owner: AtomicU64::new(1),
        state,
        locks,
        lease,
        _renewal: Some(renewal),
        _callback: callback,
        _callback_worker: callback_worker,
        callback_state,
        dircount,
        maxcount,
        rsize,
        wsize,
        acl_supported,
    })
}

async fn query_mount_parameters(
    rpc: &rpc::Client,
    auth: &Auth,
    root_fh: &Bytes,
) -> Result<(u32, u32, u32, bool, bool)> {
    let response = rpc
        .call(
            CompoundBuilder::new("lease-time")
                .putfh(root_fh)
                .getattr(&[(1 << 0) | (1 << 10) | (1 << 30) | (1 << 31)])
                .encode_with_header(auth),
            SAFE_REPLAY,
            METADATA_TIMEOUT,
        )
        .await?;
    let mut attrs = decode_getattr_compound(response)?;
    let (bitmap, mut values) = decode_fattr4_envelope(&mut attrs, "lease_time")?;
    let supported_attrs = if fattr4_has(&bitmap, 0) {
        take_bitmap4_attr(&mut values, "supported_attrs")?
    } else {
        Vec::new()
    };
    if !fattr4_has(&bitmap, 10) || values.remaining() < 4 {
        return Err(NfsError::Xdr(
            "NFSv4.0 lease_time response is missing or malformed".into(),
        ));
    }
    let seconds = values.get_u32();
    if seconds == 0 {
        return Err(NfsError::Xdr("NFSv4.0 lease_time is zero".into()));
    }
    let maxread_published = fattr4_has(&bitmap, 30);
    let server_maxread = if maxread_published {
        take_u64_attr(&mut values, "maxread")?
    } else {
        u64::MAX
    };
    let server_maxwrite = if fattr4_has(&bitmap, 31) {
        take_u64_attr(&mut values, "maxwrite")?
    } else {
        u64::MAX
    };
    ensure_attr_values_consumed(&values, "mount parameters")?;
    let rsize = crate::mount::negotiated_io_size(server_maxread)?;
    let wsize = crate::mount::negotiated_io_size(server_maxwrite)?;
    if rsize == 0 || wsize == 0 {
        return Err(NfsError::Xdr(
            "NFSv4.0 MAXREAD and MAXWRITE produced a zero effective I/O size".into(),
        ));
    }
    Ok((
        seconds,
        rsize,
        wsize,
        fattr4_has(&supported_attrs, 12),
        maxread_published,
    ))
}

pub(super) async fn return_delegation(
    rpc: &rpc::Client,
    auth: &Auth,
    fh: &Bytes,
    delegation: Option<DelegationGrant>,
) -> Result<()> {
    let Some(delegation) = delegation else {
        return Ok(());
    };
    let response = rpc
        .call(
            CompoundBuilder::new("delegreturn")
                .putfh(fh)
                .delegreturn(&delegation.stateid)
                .encode_with_header(auth),
            ReplayPolicy::ONE_ATTEMPT,
            METADATA_TIMEOUT,
        )
        .await?;
    decode_delegreturn_response(response)
}

fn context(
    operation: &str,
    owner: u64,
    seqid: u32,
    class: OperationClass,
) -> (OperationClass, RequestContext) {
    (
        class,
        RequestContext {
            operation: operation.to_string(),
            protocol: NFSVersion::NFSv4p0,
            request_id: Some(crate::error::RequestId::nfs40(owner, seqid)),
        },
    )
}

/// Decodes a `PUTFH + LOCK` reply together with whether that request consumed
/// the owner seqid. A PUTFH failure reports a non-zero overall status but never
/// ran the operation, so it must not advance the cursor.
fn decode_lock_call(response: Result<Bytes>, opcode: u32, name: &str) -> (Result<[u8; 16]>, bool) {
    match response.and_then(|response| decode_lock_op_outcome(response, opcode, name)) {
        Ok(outcome) => {
            let consumed = lock_op_consumes_seqid(&outcome);
            (outcome.into_result(), consumed)
        }
        Err(error) => (Err(error), false),
    }
}

/// CLOSE an OPEN that was never published, reporting whether the request
/// consumed the owner seqid. `None` means the transmission outcome is unknown,
/// so the caller must not replay either cursor value.
pub(super) async fn close_unpublished_state(
    rpc: &rpc::Client,
    auth: &Auth,
    fh: &Bytes,
    seqid: u32,
    stateid: &[u8; 16],
) -> Option<bool> {
    let request = CompoundBuilder::new("close-fresh")
        .putfh(fh)
        .close(seqid, stateid)
        .encode_with_header(auth);
    let response = match rpc
        .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(
                target: "nfs_rs::stateid",
                ?error,
                "CLOSE of an uncommitted OPEN state failed; its seqid consumption is unknown"
            );
            return None;
        }
    };
    match decode_stateid_op_outcome(response, 4, "CLOSE") {
        Ok(outcome) => Some(stateid_op_consumes_seqid(&outcome)),
        Err(error) => {
            tracing::debug!(
                target: "nfs_rs::stateid",
                ?error,
                "CLOSE of an uncommitted OPEN state was not decodable; its seqid consumption is unknown"
            );
            None
        }
    }
}

async fn settled_call(
    rpc: rpc::Client,
    request: Vec<u8>,
    class: OperationClass,
    ctx: RequestContext,
) -> Result<Bytes> {
    tokio::spawn(crate::retry::inherit(async move {
        call_with_busy_retry(
            &rpc,
            request,
            Bytes::new(),
            ReplayPolicy::ONE_ATTEMPT,
            METADATA_TIMEOUT,
            ctx.clone(),
        )
        .await
        .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
    }))
    .await
    .map_err(|error| NfsError::Rpc(format!("NFSv4.0 settlement task failed: {error}")))?
}

async fn call_with_busy_retry(
    rpc: &rpc::Client,
    request: Vec<u8>,
    data: Bytes,
    replay: ReplayPolicy,
    timeout: Duration,
    context: RequestContext,
) -> Result<Bytes> {
    crate::retry::scope(async {
        let budget = crate::retry::current();
        loop {
            let reply = rpc
                .call_with_data(request.clone(), data.clone(), replay, timeout)
                .await?;
            let Some(error) = super::compound::retryable_busy_response(reply.clone(), &request)?
            else {
                return Ok(reply);
            };
            budget.backoff(context.clone(), error).await?;
        }
    })
    .await
}

impl Mount40 {
    async fn acquire_lock(
        &self,
        open_lane: Arc<tokio::sync::Mutex<OwnerLane>>,
        fh: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<(Bytes, u64)> {
        let expected_generation = self.lease.begin_stateful("lock")?;
        let owner = self.next_owner.fetch_add(1, Ordering::Relaxed);
        let owner_wire = Bytes::from(format!("nfs-rs-lock-{:016x}-{owner:016x}", self.issuer));
        // RFC 7530 §9.1.9: OLD_STATEID consumes the lock-owner seqid, so a
        // retry must advance it instead of reusing lock_seqid 0 (which the
        // server would treat as a replay and answer from its cache).
        let mut lock_seqid = 0u32;
        let mut retries = 0u8;
        loop {
            let rpc = self.rpc.clone();
            let auth = self.auth.clone();
            let client_id = self.client_id.load(Ordering::Acquire);
            let locks = Arc::clone(&self.locks);
            let lease = Arc::clone(&self.lease);
            let open_lane_task = Arc::clone(&open_lane);
            let fh = fh.clone();
            let owner_wire = owner_wire.clone();
            let lock_seqid_for_task = lock_seqid;
            let used = Arc::new(std::sync::Mutex::new(None));
            let used_in_task = Arc::clone(&used);
            // Track the acquisition before spawning. If the caller is
            // cancelled, the detached task keeps this guard until it has
            // registered the granted lock, so `release_locks` can wait for it.
            let acquisition = self.locks.begin_acquisition(fh.clone());
            let stateid = tokio::spawn(async move {
                let _acquisition = acquisition;
                // CLOSE sets `closing` and takes the I/O fence. Take the same
                // fence-before-lane order as READ/WRITE/close so a LOCK cannot
                // be issued while that open state is being released.
                let io_fence = {
                    let open = open_lane_task.lock().await;
                    if open.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    Arc::clone(&open.io_fence)
                };
                let _in_flight = io_fence.read_owned().await;
                let mut open = open_lane_task.lock().await;
                if open.closing {
                    return Err(NfsError::InvalidInput(
                        "NFSv4.0 open state is closing or closed".into(),
                    ));
                }
                let open_owner = open.owner;
                *used_in_task
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(open.stateid);
                let request = CompoundBuilder::new("lock")
                    .putfh(&fh)
                    .lock_new(NewLockArgs {
                        lock_type,
                        reclaim: false,
                        offset,
                        length,
                        open_seqid: open.next_seqid,
                        open_stateid: &open.stateid,
                        lock_seqid: lock_seqid_for_task,
                        client_id,
                        owner: &owner_wire,
                    })
                    .encode_with_header(&auth);
                let (class, ctx) = context(
                    "lock",
                    owner,
                    lock_seqid_for_task,
                    OperationClass::ReplaySensitive,
                );
                let (decoded, consumed) = decode_lock_call(
                    rpc.call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                        .await,
                    12,
                    "LOCK",
                );
                if consumed {
                    open.next_seqid = open.next_seqid.wrapping_add(1);
                }
                let stateid =
                    decoded.map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
                let _publication = lease.publication_guard().await;
                // Register before the lifecycle fence: the server may already
                // have granted the lock, and losing the lane would leak it
                // until lease expiry. A lost mount removes it below; a Suspect
                // or Reconnecting mount keeps it so cleanup can unlock it after
                // recovery.
                let gate_fh = fh.clone();
                locks
                    .register(LockLane {
                        owner,
                        open_owner,
                        owner_wire,
                        next_seqid: lock_seqid_for_task.wrapping_add(1),
                        stateid,
                        fh,
                        lock_type,
                        offset,
                        length,
                    })
                    .await;
                if let Err(error) = lease.finish_stateful(expected_generation, "lock") {
                    if lease.generation() != expected_generation {
                        locks.remove(&stateid).await;
                    }
                    return Err(error);
                }
                if let Err(error) = lease.validate_stateful(expected_generation, "lock") {
                    if lease.generation() != expected_generation {
                        locks.remove(&stateid).await;
                    }
                    return Err(error);
                }
                // Only a lock that restores the lost protection resolves it: a
                // disjoint or weaker lock on the same handle acknowledges
                // nothing, and other losses on the handle stay gated.
                locks
                    .resolve_lost(&gate_fh, lock_type, offset, length)
                    .await;
                Ok::<_, NfsError>(stateid)
            })
            .await
            .map_err(|error| NfsError::Rpc(format!("NFSv4.0 lock task failed: {error}")))?;
            match stateid {
                Ok(stateid) => return Ok((Bytes::copy_from_slice(&stateid), expected_generation)),
                Err(error) => {
                    // The LOCK carries the open stateid; a stateid failure is the
                    // open lane's to recover before the lock can be created.
                    let (trigger, error) = classify_stateid_failure("lock", &mut retries, error)?;
                    let failed_stateid = used
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .ok_or_else(|| state_recovery_exhausted("lock", trigger, error))?;
                    self.refresh_lane_stateid(&open_lane, failed_stateid, trigger, "lock")
                        .await
                        .map_err(|failure| recovery_failure("lock", trigger, failure))?;
                    if trigger == StateidRecovery::Old {
                        lock_seqid = lock_seqid.wrapping_add(1);
                    }
                }
            }
        }
    }

    async fn open_file(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
        create: bool,
    ) -> Result<mount::OpenFile> {
        self.open_file_with_create(dir_fh, filename, access, create, false, None)
            .await
    }

    async fn open_guarded(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
        mode: Option<u32>,
    ) -> Result<mount::OpenFile> {
        match self
            .open_file_with_create(dir_fh.clone(), filename, access, true, true, mode)
            .await
        {
            // A definite OPEN attribute rejection has not created a file.
            // A new owner is allocated on retry. Partial OPEN success is wrapped
            // as uncertain below and must never take this compatibility fallback.
            Err(NfsError::Nfs4(crate::nfs4::fastxdr::nfsstat4::NFS4ERR_ATTRNOTSUPP))
                if mode.is_some() =>
            {
                let file = self
                    .open_file_with_create(dir_fh, filename, access, true, true, None)
                    .await?;
                if let Err(error) = self
                    .setattr(file.file_handle(), None, mode, None, None, None, None, None)
                    .await
                {
                    let _ = self.close_stateful(file).await;
                    return Err(error);
                }
                Ok(file)
            }
            result => result,
        }
    }

    async fn open_file_with_create(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
        create: bool,
        guarded: bool,
        mode: Option<u32>,
    ) -> Result<mount::OpenFile> {
        let expected_generation = self.lease.begin_stateful("open")?;
        if !matches!(
            access,
            crate::OPEN_READ | crate::OPEN_WRITE | crate::OPEN_BOTH
        ) {
            return Err(NfsError::InvalidInput(format!(
                "invalid OPEN access {access}"
            )));
        }
        let owner = self.next_owner.fetch_add(1, Ordering::Relaxed);
        let owner_wire = format!("nfs-rs-{:016x}-{owner:016x}", self.issuer);
        let filename = filename.to_string();
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let state = Arc::clone(&self.state);
        let client_id = self.client_id.load(Ordering::Acquire);
        let issuer = self.issuer;
        let lease = Arc::clone(&self.lease);
        let callback_state = self.callback_state.clone();
        let callback_publication = callback_state
            .as_ref()
            .map(|state| state.begin_open_publication());
        let opened = tokio::spawn(async move {
            let tag = if guarded {
                "create_guarded"
            } else if create {
                "create"
            } else {
                "open"
            };
            let builder = CompoundBuilder::new(tag).putfh(&dir_fh);
            let args = OpenArgs {
                seqid: 0,
                share_access: access,
                client_id,
                owner: owner_wire.as_bytes(),
                filename: &filename,
                create,
            };
            let builder = if guarded {
                builder.open_guarded(args, mode)
            } else {
                builder.open(args)
            };
            let request = builder.getfh().encode_with_header(&auth);
            let operation = if create { "create" } else { "open" };
            let (class, ctx) = context(operation, owner, 0, OperationClass::ReplaySensitive);
            let response = rpc
                .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            let partial_success = open_succeeded_before_compound_failure(response.clone());
            let (opened, fh) = decode_open_response(response).map_err(|error| {
                if partial_success {
                    NfsError::OperationOutcome(Box::new(crate::error::OperationOutcomeError::new(
                        crate::error::OperationOutcome::Uncertain,
                        class,
                        crate::error::RecoveryAction::VerifyThenResume,
                        ctx.clone(),
                        error,
                    )))
                } else {
                    classify_sent_nfs40_error(class, ctx, error)
                }
            })?;
            let mut next_seqid = 1;
            let mut stateid = opened.stateid;
            if opened.confirm_required {
                let request = CompoundBuilder::new("open_confirm")
                    .putfh(&fh)
                    .open_confirm(&stateid, next_seqid)
                    .encode_with_header(&auth);
                let (class, ctx) = context(
                    "open_confirm",
                    owner,
                    next_seqid,
                    OperationClass::ReplaySensitive,
                );
                let response = rpc
                    .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                    .await
                    .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
                stateid = decode_stateid_response(response, 20, "OPEN_CONFIRM")
                    .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
                next_seqid += 1;
            }
            // OPEN has succeeded on the server. Track it before any follow-up
            // delegation work so a failed DELEGRETURN remains closeable and
            // reclaimable instead of becoming an orphaned server-side state.
            state
                .register(OwnerLane {
                    owner,
                    next_seqid,
                    stateid,
                    fh: fh.clone(),
                    dir_fh: dir_fh.clone(),
                    name: filename.clone(),
                    access,
                    write_verifier: None,
                    io_fence: Arc::new(tokio::sync::RwLock::new(())),
                    closing: false,
                })
                .await;
            if callback_state.is_none()
                && let Some(delegation) = opened.delegation
            {
                let (class, ctx) = context(
                    "delegreturn-after-open",
                    owner,
                    next_seqid,
                    OperationClass::ReplaySensitive,
                );
                let returned = rpc
                    .call(
                        CompoundBuilder::new("delegreturn")
                            .putfh(&fh)
                            .delegreturn(&delegation.stateid)
                            .encode_with_header(&auth),
                        ReplayPolicy::ONE_ATTEMPT,
                        METADATA_TIMEOUT,
                    )
                    .await
                    .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))
                    .and_then(|response| {
                        decode_delegreturn_response(response)
                            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
                    });
                if let Err(error) = returned {
                    // The OPEN already succeeded, but the caller never receives
                    // the handle, so no later close() can complete it. Retire
                    // the lane after a best-effort CLOSE rather than leaving an
                    // unreachable open for recovery to adopt; whatever the
                    // CLOSE did not release expires with the lease. The
                    // finish_stateful path below keeps the lane instead because
                    // there the open was already published before the lifecycle
                    // gate rejected it, and the existing contract keeps
                    // published lanes on non-lost mounts.
                    close_fresh_open(&rpc, &auth, &fh, &stateid, next_seqid).await;
                    state.remove(owner, &fh).await;
                    return Err(error);
                }
            }
            let delegation_attributes = if callback_state.is_some()
                && matches!(
                    opened.delegation,
                    Some(DelegationGrant {
                        kind: DelegationKind::Write,
                        ..
                    })
                ) {
                query_callback_attributes(&rpc, &auth, &fh).await.ok()
            } else {
                None
            };
            let publication = lease.publication_guard().await;
            if let Err(error) = lease.finish_stateful(expected_generation, "open") {
                // The lifecycle gate already rejected this open, so release
                // the guard before the cleanup RPCs: holding it would delay
                // mark_lost() by a full round trip for no benefit.
                drop(publication);
                // The caller never receives the handle, so the delegation was
                // not registered with the callback state. Return it before any
                // CLOSE below: an orphaned delegation would otherwise be
                // recalled against a stateid the client no longer tracks.
                let _ = return_delegation(&rpc, &auth, &fh, opened.delegation).await;
                // A lost/rotated mount has already cleared its state maps, so
                // the lane published above would be unreachable: release the
                // state we just created instead of leaving an orphan open for
                // the next recovery pass to adopt. A Suspect or Reconnecting
                // mount still owns this open, and removing it would discard the
                // only handle that can close it.
                if lease.generation() != expected_generation {
                    close_fresh_open(&rpc, &auth, &fh, &stateid, next_seqid).await;
                    state.remove(owner, &fh).await;
                }
                return Err(error);
            }
            if let (Some(callback_state), Some(delegation)) = (&callback_state, opened.delegation) {
                callback_state.register_delegation(
                    fh.clone(),
                    delegation,
                    expected_generation,
                    delegation_attributes,
                )?;
            }
            drop(callback_publication);
            if let Err(error) = lease.validate_stateful(expected_generation, "open") {
                if lease.generation() != expected_generation {
                    state.remove(owner, &fh).await;
                }
                return Err(error);
            }
            Ok::<_, NfsError>(mount::OpenFile::with_protocol_state(
                mount::ObjRes { fh, attr: None },
                encode_owner(issuer, owner),
            ))
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 open task failed: {error}")))??;
        Ok(opened)
    }

    async fn commit_verifier(&self, fh: &Bytes, offset: u64, count: u32) -> Result<[u8; 8]> {
        let request = CompoundBuilder::new("commit")
            .putfh(fh)
            .commit(offset, count)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("commit", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        let verifier = decode_commit_response(response)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
        Ok(verifier)
    }

    async fn close_lane(&self, lane: Arc<tokio::sync::Mutex<OwnerLane>>) -> Result<()> {
        let expected_generation = self.lease.begin_stateful("close")?;
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let state = Arc::clone(&self.state);
        let lease = Arc::clone(&self.lease);
        let callback_state = self.callback_state.clone();
        tokio::spawn(async move {
            let (close_fh, io_fence) = {
                let mut lane = lane.lock().await;
                if lane.closing {
                    return Err(NfsError::InvalidInput(
                        "NFSv4.0 open state is already closing or closed".into(),
                    ));
                }
                lane.closing = true;
                (lane.fh.clone(), Arc::clone(&lane.io_fence))
            };
            // Take the callback I/O gate before the fence. Foreground I/O takes
            // the gate first and the fence second, so doing the opposite here
            // would deadlock: close would hold the fence write while waiting
            // for a reader that is itself waiting on that fence.
            let io_guard = if let Some(callback_state) = &callback_state {
                Some(callback_state.recall_io(&close_fh).await)
            } else {
                None
            };
            let _in_flight_settled = io_fence.write_owned().await;
            let _io_guard = io_guard;
            let mut lane = lane.lock().await;
            let request = CompoundBuilder::new("close")
                .putfh(&lane.fh)
                .close(lane.next_seqid, &lane.stateid)
                .encode_with_header(&auth);
            let (class, ctx) = context(
                "close",
                lane.owner,
                lane.next_seqid,
                OperationClass::ReplaySensitive,
            );
            // Only a CLOSE that actually ran consumes the open-owner seqid.
            // A COMPOUND failure at PUTFH also reports a non-zero overall
            // status, so inspect the per-operation results before advancing.
            let outcome = rpc
                .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .and_then(|response| decode_stateid_op_outcome(response, 4, "CLOSE"));
            let (result, mut close_executed, mut close_consumed) = match outcome {
                Ok(StateidOpOutcome::Completed(stateid)) => (Ok(stateid), true, true),
                Ok(StateidOpOutcome::Executed(error)) => (Err(error), true, true),
                Ok(StateidOpOutcome::Rejected(error)) => {
                    let consumed =
                        matches!(&error, NfsError::Nfs4(status) if status_consumes_seqid(*status));
                    (Err(error), true, consumed)
                }
                Ok(StateidOpOutcome::NotExecuted(error)) => (Err(error), false, false),
                Err(error) => (Err(error), false, false),
            };
            let mut result = result.map_err(|error| classify_sent_nfs40_error(class, ctx, error));
            // RFC 7530 §9.1.4.4: OLD_STATEID means the server holds this open
            // state with a newer stateid seqid. The gap is not disclosed, so a
            // client-side +1 cannot converge when it is larger than one, and
            // close_lane has no dir_fh/name with which to re-sync via a fresh
            // OPEN. Leave `result` untouched: the OLD_STATEID arm below reports
            // the loss (recovery_action=reopen) after advancing the consumed
            // open-owner seqid (§9.1.7).
            // RFC 7530 §9.1.4.4: BAD_STATEID is also returned when the stateid
            // seqid is later than the server's, in which case the open state
            // still exists. Probe once with the previous stateid seqid before
            // assuming the state is gone; a genuine loss just repeats the
            // status, and BAD_STATEID does not consume the open-owner seqid.
            if let Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID)) = &result {
                let seqid = u32::from_be_bytes([
                    lane.stateid[0],
                    lane.stateid[1],
                    lane.stateid[2],
                    lane.stateid[3],
                ]);
                if let Some(previous) = seqid.checked_sub(1) {
                    let mut probe_stateid = lane.stateid;
                    probe_stateid[..4].copy_from_slice(&previous.to_be_bytes());
                    let request = CompoundBuilder::new("close")
                        .putfh(&lane.fh)
                        .close(lane.next_seqid, &probe_stateid)
                        .encode_with_header(&auth);
                    let (class, ctx) = context(
                        "close",
                        lane.owner,
                        lane.next_seqid,
                        OperationClass::ReplaySensitive,
                    );
                    let outcome = rpc
                        .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                        .await
                        .and_then(|response| decode_stateid_op_outcome(response, 4, "CLOSE"));
                    let (probe_result, probe_executed, probe_consumed) = match outcome {
                        Ok(StateidOpOutcome::Completed(stateid)) => (Ok(stateid), true, true),
                        Ok(StateidOpOutcome::Executed(error)) => (Err(error), true, true),
                        Ok(StateidOpOutcome::Rejected(error)) => {
                            let consumed = matches!(&error, NfsError::Nfs4(status) if status_consumes_seqid(*status));
                            (Err(error), true, consumed)
                        }
                        Ok(StateidOpOutcome::NotExecuted(error)) => (Err(error), false, false),
                        Err(error) => (Err(error), false, false),
                    };
                    result =
                        probe_result.map_err(|error| classify_sent_nfs40_error(class, ctx, error));
                    // Consumption is monotonic: if the first CLOSE already ran,
                    // a probe that never executed must not erase that fact.
                    close_executed |= probe_executed;
                    close_consumed |= probe_consumed;
                }
            }
            // The client's stateid only ever comes from a server reply and is
            // never advanced beyond the server, so a two-step gap cannot arise:
            // if both the original CLOSE and the seqid-1 probe report
            // BAD_STATEID, the server has no state for this open. The remaining
            // server-side state, if any, expires with the lease.
            match result {
                Ok(stateid) => {
                    lane.stateid = stateid;
                    lane.next_seqid = lane.next_seqid.wrapping_add(1);
                }
                Err(error) if !close_executed => {
                    // CLOSE never ran (e.g. PUTFH failed), so the server did not
                    // consume the open-owner seqid. Keep it for a normal retry.
                    lane.closing = false;
                    return Err(error);
                }
                Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID)) => {
                    // The server still holds this open state with a newer stateid
                    // seqid that close_lane cannot determine without a fresh OPEN.
                    // The rejected CLOSE consumed the open-owner seqid (§9.1.7),
                    // so advance it before the caller's reopen reissues CLOSE and
                    // would otherwise trip BAD_SEQID. Keep the lane closeable and
                    // report the loss (recovery_action=reopen).
                    lane.next_seqid = lane.next_seqid.wrapping_add(1);
                    lane.closing = false;
                    return Err(state_recovery_exhausted(
                        "close",
                        StateidRecovery::Old,
                        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID),
                    ));
                }
                // RFC 7530 §9.1.4.3: BAD/STALE mean the server no longer knows
                // this state, so CLOSE is already satisfied. Treat it as
                // released instead of wedging the lane in `closing`. An
                // invalid client id after a restart proves the same thing.
                Err(error)
                    if error.is_recoverable_state()
                        || matches!(
                            &error,
                            NfsError::Nfs4(
                                crate::Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID
                                    | crate::Nfs4ErrorCode::NFS4ERR_EXPIRED
                            )
                        ) =>
                {
                    tracing::debug!(
                        target: "nfs_rs::stateid",
                        operation = "close",
                        owner = lane.owner,
                        error = %error,
                        "NFSv4.0 CLOSE found no server state; treating it as released"
                    );
                }
                Err(error) => {
                    // A definite protocol error outside the exception list
                    // consumed the open-owner seqid, whether or not this was
                    // the OLD_STATEID retry. Advance it so the next CLOSE does
                    // not replay a consumed seqid and get stuck on BAD_SEQID.
                    if close_consumed {
                        lane.next_seqid = lane.next_seqid.wrapping_add(1);
                    }
                    // Do not leave the lane permanently wedged: a failed CLOSE
                    // that did not release the state must remain closeable.
                    lane.closing = false;
                    return Err(error);
                }
            }
            let owner = lane.owner;
            let fh = lane.fh.clone();
            drop(lane);
            state.remove(owner, &fh).await;
            lease.finish_stateful(expected_generation, "close")?;
            Ok::<_, NfsError>(())
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 close task failed: {error}")))??;
        Ok(())
    }

    /// Refresh one lane after a stateid error.
    ///
    /// The lane lock is held across the fresh OPEN so concurrent I/O observes
    /// the new stateid instead of racing a second OPEN on the same lane. If the
    /// stateid already moved (another task recovered first) there is nothing to
    /// do and the caller simply retries with the current state.
    async fn refresh_lane_stateid(
        &self,
        lane: &Arc<tokio::sync::Mutex<OwnerLane>>,
        failed_stateid: [u8; 16],
        trigger: StateidRecovery,
        operation: &str,
    ) -> Result<()> {
        // Capture the generation before spawning: the detached task must not
        // send a replay-sensitive OPEN, or publish its result, after the mount
        // was marked lost (which also clears the state maps) or after a newer
        // recovery began.
        let expected_generation = self.lease.begin_stateful(operation)?;
        // Run the whole refresh in its own task. Cancelling the initiating
        // READ/WRITE after the recovery OPEN reached the server must not drop
        // the future while the lane still holds the old stateid and an
        // unpublished owner seqid: the task keeps the lane lock until it
        // publishes both.
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let issuer = self.issuer;
        let client_id = Arc::clone(&self.client_id);
        let lease = Arc::clone(&self.lease);
        let open_state = Arc::clone(&self.state);
        let lane = Arc::clone(lane);
        let operation = operation.to_string();
        tokio::spawn(async move {
            Self::refresh_lane_stateid_task(
                rpc,
                auth,
                issuer,
                client_id,
                lease,
                open_state,
                expected_generation,
                lane,
                failed_stateid,
                trigger,
                &operation,
            )
            .await
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 stateid recovery task failed: {error}")))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn refresh_lane_stateid_task(
        rpc: rpc::Client,
        auth: Auth,
        issuer: u64,
        client_id: Arc<AtomicU64>,
        lease: Arc<LeaseState>,
        open_state: Arc<OpenState>,
        expected_generation: u64,
        lane: Arc<tokio::sync::Mutex<OwnerLane>>,
        failed_stateid: [u8; 16],
        trigger: StateidRecovery,
        operation: &str,
    ) -> Result<()> {
        if trigger == StateidRecovery::Stale {
            // RFC 7530 §13.1.5.6: a stateid from an earlier server instance
            // means crash recovery (SETCLIENTID + reclaim of every open). That
            // is a mount-lifecycle operation: running it inside one I/O call
            // would race the lease renewal and other lanes, so report a typed
            // state-lost error and let the caller reopen or the renewal task
            // drive recovery.
            return Err(NfsError::LostOpenState(format!(
                "NFSv4.0 stateid belongs to an earlier server instance during {operation}; reopen the file"
            )));
        }
        let mut open = lane.lock().await;
        if open.closing {
            // A concurrent CLOSE already released (or is releasing) this state.
            // That is a lifecycle answer, not an exhausted recovery: keep it as
            // a closed-resource error so callers do not retry a reopen.
            return Err(NfsError::ClosedResource(format!(
                "NFSv4.0 open state closed during {operation} stateid recovery"
            )));
        }
        if open.stateid != failed_stateid {
            return Ok(());
        }
        // This task may have been queued while the mount was being marked lost,
        // in which case its state map was already cleared. Do not dispatch a
        // replay-sensitive OPEN that nothing will track or clean up. This check
        // is best effort: mark_lost can still win between it and the OPEN
        // reaching the server, which the publication fence below catches.
        lease.validate_stateful(expected_generation, operation)?;
        let old_stateid = open.stateid;
        // OLD_STATEID does not carry the server's current seqid, so a blind +1
        // cannot converge when the server advanced by more than one. Fall
        // through to a fresh OPEN, which re-syncs the stateid authoritatively
        // regardless of the gap.
        let current_client_id = client_id.load(Ordering::Acquire);
        let owner_wire = format!("nfs-rs-{issuer:016x}-{:016x}", open.owner);
        let spec = FreshOpenSpec {
            dir_fh: &open.dir_fh,
            name: &open.name,
            owner_wire: owner_wire.as_bytes(),
            access: open.access,
            client_id: current_client_id,
        };
        let attempt =
            fresh_open_lane_with_seqid(&rpc, &auth, &spec, open.next_seqid, operation).await;
        // RFC 7530 §9.1.7: a definitive OPEN/OPEN_CONFIRM failure consumes its
        // owner seqid unless the status is in the exception table. The helper
        // reports the authoritative cursor for every stage that was actually
        // sent, so record it even when OPEN_CONFIRM (not OPEN) was the failing
        // stage. If no stage ran (PUTFH failed), keep the lane's existing
        // cursor and do not regress it to the seqid-0 probe.
        attempt.record_consumption(&mut open);
        let fresh = match attempt.result {
            Ok(fresh) => fresh,
            Err(error) => {
                return Err(error);
            }
        };
        // The OPEN succeeded, so the server consumed its open-owner seqid
        // regardless of which stateid it returned. Record it before the early
        // returns below, otherwise the lane would keep a consumed seqid and
        // every later attempt would replay this OPEN instead of converging.
        open.next_seqid = fresh.next_seqid;
        if fresh.fh != open.fh {
            // The path now resolves to a different object (replaced or
            // re-created by another client). Release the freshly created state
            // rather than leak it, then refuse to commit the recovery. The
            // lane's cursor must still reflect the cleanup CLOSE: it consumes
            // the seqid it was sent with, so discarding the reply would make
            // the next owner-sequenced request replay it and fail with
            // BAD_SEQID. An unknown outcome makes both values unsafe to replay,
            // so retire the lane instead. The freshly created OPEN may carry a
            // delegation: return it before closing, otherwise the server keeps
            // recalling a delegation whose filehandle this lane no longer owns.
            let _ = return_delegation(&rpc, &auth, &fresh.fh, fresh.delegation).await;
            match close_unpublished_state(&rpc, &auth, &fresh.fh, fresh.next_seqid, &fresh.stateid)
                .await
            {
                Some(true) => open.next_seqid = open.next_seqid.wrapping_add(1),
                Some(false) => {}
                None => {
                    open.closing = true;
                    let owner = open.owner;
                    let stale_fh = open.fh.clone();
                    drop(open);
                    open_state.remove(owner, &stale_fh).await;
                }
            }
            return Err(NfsError::LostOpenState(
                "NFSv4.0 stateid recovery opened a different filehandle".into(),
            ));
        }
        if let Err(error) = return_delegation(&rpc, &auth, &fresh.fh, fresh.delegation).await {
            tracing::debug!(
                target: "nfs_rs::stateid",
                ?error,
                "DELEGRETURN after NFSv4.0 stateid recovery failed; the server may hold a delegation it cannot recall"
            );
        }
        if fresh.stateid == failed_stateid {
            // The OPEN succeeded but kept the stateid that just failed. The
            // caller has a single retry left and it would send exactly that
            // stateid again, so report exhaustion instead of claiming a
            // recovery that did not happen. Do not CLOSE it: the stateid names
            // the very state the lane must keep.
            return Err(state_recovery_exhausted(
                operation,
                trigger,
                NfsError::Xdr(
                    "NFSv4.0 stateid recovery returned the stateid that just failed".into(),
                ),
            ));
        }
        // Fence publication: if the mount became lost while the recovery OPEN
        // was in flight, its state maps were cleared and publishing into this
        // detached lane would leave server state with no tracked cleanup path.
        // Settle the already-sent OPEN and refuse to commit it.
        {
            let publication = lease.publication_guard().await;
            if let Err(error) = lease.validate_stateful(expected_generation, operation) {
                drop(publication);
                match close_unpublished_state(
                    &rpc,
                    &auth,
                    &fresh.fh,
                    fresh.next_seqid,
                    &fresh.stateid,
                )
                .await
                {
                    Some(true) => open.next_seqid = open.next_seqid.wrapping_add(1),
                    Some(false) => {}
                    None => {
                        open.closing = true;
                        let owner = open.owner;
                        let fh = open.fh.clone();
                        drop(open);
                        open_state.remove(owner, &fh).await;
                    }
                }
                return Err(error);
            }
            open.stateid = fresh.stateid;
        }
        tracing::debug!(
            target: "nfs_rs::stateid",
            operation,
            trigger = ?trigger,
            old_stateid = %stateid_hex(&old_stateid),
            new_stateid = %stateid_hex(&open.stateid),
            "NFSv4.0 stateid recovered with a fresh OPEN"
        );
        Ok(())
    }

    async fn readdir_payload(
        &self,
        fh: &Bytes,
        cookie: u64,
        verifier: &[u8; 8],
        bitmap: &[u32],
    ) -> Result<Bytes> {
        let request = CompoundBuilder::new("readdir")
            .putfh(fh)
            .readdir(cookie, verifier, self.dircount, self.maxcount, bitmap)
            .encode_with_header(&self.auth);
        // A reply sized from the server's MAXREAD gets the transfer time a READ of that size
        // would, at the 1.25 MB/s floor the other versions assume.
        let timeout = METADATA_TIMEOUT + Duration::from_secs(u64::from(self.maxcount) / 1_250_000);
        let payload =
            decode_readdir_response(self.activity_call(request, SAFE_REPLAY, timeout).await?)?;
        validate_readdir_payload(payload, self.maxcount)
    }

    async fn query_fattrs(
        &self,
        fh: &Bytes,
        bitmap: &[u32],
        label: &str,
    ) -> Result<(Vec<u32>, Bytes)> {
        let request = CompoundBuilder::new(label)
            .putfh(fh)
            .getattr(bitmap)
            .encode_with_header(&self.auth);
        let mut data = decode_getattr_compound(
            self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                .await?,
        )?;
        decode_fattr4_envelope(&mut data, label)
    }

    async fn query_pathconf(&self, fh: &Bytes) -> Result<mount::SupportedPathconf> {
        let requested = [
            (1 << 0) | (1 << 8) | (1 << 16) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
            1 << 2,
        ];
        let (bitmap, mut values) = self.query_fattrs(fh, &requested, "pathconf").await?;
        require_fattrs(&bitmap, &[0, 8], "PATHCONF")?;
        let _supported_attrs = take_bitmap4_attr(&mut values, "supported_attrs")?;
        let fsid = (
            take_u64_attr(&mut values, "fsid.major")?,
            take_u64_attr(&mut values, "fsid.minor")?,
        );
        let available = pathconf_support(&bitmap);
        let mut result = mount::Pathconf {
            attr: None,
            linkmax: 32767,
            name_max: 255,
            no_trunc: true,
            chown_restricted: true,
            case_insensitive: false,
            case_preserving: true,
        };
        if available.case_insensitive {
            result.case_insensitive = take_bool_attr(&mut values, "case_insensitive")?;
        }
        if available.case_preserving {
            result.case_preserving = take_bool_attr(&mut values, "case_preserving")?;
        }
        if available.chown_restricted {
            result.chown_restricted = take_bool_attr(&mut values, "chown_restricted")?;
        }
        if available.linkmax {
            result.linkmax = take_u32_attr(&mut values, "maxlink")?;
        }
        if available.name_max {
            result.name_max = take_u32_attr(&mut values, "maxname")?;
        }
        if available.no_trunc {
            result.no_trunc = take_bool_attr(&mut values, "no_trunc")?;
        }
        ensure_attr_values_consumed(&values, "pathconf")?;
        Ok(mount::SupportedPathconf {
            values: result,
            available,
            fsid: Some(fsid),
        })
    }

    async fn refresh_callback_attributes(&self, fh: &Bytes) {
        let Some(state) = &self.callback_state else {
            return;
        };
        if let Ok((change, size)) = query_callback_attributes(&self.rpc, &self.auth, fh).await {
            let _ = state.publish_attributes(fh, change, size);
        }
    }

    async fn readdir_page(
        &self,
        fh: &Bytes,
        cookie: u64,
        verifier: &[u8; 8],
    ) -> Result<(Vec<Result<mount::ReaddirEntry>>, u64, [u8; 8], bool)> {
        let mut data = self
            .readdir_payload(fh, cookie, verifier, &[1u32 << 20])
            .await?;
        let (verifier, entries, eof) = decode_directory_page(&mut data, cookie)?;
        let last_cookie = entries
            .iter()
            .rev()
            .find_map(|entry| entry.as_ref().ok().map(|entry| entry.cookie))
            .unwrap_or(cookie);
        Ok((
            entries
                .into_iter()
                .map(|entry| {
                    entry.map(|entry| mount::ReaddirEntry {
                        fileid: entry.attr.fileid,
                        file_name: entry.name,
                    })
                })
                .collect(),
            last_cookie,
            verifier,
            eof,
        ))
    }

    async fn fetch_readdirplus_page(
        &self,
        fh: &Bytes,
        cookie: u64,
        verifier: &[u8; 8],
    ) -> Result<(Vec<Result<mount::ReaddirplusEntry>>, u64, [u8; 8], bool)> {
        let mut data = self
            .readdir_payload(fh, cookie, verifier, &standard_getattr_bitmap())
            .await?;
        let (verifier, entries, eof) = decode_directory_page(&mut data, cookie)?;
        let last_cookie = entries
            .iter()
            .rev()
            .find_map(|entry| entry.as_ref().ok().map(|entry| entry.cookie))
            .unwrap_or(cookie);
        Ok((
            entries
                .into_iter()
                .map(|entry| {
                    entry.map(|mut entry| {
                        // A copy: a slice would keep the whole reply (up to the read limit)
                        // alive for as long as the caller keeps the handle.
                        entry.attr.filehandle = Bytes::copy_from_slice(&entry.attr.filehandle);
                        mount::ReaddirplusEntry {
                            fileid: entry.attr.fileid,
                            file_name: entry.name,
                            handle: entry.attr.filehandle.clone(),
                            attr: Some(entry.attr),
                        }
                    })
                })
                .collect(),
            last_cookie,
            verifier,
            eof,
        ))
    }
}

#[derive(Debug)]
struct DirectoryEntry {
    cookie: u64,
    name: String,
    attr: mount::Attr,
}

fn decode_directory_page(
    data: &mut Bytes,
    initial_cookie: u64,
) -> Result<([u8; 8], Vec<Result<DirectoryEntry>>, bool)> {
    if data.remaining() < 8 {
        return Err(NfsError::Xdr("READDIR cookie verifier truncated".into()));
    }
    let mut verifier = [0; 8];
    data.copy_to_slice(&mut verifier);
    let mut entries = Vec::new();
    let mut previous_cookie = initial_cookie;
    loop {
        if data.remaining() < 4 {
            let error = NfsError::Xdr("READDIR entry discriminator truncated".into());
            if entries.is_empty() {
                return Err(error);
            }
            entries.push(Err(error));
            return Ok((verifier, entries, true));
        }
        match data.get_u32() {
            0 => break,
            1 => {}
            value => {
                entries.push(Err(NfsError::Xdr(format!(
                    "READDIR entry discriminator is {value}, expected 0 or 1"
                ))));
                return Ok((verifier, entries, true));
            }
        }
        let decoded = decode_directory_entry(data, previous_cookie);
        let cookie = match &decoded {
            Ok(entry) => entry.cookie,
            Err(_) => {
                entries.push(decoded);
                return Ok((verifier, entries, true));
            }
        };
        previous_cookie = cookie;
        entries.push(decoded);
    }
    if data.remaining() < 4 {
        let error = NfsError::Xdr("READDIR eof flag truncated".into());
        if entries.is_empty() {
            return Err(error);
        }
        entries.push(Err(error));
        return Ok((verifier, entries, true));
    }
    match data.get_u32() {
        0 => Ok((verifier, entries, false)),
        1 => Ok((verifier, entries, true)),
        value => {
            entries.push(Err(NfsError::Xdr(format!(
                "READDIR eof flag is {value}, expected 0 or 1"
            ))));
            Ok((verifier, entries, true))
        }
    }
}

fn decode_directory_entry(data: &mut Bytes, previous_cookie: u64) -> Result<DirectoryEntry> {
    if data.remaining() < 12 {
        return Err(NfsError::Xdr("READDIR entry truncated".into()));
    }
    let cookie = data.get_u64();
    if cookie == previous_cookie {
        return Err(NfsError::Xdr("READDIR entry did not advance cookie".into()));
    }
    let name_len = data.get_u32() as usize;
    let padded = name_len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("READDIR name length overflow".into()))?
        & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("READDIR entry name truncated".into()));
    }
    let name = String::from_utf8(data.split_to(name_len).to_vec())
        .map_err(|error| NfsError::Xdr(format!("READDIR entry name is not UTF-8: {error}")))?;
    data.advance(padded - name_len);
    let attr = decode_getattr_response(data)?;
    Ok(DirectoryEntry { cookie, name, attr })
}

fn validate_readdir_payload(payload: Bytes, maxcount: u32) -> Result<Bytes> {
    if payload.len() > maxcount as usize {
        return Err(NfsError::Xdr(format!(
            "READDIR payload {} exceeds requested maxcount {maxcount}",
            payload.len()
        )));
    }
    Ok(payload)
}

fn take_u64_attr(values: &mut Bytes, label: &str) -> Result<u64> {
    if values.remaining() < 8 {
        return Err(NfsError::Xdr(format!("{label} truncated")));
    }
    Ok(values.get_u64())
}

fn take_bitmap4_attr(values: &mut Bytes, label: &str) -> Result<Vec<u32>> {
    let word_count = take_u32_attr(values, label)? as usize;
    if values.remaining() < word_count.saturating_mul(4) {
        return Err(NfsError::Xdr(format!("{label} truncated")));
    }
    Ok((0..word_count).map(|_| values.get_u32()).collect())
}

fn pathconf_support(bitmap: &[u32]) -> mount::PathconfSupport {
    mount::PathconfSupport {
        linkmax: fattr4_has(bitmap, 28),
        name_max: fattr4_has(bitmap, 29),
        no_trunc: fattr4_has(bitmap, 34),
        chown_restricted: fattr4_has(bitmap, 18),
        case_insensitive: fattr4_has(bitmap, 16),
        case_preserving: fattr4_has(bitmap, 17),
    }
}

fn take_u32_attr(values: &mut Bytes, label: &str) -> Result<u32> {
    if values.remaining() < 4 {
        return Err(NfsError::Xdr(format!("{label} truncated")));
    }
    Ok(values.get_u32())
}

fn take_bool_attr(values: &mut Bytes, label: &str) -> Result<bool> {
    match take_u32_attr(values, label)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(NfsError::Xdr(format!(
            "{label} has invalid boolean value {value}"
        ))),
    }
}

fn ensure_attr_values_consumed(values: &Bytes, label: &str) -> Result<()> {
    if values.has_remaining() {
        return Err(NfsError::Xdr(format!(
            "{label} has trailing attribute values"
        )));
    }
    Ok(())
}

fn require_fattrs(bitmap: &[u32], required: &[u32], operation: &str) -> Result<()> {
    let missing = required
        .iter()
        .copied()
        .filter(|attr| !fattr4_has(bitmap, *attr))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(NfsError::Unsupported(format!(
            "NFSv4.0 {operation} server omitted required attributes {missing:?}"
        )));
    }
    Ok(())
}

fn unsupported<T>(operation: &str) -> Result<T> {
    Err(NfsError::Unsupported(format!(
        "NFSv4.0 {operation} is not implemented in the minimal mount slice"
    )))
}

fn classify_create_compound_error(
    response: &Bytes,
    class: OperationClass,
    ctx: RequestContext,
    error: NfsError,
) -> NfsError {
    if create_succeeded_before_compound_failure(response.clone()) {
        NfsError::OperationOutcome(Box::new(crate::error::OperationOutcomeError::new(
            crate::error::OperationOutcome::Uncertain,
            class,
            crate::error::RecoveryAction::VerifyThenResume,
            ctx,
            error,
        )))
    } else {
        classify_sent_nfs40_error(class, ctx, error)
    }
}

impl Mount40 {
    async fn activity_call(
        &self,
        request: Vec<u8>,
        replay: ReplayPolicy,
        timeout: Duration,
    ) -> Result<Bytes> {
        let response = call_with_busy_retry(
            &self.rpc,
            request,
            Bytes::new(),
            replay,
            timeout,
            RequestContext {
                operation: "compound".into(),
                protocol: NFSVersion::NFSv4p0,
                request_id: None,
            },
        )
        .await?;
        Ok(response)
    }

    async fn activity_settled_call(
        &self,
        request: Vec<u8>,
        class: OperationClass,
        context: RequestContext,
    ) -> Result<Bytes> {
        let response = settled_call(self.rpc.clone(), request, class, context).await?;
        Ok(response)
    }

    async fn set_encoded_attributes(
        &self,
        operation: &'static str,
        fh: &Bytes,
        bitmap: &[u32],
        values: &[u8],
        invalidate_cached_attributes: bool,
        needs_open: bool,
    ) -> Result<()> {
        let _io_guard = if let Some(callback_state) = &self.callback_state {
            Some(callback_state.foreground_io(fh).await)
        } else {
            None
        };
        if invalidate_cached_attributes && let Some(callback_state) = &self.callback_state {
            callback_state.mark_attributes_unknown(fh)?;
        }
        let lane = self.state.for_fh(fh, crate::OPEN_WRITE).await;
        // A truncate writes the truncated range, so like WRITE it keeps its open
        // lane out of recovery between the lost-lock recheck and the send: a
        // reclaim publishes the loss under the same lane, and this fence makes
        // the lock-then-check atomic with respect to that publish. Only size
        // changes pay for it; metadata-only SETATTR stays on the cheap path.
        let _truncate_in_flight = if needs_open {
            match &lane {
                // Release the lane before waiting on the fence: close and reclaim
                // take the fence first and the lane second.
                Some(lane) => {
                    let fence = Arc::clone(&lane.lock().await.io_fence);
                    Some(fence.read_owned().await)
                }
                None => None,
            }
        } else {
            None
        };
        let mut retries = 0u8;
        loop {
            let (stateid, expected_generation) = match &lane {
                Some(lane) => {
                    let guard = lane.lock().await;
                    if guard.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    // A truncate writes data, so it observes the lost-lock gate
                    // READ and WRITE honour even when a lane survived. Both the
                    // lookup above and this wait queue behind a reclaim that
                    // publishes a loss while holding the lane, so the answer is
                    // taken under that same lane, as READ and WRITE do.
                    if needs_open && let Some(lost) = self.locks.lost(fh).await {
                        return Err(lost_lock_error(operation, &lost));
                    }
                    (guard.stateid, Some(self.lease.begin_stateful(operation)?))
                }
                None => {
                    if needs_open {
                        if let Some(lost) = self.locks.lost(fh).await {
                            return Err(lost_lock_error(operation, &lost));
                        }
                        // Without an open lane the request would go out with the
                        // anonymous stateid while the mount holds no state.
                        self.lease.gate_stateful(operation)?;
                    }
                    ([0; 16], None)
                }
            };
            let request = CompoundBuilder::new(operation)
                .putfh(fh)
                .setattr(&stateid, bitmap, values)
                .encode_with_header(&self.auth);
            let (class, ctx) = context(operation, 0, 0, OperationClass::ReplaySensitive);
            let response = self
                .activity_settled_call(request, class, ctx.clone())
                .await?;
            let result = decode_setattr_response(response, bitmap)
                .map_err(|error| classify_sent_nfs40_error(class, ctx, error));
            match result {
                Ok(()) => {
                    if let Some(generation) = expected_generation {
                        self.lease.finish_stateful(generation, operation)?;
                    }
                    return Ok(());
                }
                Err(error) => {
                    let Some(lane) = &lane else {
                        return Err(error);
                    };
                    let (trigger, _) = classify_stateid_failure(operation, &mut retries, error)?;
                    self.refresh_lane_stateid(lane, stateid, trigger, operation)
                        .await
                        .map_err(|failure| recovery_failure(operation, trigger, failure))?;
                }
            }
        }
    }
}

impl Mount40 {
    async fn write_how(
        &self,
        fh: Bytes,
        offset: u64,
        data: Bytes,
        stability: mount::WriteStability,
    ) -> Result<mount::WriteOutcome> {
        if let Some(lost) = self.locks.lost(&fh).await {
            return Err(lost_lock_error("write", &lost));
        }
        let (count, committed, verifier) = self
            .write_with_stability(fh, offset, data, stability.stable_how())
            .await?;
        Ok(mount::WriteOutcome {
            pnfs: None,
            count,
            committed: crate::WriteCommitted::try_from(committed)?,
            verifier: Some(verifier),
        })
    }

    /// WRITE with the given `stable_how4`; returns `(count, committed, verifier)`.
    /// No COMMIT is issued here; batch commit is performed by the caller.
    async fn write_with_stability(
        &self,
        fh: Bytes,
        offset: u64,
        data: Bytes,
        stable: u32,
    ) -> Result<(u32, u32, [u8; 8])> {
        if data.len() > u32::MAX as usize {
            return Err(NfsError::InvalidInput("WRITE data exceeds u32::MAX".into()));
        }
        let lane = self
            .state
            .for_fh(&fh, crate::OPEN_WRITE)
            .await
            .ok_or_else(|| {
                // A lane can be gone because crash recovery dropped it while the
                // mount itself resumed, so lifecycle alone cannot tell a
                // programming error from lost open state. Always report the
                // typed failure so callers can reopen the file.
                NfsError::LostOpenState(format!(
                    "NFSv4.0 WRITE: no open state for this filehandle while mount is {:?}",
                    self.lease.health().lifecycle
                ))
            })?;
        let refresh_delegation_attributes = if let Some(callback_state) = &self.callback_state {
            callback_state.mark_attributes_unknown(&fh)?
        } else {
            false
        };
        let mut retries = 0u8;
        let (count, committed, verifier) = loop {
            let expected_generation = self.lease.begin_stateful("write")?;
            let io_guard = if let Some(callback_state) = &self.callback_state {
                Some(callback_state.foreground_io(&fh).await)
            } else {
                None
            };
            let rpc = self.rpc.clone();
            let auth = self.auth.clone();
            let lease = Arc::clone(&self.lease);
            let locks = Arc::clone(&self.locks);
            let lane_for_settlement = Arc::clone(&lane);
            let fh_for_settlement = fh.clone();
            let data_for_settlement = data.clone();
            let used = Arc::new(std::sync::Mutex::new(None));
            let used_in_task = Arc::clone(&used);
            let settled = DetachedSettlement::new(async move {
                let _io_guard = io_guard;
                // Acquire the fence before the lane: close and reclaim take the
                // fence first, and holding the lane across read_owned() would
                // invert that order and deadlock.
                let io_fence = {
                    let guard = lane_for_settlement.lock().await;
                    if guard.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    Arc::clone(&guard.io_fence)
                };
                let _in_flight = io_fence.read_owned().await;
                let (stateid, owner, next_seqid) = {
                    let guard = lane_for_settlement.lock().await;
                    if guard.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    // The waits above can span a whole reclaim. Recovery
                    // publishes a lost lock while it holds this lane, so
                    // rechecking here cannot miss it.
                    if let Some(lost) = locks.lost(&fh_for_settlement).await {
                        return Err(lost_lock_error("write", &lost));
                    }
                    (guard.stateid, guard.owner, guard.next_seqid)
                };
                *used_in_task
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(stateid);
                let request = CompoundBuilder::new("write")
                    .putfh(&fh_for_settlement)
                    .write_header(&stateid, offset, stable, data_for_settlement.len() as u32)
                    .encode_with_header(&auth);
                let (class, ctx) =
                    context("write", owner, next_seqid, OperationClass::ReplaySensitive);
                let response = call_with_busy_retry(
                    &rpc,
                    request,
                    data_for_settlement,
                    ReplayPolicy::ONE_ATTEMPT,
                    METADATA_TIMEOUT,
                    ctx.clone(),
                )
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
                let (count, committed, verifier) = decode_write_response(response)
                    .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
                lease.finish_stateful(expected_generation, "write")?;
                Ok::<_, NfsError>((count, committed, verifier))
            })
            .await;
            match settled {
                Ok(settled) => break settled,
                Err(error) => {
                    let (trigger, error) = classify_stateid_failure("write", &mut retries, error)?;
                    let recovered = used
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .ok_or_else(|| state_recovery_exhausted("write", trigger, error))?;
                    self.refresh_lane_stateid(&lane, recovered, trigger, "write")
                        .await
                        .map_err(|failure| recovery_failure("write", trigger, failure))?;
                }
            }
        };
        if count > data.len() as u32 {
            return Err(NfsError::Xdr("WRITE count exceeds request length".into()));
        }
        if refresh_delegation_attributes {
            self.refresh_callback_attributes(&fh).await;
        }
        Ok((count, committed, verifier))
    }
}

#[async_trait]
impl Mount for Mount40 {
    fn capabilities(&self) -> crate::MountCapabilities {
        crate::MountCapabilities {
            acl: self.acl_supported,
            posix_acl: false,
            named_attributes: false,
            locks: true,
            callbacks: true,
            delegation_retention: self.callback_state.is_some(),
            pnfs: false,
            session_diagnostics: false,
        }
    }

    fn health(&self) -> crate::MountHealth {
        let mut health = self.lease.health();
        health.callback_healthy = self.callback_state.as_ref().map(|state| state.healthy());
        health
    }

    async fn callback_stats(&self) -> crate::CallbackStats {
        self.callback_state
            .as_ref()
            .map_or_else(crate::CallbackStats::default, |state| state.stats())
    }
    fn get_max_read_size(&self) -> u32 {
        self.rsize
    }
    fn get_max_write_size(&self) -> u32 {
        self.wsize
    }
    fn version(&self) -> NFSVersion {
        NFSVersion::NFSv4p0
    }
    async fn getfh(&self) -> Bytes {
        self.root_fh.clone()
    }

    async fn null(&self) -> Result<()> {
        let mut request = Vec::new();
        crate::nfs3::rpc_header(100003, 4, 0, &self.auth).encode(&mut request);
        self.rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        self.lease.mark_closing();
        if let Some(renewal) = &self._renewal {
            renewal.stop().await;
        }
        if let Some(callback) = &self._callback {
            callback.stop().await;
        }
        let callback_cleanup = if let Some(callback_state) = &self.callback_state {
            callback_state.return_all_delegations().await
        } else {
            Ok(())
        };
        if let Some(worker) = &self._callback_worker {
            worker.stop().await;
        }
        self.rpc.shutdown().await;
        self.lease.mark_closed();
        callback_cleanup
    }

    async fn access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        const ACCESS4_ALL: u32 = 0x003f;
        if mode & !ACCESS4_ALL != 0 {
            return Err(NfsError::InvalidInput(format!(
                "NFSv4 ACCESS mask contains unknown bits: {mode:#x}"
            )));
        }
        let request = CompoundBuilder::new("access")
            .putfh(&fh)
            .access(mode)
            .encode_with_header(&self.auth);
        let response = self
            .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        let (_supported, granted) = decode_access_response(response)?;
        Ok(granted)
    }
    async fn open(&self, dir_fh: Bytes, filename: &str, access: u32) -> Result<mount::ObjRes> {
        Ok(self.open_stateful(dir_fh, filename, access).await?.object)
    }
    async fn open_path(&self, path: &str, access: u32) -> Result<mount::ObjRes> {
        Ok(self.open_path_stateful(path, access).await?.object)
    }
    async fn open_stateful(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
    ) -> Result<mount::OpenFile> {
        self.open_file(dir_fh, filename, access, false).await
    }
    async fn open_path_stateful(&self, path: &str, access: u32) -> Result<mount::OpenFile> {
        let (dir, name) = crate::split_path(path)?;
        let directory = self.lookup_path(&dir).await?;
        self.open_stateful(directory.fh, &name, access).await
    }
    async fn open_path_with_options(
        &self,
        path: &str,
        options: mount::OpenOptions,
    ) -> Result<mount::OpenFile> {
        mount::open_path_guarded(self, path, options, |parent, name| async move {
            self.open_guarded(parent, &name, options.access, options.create_mode)
                .await
        })
        .await
    }
    async fn close(&self, fh: Bytes) -> Result<()> {
        if self.locks.has_fh(&fh).await {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 CLOSE requires outstanding locks to be released".into(),
            ));
        }
        let lane = match self.state.for_fh(&fh, crate::OPEN_BOTH).await {
            Some(lane) => lane,
            // Recovery dropped this handle's open state, so there is nothing to
            // release on the server. CLOSE must still succeed: the handle may be
            // gated on a lock that died with the open, and with no lane there is
            // no other way to lift that gate.
            None if self.locks.lost(&fh).await.is_some() => {
                self.locks.clear_lost(&fh).await;
                return Ok(());
            }
            // A failed recovery already dropped the lane; the server cannot
            // hold state either, so CLOSE is satisfied instead of surfacing a
            // programming error.
            None if self.lease.health().lifecycle != MountLifecycleState::Ready => return Ok(()),
            None => {
                return Err(NfsError::InvalidInput(
                    "NFSv4.0 CLOSE requires an open file".into(),
                ));
            }
        };
        let result = self.close_lane(lane).await;
        if result.is_ok() && self.state.for_fh(&fh, crate::OPEN_BOTH).await.is_none() {
            self.locks.clear_lost(&fh).await;
        }
        result
    }
    async fn close_stateful(&self, file: mount::OpenFile) -> Result<()> {
        let (object, protocol_state) = file.into_parts();
        if self.locks.has_fh(&object.fh).await {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 CLOSE requires outstanding locks to be released".into(),
            ));
        }
        let state = protocol_state
            .ok_or_else(|| NfsError::InvalidInput("open file has no NFSv4.0 owner state".into()))?;
        let (issuer, owner) = decode_owner(&state)
            .ok_or_else(|| NfsError::InvalidInput("invalid NFSv4.0 open state".into()))?;
        if issuer != self.issuer {
            return Err(NfsError::InvalidInput(
                "open file belongs to another mount generation".into(),
            ));
        }
        let lane = match self.state.by_owner(owner).await {
            Some(lane) => lane,
            // Recovery dropped this handle's open lane, so the server holds no
            // state to release. CLOSE must still succeed when it was the last
            // open: the handle may be gated on a lock that died with the open,
            // and closing it is the only way to lift that gate.
            None if self.locks.lost(&object.fh).await.is_some()
                && self
                    .state
                    .for_fh(&object.fh, crate::OPEN_BOTH)
                    .await
                    .is_none() =>
            {
                self.locks.clear_lost(&object.fh).await;
                return Ok(());
            }
            None => {
                return Err(NfsError::InvalidInput(
                    "NFSv4.0 open state is closed or stale".into(),
                ));
            }
        };
        if lane.lock().await.fh != object.fh {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 open state filehandle mismatch".into(),
            ));
        }
        let result = self.close_lane(lane).await;
        if result.is_ok()
            && self
                .state
                .for_fh(&object.fh, crate::OPEN_BOTH)
                .await
                .is_none()
        {
            self.locks.clear_lost(&object.fh).await;
        }
        result
    }
    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
        self.commit_with_verifier(fh, offset, count)
            .await
            .map(|_| ())
    }
    async fn commit_with_verifier(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
    ) -> Result<Option<[u8; 8]>> {
        let lane = self.state.for_fh(&fh, crate::OPEN_WRITE).await;
        let expected_generation = if lane.is_some() {
            Some(self.lease.begin_stateful("commit")?)
        } else {
            None
        };
        let verifier = self.commit_verifier(&fh, offset, count).await?;
        if let Some(expected_generation) = expected_generation {
            self.lease
                .validate_stateful(expected_generation, "commit")?;
        }
        if let Some(lane) = lane {
            let mut lane = lane.lock().await;
            if lane
                .write_verifier
                .is_some_and(|expected| expected != verifier)
            {
                return Err(mount::write_verifier_changed(NFSVersion::NFSv4p0));
            }
            lane.write_verifier = None;
        }
        Ok(Some(verifier))
    }
    async fn create(
        &self,
        dir_fh: Bytes,
        filename: &str,
        mode: Option<u32>,
    ) -> Result<mount::ObjRes> {
        let opened = self
            .open_file(dir_fh.clone(), filename, crate::OPEN_BOTH, true)
            .await?;
        let object = opened.object;
        if let Some(mode) = mode
            && let Err(error) = self
                .setattr(
                    object.fh.clone(),
                    None,
                    Some(mode),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
        {
            let _ = self.close(object.fh.clone()).await;
            // OPEN uses UNCHECKED4, so success does not prove that this call
            // created the directory entry. Removing it here could delete an
            // object created concurrently by another client.
            return Err(error);
        }
        let attr = self.getattr(object.fh.clone()).await?;
        Ok(mount::ObjRes {
            fh: object.fh,
            attr: Some(attr),
        })
    }
    async fn create_path(&self, path: &str, mode: Option<u32>) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(path)?;
        let parent = self.lookup_path(&dir).await?;
        self.create(parent.fh, &name, mode).await
    }
    async fn create_path_stateful(&self, path: &str, mode: Option<u32>) -> Result<mount::OpenFile> {
        self.create_path_stateful_with_access(path, mode, crate::OPEN_BOTH)
            .await
    }
    async fn create_path_stateful_with_access(
        &self,
        path: &str,
        mode: Option<u32>,
        access: u32,
    ) -> Result<mount::OpenFile> {
        let (dir, name) = crate::split_path(path)?;
        let parent = self.lookup_path(&dir).await?;
        let opened = self.open_file(parent.fh, &name, access, true).await?;
        if let Some(mode) = mode
            && let Err(error) = self
                .setattr(
                    opened.file_handle(),
                    None,
                    Some(mode),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
        {
            let _ = self.close_stateful(opened).await;
            // UNCHECKED4 cannot distinguish a newly created target from one
            // that won a concurrent name-creation race. Never remove it as a
            // compensating action after SETATTR failure.
            return Err(error);
        }
        Ok(opened)
    }
    async fn fsinfo(&self) -> Result<mount::FSInfo> {
        let bitmap = [
            (1 << 5) | (1 << 6) | (1 << 15) | (1 << 26) | (1 << 27) | (1 << 30) | (1 << 31),
            1 << 19,
        ];
        let (bitmap, mut values) = self.query_fattrs(&self.root_fh, &bitmap, "fsinfo").await?;
        require_fattrs(&bitmap, &[5, 6, 15, 27, 30, 31, 51], "FSINFO")?;
        let link = take_bool_attr(&mut values, "link_support")?;
        let symlink = take_bool_attr(&mut values, "symlink_support")?;
        let cansettime = take_bool_attr(&mut values, "cansettime")?;
        let homogeneous = if fattr4_has(&bitmap, 26) {
            take_bool_attr(&mut values, "homogeneous")?
        } else {
            false
        };
        let maxfilesize = take_u64_attr(&mut values, "maxfilesize")?;
        let maxread = take_u64_attr(&mut values, "maxread")?;
        let maxwrite = take_u64_attr(&mut values, "maxwrite")?;
        if values.remaining() < 12 {
            return Err(NfsError::Xdr("time_delta truncated".into()));
        }
        let seconds = values.get_i64();
        if !(0..=u32::MAX as i64).contains(&seconds) {
            return Err(NfsError::Xdr("time_delta seconds out of range".into()));
        }
        let time_delta = crate::Time {
            seconds: seconds as u32,
            nseconds: values.get_u32(),
        };
        ensure_attr_values_consumed(&values, "fsinfo")?;
        let properties = u32::from(link)
            | (u32::from(symlink) << 1)
            | (u32::from(homogeneous) << 3)
            | (u32::from(cansettime) << 4);
        Ok(mount::FSInfo {
            attr: None,
            rtmax: maxread.min(self.rsize as u64) as u32,
            rtpref: maxread.min(self.rsize as u64) as u32,
            rtmult: 1,
            wtmax: maxwrite.min(self.wsize as u64) as u32,
            wtpref: maxwrite.min(self.wsize as u64) as u32,
            wtmult: 1,
            dtpref: 0,
            maxfilesize,
            time_delta,
            properties,
        })
    }
    async fn fsstat(&self) -> Result<mount::FSStat> {
        let bitmap = [
            (1 << 21) | (1 << 22) | (1 << 23),
            (1 << 10) | (1 << 11) | (1 << 12),
        ];
        let (bitmap, mut values) = self.query_fattrs(&self.root_fh, &bitmap, "fsstat").await?;
        require_fattrs(&bitmap, &[21, 22, 23, 42, 43, 44], "FSSTAT")?;
        let mut result = mount::FSStat::default();
        for (attr, target) in [
            (21, &mut result.afiles),
            (22, &mut result.ffiles),
            (23, &mut result.tfiles),
            (42, &mut result.abytes),
            (43, &mut result.fbytes),
            (44, &mut result.tbytes),
        ] {
            if fattr4_has(&bitmap, attr) {
                *target = take_u64_attr(&mut values, "fsstat value")?;
            }
        }
        ensure_attr_values_consumed(&values, "fsstat")?;
        Ok(result)
    }
    async fn getattr(&self, fh: Bytes) -> Result<mount::Attr> {
        let bitmap = standard_getattr_bitmap();
        let request = CompoundBuilder::new("getattr")
            .putfh(&fh)
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let response = self
            .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        let mut data = decode_getattr_compound(response)?;
        decode_getattr_response(&mut data)
    }
    async fn getacl(&self, fh: Bytes) -> Result<mount::Acl> {
        let bitmap = [1 << 12];
        let request = CompoundBuilder::new("getacl")
            .putfh(&fh)
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let result = async {
            let response = self
                .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                .await?;
            let mut data = decode_getattr_compound(response)?;
            acl::decode_getattr_acl(&mut data)
        }
        .await;
        acl::attrnotsupp_as_unsupported("GETACL", result)
    }
    async fn setacl(&self, fh: Bytes, acl_value: &mount::Acl) -> Result<()> {
        let (bitmap, values) = acl::encode_setattr_acl(acl_value);
        let result = self
            .set_encoded_attributes("setacl", &fh, &bitmap, &values, false, false)
            .await;
        acl::attrnotsupp_as_unsupported("SETACL", result)
    }
    async fn aclsupport(&self, fh: Bytes) -> Result<mount::AclSupport> {
        let bitmap = [1 << 13];
        let request = CompoundBuilder::new("aclsupport")
            .putfh(&fh)
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let response = self
            .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        let mut data = decode_getattr_compound(response)?;
        acl::decode_getattr_aclsupport(&mut data)
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
        if guard_ctime.is_some() {
            return Err(NfsError::Unsupported(
                "NFSv4.0 guarded SETATTR is not representable by RFC 7530 SETATTR".into(),
            ));
        }
        let (bitmap, values) = encode_setattr(mode, uid, gid, size, atime, mtime);
        if bitmap.is_empty() {
            return Ok(());
        }
        // A size change writes the truncated range and therefore needs the open
        // gate when no lane exists; metadata-only changes stay allowed, since
        // chmod/utimes legitimately address the file without an open.
        self.set_encoded_attributes("setattr", &fh, &bitmap, &values, true, size.is_some())
            .await?;
        self.refresh_callback_attributes(&fh).await;
        Ok(())
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
        let object = self.lookup_path(path).await?;
        let guard = if specify_guard {
            Some(self.getattr(object.fh.clone()).await?.ctime)
        } else {
            None
        };
        self.setattr(object.fh, guard, mode, uid, gid, size, atime, mtime)
            .await
    }
    async fn lock(&self, fh: Bytes, lock_type: u32, offset: u64, length: u64) -> Result<Bytes> {
        if !matches!(lock_type, 1 | 2) || length == 0 {
            return Err(NfsError::InvalidInput(
                "LOCK requires type 1/2 and non-zero length".into(),
            ));
        }
        let access = if lock_type == 1 {
            crate::OPEN_READ
        } else {
            crate::OPEN_WRITE
        };
        let open_lane = self.state.for_fh(&fh, access).await.ok_or_else(|| {
            NfsError::InvalidInput("NFSv4.0 LOCK requires a compatible open file".into())
        })?;
        Ok(self
            .acquire_lock(open_lane, fh, lock_type, offset, length)
            .await?
            .0)
    }
    async fn lock_test(&self, fh: Bytes, lock_type: u32, offset: u64, length: u64) -> Result<()> {
        if !matches!(lock_type, 1 | 2) || length == 0 {
            return Err(NfsError::InvalidInput(
                "LOCKT requires type 1/2 and non-zero length".into(),
            ));
        }
        let owner = self.next_owner.fetch_add(1, Ordering::Relaxed);
        let owner_wire = format!("nfs-rs-lockt-{:016x}-{owner:016x}", self.issuer);
        let request = CompoundBuilder::new("lockt")
            .putfh(&fh)
            .lockt(
                lock_type,
                offset,
                length,
                self.client_id.load(Ordering::Acquire),
                owner_wire.as_bytes(),
            )
            .encode_with_header(&self.auth);
        decode_lockt_response(
            self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                .await?,
        )
    }
    async fn lock_stateful(
        &self,
        fh: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<crate::LockToken> {
        if !matches!(lock_type, 1 | 2) || length == 0 {
            return Err(NfsError::InvalidInput(
                "LOCK requires type 1/2 and non-zero length".into(),
            ));
        }
        let access = if lock_type == 1 {
            crate::OPEN_READ
        } else {
            crate::OPEN_WRITE
        };
        let open_lane = self.state.for_fh(&fh, access).await.ok_or_else(|| {
            NfsError::InvalidInput("NFSv4.0 LOCK requires a compatible open file".into())
        })?;
        let (stateid, generation) = self
            .acquire_lock(open_lane, fh.clone(), lock_type, offset, length)
            .await?;
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
    async fn lock_open_stateful(
        &self,
        opened: &mount::OpenFile,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<crate::LockToken> {
        if !matches!(lock_type, 1 | 2) || length == 0 {
            return Err(NfsError::InvalidInput(
                "LOCK requires type 1/2 and non-zero length".into(),
            ));
        }
        let (issuer, owner) = opened
            .protocol_state()
            .and_then(decode_owner)
            .ok_or_else(|| {
                NfsError::InvalidInput("open token has no NFSv4.0 owner state".into())
            })?;
        if issuer != self.issuer {
            return Err(NfsError::InvalidInput(
                "open token belongs to another mount generation".into(),
            ));
        }
        let lane = self.state.by_owner(owner).await.ok_or_else(|| {
            NfsError::InvalidInput("open token is stale or already closed".into())
        })?;
        let (stateid, generation) = self
            .acquire_lock(lane, opened.object.fh.clone(), lock_type, offset, length)
            .await?;
        Ok(crate::LockToken::new(
            opened.object.fh.clone(),
            stateid,
            lock_type,
            offset,
            length,
            self.issuer,
            generation,
        ))
    }
    async fn locku(
        &self,
        fh: Bytes,
        lock_stateid: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<()> {
        self.lease.begin_teardown("locku")?;
        if lock_stateid.len() != 16 {
            return Err(NfsError::InvalidInput(
                "lock_stateid must be 16 bytes".into(),
            ));
        }
        let mut stateid = [0; 16];
        stateid.copy_from_slice(&lock_stateid);
        let lane = self.locks.by_stateid(&stateid).await.ok_or_else(|| {
            NfsError::InvalidInput("NFSv4.0 lock state is unknown or already released".into())
        })?;
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let locks = Arc::clone(&self.locks);
        let lease = Arc::clone(&self.lease);
        let client_id = self.client_id.load(Ordering::Acquire);
        tokio::spawn(async move {
            let mut lane = lane.lock().await;
            if lane.fh != fh
                || lane.lock_type != lock_type
                || lane.offset != offset
                || lane.length != length
            {
                return Err(NfsError::InvalidInput(
                    "LOCKU parameters do not match the acquired lock".into(),
                ));
            }
            let original_stateid = lane.stateid;
            let owner = lane.owner;
            let owner_wire = lane.owner_wire.clone();
            let (mut class, mut ctx) = context(
                "locku",
                lane.owner,
                lane.next_seqid,
                OperationClass::ReplaySensitive,
            );
            let request = CompoundBuilder::new("locku")
                .putfh(&fh)
                .locku(lock_type, lane.next_seqid, &lane.stateid, offset, length)
                .encode_with_header(&auth);
            let (mut decoded, consumed) = decode_lock_call(
                rpc.call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                    .await,
                14,
                "LOCKU",
            );
            if consumed {
                lane.next_seqid = lane.next_seqid.wrapping_add(1);
            }
            // RFC 7530 §9.1.4.4: OLD_STATEID does not disclose the server's
            // current lock seqid, so a blind +1 retry cannot converge when the
            // server advanced by more than one and would leave the lane holding
            // a stateid the server never issued. Report the exhaustion and keep
            // the server-issued stateid untouched; LOCKU has no dir_fh/name to
            // re-open with, so reopening is the caller's decision.
            // RFC 7530 §9.1.4.4: BAD_STATEID is also returned when the stateid
            // seqid is later than the server's, in which case the lock still
            // exists. Probe once with the previous stateid seqid before
            // treating the lock as released; a genuine loss just repeats the
            // status, and BAD_STATEID does not consume the lock-owner seqid.
            if let Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID)) = &decoded {
                let seqid = u32::from_be_bytes([
                    lane.stateid[0],
                    lane.stateid[1],
                    lane.stateid[2],
                    lane.stateid[3],
                ]);
                if let Some(previous) = seqid.checked_sub(1) {
                    let mut probe_stateid = lane.stateid;
                    probe_stateid[..4].copy_from_slice(&previous.to_be_bytes());
                    let request = CompoundBuilder::new("locku")
                        .putfh(&fh)
                        .locku(lock_type, lane.next_seqid, &probe_stateid, offset, length)
                        .encode_with_header(&auth);
                    (class, ctx) = context(
                        "locku",
                        lane.owner,
                        lane.next_seqid,
                        OperationClass::ReplaySensitive,
                    );
                    let (retried, retry_consumed) = decode_lock_call(
                        rpc.call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                            .await,
                        14,
                        "LOCKU",
                    );
                    decoded = retried;
                    if retry_consumed {
                        lane.next_seqid = lane.next_seqid.wrapping_add(1);
                    }
                }
            }
            let decoded = decoded.map_err(|error| classify_sent_nfs40_error(class, ctx, error));
            match decoded {
                Ok(_) => {
                    // Retire the lane before dropping it so a concurrent
                    // reclaim cannot see it as current and re-establish a lock
                    // the caller just released.
                    lane.stateid = [0; 16];
                    drop(lane);
                    locks.remove(&original_stateid).await;
                    let cleanup = CompoundBuilder::new("release-lockowner")
                        .release_lockowner(client_id, &owner_wire)
                        .encode_with_header(&auth);
                    let (class, ctx) = context(
                        "release_lockowner",
                        owner,
                        0,
                        OperationClass::ReplaySensitive,
                    );
                    // Best effort: the LOCKU reply is authoritative and the
                    // lane is already retired. A stale client id or transport
                    // failure here must not turn a successful unlock into an
                    // error the caller would retry.
                    let result = async {
                        let response = rpc
                            .call(cleanup, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                            .await
                            .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
                        decode_release_lockowner_response(response)
                            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::debug!(
                            target: "nfs_rs::stateid",
                            operation = "locku",
                            owner = owner,
                            error = %error,
                            "NFSv4.0 RELEASE_LOCKOWNER failed after LOCKU; lock state is already released"
                        );
                    }
                    lease.record_activity();
                    Ok(())
                }
                Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID)) => {
                    // Still behind after one retry: the lock state exists with an
                    // even newer seqid. Keep the lane so later attempts converge.
                    Err(state_recovery_exhausted(
                        "locku",
                        StateidRecovery::Old,
                        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID),
                    ))
                }
                // RFC 7530 §9.1.4.3: BAD/STALE mean the lock state no longer
                // exists server-side, so the unlock is already satisfied. Drop
                // the lane instead of wedging it on a stateid that can never be
                // accepted again.
                Err(error) if error.is_recoverable_state() => {
                    tracing::debug!(
                        target: "nfs_rs::stateid",
                        operation = "locku",
                        owner = lane.owner,
                        error = %error,
                        "NFSv4.0 LOCKU found no server lock state; treating it as released"
                    );
                    // Same released sentinel as the success branch: the
                    // registry entry is removed just after this point, and a
                    // reclaim must not observe it as current in between.
                    lane.stateid = [0; 16];
                    drop(lane);
                    locks.remove(&original_stateid).await;
                    // The lock state is gone but the server may still hold the
                    // lock-owner record. Best-effort release so it does not live
                    // until lease expiry.
                    let cleanup = CompoundBuilder::new("release-lockowner")
                        .release_lockowner(client_id, &owner_wire)
                        .encode_with_header(&auth);
                    if let Err(cleanup_error) = rpc
                        .call(cleanup, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                        .await
                        .and_then(decode_release_lockowner_response)
                    {
                        tracing::debug!(
                            target: "nfs_rs::stateid",
                            operation = "locku",
                            error = %cleanup_error,
                            "NFSv4.0 RELEASE_LOCKOWNER after a released lock failed; ignoring"
                        );
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 unlock task failed: {error}")))??;
        Ok(())
    }
    async fn release_locks(&self, fh: Bytes) -> Result<()> {
        // A cancelled LOCK keeps settling in its detached task and registers
        // its lane only after the reply arrives. Wait for every acquisition
        // issued before this cleanup, otherwise the snapshot below misses the
        // grant and the following CLOSE collides with it (`LOCKS_HELD`).
        self.locks.wait_for_acquisitions(&fh).await;
        let lanes = self.locks.for_fh(&fh).await;
        let mut failure = None;
        for lane in lanes {
            let (stateid, lock_type, offset, length) = {
                let lane = lane.lock().await;
                (lane.stateid, lane.lock_type, lane.offset, lane.length)
            };
            // Skip lanes that a concurrent LOCKU/recovery already retired. Do
            // not use a zero stateid as the retirement marker: a broken server
            // can return the all-zero stateid for a LOCK, and skipping that
            // lane would leave it registered, so `close` would reject the fh
            // forever. A live lane is still the registry entry for its stateid;
            // releasing it (even with zero) lets the server answer and the lane
            // is removed normally.
            if !self.locks.is_current(&stateid, &lane).await {
                continue;
            }
            if let Err(error) = self
                .locku(
                    fh.clone(),
                    Bytes::copy_from_slice(&stateid),
                    lock_type,
                    offset,
                    length,
                )
                .await
            {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn unlock_stateful(&self, token: crate::LockToken) -> Result<()> {
        if token.issuer != self.issuer || token.generation != self.lease.generation() {
            return Err(NfsError::InvalidInput(
                "lock token belongs to another mount generation".into(),
            ));
        }
        self.locku(
            token.fh,
            token.stateid,
            token.lock_type,
            token.offset,
            token.length,
        )
        .await
    }
    async fn link(
        &self,
        src_fh: Bytes,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<mount::Attr> {
        let request = CompoundBuilder::new("link")
            .putfh(&src_fh)
            .savefh()
            .putfh(&dst_dir_fh)
            .link(dst_filename)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("link", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        decode_link_response(response)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
        self.getattr(src_fh).await
    }
    async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<mount::Attr> {
        let source = self.lookup_path(src_path).await?;
        let (dir, name) = crate::split_path(dst_path)?;
        let parent = self.lookup_path(&dir).await?;
        self.link(source.fh, parent.fh, &name).await
    }
    async fn symlink_path(&self, src_path: &str, dst_path: &str) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(dst_path)?;
        let parent = self.lookup_path(&dir).await?;
        self.symlink(src_path, parent.fh, &name).await
    }
    async fn symlink(
        &self,
        src_path: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<mount::ObjRes> {
        let bitmap = standard_getattr_bitmap();
        let request = CompoundBuilder::new("symlink")
            .putfh(&dst_dir_fh)
            .create_symlink(dst_filename, src_path)
            .getfh()
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("symlink", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        let (fh, mut data) = decode_create_response(response.clone())
            .map_err(|error| classify_create_compound_error(&response, class, ctx, error))?;
        Ok(mount::ObjRes {
            fh,
            attr: Some(decode_getattr_response(&mut data)?),
        })
    }
    async fn readlink(&self, fh: Bytes) -> Result<String> {
        let request = CompoundBuilder::new("readlink")
            .putfh(&fh)
            .readlink()
            .encode_with_header(&self.auth);
        decode_readlink_response(
            self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                .await?,
        )
    }
    async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<mount::ObjRes> {
        let bitmap = standard_getattr_bitmap();
        let request = CompoundBuilder::new("lookup")
            .putfh(&dir_fh)
            .lookup(filename)
            .getfh()
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let response = self
            .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        let (fh, mut data) = decode_lookup_getattr_response(response)?;
        Ok(mount::ObjRes {
            fh,
            attr: Some(decode_getattr_response(&mut data)?),
        })
    }
    async fn lookup_path(&self, path: &str) -> Result<mount::ObjRes> {
        let mut object = mount::ObjRes {
            fh: self.root_fh.clone(),
            attr: None,
        };
        for component in path.split('/').filter(|part| !part.is_empty()) {
            object = self.lookup(object.fh, component).await?;
        }
        Ok(object)
    }
    async fn pathconf(&self, fh: Bytes) -> Result<mount::Pathconf> {
        let fh = if fh.is_empty() { &self.root_fh } else { &fh };
        Ok(self.query_pathconf(fh).await?.values)
    }

    async fn pathconf_with_support(&self, fh: Bytes) -> Result<mount::SupportedPathconf> {
        let fh = if fh.is_empty() { &self.root_fh } else { &fh };
        self.query_pathconf(fh).await
    }
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        if count == 0 {
            return Ok(Bytes::new());
        }
        if let Some(lost) = self.locks.lost(&fh).await {
            return Err(lost_lock_error("read", &lost));
        }
        let lane = self.state.for_fh(&fh, crate::OPEN_READ).await;
        let Some(lane) = lane else {
            // `for_fh` waits on every lane mutex it visits, and recovery can
            // publish a loss while holding one of those lanes, so recheck the
            // gate after the wait instead of trusting the entry check.
            if let Some(lost) = self.locks.lost(&fh).await {
                return Err(lost_lock_error("read", &lost));
            }
            // An anonymous-stateid READ is legal (RFC 7530 §9.1.4.3), but only
            // while the mount still holds its state. After state loss the
            // caller's locks are gone and this handle has no lane left to gate
            // at dispatch, so serving bytes here would hide the loss and could
            // read a range another client now protects with an advisory lock
            // (servers enforce only share reservations and mandatory locks).
            self.lease.gate_stateful("read")?;
            let request = CompoundBuilder::new("read")
                .putfh(&fh)
                .read(&[0; 16], offset, count)
                .encode_with_header(&self.auth);
            return decode_read_response(
                self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                    .await?,
            );
        };
        let mut retries = 0u8;
        loop {
            let expected_generation = self.lease.begin_stateful("read")?;
            let io_guard = if let Some(callback_state) = &self.callback_state {
                Some(callback_state.foreground_io(&fh).await)
            } else {
                None
            };
            let rpc = self.rpc.clone();
            let auth = self.auth.clone();
            let lease = Arc::clone(&self.lease);
            let locks = Arc::clone(&self.locks);
            let lane_for_settlement = Arc::clone(&lane);
            let fh_for_settlement = fh.clone();
            let used = Arc::new(std::sync::Mutex::new(None));
            let used_in_task = Arc::clone(&used);
            let settled = DetachedSettlement::new(async move {
                let _callback_io = io_guard;
                // Same fence-before-lane order as close and reclaim.
                let io_fence = {
                    let guard = lane_for_settlement.lock().await;
                    if guard.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    Arc::clone(&guard.io_fence)
                };
                let _in_flight = io_fence.read_owned().await;
                let stateid = {
                    let guard = lane_for_settlement.lock().await;
                    if guard.closing {
                        return Err(NfsError::InvalidInput(
                            "NFSv4.0 open state is closing or closed".into(),
                        ));
                    }
                    // The waits above can span a whole reclaim. Recovery
                    // publishes a lost lock while it holds this lane, so
                    // rechecking here cannot miss it.
                    if let Some(lost) = locks.lost(&fh_for_settlement).await {
                        return Err(lost_lock_error("read", &lost));
                    }
                    guard.stateid
                };
                *used_in_task
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(stateid);
                let request = CompoundBuilder::new("read")
                    .putfh(&fh_for_settlement)
                    .read(&stateid, offset, count)
                    .encode_with_header(&auth);
                let data = decode_read_response(
                    call_with_busy_retry(
                        &rpc,
                        request,
                        Bytes::new(),
                        SAFE_REPLAY,
                        METADATA_TIMEOUT,
                        RequestContext {
                            operation: "read".into(),
                            protocol: NFSVersion::NFSv4p0,
                            request_id: None,
                        },
                    )
                    .await?,
                )?;
                lease.finish_stateful(expected_generation, "read")?;
                Ok::<_, NfsError>(data)
            })
            .await;
            match settled {
                Ok(data) => return Ok(data),
                Err(error) => {
                    let (trigger, error) = classify_stateid_failure("read", &mut retries, error)?;
                    let recovered = used
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .ok_or_else(|| state_recovery_exhausted("read", trigger, error))?;
                    self.refresh_lane_stateid(&lane, recovered, trigger, "read")
                        .await
                        .map_err(|failure| recovery_failure("read", trigger, failure))?;
                }
            }
        }
    }
    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<mount::WriteOutcome> {
        self.write_how(fh, offset, data, mount::WriteStability::Unstable)
            .await
    }

    async fn readdir(&self, dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        Box::pin(
            stream::try_unfold(Some((dir_fh, 0, [0; 8])), move |state| async move {
                let Some((fh, cookie, verifier)) = state else {
                    return Ok(None);
                };
                let (entries, last_cookie, verifier, eof) =
                    self.readdir_page(&fh, cookie, &verifier).await?;
                if entries.is_empty() && !eof && last_cookie == cookie {
                    return Err(NfsError::Xdr("READDIR page made no progress".into()));
                }
                let next = (!eof).then_some((fh, last_cookie, verifier));
                Ok(Some((stream::iter(entries), next)))
            })
            .try_flatten(),
        )
    }
    async fn readdirplus(&self, dir_fh: Bytes) -> mount::ReaddirplusStream<'_> {
        Box::pin(
            stream::try_unfold(Some((dir_fh, 0, [0; 8])), move |state| async move {
                let Some((fh, cookie, verifier)) = state else {
                    return Ok(None);
                };
                let (entries, last_cookie, verifier, eof) =
                    self.fetch_readdirplus_page(&fh, cookie, &verifier).await?;
                if entries.is_empty() && !eof && last_cookie == cookie {
                    return Err(NfsError::Xdr("READDIRPLUS page made no progress".into()));
                }
                let next = (!eof).then_some((fh, last_cookie, verifier));
                Ok(Some((stream::iter(entries), next)))
            })
            .try_flatten(),
        )
    }
    async fn readdirplus_page(
        &self,
        dir_fh: Bytes,
        position: mount::DirectoryCookie,
    ) -> Result<mount::ReaddirplusPage> {
        let (entries, last_cookie, verifier, eof) = self
            .fetch_readdirplus_page(&dir_fh, position.cookie, &position.verifier)
            .await?;
        let received = entries.len();
        let entries = entries.into_iter().collect::<Result<Vec<_>>>()?;
        let next =
            mount::DirectoryCursor::resume(position).page(last_cookie, verifier, received, eof)?;
        Ok(mount::ReaddirplusPage { entries, next, eof })
    }
    async fn mkdir(&self, dir_fh: Bytes, dirname: &str, mode: u32) -> Result<mount::ObjRes> {
        let bitmap = standard_getattr_bitmap();
        let request = CompoundBuilder::new("mkdir")
            .putfh(&dir_fh)
            .create_directory(dirname)
            .getfh()
            .getattr(&bitmap)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("mkdir", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        let (fh, mut data) = decode_create_response(response.clone())
            .map_err(|error| classify_create_compound_error(&response, class, ctx, error))?;
        let mut object = mount::ObjRes {
            fh: fh.clone(),
            attr: Some(decode_getattr_response(&mut data)?),
        };
        if let Err(error) = self
            .setattr(fh, None, Some(mode), None, None, None, None, None)
            .await
        {
            let _ = self.remove(dir_fh, dirname).await;
            return Err(error);
        }
        if let Some(attr) = object.attr.as_mut() {
            attr.file_mode = mode;
        }
        Ok(object)
    }
    async fn mkdir_path(&self, path: &str, mode: u32) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(path)?;
        let parent = self.lookup_path(&dir).await?;
        self.mkdir(parent.fh, &name, mode).await
    }
    async fn remove(&self, dir_fh: Bytes, filename: &str) -> Result<()> {
        let request = CompoundBuilder::new("remove")
            .putfh(&dir_fh)
            .remove(filename)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("remove", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        decode_remove_response(response)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
    }
    async fn remove_path(&self, path: &str) -> Result<()> {
        let (dir, name) = crate::split_path(path)?;
        let parent = self.lookup_path(&dir).await?;
        self.remove(parent.fh, &name).await
    }
    async fn rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()> {
        self.remove(dir_fh, dirname).await
    }
    async fn rmdir_path(&self, path: &str) -> Result<()> {
        self.remove_path(path).await
    }
    async fn rename_path(&self, from_path: &str, to_path: &str) -> Result<()> {
        let (from_dir, from_name) = crate::split_path(from_path)?;
        let (to_dir, to_name) = crate::split_path(to_path)?;
        let from_parent = self.lookup_path(&from_dir).await?;
        let to_parent = self.lookup_path(&to_dir).await?;
        self.rename(from_parent.fh, &from_name, to_parent.fh, &to_name)
            .await
    }
    async fn rename(
        &self,
        from_dir_fh: Bytes,
        from_filename: &str,
        to_dir_fh: Bytes,
        to_filename: &str,
    ) -> Result<()> {
        let request = CompoundBuilder::new("rename")
            .putfh(&from_dir_fh)
            .savefh()
            .putfh(&to_dir_fh)
            .rename(from_filename, to_filename)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("rename", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        decode_rename_response(response)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
    }
    async fn exports(&self) -> Result<Vec<mount::ExportEntry>> {
        unsupported("EXPORTS")
    }
}

#[cfg(test)]
mod tests;

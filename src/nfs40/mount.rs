use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream;
use futures::stream::TryStreamExt as _;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::callback::CallbackService;
use super::compound::{
    CallbackAddress, CompoundBuilder, NewLockArgs, OpenArgs, OpenReclaimArgs, SetClientIdArgs,
    create_succeeded_before_compound_failure, decode_access_response, decode_commit_response,
    decode_confirm_response, decode_create_response,
    decode_getattr_response as decode_getattr_compound, decode_link_response, decode_lock_response,
    decode_lockt_response, decode_lookup_getattr_response, decode_open_response,
    decode_read_response, decode_readdir_response, decode_readlink_response,
    decode_release_lockowner_response, decode_remove_response, decode_rename_response,
    decode_setattr_response, decode_setclientid_response, decode_stateid_response,
    decode_write_response, open_succeeded_before_compound_failure,
};
use super::lease::{LeaseRenewal, LeaseState, RecoveryHandler};
use super::state::{LockLane, LockState, OpenState, OwnerLane, decode_owner, encode_owner};
use crate::error::{NfsError, OperationClass, RequestContext, Result, classify_sent_nfs40_error};
use crate::mount::{self, NFSVersion};
use crate::nfs4::attrs::{
    decode_fattr4_envelope, decode_getattr_response, encode_setattr, fattr4_has,
    standard_getattr_bitmap,
};
use crate::nfs4::compound::decode_navigation_response;
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};
use crate::{Mount, MountArgs};

const NFS4_DEFAULT_PORT: u16 = 2049;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const SAFE_REPLAY: ReplayPolicy = ReplayPolicy::byte_identical(2);

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
    dircount: u32,
    maxcount: u32,
    rsize: u32,
    wsize: u32,
}

#[derive(Clone)]
struct ClientIdentity {
    verifier: [u8; 8],
    owner: String,
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
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(format!("{}:{port}", args.host))
        .await
        .map_err(NfsError::Io)?
        .collect();
    let auth = Auth::new_unix("nfs-rs", args.uid, args.gid);
    let mut last_error = None;
    for addr in addrs {
        match mount_on_addr(addr, args, auth.clone()).await {
            Ok(mount) => return Ok(Box::new(mount)),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| NfsError::Rpc("NFSv4.0 resolved no addresses".to_string())))
}

async fn mount_on_addr(addr: SocketAddr, args: &MountArgs, auth: Auth) -> Result<Mount40> {
    let mux = rpc::StreamMux::connect(addr, args.noresvport).await?;
    let rpc = rpc::Client::new(mux, None);
    let callback = if args.retain_delegations {
        Some(CallbackService::bind_for(addr).await?)
    } else {
        None
    };
    let callback_addr = callback
        .as_ref()
        .map(|service| service.universal_addr().to_string());
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
    let lease_time = query_lease_time(&rpc, &auth, &root_fh).await?;
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
    let recovery: RecoveryHandler = Arc::new(move || {
        let context = recovery_context.clone();
        Box::pin(async move { context.recover_or_lose().await })
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
        dircount: args.dircount,
        maxcount: args.maxcount,
        rsize: args.rsize,
        wsize: args.wsize,
    })
}

async fn query_lease_time(rpc: &rpc::Client, auth: &Auth, root_fh: &Bytes) -> Result<u32> {
    let response = rpc
        .call(
            CompoundBuilder::new("lease-time")
                .putfh(root_fh)
                .getattr(&[1 << 10])
                .encode_with_header(auth),
            SAFE_REPLAY,
            METADATA_TIMEOUT,
        )
        .await?;
    let mut attrs = decode_getattr_compound(response)?;
    let (bitmap, mut values) = decode_fattr4_envelope(&mut attrs, "lease_time")?;
    if !fattr4_has(&bitmap, 10) || values.remaining() != 4 {
        return Err(NfsError::Xdr(
            "NFSv4.0 lease_time response is missing or malformed".into(),
        ));
    }
    let seconds = values.get_u32();
    if seconds == 0 {
        return Err(NfsError::Xdr("NFSv4.0 lease_time is zero".into()));
    }
    Ok(seconds)
}

async fn establish_identity(
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
struct RecoveryContext {
    rpc: rpc::Client,
    auth: Auth,
    identity: ClientIdentity,
    client_id: Arc<AtomicU64>,
    issuer: u64,
    state: Arc<OpenState>,
    locks: Arc<LockState>,
    lease: Arc<LeaseState>,
    callback_addr: Option<String>,
}

impl RecoveryContext {
    async fn recover(&self) -> Result<()> {
        let renewed_client_id = establish_identity(
            &self.rpc,
            &self.auth,
            &self.identity,
            self.callback_addr.as_deref(),
        )
        .await?;
        self.client_id.store(renewed_client_id, Ordering::Release);
        self.lease.mark_reclaiming();

        stream::iter(
            self.state
                .snapshot()
                .await
                .into_iter()
                .map(Ok::<_, NfsError>),
        )
        .try_for_each_concurrent(None, |lane| async move {
            let mut open = lane.lock().await;
            let owner_wire = format!("nfs-rs-{:016x}-{:016x}", self.issuer, open.owner);
            let response = self
                .rpc
                .call(
                    CompoundBuilder::new("reclaim-open")
                        .putfh(&open.fh)
                        .open_reclaim(OpenReclaimArgs {
                            seqid: 0,
                            share_access: open.access,
                            client_id: renewed_client_id,
                            owner: owner_wire.as_bytes(),
                        })
                        .getfh()
                        .encode_with_header(&self.auth),
                    ReplayPolicy::ONE_ATTEMPT,
                    METADATA_TIMEOUT,
                )
                .await?;
            let (reclaimed, fh) = decode_open_response(response)?;
            if reclaimed.confirm_required || fh != open.fh {
                return Err(NfsError::Xdr(
                    "NFSv4.0 reclaim OPEN returned incompatible state".into(),
                ));
            }
            open.stateid = reclaimed.stateid;
            open.next_seqid = 1;
            Ok(())
        })
        .await?;

        let mut lock_groups = BTreeMap::<u64, Vec<_>>::new();
        for lane in self.locks.snapshot().await {
            let open_owner = lane.lock().await.open_owner;
            lock_groups.entry(open_owner).or_default().push(lane);
        }
        stream::iter(lock_groups.into_values().map(Ok::<_, NfsError>))
            .try_for_each_concurrent(None, |lanes| async move {
                for lane in lanes {
                    let (open_owner, old_stateid) = {
                        let lock = lane.lock().await;
                        (lock.open_owner, lock.stateid)
                    };
                    let open_lane = self.state.by_owner(open_owner).await.ok_or_else(|| {
                        NfsError::Xdr("NFSv4.0 reclaim LOCK lost its open-owner".into())
                    })?;
                    let mut open = open_lane.lock().await;
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
                                    client_id: renewed_client_id,
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
                    drop(lock);
                    self.locks
                        .rekey(old_stateid, new_stateid, Arc::clone(&lane))
                        .await;
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn recover_or_lose(&self) -> Result<()> {
        match self.recover().await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.lease.mark_lost().await;
                self.state.clear().await;
                self.locks.clear().await;
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

fn consumes_owner_seqid(result: &Result<impl Sized>) -> bool {
    match result {
        Ok(_) | Err(NfsError::LockDenied { .. }) => true,
        Err(NfsError::Nfs4(status)) => !matches!(
            *status as u32,
            10022 | 10023 | 10025 | 10026 | 10036 | 10018 | 10020 | 10019
        ),
        Err(_) => false,
    }
}

async fn settled_call(
    rpc: rpc::Client,
    request: Vec<u8>,
    class: OperationClass,
    ctx: RequestContext,
) -> Result<Bytes> {
    tokio::spawn(async move {
        rpc.call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
    })
    .await
    .map_err(|error| NfsError::Rpc(format!("NFSv4.0 settlement task failed: {error}")))?
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
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let client_id = self.client_id.load(Ordering::Acquire);
        let locks = Arc::clone(&self.locks);
        let lease = Arc::clone(&self.lease);
        let stateid = tokio::spawn(async move {
            let mut open = open_lane.lock().await;
            let open_owner = open.owner;
            let request = CompoundBuilder::new("lock")
                .putfh(&fh)
                .lock_new(NewLockArgs {
                    lock_type,
                    reclaim: false,
                    offset,
                    length,
                    open_seqid: open.next_seqid,
                    open_stateid: &open.stateid,
                    lock_seqid: 0,
                    client_id,
                    owner: &owner_wire,
                })
                .encode_with_header(&auth);
            let (class, ctx) = context("lock", owner, 0, OperationClass::ReplaySensitive);
            let response = rpc
                .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            let decoded = decode_lock_response(response, 12, "LOCK");
            if consumes_owner_seqid(&decoded) {
                open.next_seqid = open.next_seqid.wrapping_add(1);
            }
            let stateid = decoded.map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
            let _publication = lease.publication_guard().await;
            lease.finish_stateful(expected_generation, "lock")?;
            locks
                .register(LockLane {
                    owner,
                    open_owner,
                    owner_wire,
                    next_seqid: 1,
                    stateid,
                    fh,
                    lock_type,
                    offset,
                    length,
                })
                .await;
            if let Err(error) = lease.validate_stateful(expected_generation, "lock") {
                locks.remove(&stateid).await;
                return Err(error);
            }
            Ok::<_, NfsError>(stateid)
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 lock task failed: {error}")))??;
        Ok((Bytes::copy_from_slice(&stateid), expected_generation))
    }

    async fn open_file(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
        create: bool,
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
        let opened = tokio::spawn(async move {
            let request = CompoundBuilder::new(if create { "create" } else { "open" })
                .putfh(&dir_fh)
                .open(OpenArgs {
                    seqid: 0,
                    share_access: access,
                    client_id,
                    owner: owner_wire.as_bytes(),
                    filename: &filename,
                    create,
                })
                .getfh()
                .encode_with_header(&auth);
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
            let _publication = lease.publication_guard().await;
            lease.finish_stateful(expected_generation, "open")?;
            state
                .register(OwnerLane {
                    owner,
                    next_seqid,
                    stateid,
                    fh: fh.clone(),
                    access,
                    write_verifier: None,
                })
                .await;
            if let Err(error) = lease.validate_stateful(expected_generation, "open") {
                state.remove(owner, &fh).await;
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
        tokio::spawn(async move {
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
            let response = rpc
                .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            lane.stateid = decode_stateid_response(response, 4, "CLOSE")
                .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
            lane.next_seqid = lane.next_seqid.wrapping_add(1);
            let owner = lane.owner;
            let fh = lane.fh.clone();
            drop(lane);
            state.remove(owner, &fh).await;
            Ok::<_, NfsError>(())
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 close task failed: {error}")))??;
        self.lease.finish_stateful(expected_generation, "close")?;
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
        let payload = decode_readdir_response(
            self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
                .await?,
        )?;
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

    async fn readdirplus_page(
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
                    entry.map(|entry| mount::ReaddirplusEntry {
                        fileid: entry.attr.fileid,
                        file_name: entry.name,
                        handle: entry.attr.filehandle.clone(),
                        attr: Some(entry.attr),
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

fn verifier_changed_error() -> NfsError {
    NfsError::OperationOutcome(Box::new(crate::error::OperationOutcomeError::new(
        crate::error::OperationOutcome::Uncertain,
        OperationClass::ReplaySensitive,
        crate::error::RecoveryAction::VerifyThenResume,
        RequestContext {
            operation: "write_verifier".into(),
            protocol: NFSVersion::NFSv4p0,
            request_id: None,
        },
        NfsError::Rpc("NFSv4.0 WRITE verifier changed before COMMIT".into()),
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
        let response = self.rpc.call(request, replay, timeout).await?;
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
}

#[async_trait]
impl Mount for Mount40 {
    fn health(&self) -> crate::MountHealth {
        self.lease.health()
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
        self.activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        self.lease.mark_closing();
        if let Some(renewal) = &self._renewal {
            renewal.stop().await;
        }
        self.rpc.shutdown().await;
        self.lease.mark_closed();
        Ok(())
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
    async fn close(&self, fh: Bytes) -> Result<()> {
        if self.locks.has_fh(&fh).await {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 CLOSE requires outstanding locks to be released".into(),
            ));
        }
        let lane = self
            .state
            .for_fh(&fh, crate::OPEN_BOTH)
            .await
            .ok_or_else(|| NfsError::InvalidInput("NFSv4.0 CLOSE requires an open file".into()))?;
        self.close_lane(lane).await
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
        let lane = self.state.by_owner(owner).await.ok_or_else(|| {
            NfsError::InvalidInput("NFSv4.0 open state is closed or stale".into())
        })?;
        if lane.lock().await.fh != object.fh {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 open state filehandle mismatch".into(),
            ));
        }
        self.close_lane(lane).await
    }
    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
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
                return Err(verifier_changed_error());
            }
            lane.write_verifier = None;
        }
        Ok(())
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
            let _ = self.remove(dir_fh, filename).await;
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
        let lane = self.state.for_fh(&fh, crate::OPEN_WRITE).await;
        let (stateid, expected_generation) = match lane {
            Some(lane) => (
                lane.lock().await.stateid,
                Some(self.lease.begin_stateful("setattr")?),
            ),
            None => ([0; 16], None),
        };
        let request = CompoundBuilder::new("setattr")
            .putfh(&fh)
            .setattr(&stateid, &bitmap, &values)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("setattr", 0, 0, OperationClass::ReplaySensitive);
        let response = self
            .activity_settled_call(request, class, ctx.clone())
            .await?;
        decode_setattr_response(response, &bitmap)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
        if let Some(generation) = expected_generation {
            self.lease.finish_stateful(generation, "setattr")?;
        }
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
        let expected_generation = self.lease.begin_stateful("locku")?;
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
            let request = CompoundBuilder::new("locku")
                .putfh(&fh)
                .locku(lock_type, lane.next_seqid, &lane.stateid, offset, length)
                .encode_with_header(&auth);
            let (class, ctx) = context(
                "locku",
                lane.owner,
                lane.next_seqid,
                OperationClass::ReplaySensitive,
            );
            let response = rpc
                .call(request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            let old_stateid = lane.stateid;
            let owner = lane.owner;
            let owner_wire = lane.owner_wire.clone();
            let decoded = decode_lock_response(response, 14, "LOCKU");
            if consumes_owner_seqid(&decoded) {
                lane.next_seqid = lane.next_seqid.wrapping_add(1);
            }
            lane.stateid = decoded.map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
            drop(lane);
            locks.remove(&old_stateid).await;
            let cleanup = CompoundBuilder::new("release-lockowner")
                .release_lockowner(client_id, &owner_wire)
                .encode_with_header(&auth);
            let (class, ctx) = context(
                "release_lockowner",
                owner,
                0,
                OperationClass::ReplaySensitive,
            );
            let response = rpc
                .call(cleanup, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            decode_release_lockowner_response(response)
                .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
            Ok(())
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 unlock task failed: {error}")))??;
        self.lease.finish_stateful(expected_generation, "locku")?;
        Ok(())
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
        let requested = [
            (1 << 16) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
            1 << 2,
        ];
        let (bitmap, mut values) = self.query_fattrs(fh, &requested, "pathconf").await?;
        require_fattrs(&bitmap, &[16, 17, 18, 28, 29, 34], "PATHCONF")?;
        let result = mount::Pathconf {
            attr: None,
            case_insensitive: take_bool_attr(&mut values, "case_insensitive")?,
            case_preserving: take_bool_attr(&mut values, "case_preserving")?,
            chown_restricted: take_bool_attr(&mut values, "chown_restricted")?,
            linkmax: take_u32_attr(&mut values, "maxlink")?,
            name_max: take_u32_attr(&mut values, "maxname")?,
            no_trunc: take_bool_attr(&mut values, "no_trunc")?,
        };
        ensure_attr_values_consumed(&values, "pathconf")?;
        Ok(result)
    }
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        let lane = self.state.for_fh(&fh, crate::OPEN_READ).await;
        let (stateid, expected_generation) = if let Some(lane) = lane {
            let generation = self.lease.begin_stateful("read")?;
            (lane.lock().await.stateid, Some(generation))
        } else {
            ([0; 16], None)
        };
        let request = CompoundBuilder::new("read")
            .putfh(&fh)
            .read(&stateid, offset, count)
            .encode_with_header(&self.auth);
        let response = self
            .activity_call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        let data = decode_read_response(response)?;
        if let Some(generation) = expected_generation {
            self.lease.finish_stateful(generation, "read")?;
        }
        Ok(data)
    }
    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        let expected_generation = self.lease.begin_stateful("write")?;
        if data.len() > u32::MAX as usize {
            return Err(NfsError::InvalidInput("WRITE data exceeds u32::MAX".into()));
        }
        let lane = self
            .state
            .for_fh(&fh, crate::OPEN_WRITE)
            .await
            .ok_or_else(|| NfsError::InvalidInput("NFSv4.0 WRITE requires an open file".into()))?;
        let rpc = self.rpc.clone();
        let auth = self.auth.clone();
        let lane_for_settlement = Arc::clone(&lane);
        let fh_for_settlement = fh.clone();
        let settled = tokio::spawn(async move {
            // The lane guard spans request construction through reply decoding.
            // CLOSE takes the same guard, so cancellation cannot reorder CLOSE
            // ahead of a detached WRITE settlement.
            let guard = lane_for_settlement.lock().await;
            let request = CompoundBuilder::new("write")
                .putfh(&fh_for_settlement)
                .write_header(&guard.stateid, offset, 2, data.len() as u32)
                .encode_with_header(&auth);
            let (class, ctx) = context(
                "write",
                guard.owner,
                guard.next_seqid,
                OperationClass::ReplaySensitive,
            );
            let response = rpc
                .call_with_data(
                    request,
                    data.clone(),
                    ReplayPolicy::ONE_ATTEMPT,
                    METADATA_TIMEOUT,
                )
                .await
                .map_err(|error| classify_sent_nfs40_error(class, ctx.clone(), error))?;
            let (count, committed, verifier) = decode_write_response(response)
                .map_err(|error| classify_sent_nfs40_error(class, ctx, error))?;
            if committed != 2 {
                let commit_request = CompoundBuilder::new("commit")
                    .putfh(&fh_for_settlement)
                    .commit(offset, count)
                    .encode_with_header(&auth);
                let (commit_class, commit_ctx) =
                    context("commit", guard.owner, 0, OperationClass::ReplaySensitive);
                let commit_response = rpc
                    .call(commit_request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
                    .await
                    .map_err(|error| {
                        classify_sent_nfs40_error(commit_class, commit_ctx.clone(), error)
                    })?;
                let committed_verifier = decode_commit_response(commit_response)
                    .map_err(|error| classify_sent_nfs40_error(commit_class, commit_ctx, error))?;
                if committed_verifier != verifier {
                    return Err(verifier_changed_error());
                }
            }
            drop(guard);
            Ok::<_, NfsError>((count, data))
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 write task failed: {error}")))??;
        let (count, data) = settled;
        if count > data.len() as u32 {
            return Err(NfsError::Xdr("WRITE count exceeds request length".into()));
        }
        self.lease.finish_stateful(expected_generation, "write")?;
        Ok(count)
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
                    self.readdirplus_page(&fh, cookie, &verifier).await?;
                if entries.is_empty() && !eof && last_cookie == cookie {
                    return Err(NfsError::Xdr("READDIRPLUS page made no progress".into()));
                }
                let next = (!eof).then_some((fh, last_cookie, verifier));
                Ok(Some((stream::iter(entries), next)))
            })
            .try_flatten(),
        )
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
mod tests {
    use super::*;
    use crate::nfs4::compound::xdr_opaque;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Notify, oneshot};

    async fn read_record(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let marker = stream.read_u32().await?;
        let mut record = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut record).await?;
        Ok(record)
    }

    fn compound_result(tag: &str, ops: &[(u32, &[u8])]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&0u32.to_be_bytes());
        xdr_opaque(&mut result, tag.as_bytes());
        result.extend_from_slice(&(ops.len() as u32).to_be_bytes());
        for (opcode, data) in ops {
            result.extend_from_slice(&opcode.to_be_bytes());
            result.extend_from_slice(&0u32.to_be_bytes());
            result.extend_from_slice(data);
        }
        result
    }

    fn denied_lock_result(tag: &str, opcode: u32) -> Vec<u8> {
        let mut denied = Vec::new();
        denied.extend_from_slice(&7u64.to_be_bytes());
        denied.extend_from_slice(&11u64.to_be_bytes());
        denied.extend_from_slice(&2u32.to_be_bytes());
        denied.extend_from_slice(&99u64.to_be_bytes());
        xdr_opaque(&mut denied, b"holder");
        let mut result = Vec::new();
        result.extend_from_slice(&10010u32.to_be_bytes());
        xdr_opaque(&mut result, tag.as_bytes());
        result.extend_from_slice(&2u32.to_be_bytes());
        result.extend_from_slice(&22u32.to_be_bytes());
        result.extend_from_slice(&0u32.to_be_bytes());
        result.extend_from_slice(&opcode.to_be_bytes());
        result.extend_from_slice(&10010u32.to_be_bytes());
        result.extend_from_slice(&denied);
        result
    }

    fn failed_lock_result(tag: &str, opcode: u32, status: u32) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&status.to_be_bytes());
        xdr_opaque(&mut result, tag.as_bytes());
        result.extend_from_slice(&2u32.to_be_bytes());
        result.extend_from_slice(&22u32.to_be_bytes());
        result.extend_from_slice(&0u32.to_be_bytes());
        result.extend_from_slice(&opcode.to_be_bytes());
        result.extend_from_slice(&status.to_be_bytes());
        result
    }

    async fn reply(stream: &mut TcpStream, request: &[u8], payload: &[u8]) -> std::io::Result<()> {
        let xid = u32::from_be_bytes(request[0..4].try_into().unwrap());
        let mut response = Vec::new();
        response.extend_from_slice(&xid.to_be_bytes());
        response.extend_from_slice(&1u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(payload);
        stream
            .write_u32(0x8000_0000 | response.len() as u32)
            .await?;
        stream.write_all(&response).await
    }

    fn direct_mount(rpc: rpc::Client) -> Arc<Mount40> {
        Arc::new(Mount40 {
            rpc,
            auth: Auth::new_null(),
            root_fh: Bytes::from_static(b"root"),
            client_id: Arc::new(AtomicU64::new(7)),
            issuer: 9,
            next_owner: AtomicU64::new(1),
            state: Arc::new(OpenState::default()),
            locks: Arc::new(LockState::default()),
            lease: LeaseState::ready(1, 60),
            _renewal: None,
            _callback: None,
            dircount: 8192,
            maxcount: 32768,
            rsize: 1_048_576,
            wsize: 1_048_576,
        })
    }

    fn direct_mount_with_renewal(rpc: rpc::Client, interval: Duration) -> Arc<Mount40> {
        direct_mount_with_lease_renewal(rpc, interval, 60)
    }

    fn direct_mount_with_lease_renewal(
        rpc: rpc::Client,
        interval: Duration,
        lease_seconds: u32,
    ) -> Arc<Mount40> {
        direct_mount_with_recovery(rpc, interval, lease_seconds, None)
    }

    fn direct_mount_with_recovery(
        rpc: rpc::Client,
        interval: Duration,
        lease_seconds: u32,
        recovery: Option<RecoveryHandler>,
    ) -> Arc<Mount40> {
        let lease = LeaseState::ready(1, lease_seconds);
        let reconnect_lease = Arc::clone(&lease);
        rpc.set_reconnect_handler(move |_client, _generation| {
            let lease = Arc::clone(&reconnect_lease);
            async move {
                lease.mark_reconnecting();
                Ok(())
            }
        })
        .unwrap();
        let renewal = LeaseRenewal::start(
            rpc.clone(),
            Auth::new_null(),
            Arc::new(AtomicU64::new(7)),
            interval,
            Arc::clone(&lease),
            recovery,
        );
        Arc::new(Mount40 {
            rpc,
            auth: Auth::new_null(),
            root_fh: Bytes::from_static(b"root"),
            client_id: Arc::new(AtomicU64::new(7)),
            issuer: 9,
            next_owner: AtomicU64::new(1),
            state: Arc::new(OpenState::default()),
            locks: Arc::new(LockState::default()),
            lease,
            _renewal: Some(renewal),
            _callback: None,
            dircount: 8192,
            maxcount: 32768,
            rsize: 1_048_576,
            wsize: 1_048_576,
        })
    }

    fn open_result(flags: u32, stateid: [u8; 16], fh: &[u8]) -> Vec<u8> {
        let mut open = Vec::new();
        open.extend_from_slice(&stateid);
        open.extend_from_slice(&[0; 20]);
        open.extend_from_slice(&flags.to_be_bytes());
        open.extend_from_slice(&0u32.to_be_bytes());
        open.extend_from_slice(&0u32.to_be_bytes());
        let mut getfh = Vec::new();
        xdr_opaque(&mut getfh, fh);
        compound_result("open", &[(22, &[]), (18, &open), (10, &getfh)])
    }

    async fn connected_direct_mount(listener: &TcpListener) -> Arc<Mount40> {
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        direct_mount(rpc::Client::new(mux, None))
    }

    #[tokio::test]
    async fn public_health_reports_successful_background_renewal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount =
            direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
        });
        server.await.unwrap().unwrap();
        for _ in 0..20 {
            let health = mount.health();
            if health.lease_renewals == 1 {
                assert_eq!(health.lifecycle, crate::MountLifecycleState::Ready);
                assert_eq!(health.generation, 1);
                assert_eq!(health.lease_seconds, Some(60));
                assert_eq!(health.lease_healthy, Some(true));
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("successful RENEW was not reflected in public health");
    }

    #[tokio::test]
    async fn renewal_reconnects_with_byte_identical_request_and_preserves_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount =
            direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await?;
            let original = read_record(&mut first).await?;
            socket2::SockRef::from(&first).set_linger(Some(Duration::ZERO))?;
            drop(first);

            let (mut second, _) = listener.accept().await?;
            let replay = read_record(&mut second).await?;
            assert_eq!(&original[4..], &replay[4..]);
            reply(
                &mut second,
                &replay,
                &compound_result("renew", &[(30, &[])]),
            )
            .await
        });
        server.await.unwrap().unwrap();
        for _ in 0..100 {
            let health = mount.health();
            if health.lease_renewals == 1 {
                assert_eq!(health.lifecycle, crate::MountLifecycleState::Ready);
                assert_eq!(health.generation, 1);
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("replayed RENEW did not restore healthy lease state");
    }

    #[tokio::test]
    async fn grace_renewal_is_suspect_until_a_later_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount =
            direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
        let (grace_tx, grace_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            let mut grace = Vec::new();
            grace.extend_from_slice(&10013u32.to_be_bytes());
            xdr_opaque(&mut grace, b"renew");
            grace.extend_from_slice(&1u32.to_be_bytes());
            grace.extend_from_slice(&30u32.to_be_bytes());
            grace.extend_from_slice(&10013u32.to_be_bytes());
            reply(&mut stream, &first, &grace).await?;
            let _ = grace_tx.send(());
            let _ = continue_rx.await;
            let second = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &second,
                &compound_result("renew", &[(30, &[])]),
            )
            .await
        });
        grace_rx.await.unwrap();
        for _ in 0..100 {
            if mount.health().lifecycle == crate::MountLifecycleState::Suspect {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            mount.health().lifecycle,
            crate::MountLifecycleState::Suspect
        );
        assert_eq!(mount.health().generation, 1);
        let _ = continue_tx.send(());
        server.await.unwrap().unwrap();
        for _ in 0..100 {
            if mount.health().lease_renewals == 1 {
                assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("successful RENEW did not clear GRACE suspicion");
    }

    #[tokio::test]
    async fn transient_renewal_failures_lose_state_after_the_lease_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount = direct_mount_with_lease_renewal(
            rpc::Client::new(mux, None),
            Duration::from_millis(20),
            1,
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            while let Ok(request) = read_record(&mut stream).await {
                if reply(
                    &mut stream,
                    &request,
                    &failed_lock_result("renew", 30, 10013),
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Ok::<(), std::io::Error>(())
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transient renewal failures never expired the lease");
        assert_eq!(mount.health().generation, 2);
        mount.umount().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn in_flight_renewal_is_fenced_at_deadline_but_still_settles() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount = direct_mount_with_lease_renewal(
            rpc::Client::new(mux, None),
            Duration::from_millis(10),
            1,
        );
        let (sent_tx, sent_rx) = oneshot::channel();
        let (settle_tx, settle_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let _ = sent_tx.send(());
            let _ = settle_rx.await;
            reply(
                &mut stream,
                &request,
                &compound_result("renew", &[(30, &[])]),
            )
            .await
        });
        sent_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("in-flight RENEW prevented conservative lease expiry");
        assert_eq!(mount.health().generation, 2);
        let _ = settle_tx.send(());
        mount.umount().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn successful_stateid_activity_extends_an_in_flight_renew_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount = direct_mount_with_lease_renewal(
            rpc::Client::new(mux, None),
            Duration::from_millis(10),
            1,
        );
        register_scripted_open(&mount, 81, b"fh").await;
        let (renew_sent_tx, renew_sent_rx) = oneshot::channel();
        let (settle_tx, settle_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            let _ = renew_sent_tx.send(());
            let read = read_record(&mut stream).await?;
            let mut read_data = Vec::new();
            read_data.extend_from_slice(&1u32.to_be_bytes());
            xdr_opaque(&mut read_data, b"x");
            reply(
                &mut stream,
                &read,
                &compound_result("read", &[(22, &[]), (25, &read_data)]),
            )
            .await?;
            let _ = settle_rx.await;
            reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
        });
        renew_sent_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            mount.read(Bytes::from_static(b"fh"), 0, 1).await.unwrap(),
            Bytes::from_static(b"x")
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_ne!(
            mount.health().lifecycle,
            crate::MountLifecycleState::LostState,
            "successful foreground activity did not extend the lease deadline"
        );
        let _ = settle_tx.send(());
        mount.umount().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn successful_stateless_activity_does_not_extend_the_lease_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount = direct_mount_with_lease_renewal(
            rpc::Client::new(mux, None),
            Duration::from_millis(10),
            1,
        );
        let (renew_sent_tx, renew_sent_rx) = oneshot::channel();
        let (settle_tx, settle_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            let _ = renew_sent_tx.send(());
            let null = read_record(&mut stream).await?;
            reply(&mut stream, &null, &[]).await?;
            let _ = settle_rx.await;
            reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
        });
        renew_sent_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        mount.null().await.unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stateless NULL incorrectly extended the lease deadline");
        let _ = settle_tx.send(());
        mount.umount().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn expired_renewal_loses_generation_and_gates_stateful_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mount =
            direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
        register_scripted_open(&mount, 82, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            let mut result = Vec::new();
            result.extend_from_slice(&10011u32.to_be_bytes());
            xdr_opaque(&mut result, b"renew");
            result.extend_from_slice(&1u32.to_be_bytes());
            result.extend_from_slice(&30u32.to_be_bytes());
            result.extend_from_slice(&10011u32.to_be_bytes());
            reply(&mut stream, &renew, &result).await
        });
        server.await.unwrap().unwrap();
        for _ in 0..100 {
            let health = mount.health();
            if health.lease_healthy == Some(false) {
                assert_eq!(health.lifecycle, crate::MountLifecycleState::LostState);
                assert_eq!(health.generation, 2);
                let error = mount
                    .read(Bytes::from_static(b"fh"), 0, 1)
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.operation_outcome().unwrap().recovery,
                    crate::RecoveryAction::Reopen
                );
                let stale = crate::LockToken::new(
                    Bytes::from_static(b"fh"),
                    Bytes::from_static(&[0x31; 16]),
                    2,
                    0,
                    1,
                    mount.issuer,
                    1,
                );
                assert!(mount.unlock_stateful(stale).await.is_err());
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("expired RENEW was not reflected in public health");
    }

    #[tokio::test]
    async fn stale_clientid_renewal_runs_recovery_and_preserves_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let recoveries = Arc::new(AtomicU64::new(0));
        let recovery_count = Arc::clone(&recoveries);
        let recovery: RecoveryHandler = Arc::new(move || {
            let recovery_count = Arc::clone(&recovery_count);
            Box::pin(async move {
                recovery_count.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
        });
        let mount = direct_mount_with_recovery(
            rpc::Client::new(mux, None),
            Duration::from_millis(10),
            60,
            Some(recovery),
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            reply(&mut stream, &renew, &failed_lock_result("renew", 30, 10022)).await
        });
        server.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if recoveries.load(Ordering::Acquire) == 1
                    && mount.health().lifecycle == crate::MountLifecycleState::Ready
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(mount.health().generation, 1);
    }

    #[tokio::test]
    async fn late_lock_result_cannot_publish_across_a_generation_fence() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 83, b"fh").await;
        let (seen_tx, seen_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let _ = seen_tx.send(());
            let _ = reply_rx.await;
            reply(
                &mut stream,
                &request,
                &compound_result("lock", &[(22, &[]), (12, &[0x8a; 16])]),
            )
            .await
        });
        let for_lock = Arc::clone(&mount);
        let lock = tokio::spawn(async move {
            for_lock
                .lock_stateful(Bytes::from_static(b"fh"), 2, 0, 1)
                .await
        });
        seen_rx.await.unwrap();
        mount.lease.mark_lost().await;
        let _ = reply_tx.send(());
        let error = lock.await.unwrap().unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().recovery,
            crate::RecoveryAction::Reopen
        );
        assert!(mount.locks.snapshot().await.is_empty());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn public_umount_stops_renewal_and_reports_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let accepted = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        mount.umount().await.unwrap();
        let health = mount.health();
        assert_eq!(health.lifecycle, crate::MountLifecycleState::Closed);
        assert_eq!(health.lease_healthy, Some(false));
        assert!(mount.read(Bytes::from_static(b"fh"), 0, 1).await.is_err());
        accepted.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelled_umount_keeps_a_sent_reclaim_settling() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let rpc = rpc::Client::new(mux, None);
        let auth = Auth::new_null();
        let client_id = Arc::new(AtomicU64::new(7));
        let lease = LeaseState::ready(1, 60);
        let state = Arc::new(OpenState::default());
        state
            .register(OwnerLane {
                owner: 75,
                next_seqid: 1,
                stateid: [0x75; 16],
                fh: Bytes::from_static(b"fh"),
                access: crate::OPEN_BOTH,
                write_verifier: None,
            })
            .await;
        let locks = Arc::new(LockState::default());
        let context = RecoveryContext {
            rpc: rpc.clone(),
            auth: auth.clone(),
            identity: ClientIdentity {
                verifier: [0x5a; 8],
                owner: "shutdown-reclaim".into(),
            },
            client_id: Arc::clone(&client_id),
            issuer: 9,
            state: Arc::clone(&state),
            locks: Arc::clone(&locks),
            lease: Arc::clone(&lease),
            callback_addr: None,
        };
        let recovery: RecoveryHandler = Arc::new(move || {
            let context = context.clone();
            Box::pin(async move { context.recover_or_lose().await })
        });
        let renewal = LeaseRenewal::start(
            rpc.clone(),
            auth.clone(),
            Arc::clone(&client_id),
            Duration::from_millis(10),
            Arc::clone(&lease),
            Some(recovery),
        );
        let mount = Arc::new(Mount40 {
            rpc,
            auth,
            root_fh: Bytes::from_static(b"root"),
            client_id,
            issuer: 9,
            next_owner: AtomicU64::new(1),
            state,
            locks,
            lease,
            _renewal: Some(renewal),
            _callback: None,
            dircount: 8192,
            maxcount: 32768,
            rsize: 1_048_576,
            wsize: 1_048_576,
        });
        let (reclaim_seen_tx, reclaim_seen_rx) = oneshot::channel();
        let (release_reclaim_tx, release_reclaim_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let renew = read_record(&mut stream).await?;
            reply(&mut stream, &renew, &failed_lock_result("renew", 30, 10011)).await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&23u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x7a; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim = read_record(&mut stream).await?;
            let _ = reclaim_seen_tx.send(());
            let _ = release_reclaim_rx.await;
            reply(&mut stream, &reclaim, &open_result(0, [0x85; 16], b"fh")).await
        });

        reclaim_seen_rx.await.unwrap();
        let for_umount = Arc::clone(&mount);
        let mut umount = tokio::spawn(async move { for_umount.umount().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut umount)
                .await
                .is_err(),
            "umount completed while the sent reclaim was unsettled"
        );
        umount.abort();
        let _ = umount.await;
        let _ = release_reclaim_tx.send(());
        server.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if mount.state.by_owner(75).await.unwrap().lock().await.stateid == [0x85; 16] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled umount abandoned the sent reclaim settlement");
        mount.umount().await.unwrap();
        assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Closed);
    }

    #[tokio::test]
    async fn scripted_recovery_reclaims_open_before_lock() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 71, b"fh").await;
        mount
            .locks
            .register(LockLane {
                owner: 72,
                open_owner: 71,
                owner_wire: Bytes::from_static(b"lock-owner"),
                next_seqid: 1,
                stateid: [0x72; 16],
                fh: Bytes::from_static(b"fh"),
                lock_type: 2,
                offset: 7,
                length: 11,
            })
            .await;
        let original_lock_token = crate::LockToken::new(
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x72; 16]),
            2,
            7,
            11,
            mount.issuer,
            mount.lease.generation(),
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&17u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x77; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim_open = read_record(&mut stream).await?;
            assert!(
                reclaim_open
                    .windows(4)
                    .any(|wire| wire == 18u32.to_be_bytes())
            );
            reply(
                &mut stream,
                &reclaim_open,
                &open_result(0, [0x81; 16], b"fh"),
            )
            .await?;
            let reclaim_lock = read_record(&mut stream).await?;
            assert!(
                reclaim_lock
                    .windows(8)
                    .any(|wire| { wire == [2u32.to_be_bytes(), 1u32.to_be_bytes()].concat() })
            );
            reply(
                &mut stream,
                &reclaim_lock,
                &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x82; 16])]),
            )
            .await?;
            let locku = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &locku,
                &compound_result("locku", &[(22, &[]), (14, &[0x83; 16])]),
            )
            .await?;
            let release = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &release,
                &compound_result("release-lockowner", &[(39, &[])]),
            )
            .await
        });
        let identity = ClientIdentity {
            verifier: [0x55; 8],
            owner: "scripted-recovery".into(),
        };
        mount.lease.mark_recovering();
        RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity,
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        }
        .recover()
        .await
        .unwrap();
        assert_eq!(mount.client_id.load(Ordering::Acquire), 17);
        assert_eq!(
            mount.state.by_owner(71).await.unwrap().lock().await.stateid,
            [0x81; 16]
        );
        assert!(mount.locks.by_stateid(&[0x72; 16]).await.is_some());
        assert!(mount.locks.by_stateid(&[0x82; 16]).await.is_some());
        mount.lease.mark_ready();
        mount.unlock_stateful(original_lock_token).await.unwrap();
        assert!(mount.locks.by_stateid(&[0x72; 16]).await.is_none());
        assert!(mount.locks.by_stateid(&[0x82; 16]).await.is_none());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn recovery_reclaims_independent_open_and_lock_owners_concurrently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 76, b"fh-a").await;
        register_scripted_open(&mount, 77, b"fh-b").await;
        register_scripted_lock(&mount, 76, [0x76; 16]).await;
        register_scripted_lock(&mount, 77, [0x77; 16]).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&29u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x7b; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;

            // Reading both before replying proves that one owner's pending OPEN
            // does not prevent another independent owner from being reclaimed.
            let first = read_record(&mut stream).await?;
            let second = tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .expect("second owner reclaim was globally serialized")?;
            reply(&mut stream, &first, &open_result(0, [0x86; 16], b"fh-a")).await?;
            reply(&mut stream, &second, &open_result(0, [0x87; 16], b"fh-b")).await?;

            let first_lock = read_record(&mut stream).await?;
            let second_lock =
                tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                    .await
                    .expect("second owner LOCK reclaim was globally serialized")?;
            reply(
                &mut stream,
                &first_lock,
                &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x96; 16])]),
            )
            .await?;
            reply(
                &mut stream,
                &second_lock,
                &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x97; 16])]),
            )
            .await
        });
        let context = RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity: ClientIdentity {
                verifier: [0x5b; 8],
                owner: "concurrent-reclaim".into(),
            },
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        };
        context.recover().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn recovery_serializes_locks_that_share_an_open_owner() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 78, b"fh").await;
        for (owner, stateid) in [(780, [0x78; 16]), (781, [0x79; 16])] {
            mount
                .locks
                .register(LockLane {
                    owner,
                    open_owner: 78,
                    owner_wire: Bytes::from(format!("same-open-{owner}")),
                    next_seqid: 1,
                    stateid,
                    fh: Bytes::from_static(b"fh"),
                    lock_type: 2,
                    offset: owner - 780,
                    length: 1,
                })
                .await;
        }
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&31u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x7c; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let open = read_record(&mut stream).await?;
            reply(&mut stream, &open, &open_result(0, [0x88; 16], b"fh")).await?;

            let first = read_record(&mut stream).await?;
            assert!(
                tokio::time::timeout(Duration::from_millis(30), read_record(&mut stream))
                    .await
                    .is_err(),
                "same-owner LOCK reclaims were concurrently in flight"
            );
            reply(
                &mut stream,
                &first,
                &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x98; 16])]),
            )
            .await?;
            let second = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &second,
                &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x99; 16])]),
            )
            .await
        });
        RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity: ClientIdentity {
                verifier: [0x5c; 8],
                owner: "same-owner-reclaim".into(),
            },
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        }
        .recover()
        .await
        .unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn no_grace_reclaim_clears_protection_and_advances_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 73, b"fh").await;
        register_scripted_lock(&mount, 74, [0x74; 16]).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&19u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x79; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &reclaim,
                &failed_lock_result("reclaim-open", 18, 10033),
            )
            .await
        });
        let context = RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity: ClientIdentity {
                verifier: [0x59; 8],
                owner: "scripted-no-grace".into(),
            },
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        };
        let error = context.recover_or_lose().await.unwrap_err();
        let outcome = error.operation_outcome().unwrap();
        assert_eq!(outcome.outcome, crate::OperationOutcome::Uncertain);
        assert_eq!(outcome.recovery, crate::RecoveryAction::Reopen);
        assert!(matches!(
            outcome.source.as_ref(),
            NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_NO_GRACE)
        ));
        assert_eq!(
            mount.health().lifecycle,
            crate::MountLifecycleState::LostState
        );
        assert_eq!(mount.health().generation, 2);
        assert!(mount.state.snapshot().await.is_empty());
        assert!(mount.locks.snapshot().await.is_empty());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn partial_reclaim_compound_loses_state_with_structured_guidance() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 79, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&37u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x7d; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim = read_record(&mut stream).await?;
            let mut partial = open_result(0, [0x89; 16], b"fh");
            partial[..4].copy_from_slice(&10006u32.to_be_bytes());
            reply(&mut stream, &reclaim, &partial).await
        });
        let error = RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity: ClientIdentity {
                verifier: [0x5d; 8],
                owner: "partial-reclaim".into(),
            },
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        }
        .recover_or_lose()
        .await
        .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().recovery,
            crate::RecoveryAction::Reopen
        );
        assert_eq!(
            mount.health().lifecycle,
            crate::MountLifecycleState::LostState
        );
        assert!(mount.state.snapshot().await.is_empty());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reclaim_reply_loss_is_not_replayed_and_requires_reopen() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 80, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&41u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x7e; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let _reclaim = read_record(&mut stream).await?;
            socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        let error = RecoveryContext {
            rpc: mount.rpc.clone(),
            auth: mount.auth.clone(),
            identity: ClientIdentity {
                verifier: [0x5e; 8],
                owner: "reply-loss-reclaim".into(),
            },
            client_id: Arc::clone(&mount.client_id),
            issuer: mount.issuer,
            state: Arc::clone(&mount.state),
            locks: Arc::clone(&mount.locks),
            lease: Arc::clone(&mount.lease),
            callback_addr: None,
        }
        .recover_or_lose()
        .await
        .unwrap_err();
        let outcome = error.operation_outcome().unwrap();
        assert_eq!(outcome.outcome, crate::OperationOutcome::Uncertain);
        assert_eq!(outcome.recovery, crate::RecoveryAction::Reopen);
        assert_eq!(mount.health().generation, 2);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelled_open_after_send_finishes_owner_state_settlement() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let (mut stream, _) = listener.accept().await.unwrap();
        let (seen_tx, seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let request = read_record(&mut stream).await?;
            let _ = seen_tx.send(());
            tokio::task::yield_now().await;
            reply(
                &mut stream,
                &request,
                &open_result(0, [0x51; 16], b"opened-fh"),
            )
            .await
        });
        let for_task = Arc::clone(&mount);
        let task = tokio::spawn(async move {
            for_task
                .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
                .await
        });
        seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        server.await.unwrap().unwrap();
        for _ in 0..20 {
            if mount
                .state
                .for_fh(&Bytes::from_static(b"opened-fh"), crate::OPEN_BOTH)
                .await
                .is_some()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("detached OPEN reply did not register owner state");
    }

    #[tokio::test]
    async fn open_reply_loss_is_structured_as_uncertain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let _request = read_record(&mut stream).await?;
            socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        let error = mount
            .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn partial_open_compound_is_structured_as_uncertain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let mut partial = compound_result("open", &[(22, &[]), (18, &[])]);
            partial[..4].copy_from_slice(&10006u32.to_be_bytes()); // NFS4ERR_SERVERFAULT after OPEN
            reply(&mut stream, &request, &partial).await
        });
        let error = mount
            .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn conditional_open_confirm_uses_next_seqid_and_returned_stateid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let open = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &open,
                &open_result(2, [0x61; 16], b"confirmed-fh"),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            let expected = [[0x61; 16].as_slice(), 1u32.to_be_bytes().as_slice()].concat();
            assert!(confirm.windows(expected.len()).any(|wire| wire == expected));
            let data = [0x62; 16];
            reply(
                &mut stream,
                &confirm,
                &compound_result("open_confirm", &[(22, &[]), (20, &data)]),
            )
            .await
        });
        let opened = mount
            .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
            .await
            .unwrap();
        let owner = decode_owner(&opened.into_parts().1.unwrap()).unwrap().1;
        let lane = mount.state.by_owner(owner).await.unwrap();
        assert_eq!(lane.lock().await.stateid, [0x62; 16]);
        assert_eq!(lane.lock().await.next_seqid, 2);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn scripted_mount_reconnect_preserves_confirmed_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mounted = Arc::new(Notify::new());
        let mounted_for_server = Arc::clone(&mounted);
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await?;

            let identity = read_record(&mut first).await?;
            assert!(
                identity
                    .windows(4)
                    .any(|value| value == 35u32.to_be_bytes())
            );
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x77; 8]);
            reply(
                &mut first,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;

            let confirm = read_record(&mut first).await?;
            let identity_tuple = [
                0x0102_0304_0506_0708u64.to_be_bytes().as_slice(),
                [0x77; 8].as_slice(),
            ]
            .concat();
            assert!(confirm.windows(16).any(|value| value == identity_tuple));
            reply(
                &mut first,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;

            let navigation = read_record(&mut first).await?;
            let minor_and_ops = [0u32.to_be_bytes(), 3u32.to_be_bytes()].concat();
            assert!(navigation.windows(8).any(|value| value == minor_and_ops));
            let mut fh = Vec::new();
            xdr_opaque(&mut fh, b"scripted-fh");
            reply(
                &mut first,
                &navigation,
                &compound_result("navigate", &[(24, &[]), (15, &[]), (10, &fh)]),
            )
            .await?;
            let lease_time = read_record(&mut first).await?;
            let mut attrs = Vec::new();
            attrs.extend_from_slice(&1u32.to_be_bytes());
            attrs.extend_from_slice(&(1u32 << 10).to_be_bytes());
            xdr_opaque(&mut attrs, &60u32.to_be_bytes());
            reply(
                &mut first,
                &lease_time,
                &compound_result("lease-time", &[(22, &[]), (9, &attrs)]),
            )
            .await?;
            mounted_for_server.notified().await;
            socket2::SockRef::from(&first).set_linger(Some(Duration::ZERO))?;
            drop(first);

            let (mut second, _) = listener.accept().await?;
            let null = read_record(&mut second).await?;
            reply(&mut second, &null, &[]).await?;
            Ok::<(), std::io::Error>(())
        });

        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            dircount: 32 * 1024,
            maxcount: 32 * 1024,
            rsize: 0,
            wsize: 0,
            noresvport: true,
            retain_delegations: false,
        };
        let mount = mount_on_addr(addr, &args, Auth::new_null()).await.unwrap();
        assert_eq!(mount.root_fh, Bytes::from_static(b"scripted-fh"));
        mounted.notify_one();
        tokio::time::sleep(Duration::from_millis(10)).await;
        mount.null().await.unwrap();
        server.await.unwrap().unwrap();
        mount.umount().await.unwrap();
    }

    #[tokio::test]
    async fn retained_delegations_publish_callback_before_setclientid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            assert!(
                identity.windows(3).any(|value| value == b"tcp"),
                "SETCLIENTID did not publish a ready TCP callback listener"
            );
            let netid = identity
                .windows(3)
                .position(|value| value == b"tcp")
                .expect("TCP netid missing");
            let addr_len_offset = netid + 4;
            let addr_len = u32::from_be_bytes(
                identity[addr_len_offset..addr_len_offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let universal =
                std::str::from_utf8(&identity[addr_len_offset + 4..addr_len_offset + 4 + addr_len])
                    .unwrap();
            let fields: Vec<_> = universal.split('.').collect();
            assert_eq!(fields.len(), 6, "invalid callback universal address");
            let callback_addr = format!(
                "{}.{}.{}.{}:{}",
                fields[0],
                fields[1],
                fields[2],
                fields[3],
                fields[4].parse::<u16>().unwrap() * 256 + fields[5].parse::<u16>().unwrap()
            );
            TcpStream::connect(callback_addr).await?;
            Ok::<(), std::io::Error>(())
        });
        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            dircount: 32 * 1024,
            maxcount: 32 * 1024,
            rsize: 0,
            wsize: 0,
            noresvport: true,
            retain_delegations: true,
        };

        let error = mount_on_addr(addr, &args, Auth::new_null())
            .await
            .unwrap_err();
        assert!(matches!(error, NfsError::Rpc(_) | NfsError::Io(_)));
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_after_setclientid_detaches_while_confirm_settles() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (identity_seen_tx, identity_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            assert!(
                identity
                    .windows(4)
                    .any(|value| value == 35u32.to_be_bytes())
            );
            let _ = identity_seen_tx.send(());
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x77; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            Ok::<bool, std::io::Error>(confirm.windows(4).any(|value| value == 36u32.to_be_bytes()))
        });
        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            dircount: 32 * 1024,
            maxcount: 32 * 1024,
            rsize: 0,
            wsize: 0,
            noresvport: true,
            retain_delegations: false,
        };
        let task = tokio::spawn(async move { mount_on_addr(addr, &args, Auth::new_null()).await });
        identity_seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        assert!(
            server.await.unwrap().unwrap(),
            "sent SETCLIENTID was not settled with confirm"
        );
    }

    fn directory_page(entry_cookie: u64, include_eof: bool) -> Bytes {
        let mut data = Vec::new();
        data.extend_from_slice(b"verifier");
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&entry_cookie.to_be_bytes());
        xdr_opaque(&mut data, b"file");
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&(1u32 << 20).to_be_bytes());
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&42u64.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        if include_eof {
            data.extend_from_slice(&1u32.to_be_bytes());
        }
        Bytes::from(data)
    }

    fn readdir_result(verifier: [u8; 8], cookie: u64, name: &[u8], eof: bool) -> Vec<u8> {
        let mut page = Vec::new();
        page.extend_from_slice(&verifier);
        page.extend_from_slice(&1u32.to_be_bytes());
        page.extend_from_slice(&cookie.to_be_bytes());
        xdr_opaque(&mut page, name);
        page.extend_from_slice(&1u32.to_be_bytes());
        page.extend_from_slice(&(1u32 << 20).to_be_bytes());
        page.extend_from_slice(&8u32.to_be_bytes());
        page.extend_from_slice(&(cookie + 100).to_be_bytes());
        page.extend_from_slice(&0u32.to_be_bytes());
        page.extend_from_slice(&u32::from(eof).to_be_bytes());
        compound_result("readdir", &[(26 - 4, &[]), (26, &page)])
    }

    #[tokio::test]
    async fn public_readdir_stream_propagates_cookie_and_verifier_across_pages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &first,
                &readdir_result(*b"firstver", 7, b"one", false),
            )
            .await?;
            let second = read_record(&mut stream).await?;
            let continuation = [7u64.to_be_bytes().as_slice(), b"firstver"].concat();
            assert!(
                second
                    .windows(continuation.len())
                    .any(|wire| wire == continuation)
            );
            reply(
                &mut stream,
                &second,
                &readdir_result(*b"secondve", 11, b"two", true),
            )
            .await
        });
        let entries = mount
            .readdir(Bytes::from_static(b"root"))
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.file_name.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
        server.await.unwrap().unwrap();
    }

    #[test]
    fn readdir_page_decodes_cookie_name_and_fileid() {
        let mut data = directory_page(7, true);
        let (verifier, entries, eof) = decode_directory_page(&mut data, 0).unwrap();
        assert_eq!(&verifier, b"verifier");
        assert_eq!(entries.len(), 1);
        let entry = entries[0].as_ref().unwrap();
        assert_eq!(entry.cookie, 7);
        assert_eq!(entry.name, "file");
        assert_eq!(entry.attr.fileid, 42);
        assert!(eof);
    }

    #[test]
    fn readdir_page_rejects_non_advancing_cookie() {
        let mut data = directory_page(7, true);
        let (_, entries, _) = decode_directory_page(&mut data, 7).unwrap();
        let error = entries[0].as_ref().unwrap_err();
        assert!(error.to_string().contains("did not advance cookie"));
    }

    #[test]
    fn readdir_page_rejects_missing_eof_flag() {
        let mut data = directory_page(7, false);
        let (_, entries, _) = decode_directory_page(&mut data, 0).unwrap();
        let error = entries[1].as_ref().unwrap_err();
        assert!(error.to_string().contains("eof flag truncated"));
    }

    #[test]
    fn readdir_page_enforces_negotiated_maxcount() {
        let error = validate_readdir_payload(Bytes::from_static(b"12345"), 4).unwrap_err();
        assert!(error.to_string().contains("exceeds requested maxcount"));
    }

    #[test]
    fn malformed_later_entry_preserves_prior_entry_result() {
        let mut data = directory_page(7, true).to_vec();
        data.truncate(data.len() - 8);
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&8u64.to_be_bytes());
        data.extend_from_slice(&99u32.to_be_bytes());
        let (_, entries, eof) = decode_directory_page(&mut Bytes::from(data), 0).unwrap();
        assert_eq!(entries[0].as_ref().unwrap().name, "file");
        assert!(entries[1].is_err());
        assert!(eof);
    }

    #[test]
    fn metadata_results_reject_missing_required_attributes() {
        let error = require_fattrs(&[0], &[30], "FSINFO").unwrap_err();
        assert!(matches!(error, NfsError::Unsupported(_)));
    }

    #[tokio::test]
    async fn guarded_setattr_is_explicitly_unsupported() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let error = mount
            .setattr(
                Bytes::from_static(b"fh"),
                Some(crate::Time::default()),
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, NfsError::Unsupported(_)));
    }

    #[tokio::test]
    async fn remove_reply_loss_is_structured_as_uncertain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let _request = read_record(&mut stream).await?;
            socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        let error = mount
            .remove(Bytes::from_static(b"root"), "victim")
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelled_remove_after_send_is_settled() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let (seen_tx, seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let _ = seen_tx.send(());
            let change_info = [0u8; 20];
            reply(
                &mut stream,
                &request,
                &compound_result("remove", &[(22, &[]), (28, &change_info)]),
            )
            .await
        });
        let task_mount = Arc::clone(&mount);
        let task = tokio::spawn(async move {
            task_mount
                .remove(Bytes::from_static(b"root"), "victim")
                .await
        });
        seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn scripted_public_metadata_and_namespace_families_succeed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let access = read_record(&mut stream).await?;
            let access_data = [0x3fu32.to_be_bytes(), 0x21u32.to_be_bytes()].concat();
            reply(
                &mut stream,
                &access,
                &compound_result("access", &[(22, &[]), (3, &access_data)]),
            )
            .await?;

            let setattr = read_record(&mut stream).await?;
            let attrsset = [1u32.to_be_bytes(), (1u32 << 4).to_be_bytes()].concat();
            reply(
                &mut stream,
                &setattr,
                &compound_result("setattr", &[(22, &[]), (34, &attrsset)]),
            )
            .await?;

            let readlink = read_record(&mut stream).await?;
            let mut target = Vec::new();
            xdr_opaque(&mut target, b"target");
            reply(
                &mut stream,
                &readlink,
                &compound_result("readlink", &[(22, &[]), (27, &target)]),
            )
            .await?;

            let rename = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &rename,
                &compound_result("rename", &[(22, &[]), (32, &[]), (22, &[]), (29, &[0; 40])]),
            )
            .await?;

            let link = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &link,
                &compound_result("link", &[(22, &[]), (32, &[]), (22, &[]), (11, &[0; 20])]),
            )
            .await?;
            let linked_getattr = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &linked_getattr,
                &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
            )
            .await?;

            let remove = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &remove,
                &compound_result("remove", &[(22, &[]), (28, &[0; 20])]),
            )
            .await
        });

        assert_eq!(
            mount.access(Bytes::from_static(b"fh"), 0x21).await.unwrap(),
            0x21
        );
        mount
            .setattr(
                Bytes::from_static(b"fh"),
                None,
                None,
                None,
                None,
                Some(7),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            mount.readlink(Bytes::from_static(b"fh")).await.unwrap(),
            "target"
        );
        mount
            .rename(
                Bytes::from_static(b"from"),
                "old",
                Bytes::from_static(b"to"),
                "new",
            )
            .await
            .unwrap();
        mount
            .link(
                Bytes::from_static(b"source"),
                Bytes::from_static(b"target"),
                "link",
            )
            .await
            .unwrap();
        mount
            .remove(Bytes::from_static(b"root"), "file")
            .await
            .unwrap();
        server.await.unwrap().unwrap();
    }

    fn fattr_result(bitmap: &[u32], values: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(bitmap.len() as u32).to_be_bytes());
        for word in bitmap {
            result.extend_from_slice(&word.to_be_bytes());
        }
        xdr_opaque(&mut result, values);
        result
    }

    #[tokio::test]
    async fn scripted_public_getattr_and_filesystem_metadata_succeed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let getattr = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &getattr,
                &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
            )
            .await?;
            let fsinfo = read_record(&mut stream).await?;
            let bm = [
                (1 << 5) | (1 << 6) | (1 << 15) | (1 << 27) | (1 << 30) | (1 << 31),
                1 << 19,
            ];
            let values = [
                1u32.to_be_bytes().as_slice(),
                1u32.to_be_bytes().as_slice(),
                1u32.to_be_bytes().as_slice(),
                u64::MAX.to_be_bytes().as_slice(),
                65536u64.to_be_bytes().as_slice(),
                32768u64.to_be_bytes().as_slice(),
                0i64.to_be_bytes().as_slice(),
                1u32.to_be_bytes().as_slice(),
            ]
            .concat();
            let data = fattr_result(&bm, &values);
            reply(
                &mut stream,
                &fsinfo,
                &compound_result("fsinfo", &[(22, &[]), (9, &data)]),
            )
            .await?;
            let fsstat = read_record(&mut stream).await?;
            let bm = [
                (1 << 21) | (1 << 22) | (1 << 23),
                (1 << 10) | (1 << 11) | (1 << 12),
            ];
            let values = (1u64..=6).flat_map(u64::to_be_bytes).collect::<Vec<_>>();
            let data = fattr_result(&bm, &values);
            reply(
                &mut stream,
                &fsstat,
                &compound_result("fsstat", &[(22, &[]), (9, &data)]),
            )
            .await?;
            let pathconf = read_record(&mut stream).await?;
            let bm = [
                (1 << 16) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
                1 << 2,
            ];
            let values = [
                0u32.to_be_bytes(),
                1u32.to_be_bytes(),
                1u32.to_be_bytes(),
                1024u32.to_be_bytes(),
                255u32.to_be_bytes(),
                1u32.to_be_bytes(),
            ]
            .concat();
            let data = fattr_result(&bm, &values);
            reply(
                &mut stream,
                &pathconf,
                &compound_result("pathconf", &[(22, &[]), (9, &data)]),
            )
            .await
        });
        mount.getattr(Bytes::from_static(b"fh")).await.unwrap();
        let info = mount.fsinfo().await.unwrap();
        assert_eq!((info.rtmax, info.wtmax), (65536, 32768));
        let stat = mount.fsstat().await.unwrap();
        assert_eq!((stat.afiles, stat.tbytes), (1, 6));
        let pathconf = mount.pathconf(Bytes::from_static(b"fh")).await.unwrap();
        assert_eq!((pathconf.linkmax, pathconf.name_max), (1024, 255));
        server.await.unwrap().unwrap();
    }

    fn create_object_result(tag: &str, fh_value: &[u8]) -> Vec<u8> {
        let mut create = vec![0; 20];
        create.extend_from_slice(&0u32.to_be_bytes());
        let mut fh = Vec::new();
        xdr_opaque(&mut fh, fh_value);
        compound_result(tag, &[(22, &[]), (6, &create), (10, &fh), (9, &[0; 8])])
    }

    #[tokio::test]
    async fn scripted_public_create_directory_and_symlink_succeed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let create = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &create,
                &open_result(0, [0x31; 16], b"file-fh"),
            )
            .await?;
            let getattr = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &getattr,
                &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
            )
            .await?;

            let mkdir = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &mkdir,
                &create_object_result("mkdir", b"dir-fh"),
            )
            .await?;
            let setattr = read_record(&mut stream).await?;
            let attrsset = [
                2u32.to_be_bytes(),
                0u32.to_be_bytes(),
                (1u32 << 1).to_be_bytes(),
            ]
            .concat();
            reply(
                &mut stream,
                &setattr,
                &compound_result("setattr", &[(22, &[]), (34, &attrsset)]),
            )
            .await?;

            let symlink = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &symlink,
                &create_object_result("symlink", b"link-fh"),
            )
            .await
        });
        let file = mount
            .create(Bytes::from_static(b"root"), "file", None)
            .await
            .unwrap();
        assert_eq!(file.fh, Bytes::from_static(b"file-fh"));
        let directory = mount
            .mkdir(Bytes::from_static(b"root"), "dir", 0o755)
            .await
            .unwrap();
        assert_eq!(directory.fh, Bytes::from_static(b"dir-fh"));
        let symlink = mount
            .symlink("target", Bytes::from_static(b"root"), "link")
            .await
            .unwrap();
        assert_eq!(symlink.fh, Bytes::from_static(b"link-fh"));
        server.await.unwrap().unwrap();
    }

    async fn register_scripted_open(mount: &Mount40, owner: u64, fh: &'static [u8]) {
        mount
            .state
            .register(OwnerLane {
                owner,
                next_seqid: 1,
                stateid: [0x61; 16],
                fh: Bytes::from_static(fh),
                access: crate::OPEN_BOTH,
                write_verifier: None,
            })
            .await;
    }

    async fn register_scripted_lock(mount: &Mount40, owner: u64, stateid: [u8; 16]) {
        mount
            .locks
            .register(LockLane {
                owner,
                open_owner: owner,
                owner_wire: Bytes::from(format!("scripted-lock-{owner}")),
                next_seqid: 1,
                stateid,
                fh: Bytes::from_static(b"fh"),
                lock_type: 2,
                offset: 0,
                length: 1,
            })
            .await;
    }

    #[tokio::test]
    async fn public_typed_lock_blocks_close_until_exact_unlock() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 41, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let lock = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &lock,
                &compound_result("lock", &[(22, &[]), (12, &[0x71; 16])]),
            )
            .await?;
            let unlock = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &unlock,
                &compound_result("locku", &[(22, &[]), (14, &[0x72; 16])]),
            )
            .await?;
            let cleanup = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &cleanup,
                &compound_result("release-lockowner", &[(39, &[])]),
            )
            .await
        });
        let token = mount
            .lock_stateful(Bytes::from_static(b"fh"), 2, 7, 11)
            .await
            .unwrap();
        let error = mount.close(Bytes::from_static(b"fh")).await.unwrap_err();
        assert!(error.to_string().contains("outstanding locks"));
        mount.unlock_stateful(token).await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn lock_reply_loss_is_uncertain_and_does_not_register_protection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 42, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let _ = read_record(&mut stream).await?;
            socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        let error = mount
            .lock(Bytes::from_static(b"fh"), 2, 0, 1)
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelled_lock_after_send_finishes_owner_settlement() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 43, b"fh").await;
        let (seen_tx, seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let _ = seen_tx.send(());
            reply(
                &mut stream,
                &request,
                &compound_result("lock", &[(22, &[]), (12, &[0x73; 16])]),
            )
            .await
        });
        let task_mount = Arc::clone(&mount);
        let task =
            tokio::spawn(async move { task_mount.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
        seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        server.await.unwrap().unwrap();
        for _ in 0..20 {
            if mount.locks.has_fh(&Bytes::from_static(b"fh")).await {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("detached LOCK reply did not register protection");
    }

    #[tokio::test]
    async fn locku_reply_loss_is_uncertain_and_retains_local_protection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_lock(&mount, 61, [0x91; 16]).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let _ = read_record(&mut stream).await?;
            socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
            drop(stream);
            Ok::<(), std::io::Error>(())
        });
        let error = mount
            .locku(
                Bytes::from_static(b"fh"),
                Bytes::from_static(&[0x91; 16]),
                2,
                0,
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        assert!(mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authoritative_locku_error_consumes_owner_seqid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_lock(&mount, 63, [0x94; 16]).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            reply(&mut stream, &first, &failed_lock_result("locku", 14, 22)).await?;
            let second = read_record(&mut stream).await?;
            let expected = [
                2u32.to_be_bytes().as_slice(),
                2u32.to_be_bytes().as_slice(),
                &[0x94; 16],
            ]
            .concat();
            assert!(second.windows(expected.len()).any(|wire| wire == expected));
            reply(
                &mut stream,
                &second,
                &compound_result("locku", &[(22, &[]), (14, &[0x95; 16])]),
            )
            .await?;
            let cleanup = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &cleanup,
                &compound_result("release-lockowner", &[(39, &[])]),
            )
            .await
        });
        let args = || {
            (
                Bytes::from_static(b"fh"),
                Bytes::from_static(&[0x94; 16]),
                2,
                0,
                1,
            )
        };
        let (fh, stateid, kind, offset, length) = args();
        assert!(matches!(
            mount.locku(fh, stateid, kind, offset, length).await,
            Err(NfsError::Nfs4(_))
        ));
        let (fh, stateid, kind, offset, length) = args();
        mount
            .locku(fh, stateid, kind, offset, length)
            .await
            .unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelled_locku_after_send_finishes_release_settlement() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_lock(&mount, 62, [0x92; 16]).await;
        let (seen_tx, seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            let _ = seen_tx.send(());
            reply(
                &mut stream,
                &request,
                &compound_result("locku", &[(22, &[]), (14, &[0x93; 16])]),
            )
            .await?;
            let cleanup = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &cleanup,
                &compound_result("release-lockowner", &[(39, &[])]),
            )
            .await
        });
        let task_mount = Arc::clone(&mount);
        let task = tokio::spawn(async move {
            task_mount
                .locku(
                    Bytes::from_static(b"fh"),
                    Bytes::from_static(&[0x92; 16]),
                    2,
                    0,
                    1,
                )
                .await
        });
        seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        server.await.unwrap().unwrap();
        for _ in 0..20 {
            if !mount.locks.has_fh(&Bytes::from_static(b"fh")).await {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("detached LOCKU reply did not remove released protection");
    }

    #[tokio::test]
    async fn typed_lock_rejects_foreign_or_mismatched_tokens_without_rpc() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let foreign = crate::LockToken::new(
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x21; 16]),
            2,
            0,
            1,
            mount.issuer ^ 1,
            1,
        );
        assert!(mount.unlock_stateful(foreign).await.is_err());
        let unknown = crate::LockToken::new(
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x22; 16]),
            2,
            0,
            1,
            mount.issuer,
            1,
        );
        assert!(mount.unlock_stateful(unknown).await.is_err());
        let stale = crate::LockToken::new(
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x23; 16]),
            2,
            0,
            1,
            mount.issuer,
            mount.lease.generation() + 1,
        );
        assert!(mount.unlock_stateful(stale).await.is_err());
    }

    #[tokio::test]
    async fn scripted_public_lock_conflict_exposes_denied_range() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 50, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let request = read_record(&mut stream).await?;
            reply(&mut stream, &request, &denied_lock_result("lock", 12)).await?;
            let next = read_record(&mut stream).await?;
            let expected = [
                1u32.to_be_bytes().as_slice(),
                2u32.to_be_bytes().as_slice(),
                &[0x61; 16],
            ]
            .concat();
            assert!(next.windows(expected.len()).any(|wire| wire == expected));
            reply(
                &mut stream,
                &next,
                &compound_result("lock", &[(22, &[]), (12, &[0xb1; 16])]),
            )
            .await
        });
        assert!(matches!(
            mount.lock(Bytes::from_static(b"fh"), 2, 0, 1).await,
            Err(NfsError::LockDenied {
                lock_type: 2,
                offset: 7,
                length: 11,
                ..
            })
        ));
        mount
            .lock(Bytes::from_static(b"fh"), 2, 2, 1)
            .await
            .unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn independent_opens_of_same_file_lock_without_global_serialization() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 54, b"fh").await;
        register_scripted_open(&mount, 55, b"fh").await;
        let opened_a = mount::OpenFile::with_protocol_state(
            mount::ObjRes {
                fh: Bytes::from_static(b"fh"),
                attr: None,
            },
            encode_owner(mount.issuer, 54),
        );
        let opened_b = mount::OpenFile::with_protocol_state(
            mount::ObjRes {
                fh: Bytes::from_static(b"fh"),
                attr: None,
            },
            encode_owner(mount.issuer, 55),
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            let second =
                tokio::time::timeout(Duration::from_secs(1), read_record(&mut stream)).await??;
            reply(
                &mut stream,
                &first,
                &compound_result("lock", &[(22, &[]), (12, &[0xa1; 16])]),
            )
            .await?;
            reply(
                &mut stream,
                &second,
                &compound_result("lock", &[(22, &[]), (12, &[0xa2; 16])]),
            )
            .await
        });
        let (a, b) = tokio::join!(
            mount.lock_open_stateful(&opened_a, 2, 0, 1),
            mount.lock_open_stateful(&opened_b, 2, 2, 1),
        );
        assert!(a.is_ok() && b.is_ok());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn different_open_owners_send_locks_without_global_serialization() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 51, b"fh-a").await;
        register_scripted_open(&mount, 52, b"fh-b").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            let second =
                tokio::time::timeout(Duration::from_secs(1), read_record(&mut stream)).await??;
            reply(
                &mut stream,
                &first,
                &compound_result("lock", &[(22, &[]), (12, &[0x81; 16])]),
            )
            .await?;
            reply(
                &mut stream,
                &second,
                &compound_result("lock", &[(22, &[]), (12, &[0x82; 16])]),
            )
            .await
        });
        let (a, b) = tokio::join!(
            mount.lock(Bytes::from_static(b"fh-a"), 2, 0, 1),
            mount.lock(Bytes::from_static(b"fh-b"), 2, 0, 1),
        );
        assert!(a.is_ok() && b.is_ok());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn same_open_owner_serializes_lock_seqid_consumption() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        register_scripted_open(&mount, 53, b"fh").await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let first = read_record(&mut stream).await?;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                    .await
                    .is_err()
            );
            reply(
                &mut stream,
                &first,
                &compound_result("lock", &[(22, &[]), (12, &[0x83; 16])]),
            )
            .await?;
            let second = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &second,
                &compound_result("lock", &[(22, &[]), (12, &[0x84; 16])]),
            )
            .await
        });
        let (a, b) = tokio::join!(
            mount.lock(Bytes::from_static(b"fh"), 2, 0, 1),
            mount.lock(Bytes::from_static(b"fh"), 2, 2, 1),
        );
        assert!(a.is_ok() && b.is_ok());
        server.await.unwrap().unwrap();
    }
}

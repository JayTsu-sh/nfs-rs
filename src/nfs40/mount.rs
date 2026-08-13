use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream;
use futures::stream::TryStreamExt as _;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::compound::{
    CallbackAddress, CompoundBuilder, OpenArgs, SetClientIdArgs,
    create_succeeded_before_compound_failure, decode_access_response, decode_commit_response,
    decode_confirm_response, decode_create_response,
    decode_getattr_response as decode_getattr_compound, decode_link_response,
    decode_lookup_getattr_response, decode_open_response, decode_read_response,
    decode_readdir_response, decode_readlink_response, decode_remove_response,
    decode_rename_response, decode_setattr_response, decode_setclientid_response,
    decode_stateid_response, decode_write_response, open_succeeded_before_compound_failure,
};
use super::state::{OpenState, OwnerLane, decode_owner, encode_owner};
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
    client_id: u64,
    issuer: u64,
    next_owner: AtomicU64,
    state: Arc<OpenState>,
    dircount: u32,
    maxcount: u32,
    rsize: u32,
    wsize: u32,
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
    let identity_rpc = rpc.clone();
    let identity_auth = auth.clone();
    let identity =
        tokio::spawn(async move { establish_identity(&identity_rpc, &identity_auth).await });
    let client_id = identity
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 identity task failed: {error}")))??;

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

    Ok(Mount40 {
        rpc,
        auth,
        root_fh,
        client_id,
        issuer: rand::random(),
        next_owner: AtomicU64::new(1),
        state: Arc::new(OpenState::default()),
        dircount: args.dircount,
        maxcount: args.maxcount,
        rsize: args.rsize,
        wsize: args.wsize,
    })
}

async fn establish_identity(rpc: &rpc::Client, auth: &Auth) -> Result<u64> {
    let verifier: [u8; 8] = rand::random();
    let owner = format!("nfs-rs-v4.0-{:016x}", rand::random::<u64>());
    let identity_request = CompoundBuilder::new("identity")
        .setclientid(SetClientIdArgs {
            verifier,
            owner: owner.as_bytes(),
            callback: CallbackAddress::DISABLED,
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
    async fn open_file(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
        create: bool,
    ) -> Result<mount::OpenFile> {
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
        let client_id = self.client_id;
        let issuer = self.issuer;
        tokio::spawn(async move {
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
            Ok(mount::OpenFile::with_protocol_state(
                mount::ObjRes { fh, attr: None },
                encode_owner(issuer, owner),
            ))
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 open task failed: {error}")))?
    }

    async fn commit_verifier(&self, fh: &Bytes, offset: u64, count: u32) -> Result<[u8; 8]> {
        let request = CompoundBuilder::new("commit")
            .putfh(fh)
            .commit(offset, count)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("commit", 0, 0, OperationClass::ReplaySensitive);
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
        decode_commit_response(response)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
    }

    async fn close_lane(&self, lane: Arc<tokio::sync::Mutex<OwnerLane>>) -> Result<()> {
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
            Ok(())
        })
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 close task failed: {error}")))?
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
            self.rpc
                .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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
            self.rpc
                .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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

#[async_trait]
impl Mount for Mount40 {
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
        self.rpc.shutdown().await;
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
            .rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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
        let lane = self
            .state
            .for_fh(&fh, crate::OPEN_BOTH)
            .await
            .ok_or_else(|| NfsError::InvalidInput("NFSv4.0 CLOSE requires an open file".into()))?;
        self.close_lane(lane).await
    }
    async fn close_stateful(&self, file: mount::OpenFile) -> Result<()> {
        let (object, protocol_state) = file.into_parts();
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
        let verifier = self.commit_verifier(&fh, offset, count).await?;
        if let Some(lane) = self.state.for_fh(&fh, crate::OPEN_WRITE).await {
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
            .rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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
        let stateid = match self.state.for_fh(&fh, crate::OPEN_WRITE).await {
            Some(lane) => lane.lock().await.stateid,
            None => [0; 16],
        };
        let request = CompoundBuilder::new("setattr")
            .putfh(&fh)
            .setattr(&stateid, &bitmap, &values)
            .encode_with_header(&self.auth);
        let (class, ctx) = context("setattr", 0, 0, OperationClass::ReplaySensitive);
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
        decode_setattr_response(response, &bitmap)
            .map_err(|error| classify_sent_nfs40_error(class, ctx, error))
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
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
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
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
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
            self.rpc
                .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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
            .rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
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
        let stateid = if let Some(lane) = self.state.for_fh(&fh, crate::OPEN_READ).await {
            lane.lock().await.stateid
        } else {
            [0; 16]
        };
        let request = CompoundBuilder::new("read")
            .putfh(&fh)
            .read(&stateid, offset, count)
            .encode_with_header(&self.auth);
        let response = self
            .rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        decode_read_response(response)
    }
    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
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
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
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
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
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
        let response = settled_call(self.rpc.clone(), request, class, ctx.clone()).await?;
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
            client_id: 7,
            issuer: 9,
            next_owner: AtomicU64::new(1),
            state: Arc::new(OpenState::default()),
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
}

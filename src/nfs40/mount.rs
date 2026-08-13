use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::compound::{
    CallbackAddress, CompoundBuilder, OpenArgs, SetClientIdArgs, decode_commit_response,
    decode_confirm_response, decode_lookup_response, decode_open_response, decode_read_response,
    decode_setclientid_response, decode_stateid_response, decode_write_response,
    open_succeeded_before_compound_failure,
};
use super::state::{OpenState, OwnerLane, decode_owner, encode_owner};
use crate::error::{NfsError, OperationClass, RequestContext, Result, classify_sent_nfs40_error};
use crate::mount::{self, NFSVersion};
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

#[async_trait]
impl Mount for Mount40 {
    fn get_max_read_size(&self) -> u32 {
        1_048_576
    }
    fn get_max_write_size(&self) -> u32 {
        1_048_576
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

    async fn access(&self, _fh: Bytes, _mode: u32) -> Result<u32> {
        unsupported("ACCESS")
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
            let request = CompoundBuilder::new("open")
                .putfh(&dir_fh)
                .open(OpenArgs {
                    seqid: 0,
                    share_access: access,
                    client_id,
                    owner: owner_wire.as_bytes(),
                    filename: &filename,
                })
                .getfh()
                .encode_with_header(&auth);
            let (class, ctx) = context("open", owner, 0, OperationClass::ReplaySensitive);
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
        _dir_fh: Bytes,
        _filename: &str,
        _mode: Option<u32>,
    ) -> Result<mount::ObjRes> {
        unsupported("CREATE")
    }
    async fn create_path(&self, _path: &str, _mode: Option<u32>) -> Result<mount::ObjRes> {
        unsupported("CREATE")
    }
    async fn fsinfo(&self) -> Result<mount::FSInfo> {
        unsupported("FSINFO")
    }
    async fn fsstat(&self) -> Result<mount::FSStat> {
        unsupported("FSSTAT")
    }
    async fn getattr(&self, _fh: Bytes) -> Result<mount::Attr> {
        unsupported("GETATTR")
    }
    async fn setattr(
        &self,
        _fh: Bytes,
        _guard_ctime: Option<crate::Time>,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<crate::Time>,
        _mtime: Option<crate::Time>,
    ) -> Result<()> {
        unsupported("SETATTR")
    }
    async fn setattr_path(
        &self,
        _path: &str,
        _specify_guard: bool,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<crate::Time>,
        _mtime: Option<crate::Time>,
    ) -> Result<()> {
        unsupported("SETATTR")
    }
    async fn link(
        &self,
        _src_fh: Bytes,
        _dst_dir_fh: Bytes,
        _dst_filename: &str,
    ) -> Result<mount::Attr> {
        unsupported("LINK")
    }
    async fn link_path(&self, _src_path: &str, _dst_path: &str) -> Result<mount::Attr> {
        unsupported("LINK")
    }
    async fn symlink_path(&self, _src_path: &str, _dst_path: &str) -> Result<mount::ObjRes> {
        unsupported("SYMLINK")
    }
    async fn symlink(
        &self,
        _src_path: &str,
        _dst_dir_fh: Bytes,
        _dst_filename: &str,
    ) -> Result<mount::ObjRes> {
        unsupported("SYMLINK")
    }
    async fn readlink(&self, _fh: Bytes) -> Result<String> {
        unsupported("READLINK")
    }
    async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<mount::ObjRes> {
        let request = CompoundBuilder::new("lookup")
            .putfh(&dir_fh)
            .lookup(filename)
            .getfh()
            .encode_with_header(&self.auth);
        let response = self
            .rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        Ok(mount::ObjRes {
            fh: decode_lookup_response(response)?,
            attr: None,
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
    async fn pathconf(&self, _fh: Bytes) -> Result<mount::Pathconf> {
        unsupported("PATHCONF")
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
    async fn readdir(&self, _dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        Box::pin(stream::once(async { unsupported("READDIR") }))
    }
    async fn readdirplus(&self, _dir_fh: Bytes) -> mount::ReaddirplusStream<'_> {
        Box::pin(stream::once(async { unsupported("READDIRPLUS") }))
    }
    async fn mkdir(&self, _dir_fh: Bytes, _dirname: &str, _mode: u32) -> Result<mount::ObjRes> {
        unsupported("MKDIR")
    }
    async fn mkdir_path(&self, _path: &str, _mode: u32) -> Result<mount::ObjRes> {
        unsupported("MKDIR")
    }
    async fn remove(&self, _dir_fh: Bytes, _filename: &str) -> Result<()> {
        unsupported("REMOVE")
    }
    async fn remove_path(&self, _path: &str) -> Result<()> {
        unsupported("REMOVE")
    }
    async fn rmdir(&self, _dir_fh: Bytes, _dirname: &str) -> Result<()> {
        unsupported("RMDIR")
    }
    async fn rmdir_path(&self, _path: &str) -> Result<()> {
        unsupported("RMDIR")
    }
    async fn rename_path(&self, _from_path: &str, _to_path: &str) -> Result<()> {
        unsupported("RENAME")
    }
    async fn rename(
        &self,
        _from_dir_fh: Bytes,
        _from_filename: &str,
        _to_dir_fh: Bytes,
        _to_filename: &str,
    ) -> Result<()> {
        unsupported("RENAME")
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
}

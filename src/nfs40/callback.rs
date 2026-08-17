use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::error::{NfsError, Result};
use crate::nfs40::compound::{
    CompoundBuilder, DelegationGrant, DelegationKind, decode_commit_response,
    decode_delegreturn_response,
};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

pub(crate) const CB_PROGRAM: u32 = 0x4000_0000;
const MAX_CALLBACK_RECORD: usize = 64 * 1024;
const CALLBACK_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(10);
const CALLBACK_SERVICE_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const CALLBACK_WORKER_STOP_TIMEOUT: Duration = Duration::from_secs(25);
const CALLBACK_REPLAY_CACHE_SIZE: usize = 128;
const MAX_CALLBACK_OPERATIONS: u32 = 64;
const MAX_CALLBACK_CONNECTIONS: usize = 32;
const CALLBACK_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct CallbackService {
    universal_addr: String,
    state: Arc<CallbackState>,
    recall_rx: Mutex<Option<mpsc::Receiver<RecallNotification>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    stop: watch::Sender<bool>,
}

struct DelegationRecord {
    grant: DelegationGrant,
    generation: u64,
    attributes: Option<(u64, u64)>,
    recalling: bool,
}

struct ReplayEntry {
    xid: u32,
    call: Vec<u8>,
    reply: Vec<u8>,
}

pub(crate) struct CallbackState {
    delegations: Mutex<HashMap<Bytes, DelegationRecord>>,
    generation: AtomicU64,
    recall_tx: mpsc::Sender<RecallNotification>,
    open_publications: AtomicUsize,
    grants_received: AtomicU64,
    recalls_received: AtomicU64,
    returns_completed: AtomicU64,
    returns_failed: AtomicU64,
    service_healthy: AtomicBool,
    worker_healthy: AtomicBool,
    io_gates: Mutex<HashMap<Bytes, Weak<RwLock<()>>>>,
    replies: Mutex<VecDeque<ReplayEntry>>,
}

pub(crate) struct OpenPublication {
    state: Arc<CallbackState>,
}

pub(crate) struct RecallNotification {
    pub fh: Bytes,
    pub stateid: [u8; 16],
    pub generation: u64,
    pub flush: bool,
}

pub(crate) struct CallbackWorker {
    task: Mutex<Option<JoinHandle<()>>>,
    stop: watch::Sender<bool>,
    state: Arc<CallbackState>,
}

impl CallbackService {
    pub(crate) async fn bind_for(server: SocketAddr) -> Result<Self> {
        let IpAddr::V4(server_ip) = server.ip() else {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 delegation callbacks currently require an IPv4 endpoint".into(),
            ));
        };
        let route = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
            .await
            .map_err(NfsError::Io)?;
        route
            .connect((server_ip, server.port()))
            .await
            .map_err(NfsError::Io)?;
        let IpAddr::V4(local_ip) = route.local_addr().map_err(NfsError::Io)?.ip() else {
            return Err(NfsError::Rpc(
                "NFSv4.0 callback route selected a non-IPv4 address".into(),
            ));
        };
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
            .await
            .map_err(NfsError::Io)?;
        let port = listener.local_addr().map_err(NfsError::Io)?.port();
        let universal_addr = format!(
            "{}.{}.{}.{}.{}.{}",
            local_ip.octets()[0],
            local_ip.octets()[1],
            local_ip.octets()[2],
            local_ip.octets()[3],
            port >> 8,
            port & 0xff
        );
        let (recall_tx, recall_rx) = mpsc::channel(64);
        let state = Arc::new(CallbackState {
            delegations: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            recall_tx,
            open_publications: AtomicUsize::new(0),
            grants_received: AtomicU64::new(0),
            recalls_received: AtomicU64::new(0),
            returns_completed: AtomicU64::new(0),
            returns_failed: AtomicU64::new(0),
            service_healthy: AtomicBool::new(true),
            worker_healthy: AtomicBool::new(true),
            io_gates: Mutex::new(HashMap::new()),
            replies: Mutex::new(VecDeque::new()),
        });
        let service_state = Arc::clone(&state);
        let connection_slots = Arc::new(Semaphore::new(MAX_CALLBACK_CONNECTIONS));
        let (stop, mut stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    changed = stopping.changed() => {
                        if changed.is_err() || *stopping.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, peer)) = accepted else {
                            service_state.service_healthy.store(false, Ordering::Release);
                            break;
                        };
                        if peer.ip() != IpAddr::V4(server_ip) {
                            drop(stream);
                            continue;
                        }
                        let Ok(slot) = Arc::clone(&connection_slots).try_acquire_owned() else {
                            drop(stream);
                            continue;
                        };
                        let state = Arc::clone(&service_state);
                        connections.spawn(async move {
                            let _slot = slot;
                            let _ = serve_connection(stream, state).await;
                        });
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            universal_addr,
            state,
            recall_rx: Mutex::new(Some(recall_rx)),
            task: Mutex::new(Some(task)),
            stop,
        })
    }

    pub(crate) fn universal_addr(&self) -> &str {
        &self.universal_addr
    }

    pub(crate) fn state(&self) -> Arc<CallbackState> {
        Arc::clone(&self.state)
    }

    pub(crate) fn take_recall_receiver(&self) -> Result<mpsc::Receiver<RecallNotification>> {
        self.recall_rx
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback receiver lock poisoned".into()))?
            .take()
            .ok_or_else(|| NfsError::Rpc("NFSv4.0 callback receiver already taken".into()))
    }

    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        self.state.service_healthy.store(false, Ordering::Release);
        let Some(mut task) = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        if tokio::time::timeout(CALLBACK_SERVICE_STOP_TIMEOUT, &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}

impl CallbackState {
    fn cached_reply(&self, xid: u32, call: &[u8]) -> Option<Vec<u8>> {
        self.replies.lock().ok()?.iter().find_map(|record| {
            (record.xid == xid && record.call == call).then(|| record.reply.clone())
        })
    }

    fn cache_reply(&self, xid: u32, call: &[u8], reply: &[u8]) {
        let Ok(mut replies) = self.replies.lock() else {
            return;
        };
        if replies.len() == CALLBACK_REPLAY_CACHE_SIZE {
            replies.pop_front();
        }
        replies.push_back(ReplayEntry {
            xid,
            call: call.to_vec(),
            reply: reply.to_vec(),
        });
    }

    fn io_gate(&self, fh: &Bytes) -> Arc<RwLock<()>> {
        let mut gates = self
            .io_gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(gate) = gates.get(fh).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(RwLock::new(()));
        gates.insert(fh.clone(), Arc::downgrade(&gate));
        gate
    }

    pub(crate) async fn foreground_io(&self, fh: &Bytes) -> OwnedRwLockReadGuard<()> {
        self.io_gate(fh).read_owned().await
    }

    pub(crate) async fn recall_io(&self, fh: &Bytes) -> OwnedRwLockWriteGuard<()> {
        self.io_gate(fh).write_owned().await
    }

    pub(crate) fn stats(&self) -> crate::CallbackStats {
        crate::CallbackStats {
            grants_received: self.grants_received.load(Ordering::Relaxed),
            recalls_received: self.recalls_received.load(Ordering::Relaxed),
            returns_completed: self.returns_completed.load(Ordering::Relaxed),
            returns_failed: self.returns_failed.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn healthy(&self) -> bool {
        self.service_healthy.load(Ordering::Acquire) && self.worker_healthy.load(Ordering::Acquire)
    }

    pub(crate) fn mark_attributes_unknown(&self, fh: &[u8]) -> Result<bool> {
        let mut delegations = self
            .delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
        let Some(record) = delegations.get_mut(fh) else {
            return Ok(false);
        };
        if record.grant.kind != DelegationKind::Write {
            return Ok(false);
        }
        record.attributes = None;
        Ok(true)
    }

    pub(crate) fn publish_attributes(&self, fh: &[u8], change: u64, size: u64) -> Result<()> {
        let mut delegations = self
            .delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
        if let Some(record) = delegations.get_mut(fh)
            && record.grant.kind == DelegationKind::Write
        {
            record.attributes = Some((change, size));
        }
        Ok(())
    }

    pub(crate) async fn return_all_delegations(&self) -> Result<()> {
        let notifications = {
            let mut delegations = self
                .delegations
                .lock()
                .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
            delegations
                .iter_mut()
                .filter_map(|(fh, record)| {
                    if record.recalling {
                        return None;
                    }
                    record.recalling = true;
                    Some(RecallNotification {
                        fh: fh.clone(),
                        stateid: record.grant.stateid,
                        generation: record.generation,
                        flush: record.grant.kind == DelegationKind::Write,
                    })
                })
                .collect::<Vec<_>>()
        };
        for notification in notifications {
            if self.recall_tx.send(notification).await.is_err() {
                return Err(NfsError::Rpc(
                    "NFSv4.0 callback worker stopped before delegation cleanup".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn begin_open_publication(self: &Arc<Self>) -> OpenPublication {
        self.open_publications.fetch_add(1, Ordering::AcqRel);
        OpenPublication {
            state: Arc::clone(self),
        }
    }

    pub(crate) fn register_delegation(
        &self,
        fh: Bytes,
        grant: DelegationGrant,
        generation: u64,
        attributes: Option<(u64, u64)>,
    ) -> Result<()> {
        self.grants_received.fetch_add(1, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Release);
        let recall = grant.recall;
        let stateid = grant.stateid;
        let mut delegations = self
            .delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
        delegations.insert(
            fh.clone(),
            DelegationRecord {
                grant,
                generation,
                attributes,
                recalling: recall,
            },
        );
        if recall {
            match self.recall_tx.try_send(RecallNotification {
                fh: fh.clone(),
                stateid,
                generation,
                flush: grant.kind == DelegationKind::Write,
            }) {
                Ok(()) => {
                    self.recalls_received.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    if let Some(record) = delegations.get_mut(&fh) {
                        record.recalling = false;
                    }
                    self.worker_healthy.store(false, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn invalidate_delegations(&self, generation: u64) -> Result<()> {
        self.generation.store(generation, Ordering::Release);
        self.delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn is_current(&self, recall: &RecallNotification) -> bool {
        if self.generation.load(Ordering::Acquire) != recall.generation {
            return false;
        }
        self.delegations.lock().is_ok_and(|delegations| {
            delegations.get(&recall.fh).is_some_and(|record| {
                record.generation == recall.generation && record.grant.stateid == recall.stateid
            })
        })
    }

    fn finish_recall(
        &self,
        fh: &[u8],
        stateid: &[u8; 16],
        generation: u64,
        returned: bool,
    ) -> Result<()> {
        let mut delegations = self
            .delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
        if let Some(record) = delegations.get_mut(fh)
            && record.generation == generation
            && record.grant.stateid == *stateid
        {
            if returned {
                delegations.remove(fh);
            } else {
                record.recalling = false;
            }
        }
        Ok(())
    }
}

impl Drop for OpenPublication {
    fn drop(&mut self) {
        self.state.open_publications.fetch_sub(1, Ordering::AcqRel);
    }
}

impl CallbackWorker {
    pub(crate) fn start(
        mut recalls: mpsc::Receiver<RecallNotification>,
        rpc: rpc::Client,
        auth: Auth,
        state: Arc<CallbackState>,
    ) -> Self {
        let worker_state = Arc::clone(&state);
        let (stop, mut stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut draining = false;
            loop {
                let recall = if draining {
                    recalls.recv().await
                } else {
                    tokio::select! {
                        recall = recalls.recv() => recall,
                        changed = stopping.changed() => {
                            if changed.is_err() || *stopping.borrow() {
                                recalls.close();
                                draining = true;
                            }
                            continue;
                        }
                    }
                };
                let Some(recall) = recall else { break };
                if !state.is_current(&recall) {
                    continue;
                }
                let _io = state.recall_io(&recall.fh).await;
                if !state.is_current(&recall) {
                    continue;
                }
                let returned = settle_recall(&rpc, &auth, &recall).await.is_ok();
                if returned {
                    state.returns_completed.fetch_add(1, Ordering::Relaxed);
                    state.worker_healthy.store(true, Ordering::Release);
                } else {
                    state.returns_failed.fetch_add(1, Ordering::Relaxed);
                    state.worker_healthy.store(false, Ordering::Release);
                }
                let _ =
                    state.finish_recall(&recall.fh, &recall.stateid, recall.generation, returned);
            }
        });
        Self {
            task: Mutex::new(Some(task)),
            stop,
            state: worker_state,
        }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        let Some(mut task) = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        if tokio::time::timeout(CALLBACK_WORKER_STOP_TIMEOUT, &mut task)
            .await
            .is_err()
        {
            self.state.worker_healthy.store(false, Ordering::Release);
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for CallbackWorker {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        let task = match self.task.get_mut() {
            Ok(task) => task,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(task) = task.take() {
            task.abort();
        }
    }
}

impl Drop for CallbackService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.state.service_healthy.store(false, Ordering::Release);
        let task = match self.task.get_mut() {
            Ok(task) => task,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(task) = task.take() {
            task.abort();
        }
    }
}

async fn settle_recall(rpc: &rpc::Client, auth: &Auth, recall: &RecallNotification) -> Result<()> {
    if recall.flush {
        let response = rpc
            .call(
                CompoundBuilder::new("delegation-flush")
                    .putfh(&recall.fh)
                    .commit(0, 0)
                    .encode_with_header(auth),
                ReplayPolicy::ONE_ATTEMPT,
                CALLBACK_SETTLEMENT_TIMEOUT,
            )
            .await?;
        let _ = decode_commit_response(response)?;
    }
    let response = rpc
        .call(
            CompoundBuilder::new("delegreturn")
                .putfh(&recall.fh)
                .delegreturn(&recall.stateid)
                .encode_with_header(auth),
            ReplayPolicy::ONE_ATTEMPT,
            CALLBACK_SETTLEMENT_TIMEOUT,
        )
        .await?;
    match decode_delegreturn_response(response) {
        Ok(()) => Ok(()),
        Err(NfsError::Nfs4(
            crate::Nfs4ErrorCode::NFS4ERR_ADMIN_REVOKED
            | crate::Nfs4ErrorCode::NFS4ERR_DELEG_REVOKED
            | crate::Nfs4ErrorCode::NFS4ERR_EXPIRED
            | crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID
            | crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID
            | crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID,
        )) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn serve_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<CallbackState>,
) -> Result<()> {
    serve_connection_with_idle_timeout(&mut stream, state, CALLBACK_CONNECTION_IDLE_TIMEOUT).await
}

async fn serve_connection_with_idle_timeout(
    stream: &mut tokio::net::TcpStream,
    state: Arc<CallbackState>,
    idle_timeout: Duration,
) -> Result<()> {
    loop {
        let Some(call) = tokio::time::timeout(idle_timeout, read_record(stream))
            .await
            .map_err(|_| {
                NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "NFSv4.0 callback connection idle timeout",
                ))
            })??
        else {
            return Ok(());
        };
        let reply = handle_rpc_call(&call, &state)?;
        stream
            .write_u32(0x8000_0000 | reply.len() as u32)
            .await
            .map_err(NfsError::Io)?;
        stream.write_all(&reply).await.map_err(NfsError::Io)?;
    }
}

async fn read_record(stream: &mut tokio::net::TcpStream) -> Result<Option<Vec<u8>>> {
    let mut record = Vec::new();
    loop {
        let marker = match stream.read_u32().await {
            Ok(marker) => marker,
            Err(error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof && record.is_empty() =>
            {
                return Ok(None);
            }
            Err(error) => return Err(NfsError::Io(error)),
        };
        let length = (marker & 0x7fff_ffff) as usize;
        if record.len().saturating_add(length) > MAX_CALLBACK_RECORD {
            return Err(NfsError::Xdr(
                "NFSv4.0 callback RPC record exceeds the configured bound".into(),
            ));
        }
        let start = record.len();
        record.resize(start + length, 0);
        stream
            .read_exact(&mut record[start..])
            .await
            .map_err(NfsError::Io)?;
        if marker & 0x8000_0000 != 0 {
            return Ok(Some(record));
        }
    }
}

fn handle_rpc_call(call: &[u8], state: &CallbackState) -> Result<Vec<u8>> {
    if call.len() < 24 || !call.len().is_multiple_of(4) {
        return Err(NfsError::Xdr(
            "NFSv4.0 callback RPC call has an invalid length".into(),
        ));
    }
    let words: Vec<u32> = call[..24]
        .chunks_exact(4)
        .map(|word| u32::from_be_bytes([word[0], word[1], word[2], word[3]]))
        .collect();
    if let Some(reply) = state.cached_reply(words[0], call) {
        return Ok(reply);
    }
    if words[1] != 0 {
        return Err(NfsError::Rpc(
            "NFSv4.0 callback RPC message is not a CALL".into(),
        ));
    }
    if words[2] != 2 {
        return Ok(rpc_reply(&[words[0], 1, 1, 0, 2, 2]));
    }
    if words[3] != CB_PROGRAM {
        return Ok(accepted_reply(words[0], &[1]));
    }
    if words[4] != 1 {
        return Ok(accepted_reply(words[0], &[2, 1, 1]));
    }
    if words[5] > 1 {
        return Ok(accepted_reply(words[0], &[3]));
    }
    let Some(RpcAuthEnvelope {
        credential_flavor,
        credential,
        verifier_flavor,
        verifier,
        body,
    }) = decode_rpc_auth(&call[24..])
    else {
        return Ok(rpc_reply(&[words[0], 1, 1, 1, 1]));
    };
    if !valid_callback_credential(credential_flavor, credential)
        || !valid_callback_verifier(verifier_flavor, verifier)
    {
        return Ok(rpc_reply(&[words[0], 1, 1, 1, 1]));
    }
    if words[5] == 0 {
        return Ok(if body.is_empty() {
            accepted_reply(words[0], &[0])
        } else {
            accepted_reply(words[0], &[4])
        });
    }
    let compound = match compound_reply(body, state) {
        Ok(compound) => compound,
        Err(_) => return Ok(accepted_reply(words[0], &[4])),
    };
    let reply = accepted_body(words[0], &compound);
    if successful_single_recall(body, &compound) {
        state.cache_reply(words[0], call, &reply);
    }
    Ok(reply)
}

fn valid_callback_verifier(flavor: u32, verifier: &[u8]) -> bool {
    // RFC 5531 recommends AUTH_NONE here. ONTAP's NFSv4.0 callback
    // implementation sends an AUTH_SYS verifier containing one XDR word;
    // accept only that observed, bounded compatibility form. The callback
    // listener separately pins the TCP peer to the mounted server LIF.
    (flavor == 0 && verifier.is_empty()) || (flavor == 1 && verifier.len() == 4)
}

fn successful_single_recall(body: &[u8], reply: &[u8]) -> bool {
    let Some(tag_length) = body
        .get(..4)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes)
        .map(|value| value as usize)
    else {
        return false;
    };
    let Some(padded_tag) = tag_length.checked_add(3).map(|length| length & !3) else {
        return false;
    };
    let Some(minor_offset) = 4usize.checked_add(padded_tag) else {
        return false;
    };
    let operation_count = body
        .get(minor_offset + 8..minor_offset + 12)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes);
    let opcode = body
        .get(minor_offset + 12..minor_offset + 16)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes);
    let status = reply
        .get(..4)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes);
    operation_count == Some(1) && opcode == Some(4) && status == Some(0)
}

struct RpcAuthEnvelope<'a> {
    credential_flavor: u32,
    credential: &'a [u8],
    verifier_flavor: u32,
    verifier: &'a [u8],
    body: &'a [u8],
}

fn decode_rpc_auth(mut encoded: &[u8]) -> Option<RpcAuthEnvelope<'_>> {
    fn take_auth<'a>(encoded: &mut &'a [u8]) -> Option<(u32, &'a [u8])> {
        let flavor = u32::from_be_bytes(encoded.get(..4)?.try_into().ok()?);
        let length = u32::from_be_bytes(encoded.get(4..8)?.try_into().ok()?) as usize;
        if length > 400 {
            return None;
        }
        let padded = length.checked_add(3)? & !3;
        let value_end = 8usize.checked_add(length)?;
        let padded_end = 8usize.checked_add(padded)?;
        let value = encoded.get(8..value_end)?;
        *encoded = encoded.get(padded_end..)?;
        Some((flavor, value))
    }

    let (credential_flavor, credential) = take_auth(&mut encoded)?;
    let (verifier_flavor, verifier) = take_auth(&mut encoded)?;
    Some(RpcAuthEnvelope {
        credential_flavor,
        credential,
        verifier_flavor,
        verifier,
        body: encoded,
    })
}

fn valid_callback_credential(flavor: u32, mut credential: &[u8]) -> bool {
    if flavor == 0 {
        return credential.is_empty();
    }
    if flavor != 1 || credential.len() < 20 || !credential.len().is_multiple_of(4) {
        return false;
    }
    credential = &credential[4..]; // stamp is opaque to the receiver
    let machine_length = u32::from_be_bytes(
        match credential.get(..4).and_then(|value| value.try_into().ok()) {
            Some(value) => value,
            None => return false,
        },
    ) as usize;
    if machine_length > 255 {
        return false;
    }
    let Some(machine_padded) = machine_length.checked_add(3).map(|length| length & !3) else {
        return false;
    };
    let Some(after_machine) = 4usize.checked_add(machine_padded) else {
        return false;
    };
    let Some(tail) = credential.get(after_machine..) else {
        return false;
    };
    if tail.len() < 12 {
        return false;
    }
    let group_count = u32::from_be_bytes(
        match tail.get(8..12).and_then(|value| value.try_into().ok()) {
            Some(value) => value,
            None => return false,
        },
    ) as usize;
    group_count <= 16 && tail.len() == 12 + group_count * 4
}

fn compound_reply(body: &[u8], state: &CallbackState) -> Result<Vec<u8>> {
    if body.len() < 16 {
        return Err(NfsError::Xdr("CB_COMPOUND envelope truncated".into()));
    }
    let tag_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let padded_tag = tag_len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("CB_COMPOUND tag length overflow".into()))?
        & !3;
    let operations_offset = 4usize
        .checked_add(padded_tag)
        .and_then(|length| length.checked_add(12))
        .ok_or_else(|| NfsError::Xdr("CB_COMPOUND envelope length overflow".into()))?;
    if body.len() < operations_offset || body.len() < 4 + tag_len {
        return Err(NfsError::Xdr("CB_COMPOUND envelope malformed".into()));
    }
    let minor_offset = 4 + padded_tag;
    let minor = u32::from_be_bytes(
        body[minor_offset..minor_offset + 4]
            .try_into()
            .map_err(|_| NfsError::Xdr("CB_COMPOUND minorversion truncated".into()))?,
    );
    let callback_ident = u32::from_be_bytes(
        body[minor_offset + 4..minor_offset + 8]
            .try_into()
            .map_err(|_| NfsError::Xdr("CB_COMPOUND callback ident truncated".into()))?,
    );
    let operation_count = u32::from_be_bytes(
        body[minor_offset + 8..minor_offset + 12]
            .try_into()
            .map_err(|_| NfsError::Xdr("CB_COMPOUND operation count truncated".into()))?,
    );
    let status = if minor == 0 { 0u32 } else { 10021u32 };
    if minor != 0 {
        return Ok(compound_result(
            status,
            &body[4..4 + padded_tag],
            tag_len,
            0,
            &[],
        ));
    }
    if callback_ident != 1 {
        return Ok(compound_result(
            10001,
            &body[4..4 + padded_tag],
            tag_len,
            0,
            &[],
        ));
    }
    if operation_count > MAX_CALLBACK_OPERATIONS {
        return Ok(compound_result(
            10018,
            &body[4..4 + padded_tag],
            tag_len,
            0,
            &[],
        ));
    }
    let mut cursor = operations_offset;
    let mut results = Vec::new();
    let mut status = 0u32;
    let mut result_count = 0u32;
    for _ in 0..operation_count {
        let opcode = take_word(body, &mut cursor, "callback opcode")?;
        result_count += 1;
        match opcode {
            3 => {
                let fh = take_opaque(body, &mut cursor, "CB_GETATTR filehandle")?;
                let bitmap_words = take_word(body, &mut cursor, "CB_GETATTR bitmap length")?;
                if bitmap_words > 4 {
                    return Err(NfsError::Xdr("CB_GETATTR bitmap exceeds bound".into()));
                }
                let mut requested = 0u32;
                for _ in 0..bitmap_words {
                    let word = take_word(body, &mut cursor, "CB_GETATTR bitmap word")?;
                    if requested == 0 {
                        requested = word;
                    }
                }
                let delegation = state
                    .delegations
                    .lock()
                    .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
                let record = delegation.get(fh);
                let active_generation = state.generation.load(Ordering::Acquire);
                if let Some(DelegationRecord {
                    grant:
                        DelegationGrant {
                            kind: DelegationKind::Write,
                            ..
                        },
                    attributes: Some((change, size)),
                    generation,
                    ..
                }) = record
                    && *generation == active_generation
                {
                    let returned = requested & 0x18;
                    status = 0;
                    results.extend_from_slice(&3u32.to_be_bytes());
                    results.extend_from_slice(&0u32.to_be_bytes());
                    results.extend_from_slice(&1u32.to_be_bytes());
                    results.extend_from_slice(&returned.to_be_bytes());
                    let value_length = u32::from(returned & 0x08 != 0)
                        .saturating_add(u32::from(returned & 0x10 != 0))
                        * 8;
                    results.extend_from_slice(&value_length.to_be_bytes());
                    if returned & 0x08 != 0 {
                        results.extend_from_slice(&change.to_be_bytes());
                    }
                    if returned & 0x10 != 0 {
                        results.extend_from_slice(&size.to_be_bytes());
                    }
                } else {
                    status = if matches!(
                        record,
                        Some(DelegationRecord {
                            grant: DelegationGrant {
                                kind: DelegationKind::Write,
                                ..
                            },
                            attributes: None,
                            ..
                        })
                    ) {
                        10008
                    } else {
                        10001
                    };
                    results.extend_from_slice(&3u32.to_be_bytes());
                    results.extend_from_slice(&status.to_be_bytes());
                }
            }
            4 => {
                let stateid = take_fixed(body, &mut cursor, 16, "CB_RECALL stateid")?;
                let truncate = take_word(body, &mut cursor, "CB_RECALL truncate")? != 0;
                let fh = take_opaque(body, &mut cursor, "CB_RECALL filehandle")?;
                let active_generation = state.generation.load(Ordering::Acquire);
                let mut delegations = state
                    .delegations
                    .lock()
                    .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?;
                status = match delegations.get_mut(fh) {
                    None if state.open_publications.load(Ordering::Acquire) != 0 => 10008,
                    None => 10001,
                    Some(record) if record.generation != active_generation => 10001,
                    Some(record) if record.grant.stateid.as_slice() != stateid => 10025,
                    Some(record) if record.recalling => 0,
                    Some(record) => {
                        let mut callback_stateid = [0; 16];
                        callback_stateid.copy_from_slice(stateid);
                        match state.recall_tx.try_send(RecallNotification {
                            fh: Bytes::copy_from_slice(fh),
                            stateid: callback_stateid,
                            generation: record.generation,
                            flush: record.grant.kind == DelegationKind::Write && !truncate,
                        }) {
                            Ok(()) => {
                                record.recalling = true;
                                state.recalls_received.fetch_add(1, Ordering::Relaxed);
                                0
                            }
                            Err(_) => 10008,
                        }
                    }
                };
                results.extend_from_slice(&4u32.to_be_bytes());
                results.extend_from_slice(&status.to_be_bytes());
            }
            illegal_opcode => {
                status = 10044;
                results.extend_from_slice(&illegal_opcode.to_be_bytes());
                results.extend_from_slice(&status.to_be_bytes());
            }
        }
        if status != 0 {
            break;
        }
    }
    if status == 0 && cursor != body.len() {
        return Err(NfsError::Xdr("CB_COMPOUND has trailing data".into()));
    }
    Ok(compound_result(
        status,
        &body[4..4 + padded_tag],
        tag_len,
        result_count,
        &results,
    ))
}

fn compound_result(
    status: u32,
    padded_tag: &[u8],
    tag_len: usize,
    result_count: u32,
    results: &[u8],
) -> Vec<u8> {
    let mut reply = Vec::with_capacity(12 + padded_tag.len() + results.len());
    reply.extend_from_slice(&status.to_be_bytes());
    reply.extend_from_slice(&(tag_len as u32).to_be_bytes());
    reply.extend_from_slice(padded_tag);
    reply.extend_from_slice(&result_count.to_be_bytes());
    reply.extend_from_slice(results);
    reply
}

fn take_word(body: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| NfsError::Xdr(format!("{field} offset overflow")))?;
    let bytes = body
        .get(*cursor..end)
        .ok_or_else(|| NfsError::Xdr(format!("{field} truncated")))?;
    *cursor = end;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn take_opaque<'a>(body: &'a [u8], cursor: &mut usize, field: &str) -> Result<&'a [u8]> {
    let length = take_word(body, cursor, field)? as usize;
    let padded = length
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr(format!("{field} length overflow")))?
        & !3;
    let end = cursor
        .checked_add(padded)
        .ok_or_else(|| NfsError::Xdr(format!("{field} offset overflow")))?;
    let value_end = cursor
        .checked_add(length)
        .ok_or_else(|| NfsError::Xdr(format!("{field} value overflow")))?;
    let value = body
        .get(*cursor..value_end)
        .ok_or_else(|| NfsError::Xdr(format!("{field} truncated")))?;
    if end > body.len() {
        return Err(NfsError::Xdr(format!("{field} padding truncated")));
    }
    *cursor = end;
    Ok(value)
}

fn take_fixed<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    length: usize,
    field: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| NfsError::Xdr(format!("{field} offset overflow")))?;
    let value = body
        .get(*cursor..end)
        .ok_or_else(|| NfsError::Xdr(format!("{field} truncated")))?;
    *cursor = end;
    Ok(value)
}

fn accepted_reply(xid: u32, result: &[u32]) -> Vec<u8> {
    let mut words = Vec::with_capacity(5 + result.len());
    words.extend_from_slice(&[xid, 1, 0, 0, 0]);
    words.extend_from_slice(result);
    rpc_reply(&words)
}

fn accepted_body(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut reply = rpc_reply(&[xid, 1, 0, 0, 0, 0]);
    reply.extend_from_slice(body);
    reply
}

fn rpc_reply(words: &[u32]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(words.len() * 4);
    for word in words {
        reply.extend_from_slice(&word.to_be_bytes());
    }
    reply
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::nfs40::compound::{DelegationGrant, DelegationKind};
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn compound_response(tag: &[u8], opcode: u32, data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&(tag.len() as u32).to_be_bytes());
        body.extend_from_slice(tag);
        body.resize(body.len().next_multiple_of(4), 0);
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(&22u32.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&opcode.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(data);
        body
    }

    fn compound_error(tag: &[u8], status: u32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&status.to_be_bytes());
        body.extend_from_slice(&(tag.len() as u32).to_be_bytes());
        body.extend_from_slice(tag);
        body.resize(body.len().next_multiple_of(4), 0);
        body.extend_from_slice(&0u32.to_be_bytes());
        body
    }

    async fn nfs_reply(stream: &mut tokio::net::TcpStream, request: &[u8], body: &[u8]) {
        let mut reply = Vec::new();
        reply.extend_from_slice(&request[..4]);
        for word in [1u32, 0, 0, 0, 0] {
            reply.extend_from_slice(&word.to_be_bytes());
        }
        reply.extend_from_slice(body);
        stream
            .write_u32(0x8000_0000 | reply.len() as u32)
            .await
            .unwrap();
        stream.write_all(&reply).await.unwrap();
    }

    fn socket_addr(universal: &str) -> SocketAddr {
        let fields: Vec<u16> = universal
            .split('.')
            .map(|field| field.parse().unwrap())
            .collect();
        SocketAddr::from((
            [
                fields[0] as u8,
                fields[1] as u8,
                fields[2] as u8,
                fields[3] as u8,
            ],
            fields[4] * 256 + fields[5],
        ))
    }

    fn cb_null_call(xid: u32) -> Vec<u32> {
        vec![xid, 0, 2, CB_PROGRAM, 1, 0, 0, 0, 0, 0]
    }

    async fn round_trip(stream: &mut tokio::net::TcpStream, words: &[u32]) -> Vec<u32> {
        let mut call = Vec::new();
        for word in words {
            call.extend_from_slice(&word.to_be_bytes());
        }
        stream
            .write_u32(0x8000_0000 | call.len() as u32)
            .await
            .unwrap();
        stream.write_all(&call).await.unwrap();
        let marker = stream.read_u32().await.unwrap();
        assert_eq!(marker & 0x8000_0000, 0x8000_0000);
        let mut reply = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut reply).await.unwrap();
        reply
            .chunks_exact(4)
            .map(|word| u32::from_be_bytes(word.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn rfc7531_cb_null_golden_vector_round_trips() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let call = [0x1020_3040, 0, 2, CB_PROGRAM, 1, 0, 0, 0, 0, 0];
        let words = round_trip(&mut stream, &call).await;
        assert_eq!(words, [0x1020_3040, 1, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn stopping_callback_service_closes_listener_and_connections() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let address = socket_addr(service.universal_addr());
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &cb_null_call(5)).await,
            [5, 1, 0, 0, 0, 0]
        );

        service.stop().await;

        assert!(!service.state().healthy());
        let mut byte = [0; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn idle_callback_connection_is_closed_at_its_deadline() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = service.state();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            serve_connection_with_idle_timeout(&mut stream, state, Duration::from_millis(20)).await
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();

        let error = task.await.unwrap().unwrap_err();
        assert!(matches!(error, NfsError::Io(ref io) if io.kind() == std::io::ErrorKind::TimedOut));
        let mut byte = [0; 1];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
        service.stop().await;
    }

    #[tokio::test]
    async fn callback_service_rejects_connections_above_its_bound() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let address = socket_addr(service.universal_addr());
        let mut retained = Vec::new();
        for xid in 0..MAX_CALLBACK_CONNECTIONS {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            assert_eq!(
                round_trip(&mut stream, &cb_null_call(xid as u32)).await,
                [xid as u32, 1, 0, 0, 0, 0]
            );
            retained.push(stream);
        }

        let mut excess = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut byte = [0; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .expect("excess callback connection was not rejected")
            .unwrap();
        assert_eq!(read, 0);
        drop(retained);
        service.stop().await;
    }

    #[tokio::test]
    async fn cb_null_reports_precise_rpc_header_failures() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        for (index, value, expected) in [
            (2, 3, vec![7, 1, 1, 0, 2, 2]),
            (3, CB_PROGRAM + 1, vec![7, 1, 0, 0, 0, 1]),
            (4, 2, vec![7, 1, 0, 0, 0, 2, 1, 1]),
            (5, 2, vec![7, 1, 0, 0, 0, 3]),
        ] {
            let mut call = cb_null_call(7);
            call[index] = value;
            assert_eq!(round_trip(&mut stream, &call).await, expected);
        }
    }

    #[tokio::test]
    async fn callback_auth_failures_are_denied_without_poisoning_the_connection() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();

        let mut unsupported = cb_null_call(13);
        unsupported[6] = 1;
        assert_eq!(
            round_trip(&mut stream, &unsupported).await,
            [13, 1, 1, 1, 1]
        );
        let mut malformed = cb_null_call(14);
        malformed[7] = 4;
        assert_eq!(round_trip(&mut stream, &malformed).await, [14, 1, 1, 1, 1]);
        assert_eq!(
            round_trip(&mut stream, &cb_null_call(15)).await,
            [15, 1, 0, 0, 0, 0]
        );

        let auth_sys = vec![
            16,
            0,
            2,
            CB_PROGRAM,
            1,
            0,
            1,
            28,
            9, // stamp
            3, // machine-name length
            u32::from_be_bytes(*b"fas\0"),
            0, // uid
            0, // gid
            1, // auxiliary group count
            0, // group
            0, // AUTH_NONE verifier
            0,
        ];
        assert_eq!(
            round_trip(&mut stream, &auth_sys).await,
            [16, 1, 0, 0, 0, 0]
        );

        let mut ontap_auth_sys = auth_sys.clone();
        ontap_auth_sys.splice(15..17, [1, 4, 1]);
        ontap_auth_sys[0] = 17;
        assert_eq!(
            round_trip(&mut stream, &ontap_auth_sys).await,
            [17, 1, 0, 0, 0, 0]
        );

        let minimal_ontap_auth_sys = vec![18, 0, 2, CB_PROGRAM, 1, 0, 1, 20, 9, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            round_trip(&mut stream, &minimal_ontap_auth_sys).await,
            [18, 1, 0, 0, 0, 0]
        );

        let mut excessive_groups = auth_sys;
        excessive_groups[13] = 17;
        assert_eq!(
            round_trip(&mut stream, &excessive_groups).await,
            [16, 1, 1, 1, 1]
        );
    }

    #[tokio::test]
    async fn callback_listener_rejects_a_different_source_address() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.2:0".parse().unwrap()).unwrap();
        let mut stream = socket
            .connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = Vec::new();
        for word in cb_null_call(41) {
            call.extend_from_slice(&word.to_be_bytes());
        }
        let write = async {
            stream.write_u32(0x8000_0000 | call.len() as u32).await?;
            stream.write_all(&call).await
        }
        .await;
        if write.is_ok() {
            let read = tokio::time::timeout(Duration::from_millis(100), stream.read_u32()).await;
            assert!(
                !matches!(read, Ok(Ok(_))),
                "unauthorized callback source received an RPC response"
            );
        }
    }

    #[tokio::test]
    async fn rfc7531_cb_compound_golden_vector_and_minor_version_rejection() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let golden = [
            11,
            0,
            2,
            CB_PROGRAM,
            1,
            1,
            0,
            0,
            0,
            0,
            3,
            u32::from_be_bytes(*b"tag\0"),
            0,
            1,
            0,
        ];
        assert_eq!(
            round_trip(&mut stream, &golden).await,
            [11, 1, 0, 0, 0, 0, 0, 3, u32::from_be_bytes(*b"tag\0"), 0]
        );
        for (minor, expected_status) in [(1, 10021)] {
            let mut call = cb_null_call(11 + minor);
            call[5] = 1;
            call.extend_from_slice(&[3, u32::from_be_bytes(*b"tag\0"), minor, 1, 0]);
            let reply = round_trip(&mut stream, &call).await;
            assert_eq!(&reply[..6], &[11 + minor, 1, 0, 0, 0, 0]);
            assert_eq!(reply[6], expected_status);
            assert_eq!(reply[7], 3);
            assert_eq!(reply[8], u32::from_be_bytes(*b"tag\0"));
            assert_eq!(reply[9], 0);
        }
    }

    #[tokio::test]
    async fn cb_compound_bounds_operations_and_reports_the_illegal_opcode() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();

        let mut excessive = cb_null_call(17);
        excessive[5] = 1;
        excessive.extend_from_slice(&[0, 0, 1, MAX_CALLBACK_OPERATIONS + 1]);
        assert_eq!(
            round_trip(&mut stream, &excessive).await,
            [17, 1, 0, 0, 0, 0, 10018, 0, 0]
        );

        let mut illegal = cb_null_call(18);
        illegal[5] = 1;
        illegal.extend_from_slice(&[0, 0, 1, 1, 99]);
        assert_eq!(
            round_trip(&mut stream, &illegal).await,
            [18, 1, 0, 0, 0, 0, 10044, 0, 1, 99, 10044]
        );
    }

    #[tokio::test]
    async fn rfc7531_cb_getattr_golden_vector_rejects_a_missing_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let call = [
            19,
            0,
            2,
            CB_PROGRAM,
            1,
            1,
            0,
            0,
            0,
            0,
            0, // empty tag
            0, // minor version
            1, // callback ident
            1, // operation count
            3, // OP_CB_GETATTR
            2, // filehandle length
            u32::from_be_bytes(*b"fh\0\0"),
            1,    // bitmap word count
            0x18, // change + size
        ];
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [19, 1, 0, 0, 0, 0, 10001, 0, 1, 3, 10001]
        );
    }

    #[tokio::test]
    async fn rfc7531_cb_recall_golden_vector_rejects_a_missing_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let call = [
            23,
            0,
            2,
            CB_PROGRAM,
            1,
            1,
            0,
            0,
            0,
            0,
            0, // empty tag
            0, // minor version
            1, // callback ident
            1, // operation count
            4, // OP_CB_RECALL
            0x0102_0304,
            0x0506_0708,
            0x090a_0b0c,
            0x0d0e_0f10,
            0, // truncate
            2, // filehandle length
            u32::from_be_bytes(*b"fh\0\0"),
        ];
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [23, 1, 0, 0, 0, 0, 10001, 0, 1, 4, 10001]
        );
    }

    #[tokio::test]
    async fn cb_compound_rejects_the_wrong_callback_ident_before_side_effects() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut recalls = service.take_recall_receiver().unwrap();
        service
            .state()
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Read,
                    stateid: [0x44; 16],
                    recall: false,
                },
                7,
                None,
            )
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(27);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 2, 1, 4]);
        call.extend_from_slice(&[0x4444_4444; 4]);
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);

        assert_eq!(
            round_trip(&mut stream, &call).await,
            [27, 1, 0, 0, 0, 0, 10001, 0, 0]
        );
        assert!(recalls.try_recv().is_err());
        assert_eq!(
            service.state().stats(),
            crate::CallbackStats {
                grants_received: 1,
                ..crate::CallbackStats::default()
            }
        );
    }

    #[tokio::test]
    async fn cb_getattr_returns_cached_change_and_size_for_a_write_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        service
            .state()
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Write,
                    stateid: [0x44; 16],
                    recall: false,
                },
                7,
                Some((0x0102_0304_0506_0708, 0x1112_1314_1516_1718)),
            )
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(29);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 3, 2, u32::from_be_bytes(*b"fh\0\0"), 1, 0x18]);
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [
                29,
                1,
                0,
                0,
                0,
                0, // RPC accepted
                0,
                0,
                1, // CB_COMPOUND success, empty tag, one result
                3,
                0, // CB_GETATTR success
                1,
                0x18,
                16, // fattr bitmap and value length
                0x0102_0304,
                0x0506_0708,
                0x1112_1314,
                0x1516_1718,
            ]
        );
        service.state().mark_attributes_unknown(b"fh").unwrap();
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [29, 1, 0, 0, 0, 0, 10008, 0, 1, 3, 10008]
        );
        service.state().publish_attributes(b"fh", 9, 10).unwrap();
        let refreshed = round_trip(&mut stream, &call).await;
        assert_eq!(&refreshed[14..], &[0, 9, 0, 10]);
    }

    #[tokio::test]
    async fn duplicate_cb_recall_is_acknowledged_and_queued_once() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut recalls = service.take_recall_receiver().unwrap();
        service
            .state()
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Read,
                    stateid: [0x44; 16],
                    recall: false,
                },
                7,
                None,
            )
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(31);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 4]);
        for chunk in [0x4444_4444u32; 4] {
            call.push(chunk);
        }
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);
        let expected = [31, 1, 0, 0, 0, 0, 0, 0, 1, 4, 0];
        assert_eq!(round_trip(&mut stream, &call).await, expected);
        assert_eq!(round_trip(&mut stream, &call).await, expected);

        let recall = recalls.try_recv().unwrap();
        assert_eq!(recall.fh, Bytes::from_static(b"fh"));
        assert_eq!(recall.stateid, [0x44; 16]);
        assert_eq!(recall.generation, 7);
        assert!(recalls.try_recv().is_err());
        assert_eq!(
            service.state().stats(),
            crate::CallbackStats {
                grants_received: 1,
                recalls_received: 1,
                returns_completed: 0,
                returns_failed: 0,
            }
        );
    }

    #[tokio::test]
    async fn delegation_granted_with_recall_is_queued_immediately() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut recalls = service.take_recall_receiver().unwrap();
        service
            .state()
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Write,
                    stateid: [0x62; 16],
                    recall: true,
                },
                3,
                Some((4, 5)),
            )
            .unwrap();

        let recall = recalls.try_recv().unwrap();
        assert_eq!(recall.fh, Bytes::from_static(b"fh"));
        assert_eq!(recall.stateid, [0x62; 16]);
        assert!(recall.flush);
        assert_eq!(service.state().stats().recalls_received, 1);
    }

    #[tokio::test]
    async fn shutdown_cleanup_queues_every_unrecalled_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        let mut recalls = service.take_recall_receiver().unwrap();
        for (fh, kind, stateid) in [
            (b"read".as_slice(), DelegationKind::Read, [0x31; 16]),
            (b"write".as_slice(), DelegationKind::Write, [0x32; 16]),
        ] {
            state
                .register_delegation(
                    Bytes::copy_from_slice(fh),
                    DelegationGrant {
                        kind,
                        stateid,
                        recall: false,
                    },
                    8,
                    None,
                )
                .unwrap();
        }

        state.return_all_delegations().await.unwrap();

        let mut returned = [recalls.try_recv().unwrap(), recalls.try_recv().unwrap()]
            .map(|notification| notification.fh);
        returned.sort();
        assert_eq!(
            returned,
            [Bytes::from_static(b"read"), Bytes::from_static(b"write")]
        );
        assert!(recalls.try_recv().is_err());
        assert_eq!(state.stats().recalls_received, 0);
    }

    #[tokio::test]
    async fn per_file_gate_serializes_recall_after_foreground_io() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        let first = state.foreground_io(&Bytes::from_static(b"same")).await;
        let second = state.foreground_io(&Bytes::from_static(b"same")).await;
        let waiting_state = Arc::clone(&state);
        let recall =
            tokio::spawn(
                async move { waiting_state.recall_io(&Bytes::from_static(b"same")).await },
            );
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!recall.is_finished());
        drop(first);
        assert!(!recall.is_finished());
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), recall)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn per_file_gate_keeps_different_files_independent() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        let _foreground = state.foreground_io(&Bytes::from_static(b"a")).await;
        tokio::time::timeout(
            Duration::from_secs(1),
            state.recall_io(&Bytes::from_static(b"b")),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn full_recall_queue_delays_without_consuming_the_recall() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        let mut recalls = service.take_recall_receiver().unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();

        for index in 0u8..=64 {
            state
                .register_delegation(
                    Bytes::from(vec![index]),
                    DelegationGrant {
                        kind: DelegationKind::Read,
                        stateid: [index; 16],
                        recall: false,
                    },
                    7,
                    None,
                )
                .unwrap();
            let mut call = cb_null_call(u32::from(index) + 100);
            call[5] = 1;
            call.extend_from_slice(&[0, 0, 1, 1, 4]);
            call.extend_from_slice(&[u32::from_be_bytes([index; 4]); 4]);
            call.extend_from_slice(&[0, 1, u32::from(index) << 24]);
            let reply = round_trip(&mut stream, &call).await;
            if index < 64 {
                assert_eq!(reply[6], 0);
            } else {
                assert_eq!(reply, [164, 1, 0, 0, 0, 0, 10008, 0, 1, 4, 10008]);
            }
        }
        assert_eq!(state.stats().recalls_received, 64);

        recalls.try_recv().unwrap();
        let mut retry = cb_null_call(200);
        retry[5] = 1;
        retry.extend_from_slice(&[0, 0, 1, 1, 4]);
        retry.extend_from_slice(&[u32::from_be_bytes([64; 4]); 4]);
        retry.extend_from_slice(&[0, 1, 64 << 24]);
        assert_eq!(round_trip(&mut stream, &retry).await[6], 0);
        assert_eq!(state.stats().recalls_received, 65);
    }

    #[tokio::test]
    async fn recall_racing_open_publication_delays_then_queues_once() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        let publication = state.begin_open_publication();
        let mut recalls = service.take_recall_receiver().unwrap();
        let mut callback = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(35);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 4]);
        call.extend_from_slice(&[0x7777_7777; 4]);
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);
        assert_eq!(
            round_trip(&mut callback, &call).await,
            [35, 1, 0, 0, 0, 0, 10008, 0, 1, 4, 10008]
        );
        state
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Read,
                    stateid: [0x77; 16],
                    recall: false,
                },
                11,
                None,
            )
            .unwrap();
        drop(publication);
        assert_eq!(
            round_trip(&mut callback, &call).await,
            [35, 1, 0, 0, 0, 0, 0, 0, 1, 4, 0]
        );
        assert!(recalls.try_recv().is_ok());
        assert!(recalls.try_recv().is_err());
    }

    #[tokio::test]
    async fn recovery_generation_invalidates_old_delegation_callbacks() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let state = service.state();
        state
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Read,
                    stateid: [0x33; 16],
                    recall: false,
                },
                4,
                None,
            )
            .unwrap();
        state.invalidate_delegations(5).unwrap();
        let mut callback = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(43);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 4]);
        call.extend_from_slice(&[0x3333_3333; 4]);
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);
        assert_eq!(
            round_trip(&mut callback, &call).await,
            [43, 1, 0, 0, 0, 0, 10001, 0, 1, 4, 10001]
        );
    }

    #[tokio::test]
    async fn lost_callback_reply_retries_but_returns_the_delegation_exactly_once() {
        let nfs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = nfs_listener.local_addr().unwrap();
        let mux = rpc::StreamMux::connect(server_addr, true).await.unwrap();
        let rpc = rpc::Client::new(mux, None);
        let service = CallbackService::bind_for(server_addr).await.unwrap();
        let state = service.state();
        let worker = CallbackWorker::start(
            service.take_recall_receiver().unwrap(),
            rpc,
            Auth::new_null(),
            Arc::clone(&state),
        );
        state
            .register_delegation(
                Bytes::from_static(b"fh"),
                DelegationGrant {
                    kind: DelegationKind::Write,
                    stateid: [0x55; 16],
                    recall: false,
                },
                9,
                Some((1, 2)),
            )
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = nfs_listener.accept().await.unwrap();
            let commit = read_record(&mut stream).await.unwrap().unwrap();
            assert!(commit.windows(4).any(|word| word == 5u32.to_be_bytes()));
            nfs_reply(
                &mut stream,
                &commit,
                &compound_response(b"delegation-flush", 5, &[0x66; 8]),
            )
            .await;
            let delegreturn = read_record(&mut stream).await.unwrap().unwrap();
            assert!(
                delegreturn
                    .windows(4)
                    .any(|word| word == 8u32.to_be_bytes())
            );
            nfs_reply(
                &mut stream,
                &delegreturn,
                &compound_response(b"delegreturn", 8, &[]),
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                    .await
                    .is_err(),
                "duplicate callback scheduled a second return"
            );
        });

        let callback_addr = socket_addr(service.universal_addr());
        let mut callback = tokio::net::TcpStream::connect(callback_addr).await.unwrap();
        let mut call = cb_null_call(37);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 4]);
        call.extend_from_slice(&[0x5555_5555; 4]);
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);
        let expected = [37, 1, 0, 0, 0, 0, 0, 0, 1, 4, 0];
        let mut encoded = Vec::new();
        for word in &call {
            encoded.extend_from_slice(&word.to_be_bytes());
        }
        callback
            .write_u32(0x8000_0000 | encoded.len() as u32)
            .await
            .unwrap();
        callback.write_all(&encoded).await.unwrap();
        drop(callback); // Simulate loss of the successful callback reply.
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.stats().recalls_received == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut callback = tokio::net::TcpStream::connect(callback_addr).await.unwrap();
        assert_eq!(round_trip(&mut callback, &call).await, expected);
        server.await.unwrap();
        assert_eq!(
            state.stats(),
            crate::CallbackStats {
                grants_received: 1,
                recalls_received: 1,
                returns_completed: 1,
                returns_failed: 0,
            }
        );
        assert!(state.healthy());
        worker.stop().await;
    }

    #[tokio::test]
    async fn revoked_delegation_is_discarded_without_retrying_the_return() {
        assert!(matches!(
            decode_delegreturn_response(Bytes::from(compound_error(
                b"delegreturn",
                crate::Nfs4ErrorCode::NFS4ERR_ADMIN_REVOKED as u32,
            ))),
            Err(NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_ADMIN_REVOKED))
        ));
        let nfs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = nfs_listener.local_addr().unwrap();
        let mux = rpc::StreamMux::connect(server_addr, true).await.unwrap();
        let rpc = rpc::Client::new(mux, None);
        let service = CallbackService::bind_for(server_addr).await.unwrap();
        let state = service.state();
        let worker = CallbackWorker::start(
            service.take_recall_receiver().unwrap(),
            rpc,
            Auth::new_null(),
            Arc::clone(&state),
        );
        state
            .register_delegation(
                Bytes::from_static(b"revoked"),
                DelegationGrant {
                    kind: DelegationKind::Read,
                    stateid: [0x73; 16],
                    recall: true,
                },
                12,
                None,
            )
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = nfs_listener.accept().await.unwrap();
            let delegreturn = read_record(&mut stream).await.unwrap().unwrap();
            nfs_reply(
                &mut stream,
                &delegreturn,
                &compound_error(
                    b"delegreturn",
                    crate::Nfs4ErrorCode::NFS4ERR_ADMIN_REVOKED as u32,
                ),
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                    .await
                    .is_err()
            );
        });

        server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.stats().returns_completed + state.stats().returns_failed == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            state.stats(),
            crate::CallbackStats {
                grants_received: 1,
                recalls_received: 1,
                returns_completed: 1,
                returns_failed: 0,
            }
        );
        assert!(!state.is_current(&RecallNotification {
            fh: Bytes::from_static(b"revoked"),
            stateid: [0x73; 16],
            generation: 12,
            flush: false,
        }));
        worker.stop().await;
    }
}

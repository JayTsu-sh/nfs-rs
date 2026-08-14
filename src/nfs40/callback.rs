use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

pub(crate) struct CallbackService {
    universal_addr: String,
    state: Arc<CallbackState>,
    recall_rx: Mutex<Option<mpsc::Receiver<RecallNotification>>>,
    task: JoinHandle<()>,
}

struct DelegationRecord {
    grant: DelegationGrant,
    generation: u64,
    attributes: Option<(u64, u64)>,
    recalling: bool,
}

pub(crate) struct CallbackState {
    delegations: Mutex<HashMap<Bytes, DelegationRecord>>,
    generation: AtomicU64,
    recall_tx: mpsc::Sender<RecallNotification>,
    open_publications: AtomicUsize,
    recalls_received: AtomicU64,
    returns_completed: AtomicU64,
    returns_failed: AtomicU64,
    healthy: AtomicBool,
}

pub(crate) struct OpenPublication {
    state: Arc<CallbackState>,
}

pub(crate) struct RecallNotification {
    pub fh: Bytes,
    pub stateid: [u8; 16],
    pub generation: u64,
    pub truncate: bool,
}

pub(crate) struct CallbackWorker {
    task: JoinHandle<()>,
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
            recalls_received: AtomicU64::new(0),
            returns_completed: AtomicU64::new(0),
            returns_failed: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        });
        let service_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                if peer.ip() != IpAddr::V4(server_ip) {
                    drop(stream);
                    continue;
                }
                let state = Arc::clone(&service_state);
                tokio::spawn(async move {
                    let _ = serve_connection(stream, state).await;
                });
            }
        });
        Ok(Self {
            universal_addr,
            state,
            recall_rx: Mutex::new(Some(recall_rx)),
            task,
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
}

impl CallbackState {
    pub(crate) fn stats(&self) -> crate::CallbackStats {
        crate::CallbackStats {
            recalls_received: self.recalls_received.load(Ordering::Relaxed),
            returns_completed: self.returns_completed.load(Ordering::Relaxed),
            returns_failed: self.returns_failed.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
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
        self.generation.store(generation, Ordering::Release);
        self.delegations
            .lock()
            .map_err(|_| NfsError::Rpc("NFSv4.0 callback state lock poisoned".into()))?
            .insert(
                fh,
                DelegationRecord {
                    grant,
                    generation,
                    attributes,
                    recalling: false,
                },
            );
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
        let task = tokio::spawn(async move {
            while let Some(recall) = recalls.recv().await {
                if !state.is_current(&recall) {
                    continue;
                }
                let returned = settle_recall(&rpc, &auth, &recall).await.is_ok();
                if returned {
                    state.returns_completed.fetch_add(1, Ordering::Relaxed);
                } else {
                    state.returns_failed.fetch_add(1, Ordering::Relaxed);
                    state.healthy.store(false, Ordering::Release);
                }
                let _ =
                    state.finish_recall(&recall.fh, &recall.stateid, recall.generation, returned);
            }
        });
        Self { task }
    }
}

impl Drop for CallbackWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn settle_recall(rpc: &rpc::Client, auth: &Auth, recall: &RecallNotification) -> Result<()> {
    if !recall.truncate {
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
    decode_delegreturn_response(response)
}

async fn serve_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<CallbackState>,
) -> Result<()> {
    loop {
        let Some(call) = read_record(&mut stream).await? else {
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
    if call.len() < 40 || !call.len().is_multiple_of(4) {
        return Err(NfsError::Xdr(
            "NFSv4.0 callback RPC call has an invalid length".into(),
        ));
    }
    let words: Vec<u32> = call[..40]
        .chunks_exact(4)
        .map(|word| u32::from_be_bytes([word[0], word[1], word[2], word[3]]))
        .collect();
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
    if words[6..] != [0, 0, 0, 0] {
        return Err(NfsError::Rpc(
            "NFSv4.0 CB_NULL authentication does not match AUTH_NONE".into(),
        ));
    }
    if words[5] == 0 {
        return Ok(if call.len() == 40 {
            accepted_reply(words[0], &[0])
        } else {
            accepted_reply(words[0], &[4])
        });
    }
    let compound = match compound_reply(&call[40..], state) {
        Ok(compound) => compound,
        Err(_) => return Ok(accepted_reply(words[0], &[4])),
    };
    Ok(accepted_body(words[0], &compound))
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
                            truncate,
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
            _ => {
                status = 10044;
                results.extend_from_slice(&10044u32.to_be_bytes());
                results.extend_from_slice(&status.to_be_bytes());
            }
        }
        if status != 0 {
            break;
        }
    }
    if cursor != body.len() {
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

impl Drop for CallbackService {
    fn drop(&mut self) {
        self.task.abort();
    }
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
    async fn cb_null_round_trips_over_the_published_listener() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let words = round_trip(&mut stream, &cb_null_call(0x1020_3040)).await;
        assert_eq!(words, [0x1020_3040, 1, 0, 0, 0, 0]);
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
    async fn empty_cb_compound_echoes_tag_and_rejects_other_minor_versions() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        for (minor, expected_status) in [(0, 0), (1, 10021)] {
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
    async fn cb_getattr_rejects_a_file_without_a_write_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(19);
        call[5] = 1;
        call.extend_from_slice(&[
            0, // empty tag
            0, // minor version
            1, // callback ident
            1, // operation count
            3, // OP_CB_GETATTR
            2, // filehandle length
            u32::from_be_bytes(*b"fh\0\0"),
            1,    // bitmap word count
            0x18, // change + size
        ]);
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [19, 1, 0, 0, 0, 0, 10001, 0, 1, 3, 10001]
        );
    }

    #[tokio::test]
    async fn cb_recall_rejects_a_file_without_a_delegation() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(23);
        call[5] = 1;
        call.extend_from_slice(&[
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
        ]);
        assert_eq!(
            round_trip(&mut stream, &call).await,
            [23, 1, 0, 0, 0, 0, 10001, 0, 1, 4, 10001]
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
                recalls_received: 1,
                returns_completed: 0,
                returns_failed: 0,
            }
        );
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
    async fn duplicate_recall_flushes_and_returns_the_delegation_exactly_once() {
        let nfs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = nfs_listener.local_addr().unwrap();
        let mux = rpc::StreamMux::connect(server_addr, true).await.unwrap();
        let rpc = rpc::Client::new(mux, None);
        let service = CallbackService::bind_for(server_addr).await.unwrap();
        let state = service.state();
        let _worker = CallbackWorker::start(
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

        let mut callback = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let mut call = cb_null_call(37);
        call[5] = 1;
        call.extend_from_slice(&[0, 0, 1, 1, 4]);
        call.extend_from_slice(&[0x5555_5555; 4]);
        call.extend_from_slice(&[0, 2, u32::from_be_bytes(*b"fh\0\0")]);
        let expected = [37, 1, 0, 0, 0, 0, 0, 0, 1, 4, 0];
        assert_eq!(round_trip(&mut callback, &call).await, expected);
        assert_eq!(round_trip(&mut callback, &call).await, expected);
        server.await.unwrap();
        assert_eq!(
            state.stats(),
            crate::CallbackStats {
                recalls_received: 1,
                returns_completed: 1,
                returns_failed: 0,
            }
        );
        assert!(state.healthy());
    }
}

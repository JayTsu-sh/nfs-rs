// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

pub mod auth;
pub mod header;

use crate::error::{NfsError, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use auth::Auth;
pub(crate) use header::Header;

pub(crate) const RPC_VERSION: u32 = 2;
pub(crate) const PORTMAP_PROG: u32 = 100000;
pub(crate) const PORTMAP_VERSION: u32 = 2;
pub(crate) const PORTMAP_PORT: u16 = 111;
pub(crate) const MOUNT_PROG: u32 = 100005;
pub(crate) const MOUNT3_VERSION: u32 = 3;
pub(crate) const NFS_PROG: u32 = 100003;
pub(crate) const NFS3_VERSION: u32 = 3;

const IPPROTO_TCP: u32 = 6;

/// Timeout for portmap queries (lightweight metadata operations).
const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

enum PortmapProc2 {
    Null = 0,
    GetPort = 3,
}

pub(crate) async fn portmap(
    addrs: &Vec<SocketAddr>,
    prog: u32,
    vers: u32,
    auth: &Auth,
    max_retries: usize,
    noresvport: bool,
) -> Result<u16> {
    let mut last_err: Option<(SocketAddr, NfsError)> = None;
    for addr in addrs {
        debug!(addr = %addr, prog, vers, "attempting portmapper lookup");
        match portmap_on_addr(addr, prog, vers, auth, max_retries, noresvport).await {
            Ok(port) => {
                info!(addr = %addr, prog, vers, port, "portmapper resolved port");
                return Ok(port);
            }
            Err(e) => {
                warn!(addr = %addr, prog, vers, error = %e, "portmapper lookup failed on address");
                last_err = Some((*addr, e));
            }
        }
    }
    Err(NfsError::Rpc(format!(
        "portmapper lookup failed for prog={} vers={}: {}",
        prog,
        vers,
        last_err
            .map(|(addr, e)| format!("{}: {}", addr, e))
            .unwrap_or_else(|| "no addresses tried".to_string()),
    )))
}

async fn portmap_on_addr(
    addr: &SocketAddr,
    prog: u32,
    vers: u32,
    auth: &Auth,
    max_retries: usize,
    noresvport: bool,
) -> Result<u16> {
    let mux = StreamMux::connect(*addr, noresvport).await?;
    let client = Client::new(mux, None);
    let result = portmap_calls(&client, prog, vers, auth, max_retries).await;
    let _ = client.shutdown().await;
    result
}

async fn portmap_calls(
    client: &Client,
    prog: u32,
    vers: u32,
    auth: &Auth,
    max_retries: usize,
) -> Result<u16> {
    // PORTMAP NULL
    let mut buf = Vec::<u8>::new();
    Header::new(
        RPC_VERSION,
        PORTMAP_PROG,
        PORTMAP_VERSION,
        PortmapProc2::Null as u32,
        auth,
        &Auth::new_null(),
    )
    .encode(&mut buf);
    client.call(buf, max_retries, METADATA_TIMEOUT).await?;

    // PORTMAP GETPORT
    let args = GETPORT2args {
        header: Header::new(
            RPC_VERSION,
            PORTMAP_PROG,
            PORTMAP_VERSION,
            PortmapProc2::GetPort as u32,
            auth,
            &Auth::new_null(),
        ),
        prog,
        vers,
        proto: IPPROTO_TCP,
        port: 0,
    };
    let mut buf = Vec::<u8>::new();
    args.encode(&mut buf);
    let res = client.call(buf, max_retries, METADATA_TIMEOUT).await?;
    Ok(BigEndian::read_u32(&res[..4]) as u16)
}

#[derive(Debug, PartialEq)]
struct GETPORT2args {
    header: Header,
    prog: u32,
    vers: u32,
    proto: u32,
    port: u32,
}

impl GETPORT2args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.header.encode(buf);
        buf.extend_from_slice(&self.prog.to_be_bytes());
        buf.extend_from_slice(&self.vers.to_be_bytes());
        buf.extend_from_slice(&self.proto.to_be_bytes());
        buf.extend_from_slice(&self.port.to_be_bytes());
    }
}

// ─── StreamMux ───────────────────────────────────────────────────────────────
//
// Multiplexes multiple concurrent RPC calls over a single TCP connection.
// A background reader task dispatches responses by XID via oneshot channels.
// The writer is protected by a TokioMutex that is only held during the write
// phase, allowing true concurrent request/response overlap.

type PendingMap = Arc<std::sync::Mutex<HashMap<u32, oneshot::Sender<Result<Bytes>>>>>;

/// Handler for inbound NFSv4.1 backchannel CALLs (server→client CB_COMPOUND).
///
/// Input: the full RPC CALL frame, starting at the xid (record mark already stripped).
/// Output: the full RPC reply frame (also starting at the xid, without record mark),
/// or `None` to drop the message silently (e.g. on a parse error).
///
/// The handler is synchronous — CB processing is pure parsing plus a non-blocking
/// `try_send` to the recall channel, so it never needs to await.
pub(crate) type BackchannelHandler = Arc<dyn Fn(Bytes) -> Option<Vec<u8>> + Send + Sync>;

/// Shared slot for the optional backchannel handler. Installed after the session
/// is established (see `enable_backchannel`); read by the reader loop on each CALL.
type BackchannelSlot = Arc<std::sync::Mutex<Option<BackchannelHandler>>>;

pub(crate) struct StreamMux {
    /// Wrapped in an `Arc` so the reader loop can also write backchannel replies
    /// onto the same connection (NFSv4.1 backchannel rides the fore-channel TCP).
    writer: Arc<TokioMutex<OwnedWriteHalf>>,
    pending: PendingMap,
    backchannel: BackchannelSlot,
    addr: SocketAddr,
    noresvport: bool,
    generation: AtomicU64,
    reader_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// 标记 shutdown 已调用，阻止后续 reconnect 尝试
    shutdown_flag: AtomicBool,
}

impl StreamMux {
    pub(crate) async fn connect(addr: SocketAddr, noresvport: bool) -> Result<Arc<Self>> {
        let stream = crate::connect_to_target(&addr, noresvport).await?;
        let (reader, writer) = stream.into_split();
        let pending: PendingMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let writer = Arc::new(TokioMutex::new(writer));
        let backchannel: BackchannelSlot = Arc::new(std::sync::Mutex::new(None));
        let reader = BufReader::with_capacity(1_048_576, reader);
        let reader_handle = tokio::spawn(reader_loop(
            reader,
            Arc::clone(&pending),
            Arc::clone(&writer),
            Arc::clone(&backchannel),
        ));
        info!(addr = %addr, "RPC stream mux connected");
        Ok(Arc::new(Self {
            writer,
            pending,
            backchannel,
            addr,
            noresvport,
            generation: AtomicU64::new(0),
            reader_handle: std::sync::Mutex::new(Some(reader_handle)),
            shutdown_flag: AtomicBool::new(false),
        }))
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Install the backchannel handler so the reader loop dispatches inbound
    /// server CB_COMPOUND CALLs. Called once after the session is established.
    fn enable_backchannel(&self, handler: BackchannelHandler) {
        match self.backchannel.lock() {
            Ok(mut slot) => *slot = Some(handler),
            Err(_) => warn!("backchannel slot lock poisoned; cannot enable backchannel"),
        }
    }

    /// `header` is the pre-assembled RPC frame header + msg_body (prefix already prepended).
    /// `data` is the optional large payload (e.g. WRITE data), sent zero-copy after the header.
    async fn send_and_receive(
        &self,
        xid: u32,
        header: &[u8],
        data: &[u8],
        data_pad: usize,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| NfsError::Rpc("pending map lock poisoned".to_string()))?
            .insert(xid, tx);

        // Write request under the writer lock — released before awaiting the response.
        // `header` already contains the RPC frame prefix + msg_body (zero-copy, no extra alloc).
        let write_result = {
            let mut writer = self.writer.lock().await;
            async {
                writer.write_all(header).await?;
                if !data.is_empty() {
                    writer.write_all(data).await?;
                    if data_pad > 0 {
                        writer.write_all(&[0u8; 4][..data_pad]).await?;
                    }
                }
                Ok::<(), NfsError>(())
            }
            .await
        };

        if let Err(e) = write_result {
            if let Ok(mut map) = self.pending.lock() {
                map.remove(&xid);
            }
            return Err(e);
        }

        // Wait for response from the reader task with timeout.
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "reader task terminated",
            ))),
            Err(_) => {
                if let Ok(mut map) = self.pending.lock() {
                    map.remove(&xid);
                }
                Err(NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "RPC response timeout",
                )))
            }
        }
    }

    async fn reconnect(&self, failed_gen: u64) -> Result<()> {
        // 已关闭的连接不再重连
        if self.shutdown_flag.load(Ordering::Acquire) {
            return Err(NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "mux is shut down",
            )));
        }
        info!(addr = %self.addr, failed_gen, "initiating reconnection");
        // Fast path: another caller already reconnected (no lock needed).
        let current_gen = self.generation.load(Ordering::Acquire);
        if current_gen > failed_gen {
            debug!(addr = %self.addr, current_gen, failed_gen, "reconnection already performed by another caller");
            return Ok(());
        }
        // Establish new TCP connection OUTSIDE the writer lock so that
        // concurrent send_and_receive() calls are not blocked during connect.
        let stream = crate::connect_to_target(&self.addr, self.noresvport).await?;
        let (reader, new_writer) = stream.into_split();
        let reader = BufReader::with_capacity(1_048_576, reader);

        // Take the lock only to swap writer/reader (microsecond-level hold).
        let mut writer = self.writer.lock().await;
        let current_gen = self.generation.load(Ordering::Acquire);
        if current_gen > failed_gen {
            debug!(addr = %self.addr, current_gen, failed_gen, "reconnection already performed by another caller (after connect)");
            return Ok(()); // discard the connection we just built
        }
        // 再次检查 shutdown，避免在等锁期间被 shutdown
        if self.shutdown_flag.load(Ordering::Acquire) {
            return Err(NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "mux is shut down",
            )));
        }
        // Abort old reader.
        if let Ok(mut guard) = self.reader_handle.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
        // Fail all pending requests.
        {
            let mut map = self
                .pending
                .lock()
                .map_err(|_| NfsError::Rpc("pending map lock poisoned".to_string()))?;
            if !map.is_empty() {
                debug!(addr = %self.addr, pending_count = map.len(), "failing pending requests due to reconnection");
            }
            for (_, tx) in map.drain() {
                let _ = tx.send(Err(NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "reconnecting",
                ))));
            }
        }
        // Install new connection.
        *writer = new_writer;
        {
            let mut guard = self
                .reader_handle
                .lock()
                .map_err(|_| NfsError::Rpc("reader_handle lock poisoned".to_string()))?;
            *guard = Some(tokio::spawn(reader_loop(
                reader,
                Arc::clone(&self.pending),
                Arc::clone(&self.writer),
                Arc::clone(&self.backchannel),
            )));
        }
        self.generation.fetch_add(1, Ordering::Release);
        info!(addr = %self.addr, generation = self.generation.load(Ordering::Acquire), "reconnection successful");
        Ok(())
    }

    async fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        debug!(addr = %self.addr, "shutting down StreamMux");
        if let Ok(mut guard) = self.reader_handle.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
        let mut writer = self.writer.lock().await;
        let _ = writer.shutdown().await;
        if let Ok(mut map) = self.pending.lock() {
            for (_, tx) in map.drain() {
                let _ = tx.send(Err(NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "shutdown",
                ))));
            }
        }
    }
}

impl Drop for StreamMux {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.reader_handle.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
        if let Ok(mut map) = self.pending.lock() {
            for (_, tx) in map.drain() {
                let _ = tx.send(Err(NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "connection closed",
                ))));
            }
        }
    }
}

/// Background task: reads RPC messages from the TCP stream. Server REPLY messages
/// are dispatched to the waiting caller via the PendingMap; server CALL messages
/// (NFSv4.1 backchannel CB_COMPOUND, which ride the fore-channel connection) are
/// handed to the registered backchannel handler and the reply is written back on
/// the same connection.
async fn reader_loop(
    mut reader: BufReader<OwnedReadHalf>,
    pending: PendingMap,
    writer: Arc<TokioMutex<OwnedWriteHalf>>,
    backchannel: BackchannelSlot,
) {
    loop {
        match read_one_response(&mut reader).await {
            Ok((xid, data)) => {
                // RPC msg_type lives at bytes [4..8]: 0 = CALL, 1 = REPLY.
                // A CALL here is a server-initiated backchannel request, not a
                // response to one of our outstanding calls.
                let msg_type = if data.len() >= 8 {
                    BigEndian::read_u32(&data[4..8])
                } else {
                    MessageType::Response as u32
                };
                if msg_type == MessageType::Request as u32 {
                    dispatch_backchannel_call(xid, data, &writer, &backchannel).await;
                    continue;
                }
                match pending.lock() { Ok(mut map) => {
                    match map.remove(&xid) { Some(tx) => {
                        let _ = tx.send(Ok(data));
                    } _ => {
                        debug!(
                            xid,
                            "dropping response for unmatched XID (likely stale retry)"
                        );
                    }}
                } _ => {
                    warn!("pending map lock poisoned in reader loop, terminating");
                    break;
                }}
            }
            Err(e) => {
                warn!(error = %e, "reader loop terminated due to connection error");
                // Connection broken: fail all pending requests.
                if let Ok(mut map) = pending.lock() {
                    for (_, tx) in map.drain() {
                        let _ = tx.send(Err(NfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            e.to_string(),
                        ))));
                    }
                }
                break;
            }
        }
    }
}

/// Handle a server-initiated backchannel CALL (CB_COMPOUND) received on the
/// fore-channel connection: dispatch it to the registered handler and write the
/// reply back on the same connection. If no handler is registered, the CALL is
/// dropped (the server will observe this via SEQ4_STATUS on the fore channel).
async fn dispatch_backchannel_call(
    xid: u32,
    data: Bytes,
    writer: &Arc<TokioMutex<OwnedWriteHalf>>,
    backchannel: &BackchannelSlot,
) {
    let handler = match backchannel.lock() {
        Ok(slot) => slot.clone(),
        Err(_) => {
            warn!("backchannel slot lock poisoned, dropping backchannel CALL");
            return;
        }
    };
    let Some(handler) = handler else {
        debug!(xid, "backchannel CALL received but no handler registered, dropping");
        return;
    };
    let Some(reply) = handler(data) else {
        debug!(xid, "backchannel handler dropped CALL (parse error)");
        return;
    };
    // Frame with the RPC record mark (MSB = last fragment) and write on the
    // shared writer; this serializes against concurrent fore-channel requests.
    let mark = (reply.len() as u32) | 0x80000000;
    let mut out = Vec::with_capacity(4 + reply.len());
    out.extend_from_slice(&mark.to_be_bytes());
    out.extend_from_slice(&reply);
    let mut w = writer.lock().await;
    if let Err(e) = w.write_all(&out).await {
        warn!(xid, error = %e, "failed to write backchannel reply");
    }
}

/// Maximum RPC response size (4 MiB + 4 KiB overhead). Responses exceeding this
/// are rejected to prevent memory exhaustion from malicious or buggy servers.
const MAX_RPC_RESPONSE: usize = 4 * 1024 * 1024 + 4096;

async fn read_one_response(reader: &mut BufReader<OwnedReadHalf>) -> Result<(u32, Bytes)> {
    let mut hdr = [0u8; 4];
    reader.read_exact(&mut hdr).await?;
    let raw = BigEndian::read_u32(&hdr);
    let last = (raw & 0x80000000) != 0;
    let sz = (raw & 0x7FFFFFFF) as usize;
    if sz > MAX_RPC_RESPONSE {
        return Err(NfsError::Rpc(format!(
            "RPC fragment size {} exceeds maximum {}",
            sz, MAX_RPC_RESPONSE
        )));
    }

    let mut buf = BytesMut::with_capacity(sz);
    buf.resize(sz, 0);
    reader.read_exact(&mut buf[..sz]).await?;

    if !last {
        // Multi-fragment: keep reading until the last fragment.
        loop {
            reader.read_exact(&mut hdr).await?;
            let raw = BigEndian::read_u32(&hdr);
            let last = (raw & 0x80000000) != 0;
            let sz = (raw & 0x7FFFFFFF) as usize;
            let total = buf.len() + sz;
            if total > MAX_RPC_RESPONSE {
                return Err(NfsError::Rpc(format!(
                    "RPC accumulated response size {} exceeds maximum {}",
                    total, MAX_RPC_RESPONSE
                )));
            }
            let offset = buf.len();
            buf.resize(total, 0);
            reader.read_exact(&mut buf[offset..]).await?;
            if last {
                break;
            }
        }
    }

    let xid = BigEndian::read_u32(&buf[0..4]);
    Ok((xid, buf.freeze()))
}

// ─── Client ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct Client {
    nfs_mux: Arc<StreamMux>,
    mount_mux: Option<Arc<StreamMux>>,
}

impl std::fmt::Debug for StreamMux {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamMux")
            .field("addr", &self.addr)
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish()
    }
}

impl Client {
    pub(crate) fn new(nfs_mux: Arc<StreamMux>, mount_mux: Option<Arc<StreamMux>>) -> Self {
        Self { nfs_mux, mount_mux }
    }

    /// Install the NFSv4.1 backchannel handler on the NFS connection so the
    /// reader loop dispatches inbound server CB_COMPOUND CALLs.
    pub(crate) fn enable_backchannel(&self, handler: BackchannelHandler) {
        self.nfs_mux.enable_backchannel(handler);
    }

    fn get_mux(&self, program: u32) -> Result<&Arc<StreamMux>> {
        match program {
            MOUNT_PROG => Ok(self.mount_mux.as_ref().unwrap_or(&self.nfs_mux)),
            NFS_PROG | PORTMAP_PROG => Ok(&self.nfs_mux),
            _ => Err(NfsError::InvalidInput(format!(
                "unknown RPC program {}",
                program
            ))),
        }
    }

    pub(crate) async fn call(
        &self,
        msg_body: Vec<u8>,
        max_retries: usize,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        self.call_with_data(msg_body, Bytes::new(), max_retries, timeout)
            .await
    }

    /// Like `call`, but sends `data` after `msg_body` without copying it into the request buffer.
    /// `msg_body` must already contain the XDR length-prefix for the data field (4 bytes at the
    /// end); the raw payload bytes and their padding are written to the stream separately.
    pub(crate) async fn call_with_data(
        &self,
        mut msg_body: Vec<u8>,
        data: Bytes,
        max_retries: usize,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        const SIZE_HDR_BIT: u32 = 0x80000000;
        const PREFIX_LEN: usize = 12;

        let mut num_retries = 0usize;
        let start = tokio::time::Instant::now();
        // Total retry budget: 3x the per-call timeout, so we fail fast instead of
        // accumulating max_retries * timeout worth of delay.
        let max_total = timeout.saturating_mul(3);

        // Determine mux from the program field in msg_body (offset 4, big-endian u32).
        let program = if msg_body.len() >= 8 {
            BigEndian::read_u32(&msg_body[4..8])
        } else {
            NFS_PROG
        };
        let mux = self.get_mux(program)?;

        let data_len = data.len();
        let data_pad = (4 - data_len % 4) % 4;
        let payload_len = (8 + msg_body.len() + data_len + data_pad) as u32;

        // Prepend 12-byte RPC frame prefix space to msg_body (one-time allocation).
        // XID at offset 4..8 is overwritten per retry; the rest is constant.
        msg_body.splice(0..0, [0u8; PREFIX_LEN]);
        BigEndian::write_u32(&mut msg_body[0..4], payload_len | SIZE_HDR_BIT);
        // msg_body[4..8] = xid, written per retry below
        BigEndian::write_u32(&mut msg_body[8..12], MessageType::Request as u32);

        while num_retries < max_retries {
            // Bail out if total elapsed time exceeds the budget.
            if start.elapsed() > max_total {
                break;
            }

            // Each retry uses a fresh XID (the old one may have stale responses in flight).
            let xid = get_xid();
            BigEndian::write_u32(&mut msg_body[4..8], xid);

            debug!(
                xid,
                attempt = num_retries + 1,
                max_retries,
                program,
                "sending RPC request"
            );
            let r#gen = mux.generation();
            let res = mux
                .send_and_receive(xid, &msg_body, &data, data_pad, timeout)
                .await;

            match res {
                Ok(response_data) => {
                    trace!(xid, "RPC response received");
                    return parse_rpc_response(response_data, xid);
                }
                Err(ref e) => {
                    let (is_conn_error, is_timeout) = match e {
                        NfsError::Io(io_err) => (
                            matches!(
                                io_err.kind(),
                                std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::ConnectionReset
                            ),
                            io_err.kind() == std::io::ErrorKind::TimedOut,
                        ),
                        _ => (false, false),
                    };
                    if is_conn_error {
                        // Connection dead — reconnect then retry.
                        warn!(
                            xid,
                            attempt = num_retries + 1,
                            max_retries,
                            error = %e,
                            "RPC call failed (connection error), reconnecting"
                        );
                        let jitter = rand::random_range(0..50u64);
                        let backoff = std::cmp::min(100u64 << num_retries, 2000) + jitter;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                        if let Err(reconn_err) = mux.reconnect(r#gen).await {
                            warn!(error = %reconn_err, "reconnect failed, will retry");
                        }
                        num_retries += 1;
                        continue;
                    } else if is_timeout {
                        // Timeout — server may be slow but connection could still be alive.
                        // Retry without reconnect to avoid killing other in-flight requests.
                        warn!(
                            xid,
                            attempt = num_retries + 1,
                            max_retries,
                            error = %e,
                            "RPC call timed out, retrying without reconnect"
                        );
                        num_retries += 1;
                        continue;
                    } else {
                        error!(xid, error = %e, "RPC call failed with non-retryable error");
                        return Err(NfsError::Rpc(e.to_string()));
                    }
                }
            }
        }
        error!(
            max_retries,
            elapsed_ms = start.elapsed().as_millis() as u64,
            program,
            "RPC retries exhausted, giving up"
        );
        Err(NfsError::Io(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "unable to reconnect to NFS server",
        )))
    }

    pub(crate) async fn shutdown(&self) {
        self.nfs_mux.shutdown().await;
        if let Some(ref mount_mux) = self.mount_mux {
            mount_mux.shutdown().await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn new_dummy() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stream_result, _accept_result) =
            tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
        let stream = stream_result.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        let pending: PendingMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let writer = Arc::new(TokioMutex::new(writer));
        let backchannel: BackchannelSlot = Arc::new(std::sync::Mutex::new(None));
        let reader = BufReader::with_capacity(1_048_576, reader);
        let reader_handle = tokio::spawn(reader_loop(
            reader,
            Arc::clone(&pending),
            Arc::clone(&writer),
            Arc::clone(&backchannel),
        ));
        let mux = Arc::new(StreamMux {
            writer,
            pending,
            backchannel,
            addr,
            noresvport: false,
            generation: AtomicU64::new(0),
            reader_handle: std::sync::Mutex::new(Some(reader_handle)),
            shutdown_flag: AtomicBool::new(false),
        });
        Self {
            nfs_mux: mux,
            mount_mux: None,
        }
    }
}

/// Strip the RPC response envelope and return the NFS payload as a zero-copy `Bytes` slice.
///
/// Format: [xid(4)] [msgtype(4)] [msg_status(4)] [verf_flavor(4)] [verf_len(4)]
///         [verf_data…] [accept_stat(4)] [payload…]
fn parse_rpc_response(res: Bytes, xid: u32) -> Result<Bytes> {
    let read_u32 = |data: &[u8], p: usize| -> Result<u32> {
        if p + 4 > data.len() {
            return Err(NfsError::Rpc("response truncated".to_string()));
        }
        Ok(BigEndian::read_u32(&data[p..p + 4]))
    };

    if res.len() < 8 {
        error!(xid, response_len = res.len(), "RPC response too short");
        return Err(NfsError::Rpc("response too short".to_string()));
    }
    let res_xid = BigEndian::read_u32(&res[0..4]);
    let res_msgtype = BigEndian::read_u32(&res[4..8]);
    if res_xid != xid {
        error!(
            expected_xid = xid,
            actual_xid = res_xid,
            "RPC response XID mismatch"
        );
        return Err(NfsError::Rpc(
            "response id does not match expected one".to_string(),
        ));
    }
    if res_msgtype != MessageType::Response as u32 {
        error!(
            xid,
            msgtype = res_msgtype,
            "RPC response has unexpected message type"
        );
        return Err(NfsError::Rpc(
            "response type does not match expected one".to_string(),
        ));
    }

    // reply_body: [msg_status(4)] [verf_flavor(4)] [verf_len(4)] [verf_data] [accept_stat(4)] [data…]
    let mut pos = 8usize;
    let msg_status = read_u32(&res, pos)? as i32;
    pos += 4;
    if msg_status != MessageStatus::Accepted as i32 {
        error!(xid, msg_status, "RPC response rejected (bad status)");
        return Err(NfsError::Rpc(
            "could not parse response due to bad status".to_string(),
        ));
    }
    pos += 4; // skip verifier flavor
    let verf_len = read_u32(&res, pos)? as usize;
    pos += 4;
    let verf_padded = verf_len + (4 - verf_len % 4) % 4;
    if pos + verf_padded > res.len() {
        error!(
            xid,
            response_len = res.len(),
            "RPC response truncated (verifier)"
        );
        return Err(NfsError::Rpc("response truncated (verifier)".to_string()));
    }
    pos += verf_padded;
    let accept_status = read_u32(&res, pos)? as i32;
    pos += 4;
    if accept_status != AcceptStatus::Success as i32 {
        error!(xid, accept_status, "RPC request rejected by server");
        return Err(NfsError::Rpc("request rejected".to_string()));
    }

    // Zero-copy: slice off the RPC envelope, sharing the underlying buffer.
    Ok(res.slice(pos..))
}

#[derive(Debug, Clone, PartialEq)]
enum MessageType {
    Request = 0,
    Response = 1,
}

enum MessageStatus {
    Accepted = 0,
    #[allow(unused)]
    Denied = 1,
}

enum AcceptStatus {
    Success = 0,
    #[allow(unused)]
    ProgUnavail = 1,
    #[allow(unused)]
    ProgMismatch = 2,
    #[allow(unused)]
    ProcUnavail = 3,
    #[allow(unused)]
    GarbageArgs = 4,
}

static XID: AtomicU32 = AtomicU32::new(0);

fn get_xid() -> u32 {
    // Seed with wall-clock time on the very first call; a CAS ensures only one thread seeds.
    if XID.load(Ordering::Relaxed) == 0 {
        XID.compare_exchange(0, get_current_time(), Ordering::Relaxed, Ordering::Relaxed)
            .ok();
    }
    XID.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
}

pub(crate) fn get_current_time() -> u32 {
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (since_epoch.as_secs() as u32).wrapping_mul(1000) + since_epoch.subsec_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_rpc_version() {
        // Program field is at offset 4 in the body: rpcvers(4) prog(4) vers(4) proc(4)
        let body = vec![0u8, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5];
        assert_eq!(BigEndian::read_u32(&body[0..4]), 2);
    }

    #[test]
    fn message_program() {
        let body = vec![0u8, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5];
        assert_eq!(BigEndian::read_u32(&body[4..8]), 3);
    }

    #[test]
    fn message_version() {
        let body = vec![0u8, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5];
        assert_eq!(BigEndian::read_u32(&body[8..12]), 4);
    }

    #[test]
    fn message_procedure() {
        let body = vec![0u8, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5];
        assert_eq!(BigEndian::read_u32(&body[12..16]), 5);
    }

    #[tokio::test]
    async fn portmap_error_includes_underlying_detail() {
        // Connect to a localhost port nobody listens on → ConnectionRefused
        let dead_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let auth = Auth::new_null();
        let res = portmap(&vec![dead_addr], NFS_PROG, NFS3_VERSION, &auth, 2, false).await;
        let err = res.expect_err("dead port should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("127.0.0.1:1")
                || msg.to_lowercase().contains("refused")
                || msg.to_lowercase().contains("connect"),
            "portmap error should expose underlying detail, got: {}",
            msg
        );
    }
}

//! NFSv4.1 callback service — server→client backchannel (RFC 5661 §2.10.3).
//!
//! The server uses the backchannel to send CB_COMPOUND calls to the client,
//! primarily for delegation recall (CB_RECALL) and session management (CB_SEQUENCE).
//!
//! The callback service listens on a TCP port and handles incoming ONC-RPC
//! requests with program number = cb_program (provided during CREATE_SESSION).
//!
//! NOTE: This module is currently unused because the NFSv4.1 backchannel
//! (BIND_CONN_TO_SESSION) is not yet wired up. The code is retained for
//! future backchannel support.

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::error::Result;

/// Maximum callback RPC message size (callbacks are small metadata ops).
const MAX_CB_MESSAGE: usize = 65536;
/// Maximum number of operations in a CB_COMPOUND.
const MAX_CB_OPS: u32 = 64;
/// Maximum referring_call_lists entries.
const MAX_REFERRING_LISTS: usize = 256;

/// Callback program number (client-chosen, communicated in CREATE_SESSION).
pub(crate) const CB_PROGRAM: u32 = 0x40000000;

/// A delegation recall notification from the server.
#[derive(Debug, Clone)]
pub(crate) struct RecallNotification {
    /// The stateid of the delegation being recalled.
    pub stateid: [u8; 16],
    /// Whether to truncate the file (only for write delegations).
    /// RFC 5661 §20.2.1 — not yet acted upon; retained for future use.
    #[allow(dead_code)]
    pub truncate: bool,
    /// The file handle of the file whose delegation is recalled.
    pub fh: Bytes,
}

/// Callback service that listens for server-initiated CB_COMPOUND calls.
pub(crate) struct CallbackService {
    /// The TCP port we are listening on.
    pub port: u16,
    /// Channel to receive recall notifications.
    pub recall_rx: mpsc::Receiver<RecallNotification>,
    /// Handle to the background listener task.
    handle: JoinHandle<()>,
}

impl CallbackService {
    /// Start the callback service on an ephemeral port.
    /// Returns the service (with port number for CREATE_SESSION).
    pub async fn start(session_id: [u8; 16]) -> Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:0").await
            .map_err(crate::error::NfsError::Io)?;
        let port = listener.local_addr()
            .map_err(crate::error::NfsError::Io)?.port();
        let (recall_tx, recall_rx) = mpsc::channel(32);

        info!(port, "callback service started");

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!(addr = %addr, "callback connection accepted");
                        let tx = recall_tx.clone();
                        let sid = session_id;
                        tokio::spawn(async move {
                            if let Err(e) = handle_callback_connection(stream, sid, tx).await {
                                warn!(error = %e, "callback connection error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "callback accept error");
                        break;
                    }
                }
            }
        });

        Ok(Self { port, recall_rx, handle })
    }

    /// Take the recall notification receiver (can only be called once).
    /// Subsequent calls return an empty receiver.
    pub fn take_recall_rx(&mut self) -> mpsc::Receiver<RecallNotification> {
        let (_, empty_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.recall_rx, empty_rx)
    }

    /// Stop the callback service.
    pub fn stop(&self) {
        self.handle.abort();
    }
}

impl Drop for CallbackService {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Handle a single callback TCP connection from the server.
async fn handle_callback_connection(
    mut stream: tokio::net::TcpStream,
    session_id: [u8; 16],
    recall_tx: mpsc::Sender<RecallNotification>,
) -> Result<()> {
    // RFC 5661 §2.10.6.3: client must track expected sequence ID per backchannel slot.
    // slot_id → next expected sequence ID (server must use 1 for the first request).
    let mut cb_slot_seqs: HashMap<u32, u32> = HashMap::new();

    loop {
        // Read RPC record mark (4 bytes: MSB=last_fragment, lower 31 bits=length)
        let mut mark_buf = [0u8; 4];
        match stream.read_exact(&mut mark_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let mark = u32::from_be_bytes(mark_buf);
        let len = (mark & 0x7FFFFFFF) as usize;

        // H1: Bound callback message size to prevent OOM from malicious senders
        if len > MAX_CB_MESSAGE {
            return Err(crate::error::NfsError::Rpc(
                format!("callback message size {} exceeds max {}", len, MAX_CB_MESSAGE)
            ));
        }

        // Read the full RPC message
        let mut msg = vec![0u8; len];
        stream.read_exact(&mut msg).await?;
        let mut buf = Bytes::from(msg);

        // Parse RPC call header
        let reply = match parse_and_handle_cb_compound(&mut buf, &session_id, &recall_tx, &mut cb_slot_seqs).await {
            Ok(reply) => reply,
            Err(e) => {
                warn!(error = %e, "failed to handle CB_COMPOUND");
                continue;
            }
        };

        // Send RPC reply
        let reply_len = reply.len() as u32 | 0x80000000; // last fragment
        let mut out = BytesMut::with_capacity(4 + reply.len());
        out.put_u32(reply_len);
        out.extend_from_slice(&reply);
        stream.write_all(&out).await?;
    }
}

/// NFS4ERR_SEQ_MISORDERED error code (RFC 5661).
const NFS4ERR_SEQ_MISORDERED: u32 = 10063;

/// Parse a CB_COMPOUND RPC call and produce a reply.
async fn parse_and_handle_cb_compound(
    buf: &mut Bytes,
    session_id: &[u8; 16],
    recall_tx: &mpsc::Sender<RecallNotification>,
    cb_slot_seqs: &mut HashMap<u32, u32>,
) -> Result<Vec<u8>> {
    // RPC call header: xid(4) + msg_type(4) + rpc_version(4) + program(4) + version(4) + procedure(4)
    if buf.remaining() < 24 {
        return Err(crate::error::NfsError::Xdr("CB RPC header too short".to_string()));
    }
    let xid = buf.get_u32();
    let _msg_type = buf.get_u32(); // 0 = CALL
    let _rpc_vers = buf.get_u32();
    let _program = buf.get_u32();
    let _version = buf.get_u32();
    let procedure = buf.get_u32();

    // Skip auth (cred + verf)
    skip_rpc_auth(buf)?;
    skip_rpc_auth(buf)?;

    // CB_COMPOUND (procedure 1)
    if procedure != 1 {
        // CB_NULL (procedure 0) — just return success
        return Ok(build_rpc_reply(xid, &[]));
    }

    // Parse CB_COMPOUND4args: tag + minorversion + callback_ident + ops
    let _tag = skip_opaque(buf)?;
    if buf.remaining() < 8 {
        return Err(crate::error::NfsError::Xdr("CB_COMPOUND args truncated".to_string()));
    }
    let _minor_version = buf.get_u32();
    let _callback_ident = buf.get_u32();

    if buf.remaining() < 4 {
        return Err(crate::error::NfsError::Xdr("CB_COMPOUND ops count truncated".to_string()));
    }
    let num_ops = buf.get_u32();
    // H2: Bound num_ops to prevent CPU exhaustion
    if num_ops > MAX_CB_OPS {
        return Err(crate::error::NfsError::Xdr(
            format!("CB_COMPOUND has {} ops, max {}", num_ops, MAX_CB_OPS)
        ));
    }

    let mut reply_ops = Vec::new();

    for _ in 0..num_ops {
        if buf.remaining() < 4 {
            break;
        }
        let opcode = buf.get_u32();

        match opcode {
            // CB_SEQUENCE
            11 => {
                // CB_SEQUENCE4args: sessionid(16) + sequenceid(4) + slotid(4) + highest_slotid(4) + cachethis(4) + referring_call_lists
                if buf.remaining() < 32 {
                    break;
                }
                let mut cb_session_id = [0u8; 16];
                buf.copy_to_slice(&mut cb_session_id);
                let cb_sequenceid = buf.get_u32();
                let cb_slotid = buf.get_u32();
                let cb_highest_slotid = buf.get_u32();
                let _cachethis = buf.get_u32();

                // referring_call_lists<>
                if buf.remaining() >= 4 {
                    let n = buf.get_u32() as usize;
                    if n > MAX_REFERRING_LISTS {
                        return Err(crate::error::NfsError::Xdr(
                            format!("too many referring_call_lists: {}", n)
                        ));
                    }
                    for _ in 0..n {
                        if buf.remaining() < 16 {
                            return Err(crate::error::NfsError::Xdr(
                                "referring_call sessionid truncated".to_string()
                            ));
                        }
                        buf.advance(16);
                        if buf.remaining() < 4 {
                            return Err(crate::error::NfsError::Xdr(
                                "referring_call count truncated".to_string()
                            ));
                        }
                        let m = buf.get_u32() as usize;
                        let needed = m.checked_mul(8).ok_or_else(|| {
                            crate::error::NfsError::Xdr("referring_call overflow".to_string())
                        })?;
                        if buf.remaining() < needed {
                            return Err(crate::error::NfsError::Xdr(
                                "referring_call data truncated".to_string()
                            ));
                        }
                        buf.advance(needed);
                    }
                }

                // H1: Validate session ID matches expected session
                if cb_session_id != *session_id {
                    warn!("CB_SEQUENCE session ID mismatch, ignoring");
                    return Err(crate::error::NfsError::Xdr(
                        "CB_SEQUENCE session ID mismatch".to_string()
                    ));
                }

                // RFC 5661 §2.10.6.3: validate sequence ID against our slot table.
                // Server MUST use sequenceid=1 on first use of a slot.
                let expected_seq = cb_slot_seqs.get(&cb_slotid).copied().unwrap_or(1);
                if cb_sequenceid != expected_seq {
                    warn!(
                        cb_slotid,
                        cb_sequenceid,
                        expected_seq,
                        "CB_SEQUENCE misordered — rejecting"
                    );
                    let mut op_reply = Vec::new();
                    op_reply.extend_from_slice(&opcode.to_be_bytes());
                    op_reply.extend_from_slice(&NFS4ERR_SEQ_MISORDERED.to_be_bytes());
                    reply_ops.push(op_reply);
                    break; // RFC: stop processing after first error
                }
                cb_slot_seqs.insert(cb_slotid, cb_sequenceid.wrapping_add(1));

                // Reply: CB_SEQUENCE4resok
                let mut op_reply = Vec::new();
                op_reply.extend_from_slice(&opcode.to_be_bytes());
                op_reply.extend_from_slice(&0u32.to_be_bytes()); // NFS4_OK
                op_reply.extend_from_slice(&cb_session_id);
                op_reply.extend_from_slice(&cb_sequenceid.to_be_bytes());
                op_reply.extend_from_slice(&cb_slotid.to_be_bytes());
                op_reply.extend_from_slice(&cb_highest_slotid.to_be_bytes());
                op_reply.extend_from_slice(&cb_highest_slotid.to_be_bytes()); // target_highest_slotid
                reply_ops.push(op_reply);

                debug!(slotid = cb_slotid, seqid = cb_sequenceid, "CB_SEQUENCE handled");
            }

            // CB_RECALL
            4 => {
                // CB_RECALL4args: stateid(16) + truncate(4) + fh(var)
                if buf.remaining() < 20 {
                    break;
                }
                let mut stateid = [0u8; 16];
                buf.copy_to_slice(&mut stateid);
                let truncate = buf.get_u32() != 0;
                let fh = read_opaque(buf)?;

                debug!(fh_len = fh.len(), truncate, "CB_RECALL received");

                // Notify the delegation manager
                if let Err(e) = recall_tx.try_send(RecallNotification {
                    stateid,
                    truncate,
                    fh,
                }) {
                    warn!("CB_RECALL notification channel full or closed: {}", e);
                }

                // Reply: CB_RECALL4res = NFS4_OK
                let mut op_reply = Vec::new();
                op_reply.extend_from_slice(&opcode.to_be_bytes());
                op_reply.extend_from_slice(&0u32.to_be_bytes()); // NFS4_OK
                reply_ops.push(op_reply);
            }

            // Unknown callback op — return NFS4ERR_OP_ILLEGAL
            _ => {
                let mut op_reply = Vec::new();
                op_reply.extend_from_slice(&opcode.to_be_bytes());
                op_reply.extend_from_slice(&10044u32.to_be_bytes()); // NFS4ERR_OP_ILLEGAL
                reply_ops.push(op_reply);
                debug!(opcode, "unknown callback op, returning OP_ILLEGAL");
            }
        }
    }

    // Build CB_COMPOUND4res: status(4) + tag(var) + resarray
    let mut compound_res = Vec::new();
    compound_res.extend_from_slice(&0u32.to_be_bytes()); // NFS4_OK
    compound_res.extend_from_slice(&0u32.to_be_bytes()); // empty tag
    compound_res.extend_from_slice(&(reply_ops.len() as u32).to_be_bytes());
    for op in &reply_ops {
        compound_res.extend_from_slice(op);
    }

    Ok(build_rpc_reply(xid, &compound_res))
}

/// Build a minimal RPC reply message.
fn build_rpc_reply(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(24 + body.len());
    reply.extend_from_slice(&xid.to_be_bytes());       // xid
    reply.extend_from_slice(&1u32.to_be_bytes());       // msg_type = REPLY
    reply.extend_from_slice(&0u32.to_be_bytes());       // reply_stat = MSG_ACCEPTED
    // Verf: AUTH_NONE
    reply.extend_from_slice(&0u32.to_be_bytes());       // flavor = AUTH_NONE
    reply.extend_from_slice(&0u32.to_be_bytes());       // body length = 0
    reply.extend_from_slice(&0u32.to_be_bytes());       // accept_stat = SUCCESS
    reply.extend_from_slice(body);
    reply
}

fn skip_rpc_auth(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 8 {
        return Err(crate::error::NfsError::Xdr("RPC auth truncated".to_string()));
    }
    let _flavor = buf.get_u32();
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(crate::error::NfsError::Xdr("RPC auth body truncated".to_string()));
    }
    buf.advance(padded);
    Ok(())
}

fn skip_opaque(buf: &mut Bytes) -> Result<usize> {
    if buf.remaining() < 4 {
        return Err(crate::error::NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(crate::error::NfsError::Xdr("opaque data truncated".to_string()));
    }
    buf.advance(padded);
    Ok(len)
}

fn read_opaque(buf: &mut Bytes) -> Result<Bytes> {
    if buf.remaining() < 4 {
        return Err(crate::error::NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(crate::error::NfsError::Xdr("opaque data truncated".to_string()));
    }
    let data = buf.slice(..len);
    buf.advance(padded);
    Ok(data)
}

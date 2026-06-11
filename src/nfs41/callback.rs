//! NFSv4.1 callback handling — server→client backchannel (RFC 8881 §2.10.3, §20).
//!
//! In NFSv4.1 the backchannel rides the *fore-channel* TCP connection: the server
//! sends CB_COMPOUND calls (CB_SEQUENCE, CB_RECALL) as ordinary RPC CALL messages
//! back over the same connection the client opened. The RPC layer's reader loop
//! detects those inbound CALLs and routes them to the handler built here via
//! [`make_backchannel_handler`]; the reply is written back on the same connection.
//!
//! There is deliberately no separate callback listener socket — that is the
//! NFSv4.0 model (the server dials back to a client-advertised address) and is
//! unnecessary and unused in NFSv4.1.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::{NfsError, Result};
use crate::rpc::BackchannelHandler;

/// Maximum number of operations in a CB_COMPOUND.
const MAX_CB_OPS: u32 = 64;
/// Maximum referring_call_lists entries.
const MAX_REFERRING_LISTS: usize = 256;

/// Callback program number (client-chosen, communicated in CREATE_SESSION).
pub(crate) const CB_PROGRAM: u32 = 0x40000000;

/// NFS4ERR_SEQ_MISORDERED error code (RFC 8881).
const NFS4ERR_SEQ_MISORDERED: u32 = 10063;

/// A recall notification from the server (delegation or pNFS layout).
#[derive(Debug, Clone)]
pub(crate) enum RecallNotification {
    /// CB_RECALL：delegation 召回（RFC 8881 §20.2）。
    Delegation {
        /// The stateid of the delegation being recalled.
        stateid: [u8; 16],
        /// Whether to truncate the file (only for write delegations).
        /// RFC 8881 §20.2.1 — not yet acted upon; retained for future use.
        #[allow(dead_code)]
        truncate: bool,
        /// The file handle of the file whose delegation is recalled.
        fh: Bytes,
    },
    /// CB_LAYOUTRECALL4_FILE：单文件 layout 召回（RFC 8881 §20.3）。
    LayoutFile {
        /// 召回报文携带的 layout stateid，LAYOUTRETURN 时原样使用。
        stateid: [u8; 16],
        fh: Bytes,
        offset: u64,
        length: u64,
        iomode: u32,
    },
    /// CB_LAYOUTRECALL4_FSID / _ALL：召回客户端持有的全部 layout。
    LayoutAll,
}

/// Build the backchannel handler installed into the RPC layer for this session.
///
/// The returned closure is invoked by the reader loop for every inbound server
/// CB_COMPOUND CALL. It owns the per-connection backchannel slot table used to
/// validate CB_SEQUENCE sequencing (RFC 8881 §2.10.6.3).
pub(crate) fn make_backchannel_handler(
    session_id: [u8; 16],
    recall_tx: mpsc::Sender<RecallNotification>,
) -> BackchannelHandler {
    // slot_id → next expected sequence ID (server must use 1 on first use of a slot).
    let slot_seqs: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    Arc::new(move |frame: Bytes| {
        let mut buf = frame;
        handle_cb_compound(&mut buf, &session_id, &recall_tx, &slot_seqs)
    })
}

/// Parse a CB_COMPOUND RPC CALL and produce the full RPC reply frame (starting at
/// the xid, without the record mark). Returns `None` if the message cannot be
/// parsed, in which case the reader loop drops it.
fn handle_cb_compound(
    buf: &mut Bytes,
    session_id: &[u8; 16],
    recall_tx: &mpsc::Sender<RecallNotification>,
    slot_seqs: &Mutex<HashMap<u32, u32>>,
) -> Option<Vec<u8>> {
    match parse_cb_compound(buf, session_id, recall_tx, slot_seqs) {
        Ok(reply) => Some(reply),
        Err(e) => {
            warn!(error = %e, "failed to handle CB_COMPOUND");
            None
        }
    }
}

fn parse_cb_compound(
    buf: &mut Bytes,
    session_id: &[u8; 16],
    recall_tx: &mpsc::Sender<RecallNotification>,
    slot_seqs: &Mutex<HashMap<u32, u32>>,
) -> Result<Vec<u8>> {
    // RPC call header: xid(4) + msg_type(4) + rpc_version(4) + program(4) + version(4) + procedure(4)
    if buf.remaining() < 24 {
        return Err(NfsError::Xdr("CB RPC header too short".to_string()));
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
        return Err(NfsError::Xdr("CB_COMPOUND args truncated".to_string()));
    }
    let _minor_version = buf.get_u32();
    let _callback_ident = buf.get_u32();

    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("CB_COMPOUND ops count truncated".to_string()));
    }
    let num_ops = buf.get_u32();
    // Bound num_ops to prevent CPU exhaustion
    if num_ops > MAX_CB_OPS {
        return Err(NfsError::Xdr(format!(
            "CB_COMPOUND has {} ops, max {}",
            num_ops, MAX_CB_OPS
        )));
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
                        return Err(NfsError::Xdr(format!(
                            "too many referring_call_lists: {}",
                            n
                        )));
                    }
                    for _ in 0..n {
                        if buf.remaining() < 16 {
                            return Err(NfsError::Xdr(
                                "referring_call sessionid truncated".to_string(),
                            ));
                        }
                        buf.advance(16);
                        if buf.remaining() < 4 {
                            return Err(NfsError::Xdr(
                                "referring_call count truncated".to_string(),
                            ));
                        }
                        let m = buf.get_u32() as usize;
                        let needed = m
                            .checked_mul(8)
                            .ok_or_else(|| NfsError::Xdr("referring_call overflow".to_string()))?;
                        if buf.remaining() < needed {
                            return Err(NfsError::Xdr("referring_call data truncated".to_string()));
                        }
                        buf.advance(needed);
                    }
                }

                // Validate session ID matches our session
                if cb_session_id != *session_id {
                    warn!("CB_SEQUENCE session ID mismatch, ignoring");
                    return Err(NfsError::Xdr("CB_SEQUENCE session ID mismatch".to_string()));
                }

                // RFC 8881 §2.10.6.3: validate sequence ID against our slot table.
                // Server MUST use sequenceid=1 on first use of a slot.
                let expected_seq = {
                    let map = slot_seqs
                        .lock()
                        .map_err(|_| NfsError::Rpc("cb slot table lock poisoned".to_string()))?;
                    map.get(&cb_slotid).copied().unwrap_or(1)
                };
                if cb_sequenceid != expected_seq {
                    warn!(
                        cb_slotid,
                        cb_sequenceid, expected_seq, "CB_SEQUENCE misordered — rejecting"
                    );
                    let mut op_reply = Vec::new();
                    op_reply.extend_from_slice(&opcode.to_be_bytes());
                    op_reply.extend_from_slice(&NFS4ERR_SEQ_MISORDERED.to_be_bytes());
                    reply_ops.push(op_reply);
                    break; // RFC: stop processing after first error
                }
                {
                    let mut map = slot_seqs
                        .lock()
                        .map_err(|_| NfsError::Rpc("cb slot table lock poisoned".to_string()))?;
                    map.insert(cb_slotid, cb_sequenceid.wrapping_add(1));
                }

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

                debug!(
                    slotid = cb_slotid,
                    seqid = cb_sequenceid,
                    "CB_SEQUENCE handled"
                );
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
                if let Err(e) = recall_tx.try_send(RecallNotification::Delegation {
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

            // CB_LAYOUTRECALL (RFC 8881 §20.3)
            5 => {
                // CB_LAYOUTRECALL4args: clora_type(4) + clora_iomode(4) + clora_changed(4)
                //                       + lor_recalltype(4) + union body
                if buf.remaining() < 16 {
                    break;
                }
                let _layout_type = buf.get_u32();
                let iomode = buf.get_u32();
                let _changed = buf.get_u32();
                let recalltype = buf.get_u32();

                let notification = match recalltype {
                    // LAYOUTRECALL4_FILE: fh<> + offset(8) + length(8) + stateid(16)
                    1 => {
                        let fh = read_opaque(buf)?;
                        if buf.remaining() < 32 {
                            return Err(NfsError::Xdr(
                                "CB_LAYOUTRECALL file args truncated".to_string(),
                            ));
                        }
                        let offset = buf.get_u64();
                        let length = buf.get_u64();
                        let mut stateid = [0u8; 16];
                        buf.copy_to_slice(&mut stateid);
                        Some(RecallNotification::LayoutFile {
                            stateid,
                            fh,
                            offset,
                            length,
                            iomode,
                        })
                    }
                    // LAYOUTRECALL4_FSID: fsid4 = major(8) + minor(8)。
                    // 客户端不跟踪 fsid 与 layout 的映射，保守地全量归还。
                    2 => {
                        if buf.remaining() < 16 {
                            return Err(NfsError::Xdr(
                                "CB_LAYOUTRECALL fsid truncated".to_string(),
                            ));
                        }
                        buf.advance(16);
                        Some(RecallNotification::LayoutAll)
                    }
                    // LAYOUTRECALL4_ALL: no body
                    3 => Some(RecallNotification::LayoutAll),
                    _ => None,
                };

                match notification {
                    Some(n) => {
                        debug!(recalltype, iomode, "CB_LAYOUTRECALL received");
                        if let Err(e) = recall_tx.try_send(n) {
                            warn!("CB_LAYOUTRECALL notification channel full or closed: {}", e);
                        }
                        // 先回 NFS4_OK，由 recall handler 异步驱逐缓存并 LAYOUTRETURN
                        // （RFC 8881 §12.5.5.1：OK 表示客户端承诺随后归还）
                        let mut op_reply = Vec::new();
                        op_reply.extend_from_slice(&opcode.to_be_bytes());
                        op_reply.extend_from_slice(&0u32.to_be_bytes()); // NFS4_OK
                        reply_ops.push(op_reply);
                    }
                    None => {
                        let mut op_reply = Vec::new();
                        op_reply.extend_from_slice(&opcode.to_be_bytes());
                        op_reply.extend_from_slice(&10022u32.to_be_bytes()); // NFS4ERR_INVAL
                        reply_ops.push(op_reply);
                        debug!(recalltype, "CB_LAYOUTRECALL unknown recalltype");
                        break; // RFC: stop processing after first error
                    }
                }
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

/// Build a minimal RPC reply message (starting at the xid; no record mark).
fn build_rpc_reply(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(24 + body.len());
    reply.extend_from_slice(&xid.to_be_bytes()); // xid
    reply.extend_from_slice(&1u32.to_be_bytes()); // msg_type = REPLY
    reply.extend_from_slice(&0u32.to_be_bytes()); // reply_stat = MSG_ACCEPTED
                                                  // Verf: AUTH_NONE
    reply.extend_from_slice(&0u32.to_be_bytes()); // flavor = AUTH_NONE
    reply.extend_from_slice(&0u32.to_be_bytes()); // body length = 0
    reply.extend_from_slice(&0u32.to_be_bytes()); // accept_stat = SUCCESS
    reply.extend_from_slice(body);
    reply
}

fn skip_rpc_auth(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("RPC auth truncated".to_string()));
    }
    let _flavor = buf.get_u32();
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("RPC auth body truncated".to_string()));
    }
    buf.advance(padded);
    Ok(())
}

fn skip_opaque(buf: &mut Bytes) -> Result<usize> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("opaque data truncated".to_string()));
    }
    buf.advance(padded);
    Ok(len)
}

fn read_opaque(buf: &mut Bytes) -> Result<Bytes> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("opaque data truncated".to_string()));
    }
    let data = buf.slice(..len);
    buf.advance(padded);
    Ok(data)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn be(v: u32) -> [u8; 4] {
        v.to_be_bytes()
    }

    fn u32_at(b: &[u8], off: usize) -> u32 {
        u32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
    }

    /// Build a CB_COMPOUND RPC CALL carrying a single CB_SEQUENCE op.
    fn cb_sequence_frame(xid: u32, session_id: &[u8; 16], slotid: u32, seqid: u32) -> Bytes {
        let mut f = Vec::new();
        f.extend_from_slice(&be(xid)); // xid
        f.extend_from_slice(&be(0)); // msg_type = CALL
        f.extend_from_slice(&be(2)); // rpc_version
        f.extend_from_slice(&be(CB_PROGRAM)); // program
        f.extend_from_slice(&be(1)); // version
        f.extend_from_slice(&be(1)); // procedure = CB_COMPOUND
        f.extend_from_slice(&be(0)); // cred flavor = AUTH_NONE
        f.extend_from_slice(&be(0)); // cred len = 0
        f.extend_from_slice(&be(0)); // verf flavor = AUTH_NONE
        f.extend_from_slice(&be(0)); // verf len = 0
        f.extend_from_slice(&be(0)); // tag len = 0
        f.extend_from_slice(&be(1)); // minorversion
        f.extend_from_slice(&be(0)); // callback_ident
        f.extend_from_slice(&be(1)); // num_ops
        f.extend_from_slice(&be(11)); // opcode = CB_SEQUENCE
        f.extend_from_slice(session_id); // sessionid
        f.extend_from_slice(&be(seqid)); // sequenceid
        f.extend_from_slice(&be(slotid)); // slotid
        f.extend_from_slice(&be(slotid)); // highest_slotid
        f.extend_from_slice(&be(0)); // cachethis
        f.extend_from_slice(&be(0)); // referring_call_lists count
        Bytes::from(f)
    }

    /// Build a CB_COMPOUND RPC CALL carrying a single CB_RECALL op.
    fn cb_recall_frame(xid: u32, stateid: &[u8; 16], truncate: bool, fh: &[u8]) -> Bytes {
        let mut f = Vec::new();
        f.extend_from_slice(&be(xid));
        f.extend_from_slice(&be(0)); // CALL
        f.extend_from_slice(&be(2));
        f.extend_from_slice(&be(CB_PROGRAM));
        f.extend_from_slice(&be(1));
        f.extend_from_slice(&be(1)); // CB_COMPOUND
        f.extend_from_slice(&be(0)); // cred
        f.extend_from_slice(&be(0));
        f.extend_from_slice(&be(0)); // verf
        f.extend_from_slice(&be(0));
        f.extend_from_slice(&be(0)); // tag
        f.extend_from_slice(&be(1)); // minorversion
        f.extend_from_slice(&be(0)); // callback_ident
        f.extend_from_slice(&be(1)); // num_ops
        f.extend_from_slice(&be(4)); // opcode = CB_RECALL
        f.extend_from_slice(stateid); // stateid(16)
        f.extend_from_slice(&be(if truncate { 1 } else { 0 })); // truncate
        f.extend_from_slice(&be(fh.len() as u32)); // fh opaque len
        f.extend_from_slice(fh);
        let pad = (4 - fh.len() % 4) % 4;
        f.extend_from_slice(&[0u8; 4][..pad]);
        Bytes::from(f)
    }

    #[test]
    fn cb_sequence_ok_then_misordered() {
        let session_id = [7u8; 16];
        let (tx, _rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());

        // First CB_SEQUENCE on slot 0 with seqid 1 → accepted.
        let mut frame = cb_sequence_frame(0xAABBCCDD, &session_id, 0, 1);
        let reply = handle_cb_compound(&mut frame, &session_id, &tx, &slots).unwrap();
        // Reply layout: xid + msg_type(=1) + reply_stat + verf(flavor+len) + accept_stat
        //   + compound{status + tag_len + resarray_len} + op{opcode + op_status + ...}
        assert_eq!(u32_at(&reply, 0), 0xAABBCCDD); // xid echoed
        assert_eq!(u32_at(&reply, 4), 1); // msg_type = REPLY
        assert_eq!(u32_at(&reply, 24), 0); // CB_COMPOUND status = NFS4_OK
        assert_eq!(u32_at(&reply, 32), 1); // resarray len = 1
        assert_eq!(u32_at(&reply, 36), 11); // opcode = CB_SEQUENCE
        assert_eq!(u32_at(&reply, 40), 0); // op status = NFS4_OK

        // Replaying seqid 1 on slot 0 is misordered (expected is now 2).
        let mut frame2 = cb_sequence_frame(0xAABBCCDE, &session_id, 0, 1);
        let reply2 = handle_cb_compound(&mut frame2, &session_id, &tx, &slots).unwrap();
        assert_eq!(u32_at(&reply2, 36), 11); // opcode
        assert_eq!(u32_at(&reply2, 40), NFS4ERR_SEQ_MISORDERED); // misordered
    }

    #[test]
    fn cb_sequence_session_mismatch_dropped() {
        let session_id = [1u8; 16];
        let wrong = [2u8; 16];
        let (tx, _rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());
        // Frame carries `wrong` session id; handler must reject → None.
        let mut frame = cb_sequence_frame(1, &wrong, 0, 1);
        assert!(handle_cb_compound(&mut frame, &session_id, &tx, &slots).is_none());
    }

    #[test]
    fn cb_recall_forwards_notification() {
        let session_id = [9u8; 16];
        let stateid = [0xEE; 16];
        let fh = b"file-handle-xyz"; // 15 bytes → exercises XDR padding
        let (tx, mut rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());

        let mut frame = cb_recall_frame(0x11223344, &stateid, true, fh);
        let reply = handle_cb_compound(&mut frame, &session_id, &tx, &slots).unwrap();
        assert_eq!(u32_at(&reply, 36), 4); // opcode = CB_RECALL
        assert_eq!(u32_at(&reply, 40), 0); // NFS4_OK

        match rx.try_recv().expect("recall notification forwarded") {
            RecallNotification::Delegation {
                stateid: sid,
                truncate,
                fh: nfh,
            } => {
                assert_eq!(sid, stateid);
                assert!(truncate);
                assert_eq!(&nfh[..], &fh[..]);
            }
            other => panic!("expected Delegation, got {other:?}"),
        }
    }

    /// Build a CB_COMPOUND RPC CALL carrying a single CB_LAYOUTRECALL op.
    /// `file_body`: Some((fh, offset, length, stateid)) for FILE; None for FSID/ALL.
    fn cb_layoutrecall_frame(
        xid: u32,
        iomode: u32,
        recalltype: u32,
        file_body: Option<(&[u8], u64, u64, &[u8; 16])>,
    ) -> Bytes {
        let mut f = Vec::new();
        f.extend_from_slice(&be(xid));
        f.extend_from_slice(&be(0)); // CALL
        f.extend_from_slice(&be(2));
        f.extend_from_slice(&be(CB_PROGRAM));
        f.extend_from_slice(&be(1));
        f.extend_from_slice(&be(1)); // CB_COMPOUND
        f.extend_from_slice(&be(0)); // cred
        f.extend_from_slice(&be(0));
        f.extend_from_slice(&be(0)); // verf
        f.extend_from_slice(&be(0));
        f.extend_from_slice(&be(0)); // tag
        f.extend_from_slice(&be(1)); // minorversion
        f.extend_from_slice(&be(0)); // callback_ident
        f.extend_from_slice(&be(1)); // num_ops
        f.extend_from_slice(&be(5)); // opcode = CB_LAYOUTRECALL
        f.extend_from_slice(&be(1)); // clora_type = LAYOUT4_NFSV4_1_FILES
        f.extend_from_slice(&be(iomode)); // clora_iomode
        f.extend_from_slice(&be(0)); // clora_changed = false
        f.extend_from_slice(&be(recalltype));
        match recalltype {
            1 => {
                let (fh, offset, length, stateid) = file_body.expect("FILE recall needs body");
                f.extend_from_slice(&be(fh.len() as u32));
                f.extend_from_slice(fh);
                let pad = (4 - fh.len() % 4) % 4;
                f.extend_from_slice(&[0u8; 4][..pad]);
                f.extend_from_slice(&offset.to_be_bytes());
                f.extend_from_slice(&length.to_be_bytes());
                f.extend_from_slice(stateid);
            }
            2 => {
                f.extend_from_slice(&[0u8; 16]); // fsid4: major(8) + minor(8)
            }
            _ => {}
        }
        Bytes::from(f)
    }

    #[test]
    fn cb_layoutrecall_file_forwards_notification() {
        let session_id = [3u8; 16];
        let stateid = [0xAB; 16];
        let fh = b"layout-fh-123"; // 13 bytes → exercises XDR padding
        let (tx, mut rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());

        let mut frame = cb_layoutrecall_frame(
            0x55667788,
            2, // LAYOUTIOMODE4_RW
            1, // LAYOUTRECALL4_FILE
            Some((fh, 0, u64::MAX, &stateid)),
        );
        let reply = handle_cb_compound(&mut frame, &session_id, &tx, &slots).unwrap();
        assert_eq!(u32_at(&reply, 36), 5); // opcode = CB_LAYOUTRECALL
        assert_eq!(u32_at(&reply, 40), 0); // NFS4_OK

        match rx.try_recv().expect("layout recall forwarded") {
            RecallNotification::LayoutFile {
                stateid: sid,
                fh: nfh,
                offset,
                length,
                iomode,
            } => {
                assert_eq!(sid, stateid);
                assert_eq!(&nfh[..], &fh[..]);
                assert_eq!(offset, 0);
                assert_eq!(length, u64::MAX);
                assert_eq!(iomode, 2);
            }
            other => panic!("expected LayoutFile, got {other:?}"),
        }
    }

    #[test]
    fn cb_layoutrecall_all_forwards_notification() {
        let session_id = [3u8; 16];
        let (tx, mut rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());

        for recalltype in [2u32, 3u32] {
            let mut frame = cb_layoutrecall_frame(1, 3, recalltype, None);
            let reply = handle_cb_compound(&mut frame, &session_id, &tx, &slots).unwrap();
            assert_eq!(u32_at(&reply, 40), 0); // NFS4_OK
            match rx.try_recv().expect("layout recall forwarded") {
                RecallNotification::LayoutAll => {}
                other => panic!("expected LayoutAll, got {other:?}"),
            }
        }
    }

    #[test]
    fn cb_layoutrecall_truncated_no_panic() {
        let session_id = [3u8; 16];
        let (tx, mut rx) = mpsc::channel(4);
        let slots = Mutex::new(HashMap::new());

        // FILE recall 但 fh 之后的 offset/length/stateid 被截断
        let full = cb_layoutrecall_frame(1, 2, 1, Some((b"fh", 7, 9, &[1u8; 16])));
        let mut truncated = full.slice(..full.len() - 20);
        // 截断帧按错误路径丢弃（None），不 panic，也不发通知
        assert!(handle_cb_compound(&mut truncated, &session_id, &tx, &slots).is_none());
        assert!(rx.try_recv().is_err());
    }
}

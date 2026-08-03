//! NFSv4.1 callback handling and bounded backchannel replay (RFC 8881 §2.10.6.3).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::{NfsError, Result};
use crate::rpc::BackchannelHandler;

const MAX_CB_OPS: u32 = 64;
const MAX_REFERRING_LISTS: usize = 256;
const NFS4_OK: u32 = 0;
const NFS4ERR_DELAY: u32 = 10008;
const NFS4ERR_BADXDR: u32 = 10036;
const NFS4ERR_BADSESSION: u32 = 10052;
const NFS4ERR_BADSLOT: u32 = 10053;
const NFS4ERR_OP_ILLEGAL: u32 = 10044;
const NFS4ERR_SEQ_FALSE_RETRY: u32 = 10062;
const NFS4ERR_SEQ_MISORDERED: u32 = 10063;
const NFS4ERR_OP_NOT_IN_SESSION: u32 = 10071;
const NFS4ERR_MINOR_VERS_MISMATCH: u32 = 10021;
const NFS4ERR_INVAL: u32 = 22;

const CB_RECALL: u32 = 4;
const CB_LAYOUTRECALL: u32 = 5;
const CB_SEQUENCE: u32 = 11;

/// Callback program number communicated in CREATE_SESSION.
pub(crate) const CB_PROGRAM: u32 = 0x40000000;

#[derive(Debug, Clone)]
pub(crate) enum RecallNotification {
    Delegation {
        stateid: [u8; 16],
        #[allow(dead_code)]
        truncate: bool,
        fh: Bytes,
    },
    LayoutFile {
        stateid: [u8; 16],
        fh: Bytes,
        offset: u64,
        length: u64,
        iomode: u32,
    },
    LayoutAll,
}

#[derive(Default)]
struct CallbackSlot {
    next_sequence: u32,
    request: Option<Bytes>,
    compound_reply: Option<Arc<[u8]>>,
}

struct CallbackSession {
    session_id: [u8; 16],
    generation: u64,
    max_request_size: usize,
    max_operations: u32,
    slots: Vec<CallbackSlot>,
}

/// Synchronous state shared by the RPC reader and session recovery publisher.
///
/// The lock covers one small (negotiated <= 4 KiB by default) callback parse so two
/// duplicate deliveries cannot both publish a recall side effect.
pub(crate) struct CallbackState {
    inner: Mutex<CallbackSession>,
    layout_recalls_received: AtomicU64,
    layout_returns_completed: AtomicU64,
}

impl CallbackState {
    #[cfg(test)]
    pub(crate) fn new(session_id: [u8; 16], generation: u64, max_requests: u32) -> Arc<Self> {
        Self::new_negotiated(session_id, generation, max_requests, 4096, MAX_CB_OPS)
    }

    pub(crate) fn new_negotiated(
        session_id: [u8; 16],
        generation: u64,
        max_requests: u32,
        max_request_size: u32,
        max_operations: u32,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(CallbackSession {
                session_id,
                generation,
                max_request_size: max_request_size.max(1) as usize,
                max_operations: max_operations.clamp(1, MAX_CB_OPS),
                slots: callback_slots(max_requests),
            }),
            layout_recalls_received: AtomicU64::new(0),
            layout_returns_completed: AtomicU64::new(0),
        })
    }

    pub(crate) fn record_layout_recall(&self) {
        self.layout_recalls_received.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_layout_return(&self) {
        self.layout_returns_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn layout_recall_stats(&self) -> (u64, u64) {
        (
            self.layout_recalls_received.load(Ordering::Relaxed),
            self.layout_returns_completed.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn update_session(
        &self,
        session_id: [u8; 16],
        generation: u64,
        max_requests: u32,
        max_request_size: u32,
        max_operations: u32,
    ) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| NfsError::Rpc("callback state lock poisoned".to_string()))?;
        if generation < state.generation {
            return Err(NfsError::Rpc(
                "refusing stale callback session publication".to_string(),
            ));
        }
        if generation == state.generation && session_id == state.session_id {
            return Ok(());
        }
        state.session_id = session_id;
        state.generation = generation;
        state.max_request_size = max_request_size.max(1) as usize;
        state.max_operations = max_operations.clamp(1, MAX_CB_OPS);
        state.slots = callback_slots(max_requests);
        Ok(())
    }
}

fn callback_slots(max_requests: u32) -> Vec<CallbackSlot> {
    let count = max_requests.max(1) as usize;
    (0..count)
        .map(|_| CallbackSlot {
            next_sequence: 1,
            ..CallbackSlot::default()
        })
        .collect()
}

pub(crate) fn make_backchannel_handler(
    state: Arc<CallbackState>,
    recall_tx: mpsc::Sender<RecallNotification>,
) -> BackchannelHandler {
    Arc::new(move |frame| handle_cb_rpc(frame, &state, &recall_tx))
}

fn handle_cb_rpc(
    mut frame: Bytes,
    state: &CallbackState,
    recall_tx: &mpsc::Sender<RecallNotification>,
) -> Option<Vec<u8>> {
    let xid = if frame.remaining() >= 4 {
        frame.get_u32()
    } else {
        return None;
    };
    match parse_rpc_call(xid, &mut frame, state, recall_tx) {
        Ok(reply) => Some(reply),
        Err(error) => {
            warn!(error = %error, xid, "failed to handle CB_COMPOUND");
            Some(build_rpc_reply(
                xid,
                &compound_error_body(&[], NFS4ERR_BADXDR),
            ))
        }
    }
}

fn parse_rpc_call(
    xid: u32,
    buf: &mut Bytes,
    state: &CallbackState,
    recall_tx: &mpsc::Sender<RecallNotification>,
) -> Result<Vec<u8>> {
    if buf.remaining() < 20 {
        return Err(NfsError::Xdr("CB RPC header too short".to_string()));
    }
    let msg_type = buf.get_u32();
    let rpc_version = buf.get_u32();
    let program = buf.get_u32();
    let version = buf.get_u32();
    let procedure = buf.get_u32();
    if msg_type != 0 || rpc_version != 2 || program != CB_PROGRAM || version != 1 {
        return Err(NfsError::Rpc(format!(
            "invalid callback RPC header type={msg_type} rpc={rpc_version} program={program} version={version}"
        )));
    }
    let credential = read_rpc_auth(buf)?;
    let verifier = read_rpc_auth(buf)?;
    if credential != (0, 0) || verifier != (0, 0) {
        return Err(NfsError::Rpc(
            "callback RPC authentication does not match negotiated AUTH_NONE".to_string(),
        ));
    }
    if procedure == 0 {
        return Ok(build_rpc_reply(xid, &[]));
    }
    if procedure != 1 {
        return Err(NfsError::Rpc(format!(
            "unsupported callback procedure {procedure}"
        )));
    }

    let request = buf.clone();
    let mut session = state
        .inner
        .lock()
        .map_err(|_| NfsError::Rpc("callback state lock poisoned".to_string()))?;
    let compound = parse_compound(buf, request, &mut session, recall_tx)?;
    Ok(build_rpc_reply(xid, &compound))
}

fn parse_compound(
    buf: &mut Bytes,
    request: Bytes,
    session: &mut CallbackSession,
    recall_tx: &mpsc::Sender<RecallNotification>,
) -> Result<Arc<[u8]>> {
    let tag = read_opaque(buf)?;
    if buf.remaining() < 12 {
        return Err(NfsError::Xdr("CB_COMPOUND args truncated".to_string()));
    }
    let minor_version = buf.get_u32();
    let _callback_ident = buf.get_u32();
    let num_ops = buf.get_u32();
    if minor_version != 1 {
        return Ok(compound_error_body(&tag, NFS4ERR_MINOR_VERS_MISMATCH).into());
    }
    if request.len() > session.max_request_size || num_ops > session.max_operations {
        return Ok(compound_error_body(&tag, NFS4ERR_BADXDR).into());
    }
    if num_ops == 0 {
        return Ok(compound_body(&tag, NFS4_OK, &[]).into());
    }
    if buf.remaining() < 4 {
        return Ok(compound_error_body(&tag, NFS4ERR_BADXDR).into());
    }
    let first_opcode = buf.get_u32();
    if first_opcode != CB_SEQUENCE {
        let op = op_status(first_opcode, NFS4ERR_OP_NOT_IN_SESSION);
        return Ok(compound_body(&tag, NFS4ERR_OP_NOT_IN_SESSION, &[op]).into());
    }

    let sequence = match parse_sequence(buf) {
        Ok(sequence) => sequence,
        Err(_) => {
            let op = op_status(CB_SEQUENCE, NFS4ERR_BADXDR);
            return Ok(compound_body(&tag, NFS4ERR_BADXDR, &[op]).into());
        }
    };
    if sequence.session_id != session.session_id {
        let op = op_status(CB_SEQUENCE, NFS4ERR_BADSESSION);
        return Ok(compound_body(&tag, NFS4ERR_BADSESSION, &[op]).into());
    }
    let max_slot = session.slots.len().saturating_sub(1) as u32;
    let Some(slot) = session.slots.get_mut(sequence.slot_id as usize) else {
        let op = op_status(CB_SEQUENCE, NFS4ERR_BADSLOT);
        return Ok(compound_body(&tag, NFS4ERR_BADSLOT, &[op]).into());
    };

    if sequence.sequence_id == slot.next_sequence.wrapping_sub(1) && slot.request.is_some() {
        if slot.request.as_ref() == Some(&request)
            && let Some(reply) = &slot.compound_reply
        {
            debug!(
                generation = session.generation,
                slot = sequence.slot_id,
                sequence = sequence.sequence_id,
                "replaying cached callback reply"
            );
            return Ok(Arc::clone(reply));
        }
        let op = op_status(CB_SEQUENCE, NFS4ERR_SEQ_FALSE_RETRY);
        return Ok(compound_body(&tag, NFS4ERR_SEQ_FALSE_RETRY, &[op]).into());
    }
    if sequence.sequence_id != slot.next_sequence {
        let op = op_status(CB_SEQUENCE, NFS4ERR_SEQ_MISORDERED);
        return Ok(compound_body(&tag, NFS4ERR_SEQ_MISORDERED, &[op]).into());
    }

    let mut replies = vec![sequence_reply(&sequence, max_slot, session.session_id)];
    let mut status = NFS4_OK;
    for _ in 1..num_ops {
        if buf.remaining() < 4 {
            status = NFS4ERR_BADXDR;
            break;
        }
        let opcode = buf.get_u32();
        let (reply, op_status) = parse_callback_op(opcode, buf, recall_tx)?;
        replies.push(reply);
        status = op_status;
        if status != NFS4_OK {
            break;
        }
    }
    let reply: Arc<[u8]> = compound_body(&tag, status, &replies).into();
    slot.next_sequence = sequence.sequence_id.wrapping_add(1);
    slot.request = Some(request);
    slot.compound_reply = Some(Arc::clone(&reply));
    debug!(
        generation = session.generation,
        slot = sequence.slot_id,
        sequence = sequence.sequence_id,
        status,
        "callback request completed and cached"
    );
    Ok(reply)
}

struct SequenceArgs {
    session_id: [u8; 16],
    sequence_id: u32,
    slot_id: u32,
}

fn parse_sequence(buf: &mut Bytes) -> Result<SequenceArgs> {
    if buf.remaining() < 32 {
        return Err(NfsError::Xdr("CB_SEQUENCE truncated".to_string()));
    }
    let mut session_id = [0; 16];
    buf.copy_to_slice(&mut session_id);
    let sequence_id = buf.get_u32();
    let slot_id = buf.get_u32();
    let _highest_slot_id = buf.get_u32();
    let _cache_this = buf.get_u32();
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr(
            "referring call list count truncated".to_string(),
        ));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_REFERRING_LISTS {
        return Err(NfsError::Xdr("too many referring call lists".to_string()));
    }
    for _ in 0..count {
        if buf.remaining() < 20 {
            return Err(NfsError::Xdr("referring call list truncated".to_string()));
        }
        buf.advance(16);
        let calls = buf.get_u32() as usize;
        let bytes = calls
            .checked_mul(8)
            .ok_or_else(|| NfsError::Xdr("referring call list overflow".to_string()))?;
        if buf.remaining() < bytes {
            return Err(NfsError::Xdr("referring calls truncated".to_string()));
        }
        buf.advance(bytes);
    }
    Ok(SequenceArgs {
        session_id,
        sequence_id,
        slot_id,
    })
}

fn parse_callback_op(
    opcode: u32,
    buf: &mut Bytes,
    recall_tx: &mpsc::Sender<RecallNotification>,
) -> Result<(Vec<u8>, u32)> {
    match opcode {
        CB_RECALL => {
            if buf.remaining() < 20 {
                return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR));
            }
            let mut stateid = [0; 16];
            buf.copy_to_slice(&mut stateid);
            let truncate = buf.get_u32() != 0;
            let fh = match read_opaque(buf) {
                Ok(fh) => fh,
                Err(_) => return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR)),
            };
            let status = if recall_tx
                .try_send(RecallNotification::Delegation {
                    stateid,
                    truncate,
                    fh,
                })
                .is_ok()
            {
                NFS4_OK
            } else {
                NFS4ERR_DELAY
            };
            Ok((op_status(opcode, status), status))
        }
        CB_LAYOUTRECALL => parse_layout_recall(opcode, buf, recall_tx),
        _ => Ok((op_status(opcode, NFS4ERR_OP_ILLEGAL), NFS4ERR_OP_ILLEGAL)),
    }
}

fn parse_layout_recall(
    opcode: u32,
    buf: &mut Bytes,
    recall_tx: &mpsc::Sender<RecallNotification>,
) -> Result<(Vec<u8>, u32)> {
    if buf.remaining() < 16 {
        return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR));
    }
    let _layout_type = buf.get_u32();
    let iomode = buf.get_u32();
    let _changed = buf.get_u32();
    let recall_type = buf.get_u32();
    let notification = match recall_type {
        1 => {
            let fh = match read_opaque(buf) {
                Ok(fh) => fh,
                Err(_) => return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR)),
            };
            if buf.remaining() < 32 {
                return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR));
            }
            let offset = buf.get_u64();
            let length = buf.get_u64();
            let mut stateid = [0; 16];
            buf.copy_to_slice(&mut stateid);
            RecallNotification::LayoutFile {
                stateid,
                fh,
                offset,
                length,
                iomode,
            }
        }
        2 => {
            if buf.remaining() < 16 {
                return Ok((op_status(opcode, NFS4ERR_BADXDR), NFS4ERR_BADXDR));
            }
            buf.advance(16);
            RecallNotification::LayoutAll
        }
        3 => RecallNotification::LayoutAll,
        _ => return Ok((op_status(opcode, NFS4ERR_INVAL), NFS4ERR_INVAL)),
    };
    let status = if recall_tx.try_send(notification).is_ok() {
        NFS4_OK
    } else {
        NFS4ERR_DELAY
    };
    Ok((op_status(opcode, status), status))
}

fn sequence_reply(sequence: &SequenceArgs, max_slot: u32, session_id: [u8; 16]) -> Vec<u8> {
    let mut reply = op_status(CB_SEQUENCE, NFS4_OK);
    reply.extend_from_slice(&session_id);
    reply.extend_from_slice(&sequence.sequence_id.to_be_bytes());
    reply.extend_from_slice(&sequence.slot_id.to_be_bytes());
    reply.extend_from_slice(&max_slot.to_be_bytes());
    reply.extend_from_slice(&max_slot.to_be_bytes());
    reply
}

fn op_status(opcode: u32, status: u32) -> Vec<u8> {
    let mut reply = Vec::with_capacity(8);
    reply.extend_from_slice(&opcode.to_be_bytes());
    reply.extend_from_slice(&status.to_be_bytes());
    reply
}

fn compound_error_body(tag: &[u8], status: u32) -> Vec<u8> {
    compound_body(tag, status, &[])
}

fn compound_body(tag: &[u8], status: u32, replies: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&status.to_be_bytes());
    write_opaque(&mut body, tag);
    body.extend_from_slice(&(replies.len() as u32).to_be_bytes());
    for reply in replies {
        body.extend_from_slice(reply);
    }
    body
}

fn build_rpc_reply(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(24 + body.len());
    reply.extend_from_slice(&xid.to_be_bytes());
    reply.extend_from_slice(&1u32.to_be_bytes());
    reply.extend_from_slice(&0u32.to_be_bytes());
    reply.extend_from_slice(&0u32.to_be_bytes());
    reply.extend_from_slice(&0u32.to_be_bytes());
    reply.extend_from_slice(&0u32.to_be_bytes());
    reply.extend_from_slice(body);
    reply
}

fn read_rpc_auth(buf: &mut Bytes) -> Result<(u32, usize)> {
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("RPC auth truncated".to_string()));
    }
    let flavor = buf.get_u32();
    let len = buf.get_u32() as usize;
    let padded = len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("RPC auth length overflow".to_string()))?
        & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("RPC auth body truncated".to_string()));
    }
    buf.advance(padded);
    Ok((flavor, len))
}

fn read_opaque(buf: &mut Bytes) -> Result<Bytes> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("opaque length overflow".to_string()))?
        & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("opaque data truncated".to_string()));
    }
    let value = buf.slice(..len);
    buf.advance(padded);
    Ok(value)
}

fn write_opaque(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    const ZERO_PAD: [u8; 3] = [0; 3];
    buf.extend_from_slice(&ZERO_PAD[..(4 - value.len() % 4) % 4]);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn be(value: u32) -> [u8; 4] {
        value.to_be_bytes()
    }

    fn frame(
        xid: u32,
        session: [u8; 16],
        sequence: u32,
        slot: u32,
        tag: &[u8],
        recall: Option<u8>,
    ) -> Bytes {
        let mut value = Vec::new();
        for word in [xid, 0, 2, CB_PROGRAM, 1, 1, 0, 0, 0, 0] {
            value.extend_from_slice(&be(word));
        }
        write_opaque(&mut value, tag);
        value.extend_from_slice(&be(1));
        value.extend_from_slice(&be(0));
        value.extend_from_slice(&be(if recall.is_some() { 2 } else { 1 }));
        value.extend_from_slice(&be(CB_SEQUENCE));
        value.extend_from_slice(&session);
        value.extend_from_slice(&be(sequence));
        value.extend_from_slice(&be(slot));
        value.extend_from_slice(&be(slot));
        value.extend_from_slice(&be(1));
        value.extend_from_slice(&be(0));
        if let Some(marker) = recall {
            value.extend_from_slice(&be(CB_RECALL));
            value.extend_from_slice(&[marker; 16]);
            value.extend_from_slice(&be(0));
            write_opaque(&mut value, &[marker]);
        }
        value.into()
    }

    fn u32_at(value: &[u8], offset: usize) -> u32 {
        u32::from_be_bytes(value[offset..offset + 4].try_into().unwrap())
    }

    #[test]
    fn scripted_callback_reply_loss_replays_cached_body_and_executes_recall_once() {
        let session = [7; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, mut rx) = mpsc::channel(4);
        let first = handle_cb_rpc(frame(1, session, 1, 0, b"tag", Some(9)), &state, &tx).unwrap();
        let replay = handle_cb_rpc(frame(2, session, 1, 0, b"tag", Some(9)), &state, &tx).unwrap();
        assert_eq!(&first[24..], &replay[24..]);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn false_retry_misorder_session_and_slot_errors_have_no_side_effect() {
        let session = [7; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, mut rx) = mpsc::channel(8);
        let _ = handle_cb_rpc(frame(1, session, 1, 0, b"a", Some(1)), &state, &tx);
        assert!(rx.try_recv().is_ok());
        for (request, expected) in [
            (
                frame(2, session, 1, 0, b"b", Some(2)),
                NFS4ERR_SEQ_FALSE_RETRY,
            ),
            (
                frame(3, session, 3, 0, b"a", Some(3)),
                NFS4ERR_SEQ_MISORDERED,
            ),
            (frame(4, [8; 16], 2, 0, b"a", Some(4)), NFS4ERR_BADSESSION),
            (frame(5, session, 2, 1, b"a", Some(5)), NFS4ERR_BADSLOT),
        ] {
            let reply = handle_cb_rpc(request, &state, &tx).unwrap();
            assert_eq!(u32_at(&reply, 24), expected);
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn next_sequence_and_session_generation_reset_are_accepted() {
        let first_session = [1; 16];
        let state = CallbackState::new(first_session, 1, 2);
        let (tx, _) = mpsc::channel(4);
        for sequence in [1, 2] {
            let reply = handle_cb_rpc(
                frame(sequence, first_session, sequence, 1, b"", None),
                &state,
                &tx,
            )
            .unwrap();
            assert_eq!(u32_at(&reply, 24), NFS4_OK);
        }
        state
            .update_session([2; 16], 2, 1, 4096, MAX_CB_OPS)
            .unwrap();
        let old = handle_cb_rpc(frame(3, first_session, 3, 0, b"", None), &state, &tx).unwrap();
        let new = handle_cb_rpc(frame(4, [2; 16], 1, 0, b"", None), &state, &tx).unwrap();
        assert_eq!(u32_at(&old, 24), NFS4ERR_BADSESSION);
        assert_eq!(u32_at(&new, 24), NFS4_OK);
    }

    #[test]
    fn opaque_tag_is_echoed_and_operation_error_sets_compound_status() {
        let session = [3; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, _) = mpsc::channel(1);
        let tag = [0xff, 0x00, 0x81];
        let mut request = frame(1, session, 1, 0, &tag, None).to_vec();
        let op_count_offset = 40 + 4 + ((tag.len() + 3) & !3) + 8;
        request[op_count_offset..op_count_offset + 4].copy_from_slice(&be(2));
        request.extend_from_slice(&be(0xfeed));
        let reply = handle_cb_rpc(request.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_OP_ILLEGAL);
        assert_eq!(u32_at(&reply, 28), tag.len() as u32);
        assert_eq!(&reply[32..35], &tag);
    }

    #[test]
    fn concurrent_duplicates_have_one_recall_side_effect() {
        let session = [4; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, mut rx) = mpsc::channel(64);
        std::thread::scope(|scope| {
            for xid in 0..64 {
                let state = Arc::clone(&state);
                let tx = tx.clone();
                scope.spawn(move || {
                    let reply = handle_cb_rpc(frame(xid, session, 1, 0, b"", Some(1)), &state, &tx);
                    assert!(reply.is_some());
                });
            }
        });
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn malformed_and_wrong_rpc_fields_return_deterministic_errors() {
        let session = [5; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, _) = mpsc::channel(1);
        let valid = frame(1, session, 1, 0, b"", None);
        for length in [4, 12, valid.len() - 1] {
            let reply = handle_cb_rpc(valid.slice(..length), &state, &tx).unwrap();
            assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);
        }
        for offset in [8usize, 12, 16, 24, 32] {
            let mut wrong = valid.to_vec();
            wrong[offset..offset + 4].copy_from_slice(&be(99));
            let reply = handle_cb_rpc(wrong.into(), &state, &tx).unwrap();
            assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);
        }
    }

    #[test]
    fn zero_ops_minor_version_excessive_ops_and_sequence_position_are_validated() {
        let session = [6; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, _) = mpsc::channel(1);
        let valid = frame(1, session, 1, 0, b"", None);
        let minor_offset = 44;
        let op_count_offset = 52;

        let mut zero_ops = valid.to_vec();
        zero_ops[op_count_offset..op_count_offset + 4].copy_from_slice(&be(0));
        zero_ops.truncate(op_count_offset + 4);
        let reply = handle_cb_rpc(zero_ops.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4_OK);
        assert_eq!(u32_at(&reply, 32), 0);

        let mut wrong_minor = valid.to_vec();
        wrong_minor[minor_offset..minor_offset + 4].copy_from_slice(&be(0));
        let reply = handle_cb_rpc(wrong_minor.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_MINOR_VERS_MISMATCH);

        let mut excessive = valid.to_vec();
        excessive[op_count_offset..op_count_offset + 4].copy_from_slice(&be(MAX_CB_OPS + 1));
        let reply = handle_cb_rpc(excessive.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);

        let mut wrong_first_op = valid.to_vec();
        wrong_first_op[op_count_offset + 4..op_count_offset + 8].copy_from_slice(&be(CB_RECALL));
        let reply = handle_cb_rpc(wrong_first_op.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_OP_NOT_IN_SESSION);
    }

    #[test]
    fn excessive_referring_calls_and_full_recall_queue_do_not_execute_side_effect() {
        let session = [9; 16];
        let state = CallbackState::new(session, 1, 1);
        let (tx, mut rx) = mpsc::channel(1);
        let valid = frame(1, session, 1, 0, b"", None);
        let referring_count_offset = 92;
        let mut excessive = valid.to_vec();
        excessive[referring_count_offset..referring_count_offset + 4]
            .copy_from_slice(&be(MAX_REFERRING_LISTS as u32 + 1));
        let reply = handle_cb_rpc(excessive.into(), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);

        tx.try_send(RecallNotification::LayoutAll).unwrap();
        let reply = handle_cb_rpc(frame(2, session, 1, 0, b"", Some(1)), &state, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_DELAY);
        assert!(matches!(
            rx.try_recv().unwrap(),
            RecallNotification::LayoutAll
        ));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn negotiated_request_operation_and_slot_storage_bounds_are_enforced() {
        let session = [10; 16];
        let (tx, mut rx) = mpsc::channel(2);
        let request = frame(1, session, 1, 0, b"", Some(1));

        let operation_limited = CallbackState::new_negotiated(session, 1, 1, 4096, 1);
        let reply = handle_cb_rpc(request.clone(), &operation_limited, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);

        let size_limited =
            CallbackState::new_negotiated(session, 1, 1, request.len() as u32 - 41, 2);
        let reply = handle_cb_rpc(request, &size_limited, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_BADXDR);
        assert!(rx.try_recv().is_err());
        assert_eq!(
            operation_limited.inner.lock().unwrap().slots.len(),
            1,
            "slot cache must remain bounded by ca_maxrequests"
        );

        let fresh = CallbackState::new(session, 1, 1);
        let reply = handle_cb_rpc(frame(3, session, 0, 0, b"", None), &fresh, &tx).unwrap();
        assert_eq!(u32_at(&reply, 24), NFS4ERR_SEQ_MISORDERED);
    }
}

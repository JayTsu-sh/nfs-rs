#![cfg(test)]
//! Local TCP fixture exercising production RPC and COMPOUND codecs.
use bytes::{Buf, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::mount::{Mount41, test_mount};
use crate::nfs4::compound::{take_opaque, xdr_opaque, xdr_u32};
use crate::rpc::{Client, StreamMux};

pub(super) struct Reply {
    pub opcode: u32,
    pub status: u32,
    pub data: Vec<u8>,
}

pub(super) fn ok(opcode: u32, data: Vec<u8>) -> Reply {
    Reply {
        opcode,
        status: 0,
        data,
    }
}

pub(super) fn opaque(data: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    xdr_opaque(&mut encoded, data);
    encoded
}

pub(super) fn putfh(args: &mut Bytes) {
    assert_eq!(args.get_u32(), 22);
    take_opaque(args, "fh").unwrap();
}

pub(super) async fn serve(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
) -> (Mount41, JoinHandle<()>) {
    serve_with_reply_hook(handler, None, |_| {}).await
}

pub(super) async fn serve_with_reply_hook(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    reply_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    after_reply: impl FnMut(&str) + Send + 'static,
) -> (Mount41, JoinHandle<()>) {
    serve_version(handler, reply_gate, after_reply, super::MinorVersion::V1).await
}

pub(super) async fn serve_v42(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
) -> (Mount41, JoinHandle<()>) {
    serve_version(handler, None, |_| {}, super::MinorVersion::V2).await
}

pub(super) async fn serve_v42_with_reply_gate(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    gate: tokio::sync::oneshot::Receiver<()>,
) -> (Mount41, JoinHandle<()>) {
    serve_version(handler, Some(gate), |_| {}, super::MinorVersion::V2).await
}

pub(super) type ObservedSequences = std::sync::Arc<std::sync::Mutex<Vec<(u32, u32, u32)>>>;

pub(super) async fn serve_observed(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    minor: super::MinorVersion,
    observed: ObservedSequences,
) -> (Mount41, JoinHandle<()>) {
    serve_version_observed(handler, None, |_| {}, minor, Some(observed)).await
}

async fn serve_version(
    handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    reply_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    after_reply: impl FnMut(&str) + Send + 'static,
    minor: super::MinorVersion,
) -> (Mount41, JoinHandle<()>) {
    serve_version_observed(handler, reply_gate, after_reply, minor, None).await
}

async fn serve_version_observed(
    mut handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    mut reply_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    mut after_reply: impl FnMut(&str) + Send + 'static,
    minor: super::MinorVersion,
    observed: Option<ObservedSequences>,
) -> (Mount41, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        while let Ok(marker) = stream.read_u32().await {
            assert_ne!(marker & 0x80000000, 0);
            let mut frame = vec![0; (marker & 0x7fffffff) as usize];
            stream.read_exact(&mut frame).await.unwrap();
            let mut request = Bytes::from(frame);
            let xid = request.get_u32();
            request.advance(20); // msg type, RPC version, program, version, procedure
            for _ in 0..2 {
                request.advance(4); // auth flavor
                take_opaque(&mut request, "auth").unwrap();
            }
            let tag = take_opaque(&mut request, "tag").unwrap();
            assert_eq!(request.get_u32(), minor as u32);
            let _count = request.get_u32();
            let opcode = request.get_u32();
            let sequence = if opcode == 53 {
                let mut sequence = request.split_to(28).to_vec();
                if let Some(observed) = &observed {
                    let seq = u32::from_be_bytes(sequence[16..20].try_into().unwrap());
                    let slot = u32::from_be_bytes(sequence[20..24].try_into().unwrap());
                    observed.lock().unwrap().push((xid, seq, slot));
                }
                request.advance(4); // cachethis
                xdr_u32(&mut sequence, 0); // target highest
                xdr_u32(&mut sequence, 0); // status flags
                Some(sequence)
            } else {
                let mut args = opcode.to_be_bytes().to_vec();
                args.extend_from_slice(&request);
                request = Bytes::from(args);
                None
            };
            let replies = handler(std::str::from_utf8(&tag).unwrap(), request);
            // Fixtures can explicitly return a failed SEQUENCE with no later ops.
            let sequence = if replies.first().is_some_and(|op| op.opcode == 53) {
                None
            } else {
                sequence
            };
            let mut reply = Vec::new();
            for word in [xid, 1, 0, 0, 0, 0] {
                xdr_u32(&mut reply, word);
            }
            xdr_u32(&mut reply, replies.last().map_or(0, |op| op.status));
            xdr_opaque(&mut reply, &tag);
            xdr_u32(
                &mut reply,
                u32::from(sequence.is_some()) + replies.len() as u32,
            );
            if let Some(sequence) = sequence {
                xdr_u32(&mut reply, 53);
                xdr_u32(&mut reply, 0);
                reply.extend(sequence);
            }
            for op in replies {
                xdr_u32(&mut reply, op.opcode);
                xdr_u32(&mut reply, op.status);
                reply.extend(op.data);
            }
            if let Some(gate) = reply_gate.take() {
                gate.await.unwrap();
            }
            stream
                .write_u32(0x80000000 | reply.len() as u32)
                .await
                .unwrap();
            stream.write_all(&reply).await.unwrap();
            after_reply(std::str::from_utf8(&tag).unwrap());
        }
    });
    let client = Client::new(StreamMux::connect(addr, true).await.unwrap(), None);
    let mut mount = test_mount(client, addr);
    if minor == super::MinorVersion::V2 {
        mount.client_identity = super::session::ClientIdentity::with_minor_version(minor);
        mount.session_holder = std::sync::Arc::new(super::session::SessionHolder::new(
            super::session::Session::for_v42_test(1),
        ));
        mount.rsize = 4096;
        mount.wsize = 4096;
    }
    (mount, server)
}

pub(super) fn directory_page(cookie: Option<u64>, eof: bool, verifier: [u8; 8]) -> Vec<u8> {
    let mut data = verifier.to_vec();
    xdr_u32(&mut data, u32::from(cookie.is_some()));
    if let Some(cookie) = cookie {
        data.extend(cookie.to_be_bytes());
        xdr_opaque(&mut data, b"entry");
        xdr_u32(&mut data, 1);
        xdr_u32(&mut data, 1 << 20);
        xdr_opaque(&mut data, &42u64.to_be_bytes());
        xdr_u32(&mut data, 0);
    }
    xdr_u32(&mut data, u32::from(eof));
    data
}

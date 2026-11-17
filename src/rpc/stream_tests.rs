#![cfg(test)]
//! Records over caller-supplied streams: a buffered writer must be flushed
//! before a record counts as sent.

use super::*;
use std::sync::Mutex as StdMutex;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{BufStream, DuplexStream, ReadBuf};

fn nfs_body() -> Vec<u8> {
    [RPC_VERSION, NFS_PROG, 4, 1]
        .into_iter()
        .flat_map(u32::to_be_bytes)
        .collect()
}

async fn read_record<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
    let marker = stream.read_u32().await?;
    let mut record = vec![0; (marker & 0x7fff_ffff) as usize];
    stream.read_exact(&mut record).await?;
    Ok(record)
}

async fn write_record<S: AsyncWrite + Unpin>(stream: &mut S, record: &[u8]) -> std::io::Result<()> {
    stream.write_u32(0x8000_0000 | record.len() as u32).await?;
    stream.write_all(record).await?;
    stream.flush().await
}

/// An accepted reply (RFC 5531 §9) with AUTH_NONE verifier and `payload`.
fn reply_record(xid: u32, payload: &[u8]) -> Vec<u8> {
    let mut reply: Vec<u8> = [
        xid,
        MessageType::Response as u32,
        MessageStatus::Accepted as u32,
        0,
        0,
        AcceptStatus::Success as u32,
    ]
    .into_iter()
    .flat_map(u32::to_be_bytes)
    .collect();
    reply.extend_from_slice(payload);
    reply
}

/// Reads until end of stream; `true` when the peer released its end.
async fn released(stream: &mut DuplexStream) -> bool {
    let mut sink = Vec::new();
    matches!(
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut sink)).await,
        Ok(Ok(_))
    )
}

fn mux_over(stream: BoxedRpcStream) -> Arc<StreamMux> {
    StreamMux::from_stream(
        stream,
        Endpoint::new("nfs.example.internal", 2049),
        Arc::new(crate::dialer::TcpDialer::new(true)),
    )
}

#[derive(Clone, Copy)]
enum FlushFault {
    Stall,
    Fail,
}

/// Forwards reads and writes; flush stalls or fails.
struct FaultyFlush {
    inner: DuplexStream,
    fault: FlushFault,
}

impl AsyncRead for FaultyFlush {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FaultyFlush {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.fault {
            FlushFault::Stall => Poll::Pending,
            FlushFault::Fail => Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn buffered_stream_carries_the_request() {
    let (client_end, mut server_end) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        let request = read_record(&mut server_end).await?;
        let xid = BigEndian::read_u32(&request[..4]);
        write_record(&mut server_end, &reply_record(xid, b"ok")).await?;
        Ok::<_, std::io::Error>(server_end)
    });
    let client = Client::new(mux_over(Box::new(BufStream::new(client_end))), None);

    let reply = client
        .call(
            nfs_body(),
            ReplayPolicy::ONE_ATTEMPT,
            Duration::from_secs(2),
        )
        .await
        .expect("a buffered request must reach the server");

    assert_eq!(reply, Bytes::from_static(b"ok"));
    server.await.unwrap().unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn buffered_stream_carries_the_backchannel_reply() {
    let (client_end, mut server_end) = tokio::io::duplex(4096);
    let mux = mux_over(Box::new(BufStream::new(client_end)));
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let seen = Arc::clone(&calls);
    mux.enable_backchannel(Arc::new(move |call: Bytes| {
        let xid = BigEndian::read_u32(&call[..4]);
        seen.lock().unwrap().push(xid);
        Some(reply_record(xid, b"cb"))
    }));
    let call: Vec<u8> = [77u32, MessageType::Request as u32]
        .into_iter()
        .flat_map(u32::to_be_bytes)
        .collect();
    write_record(&mut server_end, &call).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(2), read_record(&mut server_end))
        .await
        .expect("a buffered backchannel reply must reach the server")
        .unwrap();

    assert_eq!(reply, reply_record(77, b"cb"));
    assert_eq!(*calls.lock().unwrap(), vec![77]);
    mux.shutdown().await;
}

#[tokio::test]
async fn a_flush_that_does_not_finish_retires_the_connection() {
    for fault in [FlushFault::Stall, FlushFault::Fail] {
        let (client_end, mut server_end) = tokio::io::duplex(4096);
        let stream = FaultyFlush {
            inner: client_end,
            fault,
        };
        let client = Client::new(mux_over(Box::new(stream)), None);

        let error = client
            .call(
                nfs_body(),
                ReplayPolicy::ONE_ATTEMPT,
                Duration::from_millis(100),
            )
            .await
            .expect_err("a request whose flush never completes must fail");

        assert_eq!(
            error.request_transmission(),
            Some(crate::RequestTransmission::Sent)
        );
        // The record reached the stream, then the connection was dropped
        // rather than left for the next request.
        read_record(&mut server_end).await.unwrap();
        assert!(released(&mut server_end).await);
        client.shutdown().await;
    }
}
